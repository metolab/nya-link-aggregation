//! Health tick, speculative migrate, failback, same-link rebalance.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{debug, info, warn};

use crate::metrics::mono_ms;

use nya_proto::ResetReason;

use crate::health;
use crate::path::PathState;
use crate::scheduler::{
    failback_target, fastest_class_set, hol_bulk_dest_ok, hol_place_bulk_fallback,
    should_rebalance_conn, FailbackReason,
};
use crate::stream::StreamState;

use super::Session;

/// Hold silent TCPs instead of `path_failed` while a survivor exists.
///
/// - Exact N−1 quiet (original peer-stall).
/// - Or a cross-link cluster: ≥3 quiet spanning ≥2 named links.
///
/// All-N (`quiet == alive`) never holds. One named link (H8) never holds.
fn correlated_hold(
    alive: usize,
    quiet: usize,
    silent: usize,
    known_quiet: usize,
    quiet_links: usize,
) -> bool {
    if alive < 3 || known_quiet < 1 || silent == 0 || quiet >= alive {
        return false;
    }
    quiet + 1 == alive || (quiet >= 3 && quiet_links >= 2)
}

/// P1.8b: one open `quiet_set` episode — `quiet ≥ alive − 1`, all-N
/// included (unlike `correlated_hold`). Carries the per-path kernel
/// retransmit counters at entry so the exit event can print deltas: a
/// network episode shows non-zero deltas on the quiet paths; a local or
/// peer-side stall shows zero everywhere.
pub(super) struct QuietEpisode {
    since: Instant,
    quiet: Vec<String>,
    survivor: Vec<String>,
    all_n: bool,
    /// (path name, total_retrans, bytes_retrans) at entry.
    retrans_at_entry: Vec<(String, u32, u64)>,
}

/// `quiet ≥ alive − 1` with at least one quiet path and ≥ 2 alive.
fn quiet_set_holds(alive: usize, quiet: usize) -> bool {
    alive >= 2 && quiet >= 1 && quiet + 1 >= alive
}

fn retrans_of(paths: &[&Arc<PathState>]) -> Vec<(String, u32, u64)> {
    paths
        .iter()
        .map(|p| {
            let t = p.tcp_info().unwrap_or_default();
            (p.name.clone(), t.total_retrans, t.bytes_retrans)
        })
        .collect()
}

fn unique_link_count(paths: &[&Arc<PathState>]) -> usize {
    let mut links = std::collections::BTreeSet::new();
    for p in paths {
        links.insert(p.link());
    }
    links.len()
}

impl Session {
    #[cfg(test)]
    pub fn debug_maintain(&self) {
        self.maintain();
    }

    /// P1.8b correlated-silence discriminator. Logs `quiet_set` on entry
    /// (quiet ≥ alive − 1, all-N included) and `quiet_set_end` on exit with
    /// the episode length and per-path `TCP_INFO` retrans deltas. Both
    /// roles log; joining the two hosts' events by `session_fp` + wall
    /// clock tells network silence (peer sees the same subset, quiet paths
    /// retransmit) from a local or peer-side stall (peer sees all-N or
    /// nothing, deltas zero).
    fn track_quiet_set(&self, alive: &[&Arc<PathState>], quiet: &[&Arc<PathState>]) {
        let holds = quiet_set_holds(alive.len(), quiet.len());
        let mut g = self.inner.quiet_episode.lock().unwrap();
        match (&*g, holds) {
            (None, true) => {
                let quiet_names: Vec<String> = quiet.iter().map(|p| p.name.clone()).collect();
                let survivor: Vec<String> = alive
                    .iter()
                    .filter(|p| !quiet.iter().any(|q| q.id == p.id))
                    .map(|p| p.name.clone())
                    .collect();
                let all_n = quiet.len() == alive.len();
                info!(
                    session_fp = %self.session_fp().unwrap_or_default(),
                    wall_ms = crate::path::now_ms(),
                    alive = alive.len(),
                    quiet = ?quiet_names,
                    survivor = ?survivor,
                    all_n,
                    quiet_ago_ms = ?quiet
                        .iter()
                        .map(|p| p.last_rx_ago().as_millis() as u64)
                        .collect::<Vec<_>>(),
                    "quiet_set"
                );
                *g = Some(QuietEpisode {
                    since: Instant::now(),
                    quiet: quiet_names,
                    survivor,
                    all_n,
                    retrans_at_entry: retrans_of(alive),
                });
            }
            (Some(_), false) => {
                let ep = g.take().unwrap();
                let now = retrans_of(alive);
                let deltas: Vec<String> = ep
                    .retrans_at_entry
                    .iter()
                    .map(
                        |(name, seg0, bytes0)| match now.iter().find(|(n, _, _)| n == name) {
                            Some((_, seg1, bytes1)) => format!(
                                "{name}:+{}seg/+{}B",
                                seg1.saturating_sub(*seg0),
                                bytes1.saturating_sub(*bytes0)
                            ),
                            None => format!("{name}:gone"),
                        },
                    )
                    .collect();
                info!(
                    session_fp = %self.session_fp().unwrap_or_default(),
                    wall_ms = crate::path::now_ms(),
                    dur_ms = ep.since.elapsed().as_millis() as u64,
                    quiet = ?ep.quiet,
                    survivor = ?ep.survivor,
                    all_n = ep.all_n,
                    retrans_delta = ?deltas,
                    "quiet_set_end"
                );
            }
            _ => {}
        }
    }

    pub(super) fn spawn_maintenance(&self) {
        let session = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(session.config().tuning.maintain_interval);
            loop {
                tokio::select! {
                    _ = session.wait_dead() => break,
                    _ = tick.tick() => session.maintain(),
                }
            }
        });
    }

    fn maintain(&self) {
        if self.is_dead() {
            return;
        }
        let paths = self.path_list();
        let mut miss_by_id = std::collections::HashMap::new();
        for p in &paths {
            if !p.is_alive() {
                continue;
            }
            let loss_for = health::loss_timeout(&self.inner.cfg, p.stable_rtt());
            let miss = p.expire_stale_pings(loss_for);
            if miss > 0 {
                self.inner
                    .metrics
                    .probe_miss
                    .fetch_add(miss, Ordering::Relaxed);
            }
            p.drop_ancient_pings(self.inner.cfg.tuning.ack_rtt_max);
            miss_by_id.insert(p.id, miss);
        }

        let alive: Vec<&Arc<PathState>> = paths.iter().filter(|p| p.is_alive()).collect();
        let quiet: Vec<&Arc<PathState>> = alive
            .iter()
            .copied()
            .filter(|p| p.last_rx_ago() >= self.degrade_for(p))
            .collect();
        let silent: Vec<&Arc<PathState>> = alive
            .iter()
            .copied()
            .filter(|p| p.last_rx_ago() >= self.down_for(p))
            .collect();
        let known_quiet = quiet.iter().filter(|p| p.rtt_known()).count();
        let quiet_links = unique_link_count(&quiet);
        // Membership at degrade_for so sequential down_for crossings still
        // form a hold; enter only once someone is actually at down_for
        // (3-of-4 at ~50 ms must not start an 8 s episode). All-N still
        // tears at down_for — TCP RTO recovery is worse than a reconnect.
        // Exact N−1 is the original peer-stall shape. A 3×2 pool also
        // holds a cross-link cluster (quiet ≥ 3 spanning ≥ 2 named links)
        // while any survivor remains — otherwise 4-of-6 congestion looks
        // like independent deaths and reconnects into an unknown-RTT storm.
        let correlated = correlated_hold(
            alive.len(),
            quiet.len(),
            silent.len(),
            known_quiet,
            quiet_links,
        );
        {
            let mut g = self.inner.correlated_since.lock().unwrap();
            if correlated {
                if g.is_none() {
                    *g = Some(Instant::now());
                    info!(
                        quiet = quiet.len(),
                        silent = silent.len(),
                        alive = alive.len(),
                        known_quiet,
                        quiet_links,
                        budget_ms = self.inner.cfg.all_down_timeout.as_millis() as u64,
                        "correlated silence"
                    );
                    self.inner
                        .metrics
                        .correlated_silence
                        .fetch_add(1, Ordering::Relaxed);
                }
            } else {
                *g = None;
            }
        }
        self.track_quiet_set(&alive, &quiet);
        let budget_elapsed = self
            .inner
            .correlated_since
            .lock()
            .unwrap()
            .map(|t| t.elapsed() >= self.inner.cfg.all_down_timeout)
            .unwrap_or(false);

        for p in &paths {
            if !p.is_alive() {
                continue;
            }
            let ago = p.last_rx_ago();
            let silent_this = ago >= self.down_for(p);
            let tear = silent_this && (!p.rtt_known() || !correlated || budget_elapsed);
            if tear {
                warn!(path = %p.name, ?ago, down = ?self.down_for(p), "path silent, marking down");
                self.path_failed(p.id);
            } else if p.is_up()
                && health::should_mark_degraded(
                    ago,
                    self.degrade_for(p),
                    miss_by_id.get(&p.id).copied().unwrap_or(0),
                    p.pending_ping_count(),
                )
            {
                debug!(
                    path = %p.name,
                    ?ago,
                    degrade = ?self.degrade_for(p),
                    "path silent, marking degraded"
                );
                p.mark_degraded();
                self.inner
                    .metrics
                    .path_degraded
                    .fetch_add(1, Ordering::Relaxed);
            }
        }

        if budget_elapsed && !self.has_alive_path() {
            let ids: Vec<u32> = self.inner.streams.lock().unwrap().keys().copied().collect();
            if !ids.is_empty() {
                warn!(
                    count = ids.len(),
                    "correlated silence past timeout, resetting streams"
                );
                self.inner
                    .metrics
                    .session_all_down_resets
                    .fetch_add(1, Ordering::Relaxed);
                for id in ids {
                    self.reset_stream(id, ResetReason::Timeout);
                }
            }
        }

        if self.inner.is_client {
            self.maybe_recycle_outliers();
        }

        self.reap_closed_streams();
        self.tune_recv_windows();
        self.inner.metrics.recv_cap_extra_bytes.store(
            self.inner.recv_cap_extra.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        let streams: Vec<_> = self
            .inner
            .streams
            .lock()
            .unwrap()
            .values()
            .filter(|st| st.is_steerable())
            .cloned()
            .collect();
        let mut bulk = Vec::new();
        let mut rest = Vec::new();
        for st in streams {
            if st.bulk.load(Ordering::Relaxed) {
                bulk.push(st);
            } else {
                rest.push(st);
            }
        }
        for st in bulk.iter().chain(rest.iter()) {
            self.maybe_hol(st);
        }
        let mut stalled = 0u64;
        for st in bulk.iter() {
            self.debug_bulk_stream(st);
        }
        for st in bulk.iter().chain(rest.iter()) {
            self.debug_zero_window(st);
            self.scan_stall(st);
            if st.stalled.load(Ordering::Relaxed) {
                stalled += 1;
            }
        }
        self.inner
            .metrics
            .streams_stalled
            .store(stalled, Ordering::Relaxed);
        for st in bulk.into_iter().chain(rest) {
            self.maybe_speculative(st.clone());
        }
        self.retry_opens();
        self.retry_closes();
        self.retry_resets();
        self.expire_early_data();
        self.expire_recv_closes();

        let all_down = !self.has_alive_path();
        if all_down {
            let since = {
                let mut g = self.inner.all_down_since.lock().unwrap();
                if g.is_none() {
                    *g = Some(Instant::now());
                }
                g.unwrap()
            };
            if since.elapsed() >= self.inner.cfg.all_down_timeout {
                if self.inner.reap_on_all_down.load(Ordering::Relaxed) {
                    let n = self.inner.streams.lock().unwrap().len();
                    warn!(count = n, "all paths down past timeout, ending session");
                    self.inner
                        .metrics
                        .session_all_down_resets
                        .fetch_add(1, Ordering::Relaxed);
                    self.shutdown();
                } else {
                    let ids: Vec<u32> =
                        self.inner.streams.lock().unwrap().keys().copied().collect();
                    if !ids.is_empty() {
                        warn!(
                            count = ids.len(),
                            "all paths down past timeout, resetting streams"
                        );
                        self.inner
                            .metrics
                            .session_all_down_resets
                            .fetch_add(1, Ordering::Relaxed);
                        for id in ids {
                            self.reset_stream(id, ResetReason::Timeout);
                        }
                    }
                }
            }
        } else {
            *self.inner.all_down_since.lock().unwrap() = None;
        }
    }

    fn maybe_recycle_outliers(&self) {
        let paths = self.path_list();
        let hold = self.inner.cfg.tuning.stable_up_hold;
        let mut recycle = Vec::new();
        for p in &paths {
            if !p.is_up() || !p.class_known() || !p.class_known_aged(hold) {
                p.clear_outlier();
                continue;
            }
            let best_sib = paths
                .iter()
                .filter(|q| q.id != p.id && q.is_up() && q.class_known() && q.link() == p.link())
                .map(|q| q.class_rtt())
                .min();
            let Some(sib) = best_sib else {
                p.clear_outlier();
                continue;
            };
            // Class-only backup races the H5/G4a walk: one 7/8 that
            // crosses the cliff is still backup for ~8 holds, so G4b
            // always won. Recycle only if fast agrees the 5-tuple is
            // still slow; recovered fast clears the timer (H6).
            if health::is_backup(&self.inner.cfg, p.class_rtt(), sib)
                && health::is_backup(&self.inner.cfg, p.rtt(), sib)
            {
                if p.mark_outlier() >= hold {
                    recycle.push(p.id);
                }
            } else {
                p.clear_outlier();
            }
        }
        for id in recycle {
            if let Some(p) = self.get_path(id) {
                info!(
                    path = %p.name,
                    class_us = p.class_rtt().as_micros() as u64,
                    "outlier recycle"
                );
                self.inner
                    .metrics
                    .path_outlier_recycle
                    .fetch_add(1, Ordering::Relaxed);
                self.path_failed(id);
            }
        }
    }

    fn reap_closed_streams(&self) {
        let now = mono_ms();
        let linger_ms = self.inner.cfg.tuning.close_linger.as_millis() as u64;
        let mut drop_ids = Vec::new();
        let mut timeout_ids = Vec::new();
        {
            let g = self.inner.streams.lock().unwrap();
            for st in g.values() {
                if st.counted_close.load(Ordering::Relaxed) || st.reset.load(Ordering::Relaxed) {
                    drop_ids.push(st.id);
                    continue;
                }
                if !st.send_fin_sent.load(Ordering::Relaxed) && !st.recv_fin.load(Ordering::Relaxed)
                {
                    continue;
                }
                let start = st.close_started_ms.load(Ordering::Relaxed);
                if start != 0 && now.saturating_sub(start) >= linger_ms {
                    timeout_ids.push(st.id);
                }
            }
        }
        for id in drop_ids {
            self.remove_held_stream(id);
        }
        for id in timeout_ids {
            match self.get_stream(id) {
                Some(st)
                    if self.overlay_progress_fine(&st) && st.recv_fin.load(Ordering::Relaxed) =>
                {
                    self.linger_reap_progress_fine(id);
                }
                Some(st)
                    if self.inner.is_client
                        && self.overlay_progress_fine(&st)
                        && !st.recv_fin.load(Ordering::Relaxed) =>
                {
                    self.residual_d_client(id);
                }
                Some(st)
                    if !self.inner.is_client
                        && self.overlay_progress_fine(&st)
                        && !st.recv_fin.load(Ordering::Relaxed) =>
                {
                    self.linger_reap_progress_fine(id);
                }
                Some(_) => self.reset_stream(id, ResetReason::Timeout),
                None => {}
            }
        }
    }

    fn maybe_speculative(&self, st: Arc<StreamState>) {
        self.retry_expired_unacked(&st);
    }

    pub(super) fn conn_has_interactive(&self, path_id: u32) -> bool {
        let now = mono_ms();
        let linger_ms = self.inner.cfg.tuning.close_linger.as_millis() as u64;
        self.inner.streams.lock().unwrap().values().any(|st| {
            if !st.is_steerable()
                || st.sticky.load(Ordering::Relaxed) != path_id
                || st.bulk.load(Ordering::Relaxed)
            {
                return false;
            }
            // Stall-only belt: unstalled hangover still pins until bounce.
            let stalled = st.stalled.load(Ordering::Relaxed);
            let from = st.stall_from_ms.load(Ordering::Relaxed);
            !(stalled && from != 0 && now.saturating_sub(from) >= linger_ms)
        })
    }

    pub(super) fn hol_place_bulk(&self, cur_id: u32) -> Option<u32> {
        if !self.conn_has_interactive(cur_id) {
            return None;
        }
        let paths = self.path_list();
        let cur = self.get_path(cur_id)?;
        if let Some(sib) = paths.iter().find(|p| {
            p.id != cur.id
                && p.link() == cur.link()
                && hol_bulk_dest_ok(&self.inner.cfg, p)
                && !self.conn_has_interactive(p.id)
        }) {
            return Some(sib.id);
        }
        hol_place_bulk_fallback(&paths, &cur, &self.inner.cfg, |id| {
            self.conn_has_interactive(id)
        })
    }

    fn maybe_hol(&self, st: &StreamState) {
        if !st.is_steerable() {
            return;
        }
        let cur_id = st.sticky.load(Ordering::Relaxed);
        let Some(cur) = self.get_path(cur_id) else {
            return;
        };
        let dest = if st.bulk.load(Ordering::Relaxed) {
            self.hol_place_bulk(cur_id)
        } else {
            let paths = self.path_list();
            paths.iter().find_map(|p| {
                if should_rebalance_conn(&cur, p, &self.inner.cfg) {
                    Some(p.id)
                } else {
                    None
                }
            })
        };
        let Some(dest) = dest else {
            return;
        };
        if dest == cur_id {
            return;
        }
        let to_path = self.get_path(dest);
        let from_inflight = cur.inflight_bytes();
        let to_inflight = to_path.as_ref().map(|p| p.inflight_bytes()).unwrap_or(0);
        let from_sticky = cur.sticky_count();
        let to_sticky = to_path.as_ref().map(|p| p.sticky_count()).unwrap_or(0);
        self.set_sticky(st.id, dest);
        self.inner
            .metrics
            .hol_rebalances
            .fetch_add(1, Ordering::Relaxed);
        let reason = if st.bulk.load(Ordering::Relaxed) {
            "hol_bulk"
        } else {
            "hol_rebalance"
        };
        debug!(
            stream_id = st.id,
            from = cur_id,
            to = dest,
            from_inflight,
            to_inflight,
            from_sticky,
            to_sticky,
            reason,
            "hol"
        );
    }

    #[allow(dead_code)]
    fn maybe_failback(&self, st: Arc<StreamState>) {
        if !st.is_steerable() {
            return;
        }
        let sticky = st.sticky.load(Ordering::Relaxed);
        let Some(cur) = self.get_path(sticky) else {
            return;
        };
        let cool = cur.rtt().max(cur.class_rtt());
        if !st.stick_changed_ago_ge(health::failback_cooldown(&self.inner.cfg, cool)) {
            return;
        }
        let paths = self.path_list();
        let Some((best_id, reason)) = failback_target(&paths, &cur, &self.inner.cfg) else {
            return;
        };
        if best_id == sticky {
            return;
        }
        // Bulk stays on a slower class. Interactive leaving slow is the
        // p50 path; bulk following it was Upgrade chatter without helping ping.
        if reason == FailbackReason::Upgrade && st.bulk.load(Ordering::Relaxed) {
            let fastest = fastest_class_set(&paths, &self.inner.cfg);
            if !fastest.iter().any(|p| p.id == cur.id) {
                return;
            }
        }
        let Some(best) = self.get_path(best_id) else {
            return;
        };
        self.set_sticky(st.id, best_id);
        let cross_link = cur.link() != best.link();
        if cross_link {
            self.inner.metrics.failbacks.fetch_add(1, Ordering::Relaxed);
            match reason {
                FailbackReason::Upgrade => {
                    self.inner
                        .metrics
                        .failbacks_upgrade
                        .fetch_add(1, Ordering::Relaxed);
                }
                FailbackReason::ClassEmpty => {
                    self.inner
                        .metrics
                        .failbacks_class_empty
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        } else {
            self.inner
                .metrics
                .failbacks_same_link
                .fetch_add(1, Ordering::Relaxed);
        }
        let why = match reason {
            FailbackReason::Upgrade => "upgrade",
            FailbackReason::ClassEmpty => "class_empty",
        };
        debug!(
            stream_id = st.id,
            from = sticky,
            to = best_id,
            from_rtt_us = cur.rtt().as_micros() as u64,
            to_rtt_us = best.rtt().as_micros() as u64,
            from_stable_us = cur.stable_rtt().as_micros() as u64,
            from_class_us = cur.class_rtt().as_micros() as u64,
            to_stable_us = best.stable_rtt().as_micros() as u64,
            to_class_us = best.class_rtt().as_micros() as u64,
            reason = why,
            cross_link,
            "failback"
        );
    }

    fn scan_stall(&self, st: &StreamState) {
        if !st.is_steerable() {
            return;
        }
        let now = mono_ms();
        let thresh = {
            let unacked = st.unacked.lock().unwrap();
            unacked
                .values()
                .filter_map(|u| self.get_path(u.path_id))
                .map(|p| health::loss_timeout(&self.inner.cfg, p.rtt()))
                .min()
                .unwrap_or(self.inner.cfg.tuning.loss_timeout_floor)
        };
        let thresh_ms = thresh.as_millis() as u64;

        // A stall is "something outstanding has not progressed for thresh".
        // The clock starts at the later of the last progress and the moment
        // the oldest outstanding item appeared; measuring from progress alone
        // turns every burst of a paced source (slow origin, video) into a
        // stall and drives speculative rehome/belt off idle gaps.
        let send_origin = {
            let unacked = st.unacked.lock().unwrap();
            if unacked.is_empty() {
                None
            } else {
                let oldest = unacked
                    .values()
                    .map(|u| u.first_sent)
                    .min()
                    .unwrap_or_else(Instant::now);
                let oldest_ms = now
                    .saturating_sub(oldest.elapsed().as_millis() as u64)
                    .max(1);
                let origin = st.last_ack_ms.load(Ordering::Relaxed).max(oldest_ms);
                (now.saturating_sub(origin) >= thresh_ms).then_some(origin)
            }
        };

        let recv_origin = {
            let hole = {
                let buf = st.recv_buf.lock().unwrap();
                let recv_next = st.recv_next.load(Ordering::Relaxed);
                !buf.is_empty() && !buf.contains_key(&recv_next)
            };
            if !hole {
                st.recv_hole_since_ms.store(0, Ordering::Relaxed);
                None
            } else {
                // Prefer the P1.4 hole tracker's open time; fall back to the
                // first scan that saw the hole.
                let opened_ms = match *st.hole.lock().unwrap() {
                    Some((_, t)) => now.saturating_sub(t.elapsed().as_millis() as u64).max(1),
                    None => {
                        let since = st.recv_hole_since_ms.load(Ordering::Relaxed);
                        if since == 0 {
                            let v = now.max(1);
                            st.recv_hole_since_ms.store(v, Ordering::Relaxed);
                            v
                        } else {
                            since
                        }
                    }
                };
                let origin = st.last_recv_ms.load(Ordering::Relaxed).max(opened_ms);
                (now.saturating_sub(origin) >= thresh_ms).then_some(origin)
            }
        };

        let origin = match (send_origin, recv_origin) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) | (None, Some(a)) => Some(a),
            (None, None) => None,
        };
        let predicate = origin.is_some();
        let was = st.stalled.load(Ordering::Relaxed);
        if predicate && !was {
            st.stall_from_ms
                .store(origin.unwrap_or(now), Ordering::Relaxed);
            st.stalled.store(true, Ordering::Relaxed);
            // P1.7: what kind of stall this is, at entry.
            let m = &self.inner.metrics;
            let kind = match (send_origin.is_some(), recv_origin.is_some()) {
                (true, true) => &m.stall_enter_both,
                (true, false) => {
                    if st.send_window.load(Ordering::Relaxed) == 0 || !st.window_ok(1) {
                        &m.stall_enter_send_zero_window
                    } else {
                        &m.stall_enter_send
                    }
                }
                _ => &m.stall_enter_recv_hole,
            };
            kind.fetch_add(1, Ordering::Relaxed);
            if tracing::enabled!(tracing::Level::DEBUG) {
                let (buf_len, buf_head) = {
                    let buf = st.recv_buf.lock().unwrap();
                    (buf.len(), buf.keys().next().copied())
                };
                debug!(
                    stream = st.id,
                    send = send_origin.is_some(),
                    recv = recv_origin.is_some(),
                    age_ms = now.saturating_sub(origin.unwrap_or(now)),
                    thresh_ms,
                    recv_next = st.recv_next.load(Ordering::Relaxed),
                    buf_len,
                    buf_head,
                    unacked = st.unacked.lock().unwrap().len(),
                    send_window = st.send_window.load(Ordering::Relaxed),
                    "stall_enter"
                );
            }
        } else if !predicate && was {
            let from = st.stall_from_ms.load(Ordering::Relaxed);
            if from != 0 {
                self.inner
                    .metrics
                    .stall_ms
                    .observe(now.saturating_sub(from));
            }
            st.stalled.store(false, Ordering::Relaxed);
            st.stall_from_ms.store(0, Ordering::Relaxed);
        }
    }

    /// Twice a second per bulk stream: the limiter numbers on this side
    /// (peer window, path budgets, in-flight, blocks), so a slow transfer
    /// can be attributed from the log of either end.
    fn debug_bulk_stream(&self, st: &StreamState) {
        if !tracing::enabled!(tracing::Level::DEBUG) {
            return;
        }
        let now_ms = crate::metrics::mono_ms();
        let last = st.bulk_logged_ms.load(Ordering::Relaxed);
        if last != 0 && now_ms.saturating_sub(last) < 500 {
            return;
        }
        st.bulk_logged_ms.store(now_ms.max(1), Ordering::Relaxed);
        let paths: Vec<String> = self
            .path_list()
            .iter()
            .filter(|p| p.is_alive())
            .map(|p| {
                format!(
                    "{}:bud={}k inf={}k bw={}k/s loop={}ms",
                    p.name,
                    p.budget_bytes() / 1024,
                    p.inflight_bytes() / 1024,
                    p.bw_bytes_s() / 1024,
                    p.ack_rtt().map(|d| d.as_millis()).unwrap_or(0)
                )
            })
            .collect();
        debug!(
            stream = st.id,
            client = self.inner.is_client,
            send_window = st.send_window.load(Ordering::Relaxed),
            inflight_send = st.inflight_send(),
            sticky = st.sticky.load(Ordering::Relaxed),
            window_blocks = st.window_blocks.load(Ordering::Relaxed),
            budget_blocks = st.budget_blocks.load(Ordering::Relaxed),
            recv_cap = st.recv_cap.load(Ordering::Relaxed),
            buffered_in = st.buffered_in.load(Ordering::Relaxed),
            recv_buffered = st.recv_buffered.load(Ordering::Relaxed),
            paths = %paths.join(" "),
            "bulk stream"
        );
    }

    /// Once a second while a stream sits on a zero window in either
    /// direction: the numbers that decide the window, so a stuck transfer
    /// can be read from the log instead of guessed at.
    fn debug_zero_window(&self, st: &StreamState) {
        if !tracing::enabled!(tracing::Level::DEBUG) {
            return;
        }
        let adv = st.advertised_window();
        let send_window = st.send_window.load(Ordering::Relaxed);
        if adv != 0 && send_window != 0 {
            st.stuck_logged_ms.store(0, Ordering::Relaxed);
            return;
        }
        let now_ms = crate::metrics::mono_ms();
        let last = st.stuck_logged_ms.load(Ordering::Relaxed);
        if last != 0 && now_ms.saturating_sub(last) < 1000 {
            return;
        }
        st.stuck_logged_ms.store(now_ms.max(1), Ordering::Relaxed);
        let last_recv_path = st.last_recv_path.load(Ordering::Relaxed);
        let unacked = st.unacked.lock().unwrap().len();
        debug!(
            stream = st.id,
            client = self.inner.is_client,
            send_window,
            send_next = st.send_next.load(Ordering::Relaxed),
            send_acked = st.send_acked.load(Ordering::Relaxed),
            unacked,
            recv_cap = st.recv_cap.load(Ordering::Relaxed),
            buffered_in = st.buffered_in.load(Ordering::Relaxed),
            recv_buffered = st.recv_buffered.load(Ordering::Relaxed),
            recv_next = st.recv_next.load(Ordering::Relaxed),
            advertised = adv,
            last_recv_path,
            last_recv_path_alive = self.get_path(last_recv_path).is_some_and(|p| p.is_alive()),
            sticky = st.sticky.load(Ordering::Relaxed),
            ack_dirty = st.ack_dirty.load(Ordering::Relaxed),
            "zero window"
        );
    }

    pub(super) fn degrade_for(&self, p: &PathState) -> Duration {
        health::degrade_timeout(&self.inner.cfg, p.rtt_known(), p.stable_rtt())
    }

    /// P7: bulk-destination freshness; the complement of `maintain`'s
    /// quiet set. See [`crate::scheduler::is_quiet_fresh`].
    pub(super) fn is_quiet_fresh(&self, p: &PathState) -> bool {
        crate::scheduler::is_quiet_fresh(&self.inner.cfg, p)
    }

    pub(super) fn down_for(&self, p: &PathState) -> Duration {
        // `assumed_rtt` is max(fast, stable) when known, so a spike can
        // already lift down. Do not also feed probe_interval_for
        // (min(fast, stable) / unknown ping_min) into this probe term —
        // that would shrink unknown 550ms → 510ms.
        let rtt = health::assumed_rtt(&self.inner.cfg, p.rtt_known(), p.rtt(), p.stable_rtt());
        health::down_timeout(
            &self.inner.cfg,
            rtt,
            health::probe_interval(&self.inner.cfg, rtt),
        )
    }

    pub fn probe_interval_for(&self, p: &PathState) -> Duration {
        if !p.rtt_known() {
            // First Pong as soon as the operator min allows. Unknown must
            // not wait 20ms (placeholder) or 50ms (assumed) before asking.
            return self.inner.cfg.ping_interval_min;
        }
        health::probe_interval(&self.inner.cfg, health::probe_rtt(p.rtt(), p.stable_rtt()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// P1.8b: `quiet_set` includes all-N and exact N−1; `correlated_hold`
    /// never holds on all-N. The two must differ exactly there.
    #[test]
    fn quiet_set_vs_correlated_hold() {
        assert!(quiet_set_holds(4, 4), "all-N is a quiet_set");
        assert!(quiet_set_holds(4, 3));
        assert!(!quiet_set_holds(4, 2));
        assert!(quiet_set_holds(2, 1));
        assert!(!quiet_set_holds(1, 1), "a lone path cannot form a set");
        assert!(!quiet_set_holds(3, 0));
        assert!(
            !correlated_hold(4, 4, 4, 4, 2),
            "correlated_hold never all-N"
        );
        assert!(correlated_hold(4, 3, 1, 3, 2));
    }
}
