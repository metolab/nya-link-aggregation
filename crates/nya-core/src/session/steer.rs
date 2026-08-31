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
    failback_target, fastest_class_set, hol_place_bulk_fallback, should_rebalance_conn,
    FailbackReason,
};
use crate::stream::StreamState;

use super::Session;

impl Session {
    #[cfg(test)]
    pub fn debug_maintain(&self) {
        self.maintain();
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
        // Membership at degrade_for so sequential down_for crossings still
        // form N−1; enter only once someone is actually at down_for (3-of-4
        // at ~50 ms must not start an 8 s episode). All-N still tears at
        // down_for — TCP RTO recovery is worse than a reconnect.
        let correlated = alive.len() >= 3
            && known_quiet >= 1
            && quiet.len() == alive.len() - 1
            && !silent.is_empty();
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
        for st in bulk.iter().chain(rest.iter()) {
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
            self.maybe_failback(st);
        }
        self.retry_opens();
        self.expire_early_data();

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
                let ids: Vec<u32> = self.inner.streams.lock().unwrap().keys().copied().collect();
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
            self.reset_stream(id, ResetReason::Timeout);
        }
    }

    fn maybe_speculative(&self, st: Arc<StreamState>) {
        self.retry_expired_unacked(&st);
    }

    fn conn_has_interactive(&self, path_id: u32) -> bool {
        self.inner.streams.lock().unwrap().values().any(|st| {
            st.is_steerable()
                && st.sticky.load(Ordering::Relaxed) == path_id
                && !st.bulk.load(Ordering::Relaxed)
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
                && p.is_schedulable()
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

        let send_origin = {
            let unacked = st.unacked.lock().unwrap();
            if unacked.is_empty() {
                None
            } else {
                let last_ack = st.last_ack_ms.load(Ordering::Relaxed);
                let origin = if last_ack != 0 {
                    last_ack
                } else {
                    let oldest = unacked
                        .values()
                        .map(|u| u.last_sent)
                        .min()
                        .unwrap_or_else(std::time::Instant::now);
                    now.saturating_sub(oldest.elapsed().as_millis() as u64)
                        .max(1)
                };
                if now.saturating_sub(origin) >= thresh_ms {
                    Some(origin)
                } else {
                    None
                }
            }
        };

        let recv_origin = {
            let buf = st.recv_buf.lock().unwrap();
            let recv_next = st.recv_next.load(Ordering::Relaxed);
            let hole = !buf.is_empty() && !buf.contains_key(&recv_next);
            if !hole {
                drop(buf);
                st.recv_hole_since_ms.store(0, Ordering::Relaxed);
                None
            } else {
                drop(buf);
                let last_recv = st.last_recv_ms.load(Ordering::Relaxed);
                let origin = if last_recv != 0 {
                    last_recv
                } else {
                    let since = st.recv_hole_since_ms.load(Ordering::Relaxed);
                    if since == 0 {
                        let v = now.max(1);
                        st.recv_hole_since_ms.store(v, Ordering::Relaxed);
                        v
                    } else {
                        since
                    }
                };
                if now.saturating_sub(origin) >= thresh_ms {
                    Some(origin)
                } else {
                    None
                }
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

    fn degrade_for(&self, p: &PathState) -> Duration {
        health::degrade_timeout(&self.inner.cfg, p.rtt_known(), p.stable_rtt())
    }

    fn down_for(&self, p: &PathState) -> Duration {
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
