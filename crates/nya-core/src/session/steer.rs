//! Health tick, speculative migrate, failback, same-link rebalance.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{debug, info, warn};

use crate::metrics::{mono_ms, path_state_label};

use nya_proto::ResetReason;

use crate::health;
use crate::path::PathState;
use crate::scheduler::{
    backup_prefer_class, failback_target, fastest_class_set, hol_place_bulk_fallback,
    should_rebalance_conn, FailbackReason,
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
        for p in &paths {
            if !p.is_alive() {
                continue;
            }
            let miss = p.expire_stale_pings(health::loss_timeout(&self.inner.cfg, p.stable_rtt()));
            if miss > 0 {
                self.inner
                    .metrics
                    .probe_miss
                    .fetch_add(miss, Ordering::Relaxed);
            }
            let ago = p.last_rx_ago();
            if ago >= self.down_for(p) {
                warn!(path = %p.name, ?ago, down = ?self.down_for(p), "path silent, marking down");
                self.path_failed(p.id);
            } else if ago >= self.degrade_for(p) {
                let ping_inflight = p
                    .pending_ping_age()
                    .map(|a| a < self.degrade_for(p))
                    .unwrap_or(false);
                if p.is_up() && !ping_inflight {
                    info!(path = %p.name, ?ago, "path silent, marking degraded");
                    p.mark_degraded();
                    self.inner
                        .metrics
                        .path_degraded
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        let streams: Vec<_> = self
            .inner
            .streams
            .lock()
            .unwrap()
            .values()
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

    fn maybe_speculative(&self, st: Arc<StreamState>) {
        if st.reset.load(Ordering::Relaxed) {
            return;
        }
        let sticky = st.sticky.load(Ordering::Relaxed);
        let cur_path = self.get_path(sticky);
        // Late unacked on a still-live path is usually HOL behind bulk, not
        // a dead link. Only restick when the path itself looks unhealthy.
        let path_unhealthy = match &cur_path {
            Some(p) => !p.is_schedulable(),
            None => true,
        };
        if path_unhealthy {
            if let Some(alt) = backup_prefer_class(&self.path_list(), sticky, &self.inner.cfg) {
                if alt != sticky {
                    let same_link = cur_path
                        .as_ref()
                        .and_then(|p| self.get_path(alt).map(|d| d.link() == p.link()))
                        .unwrap_or(false);
                    let cool = cur_path
                        .as_ref()
                        .map(|p| p.rtt().max(p.class_rtt()))
                        .unwrap_or(self.inner.cfg.tuning.failback_cooldown);
                    if same_link
                        || st.stick_changed_ago_ge(health::failback_cooldown(&self.inner.cfg, cool))
                    {
                        self.set_sticky(st.id, alt);
                        self.retransmit_all_on(&st, alt);
                        self.note_migrate("speculative");
                        if let Some(p) = cur_path.as_ref() {
                            if !p.is_up() {
                                self.observe_failover(p);
                            }
                        }
                        let from_state = cur_path
                            .as_ref()
                            .map(|p| path_state_label(p.state.load(Ordering::Relaxed)))
                            .unwrap_or("gone");
                        debug!(
                            stream_id = st.id,
                            from = sticky,
                            to = alt,
                            same_link,
                            from_state,
                            reason = "speculative",
                            "speculative migrate"
                        );
                        return;
                    }
                }
            }
        }
        {
            let mut unacked = st.unacked.lock().unwrap();
            for (offset, u) in unacked.iter_mut() {
                let thresh = self
                    .get_path(u.path_id)
                    .map(|p| self.degrade_for(&p))
                    .unwrap_or(self.inner.cfg.tuning.loss_timeout_floor);
                if u.last_sent.elapsed() < thresh {
                    continue;
                }
                // Retry in place. At most one hedge copy on the next-best
                // live path — never fan-out to every connection.
                u.last_sent = Instant::now();
                self.send_data_frame(st.id, *offset, u.data.clone(), u.path_id);
                self.inner
                    .metrics
                    .data_retransmit
                    .fetch_add(1, Ordering::Relaxed);
                if let Some(alt) =
                    backup_prefer_class(&self.path_list(), u.path_id, &self.inner.cfg)
                {
                    self.send_data_frame(st.id, *offset, u.data.clone(), alt);
                    self.inner
                        .metrics
                        .data_hedge
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    fn conn_has_interactive(&self, path_id: u32) -> bool {
        self.inner.streams.lock().unwrap().values().any(|st| {
            st.sticky.load(Ordering::Relaxed) == path_id && !st.bulk.load(Ordering::Relaxed)
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
        if st.reset.load(Ordering::Relaxed) {
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
        self.retransmit_all_on(st, dest);
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
        if st.reset.load(Ordering::Relaxed) {
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
        self.retransmit_all_on(&st, best_id);
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

    pub(super) fn migrate_from_path(&self, dead_path: u32) {
        let streams: Vec<_> = self
            .inner
            .streams
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect();
        let backup = {
            let paths = self.path_list();
            backup_prefer_class(&paths, dead_path, &self.inner.cfg)
        };
        let Some(backup) = backup else { return };
        for st in streams {
            if st.sticky.load(Ordering::Relaxed) != dead_path {
                continue;
            }
            self.set_sticky(st.id, backup);
            self.retransmit_from_on(&st, dead_path, backup);
            debug!(
                stream_id = st.id,
                from = dead_path,
                to = backup,
                reason = "path_down",
                "stream migrated"
            );
            self.note_migrate("path_down");
        }
    }

    fn scan_stall(&self, st: &StreamState) {
        if st.reset.load(Ordering::Relaxed) || st.counted_close.load(Ordering::Relaxed) {
            return;
        }
        let now = mono_ms();
        let thresh = match self.get_path(st.sticky.load(Ordering::Relaxed)) {
            Some(p) => self.degrade_for(&p),
            None => self.inner.cfg.tuning.loss_timeout_floor,
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
        let rtt = health::assumed_rtt(&self.inner.cfg, p.rtt_known(), p.rtt(), p.stable_rtt());
        health::down_timeout(
            &self.inner.cfg,
            rtt,
            health::probe_interval(&self.inner.cfg, rtt),
        )
    }

    pub fn probe_interval_for(&self, p: &PathState) -> Duration {
        // Fast RTT so a recovered path probes on its true timescale,
        // not on a spike-poisoned stable baseline.
        health::probe_interval(&self.inner.cfg, p.rtt())
    }
}
