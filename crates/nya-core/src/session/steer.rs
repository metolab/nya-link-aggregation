//! Health tick, speculative migrate, failback, same-link rebalance.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{info, warn};

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
                        self.inner.metrics.migrates.fetch_add(1, Ordering::Relaxed);
                        info!(
                            stream_id = st.id,
                            from = sticky,
                            to = alt,
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
                if let Some(alt) =
                    backup_prefer_class(&self.path_list(), u.path_id, &self.inner.cfg)
                {
                    self.send_data_frame(st.id, *offset, u.data.clone(), alt);
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
        self.set_sticky(st.id, dest);
        self.retransmit_all_on(st, dest);
        self.inner
            .metrics
            .hol_rebalances
            .fetch_add(1, Ordering::Relaxed);
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
        if cur.link() != best.link() {
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
        }
        let why = match reason {
            FailbackReason::Upgrade => "upgrade",
            FailbackReason::ClassEmpty => "class_empty",
        };
        info!(
            stream_id = st.id,
            from = sticky,
            to = best_id,
            from_rtt_us = cur.rtt().as_micros(),
            to_rtt_us = best.rtt().as_micros(),
            from_stable_us = cur.stable_rtt().as_micros(),
            from_class_us = cur.class_rtt().as_micros(),
            to_stable_us = best.stable_rtt().as_micros(),
            to_class_us = best.class_rtt().as_micros(),
            reason = why,
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
            info!(
                stream_id = st.id,
                from = dead_path,
                to = backup,
                "stream migrated"
            );
            self.inner.metrics.migrates.fetch_add(1, Ordering::Relaxed);
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
