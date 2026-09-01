//! Session and process counters, histograms, and snapshots.
//!
//! Histograms store *raw* (non-cumulative) bucket counts so snapshots can
//! `merge_add`. Prometheus exposition cumulates at export time.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use tracing::debug;

use crate::hop::{HopRole, HopSample};

use super::path::{link_key, PathState, STATE_DEGRADED, STATE_DOWN, STATE_UP};

pub const STREAM_SNAP_CAP: usize = 64;
pub const FAILOVER_MS_BOUNDS: &[u64] = &[5, 10, 20, 50, 100, 200, 500, 1000, 2000];
pub const STALL_MS_BOUNDS: &[u64] = &[20, 50, 100, 200, 500, 1000, 2000, 5000, 10000];
pub const LIFETIME_MS_BOUNDS: &[u64] = &[100, 500, 1000, 5000, 30_000, 60_000, 300_000];

fn epoch() -> Instant {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    *EPOCH.get_or_init(Instant::now)
}

/// Milliseconds since process start. Never 0 after the first millisecond.
pub fn mono_ms() -> u64 {
    epoch().elapsed().as_millis() as u64
}

pub fn path_state_label(state: u8) -> &'static str {
    match state {
        STATE_UP => "up",
        STATE_DEGRADED => "deg",
        STATE_DOWN => "down",
        _ => "gone",
    }
}

pub struct Histogram {
    bounds: &'static [u64],
    buckets: Box<[AtomicU64]>,
    sum: AtomicU64,
    count: AtomicU64,
}

impl Histogram {
    pub fn new(bounds: &'static [u64]) -> Self {
        let n = bounds.len() + 1;
        Self {
            bounds,
            buckets: (0..n).map(|_| AtomicU64::new(0)).collect::<Vec<_>>().into(),
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    pub fn observe(&self, v: u64) {
        let mut i = self.bounds.len();
        for (idx, &b) in self.bounds.iter().enumerate() {
            if v <= b {
                i = idx;
                break;
            }
        }
        self.buckets[i].fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(v, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Always copies every bucket, including zeros. `count == 0` is a full
    /// zero vec, not an empty vec.
    pub fn snap(&self) -> HistSnap {
        HistSnap {
            buckets: self
                .buckets
                .iter()
                .map(|b| b.load(Ordering::Relaxed))
                .collect(),
            sum: self.sum.load(Ordering::Relaxed),
            count: self.count.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HistSnap {
    /// Raw (non-cumulative) counts. Empty `Default` is not mergeable.
    pub buckets: Vec<u64>,
    pub sum: u64,
    pub count: u64,
}

impl HistSnap {
    pub fn zeroed(bounds: &'static [u64]) -> Self {
        Self {
            buckets: vec![0; bounds.len() + 1],
            sum: 0,
            count: 0,
        }
    }

    /// Length mismatch or empty buckets: no-op.
    pub fn merge_add(&mut self, other: &HistSnap) {
        if self.buckets.is_empty() || other.buckets.is_empty() {
            return;
        }
        if self.buckets.len() != other.buckets.len() {
            debug_assert!(
                false,
                "HistSnap::merge_add length mismatch: {} vs {}",
                self.buckets.len(),
                other.buckets.len()
            );
            return;
        }
        for (a, b) in self.buckets.iter_mut().zip(other.buckets.iter()) {
            *a += *b;
        }
        self.sum += other.sum;
        self.count += other.count;
    }
}

/// `p` ∈ (0, 100]. `count == 0` or empty buckets → None.
/// Linear interpolation inside the hit raw bucket. The +Inf bucket returns
/// that bucket's lower bound (last finite bound).
pub fn percentile(snap: &HistSnap, bounds: &[u64], p: f64) -> Option<u64> {
    if snap.count == 0 || snap.buckets.is_empty() || p <= 0.0 {
        return None;
    }
    let p = p.min(100.0);
    let target = ((p / 100.0) * snap.count as f64).ceil().max(1.0) as u64;
    let mut acc = 0u64;
    for (i, &c) in snap.buckets.iter().enumerate() {
        acc = acc.saturating_add(c);
        if acc >= target {
            if i >= bounds.len() {
                return bounds.last().copied();
            }
            let lower = if i == 0 { 0 } else { bounds[i - 1] };
            let upper = bounds[i];
            let before = acc.saturating_sub(c);
            let need = target.saturating_sub(before).max(1);
            if c == 0 {
                return Some(upper);
            }
            let frac = (need as f64 / c as f64).min(1.0);
            let v = lower as f64 + (upper.saturating_sub(lower) as f64) * frac;
            return Some(v.round() as u64);
        }
    }
    bounds.last().copied()
}

pub struct Counters {
    pub path_added: AtomicU64,
    pub path_down: AtomicU64,
    pub path_degraded: AtomicU64,
    pub path_outlier_recycle: AtomicU64,
    pub correlated_silence: AtomicU64,
    pub migrates: AtomicU64,
    pub migrates_speculative: AtomicU64,
    pub migrates_path_down: AtomicU64,
    pub migrates_ensure_sticky: AtomicU64,
    pub migrates_send_blocked: AtomicU64,
    pub data_retransmit: AtomicU64,
    pub data_hedge: AtomicU64,
    pub close_retry: AtomicU64,
    pub probe_miss: AtomicU64,
    pub window_blocks: AtomicU64,
    pub picks_unknown_rtt: AtomicU64,
    pub picks_unknown_over_known: AtomicU64,
    pub failbacks: AtomicU64,
    pub failbacks_upgrade: AtomicU64,
    pub failbacks_class_empty: AtomicU64,
    pub failbacks_same_link: AtomicU64,
    pub hol_rebalances: AtomicU64,
    pub streams_opened: AtomicU64,
    pub streams_closed: AtomicU64,
    pub stream_reaps_linger: AtomicU64,
    pub stream_resets: AtomicU64,
    pub stream_resets_dial_failed: AtomicU64,
    pub stream_resets_timeout: AtomicU64,
    pub stream_resets_peer: AtomicU64,
    pub stream_resets_session_dead: AtomicU64,
    pub stream_resets_protocol: AtomicU64,
    pub bytes_data_tx: AtomicU64,
    pub bytes_data_rx: AtomicU64,
    pub bytes_ctrl_tx: AtomicU64,
    pub bytes_ctrl_rx: AtomicU64,
    pub frame_send_drop: AtomicU64,
    pub session_all_down_resets: AtomicU64,
    /// Gauge: number of streams currently stalled (store, not add).
    pub streams_stalled: AtomicU64,
    pub failover_ms: Histogram,
    pub stall_ms: Histogram,
    pub stream_lifetime_ms: Histogram,
}

impl Default for Counters {
    fn default() -> Self {
        Self {
            path_added: AtomicU64::new(0),
            path_down: AtomicU64::new(0),
            path_degraded: AtomicU64::new(0),
            path_outlier_recycle: AtomicU64::new(0),
            correlated_silence: AtomicU64::new(0),
            migrates: AtomicU64::new(0),
            migrates_speculative: AtomicU64::new(0),
            migrates_path_down: AtomicU64::new(0),
            migrates_ensure_sticky: AtomicU64::new(0),
            migrates_send_blocked: AtomicU64::new(0),
            data_retransmit: AtomicU64::new(0),
            data_hedge: AtomicU64::new(0),
            close_retry: AtomicU64::new(0),
            probe_miss: AtomicU64::new(0),
            window_blocks: AtomicU64::new(0),
            picks_unknown_rtt: AtomicU64::new(0),
            picks_unknown_over_known: AtomicU64::new(0),
            failbacks: AtomicU64::new(0),
            failbacks_upgrade: AtomicU64::new(0),
            failbacks_class_empty: AtomicU64::new(0),
            failbacks_same_link: AtomicU64::new(0),
            hol_rebalances: AtomicU64::new(0),
            streams_opened: AtomicU64::new(0),
            streams_closed: AtomicU64::new(0),
            stream_reaps_linger: AtomicU64::new(0),
            stream_resets: AtomicU64::new(0),
            stream_resets_dial_failed: AtomicU64::new(0),
            stream_resets_timeout: AtomicU64::new(0),
            stream_resets_peer: AtomicU64::new(0),
            stream_resets_session_dead: AtomicU64::new(0),
            stream_resets_protocol: AtomicU64::new(0),
            bytes_data_tx: AtomicU64::new(0),
            bytes_data_rx: AtomicU64::new(0),
            bytes_ctrl_tx: AtomicU64::new(0),
            bytes_ctrl_rx: AtomicU64::new(0),
            frame_send_drop: AtomicU64::new(0),
            session_all_down_resets: AtomicU64::new(0),
            streams_stalled: AtomicU64::new(0),
            failover_ms: Histogram::new(FAILOVER_MS_BOUNDS),
            stall_ms: Histogram::new(STALL_MS_BOUNDS),
            stream_lifetime_ms: Histogram::new(LIFETIME_MS_BOUNDS),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct PathSnap {
    pub name: String,
    pub link: String,
    pub rtt_us: u64,
    pub stable_rtt_us: u64,
    pub class_rtt_us: u64,
    pub inflight: u64,
    pub sticky: u64,
    pub alive: bool,
    pub state: u8,
    pub congested: bool,
    pub write_stalled: bool,
    pub last_rx_ago_us: u64,
    pub last_tx_ago_us: u64,
    pub rtt_known: bool,
    pub pending_ping: u64,
    pub queued_urgent: u64,
    pub queued_bulk: u64,
    pub backup: bool,
}

/// Named WAN link (`a` / `b`), rolled up from its TCP connections (`a#0`, `a#1`).
#[derive(Clone, Debug, Default)]
pub struct LinkSnap {
    pub name: String,
    pub conns: u64,
    pub up: u64,
    pub degraded: u64,
    pub rtt_us: u64,
    pub rtt_max_us: u64,
    pub inflight: u64,
    pub sticky: u64,
    pub congested: u64,
    pub rx_fresh_us: u64,
    pub rx_stale_us: u64,
    pub queued_urgent: u64,
    pub queued_bulk: u64,
    pub rtt_known: u64,
}

#[derive(Clone, Debug, Default)]
pub struct StreamSnap {
    pub id: u32,
    pub path: String,
    pub bulk: bool,
    pub stalled: bool,
    pub unacked: u64,
}

pub fn rollup_links(paths: &[PathSnap]) -> Vec<LinkSnap> {
    use std::collections::BTreeMap;
    #[derive(Default)]
    struct Acc {
        conns: u64,
        up: u64,
        deg: u64,
        inflight: u64,
        sticky: u64,
        congested: u64,
        queued_urgent: u64,
        queued_bulk: u64,
        known: u64,
        rtt_min: Option<u64>,
        rtt_max: Option<u64>,
        rtt_any_min: Option<u64>,
        rtt_any_max: Option<u64>,
        rx_fresh: Option<u64>,
        rx_stale: Option<u64>,
    }
    let mut m: BTreeMap<String, Acc> = BTreeMap::new();
    for p in paths {
        let a = m.entry(p.link.clone()).or_default();
        a.conns += 1;
        if p.state == STATE_UP {
            a.up += 1;
        } else if p.state == STATE_DEGRADED {
            a.deg += 1;
        }
        if p.congested {
            a.congested += 1;
        }
        a.inflight += p.inflight;
        a.sticky += p.sticky;
        a.queued_urgent += p.queued_urgent;
        a.queued_bulk += p.queued_bulk;
        a.rtt_any_min = Some(a.rtt_any_min.map_or(p.rtt_us, |x| x.min(p.rtt_us)));
        a.rtt_any_max = Some(a.rtt_any_max.map_or(p.rtt_us, |x| x.max(p.rtt_us)));
        if p.rtt_known {
            a.known += 1;
            a.rtt_min = Some(a.rtt_min.map_or(p.rtt_us, |x| x.min(p.rtt_us)));
            a.rtt_max = Some(a.rtt_max.map_or(p.rtt_us, |x| x.max(p.rtt_us)));
        }
        a.rx_fresh = Some(
            a.rx_fresh
                .map_or(p.last_rx_ago_us, |x| x.min(p.last_rx_ago_us)),
        );
        a.rx_stale = Some(
            a.rx_stale
                .map_or(p.last_rx_ago_us, |x| x.max(p.last_rx_ago_us)),
        );
    }
    m.into_iter()
        .map(|(name, a)| {
            let (rtt_us, rtt_max_us) = match (a.rtt_min, a.rtt_max) {
                (Some(lo), Some(hi)) => (lo, hi),
                _ => (a.rtt_any_min.unwrap_or(0), a.rtt_any_max.unwrap_or(0)),
            };
            LinkSnap {
                name,
                conns: a.conns,
                up: a.up,
                degraded: a.deg,
                rtt_us,
                rtt_max_us,
                inflight: a.inflight,
                sticky: a.sticky,
                congested: a.congested,
                rx_fresh_us: a.rx_fresh.unwrap_or(0),
                rx_stale_us: a.rx_stale.unwrap_or(0),
                queued_urgent: a.queued_urgent,
                queued_bulk: a.queued_bulk,
                rtt_known: a.known,
            }
        })
        .collect()
}

#[derive(Clone, Debug, Default)]
pub struct Snapshot {
    pub path_added: u64,
    pub path_down: u64,
    pub path_degraded: u64,
    pub path_outlier_recycle: u64,
    pub correlated_silence: u64,
    pub migrates: u64,
    pub migrates_speculative: u64,
    pub migrates_path_down: u64,
    pub migrates_ensure_sticky: u64,
    pub migrates_send_blocked: u64,
    pub data_retransmit: u64,
    pub data_hedge: u64,
    pub close_retry: u64,
    pub probe_miss: u64,
    pub window_blocks: u64,
    pub picks_unknown_rtt: u64,
    pub picks_unknown_over_known: u64,
    pub failbacks: u64,
    pub failbacks_upgrade: u64,
    pub failbacks_class_empty: u64,
    pub failbacks_same_link: u64,
    pub hol_rebalances: u64,
    pub streams_opened: u64,
    pub streams_closed: u64,
    pub stream_reaps_linger: u64,
    pub stream_resets: u64,
    pub stream_resets_dial_failed: u64,
    pub stream_resets_timeout: u64,
    pub stream_resets_peer: u64,
    pub stream_resets_session_dead: u64,
    pub stream_resets_protocol: u64,
    pub bytes_data_tx: u64,
    pub bytes_data_rx: u64,
    pub bytes_ctrl_tx: u64,
    pub bytes_ctrl_rx: u64,
    pub frame_send_drop: u64,
    pub session_all_down_resets: u64,
    pub streams_stalled: u64,
    pub streams_live: u64,
    /// HashMap occupancy, including graceful-closed entries not yet reaped.
    pub streams_held: u64,
    pub failover_ms: HistSnap,
    pub stall_ms: HistSnap,
    pub stream_lifetime_ms: HistSnap,
    pub paths: Vec<PathSnap>,
    pub links: Vec<LinkSnap>,
    pub streams: Vec<StreamSnap>,
}

impl Snapshot {
    /// Add counters and histograms. Does **not** touch `paths`.
    pub fn add_counters(&mut self, other: &Snapshot) {
        self.path_added += other.path_added;
        self.path_down += other.path_down;
        self.path_degraded += other.path_degraded;
        self.path_outlier_recycle += other.path_outlier_recycle;
        self.correlated_silence += other.correlated_silence;
        self.migrates += other.migrates;
        self.migrates_speculative += other.migrates_speculative;
        self.migrates_path_down += other.migrates_path_down;
        self.migrates_ensure_sticky += other.migrates_ensure_sticky;
        self.migrates_send_blocked += other.migrates_send_blocked;
        self.data_retransmit += other.data_retransmit;
        self.data_hedge += other.data_hedge;
        self.close_retry += other.close_retry;
        self.probe_miss += other.probe_miss;
        self.window_blocks += other.window_blocks;
        self.picks_unknown_rtt += other.picks_unknown_rtt;
        self.picks_unknown_over_known += other.picks_unknown_over_known;
        self.failbacks += other.failbacks;
        self.failbacks_upgrade += other.failbacks_upgrade;
        self.failbacks_class_empty += other.failbacks_class_empty;
        self.failbacks_same_link += other.failbacks_same_link;
        self.hol_rebalances += other.hol_rebalances;
        self.streams_opened += other.streams_opened;
        self.streams_closed += other.streams_closed;
        self.stream_reaps_linger += other.stream_reaps_linger;
        self.stream_resets += other.stream_resets;
        self.stream_resets_dial_failed += other.stream_resets_dial_failed;
        self.stream_resets_timeout += other.stream_resets_timeout;
        self.stream_resets_peer += other.stream_resets_peer;
        self.stream_resets_session_dead += other.stream_resets_session_dead;
        self.stream_resets_protocol += other.stream_resets_protocol;
        self.bytes_data_tx += other.bytes_data_tx;
        self.bytes_data_rx += other.bytes_data_rx;
        self.bytes_ctrl_tx += other.bytes_ctrl_tx;
        self.bytes_ctrl_rx += other.bytes_ctrl_rx;
        self.frame_send_drop += other.frame_send_drop;
        self.session_all_down_resets += other.session_all_down_resets;
        self.streams_stalled += other.streams_stalled;
        self.streams_live += other.streams_live;
        self.streams_held += other.streams_held;
        self.failover_ms.merge_add(&other.failover_ms);
        self.stall_ms.merge_add(&other.stall_ms);
        self.stream_lifetime_ms.merge_add(&other.stream_lifetime_ms);
    }
}

impl Counters {
    pub fn snap_with_paths(&self, paths: &[std::sync::Arc<PathState>]) -> Snapshot {
        Snapshot {
            path_added: self.path_added.load(Ordering::Relaxed),
            path_down: self.path_down.load(Ordering::Relaxed),
            path_degraded: self.path_degraded.load(Ordering::Relaxed),
            path_outlier_recycle: self.path_outlier_recycle.load(Ordering::Relaxed),
            correlated_silence: self.correlated_silence.load(Ordering::Relaxed),
            migrates: self.migrates.load(Ordering::Relaxed),
            migrates_speculative: self.migrates_speculative.load(Ordering::Relaxed),
            migrates_path_down: self.migrates_path_down.load(Ordering::Relaxed),
            migrates_ensure_sticky: self.migrates_ensure_sticky.load(Ordering::Relaxed),
            migrates_send_blocked: self.migrates_send_blocked.load(Ordering::Relaxed),
            data_retransmit: self.data_retransmit.load(Ordering::Relaxed),
            data_hedge: self.data_hedge.load(Ordering::Relaxed),
            close_retry: self.close_retry.load(Ordering::Relaxed),
            probe_miss: self.probe_miss.load(Ordering::Relaxed),
            window_blocks: self.window_blocks.load(Ordering::Relaxed),
            picks_unknown_rtt: self.picks_unknown_rtt.load(Ordering::Relaxed),
            picks_unknown_over_known: self.picks_unknown_over_known.load(Ordering::Relaxed),
            failbacks: self.failbacks.load(Ordering::Relaxed),
            failbacks_upgrade: self.failbacks_upgrade.load(Ordering::Relaxed),
            failbacks_class_empty: self.failbacks_class_empty.load(Ordering::Relaxed),
            failbacks_same_link: self.failbacks_same_link.load(Ordering::Relaxed),
            hol_rebalances: self.hol_rebalances.load(Ordering::Relaxed),
            streams_opened: self.streams_opened.load(Ordering::Relaxed),
            streams_closed: self.streams_closed.load(Ordering::Relaxed),
            stream_reaps_linger: self.stream_reaps_linger.load(Ordering::Relaxed),
            stream_resets: self.stream_resets.load(Ordering::Relaxed),
            stream_resets_dial_failed: self.stream_resets_dial_failed.load(Ordering::Relaxed),
            stream_resets_timeout: self.stream_resets_timeout.load(Ordering::Relaxed),
            stream_resets_peer: self.stream_resets_peer.load(Ordering::Relaxed),
            stream_resets_session_dead: self.stream_resets_session_dead.load(Ordering::Relaxed),
            stream_resets_protocol: self.stream_resets_protocol.load(Ordering::Relaxed),
            bytes_data_tx: self.bytes_data_tx.load(Ordering::Relaxed),
            bytes_data_rx: self.bytes_data_rx.load(Ordering::Relaxed),
            bytes_ctrl_tx: self.bytes_ctrl_tx.load(Ordering::Relaxed),
            bytes_ctrl_rx: self.bytes_ctrl_rx.load(Ordering::Relaxed),
            frame_send_drop: self.frame_send_drop.load(Ordering::Relaxed),
            session_all_down_resets: self.session_all_down_resets.load(Ordering::Relaxed),
            streams_stalled: self.streams_stalled.load(Ordering::Relaxed),
            streams_live: 0,
            streams_held: 0,
            failover_ms: self.failover_ms.snap(),
            stall_ms: self.stall_ms.snap(),
            stream_lifetime_ms: self.stream_lifetime_ms.snap(),
            paths: paths
                .iter()
                .map(|p| PathSnap {
                    name: p.name.clone(),
                    link: link_key(&p.name).to_string(),
                    rtt_us: p.rtt_us(),
                    stable_rtt_us: p.stable_rtt().as_micros() as u64,
                    class_rtt_us: p.class_rtt().as_micros() as u64,
                    inflight: p.inflight_bytes(),
                    sticky: p.sticky_count(),
                    alive: p.is_alive(),
                    state: p.state.load(Ordering::Relaxed),
                    congested: p.is_congested(),
                    write_stalled: p.is_write_stalled(),
                    last_rx_ago_us: p.last_rx_ago().as_micros() as u64,
                    last_tx_ago_us: p.last_tx_ago().as_micros() as u64,
                    rtt_known: p.rtt_known(),
                    pending_ping: p.pending_ping_count(),
                    queued_urgent: p.queued_urgent(),
                    queued_bulk: p.queued_bulk(),
                    backup: false,
                })
                .collect(),
            links: Vec::new(),
            streams: Vec::new(),
        }
    }
}

pub struct ProcessCounters {
    pub handshake_create_ok: AtomicU64,
    pub handshake_join_ok: AtomicU64,
    pub handshake_fail_auth: AtomicU64,
    pub handshake_fail_version: AtomicU64,
    pub handshake_fail_unknown: AtomicU64,
    pub handshake_fail_other: AtomicU64,
    pub inbound_accept: AtomicU64,
    pub inbound_reject: AtomicU64,
    pub inbound_open_fail: AtomicU64,
    pub outbound_dial_ok: AtomicU64,
    pub outbound_dial_fail: AtomicU64,
    pub reconnect_ok: AtomicU64,
    pub reconnect_fail: AtomicU64,
    pub sessions_created: AtomicU64,
    pub sessions_dead: AtomicU64,
    pub sessions_live: AtomicU64,
    hop_open_ms: Histogram,
    hop_first_rx_ms: Histogram,
    hop_last_rx_ms: Histogram,
    hop_dial_ms: Histogram,
    hop_origin_first_ms: Histogram,
    hop_origin_last_ms: Histogram,
    hop_tail: Mutex<Option<HopSample>>,
}

impl Default for ProcessCounters {
    fn default() -> Self {
        Self {
            handshake_create_ok: AtomicU64::new(0),
            handshake_join_ok: AtomicU64::new(0),
            handshake_fail_auth: AtomicU64::new(0),
            handshake_fail_version: AtomicU64::new(0),
            handshake_fail_unknown: AtomicU64::new(0),
            handshake_fail_other: AtomicU64::new(0),
            inbound_accept: AtomicU64::new(0),
            inbound_reject: AtomicU64::new(0),
            inbound_open_fail: AtomicU64::new(0),
            outbound_dial_ok: AtomicU64::new(0),
            outbound_dial_fail: AtomicU64::new(0),
            reconnect_ok: AtomicU64::new(0),
            reconnect_fail: AtomicU64::new(0),
            sessions_created: AtomicU64::new(0),
            sessions_dead: AtomicU64::new(0),
            sessions_live: AtomicU64::new(0),
            hop_open_ms: Histogram::new(STALL_MS_BOUNDS),
            hop_first_rx_ms: Histogram::new(STALL_MS_BOUNDS),
            hop_last_rx_ms: Histogram::new(STALL_MS_BOUNDS),
            hop_dial_ms: Histogram::new(STALL_MS_BOUNDS),
            hop_origin_first_ms: Histogram::new(STALL_MS_BOUNDS),
            hop_origin_last_ms: Histogram::new(STALL_MS_BOUNDS),
            hop_tail: Mutex::new(None),
        }
    }
}

fn observe_us_as_ms(h: &Histogram, us: Option<u64>) {
    if let Some(us) = us {
        h.observe(us / 1000);
    }
}

impl ProcessCounters {
    pub fn record_hop(&self, sample: HopSample) {
        debug!(
            target: "nya_core::hop",
            event = "hop",
            stream_id = sample.stream_id,
            session_fp = sample.session_fp.as_str(),
            host = %sample.host,
            outcome = sample.outcome.as_str(),
            hops = %sample.format_debug_fields(),
            "hop"
        );
        sample.emit_otel_span();
        match sample.role {
            HopRole::Client => {
                observe_us_as_ms(&self.hop_open_ms, sample.open_us);
                observe_us_as_ms(&self.hop_first_rx_ms, sample.first_rx_us);
                observe_us_as_ms(&self.hop_last_rx_ms, sample.last_rx_us);
            }
            HopRole::Server => {
                observe_us_as_ms(&self.hop_dial_ms, sample.dial_us);
                observe_us_as_ms(&self.hop_origin_first_ms, sample.origin_first_rx_us);
                observe_us_as_ms(&self.hop_origin_last_ms, sample.origin_last_rx_us);
            }
        }
        let rank = sample.rank_us();
        let mut g = self.hop_tail.lock().unwrap();
        match g.as_ref() {
            Some(cur) if cur.rank_us() >= rank => {}
            _ => *g = Some(sample),
        }
    }

    pub fn take_interval_tail(&self) -> Option<HopSample> {
        self.hop_tail.lock().unwrap().take()
    }

    pub fn snap(&self) -> ProcessCountersSnap {
        ProcessCountersSnap {
            handshake_create_ok: self.handshake_create_ok.load(Ordering::Relaxed),
            handshake_join_ok: self.handshake_join_ok.load(Ordering::Relaxed),
            handshake_fail_auth: self.handshake_fail_auth.load(Ordering::Relaxed),
            handshake_fail_version: self.handshake_fail_version.load(Ordering::Relaxed),
            handshake_fail_unknown: self.handshake_fail_unknown.load(Ordering::Relaxed),
            handshake_fail_other: self.handshake_fail_other.load(Ordering::Relaxed),
            inbound_accept: self.inbound_accept.load(Ordering::Relaxed),
            inbound_reject: self.inbound_reject.load(Ordering::Relaxed),
            inbound_open_fail: self.inbound_open_fail.load(Ordering::Relaxed),
            outbound_dial_ok: self.outbound_dial_ok.load(Ordering::Relaxed),
            outbound_dial_fail: self.outbound_dial_fail.load(Ordering::Relaxed),
            reconnect_ok: self.reconnect_ok.load(Ordering::Relaxed),
            reconnect_fail: self.reconnect_fail.load(Ordering::Relaxed),
            sessions_created: self.sessions_created.load(Ordering::Relaxed),
            sessions_dead: self.sessions_dead.load(Ordering::Relaxed),
            sessions_live: self.sessions_live.load(Ordering::Relaxed),
            hop_open_ms: self.hop_open_ms.snap(),
            hop_first_rx_ms: self.hop_first_rx_ms.snap(),
            hop_last_rx_ms: self.hop_last_rx_ms.snap(),
            hop_dial_ms: self.hop_dial_ms.snap(),
            hop_origin_first_ms: self.hop_origin_first_ms.snap(),
            hop_origin_last_ms: self.hop_origin_last_ms.snap(),
        }
    }

    pub fn inc_handshake_fail(&self, e: &crate::handshake::HandshakeError) {
        use crate::handshake::HandshakeError;
        match e {
            HandshakeError::Rejected(msg) if msg.contains("version") => {
                self.handshake_fail_version.fetch_add(1, Ordering::Relaxed);
            }
            HandshakeError::Rejected(msg) if msg.contains("auth") => {
                self.handshake_fail_auth.fetch_add(1, Ordering::Relaxed);
            }
            HandshakeError::UnknownSession => {
                self.handshake_fail_unknown.fetch_add(1, Ordering::Relaxed);
            }
            _ => {
                self.handshake_fail_other.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ProcessCountersSnap {
    pub handshake_create_ok: u64,
    pub handshake_join_ok: u64,
    pub handshake_fail_auth: u64,
    pub handshake_fail_version: u64,
    pub handshake_fail_unknown: u64,
    pub handshake_fail_other: u64,
    pub inbound_accept: u64,
    pub inbound_reject: u64,
    pub inbound_open_fail: u64,
    pub outbound_dial_ok: u64,
    pub outbound_dial_fail: u64,
    pub reconnect_ok: u64,
    pub reconnect_fail: u64,
    pub sessions_created: u64,
    pub sessions_dead: u64,
    pub sessions_live: u64,
    pub hop_open_ms: HistSnap,
    pub hop_first_rx_ms: HistSnap,
    pub hop_last_rx_ms: HistSnap,
    pub hop_dial_ms: HistSnap,
    pub hop_origin_first_ms: HistSnap,
    pub hop_origin_last_ms: HistSnap,
}

#[derive(Clone, Debug, Default)]
pub struct ProcessSnapshot {
    pub process: ProcessCountersSnap,
    pub session: Snapshot,
}

pub fn flatten_paths(sessions: &[([u8; 16], Snapshot)]) -> Vec<PathSnap> {
    let prefix = sessions.len() > 1;
    sessions
        .iter()
        .flat_map(|(id, snap)| {
            snap.paths.iter().cloned().map(|mut p| {
                if prefix {
                    let tag = hex4(id);
                    p.name = format!("{tag}:{}", p.name);
                    p.link = format!("{tag}:{}", p.link);
                }
                p
            })
        })
        .collect()
}

fn hex4(id: &[u8; 16]) -> String {
    format!("{:02x}{:02x}", id[0], id[1])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observe_lands_in_expected_bucket() {
        let h = Histogram::new(STALL_MS_BOUNDS);
        for _ in 0..100 {
            h.observe(50);
        }
        let snap = h.snap();
        assert_eq!(snap.buckets.len(), STALL_MS_BOUNDS.len() + 1);
        assert_eq!(snap.count, 100);
        assert_eq!(snap.sum, 50 * 100);
        // 50 <= 50, after 20 → index 1
        assert_eq!(snap.buckets[1], 100);
        let p50 = percentile(&snap, STALL_MS_BOUNDS, 50.0).unwrap();
        let p99 = percentile(&snap, STALL_MS_BOUNDS, 99.0).unwrap();
        assert!(
            p50 > 20 && p50 <= 50,
            "p50={p50} should sit in the 50ms bucket"
        );
        assert!(
            p99 > 20 && p99 <= 50,
            "p99={p99} should sit in the 50ms bucket"
        );
    }

    #[test]
    fn empty_snap_percentile_is_none() {
        let h = Histogram::new(FAILOVER_MS_BOUNDS);
        let snap = h.snap();
        assert_eq!(snap.buckets.len(), FAILOVER_MS_BOUNDS.len() + 1);
        assert_eq!(snap.count, 0);
        assert!(percentile(&snap, FAILOVER_MS_BOUNDS, 99.0).is_none());
    }

    #[test]
    fn zeroed_merge_add_equals_real() {
        let h = Histogram::new(FAILOVER_MS_BOUNDS);
        h.observe(7);
        h.observe(80);
        let real = h.snap();
        let mut z = HistSnap::zeroed(FAILOVER_MS_BOUNDS);
        z.merge_add(&real);
        assert_eq!(z, real);
    }

    #[test]
    fn default_merge_add_is_noop_not_spec() {
        let h = Histogram::new(FAILOVER_MS_BOUNDS);
        h.observe(10);
        let real = h.snap();
        let mut d = HistSnap::default();
        d.merge_add(&real);
        assert!(d.buckets.is_empty());
        assert_eq!(d.count, 0);
    }

    #[test]
    fn counters_default_hists_have_full_buckets() {
        let c = Counters::default();
        let snap = c.snap_with_paths(&[]);
        assert_eq!(snap.failover_ms.buckets.len(), FAILOVER_MS_BOUNDS.len() + 1);
        assert_eq!(snap.stall_ms.buckets.len(), STALL_MS_BOUNDS.len() + 1);
        assert_eq!(
            snap.stream_lifetime_ms.buckets.len(),
            LIFETIME_MS_BOUNDS.len() + 1
        );
    }

    #[test]
    fn rollup_links_sums_same_link_conns() {
        let paths = vec![
            PathSnap {
                name: "a#0".into(),
                link: "a".into(),
                rtt_us: 12_000,
                state: STATE_UP,
                rtt_known: true,
                sticky: 2,
                last_rx_ago_us: 3_000,
                ..Default::default()
            },
            PathSnap {
                name: "a#1".into(),
                link: "a".into(),
                rtt_us: 28_000,
                state: STATE_DEGRADED,
                rtt_known: true,
                sticky: 1,
                congested: true,
                last_rx_ago_us: 40_000,
                queued_urgent: 3,
                ..Default::default()
            },
            PathSnap {
                name: "b#0".into(),
                link: "b".into(),
                rtt_us: 60_000,
                state: STATE_UP,
                rtt_known: true,
                last_rx_ago_us: 5_000,
                ..Default::default()
            },
        ];
        let links = rollup_links(&paths);
        assert_eq!(links.len(), 2);
        let a = links.iter().find(|l| l.name == "a").unwrap();
        assert_eq!(a.conns, 2);
        assert_eq!(a.up, 1);
        assert_eq!(a.degraded, 1);
        assert_eq!(a.rtt_us, 12_000);
        assert_eq!(a.rtt_max_us, 28_000);
        assert_eq!(a.sticky, 3);
        assert_eq!(a.congested, 1);
        assert_eq!(a.rx_fresh_us, 3_000);
        assert_eq!(a.rx_stale_us, 40_000);
        assert_eq!(a.queued_urgent, 3);
        let b = links.iter().find(|l| l.name == "b").unwrap();
        assert_eq!(b.conns, 1);
        assert_eq!(b.up, 1);
        assert_eq!(b.degraded, 0);
    }

    #[test]
    fn record_hop_observes_mapped_fields_only() {
        let pc = ProcessCounters::default();
        pc.record_hop(HopSample {
            role: HopRole::Server,
            stream_id: 7,
            host: "clients3.google.com".into(),
            copy_us: Some(5_042_000),
            dial_us: Some(80),
            origin_first_rx_us: Some(5_042_000),
            origin_last_rx_us: Some(5_042_000),
            max_gap: Some(5_000_000),
            crx_at_gap: Some(30_000),
            origin_at_gap: Some(5_030_000),
            crx_at_olast: Some(5_038_000),
            ..Default::default()
        });
        let snap = pc.snap();
        assert_eq!(snap.hop_dial_ms.count, 1);
        assert_eq!(snap.hop_origin_first_ms.count, 1);
        assert_eq!(snap.hop_origin_last_ms.count, 1);
        assert_eq!(snap.hop_open_ms.count, 0);
        assert_eq!(snap.hop_first_rx_ms.count, 0);
        assert_eq!(snap.hop_last_rx_ms.count, 0);
        let of = percentile(&snap.hop_origin_first_ms, STALL_MS_BOUNDS, 99.0).unwrap();
        assert!(of >= 2000, "origin_first p99={of}");
        let tail = pc.take_interval_tail().expect("tail");
        assert_eq!(tail.host, "clients3.google.com");
        assert_eq!(tail.stream_id, 7);
        assert_eq!(tail.copy_us, Some(5_042_000));
        assert!(pc.take_interval_tail().is_none());
        // snap() must not consume tail — already taken; record another and snap
        pc.record_hop(HopSample {
            role: HopRole::Client,
            stream_id: 1,
            host: "x".into(),
            copy_us: Some(40_000),
            open_us: Some(80),
            ..Default::default()
        });
        let _ = pc.snap();
        assert!(pc.take_interval_tail().is_some());
    }

    #[test]
    fn hop_tail_larger_rank_wins() {
        let pc = ProcessCounters::default();
        pc.record_hop(HopSample {
            role: HopRole::Client,
            stream_id: 1,
            host: "a".into(),
            copy_us: Some(40_000),
            ..Default::default()
        });
        pc.record_hop(HopSample {
            role: HopRole::Client,
            stream_id: 2,
            host: "b".into(),
            copy_us: Some(5_000_000),
            ..Default::default()
        });
        let tail = pc.take_interval_tail().unwrap();
        assert_eq!(tail.stream_id, 2);
        assert_eq!(tail.host, "b");
    }

    #[test]
    fn record_hop_40ms_is_debug_not_info() {
        use std::sync::Arc;
        use tracing::span::{Attributes, Id, Record};
        use tracing::{Event, Metadata, Subscriber};

        struct CapArc(Arc<Mutex<Vec<(String, String)>>>);
        impl Subscriber for CapArc {
            fn enabled(&self, _: &Metadata<'_>) -> bool {
                true
            }
            fn new_span(&self, _: &Attributes<'_>) -> Id {
                Id::from_u64(1)
            }
            fn record(&self, _: &Id, _: &Record<'_>) {}
            fn record_follows_from(&self, _: &Id, _: &Id) {}
            fn event(&self, event: &Event<'_>) {
                self.0.lock().unwrap().push((
                    event.metadata().level().as_str().to_string(),
                    event.metadata().target().to_string(),
                ));
            }
            fn enter(&self, _: &Id) {}
            fn exit(&self, _: &Id) {}
        }
        let store = Arc::new(Mutex::new(Vec::new()));
        tracing::subscriber::with_default(CapArc(store.clone()), || {
            let pc = ProcessCounters::default();
            pc.record_hop(HopSample {
                role: HopRole::Client,
                stream_id: 1,
                host: "example.com".into(),
                copy_us: Some(40_000),
                open_us: Some(80),
                ..Default::default()
            });
        });
        let ev = store.lock().unwrap().clone();
        assert!(
            ev.iter().any(|(l, t)| l == "DEBUG" && t == "nya_core::hop"),
            "{ev:?}"
        );
        assert!(
            !ev.iter().any(|(l, t)| l == "INFO" && t == "nya_core::hop"),
            "{ev:?}"
        );
    }

    #[test]
    fn record_hop_emits_nya_hop_span() {
        use std::sync::Arc;
        use tracing::span::{Attributes, Id, Record};
        use tracing::{Event, Metadata, Subscriber};

        struct CapArc(Arc<Mutex<Vec<String>>>);
        impl Subscriber for CapArc {
            fn enabled(&self, _: &Metadata<'_>) -> bool {
                true
            }
            fn new_span(&self, attrs: &Attributes) -> Id {
                self.0
                    .lock()
                    .unwrap()
                    .push(attrs.metadata().name().to_string());
                Id::from_u64(1)
            }
            fn record(&self, _: &Id, _: &Record<'_>) {}
            fn record_follows_from(&self, _: &Id, _: &Id) {}
            fn event(&self, _: &Event<'_>) {}
            fn enter(&self, _: &Id) {}
            fn exit(&self, _: &Id) {}
        }
        let store = Arc::new(Mutex::new(Vec::new()));
        tracing::subscriber::with_default(CapArc(store.clone()), || {
            let pc = ProcessCounters::default();
            pc.record_hop(HopSample {
                role: HopRole::Client,
                stream_id: 1,
                host: "example.com".into(),
                copy_us: Some(40_000),
                open_us: Some(80),
                first_rx_us: Some(12_000),
                ..Default::default()
            });
        });
        let names = store.lock().unwrap().clone();
        assert!(
            names.iter().any(|n| n == "nya.hop"),
            "expected nya.hop marker span, got {names:?}"
        );
    }

    #[test]
    fn missing_copy_is_not_observed() {
        let pc = ProcessCounters::default();
        pc.record_hop(HopSample {
            role: HopRole::Server,
            stream_id: 3,
            host: "www.gstatic.com".into(),
            outcome: crate::hop::HopOutcome::DialFail,
            dial_us: Some(8_000_000),
            copy_us: None,
            ..Default::default()
        });
        let snap = pc.snap();
        assert_eq!(snap.hop_dial_ms.count, 1);
        assert_eq!(snap.hop_origin_first_ms.count, 0);
        assert_eq!(pc.take_interval_tail().unwrap().copy_us, None);
    }

    #[test]
    fn flatten_paths_prefixes_link_when_multi_session() {
        let path = PathSnap {
            name: "a#0".into(),
            link: "a".into(),
            rtt_us: 12_000,
            state: STATE_UP,
            ..Default::default()
        };
        let mut s1 = Snapshot::default();
        s1.paths.push(path.clone());
        let mut s2 = Snapshot::default();
        s2.paths.push(path);
        let mut id2 = [0u8; 16];
        id2[0] = 0xab;
        let flat = flatten_paths(&[([0u8; 16], s1), (id2, s2)]);
        assert_eq!(flat.len(), 2);
        assert_eq!(flat[0].name, "0000:a#0");
        assert_eq!(flat[0].link, "0000:a");
        assert_eq!(flat[1].name, "ab00:a#0");
        assert_eq!(flat[1].link, "ab00:a");
        let links = rollup_links(&flat);
        assert_eq!(links.len(), 2);
        assert!(links.iter().any(|l| l.name == "0000:a"));
        assert!(links.iter().any(|l| l.name == "ab00:a"));
    }

    #[test]
    fn inf_bucket_percentile_returns_last_bound() {
        let h = Histogram::new(FAILOVER_MS_BOUNDS);
        h.observe(10_000);
        let snap = h.snap();
        let p = percentile(&snap, FAILOVER_MS_BOUNDS, 100.0).unwrap();
        assert_eq!(p, *FAILOVER_MS_BOUNDS.last().unwrap());
    }
}
