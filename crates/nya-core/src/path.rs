use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::{Sink, SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, Notify};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::{debug, info, warn};

use nya_proto::{Frame, Ping, StreamAck, MAX_FRAME_SIZE};

use crate::session::Session;
use crate::tuning::Tuning;

pub const STATE_UP: u8 = 1;
pub const STATE_DEGRADED: u8 = 2;
pub const STATE_DOWN: u8 = 3;

/// Same cap as `push_tried` FIFO-8. Not a Tuning field.
pub(crate) const ACK_FLUSH_K: usize = 8;

/// `a#0` / `a#1` share a link; names without `#` are their own link.
pub fn link_key(name: &str) -> &str {
    name.rsplit_once('#').map(|(l, _)| l).unwrap_or(name)
}

/// BBR keeps its RTprop over 10 s; same here for the budget's min RTT.
pub const MIN_RTT_WIN_US: u64 = 10_000_000;
/// Budget controller constants (see `PathState::end_budget_round`).
pub const BUDGET_GROW: f64 = 1.5;
/// A step is ≥ 15 % over the last full bandwidth. BBR uses 25 % with a
/// 2.89× pacing gain that overshoots the pipe before the test can fail;
/// budget steps here are ×1.5 with no overshoot, so 25 % stops one step
/// short (≈ 80 % of the link, measured) and 15 % lands at ≥ 90 %.
pub const BUDGET_GROWTH_MIN: f64 = 0.15;
/// BBR `bbr_full_bw_cnt`: rounds without a step before the pipe is full.
pub const BUDGET_FULL_ROUNDS: u64 = 3;
pub const BUDGET_PROBE: f64 = 1.25;
/// Limited rounds between probes once growth has stopped. The probe is
/// the only way up after a plateau or a sag (bandwidth cannot rise before
/// the budget does), so it must come often enough to recover within a
/// transfer: 4 loops ≈ 100–200 ms.
pub const BUDGET_PROBE_EVERY: u64 = 4;
pub const BUDGET_IDLE_SHRINK: u64 = 8;
/// The budget never exceeds this many times the bytes the path's best
/// round rate carries in one minimum loop, whatever the step test
/// believes. A TCP in loss recovery under a saturated path hands back its
/// backlog in bursts that the round test can read as headroom; without the
/// ceiling that feedback ran the budget to 5× the pipe and the loop to 4×
/// its minimum. BBR's `cwnd_gain` is 2; one more BDP here because the
/// minimum loop is measured with empty queues while a reverse bulk flow
/// (upload while downloading, the echo workloads) holds our ACKs behind
/// its data for a good part of a BDP, and 2× then starves the forward
/// direction (measured: 75–85 % of the link against 97 % at 3×).
pub const BUDGET_CAP_GAIN: u64 = 3;

fn mul(v: u64, g: f64) -> u64 {
    (v as f64 * g).min(u64::MAX as f64) as u64
}

pub struct PathState {
    pub id: u32,
    pub name: String,
    /// Bulk / large STREAM_DATA.
    pub writer: mpsc::Sender<Frame>,
    /// Close/Open/Reset, pings, and small STREAM_DATA — must not wait behind bulk.
    pub urgent: mpsc::Sender<Frame>,
    /// Latest STREAM_ACK per stream. Overwrite register; not an mpsc slot.
    pub pending_acks: std::sync::Mutex<HashMap<u32, StreamAck>>,
    /// Writer wakeup when `pending_acks` gains an entry.
    pub ack_wait: Notify,
    /// Writer dequeue wakeup. Bulk `send_data` waits here on a full queue.
    pub queue_wait: Notify,
    pub rtt_ewma_us: AtomicU64,
    pub rtt_stable_us: AtomicU64,
    /// Two-sided class membership. 0 = unset (`class_rtt()` falls back to fast).
    pub rtt_class_us: AtomicU64,
    class_init_n: AtomicU64,
    pub inflight: AtomicU64,
    /// Sticky streams currently assigned to this TCP connection.
    pub sticky_streams: AtomicU64,
    /// Writer queue was full; do not pick until a send succeeds again.
    pub congested: AtomicBool,
    /// In-flight `send_frame` exceeded `write_deadline`. Pick skips; TCP stays up.
    write_stalled: AtomicBool,
    pub last_rx: std::sync::Mutex<Instant>,
    pub last_tx: std::sync::Mutex<Instant>,
    pub up_since: std::sync::Mutex<Instant>,
    pub state: AtomicU8,
    ping_seq: AtomicU64,
    pending_ping: std::sync::Mutex<HashMap<u64, Instant>>,
    /// Instant kept after expire so a late Pong can still sample RTT.
    late_ping: std::sync::Mutex<HashMap<u64, Instant>>,
    /// How long fast RTT must stay high before stable RTT is raised.
    pub stable_up_hold_us: AtomicU64,
    high_since: std::sync::Mutex<Option<Instant>>,
    class_high_since: std::sync::Mutex<Option<Instant>>,
    class_low_since: std::sync::Mutex<Option<Instant>>,
    class_low_accum: std::sync::Mutex<Duration>,
    outlier_since: std::sync::Mutex<Option<Instant>>,
    /// First freeze of `rtt_class_us`. Recycle age-gate; never cleared.
    class_known_since: std::sync::Mutex<Option<Instant>>,
    /// Set on a class-raise store or init freeze; cleared on a drop
    /// store iff `new_us <= fast`. Happy-path freeze (class == fast)
    /// never catch-up-clears, so production paths keep this until a
    /// later dip walks class down to fast.
    class_unwind_permit: AtomicBool,
    /// CAS: one failover_ms sample per path.
    pub failover_recorded: AtomicBool,
    urgent_queued: AtomicU64,
    bulk_queued: AtomicU64,
    /// `open_stream` hits that actually sent StreamOpen on this dest.
    pub picks: AtomicU64,
    /// Bytes acknowledged on this path, cumulative (P2 ACK clock).
    pub delivered: AtomicU64,
    /// `mono_us` of the last `delivered` advance. 0 = never.
    pub delivered_at_us: AtomicU64,
    /// Loaded ACK RTT: EWMA(1/8) of last_sent→ACK for un-hedged bulk pieces
    /// on this path, µs. 0 = unknown. Drives the P4 bulk hedge clock.
    pub ack_rtt_us: AtomicU64,
    /// Windowed max of ACK-clock delivery-rate samples, bytes/s (P2).
    pub bw_filter: std::sync::Mutex<crate::bw::MinMax3>,
    /// Send time of the first piece sent after the last ACK (BBR
    /// `first_tx_mstamp`); the send-side clock of a rate sample.
    pub first_tx_us: AtomicU64,
    /// Windowed min of accepted RTT samples over `MIN_RTT_WIN_US` (P2
    /// budget uses the true floor, not a load-inflated EWMA).
    pub min_rtt_filter: std::sync::Mutex<crate::bw::WindowedMin>,
    /// Send budget bounds, bytes; set by the session at path start.
    pub budget_floor: AtomicU64,
    pub budget_ceil: AtomicU64,
    /// Budget controller (P2.3). `budget` is the current allowance;
    /// `round_start_us` marks the current ACK-RTT round; `round_bw_ref` is
    /// `bw_full`, the last bandwidth that stepped up ≥ 25 %; `round_limited`
    /// is set when a bulk send parked on this budget during the round;
    /// `flat_rounds` / `sag_rounds` count consecutive rounds without a step
    /// / with bandwidth ≥ 25 % under `bw_full`; `idle_rounds` counts rounds
    /// that never parked; `probe_from` is the budget to fall back to if a
    /// probe round fails.
    pub budget: AtomicU64,
    pub round_start_us: AtomicU64,
    pub round_bw_ref: AtomicU64,
    pub round_limited: AtomicBool,
    pub flat_rounds: AtomicU64,
    pub sag_rounds: AtomicU64,
    pub idle_rounds: AtomicU64,
    pub probe_from: AtomicU64,
    /// `delivered` at round start: the controller judges growth on the
    /// round's delivered average, not on the per-ACK max. Without pacing
    /// the send-side clock of a sample is a burst, so a stretch ACK (TCP
    /// loss recovery under us, ACKs queued behind reverse bulk) reads as a
    /// rate the link never had, and the max filter would keep it for a
    /// whole window. `prev_round_bw` is the rate of an as-yet unconfirmed
    /// step round (0 = none).
    pub round_delivered: AtomicU64,
    pub prev_round_bw: AtomicU64,
    /// Windowed max of the round averages: the bandwidth the budget
    /// ceiling `BUDGET_CAP_GAIN × bw × min_loop` is built on.
    pub round_bw_max: std::sync::Mutex<crate::bw::MinMax3>,
    /// Previous round's average (0 = none), for the two-round min the
    /// ceiling filter is fed with.
    pub last_round_bw: AtomicU64,
    /// `mono_us` of the last `record_ack_rtt` (KD7 expiry). 0 = never.
    pub ack_rtt_at_us: AtomicU64,
    /// Last loop-fit verdict sampled by `maintain` and fit→unfit count (P3.4).
    pub loop_fit_last: AtomicBool,
    pub loop_unfit_total: AtomicU64,
    /// P3.3: `mono_us` of the last incoming `loop_unfit` re-stick onto this
    /// path (0 = never). A dest takes another only once it has recorded an
    /// `ack_rtt` sample since, and never within `bw_window_us`.
    pub restick_in_at_us: AtomicU64,
    /// Duplicated socket fd for `TCP_INFO` (P6). `None` off Linux, in unit
    /// tests over duplex pipes, and after path IO exit.
    pub tcp_fd: std::sync::Mutex<Option<crate::net::PathFd>>,
}

impl PathState {
    pub fn new(id: u32, name: String, writer: mpsc::Sender<Frame>) -> Arc<Self> {
        let (urgent, _) = mpsc::channel(8);
        Self::with_writers(id, name, writer, urgent)
    }

    pub fn with_writers(
        id: u32,
        name: String,
        writer: mpsc::Sender<Frame>,
        urgent: mpsc::Sender<Frame>,
    ) -> Arc<Self> {
        let now = Instant::now();
        Arc::new(Self {
            id,
            name,
            writer,
            urgent,
            pending_acks: std::sync::Mutex::new(HashMap::new()),
            ack_wait: Notify::new(),
            queue_wait: Notify::new(),
            rtt_ewma_us: AtomicU64::new(0),
            rtt_stable_us: AtomicU64::new(0),
            rtt_class_us: AtomicU64::new(0),
            class_init_n: AtomicU64::new(0),
            inflight: AtomicU64::new(0),
            sticky_streams: AtomicU64::new(0),
            congested: AtomicBool::new(false),
            write_stalled: AtomicBool::new(false),
            last_rx: std::sync::Mutex::new(now),
            last_tx: std::sync::Mutex::new(now),
            up_since: std::sync::Mutex::new(now),
            state: AtomicU8::new(STATE_UP),
            ping_seq: AtomicU64::new(1),
            pending_ping: std::sync::Mutex::new(HashMap::new()),
            late_ping: std::sync::Mutex::new(HashMap::new()),
            stable_up_hold_us: AtomicU64::new(1_000_000),
            high_since: std::sync::Mutex::new(None),
            class_high_since: std::sync::Mutex::new(None),
            class_low_since: std::sync::Mutex::new(None),
            class_low_accum: std::sync::Mutex::new(Duration::ZERO),
            outlier_since: std::sync::Mutex::new(None),
            class_known_since: std::sync::Mutex::new(None),
            class_unwind_permit: AtomicBool::new(false),
            failover_recorded: AtomicBool::new(false),
            urgent_queued: AtomicU64::new(0),
            bulk_queued: AtomicU64::new(0),
            picks: AtomicU64::new(0),
            delivered: AtomicU64::new(0),
            delivered_at_us: AtomicU64::new(0),
            ack_rtt_us: AtomicU64::new(0),
            bw_filter: std::sync::Mutex::new(crate::bw::MinMax3::new()),
            first_tx_us: AtomicU64::new(0),
            min_rtt_filter: std::sync::Mutex::new(crate::bw::WindowedMin::new()),
            budget: AtomicU64::new(crate::tuning::Tuning::STANDARD.inflight_bias),
            round_start_us: AtomicU64::new(0),
            round_bw_ref: AtomicU64::new(0),
            round_limited: AtomicBool::new(false),
            flat_rounds: AtomicU64::new(0),
            sag_rounds: AtomicU64::new(0),
            idle_rounds: AtomicU64::new(0),
            probe_from: AtomicU64::new(0),
            round_delivered: AtomicU64::new(0),
            prev_round_bw: AtomicU64::new(0),
            round_bw_max: std::sync::Mutex::new(crate::bw::MinMax3::new()),
            last_round_bw: AtomicU64::new(0),
            budget_floor: AtomicU64::new(crate::tuning::Tuning::STANDARD.inflight_bias),
            budget_ceil: AtomicU64::new(
                (crate::tuning::Tuning::STANDARD.chan as u64)
                    .saturating_mul(nya_proto::MAX_STREAM_PAYLOAD as u64),
            ),
            ack_rtt_at_us: AtomicU64::new(0),
            loop_fit_last: AtomicBool::new(true),
            loop_unfit_total: AtomicU64::new(0),
            restick_in_at_us: AtomicU64::new(0),
            tcp_fd: std::sync::Mutex::new(None),
        })
    }

    pub fn link(&self) -> &str {
        link_key(&self.name)
    }

    /// Loaded ACK RTT (bulk piece send→ACK), if sampled.
    pub fn ack_rtt(&self) -> Option<Duration> {
        match self.ack_rtt_us.load(Ordering::Relaxed) {
            0 => None,
            us => Some(Duration::from_micros(us)),
        }
    }

    pub fn loop_fit_last(&self) -> bool {
        self.loop_fit_last.load(Ordering::Relaxed)
    }

    /// KD7: the loaded ACK RTT unless the path has been idle (nothing in
    /// flight) for longer than its bandwidth window — then a stale loop
    /// must not stretch the first rounds after the idle.
    pub fn ack_rtt_fresh(&self) -> Option<Duration> {
        let a = self.ack_rtt()?;
        if self.inflight_bytes() == 0 {
            let at = self.ack_rtt_at_us.load(Ordering::Relaxed);
            let age = crate::metrics::mono_us().saturating_sub(at);
            if at == 0 || age > self.bw_window_us() {
                return None;
            }
        }
        Some(a)
    }

    /// P3.1 `loaded(p)`: the expected sojourn of a piece placed now, from
    /// what the path is doing — the fresh ACK loop, floored by how long it
    /// has been quiet with bytes outstanding (a stuck path with no ACK for
    /// 300 ms reads ≥ 300 ms even without a sample). `None` = idle.
    pub fn loaded_sojourn(&self) -> Option<Duration> {
        let outstanding = self.inflight_bytes() > 0;
        match self.ack_rtt_fresh() {
            Some(a) if outstanding => Some(a.max(self.last_rx_ago())),
            Some(a) => Some(a),
            // First round after an idle: no fresh loop yet. The quiet
            // floor applies, but never below the path's own RTT — a
            // just-fed path with a 1 ms `last_rx_ago` is not a 1 ms loop
            // (it would make every loaded sibling a backup).
            None if outstanding => {
                let base = self.min_rtt().unwrap_or_else(|| self.rtt());
                Some(base.max(self.last_rx_ago()))
            }
            None => None,
        }
    }

    /// P3.1 `decayed(p)`: an idle path keeps its last loaded loop, decaying
    /// linearly toward `min_rtt` over `MIN_RTT_WIN_US` — otherwise under
    /// pool-wide loss every idle path looks best and N streams herd onto
    /// it each tick. `None` if never loaded or RTT unknown.
    pub fn decayed_sojourn(&self) -> Option<Duration> {
        let last = self.ack_rtt()?;
        let m = self.min_rtt()?;
        let at = self.ack_rtt_at_us.load(Ordering::Relaxed);
        if at == 0 {
            return None;
        }
        let age = crate::metrics::mono_us().saturating_sub(at);
        let frac = 1.0 - (age as f64 / MIN_RTT_WIN_US as f64).min(1.0);
        let extra = last.saturating_sub(m).mul_f64(frac);
        Some(m + extra)
    }

    /// P3.1: the expected sojourn used for the pool reference — loaded,
    /// else decayed, else the quiet min RTT.
    pub fn sojourn(&self) -> Option<Duration> {
        self.loaded_sojourn()
            .or_else(|| self.decayed_sojourn())
            .or_else(|| self.min_rtt())
    }

    /// P3.1: is this path a fit home for bulk *relative to the pool*? A
    /// path is unfit only when its (loaded or decaying) loop is a backup
    /// (`2× + 20 ms`) to the best sojourn anywhere in the pool. Never
    /// loaded = fit. Also records the fit → unfit transition counter.
    pub fn loop_fit(&self, cfg: &crate::SessionConfig, pool_ref: Option<Duration>) -> bool {
        let fit = match (
            self.loaded_sojourn().or_else(|| self.decayed_sojourn()),
            pool_ref,
        ) {
            (Some(a), Some(r)) => !crate::health::is_backup(cfg, a, r),
            _ => true,
        };
        let was = self.loop_fit_last.swap(fit, Ordering::Relaxed);
        if was && !fit {
            self.loop_unfit_total.fetch_add(1, Ordering::Relaxed);
        }
        fit
    }

    /// EWMA(1/8) update of the loaded ACK RTT.
    pub fn record_ack_rtt(&self, sample: Duration) {
        let s = (sample.as_micros() as u64).max(1);
        self.ack_rtt_at_us
            .store(crate::metrics::mono_us().max(1), Ordering::Relaxed);
        let _ = self
            .ack_rtt_us
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                Some(if cur == 0 { s } else { (cur * 7 + s) / 8 })
            });
    }

    /// Bandwidth filter window: `max(10 × min_rtt, 100 ms)`; unknown RTT
    /// uses the 20 ms placeholder (200 ms window).
    pub fn bw_window_us(&self) -> u64 {
        let rtt_us = if self.rtt_known() {
            self.rtt().as_micros() as u64
        } else {
            crate::tuning::Tuning::STANDARD.unknown_rtt_us
        };
        (rtt_us * 10).max(100_000)
    }

    /// Any round trip this path completed (probe or un-hedged data piece)
    /// feeds the budget's RTT floor. Data samples matter: they include the
    /// kernel's standing queue, which the budget must cover or it, not TCP,
    /// becomes the throughput limiter (BBR's RTprop is likewise taken from
    /// delivered data, not from idle probes).
    pub fn note_loop_rtt(&self, rtt: Duration) {
        let sample = (rtt.as_micros() as u64).max(1);
        self.min_rtt_filter.lock().unwrap().update(
            sample,
            crate::metrics::mono_us().max(1),
            MIN_RTT_WIN_US,
        );
    }

    /// Floor RTT for the budget: windowed min of loop samples; the fast EWMA
    /// when the window is empty. `None` while RTT is unknown.
    pub fn min_rtt(&self) -> Option<Duration> {
        if !self.rtt_known() {
            return None;
        }
        let now = crate::metrics::mono_us();
        let m = self
            .min_rtt_filter
            .lock()
            .unwrap()
            .get(now, MIN_RTT_WIN_US)
            .map(Duration::from_micros)
            .unwrap_or_else(|| self.rtt());
        Some(m)
    }

    /// Windowed-max delivery rate on this path, bytes/s. 0 = no fresh sample.
    pub fn bw_bytes_s(&self) -> u64 {
        let now = crate::metrics::mono_us();
        self.bw_filter.lock().unwrap().get(now, self.bw_window_us())
    }

    pub(crate) fn note_bw_sample(&self, bytes_s: u64, now_us: u64) {
        let win = self.bw_window_us();
        let was = self.bw_filter.lock().unwrap().get(now_us, win);
        let max = self.bw_filter.lock().unwrap().update(bytes_s, now_us, win);
        if was == 0 {
            // Idle gap emptied the filter: restart from the floor (BBR
            // "restart from idle"); the first limited rounds regrow it.
            self.budget
                .store(self.budget_floor.load(Ordering::Relaxed), Ordering::Relaxed);
            self.round_start_us.store(now_us, Ordering::Relaxed);
            self.round_delivered
                .store(self.delivered.load(Ordering::Relaxed), Ordering::Relaxed);
            self.prev_round_bw.store(0, Ordering::Relaxed);
            self.last_round_bw.store(0, Ordering::Relaxed);
            self.round_bw_ref.store(0, Ordering::Relaxed);
            self.round_limited.store(false, Ordering::Relaxed);
            self.flat_rounds.store(0, Ordering::Relaxed);
            self.sag_rounds.store(0, Ordering::Relaxed);
            self.idle_rounds.store(0, Ordering::Relaxed);
            self.probe_from.store(0, Ordering::Relaxed);
            return;
        }
        let round_us = self
            .ack_rtt_fresh()
            .or_else(|| self.min_rtt())
            .map(|d| d.as_micros() as u64)
            .unwrap_or(Tuning::STANDARD.unknown_rtt_us)
            .max(1);
        let start = self.round_start_us.load(Ordering::Relaxed);
        if now_us.saturating_sub(start) < round_us {
            return;
        }
        let _ = max;
        self.end_budget_round(now_us);
    }

    /// Mark this path budget-limited in the current round (a bulk send
    /// found no room here).
    pub fn note_budget_limited(&self) {
        self.round_limited.store(true, Ordering::Relaxed);
    }

    /// One ACK-RTT round of the budget controller. Sitting on TCP, the
    /// overlay cannot know the kernel's cwnd or the queue under it, so the
    /// budget is not a BDP formula (a budget-limited delivery sample is
    /// `budget / loop_rtt`, which only restates the budget). Instead it is
    /// BBR's full-pipe test applied to the budget: `bw_full` is the highest
    /// bandwidth that beat its predecessor by ≥ `BUDGET_GROWTH_MIN`; each
    /// limited round that sets a new `bw_full` raises the budget by
    /// `BUDGET_GROW`; `BUDGET_FULL_ROUNDS` limited rounds without such a
    /// step mean extra allowance buys nothing and growth stops. From then
    /// on every `BUDGET_PROBE_EVERY` limited rounds the budget is raised by
    /// `BUDGET_PROBE` for one round and kept only if bandwidth followed
    /// (ProbeBW). Rounds that never park leave the budget alone; after
    /// `BUDGET_IDLE_SHRINK` such rounds it settles to `2 × bw × loop_rtt`
    /// (≈ 2 × what is actually in flight). `BUDGET_FULL_ROUNDS` rounds with
    /// bandwidth ≥ 25 % under `bw_full` re-arm growth from the lower level.
    fn end_budget_round(&self, now_us: u64) {
        let floor = self.budget_floor.load(Ordering::Relaxed);
        let ceil = self.budget_ceil.load(Ordering::Relaxed).max(floor);
        let start = self.round_start_us.swap(now_us, Ordering::Relaxed);
        let delivered = self.delivered.load(Ordering::Relaxed);
        let dd = delivered.saturating_sub(self.round_delivered.swap(delivered, Ordering::Relaxed));
        let dt = now_us.saturating_sub(start).max(1);
        let bw_now = (dd as u128 * 1_000_000 / dt as u128).min(u64::MAX as u128) as u64;
        // Long window (as the min loop): a budget-limited sag must not pull
        // the ceiling down with it — `bw × min_loop` under a longer loop is
        // below what the budget already carries, and the ceiling would
        // then chase the budget down (the fixed point the round test
        // exists to avoid).
        // Fed with the lower of two consecutive rounds: one fat round (a
        // released backlog) cannot lift the ceiling, two in a row can.
        let last = self.last_round_bw.swap(bw_now, Ordering::Relaxed);
        let confirmed = if last == 0 { bw_now } else { bw_now.min(last) };
        let bw_max = self
            .round_bw_max
            .lock()
            .unwrap()
            .update(confirmed, now_us, MIN_RTT_WIN_US);
        let cap = match self.min_rtt() {
            Some(m) if bw_max > 0 => {
                let bdp = (bw_max as u128 * m.as_micros() / 1_000_000) as u64;
                bdp.saturating_mul(BUDGET_CAP_GAIN).max(floor)
            }
            _ => ceil,
        };
        let ceil = ceil.min(cap).max(floor);
        let limited = self.round_limited.swap(false, Ordering::Relaxed);
        let cur = self.budget.load(Ordering::Relaxed).clamp(floor, ceil);
        let full = self.round_bw_ref.load(Ordering::Relaxed);
        let step_now = bw_now as f64 >= full as f64 * (1.0 + BUDGET_GROWTH_MIN);
        // A step counts only when two consecutive rounds clear the bar. A
        // backlog the TCP under us releases after loss recovery lands as
        // one fat round followed by a lean one; real headroom shows in
        // every round after the budget grew.
        let pending = self.prev_round_bw.load(Ordering::Relaxed);
        let stepped = step_now && pending != 0;
        self.prev_round_bw.store(
            if step_now && !stepped { bw_now } else { 0 },
            Ordering::Relaxed,
        );
        let sagged = full > 0 && (bw_now as f64) < full as f64 * (1.0 - BUDGET_GROWTH_MIN);
        let probe_from = self.probe_from.swap(0, Ordering::Relaxed);
        let loop_us = self
            .ack_rtt_fresh()
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);
        let carried = (bw_now as u128 * loop_us as u128 / 1_000_000) as u64;

        if stepped {
            self.round_bw_ref
                .store(bw_now.min(pending), Ordering::Relaxed);
            self.flat_rounds.store(0, Ordering::Relaxed);
            self.sag_rounds.store(0, Ordering::Relaxed);
        } else if step_now {
            // First round over the bar: wait for the confirmation.
            self.sag_rounds.store(0, Ordering::Relaxed);
        } else if sagged {
            let sag = self.sag_rounds.fetch_add(1, Ordering::Relaxed) + 1;
            if sag >= BUDGET_FULL_ROUNDS {
                // Path lost capacity (or its loop lengthened under someone
                // else's queue): re-arm from where it is now. Bandwidth
                // cannot rise before the budget does, so from here the
                // probes carry the recovery, not the step test.
                self.round_bw_ref.store(bw_now, Ordering::Relaxed);
                self.prev_round_bw.store(0, Ordering::Relaxed);
                self.flat_rounds.store(0, Ordering::Relaxed);
                self.sag_rounds.store(0, Ordering::Relaxed);
                self.idle_rounds.store(0, Ordering::Relaxed);
                let next = cur.min(carried.saturating_mul(2)).max(floor);
                self.budget
                    .store(next.clamp(floor, ceil), Ordering::Relaxed);
                return;
            }
        } else {
            self.sag_rounds.store(0, Ordering::Relaxed);
        }

        let next = if limited {
            self.idle_rounds.store(0, Ordering::Relaxed);
            if probe_from != 0 {
                // A ×1.25 probe can raise bandwidth by at most 25 %, so the
                // full step bar would never keep one; half of it means the
                // link had room.
                if bw_now as f64 >= full as f64 * (1.0 + BUDGET_GROWTH_MIN / 2.0) {
                    self.round_bw_ref.store(bw_now, Ordering::Relaxed);
                    self.flat_rounds.store(0, Ordering::Relaxed);
                    cur
                } else {
                    // Probe bought nothing: back to the held allowance.
                    probe_from
                }
            } else if stepped {
                mul(cur, BUDGET_GROW)
            } else if step_now {
                cur
            } else {
                let flat = self.flat_rounds.fetch_add(1, Ordering::Relaxed) + 1;
                if flat >= BUDGET_FULL_ROUNDS
                    && (flat - BUDGET_FULL_ROUNDS).is_multiple_of(BUDGET_PROBE_EVERY)
                {
                    self.probe_from.store(cur, Ordering::Relaxed);
                    mul(cur, BUDGET_PROBE)
                } else {
                    cur
                }
            }
        } else {
            let idle = self.idle_rounds.fetch_add(1, Ordering::Relaxed) + 1;
            if idle >= BUDGET_IDLE_SHRINK {
                cur.min(carried.saturating_mul(2)).max(floor)
            } else {
                cur
            }
        };
        self.budget
            .store(next.clamp(floor, ceil), Ordering::Relaxed);
    }

    /// Per-path send budget (overlay cwnd), bytes.
    pub fn budget_bytes(&self) -> u64 {
        let floor = self.budget_floor.load(Ordering::Relaxed);
        let ceil = self.budget_ceil.load(Ordering::Relaxed).max(floor);
        self.budget.load(Ordering::Relaxed).clamp(floor, ceil)
    }

    /// Budget minus inflight; 0 when at or over budget.
    pub fn room_bytes(&self) -> u64 {
        self.budget_bytes().saturating_sub(self.inflight_bytes())
    }

    /// Kernel TCP state of this path's socket, if we hold a dup.
    pub fn tcp_info(&self) -> Option<crate::net::TcpInfo> {
        self.tcp_fd
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|f| f.tcp_info())
    }

    pub fn inflight_bytes(&self) -> u64 {
        self.inflight.load(Ordering::Relaxed)
    }

    pub fn add_inflight(&self, n: u64) {
        if n != 0 {
            self.inflight.fetch_add(n, Ordering::Relaxed);
        }
    }

    pub fn sub_inflight(&self, n: u64) {
        if n == 0 {
            return;
        }
        let _ = self
            .inflight
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(n))
            });
    }

    pub fn sticky_count(&self) -> u64 {
        self.sticky_streams.load(Ordering::Relaxed)
    }

    pub fn add_sticky(&self) {
        self.sticky_streams.fetch_add(1, Ordering::Relaxed);
    }

    pub fn drop_sticky(&self) {
        let _ = self
            .sticky_streams
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            });
    }

    pub fn is_up(&self) -> bool {
        self.state.load(Ordering::Relaxed) == STATE_UP
    }

    pub fn is_alive(&self) -> bool {
        let s = self.state.load(Ordering::Relaxed);
        s == STATE_UP || s == STATE_DEGRADED
    }

    pub fn is_congested(&self) -> bool {
        self.congested.load(Ordering::Relaxed)
    }

    pub fn set_congested(&self, v: bool) {
        self.congested.store(v, Ordering::Relaxed);
    }

    pub fn is_write_stalled(&self) -> bool {
        self.write_stalled.load(Ordering::Relaxed)
    }

    pub fn set_write_stalled(&self, v: bool) {
        self.write_stalled.store(v, Ordering::SeqCst);
    }

    pub fn up_age(&self) -> Duration {
        self.up_since.lock().unwrap().elapsed()
    }

    /// Eligible for new sticky assignment: up, not enqueue-blocked, not flush-stalled.
    pub fn is_schedulable(&self) -> bool {
        self.is_up() && !self.is_congested() && !self.is_write_stalled()
    }

    pub fn rtt_known(&self) -> bool {
        self.rtt_ewma_us.load(Ordering::Relaxed) != 0
    }

    pub fn rtt_us(&self) -> u64 {
        let v = self.rtt_ewma_us.load(Ordering::Relaxed);
        if v == 0 {
            Tuning::STANDARD.unknown_rtt_us
        } else {
            v
        }
    }

    /// Recent RTT (fast EWMA). Used for *score* inside a class.
    pub fn rtt(&self) -> Duration {
        Duration::from_micros(self.rtt_us())
    }

    pub fn stable_rtt(&self) -> Duration {
        let v = self.rtt_stable_us.load(Ordering::Relaxed);
        Duration::from_micros(if v == 0 { self.rtt_us() } else { v })
    }

    /// Class membership / backup / failback-class filter. Two-sided
    /// hold-EWMA, falling back to fast EWMA before the first sample.
    pub fn class_rtt(&self) -> Duration {
        let v = self.rtt_class_us.load(Ordering::Relaxed);
        Duration::from_micros(if v == 0 { self.rtt_us() } else { v })
    }

    pub fn class_known(&self) -> bool {
        self.rtt_class_us.load(Ordering::Relaxed) != 0
    }

    pub fn stable_for(&self) -> Duration {
        self.up_since.lock().unwrap().elapsed()
    }

    pub fn touch_rx(&self) {
        *self.last_rx.lock().unwrap() = Instant::now();
        if self.state.load(Ordering::Relaxed) == STATE_DEGRADED {
            // Do not reset up_since: degrade↔up flaps must not postpone failback.
            self.state.store(STATE_UP, Ordering::Relaxed);
        }
    }

    pub fn last_rx_ago(&self) -> Duration {
        self.last_rx.lock().unwrap().elapsed()
    }

    /// Age of the oldest in-flight ping, if any.
    pub fn pending_ping_age(&self) -> Option<Duration> {
        self.pending_ping
            .lock()
            .unwrap()
            .values()
            .map(|t| t.elapsed())
            .max()
    }

    pub fn last_tx_ago(&self) -> Duration {
        self.last_tx.lock().unwrap().elapsed()
    }

    pub fn queued_urgent(&self) -> u64 {
        self.urgent_queued.load(Ordering::Relaxed)
    }

    pub fn queued_bulk(&self) -> u64 {
        self.bulk_queued.load(Ordering::Relaxed)
    }

    pub fn ack_pending(&self) -> u64 {
        self.pending_acks.lock().unwrap().len() as u64
    }

    pub(crate) fn take_acks(&self, k: usize) -> Vec<StreamAck> {
        let mut g = self.pending_acks.lock().unwrap();
        let ids: Vec<u32> = g.keys().copied().take(k).collect();
        ids.into_iter().filter_map(|id| g.remove(&id)).collect()
    }

    pub(crate) fn take_all_acks(&self) -> HashMap<u32, StreamAck> {
        std::mem::take(&mut *self.pending_acks.lock().unwrap())
    }

    /// Re-insert without going backwards if a newer generation already landed.
    pub(crate) fn merge_ack(&self, ack: StreamAck) {
        let mut g = self.pending_acks.lock().unwrap();
        match g.entry(ack.stream_id) {
            std::collections::hash_map::Entry::Occupied(e)
                if e.get().acked_offset > ack.acked_offset => {}
            std::collections::hash_map::Entry::Occupied(mut e) => {
                e.insert(ack);
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(ack);
            }
        }
    }

    pub(crate) fn restore_acks(&self, acks: impl IntoIterator<Item = StreamAck>) {
        for ack in acks {
            self.merge_ack(ack);
        }
    }

    pub fn note_enqueue(&self, urgent: bool) {
        if urgent {
            self.urgent_queued.fetch_add(1, Ordering::Relaxed);
        } else {
            self.bulk_queued.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Undo `note_enqueue` after a failed `try_send`. Must not wake C4 waiters
    /// (that permit would spin send_data on a still-full queue).
    pub fn undo_enqueue(&self, urgent: bool) {
        let q = if urgent {
            &self.urgent_queued
        } else {
            &self.bulk_queued
        };
        let _ = q.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
            Some(v.saturating_sub(1))
        });
    }

    pub fn note_dequeue(&self, urgent: bool) {
        self.undo_enqueue(urgent);
        // C4 waits on bulk space. Urgent dequeue must not abort that wait.
        if !urgent {
            self.queue_wait.notify_one();
        }
    }

    pub fn pending_ping_count(&self) -> u64 {
        self.pending_ping.lock().unwrap().len() as u64
    }

    /// UP and DEGRADED probe; DOWN does not. At most one in-flight Ping.
    /// Idle-gate is `ago >= ping_every`.
    pub(crate) fn should_send_ping(&self, ago: Duration, ping_every: Duration) -> bool {
        self.is_alive() && self.pending_ping_count() == 0 && ago >= ping_every
    }

    /// Move Instant from pending to late when older than `max_age`.
    /// Returns how many were expired (probe_miss). Does not drop Instant.
    pub fn expire_stale_pings(&self, max_age: Duration) -> u64 {
        let mut pending = self.pending_ping.lock().unwrap();
        let mut late = self.late_ping.lock().unwrap();
        let stale: Vec<u64> = pending
            .iter()
            .filter(|(_, t)| t.elapsed() >= max_age)
            .map(|(seq, _)| *seq)
            .collect();
        let n = stale.len() as u64;
        for seq in stale {
            if let Some(t0) = pending.remove(&seq) {
                late.insert(seq, t0);
            }
        }
        n
    }

    pub fn drop_ancient_pings(&self, max_age: Duration) {
        let mut pending = self.pending_ping.lock().unwrap();
        let mut late = self.late_ping.lock().unwrap();
        pending.retain(|_, t| t.elapsed() < max_age);
        late.retain(|_, t| t.elapsed() < max_age);
    }

    pub fn note_tx(&self) {
        *self.last_tx.lock().unwrap() = Instant::now();
    }

    pub fn record_rtt(&self, rtt: Duration) {
        let sample = rtt.as_micros() as u64;
        self.note_loop_rtt(rtt);
        let old = self.rtt_ewma_us.load(Ordering::Relaxed);
        let fast = if old == 0 {
            sample
        } else {
            (old * 8 + sample * 2) / 10
        };
        self.rtt_ewma_us.store(fast, Ordering::Relaxed);

        // Timeout-stable. Local control flow only — do not return from record_rtt.
        {
            let s_old = self.rtt_stable_us.load(Ordering::Relaxed);
            if s_old == 0 {
                self.rtt_stable_us.store(sample, Ordering::Relaxed);
            } else if sample < s_old {
                self.rtt_stable_us
                    .store((s_old * 3 + sample) / 4, Ordering::Relaxed);
                *self.high_since.lock().unwrap() = None;
            } else {
                let t = &Tuning::STANDARD;
                let high = fast > s_old.saturating_mul(t.stable_raise_mult)
                    && fast > s_old + t.stable_raise_add_us;
                let mut g = self.high_since.lock().unwrap();
                if !high {
                    *g = None;
                } else {
                    let start = g.get_or_insert_with(Instant::now);
                    let hold =
                        Duration::from_micros(self.stable_up_hold_us.load(Ordering::Relaxed));
                    if start.elapsed() >= hold {
                        self.rtt_stable_us
                            .store((s_old * 7 + fast) / 8, Ordering::Relaxed);
                    }
                }
            }
        }

        self.update_class(fast);
    }

    fn update_class(&self, fast: u64) {
        let c_old = self.rtt_class_us.load(Ordering::Relaxed);
        if c_old == 0 {
            // Do not freeze class on the first sample — a lucky-low Pong
            // (90ms on a 180ms path) would class-jump every sibling onto it.
            let n = self.class_init_n.fetch_add(1, Ordering::Relaxed) + 1;
            if n >= 8 {
                self.rtt_class_us.store(fast, Ordering::Relaxed);
                self.note_class_known_now();
                // Class store that may need unwind if later Pongs pull
                // fast under class (including below class_should_drop).
                // class == fast here, so permit && fast < class is false.
                self.class_unwind_permit.store(true, Ordering::Relaxed);
                tracing::debug!(
                    path = %self.name,
                    old_us = 0u64,
                    new_us = fast,
                    kind = "init",
                    "class"
                );
            }
            return;
        }
        let t = &Tuning::STANDARD;
        let hold = Duration::from_micros(self.stable_up_hold_us.load(Ordering::Relaxed));
        let raise = fast > c_old.saturating_mul(t.stable_raise_mult)
            && fast > c_old + t.stable_raise_add_us;

        // Lock order: class_high_since, class_low_since, class_low_accum.
        // class_unwind_permit is Relaxed while this trio is held, like rtt_class_us.
        let mut high = self.class_high_since.lock().unwrap();
        let mut low = self.class_low_since.lock().unwrap();
        let mut accum = self.class_low_accum.lock().unwrap();
        if raise {
            *low = None;
            *accum = Duration::ZERO;
            let start = high.get_or_insert_with(Instant::now);
            if start.elapsed() >= hold {
                let new_us = (c_old * 7 + fast) / 8;
                self.rtt_class_us.store(new_us, Ordering::Relaxed);
                *high = None; // one 7/8 per hold; timeout-stable raise stays a ratchet
                self.class_unwind_permit.store(true, Ordering::Relaxed);
                tracing::info!(
                    path = %self.name,
                    old_us = c_old,
                    new_us,
                    kind = "raise",
                    "class"
                );
            }
            return;
        }
        *high = None;
        let drop = t.class_should_drop(c_old, fast)
            || (self.class_unwind_permit.load(Ordering::Relaxed) && fast < c_old);
        if drop {
            let start = low.get_or_insert_with(Instant::now);
            if start.elapsed().saturating_add(*accum) >= hold {
                let new_us = (c_old * 7 + fast) / 8;
                self.rtt_class_us.store(new_us, Ordering::Relaxed);
                *low = None;
                *accum = Duration::ZERO;
                // Clear only when integer 7/8 has met fast.
                // (7(f+1)+f)/8 = f when c_old == fast + 1.
                if new_us <= fast {
                    self.class_unwind_permit.store(false, Ordering::Relaxed);
                }
                tracing::info!(
                    path = %self.name,
                    old_us = c_old,
                    new_us,
                    kind = "drop",
                    "class"
                );
            }
            return;
        }
        // Dead zone (class, 2×class]: leave permit true and G4a-pause.
        if let Some(start) = low.take() {
            *accum = accum.saturating_add(start.elapsed());
        }
    }

    pub(crate) fn mark_outlier(&self) -> Duration {
        let mut g = self.outlier_since.lock().unwrap();
        let start = g.get_or_insert_with(Instant::now);
        start.elapsed()
    }

    pub(crate) fn clear_outlier(&self) {
        *self.outlier_since.lock().unwrap() = None;
    }

    pub(crate) fn note_class_known_now(&self) {
        *self.class_known_since.lock().unwrap() = Some(Instant::now());
    }

    pub(crate) fn class_known_aged(&self, hold: Duration) -> bool {
        match *self.class_known_since.lock().unwrap() {
            Some(t) => t.elapsed() >= hold,
            None => false,
        }
    }

    #[cfg(test)]
    pub(crate) fn backdate_up_since(&self, age: Duration) {
        *self.up_since.lock().unwrap() =
            Instant::now().checked_sub(age).unwrap_or_else(Instant::now);
    }

    #[cfg(test)]
    pub(crate) fn backdate_class_known(&self, age: Duration) {
        *self.class_known_since.lock().unwrap() =
            Some(Instant::now().checked_sub(age).unwrap_or_else(Instant::now));
    }

    #[cfg(test)]
    pub(crate) fn backdate_outlier(&self, age: Duration) {
        *self.outlier_since.lock().unwrap() =
            Some(Instant::now().checked_sub(age).unwrap_or_else(Instant::now));
    }

    #[cfg(test)]
    pub(crate) fn class_known_since_for_test(&self) -> Option<Instant> {
        *self.class_known_since.lock().unwrap()
    }

    #[cfg(test)]
    pub(crate) fn outlier_since_for_test(&self) -> Option<Instant> {
        *self.outlier_since.lock().unwrap()
    }

    #[cfg(test)]
    pub(crate) fn class_unwind_permit_for_test(&self) -> bool {
        self.class_unwind_permit.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub fn class_low_accum(&self) -> Duration {
        *self.class_low_accum.lock().unwrap()
    }

    #[cfg(test)]
    pub(crate) fn backdate_pending_ping(&self, age: Duration) {
        let mut pending = self.pending_ping.lock().unwrap();
        for t in pending.values_mut() {
            *t = Instant::now().checked_sub(age).unwrap_or(*t);
        }
    }

    pub fn mark_degraded(&self) {
        let _ = self.state.compare_exchange(
            STATE_UP,
            STATE_DEGRADED,
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
    }

    pub fn next_ping(&self) -> Ping {
        let seq = self.ping_seq.fetch_add(1, Ordering::Relaxed);
        let mut pending = self.pending_ping.lock().unwrap();
        let mut late = self.late_ping.lock().unwrap();
        let cap = Tuning::STANDARD.pending_ping_max;
        while pending.len() + late.len() >= cap {
            let Some(oldest) = late.iter().min_by_key(|(_, t)| *t).map(|(k, _)| *k) else {
                break;
            };
            late.remove(&oldest);
        }
        pending.insert(seq, Instant::now());
        Ping {
            seq,
            sent_at_ms: now_ms(),
        }
    }

    /// Prefer local Instant (µs) over the millisecond wall-clock echo.
    pub fn on_pong(&self, seq: u64, sent_at_ms: u64) {
        self.on_pong_record(seq, sent_at_ms, true, None, true);
    }

    pub(crate) fn is_tls_unexpected_eof(e: &std::io::Error) -> bool {
        e.kind() == std::io::ErrorKind::UnexpectedEof
    }

    /// Always clear the pending/late ping. Skip `record_rtt` when the
    /// sample rode behind bulk inflight. No wall-clock fallback.
    ///
    /// `allow_late`: expired (moved-to-late) Pongs on a **known** path are
    /// clear-only — TCP min-RTO must not poison EWMA. Unknown dests still
    /// take a first late sample so a 60–200 ms path can freeze class.
    pub fn on_pong_record(
        &self,
        seq: u64,
        _sent_at_ms: u64,
        record: bool,
        cap: Option<Duration>,
        allow_late: bool,
    ) {
        let started = {
            let mut pending = self.pending_ping.lock().unwrap();
            pending.remove(&seq)
        };
        let started = match started {
            Some(t0) => Some(t0),
            None => {
                let t0 = self.late_ping.lock().unwrap().remove(&seq);
                if t0.is_some() && !allow_late {
                    return;
                }
                t0
            }
        };
        if !record {
            return;
        }
        let Some(t0) = started else {
            return;
        };
        let sample = t0.elapsed();
        if let Some(cap) = cap {
            if sample > cap {
                return;
            }
        }
        self.record_rtt(sample);
    }
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

async fn send_frame<S>(
    framed: &mut S,
    session: &Session,
    path: &PathState,
    frame: Frame,
) -> std::io::Result<()>
where
    S: Sink<Bytes, Error = std::io::Error> + Unpin,
{
    path.note_tx();
    let encoded = frame.encode();
    let n = encoded.len();
    framed.send(Bytes::from(encoded)).await?;
    session.account_overlay_frame(&frame, n, true);
    Ok(())
}

/// Idle/DOWN close: flush SessionClose / Reset already in urgent, then TLS close_notify.
async fn flush_urgent_then_close<S>(
    writer: &mut S,
    urgent: &mut mpsc::Receiver<Frame>,
    session: &Session,
    path: &PathState,
    ping_max: Duration,
) where
    S: Sink<Bytes, Error = std::io::Error> + Unpin,
{
    let deadline = tokio::time::Instant::now() + ping_max;
    while let Ok(frame) = urgent.try_recv() {
        path.note_dequeue(true);
        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }
        let remain = deadline.saturating_duration_since(now);
        let _ = tokio::time::timeout(remain, send_frame(writer, session, path, frame)).await;
    }
    let now = tokio::time::Instant::now();
    let remain = deadline
        .saturating_duration_since(now)
        .max(Duration::from_millis(1));
    let _ = tokio::time::timeout(remain, writer.close()).await;
}

/// Park STREAM_DATA off urgent only until first Pong. Write-stall is pick-skip,
/// not bulk-kill: known+stalled DATA is forced onto bulk in `send_on_path`.
fn hold_stream_data(path: &PathState, frame: &Frame) -> bool {
    matches!(frame, Frame::StreamData(_)) && !path.rtt_known()
}

fn park_stream_data(path: &PathState, session: &Session, frame: Frame) {
    path.note_enqueue(false);
    if let Err(e) = path.writer.try_send(frame) {
        path.undo_enqueue(false);
        session.note_send_drop();
        // P4: the piece sits in `unacked` on this path with nothing on the
        // wire. Flag it so retry does not wait for the path to go silent.
        if let Frame::StreamData(d) = e.into_inner() {
            session.note_data_dropped(d.stream_id, d.offset);
        }
    }
}

/// Split the TLS stream with `tokio::io::split`, not `Framed::split()`.
/// `Framed::split` holds a BiLock across `send().await` flush, so a blocked
/// write still starves `next()`. `tokio::io::split` releases on `Pending`.
pub fn spawn_path_io<T>(
    session: Session,
    path: Arc<PathState>,
    io: T,
    mut rx: mpsc::Receiver<Frame>,
    mut urgent: mpsc::Receiver<Frame>,
    done: tokio::sync::oneshot::Sender<()>,
) where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut io = io;
        if let Err(e) = io.flush().await {
            warn!(path = %path.name, error = %e, "tls flush after handshake failed");
            session.path_failed(path.id);
            let _ = done.send(());
            return;
        }
        let (rd, wr) = tokio::io::split(io);
        let codec = LengthDelimitedCodec::builder()
            .max_frame_length(MAX_FRAME_SIZE)
            .new_codec();
        let mut reader = FramedRead::new(rd, codec.clone());
        let mut writer = FramedWrite::new(wr, codec);
        if let Err(e) = writer.flush().await {
            warn!(path = %path.name, error = %e, "path flush after handshake failed");
            session.path_failed(path.id);
            let _ = done.send(());
            return;
        }

        let (close_tx, mut close_rx) = tokio::sync::oneshot::channel::<()>();
        let ping_max = session.config().ping_interval_max;

        let session_r = session.clone();
        let path_r = path.clone();
        let mut read_task = tokio::spawn(async move {
            loop {
                match reader.next().await {
                    None => {
                        if path_r.is_alive() {
                            info!(
                                path = %path_r.name,
                                path_id = path_r.id,
                                "path eof"
                            );
                        } else {
                            debug!(path = %path_r.name, "path eof");
                        }
                        return Ok(());
                    }
                    Some(Err(e)) => {
                        if PathState::is_tls_unexpected_eof(&e) && !path_r.is_alive() {
                            debug!(path = %path_r.name, "path eof");
                            return Ok(());
                        }
                        warn!(path = %path_r.name, error = %e, "path read failed");
                        return Err(e);
                    }
                    Some(Ok(bytes)) => match Frame::decode(&bytes) {
                        Ok(frame) => {
                            session_r.account_overlay_frame(&frame, bytes.len(), false);
                            path_r.touch_rx();
                            session_r.handle_frame(path_r.id, frame);
                        }
                        Err(e) => {
                            warn!(path = %path_r.name, error = %e, "bad frame");
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                e.to_string(),
                            ));
                        }
                    },
                }
            }
        });

        enum WriteOne {
            Sent {
                stalled: bool,
            },
            /// close_rx fired during this send; caller flushes urgent then close_notify.
            Interrupted,
            Io(std::io::Error),
        }

        struct RestoreAcks<'a> {
            path: &'a PathState,
            rest: VecDeque<StreamAck>,
            in_flight: Option<StreamAck>,
        }

        impl Drop for RestoreAcks<'_> {
            fn drop(&mut self) {
                // Park on this Arc only. A concurrent path_failed may already
                // have removed the dest; spawn_path_io merges leftovers onto alt.
                if let Some(ack) = self.in_flight.take() {
                    self.path.merge_ack(ack);
                }
                self.path.restore_acks(self.rest.drain(..));
            }
        }

        let session_w = session.clone();
        let path_w = path.clone();
        let mut write_task = tokio::spawn(async move {
            let mut next_ping = tokio::time::Instant::now();
            loop {
                let ping_every = session_w.probe_interval_for(&path_w);
                let deadline = session_w.write_deadline(path_w.id);
                let ping_due = tokio::time::Instant::now() >= next_ping
                    && path_w.is_alive()
                    && !session_w.is_dead()
                    && path_w.should_send_ping(path_w.last_rx_ago(), ping_every);
                let acks_ready = path_w.ack_pending() > 0;

                tokio::select! {
                    biased;
                    _ = &mut close_rx => {
                        flush_urgent_then_close(
                            &mut writer,
                            &mut urgent,
                            &session_w,
                            &path_w,
                            ping_max,
                        )
                        .await;
                        return Ok(());
                    }
                    _ = std::future::ready(()), if ping_due => {
                        let ping = path_w.next_ping();
                        match write_one(
                            &mut writer,
                            &mut close_rx,
                            deadline,
                            ping_max,
                            &session_w,
                            &path_w,
                            Frame::Ping(ping),
                        )
                        .await
                        {
                            WriteOne::Sent { stalled } => {
                                if !stalled {
                                    path_w.set_write_stalled(false);
                                }
                                next_ping = tokio::time::Instant::now() + ping_every;
                            }
                            WriteOne::Interrupted => {
                                flush_urgent_then_close(
                                    &mut writer,
                                    &mut urgent,
                                    &session_w,
                                    &path_w,
                                    ping_max,
                                )
                                .await;
                                return Ok(());
                            }
                            WriteOne::Io(e) => {
                                warn!(path = %path_w.name, error = %e, "path ping failed");
                                return Err(e);
                            }
                        }
                    }
                    // STREAM_ACK is not StreamData; hold_stream_data does not apply.
                    _ = async {
                        if acks_ready {
                            std::future::ready(()).await;
                        } else {
                            path_w.ack_wait.notified().await;
                        }
                    } => {
                        let mut guard = RestoreAcks {
                            path: &path_w,
                            rest: VecDeque::from(path_w.take_acks(ACK_FLUSH_K)),
                            in_flight: None,
                        };
                        while let Some(ack) = guard.rest.pop_front() {
                            guard.in_flight = Some(ack);
                            let (sid, frame) = {
                                let ack = guard.in_flight.as_ref().expect("ACK in flight");
                                (ack.stream_id, Frame::StreamAck(ack.clone()))
                            };
                            match write_one(
                                &mut writer,
                                &mut close_rx,
                                deadline,
                                ping_max,
                                &session_w,
                                &path_w,
                                frame,
                            )
                            .await
                            {
                                WriteOne::Sent { stalled } => {
                                    guard.in_flight = None;
                                    if !stalled {
                                        path_w.set_write_stalled(false);
                                    }
                                    session_w.note_ack_sent(&path_w, sid);
                                }
                                WriteOne::Interrupted => {
                                    flush_urgent_then_close(
                                        &mut writer,
                                        &mut urgent,
                                        &session_w,
                                        &path_w,
                                        ping_max,
                                    )
                                    .await;
                                    return Ok(());
                                }
                                WriteOne::Io(e) => {
                                    warn!(path = %path_w.name, error = %e, "path ack failed");
                                    return Err(e);
                                }
                            }
                        }
                    }
                    out = urgent.recv() => {
                        let Some(frame) = out else {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::BrokenPipe,
                                "urgent writer closed",
                            ));
                        };
                        path_w.note_dequeue(true);
                        if hold_stream_data(&path_w, &frame) {
                            park_stream_data(&path_w, &session_w, frame);
                            continue;
                        }
                        match write_one(
                            &mut writer,
                            &mut close_rx,
                            deadline,
                            ping_max,
                            &session_w,
                            &path_w,
                            frame,
                        )
                        .await
                        {
                            WriteOne::Sent { stalled } => {
                                if !stalled {
                                    path_w.set_write_stalled(false);
                                }
                            }
                            WriteOne::Interrupted => {
                                flush_urgent_then_close(
                                    &mut writer,
                                    &mut urgent,
                                    &session_w,
                                    &path_w,
                                    ping_max,
                                )
                                .await;
                                return Ok(());
                            }
                            WriteOne::Io(e) => {
                                warn!(path = %path_w.name, error = %e, "path write failed");
                                return Err(e);
                            }
                        }
                    }
                    out = rx.recv(), if path_w.rtt_known() => {
                        let Some(frame) = out else {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::BrokenPipe,
                                "bulk writer closed",
                            ));
                        };
                        path_w.note_dequeue(false);
                        match write_one(
                            &mut writer,
                            &mut close_rx,
                            deadline,
                            ping_max,
                            &session_w,
                            &path_w,
                            frame,
                        )
                        .await
                        {
                            WriteOne::Sent { stalled } => {
                                if !stalled {
                                    path_w.set_write_stalled(false);
                                }
                            }
                            WriteOne::Interrupted => {
                                flush_urgent_then_close(
                                    &mut writer,
                                    &mut urgent,
                                    &session_w,
                                    &path_w,
                                    ping_max,
                                )
                                .await;
                                return Ok(());
                            }
                            WriteOne::Io(e) => {
                                warn!(path = %path_w.name, error = %e, "path write failed");
                                return Err(e);
                            }
                        }
                    }
                    _ = tokio::time::sleep_until(next_ping), if !ping_due => {}
                }
            }
        });

        async fn write_one<S>(
            writer: &mut S,
            close_rx: &mut tokio::sync::oneshot::Receiver<()>,
            deadline: Duration,
            _ping_max: Duration,
            session: &Session,
            path: &PathState,
            frame: Frame,
        ) -> WriteOne
        where
            S: Sink<Bytes, Error = std::io::Error> + Unpin,
        {
            let mut this_stalled = false;
            let t0 = tokio::time::Instant::now();
            let send = send_frame(writer, session, path, frame);
            tokio::pin!(send);
            loop {
                tokio::select! {
                    biased;
                    _ = &mut *close_rx => {
                        return WriteOne::Interrupted;
                    }
                    r = &mut send => {
                        return match r {
                            Ok(()) => WriteOne::Sent {
                                stalled: this_stalled,
                            },
                            Err(e) => WriteOne::Io(e),
                        };
                    }
                    _ = tokio::time::sleep(deadline), if !this_stalled => {
                        this_stalled = true;
                        let first = !path.is_write_stalled();
                        path.set_write_stalled(true);
                        if first {
                            info!(
                                path = %path.name,
                                path_id = path.id,
                                waited_ms = t0.elapsed().as_millis() as u64,
                                deadline_ms = deadline.as_millis() as u64,
                                "path write stalled"
                            );
                        }
                    }
                }
            }
        }

        enum Exit {
            Idle,
            Down,
            Child,
        }
        let ping_max = session.config().ping_interval_max;
        let maintain = session.config().tuning.maintain_interval;
        let exit = tokio::select! {
            biased;
            _ = &mut read_task => Exit::Child,
            _ = &mut write_task => Exit::Child,
            _ = session.wait_dead() => Exit::Idle,
            _ = async {
                loop {
                    if !path.is_alive() {
                        break;
                    }
                    tokio::time::sleep(maintain).await;
                }
            } => Exit::Down,
        };
        match exit {
            Exit::Idle | Exit::Down => {
                let _ = close_tx.send(());
                if tokio::time::timeout(ping_max, &mut write_task)
                    .await
                    .is_err()
                {
                    write_task.abort();
                    let _ = write_task.await;
                }
                read_task.abort();
            }
            Exit::Child => {
                read_task.abort();
                if !write_task.is_finished() {
                    write_task.abort();
                    let _ = write_task.await;
                }
            }
        }
        // Close our TCP_INFO dup before the socket halves drop.
        *path.tcp_fd.lock().unwrap() = None;
        session.path_failed(path.id);
        // Drop parked unsent rows on this Arc. If maintain already path_failed,
        // that call was a no-op; merge leftovers onto alt from the local Arc.
        session.merge_pending_acks(path.id, path.take_all_acks());
        let _ = done.send(());
        debug!(path = %path.name, "path io exit");
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use nya_proto::StreamData;
    use tokio::sync::mpsc;

    fn path() -> Arc<PathState> {
        let (tx, _rx) = mpsc::channel(1);
        PathState::new(1, "t".into(), tx)
    }

    #[test]
    fn restore_acks_keeps_max_offset() {
        let p = path();
        p.merge_ack(StreamAck {
            stream_id: 1,
            acked_offset: 200,
            window: 2,
            sack: vec![],
        });
        p.restore_acks([StreamAck {
            stream_id: 1,
            acked_offset: 100,
            window: 1,
            sack: vec![],
        }]);
        let g = p.pending_acks.lock().unwrap();
        let ack = g.get(&1).unwrap();
        assert_eq!(ack.acked_offset, 200);
        assert_eq!(ack.window, 2);
    }

    #[test]
    fn pending_ping_age_is_oldest() {
        let p = path();
        p.next_ping();
        std::thread::sleep(Duration::from_millis(15));
        p.next_ping();
        let age = p.pending_ping_age().expect("pending");
        assert!(
            age >= Duration::from_millis(12),
            "oldest ping age {age:?} must be the first insert, not ~0"
        );
    }

    #[test]
    fn expired_pong_does_not_raise_ewma() {
        let p = path();
        p.record_rtt(Duration::from_millis(7));
        let before = p.rtt_ewma_us.load(Ordering::Relaxed);
        p.on_pong_record(99, 0, true, None, true);
        assert_eq!(
            p.rtt_ewma_us.load(Ordering::Relaxed),
            before,
            "seq not in pending must not record wall-clock RTT"
        );
    }

    #[test]
    fn expired_pong_unknown_path_records_first_sample() {
        let p = path();
        assert!(!p.rtt_known());
        let ping = p.next_ping();
        let miss = p.expire_stale_pings(Duration::ZERO);
        assert_eq!(miss, 1);
        assert_eq!(p.pending_ping_count(), 0);
        std::thread::sleep(Duration::from_millis(12));
        p.on_pong_record(
            ping.seq,
            ping.sent_at_ms,
            true,
            Some(Duration::from_millis(300)),
            true,
        );
        assert!(p.rtt_known(), "late Instant must still freeze unknown RTT");
    }

    #[test]
    fn unknown_instant_957ms_not_recorded() {
        let p = path();
        let ping = p.next_ping();
        std::thread::sleep(Duration::from_millis(5));
        p.on_pong_record(
            ping.seq,
            ping.sent_at_ms,
            true,
            Some(Duration::from_millis(1)),
            true,
        );
        assert!(!p.rtt_known(), "sample above unknown cap must be ignored");
    }

    #[test]
    fn in_pending_pong_still_records_instant() {
        let p = path();
        p.record_rtt(Duration::from_millis(7));
        let ping = p.next_ping();
        std::thread::sleep(Duration::from_millis(12));
        p.on_pong_record(
            ping.seq,
            ping.sent_at_ms,
            true,
            Some(Duration::from_millis(50)),
            true,
        );
        let after = p.rtt_ewma_us.load(Ordering::Relaxed);
        assert!(
            after > 7_000,
            "in-pending Instant Pong must move EWMA, got {after}"
        );
        assert!(
            after < 45_000,
            "12 ms Instant must not look like a 200 ms RTO, got {after}"
        );
    }

    #[test]
    fn hold_stream_data_unknown_only() {
        let p = path();
        let data = Frame::StreamData(StreamData {
            stream_id: 1,
            offset: 0,
            data: vec![0; 64],
        });
        let ping = Frame::Ping(Ping {
            seq: 1,
            sent_at_ms: 0,
        });
        assert!(
            hold_stream_data(&p, &data),
            "unknown dest must park STREAM_DATA until first pong"
        );
        assert!(!hold_stream_data(&p, &ping));
        p.record_rtt(Duration::from_millis(7));
        p.set_write_stalled(true);
        assert!(
            !hold_stream_data(&p, &data),
            "known+stalled DATA is not parked; send_on_path forces bulk"
        );
        assert!(!hold_stream_data(&p, &ping));
    }

    #[test]
    fn queued_saturates_at_zero() {
        let p = path();
        p.note_enqueue(true);
        p.note_enqueue(true);
        assert_eq!(p.queued_urgent(), 2);
        p.note_dequeue(true);
        p.note_dequeue(true);
        p.note_dequeue(true);
        assert_eq!(p.queued_urgent(), 0);
    }

    #[test]
    fn spike_does_not_rewrite_stable_baseline() {
        let p = path();
        for _ in 0..30 {
            p.record_rtt(Duration::from_millis(10));
        }
        assert!(p.stable_rtt() <= Duration::from_millis(12));
        for _ in 0..20 {
            p.record_rtt(Duration::from_millis(400));
        }
        // stable must stay near the 10ms class, not jump to 400ms
        assert!(
            p.stable_rtt() < Duration::from_millis(80),
            "stable={:?}",
            p.stable_rtt()
        );
        // fast EWMA should have moved toward the spike
        assert!(p.rtt() > Duration::from_millis(100));
    }

    #[test]
    fn stable_recovers_quickly_after_spike() {
        let p = path();
        for _ in 0..20 {
            p.record_rtt(Duration::from_millis(10));
        }
        for _ in 0..20 {
            p.record_rtt(Duration::from_millis(400));
        }
        for _ in 0..20 {
            p.record_rtt(Duration::from_millis(10));
        }
        assert!(
            p.stable_rtt() < Duration::from_millis(25),
            "stable={:?}",
            p.stable_rtt()
        );
        assert!(p.rtt() < Duration::from_millis(25));
    }

    #[test]
    fn confirmed_shift_raises_stable() {
        let p = path();
        p.stable_up_hold_us.store(0, Ordering::Relaxed);
        for _ in 0..20 {
            p.record_rtt(Duration::from_millis(10));
        }
        for _ in 0..40 {
            p.record_rtt(Duration::from_millis(80));
        }
        assert!(
            p.stable_rtt() > Duration::from_millis(40),
            "stable={:?}",
            p.stable_rtt()
        );
    }

    #[test]
    fn confirmed_2_5x_raise_is_seven_eighths_not_assign() {
        let p = path();
        p.stable_up_hold_us.store(0, Ordering::Relaxed);
        p.rtt_ewma_us.store(180_000, Ordering::Relaxed);
        p.rtt_stable_us.store(180_000, Ordering::Relaxed);
        p.rtt_class_us.store(180_000, Ordering::Relaxed);
        for _ in 0..12 {
            p.record_rtt(Duration::from_millis(450));
        }
        let class = p.class_rtt().as_millis() as u64;
        let stable = p.stable_rtt().as_millis() as u64;
        assert!(
            class > 180 && class < 360,
            "7/8 raise must not jump to 450, class={class}"
        );
        assert!(
            stable > 180 && stable < 360,
            "timeout-stable 7/8 must not jump to 450, stable={stable}"
        );
        assert!(!crate::health::should_failback(
            &crate::cfg::SessionConfig::default(),
            p.class_rtt(),
            Duration::from_millis(255)
        ));
    }

    #[test]
    fn class_updates_on_drop_eager_stable_path() {
        let p = path();
        for _ in 0..8 {
            p.record_rtt(Duration::from_millis(180));
        }
        p.record_rtt(Duration::from_millis(90));
        assert!(
            p.rtt_class_us.load(Ordering::Relaxed) >= 100_000,
            "class must be set even when sample < stable, class={:?}",
            p.class_rtt()
        );
        assert!(
            p.stable_rtt() < p.class_rtt(),
            "timeout-stable drop-eagers; class holds"
        );
    }

    #[test]
    fn one_low_sample_does_not_collapse_class() {
        let p = path();
        p.stable_up_hold_us.store(1_000_000, Ordering::Relaxed);
        p.rtt_ewma_us.store(180_000, Ordering::Relaxed);
        p.rtt_stable_us.store(180_000, Ordering::Relaxed);
        p.rtt_class_us.store(180_000, Ordering::Relaxed);
        p.record_rtt(Duration::from_millis(90));
        assert!(
            p.stable_rtt() < p.class_rtt(),
            "stable={:?} class={:?}",
            p.stable_rtt(),
            p.class_rtt()
        );
        assert_eq!(p.class_rtt(), Duration::from_micros(180_000));
        for _ in 0..20 {
            p.record_rtt(Duration::from_millis(180));
        }
        let class_ms = p.class_rtt().as_millis() as u64;
        assert!(
            (170..=190).contains(&class_ms),
            "class stayed near 180 after one 90ms sample, class={class_ms}"
        );
    }

    #[test]
    fn class_hold_not_elapsed_does_not_store() {
        let p = path();
        p.stable_up_hold_us.store(1_000_000, Ordering::Relaxed);
        p.rtt_class_us.store(180_000, Ordering::Relaxed);
        p.rtt_ewma_us.store(90_000, Ordering::Relaxed);
        p.record_rtt(Duration::from_millis(90));
        assert_eq!(
            p.rtt_class_us.load(Ordering::Relaxed),
            180_000,
            "hold not elapsed → no 7/8 store"
        );
    }

    #[test]
    fn jitter_low_tail_does_not_drop_class() {
        let p = path();
        p.stable_up_hold_us.store(0, Ordering::Relaxed);
        p.rtt_class_us.store(180_000, Ordering::Relaxed);
        p.rtt_ewma_us.store(180_000, Ordering::Relaxed);
        p.rtt_stable_us.store(180_000, Ordering::Relaxed);
        // 40ms jitter on a 180ms path: 8ms abs would ratchet toward 140.
        p.record_rtt(Duration::from_millis(140));
        assert_eq!(
            p.rtt_class_us.load(Ordering::Relaxed),
            180_000,
            "0.45×fast drop must ignore jitter low-tail"
        );
    }

    #[test]
    fn lucky_low_first_sample_does_not_freeze_class() {
        let p = path();
        p.record_rtt(Duration::from_millis(90));
        for _ in 0..7 {
            p.record_rtt(Duration::from_millis(180));
        }
        let class_ms = p.class_rtt().as_millis() as u64;
        assert!(
            class_ms >= 140,
            "class init after 8 samples must track fast EWMA, class={class_ms}"
        );
        assert!(
            !crate::health::should_failback(
                &crate::cfg::SessionConfig::default(),
                Duration::from_millis(180),
                p.class_rtt()
            ),
            "init class must stay same-class vs 180ms siblings"
        );
    }

    #[test]
    fn class_hold_zero_drop_is_seven_eighths_vs_fast() {
        let p = path();
        p.stable_up_hold_us.store(0, Ordering::Relaxed);
        // 280 vs 180 is a same-class failback (Δ=100 ≥ 0.45×180=81).
        // 244 vs 180 (Δ=64) must not drop — that was jitter-shaped chatter.
        p.rtt_class_us.store(280_000, Ordering::Relaxed);
        p.rtt_ewma_us.store(180_000, Ordering::Relaxed);
        p.rtt_stable_us.store(180_000, Ordering::Relaxed);
        p.record_rtt(Duration::from_millis(180));
        assert_eq!(
            p.rtt_class_us.load(Ordering::Relaxed),
            (280_000 * 7 + 180_000) / 8,
            "7/8 vs fast=180, not sample"
        );
    }

    #[test]
    fn class_same_class_gap_does_not_drop() {
        let p = path();
        p.stable_up_hold_us.store(0, Ordering::Relaxed);
        p.rtt_class_us.store(220_000, Ordering::Relaxed);
        p.rtt_ewma_us.store(180_000, Ordering::Relaxed);
        p.rtt_stable_us.store(180_000, Ordering::Relaxed);
        p.record_rtt(Duration::from_millis(180));
        assert_eq!(
            p.rtt_class_us.load(Ordering::Relaxed),
            220_000,
            "220 vs 180 is below 0.25×class; class holds"
        );
    }

    #[test]
    fn single_non_drop_pauses_low_timer() {
        let p = path();
        p.stable_up_hold_us.store(80_000, Ordering::Relaxed);
        p.rtt_class_us.store(280_000, Ordering::Relaxed);
        p.rtt_ewma_us.store(180_000, Ordering::Relaxed);
        p.record_rtt(Duration::from_millis(180));
        std::thread::sleep(Duration::from_millis(40));
        p.record_rtt(Duration::from_millis(180));
        // Fast near class: 280−250=30 < 0.25×280=70, not a drop.
        p.record_rtt(Duration::from_millis(400));
        let paused = p.class_low_accum();
        assert!(
            paused >= Duration::from_millis(25),
            "non-drop must freeze accum, got {paused:?}"
        );
        assert!(p.class_low_since.lock().unwrap().is_none());
        let class_before = p.rtt_class_us.load(Ordering::Relaxed);
        std::thread::sleep(Duration::from_millis(80));
        p.record_rtt(Duration::from_millis(400));
        assert_eq!(
            p.rtt_class_us.load(Ordering::Relaxed),
            class_before,
            "paused timer must not count wall clock during non-drop"
        );
        p.rtt_ewma_us.store(180_000, Ordering::Relaxed);
        p.record_rtt(Duration::from_millis(180));
        std::thread::sleep(Duration::from_millis(50));
        p.record_rtt(Duration::from_millis(180));
        let class = p.rtt_class_us.load(Ordering::Relaxed);
        assert!(
            class < 280_000,
            "paused + resumed drop must 7/8, class={class}"
        );
        assert_eq!(p.class_low_accum(), Duration::ZERO);
    }

    #[test]
    fn raise_store_clears_high_timer() {
        let p = path();
        p.stable_up_hold_us.store(50_000, Ordering::Relaxed);
        p.rtt_class_us.store(8_000, Ordering::Relaxed);
        p.rtt_ewma_us.store(200_000, Ordering::Relaxed);
        p.rtt_stable_us.store(8_000, Ordering::Relaxed);
        p.record_rtt(Duration::from_millis(200));
        assert_eq!(
            p.rtt_class_us.load(Ordering::Relaxed),
            8_000,
            "hold not elapsed → no 7/8"
        );
        std::thread::sleep(Duration::from_millis(55));
        p.record_rtt(Duration::from_millis(200));
        let after_first = p.rtt_class_us.load(Ordering::Relaxed);
        assert_eq!(
            after_first,
            (8_000 * 7 + 200_000) / 8,
            "first hold stores 7/8 vs fast"
        );
        p.record_rtt(Duration::from_millis(200));
        assert_eq!(
            p.rtt_class_us.load(Ordering::Relaxed),
            after_first,
            "immediate raise must not store"
        );
        std::thread::sleep(Duration::from_millis(55));
        p.record_rtt(Duration::from_millis(200));
        let after_second = p.rtt_class_us.load(Ordering::Relaxed);
        assert!(
            after_second != after_first,
            "second hold must 7/8 again, class={after_second}"
        );
    }

    #[test]
    fn class_init_window_notes_known_since() {
        let p = path();
        for _ in 0..7 {
            p.record_rtt(Duration::from_millis(10));
            assert!(
                p.class_known_since_for_test().is_none(),
                "init window must not timestamp before freeze"
            );
            assert!(!p.class_known());
        }
        p.record_rtt(Duration::from_millis(10));
        assert!(p.class_known());
        assert!(p.class_known_since_for_test().is_some());
        assert!(
            p.class_unwind_permit_for_test(),
            "init freeze is a class store that may need unwind"
        );
    }

    #[test]
    fn drop_store_clears_accum() {
        let p = path();
        p.stable_up_hold_us.store(0, Ordering::Relaxed);
        p.rtt_class_us.store(280_000, Ordering::Relaxed);
        p.rtt_ewma_us.store(180_000, Ordering::Relaxed);
        p.record_rtt(Duration::from_millis(180));
        assert_eq!(p.class_low_accum(), Duration::ZERO);
        assert!(p.class_low_since.lock().unwrap().is_none());
    }

    #[test]
    fn degraded_path_still_probes() {
        let p = path();
        p.mark_degraded();
        let every = Duration::from_millis(10);
        assert!(p.should_send_ping(every, every));
    }

    #[test]
    fn down_path_does_not_probe() {
        let p = path();
        p.state.store(STATE_DOWN, Ordering::Relaxed);
        let every = Duration::from_millis(10);
        assert!(!p.should_send_ping(every, every));
    }

    #[test]
    fn pending_ping_blocks_probe() {
        let p = path();
        p.next_ping();
        let every = Duration::from_millis(10);
        assert!(!p.should_send_ping(every, every));
        p.mark_degraded();
        assert!(!p.should_send_ping(every, every));
    }

    #[test]
    fn expire_stale_allows_next_probe() {
        let p = path();
        p.next_ping();
        let every = Duration::from_millis(10);
        assert!(!p.should_send_ping(every, every));
        let miss = p.expire_stale_pings(Duration::ZERO);
        assert_eq!(miss, 1);
        assert_eq!(p.pending_ping_count(), 0);
        assert!(p.should_send_ping(every, every));
    }

    #[test]
    fn record_false_discards_instant() {
        let p = path();
        let ping = p.next_ping();
        std::thread::sleep(Duration::from_millis(5));
        p.on_pong_record(ping.seq, ping.sent_at_ms, false, None, true);
        assert!(!p.rtt_known());
        assert_eq!(p.pending_ping_count(), 0);
    }

    #[test]
    fn known_instant_above_loss_timeout_not_recorded() {
        let p = path();
        p.record_rtt(Duration::from_millis(7));
        let before = p.rtt_ewma_us.load(Ordering::Relaxed);
        let ping = p.next_ping();
        std::thread::sleep(Duration::from_millis(25));
        p.on_pong_record(
            ping.seq,
            ping.sent_at_ms,
            true,
            Some(Duration::from_millis(20)),
            true,
        );
        assert_eq!(
            p.rtt_ewma_us.load(Ordering::Relaxed),
            before,
            "known Instant above loss_timeout cap must not move EWMA"
        );
    }

    #[test]
    fn next_ping_overflow_drops_oldest_late() {
        let p = path();
        let first = p.next_ping();
        p.expire_stale_pings(Duration::ZERO);
        for _ in 1..Tuning::STANDARD.pending_ping_max {
            p.next_ping();
            p.expire_stale_pings(Duration::ZERO);
        }
        assert_eq!(p.pending_ping_count(), 0);
        p.next_ping();
        p.on_pong_record(
            first.seq,
            first.sent_at_ms,
            true,
            Some(Duration::from_millis(300)),
            true,
        );
        assert!(
            !p.rtt_known(),
            "overflow must drop oldest late Instant, never clear pending"
        );
    }

    #[test]
    fn idle_gate_does_not_probe() {
        let p = path();
        let every = Duration::from_millis(10);
        assert!(!p.should_send_ping(every - Duration::from_nanos(1), every));
    }

    #[test]
    fn up_path_still_probes() {
        let p = path();
        let every = Duration::from_millis(10);
        assert!(p.should_send_ping(every, every));
    }

    fn raise_to_unwind_class(p: &PathState) {
        p.stable_up_hold_us.store(50_000, Ordering::Relaxed);
        p.rtt_class_us.store(8_000, Ordering::Relaxed);
        p.rtt_ewma_us.store(50_000, Ordering::Relaxed);
        p.rtt_stable_us.store(8_000, Ordering::Relaxed);
        p.record_rtt(Duration::from_millis(50));
        assert_eq!(p.rtt_class_us.load(Ordering::Relaxed), 8_000);
        std::thread::sleep(Duration::from_millis(55));
        p.record_rtt(Duration::from_millis(50));
        assert_eq!(p.rtt_class_us.load(Ordering::Relaxed), 13_250);
        assert!(p.class_unwind_permit_for_test());
        assert!(!Tuning::STANDARD.class_should_drop(13_250, 8_000));
    }

    #[test]
    fn raise_permit_allows_drop_below_abs_floor() {
        let p = path();
        raise_to_unwind_class(&p);
        p.rtt_ewma_us.store(8_000, Ordering::Relaxed);
        p.rtt_stable_us.store(8_000, Ordering::Relaxed);
        p.record_rtt(Duration::from_millis(8));
        assert_eq!(
            p.rtt_class_us.load(Ordering::Relaxed),
            13_250,
            "G4a hold: first recovered sample must not store"
        );
        std::thread::sleep(Duration::from_millis(55));
        p.record_rtt(Duration::from_millis(8));
        assert_eq!(p.rtt_class_us.load(Ordering::Relaxed), 12_593);
        assert!(
            p.class_unwind_permit_for_test(),
            "12593 > 8000 is not catch-up"
        );
        p.record_rtt(Duration::from_millis(8));
        assert_eq!(
            p.rtt_class_us.load(Ordering::Relaxed),
            12_593,
            "drop store clears low; next hold starts here"
        );
        std::thread::sleep(Duration::from_millis(55));
        p.record_rtt(Duration::from_millis(8));
        assert_eq!(p.rtt_class_us.load(Ordering::Relaxed), 12_018);
        assert!(p.class_unwind_permit_for_test());
    }

    #[test]
    fn permit_survives_ewma_descent_dead_zone() {
        let p = path();
        raise_to_unwind_class(&p);
        // Walk ewma 50000 → 41600 → 34880 → 29504 (still raise vs 13250)
        // then ~25 ms into (class, 2×class]. No sleep: raise hold must not fire.
        for _ in 0..4 {
            p.record_rtt(Duration::from_millis(8));
        }
        let class = p.rtt_class_us.load(Ordering::Relaxed);
        let fast = p.rtt_ewma_us.load(Ordering::Relaxed);
        assert_eq!(class, 13_250, "dead zone must not store a drop");
        assert!(
            p.class_unwind_permit_for_test(),
            "permit must survive (class, 2×class], fast={fast}"
        );
        assert!(
            fast > class && fast <= class.saturating_mul(2),
            "expected dead zone, fast={fast} class={class}"
        );
        assert!(!Tuning::STANDARD.class_should_drop(class, fast));
    }

    #[test]
    fn permit_not_spent_on_one_us_dip() {
        let p = path();
        raise_to_unwind_class(&p);
        p.rtt_ewma_us.store(13_200, Ordering::Relaxed);
        p.rtt_stable_us.store(13_200, Ordering::Relaxed);
        p.record_rtt(Duration::from_micros(13_200));
        assert_eq!(
            p.rtt_class_us.load(Ordering::Relaxed),
            13_250,
            "G4a hold: dip sample must not store"
        );
        std::thread::sleep(Duration::from_millis(55));
        p.record_rtt(Duration::from_micros(13_200));
        assert_eq!(p.rtt_class_us.load(Ordering::Relaxed), 13_243);
        assert!(
            p.class_unwind_permit_for_test(),
            "13243 > 13200 is not catch-up"
        );
        p.rtt_ewma_us.store(8_000, Ordering::Relaxed);
        p.rtt_stable_us.store(8_000, Ordering::Relaxed);
        p.record_rtt(Duration::from_millis(8));
        assert_eq!(p.rtt_class_us.load(Ordering::Relaxed), 13_243);
        std::thread::sleep(Duration::from_millis(55));
        p.record_rtt(Duration::from_millis(8));
        let class = p.rtt_class_us.load(Ordering::Relaxed);
        assert!(
            class < 13_243,
            "recovered-8 ms hold must drop, class={class}"
        );
        assert!(p.class_unwind_permit_for_test());
    }

    #[test]
    fn permit_clears_when_seven_eighths_meets_fast() {
        let p = path();
        raise_to_unwind_class(&p);
        p.stable_up_hold_us.store(0, Ordering::Relaxed);
        p.rtt_class_us.store(8_001, Ordering::Relaxed);
        p.rtt_ewma_us.store(8_000, Ordering::Relaxed);
        p.rtt_stable_us.store(8_000, Ordering::Relaxed);
        p.record_rtt(Duration::from_micros(8_000));
        assert_eq!(p.rtt_class_us.load(Ordering::Relaxed), 8_000);
        assert!(
            !p.class_unwind_permit_for_test(),
            "new_us == fast must clear permit"
        );
        p.stable_up_hold_us.store(0, Ordering::Relaxed);
        p.rtt_class_us.store(180_000, Ordering::Relaxed);
        p.rtt_ewma_us.store(140_000, Ordering::Relaxed);
        p.rtt_stable_us.store(180_000, Ordering::Relaxed);
        p.record_rtt(Duration::from_millis(140));
        assert_eq!(
            p.rtt_class_us.load(Ordering::Relaxed),
            180_000,
            "after catch-up, 140 vs 180 must not drop"
        );
        p.rtt_class_us.store(220_000, Ordering::Relaxed);
        p.rtt_ewma_us.store(180_000, Ordering::Relaxed);
        p.record_rtt(Duration::from_millis(180));
        assert_eq!(
            p.rtt_class_us.load(Ordering::Relaxed),
            220_000,
            "after catch-up, 220 vs 180 must not drop"
        );
    }

    #[test]
    fn init_permit_walks_below_class_drop_floor() {
        let p = path();
        p.stable_up_hold_us.store(0, Ordering::Relaxed);
        for _ in 0..8 {
            p.record_rtt(Duration::from_millis(14));
        }
        assert_eq!(p.rtt_class_us.load(Ordering::Relaxed), 14_000);
        assert!(p.class_unwind_permit_for_test());
        assert!(
            !Tuning::STANDARD.class_should_drop(14_000, 7_000),
            "14 vs 7 is under the 8ms floor; 15 vs 7 would already drop"
        );
        p.rtt_ewma_us.store(7_000, Ordering::Relaxed);
        p.rtt_stable_us.store(7_000, Ordering::Relaxed);
        p.record_rtt(Duration::from_millis(7));
        assert_eq!(p.rtt_class_us.load(Ordering::Relaxed), 13_125);
        assert!(p.class_unwind_permit_for_test());
        p.record_rtt(Duration::from_millis(7));
        assert_eq!(p.rtt_class_us.load(Ordering::Relaxed), 12_359);
        assert!(
            p.class_unwind_permit_for_test(),
            "12359 > 7000 is not catch-up"
        );
    }

    #[test]
    fn init_permit_clears_when_seven_eighths_meets_fast() {
        let p = path();
        p.stable_up_hold_us.store(0, Ordering::Relaxed);
        for _ in 0..8 {
            p.record_rtt(Duration::from_millis(14));
        }
        assert!(p.class_unwind_permit_for_test());
        p.rtt_ewma_us.store(7_000, Ordering::Relaxed);
        p.rtt_stable_us.store(7_000, Ordering::Relaxed);
        assert!(!Tuning::STANDARD.class_should_drop(14_000, 7_000));
        let mut n = 0;
        while p.rtt_class_us.load(Ordering::Relaxed) > 7_000 {
            p.record_rtt(Duration::from_millis(7));
            n += 1;
            assert!(n < 200, "catch-up must finish well before 200 holds");
        }
        assert_eq!(p.rtt_class_us.load(Ordering::Relaxed), 7_000);
        assert!(
            !p.class_unwind_permit_for_test(),
            "new_us == fast must clear permit after init walk"
        );
        p.rtt_class_us.store(180_000, Ordering::Relaxed);
        p.rtt_ewma_us.store(140_000, Ordering::Relaxed);
        p.rtt_stable_us.store(180_000, Ordering::Relaxed);
        p.record_rtt(Duration::from_millis(140));
        assert_eq!(
            p.rtt_class_us.load(Ordering::Relaxed),
            180_000,
            "after init catch-up, 140 vs 180 must not drop"
        );
        p.rtt_class_us.store(220_000, Ordering::Relaxed);
        p.rtt_ewma_us.store(180_000, Ordering::Relaxed);
        p.record_rtt(Duration::from_millis(180));
        assert_eq!(
            p.rtt_class_us.load(Ordering::Relaxed),
            220_000,
            "after init catch-up, 220 vs 180 must not drop"
        );
    }

    #[test]
    fn init_freeze_equal_fast_does_not_drop() {
        let p = path();
        p.stable_up_hold_us.store(0, Ordering::Relaxed);
        for _ in 0..8 {
            p.record_rtt(Duration::from_millis(10));
        }
        assert_eq!(p.rtt_class_us.load(Ordering::Relaxed), 10_000);
        assert!(p.class_unwind_permit_for_test());
        p.record_rtt(Duration::from_millis(10));
        assert_eq!(
            p.rtt_class_us.load(Ordering::Relaxed),
            10_000,
            "identical extra sample must not 7/8 at freeze"
        );
        assert!(p.class_unwind_permit_for_test());
    }

    #[test]
    fn init_then_jitter_low_tail_does_drop() {
        let p = path();
        p.stable_up_hold_us.store(0, Ordering::Relaxed);
        for _ in 0..8 {
            p.record_rtt(Duration::from_millis(180));
        }
        assert_eq!(p.rtt_class_us.load(Ordering::Relaxed), 180_000);
        assert!(p.class_unwind_permit_for_test());
        assert!(!Tuning::STANDARD.class_should_drop(180_000, 140_000));
        p.rtt_ewma_us.store(140_000, Ordering::Relaxed);
        p.rtt_stable_us.store(140_000, Ordering::Relaxed);
        p.record_rtt(Duration::from_millis(140));
        assert_eq!(
            p.rtt_class_us.load(Ordering::Relaxed),
            175_000,
            "production init permit must chase 180→140; poke-class tests do not"
        );
        assert!(p.class_unwind_permit_for_test());
    }
}
