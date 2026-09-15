//! One overlay session: many TCP+TLS paths, many multiplexed streams.
//!
//! * [`streams`] — open/accept, windowed send, recv reorder, ACKs
//! * [`steer`] — health tick, RTT-scaled retry, failback, same-link rebalance
//!
//! Path pick lives in [`crate::scheduler`]. Timeouts in [`crate::health`].

mod steer;
mod streams;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, Notify};
use tracing::{debug, info, warn};

use nya_proto::{
    Frame, Pong, ResetReason, StreamAck, StreamClose, StreamData, StreamOpen, StreamReset, Target,
};

use crate::cfg::SessionConfig;
use crate::health;
use crate::metrics::{flatten_paths, Counters, ProcessCounters, ProcessSnapshot, Snapshot};
use crate::path::{spawn_path_io, PathState, STATE_DOWN};
use crate::scheduler::{pick_path_pref, pick_retry_path, PickPref};
use crate::stream::{StreamState, Unacked};

struct OpenUnacked {
    path_id: u32,
    sent_at: Instant,
    target: Target,
    tried: Vec<u32>,
}

struct CloseUnacked {
    path_id: u32,
    sent_at: Instant,
    started_at: Instant,
    tried: Vec<u32>,
    second_closer: bool,
    final_offset: Option<u64>,
}

type CloseRetrySnap = (u32, u32, bool, Vec<u32>, Instant, Option<u64>);
type ResetRetrySnap = (u32, u32, Vec<u32>, Instant, ResetReason);

struct ResetUnacked {
    path_id: u32,
    sent_at: Instant,
    started_at: Instant,
    tried: Vec<u32>,
    reason: ResetReason,
}

struct PendingEarly {
    at: Instant,
    path_id: u32,
    data: StreamData,
}

pub use crate::stream::TunnelStream;

/// Accepted remote stream on the server. `io` is the application side of
/// the overlay; call [`IncomingStream::reset`] if the outbound dial fails.
pub struct IncomingStream {
    pub stream_id: u32,
    pub target: nya_proto::Target,
    pub io: TunnelStream,
    session: Session,
}

impl IncomingStream {
    /// Abort the overlay stream (e.g. outbound dial failed).
    pub fn reset(self, reason: ResetReason) {
        self.session.reset_stream(self.stream_id, reason);
    }

    pub fn process(&self) -> Arc<ProcessCounters> {
        self.session.process()
    }

    pub fn session_fp(&self) -> Option<String> {
        self.session.session_fp()
    }

    /// The owning session (for `stream_stats` after `io` has been moved).
    pub fn session_handle(&self) -> Session {
        self.session.clone()
    }
}

pub(crate) struct Inner {
    cfg: SessionConfig,
    is_client: bool,
    paths: Mutex<HashMap<u32, Arc<PathState>>>,
    streams: Mutex<HashMap<u32, Arc<StreamState>>>,
    next_path_id: AtomicU32,
    next_stream_id: AtomicU32,
    incoming: Mutex<Option<mpsc::Sender<IncomingStream>>>,
    ready: Notify,
    /// P2: some path's inflight dropped or a path was added — bulk senders
    /// parked on budget re-check room. Session-level: a waiter on a full
    /// sticky must also see a sibling gain room (fan-out).
    budget_wait: Notify,
    dead: AtomicBool,
    dead_notify: Notify,
    all_down_since: Mutex<Option<Instant>>,
    correlated_since: Mutex<Option<Instant>>,
    /// P1.8b: open `quiet_set` episode (quiet ≥ alive − 1, all-N included).
    quiet_episode: Mutex<Option<steer::QuietEpisode>>,
    metrics: Counters,
    process: Arc<ProcessCounters>,
    last_rtt_us: Mutex<HashMap<String, u64>>,
    /// Last `up_since` age at `path_failed`, keyed by path name.
    last_lived: Mutex<HashMap<String, Duration>>,
    opens: Mutex<HashMap<u32, OpenUnacked>>,
    closes: Mutex<HashMap<u32, CloseUnacked>>,
    resets: Mutex<HashMap<u32, ResetUnacked>>,
    pending_early: Mutex<HashMap<u32, Vec<PendingEarly>>>,
    session_fp: Mutex<String>,
    /// Table-owned server sessions only. e2e duplex pairs stay alive so a
    /// long blackhole can Join back; production leftover cannot.
    reap_on_all_down: AtomicBool,
}

#[derive(Clone)]
pub struct Session {
    inner: Arc<Inner>,
}

impl Session {
    pub fn new_client(cfg: SessionConfig) -> Self {
        Self::new(cfg, true, None, None)
    }

    pub fn new_server(cfg: SessionConfig) -> (Self, mpsc::Receiver<IncomingStream>) {
        let (tx, rx) = mpsc::channel(cfg.tuning.chan);
        (Self::new(cfg, false, Some(tx), None), rx)
    }

    fn new(
        cfg: SessionConfig,
        is_client: bool,
        incoming: Option<mpsc::Sender<IncomingStream>>,
        process: Option<Arc<ProcessCounters>>,
    ) -> Self {
        let process = process.unwrap_or_else(|| Arc::new(ProcessCounters::default()));
        process.sessions_created.fetch_add(1, Ordering::Relaxed);
        process.sessions_live.fetch_add(1, Ordering::Relaxed);
        let inner = Arc::new(Inner {
            cfg,
            is_client,
            paths: Mutex::new(HashMap::new()),
            streams: Mutex::new(HashMap::new()),
            next_path_id: AtomicU32::new(1),
            next_stream_id: AtomicU32::new(1),
            incoming: Mutex::new(incoming),
            ready: Notify::new(),
            budget_wait: Notify::new(),
            dead: AtomicBool::new(false),
            dead_notify: Notify::new(),
            all_down_since: Mutex::new(None),
            correlated_since: Mutex::new(None),
            quiet_episode: Mutex::new(None),
            metrics: Counters::default(),
            process,
            last_rtt_us: Mutex::new(HashMap::new()),
            last_lived: Mutex::new(HashMap::new()),
            opens: Mutex::new(HashMap::new()),
            closes: Mutex::new(HashMap::new()),
            resets: Mutex::new(HashMap::new()),
            pending_early: Mutex::new(HashMap::new()),
            session_fp: Mutex::new(String::new()),
            reap_on_all_down: AtomicBool::new(false),
        });
        let session = Self { inner };
        session.spawn_maintenance();
        session
    }

    pub fn process(&self) -> Arc<ProcessCounters> {
        self.inner.process.clone()
    }

    /// First 4 bytes of the overlay session id, hex. Replaces on Create-retry.
    pub fn set_session_id(&self, id: &[u8; 16]) {
        *self.inner.session_fp.lock().unwrap() = crate::hop::session_fp_hex(id);
    }

    pub fn session_fp(&self) -> Option<String> {
        let s = self.inner.session_fp.lock().unwrap().clone();
        if s.is_empty() {
            None
        } else {
            Some(s)
        }
    }

    pub fn config(&self) -> &SessionConfig {
        &self.inner.cfg
    }

    pub fn is_dead(&self) -> bool {
        self.inner.dead.load(Ordering::Relaxed)
    }

    pub async fn wait_dead(&self) {
        loop {
            if self.is_dead() {
                return;
            }
            self.inner.dead_notify.notified().await;
        }
    }

    pub fn shutdown(&self) {
        self.mark_dead(true);
    }

    fn mark_dead(&self, send_frame: bool) {
        if self
            .inner
            .dead
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        self.inner
            .process
            .sessions_dead
            .fetch_add(1, Ordering::Relaxed);
        let _ = self.inner.process.sessions_live.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |v| Some(v.saturating_sub(1)),
        );
        info!(
            reason = if send_frame { "shutdown" } else { "drop" },
            "session dead"
        );
        self.inner.closes.lock().unwrap().clear();
        if send_frame {
            if let Some(p) = self.pick_pref(PickPref::Any) {
                self.send_on_path(p, Frame::SessionClose);
            }
        }
        let ids: Vec<u32> = self.inner.streams.lock().unwrap().keys().copied().collect();
        for id in ids {
            self.finish_stream(id, Some(ResetReason::SessionDead), send_frame);
        }
        self.inner.resets.lock().unwrap().clear();
        // After SessionClose / Reset hit the writer queues, wake path IO
        // so Idle close can flush urgent then close_notify.
        self.inner.dead_notify.notify_waiters();
    }

    pub fn has_alive_path(&self) -> bool {
        self.inner
            .paths
            .lock()
            .unwrap()
            .values()
            .any(|p| p.is_alive())
    }

    pub fn alive_path_count(&self) -> usize {
        self.inner
            .paths
            .lock()
            .unwrap()
            .values()
            .filter(|p| p.is_alive())
            .count()
    }

    pub async fn wait_ready(&self, timeout: Duration) -> Result<(), SessionError> {
        self.wait_alive(1, timeout).await
    }

    /// Wait until at least `n` overlay TCPs are alive (RTT may still be unknown).
    pub async fn wait_alive(&self, n: usize, timeout: Duration) -> Result<(), SessionError> {
        let start = Instant::now();
        loop {
            if self.alive_path_count() >= n {
                return Ok(());
            }
            if self.is_dead() {
                return Err(SessionError::Dead);
            }
            if start.elapsed() >= timeout {
                return Err(SessionError::NoPath);
            }
            tokio::select! {
                _ = self.inner.ready.notified() => {}
                _ = tokio::time::sleep(self.inner.cfg.tuning.ready_poll) => {}
            }
        }
    }

    /// Wait until at least `n` paths are alive and have a measured RTT.
    pub async fn wait_paths(&self, n: usize, timeout: Duration) -> Result<(), SessionError> {
        let start = Instant::now();
        loop {
            let known = self
                .path_list()
                .iter()
                .filter(|p| p.is_alive() && p.rtt_known())
                .count();
            if known >= n {
                return Ok(());
            }
            if self.is_dead() {
                return Err(SessionError::Dead);
            }
            if start.elapsed() >= timeout {
                return Err(SessionError::NoPath);
            }
            tokio::select! {
                _ = self.inner.ready.notified() => {}
                _ = tokio::time::sleep(self.inner.cfg.tuning.ready_poll) => {}
            }
        }
    }

    /// Drive a path until it dies. Used by client reconnect loops and server accept tasks.
    pub async fn add_path<T>(&self, name: String, io: T)
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let rx = self.start_path(name, io);
        let _ = rx.await;
    }

    pub fn start_path<T>(&self, name: String, io: T) -> tokio::sync::oneshot::Receiver<()>
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        self.start_path_fd(name, io, None)
    }

    /// [`add_path`](Self::add_path) with the underlying socket's dup for
    /// `TCP_INFO` gauges. Binaries pass `PathFd::dup_from(&tcp)`.
    pub async fn add_path_fd<T>(&self, name: String, io: T, fd: Option<crate::net::PathFd>)
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let rx = self.start_path_fd(name, io, fd);
        let _ = rx.await;
    }

    pub fn start_path_fd<T>(
        &self,
        name: String,
        io: T,
        fd: Option<crate::net::PathFd>,
    ) -> tokio::sync::oneshot::Receiver<()>
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        if self.is_dead() {
            let _ = done_tx.send(());
            return done_rx;
        }
        let n = self.inner.paths.lock().unwrap().len();
        if n >= self.inner.cfg.max_paths {
            warn!(path = %name, "max paths reached, ignoring");
            let _ = done_tx.send(());
            return done_rx;
        }
        let id = self.inner.next_path_id.fetch_add(1, Ordering::Relaxed);
        let chan = self.inner.cfg.tuning.chan;
        let (tx, rx) = mpsc::channel(chan);
        let (utx, urx) = mpsc::channel(chan);
        let path = PathState::with_writers(id, name.clone(), tx, utx);
        *path.tcp_fd.lock().unwrap() = fd;
        path.budget_floor
            .store(self.inner.cfg.tuning.inflight_bias, Ordering::Relaxed);
        path.budget_ceil.store(
            (self.inner.cfg.tuning.chan as u64)
                .saturating_mul(nya_proto::MAX_STREAM_PAYLOAD as u64),
            Ordering::Relaxed,
        );
        path.stable_up_hold_us.store(
            self.inner.cfg.tuning.stable_up_hold.as_micros() as u64,
            Ordering::Relaxed,
        );
        self.inner.paths.lock().unwrap().insert(id, path.clone());
        *self.inner.all_down_since.lock().unwrap() = None;
        self.inner.ready.notify_waiters();
        self.inner.budget_wait.notify_waiters();
        info!(path = %name, path_id = id, "path added");
        self.inner
            .metrics
            .path_added
            .fetch_add(1, Ordering::Relaxed);
        spawn_path_io(self.clone(), path, io, rx, urx, done_tx);
        done_rx
    }

    pub fn path_failed(&self, path_id: u32) {
        let Some(path) = self.get_path(path_id) else {
            return;
        };
        if path.rtt_known() {
            self.inner
                .last_rtt_us
                .lock()
                .unwrap()
                .insert(path.name.clone(), path.rtt_us());
        }
        let prev = path.state.swap(STATE_DOWN, Ordering::SeqCst);
        if prev == STATE_DOWN {
            return;
        }
        self.inner
            .last_lived
            .lock()
            .unwrap()
            .insert(path.name.clone(), path.up_age());
        info!(path = %path.name, path_id, "path down");
        self.inner.metrics.path_down.fetch_add(1, Ordering::Relaxed);
        self.observe_failover(&path);
        let mut taken = path.take_all_acks();
        self.rehome_unacked_from(path_id);
        self.inner.budget_wait.notify_waiters();
        self.retry_open_from(path_id);
        self.retry_close_from(path_id);
        self.retry_reset_from(path_id);
        self.inner.paths.lock().unwrap().remove(&path_id);
        for (id, ack) in path.take_all_acks() {
            match taken.entry(id) {
                std::collections::hash_map::Entry::Occupied(e)
                    if e.get().acked_offset >= ack.acked_offset => {}
                std::collections::hash_map::Entry::Occupied(mut e) => {
                    e.insert(ack);
                }
                std::collections::hash_map::Entry::Vacant(e) => {
                    e.insert(ack);
                }
            }
        }
        self.merge_pending_acks(path_id, taken);
        if !self.has_alive_path() {
            *self.inner.all_down_since.lock().unwrap() = Some(Instant::now());
        }
    }

    pub(crate) fn merge_pending_acks(&self, dead: u32, taken: HashMap<u32, StreamAck>) {
        if taken.is_empty() {
            return;
        }
        let Some(alt) = self.pick_retry(dead) else {
            return;
        };
        let Some(dest2) = self.get_path(alt) else {
            return;
        };
        for (_, ack) in taken {
            dest2.merge_ack(ack);
        }
        dest2.ack_wait.notify_one();
    }

    pub(crate) fn note_ack_sent(&self, path: &PathState, stream_id: u32) {
        let Some(st) = self.get_stream(stream_id) else {
            return;
        };
        {
            let g = path.pending_acks.lock().unwrap();
            if g.contains_key(&stream_id) {
                drop(g);
                path.ack_wait.notify_one();
                return;
            }
            st.ack_dirty.store(false, Ordering::Relaxed);
            let from = st.ack_flush_from_us.swap(0, Ordering::Relaxed);
            drop(g);
            if from != 0 {
                let us = crate::metrics::mono_us().saturating_sub(from);
                self.inner.metrics.ack_flush_us.observe(us);
            }
        }
    }

    pub fn handle_frame(&self, path_id: u32, frame: Frame) {
        match frame {
            Frame::Ping(p) => {
                self.send_on_path(
                    path_id,
                    Frame::Pong(Pong {
                        seq: p.seq,
                        sent_at_ms: p.sent_at_ms,
                    }),
                );
            }
            Frame::Pong(p) => {
                if let Some(path) = self.get_path(path_id) {
                    let record = path.inflight_bytes() < self.inner.cfg.tuning.inflight_bias;
                    let known = path.rtt_known();
                    path.on_pong_record(
                        p.seq,
                        p.sent_at_ms,
                        record,
                        Some(self.rtt_sample_cap(&path)),
                        !known,
                    );
                }
            }
            Frame::StreamOpen(open) => {
                if self.inner.is_client {
                    warn!("client received StreamOpen, ignoring");
                    return;
                }
                self.accept_remote_stream(path_id, open);
            }
            Frame::StreamData(data) => self.on_data(path_id, data),
            Frame::StreamAck(ack) => self.on_ack(ack),
            Frame::StreamClose(c) => self.on_peer_close(c),
            Frame::StreamReset(r) => self.on_peer_reset(r.stream_id, r.reason),
            Frame::SessionClose => self.mark_dead(false),
            other => {
                debug!(?other, "ignoring frame on established path");
            }
        }
    }

    fn pick_pref(&self, pref: PickPref) -> Option<u32> {
        let paths = self.path_list();
        pick_path_pref(&paths, &self.inner.cfg, pref)
    }

    /// Reuse last-send for Interactive DATA while that dest is still the
    /// class dest. Not sticky-as-home for bulk, failback, or retry.
    fn interactive_affinity(&self, sticky: u32) -> Option<u32> {
        if sticky == 0 {
            return None;
        }
        let p = self.get_path(sticky)?;
        if !p.is_schedulable() {
            return None;
        }
        if !crate::scheduler::is_loss_fresh(&self.inner.cfg, &p) {
            return None;
        }
        let paths = self.path_list();
        let class = crate::scheduler::interactive_class_set(&paths, &self.inner.cfg);
        if !class.iter().any(|q| q.id == sticky) {
            return None;
        }
        Some(sticky)
    }

    /// One bulk stream stays on one 5-tuple, including a write-stalled dest
    /// that is still flushing. Urgent-full without a working flush does not pin.
    fn bulk_affinity(&self, sticky: u32) -> Option<u32> {
        if sticky == 0 {
            return None;
        }
        let p = self.get_path(sticky)?;
        if !p.is_alive() {
            return None;
        }
        if p.is_congested() && !p.is_write_stalled() {
            return None;
        }
        if p.is_write_stalled() || p.is_schedulable() {
            if crate::scheduler::is_loss_fresh(&self.inner.cfg, &p) {
                Some(sticky)
            } else {
                None
            }
        } else {
            None
        }
    }

    /// P2 fan-out target for a bulk stream whose sticky is at budget.
    fn bulk_overflow_pick(&self, sticky: u32) -> Option<u32> {
        let paths = self.path_list();
        crate::scheduler::bulk_overflow_pick(&paths, &self.inner.cfg, sticky, |id| {
            self.conn_has_interactive(id)
        })
    }

    /// Per-stream limiter summary (P6), for the hop span at copy end.
    pub fn stream_stats(&self, id: u32) -> Option<crate::stream::StreamStats> {
        self.get_stream(id).map(|st| st.stats())
    }

    /// Limiter summary of every live stream.
    pub fn all_stream_stats(&self) -> Vec<(u32, crate::stream::StreamStats)> {
        let streams: Vec<_> = self
            .inner
            .streams
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect();
        streams.iter().map(|st| (st.id, st.stats())).collect()
    }

    fn pick_retry(&self, avoid: u32) -> Option<u32> {
        pick_retry_path(&self.path_list(), &self.inner.cfg, &[avoid])
    }

    fn pick_retry_tried(&self, tried: &[u32]) -> Option<u32> {
        pick_retry_path(&self.path_list(), &self.inner.cfg, tried)
    }

    /// Close/Reset timer-rehome: refuse `pick_retry_path`'s cycle rung.
    /// DATA (`retry_expired_unacked`) still uses [`Self::pick_retry_tried`].
    fn pick_retry_untried(&self, tried: &[u32]) -> Option<u32> {
        let alt = self.pick_retry_tried(tried)?;
        if tried.contains(&alt) {
            None
        } else {
            Some(alt)
        }
    }

    /// Min **fast** EWMA among alive, RTT-known dests. Honest dest RTT, not
    /// frozen class. Shared by [`Self::retry_after`] and StreamAck sample cap.
    pub(super) fn min_alive_fast_rtt(&self) -> Option<Duration> {
        self.path_list()
            .iter()
            .filter(|p| p.is_alive() && p.rtt_known())
            .map(|p| p.rtt())
            .min()
    }

    /// Cap for recording a Pong/ACK sample as path RTT.
    ///
    /// Use **class** (or stable) on this dest, not `min_alive_fast` and not
    /// `min(fast, class)`. A 7 ms peer must not drop a 60 ms backup's real
    /// samples — that poisons class into the fast set and pins interactive
    /// affinity on the slow impair. Queueing delay still exceeds *this*
    /// dest's class loss clock. Unknown dests keep the 300 ms first-sample
    /// window.
    pub(super) fn rtt_sample_cap(&self, path: &PathState) -> Duration {
        if path.class_known() {
            health::loss_timeout(&self.inner.cfg, path.class_rtt())
        } else if path.rtt_known() {
            health::loss_timeout(&self.inner.cfg, path.stable_rtt())
        } else {
            self.inner.cfg.tuning.unknown_degrade_min
        }
    }

    /// Late vs dests we could still send to. Not 2× this path's poisoned
    /// fast EWMA and not 2× frozen class.
    fn retry_after(&self, path_id: u32) -> Duration {
        match self.min_alive_fast_rtt() {
            Some(d) => health::loss_timeout(&self.inner.cfg, d),
            None => match self.get_path(path_id) {
                Some(p) => health::loss_timeout(&self.inner.cfg, p.rtt()),
                None => self.inner.cfg.tuning.loss_timeout_floor,
            },
        }
    }

    pub(crate) fn write_deadline(&self, path_id: u32) -> Duration {
        let young_or_unknown = self
            .get_path(path_id)
            .map(|p| !p.rtt_known() || p.up_age() < self.inner.cfg.tuning.unknown_degrade_min)
            .unwrap_or(true);
        if young_or_unknown || self.min_alive_fast_rtt().is_none() {
            self.inner.cfg.tuning.unknown_degrade_min
        } else {
            self.retry_after(path_id)
        }
    }

    fn note_retry(&self, from: u32, to: u32) {
        let from_link = self.get_path(from).map(|p| p.link().to_string());
        let to_link = self.get_path(to).map(|p| p.link().to_string());
        if from_link.is_some() && from_link == to_link {
            self.inner
                .metrics
                .data_retransmit
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.inner
                .metrics
                .data_hedge
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// P4 bulk hedge clock: twice this path's loaded ACK RTT, never below
    /// the pool loss clock and never above the path's down timeout.
    fn retry_after_bulk(&self, p: &PathState) -> Duration {
        let lo = self.retry_after(p.id);
        let hi = health::down_timeout(&self.inner.cfg, p.stable_rtt(), self.probe_interval_for(p))
            .max(lo);
        match p.ack_rtt() {
            Some(a) => (a * 2).clamp(lo, hi),
            None => lo,
        }
    }

    /// P4 belt for a bulk piece on a *fresh* path: `down_timeout × 2^(tries−1)`,
    /// capped at `down_timeout_ceil`. The only way a piece on a healthy path
    /// is ever re-sent (receiver dropped it silently: unknown stream after
    /// `expire_early_data`, close_off race).
    fn hedge_belt(&self, p: &PathState, tries: usize) -> Duration {
        let base =
            health::down_timeout(&self.inner.cfg, p.stable_rtt(), self.probe_interval_for(p));
        let shift = tries.clamp(1, 5) as u32 - 1;
        base.saturating_mul(1u32 << shift)
            .min(self.inner.cfg.tuning.down_timeout_ceil)
    }

    /// P4: piece is in `unacked` but nothing for it is on a writer queue.
    pub(crate) fn note_data_dropped(&self, stream_id: u32, offset: u64) {
        let Some(st) = self.get_stream(stream_id) else {
            return;
        };
        let mut g = st.unacked.lock().unwrap();
        if let Some(u) = g.get_mut(&offset) {
            u.dropped = true;
            u.retry_not_before = Instant::now();
        }
    }

    /// P2: remember the path ACK clock at (re)send so `on_ack` can turn this
    /// piece's ACK into a delivery-rate sample (BBR `tcp_rate_skb_sent`).
    pub(super) fn stamp_delivered(&self, st: &StreamState, offset: u64, p: &PathState) {
        let now_us = crate::metrics::mono_us().max(1);
        // Pipe was empty: restart both clocks so an idle gap is not rate.
        if p.inflight_bytes()
            <= st
                .unacked
                .lock()
                .unwrap()
                .get(&offset)
                .map_or(0, |u| u.data.len() as u64)
        {
            p.first_tx_us.store(now_us, Ordering::Relaxed);
            p.delivered_at_us.store(now_us, Ordering::Relaxed);
        }
        if let Some(u) = st.unacked.lock().unwrap().get_mut(&offset) {
            u.delivered_at_send = p.delivered.load(Ordering::Relaxed);
            u.delivered_time_at_send_us = p.delivered_at_us.load(Ordering::Relaxed);
            u.sent_us = now_us;
            u.first_tx_at_send_us = p.first_tx_us.load(Ordering::Relaxed);
        }
    }

    /// Expired unacked copies: one send on a *different* path. Never in-place.
    ///
    /// Interactive pieces keep the age clock (`retry_after`). Bulk pieces
    /// are re-sent only when the **path** has been silent for a full
    /// `retry_after_bulk` (≈ 2 × its loaded ACK loop), when they never
    /// reached the wire (`dropped`), or on the slow belt. A path's
    /// per-frame ACKs prove it is delivering, and a bulk ACK loop longer
    /// than the 20 ms clock is transfer delay, not loss. The silence bound
    /// is the loop, not the 20 ms `is_loss_fresh` clock: the TCP under a
    /// bulk path goes quiet for about one loop on every fast-retransmit
    /// recovery (a 1–2 % loss IX does this every few hundred ms), and
    /// hedging each of those is the duplicate storm P4 exists to end. An
    /// RTO-class stall (≥ 200 ms) or a blackhole still trips it.
    fn retry_expired_unacked(&self, st: &StreamState) {
        if !st.is_steerable() {
            return;
        }
        let now = Instant::now();
        let now_ms = crate::metrics::mono_ms();
        let linger_ms = self.inner.cfg.tuning.close_linger.as_millis() as u64;
        let stall_from = st.stall_from_ms.load(Ordering::Relaxed);
        let stalled_long = st.stalled.load(Ordering::Relaxed)
            && stall_from != 0
            && now_ms.saturating_sub(stall_from) >= linger_ms;
        let alive: Vec<u32> = self
            .path_list()
            .iter()
            .filter(|p| p.is_alive())
            .map(|p| p.id)
            .collect();
        let interactive_max = self.inner.cfg.tuning.interactive_max;
        // A short piece of a bulk stream (window/budget remainder, socket
        // read boundary) rides the same bulk queue as its neighbours; the
        // 20 ms interactive age clock would hedge it on every loaded loop.
        let stream_bulk = st.bulk.load(Ordering::Relaxed);
        // Belt: the receiver silently dropped something only if ACK
        // progress (cumulative or selective) has stopped too. While the
        // receiver keeps acknowledging, old pieces on other paths are just
        // behind a hole (or past the SACK budget), and re-sending them is
        // pure duplicate.
        let ack_stalled_ms = match st
            .last_ack_ms
            .load(Ordering::Relaxed)
            .max(st.last_sack_ms.load(Ordering::Relaxed))
        {
            0 => u64::MAX,
            t => now_ms.saturating_sub(t),
        };
        struct Due {
            offset: u64,
            from: u32,
            data: Vec<u8>,
            tried: Vec<u32>,
            dropped: bool,
            bulk: bool,
            why: &'static str,
        }
        let expired: Vec<Due> = {
            let unacked = st.unacked.lock().unwrap();
            if stalled_long
                && now_ms.saturating_sub(st.stuck_logged_ms.load(Ordering::Relaxed)) >= 1000
            {
                st.stuck_logged_ms.store(now_ms, Ordering::Relaxed);
                if let Some((off, u)) = unacked.iter().next() {
                    tracing::debug!(
                        stream = st.id,
                        unacked = unacked.len(),
                        head = off,
                        head_path = u.path_id,
                        head_tried = ?u.tried,
                        head_age_ms = u.last_sent.elapsed().as_millis() as u64,
                        head_dropped = u.dropped,
                        ack_stalled_ms,
                        send_acked = st.send_acked.load(Ordering::Relaxed),
                        send_next = st.send_next.load(Ordering::Relaxed),
                        window = st.send_window.load(Ordering::Relaxed),
                        alive = ?alive,
                        "stream stalled with unacked data"
                    );
                }
            }
            unacked
                .iter()
                .filter_map(|(off, u)| {
                    if now < u.retry_not_before {
                        return None;
                    }
                    let age = u.last_sent.elapsed();
                    let bulk = stream_bulk || u.data.len() > interactive_max;
                    let why = if u.dropped {
                        "dropped"
                    } else {
                        match self.get_path(u.path_id) {
                            None => "gone",
                            Some(p) if !p.is_alive() => "down",
                            Some(_) if !bulk => {
                                if age >= self.retry_after(u.path_id) {
                                    "age"
                                } else {
                                    return None;
                                }
                            }
                            Some(p) => {
                                let quiet = self.retry_after_bulk(&p);
                                let belt = self.hedge_belt(&p, u.tried.len());
                                if p.last_rx_ago() >= quiet && age >= quiet {
                                    "silence"
                                } else if age >= belt && ack_stalled_ms >= belt.as_millis() as u64 {
                                    "belt"
                                } else {
                                    return None;
                                }
                            }
                        }
                    };
                    Some(Due {
                        offset: *off,
                        from: u.path_id,
                        data: u.data.clone(),
                        tried: u.tried.clone(),
                        dropped: u.dropped,
                        bulk,
                        why,
                    })
                })
                .collect()
        };
        for Due {
            offset,
            from,
            data,
            mut tried,
            dropped,
            bulk,
            why,
        } in expired
        {
            if stalled_long && alive.iter().all(|id| tried.contains(id)) {
                self.note_resend_skipped("all_tried");
                continue;
            }
            let from_path = self.get_path(from);
            if !dropped
                && from_path
                    .as_ref()
                    .is_some_and(|p| p.is_alive() && p.is_write_stalled())
            {
                self.note_resend_skipped("write_stalled");
                continue;
            }
            Self::push_tried(&mut tried, from);
            let Some(alt) = self.pick_retry_tried(&tried) else {
                self.note_resend_skipped("no_alt");
                continue;
            };
            if self.get_path(alt).is_some_and(|p| p.is_write_stalled()) {
                self.note_resend_skipped("write_stalled");
                continue;
            }
            // Belt re-send leaves a *fresh* path; a silent alternative
            // (blackholed, not yet down) would only bury the copy. Wait for
            // a fresh one or for the down-rehome.
            if why == "belt"
                && self
                    .get_path(alt)
                    .is_some_and(|p| !crate::scheduler::is_loss_fresh(&self.inner.cfg, &p))
            {
                continue;
            }
            if self.send_data_frame(st.id, offset, data, alt) {
                let base = match (&from_path, bulk) {
                    (Some(p), true) => self.retry_after_bulk(p),
                    _ => Duration::ZERO,
                };
                if let Some(u) = st.unacked.lock().unwrap().get_mut(&offset) {
                    self.rehome_unacked(u, alt);
                    // Bulk backoff: base × 2^(rung−1), rung = paths tried.
                    let shift = u.tried.len().clamp(1, 5) as u32 - 1;
                    let backoff = base
                        .saturating_mul(1u32 << shift)
                        .min(self.inner.cfg.tuning.down_timeout_ceil);
                    u.retry_not_before = u.last_sent + backoff;
                }
                if dropped {
                    self.inner
                        .metrics
                        .data_dropped_resend
                        .fetch_add(1, Ordering::Relaxed);
                }
                st.hedges.fetch_add(1, Ordering::Relaxed);
                st.note_path_used(alt);
                self.note_retry(from, alt);
                self.note_resend_why(why);
                tracing::debug!(
                    stream = st.id,
                    offset,
                    from,
                    to = alt,
                    why,
                    bulk,
                    tries = tried.len(),
                    last_rx_ms = from_path
                        .as_ref()
                        .map(|p| p.last_rx_ago().as_millis() as u64),
                    "data re-sent"
                );
            } else if let Some(u) = st.unacked.lock().unwrap().get_mut(&offset) {
                Self::push_tried(&mut u.tried, alt);
                u.dropped = true;
                u.retry_not_before = Instant::now() + self.retry_after(from);
            }
        }
    }

    /// P1.7 `nya_data_resend_total{why}`.
    pub(super) fn note_resend_why(&self, why: &str) {
        let m = &self.inner.metrics;
        let c = match why {
            "silence" => &m.data_resend_silence,
            "belt" => &m.data_resend_belt,
            "down" => &m.data_resend_down,
            "gone" => &m.data_resend_gone,
            "dropped" => &m.data_resend_dropped,
            "age" => &m.data_resend_age,
            "allquiet" => &m.data_resend_allquiet,
            _ => return,
        };
        c.fetch_add(1, Ordering::Relaxed);
    }

    /// P1.7 `nya_data_resend_skipped_total{reason}`.
    pub(super) fn note_resend_skipped(&self, reason: &str) {
        let m = &self.inner.metrics;
        let c = match reason {
            "no_fresh_alt" => &m.data_resend_skipped_no_fresh_alt,
            "allquiet_wait" => &m.data_resend_skipped_allquiet_wait,
            "queue_full" => &m.data_resend_skipped_queue_full,
            "write_stalled" => &m.data_resend_skipped_write_stalled,
            "all_tried" => &m.data_resend_skipped_all_tried,
            "no_alt" => &m.data_resend_skipped_no_alt,
            _ => return,
        };
        c.fetch_add(1, Ordering::Relaxed);
    }

    fn rehome_unacked_from(&self, dead: u32) {
        let streams: Vec<_> = self
            .inner
            .streams
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect();
        let Some(alt) = self.pick_retry(dead) else {
            return;
        };
        for st in streams {
            self.retransmit_from_on(&st, dead, alt);
        }
    }

    fn push_tried(tried: &mut Vec<u32>, id: u32) {
        if tried.last() == Some(&id) {
            return;
        }
        tried.retain(|x| *x != id);
        if tried.len() == 8 {
            tried.remove(0);
        }
        tried.push(id);
    }

    fn remember_open(&self, id: u32, path_id: u32, target: Target) {
        let mut g = self.inner.opens.lock().unwrap();
        if let Some(o) = g.get_mut(&id) {
            o.path_id = path_id;
            o.sent_at = Instant::now();
            o.target = target;
            Self::push_tried(&mut o.tried, path_id);
            return;
        }
        g.insert(
            id,
            OpenUnacked {
                path_id,
                sent_at: Instant::now(),
                target,
                tried: vec![path_id],
            },
        );
    }

    fn forget_open(&self, id: u32) {
        self.inner.opens.lock().unwrap().remove(&id);
    }

    fn remember_close(
        &self,
        id: u32,
        path_id: u32,
        second_closer: bool,
        final_offset: Option<u64>,
    ) {
        let mut g = self.inner.closes.lock().unwrap();
        if let Some(c) = g.get_mut(&id) {
            c.path_id = path_id;
            c.sent_at = Instant::now();
            Self::push_tried(&mut c.tried, path_id);
            if c.final_offset.is_none() {
                c.final_offset = final_offset;
            }
            return;
        }
        let now = Instant::now();
        g.insert(
            id,
            CloseUnacked {
                path_id,
                sent_at: now,
                started_at: now,
                tried: vec![path_id],
                second_closer,
                final_offset,
            },
        );
    }

    fn forget_close(&self, id: u32) {
        self.inner.closes.lock().unwrap().remove(&id);
    }

    fn retry_opens(&self) {
        let snapshot: Vec<(u32, u32, Target)> = self
            .inner
            .opens
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, o)| o.sent_at.elapsed() >= self.retry_after(o.path_id))
            .map(|(id, o)| (*id, o.path_id, o.target.clone()))
            .collect();
        for (id, from, target) in snapshot {
            let tried = self
                .inner
                .opens
                .lock()
                .unwrap()
                .get(&id)
                .map(|o| o.tried.clone())
                .unwrap_or_else(|| vec![from]);
            let Some(alt) = self.pick_retry_tried(&tried) else {
                continue;
            };
            if self.send_on_path(
                alt,
                Frame::StreamOpen(StreamOpen {
                    stream_id: id,
                    target: target.clone(),
                }),
            ) {
                if let Some(o) = self.inner.opens.lock().unwrap().get_mut(&id) {
                    o.path_id = alt;
                    o.sent_at = Instant::now();
                    o.target = target;
                    Self::push_tried(&mut o.tried, alt);
                }
                self.note_retry(from, alt);
            } else if let Some(o) = self.inner.opens.lock().unwrap().get_mut(&id) {
                o.sent_at = Instant::now();
                Self::push_tried(&mut o.tried, alt);
            }
        }
    }

    fn retry_open_from(&self, dead: u32) {
        let snapshot: Vec<(u32, Target)> = self
            .inner
            .opens
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, o)| o.path_id == dead)
            .map(|(id, o)| (*id, o.target.clone()))
            .collect();
        for (id, target) in snapshot {
            let tried = self
                .inner
                .opens
                .lock()
                .unwrap()
                .get(&id)
                .map(|o| o.tried.clone())
                .unwrap_or_else(|| vec![dead]);
            let Some(alt) = self.pick_retry_tried(&tried) else {
                continue;
            };
            if self.send_on_path(
                alt,
                Frame::StreamOpen(StreamOpen {
                    stream_id: id,
                    target: target.clone(),
                }),
            ) {
                if let Some(o) = self.inner.opens.lock().unwrap().get_mut(&id) {
                    o.path_id = alt;
                    o.sent_at = Instant::now();
                    Self::push_tried(&mut o.tried, alt);
                }
                self.note_retry(dead, alt);
            }
        }
    }

    fn reap_closes(&self) {
        let linger = self.inner.cfg.tuning.close_linger;
        let drop: Vec<u32> = self
            .inner
            .closes
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, c)| c.started_at.elapsed() >= linger)
            .map(|(id, _)| *id)
            .collect();
        for id in drop {
            self.forget_close(id);
        }
    }

    fn retry_closes(&self) {
        self.reap_closes();
        let snapshot: Vec<CloseRetrySnap> = self
            .inner
            .closes
            .lock()
            .unwrap()
            .iter()
            .map(|(id, c)| {
                (
                    *id,
                    c.path_id,
                    c.second_closer,
                    c.tried.clone(),
                    c.sent_at,
                    c.final_offset,
                )
            })
            .collect();
        for (id, from, second, tried, sent_at, final_offset) in snapshot {
            match self.get_stream(id) {
                None => {
                    self.forget_close(id);
                    continue;
                }
                Some(st) if !second && st.recv_fin.load(Ordering::Relaxed) => {
                    self.forget_close(id);
                    continue;
                }
                Some(_) => {}
            }
            if sent_at.elapsed() < self.retry_after(from) {
                continue;
            }
            let Some(alt) = self.pick_retry_untried(&tried) else {
                continue;
            };
            if self.send_on_path(
                alt,
                Frame::StreamClose(StreamClose {
                    stream_id: id,
                    final_offset,
                }),
            ) {
                if let Some(c) = self.inner.closes.lock().unwrap().get_mut(&id) {
                    c.path_id = alt;
                    c.sent_at = Instant::now();
                    Self::push_tried(&mut c.tried, alt);
                }
                self.inner
                    .metrics
                    .close_retry
                    .fetch_add(1, Ordering::Relaxed);
                debug!(stream_id = id, from, to = alt, "close_retry");
            } else if let Some(c) = self.inner.closes.lock().unwrap().get_mut(&id) {
                c.sent_at = Instant::now();
            }
        }
    }

    fn retry_close_from(&self, dead: u32) {
        let snapshot: Vec<(u32, Vec<u32>, Option<u64>, bool)> = self
            .inner
            .closes
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, c)| c.path_id == dead)
            .map(|(id, c)| (*id, c.tried.clone(), c.final_offset, c.second_closer))
            .collect();
        for (id, tried, final_offset, second) in snapshot {
            match self.get_stream(id) {
                None => {
                    self.forget_close(id);
                    continue;
                }
                Some(st) if !second && st.recv_fin.load(Ordering::Relaxed) => {
                    self.forget_close(id);
                    continue;
                }
                Some(_) => {}
            }
            let Some(alt) = self.pick_retry_untried(&tried) else {
                continue;
            };
            if self.send_on_path(
                alt,
                Frame::StreamClose(StreamClose {
                    stream_id: id,
                    final_offset,
                }),
            ) {
                if let Some(c) = self.inner.closes.lock().unwrap().get_mut(&id) {
                    c.path_id = alt;
                    c.sent_at = Instant::now();
                    Self::push_tried(&mut c.tried, alt);
                }
                self.inner
                    .metrics
                    .close_retry
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn remember_reset(&self, id: u32, path_id: u32, reason: ResetReason) {
        let mut g = self.inner.resets.lock().unwrap();
        if let Some(r) = g.get_mut(&id) {
            r.path_id = path_id;
            r.sent_at = Instant::now();
            r.reason = reason;
            if path_id != 0 {
                Self::push_tried(&mut r.tried, path_id);
            }
            return;
        }
        let now = Instant::now();
        g.insert(
            id,
            ResetUnacked {
                path_id,
                sent_at: now,
                started_at: now,
                tried: if path_id == 0 {
                    Vec::new()
                } else {
                    vec![path_id]
                },
                reason,
            },
        );
    }

    fn forget_reset(&self, id: u32) {
        self.inner.resets.lock().unwrap().remove(&id);
    }

    fn reap_resets(&self) {
        let linger = self.inner.cfg.tuning.close_linger;
        let drop: Vec<u32> = self
            .inner
            .resets
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, r)| r.started_at.elapsed() >= linger)
            .map(|(id, _)| *id)
            .collect();
        for id in drop {
            self.forget_reset(id);
        }
    }

    fn retry_resets(&self) {
        self.reap_resets();
        let snapshot: Vec<ResetRetrySnap> = self
            .inner
            .resets
            .lock()
            .unwrap()
            .iter()
            .map(|(id, r)| (*id, r.path_id, r.tried.clone(), r.sent_at, r.reason))
            .collect();
        for (id, from, tried, sent_at, reason) in snapshot {
            if sent_at.elapsed() < self.retry_after(from) {
                continue;
            }
            let Some(alt) = self.pick_retry_untried(&tried) else {
                continue;
            };
            if self.send_on_path(
                alt,
                Frame::StreamReset(StreamReset {
                    stream_id: id,
                    reason,
                }),
            ) {
                if let Some(r) = self.inner.resets.lock().unwrap().get_mut(&id) {
                    r.path_id = alt;
                    r.sent_at = Instant::now();
                    Self::push_tried(&mut r.tried, alt);
                }
                self.inner
                    .metrics
                    .reset_retry
                    .fetch_add(1, Ordering::Relaxed);
                debug!(stream_id = id, from, to = alt, "reset_retry");
            } else if let Some(r) = self.inner.resets.lock().unwrap().get_mut(&id) {
                r.sent_at = Instant::now();
            }
        }
    }

    fn retry_reset_from(&self, dead: u32) {
        let snapshot: Vec<(u32, Vec<u32>, ResetReason)> = self
            .inner
            .resets
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, r)| r.path_id == dead || r.path_id == 0)
            .map(|(id, r)| (*id, r.tried.clone(), r.reason))
            .collect();
        for (id, tried, reason) in snapshot {
            let Some(alt) = self.pick_retry_untried(&tried) else {
                continue;
            };
            if self.send_on_path(
                alt,
                Frame::StreamReset(StreamReset {
                    stream_id: id,
                    reason,
                }),
            ) {
                if let Some(r) = self.inner.resets.lock().unwrap().get_mut(&id) {
                    r.path_id = alt;
                    r.sent_at = Instant::now();
                    Self::push_tried(&mut r.tried, alt);
                }
                self.inner
                    .metrics
                    .reset_retry
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn overlay_progress_fine(&self, st: &StreamState) -> bool {
        let send_fin = st.send_fin_sent.load(Ordering::Relaxed);
        let recv_fin = st.recv_fin.load(Ordering::Relaxed);
        let acked = st.send_acked.load(Ordering::Relaxed);
        let next = st.send_next.load(Ordering::Relaxed);
        if acked < next {
            return false;
        }
        let off = st.recv_close_off.load(Ordering::Relaxed);
        if off != u64::MAX && st.recv_next.load(Ordering::Relaxed) < off {
            return false;
        }
        if recv_fin {
            return true;
        }
        if !send_fin {
            return false;
        }
        !self.inner.opens.lock().unwrap().contains_key(&st.id)
    }

    fn min_known_rtt(&self) -> Duration {
        self.path_list()
            .iter()
            .filter(|p| p.rtt_known())
            .map(|p| p.rtt())
            .min()
            .unwrap_or(self.inner.cfg.ping_interval_max)
    }

    fn push_early_data(&self, path_id: u32, data: StreamData) {
        self.inner
            .pending_early
            .lock()
            .unwrap()
            .entry(data.stream_id)
            .or_default()
            .push(PendingEarly {
                at: Instant::now(),
                path_id,
                data,
            });
    }

    fn take_early_data(&self, id: u32) -> Vec<PendingEarly> {
        self.inner
            .pending_early
            .lock()
            .unwrap()
            .remove(&id)
            .unwrap_or_default()
    }

    fn expire_early_data(&self) {
        // Open retry fires at 1× loss_timeout; keep DATA a second cycle.
        let thresh = health::loss_timeout(&self.inner.cfg, self.min_known_rtt()).saturating_mul(2);
        let mut g = self.inner.pending_early.lock().unwrap();
        for v in g.values_mut() {
            v.retain(|e| e.at.elapsed() < thresh);
        }
        g.retain(|_, v| !v.is_empty());
    }

    pub fn last_known_rtt(&self, name: &str) -> Option<Duration> {
        self.inner
            .last_rtt_us
            .lock()
            .unwrap()
            .get(name)
            .copied()
            .map(Duration::from_micros)
    }

    pub fn last_path_lived(&self, name: &str) -> Option<Duration> {
        self.inner.last_lived.lock().unwrap().get(name).copied()
    }

    /// Handshake-ok then silent-down is a flap, not a recovery. Reconnect
    /// backoff resets only after the dest has been up for a hold.
    pub fn path_lived_stable(&self, name: &str) -> bool {
        self.last_path_lived(name)
            .is_some_and(|d| d >= self.inner.cfg.tuning.stable_up_hold)
    }

    fn set_sticky(&self, stream_id: u32, path_id: u32) {
        let Some(st) = self.get_stream(stream_id) else {
            return;
        };
        if !st.is_steerable() {
            return;
        }
        let old = st.sticky.swap(path_id, Ordering::Relaxed);
        if old == path_id {
            return;
        }
        if old != 0 {
            if let Some(p) = self.get_path(old) {
                p.drop_sticky();
            }
            st.note_stick_change();
        }
        if let Some(p) = self.get_path(path_id) {
            p.add_sticky();
        }
    }

    fn unstick(&self, st: &StreamState) {
        let old = st.sticky.swap(0, Ordering::Relaxed);
        if old == 0 {
            return;
        }
        if let Some(p) = self.get_path(old) {
            p.drop_sticky();
        }
    }

    fn release_unacked(&self, st: &StreamState) {
        let leftover: Vec<Unacked> = {
            let mut g = st.unacked.lock().unwrap();
            std::mem::take(&mut *g).into_values().collect()
        };
        let mut freed = false;
        for u in leftover {
            if let Some(p) = self.get_path(u.path_id) {
                p.sub_inflight(u.data.len() as u64);
                freed = true;
            }
        }
        if freed {
            self.inner.budget_wait.notify_waiters();
        }
    }

    fn remove_held_stream(&self, id: u32) {
        let Some(st) = self.inner.streams.lock().unwrap().remove(&id) else {
            return;
        };
        self.unstick(&st);
        self.release_unacked(&st);
        self.forget_open(id);
    }

    fn xfer_inflight(&self, from: u32, to: u32, n: u64) {
        if from == to || n == 0 {
            return;
        }
        if let Some(p) = self.get_path(from) {
            p.sub_inflight(n);
            self.inner.budget_wait.notify_waiters();
        }
        if let Some(p) = self.get_path(to) {
            p.add_inflight(n);
        }
    }

    fn rehome_unacked(&self, u: &mut Unacked, to: u32) {
        self.xfer_inflight(u.path_id, to, u.data.len() as u64);
        u.path_id = to;
        u.last_sent = Instant::now();
        u.retry_not_before = u.last_sent;
        u.dropped = false;
        Self::push_tried(&mut u.tried, to);
        if let Some(p) = self.get_path(to) {
            u.delivered_at_send = p.delivered.load(Ordering::Relaxed);
            u.delivered_time_at_send_us = p.delivered_at_us.load(Ordering::Relaxed);
            u.sent_us = crate::metrics::mono_us().max(1);
            u.first_tx_at_send_us = p.first_tx_us.load(Ordering::Relaxed);
        }
    }

    fn send_data_frame(&self, stream_id: u32, offset: u64, data: Vec<u8>, path_id: u32) -> bool {
        self.send_on_path(
            path_id,
            Frame::StreamData(StreamData {
                stream_id,
                offset,
                data,
            }),
        )
    }

    /// Retransmit unacked still assigned to `from` onto `to`.
    fn retransmit_from_on(&self, st: &StreamState, from: u32, to: u32) {
        let mut unacked = st.unacked.lock().unwrap();
        let mut n = 0u64;
        for (offset, u) in unacked.iter_mut() {
            if u.path_id != from {
                continue;
            }
            self.rehome_unacked(u, to);
            self.send_data_frame(st.id, *offset, u.data.clone(), to);
            n += 1;
        }
        if n > 0 {
            self.inner
                .metrics
                .data_retransmit
                .fetch_add(n, Ordering::Relaxed);
            st.hedges.fetch_add(n, Ordering::Relaxed);
            st.note_path_used(to);
        }
    }

    fn get_path(&self, id: u32) -> Option<Arc<PathState>> {
        self.inner.paths.lock().unwrap().get(&id).cloned()
    }

    fn get_stream(&self, id: u32) -> Option<Arc<StreamState>> {
        self.inner.streams.lock().unwrap().get(&id).cloned()
    }

    fn path_list(&self) -> Vec<Arc<PathState>> {
        self.inner.paths.lock().unwrap().values().cloned().collect()
    }

    /// Control frames and small STREAM_DATA go out ahead of bulk on the same TCP.
    fn frame_is_interactive(&self, frame: &Frame) -> bool {
        match frame {
            Frame::StreamData(d) => d.data.len() <= self.inner.cfg.tuning.interactive_max,
            _ => true,
        }
    }

    pub(crate) fn note_send_drop(&self) {
        self.inner
            .metrics
            .frame_send_drop
            .fetch_add(1, Ordering::Relaxed);
    }

    fn send_on_path(&self, path_id: u32, frame: Frame) -> bool {
        let Some(p) = self.get_path(path_id) else {
            return false;
        };
        if !p.is_alive() {
            return false;
        }
        let data = matches!(&frame, Frame::StreamData(_));
        p.note_tx();
        // Unknown dests keep STREAM_DATA off urgent so ping/control can
        // complete the first RTT before DATA pins FramedWrite. Known+stalled
        // DATA is forced onto bulk: stall is pick-skip, not "this TCP lost it".
        let urgent = if data && (!p.rtt_known() || p.is_write_stalled()) {
            false
        } else {
            self.frame_is_interactive(&frame)
        };
        let tx = if urgent { &p.urgent } else { &p.writer };
        p.note_enqueue(urgent);
        if tx.try_send(frame).is_ok() {
            if urgent {
                p.set_congested(false);
            }
            true
        } else {
            p.undo_enqueue(urgent);
            // A full bulk queue must not mark the path unusable for ACKs/pings.
            if urgent {
                p.set_congested(true);
            }
            self.note_send_drop();
            if urgent {
                debug!(path = %p.name, urgent = true, "send dropped");
            }
            false
        }
    }

    pub(crate) fn account_overlay_frame(&self, frame: &Frame, encoded: usize, tx: bool) {
        let data = match frame {
            Frame::StreamData(d) => d.data.len() as u64,
            _ => 0,
        };
        let encoded = encoded as u64;
        let ctrl = encoded.saturating_sub(data);
        let m = &self.inner.metrics;
        if tx {
            if data != 0 {
                m.bytes_data_tx.fetch_add(data, Ordering::Relaxed);
            }
            if ctrl != 0 {
                m.bytes_ctrl_tx.fetch_add(ctrl, Ordering::Relaxed);
            }
        } else {
            if data != 0 {
                m.bytes_data_rx.fetch_add(data, Ordering::Relaxed);
            }
            if ctrl != 0 {
                m.bytes_ctrl_rx.fetch_add(ctrl, Ordering::Relaxed);
            }
        }
    }

    pub(crate) fn observe_failover(&self, path: &PathState) {
        if path
            .failover_recorded
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
        {
            self.inner
                .metrics
                .failover_ms
                .observe(path.last_rx_ago().as_millis() as u64);
        }
    }

    /// Progress-fine linger: HashMap remove without wire Reset.
    /// `forget_reset` only on the `counted_close` CAS-winner path.
    pub(crate) fn linger_reap_progress_fine(&self, id: u32) {
        let Some(st) = self.get_stream(id) else {
            return;
        };
        if !self.overlay_progress_fine(&st) {
            self.reset_stream(id, ResetReason::Timeout);
            return;
        }
        if st
            .counted_close
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        if !self.overlay_progress_fine(&st) {
            self.observe_stream_end(&st, Some(ResetReason::Timeout));
            self.finish_stream(id, Some(ResetReason::Timeout), true);
            return;
        }
        self.observe_stream_end(&st, Some(ResetReason::Timeout));
        if !st.recv_fin.swap(true, Ordering::SeqCst) {
            let _ = st.inbound_tx.try_send(crate::stream::Inbound::Close);
        }
        self.remove_held_stream(id);
    }

    /// Client Residual D: wire Reset, local Close. Observe while `recv_fin`
    /// is still false so the linger split does not `forget_reset`.
    pub(crate) fn residual_d_client(&self, id: u32) {
        let Some(st) = self.get_stream(id) else {
            return;
        };
        if !self.inner.is_client
            || st.recv_fin.load(Ordering::Relaxed)
            || !self.overlay_progress_fine(&st)
        {
            if self.overlay_progress_fine(&st) {
                self.linger_reap_progress_fine(id);
            } else {
                self.reset_stream(id, ResetReason::Timeout);
            }
            return;
        }
        if st
            .counted_close
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        let why = ResetReason::Timeout;
        match self.pick_pref(PickPref::Any) {
            Some(p) => {
                self.remember_reset(id, p, why);
                if !self.send_on_path(
                    p,
                    Frame::StreamReset(StreamReset {
                        stream_id: id,
                        reason: why,
                    }),
                ) {
                    if let Some(alt) = self.pick_retry_untried(&[p]) {
                        self.remember_reset(id, alt, why);
                        if self.send_on_path(
                            alt,
                            Frame::StreamReset(StreamReset {
                                stream_id: id,
                                reason: why,
                            }),
                        ) {
                            self.inner
                                .metrics
                                .reset_retry
                                .fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
            None => self.remember_reset(id, 0, why),
        }
        self.observe_stream_end(&st, Some(ResetReason::Timeout));
        if !st.recv_fin.swap(true, Ordering::SeqCst) {
            let _ = st.inbound_tx.try_send(crate::stream::Inbound::Close);
        }
        self.remove_held_stream(id);
    }

    pub(crate) fn finish_stream(
        &self,
        id: u32,
        reset_reason: Option<ResetReason>,
        send_frame: bool,
    ) {
        let Some(st) = self.get_stream(id) else {
            return;
        };
        let first_reset = !st.reset.swap(true, Ordering::SeqCst);
        if first_reset {
            let why = reset_reason.unwrap_or(ResetReason::SessionDead);
            // Peer Reset after Close is EOF, not hop RST.
            if !(st.recv_fin.load(Ordering::Relaxed) && self.overlay_progress_fine(&st)) {
                let _ = st.inbound_tx.try_send(crate::stream::Inbound::Reset(why));
            }
            st.send_wait.notify_waiters();
            let live = send_frame && !self.inner.dead.load(Ordering::Relaxed);
            if live {
                match self.pick_pref(PickPref::Any) {
                    Some(p) => {
                        self.remember_reset(id, p, why);
                        if !self.send_on_path(
                            p,
                            Frame::StreamReset(StreamReset {
                                stream_id: id,
                                reason: why,
                            }),
                        ) {
                            if let Some(alt) = self.pick_retry(p) {
                                self.remember_reset(id, alt, why);
                                if self.send_on_path(
                                    alt,
                                    Frame::StreamReset(StreamReset {
                                        stream_id: id,
                                        reason: why,
                                    }),
                                ) {
                                    self.inner
                                        .metrics
                                        .reset_retry
                                        .fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    }
                    None => self.remember_reset(id, 0, why),
                }
            } else {
                self.forget_reset(id);
            }
        }
        if st
            .counted_close
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
        {
            self.observe_stream_end(&st, reset_reason);
        }
        self.remove_held_stream(id);
    }

    pub(crate) fn observe_stream_end(&self, st: &StreamState, reset_reason: Option<ResetReason>) {
        let now = crate::metrics::mono_ms();
        let opened = st.opened_ms.load(Ordering::Relaxed);
        if opened != 0 {
            self.inner
                .metrics
                .stream_lifetime_ms
                .observe(now.saturating_sub(opened));
        }
        if st.stalled.load(Ordering::Relaxed) {
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
        // P1.6: the receiver's max advertised cap, per stream that received.
        if st.recv_next.load(Ordering::Relaxed) > 0 {
            self.inner
                .metrics
                .recv_cap_max_bytes
                .observe(u64::from(st.recv_cap_max.load(Ordering::Relaxed)));
        }
        match reset_reason {
            None => {
                self.inner
                    .metrics
                    .streams_closed
                    .fetch_add(1, Ordering::Relaxed);
            }
            Some(ResetReason::Timeout) if self.overlay_progress_fine(st) => {
                self.inner
                    .metrics
                    .streams_closed
                    .fetch_add(1, Ordering::Relaxed);
                self.inner
                    .metrics
                    .stream_reaps_linger
                    .fetch_add(1, Ordering::Relaxed);
                debug!(stream_id = st.id, reason = "linger", "stream end");
                self.forget_close(st.id);
                if st.recv_fin.load(Ordering::Relaxed) {
                    self.forget_reset(st.id);
                }
                return;
            }
            Some(reason) => {
                self.inner
                    .metrics
                    .stream_resets
                    .fetch_add(1, Ordering::Relaxed);
                let c = match reason {
                    ResetReason::DialFailed => &self.inner.metrics.stream_resets_dial_failed,
                    ResetReason::Timeout => &self.inner.metrics.stream_resets_timeout,
                    ResetReason::PeerReset => &self.inner.metrics.stream_resets_peer,
                    ResetReason::SessionDead => &self.inner.metrics.stream_resets_session_dead,
                    ResetReason::Protocol => &self.inner.metrics.stream_resets_protocol,
                    ResetReason::Unknown => &self.inner.metrics.stream_resets_protocol,
                };
                c.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.forget_close(st.id);
        debug!(stream_id = st.id, ?reset_reason, "stream end");
    }

    pub fn snapshot(&self) -> Snapshot {
        let paths = self.path_list();
        let mut snap = self.inner.metrics.snap_with_paths(&paths);
        let min_class = snap
            .paths
            .iter()
            .filter(|p| p.rtt_known)
            .map(|p| p.class_rtt_us)
            .min();
        if let Some(min) = min_class {
            for p in &mut snap.paths {
                if !p.rtt_known {
                    continue;
                }
                p.backup = crate::health::is_backup(
                    &self.inner.cfg,
                    Duration::from_micros(p.class_rtt_us),
                    Duration::from_micros(min),
                );
            }
        }
        let (held, live, sample) = self.stream_snaps();
        snap.streams = sample;
        snap.streams_held = held;
        snap.streams_live = live;
        snap.links = crate::metrics::rollup_links(&snap.paths);
        snap
    }

    fn stream_snaps(&self) -> (u64, u64, Vec<crate::metrics::StreamSnap>) {
        use crate::metrics::{StreamSnap, STREAM_SNAP_CAP};
        let names: HashMap<u32, String> = self
            .inner
            .paths
            .lock()
            .unwrap()
            .iter()
            .map(|(id, p)| (*id, p.name.clone()))
            .collect();
        let (held_n, live): (u64, Vec<Arc<StreamState>>) = {
            let g = self.inner.streams.lock().unwrap();
            let held_n = g.len() as u64;
            let live = g.values().filter(|st| st.is_steerable()).cloned().collect();
            (held_n, live)
        };
        let sample = live
            .iter()
            .take(STREAM_SNAP_CAP)
            .map(|st| {
                let pid = st.sticky.load(Ordering::Relaxed);
                StreamSnap {
                    id: st.id,
                    path: names
                        .get(&pid)
                        .cloned()
                        .unwrap_or_else(|| format!("id:{pid}")),
                    bulk: st.bulk.load(Ordering::Relaxed),
                    stalled: st.stalled.load(Ordering::Relaxed),
                    unacked: st.unacked.lock().unwrap().len() as u64,
                }
            })
            .collect();
        (held_n, live.len() as u64, sample)
    }

    pub(crate) fn note_migrate(&self, reason: &'static str) {
        self.inner.metrics.migrates.fetch_add(1, Ordering::Relaxed);
        let c = match reason {
            "speculative" => &self.inner.metrics.migrates_speculative,
            "path_down" => &self.inner.metrics.migrates_path_down,
            "ensure_sticky" => &self.inner.metrics.migrates_ensure_sticky,
            "send_blocked" => &self.inner.metrics.migrates_send_blocked,
            "loop_unfit" => &self.inner.metrics.migrates_loop_unfit,
            _ => return,
        };
        c.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn note_unknown_pick(&self, path_id: u32) {
        let Some(p) = self.get_path(path_id) else {
            return;
        };
        if p.rtt_known() {
            return;
        }
        self.inner
            .metrics
            .picks_unknown_rtt
            .fetch_add(1, Ordering::Relaxed);
        if self.path_list().iter().any(|q| q.rtt_known()) {
            self.inner
                .metrics
                .picks_unknown_over_known
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn note_pick(&self, path_id: u32) {
        let Some(p) = self.get_path(path_id) else {
            return;
        };
        let rtt = if p.rtt_known() { p.rtt_us() } else { 0 };
        self.inner.metrics.pick_rtt_us.store(rtt, Ordering::Relaxed);
        p.picks.fetch_add(1, Ordering::Relaxed);
    }

    pub fn alive_path_names(&self) -> Vec<String> {
        self.inner
            .paths
            .lock()
            .unwrap()
            .values()
            .filter(|p| p.is_alive())
            .map(|p| p.name.clone())
            .collect()
    }

    #[cfg(test)]
    pub fn debug_mark_degraded(&self, name: &str) {
        let p = self
            .inner
            .paths
            .lock()
            .unwrap()
            .values()
            .find(|p| p.name == name)
            .cloned();
        if let Some(p) = p {
            p.mark_degraded();
        }
    }

    #[cfg(test)]
    pub fn debug_drop_path(&self, name: &str) {
        let id = self
            .inner
            .paths
            .lock()
            .unwrap()
            .values()
            .find(|p| p.name == name)
            .map(|p| p.id);
        if let Some(id) = id {
            self.path_failed(id);
        }
    }

    #[cfg(test)]
    pub fn debug_path_names(&self) -> Vec<String> {
        self.inner
            .paths
            .lock()
            .unwrap()
            .values()
            .filter(|p| p.is_alive())
            .map(|p| p.name.clone())
            .collect()
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if self
            .dead
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        self.process.sessions_dead.fetch_add(1, Ordering::Relaxed);
        let _ =
            self.process
                .sessions_live
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_sub(1))
                });
        self.dead_notify.notify_waiters();
        let now = crate::metrics::mono_ms();
        if let Ok(streams) = self.streams.get_mut() {
            for st in streams.values() {
                st.reset.store(true, Ordering::SeqCst);
                let _ = st
                    .inbound_tx
                    .try_send(crate::stream::Inbound::Reset(ResetReason::SessionDead));
                st.send_wait.notify_waiters();
                if st
                    .counted_close
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    let opened = st.opened_ms.load(Ordering::Relaxed);
                    if opened != 0 {
                        self.metrics
                            .stream_lifetime_ms
                            .observe(now.saturating_sub(opened));
                    }
                    if st.stalled.load(Ordering::Relaxed) {
                        let from = st.stall_from_ms.load(Ordering::Relaxed);
                        if from != 0 {
                            self.metrics.stall_ms.observe(now.saturating_sub(from));
                        }
                    }
                    self.metrics.stream_resets.fetch_add(1, Ordering::Relaxed);
                    self.metrics
                        .stream_resets_session_dead
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("session is dead")]
    Dead,
    #[error("no healthy path")]
    NoPath,
    #[error("unknown stream")]
    UnknownStream,
    #[error("stream reset")]
    Reset,
    #[error("server cannot open streams")]
    ServerCannotOpen,
}

pub struct SessionTable {
    cfg: SessionConfig,
    sessions: Arc<Mutex<HashMap<[u8; 16], Session>>>,
    closed: AtomicBool,
    process: Arc<ProcessCounters>,
}

impl SessionTable {
    pub fn new(cfg: SessionConfig) -> Self {
        Self {
            cfg,
            sessions: Arc::new(Mutex::new(HashMap::new())),
            closed: AtomicBool::new(false),
            process: Arc::new(ProcessCounters::default()),
        }
    }

    pub fn process(&self) -> Arc<ProcessCounters> {
        self.process.clone()
    }

    pub fn aggregate_snapshot(&self) -> ProcessSnapshot {
        let handles: Vec<([u8; 16], Session)> = {
            let mut g = self.sessions.lock().unwrap();
            g.retain(|_, s| !s.is_dead());
            g.iter().map(|(id, s)| (*id, s.clone())).collect()
        };
        let sessions: Vec<([u8; 16], Snapshot)> =
            handles.iter().map(|(id, s)| (*id, s.snapshot())).collect();
        // P1.6: seed *every* histogram (a `Default` field never merged).
        let mut acc = Snapshot::zeroed_hists();
        for (_, snap) in &sessions {
            acc.add_counters(snap);
        }
        acc.paths = flatten_paths(&sessions);
        acc.links = crate::metrics::rollup_links(&acc.paths);
        let prefix = sessions.len() > 1;
        acc.streams = sessions
            .iter()
            .flat_map(|(id, snap)| {
                snap.streams.iter().cloned().map(move |mut st| {
                    if prefix {
                        st.path = format!("{:02x}{:02x}:{}", id[0], id[1], st.path);
                    }
                    st
                })
            })
            .take(crate::metrics::STREAM_SNAP_CAP)
            .collect();
        let mut session_fps: Vec<String> =
            handles.iter().filter_map(|(_, s)| s.session_fp()).collect();
        session_fps.sort();
        ProcessSnapshot {
            process: self.process.snap(),
            session: acc,
            session_fps,
        }
    }

    pub fn shutdown_all(&self) {
        self.closed.store(true, Ordering::SeqCst);
        let sessions: Vec<_> = self
            .sessions
            .lock()
            .unwrap()
            .drain()
            .map(|(_, s)| s)
            .collect();
        for s in sessions {
            s.shutdown();
        }
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// None if the table has been shut down. Caller must send HandshakeErr.
    pub fn create_with_incoming(
        &self,
        session_id: [u8; 16],
    ) -> Option<(Session, mpsc::Receiver<IncomingStream>)> {
        if self.is_closed() {
            return None;
        }
        let (tx, rx) = mpsc::channel(self.cfg.tuning.chan);
        let session = Session::new(
            self.cfg.clone(),
            false,
            Some(tx),
            Some(self.process.clone()),
        );
        session.set_session_id(&session_id);
        session
            .inner
            .reap_on_all_down
            .store(true, Ordering::Relaxed);
        self.sessions
            .lock()
            .unwrap()
            .insert(session_id, session.clone());
        // Drop the HashMap slot when the session dies. get() / snapshot
        // already skip dead entries; without this the slot sits until the
        // next 10 s tick, and hop-tail sfp is the only leftover identity.
        let slots = self.sessions.clone();
        let reap = session.clone();
        tokio::spawn(async move {
            reap.wait_dead().await;
            slots.lock().unwrap().remove(&session_id);
        });
        Some((session, rx))
    }

    pub fn get(&self, session_id: &[u8; 16]) -> Option<Session> {
        if self.is_closed() {
            return None;
        }
        let mut g = self.sessions.lock().unwrap();
        match g.get(session_id) {
            Some(s) if s.is_dead() => {
                g.remove(session_id);
                None
            }
            Some(s) => Some(s.clone()),
            None => None,
        }
    }

    pub fn remove(&self, session_id: &[u8; 16]) {
        self.sessions.lock().unwrap().remove(session_id);
    }

    #[cfg(test)]
    pub fn debug_len(&self) -> usize {
        self.sessions.lock().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::StreamState;
    use nya_proto::{StreamAck, StreamClose, StreamData, StreamOpen, Target};
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{duplex, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

    #[tokio::test]
    async fn create_sets_session_fp_and_recreate_replaces() {
        let table = SessionTable::new(SessionConfig::default());
        let mut id = [0u8; 16];
        id[0] = 0xde;
        id[1] = 0xad;
        id[2] = 0xbe;
        id[3] = 0xef;
        let (s, _rx) = table.create_with_incoming(id).unwrap();
        assert_eq!(s.session_fp().as_deref(), Some("deadbeef"));
        let mut id2 = [0u8; 16];
        id2[0] = 0x4c;
        id2[1] = 0xcd;
        id2[2] = 0x39;
        id2[3] = 0x13;
        s.set_session_id(&id2);
        assert_eq!(s.session_fp().as_deref(), Some("4ccd3913"));
        s.shutdown();
        table.shutdown_all();
    }

    #[tokio::test]
    async fn table_get_drops_dead_session() {
        let table = SessionTable::new(SessionConfig::default());
        let id = [7u8; 16];
        let (s, _rx) = table.create_with_incoming(id).unwrap();
        assert!(table.get(&id).is_some());
        s.shutdown();
        assert!(table.get(&id).is_none());
        assert!(table.get(&id).is_none());
        table.shutdown_all();
    }

    #[tokio::test]
    async fn table_session_all_down_reaps() {
        let cfg = SessionConfig {
            all_down_timeout: Duration::ZERO,
            ..SessionConfig::default()
        };
        let table = SessionTable::new(cfg);
        let id = [8u8; 16];
        let (s, _rx) = table.create_with_incoming(id).unwrap();
        s.debug_maintain();
        assert!(s.is_dead(), "table-owned server session must reap");
        assert_eq!(s.snapshot().session_all_down_resets, 1);
        assert!(table.get(&id).is_none());
        table.shutdown_all();
    }

    #[tokio::test]
    async fn standalone_server_all_down_does_not_reap() {
        let cfg = SessionConfig {
            all_down_timeout: Duration::ZERO,
            ..SessionConfig::default()
        };
        let (server, _rx) = Session::new_server(cfg);
        server.debug_maintain();
        assert!(
            !server.is_dead(),
            "e2e duplex server must survive all-down so blackhole can recover"
        );
        assert_eq!(server.snapshot().session_all_down_resets, 0);
        server.shutdown();
    }

    #[tokio::test]
    async fn aggregate_snapshot_drops_dead_session() {
        let table = SessionTable::new(SessionConfig::default());
        let mut live_id = [0u8; 16];
        live_id[0] = 0x34;
        live_id[1] = 0x94;
        let (live, _rx1) = table.create_with_incoming(live_id).unwrap();
        let (dead, _rx2) = table.create_with_incoming([9u8; 16]).unwrap();
        dead.shutdown();
        let snap = table.aggregate_snapshot();
        assert_eq!(snap.session.links.len(), 0);
        assert!(table.get(&[9u8; 16]).is_none());
        assert!(table.get(&live_id).is_some());
        live.shutdown();
        table.shutdown_all();
    }

    #[tokio::test]
    async fn aggregate_snapshot_lists_live_session_fp() {
        let table = SessionTable::new(SessionConfig::default());
        let mut live_id = [0u8; 16];
        live_id[0] = 0x34;
        live_id[1] = 0x94;
        live_id[2] = 0x57;
        live_id[3] = 0xb6;
        let (live, _rx1) = table.create_with_incoming(live_id).unwrap();
        let (dead, _rx2) = table.create_with_incoming([9u8; 16]).unwrap();
        dead.shutdown();
        let snap = table.aggregate_snapshot();
        assert_eq!(snap.session_fps, vec!["349457b6".to_string()]);
        live.shutdown();
        let snap = table.aggregate_snapshot();
        assert!(
            snap.session_fps.is_empty(),
            "dead live-id must not stay in sfp"
        );
        table.shutdown_all();
    }

    #[tokio::test]
    async fn table_reaps_dead_session_without_get_or_snapshot() {
        let table = SessionTable::new(SessionConfig::default());
        let id = [0x11u8; 16];
        let (s, _rx) = table.create_with_incoming(id).unwrap();
        assert_eq!(table.debug_len(), 1);
        s.shutdown();
        tokio::time::timeout(Duration::from_millis(500), async {
            loop {
                if table.debug_len() == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dead session must leave the table without get/snapshot");
        table.shutdown_all();
    }

    fn pair() -> (Session, Session, mpsc::Receiver<IncomingStream>) {
        let mut cfg = SessionConfig::default();
        cfg.tuning.loss_timeout_floor = Duration::from_millis(150);
        cfg.all_down_timeout = Duration::from_secs(2);
        let client = Session::new_client(cfg.clone());
        let (server, incoming) = Session::new_server(cfg);
        (client, server, incoming)
    }

    async fn pair_echo(names: &[&str]) -> (Session, Session) {
        pair_echo_cfg(names, {
            let mut cfg = SessionConfig::default();
            cfg.tuning.loss_timeout_floor = Duration::from_millis(150);
            cfg.all_down_timeout = Duration::from_secs(2);
            cfg
        })
        .await
    }

    async fn pair_echo_cfg(names: &[&str], cfg: SessionConfig) -> (Session, Session) {
        let client = Session::new_client(cfg.clone());
        let (server, incoming) = Session::new_server(cfg);
        tokio::spawn(echo_server(incoming));
        for name in names {
            let (ca, sa) = duplex(64 * 1024);
            let c = client.clone();
            let s = server.clone();
            let n1 = name.to_string();
            let n2 = name.to_string();
            tokio::spawn(async move { c.add_path(n1, ca).await });
            tokio::spawn(async move { s.add_path(n2, sa).await });
        }
        client
            .wait_paths(names.len(), Duration::from_secs(2))
            .await
            .unwrap();
        (client, server)
    }

    async fn echo_server(mut incoming: mpsc::Receiver<IncomingStream>) {
        while let Some(mut inc) = incoming.recv().await {
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                loop {
                    match inc.io.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => {
                            if inc.io.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
            });
        }
    }

    #[tokio::test]
    async fn path_failed_completes_add_path() {
        let client = Session::new_client(SessionConfig::default());
        let (a, _peer) = duplex(64 * 1024);
        let done = client.start_path("p1".into(), a);
        let id = *client.inner.paths.lock().unwrap().keys().next().unwrap();
        client.path_failed(id);
        tokio::time::timeout(Duration::from_millis(500), done)
            .await
            .expect("add_path must complete after path_failed")
            .expect("oneshot");
        client.shutdown();
    }

    #[tokio::test]
    async fn short_lived_path_is_not_stable() {
        let client = Session::new_client(SessionConfig::default());
        inject_named(&client, 1, "nsix#1", 7);
        assert!(!client.path_lived_stable("nsix#1"));
        client.path_failed(1);
        assert!(
            !client.path_lived_stable("nsix#1"),
            "handshake-ok then immediate down must keep reconnect backoff"
        );
        client.shutdown();
    }

    #[tokio::test]
    async fn hold_aged_path_is_stable() {
        let client = Session::new_client(SessionConfig::default());
        let p = inject_named(&client, 1, "nsix#1", 7);
        p.backdate_up_since(client.config().tuning.stable_up_hold);
        client.path_failed(1);
        assert!(
            client.path_lived_stable("nsix#1"),
            "a dest that stayed up for a hold is a real recovery"
        );
        client.shutdown();
    }

    #[tokio::test]
    async fn echo_single_path() {
        let (client, server, incoming) = pair();
        tokio::spawn(echo_server(incoming));
        let (a, b) = duplex(64 * 1024);
        let c = client.clone();
        let s = server.clone();
        tokio::spawn(async move { c.add_path("p1".into(), a).await });
        tokio::spawn(async move { s.add_path("p1".into(), b).await });
        client.wait_ready(Duration::from_secs(2)).await.unwrap();

        let mut tun = client
            .open_stream(Target {
                host: "echo".into(),
                port: 1,
            })
            .await
            .unwrap();
        tun.write_all(b"hello").await.unwrap();
        let mut buf = [0u8; 5];
        tun.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");
        client.shutdown();
        server.shutdown();
    }

    #[tokio::test]
    async fn expired_unacked_retries_other_link() {
        let client = Session::new_client(SessionConfig::default());
        let _held_a = inject_live(&client, 1, "akcdn#0", 7);
        let _held_b = inject_live(&client, 2, "soy#0", 7);
        let mut tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        tun.write_all(b"hello").await.unwrap();
        let st = {
            let g = client.inner.streams.lock().unwrap();
            g.values().next().unwrap().clone()
        };
        let deadline = Instant::now() + Duration::from_millis(200);
        loop {
            if !st.unacked.lock().unwrap().is_empty() {
                break;
            }
            assert!(Instant::now() < deadline, "write must leave unacked");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let from = {
            let mut u = st.unacked.lock().unwrap();
            for x in u.values_mut() {
                x.last_sent = Instant::now() - Duration::from_millis(100);
            }
            u.values().next().unwrap().path_id
        };
        let hedge0 = client.snapshot().data_hedge;
        let rtx0 = client.snapshot().data_retransmit;
        client.debug_maintain();
        let to = st.unacked.lock().unwrap().values().next().unwrap().path_id;
        assert_ne!(to, from, "retry must leave the timed-out path");
        assert!(client.snapshot().data_hedge + client.snapshot().data_retransmit > hedge0 + rtx0);
        drop(tun);
        client.shutdown();
    }

    /// Open a stream on `client` and push one bulk-sized piece; returns the
    /// stream state once the piece sits in `unacked`.
    async fn one_bulk_piece(client: &Session) -> (crate::stream::TunnelStream, Arc<StreamState>) {
        let mut tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        tun.write_all(&vec![0x42u8; 4000]).await.unwrap();
        let st = {
            let g = client.inner.streams.lock().unwrap();
            g.values().next().unwrap().clone()
        };
        let deadline = Instant::now() + Duration::from_millis(200);
        loop {
            if !st.unacked.lock().unwrap().is_empty() {
                break;
            }
            assert!(Instant::now() < deadline, "write must leave unacked");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        (tun, st)
    }

    fn hedges(client: &Session) -> u64 {
        let s = client.snapshot();
        s.data_hedge + s.data_retransmit
    }

    /// P4: a bulk piece on a receive-fresh path is transfer delay, not loss.
    #[tokio::test]
    async fn bulk_piece_on_fresh_path_is_not_hedged() {
        let client = Session::new_client(SessionConfig::default());
        let _a = inject_live(&client, 1, "akcdn#0", 7);
        let _b = inject_live(&client, 2, "soy#0", 7);
        let (tun, st) = one_bulk_piece(&client).await;
        {
            let mut u = st.unacked.lock().unwrap();
            for x in u.values_mut() {
                x.last_sent = Instant::now() - Duration::from_millis(100);
            }
        }
        let h0 = hedges(&client);
        client.debug_maintain();
        assert_eq!(
            hedges(&client),
            h0,
            "fresh path: 100 ms old bulk must stay put"
        );
        assert_eq!(st.unacked.lock().unwrap().len(), 1);
        drop(tun);
        client.shutdown();
    }

    /// P4: the same piece moves once the *path* is silent past the loss clock.
    #[tokio::test]
    async fn bulk_piece_on_silent_path_is_hedged_with_backoff() {
        let client = Session::new_client(SessionConfig::default());
        let (pa, _wa, _ua) = inject_live(&client, 1, "akcdn#0", 7);
        let (pb, _wb, _ub) = inject_live(&client, 2, "soy#0", 7);
        let (tun, st) = one_bulk_piece(&client).await;
        let from = st.unacked.lock().unwrap().values().next().unwrap().path_id;
        let silent = if from == 1 { &pa } else { &pb };
        // Past the loss clock (20 ms floor) but short of down_for, so this
        // is the hedge path, not path_failed's rehome.
        *silent.last_rx.lock().unwrap() = Instant::now() - Duration::from_millis(100);
        {
            let mut u = st.unacked.lock().unwrap();
            for x in u.values_mut() {
                x.last_sent = Instant::now() - Duration::from_millis(100);
            }
        }
        let h0 = hedges(&client);
        client.debug_maintain();
        assert_eq!(hedges(&client), h0 + 1, "silent path must hedge once");
        let g = st.unacked.lock().unwrap();
        let u = g.values().next().unwrap();
        assert_ne!(u.path_id, from);
        assert_eq!(u.tried.len(), 2);
        // Backoff: second rung is 2 × retry_after_bulk(from) ≥ 40 ms.
        assert!(
            u.retry_not_before >= u.last_sent + Duration::from_millis(40),
            "belt must back off: {:?}",
            u.retry_not_before.saturating_duration_since(u.last_sent)
        );
        drop(g);
        drop(tun);
        client.shutdown();
    }

    /// P4: a piece that never reached a writer queue is re-sent at once,
    /// even though its path looks fresh.
    #[tokio::test]
    async fn dropped_piece_is_resent_on_fresh_path() {
        let client = Session::new_client(SessionConfig::default());
        let _a = inject_live(&client, 1, "akcdn#0", 7);
        let _b = inject_live(&client, 2, "soy#0", 7);
        let (tun, st) = one_bulk_piece(&client).await;
        let (from, offset) = {
            let g = st.unacked.lock().unwrap();
            let (o, u) = g.iter().next().unwrap();
            (u.path_id, *o)
        };
        client.note_data_dropped(st.id, offset);
        let h0 = hedges(&client);
        let d0 = client.snapshot().data_dropped_resend;
        client.debug_maintain();
        assert_eq!(hedges(&client), h0 + 1);
        assert_eq!(client.snapshot().data_dropped_resend, d0 + 1);
        let g = st.unacked.lock().unwrap();
        let u = g.values().next().unwrap();
        assert_ne!(u.path_id, from);
        assert!(!u.dropped, "successful enqueue clears the flag");
        drop(g);
        drop(tun);
        client.shutdown();
    }

    /// P4: the belt still re-sends a piece a fresh path has silently eaten.
    #[tokio::test]
    async fn belt_resends_very_old_piece_on_fresh_path() {
        let client = Session::new_client(SessionConfig::default());
        let _a = inject_live(&client, 1, "akcdn#0", 7);
        let _b = inject_live(&client, 2, "soy#0", 7);
        let (tun, st) = one_bulk_piece(&client).await;
        {
            let mut u = st.unacked.lock().unwrap();
            for x in u.values_mut() {
                x.last_sent = Instant::now() - Duration::from_secs(6);
            }
        }
        let h0 = hedges(&client);
        client.debug_maintain();
        assert_eq!(hedges(&client), h0 + 1, "belt (≤ 5 s) must fire at 6 s");
        drop(tun);
        client.shutdown();
    }

    /// P4: while the receiver keeps acknowledging (cumulative or SACK),
    /// an old piece on a fresh path is behind a hole, not eaten.
    #[tokio::test]
    async fn belt_waits_while_acks_progress() {
        let client = Session::new_client(SessionConfig::default());
        let _a = inject_live(&client, 1, "akcdn#0", 7);
        let _b = inject_live(&client, 2, "soy#0", 7);
        let (tun, st) = one_bulk_piece(&client).await;
        {
            let mut u = st.unacked.lock().unwrap();
            for x in u.values_mut() {
                x.last_sent = Instant::now() - Duration::from_secs(6);
            }
        }
        st.last_sack_ms
            .store(crate::metrics::mono_ms().max(1), Ordering::Relaxed);
        let h0 = hedges(&client);
        client.debug_maintain();
        assert_eq!(hedges(&client), h0, "SACK progress a moment ago: no belt");
        st.last_sack_ms.store(0, Ordering::Relaxed);
        st.last_ack_ms
            .store(crate::metrics::mono_ms().max(1), Ordering::Relaxed);
        client.debug_maintain();
        assert_eq!(
            hedges(&client),
            h0,
            "cumulative progress a moment ago: no belt"
        );
        drop(tun);
        client.shutdown();
    }

    /// SACK: a range past `acked_offset` releases the pieces inside it,
    /// credits the carrying path, and leaves the hole in `unacked`.
    #[tokio::test]
    async fn sack_releases_pieces_behind_hole() {
        let client = Session::new_client(SessionConfig::default());
        let (p, _w, _u) = inject_live(&client, 1, "akcdn#0", 7);
        let mut tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let st = client.get_stream(tun.id).unwrap();
        tun.write_all(&vec![0x42u8; 40_000]).await.unwrap();
        let deadline = Instant::now() + Duration::from_millis(300);
        loop {
            if st.unacked.lock().unwrap().len() >= 3 {
                break;
            }
            assert!(Instant::now() < deadline, "40 kB must leave ≥ 3 pieces");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let pieces: Vec<(u64, u64)> = st
            .unacked
            .lock()
            .unwrap()
            .iter()
            .map(|(o, u)| (*o, *o + u.data.len() as u64))
            .collect();
        let inflight0 = p.inflight_bytes();
        let (hole, second) = (pieces[0], pieces[1]);
        client.on_ack(StreamAck {
            stream_id: st.id,
            acked_offset: 0,
            window: 1 << 20,
            sack: vec![(second.0, second.1)],
        });
        {
            let g = st.unacked.lock().unwrap();
            assert!(g.contains_key(&hole.0), "hole stays");
            assert!(!g.contains_key(&second.0), "sacked piece released");
            assert_eq!(g.len(), pieces.len() - 1);
        }
        assert_eq!(p.inflight_bytes(), inflight0 - (second.1 - second.0));
        assert_eq!(client.snapshot().data_sacked, 1);
        assert_ne!(st.last_sack_ms.load(Ordering::Relaxed), 0);
        assert_eq!(
            st.send_acked.load(Ordering::Relaxed),
            0,
            "cumulative untouched"
        );
        // A range covering only part of a piece releases nothing.
        client.on_ack(StreamAck {
            stream_id: st.id,
            acked_offset: 0,
            window: 1 << 20,
            sack: vec![(hole.0 + 1, hole.1)],
        });
        assert!(st.unacked.lock().unwrap().contains_key(&hole.0));
        drop(tun);
        client.shutdown();
    }

    /// Receiver: out-of-order pieces are advertised as SACK ranges, the
    /// newest arrival's run first; in-order data is not.
    #[tokio::test]
    async fn receiver_acks_carry_sack_ranges() {
        let client = Session::new_client(SessionConfig::default());
        let (p, _w, _u) = inject_live(&client, 1, "a#0", 7);
        let tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let st = client.get_stream(tun.id).unwrap();
        let sid = tun.id;
        for off in [2000u64, 3000, 5000] {
            client.on_data(
                p.id,
                StreamData {
                    stream_id: sid,
                    offset: off,
                    data: vec![0xab; 1000],
                },
            );
        }
        assert_eq!(st.sack_ranges(), vec![(5000, 6000), (2000, 4000)]);
        let ack = p.pending_acks.lock().unwrap().get(&sid).cloned().unwrap();
        assert_eq!(ack.acked_offset, 0);
        assert_eq!(ack.sack, vec![(5000, 6000), (2000, 4000)]);
        client.on_data(
            p.id,
            StreamData {
                stream_id: sid,
                offset: 0,
                data: vec![0xab; 2000],
            },
        );
        assert_eq!(st.recv_next.load(Ordering::Relaxed), 4000);
        assert_eq!(st.sack_ranges(), vec![(5000, 6000)]);
        drop(tun);
        client.shutdown();
    }

    fn data(sid: u32, offset: u64, len: usize) -> StreamData {
        StreamData {
            stream_id: sid,
            offset,
            data: vec![0xab; len],
        }
    }

    /// P2.1 hole measurement (observability in PR 1): a missing head with
    /// later bytes buffered opens a hole; the head landing closes it and
    /// the wait becomes the `hole_us` sample. A duplicate of a buffered
    /// piece changes nothing; a head re-inserted by a full channel neither
    /// opens nor prolongs a hole.
    #[tokio::test]
    async fn hole_sample_open_close() {
        let client = Session::new_client(SessionConfig::default());
        let (p, _w, _u) = inject_live(&client, 1, "a#0", 7);
        let tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let st = client.get_stream(tun.id).unwrap();
        let sid = tun.id;
        client.on_data(p.id, data(sid, 16 * 1024, 1000));
        assert!(
            st.hole.lock().unwrap().is_some(),
            "missing head opens a hole"
        );
        assert_eq!(st.hole_us.load(Ordering::Relaxed), 0);
        assert_eq!(st.recv_hole_max.load(Ordering::Relaxed), 1000);
        tokio::time::sleep(Duration::from_millis(60)).await;
        // duplicate of the buffered piece: no sample, hole kept.
        client.on_data(p.id, data(sid, 16 * 1024, 1000));
        assert!(st.hole.lock().unwrap().is_some());
        assert_eq!(st.hole_us.load(Ordering::Relaxed), 0);
        assert_eq!(st.dup_rx_bytes.load(Ordering::Relaxed), 1000);
        tokio::time::sleep(Duration::from_millis(90)).await;
        client.on_data(p.id, data(sid, 0, 16 * 1024));
        assert!(
            st.hole.lock().unwrap().is_none(),
            "head landed, hole closed"
        );
        let us = st.hole_us.load(Ordering::Relaxed);
        assert!(
            (140_000..400_000).contains(&us),
            "first sample seeds the EWMA with the wait: {us}"
        );
        assert_eq!(st.recv_next.load(Ordering::Relaxed), 16 * 1024 + 1000);
        assert_eq!(tun.stats().hole_us, us, "TunnelStream sees the counters");
        drop(tun);
        client.shutdown();
    }

    /// Two missing pieces: the first landing samples once and re-opens the
    /// hole at the new head; the second landing samples again and clears.
    #[tokio::test]
    async fn hole_sample_second_hole_reopens() {
        let client = Session::new_client(SessionConfig::default());
        let (p, _w, _u) = inject_live(&client, 1, "a#0", 7);
        let tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let st = client.get_stream(tun.id).unwrap();
        let sid = tun.id;
        // +16K and +48K present; 0 and +32K missing.
        client.on_data(p.id, data(sid, 16_000, 16_000));
        client.on_data(p.id, data(sid, 48_000, 16_000));
        assert_eq!(st.hole.lock().unwrap().map(|(o, _)| o), Some(0));
        tokio::time::sleep(Duration::from_millis(30)).await;
        client.on_data(p.id, data(sid, 0, 16_000));
        let s1 = st.hole_us.load(Ordering::Relaxed);
        assert!(s1 >= 25_000, "first sample {s1}");
        assert_eq!(
            st.hole.lock().unwrap().map(|(o, _)| o),
            Some(32_000),
            "re-opened at the new head"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
        client.on_data(p.id, data(sid, 32_000, 16_000));
        assert!(st.hole.lock().unwrap().is_none());
        let s2 = st.hole_us.load(Ordering::Relaxed);
        assert!(
            s2 >= 25_000 && s2 != s1,
            "second sample folded: {s1} -> {s2}"
        );
        assert_eq!(st.recv_next.load(Ordering::Relaxed), 64_000);
        drop(tun);
        client.shutdown();
    }

    /// P1.4: `in_order_held` is the contiguous run at `recv_next` the full
    /// channel could not take; `hole_bytes` is everything else buffered.
    /// A head held by a full channel is app backlog, not a hole.
    #[tokio::test]
    async fn app_backlog_counts_in_order_run() {
        let mut cfg = SessionConfig::default();
        cfg.tuning.chan = 1;
        let client = Session::new_client(cfg);
        let (p, _w, _u) = inject_live(&client, 1, "a#0", 7);
        let mut tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let st = client.get_stream(tun.id).unwrap();
        let sid = tun.id;
        for off in [0u64, 1000, 2000] {
            client.on_data(p.id, data(sid, off, 1000));
        }
        // channel of 1 took the first; 1000..3000 sit in order at the head.
        assert_eq!(st.recv_next.load(Ordering::Relaxed), 1000);
        assert_eq!(client.recv_evidence(&st), (0, 1000 + 2000));
        assert!(st.hole.lock().unwrap().is_none(), "held head is not a hole");
        client.on_data(p.id, data(sid, 5000, 1000));
        assert_eq!(client.recv_evidence(&st), (1000, 3000));
        assert_eq!(st.recv_hole_max.load(Ordering::Relaxed), 1000);
        assert_eq!(st.app_backlog_max.load(Ordering::Relaxed), 3000);
        assert!(st.hole.lock().unwrap().is_none(), "still not a hole");
        let mut buf = vec![0u8; 3000];
        tun.read_exact(&mut buf).await.unwrap();
        // Now the head (3000) is missing and 5000 is buffered: a real hole.
        client.on_data(p.id, data(sid, 6000, 1000));
        assert_eq!(st.hole.lock().unwrap().map(|(o, _)| o), Some(3000));
        drop(tun);
        client.shutdown();
    }

    /// P1.4: a zero window is attributed to the hole when out-of-order
    /// bytes dominate, to the app when in-order bytes it has not read do.
    #[tokio::test]
    async fn zero_window_cause_split() {
        let mut cfg = SessionConfig::default();
        cfg.tuning.chan = 1;
        cfg.tuning.initial_window = 4000;
        let client = Session::new_client(cfg);
        let (p, _w, _u) = inject_live(&client, 1, "a#0", 7);
        let mut tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let st = client.get_stream(tun.id).unwrap();
        let sid = tun.id;
        assert_eq!(st.recv_cap.load(Ordering::Relaxed), 4000);
        // hole: 1000..4000 buffered behind a missing head ⇒ window 0.
        for off in [1000u64, 2000, 3000] {
            client.on_data(p.id, data(sid, off, 1000));
        }
        assert_eq!(st.advertised_window(), 1000);
        client.on_data(p.id, data(sid, 4000, 1000));
        assert_eq!(st.advertised_window(), 0);
        assert_eq!(st.zero_win_hole.load(Ordering::Relaxed), 1);
        assert_eq!(st.zero_win_app.load(Ordering::Relaxed), 0);
        // head lands: channel of 1 takes 1000, 1000..5000 held in order ⇒
        // window 0 again, this time the app's.
        client.on_data(p.id, data(sid, 0, 1000));
        assert_eq!(st.advertised_window(), 0);
        assert_eq!(st.zero_win_hole.load(Ordering::Relaxed), 1);
        assert_eq!(st.zero_win_app.load(Ordering::Relaxed), 1);
        let snap = client.snapshot();
        assert_eq!((snap.zero_window_hole, snap.zero_window_app), (1, 1));
        let mut buf = vec![0u8; 5000];
        tun.read_exact(&mut buf).await.unwrap();
        drop(tun);
        client.shutdown();
    }

    /// P1.1: the counters outlive the `StreamState`. After the stream is
    /// reaped, `TunnelStream::stats()` still returns its final numbers.
    #[tokio::test]
    async fn tunnel_stream_stats_survive_reap() {
        let client = Session::new_client(SessionConfig::default());
        let (p, _w, _u) = inject_live(&client, 1, "a#0", 7);
        let tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let sid = tun.id;
        client.on_data(p.id, data(sid, 2000, 1000));
        client.on_data(p.id, data(sid, 0, 2000));
        client.on_data(p.id, data(sid, 0, 2000));
        let before = tun.stats();
        assert_eq!(before.received, 3000);
        assert_eq!(before.dup_rx_bytes, 2000);
        client.on_peer_reset(sid, ResetReason::PeerReset);
        client.debug_maintain();
        assert!(client.get_stream(sid).is_none(), "reaped");
        assert!(client.stream_stats(sid).is_none());
        let after = tun.stats();
        assert_eq!(after.received, 3000);
        assert_eq!(after.dup_rx_bytes, 2000);
        assert!(after.hole_us > 0);
        client.shutdown();
    }

    /// P1.1: `TunnelStream` holds counters, not the `StreamState` — so it
    /// does not keep `inbound_tx` alive. With the channel full and a Close
    /// dropped by `try_send`, removing the table entry drops the last
    /// `StreamState` while the `TunnelStream` is still alive and readable.
    #[tokio::test]
    async fn tunnel_stream_does_not_hold_inbound_tx() {
        let mut cfg = SessionConfig::default();
        cfg.tuning.chan = 1;
        let client = Session::new_client(cfg);
        let (p, _w, _u) = inject_live(&client, 1, "a#0", 7);
        let mut tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let sid = tun.id;
        let weak = Arc::downgrade(&client.get_stream(sid).unwrap());
        client.on_data(p.id, data(sid, 0, 1000));
        client.on_data(p.id, data(sid, 1000, 1000));
        client.on_peer_close(nya_proto::StreamClose {
            stream_id: sid,
            final_offset: Some(2000),
        });
        client.remove_held_stream(sid);
        tokio::task::yield_now().await;
        assert!(
            weak.upgrade().is_none(),
            "TunnelStream must not keep StreamState (and inbound_tx) alive"
        );
        let mut buf = vec![0u8; 1000];
        tun.read_exact(&mut buf).await.unwrap();
        assert_eq!(tun.stats().received, 1000, "counters still readable");
        client.shutdown();
    }

    /// P1.6: a stream that received observes its max advertised cap.
    #[tokio::test]
    async fn recv_cap_max_observed_at_stream_end() {
        let client = Session::new_client(SessionConfig::default());
        let (p, _w, _u) = inject_live(&client, 1, "a#0", 7);
        let tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let idle = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let st = client.get_stream(tun.id).unwrap();
        client.on_data(p.id, data(tun.id, 0, 1000));
        st.recv_cap_max.fetch_max(3 << 20, Ordering::Relaxed);
        client.on_peer_reset(tun.id, ResetReason::PeerReset);
        client.on_peer_reset(idle.id, ResetReason::PeerReset);
        let h = client.snapshot().recv_cap_max_bytes;
        assert_eq!(h.count, 1, "only the stream that received");
        assert_eq!(h.sum, 3 << 20);
        drop(tun);
        drop(idle);
        client.shutdown();
    }

    /// P1.7: the stall kind at entry — receive hole alone, send alone,
    /// send with a closed window.
    #[tokio::test]
    async fn stall_enter_kind_recorded() {
        let mut cfg = SessionConfig::default();
        cfg.tuning.loss_timeout_floor = Duration::from_millis(40);
        let client = Session::new_client(cfg);
        let (p, _w, _u) = inject_live(&client, 1, "a#0", 7);
        let mut tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let st = client.get_stream(tun.id).unwrap();
        // recv hole only.
        client.on_data(p.id, data(tun.id, 5000, 1000));
        client.debug_maintain();
        assert!(!st.stalled.load(Ordering::Relaxed), "not yet past thresh");
        tokio::time::sleep(Duration::from_millis(60)).await;
        client.debug_maintain();
        assert!(st.stalled.load(Ordering::Relaxed));
        let s = client.snapshot();
        assert_eq!(
            (
                s.stall_enter_recv_hole,
                s.stall_enter_send,
                s.stall_enter_both
            ),
            (1, 0, 0)
        );
        client.on_data(p.id, data(tun.id, 0, 5000));
        client.debug_maintain();
        assert!(
            !st.stalled.load(Ordering::Relaxed),
            "hole filled, stall left"
        );
        assert_eq!(client.snapshot().stall_ms.count, 1);
        // send only: bytes out, never ACKed.
        tun.write_all(&[1u8; 4000]).await.unwrap();
        let deadline = Instant::now() + Duration::from_millis(300);
        while st.unacked.lock().unwrap().is_empty() {
            assert!(Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        tokio::time::sleep(Duration::from_millis(60)).await;
        client.debug_maintain();
        assert!(st.stalled.load(Ordering::Relaxed));
        assert_eq!(client.snapshot().stall_enter_send, 1);
        // leave, then re-enter with the peer window closed on us. The path
        // went degraded for silence meanwhile; a fresh rx makes it
        // schedulable again.
        st.unacked.lock().unwrap().clear();
        client.debug_maintain();
        assert!(!st.stalled.load(Ordering::Relaxed));
        p.touch_rx();
        tun.write_all(&[2u8; 100]).await.unwrap();
        let deadline = Instant::now() + Duration::from_millis(300);
        while st.unacked.lock().unwrap().is_empty() {
            assert!(Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        st.send_window.store(0, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(60)).await;
        client.debug_maintain();
        assert!(st.stalled.load(Ordering::Relaxed));
        let s = client.snapshot();
        assert_eq!(
            (s.stall_enter_send, s.stall_enter_send_zero_window),
            (1, 1),
            "a send stall with window 0 is send_zero_window"
        );
        drop(tun);
        client.shutdown();
    }

    /// P1.7: every hedge/re-send path is counted by `why`; skipped ticks
    /// by `reason`. Exercised through the note helpers plus the catalog.
    #[tokio::test]
    async fn resend_why_recorded() {
        let client = Session::new_client(SessionConfig::default());
        for w in [
            "silence", "belt", "down", "gone", "dropped", "age", "allquiet",
        ] {
            client.note_resend_why(w);
        }
        client.note_resend_why("bogus");
        for r in [
            "no_fresh_alt",
            "allquiet_wait",
            "queue_full",
            "write_stalled",
            "all_tried",
            "no_alt",
        ] {
            client.note_resend_skipped(r);
        }
        let s = client.snapshot();
        assert_eq!(
            [
                s.data_resend_silence,
                s.data_resend_belt,
                s.data_resend_down,
                s.data_resend_gone,
                s.data_resend_dropped,
                s.data_resend_age,
                s.data_resend_allquiet,
            ],
            [1; 7]
        );
        assert_eq!(
            [
                s.data_resend_skipped_no_fresh_alt,
                s.data_resend_skipped_allquiet_wait,
                s.data_resend_skipped_queue_full,
                s.data_resend_skipped_write_stalled,
                s.data_resend_skipped_all_tried,
                s.data_resend_skipped_no_alt,
            ],
            [1; 6]
        );
        let names =
            crate::catalog::prometheus_metric_names(&crate::metrics::ProcessSnapshot::default());
        for n in [
            "nya_data_resend_total",
            "nya_data_resend_skipped_total",
            "nya_stall_enter_total",
            "nya_zero_window_total",
        ] {
            assert!(names.iter().any(|x| x == n), "{n} missing from catalog");
        }
        client.shutdown();
    }

    /// In-order data the full channel could not take is handed over when
    /// the app reads, not only on the next arrival.
    #[tokio::test]
    async fn app_read_drains_leftover_in_order_data() {
        let mut cfg = SessionConfig::default();
        cfg.tuning.chan = 1;
        let client = Session::new_client(cfg);
        let (p, _w, _u) = inject_live(&client, 1, "a#0", 7);
        let mut tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let st = client.get_stream(tun.id).unwrap();
        let sid = tun.id;
        for off in [0u64, 1000, 2000] {
            client.on_data(
                p.id,
                StreamData {
                    stream_id: sid,
                    offset: off,
                    data: vec![(off / 1000) as u8; 1000],
                },
            );
        }
        assert_eq!(
            st.recv_next.load(Ordering::Relaxed),
            1000,
            "channel of 1 took one"
        );
        assert_eq!(st.recv_buffered.load(Ordering::Relaxed), 2000);
        // Reading the whole 3000 bytes only completes if the tail left in
        // recv_buf is drained by the app-read hook (no more arrivals).
        let mut buf = vec![0u8; 3000];
        tokio::time::timeout(Duration::from_secs(2), tun.read_exact(&mut buf))
            .await
            .expect("tail must drain without a new arrival")
            .unwrap();
        assert_eq!(buf[0], 0);
        assert_eq!(buf[1500], 1);
        assert_eq!(buf[2999], 2);
        assert_eq!(st.recv_buffered.load(Ordering::Relaxed), 0);
        drop(tun);
        client.shutdown();
    }

    #[tokio::test]
    async fn close_retry_rehomes_first_closer() {
        let (client, server) = pair_echo(&["akcdn#0", "soy#0"]).await;
        let mut tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        tun.write_all(b"hello").await.unwrap();
        let mut buf = [0u8; 5];
        tun.read_exact(&mut buf).await.unwrap();
        let (sid, from) = {
            let g = client.inner.streams.lock().unwrap();
            let st = g.values().next().unwrap();
            (st.id, st.sticky.load(Ordering::Relaxed))
        };
        client.remember_close(sid, from, false, None);
        if let Some(p) = client.get_path(from) {
            *p.last_rx.lock().unwrap() = Instant::now() - Duration::from_millis(400);
        }
        {
            let mut g = client.inner.closes.lock().unwrap();
            for c in g.values_mut() {
                c.sent_at = Instant::now() - Duration::from_millis(400);
            }
        }
        let r0 = client.snapshot().close_retry;
        client.debug_maintain();
        assert!(
            client.snapshot().close_retry > r0,
            "first closer must rehome Close"
        );
        drop(tun);
        client.shutdown();
        server.shutdown();
    }

    #[tokio::test]
    async fn linger_without_stream_empties_closes() {
        let mut cfg = SessionConfig::default();
        cfg.tuning.close_linger = Duration::from_millis(20);
        let client = Session::new_client(cfg);
        inject_named(&client, 1, "akcdn#0", 7);
        client.remember_close(7, 1, true, None);
        {
            let mut g = client.inner.closes.lock().unwrap();
            for c in g.values_mut() {
                c.started_at = Instant::now() - Duration::from_millis(50);
            }
        }
        client.debug_maintain();
        assert!(
            client.inner.closes.lock().unwrap().is_empty(),
            "reap_closes must not need streams"
        );
        client.shutdown();
    }

    #[tokio::test]
    async fn path_down_rehomes_unacked_immediately() {
        let client = Session::new_client(SessionConfig::default());
        inject_named(&client, 1, "akcdn#0", 7);
        inject_named(&client, 2, "soy#0", 7);
        let mut tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        tun.write_all(b"hello").await.unwrap();
        let st = {
            let g = client.inner.streams.lock().unwrap();
            g.values().next().unwrap().clone()
        };
        let deadline = Instant::now() + Duration::from_millis(200);
        loop {
            if !st.unacked.lock().unwrap().is_empty() {
                break;
            }
            assert!(Instant::now() < deadline, "write must leave unacked");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let from = st.unacked.lock().unwrap().values().next().unwrap().path_id;
        client.path_failed(from);
        let to = st.unacked.lock().unwrap().values().next().unwrap().path_id;
        assert_ne!(to, from);
        assert!(client.get_path(from).is_none());
        drop(tun);
        client.shutdown();
    }

    #[tokio::test]
    async fn duplicate_open_dials_origin_once() {
        let (server, mut incoming) = Session::new_server(SessionConfig::default());
        let open = StreamOpen {
            stream_id: 7,
            target: Target {
                host: "t".into(),
                port: 1,
            },
        };
        server.accept_remote_stream(1, open.clone());
        server.accept_remote_stream(2, open);
        let first = incoming.try_recv().expect("one incoming");
        assert_eq!(first.stream_id, 7);
        assert!(incoming.try_recv().is_err());
        assert_eq!(server.inner.streams.lock().unwrap().len(), 1);
        server.shutdown();
    }

    #[tokio::test]
    async fn early_data_delivered_after_open() {
        let (server, mut incoming) = Session::new_server(SessionConfig::default());
        inject_named(&server, 1, "akcdn#0", 7);
        server.handle_frame(
            1,
            Frame::StreamData(StreamData {
                stream_id: 9,
                offset: 0,
                data: b"hi!".to_vec(),
            }),
        );
        assert!(server.inner.streams.lock().unwrap().is_empty());
        server.accept_remote_stream(
            1,
            StreamOpen {
                stream_id: 9,
                target: Target {
                    host: "t".into(),
                    port: 1,
                },
            },
        );
        let mut inc = incoming.try_recv().expect("incoming");
        let mut buf = [0u8; 3];
        tokio::time::timeout(Duration::from_secs(1), inc.io.read_exact(&mut buf))
            .await
            .expect("early data")
            .unwrap();
        assert_eq!(&buf, b"hi!");
        server.shutdown();
    }

    #[tokio::test]
    async fn close_with_offset_delivers_data_that_arrives_after() {
        let (server, mut incoming) = Session::new_server(SessionConfig::default());
        inject_named(&server, 1, "akcdn#0", 7);
        server.accept_remote_stream(
            1,
            StreamOpen {
                stream_id: 9,
                target: Target {
                    host: "t".into(),
                    port: 1,
                },
            },
        );
        let mut inc = incoming.try_recv().expect("incoming");
        server.handle_frame(
            1,
            Frame::StreamClose(StreamClose {
                stream_id: 9,
                final_offset: Some(4),
            }),
        );
        let st = server.get_stream(9).expect("held until offset");
        assert!(
            !st.recv_fin.load(Ordering::Relaxed),
            "Close must not FIN while recv_next < final_offset"
        );
        server.handle_frame(
            1,
            Frame::StreamData(StreamData {
                stream_id: 9,
                offset: 0,
                data: b"abcd".to_vec(),
            }),
        );
        let mut buf = [0u8; 4];
        tokio::time::timeout(Duration::from_secs(1), inc.io.read_exact(&mut buf))
            .await
            .expect("late data after Close")
            .unwrap();
        assert_eq!(&buf, b"abcd");
        let n = tokio::time::timeout(Duration::from_secs(1), inc.io.read(&mut buf))
            .await
            .expect("eof after reassembly")
            .unwrap();
        assert_eq!(n, 0, "FIN after recv_next reaches final_offset");
        server.shutdown();
    }

    #[tokio::test]
    async fn shutdown_session_close_ends_peer_without_all_down_wait() {
        let (client, server) = pair_echo(&["p1"]).await;
        assert!(!server.is_dead());
        client.shutdown();
        let deadline = Instant::now() + Duration::from_millis(400);
        while !server.is_dead() {
            assert!(
                Instant::now() < deadline,
                "peer must die on SessionClose, not all_down_timeout"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        server.shutdown();
    }

    #[tokio::test]
    async fn failover_keeps_stream() {
        let (client, server, incoming) = pair();
        tokio::spawn(echo_server(incoming));
        for name in ["a", "b"] {
            let (ca, sa) = duplex(64 * 1024);
            let c = client.clone();
            let s = server.clone();
            let n1 = name.to_string();
            let n2 = name.to_string();
            tokio::spawn(async move { c.add_path(n1, ca).await });
            tokio::spawn(async move { s.add_path(n2, sa).await });
        }
        client.wait_ready(Duration::from_secs(2)).await.unwrap();
        assert!(client.debug_path_names().len() >= 2);

        let mut tun = client
            .open_stream(Target {
                host: "echo".into(),
                port: 1,
            })
            .await
            .unwrap();
        tun.write_all(b"one").await.unwrap();
        let mut buf = [0u8; 3];
        tun.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"one");

        let names = client.debug_path_names();
        client.debug_drop_path(&names[0]);
        tokio::time::sleep(Duration::from_millis(200)).await;

        tun.write_all(b"two").await.unwrap();
        let mut buf = [0u8; 3];
        tokio::time::timeout(Duration::from_secs(2), tun.read_exact(&mut buf))
            .await
            .expect("failover read timed out")
            .unwrap();
        assert_eq!(&buf, b"two");
        client.shutdown();
        server.shutdown();
    }

    #[tokio::test]
    async fn dropping_one_conn_does_not_yank_other_streams() {
        let (client, server, incoming) = pair();
        tokio::spawn(echo_server(incoming));
        for name in ["a0", "a1", "b"] {
            let (ca, sa) = duplex(64 * 1024);
            let c = client.clone();
            let s = server.clone();
            let n1 = name.to_string();
            let n2 = name.to_string();
            tokio::spawn(async move { c.add_path(n1, ca).await });
            tokio::spawn(async move { s.add_path(n2, sa).await });
        }
        client.wait_paths(3, Duration::from_secs(2)).await.unwrap();

        let mut tun = client
            .open_stream(Target {
                host: "echo".into(),
                port: 1,
            })
            .await
            .unwrap();
        tun.write_all(b"pin").await.unwrap();
        let mut buf = [0u8; 3];
        tun.read_exact(&mut buf).await.unwrap();

        let sticky = {
            let streams = client.inner.streams.lock().unwrap();
            streams
                .values()
                .next()
                .unwrap()
                .sticky
                .load(Ordering::Relaxed)
        };
        let other = client
            .path_list()
            .into_iter()
            .find(|p| p.id != sticky)
            .map(|p| p.name.clone())
            .unwrap();
        client.debug_drop_path(&other);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let still = {
            let streams = client.inner.streams.lock().unwrap();
            streams
                .values()
                .next()
                .unwrap()
                .sticky
                .load(Ordering::Relaxed)
        };
        assert_eq!(still, sticky, "unrelated conn death must not restick");
        tun.write_all(b"ok!").await.unwrap();
        let mut buf = [0u8; 3];
        tokio::time::timeout(Duration::from_secs(2), tun.read_exact(&mut buf))
            .await
            .expect("read after unrelated drop")
            .unwrap();
        assert_eq!(&buf, b"ok!");
        client.shutdown();
        server.shutdown();
    }

    #[tokio::test]
    async fn degraded_migrates_to_sibling_without_path_down() {
        let (client, server, incoming) = pair();
        tokio::spawn(echo_server(incoming));
        for name in ["a#0", "a#1"] {
            let (ca, sa) = duplex(64 * 1024);
            let c = client.clone();
            let s = server.clone();
            let n1 = name.to_string();
            let n2 = name.to_string();
            tokio::spawn(async move { c.add_path(n1, ca).await });
            tokio::spawn(async move { s.add_path(n2, sa).await });
        }
        client.wait_paths(2, Duration::from_secs(2)).await.unwrap();
        let mut tun = client
            .open_stream(Target {
                host: "echo".into(),
                port: 1,
            })
            .await
            .unwrap();
        tun.write_all(b"hi!").await.unwrap();
        let mut buf = [0u8; 3];
        tun.read_exact(&mut buf).await.unwrap();

        let sticky = {
            let streams = client.inner.streams.lock().unwrap();
            streams
                .values()
                .next()
                .unwrap()
                .sticky
                .load(Ordering::Relaxed)
        };
        let name = client
            .path_list()
            .into_iter()
            .find(|p| p.id == sticky)
            .unwrap()
            .name
            .clone();
        let down0 = client.snapshot().path_down;
        client.debug_mark_degraded(&name);
        if let Some(p) = client.path_list().into_iter().find(|p| p.id == sticky) {
            p.set_congested(true);
        }
        client.debug_maintain();
        assert_eq!(client.snapshot().path_down, down0);
        tun.write_all(b"ok!").await.unwrap();
        let deadline = Instant::now() + Duration::from_millis(200);
        let still = loop {
            let s = {
                let streams = client.inner.streams.lock().unwrap();
                streams
                    .values()
                    .next()
                    .unwrap()
                    .sticky
                    .load(Ordering::Relaxed)
            };
            if s != sticky || Instant::now() >= deadline {
                break s;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        assert_ne!(still, sticky, "next send must leave the degraded path");
        tokio::time::timeout(Duration::from_secs(2), tun.read_exact(&mut buf))
            .await
            .expect("read after degrade migrate")
            .unwrap();
        client.shutdown();
        server.shutdown();
    }

    #[tokio::test]
    async fn degraded_migrates_to_slower_backup_without_path_down() {
        let (client, server, incoming) = pair();
        tokio::spawn(echo_server(incoming));
        for name in ["a", "b"] {
            let (ca, sa) = duplex(64 * 1024);
            let c = client.clone();
            let s = server.clone();
            let n1 = name.to_string();
            let n2 = name.to_string();
            tokio::spawn(async move { c.add_path(n1, ca).await });
            tokio::spawn(async move { s.add_path(n2, sa).await });
        }
        client.wait_paths(2, Duration::from_secs(2)).await.unwrap();
        // Force distinct RTTs so a is preferred then degraded onto b.
        let paths = client.path_list();
        paths[0].rtt_ewma_us.store(12_000, Ordering::Relaxed);
        paths[1].rtt_ewma_us.store(60_000, Ordering::Relaxed);

        let mut tun = client
            .open_stream(Target {
                host: "echo".into(),
                port: 1,
            })
            .await
            .unwrap();
        tun.write_all(b"hi!").await.unwrap();
        let mut buf = [0u8; 3];
        tun.read_exact(&mut buf).await.unwrap();

        let sticky = {
            let streams = client.inner.streams.lock().unwrap();
            streams
                .values()
                .next()
                .unwrap()
                .sticky
                .load(Ordering::Relaxed)
        };
        let name = client
            .path_list()
            .into_iter()
            .find(|p| p.id == sticky)
            .unwrap()
            .name
            .clone();
        let down0 = client.snapshot().path_down;
        client.debug_mark_degraded(&name);
        tokio::time::sleep(Duration::from_millis(20)).await;
        let still = {
            let streams = client.inner.streams.lock().unwrap();
            streams
                .values()
                .next()
                .unwrap()
                .sticky
                .load(Ordering::Relaxed)
        };
        assert_eq!(
            still, sticky,
            "DEGRADED last in-class peer must not dump onto the slower backup"
        );
        assert_eq!(client.snapshot().path_down, down0);
        client.shutdown();
        server.shutdown();
    }

    #[tokio::test]
    async fn degrade_migrate_increments_migrates_not_path_down() {
        let (client, server, incoming) = pair();
        tokio::spawn(echo_server(incoming));
        for name in ["a#0", "a#1"] {
            let (ca, sa) = duplex(64 * 1024);
            let c = client.clone();
            let s = server.clone();
            let n1 = name.to_string();
            let n2 = name.to_string();
            tokio::spawn(async move { c.add_path(n1, ca).await });
            tokio::spawn(async move { s.add_path(n2, sa).await });
        }
        client.wait_paths(2, Duration::from_secs(2)).await.unwrap();
        let mut tun = client
            .open_stream(Target {
                host: "echo".into(),
                port: 1,
            })
            .await
            .unwrap();
        tun.write_all(b"hi!").await.unwrap();
        let mut buf = [0u8; 3];
        tun.read_exact(&mut buf).await.unwrap();
        let sticky = {
            let streams = client.inner.streams.lock().unwrap();
            streams
                .values()
                .next()
                .unwrap()
                .sticky
                .load(Ordering::Relaxed)
        };
        let name = client
            .path_list()
            .into_iter()
            .find(|p| p.id == sticky)
            .unwrap()
            .name
            .clone();
        let snap0 = client.snapshot();
        client.debug_mark_degraded(&name);
        if let Some(p) = client.path_list().into_iter().find(|p| p.id == sticky) {
            p.set_congested(true);
        }
        client.debug_maintain();
        tun.write_all(b"more").await.unwrap();
        let snap1 = client.snapshot();
        assert_eq!(snap1.path_down, snap0.path_down);
        assert!(!snap1.links.is_empty(), "snapshot must roll up links");
        client.shutdown();
        server.shutdown();
    }

    #[tokio::test]
    async fn stall_observes_frozen_origin_not_zero_after_ack() {
        let (client, server, incoming) = pair();
        tokio::spawn(echo_server(incoming));
        let (a, b) = duplex(64 * 1024);
        let c = client.clone();
        let s = server.clone();
        tokio::spawn(async move { c.add_path("p1".into(), a).await });
        tokio::spawn(async move { s.add_path("p1".into(), b).await });
        client.wait_ready(Duration::from_secs(2)).await.unwrap();
        let _tun = client
            .open_stream(Target {
                host: "echo".into(),
                port: 1,
            })
            .await
            .unwrap();
        let st = {
            let streams = client.inner.streams.lock().unwrap();
            streams.values().next().unwrap().clone()
        };
        let path_id = st.sticky.load(Ordering::Relaxed);
        st.last_ack_ms.store(1, Ordering::Relaxed);
        while crate::metrics::mono_ms() < 300 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        {
            let mut u = st.unacked.lock().unwrap();
            u.insert(
                0,
                Unacked {
                    data: vec![1, 2, 3],
                    path_id,
                    last_sent: Instant::now(),
                    tried: vec![path_id],
                    retry_not_before: Instant::now(),
                    dropped: false,
                    delivered_at_send: 0,
                    delivered_time_at_send_us: 0,
                    sent_us: 0,
                    first_tx_at_send_us: 0,
                },
            );
        }
        client.debug_maintain();
        assert!(
            st.stalled.load(Ordering::Relaxed),
            "stale unacked must stall"
        );
        st.unacked.lock().unwrap().clear();
        st.last_ack_ms
            .store(crate::metrics::mono_ms().max(1), Ordering::Relaxed);
        client.debug_maintain();
        assert!(!st.stalled.load(Ordering::Relaxed));
        let snap = client.snapshot();
        assert!(snap.stall_ms.count >= 1, "recovery must observe stall_ms");
        assert!(
            snap.stall_ms.sum >= 20,
            "observed stall must be >= threshold, not ~0; sum={}",
            snap.stall_ms.sum
        );
        client.shutdown();
        server.shutdown();
    }

    #[tokio::test]
    async fn idle_then_first_send_does_not_stall() {
        let (client, server, incoming) = pair();
        tokio::spawn(echo_server(incoming));
        let (a, b) = duplex(64 * 1024);
        let c = client.clone();
        let s = server.clone();
        tokio::spawn(async move { c.add_path("p1".into(), a).await });
        tokio::spawn(async move { s.add_path("p1".into(), b).await });
        client.wait_ready(Duration::from_secs(2)).await.unwrap();
        let _tun = client
            .open_stream(Target {
                host: "echo".into(),
                port: 1,
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let st = {
            let streams = client.inner.streams.lock().unwrap();
            streams.values().next().unwrap().clone()
        };
        let path_id = st.sticky.load(Ordering::Relaxed);
        {
            let mut u = st.unacked.lock().unwrap();
            u.insert(
                0,
                Unacked {
                    data: vec![9],
                    path_id,
                    last_sent: Instant::now(),
                    tried: vec![path_id],
                    retry_not_before: Instant::now(),
                    dropped: false,
                    delivered_at_send: 0,
                    delivered_time_at_send_us: 0,
                    sent_us: 0,
                    first_tx_at_send_us: 0,
                },
            );
        }
        client.debug_maintain();
        assert!(
            !st.stalled.load(Ordering::Relaxed),
            "fresh last_sent must not stall"
        );
        client.shutdown();
        server.shutdown();
    }

    #[tokio::test]
    async fn in_order_parked_recv_is_not_stall() {
        let (client, server, incoming) = pair();
        tokio::spawn(echo_server(incoming));
        let (a, b) = duplex(64 * 1024);
        let c = client.clone();
        let s = server.clone();
        tokio::spawn(async move { c.add_path("p1".into(), a).await });
        tokio::spawn(async move { s.add_path("p1".into(), b).await });
        client.wait_ready(Duration::from_secs(2)).await.unwrap();
        let _tun = client
            .open_stream(Target {
                host: "echo".into(),
                port: 1,
            })
            .await
            .unwrap();
        let st = {
            let streams = client.inner.streams.lock().unwrap();
            streams.values().next().unwrap().clone()
        };
        st.recv_buf.lock().unwrap().insert(0, vec![1, 2, 3]);
        st.last_recv_ms.store(1, Ordering::Relaxed);
        client.debug_maintain();
        assert!(
            !st.stalled.load(Ordering::Relaxed),
            "in-order parked (slow consumer) is not a hole"
        );
        client.shutdown();
        server.shutdown();
    }

    #[tokio::test]
    async fn open_without_path_does_not_count_opened() {
        let cfg = SessionConfig {
            all_down_timeout: Duration::from_millis(20),
            ..Default::default()
        };
        let client = Session::new_client(cfg);
        let err = client
            .open_stream(Target {
                host: "x".into(),
                port: 1,
            })
            .await;
        assert!(matches!(
            err,
            Err(SessionError::NoPath) | Err(SessionError::Dead)
        ));
        assert_eq!(client.snapshot().streams_opened, 0);
        client.shutdown();
    }

    #[tokio::test]
    async fn graceful_close_reaps_stream_table() {
        let (client, server) = pair_echo(&["a", "b"]).await;

        const N: usize = 20;
        for _ in 0..N {
            let mut tun = client
                .open_stream(Target {
                    host: "echo".into(),
                    port: 1,
                })
                .await
                .unwrap();
            tun.write_all(b"ping").await.unwrap();
            let mut buf = [0u8; 4];
            tun.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"ping");
            drop(tun);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;

        let client_held = client.inner.streams.lock().unwrap().len();
        let server_held = server.inner.streams.lock().unwrap().len();
        let snap = client.snapshot();
        assert_eq!(
            client_held, 0,
            "client must reap graceful closes, held={client_held} opened={} closed={}",
            snap.streams_opened, snap.streams_closed
        );
        assert_eq!(
            server_held, 0,
            "server must reap graceful closes, held={server_held}"
        );
        assert_eq!(snap.streams_held, 0);
        assert_eq!(snap.streams_live, 0);

        let mig0 = snap.migrates;
        let names = client.debug_path_names();
        client.debug_drop_path(&names[0]);
        tokio::time::sleep(Duration::from_millis(80)).await;
        let mig1 = client.snapshot().migrates;
        assert_eq!(
            mig1, mig0,
            "closed streams must not migrate on path down ({mig0} -> {mig1})"
        );

        client.shutdown();
        server.shutdown();
    }

    #[tokio::test]
    async fn concurrent_open_close_reaps_stream_table() {
        let (client, server) = pair_echo(&["a", "b"]).await;

        const N: usize = 16;
        let mut joins = Vec::new();
        for _ in 0..N {
            let client = client.clone();
            joins.push(tokio::spawn(async move {
                let mut tun = client
                    .open_stream(Target {
                        host: "echo".into(),
                        port: 1,
                    })
                    .await
                    .unwrap();
                tun.write_all(b"ping").await.unwrap();
                let mut buf = [0u8; 4];
                tun.read_exact(&mut buf).await.unwrap();
            }));
        }
        for j in joins {
            j.await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(
            client.inner.streams.lock().unwrap().len(),
            0,
            "client concurrent churn leak"
        );
        assert_eq!(
            server.inner.streams.lock().unwrap().len(),
            0,
            "server concurrent churn leak"
        );
        client.shutdown();
        server.shutdown();
    }

    #[tokio::test]
    async fn half_close_linger_reaps_stream_table() {
        let mut cfg = SessionConfig::default();
        cfg.tuning.loss_timeout_floor = Duration::from_millis(150);
        cfg.tuning.close_linger = Duration::from_millis(80);
        cfg.all_down_timeout = Duration::from_secs(2);
        let client = Session::new_client(cfg.clone());
        let (server, incoming) = Session::new_server(cfg);
        tokio::spawn(async move {
            let mut held = Vec::new();
            let mut incoming = incoming;
            while let Some(inc) = incoming.recv().await {
                held.push(inc);
            }
            let _ = held;
        });
        let (a, b) = duplex(64 * 1024);
        let c = client.clone();
        let s = server.clone();
        tokio::spawn(async move { c.add_path("p1".into(), a).await });
        tokio::spawn(async move { s.add_path("p1".into(), b).await });
        client.wait_ready(Duration::from_secs(2)).await.unwrap();

        let mut tun = client
            .open_stream(Target {
                host: "echo".into(),
                port: 1,
            })
            .await
            .unwrap();
        tun.write_all(b"hi").await.unwrap();
        drop(tun);

        tokio::time::sleep(Duration::from_millis(250)).await;
        let client_held = client.inner.streams.lock().unwrap().len();
        let server_held = server.inner.streams.lock().unwrap().len();
        assert_eq!(
            client_held, 0,
            "half-closed client stream must linger-reap, held={client_held}"
        );
        assert_eq!(
            server_held, 0,
            "half-closed server stream must linger-reap, held={server_held}"
        );
        client.shutdown();
        server.shutdown();
    }

    #[tokio::test]
    async fn close_retry_continues_while_path_pongs() {
        let mut cfg = SessionConfig::default();
        cfg.tuning.close_linger = Duration::from_millis(400);
        cfg.tuning.loss_timeout_floor = Duration::from_millis(20);
        let client = Session::new_client(cfg);
        let (p1, _w1, _u1) = inject_live(&client, 1, "a#0", 7);
        let _p2 = inject_live(&client, 2, "b#0", 7);
        let tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        drop(tun);
        tokio::time::sleep(Duration::from_millis(20)).await;
        *p1.last_rx.lock().unwrap() = Instant::now();
        let before = client.snapshot().close_retry;
        for _ in 0..8 {
            tokio::time::sleep(Duration::from_millis(25)).await;
            *p1.last_rx.lock().unwrap() = Instant::now();
            client.debug_maintain();
        }
        assert!(
            client.snapshot().close_retry > before,
            "Pong/last_rx must not cancel first-closer Close retry"
        );
        assert!(
            client.inner.closes.lock().unwrap().contains_key(
                &client
                    .inner
                    .streams
                    .lock()
                    .unwrap()
                    .keys()
                    .next()
                    .copied()
                    .or_else(|| client.inner.closes.lock().unwrap().keys().next().copied())
                    .unwrap_or(1)
            ) || !client.inner.closes.lock().unwrap().is_empty(),
            "Close table must still hold the id until recv_fin or linger"
        );
        client.shutdown();
    }

    #[tokio::test]
    async fn reset_retry_rehomes_when_enqueue_fails() {
        let client = Session::new_client(SessionConfig::default());
        let (p1, _w1, _u1) = inject_live(&client, 1, "a#0", 7);
        let _p2 = inject_live(&client, 2, "b#0", 7);
        let tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let id = tun.id;
        let pref = client.pick_pref(PickPref::Any).expect("dest");
        let stuffed = client.get_path(pref).expect("path");
        stuff_urgent_keep_schedulable(&stuffed);
        assert!(stuffed.is_schedulable());
        let _ = p1;
        let before = client.snapshot().reset_retry;
        client.finish_stream(id, Some(ResetReason::Timeout), true);
        assert!(
            client.snapshot().reset_retry > before,
            "failed first Reset enqueue must rehome and count reset_retry"
        );
        client.shutdown();
    }

    #[tokio::test]
    async fn reset_retry_remembers_when_pick_pref_none() {
        let mut cfg = SessionConfig::default();
        cfg.tuning.close_linger = Duration::from_millis(400);
        let client = Session::new_client(cfg);
        let (_p, _w, _u) = inject_live(&client, 1, "a#0", 7);
        let tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let id = tun.id;
        let pid = *client.inner.paths.lock().unwrap().keys().next().unwrap();
        client.path_failed(pid);
        client.finish_stream(id, Some(ResetReason::Timeout), true);
        assert!(
            client.inner.resets.lock().unwrap().contains_key(&id),
            "must remember Reset even with no dest"
        );
        let _p2 = inject_live(&client, 2, "b#0", 7);
        tokio::time::sleep(Duration::from_millis(25)).await;
        client.debug_maintain();
        client.shutdown();
    }

    #[tokio::test]
    async fn reset_retry_stops_at_close_linger() {
        let mut cfg = SessionConfig::default();
        cfg.tuning.close_linger = Duration::from_millis(30);
        let client = Session::new_client(cfg);
        client.remember_reset(9, 0, ResetReason::Timeout);
        tokio::time::sleep(Duration::from_millis(50)).await;
        client.debug_maintain();
        assert!(
            client.inner.resets.lock().unwrap().is_empty(),
            "reap_resets must drop after close_linger"
        );
        client.shutdown();
    }

    #[tokio::test]
    async fn on_peer_reset_forgets_reset_table() {
        let client = Session::new_client(SessionConfig::default());
        let _p = inject_live(&client, 1, "a#0", 7);
        let tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let id = tun.id;
        client.remember_reset(id, 1, ResetReason::Timeout);
        client.on_peer_reset(id, ResetReason::PeerReset);
        assert!(
            client.inner.resets.lock().unwrap().is_empty(),
            "peer Reset must forget our Reset table"
        );
        client.shutdown();
    }

    #[tokio::test]
    async fn second_closer_does_not_retry_until_linger() {
        let (client, server) = pair_echo(&["a", "b"]).await;
        let mut tun = client
            .open_stream(Target {
                host: "echo".into(),
                port: 1,
            })
            .await
            .unwrap();
        tun.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        tun.read_exact(&mut buf).await.unwrap();
        drop(tun);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            client.snapshot().close_retry < 10,
            "second closer must not Close-storm; close_retry={}",
            client.snapshot().close_retry
        );
        assert!(client.inner.closes.lock().unwrap().is_empty());
        client.shutdown();
        server.shutdown();
    }

    #[tokio::test]
    async fn in_flight_copy_not_reaped_before_fin() {
        let mut cfg = SessionConfig::default();
        cfg.tuning.close_linger = Duration::from_millis(40);
        let (client, server) = pair_echo_cfg(&["a"], cfg).await;
        let mut tun = client
            .open_stream(Target {
                host: "echo".into(),
                port: 1,
            })
            .await
            .unwrap();
        tun.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        tun.read_exact(&mut buf).await.unwrap();
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert_eq!(
            client.inner.streams.lock().unwrap().len(),
            1,
            "in-flight must survive 2× linger"
        );
        assert_eq!(server.inner.streams.lock().unwrap().len(), 1);
        assert_eq!(client.snapshot().stream_reaps_linger, 0);
        drop(tun);
        client.shutdown();
        server.shutdown();
    }

    #[tokio::test]
    async fn server_leftover_close_swallowed_reset_retried() {
        let mut cfg = SessionConfig::default();
        cfg.tuning.close_linger = Duration::from_millis(80);
        cfg.tuning.loss_timeout_floor = Duration::from_millis(20);
        let client = Session::new_client(cfg.clone());
        let (server, incoming) = Session::new_server(cfg);
        tokio::spawn(async move {
            let mut held = Vec::new();
            let mut incoming = incoming;
            while let Some(inc) = incoming.recv().await {
                held.push(inc);
            }
            let _ = held;
        });
        let (p1, mut cw1, mut cu1) = inject_live(&client, 1, "a#0", 7);
        let (_p2, mut cw2, mut cu2) = inject_live(&client, 2, "b#0", 7);
        let _s1 = inject_live(&server, 1, "a#0", 7);
        let _s2 = inject_live(&server, 2, "b#0", 7);

        fn take(w: &mut mpsc::Receiver<Frame>, u: &mut mpsc::Receiver<Frame>) -> Option<Frame> {
            u.try_recv().ok().or_else(|| w.try_recv().ok())
        }

        let mut tun = client
            .open_stream(Target {
                host: "echo".into(),
                port: 1,
            })
            .await
            .unwrap();
        let mut open = None;
        for _ in 0..32 {
            if let Some(Frame::StreamOpen(o)) = take(&mut cw1, &mut cu1) {
                open = Some(o);
                break;
            }
            if let Some(Frame::StreamOpen(o)) = take(&mut cw2, &mut cu2) {
                open = Some(o);
                break;
            }
            tokio::task::yield_now().await;
        }
        let open = open.expect("client sent StreamOpen");
        server.handle_frame(1, Frame::StreamOpen(open.clone()));
        tun.write_all(b"hi").await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        while take(&mut cw1, &mut cu1).is_some() {}
        while take(&mut cw2, &mut cu2).is_some() {}
        drop(tun);
        tokio::time::sleep(Duration::from_millis(15)).await;
        // Swallow Close on every path; do not feed the server.
        loop {
            let f = take(&mut cw1, &mut cu1).or_else(|| take(&mut cw2, &mut cu2));
            match f {
                Some(Frame::StreamClose(_)) => {}
                Some(_) => {}
                None => break,
            }
        }
        *p1.last_rx.lock().unwrap() = Instant::now();
        stuff_urgent_keep_schedulable(&p1);
        let deadline = Instant::now() + Duration::from_millis(250);
        while Instant::now() < deadline {
            client.debug_maintain();
            server.debug_maintain();
            while let Some(f) = take(&mut cw1, &mut cu1).or_else(|| take(&mut cw2, &mut cu2)) {
                if matches!(f, Frame::StreamReset(_)) {
                    server.handle_frame(2, f);
                }
            }
            if server.inner.streams.lock().unwrap().is_empty() && client.snapshot().reset_retry >= 1
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            server.inner.streams.lock().unwrap().len(),
            0,
            "server leftover must reap after Reset rehome"
        );
        assert!(
            client.snapshot().reset_retry >= 1,
            "Yuusei miss is Reset rehome, not Close retry alone"
        );
        client.shutdown();
        server.shutdown();
    }

    fn age_close_sent(client: &Session) {
        let mut g = client.inner.closes.lock().unwrap();
        for c in g.values_mut() {
            c.sent_at = Instant::now() - Duration::from_millis(400);
        }
    }

    fn drain_frames(w: &mut mpsc::Receiver<Frame>, u: &mut mpsc::Receiver<Frame>) -> Vec<Frame> {
        let mut out = Vec::new();
        while let Ok(f) = u.try_recv() {
            out.push(f);
        }
        while let Ok(f) = w.try_recv() {
            out.push(f);
        }
        out
    }

    #[tokio::test]
    async fn close_retry_stops_when_stream_gone() {
        let client = Session::new_client(SessionConfig::default());
        let _p = inject_live(&client, 1, "a#0", 7);
        client.remember_close(7, 1, false, None);
        age_close_sent(&client);
        let r0 = client.snapshot().close_retry;
        client.debug_maintain();
        assert!(
            client.inner.closes.lock().unwrap().is_empty(),
            "HashMap-gone must forget Close"
        );
        assert_eq!(
            client.snapshot().close_retry,
            r0,
            "must not send Close for a missing stream"
        );
        client.shutdown();
    }

    #[tokio::test]
    async fn close_retry_does_not_cycle_tried_paths() {
        let client = Session::new_client(SessionConfig::default());
        let _p1 = inject_live(&client, 1, "a#0", 7);
        let _p2 = inject_live(&client, 2, "b#0", 7);
        let tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let id = tun.id;
        client.remember_close(id, 1, false, None);
        age_close_sent(&client);
        client.debug_maintain();
        let after_first = client.snapshot().close_retry;
        assert!(after_first >= 1, "first timer rehome must count");
        age_close_sent(&client);
        client.debug_maintain();
        age_close_sent(&client);
        let r = client.snapshot().close_retry;
        client.debug_maintain();
        assert_eq!(
            client.snapshot().close_retry,
            r,
            "must not cycle tried live paths"
        );
        assert!(
            client.inner.closes.lock().unwrap().contains_key(&id),
            "table stays until linger"
        );
        drop(tun);
        client.shutdown();
    }

    #[tokio::test]
    async fn close_retry_first_closer_six_path_capped() {
        let client = Session::new_client(SessionConfig::default());
        for i in 1..=6 {
            let _ = inject_live(&client, i, &format!("l{i}#0"), 7);
        }
        let tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let id = tun.id;
        client.remember_close(id, 1, false, None);
        for _ in 0..12 {
            age_close_sent(&client);
            client.debug_maintain();
        }
        assert!(
            client.snapshot().close_retry <= 6,
            "close_retry={} must be <= path count",
            client.snapshot().close_retry
        );
        let r = client.snapshot().close_retry;
        age_close_sent(&client);
        client.debug_maintain();
        assert_eq!(
            client.snapshot().close_retry,
            r,
            "further maintains must not spray"
        );
        drop(tun);
        client.shutdown();
    }

    #[tokio::test]
    async fn close_retry_completed_short_stream_capped() {
        let (client, server) =
            pair_echo_cfg(&["a", "b", "c", "d", "e", "f"], SessionConfig::default()).await;
        let mut tun = client
            .open_stream(Target {
                host: "echo".into(),
                port: 1,
            })
            .await
            .unwrap();
        tun.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        tun.read_exact(&mut buf).await.unwrap();
        drop(tun);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            client.snapshot().close_retry <= 6,
            "control: graceful echo close_retry={}",
            client.snapshot().close_retry
        );
        client.shutdown();
        server.shutdown();
    }

    #[tokio::test]
    async fn close_retry_enqueue_fail_does_not_burn_dest() {
        let client = Session::new_client(SessionConfig::default());
        let (p1, w1, u1) = inject_live(&client, 1, "a#0", 7);
        let (p2, mut w2, mut u2) = inject_live(&client, 2, "b#0", 7);
        let tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let id = tun.id;
        client.remember_close(id, 1, false, None);
        stuff_urgent_keep_schedulable(&p1);
        stuff_urgent_keep_schedulable(&p2);
        age_close_sent(&client);
        let r0 = client.snapshot().close_retry;
        client.debug_maintain();
        assert_eq!(
            client.snapshot().close_retry,
            r0,
            "full urgent must not count"
        );
        {
            let g = client.inner.closes.lock().unwrap();
            let c = g.get(&id).expect("close table");
            assert!(
                !c.tried.contains(&2) || c.tried == vec![1],
                "failed alt must not enter tried: {:?}",
                c.tried
            );
        }
        age_close_sent(&client);
        while u2.try_recv().is_ok() {}
        while w2.try_recv().is_ok() {}
        client.debug_maintain();
        assert!(
            client.snapshot().close_retry > r0,
            "drained dest must still be eligible after enqueue-fail"
        );
        let _ = (p1, p2, w1, u1, w2, u2);
        drop(tun);
        client.shutdown();
    }

    #[tokio::test]
    async fn expire_recv_close_does_not_fin_holes() {
        let mut cfg = SessionConfig::default();
        cfg.tuning.loss_timeout_floor = Duration::from_millis(20);
        let (server, mut incoming) = Session::new_server(cfg);
        let _p1 = inject_live(&server, 1, "a#0", 7);
        let _p2 = inject_live(&server, 2, "b#0", 7);
        server.handle_frame(
            1,
            Frame::StreamOpen(StreamOpen {
                stream_id: 1,
                target: Target {
                    host: "t".into(),
                    port: 1,
                },
            }),
        );
        let _inc = incoming.try_recv().expect("accepted");
        server.handle_frame(
            2,
            Frame::StreamData(StreamData {
                stream_id: 1,
                offset: 100,
                data: vec![0; 50],
            }),
        );
        server.handle_frame(
            2,
            Frame::StreamClose(StreamClose {
                stream_id: 1,
                final_offset: Some(200),
            }),
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
        server.debug_maintain();
        let st = server.get_stream(1).expect("stream");
        assert!(!st.recv_fin.load(Ordering::Relaxed), "holes must not FIN");
        assert_eq!(st.recv_next.load(Ordering::Relaxed), 0);
        server.handle_frame(
            1,
            Frame::StreamData(StreamData {
                stream_id: 1,
                offset: 0,
                data: vec![1; 200],
            }),
        );
        let st = server.get_stream(1).expect("stream");
        assert!(
            st.recv_fin.load(Ordering::Relaxed) || st.recv_next.load(Ordering::Relaxed) >= 200,
            "contiguous DATA must fill then FIN"
        );
        server.shutdown();
    }

    #[tokio::test]
    async fn expire_recv_close_fins_when_contiguous() {
        let (server, mut incoming) = Session::new_server(SessionConfig::default());
        let _p = inject_live(&server, 1, "a#0", 7);
        server.handle_frame(
            1,
            Frame::StreamOpen(StreamOpen {
                stream_id: 1,
                target: Target {
                    host: "t".into(),
                    port: 1,
                },
            }),
        );
        let _inc = incoming.try_recv().expect("accepted");
        server.handle_frame(
            1,
            Frame::StreamClose(StreamClose {
                stream_id: 1,
                final_offset: Some(0),
            }),
        );
        server.debug_maintain();
        let st = server.get_stream(1).expect("stream");
        assert!(st.recv_fin.load(Ordering::Relaxed));
        server.shutdown();
    }

    /// P5: DATA re-sent after we already FIN'd (or lying past close_off)
    /// must be answered with an ACK at close_off, not dropped silently —
    /// otherwise the sender keeps hedging the tail forever.
    #[tokio::test]
    async fn dup_data_after_fin_is_acked() {
        let (server, mut incoming) = Session::new_server(SessionConfig::default());
        let (p, _rx, _urx) = inject_live(&server, 1, "a#0", 7);
        server.handle_frame(
            1,
            Frame::StreamOpen(StreamOpen {
                stream_id: 1,
                target: Target {
                    host: "t".into(),
                    port: 1,
                },
            }),
        );
        let _inc = incoming.try_recv().expect("accepted");
        server.handle_frame(
            1,
            Frame::StreamData(StreamData {
                stream_id: 1,
                offset: 0,
                data: vec![7; 100],
            }),
        );
        server.handle_frame(
            1,
            Frame::StreamClose(StreamClose {
                stream_id: 1,
                final_offset: Some(100),
            }),
        );
        server.debug_maintain();
        let st = server.get_stream(1).expect("stream");
        assert!(st.recv_fin.load(Ordering::Relaxed));
        // Drain whatever ACK the delivery already staged.
        let _ = p.take_all_acks();
        let dup0 = server
            .inner
            .metrics
            .data_dup_rx_bytes
            .load(Ordering::Relaxed);
        server.handle_frame(
            1,
            Frame::StreamData(StreamData {
                stream_id: 1,
                offset: 60,
                data: vec![7; 40],
            }),
        );
        let acks = p.take_all_acks();
        let ack = acks.get(&1).expect("dup after FIN must re-ACK");
        assert_eq!(ack.acked_offset, 100, "ACK must carry close_off");
        assert_eq!(
            server
                .inner
                .metrics
                .data_dup_rx_bytes
                .load(Ordering::Relaxed)
                - dup0,
            40
        );
        assert_eq!(
            server.inner.metrics.ack_after_fin.load(Ordering::Relaxed),
            1
        );
        // Past close_off (sender bug / stale piece): same treatment.
        server.handle_frame(
            1,
            Frame::StreamData(StreamData {
                stream_id: 1,
                offset: 100,
                data: vec![7; 10],
            }),
        );
        assert!(p.take_all_acks().contains_key(&1));
        assert_eq!(
            server.inner.metrics.ack_after_fin.load(Ordering::Relaxed),
            2
        );
        assert_eq!(st.recv_next.load(Ordering::Relaxed), 100);
        server.shutdown();
    }

    #[tokio::test]
    async fn linger_progress_fine_without_recv_fin_sends_reset() {
        let mut cfg = SessionConfig::default();
        cfg.tuning.close_linger = Duration::from_millis(80);
        let client = Session::new_client(cfg);
        let (_p1, mut w1, mut u1) = inject_live(&client, 1, "a#0", 7);
        let (_p2, mut w2, mut u2) = inject_live(&client, 2, "b#0", 7);
        let mut tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        tun.write_all(b"hi").await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        let id = tun.id;
        let st = client.get_stream(id).unwrap();
        let next = st.send_next.load(Ordering::Relaxed);
        client.handle_frame(
            1,
            Frame::StreamAck(StreamAck {
                stream_id: id,
                acked_offset: next,
                window: 128 * 1024,
                sack: vec![],
            }),
        );
        tun.shutdown().await.unwrap();
        tokio::time::sleep(Duration::from_millis(120)).await;
        client.debug_maintain();
        let frames: Vec<_> = drain_frames(&mut w1, &mut u1)
            .into_iter()
            .chain(drain_frames(&mut w2, &mut u2))
            .collect();
        assert!(
            frames.iter().any(|f| matches!(f, Frame::StreamReset(_))),
            "client Residual D linger must send wire Reset"
        );
        assert!(
            !st.reset.load(Ordering::SeqCst),
            "Residual D must not set reset (that is finish_stream hop RST)"
        );
        let mut buf = [0u8; 1];
        let n = tokio::time::timeout(Duration::from_millis(200), tun.read(&mut buf))
            .await
            .expect("closer pump must unblock");
        assert!(matches!(n, Ok(0)), "closer pump must unblock, got {n:?}");
        assert_eq!(client.inner.streams.lock().unwrap().len(), 0);
        assert!(
            client.inner.resets.lock().unwrap().contains_key(&id),
            "Inner.resets must stay after maintain (observe before recv_fin.swap)"
        );
        assert!(client.snapshot().stream_reaps_linger >= 1);
        assert_eq!(client.snapshot().stream_resets_timeout, 0);
        client.shutdown();
    }

    #[tokio::test]
    async fn linger_silent_cas_loss_preserves_reset_table() {
        let client = Session::new_client(SessionConfig::default());
        let _p = inject_live(&client, 1, "a#0", 7);
        let tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let id = tun.id;
        let st = client.get_stream(id).unwrap();
        st.send_fin_sent.store(true, Ordering::Relaxed);
        client.handle_frame(
            1,
            Frame::StreamAck(StreamAck {
                stream_id: id,
                acked_offset: st.send_next.load(Ordering::Relaxed),
                window: 128 * 1024,
                sack: vec![],
            }),
        );
        client.remember_reset(id, 1, ResetReason::Timeout);
        st.counted_close.store(true, Ordering::SeqCst);
        client.linger_reap_progress_fine(id);
        assert!(
            client.inner.resets.lock().unwrap().contains_key(&id),
            "lost CAS must not wipe leftover Reset"
        );
        drop(tun);
        client.shutdown();
    }

    #[tokio::test]
    async fn leftover_drains_via_close_retry_when_progress_fine() {
        let mut cfg = SessionConfig::default();
        cfg.tuning.close_linger = Duration::from_millis(80);
        cfg.tuning.loss_timeout_floor = Duration::from_millis(20);
        let client = Session::new_client(cfg.clone());
        let (server, incoming) = Session::new_server(cfg);
        tokio::spawn(async move {
            let mut held = Vec::new();
            let mut incoming = incoming;
            while let Some(inc) = incoming.recv().await {
                held.push(inc);
            }
            let _ = held;
        });
        let (_p1, mut cw1, mut cu1) = inject_live(&client, 1, "a#0", 7);
        let (_p2, mut cw2, mut cu2) = inject_live(&client, 2, "b#0", 7);
        let _s1 = inject_live(&server, 1, "a#0", 7);
        let _s2 = inject_live(&server, 2, "b#0", 7);

        fn take(w: &mut mpsc::Receiver<Frame>, u: &mut mpsc::Receiver<Frame>) -> Option<Frame> {
            u.try_recv().ok().or_else(|| w.try_recv().ok())
        }

        let tun = client
            .open_stream(Target {
                host: "echo".into(),
                port: 1,
            })
            .await
            .unwrap();
        let mut open = None;
        for _ in 0..32 {
            if let Some(Frame::StreamOpen(o)) = take(&mut cw1, &mut cu1) {
                open = Some(o);
                break;
            }
            if let Some(Frame::StreamOpen(o)) = take(&mut cw2, &mut cu2) {
                open = Some(o);
                break;
            }
            tokio::task::yield_now().await;
        }
        let open = open.expect("client sent StreamOpen");
        server.handle_frame(1, Frame::StreamOpen(open.clone()));
        let id = tun.id;
        client.handle_frame(
            1,
            Frame::StreamAck(StreamAck {
                stream_id: id,
                acked_offset: 0,
                window: 128 * 1024,
                sack: vec![],
            }),
        );
        drop(tun);
        tokio::time::sleep(Duration::from_millis(15)).await;
        let mut fed = false;
        loop {
            let f = take(&mut cw1, &mut cu1).or_else(|| take(&mut cw2, &mut cu2));
            match f {
                Some(Frame::StreamClose(c)) if !fed => {
                    server.handle_frame(2, Frame::StreamClose(c));
                    fed = true;
                }
                Some(Frame::StreamClose(_)) => {}
                Some(Frame::StreamReset(_)) => panic!("progress-fine leftover must not Reset"),
                Some(_) => {}
                None => break,
            }
        }
        age_close_sent(&client);
        client.debug_maintain();
        server.debug_maintain();
        loop {
            let f = take(&mut cw1, &mut cu1).or_else(|| take(&mut cw2, &mut cu2));
            match f {
                Some(Frame::StreamClose(c)) => {
                    server.handle_frame(2, Frame::StreamClose(c));
                    fed = true;
                }
                Some(Frame::StreamReset(_)) => panic!("progress-fine leftover must not Reset"),
                Some(_) => {}
                None => break,
            }
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
        client.debug_maintain();
        server.debug_maintain();
        assert!(fed, "Close must land on dest 2");
        assert_eq!(server.inner.streams.lock().unwrap().len(), 0);
        client.shutdown();
        server.shutdown();
    }

    #[tokio::test]
    async fn server_origin_eof_linger_does_not_send_reset() {
        let mut cfg = SessionConfig::default();
        cfg.tuning.close_linger = Duration::from_millis(80);
        let (server, mut incoming) = Session::new_server(cfg);
        let (_p, mut w, mut u) = inject_live(&server, 1, "a#0", 7);
        server.handle_frame(
            1,
            Frame::StreamOpen(StreamOpen {
                stream_id: 1,
                target: Target {
                    host: "t".into(),
                    port: 1,
                },
            }),
        );
        let inc = incoming.try_recv().expect("accepted");
        let next = server
            .get_stream(1)
            .unwrap()
            .send_next
            .load(Ordering::Relaxed);
        server.handle_frame(
            1,
            Frame::StreamAck(StreamAck {
                stream_id: 1,
                acked_offset: next,
                window: 128 * 1024,
                sack: vec![],
            }),
        );
        drop(inc);
        tokio::time::sleep(Duration::from_millis(120)).await;
        server.debug_maintain();
        let frames = drain_frames(&mut w, &mut u);
        assert!(
            !frames.iter().any(|f| matches!(f, Frame::StreamReset(_))),
            "server origin-EOF linger must stay silent"
        );
        assert_eq!(server.inner.streams.lock().unwrap().len(), 0);
        assert!(server.inner.resets.lock().unwrap().is_empty());
        assert_eq!(server.snapshot().stream_resets_timeout, 0);
        server.shutdown();
    }

    #[tokio::test]
    async fn linger_progress_fine_with_recv_fin_does_not_send_reset() {
        let mut cfg = SessionConfig::default();
        cfg.tuning.close_linger = Duration::from_millis(80);
        let client = Session::new_client(cfg);
        let (_p1, mut w1, mut u1) = inject_live(&client, 1, "a#0", 7);
        let (_p2, mut w2, mut u2) = inject_live(&client, 2, "b#0", 7);
        let tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let id = tun.id;
        client.handle_frame(
            1,
            Frame::StreamAck(StreamAck {
                stream_id: id,
                acked_offset: 0,
                window: 128 * 1024,
                sack: vec![],
            }),
        );
        client.handle_frame(
            1,
            Frame::StreamClose(StreamClose {
                stream_id: id,
                final_offset: Some(0),
            }),
        );
        tokio::time::sleep(Duration::from_millis(120)).await;
        client.debug_maintain();
        let frames: Vec<_> = drain_frames(&mut w1, &mut u1)
            .into_iter()
            .chain(drain_frames(&mut w2, &mut u2))
            .collect();
        assert!(
            !frames.iter().any(|f| matches!(f, Frame::StreamReset(_))),
            "progress-fine + recv_fin linger must not send Reset"
        );
        assert_eq!(client.inner.streams.lock().unwrap().len(), 0);
        assert!(client.inner.resets.lock().unwrap().is_empty());
        drop(tun);
        client.shutdown();
    }

    #[tokio::test]
    async fn peer_reset_after_recv_fin_is_eof_not_hop_rst() {
        let (server, mut incoming) = Session::new_server(SessionConfig::default());
        let _p = inject_live(&server, 1, "a#0", 7);
        server.handle_frame(
            1,
            Frame::StreamOpen(StreamOpen {
                stream_id: 1,
                target: Target {
                    host: "t".into(),
                    port: 1,
                },
            }),
        );
        let mut inc = incoming.try_recv().expect("accepted");
        server.handle_frame(
            1,
            Frame::StreamClose(StreamClose {
                stream_id: 1,
                final_offset: Some(0),
            }),
        );
        let st = server.get_stream(1).unwrap();
        assert!(st.recv_fin.load(Ordering::SeqCst));
        let mut buf = [0u8; 1];
        let n = tokio::time::timeout(Duration::from_millis(200), inc.io.read(&mut buf))
            .await
            .expect("Close must EOF the pump");
        assert!(matches!(n, Ok(0)), "pump must EOF from Close, got {n:?}");
        let timeout0 = server.snapshot().stream_resets_timeout;
        server.on_peer_reset(1, ResetReason::Timeout);
        assert_eq!(server.snapshot().stream_resets_timeout, timeout0);
        assert!(
            st.reset.load(Ordering::SeqCst),
            "finish_stream still marks reset"
        );
        let n2 = tokio::time::timeout(Duration::from_millis(50), inc.io.read(&mut buf))
            .await
            .expect("read after peer Reset must not hang");
        assert!(
            matches!(n2, Ok(0)),
            "peer Reset after Close must stay EOF, not hop RST, got {n2:?}"
        );
        server.shutdown();
    }

    #[tokio::test]
    async fn stalled_leftover_retry_stops_after_all_dests_and_linger() {
        let mut cfg = SessionConfig::default();
        cfg.tuning.close_linger = Duration::from_millis(40);
        let client = Session::new_client(cfg);
        let _p1 = inject_live(&client, 1, "a#0", 7);
        let _p2 = inject_live(&client, 2, "b#0", 7);
        let mut tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        tun.write_all(b"hello").await.unwrap();
        let st = {
            let g = client.inner.streams.lock().unwrap();
            g.values().next().unwrap().clone()
        };
        let id = st.id;
        let deadline = Instant::now() + Duration::from_millis(200);
        loop {
            if !st.unacked.lock().unwrap().is_empty() {
                break;
            }
            assert!(Instant::now() < deadline, "write must leave unacked");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        while crate::metrics::mono_ms() < 100 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        {
            let mut u = st.unacked.lock().unwrap();
            for x in u.values_mut() {
                x.tried = vec![1, 2];
                x.last_sent = Instant::now() - Duration::from_millis(100);
                x.retry_not_before = Instant::now() - Duration::from_millis(100);
            }
        }
        st.stalled.store(true, Ordering::Relaxed);
        st.stall_from_ms.store(
            crate::metrics::mono_ms().saturating_sub(80).max(1),
            Ordering::Relaxed,
        );
        let hedge0 = client.snapshot().data_hedge;
        let rtx0 = client.snapshot().data_retransmit;
        client.debug_maintain();
        client.debug_maintain();
        assert_eq!(client.snapshot().data_hedge, hedge0);
        assert_eq!(client.snapshot().data_retransmit, rtx0);
        assert!(
            client.inner.streams.lock().unwrap().contains_key(&id),
            "A3 must not GC"
        );
        drop(tun);
        client.shutdown();
    }

    #[tokio::test]
    async fn retry_expired_unacked_skips_alive_write_stalled_from() {
        let client = Session::new_client(SessionConfig::default());
        let (p1, _w1, _u1) = inject_live(&client, 1, "a#0", 7);
        let _p2 = inject_live(&client, 2, "b#0", 7);
        let mut tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        tun.write_all(b"hello").await.unwrap();
        let st = {
            let g = client.inner.streams.lock().unwrap();
            g.values().next().unwrap().clone()
        };
        let deadline = Instant::now() + Duration::from_millis(200);
        loop {
            if !st.unacked.lock().unwrap().is_empty() {
                break;
            }
            assert!(Instant::now() < deadline, "write must leave unacked");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        {
            let mut u = st.unacked.lock().unwrap();
            for x in u.values_mut() {
                x.path_id = 1;
                x.tried = vec![1];
                x.last_sent = Instant::now() - Duration::from_millis(100);
                x.retry_not_before = Instant::now() - Duration::from_millis(100);
            }
        }
        p1.set_write_stalled(true);
        let hedge0 = client.snapshot().data_hedge;
        let rtx0 = client.snapshot().data_retransmit;
        client.debug_maintain();
        assert_eq!(client.snapshot().data_hedge, hedge0);
        assert_eq!(client.snapshot().data_retransmit, rtx0);
        let from = st.unacked.lock().unwrap().values().next().unwrap().path_id;
        assert_eq!(from, 1, "copy must stay on stalled-alive from");
        drop(tun);
        client.shutdown();
    }

    #[tokio::test]
    async fn retry_does_not_pick_write_stalled_alt() {
        let client = Session::new_client(SessionConfig::default());
        let (_p1, _w1, _u1) = inject_live(&client, 1, "a#0", 7);
        let (p2, _w2, _u2) = inject_live(&client, 2, "b#0", 7);
        let mut tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        tun.write_all(b"hello").await.unwrap();
        let st = {
            let g = client.inner.streams.lock().unwrap();
            g.values().next().unwrap().clone()
        };
        let deadline = Instant::now() + Duration::from_millis(200);
        loop {
            if !st.unacked.lock().unwrap().is_empty() {
                break;
            }
            assert!(Instant::now() < deadline, "write must leave unacked");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        {
            let mut u = st.unacked.lock().unwrap();
            for x in u.values_mut() {
                x.path_id = 1;
                x.tried = vec![1];
                x.last_sent = Instant::now() - Duration::from_millis(100);
                x.retry_not_before = Instant::now() - Duration::from_millis(100);
            }
        }
        p2.set_write_stalled(true);
        let hedge0 = client.snapshot().data_hedge;
        client.debug_maintain();
        assert_eq!(client.snapshot().data_hedge, hedge0);
        let tried = st
            .unacked
            .lock()
            .unwrap()
            .values()
            .next()
            .unwrap()
            .tried
            .clone();
        assert!(
            !tried.contains(&2),
            "must not spray onto write-stalled alt: {tried:?}"
        );
        drop(tun);
        client.shutdown();
    }

    #[tokio::test]
    async fn ack_overwrite_does_not_use_urgent_chan() {
        let client = Session::new_client(SessionConfig::default());
        let (p, _w, _u) = inject_live(&client, 1, "a#0", 7);
        let tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let st = client.get_stream(tun.id).unwrap();
        stuff_urgent_keep_schedulable(&p);
        let drop0 = client.snapshot().frame_send_drop;
        let cong0 = p.is_congested();
        let q0 = p.queued_urgent();
        client.send_ack(&st, p.id);
        assert_eq!(p.queued_urgent(), q0);
        assert_eq!(client.snapshot().frame_send_drop, drop0);
        assert_eq!(p.is_congested(), cong0);
        assert!(
            p.pending_acks.lock().unwrap().contains_key(&st.id),
            "ACK must land in the overwrite register"
        );
        let ping = Frame::Ping(nya_proto::Ping {
            seq: 1,
            sent_at_ms: 0,
        });
        assert!(
            p.urgent.try_send(ping).is_err(),
            "urgent must stay full; ACK is not an mpsc slot"
        );
        drop(tun);
        client.shutdown();
    }

    #[tokio::test]
    async fn ack_coalesce_keeps_latest_offset() {
        let client = Session::new_client(SessionConfig::default());
        let (p, _w, _u) = inject_live(&client, 1, "a#0", 7);
        let tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let st = client.get_stream(tun.id).unwrap();
        st.recv_next.store(100, Ordering::Relaxed);
        client.send_ack(&st, p.id);
        st.recv_next.store(200, Ordering::Relaxed);
        client.send_ack(&st, p.id);
        let g = p.pending_acks.lock().unwrap();
        assert_eq!(g.len(), 1);
        let ack = g.get(&st.id).expect("coalesced ACK");
        assert_eq!(ack.acked_offset, 200);
        drop(tun);
        client.shutdown();
    }

    #[tokio::test]
    async fn ack_unblocks_window_when_urgent_full() {
        let client = Session::new_client(SessionConfig::default());
        let (server, mut incoming) = Session::new_server(SessionConfig::default());
        let (a, b) = duplex(64 * 1024);
        let _cd = client.start_path("a#0".into(), a);
        let _sd = server.start_path("a#0".into(), b);
        client.wait_alive(1, Duration::from_secs(1)).await.unwrap();
        server.wait_alive(1, Duration::from_secs(1)).await.unwrap();
        let tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let inc = tokio::time::timeout(Duration::from_secs(1), incoming.recv())
            .await
            .expect("incoming")
            .expect("stream");
        let sid = tun.id;
        let cst = client.get_stream(sid).unwrap();
        cst.send_window.store(0, Ordering::Relaxed);
        let sp = server.path_list()[0].clone();
        stuff_urgent_keep_schedulable(&sp);
        let drop0 = server.snapshot().frame_send_drop;
        let cong0 = sp.is_congested();
        let blocked = tokio::spawn(async move {
            let mut tun = tun;
            tun.write_all(b"x").await
        });
        let deadline = Instant::now() + Duration::from_millis(500);
        loop {
            if client.snapshot().window_blocks > 0 {
                break;
            }
            assert!(Instant::now() < deadline, "send must block on window");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let sst = server.get_stream(sid).unwrap();
        server.send_ack(&sst, sp.id);
        tokio::time::timeout(Duration::from_secs(1), blocked)
            .await
            .expect("ACK register must unblock window_ok while urgent is full")
            .unwrap()
            .unwrap();
        assert_eq!(server.snapshot().frame_send_drop, drop0);
        assert_eq!(sp.is_congested(), cong0);
        drop(inc);
        client.shutdown();
        server.shutdown();
    }

    #[tokio::test]
    async fn ack_idle_writer_emits_before_ping_interval() {
        let cfg = SessionConfig {
            ping_interval_min: Duration::from_millis(200),
            ping_interval_max: Duration::from_millis(200),
            ..SessionConfig::default()
        };
        let client = Session::new_client(cfg);
        let (a, mut peer) = duplex(64 * 1024);
        let _done = client.start_path("a#0".into(), a);
        let p = client.path_list()[0].clone();
        p.record_rtt(Duration::from_millis(7));
        age_rx(&p, 1000);
        let first = tokio::time::timeout(Duration::from_millis(400), read_len_frame(&mut peer))
            .await
            .expect("first ping");
        assert!(matches!(first, Frame::Ping(_)), "{first:?}");
        let (tx, _rx) = mpsc::channel(8);
        let st = StreamState::new(7, tx, client.inner.cfg.tuning.initial_window);
        client.inner.streams.lock().unwrap().insert(7, st.clone());
        let t0 = Instant::now();
        client.send_ack(&st, p.id);
        let f = tokio::time::timeout(Duration::from_millis(30), read_len_frame(&mut peer))
            .await
            .expect("ACK must not wait for ping cadence");
        match f {
            Frame::StreamAck(a) => assert_eq!(a.stream_id, 7),
            other => panic!("expected STREAM_ACK, got {other:?}"),
        }
        assert!(
            t0.elapsed() < Duration::from_millis(50),
            "idle ACK {:?} must be ≪ ping_interval_max",
            t0.elapsed()
        );
        client.shutdown();
    }

    #[tokio::test]
    async fn ack_dirty_survives_in_flight_overwrite() {
        let client = Session::new_client(SessionConfig::default());
        let (a, mut peer) = duplex(8);
        let _done = client.start_path("a#0".into(), a);
        let p = client.path_list()[0].clone();
        let tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let st = client.get_stream(tun.id).unwrap();
        let _open = tokio::time::timeout(Duration::from_secs(1), read_len_frame(&mut peer))
            .await
            .expect("StreamOpen");
        st.recv_next.store(100, Ordering::Relaxed);
        client.send_ack(&st, p.id);
        let deadline = Instant::now() + Duration::from_millis(500);
        loop {
            if !p.pending_acks.lock().unwrap().contains_key(&st.id) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "gen1 must be taken for write_one"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        st.recv_next.store(200, Ordering::Relaxed);
        client.send_ack(&st, p.id);
        assert!(st.ack_dirty.load(Ordering::Relaxed));
        assert_eq!(
            p.pending_acks
                .lock()
                .unwrap()
                .get(&st.id)
                .map(|a| a.acked_offset),
            Some(200),
            "gen2 must sit in the map during gen1 write_one"
        );
        let gen1 = tokio::time::timeout(Duration::from_secs(1), read_len_frame(&mut peer))
            .await
            .expect("gen1 ACK");
        match gen1 {
            Frame::StreamAck(a) => assert_eq!(a.acked_offset, 100),
            other => panic!("expected gen1 ACK, got {other:?}"),
        }
        assert!(
            st.ack_dirty.load(Ordering::Relaxed),
            "Sent of gen1 must not clear dirty while gen2 is pending"
        );
        drop(tun);
        client.shutdown();
    }

    #[tokio::test]
    async fn ack_take_k_does_not_starve_ping() {
        let cfg = SessionConfig {
            ping_interval_min: Duration::from_millis(500),
            ping_interval_max: Duration::from_millis(500),
            ..SessionConfig::default()
        };
        let client = Session::new_client(cfg);
        let (a, mut peer) = duplex(8);
        let _done = client.start_path("a#0".into(), a);
        let p = client.path_list()[0].clone();
        p.record_rtt(Duration::from_millis(7));
        {
            let mut g = p.pending_acks.lock().unwrap();
            for i in 1..=16u32 {
                g.insert(
                    i,
                    StreamAck {
                        stream_id: i,
                        acked_offset: 0,
                        window: 1,
                        sack: vec![],
                    },
                );
            }
        }
        p.ack_wait.notify_one();
        let deadline = Instant::now() + Duration::from_millis(500);
        loop {
            if p.ack_pending() <= crate::path::ACK_FLUSH_K as u64 {
                break;
            }
            assert!(Instant::now() < deadline, "writer must take K ACKs");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        age_rx(&p, 1000);
        let mut acks = 0u32;
        loop {
            let f = tokio::time::timeout(Duration::from_secs(1), read_len_frame(&mut peer))
                .await
                .expect("frame");
            match f {
                Frame::StreamAck(_) => {
                    acks += 1;
                    assert!(
                        acks <= crate::path::ACK_FLUSH_K as u32,
                        "ping-due must run before ACK {acks} (K=8)"
                    );
                }
                Frame::Ping(_) => break,
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(acks, crate::path::ACK_FLUSH_K as u32);
        client.shutdown();
    }

    #[tokio::test]
    async fn ack_moves_on_path_failed() {
        let client = Session::new_client(SessionConfig::default());
        let (p1, _w1, _u1) = inject_live(&client, 1, "a#0", 7);
        let (p2, _w2, _u2) = inject_live(&client, 2, "b#0", 7);
        let tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let st = client.get_stream(tun.id).unwrap();
        p2.pending_acks.lock().unwrap().insert(
            st.id,
            StreamAck {
                stream_id: st.id,
                acked_offset: 100,
                window: 1,
                sack: vec![],
            },
        );
        st.recv_next.store(200, Ordering::Relaxed);
        client.send_ack(&st, p1.id);
        assert_eq!(
            p1.pending_acks
                .lock()
                .unwrap()
                .get(&st.id)
                .map(|a| a.acked_offset),
            Some(200)
        );
        client.path_failed(p1.id);
        let g = p2.pending_acks.lock().unwrap();
        let ack = g.get(&st.id).expect("merged onto alt");
        assert_eq!(ack.acked_offset, 200, "max offset must win");
        drop(tun);
        client.shutdown();
    }

    #[tokio::test]
    async fn ack_send_on_down_dest_stores_on_alt() {
        let client = Session::new_client(SessionConfig::default());
        let (p1, _w1, _u1) = inject_live(&client, 1, "a#0", 7);
        let (p2, _w2, _u2) = inject_live(&client, 2, "b#0", 7);
        let tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let st = client.get_stream(tun.id).unwrap();
        p1.state.store(crate::path::STATE_DOWN, Ordering::SeqCst);
        client.send_ack(&st, p1.id);
        assert!(
            p1.pending_acks.lock().unwrap().get(&st.id).is_none(),
            "DOWN dest must not keep a raced insert"
        );
        assert!(
            p2.pending_acks.lock().unwrap().contains_key(&st.id),
            "ACK must land on an alive alt"
        );
        drop(tun);
        client.shutdown();
    }

    #[tokio::test]
    async fn ack_taken_batch_merges_on_path_failed() {
        let client = Session::new_client(SessionConfig::default());
        let (p1, _w1, _u1) = inject_live(&client, 1, "a#0", 7);
        let (p2, _w2, _u2) = inject_live(&client, 2, "b#0", 7);
        let tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let st = client.get_stream(tun.id).unwrap();
        st.recv_next.store(200, Ordering::Relaxed);
        client.send_ack(&st, p1.id);
        let batch = p1.take_acks(8);
        assert!(
            !batch.is_empty(),
            "writer-taken batch must leave the live map"
        );
        client.path_failed(p1.id);
        assert!(
            p2.pending_acks.lock().unwrap().get(&st.id).is_none(),
            "in-flight batch is not in the first path_failed take"
        );
        p1.restore_acks(batch);
        client.merge_pending_acks(p1.id, p1.take_all_acks());
        let g = p2.pending_acks.lock().unwrap();
        let ack = g.get(&st.id).expect("leftover must merge onto alt");
        assert_eq!(ack.acked_offset, 200);
        drop(tun);
        client.shutdown();
    }

    #[tokio::test]
    async fn hol_leftover_interactive_does_not_pin_after_linger_stall() {
        let mut cfg = SessionConfig::default();
        cfg.tuning.close_linger = Duration::from_millis(40);
        let client = Session::new_client(cfg);
        let _p1 = inject_live(&client, 1, "a#0", 7);
        let _p2 = inject_live(&client, 2, "a#1", 7);
        let mut tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        tun.write_all(b"hi").await.unwrap();
        let st = {
            let g = client.inner.streams.lock().unwrap();
            g.values().next().unwrap().clone()
        };
        while crate::metrics::mono_ms() < 100 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        st.sticky.store(1, Ordering::Relaxed);
        st.stalled.store(true, Ordering::Relaxed);
        st.stall_from_ms.store(
            crate::metrics::mono_ms().saturating_sub(80).max(1),
            Ordering::Relaxed,
        );
        assert!(
            client.hol_place_bulk(1).is_none(),
            "linger-stalled leftover must not pin as interactive"
        );
        drop(tun);
        client.shutdown();
    }

    #[tokio::test]
    async fn hol_place_bulk_accepts_write_stalled_sibling() {
        // Two dests only: a schedulable third would hide stalled soy in
        // fastest_class_set. D2 must pick the stalled sibling directly.
        let client = Session::new_client(SessionConfig::default());
        let (_p0, _w0, _u0) = inject_live(&client, 1, "a#0", 7);
        let (p1, _w1, _u1) = inject_live(&client, 2, "a#1", 7);
        p1.set_write_stalled(true);
        assert!(!p1.is_congested());
        let mut tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        tun.write_all(b"hi").await.unwrap();
        let st = {
            let g = client.inner.streams.lock().unwrap();
            g.values().next().unwrap().clone()
        };
        st.sticky.store(1, Ordering::Relaxed);
        assert!(
            !st.bulk.load(Ordering::Relaxed),
            "interactive sticky on dest 1"
        );
        assert_eq!(
            client.hol_place_bulk(1),
            Some(2),
            "write-stalled same-link dest is a bulk HOL target"
        );
        drop(tun);
        client.shutdown();
    }

    #[tokio::test]
    async fn bulk_affinity_stays_on_write_stalled_sticky() {
        let client = Session::new_client(SessionConfig::default());
        let (p1, _w1, _u1) = inject_live(&client, 1, "a#0", 7);
        let (_p2, _w2, _u2) = inject_live(&client, 2, "b#0", 7);
        let mut tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        tun.write_all(b"hi").await.unwrap();
        let st = {
            let g = client.inner.streams.lock().unwrap();
            g.values().next().unwrap().clone()
        };
        let deadline = Instant::now() + Duration::from_millis(200);
        loop {
            if !st.unacked.lock().unwrap().is_empty() {
                break;
            }
            assert!(Instant::now() < deadline, "write must leave unacked");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        st.bulk.store(true, Ordering::Relaxed);
        st.sticky.store(1, Ordering::Relaxed);
        p1.set_write_stalled(true);
        let before = st.send_next.load(Ordering::Relaxed);
        tun.write_all(b"more-bulk").await.unwrap();
        let deadline = Instant::now() + Duration::from_millis(200);
        loop {
            if st.send_next.load(Ordering::Relaxed) > before {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "bulk write must advance send_next"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let last = st
            .unacked
            .lock()
            .unwrap()
            .values()
            .max_by_key(|u| u.last_sent)
            .map(|u| u.path_id);
        assert_eq!(
            last,
            Some(1),
            "bulk affinity must stay on write-stalled sticky"
        );
        assert_eq!(st.sticky.load(Ordering::Relaxed), 1);
        drop(tun);
        client.shutdown();
    }

    #[tokio::test]
    async fn interactive_affinity_still_skips_write_stalled() {
        let client = Session::new_client(SessionConfig::default());
        let (p1, _w1, _u1) = inject_live(&client, 1, "a#0", 7);
        let (_p2, _w2, _u2) = inject_live(&client, 2, "b#0", 7);
        p1.set_write_stalled(true);
        assert!(
            client.interactive_affinity(1).is_none(),
            "interactive affinity must still skip write-stalled dests"
        );
        client.shutdown();
    }

    #[tokio::test]
    async fn interactive_affinity_skips_far_slow_on_equal_clock_200() {
        let client = Session::new_client(SessionConfig::default());
        let (a, ..) = inject_live(&client, 1, "a#0", 168);
        let (s, ..) = inject_live(&client, 2, "s#0", 258);
        a.rtt_ewma_us.store(213_000, Ordering::Relaxed);
        a.rtt_class_us.store(200_000, Ordering::Relaxed);
        a.rtt_stable_us.store(200_000, Ordering::Relaxed);
        s.rtt_ewma_us.store(283_000, Ordering::Relaxed);
        s.rtt_class_us.store(244_000, Ordering::Relaxed);
        s.rtt_stable_us.store(152_000, Ordering::Relaxed);
        assert!(
            client.interactive_affinity(2).is_none(),
            "s must not keep Interactive sticky vs equal-clock 200"
        );
        assert_eq!(client.pick_pref(PickPref::Interactive).unwrap(), 1);
        client.shutdown();
    }

    #[tokio::test]
    async fn interactive_affinity_skips_far_slow_when_peer_stable_is_168() {
        let client = Session::new_client(SessionConfig::default());
        let (a, ..) = inject_live(&client, 1, "a#0", 168);
        let (s, ..) = inject_live(&client, 2, "s#0", 258);
        a.rtt_ewma_us.store(213_000, Ordering::Relaxed);
        a.rtt_class_us.store(200_000, Ordering::Relaxed);
        a.rtt_stable_us.store(168_000, Ordering::Relaxed);
        s.rtt_ewma_us.store(283_000, Ordering::Relaxed);
        s.rtt_class_us.store(244_000, Ordering::Relaxed);
        s.rtt_stable_us.store(152_000, Ordering::Relaxed);
        assert!(client.interactive_affinity(2).is_none());
        assert_eq!(client.pick_pref(PickPref::Interactive).unwrap(), 1);
        client.shutdown();
    }

    #[tokio::test]
    async fn note_pick_credits_alt_when_primary_enqueue_fails() {
        let client = Session::new_client(SessionConfig::default());
        let (p1, _w1, _u1) = inject_live(&client, 1, "a#0", 80);
        let p2 = inject_live(&client, 2, "b#0", 7);
        stuff_urgent_keep_schedulable(&p1);
        let _tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let rtt = client.snapshot().pick_rtt_us;
        assert!(
            (6_000..=10_000).contains(&rtt),
            "pick_rtt_us should be the alt 7ms dest, got {rtt}"
        );
        let snaps = client.snapshot();
        let p1s = snaps.paths.iter().find(|p| p.name == "a#0").unwrap();
        let p2s = snaps.paths.iter().find(|p| p.name == "b#0").unwrap();
        assert_eq!(
            p1s.picks, 0,
            "primary enqueue failed; must not credit picks"
        );
        assert_eq!(p2s.picks, 1);
        let _ = p2;
        client.shutdown();
    }

    #[tokio::test]
    async fn note_pick_unknown_stores_zero() {
        let client = Session::new_client(SessionConfig::default());
        let (_p, _w, _u) = inject_live(&client, 1, "a#0", 0);
        // rtt 0 ms store still sets ewma 0 → unknown
        client
            .inner
            .paths
            .lock()
            .unwrap()
            .get(&1)
            .unwrap()
            .rtt_ewma_us
            .store(0, Ordering::Relaxed);
        let _tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        assert_eq!(client.snapshot().pick_rtt_us, 0);
        assert!(client.snapshot().picks_unknown_rtt >= 1);
        client.shutdown();
    }

    #[tokio::test]
    async fn open_stream_pick_rtt_prefers_fast_dest() {
        let client = Session::new_client(SessionConfig::default());
        let _slow = inject_live(&client, 1, "soy#0", 80);
        let _fast = inject_live(&client, 2, "akcdn#0", 7);
        let _tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let rtt = client.snapshot().pick_rtt_us;
        assert!(
            (6_000..=10_000).contains(&rtt),
            "pick_rtt_us should be 7ms dest, got {rtt}"
        );
        let snaps = client.snapshot();
        assert_eq!(
            snaps
                .paths
                .iter()
                .find(|p| p.name == "akcdn#0")
                .unwrap()
                .picks,
            1
        );
        assert_eq!(
            snaps
                .paths
                .iter()
                .find(|p| p.name == "soy#0")
                .unwrap()
                .picks,
            0
        );
        client.shutdown();
    }

    #[tokio::test]
    async fn path_failed_does_not_tear_sisters() {
        let client = Session::new_client(SessionConfig::default());
        let a = inject_named(&client, 1, "soy#0", 7);
        let b = inject_named(&client, 2, "soy#1", 7);
        client.path_failed(1);
        assert!(b.is_alive());
        assert!(!a.is_alive());
        assert_eq!(client.snapshot().path_down, 1);
        client.shutdown();
    }

    #[tokio::test]
    async fn short_lived_unknown_not_pick_best_with_known_fresh_sister() {
        let client = Session::new_client(SessionConfig::default());
        let _known = inject_live(&client, 1, "akcdn#0", 7);
        inject_named(&client, 2, "soy#0", 7);
        client.path_failed(2);
        let (_unk, _w, _u) = inject_live(&client, 3, "soy#0", 0);
        client
            .inner
            .paths
            .lock()
            .unwrap()
            .get(&3)
            .unwrap()
            .rtt_ewma_us
            .store(0, Ordering::Relaxed);
        let _tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        assert_eq!(
            client
                .snapshot()
                .paths
                .iter()
                .find(|p| p.name == "akcdn#0")
                .unwrap()
                .picks,
            1
        );
        assert_eq!(
            client
                .snapshot()
                .paths
                .iter()
                .find(|p| p.name == "soy#0")
                .unwrap()
                .picks,
            0
        );
        client.shutdown();
    }

    #[tokio::test]
    async fn reset_after_graceful_does_not_double_count() {
        let (client, server, incoming) = pair();
        tokio::spawn(echo_server(incoming));
        let (a, b) = duplex(64 * 1024);
        let c = client.clone();
        let s = server.clone();
        tokio::spawn(async move { c.add_path("p1".into(), a).await });
        tokio::spawn(async move { s.add_path("p1".into(), b).await });
        client.wait_ready(Duration::from_secs(2)).await.unwrap();
        let tun = client
            .open_stream(Target {
                host: "echo".into(),
                port: 1,
            })
            .await
            .unwrap();
        let id = tun.id;
        drop(tun);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let st = client.get_stream(id);
        if let Some(st) = st {
            let pid = st.sticky.load(Ordering::Relaxed);
            let sticky0 = client.get_path(pid).map(|p| p.sticky_count()).unwrap_or(0);
            st.send_fin_sent.store(true, Ordering::SeqCst);
            st.recv_fin.store(true, Ordering::SeqCst);
            client.maybe_count_graceful(&st);
            let sticky1 = client.get_path(pid).map(|p| p.sticky_count()).unwrap_or(0);
            assert!(
                sticky1 < sticky0 || sticky0 == 0,
                "graceful close must unstick, {sticky0} -> {sticky1}"
            );
            assert_eq!(st.sticky.load(Ordering::Relaxed), 0);
        }
        let snap0 = client.snapshot();
        client.reset_stream(id, ResetReason::PeerReset);
        let snap1 = client.snapshot();
        assert_eq!(
            snap1.stream_resets, snap0.stream_resets,
            "already closed must not increment resets"
        );
        client.shutdown();
        server.shutdown();
    }

    fn inject_known_path(client: &Session, id: u32) -> Arc<PathState> {
        inject_named(client, id, &format!("t#{id}"), 7)
    }

    fn age_path(p: &PathState, ago: Duration) {
        let now = Instant::now();
        *p.up_since.lock().unwrap() = now.checked_sub(ago).unwrap_or(now);
    }

    fn inject_named(client: &Session, id: u32, name: &str, rtt_ms: u64) -> Arc<PathState> {
        let (tx, _rx) = mpsc::channel(8);
        let p = PathState::new(id, name.into(), tx);
        p.rtt_ewma_us.store(rtt_ms * 1000, Ordering::Relaxed);
        p.rtt_stable_us.store(rtt_ms * 1000, Ordering::Relaxed);
        p.rtt_class_us.store(rtt_ms * 1000, Ordering::Relaxed);
        p.note_class_known_now();
        client.inner.paths.lock().unwrap().insert(id, p.clone());
        p
    }

    /// Keep both writer receivers so `try_send` succeeds until the channel fills.
    fn inject_live(
        client: &Session,
        id: u32,
        name: &str,
        rtt_ms: u64,
    ) -> (Arc<PathState>, mpsc::Receiver<Frame>, mpsc::Receiver<Frame>) {
        inject_live_cap(client, id, name, rtt_ms, 64)
    }

    fn inject_live_cap(
        client: &Session,
        id: u32,
        name: &str,
        rtt_ms: u64,
        cap: usize,
    ) -> (Arc<PathState>, mpsc::Receiver<Frame>, mpsc::Receiver<Frame>) {
        let (wtx, wrx) = mpsc::channel(cap);
        let (utx, urx) = mpsc::channel(cap);
        let p = PathState::with_writers(id, name.into(), wtx, utx);
        p.rtt_ewma_us.store(rtt_ms * 1000, Ordering::Relaxed);
        p.rtt_stable_us.store(rtt_ms * 1000, Ordering::Relaxed);
        p.rtt_class_us.store(rtt_ms * 1000, Ordering::Relaxed);
        p.note_class_known_now();
        client.inner.paths.lock().unwrap().insert(id, p.clone());
        (p, wrx, urx)
    }

    fn fill_urgent(client: &Session, path_id: u32) {
        let ping = Frame::Ping(nya_proto::Ping {
            seq: 1,
            sent_at_ms: 0,
        });
        for _ in 0..128 {
            if !client.send_on_path(path_id, ping.clone()) {
                return;
            }
        }
        panic!("urgent queue did not fill");
    }

    fn fill_bulk(client: &Session, path_id: u32) {
        let data = Frame::StreamData(StreamData {
            stream_id: 0,
            offset: 0,
            data: vec![0; 1600],
        });
        for _ in 0..128 {
            if !client.send_on_path(path_id, data.clone()) {
                return;
            }
        }
        panic!("bulk queue did not fill");
    }

    /// Fill urgent without `set_congested`, so `pick_pref` still returns this dest.
    fn stuff_urgent_keep_schedulable(p: &PathState) {
        let ping = Frame::Ping(nya_proto::Ping {
            seq: 1,
            sent_at_ms: 0,
        });
        loop {
            if p.urgent.try_send(ping.clone()).is_err() {
                return;
            }
        }
    }

    async fn read_len_frame(r: &mut (impl AsyncRead + Unpin)) -> Frame {
        let mut hdr = [0u8; 4];
        r.read_exact(&mut hdr).await.unwrap();
        let n = u32::from_be_bytes(hdr) as usize;
        let mut buf = vec![0u8; n];
        r.read_exact(&mut buf).await.unwrap();
        Frame::decode(&buf).unwrap()
    }

    fn age_rx(p: &PathState, ms: u64) {
        *p.last_rx.lock().unwrap() = Instant::now() - Duration::from_millis(ms);
    }

    #[tokio::test]
    async fn stream_ack_200ms_does_not_raise_ewma() {
        let client = Session::new_client(SessionConfig::default());
        let p = inject_named(&client, 1, "a#0", 7);
        let before = p.rtt_ewma_us.load(Ordering::Relaxed);
        let mut tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        tun.write_all(b"hello").await.unwrap();
        let st = {
            let g = client.inner.streams.lock().unwrap();
            g.values().next().unwrap().clone()
        };
        let deadline = Instant::now() + Duration::from_millis(200);
        loop {
            if !st.unacked.lock().unwrap().is_empty() {
                break;
            }
            assert!(Instant::now() < deadline, "write must leave unacked");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        {
            let mut u = st.unacked.lock().unwrap();
            for x in u.values_mut() {
                x.last_sent = Instant::now() - Duration::from_millis(200);
            }
        }
        let acked = st.send_next.load(Ordering::Relaxed);
        client.on_ack(StreamAck {
            stream_id: st.id,
            acked_offset: acked,
            window: 128 * 1024,
            sack: vec![],
        });
        assert_eq!(
            p.rtt_ewma_us.load(Ordering::Relaxed),
            before,
            "200 ms ACK must not raise EWMA on a 7 ms dest pool"
        );
        drop(tun);
        client.shutdown();
    }

    #[tokio::test]
    async fn stream_ack_10ms_may_record() {
        let client = Session::new_client(SessionConfig::default());
        let p = inject_named(&client, 1, "a#0", 7);
        let before = p.rtt_ewma_us.load(Ordering::Relaxed);
        let mut tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        tun.write_all(b"hello").await.unwrap();
        let st = {
            let g = client.inner.streams.lock().unwrap();
            g.values().next().unwrap().clone()
        };
        let deadline = Instant::now() + Duration::from_millis(200);
        loop {
            if !st.unacked.lock().unwrap().is_empty() {
                break;
            }
            assert!(Instant::now() < deadline, "write must leave unacked");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        {
            let mut u = st.unacked.lock().unwrap();
            for x in u.values_mut() {
                x.last_sent = Instant::now() - Duration::from_millis(10);
            }
        }
        let acked = st.send_next.load(Ordering::Relaxed);
        client.on_ack(StreamAck {
            stream_id: st.id,
            acked_offset: acked,
            window: 128 * 1024,
            sack: vec![],
        });
        let after = p.rtt_ewma_us.load(Ordering::Relaxed);
        assert_ne!(after, before, "~10 ms ACK may still move EWMA");
        assert!(
            after < 20_000,
            "10 ms ACK must stay well under 20 ms, got {after}"
        );
        drop(tun);
        client.shutdown();
    }

    #[tokio::test]
    async fn silence_without_ping_marks_degraded() {
        let client = Session::new_client(SessionConfig::default());
        let p = inject_known_path(&client, 1);
        age_rx(&p, 60);
        let before = client.snapshot().path_degraded;
        client.debug_maintain();
        assert_eq!(client.snapshot().path_degraded, before + 1);
        assert_eq!(p.state.load(Ordering::Relaxed), crate::path::STATE_DEGRADED);
        client.shutdown();
    }

    #[tokio::test]
    async fn young_inflight_ping_does_not_degrade() {
        let client = Session::new_client(SessionConfig::default());
        let p = inject_known_path(&client, 1);
        p.next_ping();
        age_rx(&p, 60);
        let before = client.snapshot().path_degraded;
        client.debug_maintain();
        assert_eq!(client.snapshot().path_degraded, before);
        assert_eq!(p.state.load(Ordering::Relaxed), crate::path::STATE_UP);
        client.shutdown();
    }

    #[tokio::test]
    async fn expired_ping_marks_degraded() {
        let client = Session::new_client(SessionConfig::default());
        let p = inject_known_path(&client, 1);
        p.next_ping();
        tokio::time::sleep(Duration::from_millis(25)).await;
        age_rx(&p, 60);
        let before = client.snapshot().path_degraded;
        client.debug_maintain();
        assert_eq!(client.snapshot().path_degraded, before + 1);
        client.shutdown();
    }

    #[tokio::test]
    async fn open_then_interactive_data_share_path() {
        let (client, server) = pair_echo(&["a#0", "a#1", "b#0", "b#1", "c#0", "c#1"]).await;
        let mut tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let st = {
            let g = client.inner.streams.lock().unwrap();
            g.values().next().unwrap().clone()
        };
        let open_pid = st.sticky.load(Ordering::Relaxed);
        assert_ne!(open_pid, 0, "Open must set sticky");
        tun.write_all(&[0u8; 200]).await.unwrap();
        let deadline = Instant::now() + Duration::from_millis(400);
        loop {
            if st.send_next.load(Ordering::Relaxed) > 0 {
                break;
            }
            assert!(Instant::now() < deadline, "write must advance send_next");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            st.sticky.load(Ordering::Relaxed),
            open_pid,
            "first Interactive DATA must share Open's 5-tuple"
        );
        drop(tun);
        client.shutdown();
        server.shutdown();
    }

    #[tokio::test]
    async fn retry_after_poisoned_path_with_live_fast_dest_is_floor() {
        let client = Session::new_client(SessionConfig::default());
        let (sick, _sw, _su) = inject_live(&client, 1, "nsix#0", 7);
        let _held_b = inject_live(&client, 2, "soy#0", 7);
        sick.rtt_ewma_us.store(200_000, Ordering::Relaxed);
        let mut tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        tun.write_all(b"hello").await.unwrap();
        let st = {
            let g = client.inner.streams.lock().unwrap();
            g.values().next().unwrap().clone()
        };
        let deadline = Instant::now() + Duration::from_millis(200);
        loop {
            if !st.unacked.lock().unwrap().is_empty() {
                break;
            }
            assert!(Instant::now() < deadline, "write must leave unacked");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        {
            let mut u = st.unacked.lock().unwrap();
            for x in u.values_mut() {
                x.last_sent = Instant::now() - Duration::from_millis(25);
                x.path_id = 1;
                x.tried = vec![1];
            }
        }
        let from = st.unacked.lock().unwrap().values().next().unwrap().path_id;
        client.debug_maintain();
        let to = st.unacked.lock().unwrap().values().next().unwrap().path_id;
        assert_ne!(
            to, from,
            "20 ms floor must rehome off poisoned nsix onto soy"
        );
        drop(tun);
        client.shutdown();
    }

    #[tokio::test]
    async fn retry_after_only_dest_does_not_replace() {
        let client = Session::new_client(SessionConfig::default());
        let p = inject_named(&client, 1, "only#0", 7);
        p.rtt_ewma_us.store(180_000, Ordering::Relaxed);
        let mut tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        tun.write_all(b"hello").await.unwrap();
        let st = {
            let g = client.inner.streams.lock().unwrap();
            g.values().next().unwrap().clone()
        };
        let deadline = Instant::now() + Duration::from_millis(200);
        loop {
            if !st.unacked.lock().unwrap().is_empty() {
                break;
            }
            assert!(Instant::now() < deadline, "write must leave unacked");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        {
            let mut u = st.unacked.lock().unwrap();
            for x in u.values_mut() {
                x.last_sent = Instant::now() - Duration::from_millis(30);
            }
        }
        let from = st.unacked.lock().unwrap().values().next().unwrap().path_id;
        let hedge0 = client.snapshot().data_hedge;
        client.debug_maintain();
        let to = st.unacked.lock().unwrap().values().next().unwrap().path_id;
        assert_eq!(to, from, "only dest must keep the in-flight copy");
        assert_eq!(client.snapshot().data_hedge, hedge0);
        drop(tun);
        client.shutdown();
    }

    #[tokio::test]
    async fn retry_after_two_slow_does_not_rotate_at_floor() {
        let client = Session::new_client(SessionConfig::default());
        let a = inject_named(&client, 1, "a#0", 7);
        let b = inject_named(&client, 2, "b#0", 7);
        a.rtt_ewma_us.store(180_000, Ordering::Relaxed);
        b.rtt_ewma_us.store(180_000, Ordering::Relaxed);
        let mut tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        tun.write_all(b"hello").await.unwrap();
        let st = {
            let g = client.inner.streams.lock().unwrap();
            g.values().next().unwrap().clone()
        };
        let deadline = Instant::now() + Duration::from_millis(200);
        loop {
            if !st.unacked.lock().unwrap().is_empty() {
                break;
            }
            assert!(Instant::now() < deadline, "write must leave unacked");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        {
            let mut u = st.unacked.lock().unwrap();
            for x in u.values_mut() {
                x.last_sent = Instant::now() - Duration::from_millis(30);
            }
        }
        let from = st.unacked.lock().unwrap().values().next().unwrap().path_id;
        client.debug_maintain();
        let to = st.unacked.lock().unwrap().values().next().unwrap().path_id;
        assert_eq!(
            to, from,
            "two 180 ms dests must wait loss_timeout(180ms), not rotate at 20 ms"
        );
        drop(tun);
        client.shutdown();
    }

    #[tokio::test]
    async fn single_path_silence_still_downs_without_degraded() {
        let client = Session::new_client(SessionConfig::default());
        let p = inject_known_path(&client, 1);
        age_rx(&p, 400);
        let before_d = client.snapshot().path_degraded;
        let before_n = client.snapshot().path_down;
        client.debug_maintain();
        assert_eq!(client.snapshot().path_degraded, before_d);
        assert_eq!(client.snapshot().path_down, before_n + 1);
        assert!(!client.inner.paths.lock().unwrap().contains_key(&1));
        client.shutdown();
    }

    #[tokio::test]
    async fn n2_both_silent_tears() {
        let client = Session::new_client(SessionConfig::default());
        let a = inject_named(&client, 1, "a#0", 7);
        let b = inject_named(&client, 2, "b#0", 7);
        age_rx(&a, 400);
        age_rx(&b, 400);
        let before_d = client.snapshot().path_degraded;
        let before_n = client.snapshot().path_down;
        client.debug_maintain();
        assert_eq!(client.snapshot().path_degraded, before_d);
        assert_eq!(client.snapshot().path_down, before_n + 2);
        client.shutdown();
    }

    #[tokio::test]
    async fn n2_one_silent_downs() {
        let client = Session::new_client(SessionConfig::default());
        let a = inject_named(&client, 1, "a#0", 7);
        inject_named(&client, 2, "b#0", 7);
        age_rx(&a, 400);
        let before_d = client.snapshot().path_degraded;
        let before_n = client.snapshot().path_down;
        client.debug_maintain();
        assert_eq!(client.snapshot().path_degraded, before_d);
        assert_eq!(client.snapshot().path_down, before_n + 1);
        client.shutdown();
    }

    #[tokio::test]
    async fn n4_three_silent_migrates_without_path_down() {
        let client = Session::new_client(SessionConfig::default());
        let a0 = inject_named(&client, 1, "a#0", 7);
        inject_named(&client, 2, "a#1", 7);
        let b0 = inject_named(&client, 3, "b#0", 7);
        let b1 = inject_named(&client, 4, "b#1", 7);
        let tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let sid = {
            let g = client.inner.streams.lock().unwrap();
            g.values().next().unwrap().id
        };
        client.set_sticky(sid, 1);
        age_rx(&a0, 400);
        age_rx(&b0, 400);
        age_rx(&b1, 400);
        let before_n = client.snapshot().path_down;
        let before_m = client.snapshot().migrates_speculative;
        client.debug_maintain();
        assert_eq!(client.snapshot().path_down, before_n);
        let _ = before_m;
        assert!(
            client.inner.paths.lock().unwrap().contains_key(&1),
            "silent path must be held, not torn, while one peer is still up"
        );
        drop(tun);
        client.shutdown();
    }

    #[tokio::test]
    async fn n4_all_silent_tears() {
        let client = Session::new_client(SessionConfig::default());
        let ps: Vec<_> = (1..=4)
            .map(|i| inject_named(&client, i, &format!("p{i}"), 7))
            .collect();
        for p in &ps {
            age_rx(p, 400);
        }
        let before_d = client.snapshot().path_degraded;
        let before_n = client.snapshot().path_down;
        client.debug_maintain();
        assert_eq!(client.snapshot().path_degraded, before_d);
        assert_eq!(client.snapshot().path_down, before_n + 4);
        client.shutdown();
    }

    #[tokio::test]
    async fn unknown_rtt_still_tears() {
        let client = Session::new_client(SessionConfig::default());
        let (tx, _rx) = mpsc::channel(8);
        let p = PathState::new(1, "a#0".into(), tx);
        client.inner.paths.lock().unwrap().insert(1, p.clone());
        inject_named(&client, 2, "b#0", 7);
        age_rx(&p, 600);
        let before_n = client.snapshot().path_down;
        client.debug_maintain();
        assert_eq!(client.snapshot().path_down, before_n + 1);
        client.shutdown();
    }

    #[tokio::test]
    async fn correlated_budget_tears_silent_keeps_up() {
        let cfg = SessionConfig {
            all_down_timeout: Duration::ZERO,
            ..SessionConfig::default()
        };
        let client = Session::new_client(cfg);
        let a0 = inject_named(&client, 1, "a#0", 7);
        inject_named(&client, 2, "a#1", 7);
        let b0 = inject_named(&client, 3, "b#0", 7);
        let b1 = inject_named(&client, 4, "b#1", 7);
        age_rx(&a0, 400);
        age_rx(&b0, 400);
        age_rx(&b1, 400);
        let before_n = client.snapshot().path_down;
        client.debug_maintain();
        assert_eq!(client.snapshot().path_down, before_n + 3);
        assert!(client.inner.paths.lock().unwrap().contains_key(&2));
        assert_eq!(client.snapshot().session_all_down_resets, 0);
        client.shutdown();
    }

    #[tokio::test]
    async fn outlier_recycle_same_link_client() {
        let mut cfg = SessionConfig::default();
        cfg.tuning.stable_up_hold = Duration::ZERO;
        let client = Session::new_client(cfg);
        let bad = inject_named(&client, 1, "soy#0", 7);
        bad.rtt_class_us.store(227_000, Ordering::Relaxed);
        bad.rtt_ewma_us.store(227_000, Ordering::Relaxed);
        bad.stable_up_hold_us
            .store(1_000_000_000, Ordering::Relaxed);
        inject_named(&client, 2, "soy#1", 7);
        inject_named(&client, 3, "akcdn#0", 7);
        let before = client.snapshot().path_outlier_recycle;
        client.debug_maintain();
        assert_eq!(client.snapshot().path_outlier_recycle, before + 1);
        assert!(!client.inner.paths.lock().unwrap().contains_key(&1));
        client.shutdown();
    }

    #[tokio::test]
    async fn outlier_recycle_not_on_server() {
        let mut cfg = SessionConfig::default();
        cfg.tuning.stable_up_hold = Duration::ZERO;
        let (server, _inc) = Session::new_server(cfg);
        let bad = inject_named(&server, 1, "soy#0", 7);
        bad.rtt_class_us.store(227_000, Ordering::Relaxed);
        bad.rtt_ewma_us.store(227_000, Ordering::Relaxed);
        inject_named(&server, 2, "soy#1", 7);
        server.debug_maintain();
        assert_eq!(server.snapshot().path_outlier_recycle, 0);
        assert!(server.inner.paths.lock().unwrap().contains_key(&1));
        server.shutdown();
    }

    #[tokio::test]
    async fn outlier_recycle_ignores_other_link() {
        let mut cfg = SessionConfig::default();
        cfg.tuning.stable_up_hold = Duration::ZERO;
        let client = Session::new_client(cfg);
        let slow = inject_named(&client, 1, "far#0", 227);
        slow.rtt_class_us.store(227_000, Ordering::Relaxed);
        inject_named(&client, 2, "near#0", 7);
        client.debug_maintain();
        assert_eq!(client.snapshot().path_outlier_recycle, 0);
        assert!(client.inner.paths.lock().unwrap().contains_key(&1));
        client.shutdown();
    }

    #[tokio::test]
    async fn outlier_recycle_young_class_waits_hold() {
        let mut cfg = SessionConfig::default();
        cfg.tuning.stable_up_hold = Duration::from_millis(50);
        let client = Session::new_client(cfg);
        let bad = inject_named(&client, 1, "soy#0", 7);
        bad.rtt_class_us.store(227_000, Ordering::Relaxed);
        bad.rtt_ewma_us.store(227_000, Ordering::Relaxed);
        bad.note_class_known_now();
        inject_named(&client, 2, "soy#1", 7);
        let before = client.snapshot().path_outlier_recycle;
        client.debug_maintain();
        assert_eq!(client.snapshot().path_outlier_recycle, before);
        assert!(client.inner.paths.lock().unwrap().contains_key(&1));
        assert!(
            bad.outlier_since_for_test().is_none(),
            "age floor must clear_outlier, not start the backup timer"
        );
        bad.backdate_class_known(Duration::from_millis(50));
        client.debug_maintain();
        assert_eq!(
            client.snapshot().path_outlier_recycle,
            before,
            "backup timer starts only after class-known age; must not recycle yet"
        );
        assert!(client.inner.paths.lock().unwrap().contains_key(&1));
        bad.backdate_outlier(Duration::from_millis(50));
        client.debug_maintain();
        assert_eq!(client.snapshot().path_outlier_recycle, before + 1);
        assert!(!client.inner.paths.lock().unwrap().contains_key(&1));
        client.shutdown();
    }

    #[tokio::test]
    async fn outlier_skips_recovered_fast() {
        let mut cfg = SessionConfig::default();
        cfg.tuning.stable_up_hold = Duration::ZERO;
        let client = Session::new_client(cfg);
        let bad = inject_named(&client, 1, "soy#0", 7);
        bad.rtt_class_us.store(227_000, Ordering::Relaxed);
        inject_named(&client, 2, "soy#1", 7);
        let before = client.snapshot().path_outlier_recycle;
        client.debug_maintain();
        assert_eq!(client.snapshot().path_outlier_recycle, before);
        assert!(client.inner.paths.lock().unwrap().contains_key(&1));
        assert!(
            bad.outlier_since_for_test().is_none(),
            "recovered fast must clear_outlier, not start the backup timer"
        );
        client.shutdown();
    }

    #[tokio::test]
    async fn outlier_clears_when_fast_recovers_mid_hold() {
        let mut cfg = SessionConfig::default();
        cfg.tuning.stable_up_hold = Duration::from_millis(50);
        let client = Session::new_client(cfg);
        let bad = inject_named(&client, 1, "soy#0", 7);
        bad.rtt_class_us.store(227_000, Ordering::Relaxed);
        bad.rtt_ewma_us.store(227_000, Ordering::Relaxed);
        inject_named(&client, 2, "soy#1", 7);
        let before = client.snapshot().path_outlier_recycle;
        bad.backdate_class_known(Duration::from_millis(50));
        client.debug_maintain();
        assert_eq!(client.snapshot().path_outlier_recycle, before);
        assert!(client.inner.paths.lock().unwrap().contains_key(&1));
        assert!(
            bad.outlier_since_for_test().is_some(),
            "both clocks backup after age must start the timer"
        );
        bad.rtt_ewma_us.store(7_000, Ordering::Relaxed);
        client.debug_maintain();
        assert_eq!(client.snapshot().path_outlier_recycle, before);
        assert!(client.inner.paths.lock().unwrap().contains_key(&1));
        assert!(
            bad.outlier_since_for_test().is_none(),
            "fast recovered under the cliff must clear_outlier mid-hold"
        );
        client.shutdown();
    }

    #[tokio::test]
    async fn n4_three_quiet_sequential_holds_until_budget() {
        let client = Session::new_client(SessionConfig::default());
        let a0 = inject_named(&client, 1, "a#0", 7);
        inject_named(&client, 2, "a#1", 7);
        let b0 = inject_named(&client, 3, "b#0", 7);
        let b1 = inject_named(&client, 4, "b#1", 7);
        let tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let sid = {
            let g = client.inner.streams.lock().unwrap();
            g.values().next().unwrap().id
        };
        client.set_sticky(sid, 1);
        age_rx(&a0, 400);
        age_rx(&b0, 80);
        age_rx(&b1, 80);
        let before_n = client.snapshot().path_down;
        let before_c = client.snapshot().correlated_silence;
        let before_m = client.snapshot().migrates_speculative;
        client.debug_maintain();
        assert_eq!(
            client.snapshot().path_down,
            before_n,
            "A past down_for must be held when B/C are quiet"
        );
        assert_eq!(client.snapshot().correlated_silence, before_c + 1);
        let _ = before_m;
        assert!(
            client.inner.paths.lock().unwrap().contains_key(&1),
            "A past down_for must remain in the pool while correlated"
        );
        age_rx(&b0, 400);
        age_rx(&b1, 400);
        client.debug_maintain();
        assert_eq!(client.snapshot().path_down, before_n);
        assert!(client.inner.paths.lock().unwrap().contains_key(&2));
        drop(tun);
        client.shutdown();
    }

    #[tokio::test]
    async fn n4_three_quiet_no_down_for_does_not_hold() {
        let client = Session::new_client(SessionConfig::default());
        let a0 = inject_named(&client, 1, "a#0", 7);
        inject_named(&client, 2, "a#1", 7);
        let b0 = inject_named(&client, 3, "b#0", 7);
        let b1 = inject_named(&client, 4, "b#1", 7);
        age_rx(&a0, 80);
        age_rx(&b0, 80);
        age_rx(&b1, 80);
        let before_n = client.snapshot().path_down;
        let before_c = client.snapshot().correlated_silence;
        client.debug_maintain();
        assert_eq!(client.snapshot().path_down, before_n);
        assert_eq!(
            client.snapshot().correlated_silence,
            before_c,
            "3-of-4 at degrade_for with nobody at down_for must not enter"
        );
        client.shutdown();
    }

    #[tokio::test]
    async fn n4_quiet_recovers_before_down_for_tears() {
        let client = Session::new_client(SessionConfig::default());
        let a0 = inject_named(&client, 1, "a#0", 7);
        inject_named(&client, 2, "a#1", 7);
        let b0 = inject_named(&client, 3, "b#0", 7);
        let b1 = inject_named(&client, 4, "b#1", 7);
        age_rx(&a0, 80);
        age_rx(&b0, 80);
        age_rx(&b1, 80);
        client.debug_maintain();
        assert_eq!(client.snapshot().correlated_silence, 0);
        b0.touch_rx();
        age_rx(&a0, 400);
        let before_n = client.snapshot().path_down;
        client.debug_maintain();
        assert_eq!(client.snapshot().path_down, before_n + 1);
        assert!(!client.inner.paths.lock().unwrap().contains_key(&1));
        client.shutdown();
    }

    #[tokio::test]
    async fn n4_correlated_falling_edge_tears_silent() {
        let client = Session::new_client(SessionConfig::default());
        let a0 = inject_named(&client, 1, "a#0", 7);
        inject_named(&client, 2, "a#1", 7);
        let b0 = inject_named(&client, 3, "b#0", 7);
        let b1 = inject_named(&client, 4, "b#1", 7);
        age_rx(&a0, 400);
        age_rx(&b0, 80);
        age_rx(&b1, 80);
        client.debug_maintain();
        assert_eq!(client.snapshot().correlated_silence, 1);
        assert!(client.inner.paths.lock().unwrap().contains_key(&1));
        b1.touch_rx();
        let before_n = client.snapshot().path_down;
        client.debug_maintain();
        assert_eq!(
            client.snapshot().path_down,
            before_n + 1,
            "falling edge must tear A already past down_for"
        );
        assert!(!client.inner.paths.lock().unwrap().contains_key(&1));
        client.shutdown();
    }

    #[tokio::test]
    async fn n6_four_cross_link_holds() {
        let client = Session::new_client(SessionConfig::default());
        let a0 = inject_named(&client, 1, "akcdn#0", 7);
        let a1 = inject_named(&client, 2, "akcdn#1", 7);
        let s0 = inject_named(&client, 3, "soy#0", 7);
        let s1 = inject_named(&client, 4, "soy#1", 7);
        inject_named(&client, 5, "nsix#0", 7);
        inject_named(&client, 6, "nsix#1", 7);
        age_rx(&a0, 400);
        age_rx(&a1, 400);
        age_rx(&s0, 400);
        age_rx(&s1, 400);
        let before_n = client.snapshot().path_down;
        let before_c = client.snapshot().correlated_silence;
        client.debug_maintain();
        assert_eq!(
            client.snapshot().path_down,
            before_n,
            "4-of-6 across two named links must hold, not reconnect-storm"
        );
        assert_eq!(client.snapshot().correlated_silence, before_c + 1);
        for id in 1..=6 {
            assert!(
                client.inner.paths.lock().unwrap().contains_key(&id),
                "path {id} must remain in the pool"
            );
        }
        client.shutdown();
    }

    #[tokio::test]
    async fn n6_three_cross_link_holds() {
        let client = Session::new_client(SessionConfig::default());
        let a0 = inject_named(&client, 1, "akcdn#0", 7);
        inject_named(&client, 2, "akcdn#1", 7);
        let s0 = inject_named(&client, 3, "soy#0", 7);
        inject_named(&client, 4, "soy#1", 7);
        let n0 = inject_named(&client, 5, "nsix#0", 7);
        inject_named(&client, 6, "nsix#1", 7);
        age_rx(&a0, 400);
        age_rx(&s0, 400);
        age_rx(&n0, 400);
        let before_n = client.snapshot().path_down;
        let before_c = client.snapshot().correlated_silence;
        client.debug_maintain();
        assert_eq!(
            client.snapshot().path_down,
            before_n,
            "one silent dest per named link is a cross-link cluster, not three independent deaths"
        );
        assert_eq!(client.snapshot().correlated_silence, before_c + 1);
        client.shutdown();
    }

    #[tokio::test]
    async fn n6_two_same_link_tears() {
        let client = Session::new_client(SessionConfig::default());
        let a0 = inject_named(&client, 1, "akcdn#0", 7);
        let a1 = inject_named(&client, 2, "akcdn#1", 7);
        inject_named(&client, 3, "soy#0", 7);
        inject_named(&client, 4, "soy#1", 7);
        inject_named(&client, 5, "nsix#0", 7);
        inject_named(&client, 6, "nsix#1", 7);
        age_rx(&a0, 400);
        age_rx(&a1, 400);
        let before_n = client.snapshot().path_down;
        let before_c = client.snapshot().correlated_silence;
        client.debug_maintain();
        assert_eq!(
            client.snapshot().path_down,
            before_n + 2,
            "one named link (H8) must still tear at down_for"
        );
        assert_eq!(client.snapshot().correlated_silence, before_c);
        client.shutdown();
    }

    #[tokio::test]
    async fn snapshot_marks_backup_flag() {
        let client = Session::new_client(SessionConfig::default());
        inject_named(&client, 1, "a#0", 7);
        inject_named(&client, 2, "b#0", 80);
        let snap = client.snapshot();
        let a = snap.paths.iter().find(|p| p.name == "a#0").unwrap();
        let b = snap.paths.iter().find(|p| p.name == "b#0").unwrap();
        assert!(!a.backup);
        assert!(b.backup);
        client.shutdown();
    }

    #[tokio::test]
    async fn stale_ping_degrades_even_if_young_ping_remains() {
        let client = Session::new_client(SessionConfig::default());
        let p = inject_known_path(&client, 1);
        p.next_ping();
        tokio::time::sleep(Duration::from_millis(12)).await;
        p.next_ping();
        age_rx(&p, 60);
        tokio::time::sleep(Duration::from_millis(15)).await;
        client.debug_maintain();
        assert_eq!(
            p.state.load(Ordering::Relaxed),
            crate::path::STATE_DEGRADED,
            "older ping past loss_timeout must degrade even with a young ping left"
        );
        client.shutdown();
    }

    #[tokio::test]
    async fn failed_retry_does_not_hedge_or_tear() {
        let client = Session::new_client(SessionConfig::default());
        let (_a, _aw, _au) = inject_live_cap(&client, 1, "a#0", 7, 8);
        let (_b, _bw, _bu) = inject_live_cap(&client, 2, "b#0", 7, 8);
        let mut tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        tun.write_all(b"hello").await.unwrap();
        let st = {
            let g = client.inner.streams.lock().unwrap();
            g.values().next().unwrap().clone()
        };
        let deadline = Instant::now() + Duration::from_millis(200);
        loop {
            if !st.unacked.lock().unwrap().is_empty() {
                break;
            }
            assert!(Instant::now() < deadline, "write must leave unacked");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        fill_urgent(&client, 1);
        fill_urgent(&client, 2);
        let aged = Instant::now() - Duration::from_millis(100);
        let from = {
            let mut u = st.unacked.lock().unwrap();
            for x in u.values_mut() {
                x.last_sent = aged;
                x.retry_not_before = Instant::now() - Duration::from_millis(100);
            }
            u.values().next().unwrap().path_id
        };
        let hedge0 = client.snapshot().data_hedge;
        let down0 = client.snapshot().path_down;
        client.debug_maintain();
        let (last, pid) = {
            let u = st.unacked.lock().unwrap();
            let got = u.values().next().unwrap();
            (got.last_sent, got.path_id)
        };
        assert_eq!(last, aged, "failed send must not bump last_sent");
        assert_eq!(pid, from);
        assert_eq!(client.snapshot().data_hedge, hedge0);
        assert_eq!(client.snapshot().path_down, down0);
        assert!(client.get_path(1).is_some());
        assert!(client.get_path(2).is_some());
        drop(tun);
        client.shutdown();
    }

    #[tokio::test]
    async fn urgent_full_on_one_leaves_five_up() {
        let client = Session::new_client(SessionConfig::default());
        let mut held = Vec::new();
        for i in 1..=6u32 {
            held.push(inject_live_cap(&client, i, &format!("p{i}#0"), 7, 8));
        }
        fill_urgent(&client, 1);
        assert!(held[0].0.is_congested());
        let down0 = client.snapshot().path_down;
        assert!(!client.send_on_path(
            1,
            Frame::Ping(nya_proto::Ping {
                seq: 9,
                sent_at_ms: 0,
            })
        ));
        assert_eq!(client.snapshot().path_down, down0);
        assert_eq!(client.alive_path_count(), 6);
        for i in 2..=6u32 {
            assert!(client.get_path(i).unwrap().is_alive());
            assert!(!client.get_path(i).unwrap().is_congested());
        }
        drop(held);
        client.shutdown();
    }

    #[tokio::test]
    async fn path_failed_during_blocked_write_completes_add_path() {
        let client = Session::new_client(SessionConfig::default());
        let (a, _peer) = duplex(8);
        let done = client.start_path("p1".into(), a);
        let id = *client.inner.paths.lock().unwrap().keys().next().unwrap();
        let ping = Frame::Ping(nya_proto::Ping {
            seq: 1,
            sent_at_ms: 0,
        });
        for _ in 0..80 {
            let _ = client.send_on_path(id, ping.clone());
        }
        client.path_failed(id);
        tokio::time::timeout(Duration::from_millis(500), done)
            .await
            .expect("add_path must complete after path_failed while write blocked")
            .expect("oneshot");
        client.shutdown();
    }

    #[tokio::test]
    async fn write_stall_does_not_tear_blocked_path() {
        let client = Session::new_client(SessionConfig::default());
        let (a, _peer) = duplex(8);
        let done = client.start_path("stall".into(), a);
        let stall_id = *client.inner.paths.lock().unwrap().keys().next().unwrap();
        client
            .get_path(stall_id)
            .unwrap()
            .record_rtt(Duration::from_millis(7));
        age_path(&client.get_path(stall_id).unwrap(), Duration::from_secs(1));
        let _live = inject_live(&client, 99, "live#0", 7);
        let ping = Frame::Ping(nya_proto::Ping {
            seq: 1,
            sent_at_ms: 0,
        });
        for _ in 0..80 {
            let _ = client.send_on_path(stall_id, ping.clone());
        }
        tokio::time::sleep(Duration::from_millis(80)).await;
        let stall = client
            .get_path(stall_id)
            .expect("blocked writer must stay up");
        assert!(stall.is_alive(), "write stall is not path_failed");
        assert!(
            stall.is_write_stalled(),
            "deadline on known dest marks write_stalled"
        );
        assert!(!stall.is_schedulable());
        assert!(client.get_path(99).is_some(), "sibling dest must stay up");
        assert_eq!(client.snapshot().path_down, 0);
        drop(done);
        client.shutdown();
    }

    #[tokio::test]
    async fn write_deadline_unknown_dest_is_unknown_degrade_min() {
        let client = Session::new_client(SessionConfig::default());
        let (a, _peer) = duplex(64);
        let _done = client.start_path("new".into(), a);
        let id = *client.inner.paths.lock().unwrap().keys().next().unwrap();
        let _live = inject_live(&client, 99, "live#0", 7);
        assert_eq!(
            client.write_deadline(id),
            client.inner.cfg.tuning.unknown_degrade_min
        );
        client.shutdown();
    }

    #[tokio::test]
    async fn write_deadline_known_fast_pool_is_floor() {
        let client = Session::new_client(SessionConfig::default());
        let (a, _peer) = duplex(64);
        let _done = client.start_path("known".into(), a);
        let id = *client.inner.paths.lock().unwrap().keys().next().unwrap();
        client
            .get_path(id)
            .unwrap()
            .record_rtt(Duration::from_millis(7));
        age_path(&client.get_path(id).unwrap(), Duration::from_secs(1));
        assert_eq!(
            client.write_deadline(id),
            client.inner.cfg.tuning.loss_timeout_floor
        );
        client.shutdown();
    }

    #[tokio::test]
    async fn write_deadline_just_joined_known_rtt_is_unknown_degrade_min() {
        let client = Session::new_client(SessionConfig::default());
        let (a, _peer) = duplex(64);
        let _done = client.start_path("new".into(), a);
        let id = *client.inner.paths.lock().unwrap().keys().next().unwrap();
        client
            .get_path(id)
            .unwrap()
            .record_rtt(Duration::from_millis(7));
        let _live = inject_live(&client, 99, "live#0", 7);
        assert_eq!(
            client.write_deadline(id),
            client.inner.cfg.tuning.unknown_degrade_min,
            "first pong does not make a just-joined dest a 20ms writer"
        );
        client.shutdown();
    }

    #[tokio::test]
    async fn write_stall_unknown_just_joined_does_not_stall_at_20ms() {
        let client = Session::new_client(SessionConfig::default());
        let (a, _peer) = duplex(8);
        let done = client.start_path("new".into(), a);
        let id = *client.inner.paths.lock().unwrap().keys().next().unwrap();
        let _live = inject_live(&client, 99, "live#0", 7);
        let ping = Frame::Ping(nya_proto::Ping {
            seq: 1,
            sent_at_ms: 0,
        });
        for _ in 0..80 {
            let _ = client.send_on_path(id, ping.clone());
        }
        tokio::time::sleep(Duration::from_millis(80)).await;
        let p = client.get_path(id).expect("just-joined dest stays up");
        assert!(p.is_alive());
        assert!(
            !p.is_write_stalled(),
            "unknown dest write_deadline is 300ms, not 20ms"
        );
        assert_eq!(client.snapshot().path_down, 0);
        drop(done);
        client.shutdown();
    }

    #[tokio::test]
    async fn write_stall_just_joined_after_pong_does_not_stall_at_20ms() {
        let client = Session::new_client(SessionConfig::default());
        let (a, _peer) = duplex(8);
        let done = client.start_path("new".into(), a);
        let id = *client.inner.paths.lock().unwrap().keys().next().unwrap();
        client
            .get_path(id)
            .unwrap()
            .record_rtt(Duration::from_millis(7));
        let _live = inject_live(&client, 99, "live#0", 7);
        let ping = Frame::Ping(nya_proto::Ping {
            seq: 1,
            sent_at_ms: 0,
        });
        for _ in 0..80 {
            let _ = client.send_on_path(id, ping.clone());
        }
        tokio::time::sleep(Duration::from_millis(80)).await;
        let p = client.get_path(id).expect("just-joined dest stays up");
        assert!(p.is_alive());
        assert!(
            !p.is_write_stalled(),
            "just-joined dest keeps 300ms write_deadline after first pong"
        );
        assert_eq!(client.snapshot().path_down, 0);
        drop(done);
        client.shutdown();
    }

    #[tokio::test]
    async fn unknown_dest_parks_stream_data_and_does_not_stall() {
        let client = Session::new_client(SessionConfig::default());
        let (a, _peer) = duplex(8);
        let done = client.start_path("new".into(), a);
        let id = *client.inner.paths.lock().unwrap().keys().next().unwrap();
        let _live = inject_live(&client, 99, "live#0", 7);
        let data = Frame::StreamData(StreamData {
            stream_id: 1,
            offset: 0,
            data: vec![0; 64],
        });
        for _ in 0..80 {
            let _ = client.send_on_path(id, data.clone());
        }
        tokio::time::sleep(Duration::from_millis(80)).await;
        let p = client.get_path(id).expect("unknown dest stays up");
        assert!(p.is_alive());
        assert!(
            !p.is_write_stalled(),
            "STREAM_DATA must not pin a dest before first pong"
        );
        assert_eq!(client.snapshot().path_down, 0);
        drop(done);
        client.shutdown();
    }

    #[tokio::test]
    async fn send_on_path_data_enqueues_while_write_stalled() {
        let client = Session::new_client(SessionConfig::default());
        let (p, mut wrx, mut urx) = inject_live(&client, 1, "a#0", 7);
        p.set_write_stalled(true);
        let data = Frame::StreamData(StreamData {
            stream_id: 1,
            offset: 0,
            data: vec![1; 64],
        });
        assert!(
            client.send_on_path(1, data.clone()),
            "known+stalled DATA must enqueue onto bulk"
        );
        assert!(
            urx.try_recv().is_err(),
            "stalled DATA must not occupy urgent"
        );
        match wrx.try_recv() {
            Ok(Frame::StreamData(d)) => assert_eq!(d.stream_id, 1),
            other => panic!("expected bulk STREAM_DATA, got {other:?}"),
        }
        assert!(p.is_alive());
        assert_eq!(client.snapshot().path_down, 0);
        client.shutdown();
    }

    #[tokio::test]
    async fn write_stall_on_one_of_six_leaves_six_up() {
        let client = Session::new_client(SessionConfig::default());
        let mut peers = Vec::new();
        let mut dones = Vec::new();
        for i in 0..6 {
            // Stall dest uses a tiny buffer so send_frame blocks; siblings
            // must still run spawn_path_io (not inject_live) but need room
            // for maintain pings so they are not themselves write_stalled.
            let cap = if i == 0 { 8 } else { 64 * 1024 };
            let (a, peer) = duplex(cap);
            dones.push(client.start_path(format!("p{i}#0"), a));
            peers.push(peer);
        }
        let ids: Vec<u32> = {
            let paths = client.inner.paths.lock().unwrap();
            let mut v: Vec<_> = paths.keys().copied().collect();
            v.sort();
            v
        };
        assert_eq!(ids.len(), 6);
        for &id in &ids {
            client
                .get_path(id)
                .unwrap()
                .record_rtt(Duration::from_millis(7));
        }
        let stall_id = ids[0];
        age_path(&client.get_path(stall_id).unwrap(), Duration::from_secs(1));
        let ping = Frame::Ping(nya_proto::Ping {
            seq: 1,
            sent_at_ms: 0,
        });
        for _ in 0..80 {
            let _ = client.send_on_path(stall_id, ping.clone());
        }
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(client.alive_path_count(), 6);
        assert_eq!(client.snapshot().path_down, 0);
        assert!(client.get_path(stall_id).unwrap().is_write_stalled());
        for &id in &ids[1..] {
            assert!(!client.get_path(id).unwrap().is_write_stalled());
            assert!(client.get_path(id).unwrap().is_alive());
        }
        drop(dones);
        drop(peers);
        client.shutdown();
    }

    #[tokio::test]
    async fn write_stall_still_dequeues_bulk() {
        let client = Session::new_client(SessionConfig::default());
        let (a, mut peer) = duplex(64 * 1024);
        let _done = client.start_path("stall".into(), a);
        let id = *client.inner.paths.lock().unwrap().keys().next().unwrap();
        client
            .get_path(id)
            .unwrap()
            .record_rtt(Duration::from_millis(7));
        age_path(&client.get_path(id).unwrap(), Duration::from_secs(1));
        client.get_path(id).unwrap().set_write_stalled(true);
        let data = Frame::StreamData(StreamData {
            stream_id: 1,
            offset: 0,
            data: vec![7; 1600],
        });
        assert!(client.send_on_path(id, data));
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let f = tokio::time::timeout(
                deadline.saturating_duration_since(Instant::now()),
                read_len_frame(&mut peer),
            )
            .await
            .expect("stalled writer must still dequeue bulk");
            match f {
                Frame::StreamData(d) => {
                    assert_eq!(d.stream_id, 1);
                    break;
                }
                Frame::Ping(_) | Frame::StreamAck(_) => {
                    assert!(Instant::now() < deadline, "bulk DATA never flushed");
                }
                other => panic!("unexpected frame {other:?}"),
            }
        }
        client.shutdown();
    }

    #[tokio::test]
    async fn stalled_urgent_bulk_full_does_not_drop_copy() {
        let client = Session::new_client(SessionConfig::default());
        let (p, _wrx, _urx) = inject_live_cap(&client, 1, "a#0", 7, 4);
        let mut tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        tun.write_all(b"hello").await.unwrap();
        let st = {
            let g = client.inner.streams.lock().unwrap();
            g.values().next().unwrap().clone()
        };
        let deadline = Instant::now() + Duration::from_millis(200);
        loop {
            if !st.unacked.lock().unwrap().is_empty() {
                break;
            }
            assert!(Instant::now() < deadline, "write must leave unacked");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        fill_urgent(&client, 1);
        fill_bulk(&client, 1);
        p.set_write_stalled(true);
        let (last, offset) = {
            let u = st.unacked.lock().unwrap();
            let (off, got) = u.iter().next().unwrap();
            (got.last_sent, *off)
        };
        let extra = Frame::StreamData(StreamData {
            stream_id: st.id,
            offset: 99_000,
            data: vec![1; 64],
        });
        assert!(
            !client.send_on_path(1, extra),
            "stalled+full bulk must return false, not drop"
        );
        let u = st.unacked.lock().unwrap();
        let got = u.get(&offset).expect("copy must stay in HashMap");
        assert_eq!(
            got.last_sent, last,
            "failed enqueue must not bump last_sent"
        );
        drop(u);
        drop(tun);
        client.shutdown();
    }

    #[tokio::test]
    async fn bulk_send_blocked_waits_does_not_migrate() {
        let client = Session::new_client(SessionConfig::default());
        let (_p1, _w1, _u1) = inject_live_cap(&client, 1, "a#0", 7, 4);
        let (p2, _w2, _u2) = inject_live(&client, 2, "b#0", 7);
        p2.add_inflight(10 * 1024 * 1024);
        let mut tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        fill_bulk(&client, 1);
        let mig0 = client.snapshot().migrates_send_blocked;
        let buf = vec![0x5au8; 2000];
        let write = tokio::spawn(async move { tun.write_all(&buf).await });
        let deadline = Instant::now() + Duration::from_millis(500);
        let st = loop {
            let found = {
                let g = client.inner.streams.lock().unwrap();
                g.values()
                    .next()
                    .cloned()
                    .filter(|st| !st.unacked.lock().unwrap().is_empty())
            };
            if let Some(st) = found {
                break st;
            }
            assert!(Instant::now() < deadline, "bulk write must leave unacked");
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        let from = st.unacked.lock().unwrap().values().next().unwrap().path_id;
        assert_eq!(from, 1, "load must pin bulk onto the full dest");
        st.send_wait.notify_waiters();
        client.get_path(1).unwrap().queue_wait.notify_one();
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(
            client.snapshot().migrates_send_blocked,
            mig0,
            "C4 must wait, not spray, before down_timeout"
        );
        assert_eq!(
            st.unacked.lock().unwrap().values().next().unwrap().path_id,
            1
        );
        write.abort();
        client.shutdown();
    }

    #[tokio::test]
    async fn c4_wait_pins_retry_not_before() {
        let client = Session::new_client(SessionConfig::default());
        let (p1, _w1, _u1) = inject_live_cap(&client, 1, "a#0", 7, 4);
        let (p2, _w2, _u2) = inject_live(&client, 2, "b#0", 7);
        p2.add_inflight(10 * 1024 * 1024);
        let mut tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        fill_bulk(&client, 1);
        let buf = vec![0x5au8; 2000];
        let write = tokio::spawn(async move { tun.write_all(&buf).await });
        let deadline = Instant::now() + Duration::from_millis(500);
        let st = loop {
            let found = {
                let g = client.inner.streams.lock().unwrap();
                g.values().next().cloned().filter(|st| {
                    st.unacked.lock().unwrap().values().any(|u| {
                        u.path_id == 1
                            && u.retry_not_before > Instant::now() + Duration::from_millis(50)
                    })
                })
            };
            if let Some(st) = found {
                break st;
            }
            assert!(
                Instant::now() < deadline,
                "C4 must pin retry_not_before to down_timeout"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        let probe = client.probe_interval_for(&p1);
        let wait = crate::health::down_timeout(&client.inner.cfg, p1.stable_rtt(), probe);
        let retry_after = client.inner.cfg.tuning.loss_timeout_floor;
        assert!(wait > retry_after);
        let pin = st
            .unacked
            .lock()
            .unwrap()
            .values()
            .find(|u| u.path_id == 1)
            .unwrap()
            .retry_not_before;
        let remain = pin.saturating_duration_since(Instant::now());
        assert!(
            remain > retry_after,
            "pin {remain:?} must exceed retry_after {retry_after:?}"
        );
        assert!(
            remain <= wait + Duration::from_millis(50),
            "pin {remain:?} must be path down_timeout {wait:?}, not all_down"
        );
        st.send_wait.notify_waiters();
        p1.queue_wait.notify_one();
        tokio::time::sleep(retry_after + Duration::from_millis(10)).await;
        let hedge0 = client.snapshot().data_hedge;
        let rtx0 = client.snapshot().data_retransmit;
        client.debug_maintain();
        assert_eq!(client.snapshot().data_hedge, hedge0);
        assert_eq!(client.snapshot().data_retransmit, rtx0);
        let pin_after = st
            .unacked
            .lock()
            .unwrap()
            .values()
            .find(|u| u.path_id == 1)
            .unwrap()
            .retry_not_before;
        assert!(
            pin_after > Instant::now(),
            "pin must survive send_wait / non-space queue_wait"
        );
        write.abort();
        client.shutdown();
    }

    #[tokio::test]
    async fn advertised_window_counts_recv_buf() {
        let client = Session::new_client(SessionConfig::default());
        let (p, _w, _u) = inject_live(&client, 1, "a#0", 7);
        let tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let st = client.get_stream(tun.id).unwrap();
        let floor = st.initial_window;
        assert_eq!(st.recv_cap.load(Ordering::Relaxed), floor);
        assert_eq!(st.advertised_window(), floor);
        client.on_data(
            p.id,
            StreamData {
                stream_id: tun.id,
                offset: 100,
                data: vec![0xab; 50],
            },
        );
        assert_eq!(
            st.recv_buf.lock().unwrap().get(&100).map(|v| v.len()),
            Some(50),
            "hole must stay in recv_buf"
        );
        assert_eq!(st.recv_buffered.load(Ordering::Relaxed), 50);
        assert_eq!(st.buffered_in.load(Ordering::Relaxed), 0);
        assert_eq!(
            st.advertised_window(),
            floor - 50,
            "advertise must shrink by recv_buf bytes"
        );
        st.buffered_in.store(10, Ordering::Relaxed);
        assert_eq!(
            st.advertised_window(),
            floor - 60,
            "must subtract buffered_in and recv_buffered, not one"
        );
        drop(tun);
        client.shutdown();
    }

    #[tokio::test]
    async fn recv_cap_grows_with_bdp_and_not_below_floor() {
        let client = Session::new_client(SessionConfig::default());
        let (p, _w, _u) = inject_live(&client, 1, "a#0", 7);
        let (_p2, _w2, _u2) = inject_live(&client, 2, "b#0", 10);
        let tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let st = client.get_stream(tun.id).unwrap();
        st.sticky.store(p.id, Ordering::Relaxed);
        let floor = st.initial_window;
        let ceil = floor.saturating_mul(client.inner.cfg.tuning.chan as u32);
        assert_eq!(st.recv_cap.load(Ordering::Relaxed), floor);
        assert_eq!(ceil, 8 * 1024 * 1024);

        // 50 MB/s × 7 ms × 2 = 700 KiB, above 128 KiB floor.
        st.deliver_rate_ewma.store(50_000_000, Ordering::Relaxed);
        client.debug_maintain();
        let cap = st.recv_cap.load(Ordering::Relaxed);
        let expect = {
            let bdp = 50_000_000.0 * Duration::from_millis(7).as_secs_f64();
            let twice = 2.0 * bdp;
            twice as u32
        };
        assert!(cap > floor, "cap {cap} must grow above floor {floor}");
        assert!(cap <= ceil, "cap {cap} must not exceed ceil {ceil}");
        assert_eq!(cap, expect);

        st.deliver_rate_ewma.store(0, Ordering::Relaxed);
        client.debug_maintain();
        assert_eq!(
            st.recv_cap.load(Ordering::Relaxed),
            floor,
            "zero rate must not grow"
        );

        st.deliver_rate_ewma.store(50_000_000, Ordering::Relaxed);
        p.rtt_ewma_us.store(0, Ordering::Relaxed);
        client.debug_maintain();
        let cap = st.recv_cap.load(Ordering::Relaxed);
        let expect_pool = {
            let bdp = 50_000_000.0 * Duration::from_millis(10).as_secs_f64();
            (2.0 * bdp) as u32
        };
        assert_eq!(
            cap, expect_pool,
            "unknown sticky RTT must use min_alive_fast, not unknown_rtt_us"
        );

        p.state.store(crate::path::STATE_DOWN, Ordering::Relaxed);
        client.inner.paths.lock().unwrap().remove(&2);
        client.debug_maintain();
        assert_eq!(
            st.recv_cap.load(Ordering::Relaxed),
            floor,
            "no alive known RTT must not grow"
        );

        p.state.store(crate::path::STATE_UP, Ordering::Relaxed);
        p.rtt_ewma_us.store(7_000, Ordering::Relaxed);
        st.deliver_rate_ewma.store(u64::MAX / 2, Ordering::Relaxed);
        client.debug_maintain();
        assert_eq!(
            st.recv_cap.load(Ordering::Relaxed),
            ceil,
            "huge rate must clamp to initial_window * chan"
        );

        drop(tun);
        client.shutdown();
    }

    #[tokio::test]
    async fn backup_pong_records_even_when_pool_has_fast_peer() {
        let client = Session::new_client(SessionConfig::default());
        let (fast, _fw, _fu) = inject_live(&client, 1, "f#0", 7);
        let (slow, _sw, _su) = inject_live(&client, 2, "b#0", 80);
        let fast_cap = client.rtt_sample_cap(&fast);
        let slow_cap = client.rtt_sample_cap(&slow);
        assert_eq!(
            fast_cap,
            Duration::from_millis(20),
            "7 ms dest stays on the loss floor"
        );
        assert!(
            slow_cap >= Duration::from_millis(100),
            "80 ms dest cap must be 2× own RTT, not the 20 ms fast-peer floor, got {slow_cap:?}"
        );

        let ping = slow.next_ping();
        slow.backdate_pending_ping(Duration::from_millis(60));
        client.handle_frame(
            2,
            Frame::Pong(nya_proto::Pong {
                seq: ping.seq,
                sent_at_ms: ping.sent_at_ms,
            }),
        );
        // (80ms×8 + ~60ms×2)/10 ≈ 76ms. A 20 ms pool cap would drop the
        // sample and leave EWMA at 80_000.
        let got = slow.rtt_us();
        assert!(
            (70_000..79_000).contains(&got),
            "backup Pong must update EWMA; got {got} us"
        );

        let before = slow.rtt_us();
        let late = slow.next_ping();
        slow.expire_stale_pings(Duration::ZERO);
        slow.backdate_pending_ping(Duration::from_millis(200));
        client.handle_frame(
            2,
            Frame::Pong(nya_proto::Pong {
                seq: late.seq,
                sent_at_ms: late.sent_at_ms,
            }),
        );
        assert_eq!(
            slow.rtt_us(),
            before,
            "known-path expired Pong must be clear-only"
        );
        client.shutdown();
    }

    #[tokio::test]
    async fn recv_cap_does_not_grow_from_back_to_back_data() {
        let client = Session::new_client(SessionConfig::default());
        let (p, _w, _u) = inject_live(&client, 1, "a#0", 7);
        let tun = client
            .open_stream(Target {
                host: "t".into(),
                port: 1,
            })
            .await
            .unwrap();
        let st = client.get_stream(tun.id).unwrap();
        st.sticky.store(p.id, Ordering::Relaxed);
        let floor = st.initial_window;
        let ceil = floor.saturating_mul(client.inner.cfg.tuning.chan as u32);
        let sid = tun.id;
        client.on_data(
            p.id,
            StreamData {
                stream_id: sid,
                offset: 0,
                data: vec![0xab; 16 * 1024],
            },
        );
        client.on_data(
            p.id,
            StreamData {
                stream_id: sid,
                offset: 16 * 1024,
                data: vec![0xcd; 16 * 1024],
            },
        );
        assert_eq!(
            st.deliver_rate_ewma.load(Ordering::Relaxed),
            0,
            "FramedRead callback gap is not a rate"
        );
        assert_eq!(
            st.recv_cap.load(Ordering::Relaxed),
            floor,
            "two in-order frames must not ACK ceil"
        );
        assert_ne!(st.recv_cap.load(Ordering::Relaxed), ceil);

        const TOTAL: u64 = 350_000;
        let already = 32 * 1024u64;
        let rtt = Duration::from_millis(7);
        let start = Instant::now().checked_sub(rtt).unwrap();
        st.debug_set_deliver_clock(start);
        client.on_data(
            p.id,
            StreamData {
                stream_id: sid,
                offset: already,
                data: vec![0xef; (TOTAL - already) as usize],
            },
        );
        let cap = st.recv_cap.load(Ordering::Relaxed);
        let dt = start.elapsed();
        let expect = {
            let rate = TOTAL as f64 / dt.as_secs_f64();
            let twice = 2.0 * rate * rtt.as_secs_f64();
            (twice as u32).clamp(floor, ceil)
        };
        assert!(cap > floor, "cap {cap} must grow after one RTT of 50 MB/s");
        assert!(cap < ceil, "cap {cap} must not be decode-speed ceil");
        assert!(
            cap.abs_diff(expect) < 2_000,
            "cap {cap} expect {expect} (2·{TOTAL}/{dt:?}·7ms), ~700 KiB class"
        );
        drop(tun);
        client.shutdown();
    }

    struct WarnCap(std::sync::Arc<Mutex<Vec<(String, String)>>>);
    impl tracing::Subscriber for WarnCap {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::Id {
            tracing::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::Id, _: &tracing::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            struct Msg(String);
            impl tracing::field::Visit for Msg {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    if field.name() == "message" {
                        self.0 = format!("{value:?}");
                    }
                }
                fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                    if field.name() == "message" {
                        self.0 = value.to_string();
                    }
                }
            }
            let mut msg = Msg(String::new());
            event.record(&mut msg);
            self.0
                .lock()
                .unwrap()
                .push((event.metadata().level().as_str().to_string(), msg.0));
        }
        fn enter(&self, _: &tracing::Id) {}
        fn exit(&self, _: &tracing::Id) {}
    }

    fn warn_read_failed(ev: &[(String, String)]) -> bool {
        ev.iter()
            .any(|(l, m)| l == "WARN" && m.contains("path read failed"))
    }

    struct InjectEof<T> {
        inner: T,
        eof: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }

    impl<T: AsyncRead + Unpin> AsyncRead for InjectEof<T> {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            if self.eof.load(Ordering::SeqCst) {
                return Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "peer closed connection without sending TLS close_notify",
                )));
            }
            let r = Pin::new(&mut self.inner).poll_read(cx, buf);
            if self.eof.load(Ordering::SeqCst) {
                return Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "peer closed connection without sending TLS close_notify",
                )));
            }
            r
        }
    }

    impl<T: AsyncWrite + Unpin> AsyncWrite for InjectEof<T> {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, std::io::Error>> {
            Pin::new(&mut self.inner).poll_write(cx, buf)
        }
        fn poll_flush(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            Pin::new(&mut self.inner).poll_flush(cx)
        }
        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    #[tokio::test]
    async fn unexpected_eof_on_up_path_warns() {
        let store = std::sync::Arc::new(Mutex::new(Vec::new()));
        let _guard = tracing::subscriber::set_default(WarnCap(store.clone()));
        let client = Session::new_client(SessionConfig::default());
        let (a, mut b) = duplex(64 * 1024);
        let trip = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let done = client.start_path(
            "p1".into(),
            InjectEof {
                inner: a,
                eof: trip.clone(),
            },
        );
        tokio::time::sleep(Duration::from_millis(15)).await;
        trip.store(true, Ordering::SeqCst);
        let _ = b.write_all(&[1]).await;
        let _ = tokio::time::timeout(Duration::from_millis(500), done).await;
        client.shutdown();
        let ev = store.lock().unwrap().clone();
        assert!(
            warn_read_failed(&ev),
            "UP unexpected-eof must WARN, got {ev:?}"
        );
    }

    #[tokio::test]
    async fn unexpected_eof_after_local_down_does_not_warn() {
        let store = std::sync::Arc::new(Mutex::new(Vec::new()));
        let _guard = tracing::subscriber::set_default(WarnCap(store.clone()));
        let client = Session::new_client(SessionConfig::default());
        let (a, mut b) = duplex(64 * 1024);
        let trip = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let done = client.start_path(
            "p1".into(),
            InjectEof {
                inner: a,
                eof: trip.clone(),
            },
        );
        tokio::time::sleep(Duration::from_millis(15)).await;
        let id = *client.inner.paths.lock().unwrap().keys().next().unwrap();
        client.path_failed(id);
        trip.store(true, Ordering::SeqCst);
        let _ = b.write_all(&[1]).await;
        let _ = tokio::time::timeout(Duration::from_millis(500), done).await;
        client.shutdown();
        let ev = store.lock().unwrap().clone();
        assert!(
            !warn_read_failed(&ev),
            "local DOWN unexpected-eof must not WARN, got {ev:?}"
        );
    }
}
