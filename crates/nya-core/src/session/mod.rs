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

use nya_proto::{Frame, Pong, ResetReason, StreamClose, StreamData, StreamOpen, Target};

use crate::cfg::SessionConfig;
use crate::health;
use crate::metrics::HistSnap;
use crate::metrics::{
    flatten_paths, Counters, ProcessCounters, ProcessSnapshot, Snapshot, FAILOVER_MS_BOUNDS,
    LIFETIME_MS_BOUNDS, STALL_MS_BOUNDS,
};
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
    dead: AtomicBool,
    dead_notify: Notify,
    all_down_since: Mutex<Option<Instant>>,
    correlated_since: Mutex<Option<Instant>>,
    metrics: Counters,
    process: Arc<ProcessCounters>,
    last_rtt_us: Mutex<HashMap<String, u64>>,
    opens: Mutex<HashMap<u32, OpenUnacked>>,
    closes: Mutex<HashMap<u32, CloseUnacked>>,
    pending_early: Mutex<HashMap<u32, Vec<PendingEarly>>>,
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
            dead: AtomicBool::new(false),
            dead_notify: Notify::new(),
            all_down_since: Mutex::new(None),
            correlated_since: Mutex::new(None),
            metrics: Counters::default(),
            process,
            last_rtt_us: Mutex::new(HashMap::new()),
            opens: Mutex::new(HashMap::new()),
            closes: Mutex::new(HashMap::new()),
            pending_early: Mutex::new(HashMap::new()),
        });
        let session = Self { inner };
        session.spawn_maintenance();
        session
    }

    pub fn process(&self) -> Arc<ProcessCounters> {
        self.inner.process.clone()
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
        self.inner.dead_notify.notify_waiters();
        info!(
            reason = if send_frame { "shutdown" } else { "drop" },
            "session dead"
        );
        self.inner.closes.lock().unwrap().clear();
        let ids: Vec<u32> = self.inner.streams.lock().unwrap().keys().copied().collect();
        for id in ids {
            self.finish_stream(id, Some(ResetReason::SessionDead), send_frame);
        }
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
        path.stable_up_hold_us.store(
            self.inner.cfg.tuning.stable_up_hold.as_micros() as u64,
            Ordering::Relaxed,
        );
        self.inner.paths.lock().unwrap().insert(id, path.clone());
        *self.inner.all_down_since.lock().unwrap() = None;
        self.inner.ready.notify_waiters();
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
        info!(path = %path.name, path_id, "path down");
        self.inner.metrics.path_down.fetch_add(1, Ordering::Relaxed);
        self.observe_failover(&path);
        self.rehome_unacked_from(path_id);
        self.retry_open_from(path_id);
        self.retry_close_from(path_id);
        self.inner.paths.lock().unwrap().remove(&path_id);
        if !self.has_alive_path() {
            *self.inner.all_down_since.lock().unwrap() = Some(Instant::now());
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
                    let cap = if path.rtt_known() {
                        Some(health::loss_timeout(
                            &self.inner.cfg,
                            self.min_alive_fast_rtt()
                                .unwrap_or_else(|| crate::scheduler::path_loss_rtt(&path)),
                        ))
                    } else {
                        Some(self.inner.cfg.tuning.unknown_degrade_min)
                    };
                    path.on_pong_record(p.seq, p.sent_at_ms, record, cap);
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
            Frame::StreamClose(c) => self.on_peer_close(c.stream_id),
            Frame::StreamReset(r) => self.on_peer_reset(r.stream_id, r.reason),
            Frame::SessionClose => self.shutdown(),
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
        let class = crate::scheduler::fastest_class_set(&paths, &self.inner.cfg);
        if !class.iter().any(|q| q.id == sticky) {
            return None;
        }
        Some(sticky)
    }

    fn pick_retry(&self, avoid: u32) -> Option<u32> {
        pick_retry_path(&self.path_list(), &self.inner.cfg, &[avoid])
    }

    fn pick_retry_tried(&self, tried: &[u32]) -> Option<u32> {
        pick_retry_path(&self.path_list(), &self.inner.cfg, tried)
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
        self.retry_after(path_id)
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

    /// Expired unacked copies: one send on a *different* path. Never in-place.
    fn retry_expired_unacked(&self, st: &StreamState) {
        if !st.is_steerable() {
            return;
        }
        let now = Instant::now();
        let expired: Vec<(u64, u32, Vec<u8>, Vec<u32>)> = {
            let unacked = st.unacked.lock().unwrap();
            unacked
                .iter()
                .filter(|(_, u)| {
                    u.last_sent.elapsed() >= self.retry_after(u.path_id)
                        && now >= u.retry_not_before
                })
                .map(|(off, u)| (*off, u.path_id, u.data.clone(), u.tried.clone()))
                .collect()
        };
        for (offset, from, data, mut tried) in expired {
            Self::push_tried(&mut tried, from);
            let Some(alt) = self.pick_retry_tried(&tried) else {
                continue;
            };
            if self.send_data_frame(st.id, offset, data, alt) {
                if let Some(u) = st.unacked.lock().unwrap().get_mut(&offset) {
                    self.rehome_unacked(u, alt);
                }
                self.note_retry(from, alt);
            } else if let Some(u) = st.unacked.lock().unwrap().get_mut(&offset) {
                Self::push_tried(&mut u.tried, alt);
                u.retry_not_before = Instant::now() + self.retry_after(from);
            }
        }
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

    fn remember_close(&self, id: u32, path_id: u32, second_closer: bool) {
        let mut g = self.inner.closes.lock().unwrap();
        if let Some(c) = g.get_mut(&id) {
            c.path_id = path_id;
            c.sent_at = Instant::now();
            Self::push_tried(&mut c.tried, path_id);
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

    fn close_rx_after_send(&self, path_id: u32, sent_at: Instant) -> bool {
        let Some(p) = self.get_path(path_id) else {
            return false;
        };
        let last = *p.last_rx.lock().unwrap();
        last > sent_at
    }

    fn retry_closes(&self) {
        self.reap_closes();
        let snapshot: Vec<(u32, u32, bool, Vec<u32>, Instant)> = self
            .inner
            .closes
            .lock()
            .unwrap()
            .iter()
            .map(|(id, c)| (*id, c.path_id, c.second_closer, c.tried.clone(), c.sent_at))
            .collect();
        for (id, from, second, tried, sent_at) in snapshot {
            if !second {
                if let Some(st) = self.get_stream(id) {
                    if st.recv_fin.load(Ordering::Relaxed) {
                        self.forget_close(id);
                        continue;
                    }
                }
            }
            if sent_at.elapsed() < self.retry_after(from) {
                continue;
            }
            if self.close_rx_after_send(from, sent_at) {
                self.forget_close(id);
                continue;
            }
            let Some(alt) = self.pick_retry_tried(&tried) else {
                continue;
            };
            if self.send_on_path(alt, Frame::StreamClose(StreamClose { stream_id: id })) {
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
                Self::push_tried(&mut c.tried, alt);
            }
        }
    }

    fn retry_close_from(&self, dead: u32) {
        let snapshot: Vec<(u32, Vec<u32>)> = self
            .inner
            .closes
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, c)| c.path_id == dead)
            .map(|(id, c)| (*id, c.tried.clone()))
            .collect();
        for (id, tried) in snapshot {
            let Some(alt) = self.pick_retry_tried(&tried) else {
                continue;
            };
            if self.send_on_path(alt, Frame::StreamClose(StreamClose { stream_id: id })) {
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

    fn overlay_progress_fine(&self, st: &StreamState) -> bool {
        let send_fin = st.send_fin_sent.load(Ordering::Relaxed);
        let recv_fin = st.recv_fin.load(Ordering::Relaxed);
        let acked = st.send_acked.load(Ordering::Relaxed);
        let next = st.send_next.load(Ordering::Relaxed);
        if acked < next {
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
        for u in leftover {
            if let Some(p) = self.get_path(u.path_id) {
                p.sub_inflight(u.data.len() as u64);
            }
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
        Self::push_tried(&mut u.tried, to);
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
            Frame::SessionClose => false,
            _ => true,
        }
    }

    fn send_on_path(&self, path_id: u32, frame: Frame) -> bool {
        let Some(p) = self.get_path(path_id) else {
            return false;
        };
        if !p.is_alive() {
            return false;
        }
        p.note_tx();
        let urgent = self.frame_is_interactive(&frame);
        let tx = if urgent { &p.urgent } else { &p.writer };
        p.note_enqueue(urgent);
        if tx.try_send(frame).is_ok() {
            if urgent {
                p.set_congested(false);
            }
            true
        } else {
            p.note_dequeue(urgent);
            // A full bulk queue must not mark the path unusable for ACKs/pings.
            if urgent {
                p.set_congested(true);
            }
            self.inner
                .metrics
                .frame_send_drop
                .fetch_add(1, Ordering::Relaxed);
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
            let _ = st.inbound_tx.try_send(crate::stream::Inbound::Reset(why));
            st.send_wait.notify_waiters();
            if send_frame {
                if let Some(p) = self.pick_pref(PickPref::Any) {
                    self.send_on_path(
                        p,
                        Frame::StreamReset(nya_proto::StreamReset {
                            stream_id: id,
                            reason: why,
                        }),
                    );
                }
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
    sessions: Mutex<HashMap<[u8; 16], Session>>,
    closed: AtomicBool,
    process: Arc<ProcessCounters>,
}

impl SessionTable {
    pub fn new(cfg: SessionConfig) -> Self {
        Self {
            cfg,
            sessions: Mutex::new(HashMap::new()),
            closed: AtomicBool::new(false),
            process: Arc::new(ProcessCounters::default()),
        }
    }

    pub fn process(&self) -> Arc<ProcessCounters> {
        self.process.clone()
    }

    pub fn aggregate_snapshot(&self) -> ProcessSnapshot {
        let handles: Vec<([u8; 16], Session)> = self
            .sessions
            .lock()
            .unwrap()
            .iter()
            .map(|(id, s)| (*id, s.clone()))
            .collect();
        let sessions: Vec<([u8; 16], Snapshot)> =
            handles.iter().map(|(id, s)| (*id, s.snapshot())).collect();
        let mut acc = Snapshot {
            failover_ms: HistSnap::zeroed(FAILOVER_MS_BOUNDS),
            stall_ms: HistSnap::zeroed(STALL_MS_BOUNDS),
            stream_lifetime_ms: HistSnap::zeroed(LIFETIME_MS_BOUNDS),
            ..Snapshot::default()
        };
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
        ProcessSnapshot {
            process: self.process.snap(),
            session: acc,
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
        self.sessions
            .lock()
            .unwrap()
            .insert(session_id, session.clone());
        Some((session, rx))
    }

    pub fn get(&self, session_id: &[u8; 16]) -> Option<Session> {
        if self.is_closed() {
            return None;
        }
        self.sessions.lock().unwrap().get(session_id).cloned()
    }

    pub fn remove(&self, session_id: &[u8; 16]) {
        self.sessions.lock().unwrap().remove(session_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nya_proto::{StreamAck, Target};
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{duplex, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

    fn pair() -> (Session, Session, mpsc::Receiver<IncomingStream>) {
        let mut cfg = SessionConfig::default();
        cfg.tuning.loss_timeout_floor = Duration::from_millis(150);
        cfg.all_down_timeout = Duration::from_secs(2);
        let client = Session::new_client(cfg.clone());
        let (server, incoming) = Session::new_server(cfg);
        (client, server, incoming)
    }

    async fn pair_echo(names: &[&str]) -> (Session, Session) {
        let (client, server, incoming) = pair();
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
        client.remember_close(sid, from, false);
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
        client.remember_close(7, 1, true);
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
    async fn write_timeout_tears_only_blocked_path() {
        let client = Session::new_client(SessionConfig::default());
        let (a, _peer) = duplex(8);
        let done = client.start_path("stall".into(), a);
        let stall_id = *client.inner.paths.lock().unwrap().keys().next().unwrap();
        let _live = inject_live(&client, 99, "live#0", 7);
        let ping = Frame::Ping(nya_proto::Ping {
            seq: 1,
            sent_at_ms: 0,
        });
        for _ in 0..80 {
            let _ = client.send_on_path(stall_id, ping.clone());
        }
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(
            client.get_path(stall_id).is_none(),
            "blocked writer must path_failed that dest"
        );
        assert!(client.get_path(99).is_some(), "sibling dest must stay up");
        let _ = tokio::time::timeout(Duration::from_millis(500), done).await;
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
