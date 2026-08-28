//! One overlay session: many TCP+TLS paths, many multiplexed streams.
//!
//! * [`streams`] — open/accept, windowed send, recv reorder, ACKs
//! * [`steer`] — health tick, speculative migrate, failback, same-link rebalance
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

use nya_proto::{Frame, Pong, ResetReason, StreamData};

use crate::cfg::SessionConfig;
use crate::metrics::HistSnap;
use crate::metrics::{
    flatten_paths, Counters, ProcessCounters, ProcessSnapshot, Snapshot, FAILOVER_MS_BOUNDS,
    LIFETIME_MS_BOUNDS, STALL_MS_BOUNDS,
};
use crate::path::{spawn_path_io, PathState, STATE_DOWN};
use crate::scheduler::{backup_prefer_class, pick_path_pref, PickPref};
use crate::stream::{StreamState, Unacked};

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
    metrics: Counters,
    process: Arc<ProcessCounters>,
    last_rtt_us: Mutex<HashMap<String, u64>>,
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
            metrics: Counters::default(),
            process,
            last_rtt_us: Mutex::new(HashMap::new()),
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
        self.wait_paths(1, timeout).await
    }

    /// Wait until at least `n` paths are alive (multiple TCP conns per link).
    pub async fn wait_paths(&self, n: usize, timeout: Duration) -> Result<(), SessionError> {
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
        self.migrate_from_path(path_id);
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
                    path.on_pong_record(p.seq, p.sent_at_ms, record);
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

    fn ensure_sticky(&self, stream_id: u32) -> Option<u32> {
        let st = self.get_stream(stream_id)?;
        let cur = st.sticky.load(Ordering::Relaxed);
        if let Some(p) = self.get_path(cur) {
            if p.is_schedulable() {
                return Some(cur);
            }
            if p.is_alive() {
                if let Some(alt) = backup_prefer_class(&self.path_list(), cur, &self.inner.cfg) {
                    if alt != cur {
                        self.set_sticky(stream_id, alt);
                        self.note_migrate("ensure_sticky");
                        debug!(
                            stream_id,
                            from = cur,
                            to = alt,
                            reason = "ensure_sticky",
                            "migrate"
                        );
                        return Some(alt);
                    }
                }
                return Some(cur);
            }
        }
        let pref = if st.bulk.load(Ordering::Relaxed) {
            PickPref::Any
        } else {
            PickPref::Interactive
        };
        let picked = self.pick_pref(pref)?;
        self.set_sticky(stream_id, picked);
        self.note_migrate("ensure_sticky");
        debug!(
            stream_id,
            from = cur,
            to = picked,
            reason = "ensure_sticky",
            "migrate"
        );
        Some(picked)
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

    /// Retransmit every unacked chunk on `to`, rehoming inflight.
    fn retransmit_all_on(&self, st: &StreamState, to: u32) {
        let mut unacked = st.unacked.lock().unwrap();
        let mut n = 0u64;
        for (offset, u) in unacked.iter_mut() {
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
                let cur = st.sticky.load(Ordering::Relaxed);
                if self.get_path(cur).is_some_and(|p| p.is_alive()) {
                    self.send_on_path(
                        cur,
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
        self.unstick(&st);
        if reset_reason.is_some() {
            self.inner.streams.lock().unwrap().remove(&id);
        }
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
        debug!(stream_id = st.id, ?reset_reason, "stream end");
    }

    pub fn snapshot(&self) -> Snapshot {
        let paths = self.path_list();
        let mut snap = self.inner.metrics.snap_with_paths(&paths);
        let (live, sample) = self.stream_snaps();
        snap.streams = sample;
        snap.streams_live = live;
        snap.links = crate::metrics::rollup_links(&snap.paths);
        snap
    }

    fn stream_snaps(&self) -> (u64, Vec<crate::metrics::StreamSnap>) {
        use crate::metrics::{StreamSnap, STREAM_SNAP_CAP};
        let names: HashMap<u32, String> = self
            .inner
            .paths
            .lock()
            .unwrap()
            .iter()
            .map(|(id, p)| (*id, p.name.clone()))
            .collect();
        let held: Vec<Arc<StreamState>> = {
            let g = self.inner.streams.lock().unwrap();
            g.values()
                .filter(|st| !st.counted_close.load(Ordering::Relaxed))
                .cloned()
                .collect()
        };
        let live = held.len() as u64;
        let sample = held
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
        (live, sample)
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
    use nya_proto::Target;
    use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};

    fn pair() -> (Session, Session, mpsc::Receiver<IncomingStream>) {
        let mut cfg = SessionConfig::default();
        cfg.tuning.loss_timeout_floor = Duration::from_millis(150);
        cfg.all_down_timeout = Duration::from_secs(2);
        let client = Session::new_client(cfg.clone());
        let (server, incoming) = Session::new_server(cfg);
        (client, server, incoming)
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
        let still = {
            let streams = client.inner.streams.lock().unwrap();
            streams
                .values()
                .next()
                .unwrap()
                .sticky
                .load(Ordering::Relaxed)
        };
        assert_ne!(still, sticky, "DEGRADED must restick to sibling");
        assert_eq!(client.snapshot().path_down, down0);
        tun.write_all(b"ok!").await.unwrap();
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
        let snap1 = client.snapshot();
        assert_eq!(snap1.path_down, snap0.path_down);
        assert!(
            snap1.migrates > snap0.migrates,
            "degrade migrate must count migrates, {snap0:?} -> {snap1:?}"
        );
        assert!(
            snap1.migrates_speculative > snap0.migrates_speculative,
            "degrade migrate is speculative"
        );
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
}
