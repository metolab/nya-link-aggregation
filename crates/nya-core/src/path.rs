use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::{Sink, SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::{debug, warn};

use nya_proto::{Frame, Ping, MAX_FRAME_SIZE};

use crate::session::Session;
use crate::tuning::Tuning;

pub const STATE_UP: u8 = 1;
pub const STATE_DEGRADED: u8 = 2;
pub const STATE_DOWN: u8 = 3;

/// `a#0` / `a#1` share a link; names without `#` are their own link.
pub fn link_key(name: &str) -> &str {
    name.rsplit_once('#').map(|(l, _)| l).unwrap_or(name)
}

pub struct PathState {
    pub id: u32,
    pub name: String,
    /// Bulk / large STREAM_DATA.
    pub writer: mpsc::Sender<Frame>,
    /// ACKs, pings, and small STREAM_DATA — must not wait behind bulk.
    pub urgent: mpsc::Sender<Frame>,
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
            rtt_ewma_us: AtomicU64::new(0),
            rtt_stable_us: AtomicU64::new(0),
            rtt_class_us: AtomicU64::new(0),
            class_init_n: AtomicU64::new(0),
            inflight: AtomicU64::new(0),
            sticky_streams: AtomicU64::new(0),
            congested: AtomicBool::new(false),
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
        })
    }

    pub fn link(&self) -> &str {
        link_key(&self.name)
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

    /// Eligible for new sticky assignment: up and not send-blocked.
    pub fn is_schedulable(&self) -> bool {
        self.is_up() && !self.is_congested()
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

    pub fn note_enqueue(&self, urgent: bool) {
        if urgent {
            self.urgent_queued.fetch_add(1, Ordering::Relaxed);
        } else {
            self.bulk_queued.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn note_dequeue(&self, urgent: bool) {
        let q = if urgent {
            &self.urgent_queued
        } else {
            &self.bulk_queued
        };
        let _ = q.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
            Some(v.saturating_sub(1))
        });
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
        self.on_pong_record(seq, sent_at_ms, true, None);
    }

    pub(crate) fn is_tls_unexpected_eof(e: &std::io::Error) -> bool {
        e.kind() == std::io::ErrorKind::UnexpectedEof
    }

    /// Always clear the pending/late ping. Skip `record_rtt` when the
    /// sample rode behind bulk inflight. No wall-clock fallback.
    pub fn on_pong_record(&self, seq: u64, _sent_at_ms: u64, record: bool, cap: Option<Duration>) {
        let started = {
            let mut pending = self.pending_ping.lock().unwrap();
            pending.remove(&seq)
        };
        let started = match started {
            Some(t0) => Some(t0),
            None => self.late_ping.lock().unwrap().remove(&seq),
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
                        debug!(path = %path_r.name, "path eof");
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
            Sent,
            Closed,
            TimedOut,
            Io(std::io::Error),
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

                tokio::select! {
                    biased;
                    _ = &mut close_rx => {
                        let _ = tokio::time::timeout(ping_max, writer.close()).await;
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
                            WriteOne::Sent => {
                                next_ping = tokio::time::Instant::now() + ping_every;
                            }
                            WriteOne::Closed => return Ok(()),
                            WriteOne::TimedOut => {
                                return Err(std::io::Error::new(
                                    std::io::ErrorKind::TimedOut,
                                    "path write",
                                ));
                            }
                            WriteOne::Io(e) => {
                                warn!(path = %path_w.name, error = %e, "path ping failed");
                                return Err(e);
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
                            WriteOne::Sent => {}
                            WriteOne::Closed => return Ok(()),
                            WriteOne::TimedOut => {
                                return Err(std::io::Error::new(
                                    std::io::ErrorKind::TimedOut,
                                    "path write",
                                ));
                            }
                            WriteOne::Io(e) => {
                                warn!(path = %path_w.name, error = %e, "path write failed");
                                return Err(e);
                            }
                        }
                    }
                    out = rx.recv() => {
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
                            WriteOne::Sent => {}
                            WriteOne::Closed => return Ok(()),
                            WriteOne::TimedOut => {
                                return Err(std::io::Error::new(
                                    std::io::ErrorKind::TimedOut,
                                    "path write",
                                ));
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
            ping_max: Duration,
            session: &Session,
            path: &PathState,
            frame: Frame,
        ) -> WriteOne
        where
            S: Sink<Bytes, Error = std::io::Error> + Unpin,
        {
            tokio::select! {
                biased;
                _ = &mut *close_rx => {
                    let _ = tokio::time::timeout(ping_max, writer.close()).await;
                    WriteOne::Closed
                }
                r = tokio::time::timeout(deadline, send_frame(writer, session, path, frame)) => {
                    match r {
                        Ok(Ok(())) => WriteOne::Sent,
                        Ok(Err(e)) => WriteOne::Io(e),
                        Err(_) => WriteOne::TimedOut,
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
                let _ = tokio::time::timeout(ping_max, &mut write_task).await;
                read_task.abort();
                write_task.abort();
            }
            Exit::Child => {
                read_task.abort();
                write_task.abort();
            }
        }
        session.path_failed(path.id);
        let _ = done.send(());
        debug!(path = %path.name, "path io exit");
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    fn path() -> Arc<PathState> {
        let (tx, _rx) = mpsc::channel(1);
        PathState::new(1, "t".into(), tx)
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
        p.on_pong_record(99, 0, true, None);
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
        p.on_pong_record(ping.seq, ping.sent_at_ms, false, None);
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
