use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
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
    /// How long fast RTT must stay high before stable RTT is raised.
    pub stable_up_hold_us: AtomicU64,
    high_since: std::sync::Mutex<Option<Instant>>,
    class_high_since: std::sync::Mutex<Option<Instant>>,
    class_low_since: std::sync::Mutex<Option<Instant>>,
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
            stable_up_hold_us: AtomicU64::new(1_000_000),
            high_since: std::sync::Mutex::new(None),
            class_high_since: std::sync::Mutex::new(None),
            class_low_since: std::sync::Mutex::new(None),
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
            .min()
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

    /// Drop pings with no Pong within `max_age`. Returns how many were expired.
    pub fn expire_stale_pings(&self, max_age: Duration) -> u64 {
        let mut g = self.pending_ping.lock().unwrap();
        let n0 = g.len();
        g.retain(|_, t| t.elapsed() < max_age);
        n0.saturating_sub(g.len()) as u64
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
        let drop = t.class_should_drop(c_old, fast);

        // Lock order: class_high_since then class_low_since, both paths.
        let mut high = self.class_high_since.lock().unwrap();
        let mut low = self.class_low_since.lock().unwrap();
        if raise {
            *low = None;
            let start = high.get_or_insert_with(Instant::now);
            if start.elapsed() >= hold {
                let new_us = (c_old * 7 + fast) / 8;
                self.rtt_class_us.store(new_us, Ordering::Relaxed);
                tracing::debug!(
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
        if drop {
            let start = low.get_or_insert_with(Instant::now);
            if start.elapsed() >= hold {
                let new_us = (c_old * 7 + fast) / 8;
                self.rtt_class_us.store(new_us, Ordering::Relaxed);
                tracing::debug!(
                    path = %self.name,
                    old_us = c_old,
                    new_us,
                    kind = "drop",
                    "class"
                );
            }
            return;
        }
        *low = None;
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
        let mut g = self.pending_ping.lock().unwrap();
        if g.len() > Tuning::STANDARD.pending_ping_max {
            g.clear();
        }
        g.insert(seq, Instant::now());
        Ping {
            seq,
            sent_at_ms: now_ms(),
        }
    }

    /// Prefer local Instant (µs) over the millisecond wall-clock echo.
    pub fn on_pong(&self, seq: u64, sent_at_ms: u64) {
        self.on_pong_record(seq, sent_at_ms, true);
    }

    /// Always clear the pending ping (degrade clocks). Skip `record_rtt`
    /// when the sample rode behind bulk inflight.
    pub fn on_pong_record(&self, seq: u64, sent_at_ms: u64, record: bool) {
        let started = self.pending_ping.lock().unwrap().remove(&seq);
        if !record {
            return;
        }
        if let Some(t0) = started {
            self.record_rtt(t0.elapsed());
            return;
        }
        let now = now_ms();
        if now >= sent_at_ms {
            self.record_rtt(Duration::from_millis(now - sent_at_ms));
        }
    }
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

async fn send_frame<T>(
    framed: &mut Framed<T, LengthDelimitedCodec>,
    session: &Session,
    path: &PathState,
    frame: Frame,
) -> std::io::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    path.note_tx();
    let encoded = frame.encode();
    let n = encoded.len();
    framed.send(Bytes::from(encoded)).await?;
    session.account_overlay_frame(&frame, n, true);
    Ok(())
}

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
        let codec = LengthDelimitedCodec::builder()
            .max_frame_length(MAX_FRAME_SIZE)
            .new_codec();
        let mut framed = Framed::new(io, codec);
        if let Err(e) = framed.flush().await {
            warn!(path = %path.name, error = %e, "path flush after handshake failed");
            session.path_failed(path.id);
            let _ = done.send(());
            return;
        }

        loop {
            if session.is_dead() || !path.is_alive() {
                break;
            }
            let ping_every = session.probe_interval_for(&path);
            tokio::select! {
                biased;
                out = urgent.recv() => {
                    let Some(frame) = out else { break; };
                    path.note_dequeue(true);
                    if let Err(e) = send_frame(&mut framed, &session, &path, frame).await {
                        warn!(path = %path.name, error = %e, "path write failed");
                        break;
                    }
                }
                incoming = framed.next() => {
                    match incoming {
                        None => {
                            debug!(path = %path.name, "path eof");
                            break;
                        }
                        Some(Err(e)) => {
                            warn!(path = %path.name, error = %e, "path read failed");
                            break;
                        }
                        Some(Ok(bytes)) => {
                            match Frame::decode(&bytes) {
                                Ok(frame) => {
                                    session.account_overlay_frame(&frame, bytes.len(), false);
                                    path.touch_rx();
                                    session.handle_frame(path.id, frame);
                                }
                                Err(e) => {
                                    warn!(path = %path.name, error = %e, "bad frame");
                                    break;
                                }
                            }
                        }
                    }
                }
                out = rx.recv() => {
                    let Some(frame) = out else { break; };
                    path.note_dequeue(false);
                    if let Err(e) = send_frame(&mut framed, &session, &path, frame).await {
                        warn!(path = %path.name, error = %e, "path write failed");
                        break;
                    }
                }
                _ = tokio::time::sleep(ping_every) => {
                    if session.is_dead() || !path.is_alive() {
                        break;
                    }
                    if path.last_rx_ago() >= ping_every {
                        let ping = path.next_ping();
                        if let Err(e) =
                            send_frame(&mut framed, &session, &path, Frame::Ping(ping)).await
                        {
                            warn!(path = %path.name, error = %e, "path ping failed");
                            break;
                        }
                    }
                }
                _ = session.wait_dead() => break,
            }
        }
        let _ = framed.close().await;
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
}
