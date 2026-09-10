use std::collections::BTreeMap;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
use tokio::sync::mpsc;
use tokio::sync::Notify;

use nya_proto::ResetReason;

use crate::metrics::mono_ms;

pub enum Inbound {
    Data(Bytes),
    Close,
    Reset(#[allow(dead_code)] ResetReason),
}

pub struct Unacked {
    pub data: Vec<u8>,
    pub path_id: u32,
    pub last_sent: Instant,
    pub tried: Vec<u32>,
    /// Rate-limit failed retry attempts without moving last_sent (ACK RTT).
    pub retry_not_before: Instant,
}

pub struct StreamState {
    pub id: u32,
    pub sticky: AtomicU32,
    pub send_next: AtomicU64,
    pub send_acked: AtomicU64,
    pub send_window: AtomicU32,
    pub unacked: Mutex<BTreeMap<u64, Unacked>>,
    pub send_wait: Notify,
    pub inbound_tx: mpsc::Sender<Inbound>,
    pub recv_next: AtomicU64,
    pub recv_buf: Mutex<BTreeMap<u64, Vec<u8>>>,
    /// Bytes currently in `recv_buf`.
    pub recv_buffered: AtomicU64,
    /// Last STREAM_DATA arrival path. 0 = none.
    pub last_recv_path: AtomicU32,
    pub ack_dirty: AtomicBool,
    /// 0 = not waiting. Set when dirty goes 0→1; kept until that gen is Sent.
    pub ack_flush_from_us: AtomicU64,
    pub recv_fin: AtomicBool,
    /// `u64::MAX` = no peer Close yet. Else the sender's `send_next` at FIN.
    pub recv_close_off: AtomicU64,
    pub send_fin_sent: AtomicBool,
    pub reset: AtomicBool,
    /// Set on first STREAM_DATA larger than `tuning.interactive_max`.
    pub bulk: AtomicBool,
    pub buffered_in: AtomicU64,
    pub initial_window: u32,
    /// BDP advertise cap. Floor = `initial_window`; ceil = floor * chan.
    pub recv_cap: AtomicU32,
    /// `drain_recv` bytes/s EWMA (α = 1/8). 0 until a sample spanning ≥ one RTT.
    pub deliver_rate_ewma: AtomicU64,
    /// Bytes since the last rate sample. Coalesced so FramedRead gaps are not a rate.
    pending_deliver: AtomicU64,
    last_deliver: Mutex<Option<Instant>>,
    last_stick_change: Mutex<Instant>,
    /// 0 = never. Written when `send_acked` advances.
    pub last_ack_ms: AtomicU64,
    /// 0 = never. Written on successful inbound `try_send`.
    pub last_recv_ms: AtomicU64,
    /// 0 = no hole. First-seen hole clock when `last_recv_ms == 0`.
    pub recv_hole_since_ms: AtomicU64,
    pub stalled: AtomicBool,
    /// 0 = not in stall. Frozen origin on enter; read on leave.
    pub stall_from_ms: AtomicU64,
    /// Lifetime only. Never used as a stall origin.
    pub opened_ms: AtomicU64,
    /// CAS winner owns closed-vs-reset accounting.
    pub counted_close: AtomicBool,
    /// 0 = not closing. First FIN (local or peer) stamps `mono_ms`.
    pub close_started_ms: AtomicU64,
}

impl StreamState {
    pub fn new(id: u32, inbound_tx: mpsc::Sender<Inbound>, initial_window: u32) -> Arc<Self> {
        Arc::new(Self {
            id,
            sticky: AtomicU32::new(0),
            send_next: AtomicU64::new(0),
            send_acked: AtomicU64::new(0),
            send_window: AtomicU32::new(initial_window),
            unacked: Mutex::new(BTreeMap::new()),
            send_wait: Notify::new(),
            inbound_tx,
            recv_next: AtomicU64::new(0),
            recv_buf: Mutex::new(BTreeMap::new()),
            recv_buffered: AtomicU64::new(0),
            last_recv_path: AtomicU32::new(0),
            ack_dirty: AtomicBool::new(false),
            ack_flush_from_us: AtomicU64::new(0),
            recv_fin: AtomicBool::new(false),
            recv_close_off: AtomicU64::new(u64::MAX),
            send_fin_sent: AtomicBool::new(false),
            reset: AtomicBool::new(false),
            bulk: AtomicBool::new(false),
            buffered_in: AtomicU64::new(0),
            initial_window,
            recv_cap: AtomicU32::new(initial_window),
            deliver_rate_ewma: AtomicU64::new(0),
            pending_deliver: AtomicU64::new(0),
            last_deliver: Mutex::new(None),
            last_stick_change: Mutex::new(Instant::now()),
            last_ack_ms: AtomicU64::new(0),
            last_recv_ms: AtomicU64::new(0),
            recv_hole_since_ms: AtomicU64::new(0),
            stalled: AtomicBool::new(false),
            stall_from_ms: AtomicU64::new(0),
            opened_ms: AtomicU64::new(mono_ms().max(1)),
            counted_close: AtomicBool::new(false),
            close_started_ms: AtomicU64::new(0),
        })
    }

    pub fn is_steerable(&self) -> bool {
        !self.reset.load(Ordering::Relaxed)
            && !self.counted_close.load(Ordering::Relaxed)
            && !self.send_fin_sent.load(Ordering::Relaxed)
            && !self.recv_fin.load(Ordering::Relaxed)
    }

    pub fn note_close_started(&self) {
        let now = mono_ms().max(1);
        let _ =
            self.close_started_ms
                .compare_exchange(0, now, Ordering::Relaxed, Ordering::Relaxed);
    }

    pub fn note_stick_change(&self) {
        *self.last_stick_change.lock().unwrap() = Instant::now();
    }

    pub fn stick_changed_ago_ge(&self, d: Duration) -> bool {
        self.last_stick_change.lock().unwrap().elapsed() >= d
    }

    pub fn inflight_send(&self) -> u64 {
        self.send_next
            .load(Ordering::Relaxed)
            .saturating_sub(self.send_acked.load(Ordering::Relaxed))
    }

    pub fn window_ok(&self, extra: u64) -> bool {
        self.inflight_send() + extra <= u64::from(self.send_window.load(Ordering::Relaxed))
    }

    pub fn advertised_window(&self) -> u32 {
        // recv_buf is the BDP buffer; duplex stays initial_window.
        let cap = u64::from(self.recv_cap.load(Ordering::Relaxed));
        cap.saturating_sub(self.buffered_in.load(Ordering::Relaxed))
            .saturating_sub(self.recv_buffered.load(Ordering::Relaxed))
            .min(u32::MAX as u64) as u32
    }

    /// Coalesce until `dt >= min_dt` (one RTT, or maintain_interval). First
    /// bytes are kept; a collapsed-but-nonzero Instant is not a rate.
    pub(crate) fn note_deliver(&self, len: u64, min_dt: Duration) {
        if len == 0 {
            return;
        }
        let now = Instant::now();
        let mut last = self.last_deliver.lock().unwrap();
        let pending = self.pending_deliver.fetch_add(len, Ordering::Relaxed) + len;
        let Some(prev) = *last else {
            *last = Some(now);
            return;
        };
        let dt = now.saturating_duration_since(prev);
        // Decode-speed FramedRead gaps are tens of µs; require a full RTT.
        if dt.is_zero() || dt < min_dt {
            return;
        }
        *last = Some(now);
        self.pending_deliver.store(0, Ordering::Relaxed);
        drop(last);
        let sample = (pending as f64 / dt.as_secs_f64()) as u64;
        if sample == 0 {
            return;
        }
        let old = self.deliver_rate_ewma.load(Ordering::Relaxed);
        // α = 1/8, same 7/8 as class RTT — not a Tuning field.
        let ewma = if old == 0 {
            sample
        } else {
            old.saturating_mul(7).saturating_add(sample) / 8
        };
        self.deliver_rate_ewma.store(ewma, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(crate) fn debug_set_deliver_clock(&self, start: Instant) {
        *self.last_deliver.lock().unwrap() = Some(start);
    }
}

pub struct TunnelStream {
    pub id: u32,
    inner: DuplexStream,
}

impl TunnelStream {
    pub fn from_duplex(id: u32, inner: DuplexStream) -> Self {
        Self { id, inner }
    }
}

impl AsyncRead for TunnelStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for TunnelStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
