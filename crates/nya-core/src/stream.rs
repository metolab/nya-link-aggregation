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
    pub recv_fin: AtomicBool,
    pub send_fin_sent: AtomicBool,
    pub reset: AtomicBool,
    /// Set on first STREAM_DATA larger than `tuning.interactive_max`.
    pub bulk: AtomicBool,
    pub buffered_in: AtomicU64,
    pub initial_window: u32,
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
            recv_fin: AtomicBool::new(false),
            send_fin_sent: AtomicBool::new(false),
            reset: AtomicBool::new(false),
            bulk: AtomicBool::new(false),
            buffered_in: AtomicU64::new(0),
            initial_window,
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
        !self.reset.load(Ordering::Relaxed) && !self.counted_close.load(Ordering::Relaxed)
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
        (self.initial_window as u64)
            .saturating_sub(self.buffered_in.load(Ordering::Relaxed))
            .min(u32::MAX as u64) as u32
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
