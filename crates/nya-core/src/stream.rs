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
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};

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
    /// When this piece first left the stream (never moved by a re-send):
    /// the send-stall clock cannot start before this.
    pub first_sent: Instant,
    pub tried: Vec<u32>,
    /// Rate-limit failed retry attempts without moving last_sent (ACK RTT).
    pub retry_not_before: Instant,
    /// P4: no frame for this piece reached a writer queue (park drop,
    /// `send_on_path` false, give-up with no alt). Retry ignores path
    /// freshness for such pieces; cleared on any successful enqueue.
    pub dropped: bool,
    /// P2: `path.delivered` / `path.delivered_at_us` captured at (re)send.
    /// Bandwidth is sampled only from un-hedged pieces (`tried.len() == 1`).
    pub delivered_at_send: u64,
    pub delivered_time_at_send_us: u64,
    /// `mono_us` of this (re)send and of the path's first send since its
    /// last ACK; the send-side interval of the BBR rate sample.
    pub sent_us: u64,
    pub first_tx_at_send_us: u64,
}

/// The atomics `StreamStats` reads, split out of `StreamState` so a
/// `TunnelStream` can read them after the session has reaped the stream
/// (KD13). Holds nothing that can keep the pump alive (no channel, no
/// buffer; the cancellation token owns no task). `StreamState` derefs to it.
pub struct StreamCounters {
    /// Cancelled by `remove_held_stream`: the session has forgotten this
    /// stream and no byte can cross the overlay for it any more. The hop
    /// copier watches it so a local peer that never sends EOF cannot keep
    /// the copy (and its socket) alive past the stream.
    pub gone: CancellationToken,
    pub send_next: AtomicU64,
    pub send_window: AtomicU32,
    pub recv_next: AtomicU64,
    /// BDP advertise cap. Floor = `initial_window`; ceil = floor * chan.
    pub recv_cap: AtomicU32,
    /// Per-stream limiter evidence, read at stream end for the hop span (P6).
    pub window_blocks: AtomicU64,
    pub budget_blocks: AtomicU64,
    pub window_limited_with_room: AtomicU64,
    /// Max `recv_cap` ever advertised (receiver side).
    pub recv_cap_max: AtomicU32,
    /// Bulk/interactive pieces re-sent on another path for this stream.
    pub hedges: AtomicU64,
    /// Duplicate payload bytes received on this stream.
    pub dup_rx_bytes: AtomicU64,
    /// Distinct dests this stream sent DATA on (bounded set).
    pub paths_used: Mutex<Vec<u32>>,
    /// Receiver evidence (P1.4): max out-of-order bytes held behind a
    /// hole, max in-order bytes the app had not taken, zero-window ACKs
    /// by cause, the hole EWMA (µs), and sticky-full diverts (P4).
    pub recv_hole_max: AtomicU64,
    pub app_backlog_max: AtomicU64,
    pub zero_win_hole: AtomicU64,
    pub zero_win_app: AtomicU64,
    pub hole_us: AtomicU64,
    pub budget_diverts: AtomicU64,
}

impl StreamCounters {
    fn new(initial_window: u32) -> Arc<Self> {
        Arc::new(Self {
            gone: CancellationToken::new(),
            send_next: AtomicU64::new(0),
            send_window: AtomicU32::new(initial_window),
            recv_next: AtomicU64::new(0),
            recv_cap: AtomicU32::new(initial_window),
            window_blocks: AtomicU64::new(0),
            budget_blocks: AtomicU64::new(0),
            window_limited_with_room: AtomicU64::new(0),
            recv_cap_max: AtomicU32::new(initial_window),
            hedges: AtomicU64::new(0),
            dup_rx_bytes: AtomicU64::new(0),
            paths_used: Mutex::new(Vec::new()),
            recv_hole_max: AtomicU64::new(0),
            app_backlog_max: AtomicU64::new(0),
            zero_win_hole: AtomicU64::new(0),
            zero_win_app: AtomicU64::new(0),
            hole_us: AtomicU64::new(0),
            budget_diverts: AtomicU64::new(0),
        })
    }

    /// Remember a dest this stream sent DATA on (first 16 distinct).
    pub fn note_path_used(&self, path_id: u32) {
        let mut g = self.paths_used.lock().unwrap();
        if !g.contains(&path_id) && g.len() < 16 {
            g.push(path_id);
        }
    }

    pub fn stats(&self) -> StreamStats {
        StreamStats {
            recv_cap: self.recv_cap.load(Ordering::Relaxed),
            send_window: self.send_window.load(Ordering::Relaxed),
            window_blocks: self.window_blocks.load(Ordering::Relaxed),
            budget_blocks: self.budget_blocks.load(Ordering::Relaxed),
            window_limited_with_room: self.window_limited_with_room.load(Ordering::Relaxed),
            recv_cap_max: self.recv_cap_max.load(Ordering::Relaxed),
            hedges: self.hedges.load(Ordering::Relaxed),
            dup_rx_bytes: self.dup_rx_bytes.load(Ordering::Relaxed),
            paths_used: self.paths_used.lock().unwrap().len() as u32,
            sent: self.send_next.load(Ordering::Relaxed),
            received: self.recv_next.load(Ordering::Relaxed),
            recv_hole_max: self.recv_hole_max.load(Ordering::Relaxed),
            app_backlog_max: self.app_backlog_max.load(Ordering::Relaxed),
            zero_win_hole: self.zero_win_hole.load(Ordering::Relaxed),
            zero_win_app: self.zero_win_app.load(Ordering::Relaxed),
            hole_us: self.hole_us.load(Ordering::Relaxed),
            budget_diverts: self.budget_diverts.load(Ordering::Relaxed),
        }
    }
}

pub struct StreamState {
    pub id: u32,
    pub counters: Arc<StreamCounters>,
    pub sticky: AtomicU32,
    pub send_acked: AtomicU64,
    pub unacked: Mutex<BTreeMap<u64, Unacked>>,
    pub send_wait: Notify,
    pub inbound_tx: mpsc::Sender<Inbound>,
    pub recv_buf: Mutex<BTreeMap<u64, Vec<u8>>>,
    /// Bytes currently in `recv_buf`.
    pub recv_buffered: AtomicU64,
    /// Last STREAM_DATA arrival path. 0 = none.
    pub last_recv_path: AtomicU32,
    /// The peer has shown it holds this stream (first DATA from it). Set
    /// once; lets `deliver_data` stop the StreamOpen retry without a table
    /// lookup per frame.
    pub peer_seen: AtomicBool,
    /// Offset of the newest out-of-order piece buffered; its SACK range
    /// goes first (TCP's "most recent block" rule).
    pub last_hole_arrival: AtomicU64,
    /// Pieces released by SACK (delivered behind a hole on another path).
    pub sacked: AtomicU64,
    /// 0 = never. Written when a SACK range released a piece.
    pub last_sack_ms: AtomicU64,
    /// Rate limit for the stuck-stream debug line. 0 = never logged.
    pub stuck_logged_ms: AtomicU64,
    /// Rate limit for the bulk-stream debug line. 0 = never logged.
    pub bulk_logged_ms: AtomicU64,
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
    /// `drain_recv` bytes/s EWMA (α = 1/8). 0 until a sample spanning ≥ one RTT.
    pub deliver_rate_ewma: AtomicU64,
    /// P2.4: the session's Σ over live streams of `recv_cap − floor`. This
    /// stream's share is added by `tune_recv_cap` and released in `Drop`.
    pub(crate) recv_cap_extra: Arc<AtomicU64>,
    /// P2.1 hole: the missing offset in-order delivery is waiting for and
    /// since when. Only taken inside `drain_recv` under the `recv_buf` lock.
    pub(crate) hole: Mutex<Option<(u64, Instant)>>,
    /// `mono_us` of the last hole sample. 0 = never.
    pub hole_sampled_at_us: AtomicU64,
    /// P2.2: bytes waiting behind a hole (`recv_buffered − in_order_held`),
    /// EWMA (α = 1/8) sampled at every drain. By Little's law
    /// `hole_bytes / deliver_rate` is the byte-weighted extra time a byte
    /// spends in the reorder buffer — the part of the in-order loop the
    /// receiver can see. A per-hole time EWMA (`hole_us`) is event-weighted
    /// and under-reads when many short holes surround a few RTO-long ones.
    pub hole_bytes_ewma: AtomicU64,
    /// `mono_us` of the last `hole_bytes_ewma` sample. 0 = never.
    pub hole_bytes_at_us: AtomicU64,
    /// Bytes since the last rate sample. Coalesced so FramedRead gaps are not a rate.
    pending_deliver: AtomicU64,
    last_deliver: Mutex<Option<Instant>>,
    pub(crate) last_stick_change: Mutex<Instant>,
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

impl std::ops::Deref for StreamState {
    type Target = StreamCounters;

    fn deref(&self) -> &StreamCounters {
        &self.counters
    }
}

/// Per-stream limiter summary for `nya.hop` spans (P6).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StreamStats {
    /// Current receive cap we advertise from and the peer's last window.
    pub recv_cap: u32,
    pub send_window: u32,
    pub window_blocks: u64,
    pub budget_blocks: u64,
    pub window_limited_with_room: u64,
    pub recv_cap_max: u32,
    pub hedges: u64,
    pub dup_rx_bytes: u64,
    pub paths_used: u32,
    /// Bytes sent by this side (`send_next`).
    pub sent: u64,
    /// Bytes delivered in order to the application (`recv_next`).
    pub received: u64,
    /// Receiver evidence (P1.4).
    pub recv_hole_max: u64,
    pub app_backlog_max: u64,
    pub zero_win_hole: u64,
    pub zero_win_app: u64,
    /// Receiver hole EWMA, µs (P2.1). 0 = no hole ever closed.
    pub hole_us: u64,
    /// Bulk pieces diverted or parked because the sticky lacked room (P4).
    pub budget_diverts: u64,
}

/// Where the copy task waited, per transfer direction (P1.2), µs.
/// `far` = waited for the side that produces the bytes of the dominant
/// direction; `near` = waited for the side that consumes them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LimiterWaits {
    pub far_us: u64,
    pub near_us: u64,
    pub copy_us: u64,
    /// This side is the overlay *sender* for the dominant direction.
    pub overlay_sender: bool,
    /// Name of the local far/near side: `"origin"` on the server, `"app"`
    /// on the client.
    pub local: &'static str,
}

impl StreamStats {
    /// Which side of the loop waited most (counter-only form, kept for
    /// callers without wait times): `window` (peer's advertised window
    /// with path room available), `budget` (own path budget), `path`
    /// (window with no room anywhere), or `none`.
    pub fn limiter(&self) -> &'static str {
        if self.window_blocks == 0 && self.budget_blocks == 0 {
            return "none";
        }
        self.sender_limiter()
    }

    fn sender_limiter(&self) -> &'static str {
        if self.budget_blocks >= self.window_blocks {
            return "budget";
        }
        if self.window_limited_with_room * 2 >= self.window_blocks {
            "window"
        } else {
            "path"
        }
    }

    /// P1.3: limiter by *time*. An overlay sender that waited for its
    /// local producer was paced by it (`origin` / `app`); one that waited
    /// for the tunnel is refined by the counters. An overlay receiver that
    /// waited for the tunnel says `overlay` (read the peer's span); one
    /// that waited for its local consumer names it. A hop that waited less
    /// than a tenth of its life anywhere is `none`.
    pub fn limiter_by_time(&self, w: LimiterWaits) -> &'static str {
        if w.far_us.saturating_add(w.near_us) < w.copy_us / 10 {
            return "none";
        }
        if w.overlay_sender {
            if w.far_us >= w.near_us {
                w.local
            } else {
                self.sender_limiter()
            }
        } else if w.far_us >= w.near_us {
            "overlay"
        } else {
            w.local
        }
    }
}

impl StreamState {
    #[cfg(test)]
    pub fn new(id: u32, inbound_tx: mpsc::Sender<Inbound>, initial_window: u32) -> Arc<Self> {
        Self::new_in(id, inbound_tx, initial_window, Arc::new(AtomicU64::new(0)))
    }

    /// `recv_cap_extra` is the owning session's P2.4 memory-guard sum.
    pub fn new_in(
        id: u32,
        inbound_tx: mpsc::Sender<Inbound>,
        initial_window: u32,
        recv_cap_extra: Arc<AtomicU64>,
    ) -> Arc<Self> {
        Arc::new(Self {
            id,
            counters: StreamCounters::new(initial_window),
            sticky: AtomicU32::new(0),
            send_acked: AtomicU64::new(0),
            unacked: Mutex::new(BTreeMap::new()),
            send_wait: Notify::new(),
            inbound_tx,
            recv_buf: Mutex::new(BTreeMap::new()),
            recv_buffered: AtomicU64::new(0),
            last_recv_path: AtomicU32::new(0),
            peer_seen: AtomicBool::new(false),
            last_hole_arrival: AtomicU64::new(0),
            sacked: AtomicU64::new(0),
            last_sack_ms: AtomicU64::new(0),
            stuck_logged_ms: AtomicU64::new(0),
            bulk_logged_ms: AtomicU64::new(0),
            ack_dirty: AtomicBool::new(false),
            ack_flush_from_us: AtomicU64::new(0),
            recv_fin: AtomicBool::new(false),
            recv_close_off: AtomicU64::new(u64::MAX),
            send_fin_sent: AtomicBool::new(false),
            reset: AtomicBool::new(false),
            bulk: AtomicBool::new(false),
            buffered_in: AtomicU64::new(0),
            initial_window,
            deliver_rate_ewma: AtomicU64::new(0),
            recv_cap_extra,
            hole: Mutex::new(None),
            hole_sampled_at_us: AtomicU64::new(0),
            hole_bytes_ewma: AtomicU64::new(0),
            hole_bytes_at_us: AtomicU64::new(0),
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

    /// SACK ranges for the next ACK: coalesced runs of `recv_buf`, the run
    /// holding the newest arrival first, then the highest runs. Bounded.
    pub fn sack_ranges(&self) -> Vec<(u64, u64)> {
        let buf = self.recv_buf.lock().unwrap();
        if buf.is_empty() {
            return Vec::new();
        }
        let mut runs: Vec<(u64, u64)> = Vec::new();
        for (off, chunk) in buf.iter() {
            let end = off + chunk.len() as u64;
            match runs.last_mut() {
                Some(last) if last.1 == *off => last.1 = end,
                _ => runs.push((*off, end)),
            }
        }
        drop(buf);
        let newest = self.last_hole_arrival.load(Ordering::Relaxed);
        let mut out: Vec<(u64, u64)> = Vec::with_capacity(nya_proto::MAX_SACK_RANGES);
        if let Some(r) = runs.iter().find(|r| r.0 <= newest && newest < r.1) {
            out.push(*r);
        }
        for r in runs.iter().rev() {
            if out.len() >= nya_proto::MAX_SACK_RANGES {
                break;
            }
            if !out.contains(r) {
                out.push(*r);
            }
        }
        out
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

    /// P1.4: bytes of the contiguous run starting at `recv_next` still in
    /// `recv_buf` (non-zero only when `drain_recv` re-inserted the head
    /// because the inbound channel was full). Caller holds the lock.
    pub(crate) fn in_order_held_locked(&self, buf: &BTreeMap<u64, Vec<u8>>) -> u64 {
        let mut next = self.recv_next.load(Ordering::Relaxed);
        let mut held = 0u64;
        for (off, chunk) in buf.range(next..) {
            if *off != next {
                break;
            }
            held += chunk.len() as u64;
            next += chunk.len() as u64;
        }
        held
    }

    /// P2.1: fold one closed hole's wait into the EWMA (α = 1/8; first
    /// sample seeds) and stamp the sample clock.
    pub(crate) fn sample_hole(&self, d: Duration) {
        let s = (d.as_micros() as u64).max(1);
        let _ = self
            .hole_us
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                Some(if cur == 0 { s } else { (cur * 7 + s) / 8 })
            });
        self.hole_sampled_at_us
            .store(crate::metrics::mono_us().max(1), Ordering::Relaxed);
    }

    /// P2.2: fold one drain's `hole_bytes` into the occupancy EWMA.
    pub(crate) fn sample_hole_bytes(&self, hole_bytes: u64) {
        let _ = self
            .hole_bytes_ewma
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                Some(if cur == 0 {
                    hole_bytes
                } else {
                    (cur * 7 + hole_bytes) / 8
                })
            });
        self.hole_bytes_at_us
            .store(crate::metrics::mono_us().max(1), Ordering::Relaxed);
    }

    /// Coalesce until `dt >= min_dt` (one RTT, or maintain_interval). First
    /// bytes are kept; a collapsed-but-nonzero Instant is not a rate.
    /// Returns `true` when a new rate sample was recorded.
    pub(crate) fn note_deliver(&self, len: u64, min_dt: Duration) -> bool {
        if len == 0 {
            return false;
        }
        let now = Instant::now();
        let mut last = self.last_deliver.lock().unwrap();
        let pending = self.pending_deliver.fetch_add(len, Ordering::Relaxed) + len;
        let Some(prev) = *last else {
            *last = Some(now);
            return false;
        };
        let dt = now.saturating_duration_since(prev);
        // Decode-speed FramedRead gaps are tens of µs; require a full RTT.
        if dt.is_zero() || dt < min_dt {
            return false;
        }
        *last = Some(now);
        self.pending_deliver.store(0, Ordering::Relaxed);
        drop(last);
        let sample = (pending as f64 / dt.as_secs_f64()) as u64;
        if sample == 0 {
            return false;
        }
        let old = self.deliver_rate_ewma.load(Ordering::Relaxed);
        // α = 1/8, same 7/8 as class RTT — not a Tuning field.
        let ewma = if old == 0 {
            sample
        } else {
            old.saturating_mul(7).saturating_add(sample) / 8
        };
        self.deliver_rate_ewma.store(ewma, Ordering::Relaxed);
        true
    }

    #[cfg(test)]
    pub(crate) fn debug_set_deliver_clock(&self, start: Instant) {
        *self.last_deliver.lock().unwrap() = Some(start);
    }
}

/// P2.4: a stream's share of the session's window memory is released
/// with its last `Arc`, whoever drops it — a `tune_recv_cap` racing the
/// map removal is accounted by the same Drop that runs after it.
impl Drop for StreamState {
    fn drop(&mut self) {
        let extra = u64::from(
            self.recv_cap
                .load(Ordering::Relaxed)
                .saturating_sub(self.initial_window),
        );
        if extra > 0 {
            let _ = self
                .recv_cap_extra
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_sub(extra))
                });
        }
    }
}

pub struct TunnelStream {
    pub id: u32,
    inner: DuplexStream,
    counters: Arc<StreamCounters>,
}

impl TunnelStream {
    pub fn from_duplex(id: u32, inner: DuplexStream, counters: Arc<StreamCounters>) -> Self {
        Self {
            id,
            inner,
            counters,
        }
    }

    /// Limiter summary of this stream, valid after the session reaped it.
    pub fn stats(&self) -> StreamStats {
        self.counters.stats()
    }

    pub fn counters(&self) -> &Arc<StreamCounters> {
        &self.counters
    }

    /// Resolves once the session has removed this stream from its table.
    /// Owned (`'static`), so it can be taken before the stream is borrowed
    /// mutably by the copier.
    pub fn gone(&self) -> WaitForCancellationFutureOwned {
        self.counters.gone.clone().cancelled_owned()
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
