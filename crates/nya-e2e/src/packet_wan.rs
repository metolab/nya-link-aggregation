//! Packet WAN between two TCP sockets.
//!
//! Bytes are sliced into MSS-sized packets, independently delayed, and
//! independently dropped. Loss recovery has the same *shape* as Linux TCP
//! reacting to IP-layer drops: the receiver acknowledges every packet it
//! gets (SACK), a packet with three later packets acknowledged is
//! retransmitted at once (RACK/FACK), a tail loss is probed at 2×SRTT (TLP),
//! and RTO = SRTT + 4×RTTVAR with the kernel's 200 ms floor is the backstop.
//! cwnd is Reno: slow start to `ssthresh`, then +1 MSS per RTT; each loss
//! episode halves it once. (This host has no CAP_NET_ADMIN, so
//! we cannot insert `tc netem` in front of Linux TCP.)

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::Rng;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use super::impair::{ConnCtrl, ImpairInner};

const MSS: usize = 1200;
const INIT_CWND: u32 = 16;
const MIN_CWND: u32 = 2;

struct Pkt {
    seq: u64,
    buf: Vec<u8>,
}

/// A packet on the wire: arrives at the far end at `at`.
struct Scheduled {
    at: Instant,
    pkt: Pkt,
}

/// One ordered delivery clock per pipe: packets arrive in `at` order, so a
/// FIFO bottleneck plus constant propagation delay never reorders (only
/// jitter does). Per-packet timers would reorder every burst inside one
/// timer tick and make any RACK-style loss detector fire spuriously.
fn spawn_wire(
    inner: Arc<ImpairInner>,
    wire_tx: mpsc::UnboundedSender<Pkt>,
) -> mpsc::UnboundedSender<Scheduled> {
    let (tx, mut rx) = mpsc::unbounded_channel::<Scheduled>();
    tokio::spawn(async move {
        let mut heap: BTreeMap<(Instant, u64), Pkt> = BTreeMap::new();
        let mut k: u64 = 0;
        let mut closed = false;
        loop {
            if closed && heap.is_empty() {
                break;
            }
            let next = heap.keys().next().map(|(at, _)| *at);
            tokio::select! {
                biased;
                m = rx.recv(), if !closed => {
                    match m {
                        Some(s) => {
                            heap.insert((s.at, k), s.pkt);
                            k += 1;
                        }
                        None => closed = true,
                    }
                }
                _ = async {
                    match next {
                        Some(at) => tokio::time::sleep_until(tokio::time::Instant::from_std(at)).await,
                        None => std::future::pending::<()>().await,
                    }
                }, if next.is_some() => {
                    if let Some((_, pkt)) = heap.pop_first() {
                        if wire_tx.send(pkt).is_err() {
                            break;
                        }
                        inner.wake.notify_waiters();
                    }
                }
            }
        }
    });
    tx
}

pub(crate) async fn wan_pipe<R, W>(
    rd: &mut R,
    wr: &mut W,
    inner: Arc<ImpairInner>,
    conn: Arc<ConnCtrl>,
    fwd: bool,
) -> std::io::Result<()>
where
    R: AsyncReadExt + Unpin,
    W: AsyncWriteExt + Unpin,
{
    let (wire_pkt_tx, mut wire_rx) = mpsc::unbounded_channel::<Pkt>();
    let wire_tx = spawn_wire(inner.clone(), wire_pkt_tx);
    let (ack_tx, mut ack_rx) = mpsc::unbounded_channel::<u64>();

    let deliver = {
        let inner = inner.clone();
        let wr_ack = ack_tx.clone();
        async move {
            let mut expected: u64 = 0;
            let mut reorder: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
            while let Some(pkt) = wire_rx.recv().await {
                if pkt.seq == expected {
                    if wr.write_all(&pkt.buf).await.is_err() {
                        break;
                    }
                    let n = pkt.buf.len() as u64;
                    if fwd {
                        inner.bytes_fwd.fetch_add(n, Ordering::Relaxed);
                    } else {
                        inner.bytes_rev.fetch_add(n, Ordering::Relaxed);
                    }
                    let _ = wr_ack.send(pkt.seq);
                    expected += 1;
                    while let Some(buf) = reorder.remove(&expected) {
                        if wr.write_all(&buf).await.is_err() {
                            return;
                        }
                        let n = buf.len() as u64;
                        if fwd {
                            inner.bytes_fwd.fetch_add(n, Ordering::Relaxed);
                        } else {
                            inner.bytes_rev.fetch_add(n, Ordering::Relaxed);
                        }
                        let _ = wr_ack.send(expected);
                        expected += 1;
                    }
                    let _ = wr.flush().await;
                } else if pkt.seq > expected {
                    // SACK: acknowledge exactly what arrived.
                    reorder.insert(pkt.seq, pkt.buf);
                    let _ = wr_ack.send(pkt.seq);
                } else {
                    // duplicate
                    let _ = wr_ack.send(pkt.seq);
                }
            }
        }
    };

    let ingress = async move {
        let mut inflight: BTreeMap<u64, (Vec<u8>, Instant, u32)> = BTreeMap::new();
        let mut next_seq: u64 = 0;
        let mut cwnd: u32 = INIT_CWND;
        let mut ssthresh: u32 = u32::MAX;
        let mut ca_acc: u32 = 0;
        let mut srtt = inner.rtt_us.load(Ordering::Relaxed).max(1);
        let mut rttvar = srtt / 2;
        let mut have_sample = false;
        // Newest sequence a tail-loss probe has been sent for.
        let mut tlp_done: Option<u64> = None;
        // Sequence number past which the current loss episode ends; cwnd is
        // halved once per episode (Linux "recovery point").
        let mut recovery_end: Option<u64> = None;
        let mut buf = vec![0u8; MSS];
        let mut leftover: Vec<u8> = Vec::new();

        loop {
            if inner.drop_all.load(Ordering::Relaxed) {
                break;
            }
            if fwd {
                inner.cwnd_fwd.store(cwnd as u64, Ordering::Relaxed);
            } else {
                inner.cwnd_rev.store(cwnd as u64, Ordering::Relaxed);
            }
            let rto = rto_of(srtt, rttvar);
            let rto_at = inflight.values().map(|(_, t, _)| *t + rto).min();
            let tlp_at = inflight
                .iter()
                .next_back()
                .filter(|(s, _)| tlp_done != Some(**s))
                .map(|(_, (_, t, _))| *t + tlp_of(srtt));
            let next_deadline = match (rto_at, tlp_at) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };

            tokio::select! {
                biased;
                _ = inner.wake.notified() => {}
                ack = ack_rx.recv() => {
                    let Some(ack) = ack else { break; };
                    let mut acked_sent: Option<Instant> = None;
                    if let Some((_, sent, tries)) = inflight.remove(&ack) {
                        acked_sent = Some(sent);
                        if tries == 1 {
                            // Karn: only never-retransmitted packets sample RTT.
                            let sample = sent.elapsed().as_micros().max(1) as u64;
                            if !have_sample {
                                srtt = sample;
                                rttvar = sample / 2;
                                have_sample = true;
                            } else {
                                rttvar = (rttvar * 3 + srtt.abs_diff(sample)) / 4;
                                srtt = (srtt * 7 + sample) / 8;
                            }
                        }
                        if cwnd < ssthresh {
                            cwnd += 1;
                        } else {
                            ca_acc += 1;
                            if ca_acc >= cwnd {
                                ca_acc = 0;
                                cwnd += 1;
                            }
                        }
                        cwnd = cwnd.min(inner.max_cwnd(MSS));
                    }
                    if recovery_end.is_some_and(|e| ack >= e) {
                        recovery_end = None;
                    }
                    // RACK: a packet sent a reordering window before one
                    // that has been acknowledged is lost. Time-based, so
                    // timer-granularity reordering of a burst is tolerated.
                    let reo_wnd = Duration::from_micros((srtt / 4).max(1_000));
                    let lost: Vec<u64> = match acked_sent {
                        Some(t) => inflight
                            .iter()
                            .filter(|(_, (_, sent, _))| *sent + reo_wnd < t)
                            .map(|(s, _)| *s)
                            .collect(),
                        None => Vec::new(),
                    };
                    if !lost.is_empty() {
                        if recovery_end.is_none() {
                            ssthresh = MIN_CWND.max(cwnd / 2);
                            cwnd = ssthresh;
                            recovery_end = Some(next_seq);
                        }
                        for seq in lost {
                            if let Some((buf, last, tries)) = inflight.get_mut(&seq) {
                                // One fast retransmit per packet per RTO.
                                if last.elapsed() < tlp_of(srtt) {
                                    continue;
                                }
                                *tries += 1;
                                inner.retrans.fetch_add(1, Ordering::Relaxed);
                                *last = Instant::now();
                                transmit(&inner, &conn, seq, buf.clone(), &wire_tx, fwd);
                            }
                        }
                    }
                }
                _ = async {
                    if let Some(at) = next_deadline {
                        tokio::time::sleep(at.saturating_duration_since(Instant::now())).await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                }, if next_deadline.is_some() => {
                    let now = Instant::now();
                    let rto = rto_of(srtt, rttvar);
                    let due: Vec<u64> = inflight
                        .iter()
                        .filter(|(_, (_, t, _))| now >= *t + rto)
                        .map(|(s, _)| *s)
                        .collect();
                    if !due.is_empty() {
                        // RTO: Linux collapses cwnd to 1 MSS; halving keeps
                        // the shape without the full restart.
                        ssthresh = MIN_CWND.max(cwnd / 2);
                        cwnd = ssthresh;
                        recovery_end = Some(next_seq);
                        for seq in due {
                            if let Some((buf, last, tries)) = inflight.get_mut(&seq) {
                                *tries += 1;
                                if *tries > 12 {
                                    return Err(std::io::Error::other("wan rto give up"));
                                }
                                inner.retrans.fetch_add(1, Ordering::Relaxed);
                                *last = Instant::now();
                                transmit(&inner, &conn, seq, buf.clone(), &wire_tx, fwd);
                            }
                        }
                    } else if let Some((seq, (buf, _, tries))) = inflight.iter_mut().next_back() {
                        // TLP: re-send the newest packet; its SACK exposes
                        // any hole before it. Not a loss signal by itself.
                        if tlp_done != Some(*seq) && now >= tlp_at.unwrap_or(now) {
                            tlp_done = Some(*seq);
                            *tries += 1;
                            inner.retrans.fetch_add(1, Ordering::Relaxed);
                            transmit(&inner, &conn, *seq, buf.clone(), &wire_tx, fwd);
                        }
                    }
                }
                n = rd.read(&mut buf), if inflight.len() < cwnd as usize && !blocked(&inner, &conn, fwd) => {
                    let n = n?;
                    if n == 0 && leftover.is_empty() {
                        break;
                    }
                    leftover.extend_from_slice(&buf[..n]);
                    // Flush immediately (handshake is small records). Split only at MSS.
                    while !leftover.is_empty() && inflight.len() < cwnd as usize {
                        let take = leftover.len().min(MSS);
                        let pkt = leftover.drain(..take).collect::<Vec<_>>();
                        let seq = next_seq;
                        next_seq += 1;
                        inflight.insert(seq, (pkt.clone(), Instant::now(), 1));
                        transmit(&inner, &conn, seq, pkt, &wire_tx, fwd);
                        if take < MSS {
                            break;
                        }
                    }
                    if n == 0 {
                        break;
                    }
                }
            }
        }
        Ok(())
    };

    tokio::select! {
        r = ingress => { r?; }
        _ = deliver => {}
    }
    Ok(())
}

/// Linux RTO: `SRTT + 4×RTTVAR`, floored at `TCP_RTO_MIN` (200 ms).
fn rto_of(srtt_us: u64, rttvar_us: u64) -> Duration {
    let us = (srtt_us + 4 * rttvar_us).clamp(200_000, 1_000_000);
    Duration::from_micros(us)
}

/// Tail-loss probe: `2×SRTT`, floor 10 ms (Linux `tcp_schedule_loss_probe`).
fn tlp_of(srtt_us: u64) -> Duration {
    Duration::from_micros((srtt_us * 2).clamp(10_000, 1_000_000))
}

fn blocked(inner: &ImpairInner, conn: &ConnCtrl, fwd: bool) -> bool {
    inner.blackhole.load(Ordering::Relaxed)
        || conn.blackhole.load(Ordering::Relaxed)
        || (fwd && conn.stall.load(Ordering::Relaxed))
}

fn transmit(
    inner: &Arc<ImpairInner>,
    conn: &ConnCtrl,
    seq: u64,
    buf: Vec<u8>,
    wire: &mpsc::UnboundedSender<Scheduled>,
    fwd: bool,
) {
    if inner.blackhole.load(Ordering::Relaxed) || conn.blackhole.load(Ordering::Relaxed) {
        return;
    }
    let p = inner.loss_ppm.load(Ordering::Relaxed) as f64 / 1_000_000.0;
    if p > 0.0 && rand::thread_rng().gen::<f64>() < p {
        inner.drops.fetch_add(1, Ordering::Relaxed);
        return; // lost this attempt; sender will RTO
    }
    // Shared bottleneck: one FIFO + departure clock per link direction. All
    // connections on the link queue behind each other, exactly like a real
    // access link; tail-drop when the byte queue is full.
    let depart = match bottleneck_depart(inner, fwd, buf.len() as u64) {
        Ok(d) => d,
        Err(()) => {
            inner.queue_drops.fetch_add(1, Ordering::Relaxed);
            inner.drops.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    let delay = inner.one_way();
    let at = depart.unwrap_or_else(Instant::now) + delay;
    let _ = wire.send(Scheduled {
        at,
        pkt: Pkt { seq, buf },
    });
}

/// `Ok(None)`: no rate limit. `Ok(Some(t))`: departs the bottleneck at `t`.
/// `Err`: queue full, packet dropped.
fn bottleneck_depart(inner: &ImpairInner, fwd: bool, n: u64) -> Result<Option<Instant>, ()> {
    let rate = inner.rate_bps.load(Ordering::Relaxed);
    if rate == 0 {
        return Ok(None);
    }
    let qmax = inner.queue_bytes.load(Ordering::Relaxed);
    let mut q = if fwd {
        inner.q_fwd.lock().unwrap()
    } else {
        inner.q_rev.lock().unwrap()
    };
    let now = Instant::now();
    let start = q.next_free.filter(|t| *t > now).unwrap_or(now);
    // Backlog still to be serialised, in bytes.
    let backlog = (start.saturating_duration_since(now).as_nanos() as u64).saturating_mul(rate)
        / 8
        / 1_000_000_000;
    if qmax != 0 && backlog + n > qmax {
        return Err(());
    }
    let ser = Duration::from_nanos(n.saturating_mul(8).saturating_mul(1_000_000_000) / rate);
    let depart = start + ser;
    q.next_free = Some(depart);
    Ok(Some(depart))
}
