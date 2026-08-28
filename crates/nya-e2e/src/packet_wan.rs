//! Packet WAN between two TCP sockets.
//!
//! Bytes are sliced into MSS-sized packets, independently delayed, and
//! independently dropped. The sender retransmits on RTO (2×SRTT, floor 20ms)
//! and on loss halves cwnd — the same *shape* of behaviour as kernel TCP
//! reacting to IP-layer drops. (This host has no CAP_NET_ADMIN, so we cannot
//! insert `tc netem` in front of Linux TCP.)

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
const MAX_CWND: u32 = 64;

struct Pkt {
    seq: u64,
    buf: Vec<u8>,
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
    let (wire_tx, mut wire_rx) = mpsc::unbounded_channel::<Pkt>();
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
                    reorder.insert(pkt.seq, pkt.buf);
                    // dup ack for last in-order
                    if expected > 0 {
                        let _ = wr_ack.send(expected - 1);
                    }
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
        let mut srtt = inner.rtt_us.load(Ordering::Relaxed).max(1);
        let mut buf = vec![0u8; MSS];
        let mut leftover: Vec<u8> = Vec::new();

        loop {
            if inner.drop_all.load(Ordering::Relaxed) {
                break;
            }
            let rto = rto_of(srtt);
            let next_deadline = inflight.values().map(|(_, t, _)| *t + rto).min();

            tokio::select! {
                biased;
                _ = inner.wake.notified() => {}
                ack = ack_rx.recv() => {
                    let Some(ack) = ack else { break; };
                    if let Some((_, sent, _)) = inflight.remove(&ack) {
                        let sample = sent.elapsed().as_micros() as u64;
                        srtt = if srtt == inner.rtt_us.load(Ordering::Relaxed) {
                            sample.max(1)
                        } else {
                            (srtt * 7 + sample) / 8
                        };
                        cwnd = (cwnd + 1).min(MAX_CWND);
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
                    let rto = rto_of(srtt);
                    let due: Vec<u64> = inflight
                        .iter()
                        .filter(|(_, (_, t, _))| now >= *t + rto)
                        .map(|(s, _)| *s)
                        .collect();
                    if !due.is_empty() {
                        cwnd = MIN_CWND.max(cwnd / 2);
                    }
                    for seq in due {
                        if let Some((buf, last, tries)) = inflight.get_mut(&seq) {
                            *tries += 1;
                            if *tries > 12 {
                                return Err(std::io::Error::other("wan rto give up"));
                            }
                            inner.retrans.fetch_add(1, Ordering::Relaxed);
                            *last = Instant::now();
                            transmit(&inner, &conn, seq, buf.clone(), &wire_tx);
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
                        transmit(&inner, &conn, seq, pkt, &wire_tx);
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

fn rto_of(srtt_us: u64) -> Duration {
    let us = (srtt_us * 2).clamp(20_000, 1_000_000);
    Duration::from_micros(us)
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
    wire: &mpsc::UnboundedSender<Pkt>,
) {
    if inner.blackhole.load(Ordering::Relaxed) || conn.blackhole.load(Ordering::Relaxed) {
        return;
    }
    let p = inner.loss_ppm.load(Ordering::Relaxed) as f64 / 1_000_000.0;
    if p > 0.0 && rand::thread_rng().gen::<f64>() < p {
        inner.drops.fetch_add(1, Ordering::Relaxed);
        return; // lost this attempt; sender will RTO
    }
    let delay = inner.one_way();
    let tx = wire.clone();
    let inner = inner.clone();
    tokio::spawn(async move {
        if delay > Duration::ZERO {
            tokio::time::sleep(delay).await;
        }
        let _ = tx.send(Pkt { seq, buf });
        inner.wake.notify_waiters();
    });
}
