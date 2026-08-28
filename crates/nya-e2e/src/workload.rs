use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Clone, Debug)]
pub struct PingSample {
    pub at: Instant,
    pub rtt: Option<Duration>,
}

#[derive(Clone, Debug, Default)]
pub struct WorkloadStats {
    pub samples: Vec<PingSample>,
    pub timeouts: u64,
    pub io_errors: u64,
    pub bytes_ok: u64,
    pub disconnect: bool,
}

impl WorkloadStats {
    pub fn rtts_us(&self) -> Vec<u64> {
        self.samples
            .iter()
            .filter_map(|s| s.rtt.map(|d| d.as_micros() as u64))
            .collect()
    }

    pub fn percentile_us(&self, p: f64) -> Option<u64> {
        let mut v = self.rtts_us();
        if v.is_empty() {
            return None;
        }
        v.sort_unstable();
        let idx = ((p / 100.0) * (v.len() as f64 - 1.0)).round() as usize;
        Some(v[idx.min(v.len() - 1)])
    }

    pub fn n_samples(&self) -> usize {
        self.samples.len()
    }

    pub fn n_ok(&self) -> usize {
        self.samples.iter().filter(|s| s.rtt.is_some()).count()
    }

    pub fn min_us(&self) -> Option<u64> {
        self.rtts_us().into_iter().min()
    }

    pub fn max_us(&self) -> Option<u64> {
        self.rtts_us().into_iter().max()
    }

    pub fn mean_us(&self) -> Option<f64> {
        let v = self.rtts_us();
        if v.is_empty() {
            return None;
        }
        Some(v.iter().sum::<u64>() as f64 / v.len() as f64)
    }

    pub fn merge(&mut self, add: &WorkloadStats) {
        self.samples.extend(add.samples.iter().cloned());
        self.timeouts += add.timeouts;
        self.io_errors += add.io_errors;
        self.bytes_ok += add.bytes_ok;
        self.disconnect |= add.disconnect;
    }

    pub fn success_rate(&self) -> f64 {
        let n = self.samples.len() as f64;
        if n == 0.0 {
            return 0.0;
        }
        self.n_ok() as f64 / n
    }

    pub fn resume_after(&self, t0: Instant) -> Option<Duration> {
        self.samples
            .iter()
            .find(|s| s.at >= t0 && s.rtt.is_some())
            .map(|s| s.at.saturating_duration_since(t0))
    }

    /// Largest gap between consecutive successful pings (whole run).
    pub fn max_ok_gap(&self) -> Duration {
        let mut prev_ok: Option<Instant> = None;
        let mut max_gap = Duration::ZERO;
        for s in &self.samples {
            if s.rtt.is_none() {
                continue;
            }
            if let Some(p) = prev_ok {
                max_gap = max_gap.max(s.at.saturating_duration_since(p));
            }
            prev_ok = Some(s.at);
        }
        max_gap
    }

    pub fn slice_from(&self, start: Instant, end: Instant) -> WorkloadStats {
        let samples: Vec<_> = self
            .samples
            .iter()
            .filter(|s| s.at >= start && s.at < end)
            .cloned()
            .collect();
        let timeouts = samples.iter().filter(|s| s.rtt.is_none()).count() as u64;
        WorkloadStats {
            samples,
            timeouts,
            io_errors: 0,
            bytes_ok: 0,
            disconnect: false,
        }
    }

    /// Largest interval between two successful pings that straddle `t0`.
    pub fn gap_around(&self, t0: Instant) -> Duration {
        let mut prev_ok: Option<Instant> = None;
        let mut max_gap = Duration::ZERO;
        for s in &self.samples {
            if s.rtt.is_none() {
                continue;
            }
            if let Some(p) = prev_ok {
                if p <= t0 && s.at >= t0 {
                    max_gap = max_gap.max(s.at.saturating_duration_since(p));
                }
            }
            prev_ok = Some(s.at);
        }
        max_gap
    }
}

enum EchoRead {
    Match,
    SkippedLate,
    Timeout,
    Dead,
}

async fn read_echo(tcp: &mut TcpStream, want: u64, deadline: Instant) -> EchoRead {
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            drain_late(tcp).await;
            return EchoRead::Timeout;
        }
        let mut got = [0u8; 16];
        match tokio::time::timeout(left, tcp.read_exact(&mut got)).await {
            Ok(Ok(_)) => {
                let seq = u64::from_be_bytes(got[..8].try_into().unwrap());
                if seq == want {
                    return EchoRead::Match;
                }
                if seq < want {
                    // late reply from a previous timed-out ping
                    continue;
                }
                drain_late(tcp).await;
                return EchoRead::SkippedLate;
            }
            Ok(Err(_)) => return EchoRead::Dead,
            Err(_) => {
                drain_late(tcp).await;
                return EchoRead::Timeout;
            }
        }
    }
}

async fn drain_late(tcp: &mut TcpStream) {
    let until = Instant::now() + Duration::from_millis(40);
    while Instant::now() < until {
        let mut junk = [0u8; 16];
        let left = until.saturating_duration_since(Instant::now());
        match tokio::time::timeout(left, tcp.read_exact(&mut junk)).await {
            Ok(Ok(_)) => continue,
            _ => break,
        }
    }
}

pub async fn ping_for(
    tcp: &mut TcpStream,
    duration: Duration,
    interval: Duration,
    timeout: Duration,
) -> WorkloadStats {
    let mut stats = WorkloadStats::default();
    let deadline = Instant::now() + duration;
    let mut seq: u64 = 0;
    while Instant::now() < deadline {
        seq += 1;
        let start = Instant::now();
        let mut msg = [0u8; 16];
        msg[..8].copy_from_slice(&seq.to_be_bytes());
        if tcp.write_all(&msg).await.is_err() {
            stats.io_errors += 1;
            stats.disconnect = true;
            stats.samples.push(PingSample {
                at: start,
                rtt: None,
            });
            break;
        }
        match read_echo(tcp, seq, start + timeout).await {
            EchoRead::Match => {
                stats.bytes_ok += 16;
                stats.samples.push(PingSample {
                    at: start,
                    rtt: Some(start.elapsed()),
                });
            }
            EchoRead::SkippedLate => {
                stats.io_errors += 1;
                stats.timeouts += 1;
                stats.samples.push(PingSample {
                    at: start,
                    rtt: None,
                });
            }
            EchoRead::Timeout => {
                stats.timeouts += 1;
                stats.samples.push(PingSample {
                    at: start,
                    rtt: None,
                });
            }
            EchoRead::Dead => {
                stats.io_errors += 1;
                stats.disconnect = true;
                stats.samples.push(PingSample {
                    at: start,
                    rtt: None,
                });
                break;
            }
        }
        let remain = interval.saturating_sub(start.elapsed());
        if !remain.is_zero() {
            tokio::time::sleep(remain).await;
        }
    }
    stats
}

pub async fn bulk_echo(tcp: &mut TcpStream, nbytes: usize) -> Result<(Duration, bool)> {
    let mut send = vec![0u8; nbytes];
    for (i, b) in send.iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }
    let t0 = Instant::now();
    let mut recv = vec![0u8; nbytes];
    let mut off = 0;
    while off < nbytes {
        let n = (nbytes - off).min(16 * 1024);
        tcp.write_all(&send[off..off + n]).await?;
        tcp.read_exact(&mut recv[off..off + n]).await?;
        off += n;
    }
    Ok((t0.elapsed(), recv == send))
}
