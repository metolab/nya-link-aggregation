//! Process-edge first/last-byte clocks for overlay vs origin attribution.
//!
//! No tracing in poll. Missing hops are `None`, never 0.

use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::task::JoinSet;

const HOST_TAIL_MAX: usize = 48;

fn nz(v: u64) -> Option<u64> {
    (v != 0).then_some(v)
}

fn elapsed_us(start: Instant) -> u64 {
    (start.elapsed().as_micros() as u64).max(1)
}

pub struct HopClock {
    start: Instant,
    first_rx_us: AtomicU64,
    first_tx_us: AtomicU64,
    last_rx_us: AtomicU64,
}

impl HopClock {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            start: Instant::now(),
            first_rx_us: AtomicU64::new(0),
            first_tx_us: AtomicU64::new(0),
            last_rx_us: AtomicU64::new(0),
        })
    }

    pub fn first_rx_us(&self) -> Option<u64> {
        nz(self.first_rx_us.load(Ordering::Relaxed))
    }

    pub fn first_tx_us(&self) -> Option<u64> {
        nz(self.first_tx_us.load(Ordering::Relaxed))
    }

    pub fn last_rx_us(&self) -> Option<u64> {
        nz(self.last_rx_us.load(Ordering::Relaxed))
    }
}

/// Origin-probe peer samples. All 0 = never. Debug + tail only; never observe.
pub struct OriginPeerSlots {
    /// Overlay last_rx at the *last* origin byte (close_notify can overwrite GET).
    pub crx_at_olast: AtomicU64,
    /// Winning `origin_elapsed − overlay.last_rx` (missing overlay last_rx as 0).
    pub max_gap_us: AtomicU64,
    /// Overlay last_rx when max_gap won (GET-arrival for origin-think).
    pub crx_at_gap: AtomicU64,
    /// Origin elapsed when max_gap won.
    pub origin_at_gap: AtomicU64,
}

impl Default for OriginPeerSlots {
    fn default() -> Self {
        Self {
            crx_at_olast: AtomicU64::new(0),
            max_gap_us: AtomicU64::new(0),
            crx_at_gap: AtomicU64::new(0),
            origin_at_gap: AtomicU64::new(0),
        }
    }
}

impl OriginPeerSlots {
    pub fn crx_at_olast(&self) -> Option<u64> {
        nz(self.crx_at_olast.load(Ordering::Relaxed))
    }

    pub fn max_gap_us(&self) -> Option<u64> {
        nz(self.max_gap_us.load(Ordering::Relaxed))
    }

    pub fn crx_at_gap(&self) -> Option<u64> {
        nz(self.crx_at_gap.load(Ordering::Relaxed))
    }

    pub fn origin_at_gap(&self) -> Option<u64> {
        nz(self.origin_at_gap.load(Ordering::Relaxed))
    }
}

pub struct HopProbe<T> {
    inner: T,
    clock: Arc<HopClock>,
    peer: Option<Arc<HopClock>>,
    slots: Option<Arc<OriginPeerSlots>>,
}

impl<T> HopProbe<T> {
    pub fn wrap(inner: T, clock: Arc<HopClock>) -> Self {
        Self {
            inner,
            clock,
            peer: None,
            slots: None,
        }
    }

    /// Origin side. Samples overlay `last_rx` on every non-empty origin read.
    pub fn sample_peer_last_on_read(
        mut self,
        peer: Arc<HopClock>,
        slots: Arc<OriginPeerSlots>,
    ) -> Self {
        self.peer = Some(peer);
        self.slots = Some(slots);
        self
    }

    pub fn clock(&self) -> &Arc<HopClock> {
        &self.clock
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for HopProbe<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let polled = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &polled {
            if buf.filled().len() > before {
                let us = elapsed_us(self.clock.start);
                let _ = self.clock.first_rx_us.compare_exchange(
                    0,
                    us,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                );
                self.clock.last_rx_us.store(us, Ordering::Relaxed);
                if let (Some(peer), Some(slots)) = (self.peer.as_ref(), self.slots.as_ref()) {
                    let crx = peer.last_rx_us.load(Ordering::Relaxed);
                    let gap = us.saturating_sub(crx);
                    slots.crx_at_olast.store(crx, Ordering::Relaxed);
                    if gap > slots.max_gap_us.load(Ordering::Relaxed) {
                        slots.max_gap_us.store(gap, Ordering::Relaxed);
                        slots.crx_at_gap.store(crx, Ordering::Relaxed);
                        slots.origin_at_gap.store(us, Ordering::Relaxed);
                    }
                }
            }
        }
        polled
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for HopProbe<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let polled = Pin::new(&mut self.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = &polled {
            if *n > 0 {
                let us = elapsed_us(self.clock.start);
                let _ = self.clock.first_tx_us.compare_exchange(
                    0,
                    us,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                );
            }
        }
        polled
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum HopRole {
    #[default]
    Client,
    Server,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum HopOutcome {
    #[default]
    Ok,
    OpenFail,
    DialFail,
    CopyErr,
}

impl HopOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::OpenFail => "open_fail",
            Self::DialFail => "dial_fail",
            Self::CopyErr => "copy_err",
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct HopSample {
    pub role: HopRole,
    pub stream_id: u32,
    pub host: String,
    pub outcome: HopOutcome,
    pub copy_us: Option<u64>,
    pub open_us: Option<u64>,
    pub first_rx_us: Option<u64>,
    pub last_rx_us: Option<u64>,
    pub first_tx_us: Option<u64>,
    pub dial_us: Option<u64>,
    pub origin_first_rx_us: Option<u64>,
    pub origin_last_rx_us: Option<u64>,
    pub client_first_rx_us: Option<u64>,
    pub client_last_rx_us: Option<u64>,
    pub crx_at_olast: Option<u64>,
    pub max_gap: Option<u64>,
    pub crx_at_gap: Option<u64>,
    pub origin_at_gap: Option<u64>,
}

impl HopSample {
    pub fn rank_us(&self) -> u64 {
        [
            self.copy_us,
            self.open_us,
            self.first_rx_us,
            self.last_rx_us,
            self.first_tx_us,
            self.dial_us,
            self.origin_first_rx_us,
            self.origin_last_rx_us,
            self.client_first_rx_us,
            self.client_last_rx_us,
            self.crx_at_olast,
            self.max_gap,
            self.crx_at_gap,
            self.origin_at_gap,
        ]
        .into_iter()
        .flatten()
        .max()
        .unwrap_or(0)
    }

    pub fn format_debug_fields(&self) -> String {
        let mut parts = Vec::new();
        fn push(parts: &mut Vec<String>, k: &str, v: Option<u64>) {
            if let Some(x) = v {
                parts.push(format!("{k}={x}"));
            }
        }
        push(&mut parts, "copy_us", self.copy_us);
        push(&mut parts, "open_us", self.open_us);
        push(&mut parts, "first_rx_us", self.first_rx_us);
        push(&mut parts, "last_rx_us", self.last_rx_us);
        push(&mut parts, "first_tx_us", self.first_tx_us);
        push(&mut parts, "dial_us", self.dial_us);
        push(&mut parts, "origin_first_rx_us", self.origin_first_rx_us);
        push(&mut parts, "origin_last_rx_us", self.origin_last_rx_us);
        push(&mut parts, "client_first_rx_us", self.client_first_rx_us);
        push(&mut parts, "client_last_rx_us", self.client_last_rx_us);
        push(&mut parts, "crx_at_olast", self.crx_at_olast);
        push(&mut parts, "max_gap", self.max_gap);
        push(&mut parts, "crx_at_gap", self.crx_at_gap);
        push(&mut parts, "origin_at_gap", self.origin_at_gap);
        parts.join(" ")
    }

    /// One `tail=` grammar for the info snapshot.
    pub fn format_tail(&self) -> String {
        let host = truncate_host(&self.host);
        let copy = match self.copy_us {
            Some(v) => v.to_string(),
            None => "-".to_string(),
        };
        let mut s = format!("{host} copy={copy}");
        let copy_ran = self.copy_us.is_some();
        match self.role {
            HopRole::Client => {
                if let Some(v) = self.open_us {
                    s.push_str(&format!(" open={v}"));
                }
                push_dashable(&mut s, "first_rx", self.first_rx_us, copy_ran);
                push_dashable(&mut s, "last_rx", self.last_rx_us, copy_ran);
            }
            HopRole::Server => {
                if let Some(v) = self.dial_us {
                    s.push_str(&format!(" dial={v}"));
                }
                push_dashable(&mut s, "ofirst", self.origin_first_rx_us, copy_ran);
                push_dashable(&mut s, "olast", self.origin_last_rx_us, copy_ran);
                push_dashable(&mut s, "cfirst", self.client_first_rx_us, copy_ran);
                push_dashable(&mut s, "clast", self.client_last_rx_us, copy_ran);
                push_dashable(&mut s, "crx_at_olast", self.crx_at_olast, copy_ran);
                push_dashable(&mut s, "max_gap", self.max_gap, copy_ran);
                push_dashable(&mut s, "crx_at_gap", self.crx_at_gap, copy_ran);
                push_dashable(&mut s, "origin_at_gap", self.origin_at_gap, copy_ran);
            }
        }
        s.push_str(&format!(" sid={}", self.stream_id));
        s
    }
}

fn push_dashable(s: &mut String, key: &str, v: Option<u64>, copy_ran: bool) {
    match v {
        Some(x) => s.push_str(&format!(" {key}={x}")),
        None if copy_ran => s.push_str(&format!(" {key}=-")),
        None => {}
    }
}

fn truncate_host(host: &str) -> String {
    if host.len() <= HOST_TAIL_MAX {
        host.to_string()
    } else {
        host.chars().take(HOST_TAIL_MAX).collect()
    }
}

async fn connect_and_nodelay(addr: SocketAddr) -> io::Result<TcpStream> {
    let tcp = TcpStream::connect(addr).await?;
    let _ = tcp.set_nodelay(true);
    Ok(tcp)
}

/// Stable-within-family, then alternate families starting with `addrs[0]`'s family.
pub fn interleave_families(addrs: Vec<SocketAddr>) -> Vec<SocketAddr> {
    if addrs.is_empty() {
        return addrs;
    }
    let v6: Vec<_> = addrs.iter().copied().filter(|a| a.is_ipv6()).collect();
    let v4: Vec<_> = addrs.iter().copied().filter(|a| a.is_ipv4()).collect();
    let (head, tail) = if addrs[0].is_ipv6() {
        (v6, v4)
    } else {
        (v4, v6)
    };
    let mut out = Vec::with_capacity(head.len() + tail.len());
    for i in 0..head.len().max(tail.len()) {
        if let Some(&a) = head.get(i) {
            out.push(a);
        }
        if let Some(&a) = tail.get(i) {
            out.push(a);
        }
    }
    out
}

/// Literal IP: one connect, nodelay. Hostname: lookup then race families.
pub async fn connect_origin(host: &str, port: u16, cad: Duration) -> io::Result<TcpStream> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return connect_and_nodelay(SocketAddr::new(ip, port)).await;
    }
    let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host, port)).await?.collect();
    race_origin_addrs(addrs, cad).await
}

type OriginConnect = Pin<Box<dyn Future<Output = io::Result<TcpStream>> + Send>>;

/// Interleave families, then each addr → connect+nodelay.
pub async fn race_origin_addrs(addrs: Vec<SocketAddr>, cad: Duration) -> io::Result<TcpStream> {
    if addrs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            "no addresses",
        ));
    }
    let futs: Vec<OriginConnect> = interleave_families(addrs)
        .into_iter()
        .map(|a| Box::pin(connect_and_nodelay(a)) as OriginConnect)
        .collect();
    race_origin_connects(futs, cad).await
}

/// Already-ordered unit seam. First success wins; losers aborted.
pub async fn race_origin_connects(
    connects: Vec<OriginConnect>,
    cad: Duration,
) -> io::Result<TcpStream> {
    if connects.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            "no addresses",
        ));
    }
    let mut pending: Vec<Option<OriginConnect>> = connects.into_iter().map(Some).collect();
    let n = pending.len();
    let mut next = 0usize;
    let mut set: JoinSet<io::Result<TcpStream>> = JoinSet::new();
    let mut last_err: Option<io::Error> = None;

    let mut start_one = |set: &mut JoinSet<_>, next: &mut usize| {
        while *next < n {
            let i = *next;
            *next += 1;
            if let Some(fut) = pending[i].take() {
                set.spawn(fut);
                return true;
            }
        }
        false
    };
    start_one(&mut set, &mut next);

    loop {
        if set.is_empty() {
            if next < n {
                start_one(&mut set, &mut next);
                continue;
            }
            return Err(last_err.unwrap_or_else(|| {
                io::Error::new(io::ErrorKind::AddrNotAvailable, "no addresses")
            }));
        }
        let more = next < n;
        tokio::select! {
            Some(joined) = set.join_next() => {
                match joined {
                    Ok(Ok(tcp)) => {
                        set.abort_all();
                        while set.join_next().await.is_some() {}
                        return Ok(tcp);
                    }
                    Ok(Err(e)) => {
                        last_err = Some(e);
                        start_one(&mut set, &mut next);
                    }
                    Err(_) => {}
                }
            }
            _ = tokio::time::sleep(cad), if more => {
                start_one(&mut set, &mut next);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn first_rx_on_non_empty_read() {
        let (a, mut b) = tokio::io::duplex(64);
        let clock = HopClock::new();
        let mut probe = HopProbe::wrap(a, clock.clone());
        b.write_all(&[1, 2, 3]).await.unwrap();
        let mut buf = [0u8; 8];
        let n = probe.read(&mut buf).await.unwrap();
        assert_eq!(n, 3);
        assert!(clock.first_rx_us().unwrap() >= 1);
        assert_eq!(clock.first_rx_us(), clock.last_rx_us());
    }

    #[tokio::test]
    async fn eof_does_not_set() {
        let (a, b) = tokio::io::duplex(64);
        drop(b);
        let clock = HopClock::new();
        let mut probe = HopProbe::wrap(a, clock.clone());
        let mut buf = [0u8; 8];
        let n = probe.read(&mut buf).await.unwrap();
        assert_eq!(n, 0);
        assert!(clock.first_rx_us().is_none());
        assert!(clock.last_rx_us().is_none());
    }

    #[tokio::test]
    async fn write_sets_first_tx() {
        let (a, mut b) = tokio::io::duplex(64);
        let clock = HopClock::new();
        let mut probe = HopProbe::wrap(a, clock.clone());
        probe.write_all(&[9]).await.unwrap();
        let mut buf = [0u8; 4];
        let n = b.read(&mut buf).await.unwrap();
        assert_eq!(n, 1);
        assert!(clock.first_tx_us().unwrap() >= 1);
    }

    #[tokio::test]
    async fn last_rx_advances_on_second_read() {
        let (a, mut b) = tokio::io::duplex(64);
        let clock = HopClock::new();
        let mut probe = HopProbe::wrap(a, clock.clone());
        b.write_all(&[1]).await.unwrap();
        let mut buf = [0u8; 1];
        probe.read_exact(&mut buf).await.unwrap();
        let first = clock.first_rx_us().unwrap();
        let last1 = clock.last_rx_us().unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        b.write_all(&[2]).await.unwrap();
        probe.read_exact(&mut buf).await.unwrap();
        assert_eq!(clock.first_rx_us().unwrap(), first);
        assert!(clock.last_rx_us().unwrap() > last1);
    }

    #[tokio::test]
    async fn max_gap_keeps_get_across_close_notify() {
        let (orig_w, orig_r) = tokio::io::duplex(64);
        let (ov_w, ov_r) = tokio::io::duplex(64);
        let origin_clock = HopClock::new();
        let overlay_clock = HopClock::new();
        let slots = Arc::new(OriginPeerSlots::default());
        let mut origin = HopProbe::wrap(orig_r, origin_clock.clone())
            .sample_peer_last_on_read(overlay_clock.clone(), slots.clone());
        let mut overlay = HopProbe::wrap(ov_r, overlay_clock.clone());
        let mut orig_w = orig_w;
        let mut ov_w = ov_w;

        ov_w.write_all(b"GET").await.unwrap();
        let mut get = [0u8; 3];
        overlay.read_exact(&mut get).await.unwrap();
        let t_get = overlay_clock.last_rx_us().unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;

        orig_w.write_all(b"204").await.unwrap();
        let mut http = [0u8; 3];
        origin.read_exact(&mut http).await.unwrap();
        let gap1 = slots.max_gap_us.load(Ordering::Relaxed);
        let crx_gap = slots.crx_at_gap.load(Ordering::Relaxed);
        let origin_at = slots.origin_at_gap.load(Ordering::Relaxed);
        assert!(gap1 >= 15_000, "gap={gap1}");
        assert_eq!(crx_gap, t_get);
        assert!(origin_at > t_get);

        ov_w.write_all(b"CN").await.unwrap();
        let mut cn = [0u8; 2];
        overlay.read_exact(&mut cn).await.unwrap();
        orig_w.write_all(b"X").await.unwrap();
        let mut x = [0u8; 1];
        origin.read_exact(&mut x).await.unwrap();

        assert_eq!(slots.max_gap_us.load(Ordering::Relaxed), gap1);
        assert_eq!(slots.crx_at_gap.load(Ordering::Relaxed), t_get);
        assert_eq!(slots.origin_at_gap.load(Ordering::Relaxed), origin_at);
        assert!(slots.crx_at_olast.load(Ordering::Relaxed) > crx_gap);
    }

    #[tokio::test]
    async fn origin_read_with_overlay_never_read_gap_is_elapsed() {
        let (orig_w, orig_r) = tokio::io::duplex(64);
        let origin_clock = HopClock::new();
        let overlay_clock = HopClock::new();
        let slots = Arc::new(OriginPeerSlots::default());
        let mut origin = HopProbe::wrap(orig_r, origin_clock.clone())
            .sample_peer_last_on_read(overlay_clock, slots.clone());
        tokio::time::sleep(Duration::from_millis(5)).await;
        let mut orig_w = orig_w;
        orig_w.write_all(b"x").await.unwrap();
        let mut buf = [0u8; 1];
        origin.read_exact(&mut buf).await.unwrap();
        let gap = slots.max_gap_us.load(Ordering::Relaxed);
        assert!(gap >= 1);
        assert_eq!(slots.crx_at_gap.load(Ordering::Relaxed), 0);
        assert_eq!(slots.crx_at_olast.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn format_tail_client_and_server() {
        let c = HopSample {
            role: HopRole::Client,
            stream_id: 99,
            host: "www.gstatic.com".into(),
            copy_us: Some(8_012_000),
            open_us: Some(75),
            ..Default::default()
        };
        assert_eq!(
            c.format_tail(),
            "www.gstatic.com copy=8012000 open=75 first_rx=- last_rx=- sid=99"
        );
        let fail = HopSample {
            role: HopRole::Server,
            stream_id: 99,
            host: "www.gstatic.com".into(),
            outcome: HopOutcome::DialFail,
            dial_us: Some(8_001_000),
            ..Default::default()
        };
        assert_eq!(
            fail.format_tail(),
            "www.gstatic.com copy=- dial=8001000 sid=99"
        );
    }

    #[test]
    fn interleave_v6_first_puts_v4_second() {
        let v6a: SocketAddr = "[2001:db8::1]:443".parse().unwrap();
        let v6b: SocketAddr = "[2001:db8::2]:443".parse().unwrap();
        let v4: SocketAddr = "192.0.2.1:443".parse().unwrap();
        assert_eq!(
            interleave_families(vec![v6a, v6b, v4]),
            vec![v6a, v4, v6b]
        );
        assert_eq!(interleave_families(vec![v4, v6a]), vec![v4, v6a]);
    }

    #[tokio::test]
    async fn race_hang_loses_to_fast_second() {
        let cad = Duration::from_millis(20);
        let t0 = Instant::now();
        let tcp = race_origin_connects(
            vec![
                Box::pin(std::future::pending()),
                Box::pin(async {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let addr = listener.local_addr().unwrap();
                    tokio::spawn(async move {
                        let _ = listener.accept().await;
                    });
                    TcpStream::connect(addr).await
                }),
            ],
            cad,
        )
        .await
        .unwrap();
        drop(tcp);
        assert!(
            t0.elapsed() < Duration::from_millis(80),
            "hanging first future must not block TTFB"
        );
    }

    #[tokio::test]
    async fn race_refused_skips_cad() {
        let cad = Duration::from_millis(80);
        let t0 = Instant::now();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = listener.accept().await;
        });
        let tcp = race_origin_connects(
            vec![
                Box::pin(async {
                    Err(io::Error::new(
                        io::ErrorKind::ConnectionRefused,
                        "refused",
                    ))
                }),
                Box::pin(async move { TcpStream::connect(addr).await }),
            ],
            cad,
        )
        .await
        .unwrap();
        drop(tcp);
        assert!(
            t0.elapsed() < cad,
            "hard-fail must not wait CAD, elapsed={:?}",
            t0.elapsed()
        );
    }

    #[tokio::test]
    async fn race_slow_first_loses_to_fast_second() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let _ = listener.accept().await;
            }
        });
        let cad = Duration::from_millis(20);
        let t0 = Instant::now();
        let tcp = race_origin_connects(
            vec![
                Box::pin(async move {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    TcpStream::connect(addr).await
                }),
                Box::pin(async move {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    TcpStream::connect(addr).await
                }),
            ],
            cad,
        )
        .await
        .unwrap();
        drop(tcp);
        assert!(
            t0.elapsed() < Duration::from_millis(80),
            "sequential 200ms first must not become TTFB, elapsed={:?}",
            t0.elapsed()
        );
    }

    #[tokio::test]
    async fn race_abort_all_drops_losers() {
        use std::sync::atomic::AtomicBool;
        struct Flag(Arc<AtomicBool>);
        impl Drop for Flag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let flag = Flag(dropped.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = listener.accept().await;
        });
        let tcp = race_origin_connects(
            vec![
                Box::pin(async move {
                    let _flag = flag;
                    std::future::pending::<io::Result<TcpStream>>().await
                }),
                Box::pin(async move { TcpStream::connect(addr).await }),
            ],
            Duration::from_millis(20),
        )
        .await
        .unwrap();
        drop(tcp);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            dropped.load(Ordering::SeqCst),
            "losing connect future must be aborted"
        );
    }

    #[tokio::test]
    async fn connect_origin_literal_ipv4() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = listener.accept().await;
        });
        let tcp = connect_origin(
            &addr.ip().to_string(),
            addr.port(),
            Duration::from_millis(20),
        )
        .await
        .unwrap();
        drop(tcp);
    }

    #[tokio::test]
    async fn race_single_v4_is_immediate() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = listener.accept().await;
        });
        let t0 = Instant::now();
        let tcp = race_origin_addrs(vec![addr], Duration::from_millis(80))
            .await
            .unwrap();
        drop(tcp);
        assert!(
            t0.elapsed() < Duration::from_millis(80),
            "IPv4-only must not wait CAD"
        );
    }

    #[test]
    fn interleave_empty_and_v4_only() {
        assert!(interleave_families(vec![]).is_empty());
        let v4: SocketAddr = "192.0.2.1:443".parse().unwrap();
        assert_eq!(interleave_families(vec![v4]), vec![v4]);
    }
}
