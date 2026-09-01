//! Process-edge first/last-byte clocks for overlay vs origin attribution.
//!
//! No tracing in poll. Missing hops are `None`, never 0.

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
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
    rx_bytes: AtomicU64,
    tx_bytes: AtomicU64,
}

impl HopClock {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            start: Instant::now(),
            first_rx_us: AtomicU64::new(0),
            first_tx_us: AtomicU64::new(0),
            last_rx_us: AtomicU64::new(0),
            rx_bytes: AtomicU64::new(0),
            tx_bytes: AtomicU64::new(0),
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

    pub fn rx_bytes(&self) -> u64 {
        self.rx_bytes.load(Ordering::Relaxed)
    }

    pub fn tx_bytes(&self) -> u64 {
        self.tx_bytes.load(Ordering::Relaxed)
    }
}

/// First 4 bytes of the 16-byte session id, hex. Join key across
/// client/server hops when the server has more than one session.
pub fn session_fp_hex(id: &[u8; 16]) -> String {
    format!("{:02x}{:02x}{:02x}{:02x}", id[0], id[1], id[2], id[3])
}

pub fn io_err_kind(e: &io::Error) -> String {
    format!("{:?}", e.kind())
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
                let n = (buf.filled().len() - before) as u64;
                self.clock.rx_bytes.fetch_add(n, Ordering::Relaxed);
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
                self.clock.tx_bytes.fetch_add(*n as u64, Ordering::Relaxed);
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

impl HopRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Client => "client",
            Self::Server => "server",
        }
    }
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
    /// 8 hex chars; empty if the session id was never set (unit tests).
    pub session_fp: String,
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
    pub rx_bytes: Option<u64>,
    pub tx_bytes: Option<u64>,
    pub copy_err: Option<String>,
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
        push(&mut parts, "rx_bytes", self.rx_bytes);
        push(&mut parts, "tx_bytes", self.tx_bytes);
        if !self.session_fp.is_empty() {
            parts.push(format!("session_fp={}", self.session_fp));
        }
        if let Some(ref e) = self.copy_err {
            parts.push(format!("copy_err={e}"));
        }
        parts.join(" ")
    }

    /// Marker span at copy-end. Does not wrap `copy_bidirectional`.
    /// Default traces (`nya_otel=info`) carry first_rx / ofirst / max_gap so
    /// soak `tls≪ttfb` can be attributed without `nya_core::hop=debug`.
    pub fn emit_otel_span(&self) {
        let span = tracing::info_span!(
            target: "nya_otel",
            "nya.hop",
            otel.kind = "internal",
            nya.host = %self.host,
            nya.stream_id = self.stream_id,
            nya.session_fp = tracing::field::Empty,
            nya.hop_role = self.role.as_str(),
            nya.outcome = self.outcome.as_str(),
            nya.copy_us = tracing::field::Empty,
            nya.open_us = tracing::field::Empty,
            nya.first_rx_us = tracing::field::Empty,
            nya.last_rx_us = tracing::field::Empty,
            nya.first_tx_us = tracing::field::Empty,
            nya.dial_us = tracing::field::Empty,
            nya.origin_first_rx_us = tracing::field::Empty,
            nya.origin_last_rx_us = tracing::field::Empty,
            nya.client_first_rx_us = tracing::field::Empty,
            nya.client_last_rx_us = tracing::field::Empty,
            nya.crx_at_olast = tracing::field::Empty,
            nya.max_gap_us = tracing::field::Empty,
            nya.crx_at_gap = tracing::field::Empty,
            nya.origin_at_gap = tracing::field::Empty,
            nya.rx_bytes = tracing::field::Empty,
            nya.tx_bytes = tracing::field::Empty,
            nya.copy_err = tracing::field::Empty,
        );
        fn rec(span: &tracing::Span, name: &'static str, v: Option<u64>) {
            if let Some(v) = v {
                span.record(name, v);
            }
        }
        if !self.session_fp.is_empty() {
            span.record("nya.session_fp", self.session_fp.as_str());
        }
        rec(&span, "nya.copy_us", self.copy_us);
        rec(&span, "nya.open_us", self.open_us);
        rec(&span, "nya.first_rx_us", self.first_rx_us);
        rec(&span, "nya.last_rx_us", self.last_rx_us);
        rec(&span, "nya.first_tx_us", self.first_tx_us);
        rec(&span, "nya.dial_us", self.dial_us);
        rec(&span, "nya.origin_first_rx_us", self.origin_first_rx_us);
        rec(&span, "nya.origin_last_rx_us", self.origin_last_rx_us);
        rec(&span, "nya.client_first_rx_us", self.client_first_rx_us);
        rec(&span, "nya.client_last_rx_us", self.client_last_rx_us);
        rec(&span, "nya.crx_at_olast", self.crx_at_olast);
        rec(&span, "nya.max_gap_us", self.max_gap);
        rec(&span, "nya.crx_at_gap", self.crx_at_gap);
        rec(&span, "nya.origin_at_gap", self.origin_at_gap);
        rec(&span, "nya.rx_bytes", self.rx_bytes);
        rec(&span, "nya.tx_bytes", self.tx_bytes);
        if let Some(ref e) = self.copy_err {
            span.record("nya.copy_err", e.as_str());
        }
        let _g = span.entered();
    }

    /// One `tail=` grammar for the info snapshot.
    pub fn format_tail(&self) -> String {
        let host = truncate_host(&self.host);
        let copy = match self.copy_us {
            Some(v) => v.to_string(),
            None => "-".to_string(),
        };
        let mut s = format!("{host} copy={copy}");
        if !self.session_fp.is_empty() {
            s.push_str(&format!(" sfp={}", self.session_fp));
        }
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
        if let Some(v) = self.rx_bytes {
            s.push_str(&format!(" rx={v}"));
        }
        if let Some(v) = self.tx_bytes {
            s.push_str(&format!(" tx={v}"));
        }
        if let Some(ref e) = self.copy_err {
            s.push_str(&format!(" err={e}"));
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OriginFamily {
    V4,
    V6,
}

/// Lookup / winner metadata for `nya.outbound.dial`. Pending family is `None`,
/// not `0` (`Some(0)` = that family completed empty).
#[derive(Clone, Debug, Default)]
pub struct OriginDialMeta {
    pub lookup_a_us: Option<u64>,
    pub lookup_aaaa_us: Option<u64>,
    pub n_v4: Option<u32>,
    pub n_v6: Option<u32>,
    pub cache_v4: u32,
    pub cache_v6: u32,
    pub winner: &'static str,
}

pub struct OriginDial {
    pub stream: TcpStream,
    pub meta: OriginDialMeta,
}

fn is_family_empty(e: &dns_lookup::LookupError) -> bool {
    matches!(
        e.kind(),
        dns_lookup::LookupErrorKind::NoName
            | dns_lookup::LookupErrorKind::NoData
            | dns_lookup::LookupErrorKind::Family
            | dns_lookup::LookupErrorKind::Again
            | dns_lookup::LookupErrorKind::Socktype
    )
}

/// SOCKS/origin hostnames are FQDNs. A trailing dot skips resolv search
/// (`ndots` / `search`), so a hanging search-domain A query cannot sit
/// in front of a working AAAA (or the reverse).
fn origin_lookup_name(host: &str) -> String {
    if host.ends_with('.') {
        host.to_string()
    } else {
        let mut s = String::with_capacity(host.len() + 1);
        s.push_str(host);
        s.push('.');
        s
    }
}

#[derive(Clone, Default)]
struct CachedOrigin {
    v4: Vec<IpAddr>,
    v6: Vec<IpAddr>,
}

const ORIGIN_CACHE_HOSTS: usize = 4096;
const ORIGIN_CACHE_PER_FAMILY: usize = 8;

fn origin_addr_cache() -> &'static Mutex<HashMap<(String, u16), CachedOrigin>> {
    static CACHE: OnceLock<Mutex<HashMap<(String, u16), CachedOrigin>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn cache_lock() -> std::sync::MutexGuard<'static, HashMap<(String, u16), CachedOrigin>> {
    origin_addr_cache()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

fn cached_origin_addrs(host: &str, port: u16) -> (Vec<SocketAddr>, Vec<SocketAddr>) {
    let key = (host.to_ascii_lowercase(), port);
    let g = cache_lock();
    match g.get(&key) {
        Some(c) => (
            c.v4.iter().map(|ip| SocketAddr::new(*ip, port)).collect(),
            c.v6.iter().map(|ip| SocketAddr::new(*ip, port)).collect(),
        ),
        None => (Vec::new(), Vec::new()),
    }
}

fn remember_origin_addr(host: &str, port: u16, addr: SocketAddr) {
    let key = (host.to_ascii_lowercase(), port);
    let mut g = cache_lock();
    if g.len() >= ORIGIN_CACHE_HOSTS && !g.contains_key(&key) {
        g.clear();
    }
    let e = g.entry(key).or_default();
    let ip = addr.ip();
    let slot = if ip.is_ipv4() { &mut e.v4 } else { &mut e.v6 };
    if !slot.contains(&ip) {
        slot.push(ip);
        if slot.len() > ORIGIN_CACHE_PER_FAMILY {
            slot.remove(0);
        }
    }
}

#[cfg(test)]
pub fn clear_origin_addr_cache() {
    cache_lock().clear();
}

fn lookup_family_blocking(
    host: &str,
    port: u16,
    family: OriginFamily,
) -> io::Result<Vec<SocketAddr>> {
    use dns_lookup::{getaddrinfo, AddrFamily, AddrInfoHints, SockType};
    let host = origin_lookup_name(host);
    let hints = AddrInfoHints {
        socktype: SockType::Stream.into(),
        address: match family {
            OriginFamily::V4 => AddrFamily::Inet.into(),
            OriginFamily::V6 => AddrFamily::Inet6.into(),
        },
        ..AddrInfoHints::default()
    };
    match getaddrinfo(Some(&host), Some(&port.to_string()), Some(hints)) {
        Ok(iter) => Ok(iter
            .filter_map(Result::ok)
            .map(|a| a.sockaddr)
            .filter(|a| match family {
                OriginFamily::V4 => a.is_ipv4(),
                OriginFamily::V6 => a.is_ipv6(),
            })
            .collect()),
        Err(e) if is_family_empty(&e) => Ok(Vec::new()),
        Err(e) => Err(e.into()),
    }
}

async fn lookup_family(
    host: String,
    port: u16,
    family: OriginFamily,
) -> io::Result<Vec<SocketAddr>> {
    tokio::task::spawn_blocking(move || lookup_family_blocking(&host, port, family))
        .await
        .unwrap_or_else(|e| Err(io::Error::other(e)))
}

/// Literal IP: one connect, nodelay. Hostname: split A/AAAA, connect as soon as
/// one family returns. Last origin IP that actually connected is retried
/// immediately so a hanging family lookup cannot sit in front of a known-good
/// 5-tuple. Do not wait for dual-stack `lookup_host`.
pub async fn connect_origin(host: &str, port: u16, cad: Duration) -> io::Result<TcpStream> {
    Ok(connect_origin_meta(host, port, cad).await?.stream)
}

pub async fn connect_origin_meta(host: &str, port: u16, cad: Duration) -> io::Result<OriginDial> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        let stream = connect_and_nodelay(SocketAddr::new(ip, port)).await?;
        let meta = OriginDialMeta {
            n_v4: Some(u32::from(ip.is_ipv4())),
            n_v6: Some(u32::from(ip.is_ipv6())),
            winner: "literal",
            ..OriginDialMeta::default()
        };
        return Ok(OriginDial { stream, meta });
    }
    let host_v4 = host.to_string();
    let host_v6 = host.to_string();
    let (seed_v4, seed_v6) = cached_origin_addrs(host, port);
    let dial = race_origin_lookups_seeded(
        Box::pin(lookup_family(host_v4, port, OriginFamily::V4)),
        Box::pin(lookup_family(host_v6, port, OriginFamily::V6)),
        cad,
        |addr| Box::pin(connect_and_nodelay(addr)) as OriginConnect,
        seed_v4,
        seed_v6,
    )
    .await?;
    if let Ok(peer) = dial.stream.peer_addr() {
        remember_origin_addr(host, port, peer);
    }
    Ok(dial)
}

type OriginConnect = Pin<Box<dyn Future<Output = io::Result<TcpStream>> + Send>>;
pub type FamilyLookup = Pin<Box<dyn Future<Output = io::Result<Vec<SocketAddr>>> + Send>>;

/// First family that returns addresses starts connecting immediately; the other
/// joins CAD-spaced. `connect` is the per-addr factory (production: nodelay).
pub async fn race_origin_lookups(
    v4: FamilyLookup,
    v6: FamilyLookup,
    cad: Duration,
) -> io::Result<TcpStream> {
    Ok(race_origin_lookups_meta(v4, v6, cad, |addr| {
        Box::pin(connect_and_nodelay(addr)) as OriginConnect
    })
    .await?
    .stream)
}

pub async fn race_origin_lookups_meta(
    v4: FamilyLookup,
    v6: FamilyLookup,
    cad: Duration,
    connect: impl FnMut(SocketAddr) -> OriginConnect,
) -> io::Result<OriginDial> {
    race_origin_lookups_seeded(v4, v6, cad, connect, Vec::new(), Vec::new()).await
}

/// Like `race_origin_lookups_meta`, plus last-good addresses that may start
/// before either lookup completes. First address of a family that just
/// arrived starts immediately; CAD spaces later attempts.
pub async fn race_origin_lookups_seeded(
    v4: FamilyLookup,
    v6: FamilyLookup,
    cad: Duration,
    mut connect: impl FnMut(SocketAddr) -> OriginConnect,
    seed_v4: Vec<SocketAddr>,
    seed_v6: Vec<SocketAddr>,
) -> io::Result<OriginDial> {
    let t0 = Instant::now();
    let mut v4 = v4;
    let mut v6 = v6;
    let mut v4_pending = true;
    let mut v6_pending = true;
    let mut q4: VecDeque<SocketAddr> = seed_v4.into();
    let mut q6: VecDeque<SocketAddr> = seed_v6.into();
    let mut last_err: Option<io::Error> = None;
    let mut last_start: Option<Instant> = None;
    let mut last_family: Option<OriginFamily> = None;
    let mut prefer: Option<OriginFamily> = None;
    let mut started_any = false;
    let mut started: HashSet<SocketAddr> = HashSet::new();
    let mut meta = OriginDialMeta {
        cache_v4: q4.len() as u32,
        cache_v6: q6.len() as u32,
        ..OriginDialMeta::default()
    };
    let mut set: JoinSet<(OriginFamily, io::Result<TcpStream>)> = JoinSet::new();

    let start_one = |set: &mut JoinSet<_>,
                     q4: &mut VecDeque<SocketAddr>,
                     q6: &mut VecDeque<SocketAddr>,
                     prefer: &mut Option<OriginFamily>,
                     last_family: &mut Option<OriginFamily>,
                     last_start: &mut Option<Instant>,
                     started_any: &mut bool,
                     started: &mut HashSet<SocketAddr>,
                     connect: &mut dyn FnMut(SocketAddr) -> OriginConnect|
     -> bool {
        loop {
            let pick = if let Some(f) = *prefer {
                let got = match f {
                    OriginFamily::V4 => q4.pop_front().map(|a| (OriginFamily::V4, a)),
                    OriginFamily::V6 => q6.pop_front().map(|a| (OriginFamily::V6, a)),
                };
                if got.is_none() {
                    *prefer = None;
                } else {
                    let empty = match f {
                        OriginFamily::V4 => q4.is_empty(),
                        OriginFamily::V6 => q6.is_empty(),
                    };
                    if empty {
                        *prefer = None;
                    }
                }
                got
            } else {
                None
            };
            let pick = pick.or_else(|| match *last_family {
                Some(OriginFamily::V4) if !q6.is_empty() => {
                    q6.pop_front().map(|a| (OriginFamily::V6, a))
                }
                Some(OriginFamily::V6) if !q4.is_empty() => {
                    q4.pop_front().map(|a| (OriginFamily::V4, a))
                }
                _ if !q4.is_empty() => q4.pop_front().map(|a| (OriginFamily::V4, a)),
                _ if !q6.is_empty() => q6.pop_front().map(|a| (OriginFamily::V6, a)),
                _ => None,
            });
            let Some((family, addr)) = pick else {
                return false;
            };
            if !started.insert(addr) {
                continue;
            }
            let fut = connect(addr);
            set.spawn(async move { (family, fut.await) });
            *last_start = Some(Instant::now());
            *last_family = Some(family);
            *started_any = true;
            return true;
        }
    };

    loop {
        if set.is_empty()
            && !start_one(
                &mut set,
                &mut q4,
                &mut q6,
                &mut prefer,
                &mut last_family,
                &mut last_start,
                &mut started_any,
                &mut started,
                &mut connect,
            )
            && !v4_pending
            && !v6_pending
        {
            return Err(last_err.unwrap_or_else(|| {
                io::Error::new(io::ErrorKind::AddrNotAvailable, "no addresses")
            }));
        }
        let more = !q4.is_empty() || !q6.is_empty();
        let cad_wait = if more {
            match last_start {
                Some(t) => cad.saturating_sub(t.elapsed()),
                None => Duration::ZERO,
            }
        } else {
            cad
        };
        tokio::select! {
            r = &mut v4, if v4_pending => {
                v4_pending = false;
                meta.lookup_a_us = Some(elapsed_us(t0));
                match r {
                    Ok(addrs) => {
                        meta.n_v4 = Some(addrs.len() as u32);
                        if started_any && last_family != Some(OriginFamily::V4) && !addrs.is_empty() {
                            prefer = Some(OriginFamily::V4);
                        }
                        q4.extend(addrs);
                        // First address of this family, or first address at all:
                        // start now. CAD spaces later attempts of a family
                        // already in flight.
                        if !started_any || prefer == Some(OriginFamily::V4) {
                            let _ = start_one(
                                &mut set, &mut q4, &mut q6, &mut prefer,
                                &mut last_family, &mut last_start, &mut started_any,
                                &mut started, &mut connect,
                            );
                        }
                    }
                    Err(e) => {
                        meta.n_v4 = Some(0);
                        last_err = Some(e);
                    }
                }
            }
            r = &mut v6, if v6_pending => {
                v6_pending = false;
                meta.lookup_aaaa_us = Some(elapsed_us(t0));
                match r {
                    Ok(addrs) => {
                        meta.n_v6 = Some(addrs.len() as u32);
                        if started_any && last_family != Some(OriginFamily::V6) && !addrs.is_empty() {
                            prefer = Some(OriginFamily::V6);
                        }
                        q6.extend(addrs);
                        if !started_any || prefer == Some(OriginFamily::V6) {
                            let _ = start_one(
                                &mut set, &mut q4, &mut q6, &mut prefer,
                                &mut last_family, &mut last_start, &mut started_any,
                                &mut started, &mut connect,
                            );
                        }
                    }
                    Err(e) => {
                        meta.n_v6 = Some(0);
                        last_err = Some(e);
                    }
                }
            }
            Some(joined) = set.join_next(), if !set.is_empty() => {
                match joined {
                    Ok((family, Ok(tcp))) => {
                        set.abort_all();
                        while set.join_next().await.is_some() {}
                        meta.winner = match family {
                            OriginFamily::V4 => "v4",
                            OriginFamily::V6 => "v6",
                        };
                        return Ok(OriginDial { stream: tcp, meta });
                    }
                    Ok((_, Err(e))) => {
                        last_err = Some(e);
                        let _ = start_one(
                            &mut set, &mut q4, &mut q6, &mut prefer,
                            &mut last_family, &mut last_start, &mut started_any,
                            &mut started, &mut connect,
                        );
                    }
                    Err(_) => {}
                }
            }
            _ = tokio::time::sleep(cad_wait), if more => {
                let _ = start_one(
                    &mut set, &mut q4, &mut q6, &mut prefer,
                    &mut last_family, &mut last_start, &mut started_any,
                    &mut started, &mut connect,
                );
            }
        }
    }
}

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
        assert_eq!(clock.rx_bytes(), 3);
        assert_eq!(clock.tx_bytes(), 0);
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
        assert_eq!(clock.rx_bytes(), 0);
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
        assert_eq!(clock.tx_bytes(), 1);
        assert_eq!(clock.rx_bytes(), 0);
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
        let joined = HopSample {
            role: HopRole::Client,
            stream_id: 22346,
            host: "cp.cloudflare.com".into(),
            session_fp: "a1b2c3d4".into(),
            outcome: HopOutcome::CopyErr,
            copy_us: Some(46_630),
            open_us: Some(32),
            first_rx_us: Some(14_688),
            last_rx_us: Some(45_079),
            rx_bytes: Some(1200),
            tx_bytes: Some(517),
            copy_err: Some("ConnectionReset".into()),
            ..Default::default()
        };
        assert_eq!(
            joined.format_tail(),
            "cp.cloudflare.com copy=46630 sfp=a1b2c3d4 open=32 first_rx=14688 last_rx=45079 rx=1200 tx=517 err=ConnectionReset sid=22346"
        );
    }

    #[test]
    fn session_fp_hex_is_first_4_bytes() {
        let mut id = [0u8; 16];
        id[0] = 0x4c;
        id[1] = 0xcd;
        id[2] = 0x39;
        id[3] = 0x13;
        id[4] = 0xff;
        assert_eq!(session_fp_hex(&id), "4ccd3913");
    }

    #[test]
    fn interleave_v6_first_puts_v4_second() {
        let v6a: SocketAddr = "[2001:db8::1]:443".parse().unwrap();
        let v6b: SocketAddr = "[2001:db8::2]:443".parse().unwrap();
        let v4: SocketAddr = "192.0.2.1:443".parse().unwrap();
        assert_eq!(interleave_families(vec![v6a, v6b, v4]), vec![v6a, v4, v6b]);
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
                    Err(io::Error::new(io::ErrorKind::ConnectionRefused, "refused"))
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

    #[tokio::test]
    async fn lookup_aaaa_hang_starts_v4_connect_immediately() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = listener.accept().await;
        });
        let cad = Duration::from_millis(20);
        let t0 = Instant::now();
        let v4: FamilyLookup = Box::pin(async move { Ok(vec![addr]) });
        let v6: FamilyLookup = Box::pin(async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok(Vec::new())
        });
        let tcp = race_origin_lookups(v4, v6, cad).await.unwrap();
        drop(tcp);
        assert!(
            t0.elapsed() < Duration::from_millis(80),
            "hanging AAAA lookup must not block TTFB, elapsed={:?}",
            t0.elapsed()
        );
    }

    #[tokio::test]
    async fn sequential_join_then_race_waits_for_slow_family() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = listener.accept().await;
        });
        let t0 = Instant::now();
        let v4 = async { Ok::<Vec<SocketAddr>, io::Error>(vec![addr]) };
        let v6 = async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok::<Vec<SocketAddr>, io::Error>(Vec::new())
        };
        let (a, b) = tokio::join!(v4, v6);
        let mut addrs = a.unwrap();
        addrs.extend(b.unwrap());
        let tcp = race_origin_addrs(addrs, Duration::from_millis(20))
            .await
            .unwrap();
        drop(tcp);
        assert!(
            t0.elapsed() >= Duration::from_millis(180),
            "sequential join must wait for the slow family, elapsed={:?}",
            t0.elapsed()
        );
    }

    #[tokio::test]
    async fn lookup_empty_v6_does_not_delay_v4() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = listener.accept().await;
        });
        let t0 = Instant::now();
        let v4: FamilyLookup = Box::pin(async move { Ok(vec![addr]) });
        let v6: FamilyLookup = Box::pin(async { Ok(Vec::new()) });
        let tcp = race_origin_lookups(v4, v6, Duration::from_millis(80))
            .await
            .unwrap();
        drop(tcp);
        assert!(
            t0.elapsed() < Duration::from_millis(80),
            "empty AAAA must not delay IPv4"
        );
    }

    #[tokio::test]
    async fn lookup_v4_empty_waits_for_v6() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = listener.accept().await;
        });
        let t0 = Instant::now();
        let v4: FamilyLookup = Box::pin(async { Ok(Vec::new()) });
        let v6: FamilyLookup = Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            Ok(vec![addr])
        });
        let r = race_origin_lookups(v4, v6, Duration::from_millis(20)).await;
        // v6 addr is IPv4 loopback; connect still works. elapsed ~30ms not CAD-only.
        let _ = r; // may fail if we filter v6-only addrs that are ipv4
        let elapsed = t0.elapsed();
        assert!(
            elapsed >= Duration::from_millis(25),
            "must wait for AAAA when A is empty, elapsed={elapsed:?}"
        );
        assert!(elapsed < Duration::from_millis(120));
    }

    #[tokio::test]
    async fn second_family_joins_race_prefers_other_family() {
        use std::sync::Mutex;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ok = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = listener.accept().await;
        });
        let v4a: SocketAddr = "192.0.2.1:443".parse().unwrap();
        let v4b: SocketAddr = "192.0.2.2:443".parse().unwrap();
        let v6a: SocketAddr = "[2001:db8::1]:443".parse().unwrap();
        let order = Arc::new(Mutex::new(Vec::new()));
        let order_c = order.clone();
        let v4: FamilyLookup = Box::pin(async move { Ok(vec![v4a, v4b]) });
        let v6: FamilyLookup = Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            Ok(vec![v6a])
        });
        let tcp = race_origin_lookups_meta(v4, v6, Duration::from_millis(20), move |addr| {
            order_c.lock().unwrap().push(addr);
            if addr.is_ipv6() {
                Box::pin(TcpStream::connect(ok)) as OriginConnect
            } else {
                Box::pin(std::future::pending()) as OriginConnect
            }
        })
        .await
        .unwrap();
        drop(tcp.stream);
        let started = order.lock().unwrap().clone();
        assert!(started.len() >= 2, "started={started:?}");
        assert!(started[0].is_ipv4(), "first start is v4");
        assert!(
            started.iter().any(|a| a.is_ipv6()),
            "v6 must join before remaining v4, started={started:?}"
        );
        let v6_at = started.iter().position(|a| a.is_ipv6()).unwrap();
        assert_eq!(
            v6_at, 1,
            "next start after first v4 is v6, started={started:?}"
        );
    }

    #[tokio::test]
    async fn lookup_hard_fail_one_family_uses_the_other() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = listener.accept().await;
        });
        let v4: FamilyLookup = Box::pin(async move { Ok(vec![addr]) });
        let v6: FamilyLookup = Box::pin(async { Err(io::Error::other("servfail")) });
        let tcp = race_origin_lookups(v4, v6, Duration::from_millis(20))
            .await
            .unwrap();
        drop(tcp);
    }

    #[test]
    fn origin_lookup_name_is_fqdn() {
        assert_eq!(origin_lookup_name("www.youtube.com"), "www.youtube.com.");
        assert_eq!(origin_lookup_name("www.youtube.com."), "www.youtube.com.");
    }

    #[test]
    fn origin_addr_cache_roundtrip() {
        clear_origin_addr_cache();
        let addr: SocketAddr = "192.0.2.8:443".parse().unwrap();
        remember_origin_addr("Www.Example.Test", 443, addr);
        let (v4, v6) = cached_origin_addrs("www.example.test", 443);
        assert_eq!(v4, vec![addr]);
        assert!(v6.is_empty());
        clear_origin_addr_cache();
    }

    #[tokio::test]
    async fn cached_v4_used_while_a_lookup_hangs() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = listener.accept().await;
        });
        let cad = Duration::from_millis(20);
        let t0 = Instant::now();
        let v4: FamilyLookup = Box::pin(async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok(Vec::new())
        });
        let v6: FamilyLookup = Box::pin(async { Ok(vec!["[2001:db8::1]:443".parse().unwrap()]) });
        let tcp = race_origin_lookups_seeded(
            v4,
            v6,
            cad,
            |a| {
                if a.is_ipv6() {
                    Box::pin(std::future::pending()) as OriginConnect
                } else {
                    Box::pin(TcpStream::connect(a)) as OriginConnect
                }
            },
            vec![addr],
            Vec::new(),
        )
        .await
        .unwrap();
        drop(tcp.stream);
        assert!(
            t0.elapsed() < Duration::from_millis(80),
            "cached v4 must not wait for hanging A or blackholed v6, elapsed={:?}",
            t0.elapsed()
        );
    }

    #[tokio::test]
    async fn second_family_starts_without_remaining_cad() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ok = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = listener.accept().await;
        });
        let v4a: SocketAddr = "192.0.2.1:443".parse().unwrap();
        let v6a: SocketAddr = "[2001:db8::1]:443".parse().unwrap();
        let cad = Duration::from_millis(80);
        let t0 = Instant::now();
        let v4: FamilyLookup = Box::pin(async move { Ok(vec![v4a]) });
        let v6: FamilyLookup = Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            Ok(vec![v6a])
        });
        let tcp = race_origin_lookups_meta(v4, v6, cad, move |addr| {
            if addr.is_ipv6() {
                Box::pin(TcpStream::connect(ok)) as OriginConnect
            } else {
                Box::pin(std::future::pending()) as OriginConnect
            }
        })
        .await
        .unwrap();
        drop(tcp.stream);
        assert!(
            t0.elapsed() < Duration::from_millis(40),
            "first address of the newly arrived family must not wait remaining CAD, elapsed={:?}",
            t0.elapsed()
        );
    }

    #[tokio::test]
    async fn seed_does_not_double_connect_same_addr() {
        use std::sync::atomic::AtomicUsize;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let _ = listener.accept().await;
            }
        });
        let n = Arc::new(AtomicUsize::new(0));
        let n2 = n.clone();
        let v4: FamilyLookup = Box::pin(async move { Ok(vec![addr]) });
        let v6: FamilyLookup = Box::pin(async { Ok(Vec::new()) });
        let tcp = race_origin_lookups_seeded(
            v4,
            v6,
            Duration::from_millis(20),
            move |a| {
                n2.fetch_add(1, Ordering::SeqCst);
                Box::pin(TcpStream::connect(a)) as OriginConnect
            },
            vec![addr],
            Vec::new(),
        )
        .await
        .unwrap();
        drop(tcp.stream);
        assert_eq!(
            n.load(Ordering::SeqCst),
            1,
            "same addr from seed and lookup must connect once"
        );
    }
}
