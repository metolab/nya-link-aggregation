//! Overlay path socket tuning and kernel TCP introspection.
//!
//! P1 of `design-path-budget-ack-clock`: bound the kernel send queue on every
//! overlay 5-tuple (`TCP_NOTSENT_LOWAT`) and ask for a loss-tolerant
//! congestion controller (`TCP_CONGESTION=bbr`, Linux, best-effort). P6:
//! read `TCP_INFO` from a duplicated fd so the exporter can say whether the
//! kernel, not the overlay, is the limiter.
//!
//! Only overlay path sockets go through here. Origin / SOCKS sockets are
//! untouched. `socket2` covers the safe setsockopts; `TCP_INFO` has no safe
//! wrapper in the tree and is the one `unsafe` block in this crate.

use std::sync::atomic::{AtomicBool, Ordering};

use tokio::net::TcpStream;

use crate::tuning::Tuning;

/// What the kernel accepted for one path socket.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SocketTuning {
    /// `TCP_NOTSENT_LOWAT` in bytes, if set.
    pub notsent_lowat: Option<u32>,
    /// Congestion controller in effect after our request, if readable.
    pub congestion: Option<String>,
}

/// Congestion controller requested for overlay TCPs on Linux. A 1–2 % loss
/// IX collapses CUBIC (throughput ∝ 1/√p); BBR paces on delivery rate.
/// Best-effort: missing module ⇒ kernel default stays and is reported.
pub const OVERLAY_TCP_CC: &str = "bbr";

/// `TCP_NOTSENT_LOWAT` = `inflight_bias`. Not-sent bytes are the part of the
/// kernel queue beyond cwnd; 64 KiB is four frames of writer lead, enough at
/// 1 Gbps × 10 ms (the BDP lives in cwnd, not in unsent) and small enough
/// that an urgent frame behind bulk on a 1 MB/s path waits ≤ 64 ms.
pub fn notsent_lowat_bytes() -> u32 {
    Tuning::STANDARD.inflight_bias.min(u32::MAX as u64) as u32
}

static TUNE_LOGGED: AtomicBool = AtomicBool::new(false);

/// Apply P1 to an overlay path socket. Idempotent, never fails the dial.
pub fn tune_path_socket(tcp: &TcpStream) -> SocketTuning {
    let _ = tcp.set_nodelay(true);
    let t = imp::tune(tcp);
    if !TUNE_LOGGED.swap(true, Ordering::Relaxed) {
        tracing::info!(
            notsent_lowat = ?t.notsent_lowat,
            congestion = ?t.congestion,
            "overlay tcp socket tuning"
        );
    }
    t
}

/// Kernel view of one overlay TCP (Linux `TCP_INFO`). Byte fields are
/// derived with `snd_mss`; fields the running kernel does not return are 0.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TcpInfo {
    pub snd_mss: u32,
    pub cwnd_bytes: u64,
    pub rtt_us: u32,
    pub rttvar_us: u32,
    pub unacked_bytes: u64,
    pub lost_segs: u32,
    pub retrans_segs: u32,
    pub total_retrans: u32,
    pub notsent_bytes: u32,
    pub min_rtt_us: u32,
    pub delivery_rate_bytes_s: u64,
    pub pacing_rate_bytes_s: u64,
}

/// Owned duplicate of a socket fd for `TCP_INFO` reads that outlive the
/// tokio `TcpStream` split halves. Dropping it closes only the duplicate.
pub struct PathFd(#[allow(dead_code)] imp::Owned);

impl PathFd {
    /// Duplicate the stream's fd. `None` when unsupported or on error.
    pub fn dup_from(tcp: &TcpStream) -> Option<PathFd> {
        imp::dup(tcp).map(PathFd)
    }

    /// Kernel TCP state now. `None` off Linux or on error (e.g. closed).
    pub fn tcp_info(&self) -> Option<TcpInfo> {
        imp::tcp_info(&self.0)
    }
}

impl std::fmt::Debug for PathFd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PathFd")
    }
}

/// Parse a raw Linux `struct tcp_info` buffer by kernel-documented offsets
/// (`include/uapi/linux/tcp.h`). Tolerates short buffers (older kernels);
/// missing fields read as 0.
pub fn parse_tcp_info(buf: &[u8]) -> TcpInfo {
    fn u32_at(b: &[u8], off: usize) -> u32 {
        if b.len() < off + 4 {
            return 0;
        }
        u32::from_ne_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]])
    }
    fn u64_at(b: &[u8], off: usize) -> u64 {
        if b.len() < off + 8 {
            return 0;
        }
        let mut a = [0u8; 8];
        a.copy_from_slice(&b[off..off + 8]);
        u64::from_ne_bytes(a)
    }
    let snd_mss = u32_at(buf, 16);
    let mss = snd_mss.max(1) as u64;
    TcpInfo {
        snd_mss,
        unacked_bytes: u32_at(buf, 24) as u64 * mss,
        lost_segs: u32_at(buf, 32),
        retrans_segs: u32_at(buf, 36),
        rtt_us: u32_at(buf, 68),
        rttvar_us: u32_at(buf, 72),
        cwnd_bytes: u32_at(buf, 80) as u64 * mss,
        total_retrans: u32_at(buf, 100),
        pacing_rate_bytes_s: u64_at(buf, 104),
        notsent_bytes: u32_at(buf, 144),
        min_rtt_us: u32_at(buf, 148),
        delivery_rate_bytes_s: u64_at(buf, 160),
    }
}

#[cfg(unix)]
mod imp {
    use super::{SocketTuning, TcpInfo};
    use std::os::fd::{AsFd, OwnedFd};
    use tokio::net::TcpStream;

    pub type Owned = OwnedFd;

    pub fn dup(tcp: &TcpStream) -> Option<OwnedFd> {
        tcp.as_fd().try_clone_to_owned().ok()
    }

    pub fn tune(tcp: &TcpStream) -> SocketTuning {
        let sock = socket2::SockRef::from(tcp);
        let lowat = super::notsent_lowat_bytes();
        SocketTuning {
            notsent_lowat: set_lowat(&sock, lowat).then_some(lowat),
            congestion: congestion(&sock),
        }
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn set_lowat(sock: &socket2::SockRef<'_>, lowat: u32) -> bool {
        sock.set_tcp_notsent_lowat(lowat).is_ok()
    }

    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    fn set_lowat(_sock: &socket2::SockRef<'_>, _lowat: u32) -> bool {
        false
    }

    #[cfg(target_os = "linux")]
    fn congestion(sock: &socket2::SockRef<'_>) -> Option<String> {
        let _ = sock.set_tcp_congestion(super::OVERLAY_TCP_CC.as_bytes());
        let name = sock.tcp_congestion().ok()?;
        let n = name.iter().position(|b| *b == 0).unwrap_or(name.len());
        Some(String::from_utf8_lossy(&name[..n]).into_owned())
    }

    #[cfg(not(target_os = "linux"))]
    fn congestion(_sock: &socket2::SockRef<'_>) -> Option<String> {
        None
    }

    /// The single `unsafe` in `nya-core`: `getsockopt(TCP_INFO)` into a
    /// byte buffer. No safe wrapper exists in the dependency tree.
    #[cfg(target_os = "linux")]
    #[allow(unsafe_code)]
    pub fn tcp_info(fd: &OwnedFd) -> Option<TcpInfo> {
        use std::os::fd::AsRawFd;
        let mut buf = [0u8; 256];
        let mut len = buf.len() as libc::socklen_t;
        // SAFETY: `fd` is an open descriptor we own; `buf` is a valid,
        // writable region of `len` bytes and `len` is updated by the kernel
        // to the number of bytes written, which we bound by `buf.len()`.
        let r = unsafe {
            libc::getsockopt(
                fd.as_raw_fd(),
                libc::IPPROTO_TCP,
                libc::TCP_INFO,
                buf.as_mut_ptr() as *mut libc::c_void,
                &mut len,
            )
        };
        if r != 0 {
            return None;
        }
        let n = (len as usize).min(buf.len());
        Some(super::parse_tcp_info(&buf[..n]))
    }

    #[cfg(not(target_os = "linux"))]
    pub fn tcp_info(_fd: &OwnedFd) -> Option<TcpInfo> {
        None
    }
}

#[cfg(not(unix))]
mod imp {
    use super::{SocketTuning, TcpInfo};
    use tokio::net::TcpStream;

    pub struct Owned;

    pub fn dup(_tcp: &TcpStream) -> Option<Owned> {
        None
    }

    pub fn tune(tcp: &TcpStream) -> SocketTuning {
        let _ = tcp;
        SocketTuning::default()
    }

    pub fn tcp_info(_fd: &Owned) -> Option<TcpInfo> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_short_buffer_is_zero() {
        let t = parse_tcp_info(&[0u8; 20]);
        assert_eq!(t.snd_mss, 0);
        assert_eq!(t.cwnd_bytes, 0);
        assert_eq!(t.delivery_rate_bytes_s, 0);
    }

    #[test]
    fn parse_known_offsets() {
        let mut b = vec![0u8; 232];
        b[16..20].copy_from_slice(&1448u32.to_ne_bytes());
        b[24..28].copy_from_slice(&10u32.to_ne_bytes());
        b[68..72].copy_from_slice(&10_500u32.to_ne_bytes());
        b[80..84].copy_from_slice(&40u32.to_ne_bytes());
        b[100..104].copy_from_slice(&7u32.to_ne_bytes());
        b[144..148].copy_from_slice(&65_536u32.to_ne_bytes());
        b[160..168].copy_from_slice(&6_250_000u64.to_ne_bytes());
        let t = parse_tcp_info(&b);
        assert_eq!(t.snd_mss, 1448);
        assert_eq!(t.unacked_bytes, 14_480);
        assert_eq!(t.rtt_us, 10_500);
        assert_eq!(t.cwnd_bytes, 57_920);
        assert_eq!(t.total_retrans, 7);
        assert_eq!(t.notsent_bytes, 65_536);
        assert_eq!(t.delivery_rate_bytes_s, 6_250_000);
    }

    #[test]
    fn lowat_is_inflight_bias() {
        assert_eq!(notsent_lowat_bytes() as u64, Tuning::STANDARD.inflight_bias);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn tune_and_tcp_info_on_loopback() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let c = TcpStream::connect(addr).await.unwrap();
        let (_s, _) = l.accept().await.unwrap();
        let t = tune_path_socket(&c);
        assert_eq!(t.notsent_lowat, Some(notsent_lowat_bytes()));
        // CC name is whatever the kernel granted; must be readable.
        assert!(t.congestion.as_deref().is_some_and(|s| !s.is_empty()));
        let fd = PathFd::dup_from(&c).unwrap();
        let info = fd.tcp_info().unwrap();
        assert!(info.snd_mss > 0);
        assert!(info.cwnd_bytes > 0);
        drop(c);
        // The dup keeps the socket object alive; TCP_INFO still answers.
        assert!(fd.tcp_info().is_some());
    }
}
