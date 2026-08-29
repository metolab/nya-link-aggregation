//! Server: TLS listener, PSK handshake, multiplexed outbound dials.
#![forbid(unsafe_code)]

mod config;
mod outbound;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;
use tracing::{info, warn};

use nya_core::{
    export_from_server, load_server_config, server_accept_handshake, spawn_obs_table, spki_sha256,
    HandshakeError, HandshakeResult, SessionTable,
};
use nya_proto::ProtoError;

pub use config::ServerConfig;
use outbound::handle_incoming;

pub fn hex_encode(b: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        s.push(HEX[(x >> 4) as usize] as char);
        s.push(HEX[(x & 0xf) as usize] as char);
    }
    s
}

pub fn gen_cert(out: &Path, name: &str) -> Result<[u8; 32]> {
    std::fs::create_dir_all(out)?;
    let ck = rcgen::generate_simple_self_signed(vec![name.to_string(), "localhost".into()])?;
    std::fs::write(out.join("server.crt"), ck.cert.pem())?;
    std::fs::write(out.join("server.key"), ck.key_pair.serialize_pem())?;
    let pin = spki_sha256(ck.cert.der().as_ref()).map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(pin)
}

pub async fn run(cfg: ServerConfig) -> Result<()> {
    let listener = TcpListener::bind(&cfg.listen)
        .await
        .with_context(|| format!("bind {}", cfg.listen))?;
    run_on(listener, cfg).await
}

/// Serve on an already-bound listener so the caller can publish the port
/// without a bind/drop/rebind race.
pub async fn run_on(listener: TcpListener, cfg: ServerConfig) -> Result<()> {
    let (_tx, rx) = watch::channel(false);
    run_on_until(listener, cfg, rx).await
}

pub fn new_session_table(cfg: &ServerConfig) -> Arc<SessionTable> {
    Arc::new(SessionTable::new(cfg.session_config()))
}

/// Like [`run_on`], but return after `stop` becomes true (shuts every session).
pub async fn run_on_until(
    listener: TcpListener,
    cfg: ServerConfig,
    stop: watch::Receiver<bool>,
) -> Result<()> {
    let table = new_session_table(&cfg);
    run_on_table(listener, cfg, stop, table).await
}

/// Serve on `listener` using an already-built session table.
pub async fn run_on_table(
    listener: TcpListener,
    cfg: ServerConfig,
    mut stop: watch::Receiver<bool>,
    table: Arc<SessionTable>,
) -> Result<()> {
    let tls = load_server_config(&cfg.cert, &cfg.key).map_err(|e| anyhow::anyhow!("{e}"))?;
    let acceptor = TlsAcceptor::from(Arc::new(tls));
    spawn_obs_table(table.clone(), cfg.obs.clone(), stop.clone());
    let psk = Arc::new(cfg.psk.clone().into_bytes());
    info!("listening on {}", listener.local_addr()?);

    loop {
        tokio::select! {
            _ = stop.wait_for(|v| *v) => {
                table.shutdown_all();
                return Ok(());
            }
            acc = listener.accept() => {
                if table.is_closed() {
                    return Ok(());
                }
                let (tcp, peer) = acc?;
                let acceptor = acceptor.clone();
                let table = table.clone();
                let psk = psk.clone();
                tokio::spawn(async move {
                    if let Err(e) = serve_one(acceptor, tcp, peer, table, psk).await {
                        warn!(%peer, error = %e, "connection closed");
                    }
                });
            }
        }
    }
}

// Same 60 s cap as nya-obs ExportErrorPulse, plus class-change. Process-wide;
// never construct inside serve_one (per-TCP spawn would restore scanner floods).
static TLS_ACCEPT_PULSE: OnceLock<Mutex<TlsPulseState>> = OnceLock::new();

#[derive(Default)]
struct TlsPulseState {
    last_emit: Option<Instant>,
    last_error: String,
    #[allow(dead_code)]
    last_peer: String,
    suppressed: u64,
}

fn tls_pulse_should_emit(
    st: &mut TlsPulseState,
    now: Instant,
    every: Duration,
    error: &str,
) -> Option<u64> {
    match st.last_emit {
        None => {
            st.last_emit = Some(now);
            st.last_error = error.to_string();
            st.suppressed = 0;
            Some(0)
        }
        Some(_) if error != st.last_error => {
            let n = st.suppressed;
            st.last_emit = Some(now);
            st.last_error = error.to_string();
            st.suppressed = 0;
            Some(n)
        }
        Some(prev) if now.duration_since(prev) >= every => {
            let n = st.suppressed;
            st.last_emit = Some(now);
            st.last_error = error.to_string();
            st.suppressed = 0;
            Some(n)
        }
        Some(_) => {
            st.suppressed += 1;
            None
        }
    }
}

fn tls_accept_warn(peer: std::net::SocketAddr, err: &impl std::fmt::Display) {
    let slot = TLS_ACCEPT_PULSE.get_or_init(|| Mutex::new(TlsPulseState::default()));
    let msg = err.to_string();
    let n = {
        let mut g = slot.lock().unwrap_or_else(|e| e.into_inner());
        let out = tls_pulse_should_emit(&mut g, Instant::now(), Duration::from_secs(60), &msg);
        if out.is_some() {
            g.last_peer = peer.to_string();
        }
        out
    };
    if let Some(n) = n {
        tracing::warn!(%peer, error = %msg, suppressed = n, "tls accept");
    }
}

fn handshake_is_noise(e: &HandshakeError) -> bool {
    match e {
        HandshakeError::Unexpected => true,
        HandshakeError::Proto(
            ProtoError::BadLength(_)
            | ProtoError::UnknownType(_)
            | ProtoError::Truncated
            | ProtoError::Invalid(_),
        ) => true,
        HandshakeError::Proto(ProtoError::Io(_) | ProtoError::Version(_)) => false,
        HandshakeError::Rejected(_) | HandshakeError::UnknownSession => false,
    }
}

async fn serve_one(
    acceptor: TlsAcceptor,
    tcp: tokio::net::TcpStream,
    peer: std::net::SocketAddr,
    table: Arc<SessionTable>,
    psk: Arc<Vec<u8>>,
) -> Result<()> {
    tcp.set_nodelay(true)?;
    if table.is_closed() {
        return Ok(());
    }
    let t0 = Instant::now();
    let mut tls = match acceptor.accept(tcp).await {
        Ok(t) => t,
        Err(e) => {
            tls_accept_warn(peer, &e);
            return Ok(());
        }
    };
    if table.is_closed() {
        return Ok(());
    }
    {
        let _s = tracing::info_span!(
            target: "nya_otel",
            "nya.link.accept",
            otel.kind = "server",
            peer = %peer,
            tls_ms = t0.elapsed().as_millis() as u64,
        )
        .entered();
    }
    let exporter = export_from_server(&tls).map_err(|e| anyhow::anyhow!("exporter: {e}"))?;
    let hs_t0 = Instant::now();
    let result = server_accept_handshake(&mut tls, &psk, &exporter, &table).await;
    match result {
        Ok(HandshakeResult::Created {
            session,
            incoming,
            path_name,
            session_id,
        }) => {
            {
                let _g = tracing::info_span!(
                    target: "nya_otel",
                    "nya.handshake",
                    otel.kind = "server",
                    peer = %peer,
                    nya.kind = "create",
                    hs_ms = hs_t0.elapsed().as_millis() as u64,
                )
                .entered();
                session
                    .process()
                    .handshake_create_ok
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                info!(%peer, session = %hex_encode(&session_id), "session created");
            }
            if table.is_closed() {
                session.shutdown();
                return Ok(());
            }
            tokio::spawn(handle_incoming(incoming));
            {
                let _up = tracing::info_span!(
                    target: "nya_otel",
                    "nya.path.up",
                    nya.path_name = %path_name,
                )
                .entered();
            }
            session.add_path(path_name, tls).await;
        }
        Ok(HandshakeResult::Joined { session, path_name }) => {
            {
                let _g = tracing::info_span!(
                    target: "nya_otel",
                    "nya.handshake",
                    otel.kind = "server",
                    peer = %peer,
                    nya.kind = "join",
                    hs_ms = hs_t0.elapsed().as_millis() as u64,
                )
                .entered();
                session
                    .process()
                    .handshake_join_ok
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                info!(%peer, path = %path_name, "path joined");
            }
            if table.is_closed() {
                return Ok(());
            }
            {
                let _up = tracing::info_span!(
                    target: "nya_otel",
                    "nya.path.up",
                    nya.path_name = %path_name,
                )
                .entered();
            }
            session.add_path(path_name, tls).await;
        }
        Err(e) if handshake_is_noise(&e) => {
            table.process().inc_handshake_fail(&e);
            tracing::debug!(%peer, error = %e, "handshake discarded");
        }
        Err(e) => {
            table.process().inc_handshake_fail(&e);
            let _g = tracing::info_span!(
                target: "nya_otel",
                "nya.handshake",
                otel.kind = "server",
                peer = %peer,
                otel.status_code = "ERROR",
                hs_ms = hs_t0.elapsed().as_millis() as u64,
            )
            .entered();
            tracing::warn!(%peer, error = %e, "handshake failed");
        }
    }
    Ok(())
}

pub fn cert_paths(dir: &Path) -> (PathBuf, PathBuf) {
    (dir.join("server.crt"), dir.join("server.key"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handshake_noise_is_codec_not_io() {
        assert!(handshake_is_noise(&HandshakeError::Unexpected));
        assert!(handshake_is_noise(&HandshakeError::Proto(
            ProtoError::BadLength(1195725856)
        )));
        assert!(handshake_is_noise(&HandshakeError::Proto(
            ProtoError::UnknownType(0x47)
        )));
        assert!(handshake_is_noise(&HandshakeError::Proto(
            ProtoError::Truncated
        )));
        assert!(handshake_is_noise(&HandshakeError::Proto(
            ProtoError::Invalid("x")
        )));
        assert!(!handshake_is_noise(&HandshakeError::Proto(ProtoError::Io(
            std::io::Error::new(std::io::ErrorKind::ConnectionReset, "rst")
        ))));
        assert!(!handshake_is_noise(&HandshakeError::Proto(
            ProtoError::Version(9)
        )));
        assert!(!handshake_is_noise(&HandshakeError::Rejected(
            "auth".into()
        )));
        assert!(!handshake_is_noise(&HandshakeError::UnknownSession));
    }

    #[test]
    fn tls_pulse_shared_state_and_class_change() {
        let mut st = TlsPulseState::default();
        let every = Duration::from_secs(60);
        let t0 = Instant::now();
        assert_eq!(tls_pulse_should_emit(&mut st, t0, every, "eof"), Some(0));
        assert_eq!(
            tls_pulse_should_emit(&mut st, t0 + Duration::from_secs(10), every, "eof"),
            None
        );
        assert_eq!(st.suppressed, 1);
        assert_eq!(
            tls_pulse_should_emit(&mut st, t0 + Duration::from_secs(20), every, "expired cert"),
            Some(1)
        );
        assert_eq!(st.last_error, "expired cert");
        assert_eq!(
            tls_pulse_should_emit(&mut st, t0 + Duration::from_secs(30), every, "expired cert"),
            None
        );
        assert_eq!(
            tls_pulse_should_emit(
                &mut st,
                t0 + Duration::from_secs(20) + Duration::from_secs(61),
                every,
                "expired cert"
            ),
            Some(1)
        );
    }
}
