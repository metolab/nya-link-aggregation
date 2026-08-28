//! Server: TLS listener, PSK handshake, multiplexed outbound dials.
#![forbid(unsafe_code)]

mod config;
mod outbound;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;
use tracing::{error, info, warn};

use nya_core::{
    export_from_server, load_server_config, server_accept_handshake, spawn_obs_table, spki_sha256,
    HandshakeResult, SessionTable,
};

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

/// Like [`run_on`], but return after `stop` becomes true (shuts every session).
pub async fn run_on_until(
    listener: TcpListener,
    cfg: ServerConfig,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    let tls = load_server_config(&cfg.cert, &cfg.key).map_err(|e| anyhow::anyhow!("{e}"))?;
    let acceptor = TlsAcceptor::from(Arc::new(tls));
    let table = Arc::new(SessionTable::new(cfg.session_config()));
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
    let mut tls = acceptor.accept(tcp).await.context("tls accept")?;
    if table.is_closed() {
        return Ok(());
    }
    let exporter = export_from_server(&tls).map_err(|e| anyhow::anyhow!("exporter: {e}"))?;
    match server_accept_handshake(&mut tls, &psk, &exporter, &table).await {
        Ok(HandshakeResult::Created {
            session,
            incoming,
            path_name,
            session_id,
        }) => {
            session
                .process()
                .handshake_create_ok
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            info!(%peer, session = %hex_encode(&session_id), "session created");
            if table.is_closed() {
                session.shutdown();
                return Ok(());
            }
            tokio::spawn(handle_incoming(incoming));
            session.add_path(path_name, tls).await;
        }
        Ok(HandshakeResult::Joined { session, path_name }) => {
            session
                .process()
                .handshake_join_ok
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            info!(%peer, path = %path_name, "path joined");
            if table.is_closed() {
                return Ok(());
            }
            session.add_path(path_name, tls).await;
        }
        Err(e) => {
            table.process().inc_handshake_fail(&e);
            error!(%peer, error = %e, "handshake failed");
        }
    }
    Ok(())
}

pub fn cert_paths(dir: &Path) -> (PathBuf, PathBuf) {
    (dir.join("server.crt"), dir.join("server.key"))
}
