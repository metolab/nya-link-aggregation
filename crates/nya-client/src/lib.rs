//! Client: pin the server SPKI, open N TCP+TLS paths, join one session,
//! expose SOCKS5 / TCP-forward inbounds.
#![forbid(unsafe_code)]

mod config;
mod inbound;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::Notify;
use tracing::{info, warn, Instrument};

use nya_core::{
    client_create_session, client_join_session, connect_pinned, export_from_client, parse_pin_hex,
    spawn_obs_session, Session,
};
use nya_proto::SESSION_ID_LEN;

pub use config::{ClientConfig, Inbound, Link};
pub use inbound::{serve_forward_listener, serve_inbounds, serve_socks5_listener};

struct SessionJoin {
    id: std::sync::Mutex<Option<[u8; SESSION_ID_LEN]>>,
    creating: AtomicBool,
    ready: Notify,
}

impl SessionJoin {
    fn new() -> Self {
        Self {
            id: std::sync::Mutex::new(None),
            creating: AtomicBool::new(false),
            ready: Notify::new(),
        }
    }

    fn get_id(&self) -> Option<[u8; SESSION_ID_LEN]> {
        *self.id.lock().unwrap()
    }

    fn set_id(&self, sid: [u8; SESSION_ID_LEN]) {
        *self.id.lock().unwrap() = Some(sid);
    }
}

/// Alias of [`start`].
pub async fn run(cfg: ClientConfig) -> Result<Session> {
    start(cfg).await
}

pub fn spawn_links(cfg: &ClientConfig, session: Session, pin: [u8; 32]) {
    let join = Arc::new(SessionJoin::new());
    let psk = Arc::new(cfg.psk.clone().into_bytes());
    for link in cfg.links.clone() {
        let n = link.connections.max(1);
        for i in 0..n {
            let session = session.clone();
            let join = join.clone();
            let psk = psk.clone();
            let link = link.clone();
            let path_name = format!("{}#{}", link.name, i);
            tokio::spawn(async move {
                run_link(link, path_name, session, pin, join, psk).await;
            });
        }
    }
}

pub async fn run_with_inbounds(cfg: ClientConfig) -> Result<()> {
    let session = start(cfg.clone()).await?;
    serve_inbounds(cfg.inbounds, session).await
}

/// Spawn link supervisors and return the session. Caller starts inbounds.
pub async fn start(cfg: ClientConfig) -> Result<Session> {
    let pin = parse_pin_hex(&cfg.pinned_spki_sha256).map_err(|e| anyhow::anyhow!("{e}"))?;
    let session = Session::new_client(cfg.session_config());
    spawn_obs_session(session.clone(), cfg.obs.clone());
    spawn_links(&cfg, session.clone(), pin);
    Ok(session)
}

async fn run_link(
    link: Link,
    path_name: String,
    session: Session,
    pin: [u8; 32],
    join: Arc<SessionJoin>,
    psk: Arc<Vec<u8>>,
) {
    let backoff_min = session.config().tuning.reconnect_backoff_min;
    let backoff_max = session.config().tuning.reconnect_backoff_max;
    let mut backoff = backoff_min;
    loop {
        if session.is_dead() {
            return;
        }
        let hs = match session.last_known_rtt(&path_name) {
            None => session.config().tuning.handshake_timeout,
            Some(rtt) => (rtt * 20).clamp(Duration::from_millis(400), Duration::from_secs(3)),
        };
        tokio::select! {
            _ = session.wait_dead() => return,
            r = connect_one(&link, &path_name, &session, pin, &join, &psk, hs) => {
                match r {
                    Ok(()) => backoff = backoff_min,
                    Err(e) => {
                        session.process().reconnect_fail.fetch_add(1, Ordering::Relaxed);
                        warn!(path = %path_name, error = %e, "link failed");
                    }
                }
            }
        }
        if session.is_dead() {
            return;
        }
        tokio::select! {
            _ = session.wait_dead() => return,
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(backoff_max);
    }
}

async fn connect_one(
    link: &Link,
    path_name: &str,
    session: &Session,
    pin: [u8; 32],
    join: &SessionJoin,
    psk: &[u8],
    hs: Duration,
) -> Result<()> {
    info!(path = %path_name, addr = %link.addr, "dialing");
    let mut tls = tokio::time::timeout(
        hs,
        connect_pinned(&link.addr, pin).instrument(tracing::info_span!(
            target: "nya_otel",
            "nya.link.dial",
            otel.kind = "client",
            nya.path_name = %path_name,
            server.address = %link.addr,
        )),
    )
    .await
    .map_err(|_| anyhow::anyhow!("tls connect timeout"))?
    .map_err(|e| anyhow::anyhow!("tls connect: {e}"))?;
    let exporter = export_from_client(&tls).map_err(|e| anyhow::anyhow!("exporter: {e}"))?;

    enum Role {
        Create,
        Join([u8; SESSION_ID_LEN]),
    }

    let role = if let Some(sid) = join.get_id() {
        Role::Join(sid)
    } else if join
        .creating
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
    {
        if let Some(sid) = join.get_id() {
            Role::Join(sid)
        } else {
            Role::Create
        }
    } else {
        loop {
            if let Some(sid) = join.get_id() {
                break Role::Join(sid);
            }
            if !join.creating.load(Ordering::SeqCst) {
                return Err(anyhow::anyhow!("session create failed"));
            }
            tokio::select! {
                _ = join.ready.notified() => {}
                _ = tokio::time::sleep(session.config().tuning.join_poll) => {}
            }
        }
    };

    match role {
        Role::Create => {
            let span = tracing::info_span!(
                target: "nya_otel",
                "nya.handshake",
                otel.kind = "client",
                nya.kind = "create",
                nya.path_name = %path_name,
                otel.status_code = tracing::field::Empty,
            );
            match tokio::time::timeout(
                hs,
                client_create_session(&mut tls, psk, &exporter, "default").instrument(span.clone()),
            )
            .await
            {
                Ok(Ok(sid)) => {
                    session
                        .process()
                        .handshake_create_ok
                        .fetch_add(1, Ordering::Relaxed);
                    join.set_id(sid);
                    join.ready.notify_waiters();
                    info!(path = %path_name, "session created");
                }
                Ok(Err(e)) => {
                    span.record("otel.status_code", "ERROR");
                    session.process().inc_handshake_fail(&e);
                    join.creating.store(false, Ordering::SeqCst);
                    join.ready.notify_waiters();
                    return Err(e.into());
                }
                Err(_) => {
                    span.record("otel.status_code", "ERROR");
                    join.creating.store(false, Ordering::SeqCst);
                    join.ready.notify_waiters();
                    return Err(anyhow::anyhow!("create-session timeout"));
                }
            }
        }
        Role::Join(sid) => {
            let span = tracing::info_span!(
                target: "nya_otel",
                "nya.handshake",
                otel.kind = "client",
                nya.kind = "join",
                nya.path_name = %path_name,
                otel.status_code = tracing::field::Empty,
            );
            match tokio::time::timeout(
                hs,
                client_join_session(&mut tls, psk, &exporter, sid, path_name)
                    .instrument(span.clone()),
            )
            .await
            {
                Ok(Ok(())) => {
                    session
                        .process()
                        .handshake_join_ok
                        .fetch_add(1, Ordering::Relaxed);
                }
                Ok(Err(e)) => {
                    span.record("otel.status_code", "ERROR");
                    session.process().inc_handshake_fail(&e);
                    return Err(anyhow::anyhow!("join: {e}"));
                }
                Err(_) => {
                    span.record("otel.status_code", "ERROR");
                    return Err(anyhow::anyhow!("join timeout"));
                }
            }
        }
    }

    info!(path = %path_name, addr = %link.addr, "path up");
    {
        let _up = tracing::info_span!(
            target: "nya_otel",
            "nya.path.up",
            nya.path_name = %path_name,
        )
        .entered();
    }
    session
        .process()
        .reconnect_ok
        .fetch_add(1, Ordering::Relaxed);
    session.add_path(path_name.to_string(), tls).await;
    Ok(())
}
