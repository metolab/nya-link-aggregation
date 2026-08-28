use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tracing::info;

use nya_client::{serve_forward_listener, serve_socks5_listener, spawn_links, ClientConfig, Link};
use nya_core::{install_crypto, parse_pin_hex, Session, SessionConfig};
use nya_server::{cert_paths, gen_cert, run_on_until, ServerConfig};

use crate::impair::{spawn_link, ImpairConfig, LinkHandle};

pub struct Harness {
    pub echo: SocketAddr,
    pub server: SocketAddr,
    pub forward: SocketAddr,
    pub socks: SocketAddr,
    pub links: Vec<LinkHandle>,
    pub session: Session,
    server_stop: watch::Sender<bool>,
    echo_abort: tokio::task::AbortHandle,
    _tmpdir: PathBuf,
}

pub struct HarnessSpec {
    pub link_cfgs: Vec<(String, ImpairConfig)>,
    /// TCP+TLS connections opened per configured link.
    pub connections: u32,
    pub psk: String,
}

impl Default for HarnessSpec {
    fn default() -> Self {
        Self {
            link_cfgs: vec![
                (
                    "a".into(),
                    ImpairConfig {
                        rtt: Duration::from_millis(10),
                        ..Default::default()
                    },
                ),
                (
                    "b".into(),
                    ImpairConfig {
                        rtt: Duration::from_millis(60),
                        ..Default::default()
                    },
                ),
                (
                    "c".into(),
                    ImpairConfig {
                        rtt: Duration::from_millis(150),
                        ..Default::default()
                    },
                ),
            ],
            connections: 2,
            psk: "e2e-psk".into(),
        }
    }
}

pub async fn bind_local() -> Result<(TcpListener, SocketAddr)> {
    let l = TcpListener::bind("127.0.0.1:0").await?;
    let a = l.local_addr()?;
    Ok((l, a))
}

async fn echo_server(listener: TcpListener) {
    loop {
        let Ok((mut tcp, _)) = listener.accept().await else {
            break;
        };
        tokio::spawn(async move {
            let _ = tcp.set_nodelay(true);
            let mut buf = vec![0u8; 16 * 1024];
            loop {
                match tcp.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if tcp.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });
    }
}

pub async fn start(spec: HarnessSpec) -> Result<Harness> {
    install_crypto();
    let tmpdir = std::env::temp_dir().join(format!(
        "nya-e2e-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let pin = gen_cert(&tmpdir, "nya.local")?;
    let (cert, key) = cert_paths(&tmpdir);

    let (echo_l, echo) = bind_local().await?;
    let echo_task = tokio::spawn(echo_server(echo_l));
    let echo_abort = echo_task.abort_handle();

    let (srv_l, server) = bind_local().await?;
    let srv_cfg = ServerConfig {
        listen: server.to_string(),
        psk: spec.psk.clone(),
        cert,
        key,
        session: Default::default(),
    };
    let (server_stop, stop_rx) = watch::channel(false);
    tokio::spawn(async move {
        if let Err(e) = run_on_until(srv_l, srv_cfg, stop_rx).await {
            tracing::error!(error = %e, "nya-server exited");
        }
    });

    let max_rtt = spec
        .link_cfgs
        .iter()
        .map(|(_, c)| c.rtt)
        .max()
        .unwrap_or(Duration::from_millis(10));
    let mut links = Vec::new();
    for (name, cfg) in spec.link_cfgs {
        let h = spawn_link(name, server, cfg).await?;
        links.push(h);
    }

    let (fwd_l, forward) = bind_local().await?;
    let (socks_l, socks) = bind_local().await?;
    let client_cfg = ClientConfig {
        psk: spec.psk,
        pinned_spki_sha256: hex_pin(&pin),
        session: Default::default(),
        links: links
            .iter()
            .map(|l| Link {
                name: l.name.clone(),
                addr: l.listen.to_string(),
                connections: spec.connections.max(1),
            })
            .collect(),
        inbounds: vec![],
    };
    let pin_arr =
        parse_pin_hex(&client_cfg.pinned_spki_sha256).map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut sc = SessionConfig::default();
    sc.tuning.handshake_timeout = (max_rtt * 30).max(Duration::from_secs(3));
    let session = Session::new_client(sc);
    spawn_links(&client_cfg, session.clone(), pin_arr);
    let sess_fwd = session.clone();
    let echo_s = echo.to_string();
    tokio::spawn(async move {
        let _ = serve_forward_listener(fwd_l, sess_fwd, echo_s).await;
    });
    let sess_socks = session.clone();
    tokio::spawn(async move {
        let _ = serve_socks5_listener(socks_l, sess_socks).await;
    });
    let expect = (spec.connections.max(1) as usize)
        .saturating_mul(links.len())
        .max(1);
    session
        .wait_paths(expect, Duration::from_secs(8) + max_rtt * 40)
        .await
        .context("client paths not ready")?;
    info!(%echo, %server, %forward, nlinks = links.len(), npaths = expect, "harness ready");
    Ok(Harness {
        echo,
        server,
        forward,
        socks,
        links,
        session,
        server_stop,
        echo_abort,
        _tmpdir: tmpdir,
    })
}

static NEXT: AtomicU64 = AtomicU64::new(1);

fn hex_pin(pin: &[u8; 32]) -> String {
    pin.iter().map(|b| format!("{b:02x}")).collect()
}

impl Harness {
    pub fn link(&self, name: &str) -> &LinkHandle {
        self.links
            .iter()
            .find(|l| l.name == name)
            .expect("unknown link")
    }

    pub async fn connect_forward(&self) -> Result<TcpStream> {
        let s = TcpStream::connect(self.forward).await?;
        let _ = s.set_nodelay(true);
        Ok(s)
    }

    pub fn shutdown(&self) {
        self.session.shutdown();
        for l in &self.links {
            l.shutdown();
        }
        let _ = self.server_stop.send(true);
        self.echo_abort.abort();
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.shutdown();
        let _ = std::fs::remove_dir_all(&self._tmpdir);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::impair::ImpairConfig;

    #[tokio::test]
    async fn harness_drop_stops_accept() {
        let h = start(HarnessSpec {
            link_cfgs: vec![(
                "a".into(),
                ImpairConfig {
                    rtt: Duration::from_millis(10),
                    ..Default::default()
                },
            )],
            connections: 1,
            psk: "e2e-psk".into(),
        })
        .await
        .unwrap();
        let listen = h.links[0].listen;
        drop(h);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            TcpStream::connect(listen).await.is_err(),
            "impair listener must be gone after Drop"
        );
    }
}
