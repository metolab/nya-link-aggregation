use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::atomic::Ordering;

use anyhow::{bail, Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn};

use nya_core::Session;
use nya_proto::Target;

use crate::config::Inbound;

pub async fn serve_inbounds(inbounds: Vec<Inbound>, session: Session) -> Result<()> {
    if inbounds.is_empty() {
        bail!("no inbounds configured");
    }
    let mut joins = Vec::new();
    for ib in inbounds {
        let session = session.clone();
        joins.push(tokio::spawn(async move {
            if let Err(e) = run_inbound(ib, session).await {
                warn!(error = %e, "inbound exited");
            }
        }));
    }
    for j in joins {
        let _ = j.await;
    }
    Ok(())
}

async fn run_inbound(ib: Inbound, session: Session) -> Result<()> {
    match ib {
        Inbound::Socks5 { listen } => {
            let listener = TcpListener::bind(&listen).await?;
            serve_socks5_listener(listener, session).await
        }
        Inbound::Forward { listen, target } => {
            let listener = TcpListener::bind(&listen).await?;
            serve_forward_listener(listener, session, target).await
        }
    }
}

pub async fn serve_socks5_listener(listener: TcpListener, session: Session) -> Result<()> {
    info!(listen = %listener.local_addr()?, "socks5 listen");
    loop {
        tokio::select! {
            _ = session.wait_dead() => break,
            acc = listener.accept() => {
                let (tcp, peer) = acc?;
                let session = session.clone();
                tokio::spawn(async move {
                    if let Err(e) = serve_socks5(tcp, session).await {
                        warn!(%peer, error = %e, "socks5 session");
                    }
                });
            }
        }
    }
    Ok(())
}

pub async fn serve_forward_listener(
    listener: TcpListener,
    session: Session,
    target: String,
) -> Result<()> {
    let dest = Target::parse(&target).context("forward target")?;
    info!(listen = %listener.local_addr()?, %target, "forward listen");
    loop {
        tokio::select! {
            _ = session.wait_dead() => break,
            acc = listener.accept() => {
                let (mut tcp, peer) = acc?;
                let session = session.clone();
                let dest = dest.clone();
                tokio::spawn(async move {
                    match session.open_stream(dest).await {
                        Ok(mut tun) => {
                            session
                                .process()
                                .inbound_accept
                                .fetch_add(1, Ordering::Relaxed);
                            let _ = tokio::io::copy_bidirectional(&mut tcp, &mut tun).await;
                        }
                        Err(e) => {
                            session
                                .process()
                                .inbound_open_fail
                                .fetch_add(1, Ordering::Relaxed);
                            warn!(%peer, error = %e, "open forward stream");
                        }
                    }
                });
            }
        }
    }
    Ok(())
}

async fn serve_socks5(mut tcp: TcpStream, session: Session) -> Result<()> {
    let mut hdr = [0u8; 2];
    tcp.read_exact(&mut hdr).await?;
    if hdr[0] != 0x05 {
        session
            .process()
            .inbound_reject
            .fetch_add(1, Ordering::Relaxed);
        bail!("not socks5");
    }
    let n = hdr[1] as usize;
    let mut methods = vec![0u8; n];
    tcp.read_exact(&mut methods).await?;
    tcp.write_all(&[0x05, 0x00]).await?;

    let mut req = [0u8; 4];
    tcp.read_exact(&mut req).await?;
    if req[0] != 0x05 {
        session
            .process()
            .inbound_reject
            .fetch_add(1, Ordering::Relaxed);
        bail!("bad socks ver");
    }
    if req[1] != 0x01 {
        session
            .process()
            .inbound_reject
            .fetch_add(1, Ordering::Relaxed);
        reply(&mut tcp, 0x07).await?;
        bail!("only CONNECT supported");
    }
    let host = match req[3] {
        0x01 => {
            let mut a = [0u8; 4];
            tcp.read_exact(&mut a).await?;
            Ipv4Addr::from(a).to_string()
        }
        0x03 => {
            let mut l = [0u8; 1];
            tcp.read_exact(&mut l).await?;
            let mut d = vec![0u8; l[0] as usize];
            tcp.read_exact(&mut d).await?;
            String::from_utf8(d).context("socks domain")?
        }
        0x04 => {
            let mut a = [0u8; 16];
            tcp.read_exact(&mut a).await?;
            Ipv6Addr::from(a).to_string()
        }
        _ => {
            session
                .process()
                .inbound_reject
                .fetch_add(1, Ordering::Relaxed);
            reply(&mut tcp, 0x08).await?;
            bail!("bad atyp");
        }
    };
    let mut pb = [0u8; 2];
    tcp.read_exact(&mut pb).await?;
    let port = u16::from_be_bytes(pb);

    match session
        .open_stream(Target {
            host: host.clone(),
            port,
        })
        .await
    {
        Ok(mut tun) => {
            session
                .process()
                .inbound_accept
                .fetch_add(1, Ordering::Relaxed);
            reply(&mut tcp, 0x00).await?;
            let _ = tokio::io::copy_bidirectional(&mut tcp, &mut tun).await;
        }
        Err(e) => {
            session
                .process()
                .inbound_open_fail
                .fetch_add(1, Ordering::Relaxed);
            reply(&mut tcp, 0x04).await?;
            warn!(%host, port, error = %e, "socks connect failed");
        }
    }
    Ok(())
}

async fn reply(tcp: &mut TcpStream, rep: u8) -> Result<()> {
    tcp.write_all(&[0x05, rep, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
        .await?;
    Ok(())
}
