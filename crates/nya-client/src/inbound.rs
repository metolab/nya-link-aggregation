use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::atomic::Ordering;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn, Instrument};

use nya_core::{
    io_err_kind, HopClock, HopOutcome, HopProbe, HopRole, HopSample, Session, TunnelStream,
};
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
                configure_inbound_tcp(&tcp);
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
                let (tcp, peer) = acc?;
                configure_inbound_tcp(&tcp);
                let session = session.clone();
                let dest = dest.clone();
                tokio::spawn(async move {
                    let span = tracing::info_span!(
                        target: "nya_otel",
                        "nya.inbound.forward",
                        otel.kind = "server",
                        nya.target = %dest,
                        otel.status_code = tracing::field::Empty,
                        nya.open_us = tracing::field::Empty,
                    );
                    let t0 = Instant::now();
                    let opened = session
                        .open_stream(dest.clone())
                        .instrument(span.clone())
                        .await;
                    let open_us = (t0.elapsed().as_micros() as u64).max(1);
                    span.record("nya.open_us", open_us);
                    match opened {
                        Ok(tun) => {
                            drop(span);
                            session
                                .process()
                                .inbound_accept
                                .fetch_add(1, Ordering::Relaxed);
                            copy_with_hop(&session, tcp, tun, dest.host.clone(), open_us).await;
                        }
                        Err(e) => {
                            span.record("otel.status_code", "ERROR");
                            session
                                .process()
                                .inbound_open_fail
                                .fetch_add(1, Ordering::Relaxed);
                            record_open_fail(&session, dest.host.clone(), open_us);
                            {
                                let _g = span.enter();
                                warn!(%peer, error = %e, "open forward stream");
                            }
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

    let span = tracing::info_span!(
        target: "nya_otel",
        "nya.inbound.socks5",
        otel.kind = "server",
        nya.host = %host,
        nya.port = port,
        nya.session_fp = tracing::field::Empty,
        nya.stream_id = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
        nya.open_us = tracing::field::Empty,
    );
    if let Some(fp) = session.session_fp() {
        span.record("nya.session_fp", fp.as_str());
    }
    let t0 = Instant::now();
    let opened = session
        .open_stream(Target {
            host: host.clone(),
            port,
        })
        .instrument(span.clone())
        .await;
    let open_us = (t0.elapsed().as_micros() as u64).max(1);
    span.record("nya.open_us", open_us);
    match opened {
        Ok(tun) => {
            span.record("nya.stream_id", tun.id);
            drop(span);
            session
                .process()
                .inbound_accept
                .fetch_add(1, Ordering::Relaxed);
            reply(&mut tcp, 0x00).await?;
            copy_with_hop(&session, tcp, tun, host, open_us).await;
        }
        Err(e) => {
            span.record("otel.status_code", "ERROR");
            session
                .process()
                .inbound_open_fail
                .fetch_add(1, Ordering::Relaxed);
            record_open_fail(&session, host.clone(), open_us);
            reply(&mut tcp, 0x04).await?;
            {
                let _g = span.enter();
                warn!(%host, port, error = %e, "socks connect failed");
            }
        }
    }
    Ok(())
}

fn record_open_fail(session: &Session, host: String, open_us: u64) {
    session.process().record_hop(HopSample {
        role: HopRole::Client,
        stream_id: 0,
        host,
        session_fp: session.session_fp().unwrap_or_default(),
        outcome: HopOutcome::OpenFail,
        open_us: Some(open_us),
        copy_us: None,
        ..Default::default()
    });
}

async fn copy_with_hop(
    session: &Session,
    mut tcp: TcpStream,
    tun: TunnelStream,
    host: String,
    open_us: u64,
) {
    let clock = HopClock::new();
    let stream_id = tun.id;
    let mut overlay = HopProbe::wrap(tun, clock.clone());
    let t_copy = Instant::now();
    let copy = tokio::io::copy_bidirectional(&mut tcp, &mut overlay).await;
    let (outcome, copy_err) = match &copy {
        Ok(_) => (HopOutcome::Ok, None),
        Err(e) => (HopOutcome::CopyErr, Some(io_err_kind(e))),
    };
    session.process().record_hop(HopSample {
        role: HopRole::Client,
        stream_id,
        host,
        session_fp: session.session_fp().unwrap_or_default(),
        outcome,
        copy_us: Some((t_copy.elapsed().as_micros() as u64).max(1)),
        open_us: Some(open_us),
        first_rx_us: clock.first_rx_us(),
        last_rx_us: clock.last_rx_us(),
        first_tx_us: clock.first_tx_us(),
        rx_bytes: Some(clock.rx_bytes()),
        tx_bytes: Some(clock.tx_bytes()),
        copy_err,
        ..Default::default()
    });
}

fn configure_inbound_tcp(tcp: &TcpStream) {
    let _ = tcp.set_nodelay(true);
}

async fn reply(tcp: &mut TcpStream, rep: u8) -> Result<()> {
    tcp.write_all(&[0x05, rep, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn configure_inbound_tcp_sets_nodelay() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });
        let (server, _) = listener.accept().await.unwrap();
        configure_inbound_tcp(&server);
        assert!(server.nodelay().unwrap());
        drop(client.await.unwrap());
    }
}
