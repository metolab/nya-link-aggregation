//! Periodic snapshot logs and optional loopback `/metrics`.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tracing::{info, warn};

use crate::catalog::{format_snapshot_metrics, snapshot_p99};
use crate::cfg::ObsOpts;
use crate::metrics::{path_state_label, ProcessSnapshot};
use crate::session::{Session, SessionTable};

const HTTP_READ_CAP: usize = 8 * 1024;

/// Numeric `SocketAddr` that is loopback. Hostname (`localhost`) and
/// non-loopback (`0.0.0.0`) are refused.
pub fn parse_metrics_listen(s: &str) -> Option<SocketAddr> {
    let addr: SocketAddr = s.parse().ok()?;
    if addr.ip().is_loopback() {
        Some(addr)
    } else {
        None
    }
}

pub fn spawn_obs_session(session: Session, obs: ObsOpts) {
    let interval = obs.snapshot_interval();
    let listen = obs.metrics_listen().map(str::to_string);
    if interval.is_none() && listen.is_none() {
        return;
    }
    tokio::spawn(async move {
        let snap = {
            let session = session.clone();
            move || ProcessSnapshot {
                process: session.process().snap(),
                session: session.snapshot(),
            }
        };
        run_obs(interval, listen, snap, async {
            session.wait_dead().await;
        })
        .await;
    });
}

pub fn spawn_obs_table(table: Arc<SessionTable>, obs: ObsOpts, mut stop: watch::Receiver<bool>) {
    let interval = obs.snapshot_interval();
    let listen = obs.metrics_listen().map(str::to_string);
    if interval.is_none() && listen.is_none() {
        return;
    }
    tokio::spawn(async move {
        let snap = {
            let table = table.clone();
            move || table.aggregate_snapshot()
        };
        run_obs(interval, listen, snap, async {
            tokio::select! {
                _ = stop.wait_for(|v| *v) => {}
                _ = async {
                    loop {
                        if table.is_closed() {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                } => {}
            }
        })
        .await;
    });
}

async fn run_obs<F, S>(interval: Option<Duration>, listen: Option<String>, snap: F, stop: S)
where
    F: Fn() -> ProcessSnapshot + Send + Sync + 'static,
    S: std::future::Future<Output = ()> + Send,
{
    let snap = Arc::new(snap);
    let http = match listen.as_deref() {
        None => None,
        Some(s) => match parse_metrics_listen(s) {
            Some(addr) => match TcpListener::bind(addr).await {
                Ok(l) => {
                    info!(%addr, "metrics listening");
                    let snap = snap.clone();
                    Some(tokio::spawn(async move {
                        serve_metrics(l, snap).await;
                    }))
                }
                Err(e) => {
                    warn!(listen = %s, error = %e, "metrics_listen bind failed");
                    None
                }
            },
            None => {
                warn!(
                    listen = %s,
                    "metrics_listen invalid or not loopback, refusing"
                );
                None
            }
        },
    };

    if let Some(d) = interval {
        let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + d, d);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        tokio::pin!(stop);
        loop {
            tokio::select! {
                _ = &mut stop => break,
                _ = tick.tick() => emit_snapshot(&snap()),
            }
        }
    } else {
        stop.await;
    }
    if let Some(h) = http {
        h.abort();
    }
}

fn emit_snapshot(ps: &ProcessSnapshot) {
    let s = &ps.session;
    let (stall_p99, failover_p99) = snapshot_p99(ps);
    let metrics = format_snapshot_metrics(ps);
    info!(
        target: "nya_core::obs",
        stall_p99_ms = stall_p99,
        failover_p99_ms = failover_p99,
        paths_alive = s.paths.len() as u64,
        paths = %format_paths(&s.paths),
        links = %format_links(&s.links),
        streams = %format_streams(&s.streams, s.streams_live),
        metrics = %metrics,
        "snapshot"
    );
}

fn format_paths(paths: &[crate::metrics::PathSnap]) -> String {
    paths
        .iter()
        .map(|p| {
            format!(
                "{}={}/{}/{}ms {} inf={} st={} cong={} rx={} tx={} ping={} q={}/{}{}",
                p.name,
                p.rtt_us / 1000,
                p.stable_rtt_us / 1000,
                p.class_rtt_us / 1000,
                path_state_label(p.state),
                p.inflight,
                p.sticky,
                if p.congested { 1 } else { 0 },
                p.last_rx_ago_us / 1000,
                p.last_tx_ago_us / 1000,
                p.pending_ping,
                p.queued_urgent,
                p.queued_bulk,
                if p.rtt_known { "" } else { " unk" },
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn format_links(links: &[crate::metrics::LinkSnap]) -> String {
    links
        .iter()
        .map(|l| {
            format!(
                "{}={}/{} {}-{}ms st={} inf={} cong={} rx={}/{} q={}/{}",
                l.name,
                l.up,
                l.degraded,
                l.rtt_us / 1000,
                l.rtt_max_us / 1000,
                l.sticky,
                l.inflight,
                l.congested,
                l.rx_fresh_us / 1000,
                l.rx_stale_us / 1000,
                l.queued_urgent,
                l.queued_bulk,
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn format_streams(streams: &[crate::metrics::StreamSnap], live: u64) -> String {
    let extra = live.saturating_sub(streams.len() as u64);
    let mut parts: Vec<String> = streams
        .iter()
        .map(|s| {
            format!(
                "{}={}{}{} u={}",
                s.id,
                s.path,
                if s.bulk { " bulk" } else { " ping" },
                if s.stalled { " stall" } else { "" },
                s.unacked,
            )
        })
        .collect();
    if extra > 0 {
        parts.push(format!("+{extra}"));
    }
    parts.join("; ")
}

async fn serve_metrics<F>(listener: TcpListener, snap: Arc<F>)
where
    F: Fn() -> ProcessSnapshot + Send + Sync + 'static,
{
    loop {
        let Ok((tcp, _)) = listener.accept().await else {
            break;
        };
        let snap = snap.clone();
        tokio::spawn(async move {
            let _ = handle_http(tcp, snap.as_ref()).await;
        });
    }
}

async fn handle_http<F>(mut tcp: TcpStream, snap: &F) -> std::io::Result<()>
where
    F: Fn() -> ProcessSnapshot,
{
    let mut buf = vec![0u8; HTTP_READ_CAP];
    let mut n = 0usize;
    loop {
        if n >= HTTP_READ_CAP {
            return Ok(());
        }
        let got = tcp.read(&mut buf[n..]).await?;
        if got == 0 {
            return Ok(());
        }
        n += got;
        if buf[..n].windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }
    let req = std::str::from_utf8(&buf[..n]).unwrap_or("");
    let line = req.lines().next().unwrap_or("");
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");
    if method != "GET" {
        tcp.write_all(
            b"HTTP/1.1 405 Method Not Allowed\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
        )
        .await?;
        return Ok(());
    }
    if path != "/metrics" && path != "/" {
        tcp.write_all(b"HTTP/1.1 404 Not Found\r\nConnection: close\r\nContent-Length: 0\r\n\r\n")
            .await?;
        return Ok(());
    }
    let body = crate::catalog::render_prometheus(&snap());
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    tcp.write_all(head.as_bytes()).await?;
    tcp.write_all(body.as_bytes()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{format_snapshot_metrics, prometheus_metric_names, render_prometheus};
    use crate::metrics::{
        HistSnap, Snapshot, FAILOVER_MS_BOUNDS, LIFETIME_MS_BOUNDS, STALL_MS_BOUNDS,
    };

    #[test]
    fn localhost_and_unspecified_refused() {
        assert!(parse_metrics_listen("localhost:9100").is_none());
        assert!(parse_metrics_listen("0.0.0.0:9100").is_none());
        assert!(parse_metrics_listen("[::]:9100").is_none());
        assert!(parse_metrics_listen("127.0.0.1:9100").is_some());
        assert!(parse_metrics_listen("[::1]:9100").is_some());
    }

    #[test]
    fn prometheus_has_type_and_cumulative_buckets() {
        let mut s = Snapshot {
            failover_ms: HistSnap::zeroed(FAILOVER_MS_BOUNDS),
            stall_ms: HistSnap::zeroed(STALL_MS_BOUNDS),
            stream_lifetime_ms: HistSnap::zeroed(LIFETIME_MS_BOUNDS),
            ..Default::default()
        };
        s.failover_ms.buckets[0] = 2;
        s.failover_ms.buckets[1] = 3;
        s.failover_ms.count = 5;
        s.failover_ms.sum = 20;
        let body = render_prometheus(&ProcessSnapshot {
            process: Default::default(),
            session: s,
        });
        assert!(body.contains("# TYPE nya_failover_ms histogram"));
        assert!(body.contains("nya_failover_ms_bucket{le=\"5\"} 2"));
        assert!(body.contains("nya_failover_ms_bucket{le=\"10\"} 5"));
        assert!(body.contains("nya_failover_ms_count 5"));
        assert!(body.contains("# TYPE nya_sessions_live gauge"));
        assert!(body.contains("# TYPE nya_streams_held gauge"));
        assert!(body.contains("# TYPE nya_path_added_total counter"));
    }

    #[test]
    fn catalog_includes_held_and_snapshot_uses_catalog_names() {
        let mut s = Snapshot {
            failover_ms: HistSnap::zeroed(FAILOVER_MS_BOUNDS),
            stall_ms: HistSnap::zeroed(STALL_MS_BOUNDS),
            stream_lifetime_ms: HistSnap::zeroed(LIFETIME_MS_BOUNDS),
            failbacks: 3,
            streams_held: 2,
            ..Default::default()
        };
        s.paths.push(crate::metrics::PathSnap {
            name: "a#0".into(),
            link: "a".into(),
            rtt_us: 12_000,
            ..Default::default()
        });
        let ps = ProcessSnapshot {
            process: Default::default(),
            session: s,
        };
        let names = prometheus_metric_names(&ps);
        assert!(names.contains("nya_streams_held"));
        assert!(names.contains("nya_failbacks_total"));
        assert!(names.contains("nya_path_rtt_us"));
        assert!(names.contains("nya_failover_ms_bucket"));
        let n_counter = names.iter().filter(|n| n.ends_with("_total")).count();
        assert_eq!(n_counter, 48, "{names:?}");
        let kv = format_snapshot_metrics(&ps);
        assert!(kv.contains("nya_failbacks_total=3"), "{kv}");
        assert!(kv.contains("nya_streams_held=2"), "{kv}");
        assert!(
            kv.contains("nya_path_rtt_us{path=\"a#0\",link=\"a\"}=12000"),
            "{kv}"
        );
        let body = render_prometheus(&ps);
        assert!(body.contains("nya_failbacks_total 3"));
        assert!(kv.contains("nya_failbacks_total=3"));
    }
}
