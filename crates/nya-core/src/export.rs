//! Periodic snapshot logs and optional loopback `/metrics`.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tracing::{info, warn};

use crate::cfg::ObsOpts;
use crate::metrics::{
    path_state_label, percentile, ProcessSnapshot, FAILOVER_MS_BOUNDS, LIFETIME_MS_BOUNDS,
    STALL_MS_BOUNDS,
};
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
    let p = &ps.process;
    let stall_p99 = percentile(&s.stall_ms, STALL_MS_BOUNDS, 99.0);
    let failover_p99 = percentile(&s.failover_ms, FAILOVER_MS_BOUNDS, 99.0);
    info!(
        target: "nya_core::obs",
        streams_opened = s.streams_opened,
        streams_closed = s.streams_closed,
        stream_resets = s.stream_resets,
        stream_resets_dial_failed = s.stream_resets_dial_failed,
        stream_resets_timeout = s.stream_resets_timeout,
        stream_resets_peer = s.stream_resets_peer,
        stream_resets_session_dead = s.stream_resets_session_dead,
        stream_resets_protocol = s.stream_resets_protocol,
        streams_stalled = s.streams_stalled,
        streams_live = s.streams_live,
        stall_count = s.stall_ms.count,
        stall_p99_ms = stall_p99,
        failover_count = s.failover_ms.count,
        failover_p99_ms = failover_p99,
        migrates = s.migrates,
        migrates_speculative = s.migrates_speculative,
        migrates_path_down = s.migrates_path_down,
        migrates_ensure_sticky = s.migrates_ensure_sticky,
        migrates_send_blocked = s.migrates_send_blocked,
        data_retransmit = s.data_retransmit,
        data_hedge = s.data_hedge,
        probe_miss = s.probe_miss,
        window_blocks = s.window_blocks,
        picks_unknown_rtt = s.picks_unknown_rtt,
        picks_unknown_over_known = s.picks_unknown_over_known,
        failbacks = s.failbacks,
        failbacks_upgrade = s.failbacks_upgrade,
        failbacks_class_empty = s.failbacks_class_empty,
        failbacks_same_link = s.failbacks_same_link,
        hol_rebalances = s.hol_rebalances,
        path_added = s.path_added,
        path_down = s.path_down,
        path_degraded = s.path_degraded,
        bytes_data_tx = s.bytes_data_tx,
        bytes_data_rx = s.bytes_data_rx,
        bytes_ctrl_tx = s.bytes_ctrl_tx,
        bytes_ctrl_rx = s.bytes_ctrl_rx,
        frame_send_drop = s.frame_send_drop,
        session_all_down_resets = s.session_all_down_resets,
        sessions_live = p.sessions_live,
        sessions_created = p.sessions_created,
        sessions_dead = p.sessions_dead,
        inbound_accept = p.inbound_accept,
        inbound_reject = p.inbound_reject,
        inbound_open_fail = p.inbound_open_fail,
        outbound_dial_ok = p.outbound_dial_ok,
        outbound_dial_fail = p.outbound_dial_fail,
        handshake_create_ok = p.handshake_create_ok,
        handshake_join_ok = p.handshake_join_ok,
        handshake_fail_auth = p.handshake_fail_auth,
        handshake_fail_version = p.handshake_fail_version,
        handshake_fail_unknown = p.handshake_fail_unknown,
        handshake_fail_other = p.handshake_fail_other,
        reconnect_ok = p.reconnect_ok,
        reconnect_fail = p.reconnect_fail,
        paths_alive = s.paths.len() as u64,
        paths = %format_paths(&s.paths),
        links = %format_links(&s.links),
        streams = %format_streams(&s.streams, s.streams_live),
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
    let body = render_prometheus(&snap());
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    tcp.write_all(head.as_bytes()).await?;
    tcp.write_all(body.as_bytes()).await?;
    Ok(())
}

pub fn render_prometheus(ps: &ProcessSnapshot) -> String {
    let mut o = String::with_capacity(4096);
    let s = &ps.session;
    let p = &ps.process;
    counter(&mut o, "nya_path_added_total", "paths added", s.path_added);
    counter(
        &mut o,
        "nya_path_down_total",
        "paths marked down",
        s.path_down,
    );
    counter(
        &mut o,
        "nya_path_degraded_total",
        "paths marked degraded",
        s.path_degraded,
    );
    counter(&mut o, "nya_migrates_total", "stream resticks", s.migrates);
    counter(
        &mut o,
        "nya_migrates_speculative_total",
        "speculative resticks",
        s.migrates_speculative,
    );
    counter(
        &mut o,
        "nya_migrates_path_down_total",
        "resticks after path down",
        s.migrates_path_down,
    );
    counter(
        &mut o,
        "nya_migrates_ensure_sticky_total",
        "ensure_sticky resticks",
        s.migrates_ensure_sticky,
    );
    counter(
        &mut o,
        "nya_migrates_send_blocked_total",
        "send-blocked resticks",
        s.migrates_send_blocked,
    );
    counter(
        &mut o,
        "nya_data_retransmit_total",
        "STREAM_DATA retransmits",
        s.data_retransmit,
    );
    counter(
        &mut o,
        "nya_data_hedge_total",
        "STREAM_DATA hedge copies",
        s.data_hedge,
    );
    counter(
        &mut o,
        "nya_probe_miss_total",
        "pings expired without pong",
        s.probe_miss,
    );
    counter(
        &mut o,
        "nya_window_blocks_total",
        "times send waited on stream window",
        s.window_blocks,
    );
    counter(
        &mut o,
        "nya_picks_unknown_rtt_total",
        "new streams picked onto unknown-RTT path",
        s.picks_unknown_rtt,
    );
    counter(
        &mut o,
        "nya_picks_unknown_over_known_total",
        "unknown-RTT pick while a sampled path existed",
        s.picks_unknown_over_known,
    );
    counter(
        &mut o,
        "nya_failbacks_total",
        "cross-link failbacks",
        s.failbacks,
    );
    counter(
        &mut o,
        "nya_failbacks_upgrade_total",
        "cross-link upgrade failbacks",
        s.failbacks_upgrade,
    );
    counter(
        &mut o,
        "nya_failbacks_class_empty_total",
        "cross-link class-empty failbacks",
        s.failbacks_class_empty,
    );
    counter(
        &mut o,
        "nya_failbacks_same_link_total",
        "same-link failbacks",
        s.failbacks_same_link,
    );
    counter(
        &mut o,
        "nya_hol_rebalances_total",
        "HOL rebalances",
        s.hol_rebalances,
    );
    counter(
        &mut o,
        "nya_streams_opened_total",
        "streams opened",
        s.streams_opened,
    );
    counter(
        &mut o,
        "nya_streams_closed_total",
        "streams closed gracefully",
        s.streams_closed,
    );
    counter(
        &mut o,
        "nya_stream_resets_total",
        "streams reset",
        s.stream_resets,
    );
    counter(
        &mut o,
        "nya_stream_resets_dial_failed_total",
        "resets: dial failed",
        s.stream_resets_dial_failed,
    );
    counter(
        &mut o,
        "nya_stream_resets_timeout_total",
        "resets: timeout",
        s.stream_resets_timeout,
    );
    counter(
        &mut o,
        "nya_stream_resets_peer_total",
        "resets: peer",
        s.stream_resets_peer,
    );
    counter(
        &mut o,
        "nya_stream_resets_session_dead_total",
        "resets: session dead",
        s.stream_resets_session_dead,
    );
    counter(
        &mut o,
        "nya_stream_resets_protocol_total",
        "resets: protocol",
        s.stream_resets_protocol,
    );
    counter(
        &mut o,
        "nya_bytes_data_tx_total",
        "overlay StreamData payload tx",
        s.bytes_data_tx,
    );
    counter(
        &mut o,
        "nya_bytes_data_rx_total",
        "overlay StreamData payload rx",
        s.bytes_data_rx,
    );
    counter(
        &mut o,
        "nya_bytes_ctrl_tx_total",
        "overlay control bytes tx",
        s.bytes_ctrl_tx,
    );
    counter(
        &mut o,
        "nya_bytes_ctrl_rx_total",
        "overlay control bytes rx",
        s.bytes_ctrl_rx,
    );
    counter(
        &mut o,
        "nya_frame_send_drop_total",
        "frames dropped (queue full)",
        s.frame_send_drop,
    );
    counter(
        &mut o,
        "nya_session_all_down_resets_total",
        "all-down session resets",
        s.session_all_down_resets,
    );
    counter(
        &mut o,
        "nya_handshake_create_ok_total",
        "create-session ok",
        p.handshake_create_ok,
    );
    counter(
        &mut o,
        "nya_handshake_join_ok_total",
        "join-session ok",
        p.handshake_join_ok,
    );
    counter(
        &mut o,
        "nya_handshake_fail_auth_total",
        "handshake auth fail",
        p.handshake_fail_auth,
    );
    counter(
        &mut o,
        "nya_handshake_fail_version_total",
        "handshake version fail",
        p.handshake_fail_version,
    );
    counter(
        &mut o,
        "nya_handshake_fail_unknown_total",
        "handshake unknown session",
        p.handshake_fail_unknown,
    );
    counter(
        &mut o,
        "nya_handshake_fail_other_total",
        "handshake other fail",
        p.handshake_fail_other,
    );
    counter(
        &mut o,
        "nya_inbound_accept_total",
        "inbound accepts",
        p.inbound_accept,
    );
    counter(
        &mut o,
        "nya_inbound_reject_total",
        "inbound rejects",
        p.inbound_reject,
    );
    counter(
        &mut o,
        "nya_inbound_open_fail_total",
        "inbound open_stream fail",
        p.inbound_open_fail,
    );
    counter(
        &mut o,
        "nya_outbound_dial_ok_total",
        "outbound dial ok",
        p.outbound_dial_ok,
    );
    counter(
        &mut o,
        "nya_outbound_dial_fail_total",
        "outbound dial fail",
        p.outbound_dial_fail,
    );
    counter(
        &mut o,
        "nya_reconnect_ok_total",
        "path up (incl first)",
        p.reconnect_ok,
    );
    counter(
        &mut o,
        "nya_reconnect_fail_total",
        "link connect fail",
        p.reconnect_fail,
    );
    counter(
        &mut o,
        "nya_sessions_created_total",
        "sessions created",
        p.sessions_created,
    );
    counter(
        &mut o,
        "nya_sessions_dead_total",
        "sessions dead",
        p.sessions_dead,
    );
    gauge(
        &mut o,
        "nya_streams_stalled",
        "streams currently stalled",
        s.streams_stalled,
    );
    gauge(&mut o, "nya_streams_live", "live streams", s.streams_live);
    gauge(
        &mut o,
        "nya_sessions_live",
        "live sessions",
        p.sessions_live,
    );
    hist(
        &mut o,
        "nya_failover_ms",
        "overlay path-silence to restick/down, milliseconds",
        FAILOVER_MS_BOUNDS,
        &s.failover_ms,
    );
    hist(
        &mut o,
        "nya_stall_ms",
        "send-unacked or recv-hole stall duration, milliseconds",
        STALL_MS_BOUNDS,
        &s.stall_ms,
    );
    hist(
        &mut o,
        "nya_stream_lifetime_ms",
        "stream lifetime, milliseconds",
        LIFETIME_MS_BOUNDS,
        &s.stream_lifetime_ms,
    );
    for ln in &s.links {
        let l = prometheus_label(&ln.name);
        gauge_l(
            &mut o,
            "nya_link_conns",
            "TCP connections on link",
            "link",
            &l,
            ln.conns,
        );
        gauge_l(
            &mut o,
            "nya_link_up",
            "UP connections on link",
            "link",
            &l,
            ln.up,
        );
        gauge_l(
            &mut o,
            "nya_link_degraded",
            "DEGRADED connections on link",
            "link",
            &l,
            ln.degraded,
        );
        gauge_l(
            &mut o,
            "nya_link_rtt_us",
            "best known RTT on link",
            "link",
            &l,
            ln.rtt_us,
        );
        gauge_l(
            &mut o,
            "nya_link_rtt_max_us",
            "worst known RTT on link",
            "link",
            &l,
            ln.rtt_max_us,
        );
        gauge_l(
            &mut o,
            "nya_link_inflight_bytes",
            "inflight on link",
            "link",
            &l,
            ln.inflight,
        );
        gauge_l(
            &mut o,
            "nya_link_sticky",
            "sticky streams on link",
            "link",
            &l,
            ln.sticky,
        );
        gauge_l(
            &mut o,
            "nya_link_congested",
            "congested connections on link",
            "link",
            &l,
            ln.congested,
        );
        gauge_l(
            &mut o,
            "nya_link_rx_fresh_us",
            "freshest last-rx on link",
            "link",
            &l,
            ln.rx_fresh_us,
        );
        gauge_l(
            &mut o,
            "nya_link_rx_stale_us",
            "stalest last-rx on link",
            "link",
            &l,
            ln.rx_stale_us,
        );
        gauge_l(
            &mut o,
            "nya_link_queued_urgent",
            "urgent queue on link",
            "link",
            &l,
            ln.queued_urgent,
        );
        gauge_l(
            &mut o,
            "nya_link_queued_bulk",
            "bulk queue on link",
            "link",
            &l,
            ln.queued_bulk,
        );
    }
    for pth in &s.paths {
        let l = prometheus_label(&pth.name);
        let lk = prometheus_label(&pth.link);
        let pl = [("path", l.as_str()), ("link", lk.as_str())];
        gauge_ll(&mut o, "nya_path_rtt_us", "path fast RTT", &pl, pth.rtt_us);
        gauge_ll(
            &mut o,
            "nya_path_stable_rtt_us",
            "path stable RTT",
            &pl,
            pth.stable_rtt_us,
        );
        gauge_ll(
            &mut o,
            "nya_path_class_rtt_us",
            "path class RTT",
            &pl,
            pth.class_rtt_us,
        );
        gauge_ll(
            &mut o,
            "nya_path_inflight_bytes",
            "path inflight",
            &pl,
            pth.inflight,
        );
        gauge_ll(&mut o, "nya_path_sticky", "sticky streams", &pl, pth.sticky);
        gauge_ll(
            &mut o,
            "nya_path_alive",
            "path alive",
            &pl,
            u64::from(pth.alive),
        );
        gauge_ll(
            &mut o,
            "nya_path_state",
            "1=up 2=deg",
            &pl,
            u64::from(pth.state),
        );
        gauge_ll(
            &mut o,
            "nya_path_congested",
            "path send-blocked",
            &pl,
            u64::from(pth.congested),
        );
        gauge_ll(
            &mut o,
            "nya_path_last_rx_ago_us",
            "us since last rx",
            &pl,
            pth.last_rx_ago_us,
        );
        gauge_ll(
            &mut o,
            "nya_path_last_tx_ago_us",
            "us since last tx",
            &pl,
            pth.last_tx_ago_us,
        );
        gauge_ll(
            &mut o,
            "nya_path_pending_ping",
            "in-flight pings",
            &pl,
            pth.pending_ping,
        );
        gauge_ll(
            &mut o,
            "nya_path_queued_urgent",
            "urgent writer queue",
            &pl,
            pth.queued_urgent,
        );
        gauge_ll(
            &mut o,
            "nya_path_queued_bulk",
            "bulk writer queue",
            &pl,
            pth.queued_bulk,
        );
        gauge_ll(
            &mut o,
            "nya_path_rtt_known",
            "1 if RTT sampled",
            &pl,
            u64::from(pth.rtt_known),
        );
    }
    o
}

fn counter(o: &mut String, name: &str, help: &str, v: u64) {
    o.push_str("# HELP ");
    o.push_str(name);
    o.push(' ');
    o.push_str(help);
    o.push('\n');
    o.push_str("# TYPE ");
    o.push_str(name);
    o.push_str(" counter\n");
    o.push_str(name);
    o.push(' ');
    o.push_str(&v.to_string());
    o.push('\n');
}

fn gauge(o: &mut String, name: &str, help: &str, v: u64) {
    o.push_str("# HELP ");
    o.push_str(name);
    o.push(' ');
    o.push_str(help);
    o.push('\n');
    o.push_str("# TYPE ");
    o.push_str(name);
    o.push_str(" gauge\n");
    o.push_str(name);
    o.push(' ');
    o.push_str(&v.to_string());
    o.push('\n');
}

fn gauge_l(o: &mut String, name: &str, help: &str, key: &str, val: &str, v: u64) {
    gauge_ll(o, name, help, &[(key, val)], v);
}

fn gauge_ll(o: &mut String, name: &str, help: &str, labels: &[(&str, &str)], v: u64) {
    if !o.contains(&format!("# TYPE {name} ")) {
        o.push_str("# HELP ");
        o.push_str(name);
        o.push(' ');
        o.push_str(help);
        o.push('\n');
        o.push_str("# TYPE ");
        o.push_str(name);
        o.push_str(" gauge\n");
    }
    o.push_str(name);
    o.push('{');
    for (i, (k, val)) in labels.iter().enumerate() {
        if i > 0 {
            o.push(',');
        }
        o.push_str(k);
        o.push_str("=\"");
        o.push_str(val);
        o.push('"');
    }
    o.push_str("} ");
    o.push_str(&v.to_string());
    o.push('\n');
}

fn hist(o: &mut String, name: &str, help: &str, bounds: &[u64], snap: &crate::metrics::HistSnap) {
    o.push_str("# HELP ");
    o.push_str(name);
    o.push(' ');
    o.push_str(help);
    o.push('\n');
    o.push_str("# TYPE ");
    o.push_str(name);
    o.push_str(" histogram\n");
    let mut cum = 0u64;
    for (i, &le) in bounds.iter().enumerate() {
        cum += snap.buckets.get(i).copied().unwrap_or(0);
        o.push_str(name);
        o.push_str("_bucket{le=\"");
        o.push_str(&le.to_string());
        o.push_str("\"} ");
        o.push_str(&cum.to_string());
        o.push('\n');
    }
    if snap.buckets.len() > bounds.len() {
        cum += snap.buckets[bounds.len()];
    }
    o.push_str(name);
    o.push_str("_bucket{le=\"+Inf\"} ");
    o.push_str(&cum.to_string());
    o.push('\n');
    o.push_str(name);
    o.push_str("_sum ");
    o.push_str(&snap.sum.to_string());
    o.push('\n');
    o.push_str(name);
    o.push_str("_count ");
    o.push_str(&snap.count.to_string());
    o.push('\n');
}

fn prometheus_label(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{HistSnap, Snapshot};

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
        assert!(body.contains("# TYPE nya_path_added_total counter"));
    }
}
