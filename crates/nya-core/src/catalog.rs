//! Single metric catalog. Prometheus text, stderr snapshot, and OTLP all
//! consume [`visit_metrics`]. Names live only here.

use std::collections::BTreeSet;

use crate::metrics::{
    percentile, HistSnap, ProcessSnapshot, FAILOVER_MS_BOUNDS, LIFETIME_MS_BOUNDS, STALL_MS_BOUNDS,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InstrumentKind {
    Counter,
    Gauge,
    Histogram,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetricDesc {
    pub name: &'static str,
    pub help: &'static str,
    pub kind: InstrumentKind,
    pub label_keys: Vec<&'static str>,
}

/// Static descriptors (unlabeled + hist + path/link names). Independent of live series.
pub fn metric_descriptors() -> Vec<MetricDesc> {
    let mut out = Vec::new();
    struct FirstSeen<'a>(&'a mut Vec<MetricDesc>);
    impl MetricSink for FirstSeen<'_> {
        fn counter(&mut self, name: &'static str, help: &'static str, _value: u64) {
            if self.0.iter().any(|d| d.name == name) {
                return;
            }
            self.0.push(MetricDesc {
                name,
                help,
                kind: InstrumentKind::Counter,
                label_keys: Vec::new(),
            });
        }
        fn gauge(
            &mut self,
            name: &'static str,
            help: &'static str,
            labels: &[(&'static str, &str)],
            _value: u64,
        ) {
            if self.0.iter().any(|d| d.name == name) {
                return;
            }
            self.0.push(MetricDesc {
                name,
                help,
                kind: InstrumentKind::Gauge,
                label_keys: labels.iter().map(|(k, _)| *k).collect(),
            });
        }
        fn histogram(
            &mut self,
            name: &'static str,
            help: &'static str,
            _bounds: &'static [u64],
            _snap: &HistSnap,
        ) {
            if self.0.iter().any(|d| d.name == name) {
                return;
            }
            self.0.push(MetricDesc {
                name,
                help,
                kind: InstrumentKind::Histogram,
                label_keys: Vec::new(),
            });
        }
    }
    let mut ps = ProcessSnapshot::default();
    ps.session.failover_ms = HistSnap::zeroed(FAILOVER_MS_BOUNDS);
    ps.session.stall_ms = HistSnap::zeroed(STALL_MS_BOUNDS);
    ps.session.stream_lifetime_ms = HistSnap::zeroed(LIFETIME_MS_BOUNDS);
    ps.session.links.push(crate::metrics::LinkSnap {
        name: "_".into(),
        ..Default::default()
    });
    ps.session.paths.push(crate::metrics::PathSnap {
        name: "_".into(),
        link: "_".into(),
        ..Default::default()
    });
    visit_metrics(&ps, &mut FirstSeen(&mut out));
    out
}

pub trait MetricSink {
    fn counter(&mut self, name: &'static str, help: &'static str, value: u64);
    fn gauge(
        &mut self,
        name: &'static str,
        help: &'static str,
        labels: &[(&'static str, &str)],
        value: u64,
    );
    fn histogram(
        &mut self,
        name: &'static str,
        help: &'static str,
        bounds: &'static [u64],
        snap: &HistSnap,
    );
}

/// Walk `ps`. Name order is part of the catalog contract.
pub fn visit_metrics(ps: &ProcessSnapshot, sink: &mut impl MetricSink) {
    let s = &ps.session;
    let p = &ps.process;
    sink.counter("nya_path_added_total", "paths added", s.path_added);
    sink.counter("nya_path_down_total", "paths marked down", s.path_down);
    sink.counter(
        "nya_path_degraded_total",
        "paths marked degraded",
        s.path_degraded,
    );
    sink.counter(
        "nya_path_outlier_recycle_total",
        "same-link outlier TCP recycles",
        s.path_outlier_recycle,
    );
    sink.counter(
        "nya_correlated_silence_total",
        "correlated-silence episodes",
        s.correlated_silence,
    );
    sink.counter("nya_migrates_total", "stream resticks", s.migrates);
    sink.counter(
        "nya_migrates_speculative_total",
        "speculative resticks",
        s.migrates_speculative,
    );
    sink.counter(
        "nya_migrates_path_down_total",
        "resticks after path down",
        s.migrates_path_down,
    );
    sink.counter(
        "nya_migrates_ensure_sticky_total",
        "ensure_sticky resticks",
        s.migrates_ensure_sticky,
    );
    sink.counter(
        "nya_migrates_send_blocked_total",
        "send-blocked resticks",
        s.migrates_send_blocked,
    );
    sink.counter(
        "nya_data_retransmit_total",
        "STREAM_DATA retransmits",
        s.data_retransmit,
    );
    sink.counter(
        "nya_data_hedge_total",
        "STREAM_DATA hedge copies",
        s.data_hedge,
    );
    sink.counter(
        "nya_close_retry_total",
        "STREAM_CLOSE rehomes onto another path",
        s.close_retry,
    );
    sink.counter(
        "nya_probe_miss_total",
        "pings expired without pong",
        s.probe_miss,
    );
    sink.counter(
        "nya_window_blocks_total",
        "times send waited on stream window",
        s.window_blocks,
    );
    sink.counter(
        "nya_picks_unknown_rtt_total",
        "new streams picked onto unknown-RTT path",
        s.picks_unknown_rtt,
    );
    sink.counter(
        "nya_picks_unknown_over_known_total",
        "unknown-RTT pick while a sampled path existed",
        s.picks_unknown_over_known,
    );
    sink.counter("nya_failbacks_total", "cross-link failbacks", s.failbacks);
    sink.counter(
        "nya_failbacks_upgrade_total",
        "cross-link upgrade failbacks",
        s.failbacks_upgrade,
    );
    sink.counter(
        "nya_failbacks_class_empty_total",
        "cross-link class-empty failbacks",
        s.failbacks_class_empty,
    );
    sink.counter(
        "nya_failbacks_same_link_total",
        "same-link failbacks",
        s.failbacks_same_link,
    );
    sink.counter(
        "nya_hol_rebalances_total",
        "HOL rebalances",
        s.hol_rebalances,
    );
    sink.counter(
        "nya_streams_opened_total",
        "streams opened",
        s.streams_opened,
    );
    sink.counter(
        "nya_streams_closed_total",
        "streams closed gracefully",
        s.streams_closed,
    );
    sink.counter(
        "nya_stream_reaps_linger_total",
        "half-close linger reaps; not a TTFB timeout",
        s.stream_reaps_linger,
    );
    sink.counter("nya_stream_resets_total", "streams reset", s.stream_resets);
    sink.counter(
        "nya_stream_resets_dial_failed_total",
        "resets: dial failed",
        s.stream_resets_dial_failed,
    );
    sink.counter(
        "nya_stream_resets_timeout_total",
        "resets: timeout",
        s.stream_resets_timeout,
    );
    sink.counter(
        "nya_stream_resets_peer_total",
        "resets: peer",
        s.stream_resets_peer,
    );
    sink.counter(
        "nya_stream_resets_session_dead_total",
        "resets: session dead",
        s.stream_resets_session_dead,
    );
    sink.counter(
        "nya_stream_resets_protocol_total",
        "resets: protocol",
        s.stream_resets_protocol,
    );
    sink.counter(
        "nya_bytes_data_tx_total",
        "overlay StreamData payload tx",
        s.bytes_data_tx,
    );
    sink.counter(
        "nya_bytes_data_rx_total",
        "overlay StreamData payload rx",
        s.bytes_data_rx,
    );
    sink.counter(
        "nya_bytes_ctrl_tx_total",
        "overlay control bytes tx",
        s.bytes_ctrl_tx,
    );
    sink.counter(
        "nya_bytes_ctrl_rx_total",
        "overlay control bytes rx",
        s.bytes_ctrl_rx,
    );
    sink.counter(
        "nya_frame_send_drop_total",
        "frames dropped (queue full)",
        s.frame_send_drop,
    );
    sink.counter(
        "nya_session_all_down_resets_total",
        "all-down session resets",
        s.session_all_down_resets,
    );
    sink.counter(
        "nya_handshake_create_ok_total",
        "create-session ok",
        p.handshake_create_ok,
    );
    sink.counter(
        "nya_handshake_join_ok_total",
        "join-session ok",
        p.handshake_join_ok,
    );
    sink.counter(
        "nya_handshake_fail_auth_total",
        "handshake auth fail",
        p.handshake_fail_auth,
    );
    sink.counter(
        "nya_handshake_fail_version_total",
        "handshake version fail",
        p.handshake_fail_version,
    );
    sink.counter(
        "nya_handshake_fail_unknown_total",
        "handshake unknown session",
        p.handshake_fail_unknown,
    );
    sink.counter(
        "nya_handshake_fail_other_total",
        "handshake other fail",
        p.handshake_fail_other,
    );
    sink.counter(
        "nya_inbound_accept_total",
        "inbound accepts",
        p.inbound_accept,
    );
    sink.counter(
        "nya_inbound_reject_total",
        "inbound rejects",
        p.inbound_reject,
    );
    sink.counter(
        "nya_inbound_open_fail_total",
        "inbound open_stream fail",
        p.inbound_open_fail,
    );
    sink.counter(
        "nya_outbound_dial_ok_total",
        "outbound dial ok",
        p.outbound_dial_ok,
    );
    sink.counter(
        "nya_outbound_dial_fail_total",
        "outbound dial fail",
        p.outbound_dial_fail,
    );
    sink.counter(
        "nya_reconnect_ok_total",
        "path up (incl first)",
        p.reconnect_ok,
    );
    sink.counter(
        "nya_reconnect_fail_total",
        "link connect fail",
        p.reconnect_fail,
    );
    sink.counter(
        "nya_sessions_created_total",
        "sessions created",
        p.sessions_created,
    );
    sink.counter("nya_sessions_dead_total", "sessions dead", p.sessions_dead);

    sink.gauge(
        "nya_streams_stalled",
        "streams currently stalled",
        &[],
        s.streams_stalled,
    );
    sink.gauge("nya_streams_live", "live streams", &[], s.streams_live);
    sink.gauge(
        "nya_streams_held",
        "streams still in the session table (incl. unreaped closes)",
        &[],
        s.streams_held,
    );
    sink.gauge("nya_sessions_live", "live sessions", &[], p.sessions_live);

    sink.histogram(
        "nya_failover_ms",
        "overlay path-silence to restick/down, milliseconds",
        FAILOVER_MS_BOUNDS,
        &s.failover_ms,
    );
    sink.histogram(
        "nya_stall_ms",
        "send-unacked or recv-hole stall duration, milliseconds",
        STALL_MS_BOUNDS,
        &s.stall_ms,
    );
    sink.histogram(
        "nya_stream_lifetime_ms",
        "stream lifetime, milliseconds",
        LIFETIME_MS_BOUNDS,
        &s.stream_lifetime_ms,
    );

    for ln in &s.links {
        let l = ln.name.as_str();
        let lab = [("link", l)];
        sink.gauge("nya_link_conns", "TCP connections on link", &lab, ln.conns);
        sink.gauge("nya_link_up", "UP connections on link", &lab, ln.up);
        sink.gauge(
            "nya_link_degraded",
            "DEGRADED connections on link",
            &lab,
            ln.degraded,
        );
        sink.gauge("nya_link_rtt_us", "best known RTT on link", &lab, ln.rtt_us);
        sink.gauge(
            "nya_link_rtt_max_us",
            "worst known RTT on link",
            &lab,
            ln.rtt_max_us,
        );
        sink.gauge(
            "nya_link_inflight_bytes",
            "inflight on link",
            &lab,
            ln.inflight,
        );
        sink.gauge("nya_link_sticky", "sticky streams on link", &lab, ln.sticky);
        sink.gauge(
            "nya_link_congested",
            "congested connections on link",
            &lab,
            ln.congested,
        );
        sink.gauge(
            "nya_link_rx_fresh_us",
            "freshest last-rx on link",
            &lab,
            ln.rx_fresh_us,
        );
        sink.gauge(
            "nya_link_rx_stale_us",
            "stalest last-rx on link",
            &lab,
            ln.rx_stale_us,
        );
        sink.gauge(
            "nya_link_queued_urgent",
            "urgent queue on link",
            &lab,
            ln.queued_urgent,
        );
        sink.gauge(
            "nya_link_queued_bulk",
            "bulk queue on link",
            &lab,
            ln.queued_bulk,
        );
    }

    for pth in &s.paths {
        let lab = [("path", pth.name.as_str()), ("link", pth.link.as_str())];
        sink.gauge("nya_path_rtt_us", "path fast RTT", &lab, pth.rtt_us);
        sink.gauge(
            "nya_path_stable_rtt_us",
            "path stable RTT",
            &lab,
            pth.stable_rtt_us,
        );
        sink.gauge(
            "nya_path_class_rtt_us",
            "path class RTT",
            &lab,
            pth.class_rtt_us,
        );
        sink.gauge(
            "nya_path_inflight_bytes",
            "path inflight",
            &lab,
            pth.inflight,
        );
        sink.gauge("nya_path_sticky", "sticky streams", &lab, pth.sticky);
        sink.gauge("nya_path_alive", "path alive", &lab, u64::from(pth.alive));
        sink.gauge("nya_path_state", "1=up 2=deg", &lab, u64::from(pth.state));
        sink.gauge(
            "nya_path_congested",
            "path send-blocked",
            &lab,
            u64::from(pth.congested),
        );
        sink.gauge(
            "nya_path_last_rx_ago_us",
            "us since last rx",
            &lab,
            pth.last_rx_ago_us,
        );
        sink.gauge(
            "nya_path_last_tx_ago_us",
            "us since last tx",
            &lab,
            pth.last_tx_ago_us,
        );
        sink.gauge(
            "nya_path_pending_ping",
            "in-flight pings",
            &lab,
            pth.pending_ping,
        );
        sink.gauge(
            "nya_path_queued_urgent",
            "urgent writer queue",
            &lab,
            pth.queued_urgent,
        );
        sink.gauge(
            "nya_path_queued_bulk",
            "bulk writer queue",
            &lab,
            pth.queued_bulk,
        );
        sink.gauge(
            "nya_path_rtt_known",
            "1 if RTT sampled",
            &lab,
            u64::from(pth.rtt_known),
        );
    }
}

/// Static instrument names, including histogram `_bucket`/`_sum`/`_count`.
/// Path/link names are always listed even when the snapshot has none.
pub fn prometheus_metric_names(_ps: &ProcessSnapshot) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    struct Names<'a>(&'a mut BTreeSet<String>);
    impl MetricSink for Names<'_> {
        fn counter(&mut self, name: &'static str, _help: &'static str, _value: u64) {
            self.0.insert(name.to_string());
        }
        fn gauge(
            &mut self,
            name: &'static str,
            _help: &'static str,
            _labels: &[(&'static str, &str)],
            _value: u64,
        ) {
            self.0.insert(name.to_string());
        }
        fn histogram(
            &mut self,
            name: &'static str,
            _help: &'static str,
            _bounds: &'static [u64],
            _snap: &HistSnap,
        ) {
            self.0.insert(format!("{name}_bucket"));
            self.0.insert(format!("{name}_sum"));
            self.0.insert(format!("{name}_count"));
        }
    }
    // Empty snapshot still emits unlabeled + hists; inject dummy path/link so
    // labeled names are part of the static set.
    let mut ps = ProcessSnapshot::default();
    ps.session.failover_ms = HistSnap::zeroed(FAILOVER_MS_BOUNDS);
    ps.session.stall_ms = HistSnap::zeroed(STALL_MS_BOUNDS);
    ps.session.stream_lifetime_ms = HistSnap::zeroed(LIFETIME_MS_BOUNDS);
    ps.session.links.push(crate::metrics::LinkSnap {
        name: "_".into(),
        ..Default::default()
    });
    ps.session.paths.push(crate::metrics::PathSnap {
        name: "_".into(),
        link: "_".into(),
        ..Default::default()
    });
    visit_metrics(&ps, &mut Names(&mut names));
    names
}

pub fn render_prometheus(ps: &ProcessSnapshot) -> String {
    let mut sink = PrometheusTextSink {
        o: String::with_capacity(4096),
    };
    visit_metrics(ps, &mut sink);
    sink.o
}

struct PrometheusTextSink {
    o: String,
}

impl MetricSink for PrometheusTextSink {
    fn counter(&mut self, name: &'static str, help: &'static str, value: u64) {
        write_help_type(&mut self.o, name, help, "counter");
        self.o.push_str(name);
        self.o.push(' ');
        self.o.push_str(&value.to_string());
        self.o.push('\n');
    }

    fn gauge(
        &mut self,
        name: &'static str,
        help: &'static str,
        labels: &[(&'static str, &str)],
        value: u64,
    ) {
        if !self.o.contains(&format!("# TYPE {name} ")) {
            write_help_type(&mut self.o, name, help, "gauge");
        }
        self.o.push_str(name);
        if !labels.is_empty() {
            self.o.push('{');
            for (i, (k, val)) in labels.iter().enumerate() {
                if i > 0 {
                    self.o.push(',');
                }
                self.o.push_str(k);
                self.o.push_str("=\"");
                self.o.push_str(&prometheus_label(val));
                self.o.push('"');
            }
            self.o.push('}');
        }
        self.o.push(' ');
        self.o.push_str(&value.to_string());
        self.o.push('\n');
    }

    fn histogram(
        &mut self,
        name: &'static str,
        help: &'static str,
        bounds: &'static [u64],
        snap: &HistSnap,
    ) {
        write_help_type(&mut self.o, name, help, "histogram");
        let mut cum = 0u64;
        for (i, &le) in bounds.iter().enumerate() {
            cum += snap.buckets.get(i).copied().unwrap_or(0);
            self.o.push_str(name);
            self.o.push_str("_bucket{le=\"");
            self.o.push_str(&le.to_string());
            self.o.push_str("\"} ");
            self.o.push_str(&cum.to_string());
            self.o.push('\n');
        }
        if snap.buckets.len() > bounds.len() {
            cum += snap.buckets[bounds.len()];
        }
        self.o.push_str(name);
        self.o.push_str("_bucket{le=\"+Inf\"} ");
        self.o.push_str(&cum.to_string());
        self.o.push('\n');
        self.o.push_str(name);
        self.o.push_str("_sum ");
        self.o.push_str(&snap.sum.to_string());
        self.o.push('\n');
        self.o.push_str(name);
        self.o.push_str("_count ");
        self.o.push_str(&snap.count.to_string());
        self.o.push('\n');
    }
}

fn write_help_type(o: &mut String, name: &str, help: &str, ty: &str) {
    o.push_str("# HELP ");
    o.push_str(name);
    o.push(' ');
    o.push_str(help);
    o.push('\n');
    o.push_str("# TYPE ");
    o.push_str(name);
    o.push(' ');
    o.push_str(ty);
    o.push('\n');
}

fn prometheus_label(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

pub fn format_snapshot_metrics(ps: &ProcessSnapshot) -> String {
    let mut sink = SnapshotKv(String::new());
    visit_metrics(ps, &mut sink);
    sink.0
}

struct SnapshotKv(String);

impl MetricSink for SnapshotKv {
    fn counter(&mut self, name: &'static str, _help: &'static str, value: u64) {
        self.push_kv(name, None, value);
    }
    fn gauge(
        &mut self,
        name: &'static str,
        _help: &'static str,
        labels: &[(&'static str, &str)],
        value: u64,
    ) {
        self.push_kv(name, Some(labels), value);
    }
    fn histogram(
        &mut self,
        name: &'static str,
        _help: &'static str,
        bounds: &'static [u64],
        snap: &HistSnap,
    ) {
        let mut cum = 0u64;
        for (i, &le) in bounds.iter().enumerate() {
            cum += snap.buckets.get(i).copied().unwrap_or(0);
            let le_s = le.to_string();
            self.push_kv(
                &format!("{name}_bucket"),
                Some(&[("le", le_s.as_str())]),
                cum,
            );
        }
        if snap.buckets.len() > bounds.len() {
            cum += snap.buckets[bounds.len()];
        }
        self.push_kv(&format!("{name}_bucket"), Some(&[("le", "+Inf")]), cum);
        self.push_kv(&format!("{name}_sum"), None, snap.sum);
        self.push_kv(&format!("{name}_count"), None, snap.count);
    }
}

impl SnapshotKv {
    fn push_kv(&mut self, name: &str, labels: Option<&[(&str, &str)]>, value: u64) {
        if !self.0.is_empty() {
            self.0.push(' ');
        }
        self.0.push_str(name);
        if let Some(labels) = labels {
            if !labels.is_empty() {
                self.0.push('{');
                for (i, (k, v)) in labels.iter().enumerate() {
                    if i > 0 {
                        self.0.push(',');
                    }
                    self.0.push_str(k);
                    self.0.push_str("=\"");
                    self.0.push_str(v);
                    self.0.push('"');
                }
                self.0.push('}');
            }
        }
        self.0.push('=');
        self.0.push_str(&value.to_string());
    }
}

pub fn snapshot_p99(ps: &ProcessSnapshot) -> (Option<u64>, Option<u64>) {
    (
        percentile(&ps.session.stall_ms, STALL_MS_BOUNDS, 99.0),
        percentile(&ps.session.failover_ms, FAILOVER_MS_BOUNDS, 99.0),
    )
}
