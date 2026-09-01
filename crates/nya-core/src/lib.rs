//! Multi-path TCP+TLS overlay (path-agnostic offsets, RTT-scaled retry).
//!
//! Data path:
//! ```text
//! inbound  →  session::streams  →  scheduler::pick_path  →  PathState
//!                 │                                           │
//!                 └──── session::steer (migrate / failback)   │
//!                                                             ▼
//!                                              urgent / bulk writer
//!                                                             ▼
//!                                                   TLS framed IO
//! ```
//!
//! * [`health`] — RTT-adaptive loss / down / failback thresholds
//! * [`scheduler`] — path pick, backup, same-link rebalance
//! * [`path`] — per-connection RTT, inflight, dual writer queues
//! * [`session`] — multiplexed streams + steering
//! * [`Tuning`] — hidden implementation knobs (not TOML)
#![forbid(unsafe_code)]

mod auth;
mod catalog;
mod cfg;
mod export;
mod handshake;
mod health;
mod hop;
mod metrics;
mod path;
mod scheduler;
mod session;
mod stream;
pub mod tls;
mod tuning;

pub use catalog::{
    metric_descriptors, prometheus_metric_names, render_prometheus, visit_metrics, InstrumentKind,
    MetricDesc, MetricSink,
};
pub use cfg::{ObsOpts, OtelOpts, OtelProtocol, OtelSignalOpts, SessionConfig, SessionOpts};
pub use export::{parse_metrics_listen, spawn_obs_session, spawn_obs_table};
pub use handshake::{
    client_create_session, client_join_session, server_accept_handshake, HandshakeError,
    HandshakeResult,
};
pub use hop::{
    connect_origin, connect_origin_meta, interleave_families, io_err_kind, race_origin_addrs,
    race_origin_connects, race_origin_lookups, session_fp_hex, HopClock, HopOutcome, HopProbe,
    HopRole, HopSample, OriginDial, OriginDialMeta, OriginPeerSlots,
};
pub use metrics::{
    percentile, rollup_links, HistSnap, Histogram, LinkSnap, PathSnap, ProcessCounters,
    ProcessSnapshot, Snapshot as SessionSnapshot, FAILOVER_MS_BOUNDS, LIFETIME_MS_BOUNDS,
    STALL_MS_BOUNDS,
};
pub use session::{IncomingStream, Session, SessionError, SessionTable};
pub use stream::TunnelStream;
pub use tls::{
    client_tls_config, connect_pinned, export_from_client, export_from_server,
    export_keying_material, install_crypto, load_server_config, parse_pin_hex, spki_sha256,
    spki_sha256_from_pem, PinnedSpkiVerifier,
};
pub use tuning::Tuning;

use nya_proto::Target;

pub type StreamTarget = Target;
