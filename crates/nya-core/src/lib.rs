//! Multi-path TCP+TLS overlay (sticky-per-stream, failover / failback).
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
mod cfg;
mod handshake;
mod health;
mod metrics;
mod path;
mod scheduler;
mod session;
mod stream;
pub mod tls;
mod tuning;

pub use cfg::{SessionConfig, SessionOpts};
pub use handshake::{
    client_create_session, client_join_session, server_accept_handshake, HandshakeError,
    HandshakeResult,
};
pub use metrics::{PathSnap, Snapshot as SessionSnapshot};
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
