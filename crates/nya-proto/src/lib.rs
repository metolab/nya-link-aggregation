//! Wire protocol for nya-link-aggregation.
//!
//! Every message on a path is a length-prefixed frame:
//! `u32be length || payload`, where `payload` is `u8 type || body`.
//! `length` is the size of `payload` (type + body). Max payload is 16 KiB.
#![forbid(unsafe_code)]

mod codec;
mod frame;

pub use codec::{read_frame, write_frame, MAX_FRAME_SIZE};
pub use frame::{
    CreateSession, CreateSessionOk, Frame, HandshakeErr, JoinSession, JoinSessionOk, Ping, Pong,
    ProtoError, ResetReason, StreamAck, StreamClose, StreamData, StreamOpen, StreamReset, Target,
};

pub const PROTOCOL_VERSION: u8 = 1;
pub const ALPN: &[u8] = b"nya/1";
pub const TLS_EXPORTER_LABEL: &str = "nya-link-aggregation";
pub const SESSION_ID_LEN: usize = 16;
pub const NONCE_LEN: usize = 32;
pub const PROOF_LEN: usize = 32;
pub const MAX_STREAM_PAYLOAD: usize = 16 * 1024 - 16;
