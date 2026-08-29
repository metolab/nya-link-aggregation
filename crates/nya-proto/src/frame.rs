use std::fmt;
use std::io;

use crate::{NONCE_LEN, PROOF_LEN, SESSION_ID_LEN};

#[derive(Debug, thiserror::Error)]
pub enum ProtoError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("frame length {0} out of range")]
    BadLength(usize),
    #[error("truncated frame")]
    Truncated,
    #[error("unknown frame type {0}")]
    UnknownType(u8),
    #[error("invalid field: {0}")]
    Invalid(&'static str),
    #[error("unsupported protocol version {0}")]
    Version(u8),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    pub host: String,
    pub port: u16,
}

impl fmt::Display for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(f, "[{}]:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

impl Target {
    pub fn parse(s: &str) -> Result<Self, ProtoError> {
        if let Some(rest) = s.strip_prefix('[') {
            let (host, port) = rest
                .rsplit_once("]:")
                .ok_or(ProtoError::Invalid("target"))?;
            let port: u16 = port.parse().map_err(|_| ProtoError::Invalid("port"))?;
            return Ok(Target {
                host: host.to_string(),
                port,
            });
        }
        let (host, port) = s.rsplit_once(':').ok_or(ProtoError::Invalid("target"))?;
        let port: u16 = port.parse().map_err(|_| ProtoError::Invalid("port"))?;
        Ok(Target {
            host: host.to_string(),
            port,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResetReason {
    Unknown = 0,
    DialFailed = 1,
    Timeout = 2,
    PeerReset = 3,
    SessionDead = 4,
    Protocol = 5,
}

impl ResetReason {
    fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::DialFailed,
            2 => Self::Timeout,
            3 => Self::PeerReset,
            4 => Self::SessionDead,
            5 => Self::Protocol,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ping {
    pub seq: u64,
    pub sent_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pong {
    pub seq: u64,
    pub sent_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateSession {
    pub version: u8,
    pub user_id: String,
    pub nonce: [u8; NONCE_LEN],
    pub proof: [u8; PROOF_LEN],
    /// Optional. Empty when an old peer omitted the trailing field.
    pub path_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateSessionOk {
    pub session_id: [u8; SESSION_ID_LEN],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinSession {
    pub session_id: [u8; SESSION_ID_LEN],
    pub path_name: String,
    pub proof: [u8; PROOF_LEN],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinSessionOk;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandshakeErr {
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamOpen {
    pub stream_id: u32,
    pub target: Target,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamData {
    pub stream_id: u32,
    pub offset: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamAck {
    pub stream_id: u32,
    pub acked_offset: u64,
    pub window: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamClose {
    pub stream_id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamReset {
    pub stream_id: u32,
    pub reason: ResetReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    Ping(Ping),
    Pong(Pong),
    CreateSession(CreateSession),
    CreateSessionOk(CreateSessionOk),
    JoinSession(JoinSession),
    JoinSessionOk(JoinSessionOk),
    HandshakeErr(HandshakeErr),
    StreamOpen(StreamOpen),
    StreamData(StreamData),
    StreamAck(StreamAck),
    StreamClose(StreamClose),
    StreamReset(StreamReset),
    SessionClose,
}

const T_PING: u8 = 0x01;
const T_PONG: u8 = 0x02;
const T_CREATE: u8 = 0x03;
const T_CREATE_OK: u8 = 0x04;
const T_JOIN: u8 = 0x05;
const T_JOIN_OK: u8 = 0x06;
const T_HS_ERR: u8 = 0x07;
const T_OPEN: u8 = 0x08;
const T_DATA: u8 = 0x09;
const T_ACK: u8 = 0x0a;
const T_CLOSE: u8 = 0x0b;
const T_RESET: u8 = 0x0c;
const T_SESS_CLOSE: u8 = 0x0e;

impl Frame {
    pub fn encode(&self) -> Vec<u8> {
        let mut o = Vec::new();
        match self {
            Frame::Ping(p) => {
                o.push(T_PING);
                o.extend_from_slice(&p.seq.to_be_bytes());
                o.extend_from_slice(&p.sent_at_ms.to_be_bytes());
            }
            Frame::Pong(p) => {
                o.push(T_PONG);
                o.extend_from_slice(&p.seq.to_be_bytes());
                o.extend_from_slice(&p.sent_at_ms.to_be_bytes());
            }
            Frame::CreateSession(c) => {
                o.push(T_CREATE);
                o.push(c.version);
                put_str(&mut o, &c.user_id);
                o.extend_from_slice(&c.nonce);
                o.extend_from_slice(&c.proof);
                put_str(&mut o, &c.path_name);
            }
            Frame::CreateSessionOk(c) => {
                o.push(T_CREATE_OK);
                o.extend_from_slice(&c.session_id);
            }
            Frame::JoinSession(j) => {
                o.push(T_JOIN);
                o.extend_from_slice(&j.session_id);
                put_str(&mut o, &j.path_name);
                o.extend_from_slice(&j.proof);
            }
            Frame::JoinSessionOk(_) => o.push(T_JOIN_OK),
            Frame::HandshakeErr(e) => {
                o.push(T_HS_ERR);
                put_str(&mut o, &e.message);
            }
            Frame::StreamOpen(s) => {
                o.push(T_OPEN);
                o.extend_from_slice(&s.stream_id.to_be_bytes());
                o.extend_from_slice(&s.target.port.to_be_bytes());
                put_str(&mut o, &s.target.host);
            }
            Frame::StreamData(s) => {
                o.push(T_DATA);
                o.extend_from_slice(&s.stream_id.to_be_bytes());
                o.extend_from_slice(&s.offset.to_be_bytes());
                o.extend_from_slice(&s.data);
            }
            Frame::StreamAck(s) => {
                o.push(T_ACK);
                o.extend_from_slice(&s.stream_id.to_be_bytes());
                o.extend_from_slice(&s.acked_offset.to_be_bytes());
                o.extend_from_slice(&s.window.to_be_bytes());
            }
            Frame::StreamClose(s) => {
                o.push(T_CLOSE);
                o.extend_from_slice(&s.stream_id.to_be_bytes());
            }
            Frame::StreamReset(s) => {
                o.push(T_RESET);
                o.extend_from_slice(&s.stream_id.to_be_bytes());
                o.push(s.reason as u8);
            }
            Frame::SessionClose => o.push(T_SESS_CLOSE),
        }
        o
    }

    pub fn decode(buf: &[u8]) -> Result<Self, ProtoError> {
        if buf.is_empty() {
            return Err(ProtoError::Truncated);
        }
        let typ = buf[0];
        let mut p = Parser { buf, off: 1 };
        let frame = match typ {
            T_PING => Frame::Ping(Ping {
                seq: p.u64()?,
                sent_at_ms: p.u64()?,
            }),
            T_PONG => Frame::Pong(Pong {
                seq: p.u64()?,
                sent_at_ms: p.u64()?,
            }),
            T_CREATE => {
                let version = p.u8()?;
                let user_id = p.str()?;
                let mut nonce = [0u8; NONCE_LEN];
                nonce.copy_from_slice(p.bytes(NONCE_LEN)?);
                let mut proof = [0u8; PROOF_LEN];
                proof.copy_from_slice(p.bytes(PROOF_LEN)?);
                let path_name = if p.rest().is_empty() {
                    String::new()
                } else {
                    p.str()?
                };
                Frame::CreateSession(CreateSession {
                    version,
                    user_id,
                    nonce,
                    proof,
                    path_name,
                })
            }
            T_CREATE_OK => {
                let mut session_id = [0u8; SESSION_ID_LEN];
                session_id.copy_from_slice(p.bytes(SESSION_ID_LEN)?);
                Frame::CreateSessionOk(CreateSessionOk { session_id })
            }
            T_JOIN => {
                let mut session_id = [0u8; SESSION_ID_LEN];
                session_id.copy_from_slice(p.bytes(SESSION_ID_LEN)?);
                let path_name = p.str()?;
                let mut proof = [0u8; PROOF_LEN];
                proof.copy_from_slice(p.bytes(PROOF_LEN)?);
                Frame::JoinSession(JoinSession {
                    session_id,
                    path_name,
                    proof,
                })
            }
            T_JOIN_OK => Frame::JoinSessionOk(JoinSessionOk),
            T_HS_ERR => Frame::HandshakeErr(HandshakeErr { message: p.str()? }),
            T_OPEN => {
                let stream_id = p.u32()?;
                let port = p.u16()?;
                let host = p.str()?;
                Frame::StreamOpen(StreamOpen {
                    stream_id,
                    target: Target { host, port },
                })
            }
            T_DATA => {
                let stream_id = p.u32()?;
                let offset = p.u64()?;
                let data = p.rest().to_vec();
                Frame::StreamData(StreamData {
                    stream_id,
                    offset,
                    data,
                })
            }
            T_ACK => Frame::StreamAck(StreamAck {
                stream_id: p.u32()?,
                acked_offset: p.u64()?,
                window: p.u32()?,
            }),
            T_CLOSE => Frame::StreamClose(StreamClose {
                stream_id: p.u32()?,
            }),
            T_RESET => {
                let stream_id = p.u32()?;
                let reason = ResetReason::from_u8(p.u8()?);
                Frame::StreamReset(StreamReset { stream_id, reason })
            }
            T_SESS_CLOSE => Frame::SessionClose,
            t => return Err(ProtoError::UnknownType(t)),
        };
        Ok(frame)
    }
}

fn put_str(o: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    let len = u16::try_from(b.len()).unwrap_or(u16::MAX);
    o.extend_from_slice(&len.to_be_bytes());
    o.extend_from_slice(&b[..len as usize]);
}

struct Parser<'a> {
    buf: &'a [u8],
    off: usize,
}

impl<'a> Parser<'a> {
    fn need(&self, n: usize) -> Result<(), ProtoError> {
        if self.off + n > self.buf.len() {
            Err(ProtoError::Truncated)
        } else {
            Ok(())
        }
    }

    fn u8(&mut self) -> Result<u8, ProtoError> {
        self.need(1)?;
        let v = self.buf[self.off];
        self.off += 1;
        Ok(v)
    }

    fn u16(&mut self) -> Result<u16, ProtoError> {
        self.need(2)?;
        let v = u16::from_be_bytes(self.buf[self.off..self.off + 2].try_into().unwrap());
        self.off += 2;
        Ok(v)
    }

    fn u32(&mut self) -> Result<u32, ProtoError> {
        self.need(4)?;
        let v = u32::from_be_bytes(self.buf[self.off..self.off + 4].try_into().unwrap());
        self.off += 4;
        Ok(v)
    }

    fn u64(&mut self) -> Result<u64, ProtoError> {
        self.need(8)?;
        let v = u64::from_be_bytes(self.buf[self.off..self.off + 8].try_into().unwrap());
        self.off += 8;
        Ok(v)
    }

    fn bytes(&mut self, n: usize) -> Result<&'a [u8], ProtoError> {
        self.need(n)?;
        let s = &self.buf[self.off..self.off + n];
        self.off += n;
        Ok(s)
    }

    fn str(&mut self) -> Result<String, ProtoError> {
        let len = self.u16()? as usize;
        let b = self.bytes(len)?;
        String::from_utf8(b.to_vec()).map_err(|_| ProtoError::Invalid("utf8"))
    }

    fn rest(&self) -> &'a [u8] {
        &self.buf[self.off..]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(f: Frame) {
        let enc = f.encode();
        let dec = Frame::decode(&enc).unwrap();
        assert_eq!(f, dec);
    }

    #[test]
    fn frames_roundtrip() {
        roundtrip(Frame::Ping(Ping {
            seq: 7,
            sent_at_ms: 123,
        }));
        roundtrip(Frame::StreamOpen(StreamOpen {
            stream_id: 9,
            target: Target {
                host: "example.com".into(),
                port: 443,
            },
        }));
        roundtrip(Frame::StreamData(StreamData {
            stream_id: 1,
            offset: 4096,
            data: b"hello".to_vec(),
        }));
        roundtrip(Frame::StreamAck(StreamAck {
            stream_id: 1,
            acked_offset: 5,
            window: 128000,
        }));
        roundtrip(Frame::StreamReset(StreamReset {
            stream_id: 3,
            reason: ResetReason::DialFailed,
        }));
        roundtrip(Frame::SessionClose);
        roundtrip(Frame::CreateSession(CreateSession {
            version: crate::PROTOCOL_VERSION,
            user_id: "default".into(),
            nonce: [3u8; NONCE_LEN],
            proof: [9u8; PROOF_LEN],
            path_name: "soy#0".into(),
        }));
    }

    #[test]
    fn create_session_old_bytes_have_empty_path_name() {
        let mut o = Vec::new();
        o.push(T_CREATE);
        o.push(crate::PROTOCOL_VERSION);
        put_str(&mut o, "default");
        o.extend_from_slice(&[3u8; NONCE_LEN]);
        o.extend_from_slice(&[9u8; PROOF_LEN]);
        match Frame::decode(&o).unwrap() {
            Frame::CreateSession(c) => {
                assert_eq!(c.user_id, "default");
                assert!(c.path_name.is_empty());
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn create_session_one_byte_tail_is_truncated() {
        let mut o = Vec::new();
        o.push(T_CREATE);
        o.push(crate::PROTOCOL_VERSION);
        put_str(&mut o, "default");
        o.extend_from_slice(&[3u8; NONCE_LEN]);
        o.extend_from_slice(&[9u8; PROOF_LEN]);
        o.push(0x00);
        assert!(matches!(Frame::decode(&o), Err(ProtoError::Truncated)));
    }

    #[test]
    fn target_parse() {
        let t = Target::parse("127.0.0.1:8080").unwrap();
        assert_eq!(t.host, "127.0.0.1");
        assert_eq!(t.port, 8080);
        let t = Target::parse("[::1]:80").unwrap();
        assert_eq!(t.host, "::1");
        assert_eq!(t.port, 80);
    }
}
