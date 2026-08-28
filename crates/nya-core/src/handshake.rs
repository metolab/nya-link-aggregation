use rand::RngCore;
use tokio::io::{AsyncRead, AsyncWrite};

use nya_proto::{
    read_frame, write_frame, CreateSession, CreateSessionOk, Frame, HandshakeErr, JoinSession,
    JoinSessionOk, NONCE_LEN, PROTOCOL_VERSION, SESSION_ID_LEN,
};

use crate::auth::{create_proof, join_proof, proofs_equal, session_key};
use crate::session::Session;

#[derive(Debug, thiserror::Error)]
pub enum HandshakeError {
    #[error(transparent)]
    Proto(#[from] nya_proto::ProtoError),
    #[error("handshake rejected: {0}")]
    Rejected(String),
    #[error("unexpected frame during handshake")]
    Unexpected,
    #[error("unknown session")]
    UnknownSession,
}

pub async fn client_create_session<T: AsyncRead + AsyncWrite + Unpin>(
    io: &mut T,
    psk: &[u8],
    exporter: &[u8],
    user_id: &str,
) -> Result<[u8; SESSION_ID_LEN], HandshakeError> {
    let mut nonce = [0u8; NONCE_LEN];
    rand::thread_rng().fill_bytes(&mut nonce);
    let proof = create_proof(psk, exporter, &nonce, user_id.as_bytes());
    write_frame(
        io,
        &Frame::CreateSession(CreateSession {
            version: PROTOCOL_VERSION,
            user_id: user_id.to_string(),
            nonce,
            proof,
        }),
    )
    .await?;
    tracing::debug!(role = "create", "create-session written, waiting for ok");
    tokio::task::yield_now().await;
    match read_frame(io).await? {
        Frame::CreateSessionOk(ok) => Ok(ok.session_id),
        Frame::HandshakeErr(e) => Err(HandshakeError::Rejected(e.message)),
        _ => Err(HandshakeError::Unexpected),
    }
}

pub async fn client_join_session<T: AsyncRead + AsyncWrite + Unpin>(
    io: &mut T,
    psk: &[u8],
    exporter: &[u8],
    session_id: [u8; SESSION_ID_LEN],
    path_name: &str,
) -> Result<(), HandshakeError> {
    let key = session_key(psk, &session_id);
    let proof = join_proof(&key, exporter, path_name.as_bytes());
    write_frame(
        io,
        &Frame::JoinSession(JoinSession {
            session_id,
            path_name: path_name.to_string(),
            proof,
        }),
    )
    .await?;
    tokio::task::yield_now().await;
    match read_frame(io).await? {
        Frame::JoinSessionOk(_) => Ok(()),
        Frame::HandshakeErr(e) => Err(HandshakeError::Rejected(e.message)),
        _ => Err(HandshakeError::Unexpected),
    }
}

pub enum HandshakeResult {
    Created {
        session: Session,
        session_id: [u8; SESSION_ID_LEN],
        incoming: tokio::sync::mpsc::Receiver<crate::IncomingStream>,
        path_name: String,
    },
    Joined {
        session: Session,
        path_name: String,
    },
}

pub async fn server_accept_handshake<T: AsyncRead + AsyncWrite + Unpin>(
    io: &mut T,
    psk: &[u8],
    exporter: &[u8],
    table: &crate::session::SessionTable,
) -> Result<HandshakeResult, HandshakeError> {
    match read_frame(io).await? {
        Frame::CreateSession(c) => {
            if c.version != PROTOCOL_VERSION {
                let _ = write_frame(
                    io,
                    &Frame::HandshakeErr(HandshakeErr {
                        message: format!("unsupported version {}", c.version),
                    }),
                )
                .await;
                return Err(HandshakeError::Rejected("version".into()));
            }
            let expect = create_proof(psk, exporter, &c.nonce, c.user_id.as_bytes());
            if !proofs_equal(&expect, &c.proof) {
                let _ = write_frame(
                    io,
                    &Frame::HandshakeErr(HandshakeErr {
                        message: "auth failed".into(),
                    }),
                )
                .await;
                return Err(HandshakeError::Rejected("auth".into()));
            }
            let mut session_id = [0u8; SESSION_ID_LEN];
            rand::thread_rng().fill_bytes(&mut session_id);
            let Some((session, incoming)) = table.create_with_incoming(session_id) else {
                let _ = write_frame(
                    io,
                    &Frame::HandshakeErr(HandshakeErr {
                        message: "session table closed".into(),
                    }),
                )
                .await;
                return Err(HandshakeError::Rejected("closed".into()));
            };
            write_frame(io, &Frame::CreateSessionOk(CreateSessionOk { session_id })).await?;
            tracing::debug!("create-session ok written");
            Ok(HandshakeResult::Created {
                session,
                session_id,
                incoming,
                path_name: "init".into(),
            })
        }
        Frame::JoinSession(j) => {
            let Some(session) = table.get(&j.session_id) else {
                let _ = write_frame(
                    io,
                    &Frame::HandshakeErr(HandshakeErr {
                        message: "unknown session".into(),
                    }),
                )
                .await;
                return Err(HandshakeError::UnknownSession);
            };
            let key = session_key(psk, &j.session_id);
            let expect = join_proof(&key, exporter, j.path_name.as_bytes());
            if !proofs_equal(&expect, &j.proof) {
                let _ = write_frame(
                    io,
                    &Frame::HandshakeErr(HandshakeErr {
                        message: "auth failed".into(),
                    }),
                )
                .await;
                return Err(HandshakeError::Rejected("auth".into()));
            }
            write_frame(io, &Frame::JoinSessionOk(JoinSessionOk)).await?;
            Ok(HandshakeResult::Joined {
                session,
                path_name: j.path_name,
            })
        }
        _ => Err(HandshakeError::Unexpected),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::SessionTable;
    use crate::SessionConfig;
    use tokio::io::duplex;

    #[tokio::test]
    async fn create_then_join() {
        let psk = b"psk";
        let exp = [7u8; 32];
        let table = SessionTable::new(SessionConfig::default());
        let (mut c0, mut s0) = duplex(16 * 1024);
        let (mut c1, mut s1) = duplex(16 * 1024);

        let server = async {
            let a = server_accept_handshake(&mut s0, psk, &exp, &table)
                .await
                .unwrap();
            let HandshakeResult::Created { session, .. } = a else {
                panic!("expected create");
            };
            let b = server_accept_handshake(&mut s1, psk, &exp, &table)
                .await
                .unwrap();
            assert!(matches!(b, HandshakeResult::Joined { .. }));
            session.shutdown();
        };

        let client = async {
            let sid = client_create_session(&mut c0, psk, &exp, "default")
                .await
                .unwrap();
            client_join_session(&mut c1, psk, &exp, sid, "b")
                .await
                .unwrap();
        };

        tokio::join!(server, client);
    }

    #[tokio::test]
    async fn bad_psk_rejected() {
        let table = SessionTable::new(SessionConfig::default());
        let (mut c, mut s) = duplex(16 * 1024);
        let server = server_accept_handshake(&mut s, b"one", &[1u8; 32], &table);
        let client = client_create_session(&mut c, b"two", &[1u8; 32], "default");
        let (sr, cr) = tokio::join!(server, client);
        assert!(sr.is_err());
        assert!(cr.is_err());
    }
}
