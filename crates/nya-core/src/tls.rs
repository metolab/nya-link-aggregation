use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{
    ClientConfig, DigitallySignedStruct, Error as TlsError, ServerConfig, SignatureScheme,
};
use rustls_pemfile::{certs, private_key};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use tokio_rustls::TlsStream;
use x509_parser::prelude::*;

use nya_proto::TLS_EXPORTER_LABEL;

pub fn install_crypto() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

pub fn spki_sha256(cert_der: &[u8]) -> Result<[u8; 32], TlsError> {
    let (_, cert) = X509Certificate::from_der(cert_der)
        .map_err(|_| TlsError::General("failed to parse certificate".into()))?;
    let spki = cert.public_key().raw;
    let digest = Sha256::digest(spki);
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest);
    Ok(out)
}

pub fn spki_sha256_from_pem(pem: &str) -> Result<[u8; 32], TlsError> {
    let mut cur = Cursor::new(pem.as_bytes());
    let cert = certs(&mut cur)
        .next()
        .ok_or_else(|| TlsError::General("no certificate in pem".into()))?
        .map_err(|_| TlsError::General("invalid cert pem".into()))?;
    spki_sha256(&cert)
}

pub fn parse_pin_hex(s: &str) -> Result<[u8; 32], TlsError> {
    let raw = hex::decode(s.trim()).map_err(|_| TlsError::General("pin is not hex".into()))?;
    if raw.len() != 32 {
        return Err(TlsError::General(
            "pin must be 32 bytes (64 hex chars)".into(),
        ));
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&raw);
    Ok(out)
}

#[derive(Debug)]
pub struct PinnedSpkiVerifier {
    pins: Vec<[u8; 32]>,
    provider: rustls::crypto::CryptoProvider,
}

impl PinnedSpkiVerifier {
    pub fn new(pins: Vec<[u8; 32]>) -> Self {
        Self {
            pins,
            provider: rustls::crypto::ring::default_provider(),
        }
    }
}

impl ServerCertVerifier for PinnedSpkiVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, TlsError> {
        let pin = spki_sha256(end_entity.as_ref())?;
        if self.pins.iter().any(|p| p == &pin) {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(TlsError::General(
                "server certificate SPKI pin mismatch".into(),
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

pub fn client_tls_config(pins: Vec<[u8; 32]>) -> Result<ClientConfig, TlsError> {
    install_crypto();
    let mut cfg = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedSpkiVerifier::new(pins)))
        .with_no_client_auth();
    cfg.alpn_protocols = vec![nya_proto::ALPN.to_vec()];
    Ok(cfg)
}

pub fn load_server_config(cert_path: &Path, key_path: &Path) -> Result<ServerConfig, TlsError> {
    install_crypto();
    let cert_pem = fs::read(cert_path).map_err(|e| TlsError::General(e.to_string()))?;
    let key_pem = fs::read(key_path).map_err(|e| TlsError::General(e.to_string()))?;
    let mut cert_cur = Cursor::new(cert_pem);
    let certs: Vec<CertificateDer<'static>> =
        certs(&mut cert_cur)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| TlsError::General("invalid server cert pem".into()))?;
    if certs.is_empty() {
        return Err(TlsError::General("no certificates in cert file".into()));
    }
    let mut key_cur = Cursor::new(key_pem);
    let key: PrivateKeyDer<'static> = private_key(&mut key_cur)
        .map_err(|_| TlsError::General("invalid server key pem".into()))?
        .ok_or_else(|| TlsError::General("no private key in key file".into()))?;
    let mut cfg = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| TlsError::General(e.to_string()))?;
    cfg.alpn_protocols = vec![nya_proto::ALPN.to_vec()];
    Ok(cfg)
}

pub fn export_from_client<IO>(
    stream: &tokio_rustls::client::TlsStream<IO>,
) -> Result<[u8; 32], TlsError> {
    let mut out = [0u8; 32];
    let _ =
        stream
            .get_ref()
            .1
            .export_keying_material(&mut out, TLS_EXPORTER_LABEL.as_bytes(), None)?;
    Ok(out)
}

pub fn export_from_server<IO>(
    stream: &tokio_rustls::server::TlsStream<IO>,
) -> Result<[u8; 32], TlsError> {
    let mut out = [0u8; 32];
    let _ =
        stream
            .get_ref()
            .1
            .export_keying_material(&mut out, TLS_EXPORTER_LABEL.as_bytes(), None)?;
    Ok(out)
}

pub fn export_keying_material<IO>(stream: &TlsStream<IO>) -> Result<[u8; 32], TlsError> {
    match stream {
        TlsStream::Client(s) => export_from_client(s),
        TlsStream::Server(s) => export_from_server(s),
    }
}

pub async fn connect_pinned(
    addr: &str,
    pin: [u8; 32],
) -> Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>, TlsError> {
    tracing::debug!(%addr, "tcp connect");
    let tcp = tokio::net::TcpStream::connect(addr)
        .await
        .map_err(|e| TlsError::General(e.to_string()))?;
    tracing::debug!(%addr, "tcp connected");
    let _ = tcp.set_nodelay(true);
    let host = addr
        .rsplit_once(':')
        .map(|(h, _)| h.trim_matches(|c| c == '[' || c == ']'))
        .unwrap_or(addr);
    let server_name = ServerName::try_from(host.to_string())
        .map_err(|_| TlsError::General(format!("invalid server name {host}")))?;
    let cfg = client_tls_config(vec![pin])?;
    let connector = tokio_rustls::TlsConnector::from(Arc::new(cfg));
    connector
        .connect(server_name, tcp)
        .await
        .map_err(|e| TlsError::General(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::SessionConfig;
    use crate::handshake::{client_create_session, server_accept_handshake, HandshakeResult};
    use crate::session::SessionTable;
    use rustls::pki_types::ServerName;
    use std::sync::Arc;
    use tokio::net::{TcpListener, TcpStream};
    use tokio_rustls::{TlsAcceptor, TlsConnector};

    #[tokio::test]
    async fn tls_create_session_ok_reaches_client() {
        install_crypto();
        let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let dir = std::env::temp_dir().join(format!("nya-tls-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("server.crt"), ck.cert.pem()).unwrap();
        std::fs::write(dir.join("server.key"), ck.key_pair.serialize_pem()).unwrap();
        let pin = spki_sha256(ck.cert.der().as_ref()).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_cfg =
            load_server_config(&dir.join("server.crt"), &dir.join("server.key")).unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(server_cfg));
        let table = SessionTable::new(SessionConfig::default());
        let psk = b"psk".to_vec();

        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut tls = acceptor.accept(tcp).await.unwrap();
            let exp = export_from_server(&tls).unwrap();
            let hs = server_accept_handshake(&mut tls, &psk, &exp, &table)
                .await
                .unwrap();
            match hs {
                HandshakeResult::Created {
                    session, path_name, ..
                } => {
                    session.start_path(path_name, tls);
                    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
                    session.shutdown();
                }
                _ => panic!("expected create"),
            }
        });

        let client_cfg = client_tls_config(vec![pin]).unwrap();
        let connector = TlsConnector::from(Arc::new(client_cfg));
        let tcp = TcpStream::connect(addr).await.unwrap();
        let name = ServerName::try_from("127.0.0.1").unwrap();
        let mut tls = connector.connect(name, tcp).await.unwrap();
        let exp = export_from_client(&tls).unwrap();
        let sid = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            client_create_session(&mut tls, b"psk", &exp, "default", "a#0"),
        )
        .await
        .expect("client handshake timed out")
        .unwrap();
        assert_eq!(sid.len(), 16);
        let _ = server.await;
    }

    #[tokio::test]
    async fn tls_create_separate_runtime() {
        install_crypto();
        let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let dir = std::env::temp_dir().join(format!("nya-tls2-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("server.crt"), ck.cert.pem()).unwrap();
        std::fs::write(dir.join("server.key"), ck.key_pair.serialize_pem()).unwrap();
        let pin = spki_sha256(ck.cert.der().as_ref()).unwrap();
        let (port_tx, port_rx) = std::sync::mpsc::channel();

        let server = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                port_tx.send(listener.local_addr().unwrap()).unwrap();
                let server_cfg =
                    load_server_config(&dir.join("server.crt"), &dir.join("server.key")).unwrap();
                let acceptor = TlsAcceptor::from(Arc::new(server_cfg));
                let table = SessionTable::new(SessionConfig::default());
                let (tcp, _) = listener.accept().await.unwrap();
                tcp.set_nodelay(true).unwrap();
                let mut tls = acceptor.accept(tcp).await.unwrap();
                let exp = export_from_server(&tls).unwrap();
                let hs = server_accept_handshake(&mut tls, b"psk", &exp, &table)
                    .await
                    .unwrap();
                assert!(matches!(hs, HandshakeResult::Created { .. }));
                tokio::time::sleep(std::time::Duration::from_millis(400)).await;
            });
        });

        let addr = port_rx.recv().unwrap();
        let client_cfg = client_tls_config(vec![pin]).unwrap();
        let connector = TlsConnector::from(Arc::new(client_cfg));
        let tcp = TcpStream::connect(addr).await.unwrap();
        tcp.set_nodelay(true).unwrap();
        let name = ServerName::try_from("127.0.0.1").unwrap();
        let mut tls = connector.connect(name, tcp).await.unwrap();
        let exp = export_from_client(&tls).unwrap();
        let sid = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            client_create_session(&mut tls, b"psk", &exp, "default", "a#0"),
        )
        .await
        .expect("client handshake timed out")
        .unwrap();
        assert_eq!(sid.len(), 16);
        server.join().unwrap();
    }
}
