//! Two-process handshake probe: `hs_pair server <addr> <dir>` / `hs_pair client <addr> <dir>`
use std::sync::Arc;

use nya_core::{
    client_create_session, client_tls_config, export_from_client, export_from_server,
    install_crypto, load_server_config, server_accept_handshake, SessionConfig, SessionTable,
};
use rustls::pki_types::ServerName;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, TlsConnector};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    install_crypto();
    let mut args = std::env::args().skip(1);
    let role = args.next().expect("role");
    let addr = args.next().expect("addr");
    let dir = std::path::PathBuf::from(args.next().expect("dir"));
    match role.as_str() {
        "server" => {
            let listener = TcpListener::bind(&addr).await.unwrap();
            eprintln!("server listening {addr}");
            let cfg = load_server_config(&dir.join("server.crt"), &dir.join("server.key")).unwrap();
            let acceptor = TlsAcceptor::from(Arc::new(cfg));
            let table = SessionTable::new(SessionConfig::default());
            let (tcp, peer) = listener.accept().await.unwrap();
            tcp.set_nodelay(true).unwrap();
            eprintln!("accepted {peer}");
            let mut tls = acceptor.accept(tcp).await.unwrap();
            let exp = export_from_server(&tls).unwrap();
            server_accept_handshake(&mut tls, b"smoke-psk", &exp, &table)
                .await
                .unwrap();
            eprintln!("handshake ok");
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
        "client" => {
            let pin = nya_core::spki_sha256_from_pem(
                &std::fs::read_to_string(dir.join("server.crt")).unwrap(),
            )
            .unwrap();
            let cfg = client_tls_config(vec![pin]).unwrap();
            let connector = TlsConnector::from(Arc::new(cfg));
            let tcp = TcpStream::connect(&addr).await.unwrap();
            tcp.set_nodelay(true).unwrap();
            let host = addr.rsplit_once(':').unwrap().0;
            let name = ServerName::try_from(host.to_string()).unwrap();
            eprintln!("tls connecting");
            let mut tls = connector.connect(name, tcp).await.unwrap();
            let exp = export_from_client(&tls).unwrap();
            eprintln!("tls connected, creating session");
            let sid = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                client_create_session(&mut tls, b"smoke-psk", &exp, "default"),
            )
            .await
            .expect("timeout")
            .unwrap();
            eprintln!("session {}", hex(&sid));
        }
        _ => panic!("role"),
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}
