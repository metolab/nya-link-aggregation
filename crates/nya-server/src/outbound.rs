use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::{info, warn};

use nya_core::IncomingStream;
use nya_proto::ResetReason;

pub async fn handle_incoming(mut incoming: mpsc::Receiver<IncomingStream>) {
    while let Some(mut inc) = incoming.recv().await {
        tokio::spawn(async move {
            let target = inc.target.to_string();
            match TcpStream::connect((inc.target.host.as_str(), inc.target.port)).await {
                Ok(mut tcp) => {
                    let _ = tcp.set_nodelay(true);
                    info!(stream_id = inc.stream_id, %target, "outbound connected");
                    let _ = tokio::io::copy_bidirectional(&mut tcp, &mut inc.io).await;
                }
                Err(e) => {
                    warn!(stream_id = inc.stream_id, %target, error = %e, "outbound dial failed");
                    inc.reset(ResetReason::DialFailed);
                }
            }
        });
    }
}
