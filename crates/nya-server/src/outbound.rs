use std::sync::atomic::Ordering;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use tracing::{debug, warn};

use nya_core::IncomingStream;
use nya_proto::ResetReason;

pub async fn handle_incoming(mut incoming: mpsc::Receiver<IncomingStream>) {
    while let Some(mut inc) = incoming.recv().await {
        tokio::spawn(async move {
            let target = inc.target.to_string();
            match TcpStream::connect((inc.target.host.as_str(), inc.target.port)).await {
                Ok(mut tcp) => {
                    let _ = tcp.set_nodelay(true);
                    inc.process()
                        .outbound_dial_ok
                        .fetch_add(1, Ordering::Relaxed);
                    debug!(stream_id = inc.stream_id, %target, "outbound connected");
                    let _ = tokio::io::copy_bidirectional(&mut tcp, &mut inc.io).await;
                }
                Err(e) => {
                    inc.process()
                        .outbound_dial_fail
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(stream_id = inc.stream_id, %target, error = %e, "outbound dial failed");
                    inc.reset(ResetReason::DialFailed);
                }
            }
        });
    }
}
