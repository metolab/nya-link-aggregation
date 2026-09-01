use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::mpsc;

use tracing::{debug, info, warn, Instrument};

use nya_core::{
    connect_origin_meta, io_err_kind, HopClock, HopOutcome, HopProbe, HopRole, HopSample,
    IncomingStream, OriginDialMeta, OriginPeerSlots, Tuning,
};
use nya_proto::ResetReason;

pub async fn handle_incoming(mut incoming: mpsc::Receiver<IncomingStream>) {
    while let Some(inc) = incoming.recv().await {
        tokio::spawn(async move {
            let target = inc.target.to_string();
            let span = tracing::info_span!(
                target: "nya_otel",
                "nya.outbound.dial",
                otel.kind = "client",
                server.address = %target,
                nya.stream_id = inc.stream_id,
                nya.session_fp = tracing::field::Empty,
                otel.status_code = tracing::field::Empty,
                nya.dial_us = tracing::field::Empty,
                nya.lookup_a_us = tracing::field::Empty,
                nya.lookup_aaaa_us = tracing::field::Empty,
                nya.n_v4 = tracing::field::Empty,
                nya.n_v6 = tracing::field::Empty,
                nya.winner = tracing::field::Empty,
            );
            if let Some(fp) = inc.session_fp() {
                span.record("nya.session_fp", fp.as_str());
            }
            let t0 = Instant::now();
            let connected = connect_origin_meta(
                inc.target.host.as_str(),
                inc.target.port,
                Tuning::STANDARD.origin_connect_attempt_delay,
            )
            .instrument(span.clone())
            .await;
            let dial_us = (t0.elapsed().as_micros() as u64).max(1);
            span.record("nya.dial_us", dial_us);
            let meta = match &connected {
                Ok(d) => d.meta.clone(),
                Err(_) => OriginDialMeta::default(),
            };
            if let Some(us) = meta.lookup_a_us {
                span.record("nya.lookup_a_us", us);
            }
            if let Some(us) = meta.lookup_aaaa_us {
                span.record("nya.lookup_aaaa_us", us);
            }
            if let Some(n) = meta.n_v4 {
                span.record("nya.n_v4", n);
            }
            if let Some(n) = meta.n_v6 {
                span.record("nya.n_v6", n);
            }
            if !meta.winner.is_empty() {
                span.record("nya.winner", meta.winner);
            }
            if dial_us >= 100_000 {
                info!(
                    lookup_a_us = ?meta.lookup_a_us,
                    lookup_aaaa_us = ?meta.lookup_aaaa_us,
                    n_v4 = ?meta.n_v4,
                    n_v6 = ?meta.n_v6,
                    winner = meta.winner,
                    dial_us,
                    "outbound dial slow"
                );
            }
            match connected {
                Ok(dial) => {
                    let tcp = dial.stream;
                    drop(span);
                    inc.process()
                        .outbound_dial_ok
                        .fetch_add(1, Ordering::Relaxed);
                    debug!(stream_id = inc.stream_id, %target, "outbound connected");
                    let process = inc.process();
                    let stream_id = inc.stream_id;
                    let host = inc.target.host.clone();
                    let session_fp = inc.session_fp().unwrap_or_default();
                    let origin_clock = HopClock::new();
                    let overlay_clock = HopClock::new();
                    let slots = Arc::new(OriginPeerSlots::default());
                    let mut origin = HopProbe::wrap(tcp, origin_clock.clone())
                        .sample_peer_last_on_read(overlay_clock.clone(), slots.clone());
                    let mut overlay = HopProbe::wrap(inc.io, overlay_clock.clone());
                    let t_copy = Instant::now();
                    let copy = tokio::io::copy_bidirectional(&mut origin, &mut overlay).await;
                    let (outcome, copy_err) = match &copy {
                        Ok(_) => (HopOutcome::Ok, None),
                        Err(e) => (HopOutcome::CopyErr, Some(io_err_kind(e))),
                    };
                    process.record_hop(HopSample {
                        role: HopRole::Server,
                        stream_id,
                        host,
                        session_fp,
                        outcome,
                        copy_us: Some((t_copy.elapsed().as_micros() as u64).max(1)),
                        dial_us: Some(dial_us),
                        origin_first_rx_us: origin_clock.first_rx_us(),
                        origin_last_rx_us: origin_clock.last_rx_us(),
                        client_first_rx_us: overlay_clock.first_rx_us(),
                        client_last_rx_us: overlay_clock.last_rx_us(),
                        crx_at_olast: slots.crx_at_olast(),
                        max_gap: slots.max_gap_us(),
                        crx_at_gap: slots.crx_at_gap(),
                        origin_at_gap: slots.origin_at_gap(),
                        rx_bytes: Some(origin_clock.rx_bytes()),
                        tx_bytes: Some(origin_clock.tx_bytes()),
                        copy_err,
                        ..Default::default()
                    });
                }
                Err(e) => {
                    span.record("otel.status_code", "ERROR");
                    inc.process()
                        .outbound_dial_fail
                        .fetch_add(1, Ordering::Relaxed);
                    inc.process().record_hop(HopSample {
                        role: HopRole::Server,
                        stream_id: inc.stream_id,
                        host: inc.target.host.clone(),
                        session_fp: inc.session_fp().unwrap_or_default(),
                        outcome: HopOutcome::DialFail,
                        dial_us: Some(dial_us),
                        copy_us: None,
                        ..Default::default()
                    });
                    {
                        let _g = span.enter();
                        warn!(stream_id = inc.stream_id, %target, error = %e, "outbound dial failed");
                    }
                    inc.reset(ResetReason::DialFailed);
                }
            }
        });
    }
}
