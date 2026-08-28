//! Stream open/accept, windowed send, recv reorder, ACKs.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tracing::Level;
use tracing::{debug, warn};

use nya_proto::{
    Frame, ResetReason, StreamAck, StreamClose, StreamData, StreamOpen, Target, MAX_STREAM_PAYLOAD,
};

use crate::scheduler::backup_path;
use crate::stream::{Inbound, StreamState, TunnelStream, Unacked};

use super::{IncomingStream, Session, SessionError};

impl Session {
    pub async fn open_stream(&self, target: Target) -> Result<TunnelStream, SessionError> {
        if !self.inner.is_client {
            return Err(SessionError::ServerCannotOpen);
        }
        self.wait_ready(self.inner.cfg.all_down_timeout).await?;
        let path_id = self
            .pick_pref(crate::scheduler::PickPref::Interactive)
            .ok_or(SessionError::NoPath)?;
        let id = self.inner.next_stream_id.fetch_add(1, Ordering::Relaxed);
        let (tun, _st) = self.alloc_local_stream(id);
        self.set_sticky(id, path_id);
        self.inner
            .metrics
            .streams_opened
            .fetch_add(1, Ordering::Relaxed);
        self.note_unknown_pick(path_id);
        if tracing::enabled!(Level::DEBUG) {
            let cands = crate::scheduler::format_candidates(
                &self.path_list(),
                &self.inner.cfg,
                crate::scheduler::PickPref::Interactive,
                Some(path_id),
            );
            debug!(
                stream_id = id,
                path_id,
                pref = "pick",
                candidates = %cands,
                "pick"
            );
        }
        self.send_on_path(
            path_id,
            Frame::StreamOpen(StreamOpen {
                stream_id: id,
                target,
            }),
        );
        Ok(tun)
    }

    fn alloc_local_stream(&self, id: u32) -> (TunnelStream, Arc<StreamState>) {
        let win = self.inner.cfg.tuning.initial_window;
        let (app, peer) = tokio::io::duplex(win as usize);
        let (inbound_tx, inbound_rx) = mpsc::channel(self.inner.cfg.tuning.chan);
        let st = StreamState::new(id, inbound_tx, win);
        self.inner.streams.lock().unwrap().insert(id, st.clone());
        self.spawn_pump(id, peer, inbound_rx);
        (TunnelStream::from_duplex(id, app), st)
    }

    pub(super) fn accept_remote_stream(&self, path_id: u32, open: StreamOpen) {
        let id = open.stream_id;
        {
            let streams = self.inner.streams.lock().unwrap();
            if streams.contains_key(&id) {
                warn!(stream_id = id, "duplicate StreamOpen");
                return;
            }
        }
        let (tun, _st) = self.alloc_local_stream(id);
        self.set_sticky(id, path_id);
        self.inner
            .metrics
            .streams_opened
            .fetch_add(1, Ordering::Relaxed);
        let incoming = self.inner.incoming.lock().unwrap().clone();
        if let Some(tx) = incoming {
            let msg = IncomingStream {
                stream_id: id,
                target: open.target,
                io: tun,
                session: self.clone(),
            };
            if tx.try_send(msg).is_err() {
                self.reset_stream(id, ResetReason::Protocol);
            }
        }
    }

    fn spawn_pump(
        &self,
        id: u32,
        peer: tokio::io::DuplexStream,
        mut inbound_rx: mpsc::Receiver<Inbound>,
    ) {
        let session = self.clone();
        tokio::spawn(async move {
            let (mut r, mut w) = tokio::io::split(peer);
            let send = {
                let session = session.clone();
                async move {
                    let mut buf = vec![0u8; MAX_STREAM_PAYLOAD];
                    loop {
                        match r.read(&mut buf).await {
                            Ok(0) => {
                                let _ = session.close_send(id);
                                break;
                            }
                            Ok(n) => {
                                if session.send_data(id, &buf[..n]).await.is_err() {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                }
            };
            let recv = async move {
                loop {
                    match inbound_rx.recv().await {
                        Some(Inbound::Data(b)) => {
                            let n = b.len();
                            if w.write_all(&b).await.is_err() {
                                break;
                            }
                            session.note_app_read(id, n);
                        }
                        Some(Inbound::Close) => {
                            let _ = w.shutdown().await;
                            break;
                        }
                        Some(Inbound::Reset(_)) | None => break,
                    }
                }
            };
            tokio::join!(send, recv);
        });
    }

    async fn send_data(&self, id: u32, data: &[u8]) -> Result<(), SessionError> {
        let st = self.get_stream(id).ok_or(SessionError::UnknownStream)?;
        if st.reset.load(Ordering::Relaxed) {
            return Err(SessionError::Reset);
        }
        let mut offset_cursor = 0;
        while offset_cursor < data.len() {
            let mut window_waited = false;
            while !st.window_ok(1) {
                if !window_waited {
                    self.inner
                        .metrics
                        .window_blocks
                        .fetch_add(1, Ordering::Relaxed);
                    window_waited = true;
                }
                if self.is_dead() || st.reset.load(Ordering::Relaxed) {
                    return Err(SessionError::Reset);
                }
                tokio::select! {
                    _ = st.send_wait.notified() => {}
                    _ = tokio::time::sleep(self.inner.cfg.all_down_timeout) => {
                        if !st.window_ok(1) && !self.has_alive_path() {
                            return Err(SessionError::NoPath);
                        }
                    }
                }
            }
            let room = (u64::from(st.send_window.load(Ordering::Relaxed)))
                .saturating_sub(st.inflight_send()) as usize;
            let n = data.len() - offset_cursor;
            let n = n.min(room.max(1)).min(MAX_STREAM_PAYLOAD);
            let piece = data[offset_cursor..offset_cursor + n].to_vec();
            offset_cursor += n;
            let offset = st.send_next.fetch_add(n as u64, Ordering::Relaxed);
            let mut path_id = loop {
                if let Some(p) = self.ensure_sticky(id) {
                    break p;
                }
                if self.is_dead() || st.reset.load(Ordering::Relaxed) {
                    return Err(SessionError::Reset);
                }
                tokio::select! {
                    _ = self.inner.ready.notified() => {}
                    _ = tokio::time::sleep(self.inner.cfg.all_down_timeout) => {
                        if !self.has_alive_path() {
                            return Err(SessionError::NoPath);
                        }
                    }
                }
            };
            let becoming_bulk =
                n > self.inner.cfg.tuning.interactive_max && !st.bulk.swap(true, Ordering::Relaxed);
            if becoming_bulk {
                if let Some(dest) = self.hol_place_bulk(path_id) {
                    debug!(
                        stream_id = st.id,
                        from = path_id,
                        to = dest,
                        reason = "hol_initial",
                        "hol"
                    );
                    path_id = dest;
                    self.set_sticky(st.id, dest);
                    self.inner
                        .metrics
                        .hol_rebalances
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            {
                let mut unacked = st.unacked.lock().unwrap();
                unacked.insert(
                    offset,
                    Unacked {
                        data: piece.clone(),
                        path_id,
                        last_sent: std::time::Instant::now(),
                    },
                );
            }
            if let Some(p) = self.get_path(path_id) {
                p.add_inflight(n as u64);
            }
            let frame = Frame::StreamData(StreamData {
                stream_id: id,
                offset,
                data: piece,
            });
            if !self.send_on_path(path_id, frame.clone()) {
                // This TCP connection is send-blocked; hop to a sibling conn.
                if let Some(alt) = backup_path(&self.path_list(), path_id) {
                    self.set_sticky(id, alt);
                    {
                        let mut unacked = st.unacked.lock().unwrap();
                        if let Some(u) = unacked.get_mut(&offset) {
                            u.path_id = alt;
                            u.last_sent = std::time::Instant::now();
                        }
                    }
                    self.xfer_inflight(path_id, alt, n as u64);
                    self.send_on_path(alt, frame);
                    self.note_migrate("send_blocked");
                    debug!(
                        stream_id = id,
                        from = path_id,
                        to = alt,
                        reason = "send_blocked",
                        "migrate"
                    );
                }
            }
        }
        Ok(())
    }

    fn close_send(&self, id: u32) -> Result<(), SessionError> {
        let Some(st) = self.get_stream(id) else {
            return Ok(());
        };
        if st
            .send_fin_sent
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            return Ok(());
        }
        if let Some(path_id) = self.ensure_sticky(id) {
            self.send_on_path(path_id, Frame::StreamClose(StreamClose { stream_id: id }));
        }
        self.maybe_count_graceful(&st);
        Ok(())
    }

    pub(crate) fn reset_stream(&self, id: u32, reason: ResetReason) {
        self.finish_stream(id, Some(reason), true);
    }

    pub(super) fn maybe_count_graceful(&self, st: &StreamState) {
        if !st.send_fin_sent.load(Ordering::Relaxed) || !st.recv_fin.load(Ordering::Relaxed) {
            return;
        }
        if st
            .counted_close
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
        {
            self.observe_stream_end(st, None);
            self.unstick(st);
        }
    }

    pub fn note_app_read(&self, id: u32, n: usize) {
        let Some(st) = self.get_stream(id) else {
            return;
        };
        let n = n as u64;
        let mut cur = st.buffered_in.load(Ordering::Relaxed);
        loop {
            let next = cur.saturating_sub(n);
            match st
                .buffered_in
                .compare_exchange(cur, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
        if let Some(p) = self.ensure_sticky(id) {
            self.send_ack(&st, p);
        }
    }

    pub(super) fn on_data(&self, path_id: u32, data: StreamData) {
        let Some(st) = self.get_stream(data.stream_id) else {
            return;
        };
        if st.reset.load(Ordering::Relaxed) || st.recv_fin.load(Ordering::Relaxed) {
            return;
        }
        let mut buf = st.recv_buf.lock().unwrap();
        if data.offset < st.recv_next.load(Ordering::Relaxed) {
            drop(buf);
            self.send_ack(&st, path_id);
            return;
        }
        buf.insert(data.offset, data.data);
        drop(buf);
        self.drain_recv(&st, path_id);
    }

    fn drain_recv(&self, st: &StreamState, ack_path: u32) {
        loop {
            let mut buf = st.recv_buf.lock().unwrap();
            let next = st.recv_next.load(Ordering::Relaxed);
            let Some(chunk) = buf.remove(&next) else {
                break;
            };
            let len = chunk.len() as u64;
            st.recv_next.store(next + len, Ordering::Relaxed);
            st.buffered_in.fetch_add(len, Ordering::Relaxed);
            drop(buf);
            if st
                .inbound_tx
                .try_send(Inbound::Data(Bytes::from(chunk.clone())))
                .is_err()
            {
                st.recv_next.store(next, Ordering::Relaxed);
                st.buffered_in.fetch_sub(len, Ordering::Relaxed);
                st.recv_buf.lock().unwrap().insert(next, chunk);
                break;
            }
            st.last_recv_ms
                .store(crate::metrics::mono_ms().max(1), Ordering::Relaxed);
        }
        self.send_ack(st, ack_path);
    }

    fn send_ack(&self, st: &StreamState, path_id: u32) {
        let acked = st.recv_next.load(Ordering::Relaxed);
        let window = st.advertised_window();
        let frame = Frame::StreamAck(StreamAck {
            stream_id: st.id,
            acked_offset: acked,
            window,
        });
        if !self.send_on_path(path_id, frame.clone()) {
            if let Some(p) = self.pick_pref(crate::scheduler::PickPref::Interactive) {
                self.send_on_path(p, frame);
            }
        }
    }

    pub(super) fn on_ack(&self, ack: StreamAck) {
        let Some(st) = self.get_stream(ack.stream_id) else {
            return;
        };
        st.send_window.store(ack.window, Ordering::Relaxed);
        let prev = st.send_acked.load(Ordering::Relaxed);
        if ack.acked_offset > prev {
            st.send_acked.store(ack.acked_offset, Ordering::Relaxed);
            st.last_ack_ms
                .store(crate::metrics::mono_ms().max(1), Ordering::Relaxed);
            let mut unacked = st.unacked.lock().unwrap();
            let drop_keys: Vec<u64> = unacked
                .iter()
                .filter(|(off, u)| **off + u.data.len() as u64 <= ack.acked_offset)
                .map(|(off, _)| *off)
                .collect();
            for k in drop_keys {
                if let Some(u) = unacked.remove(&k) {
                    if let Some(p) = self.get_path(u.path_id) {
                        let loaded = p.inflight_bytes();
                        p.sub_inflight(u.data.len() as u64);
                        // Only small frames (control / interactive). Bulk ACK
                        // elapsed time is transfer delay, not path RTT. Skip
                        // when the sample waited behind bulk inflight.
                        let sample = u.last_sent.elapsed();
                        let t = &self.inner.cfg.tuning;
                        if u.data.len() <= t.interactive_max
                            && sample > t.ack_rtt_min
                            && sample < t.ack_rtt_max
                            && loaded < t.inflight_bias
                        {
                            p.record_rtt(sample);
                        }
                    }
                }
            }
        }
        st.send_wait.notify_waiters();
    }

    pub(super) fn on_peer_close(&self, id: u32) {
        let Some(st) = self.get_stream(id) else {
            return;
        };
        if !st.recv_fin.swap(true, Ordering::SeqCst) {
            let _ = st.inbound_tx.try_send(Inbound::Close);
        }
        self.maybe_count_graceful(&st);
    }

    pub(super) fn on_peer_reset(&self, id: u32, reason: ResetReason) {
        self.finish_stream(id, Some(reason), false);
    }
}
