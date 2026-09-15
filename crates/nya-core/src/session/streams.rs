//! Stream open/accept, windowed send, recv reorder, ACKs.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tracing::debug;
use tracing::Level;

use nya_proto::{
    Frame, ResetReason, StreamAck, StreamClose, StreamData, StreamOpen, Target, MAX_STREAM_PAYLOAD,
};

use crate::health;
use crate::scheduler::PickPref;
use crate::stream::{Inbound, StreamState, TunnelStream, Unacked};

use super::{IncomingStream, Session, SessionError};

impl Session {
    pub async fn open_stream(&self, target: Target) -> Result<TunnelStream, SessionError> {
        if !self.inner.is_client {
            return Err(SessionError::ServerCannotOpen);
        }
        self.wait_ready(self.inner.cfg.all_down_timeout).await?;
        let id = self.inner.next_stream_id.fetch_add(1, Ordering::Relaxed);
        let path_id = self
            .pick_pref(crate::scheduler::PickPref::Interactive)
            .ok_or(SessionError::NoPath)?;
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
        let open = Frame::StreamOpen(StreamOpen {
            stream_id: id,
            target: target.clone(),
        });
        self.remember_open(id, path_id, target.clone());
        if self.send_on_path(path_id, open.clone()) {
            self.note_pick(path_id);
        } else if let Some(alt) = self.pick_retry(path_id) {
            if self.send_on_path(alt, open) {
                self.set_sticky(id, alt);
                self.remember_open(id, alt, target);
                self.note_pick(alt);
            }
        }
        Ok(tun)
    }

    fn alloc_local_stream(&self, id: u32) -> (TunnelStream, Arc<StreamState>) {
        self.try_alloc_local_stream(id)
            .expect("stream id must be unique on the opening side")
    }

    fn try_alloc_local_stream(&self, id: u32) -> Option<(TunnelStream, Arc<StreamState>)> {
        let win = self.inner.cfg.tuning.initial_window;
        let (app, peer) = tokio::io::duplex(win as usize);
        let (inbound_tx, inbound_rx) = mpsc::channel(self.inner.cfg.tuning.chan);
        let st = StreamState::new(id, inbound_tx, win);
        {
            let mut streams = self.inner.streams.lock().unwrap();
            if streams.contains_key(&id) {
                return None;
            }
            streams.insert(id, st.clone());
        }
        self.spawn_pump(id, peer, inbound_rx);
        Some((TunnelStream::from_duplex(id, app, st.counters.clone()), st))
    }

    pub(super) fn accept_remote_stream(&self, path_id: u32, open: StreamOpen) {
        let id = open.stream_id;
        let Some((tun, _st)) = self.try_alloc_local_stream(id) else {
            debug!(stream_id = id, "duplicate StreamOpen");
            return;
        };
        self.set_sticky(id, path_id);
        self.inner
            .metrics
            .streams_opened
            .fetch_add(1, Ordering::Relaxed);
        for early in self.take_early_data(id) {
            self.deliver_data(early.path_id, early.data);
        }
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
            let session_reap = session.clone();
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
            session_reap.reap_stream(id);
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
                    st.window_blocks.fetch_add(1, Ordering::Relaxed);
                    // P3b evidence: the peer's window, not our paths, is
                    // the limiter right now.
                    if crate::scheduler::any_bulk_room(&self.path_list()) {
                        self.inner
                            .metrics
                            .send_window_limited_with_room
                            .fetch_add(1, Ordering::Relaxed);
                        st.window_limited_with_room.fetch_add(1, Ordering::Relaxed);
                    }
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
            let pref = if st.bulk.load(Ordering::Relaxed) {
                PickPref::Any
            } else {
                PickPref::Interactive
            };
            let mut budget_waited = false;
            let parked_at = Instant::now();
            // Overflow pieces ride a sibling without moving sticky (KD6).
            let mut overflow = false;
            let mut path_id = loop {
                // Arm the budget wake before looking, so an ACK that lands
                // between the room check and the await is not lost.
                let budget_wake = self.inner.budget_wait.notified();
                tokio::pin!(budget_wake);
                budget_wake.as_mut().enable();
                let sticky = st.sticky.load(Ordering::Relaxed);
                let picked = if pref == PickPref::Interactive {
                    self.interactive_affinity(sticky)
                        .or_else(|| self.pick_pref(pref))
                } else {
                    overflow = false;
                    self.bulk_affinity(sticky)
                        .filter(|id| self.get_path(*id).is_some_and(|p| p.room_bytes() >= 1))
                        .or_else(|| {
                            let alt = self.bulk_overflow_pick(sticky);
                            overflow = alt.is_some();
                            alt
                        })
                        .or_else(|| {
                            if crate::scheduler::any_bulk_room(&self.path_list()) {
                                self.pick_pref(pref)
                            } else {
                                None
                            }
                        })
                };
                if let Some(p) = picked {
                    break p;
                }
                if self.is_dead() || st.reset.load(Ordering::Relaxed) {
                    return Err(SessionError::Reset);
                }
                if pref == PickPref::Any && !budget_waited && self.has_alive_path() {
                    self.inner
                        .metrics
                        .send_budget_blocks
                        .fetch_add(1, Ordering::Relaxed);
                    st.budget_blocks.fetch_add(1, Ordering::Relaxed);
                    budget_waited = true;
                    for p in self.path_list() {
                        if p.is_alive() && p.room_bytes() == 0 {
                            p.note_budget_limited();
                        }
                    }
                }
                if parked_at.elapsed() >= self.inner.cfg.all_down_timeout && !self.has_alive_path()
                {
                    return Err(SessionError::NoPath);
                }
                tokio::select! {
                    _ = self.inner.ready.notified() => {}
                    _ = &mut budget_wake => {}
                    _ = st.send_wait.notified() => {}
                    _ = tokio::time::sleep(self.inner.cfg.all_down_timeout) => {}
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
                    overflow = false;
                    self.inner
                        .metrics
                        .hol_rebalances
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            if !overflow {
                self.set_sticky(id, path_id);
            }
            st.note_path_used(path_id);
            {
                let mut unacked = st.unacked.lock().unwrap();
                unacked.insert(
                    offset,
                    Unacked {
                        data: piece.clone(),
                        path_id,
                        last_sent: Instant::now(),
                        first_sent: Instant::now(),
                        tried: vec![path_id],
                        retry_not_before: Instant::now(),
                        dropped: false,
                        delivered_at_send: 0,
                        delivered_time_at_send_us: 0,
                        sent_us: 0,
                        first_tx_at_send_us: 0,
                    },
                );
            }
            if let Some(p) = self.get_path(path_id) {
                p.add_inflight(n as u64);
                self.stamp_delivered(&st, offset, &p);
            }
            let frame = Frame::StreamData(StreamData {
                stream_id: id,
                offset,
                data: piece,
            });
            if !self.send_on_path(path_id, frame.clone()) {
                let bulk = st.bulk.load(Ordering::Relaxed);
                if bulk && self.get_path(path_id).is_some_and(|p| p.is_alive()) {
                    self.wait_bulk_send(&st, path_id, offset, n as u64, frame)
                        .await;
                } else {
                    self.migrate_send_blocked(&st, path_id, offset, n as u64, frame);
                }
            }
        }
        Ok(())
    }

    /// C4: wait for bulk queue space up to path `down_timeout`, then give-up
    /// `pick_retry`. Pin `retry_not_before` for the whole wait so maintain
    /// cannot dual-send. `send_wait` is a reset/window check, not give-up.
    async fn wait_bulk_send(
        &self,
        st: &StreamState,
        path_id: u32,
        offset: u64,
        n: u64,
        frame: Frame,
    ) {
        let Some(p) = self.get_path(path_id) else {
            self.migrate_send_blocked(st, path_id, offset, n, frame);
            return;
        };
        let probe = self.probe_interval_for(&p);
        let wait = health::down_timeout(&self.inner.cfg, p.stable_rtt(), probe);
        let deadline = Instant::now() + wait;
        {
            let mut unacked = st.unacked.lock().unwrap();
            if let Some(u) = unacked.get_mut(&offset) {
                u.retry_not_before = deadline;
            }
        }
        let mark_enqueued = || {
            let now = Instant::now();
            if let Some(u) = st.unacked.lock().unwrap().get_mut(&offset) {
                u.last_sent = now;
                u.retry_not_before = now;
                u.dropped = false;
            }
            self.stamp_delivered(st, offset, &p);
        };
        loop {
            if self.is_dead() || st.reset.load(Ordering::Relaxed) {
                return;
            }
            let timed_out = Instant::now() >= deadline;
            if !p.is_alive() || timed_out {
                if p.is_alive() && self.send_on_path(path_id, frame.clone()) {
                    mark_enqueued();
                    return;
                }
                self.migrate_send_blocked(st, path_id, offset, n, frame);
                return;
            }
            let remain: Duration = deadline.saturating_duration_since(Instant::now());
            tokio::select! {
                _ = p.queue_wait.notified() => {}
                _ = st.send_wait.notified() => {
                    continue;
                }
                _ = tokio::time::sleep(remain) => {
                    continue;
                }
            }
            if p.queued_bulk() >= self.inner.cfg.tuning.chan as u64 {
                continue;
            }
            if self.send_on_path(path_id, frame.clone()) {
                mark_enqueued();
                return;
            }
        }
    }

    fn migrate_send_blocked(
        &self,
        st: &StreamState,
        path_id: u32,
        offset: u64,
        n: u64,
        frame: Frame,
    ) {
        let Some(alt) = self.pick_retry(path_id) else {
            self.note_data_dropped(st.id, offset);
            return;
        };
        if !self.send_on_path(alt, frame) {
            self.note_data_dropped(st.id, offset);
            return;
        }
        self.set_sticky(st.id, alt);
        {
            let mut unacked = st.unacked.lock().unwrap();
            if let Some(u) = unacked.get_mut(&offset) {
                u.path_id = alt;
                u.last_sent = Instant::now();
                u.retry_not_before = u.last_sent;
                u.dropped = false;
                Session::push_tried(&mut u.tried, alt);
            }
        }
        self.xfer_inflight(path_id, alt, n);
        if let Some(p) = self.get_path(alt) {
            self.stamp_delivered(st, offset, &p);
        }
        self.note_migrate("send_blocked");
        debug!(
            stream_id = st.id,
            from = path_id,
            to = alt,
            reason = "send_blocked",
            "migrate"
        );
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
        st.note_close_started();
        self.unstick(&st);
        let second = st.recv_fin.load(Ordering::Relaxed);
        let final_offset = Some(st.send_next.load(Ordering::Relaxed));
        let close = StreamClose {
            stream_id: id,
            final_offset,
        };
        let path_id = self.pick_pref(PickPref::Any);
        if let Some(path_id) = path_id {
            self.remember_close(id, path_id, second, final_offset);
            if !self.send_on_path(path_id, Frame::StreamClose(close.clone())) {
                if let Some(alt) = self.pick_retry(path_id) {
                    self.remember_close(id, alt, second, final_offset);
                    self.send_on_path(alt, Frame::StreamClose(close));
                }
            }
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
        }
        self.remove_held_stream(st.id);
    }

    pub(super) fn reap_stream(&self, id: u32) {
        let Some(st) = self.get_stream(id) else {
            return;
        };
        if st.counted_close.load(Ordering::Relaxed) {
            self.remove_held_stream(id);
            return;
        }
        if st.send_fin_sent.load(Ordering::Relaxed) && st.recv_fin.load(Ordering::Relaxed) {
            self.maybe_count_graceful(&st);
            return;
        }
        // Progress-fine half-close stays until reap_closed_streams so Close
        // retry can still land. Immediate linger_reap here would drop the
        // Close table and reopen leftover. No-progress still Timeout-Resets.
        if self.overlay_progress_fine(&st) {
            return;
        }
        self.finish_stream(id, Some(ResetReason::Timeout), true);
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
        // The app freed a channel slot: hand over whatever in-order data
        // `drain_recv` left in `recv_buf` when the channel was full. Arrival
        // is the only other drain trigger, and the tail of a transfer (or a
        // sender that is waiting on our window) brings no arrival. This
        // also sends the window update.
        self.drain_recv(&st, st.last_recv_path.load(Ordering::Relaxed));
    }

    pub(super) fn on_data(&self, path_id: u32, data: StreamData) {
        if self.get_stream(data.stream_id).is_none() {
            self.push_early_data(path_id, data);
            return;
        }
        self.deliver_data(path_id, data);
    }

    fn deliver_data(&self, path_id: u32, data: StreamData) {
        let Some(st) = self.get_stream(data.stream_id) else {
            return;
        };
        if st.reset.load(Ordering::Relaxed) {
            return;
        }
        let len = data.data.len() as u64;
        // P5: the receive side is complete (or this piece lies past the
        // sender's final offset). The sender only re-sends because it has
        // not seen our ACK reach `close_off`; every silent drop here costs
        // it another hedge round. Re-ACK — `recv_next == close_off` — so the
        // sender's `unacked` empties and the hedge belt stops.
        let close_off = st.recv_close_off.load(Ordering::Relaxed);
        if st.recv_fin.load(Ordering::Relaxed)
            || (close_off != u64::MAX && data.offset >= close_off)
        {
            self.inner
                .metrics
                .data_dup_rx_bytes
                .fetch_add(len, Ordering::Relaxed);
            st.dup_rx_bytes.fetch_add(len, Ordering::Relaxed);
            self.inner
                .metrics
                .ack_after_fin
                .fetch_add(1, Ordering::Relaxed);
            self.send_ack(&st, path_id);
            return;
        }
        st.last_recv_path.store(path_id, Ordering::Relaxed);
        // DATA from the peer proves it holds the stream: a download whose
        // client never writes (so no ACK ever comes back) must not re-send
        // StreamOpen every retry_after for the whole transfer.
        if !st.peer_seen.swap(true, Ordering::Relaxed) {
            self.forget_open(data.stream_id);
        }
        let mut buf = st.recv_buf.lock().unwrap();
        if data.offset < st.recv_next.load(Ordering::Relaxed) {
            drop(buf);
            self.inner
                .metrics
                .data_dup_rx_bytes
                .fetch_add(len, Ordering::Relaxed);
            st.dup_rx_bytes.fetch_add(len, Ordering::Relaxed);
            self.send_ack(&st, path_id);
            return;
        }
        let new_len = len;
        if let Some(old) = buf.insert(data.offset, data.data) {
            // A re-sent piece landing on an offset already buffered: the
            // first arrival is what opened/closed holes and drove the
            // drain; this one only replaces bytes. Account and ACK, no
            // drain (P2.1: only first arrivals reach the hole logic).
            let old_len = old.len() as u64;
            self.inner
                .metrics
                .data_dup_rx_bytes
                .fetch_add(old_len, Ordering::Relaxed);
            st.dup_rx_bytes.fetch_add(old_len, Ordering::Relaxed);
            let _ = st
                .recv_buffered
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_sub(old_len))
                });
            st.recv_buffered.fetch_add(new_len, Ordering::Relaxed);
            drop(buf);
            self.send_ack(&st, path_id);
            return;
        }
        st.recv_buffered.fetch_add(new_len, Ordering::Relaxed);
        drop(buf);
        if data.offset > st.recv_next.load(Ordering::Relaxed) {
            st.last_hole_arrival.store(data.offset, Ordering::Relaxed);
        }
        // P3b: did this piece end on an edge we advertised (sender was
        // window-limited)?
        let rate = st.deliver_rate_ewma.load(Ordering::Relaxed);
        let min_rtt = self.bdp_rtt(&st).unwrap_or(Duration::ZERO);
        let tol = (nya_proto::MAX_STREAM_PAYLOAD as u64)
            .max((rate as f64 * min_rtt.as_secs_f64() / 8.0) as u64);
        st.note_arrival(data.offset + new_len, tol);
        self.drain_recv(&st, path_id);
    }

    /// Hand in-order chunks to the application. The whole step — take the
    /// chunk at `recv_next`, `try_send`, advance — runs under the
    /// `recv_buf` lock: DATA for one stream arrives on several path reader
    /// tasks at once, and releasing the lock between "advance `recv_next`"
    /// and "the channel was full, rewind" let a second drain deliver the
    /// *following* chunk first — out-of-order bytes to the app. `try_send`
    /// never blocks, so holding the lock across it is safe.
    fn drain_recv(&self, st: &StreamState, ack_path: u32) {
        let mut delivered = 0u64;
        {
            let mut buf = st.recv_buf.lock().unwrap();
            let mut full = false;
            loop {
                let next = st.recv_next.load(Ordering::Relaxed);
                let Some(chunk) = buf.remove(&next) else {
                    break;
                };
                let len = chunk.len() as u64;
                match st.inbound_tx.try_send(Inbound::Data(Bytes::from(chunk))) {
                    Ok(()) => {}
                    Err(e) => {
                        let chunk = match e.into_inner() {
                            Inbound::Data(b) => b.to_vec(),
                            _ => unreachable!("drain_recv only sends Data"),
                        };
                        buf.insert(next, chunk);
                        full = true;
                        break;
                    }
                }
                let _ = st
                    .recv_buffered
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                        Some(v.saturating_sub(len))
                    });
                st.recv_next.store(next + len, Ordering::Relaxed);
                st.buffered_in.fetch_add(len, Ordering::Relaxed);
                delivered += len;
                st.last_recv_ms
                    .store(crate::metrics::mono_ms().max(1), Ordering::Relaxed);
            }
            self.track_hole(st, &buf, full);
        }
        if delivered > 0 {
            let sampled = st.note_deliver(delivered, self.deliver_min_dt(st));
            self.tune_recv_cap_sampled(st, sampled);
        }
        self.send_ack(st, ack_path);
        self.try_finish_recv_close(st);
    }

    /// P2.1 hole state machine, derived from buffer contents under the
    /// `recv_buf` lock (never from one piece's landing offset: several
    /// path-reader tasks insert concurrently). A hole opens when the head
    /// is missing while later pieces are buffered; it closes when the
    /// missing piece is present — drained past, or sitting at the head of
    /// a full channel. A head that is present but undeliverable (`full`)
    /// is the application waiting, not the network: it neither opens nor
    /// prolongs a hole. Also refreshes the P1.4 receiver evidence.
    fn track_hole(
        &self,
        st: &StreamState,
        buf: &std::collections::BTreeMap<u64, Vec<u8>>,
        full: bool,
    ) {
        let now = Instant::now();
        let recv_next = st.recv_next.load(Ordering::Relaxed);
        let head_missing = !full && !buf.is_empty() && !buf.contains_key(&recv_next);
        {
            let mut hole = st.hole.lock().unwrap();
            match *hole {
                Some((off, t)) if off < recv_next || buf.contains_key(&off) => {
                    st.sample_hole(now.saturating_duration_since(t));
                    *hole = head_missing.then_some((recv_next, now));
                }
                None if head_missing => *hole = Some((recv_next, now)),
                _ => {}
            }
        }
        let in_order_held = st.in_order_held_locked(buf);
        let hole_bytes = st
            .recv_buffered
            .load(Ordering::Relaxed)
            .saturating_sub(in_order_held);
        let app_backlog = st.buffered_in.load(Ordering::Relaxed) + in_order_held;
        st.recv_hole_max.fetch_max(hole_bytes, Ordering::Relaxed);
        st.app_backlog_max.fetch_max(app_backlog, Ordering::Relaxed);
    }

    /// P1.4: `(hole_bytes, app_backlog)` — out-of-order bytes held behind
    /// a hole vs in-order bytes the app has not taken.
    pub(super) fn recv_evidence(&self, st: &StreamState) -> (u64, u64) {
        let buf = st.recv_buf.lock().unwrap();
        let in_order_held = st.in_order_held_locked(&buf);
        drop(buf);
        let hole_bytes = st
            .recv_buffered
            .load(Ordering::Relaxed)
            .saturating_sub(in_order_held);
        (
            hole_bytes,
            st.buffered_in.load(Ordering::Relaxed) + in_order_held,
        )
    }

    pub(crate) fn tune_recv_windows(&self) {
        let streams: Vec<_> = self
            .inner
            .streams
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect();
        for st in streams {
            self.tune_recv_cap(&st);
        }
    }

    fn tune_recv_cap(&self, st: &StreamState) {
        self.tune_recv_cap_sampled(st, false);
    }

    /// Receiver window: the BDP formula (`2 · rate · rtt`) as the base, and
    /// the P3b probe controller on top. The formula alone is a fixed point
    /// at the floor whenever the ACK loop carries more than one RTT of
    /// queueing (`W' = W · 2R/L`), and the kernel TCP under us builds that
    /// queue on its own; so when the sender is provably window-limited
    /// (DATA keeps ending on our advertised edge) and the app keeps up, the
    /// cap is doubled for `4 · min_rtt`. The probe's own delivery rate is
    /// then compared with the rate before it: it is kept unless delivery
    /// got *worse* (≥ 10 % down) or the app fell behind — with per-path
    /// send budgets (P2) bounding what is on the wire, a larger window
    /// costs memory, not queue, and shrinking it under a sender that has
    /// filled it collapses the advertised window to zero. A kept cap is
    /// held while the rate stays within half of what earned it. `sampled`
    /// = a new rate sample closed this call; decisions happen only then.
    fn tune_recv_cap_sampled(&self, st: &StreamState, sampled: bool) {
        let base = self.recv_cap_target(st);
        let floor = st.initial_window;
        let ceil = floor.saturating_mul(self.inner.cfg.tuning.chan as u32);
        let cur = st.recv_cap.load(Ordering::Relaxed).clamp(floor, ceil);
        let rate = st.deliver_rate_ewma.load(Ordering::Relaxed);
        let app = st.buffered_in.load(Ordering::Relaxed) + st.recv_buffered.load(Ordering::Relaxed)
            > u64::from(cur) / 2;
        let mut c = st.win_ctl.lock().unwrap();
        let held = |c: &crate::stream::WinCtl| {
            if rate.saturating_mul(2) >= c.hold_rate && !app {
                c.hold_cap
            } else {
                0
            }
        };
        let mut cap = cur;
        if sampled {
            let now = Instant::now();
            let min_rtt = self
                .bdp_rtt(st)
                .unwrap_or(Duration::from_micros(self.inner.cfg.tuning.unknown_rtt_us));
            let wl = st.edge_hits.swap(0, Ordering::Relaxed) > 0;
            if app {
                c.hold_cap = 0;
                c.hold_rate = 0;
            }
            match c.probing {
                Some((cap0, rate0, since, next0)) => {
                    let el = now.saturating_duration_since(since);
                    if el >= min_rtt * 4 {
                        let got = st.recv_next.load(Ordering::Relaxed).saturating_sub(next0);
                        let probe_rate = (got as f64 / el.as_secs_f64()) as u64;
                        let worse = probe_rate.saturating_mul(10) < rate0.saturating_mul(9);
                        if worse || app {
                            cap = cap0.max(base);
                            self.inner
                                .metrics
                                .recv_cap_probe_reverted
                                .fetch_add(1, Ordering::Relaxed);
                        } else {
                            c.hold_cap = cur;
                            c.hold_rate = probe_rate.max(rate);
                            self.inner
                                .metrics
                                .recv_cap_probe_kept
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        c.probing = None;
                        c.last_probe_end = Some(now);
                    }
                }
                None => {
                    let cooled = c
                        .last_probe_end
                        .is_none_or(|t| now.saturating_duration_since(t) >= min_rtt * 8);
                    if wl && !app && cooled && cur < ceil && rate > 0 {
                        c.probing = Some((cur, rate, now, st.recv_next.load(Ordering::Relaxed)));
                        cap = cur.saturating_mul(2).min(ceil);
                        self.inner
                            .metrics
                            .recv_cap_probes
                            .fetch_add(1, Ordering::Relaxed);
                    } else {
                        cap = base.max(held(&c));
                    }
                }
            }
        } else if c.probing.is_none() {
            cap = base.max(held(&c));
        }
        drop(c);
        let cap = cap.clamp(floor, ceil);
        st.recv_cap.store(cap, Ordering::Relaxed);
        st.recv_cap_max.fetch_max(cap, Ordering::Relaxed);
    }

    fn recv_cap_target(&self, st: &StreamState) -> u32 {
        let floor = st.initial_window;
        let ceil = floor.saturating_mul(self.inner.cfg.tuning.chan as u32);
        let rate = st.deliver_rate_ewma.load(Ordering::Relaxed);
        let Some(rtt) = self.bdp_rtt(st) else {
            return floor;
        };
        if rate == 0 {
            return floor;
        }
        let bdp = rate as f64 * rtt.as_secs_f64();
        if !bdp.is_finite() {
            return floor;
        }
        let twice = 2.0 * bdp;
        if twice >= f64::from(ceil) {
            ceil
        } else if twice <= f64::from(floor) {
            floor
        } else {
            twice as u32
        }
    }

    fn deliver_min_dt(&self, st: &StreamState) -> Duration {
        self.bdp_rtt(st)
            .unwrap_or(self.inner.cfg.tuning.maintain_interval)
    }

    /// RTT of the loop this stream's DATA actually rides (P3a): the last
    /// arrival path first (a download receiver's sticky is only its request
    /// path), then sticky, then the pool min. Unknown 20 ms is not an RTT —
    /// do not grow.
    fn bdp_rtt(&self, st: &StreamState) -> Option<Duration> {
        for id in [
            st.last_recv_path.load(Ordering::Relaxed),
            st.sticky.load(Ordering::Relaxed),
        ] {
            if id == 0 {
                continue;
            }
            if let Some(p) = self.get_path(id) {
                if p.is_alive() && p.rtt_known() {
                    return Some(p.rtt());
                }
            }
        }
        self.min_alive_fast_rtt()
    }

    pub(super) fn send_ack(&self, st: &StreamState, path_id: u32) {
        let ack = StreamAck {
            stream_id: st.id,
            acked_offset: st.recv_next.load(Ordering::Relaxed),
            window: st.advertised_window(),
            sack: st.sack_ranges(),
        };
        if ack.window == 0 {
            // P1.4: who closed the window — bytes stuck behind a hole, or
            // in-order bytes the application has not read.
            let (hole_bytes, app_backlog) = self.recv_evidence(st);
            if hole_bytes >= app_backlog {
                st.zero_win_hole.fetch_add(1, Ordering::Relaxed);
                self.inner
                    .metrics
                    .zero_window_hole
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                st.zero_win_app.fetch_add(1, Ordering::Relaxed);
                self.inner
                    .metrics
                    .zero_window_app
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        // P3b: the sender may run up to exactly this edge.
        let min_rtt = self
            .bdp_rtt(st)
            .unwrap_or(Duration::from_micros(self.inner.cfg.tuning.unknown_rtt_us));
        st.note_ack_edge(ack.acked_offset + u64::from(ack.window), min_rtt);
        if self.store_ack(st, path_id, &ack) {
            return;
        }
        let sticky = st.sticky.load(Ordering::Relaxed);
        if sticky != 0 && sticky != path_id && self.store_ack(st, sticky, &ack) {
            return;
        }
        if let Some(picked) = self.pick_pref(PickPref::Interactive) {
            if picked != path_id && picked != sticky && self.store_ack(st, picked, &ack) {
                return;
            }
        }
        Self::mark_ack_dirty(st);
    }

    fn store_ack(&self, st: &StreamState, path_id: u32, ack: &StreamAck) -> bool {
        let Some(p) = self.get_path(path_id) else {
            return false;
        };
        {
            let mut g = p.pending_acks.lock().unwrap();
            if !p.is_alive() {
                return false;
            }
            g.insert(st.id, ack.clone());
        }
        Self::mark_ack_dirty(st);
        p.ack_wait.notify_one();
        true
    }

    fn mark_ack_dirty(st: &StreamState) {
        if !st.ack_dirty.swap(true, Ordering::Relaxed) {
            st.ack_flush_from_us
                .store(crate::metrics::mono_us().max(1), Ordering::Relaxed);
        }
    }

    pub(super) fn on_ack(&self, ack: StreamAck) {
        let Some(st) = self.get_stream(ack.stream_id) else {
            return;
        };
        st.send_window.store(ack.window, Ordering::Relaxed);
        self.forget_open(ack.stream_id);
        let prev = st.send_acked.load(Ordering::Relaxed);
        let mut freed = false;
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
                    freed |= self.release_piece(&u);
                }
            }
        }
        // SACK: pieces the receiver holds behind a hole. Delivered, so they
        // must not be hedged again; the path that carried them gets its
        // ACK-clock and inflight credit now, not when the hole fills.
        if !ack.sack.is_empty() {
            let mut unacked = st.unacked.lock().unwrap();
            let drop_keys: Vec<u64> = unacked
                .iter()
                .filter(|(off, u)| {
                    let end = **off + u.data.len() as u64;
                    ack.sack.iter().any(|(a, b)| *a <= **off && end <= *b)
                })
                .map(|(off, _)| *off)
                .collect();
            let n = drop_keys.len() as u64;
            for k in drop_keys {
                if let Some(u) = unacked.remove(&k) {
                    freed |= self.release_piece(&u);
                }
            }
            if n > 0 {
                st.last_sack_ms
                    .store(crate::metrics::mono_ms().max(1), Ordering::Relaxed);
                st.sacked.fetch_add(n, Ordering::Relaxed);
                self.inner
                    .metrics
                    .data_sacked
                    .fetch_add(n, Ordering::Relaxed);
            }
        }
        if freed {
            self.inner.budget_wait.notify_waiters();
        }
        st.send_wait.notify_waiters();
    }

    /// Accounting for one acknowledged piece: path inflight, ACK-clock
    /// sample, and (small frames only) an RTT sample. `true` when a path
    /// got inflight back.
    fn release_piece(&self, u: &Unacked) -> bool {
        let Some(p) = self.get_path(u.path_id) else {
            return false;
        };
        let loaded = p.inflight_bytes();
        p.sub_inflight(u.data.len() as u64);
        let sample = u.last_sent.elapsed();
        let t = &self.inner.cfg.tuning;
        self.note_ack_clock(&p, u, sample);
        // Only small frames (control / interactive). Bulk ACK elapsed time
        // is transfer delay, not path RTT. Skip when the sample waited
        // behind bulk inflight.
        let cap = self.rtt_sample_cap(&p);
        // A lucky-low ACK (fast return path) must not pull a 60 ms class
        // down into the 7 ms set.
        let not_lucky_low = !p.class_known() || sample * 2 >= p.class_rtt();
        if u.data.len() <= t.interactive_max
            && sample > t.ack_rtt_min
            && sample < t.ack_rtt_max
            && sample <= cap
            && not_lucky_low
            && loaded < t.inflight_bias
        {
            p.record_rtt(sample);
        }
        true
    }

    /// P2.2: ACK-clock delivery-rate sample for the path that carried `u`.
    /// `Δd` = bytes ACKed on the path since this piece went out, `Δt` = time
    /// since the ACK that preceded its send. Independent of the standing
    /// queue: a budget-limited path yields `budget/min_rtt` (budget
    /// doubles), a saturated one yields the bottleneck rate. Only un-hedged
    /// pieces sample (a hedged copy's clock started on another path).
    fn note_ack_clock(&self, p: &crate::path::PathState, u: &Unacked, sample: Duration) {
        let now_us = crate::metrics::mono_us().max(1);
        let len = u.data.len() as u64;
        let delivered = p.delivered.fetch_add(len, Ordering::Relaxed) + len;
        let bulk = len > self.inner.cfg.tuning.interactive_max as u64;
        if u.tried.len() == 1 {
            let dd = delivered.saturating_sub(u.delivered_at_send);
            let ack_us = if u.delivered_time_at_send_us == 0 {
                sample.as_micros() as u64
            } else {
                now_us.saturating_sub(u.delivered_time_at_send_us)
            };
            // BBR: the sample interval is the longer of the send-side and
            // ACK-side clocks, so a burst of coalesced ACKs cannot read as
            // a rate the sender never achieved.
            let snd_us = u.sent_us.saturating_sub(u.first_tx_at_send_us);
            let dt = ack_us.max(snd_us);
            let min_rtt_us = p
                .min_rtt()
                .map(|d| d.as_micros() as u64)
                .unwrap_or(self.inner.cfg.tuning.unknown_rtt_us);
            if dd > 0 && dt >= min_rtt_us / 4 && dt > 0 {
                let rate = (dd as u128 * 1_000_000 / dt as u128).min(u64::MAX as u128) as u64;
                p.note_bw_sample(rate, now_us);
            }
            p.note_loop_rtt(sample);
            if bulk {
                p.record_ack_rtt(sample);
            }
            // Newest delivered piece moves the send-side clock forward.
            if u.sent_us > p.first_tx_us.load(Ordering::Relaxed) {
                p.first_tx_us.store(u.sent_us, Ordering::Relaxed);
            }
        }
        if bulk {
            self.inner
                .metrics
                .ack_loop_ms
                .observe(sample.as_millis() as u64);
        }
        p.delivered_at_us.store(now_us, Ordering::Relaxed);
    }

    pub(super) fn on_peer_close(&self, close: StreamClose) {
        let Some(st) = self.get_stream(close.stream_id) else {
            return;
        };
        st.note_close_started();
        let off = close
            .final_offset
            .unwrap_or_else(|| st.recv_next.load(Ordering::Relaxed));
        let _ =
            st.recv_close_off
                .compare_exchange(u64::MAX, off, Ordering::SeqCst, Ordering::Relaxed);
        self.try_finish_recv_close(&st);
    }

    fn apply_recv_fin(&self, st: &StreamState) {
        if !st.recv_fin.swap(true, Ordering::SeqCst) {
            let _ = st.inbound_tx.try_send(Inbound::Close);
        }
        self.unstick(st);
        self.forget_close(st.id);
        self.maybe_count_graceful(st);
    }

    fn try_finish_recv_close(&self, st: &StreamState) {
        let off = st.recv_close_off.load(Ordering::Relaxed);
        if off == u64::MAX || st.recv_fin.load(Ordering::Relaxed) {
            return;
        }
        if st.recv_next.load(Ordering::Relaxed) < off {
            return;
        }
        self.apply_recv_fin(st);
    }

    /// Belt: FIN only when `recv_next >= off`. Not a clock.
    pub(super) fn expire_recv_closes(&self) {
        let sts: Vec<Arc<StreamState>> = self
            .inner
            .streams
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect();
        for st in sts {
            self.try_finish_recv_close(&st);
        }
    }

    pub(super) fn on_peer_reset(&self, id: u32, reason: ResetReason) {
        self.forget_reset(id);
        self.finish_stream(id, Some(reason), false);
    }
}
