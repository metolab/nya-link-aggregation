# Bulk goodput II: per-path send budget, ACK-clock bandwidth, hedge-on-silence, kernel queue bound

| Field | Value |
| --- | --- |
| **Title** | Remove the 128 KiB/loop floor lock, the 20 ms bulk hedge storm, and per-5-tuple kernel bufferbloat; make the limiter observable |
| **Author** | nya-link-aggregation maintainers |
| **Date** | 2026-09-14 |
| **Status** | **Implemented** on `main` (post-v0.1.5); see "Implementation status" below for deviations from the draft |
| **Audience** | Senior engineers in `nya-core` session / scheduler / path IO, `nya-client` dial, `nya-server` accept, `nya-e2e` WAN emulation |
| **Predecessor** | `docs/design-hytron-bulk-goodput.md` (**Implemented** in v0.1.4: leftover Reset, ACK register, write-stall pick-skip, HOL, window auto-tune). `docs/design-interactive-class-set.md` (v0.1.5). This document does **not** reopen those; it fixes what v0.1.5 production shows they left. |
| **Compatibility** | `PROTOCOL_VERSION` **stays 2**. One backward-compatible wire addition made during implementation: `StreamAck` may carry trailing SACK ranges (old decoders stop at `window`; old encoders produce none). No new TOML keys; `[session]` stays `deny_unknown_fields`. One production `Tuning::STANDARD`. Do **not** retune `initial_window`, `chan`, `inflight_bias`, `loss_timeout_*`, `down_*`, `close_linger`, `interactive_max`, `class_drop_*`, `path_score` weights. New mechanism constants are **derived** from existing ones or are textbook control constants (gain 2, α = 1/8, BBR 10-RTT window) and are documented at the definition site. |
| **Intended repo path** | `docs/design-path-budget-ack-clock.md` |

---

## Overview

v0.1.5 on all three production instances (`prod-gz-yuusei`, `prod-gz-hytron`, `prod-gz-datawave`, 35 h window 2026-09-12T15:35Z → 09-14T02:20Z) confirms the v0.1.4 mechanisms landed: leftover table `held == live` (Hytron 35/54 vs 419/36 before), `ack_flush_us` p50 < 0.5 ms, server bulk/urgent queues ≈ 0, `frame_send_drop` 65/149 per h (was 22k/52k), `migrates_send_blocked` 0 (was 2079/h), stream success 99.9 % (was 96–98 %), hedge collapsed 20–40×. Typical Hytron download went from 100–150 KB/s to **p50 ≈ 450 KB/s**, single hops up to 4.2 MB/s (44 MB) and 5.1 MB/s (Yuusei, 180 MB).

Four holes remain, all mechanism, none tuning:

1. **Per-stream window is bistable at the floor.** `recv_cap = clamp(2·deliver_rate·rtt_fast, 128 KiB, 8 MiB)` (`streams.rs` L579–601). `deliver_rate` is the window-limited throughput `cap / loop`; `rtt_fast` is the *quiet* path RTT (loaded Pong/ACK samples are capped out by `rtt_sample_cap`). So `cap' = 2·cap·rtt_fast/loop`: the window grows **iff `loop < 2·rtt_fast` (≈ 20 ms on this pool)** and otherwise sits at 128 KiB forever. Any queueing delay above one RTT on the sticky 5-tuple — other streams' bytes in the kernel send buffer, hedge copies, TCP recovery — locks the stream at `128 KiB / loop`. Production: server `window_blocks` ≈ 4/s during a 370 MB download at 0.6 MB/s ⇒ **128–300 KB per block, loop ≈ 200–250 ms** while path RTT is 10 ms and ACK flush is < 1 ms. Server `stall_ms` (ACK gap ≥ 20 ms): p50 ≈ 150–200 ms, p75 ≈ 1 s, p90 ≈ 4 s. The user-visible shape "first seconds at ~100 KB/s, then very fast" is this fixed point flipping when the loop happens to drop below 20 ms: `128 KiB / 1.3 s ≈ 100 KB/s`, then doubling per loop up to 8 MiB. The controller code is correct (verified locally: 11.5 MB/s × 11.9 ms → cap 275 KB = 2×BDP); the **measurement** is what it cannot be: a window-limited rate against a quiet RTT.
2. **Bulk hedge is a per-piece 20 ms clock on top of in-order TCP.** `retry_expired_unacked` (`mod.rs` L629–684) re-sends any unacked piece whose `last_sent.elapsed() ≥ retry_after` (= `loss_timeout(min_alive_fast)` = 20 ms floor) onto another path, resets `last_sent`, and the last rung of `pick_retry_path` (`scheduler.rs` L620–625) always yields a dest. A piece whose true ACK loop is 200 ms is copied ~10 times across the pool; the only bounds are the A3 stall ≥ 1 s + all-8-tried rule and write-stalled skips. Production: server hedge 4226/h (1.6 per stream); receiver overlay payload 8.57 GB vs application payload 7.37 GB ⇒ **≈ 14–16 % of download bytes on Hytron are duplicates** (Yuusei ≈ 18 %, Datawave ≈ 4 %). Duplicates land on *other* paths' kernel queues and inflate everyone's loop — hole 1 feeds on hole 2. The structural fact the clock ignores: a path is one TCP connection; if **any** later byte on that TCP was acknowledged, every earlier frame on it was delivered. Per-piece loss does not exist under the overlay; only path silence does.
3. **No per-5-tuple send bound.** The overlay bounds bytes per *stream* (window) but not per *path*. The writer (`path.rs` L991–1031) pushes bulk frames into a `FramedWrite` until the kernel send buffer (autotuned to MBs) refuses; nothing stops N streams (plus hedges) from parking megabytes on one TCP. That queue is the `loop` in hole 1, the hedge trigger in hole 2, and the reason Pings/ACKs on that TCP see hundreds of ms. Hytron 1-min download peak over 35 h is **2.48 MB/s** while upload sustained **7.5 MB/s**; the pool has 8 paths.
4. **Duplicate DATA after FIN is never ACKed.** `deliver_data` (`streams.rs` L492–494) returns before `send_ack` when `recv_fin` is set. A sender whose last ACK was lost with the path retries forever (hole 2), then sits stalled until linger. Yuusei server: 567 stall samples > 10 s averaging ~200 s each; Hytron client 22k samples > 10 s.

Plus a blind spot: nothing today tells us **which layer is the limiter** (stream window, path budget, application, kernel TCP cwnd/loss, origin). 60 s metrics could not resolve the ramp; the analysis above is inference. Hytron download ≪ upload could be HK→GZ ingress congestion, CUBIC collapsing on 1–2 % loss, or our own bloat; the design must make that answerable from Signoz.

### What this design does

| # | Mechanism | Hole |
| --- | --- | --- |
| **P1** | `TCP_NOTSENT_LOWAT` on every overlay path socket (client dial + server accept); best-effort `TCP_CONGESTION=bbr` on Linux, decision recorded | 3 (kernel side), 1, 2 |
| **P2** | **Per-path send budget** = overlay cwnd: `clamp(2·bw_p·min_rtt_p, inflight_bias, chan·MAX_STREAM_PAYLOAD)`, `bw_p` from **ACK-clock** delivery-rate samples (BBR-style windowed max), enforced at bulk placement with fan-out to same-class paths that have room, wait when none | 3 (overlay side), 1 |
| **P3** | Receiver window controller uses the **arrival path's** RTT; sender-side **limiter attribution** (window vs budget vs app) so the residual bistability, if any after P2, is measured not guessed. Probe-based growth (P3b) is specified but gated on that evidence | 1 |
| **P4** | **Hedge on path silence, not piece age.** Bulk pieces rehome only when their path is not `is_loss_fresh` (or the frame was dropped before the wire), with per-piece exponential backoff and a `down_timeout` belt. Interactive pieces keep today's clock | 2 |
| **P5** | ACK duplicates after `recv_fin` / past `close_off` / below `recv_next` | 4 |
| **P6** | Observability: per-path `TCP_INFO` gauges (Linux), per-path `bw`/`budget`/`ack_rtt`, limiter counters, duplicate-bytes counter, `recv_cap_max` histogram, per-stream attribution on `nya.hop` spans; e2e WAN gets a **rate + queue** bottleneck so all of the above is testable | blind spot |

Nothing here changes the TOML, `PROTOCOL_VERSION`, `failbacks`, all-down, class/failback clocks, Close/Reset delivery, or the leftover contract. The only wire change is the backward-compatible SACK tail on `StreamAck` (see below).

### Implementation status (2026-09-14, `main`)

All of P1–P6 landed, in the PR-plan order, as direct commits on `main`. Deviations from the draft, each forced by the e2e bottleneck (PR 5) rather than by taste:

| Area | Draft | Shipped | Why |
| --- | --- | --- | --- |
| P2 budget | closed form `clamp(2·bw·min_rtt, floor, ceil)` | **Growth controller** (`PathState::end_budget_round`): per min-RTT round, a step ×1.5 needs two consecutive rounds ≥ +15 % over the last confirmed bandwidth; 3 flat rounds ⇒ full, then a ×1.25 probe every 4 limited rounds (kept if bandwidth rose ≥ 7.5 %); 3 sag rounds or 8 unlimited rounds ⇒ shrink to `2·bw·ack_rtt`. Ceiling `3·bw_max·min_rtt` with `bw_max` fed the min of two consecutive rounds over 10 s | The closed form is a fixed point at the floor under a shared bottleneck: `bw` measured under a budget is at most `budget/loop`, so the budget can never exceed what it already carries. BBR resolves this with a pacing gain overshoot; a cwnd-only budget needs an explicit "grow while it buys throughput" test. Gain 2 starved the forward direction under a reverse bulk flow (measured 75–85 % vs 97 % at 3) |
| P3b | specified, gated on production evidence | **Shipped** (`tune_recv_cap_sampled`, `edge_ring`, `recv_cap_probes/kept/reverted`) | `bulk_bottleneck_single` reproduced the floor lock locally: with P2 in place the window, not the path, was the limiter (`window_limited_with_room / window_blocks` > 0.5) |
| P4 trigger | rehome when path `!is_loss_fresh` | rehome when path silent for `retry_after_bulk` (= `clamp(2·ack_rtt, loss, down)`) **and** the piece itself is at least that old; `dropped` and the `down_timeout·2^(tries−1)` belt unchanged | `is_loss_fresh` alone fired during the emulated TCP's Reno recovery (a healthy path that is briefly quiet), producing the very hedges `hedge_only_on_silence` forbids |
| ACK | cumulative only | **SACK ranges** on `StreamAck` (`MAX_SACK_RANGES`, trailing, optional; `data_sacked` counter) | With per-piece hedging, one hole on one path makes every later piece behind it look unacked on the cumulative ACK, and the belt then re-sends all of them — a duplicate storm proportional to the budget. SACK lets the sender release what arrived out of order |
| Receiver drain | ACK only on DATA arrival | `note_app_read` drains `recv_buf` and re-ACKs | A window shrunk by P3b's revert could reach 0 and stay there when no new DATA arrived to trigger the next ACK |
| `drain_recv` | — | holds `recv_buf` across `try_send` | Concurrent deliveries on fan-out paths could interleave and hand the app out-of-order bytes (`bulk_fanout_three_paths` `intact=false`) |
| e2e emulator | rate + queue | plus Reno cwnd (slow start / halving), time-based RACK reordering window, Linux-like RTO + TLP, one ordered wire task per pipe, `cwnd_fwd/rev` and `queue_drops` in `LinkStats` | A rate/queue without congestion control made every overflow a hard loss with no recovery and produced spurious reordering from timer granularity; the bottleneck scenarios were measuring the emulator |

Measured on the e2e bottleneck (50 Mbps / 10 ms / 128 KiB queue, release binary, `--jobs 4`), before core changes (commit `a47e194`, e2e only) → after:

| Scenario | Before | After (two consecutive full catalogs) |
| --- | --- | --- |
| `bulk_bottleneck_single` 12 MiB | 87 % of link | 5.9–6.1 MB/s ≈ **95–97 %**, 0 hedges, 0 dup; occasional Reno-loss run at 74–77 % (gate 70 %, see scenario comment) |
| `bulk_shared_two_streams` 2×6 MiB | 52 % aggregate, **FAIL** (queue overflow) | 6.1–6.2 MB/s aggregate ≈ **98 %**, 0 dup |
| `bulk_fanout_three_paths` 12 MiB over 3×20 Mbps (64 KiB queues; gate = 80 % of two links) | timeout at 60 s, 298 MB sent for 12 MiB, 34 664 hedges | **2.1–2.2 s** = 6.0 MB/s ≈ 2.4 links' worth, 0 hedges, 0 dup |
| `ping_under_bulk_bounded` | p99 ≈ 250 ms | p99 **84–111 ms** |
| `hedge_only_on_silence` | **FAIL** (hedges on the healthy phase, copy errors) | PASS, dup_rx 0.5 %, hedges only after the blackhole |
| Full short catalog (51) | — | 51/51, twice; `stream_lifecycle` PASS |

Production follow-up (unchanged from the Rollout Plan): the limiter counters and `nya.hop` attributes now answer "window / budget / path / kernel TCP" per stream; the next pull should read `nya.limiter` distribution on ≥ 8 MB Hytron hops, `nya_path_tcp_cwnd_bytes` vs `nya_path_budget_bytes`, and `nya_data_dup_rx_bytes_total / nya_bytes_data_rx_total` (was 14–18 %).

---

## Background & Motivation

### Production evidence (Signoz, pulled 2026-09-14, window 09-12T15:35Z → 09-14T02:20Z, ≈ 35 h)

All three instances `service.version=0.1.5` both ends; client processes restarted 09-12T15:34Z and lived the whole window; `sessions_live=1/1`, `sessions_dead=0`, `session_all_down_resets=0`, `failbacks=0`.

**Hytron per hour (client / server), three binaries**

| metric | 0.1.3 (47.8 h) | 0.1.4 (33.3 h) | 0.1.5 (34.8 h) |
| --- | --- | --- | --- |
| stream success | 97.9 % / 96.0 % | 99.85 % / 99.88 % | 99.89 % / 99.87 % |
| `stream_resets` | 107 / 215 | 5 / 4 | 3 / 3 |
| `frame_send_drop` | 22563 / 51969 | 50 / 149 | 65 / 149 |
| `migrates_send_blocked` | 45 / 2079 | 0 / 1 | 0 / 0 |
| `data_hedge` | 97540 / 53176 | 2514 / 4263 | 2576 / 4226 |
| `data_retransmit` | 28552 / 18572 | 155 / 414 | 65 / 187 |
| `hol_rebalances` per stream (server) | 10.7 | 1.1 | 1.1 |
| `path_down` | 399 | 108 | 173 (Yuusei 190, Datawave 158 — IX, not version) |
| stall avg (server) | 1558 ms | 2267 ms | 2396 ms |

**Throughput**

| Signal | Value |
| --- | --- |
| Hytron `217.116.175.244` downloads, dur < 120 s (n=164) | KB/s p10 115, **p50 450**, p90 2743, max 6401 |
| Hytron largest fast hop | 44.5 MB in 10.6 s = **4.2 MB/s** |
| Yuusei largest hop | 180 MB in 35.2 s = **5.1 MB/s** |
| Hytron long downloads (same host, overlapping) | 383 MB @ 375 KB/s, 370 MB @ 673, 299 MB @ 591, 155 MB @ 151, 105 MB @ 87, 97 MB @ 101 **and** 96 MB @ 871 in the same minute |
| Hytron 1-min **download** peak (server `bytes_data_tx`) over 35 h | **2.48 MB/s**; p99 0.95 MB/s |
| Hytron 1-min **upload** peak (client `bytes_data_tx`) | **7.54 MB/s**, sustained 6–7 MB/s for 3-minute runs |
| Server `window_blocks` | 60583 / 35 h; ≈ 4/s during the 370 MB download at 0.5–1.2 MB/s ⇒ 128–300 KB per block |
| Server bulk / urgent queues, `path_congested` during that download | 0 / 0 / 0 (1-min avg per path) |
| Server per-path `inflight_bytes` during it | 20–50 KB avg |
| `ack_flush_us` | client 75 % ≤ 500 µs, 99.99 % ≤ 20 ms; server 68 % ≤ 500 µs |
| Server `stall_ms` distribution | ≤ 50 ms 25 %, ≤ 200 ms 57 %, ≤ 1 s 70 %, ≤ 5 s 94 %, > 10 s 4.8 % (20k samples) |
| Client overlay `bytes_data_rx` vs Σ `nya.hop` `rx_bytes` (client) | 8.57 GB vs 7.37 GB (Hytron); 658 MB vs 539 MB (Yuusei); 192 MB vs 184 MB (Datawave) |
| Yuusei server stall samples > 10 s | 567, ≈ 200 s each (≈ 116 ks of the 136 ks total) |

The window is binding (blocks), the overlay queues are empty, the ACK path is sub-millisecond, path RTT is 10 ms — the 200 ms is **inside the kernel TCP send buffers and the network**, and it is what the window controller divides by.

### Line-accurate current code

**Receiver window** — `crates/nya-core/src/session/streams.rs`

```574:601:crates/nya-core/src/session/streams.rs
    fn tune_recv_cap(&self, st: &StreamState) {
        st.recv_cap
            .store(self.recv_cap_target(st), Ordering::Relaxed);
    }

    fn recv_cap_target(&self, st: &StreamState) -> u32 {
        let floor = st.initial_window;
        let ceil = floor.saturating_mul(self.inner.cfg.tuning.chan as u32);
        let rate = st.deliver_rate_ewma.load(Ordering::Relaxed);
        let Some(rtt) = self.bdp_rtt(st) else {
            return floor;
        };
        // ...
        let bdp = rate as f64 * rtt.as_secs_f64();
        // ...
        let twice = 2.0 * bdp;
```

`bdp_rtt` (L610–620) uses **`st.sticky`** — on the receiver of a download that is the path the receiver used for its own request, not the path DATA arrives on (`st.last_recv_path`). `note_deliver` (`stream.rs` L143–177) coalesces ≥ one RTT and so measures `bytes / wall-time` including the idle wait for the sender's window.

**Sender window wait** — `send_data` L180–201 counts `window_blocks` once per wait; `on_ack` L666–711 stores `ack.window` and samples path RTT only for `data.len() <= interactive_max && loaded < inflight_bias`.

**Bulk hedge** — `crates/nya-core/src/session/mod.rs`

```646:684:crates/nya-core/src/session/mod.rs
        let expired: Vec<(u64, u32, Vec<u8>, Vec<u32>)> = {
            let unacked = st.unacked.lock().unwrap();
            unacked
                .iter()
                .filter(|(_, u)| {
                    u.last_sent.elapsed() >= self.retry_after(u.path_id)
                        && now >= u.retry_not_before
                })
        // ...
        for (offset, from, data, mut tried) in expired {
            if stalled_long && alive.iter().all(|id| tried.contains(id)) {
                continue;
            }
            if self.get_path(from).is_some_and(|p| p.is_alive() && p.is_write_stalled()) {
                continue;
            }
            Self::push_tried(&mut tried, from);
            let Some(alt) = self.pick_retry_tried(&tried) else { continue; };
            // ...
            if self.send_data_frame(st.id, offset, data, alt) {
                if let Some(u) = st.unacked.lock().unwrap().get_mut(&offset) {
                    self.rehome_unacked(u, alt);
                }
                self.note_retry(from, alt);
```

`retry_after` (L590–598) = `loss_timeout(min_alive_fast_rtt)`, 20 ms floor. `rehome_unacked` (L1247–1253) resets `last_sent`, so the same piece expires again 20 ms later. `pick_retry_path` last rung (`scheduler.rs` L620–625) `p.is_alive() && p.id != current`.

**Path writer** — `crates/nya-core/src/path.rs` L991–1031: `rx.recv()` (bulk) → `write_one` → `send_frame` → `framed.send().await`; the only back-pressure is the kernel. `write_one` flags `write_stalled` after `write_deadline` (20 ms floor) but keeps writing. Sockets: `tls.rs` L195 and `nya-server/src/lib.rs` L194 set only `TCP_NODELAY`.

**Placement** — `bulk_affinity` (`mod.rs` L519–539) returns the sticky whenever alive, not-congested-or-write-stalled, and `is_loss_fresh`; `pick_pref(Any)` scores `class·load·1024 + fast·load` with `load = 1 + inflight/inflight_bias + sticky` (`scheduler.rs` L30–38, L244–256). Nothing caps `inflight` per path.

**Post-FIN duplicate** — `streams.rs` L488–518:

```492:505:crates/nya-core/src/session/streams.rs
        if st.reset.load(Ordering::Relaxed) || st.recv_fin.load(Ordering::Relaxed) {
            return;
        }
        st.last_recv_path.store(path_id, Ordering::Relaxed);
        let close_off = st.recv_close_off.load(Ordering::Relaxed);
        if close_off != u64::MAX && data.offset >= close_off {
            return;
        }
        let mut buf = st.recv_buf.lock().unwrap();
        if data.offset < st.recv_next.load(Ordering::Relaxed) {
            drop(buf);
            self.send_ack(&st, path_id);
            return;
        }
```

Only the third early-return ACKs.

### Why the current window formula is a fixed point at the floor

Let `W` = advertised cap, `R` = quiet path RTT (`rtt_fast`), `Q` = extra delay in the ACK loop (kernel queue ahead of us, recovery), `L = R + Q` = loop. Window-limited: `rate = W/L`. Then

```text
W' = 2 · (W/L) · R = W · 2R/L            grows  ⇔  L < 2R  ⇔  Q < R
```

With `R = 10 ms`, **10 ms of queueing on the 5-tuple is enough to freeze the window at 128 KiB**. 10 ms at 5 MB/s is 50 KB — less than one other stream's window. When the path is *ours alone* and not saturated, `Q ≈ 0` and the formula doubles per loop to `2·BDP` — that is the case the v0.1.4 unit tests cover and the case the local harness reproduces. Production paths are shared by 35–70 live streams and by hedge copies; `Q ≫ R` is the norm. The receiver cannot fix this by measuring better: in a shared FIFO its arrival rate *is* its share. Only bounding `Q` (P2 + P1) restores `L ≈ R` and lets the existing formula work; P3b probing is the fallback if `Q` from outside the overlay (TCP recovery) still keeps `L > 2R`.

### Why hedge must be per path, not per piece

A path is a single TCP connection with in-order reliable delivery. For any two frames enqueued on the same path, the later one cannot be delivered before the earlier one. Therefore if the sender observes **any** receive on path `p` (STREAM_ACK for any stream, Pong) after frame `f` was written, `f` was delivered; if its own ACK has not come back, the ACK is in flight, batched in the register, or was lost with a *return* path (already merged by `merge_pending_acks` on `path_failed`). Re-sending `f` on another path in that situation is pure waste. The one legitimate hedge trigger is **silence on `p`** (`!is_loss_fresh(p)` — no rx for `loss_timeout(min(fast, class))`) or `p` dead (`path_failed` → `rehome_unacked_from`, unchanged). The clock therefore belongs to the path, and `is_loss_fresh` already exists.

Two exceptions keep correctness: a frame that never reached the wire (`park_stream_data` / `send_on_path` drop — today `frame_send_drop`) and a receiver that silently drops (unknown stream after early-data expiry). Both are handled with an explicit flag and a `down_timeout` belt below.

### Why the kernel queue must be bounded from both sides

`TCP_NOTSENT_LOWAT` makes the socket un-writable once unsent bytes exceed the threshold, so the kernel holds at most `cwnd + lowat` per connection. That alone bounds Ping/ACK latency on a saturated 5-tuple to `(cwnd + lowat)/bw` and turns `write_stalled` into a prompt "this TCP is saturated" signal. It does **not** stop the overlay from parking a megabyte in the bulk mpsc (`chan = 64` frames) or from choosing that path for the next stream; P2 does that with unacked-byte accounting the scheduler already has (`inflight`).

---

## Goals & Non-Goals

### Goals

1. A single bulk stream on an otherwise idle path reaches ≥ 80 % of that path's TCP delivery rate within 1 s and stays there; no bistable floor lock.
2. N bulk streams on one saturated path share it with **zero** hedge copies and bounded per-path queueing (`ack_rtt_p` ≤ ~3× quiet RTT).
3. One bulk stream fans out across same-class paths once its sticky path is at budget; aggregate ≥ 2× single-path on a 3-path pool in e2e.
4. Duplicate payload bytes at the receiver ≤ 2 % of delivered bytes (from 14–18 %).
5. Post-FIN duplicates are ACKed; no stall > 10 s attributable to a lost final ACK.
6. From Signoz alone, attribute a slow transfer to: stream window / path budget / application / kernel TCP (cwnd, loss) / origin.
7. Keep every v0.1.3–v0.1.5 invariant: leftover `held == live`, Close/Reset delivery, `failbacks = 0`, all-down semantics, interactive TTFB (`interactive_class_set`, 20 ms interactive hedge), write-stall pick-skip, HOL rules, Yuusei hop-RST ≈ 1/h.

### Non-Goals

- No wire change, no `PROTOCOL_VERSION 3`, no ACK flags, no per-frame timestamps.
- No new TOML keys; no `Tuning::STANDARD` numeric change; no `initial_window` / `chan` growth.
- Not a TCP congestion-control reimplementation. P2 is an overlay cwnd over a reliable substrate; it borrows BBR's *estimator* (ACK-clock rate, windowed max, min RTT), not its state machine.
- Not fixing HK→GZ IX loss or origin speed. Making them **visible** is in scope.
- No change to `open_stream` pick, class / failback clocks, `maybe_hol`, correlated silence, outlier recycle.
- No idle-GC of streams; no change to linger / Residual D.

---

## Key Decisions

| # | Decision | Why |
| --- | --- | --- |
| KD1 | Bound the per-path queue at **two** layers: kernel (`TCP_NOTSENT_LOWAT`) and overlay (`budget_p` on unacked bytes). | Either alone leaks: lowat leaves the bulk mpsc and scheduler blind; overlay budget cannot stop the kernel from autotuning to 4 MB when the estimate overshoots. |
| KD2 | Budget is on **unacked bytes** (`PathState.inflight`), not on queued frames. | `inflight` already exists, is transferred on rehome (`xfer_inflight`), released on reap; it is the overlay's true in-flight. Queued frames miss bytes already in the kernel. |
| KD3 | `bw_p` = **sender-side ACK-clock** delivery rate with a windowed max over `max(10·min_rtt_p, 100 ms)`; `min_rtt_p` = existing quiet `rtt()`. | Textbook (BBR). ACK-clock (`Δdelivered / Δdelivered_time`) measures the bottleneck when a queue exists and `budget/RTT` when we are budget-limited; with gain 2 that doubles until the bottleneck is found. Receiver-side arrival rate is equivalent but the sender owns `inflight`. |
| KD4 | Budget floor = `inflight_bias` (64 KiB), ceil = `chan × MAX_STREAM_PAYLOAD` (≈ 1 MiB). | Both exist. Floor is 6.4 MB/s at 10 ms per path, 51 MB/s over 8 paths — above any observed production aggregate, so a wrong estimate cannot regress below today's peaks. Ceil equals the bulk queue depth; more is bufferbloat by construction. |
| KD5 | Budget applies to **bulk STREAM_DATA placement only**. Interactive DATA, control, ACKs, retries bypass but still count in `inflight`. | TTFB and Close/Reset delivery unchanged; retries are rare after P4. |
| KD6 | When the sticky is at budget, **fan out** a bulk stream onto another `fastest_class_set` path with room; keep `sticky` unchanged. Only when no path has room, wait. | This is link aggregation. Same-class bound keeps reordering to one class RTT spread. Sticky stays so HOL / affinity semantics are untouched. |
| KD7 | Bulk hedge trigger = **path not `is_loss_fresh`** (or frame dropped), with per-piece exponential backoff and a `down_timeout(p)` belt. Interactive pieces keep the 20 ms per-piece clock. | In-order TCP argument above. Interactive hedge is the TTFB mechanism from `design-interactive-ttfb-rto.md`; a 1500 B copy costs nothing. |
| KD8 | Receiver window: fix the RTT source (arrival path) now; **probe-based growth (P3b) only if P6 shows `window_limited_with_room` after P2.** | P2 restores `L ≈ R` where the formula is correct; adding a probing state machine without evidence is complexity risk. The evidence counter is specified and cheap. |
| KD9 | Post-FIN duplicate ACK is unconditional as long as the `StreamState` exists. | An ACK is 13 bytes on the register; it ends a sender stall. `reset` streams are excluded — peer Reset follows. |
| KD10 | `TCP_CONGESTION="bbr"` is set **best-effort on Linux** for overlay path sockets, logged once; failure is silent fallback. | Overlay TCPs cross a 1–2 % loss IX; CUBIC ∝ 1/√p. This is which transport the overlay asks for, not a Tuning number. Ops can still override via sysctl by refusing the module. Reversible: one line. |
| KD11 | `TCP_INFO` is read from a `dup()`ed fd owned by `PathState`, closed on path IO exit. | Reading the original fd races with fd reuse after the task drops the socket. |
| KD12 | e2e WAN emulation gains a token-bucket rate and a byte-bounded FIFO. | Without a bottleneck none of P1–P4 is testable; `packet_wan` today caps cwnd at 64 × 1200 B and has no rate. |

---

## Proposed Design

### Architecture

```text
                 sender (either end)                                  receiver
  origin/app ─▶ TunnelStream duplex(128 KiB) ─▶ pump ─▶ send_data
                                                        │  stream window (recv_cap from ACK)   ◀── advertised = cap − buffered_in − recv_buffered
                                                        │  P2 path budget: pick sticky if room, else same-class with room, else wait
                                                        ▼
                                              PathState  inflight ≤ budget_p = clamp(2·bw_p·min_rtt_p, 64 KiB, 1 MiB)
                                                        │  bw_p ← ACK-clock samples (on_ack), windowed max
                                                        ▼
                                              bulk mpsc(chan) ─▶ writer ─▶ FramedWrite/TLS ─▶ kernel: unsent ≤ NOTSENT_LOWAT (P1)
                                                                                              cwnd, retrans, rtt ── TCP_INFO gauges (P6)
  maintain 5 ms: retry_expired_unacked  ── P4: rehome only if !is_loss_fresh(path) || dropped; backoff; down_timeout belt
                                                                                                        deliver_data ── P5: ACK dup after FIN
```

### P1 — Kernel queue bound and transport choice

**Where.** One helper, called at both socket creation sites:

- `crates/nya-core/src/tls.rs` `connect_pinned` after L195 (`set_nodelay`), before the TLS connector.
- `crates/nya-server/src/lib.rs` `serve_one` after L194.

```rust
// crates/nya-core/src/net.rs (new)
pub struct SocketTuning { pub notsent_lowat: Option<u32>, pub congestion: Option<String> }

/// Overlay path sockets only. Origin / SOCKS sockets are not touched.
pub fn tune_path_socket(tcp: &tokio::net::TcpStream, tuning: &Tuning) -> SocketTuning
```

**What.**

- `TCP_NOTSENT_LOWAT = tuning.inflight_bias as u32` (64 KiB). Linux (`libc::TCP_NOTSENT_LOWAT`, 3.12+) and macOS (`TCP_NOTSENT_LOWAT` 0x201). Failure → `None`, `debug!` per socket, `warn!` once per process (static `Once`).
- `TCP_CONGESTION = "bbr"` — Linux only, `setsockopt(IPPROTO_TCP, TCP_CONGESTION, b"bbr")`; on error read back the current name via `getsockopt` and report it. `info!` once per process: `overlay tcp congestion=<name> notsent_lowat=<n>`; export as a process-level info gauge `nya_process_tcp_cc{cc="bbr"} 1`. Not attempted on macOS.
- `nya-core` gains `libc.workspace = true` (already a workspace dep, used by `nya-obs`).

**Why `inflight_bias`.** Not-sent bytes are beyond cwnd; the kernel still keeps a full cwnd on the wire. 64 KiB unsent = 4 frames of writer lead — enough to keep the writer ahead of the NIC at 1 Gbps × 10 ms (BDP 1.25 MB sits in cwnd, not in unsent), small enough that at 1 MB/s an urgent frame waits ≤ 64 ms behind bulk instead of seconds.

**Effect on `write_stalled`.** `send_frame` now returns `Pending` when unsent > 64 KiB. On a path draining at `bw`, a 16 KiB write can wait up to `64 KiB / bw`: 13 ms at 5 MB/s (< 20 ms deadline), 64 ms at 1 MB/s (> deadline ⇒ `write_stalled`). That is the intended C-design meaning of write-stall (pick-skip for *new* interactive/Open, bulk keeps flowing, path not torn). It now fires when the 5-tuple is saturated rather than when the kernel buffer is exhausted. `bulk_affinity` keeps a write-stalled sticky (L530). Test: `write_stall_under_lowat_does_not_kill_bulk`.

### P2 — Per-path send budget with ACK-clock bandwidth

#### P2.1 Path state

`PathState` (`path.rs` L30–83) gains:

```rust
/// Bytes acknowledged on this path, cumulative. Advances in on_ack for pieces whose u.path_id == self.id.
pub delivered: AtomicU64,
/// Instant of the last `delivered` advance (ACK clock). mono_us; 0 = never.
pub delivered_at_us: AtomicU64,
/// Windowed max of delivery-rate samples, bytes/s. 3-slot minmax over max(10·min_rtt, 100 ms).
bw_filter: Mutex<MinMax3>,
/// Loaded ACK RTT: EWMA(α=1/8) of last_sent→ack for un-hedged bulk pieces on this path. µs; 0 = unknown.
pub ack_rtt_us: AtomicU64,
/// dup()ed fd for TCP_INFO (P6). None off-Linux or after IO exit.
tcp_fd: Mutex<Option<RawFd>>,
```

`Unacked` (`stream.rs` L24–31) gains:

```rust
/// P2: path.delivered / path.delivered_at_us captured at (re)send. Sample only if tried.len() == 1.
pub delivered_at_send: u64,
pub delivered_time_at_send_us: u64,
/// P4: enqueue failed (park drop / send_on_path false). Retry ignores freshness; cleared on successful enqueue.
pub dropped: bool,
```

Set in `send_data` (L257–267), `rehome_unacked` (L1247), `retransmit_from_on` (L1266), `migrate_send_blocked` (L354), `wait_bulk_send.mark_enqueued` (L314).

#### P2.2 Bandwidth estimator (sender side, `on_ack`)

In `on_ack` (`streams.rs` L677–708), inside the `drop_keys` loop, for each acked `u` with path `p = get_path(u.path_id)`:

```text
now_us = mono_us()
p.delivered += u.data.len()
sample allowed iff u.tried.len() == 1                      // never learn from a hedged copy
  Δd  = p.delivered − u.delivered_at_send                   // bytes acked since this piece was sent (ACK clock)
  Δt  = now_us − u.delivered_time_at_send_us                // time since the ACK that preceded its send
  if u.delivered_time_at_send_us == 0: Δt = now_us − u.last_sent_us   // first sample on this path
  if Δt ≥ min_rtt_p / 4 and Δd > 0:                         // guard against decode-gap micro-samples
      p.bw_filter.update(Δd·1e6/Δt, now_us, win = max(10·min_rtt_p, 100 ms))
  if u.data.len() > interactive_max:                        // loaded ACK RTT (P4 uses it; P6 exports it)
      p.ack_rtt_us = ewma(1/8, u.last_sent.elapsed())
p.delivered_at_us = now_us
inner.budget_wait.notify_waiters()  // session-level, after sub_inflight (P2.4)
```

`min_rtt_p = p.rtt()` if `rtt_known()` else `unknown_rtt_us` (existing 20 ms) — unknown paths sit at the budget floor anyway.

`MinMax3` is the BBR windowed max (three (value, time) slots; new sample replaces slots older than `win`; result is slot 0). Pure function, unit-tested.

Why this is not the floor-lock again: when budget-limited on an unsaturated path, ACKs arrive `min_rtt` after sends and `Δd/Δt = budget/min_rtt`; `budget' = 2·budget` — doubling. Once the path is saturated, ACK spacing equals bottleneck spacing, `Δd/Δt = bw`, `budget = 2·bw·min_rtt = 2·BDP`, in-flight 2·BDP ⇒ 1 BDP standing queue, `ack_rtt ≈ 2·min_rtt`. Stable, and the sample is independent of the queue.

App-limited samples: BBR discards samples taken while app-limited unless they raise the max. We get the same effect for free because the filter *is* a max and a budget-limited sample is a lower bound that only raises it; an app-limited sample (sender had nothing to send) is also a lower bound. Nothing is needed beyond the max filter and the window expiry.

Idle expiry: after `max(10·min_rtt, 100 ms)` without ACKs on a path the filter empties and `budget` returns to the floor; the next transfer restarts at 64 KiB and doubles per RTT (≈ 5 RTT to the 1 MiB ceil). This is BBR's behaviour too and is the price of not carrying a stale estimate across an idle IX. Expiry while `inflight` is still high makes `room` negative; waiters simply wait for ACKs — no deadlock (Review R4). Note that pings keep the TCP non-idle, so the kernel's `tcp_slow_start_after_idle` does not add a second restart.

#### P2.3 Budget

```rust
impl Session {
    fn path_budget(&self, p: &PathState) -> u64 {
        let t = &self.inner.cfg.tuning;
        let floor = t.inflight_bias;                              // 64 KiB
        let ceil  = (t.chan as u64) * (MAX_STREAM_PAYLOAD as u64); // ≈ 1 MiB
        let bw = p.bw_bytes_s();                                   // windowed max; 0 = none
        if bw == 0 || !p.rtt_known() { return floor; }
        let bdp = bw as f64 * p.rtt().as_secs_f64();
        ((2.0 * bdp) as u64).clamp(floor, ceil)
    }
    fn path_room(&self, p: &PathState) -> u64 {
        self.path_budget(p).saturating_sub(p.inflight_bytes())
    }
}
```

Gain 2 is the same gain `recv_cap_target` already uses. Exported per path (P6).

#### P2.4 Placement in `send_data`

Replace the bulk arm of the pick loop (`streams.rs` L214–236):

```text
loop {
    picked =
      if pref == Interactive: interactive_affinity(sticky).or_else(pick_pref(Interactive))     // unchanged
      else:
        sticky_ok = bulk_affinity(sticky) and path_room(sticky) ≥ 1                             // affinity + room
        sticky_ok
          .or_else(|| bulk_overflow_pick(sticky))                                                // KD6 fan-out
          .or_else(|| if !any_bulk_room(): None else pick_pref(Any))                              // legacy pick only if someone has room
    if picked: break
    if dead/reset: return Err(Reset)
    select! {
        ready.notified()          => {}   // path set changed
        budget_wait_any()         => {}   // any path's inflight dropped (session-level Notify, see below)
        st.send_wait.notified()   => {}   // window/reset
        sleep(all_down_timeout)   => if !has_alive_path() { return Err(NoPath) }
    }
}
```

`bulk_overflow_pick(sticky)`:

```text
paths = path_list(); set = fastest_class_set(paths, cfg)             // full class set, not interactive_class_set
room  = set.filter(p.id != sticky && p.is_schedulable() && is_loss_fresh(p) && path_room(p) ≥ MAX_STREAM_PAYLOAD)
cands = room.filter(!conn_has_interactive(p))                         // HOL: do not overflow onto an interactive TCP
if cands.is_empty(): cands = room                                     // bulk must move somewhere; maybe_hol rebalances next tick
if cands.is_empty(): None else argmin over cands of path_score(p, cfg, Any)   // existing score; room is the filter
```

Piece size stays `n = min(remaining, room_window, MAX_STREAM_PAYLOAD)` (L203–205); additionally `n = n.min(path_room(picked)).max(1)`? **No** — a partial piece adds fragmentation; budget is enforced at ≥ 1 byte of room, allowing one frame of overshoot per path (≤ 16 KiB, < floor/4). Documented; test `budget_overshoot_at_most_one_frame`.

`sticky` is **not** changed by overflow (KD6). `set_sticky(id, path_id)` at L255 currently runs for every piece — change to run only when `path_id == sticky || sticky == 0 || hol_initial moved it`; overflow pieces do not restick. `hol_initial` (L237–254) unchanged.

Session-level wake: `Inner.budget_wait: Notify`; `on_ack` (after `sub_inflight`), `release_unacked`, `xfer_inflight`, `path_failed` (after `rehome_unacked_from`) and `add_path` (a new path is fresh room) call `notify_waiters()`. Waiters are streams, not paths, and a stream parked on a full sticky must also wake when *any* same-class path gains room (fan-out), so there is no per-path Notify (Review R3).

Wait is bounded by `all_down_timeout` for the `NoPath` exit exactly as the window wait is (L193–200); a live pool with all paths at budget is a legitimate steady state and the waiter is woken by the next ACK.

#### P2.5 What bypasses the budget

- Interactive DATA (`pref == Interactive`), STREAM_OPEN/CLOSE/RESET, ACK, Ping — never wait on room.
- `retry_expired_unacked`, `rehome_unacked_from`, `migrate_send_blocked`, `wait_bulk_send` give-up: place without room check (they must move bytes off a bad path), `xfer_inflight` still accounts them. After P4 these are rare.
- `hol_place_bulk` / `maybe_hol` choose a dest without room check; the next piece then respects room on the new sticky. HOL moves the sticky; budget shapes what follows.

#### P2.6 Interaction with C4 `wait_bulk_send`

Unchanged. Budget ceil equals the bulk queue capacity, so a path at ceil with a lowat-blocked writer can still fill its mpsc; C4 waits `down_timeout` then `pick_retry`. In practice budget-limited sending keeps `queued_bulk` ≤ `budget/16 KiB` ≤ 64.

### P3 — Receiver window controller

#### P3a (ship now)

1. `bdp_rtt` (`streams.rs` L610–620) and `deliver_min_dt` use **`st.last_recv_path`** first, then `sticky`, then `min_alive_fast_rtt`. A download receiver's `sticky` is its request path; DATA arrives on the sender's sticky. Same pool, usually same class — but on a mixed pool the wrong RTT can be 3× off in either direction.
2. `advertised_window` and `recv_cap_target` unchanged.
3. `StreamState` gains counters read at stream end (P6): `window_blocks: AtomicU64` (sender; move the per-stream count next to the process counter), `budget_blocks: AtomicU64`, `window_limited_with_room: AtomicU64` — incremented in `send_data` when `!window_ok(1)` **and** `any_bulk_room()` is true at that instant. That is the exact evidence for KD8: the stream window, not the path, was the limiter.

#### P3b (specified; implement only on evidence)

Trigger for shipping: post-P2 production shows `nya_send_window_limited_with_room_total / nya_window_blocks_total ≥ 0.2` on a download-heavy instance over ≥ 6 h, **or** `recv_cap_max_bytes` p90 == floor on hops ≥ 8 MB while `nya_path_budget_bytes` p50 > 2 × floor.

Mechanism: a receiver cannot see the sender's `window_blocks`, but a window-limited sender always sends up to exactly `acked_offset + window` of the ACK it last received. The receiver knows what it advertised. Keep per stream a ring of `(edge = acked_offset + window, t)` sampled at most every `min_rtt/8`, 64 entries. On DATA arrival with `arr = offset + len`: hit if ∃ entry with `|entry.edge − arr| ≤ max(MAX_STREAM_PAYLOAD, rate·min_rtt/8)`. Controller per stream:

```text
state ∈ { Steady, Probing{cap0, rate0, since} }, hold = { cap_hold, rate_hold }
each note_deliver sample (≥ one min_rtt):
  wl  = hits since last sample > 0
  app = buffered_in + recv_buffered > cap/2
  base = clamp(2·rate·rtt, floor, ceil)
  Steady:  if wl && !app && now − last_probe_end ≥ 8·min_rtt: Probing{cap, rate, now}; cap = min(2·cap, ceil)
           else cap = max(base, if rate ≥ rate_hold/2 && !app { cap_hold } else { 0 })
  Probing: if now − since ≥ 4·min_rtt:
              if rate·4 ≥ rate0·5: hold = {cap, rate}; Steady        // +25 % — path had room
              else: cap = max(cap0, base); Steady; last_probe_end = now
  if app: hold = {0,0}
```

Bounded by `ceil`, by P2 (per-path bytes cannot exceed budget regardless of window), and by the 8·min_rtt cooldown. Tests listed below. Not in the first PR series.

### P4 — Hedge on path silence

Rewrite the filter and the loop body of `retry_expired_unacked` (`mod.rs` L646–684):

```text
for (offset, u) in unacked:
    p = get_path(u.path_id)
    bulk = u.data.len() > interactive_max
    age  = u.last_sent.elapsed()
    if now < u.retry_not_before: continue
    eligible =
        u.dropped                                                      // never reached the wire
     || p.is_none() || !p.is_alive()                                   // path gone (rehome_unacked_from normally beat us)
     || (!bulk && age ≥ retry_after(u.path_id))                        // interactive: today's clock, unchanged
     || (bulk && !is_loss_fresh(p) && age ≥ retry_after_bulk(p))       // path silent
     || (bulk && age ≥ belt(p, u))                                     // receiver-side silent drop belt
    if !eligible: continue
    if stalled_long && alive.all(tried.contains): continue             // A3 unchanged
    if p.is_alive() && p.is_write_stalled() && !u.dropped: continue    // C unchanged (a stalled writer is still flushing)
    push_tried(from); alt = pick_retry_tried(tried) ...; skip write-stalled alt (unchanged)
    if send_data_frame(alt): rehome_unacked(u, alt); u.dropped = false; note_retry
                             u.retry_not_before = now + backoff(u)
    else: push_tried(alt); u.retry_not_before = now + retry_after(from)   // unchanged
```

Where:

- `retry_after_bulk(p) = clamp(2 · ack_rtt_p, loss_timeout(min_alive_fast), down_timeout(p))` — `ack_rtt_p` from P2.2; unknown → `loss_timeout(min_alive_fast)` (today's value).
- `belt(p, u) = down_timeout(p) · 2^(min(u.tried.len(), 4) − 1)`, capped by `down_timeout_ceil` (5 s): 330 ms, 660 ms, 1.3 s, 2.6 s, 5 s. This is the only way a bulk piece on a fresh path is ever re-sent; it exists for the receiver-drops-silently case (unknown stream after `expire_early_data`, `close_off` race). With P5 the FIN case no longer needs it.
- `backoff(u) = retry_after_bulk(from) · 2^(min(u.tried.len(), 4) − 1)` for bulk; `retry_after(from)` for interactive (unchanged).
- `u.dropped = true` is set at every point where a piece is in `unacked` but no frame reached a writer queue: (i) `send_data` L277–285 when `wait_bulk_send` gives up and `migrate_send_blocked` (`streams.rs` L354–366) returns early because `pick_retry` is `None` **or** its `send_on_path(alt)` fails — today both returns are silent; (ii) `park_stream_data` (`path.rs` L715–721) dropping a parked DATA frame, via a new `session.note_data_park_dropped(stream_id, offset)` that looks the piece up and flags it; (iii) `retry_expired_unacked` itself when `send_data_frame(alt)` fails (already pushes `tried`; add the flag). Today those bytes are silently lost until the 20 ms clock; after P4 they would be lost until the belt — hence the flag. Cleared on any successful enqueue.

`rehome_unacked_from` on `path_failed` (L686–700) is unchanged: a dead path rehomes everything immediately.

Consequences: on a healthy saturated path `is_loss_fresh` is true (per-frame ACKs and 10–50 ms Pongs) ⇒ **zero** bulk hedges. A silent path (TCP RTO storm, blackhole) still rehomes within `retry_after_bulk` after `loss_timeout` of silence — the same order as today for the case hedge was designed for. Interactive TTFB unchanged.

### P5 — ACK duplicates after FIN

`deliver_data` (`streams.rs` L488–518):

```text
if st.reset: return                                      // peer Reset follows; unchanged
if st.recv_fin: send_ack(st, path_id); return            // new: receiver has everything the sender needs acked
st.last_recv_path = path_id
if close_off known && offset ≥ close_off: send_ack(st, path_id); return   // new
if offset < recv_next: send_ack; return                  // unchanged
```

`send_ack` reads `recv_next` and `advertised_window()` — for a FIN'd stream `recv_next == close_off`, which acks the sender's entire `send_next`; `on_ack` drops all `unacked`, `stall` clears, `scan_stall` observes a bounded sample, `inflight` is released on the right path. The ACK rides the register; the stream still exists (`recv_fin` streams stay until `maybe_count_graceful`/linger), so `store_ack` finds `st.id`. Metric: `nya_ack_after_fin_total`.

### P6 — Observability

**Per-path gauges** (catalog `nya_path_*`, labels as today):

| name | source |
| --- | --- |
| `nya_path_budget_bytes` | `path_budget(p)` |
| `nya_path_bw_bytes_s` | `bw_filter` max |
| `nya_path_ack_rtt_us` | `ack_rtt_us` |
| `nya_path_tcp_cwnd_bytes` | `tcpi_snd_cwnd × tcpi_snd_mss` |
| `nya_path_tcp_rtt_us` / `nya_path_tcp_rttvar_us` | `tcpi_rtt`, `tcpi_rttvar` |
| `nya_path_tcp_unacked_bytes` | `tcpi_unacked × mss` |
| `nya_path_tcp_notsent_bytes` | `tcpi_notsent_bytes` |
| `nya_path_tcp_retrans` | `tcpi_total_retrans` (monotone, exported as gauge; Signoz `increase` on gauge is not available — also add counter below) |
| `nya_path_tcp_delivery_rate_bytes_s` | `tcpi_delivery_rate` |

`TCP_INFO` read at snapshot time (10 s) via `getsockopt(dup_fd, SOL_TCP, TCP_INFO)` into a 256-byte buffer parsed by kernel-documented offsets (tolerant of short returns, kernel ≥ 4.9 for delivery_rate). Linux only; elsewhere the gauges are absent. `tcp_fd` is `dup()`ed in `spawn_path_io` before `tokio::io::split` (need `AsRawFd` on the inner `TcpStream`: pass `Option<RawFd>` into `spawn_path_io` from the two call sites where the TLS stream's `get_ref()` is still visible), cleared and closed at `path io exit` before `path_failed`.

**Counters** (process `n_counter` 54 → 60; update `export.rs` L428–429):

| name | meaning |
| --- | --- |
| `nya_send_budget_blocks_total` | bulk piece waited for path room |
| `nya_send_window_limited_with_room_total` | window wait while some path had room (P3b evidence) |
| `nya_data_dup_rx_bytes_total` | payload bytes received with `offset < recv_next` or fully inside `recv_buf` — the duplicate waste that `bytes_data_rx` hides |
| `nya_ack_after_fin_total` | P5 |
| `nya_data_dropped_resend_total` | P4 `dropped` resends |
| `nya_path_tcp_retrans_total` | Σ of per-path `tcpi_total_retrans` deltas at snapshot |

**Histograms** (`_bucket/_sum/_count`, not counted in `n_counter`): `nya_recv_cap_max_bytes` (per stream at end; bounds 128 K, 256 K, 512 K, 1 M, 2 M, 4 M, 8 M), `nya_ack_loop_ms` (bulk `last_sent→ack`, bounds = `STALL_MS_BOUNDS`).

**`nya.hop` span attributes** (both roles, at copy end): `nya.recv_cap_max`, `nya.window_blocks`, `nya.budget_blocks`, `nya.window_limited_with_room`, `nya.hedges`, `nya.dup_rx_bytes`, `nya.paths_used` (distinct dests this stream sent on). Provided by `Session::stream_stats(id) -> Option<StreamStats>`, called by `copy_with_hop` (`nya-client/src/inbound.rs` L257) and the server copy (`nya-server/src/outbound.rs` L103–107) **before** `reap_stream`. Also record `nya.limiter` = argmax of {window, budget, app(advertised==0 with buffered_in>0), none} by time waited — a single string the report can group by.

**Snapshot line** (`export.rs` `format_paths`): append `bud=<KiB> bw=<KB/s> ackrtt=<ms>` per path; `tcp=cwnd/unacked/notsent/retrans` when available. Keep under the compact budget; `metrics=` still not attached.

**Server hop spans missing.** Several ≥ 50 MB Hytron hops had a client span but no server span (`origin=n/a` in the pull). Check `nya-obs` exporter drop accounting (`queue_size` 8192 / `batch_size` 512) and add `nya_obs_spans_dropped_total`; not a `nya-core` change but part of "make the limiter observable".

### e2e WAN bottleneck

`crates/nya-e2e/src/impair.rs` `ImpairConfig` gains `rate_bps: Option<u64>` and `queue_bytes: Option<usize>` (default `None` = today). `packet_wan.rs` `wan_pipe`: a token bucket at `rate_bps` per direction paces packet release; a byte-bounded FIFO ahead of it drops tail when full (`drops` counter). `MAX_CWND` unchanged for existing scenarios; with `rate_bps` set, `MAX_CWND` is raised to `queue_bytes / MSS` so the emulated cwnd is not the bottleneck. `LinkHandle` exposes `set_rate(Option<u64>)` for mid-run changes.

---

## API / Interface Changes

- Public crate API unchanged (`Session::open_stream`, SOCKS, `IncomingStream`). New `pub fn Session::stream_stats(&self, id: u32) -> Option<StreamStats>` for the hop probes.
- `nya_core::net::tune_path_socket` (new, `pub`).
- `spawn_path_io` gains `tcp_fd: Option<RawFd>` — internal.
- Wire, TOML, `PROTOCOL_VERSION` unchanged.

## Data Model Changes

- `PathState`: `delivered`, `delivered_at_us`, `bw_filter`, `ack_rtt_us`, `tcp_fd`.
- `Unacked`: `delivered_at_send`, `delivered_time_at_send_us`, `dropped`.
- `StreamState`: `window_blocks`, `budget_blocks`, `window_limited_with_room`, `recv_cap_max`, `dup_rx_bytes`, `hedges`, `paths_used` (small `Mutex<Vec<u32>>` or bitset over path ids).
- `Inner`: `budget_wait: Notify`.
- `Counters`: six new counters, two histograms. `n_counter` assertion 54 → 60.

---

## Alternatives Considered

### 1. Use loaded RTT in `recv_cap_target` (`cap = 2·rate·loop`) — rejected
`rate·loop` is exactly the bytes in flight, so `cap' = 2·cap` unconditionally: unbounded growth to ceil on every stream, then bufferbloat. The formula needs a rate that is independent of the window; that is what P2's ACK-clock gives at the path level.

### 2. Receiver-side "active arrival rate" (exclude idle gaps) — rejected as the fix
In a shared FIFO the receiver's arrival spacing is its share; the estimate equals `cap/loop` again. Only helps when the sender is bursty for other reasons. Not worth a second estimator.

### 3. Raise `initial_window` / lower the growth gate — rejected
Tuning, forbidden by policy, and does not address queueing: a bigger floor on a shared 5-tuple deepens the kernel queue.

### 4. Per-piece hedge clock from `ack_rtt_p` only (keep per-piece semantics) — rejected as insufficient
Reduces the storm 10× but keeps the wrong model: on in-order TCP a piece behind a queue is not lost. Silence-based trigger is both correct and simpler; the `ack_rtt_p` clock survives as `retry_after_bulk` for the silent case.

### 5. Hard cap on bulk mpsc depth per path instead of unacked budget — rejected
Frames already in the kernel are invisible to the mpsc; that is where the bytes are. `inflight` is the right variable (KD2).

### 6. Pace bulk sends per path (token bucket at `bw_p`) instead of a cwnd budget — deferred
Pacing removes the standing 1·BDP queue and is what BBR does. It needs a per-path timer wheel in the writer and interacts with `write_deadline`. The cwnd budget already bounds the queue to 1 BDP; pacing is a follow-up if `ack_rtt_p / min_rtt_p` sits at 2 in production and interactive p50 on shared paths suffers.

### 7. Wire flag "sender window-limited" on STREAM_DATA / new ACK field — rejected
`PROTOCOL_VERSION` stays 2. P3a's counter gives the sender-side truth in metrics; P3b's ring reconstructs it at the receiver without a wire bit if ever needed.

### 8. `TCP_CONGESTION` via TOML — rejected
No new keys. A socket option the overlay asks for is a mechanism decision (KD10); ops override by kernel configuration.

### 9. Split each bulk stream across all paths round-robin ("true striping") — rejected
Reordering across heterogeneous classes wrecks TTFB and the recv-hole stall clock; HOL isolation depends on bulk having a home. KD6 fan-out is striping only when the home is full and only within the fastest class.

### 10. Drop the A3 stall bound / the cycle rung for DATA — rejected
A3 protects the pool from leftover ghosts (v0.1.4 A3); the cycle rung is what rehomes DATA when every path has been tried and one recovered. Both are now reached far less often because eligibility requires silence.

---

## Security & Privacy Considerations

No new wire fields, no new authentication paths. `TCP_INFO` exposes kernel transport counters of the overlay's own sockets to the local metrics endpoint / OTLP — same trust boundary as existing path gauges. `dup()` fds are owned by `PathState` and closed on IO exit; leak is bounded by `max_paths`. `TCP_CONGESTION` failure is non-fatal and logged once. `nya.hop` new attributes carry no addresses beyond what the span already has.

---

## Observability

Product gate (Signoz, no iperf): see Rollout §4. Reading guide for the new signals:

| Question | Signal | Healthy |
| --- | --- | --- |
| Is the overlay the limiter? | `nya.limiter` on large hops; `send_budget_blocks` vs `window_blocks` | `limiter=none` or `app` on ≥ 80 % of large hops |
| Is a path saturated by us? | `nya_path_inflight_bytes ≈ nya_path_budget_bytes`, `nya_path_tcp_notsent_bytes > 0` | expected on the busiest path only |
| Is TCP the limiter? | `tcp_cwnd_bytes` small vs `budget`, `tcp_retrans_total` rate, `tcp_delivery_rate` ≪ `bw` | cwnd ≥ budget/2 |
| Is the window controller stuck? | `recv_cap_max_bytes` p90 == floor on ≥ 8 MB hops **and** `window_limited_with_room` > 0 | ≈ 0 (else ship P3b) |
| Hedge waste | `nya_data_dup_rx_bytes_total / nya_bytes_data_rx_total` | ≤ 2 % |
| Kernel queue | `nya_path_ack_rtt_us / nya_path_rtt_us` | ≤ 3 |
| Post-FIN stalls | `nya_ack_after_fin_total`; `stall_ms` > 10 s share | > 0; share ≪ today's 4.8 % |

Logs: no new hot-path logs. `info!` once per process for socket tuning result. `debug!` on budget wait give-up and on P4 belt resend (`reason = "belt"`).

---

## Rollout Plan

1. Land PRs in order (below). `cargo test -p nya-core`; `cargo test -p nya-e2e --test matrix short_matrix`; new bottleneck scenarios; mixed soak: no new SLA red, `failbacks = 0`, no `all_down`.
2. Canary **Datawave** (idle, 0 large hops — validates no-regression on 204/interactive and socket tuning), 6 h. Then **Yuusei** (204 + occasional downloads; leftover ≈ 0 and hop RST ~1/h must hold), 12 h. Then **Hytron**.
3. Bounce is **not** required (no table shape change). Both ends must run the new binary for P4/P5 to matter on both directions; P1/P2/P6 are unilateral.
4. Watch 24 h post-Hytron:

   | Signal | Expect |
   | --- | --- |
   | Hytron 1-min download peak vs upload peak | ratio ≥ 0.5 (today 0.33); single large hop ≥ min(origin, 0.8 × sticky `tcp_delivery_rate`) |
   | `nya_data_dup_rx_bytes_total / bytes_data_rx` | ≤ 2 % (today 14–16 %) |
   | server `data_hedge` per stream | ≤ 0.1 (today 1.6) |
   | `nya_path_ack_rtt_us / nya_path_rtt_us` (busiest path) | ≤ 3 |
   | server `stall_ms` p75 | ≤ 200 ms (today ≈ 1 s); > 10 s share ≤ 0.5 % |
   | `window_limited_with_room / window_blocks` | < 0.2, else P3b |
   | `recv_cap_max_bytes` p50 on ≥ 8 MB hops | > floor |
   | Yuusei `streams_held` leftover, hop RST/h, 204 first-byte | unchanged |
   | `failbacks`, `session_all_down_resets`, `path_down`/h | unchanged (path_down is IX) |
   | `tcp_cc` gauge | `bbr` on Linux hosts, or explain |

5. Rollback: revert the PR(s) individually. P1 revert = `TCP_NODELAY` only. P2 revert = no room check (today). P4 revert = per-piece clock (today's waste). P5 revert restores post-FIN stalls. Wire v2 throughout, mixed versions are safe.

---

## Risks

| Risk | Sev | Mitigation |
| --- | --- | --- |
| `bw_p` underestimates ⇒ budget below true path capacity ⇒ throughput regression on a path we could fill today | **High** | Floor 64 KiB = 6.4 MB/s/path at 10 ms (> any observed aggregate); gain 2 doubles per RTT while budget-limited; max filter over 10 RTT ignores dips; canary table compares `budget` to `tcp_delivery_rate` and `send_budget_blocks` while `tcp_notsent == 0` (budget binding while TCP idle = estimator wrong). |
| Budget-limited sending reduces `inflight` ⇒ `path_score` load_term spreads new streams differently | Med | `load_term` unchanged; room filter only prunes; unit test that a 3-path pool still opens streams on the emptiest path. |
| KD6 fan-out creates recv holes across paths ⇒ recv-hole `stall_ms` samples | Med | Fan-out only within `fastest_class_set` and only when sticky is at budget; hole duration bounded by class RTT spread + 1 BDP queue. Metric `nya.paths_used` on hops; mixed soak `p50` doors unchanged (interactive never fans out). |
| `NOTSENT_LOWAT` makes `write_stalled` flip more often ⇒ interactive pick-skip on saturated paths | Med | Intended C semantics; interactive affinity already skips stalled; `hol_place_bulk` accepts stalled siblings. Watch `path write stalled` info count and interactive `open`/`first_rx` p99 on canary. |
| `TCP_CONGESTION=bbr` unavailable / behaves worse on some host | Low | Best-effort; gauge shows actual CC; revert is one line; BBR loss-tolerance is the point on a 1–2 % loss IX. |
| P4 misses a real loss case: frame lost by a *fresh* path | Low | Impossible on TCP unless the receiver drops; the receiver drops only for `reset`/`recv_fin`/`close_off`/unknown stream — P5 acks three of these, the belt covers unknown-stream within `down_timeout`. |
| P4 belt still storms on pathological ACK loss | Low | Exponential 330 ms → 5 s per piece, A3 unchanged. |
| `dropped` flag missed on some drop path ⇒ silent piece until belt | Med | Both drop sites (`send_on_path` false after insert, `park_stream_data`) flagged; unit test `dropped_frame_resent_on_fresh_path_before_belt`. |
| Budget waiters never woken (deadlock) | **High** | Wake on every `sub_inflight`/`xfer_inflight`/`release_unacked`/`path_failed`; bounded by `all_down_timeout` select arm; test `budget_wait_wakes_on_ack` and `budget_wait_wakes_on_path_failed`. |
| Session-level `budget_wait.notify_waiters()` on every ACK is a thundering herd with 500 streams | Med | `Notify::notify_waiters` is O(waiters) and only waiters at budget are parked; ACK rate ≤ 60/s per path per stream at 16 KiB frames. Measure in `bulk_shared_two_streams` CPU; if hot, coalesce to once per `maintain` tick. |
| `dup()` fd leak on abnormal exit | Low | Closed in `spawn_path_io` exit path (both `Exit` arms) and in `Drop` of a small guard. |
| `TCP_INFO` struct layout differs across kernels | Low | Parse by offset with length check; fields beyond returned length → absent. |
| `n_counter` drift / catalog tests | Med | Update assertion and `catalog_from_source` in `.local/nya-signoz.py completeness`. |
| Memory: `paths_used` per stream | Low | `u64` bitset keyed by `path_id % 64` is enough for counting distinct dests. |

---

## Open Questions

None blocking. Recorded:

- Pacing (Alt. 6) — revisit after seeing `ack_rtt_p / min_rtt_p` in production.
- P3b — revisit on the KD8 evidence counter.
- Whether the overflow candidate set should be `interactive_class_set` instead of `fastest_class_set` for streams whose receiver is interactive-sensitive — no: bulk is bulk; the class set already excludes backups.
- `MinMax3` window `max(10·min_rtt, 100 ms)`: 100 ms is the BBR default floor at 10 ms RTT; on far bands 10·rtt dominates. Documented constant, not Tuning.

---

## Tests required

### `nya-core` units

Path / estimator (`path.rs`):
- `minmax3_expires_old_samples`, `minmax3_keeps_max_within_window`
- `budget_floor_when_bw_unknown`, `budget_is_2bdp_clamped_to_ceil`, `budget_floor_is_inflight_bias`
- `ack_clock_sample_skips_hedged_piece`, `ack_clock_sample_ignores_micro_dt`
- `ack_rtt_ewma_only_bulk`

Session (`session/mod.rs`, `streams.rs`):
- `bulk_stays_on_sticky_while_room`
- `bulk_overflows_to_same_class_path_when_sticky_at_budget` (sticky unchanged, `paths_used == 2`)
- `bulk_waits_when_no_room_and_wakes_on_ack`
- `budget_wait_wakes_on_path_failed`
- `budget_overshoot_at_most_one_frame`
- `interactive_data_ignores_budget`
- `retry_bypasses_budget_but_accounts_inflight`
- `hedge_not_fired_on_fresh_path_for_bulk` (200 ms delayed ACKs, path has Pongs ⇒ 0 hedges)
- `hedge_fires_after_loss_timeout_silence` (blackhole path ⇒ rehome within `retry_after_bulk`)
- `interactive_hedge_clock_unchanged`
- `dropped_frame_resent_on_fresh_path_before_belt`
- `belt_backoff_doubles_and_caps_at_down_timeout_ceil`
- `dup_after_fin_is_acked` (sender `unacked` drains, `stall` clears, `ack_after_fin == 1`)
- `dup_past_close_off_is_acked`
- `dup_rx_bytes_counts_offset_below_recv_next_and_inside_buf`
- `bdp_rtt_prefers_last_recv_path`
- `window_limited_with_room_counted_only_when_a_path_has_room`
- `stream_stats_available_before_reap`
- existing: `recv_cap_grows_with_bdp_and_not_below_floor`, `recv_cap_does_not_grow_from_back_to_back_data`, `stall_observes_frozen_origin_not_zero_after_ack`, all A/B/C/D v0.1.4 tests, `interactive_class_set` tests — unchanged and green.

Socket (`net.rs`): `tune_path_socket_sets_lowat_on_linux` (skip if EPERM), `tcp_info_parse_short_buffer`.

Export: `n_counter == 60`, catalog names, snapshot line contains `bud=`.

### `nya-e2e`

New scenarios (short matrix):
- `bulk_bottleneck_single` — 1 link × 1 conn, 10 ms, `rate 50 Mbps`, `queue 256 KiB`, 64 MiB copy: goodput ≥ 80 % of rate; `recv_cap_max` > floor; hedge = 0.
- `bulk_shared_two_streams` — same link, two 32 MiB copies: aggregate ≥ 80 %; hedge = 0; `dup_rx_bytes` ≤ 1 %; `ack_rtt_p ≤ 3 × rtt`.
- `bulk_fanout_three_paths` — 3 links × 1 conn each 20 Mbps, one 96 MiB copy: goodput ≥ 2 × 20 Mbps × 0.8; `paths_used ≥ 2`.
- `ping_under_bulk_bounded` — 1 link 20 Mbps with bulk running: interactive echo p50 ≤ rtt + 2·(lowat + 16 KiB)/rate (+ jitter); today's harness shows seconds.
- `hedge_only_on_silence` — 2 links, bulk on `a`, blackhole `a` for 2 s: rehome within `retry_after_bulk + loss_timeout`; before blackhole hedge = 0.
- `prod_like_bulk_copy` unchanged and green.
- Mixed soak: all existing doors; add `dup_rx_bytes ≤ 2 %` and `hedge/stream ≤ 0.1` to the report (not a door in this series).

`packet_wan` units: `token_bucket_paces_at_rate`, `queue_drops_tail_when_full`, `rate_none_is_today`.

---

## Docs

- `docs/ARCHITECTURE.md` 流控制: add per-path budget, ACK-clock `bw`, hedge-on-silence, `NOTSENT_LOWAT`; correct "未 ACK 则按 2×RTT 换路再发" to "bulk 按路径静默换路，interactive 按 2×RTT".
- `docs/OBSERVABILITY.md`: new gauges/counters/histograms, `nya.limiter`, reading guide above.
- `README.md` 特性: "每条 5-tuple 有 overlay cwnd（2×BDP），内核 unsent 有界" one line.
- `.local/README.md`: `completeness` expects 60 counters; `analyze` derived row `dup_rx %` and `hedge/stream`.

---

## References

- v0.1.5 production pull 2026-09-14 (this document's tables); `/tmp/nya015` scripts are not in-tree.
- `docs/design-hytron-bulk-goodput.md` §E (window auto-tune), §A3 (stall bound), §C (write-stall), §D (HOL).
- Cardwell et al., *BBR: Congestion-Based Congestion Control*, ACM Queue 2016 — delivery-rate sampling and windowed max/min filters.
- Linux `tcp(7)`: `TCP_NOTSENT_LOWAT`, `TCP_CONGESTION`, `TCP_INFO`.

---

## PR Plan

### PR 1 — `net: TCP_NOTSENT_LOWAT + best-effort BBR on overlay path sockets; TCP_INFO gauges`
P1 + the `TCP_INFO` half of P6. Unilateral, no algorithm change. Canary alone first — it already answers "is TCP the limiter".

### PR 2 — `session: ACK duplicates after FIN / past close_off; dup_rx_bytes counter`
P5 + `nya_data_dup_rx_bytes_total`. Tiny, independent, removes the stall tail.

### PR 3 — `session: bulk hedge on path silence; dropped flag; backoff belt`
P4. Depends on `ack_rtt_us` (part of PR 4's estimator) — carry `ack_rtt_us` EWMA in this PR; PR 4 adds `delivered`/`bw_filter`.

### PR 4 — `path: ACK-clock bandwidth estimator; per-path send budget; bulk fan-out`
P2 + P3a + the remaining P6 counters/histograms/hop attributes + `stream_stats`. Largest PR; ships with the e2e bottleneck (PR 5) or after it.

### PR 5 — `e2e: rate/queue bottleneck in packet_wan; bulk_* scenarios`
Can land before PR 4 (scenarios red until then are marked `expected_fail` in the catalog, as prior series did).

### PR 6 (only on evidence) — `session: receiver window probe (P3b)`

---

## Review log

Self-review against the 2026-09-14 findings and the v0.1.3–v0.1.5 invariants. Each item was checked against the text above; where the review found a gap the design was amended in place and the amendment is noted.

| # | Check | Result |
| --- | --- | --- |
| R1 | Every finding in the observation report has a mechanism: floor lock (P2 primary, P3 secondary), hedge storm (P4), kernel bloat (P1+P2), post-FIN stall tail (P5), blind spot (P6), download≪upload unexplained (P1 KD10 + `TCP_INFO`), missing server hop spans (P6 last paragraph). | covered |
| R2 | Does P2 alone reintroduce a fixed point? Budget-limited on unsaturated path: ACK-clock sample = `budget/min_rtt` ⇒ doubling. Saturated: sample = bottleneck ⇒ `2·BDP`. Analysed in P2.2; the pitfall (measuring over the full loop) is Alt. 1. | ok |
| R3 | P2.1 listed a per-path `budget_wait` while P2.4 uses a session-level Notify. Waiters are streams, not paths; a stream at budget on its sticky must also wake when *any* same-class path gains room (fan-out). **Amended:** single `Inner.budget_wait`; per-path field removed. | amended |
| R4 | Deadlock: waiter parked with no future wake. Wakes enumerated (ACK, xfer, release, path_failed) + `all_down_timeout` arm. Stream reset while waiting: `send_wait` is notified by `finish_stream`/`Drop` (existing L1787) and the loop re-checks `st.reset`. | ok |
| R5 | Does the budget starve interactive or Close/Reset? KD5: bypass. Does it starve retries? Bypass. Does it break C4? P2.6. | ok |
| R6 | Does hedge-on-silence break the blackhole / `flash_disconnect` catalog cases? Silence ⇒ `!is_loss_fresh` after `loss_timeout(min(fast,class))` (20 ms floor), then `retry_after_bulk` ≥ that ⇒ rehome within ≈ 2× today's latency for bulk; interactive unchanged; `path_failed` rehome unchanged. `hedge_only_on_silence` scenario asserts the bound. | ok |
| R7 | ACK return-path loss: DATA on `p`, ACK stored on `p` (arrival) — if `p` dies the register merges (v0.1.4 `636589a`), `path_failed` rehomes unacked. If the receiver stored the ACK on another path that died, sender sees `p` fresh but no ACK ⇒ belt at `down_timeout(p)`. Bounded. | ok |
| R8 | `is_loss_fresh` uses `last_rx_ago` on the *sender's* view of `p`; Pongs every 10–50 ms keep an idle-but-alive TCP fresh — correct: alive TCP ⇒ delivered. A TCP whose *send* direction is dead but receive alive (half-dead) would look fresh: kernel detects via RTO ⇒ eventually `path read failed`/`down_for`; the belt covers the window in between. | ok |
| R9 | `dropped` coverage: both drop sites named; `wait_bulk_send` give-up → `migrate_send_blocked` → if `pick_retry` is `None` the piece is left with `dropped=true`. Confirmed by reading L354–366 — `migrate_send_blocked` returns silently on `None`; P4 must set `dropped` there. **Amended** in P4 text ("both fail to enqueue"). | amended |
| R10 | Overflow and `set_sticky`: L255 resticks every piece today; if unchanged, overflow would move the sticky and defeat affinity/HOL. **Amended** P2.4: restick only on sticky/hol paths. | amended |
| R11 | HOL `conn_has_interactive` and fan-out: overflow pieces on an interactive-carrying sibling? `bulk_overflow_pick` requires `is_schedulable && is_loss_fresh && room`; it does **not** check `conn_has_interactive`. A saturated sticky is exactly when bulk should not land on the interactive sibling. **Amended:** add `!conn_has_interactive(p)` to `bulk_overflow_pick`; fall back to any same-class path with room if that empties the set (bulk must move somewhere; HOL will rebalance next tick). | amended |
| R12 | P3a `last_recv_path` may be 0 before first DATA — falls through to sticky then pool min, as today. | ok |
| R13 | P5 ACK on `recv_fin` for a stream the peer *reset*: excluded by the `reset` check first. ACK on unknown stream: not possible (no `StreamState`); early-data path unchanged. | ok |
| R14 | `n_counter` list: budget_blocks, window_limited_with_room, dup_rx_bytes, ack_after_fin, dropped_resend, tcp_retrans = 6 ⇒ 60. Histograms excluded from the count as today. | ok |
| R15 | Wire/TOML/Tuning: no new field in `Tuning`; constants derived (`inflight_bias`, `chan × MAX_STREAM_PAYLOAD`, `unknown_rtt_us`, `down_timeout_ceil`) or textbook (gain 2 already used; α 1/8 already used; BBR 10-RTT/100 ms; ±25 % probe check in P3b only). `PROTOCOL_VERSION 2`. | ok |
| R16 | Yuusei invariants (leftover, Residual D, server origin-EOF silence, hop RST): untouched code paths (`reap_closed_streams`, `residual_d_client`, `finish_stream`). P5 adds ACKs, not Resets. | ok |
| R17 | Interactive TTFB doors (`far_peer3_slow1` p50, `interactive_class_set`): interactive placement, affinity, hedge clock unchanged; P1 may add write-stall pick-skips on saturated paths — that is the existing C rule firing earlier, and interactive already avoids stalled dests. Soak doors are in the test list. | ok |
| R18 | Upload direction (client → server) gets the same P2/P4 on the client; the client's budget floor sum also exceeds the 7.5 MB/s observed. | ok |
| R19 | macOS build: `libc` consts gated `cfg(target_os)`; `TCP_INFO` absent ⇒ gauges absent, not zero. | ok |
| R20 | Rollback safety with mixed versions (one end old): P4/P5 are sender/receiver-local; an old receiver still never ACKs post-FIN ⇒ new sender's belt bounds it; an old sender still storms hedges ⇒ new receiver counts `dup_rx_bytes`. No protocol dependency. | ok |
| R21 | e2e `MAX_CWND` 64 × 1200 B would cap the emulated TCP below the budget ceil and hide fan-out; §e2e raises it when `rate_bps` is set. | ok |
| R22 | Observability gate is measurable without iperf: every Rollout §4 row names a metric that exists after PR 1/2/4. | ok |
| R23 | ACK-clock sample arithmetic checked for the three regimes (first burst, ACK-clocked steady state below saturation, saturated): first two give `budget/min_rtt` (doubling), third gives bottleneck `bw`. `Unacked.tried` starts as `vec![path_id]` (`streams.rs` L264), so `tried.len() == 1` is the right "never hedged" test. | ok |
| R24 | Filter expiry on idle paths drops budget to floor and can make `room` negative while bytes are still in flight. Added to P2.2; behaviour is wait-for-ACK, not error. `add_path` added to the wake list so a replacement 5-tuple frees parked streams. | amended |
| R25 | Second-order effect of P1 on `hold_stream_data` (`path.rs` L711): unknown-RTT dests still park DATA off urgent until the first Pong; lowat does not change that. `write_stalled` clearing (`WriteOne::Sent{stalled:false}`) is per successful write, so a saturated path oscillates stalled/unstalled at the lowat cadence — that is what `is_schedulable` should reflect; `bulk_affinity` keeps the stalled sticky. | ok |
