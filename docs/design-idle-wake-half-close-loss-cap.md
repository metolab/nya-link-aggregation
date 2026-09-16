# Idle writer wake, copier bounded by stream life, path-loss attribution

| Field | Value |
| --- | --- |
| **Title** | Idle writer wake, copier bounded by stream life, path-loss attribution |
| **Author** | nya-link-aggregation maintainers |
| **Date** | 2026-09-16 |
| **Status** | Draft — review loop closed after round 4 (see *Review log*) |
| **Audience** | Senior engineers in `nya-core` path IO (`path.rs` write task), hop copy (`hop.rs`), stream lifecycle (`session/mod.rs`, `session/steer.rs::reap_closed_streams`, `session/streams.rs::spawn_pump`), `net.rs` `TcpInfo`, metrics catalog / `export.rs`; `nya-server/outbound.rs`, `nya-client/inbound.rs` |
| **Predecessor** | `docs/design-loop-aware-window-fresh-rehome.md` (P1–P7, deployed as v0.1.7 on 2026-09-15). G3/G5/G6/G7 landed in production; G1 is endpoint-limited in prod (not testable there); G2/G4b did not land as worded. |
| **Compatibility** | `PROTOCOL_VERSION` stays / ALPN unchanged. No new TOML keys. `[session]` stays `deny_unknown_fields`. One production `Tuning::STANDARD`. Ping cadence, probe-miss semantics, `close_linger`, the budget controller and all its constants, `COPY_BUF`, existing hop span attributes all **unchanged** except where listed under *API / Interface Changes*. |
| **Policy** | LOOP-POLICY: every change below is a bug fix or a missing mechanism. No constant is added or retuned. §B adds **no** clock (it couples two lifetimes that already exist). §C adds **no** control (production data falsified the hypothesis a controller would have rested on; §C exports the two fields needed to keep that question answerable). |

---

## Overview

v0.1.7 ran ~19.5 h on three production pairs (`yuusei`, `hytron`, `datawave`; NRestarts=0). The overlay contracts it set out to land mostly did (`dup_rx/data_rx` ↓, `hedge/stream` ↓, `nya.limiter` attributed, `budget_bytes` above floor, `recv_cap` reverts gone). The host view, sampled 04:37Z, shows two defects that are **older than v0.1.7** but were never measured before, and one behaviour that v0.1.7 **surfaced**:

1. **Every process spins.** Clients 65 % of a 2-core box each (3 × 2 tokio workers, state R, load 5.85 on 2 cores); servers 93–97 % of their single core, load 1.00. Not traffic: `datawave` client had 0 SOCKS connections, 34–52 KB/s written, 65 % CPU. Root cause is in the per-path write task's `select!` (`path.rs`, write task): `sleep_until(next_ping)` with `next_ping` already in the past while the ping is *gated* (Pong outstanding, or the path received something within `ping_every`). The branch fires immediately, `ping_due` stays false, the loop spins without yielding. Local repro: 335 % CPU across 3 idle client processes → 7.5 % with the wait target corrected. Present since v0.1.1.
2. **`nya-server-hytron` holds 843 fds / 84 MB RSS** against 45–63 fds / 24–26 MB for its siblings. Signoz shows ~745 successful `nya.outbound.dial` spans with **no matching `nya.hop` span** over the run — copies that never ended. `copy_bidirectional_timed` inherits tokio's `copy_bidirectional` close semantics: EOF on one side shuts down the other side's writer, then the copy **waits for the other side's EOF with no bound**. Certain hytron origins never FIN after our half-close. Meanwhile the *session* has already given the stream up: `reap_closed_streams` removes any half-closed stream `close_linger` (1 s) after its FIN, after which no byte can cross the overlay for that stream in either direction. Each ghost is a hop task + copier + origin `TcpStream` (fd) + `TunnelStream` + pump task + `duplex()` pair + two `COPY_BUF`s + open hop span, kept alive for a stream the session no longer knows. ~38 ghosts/h, ≈ 80 KB each measured. Same pattern in v0.1.5/v0.1.6 data → host-specific, pre-existing. The client side has the same hole with a local app that never closes its socket.
3. **Clean links carry heavy retransmission during large downloads on `yuusei`** (6–11 % of bytes over the window; 10–70 % in individual busy 30 s steps). v0.1.6 showed ≈ 0 % — at 242 KB/s. The predecessor's *Risks* named this as a possible consequence of the larger budget. **The production data says otherwise** (§C evidence): in every lossy step the per-path overlay budget (64–366 KB) sits *below* the kernel's cwnd (220–1037 KB), `tcp_notsent` is 0 and `tcp_unacked` is 1–3 KB at p50. The overlay is already app-limiting the kernel; nothing it queues is standing behind cwnd. The loss is the network's response to ~8 MB/s aggregate across eight parallel flows through one shared bottleneck (the signature — 40–70 % retransmitted bytes with a nearly empty cwnd — is a policer or very shallow buffer, not BBR ProbeBW overshoot). A budget-side controller cannot fix that without simply sending less; whether to send less is an aggregate-rate product decision, not a mechanism gap, and is out of scope for this loop. §C therefore exports the two `TCP_INFO` fields that make this attribution readable from Signoz on any future window (`bytes_sent` as denominator, `delivery_rate_app_limited` as the "who is pushing" bit) and records the falsified hypothesis so it is not re-proposed.

Two acceptance gates of the predecessor also need to be re-worded rather than re-engineered, because the production data shows they measured the wrong thing (§D): G2 (`window_limited_with_room` counted peer-app back-pressure as an overlay window fault) and G4b (an origin that *bursts* is a fast hop while it bursts; fan-out during the burst is P3 working as specified).

This design is delivered as two PRs:

* **PR-1 — A. Writer wake target** + the process self-metrics and ping counter that gate it. The write task's timer wakes at the earliest instant at which `should_send_ping` could flip: `next_ping` when it is still ahead; the pending-ping expiry when a Pong is outstanding; the idle gate's opening when the path received something recently; never at an instant already past. Ping cadence and probe-miss semantics unchanged.
* **PR-2 — B. Copier bounded by the overlay stream's life**, **C. Path-loss attribution fields**, **D. Remaining observability and gate re-wording.** When the session removes a stream from its table (`remove_held_stream`), a `CancellationToken` shared with the stream's `TunnelStream` is cancelled. The copier treats that as EOF on the direction that reads the local socket and writes the overlay (those bytes could never be delivered anyway); shutting down that direction's overlay writer is what lets the pump exit and the overlay→local direction reach EOF after draining. No new clock. Hop span records `nya.close = "reaped"`.

---

## Background & Motivation

### What already works (do not reopen)

* P1 socket tuning, P2 recv-window loop fit, P3 fit-or-wait bulk placement, P4 fresh-rehome, P5 stall redefinition, P6 limiter attribution, P7 sticky/quiet handling — all confirmed by the v0.1.7 windows (`dup_rx/data_rx` 0.6 % → 0.03 %, `hedge/stream` 4.1 → 0.4, `nya.limiter` populated on 99 % of hops, `recv_cap` probe reverts 0).
* Ping cadence (`probe_interval_for`, `SessionConfig::ping_interval_min/max`), `should_send_ping` gating (`is_alive ∧ no pending Pong ∧ last_rx_ago ≥ ping_every`), `expire_stale_pings(loss_timeout)` from `maintain`, probe-miss → loss → down clocks. §A changes *when the task wakes*, not *when a ping is sent*.
* Stream-table lifecycle: `close_linger` (1 s) → `reap_closed_streams` → `linger_reap_progress_fine` / `residual_d_client` / `reset_stream(Timeout)` → `remove_held_stream`; test `half_close_linger_reaps_stream_table`. §B does not change *when* the session forgets a stream; it makes the copier *hear* it.
* The budget controller (`end_budget_round`), its step/probe/sag/idle logic and constants, including `BUDGET_CAP_GAIN = 3`. §C adds no input to it (see §C evidence).

### Production evidence (v0.1.7, 2026-09-15 08:56Z → 2026-09-16 04:37Z)

**Host (sampled 04:37Z, all six units NRestarts=0, ~19.5 h):**

| Process | Host | Lifetime %CPU | Threads | RSS | fd |
| --- | --- | --- | --- | --- | --- |
| client yuusei / hytron / datawave | GZ 2c | 64.9 / 64.8 / 65.1 % | 2 × tokio worker each ≈ 32 %, state R | 42–47 / 35–37 MB | 53 / 78 / 41 |
| server yuusei / hytron / datawave | HK 1c | 93.3 / 96.7 / 95.1 % | 1 × tokio worker R | 26 / **84** / 24 MB | 63 / **843** / 45 |

CPU does not follow traffic (datawave client: 0 SOCKS conns, 65 %). Load averages equal the number of spinning workers (GZ 5.85 ≈ 6; HK 1.00).

**Local repro of the spin** (3 clients + 3 servers on one host, one idle SOCKS session each): 335 % aggregate CPU. With the write task's wait target changed as in §A: 7.5 %. (Diff kept as `/tmp/nya-ping-spin.patch` during analysis; not committed.)

**Ghost copies on hytron:** successful `nya.outbound.dial` spans minus `nya.hop` spans with the same `nya.session_fp`/`nya.stream_id` ≈ 745 over the run (~38/h), concentrated on a handful of origin hosts. Per ghost (after the session's 1 s reap): hop task + `CopyTimed` (2 × 8 KiB `COPY_BUF`) + origin `TcpStream` fd + `TunnelStream` + pump task + `duplex()` pair (`BytesMut` capacity retained from the transfer) + `inbound_rx` + open hop span ≈ 80 KB measured → the ~60 MB RSS gap to the sibling servers. `yuusei`/`datawave` origins FIN promptly, so the same code does not leak there.

**Retransmission on yuusei clean links — budget vs kernel cwnd** (server side, 30 s steps with ≥ 5 MB delivered, 2026-09-16 02:30Z–03:00Z burst; p50 over steps; "lossy" = retransmitted bytes > 1 % of delivered):

| Path | Busy steps | Lossy | Budget p50 | Kernel cwnd p50 | `tcp_unacked` p50 | `tcp_notsent` p50 | Steps with budget > cwnd + notsent |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `nsix#0` | 15 | 10 | 157 KB | 428 KB | 17 KB | 0 | **0 / 10** |
| `akcdn#0` | 15 | 11 | 175 KB | 317 KB | 1 KB | 0 | **0 / 11** |
| `nsix#1` | 15 | 11 | 216 KB | 471 KB | 3 KB | 0 | **0 / 11** |
| `akcdn#1` | 15 | 8 | 207 KB | 424 KB | 3 KB | 0 | **0 / 8** |

Worst steps: `nsix#0` 02:45Z delivered 39.6 MB, retransmitted 28.4 MB, budget 64 KB, cwnd 484 KB, unacked 3 KB; `akcdn#1` 02:33Z delivered 41.4 MB, retransmitted 29.8 MB, budget 359 KB, cwnd 440 KB. All four clean paths go lossy in the same steps (shared bottleneck). The budget never exceeded cwnd; the kernel's send queue was empty; the kernel was app-limited by us. v0.1.6's ≈ 0 % was at 242 KB/s — below the regime where this bottleneck bites.

**Gates that measured the wrong thing:**

* G2 (`window_limited_with_room` ratio should fall): stayed ≈ 1.0. Receiver-side `nya.zero_win_app` ≫ `nya.zero_win_hole` on the same hops: the sender is blocked because the *peer app* is slow, which is the window doing its job. The gate cannot distinguish that from an overlay hole without the receiver's cause.
* G4b (slow-origin streams stay on one sticky, no hedges): hops with origin-paced rates of ~130 KB/s show `paths_used` up to 16 and `budget_diverts > 0`. Those origins deliver in bursts (hundreds of KB, then silence; visible as large `nya.max_gap_us` with high `nya.origin_read_wait_us`); during the burst the hop *is* fast and P3 overflows the `inflight_bias` sticky budget by design. The gate's premise (smooth pacing) does not hold for these origins.

### Clocks (do not retune)

`SessionConfig::ping_interval_min/max`, `probe_interval_for`, `loss_timeout_floor/ceil` (20 ms / 2 s), `down_timeout_ceil` (5 s), `SessionConfig::all_down_timeout` (8 s), `Tuning::close_linger` (1 s), `maintain_interval` (5 ms), `MIN_RTT_WIN_US` (10 s).

---

## Goals & Non-Goals

### Goals

* **G-A** Idle CPU per process ≤ ~2 % of one core on the production hosts (clients: `nya_streams_held == 0`; servers: idle session). Measured via `rate(nya_process_cpu_seconds_total[10m])`. Ping cadence unchanged: `rate(nya_pings_total[10m])` matches the local 3×3 repro with A off/on on the same binary within ±5 % (primary), and stays ≤ the analytic upper bound `Σ_paths 1 / probe_interval_for(path)` (the idle gate `last_rx_ago ≥ ping_every` is reset by the peer's own pings/pongs, so the wire rate is phase-dependent and at most the analytic figure). No v0.1.7 series exists for pings; the baseline is repro + bound, not historical.
* **G-B** `nya-server-hytron` fd count tracks live work: `nya_process_open_fds − nya_streams_held − 2 × count(nya_path_rtt_known)` (each path holds its socket and the dup'd `PathFd`) is flat over 24 h and equal to the process's static fd baseline (listeners, eventfd, log, exporter — measured once at startup and exported as `nya_process_open_fds_baseline`); RSS within 2× of sibling servers after 24 h; `nya_hop_reaped_total` rate ≈ the pre-fix ghost rate (~38/h) **plus** the former session-reset `copy_err` rate on hytron, ≈ the latter alone elsewhere; no increase in the per-reason `nya_stream_resets_*_total` series or client `unexpected-eof` WARN.
* **G-C** Path-loss attribution readable from Signoz alone: per path and 10 s snapshot, `Δnya_path_tcp_bytes_retrans_total / Δnya_path_tcp_bytes_sent_total` and `nya_path_tcp_app_limited`. On the next yuusei burst the table above can be reproduced without the ad-hoc join, and the "budget < cwnd, app-limited" attribution confirmed or refuted per step.
* **G-D** The two host defects are readable from Signoz alone; G2/G4b are re-worded so they can be evaluated from existing span attributes.

### Non-goals

* No change to the ping cadence, probe-miss, loss/down clocks (§A only fixes the wake instant).
* No change to `close_linger` or to *when* the session forgets a half-closed stream (§B only propagates that event). The observation that a stream half-closed by the peer while a transfer is still in flight (`acked < next`) is `reset_stream(Timeout)` at 1 s is recorded under *Open Questions*; it is not this design's scope.
* **No budget/loss controller.** The v0.1.7 data (§C evidence) shows the overlay budget below kernel cwnd with an empty send queue during loss; a controller that pins the budget lower can only lower offered load. Aggregate rate control is a product decision outside LOOP-POLICY's scope; if taken up, it needs its own design with the G-C series as input. The controller sketch reviewed in rounds 1–3 is preserved under *Alternatives §10* for the record.
* No fix for `Exit::Down`'s 5 ms poll (per path) or `maintain()`'s 5 ms tick (per session): both are bounded (≤ 200 wakes/s per path / per session) and not the observed spin.
* No new TOML keys. No change to `hytron` client `data_retransmit` (+ modest, within noise; watch).
* G1 (link-rate downloads) stays untestable in prod (hytron large downloads are origin-limited).

---

## Key Decisions

| # | Decision | Why |
| --- | --- | --- |
| KD1 | §A recomputes the timer target; it does **not** add a Pong/RX `Notify`. | A timer with a correct target bounds wakes at ≤ 1 per gate interval per path. A Pong/RX notifier would wake the writer to re-evaluate a ping still gated by `next_ping` — under bulk RX that is a wake per frame for nothing. Event-driven wake is *Alternatives §1*. |
| KD2 | §A's Pong-outstanding branch waits for the *pending-ping expiry*, not a fixed `ping_every`. | The pending gate clears only when `maintain` runs `expire_stale_pings(loss_timeout)`; the instant is known (`loss_timeout − pending_ping_age`). Waiting exactly that long keeps post-miss cadence identical to today's (spinning) behaviour to within one `maintain_interval`. |
| KD3 | §B couples the copier to `remove_held_stream`, adds **no** linger clock. | After `remove_held_stream` no byte can cross the overlay for that stream (`send_data → UnknownStream`, inbound frames dropped). Any copier still running is dead work — except for the bytes already in the overlay→local pipe, which still deserve delivery (see *Residual*); the only missing piece is the wake. A separate copier clock would be a second opinion on a decision the session has already taken. |
| KD4 | §B applies *gone* as a synthetic EOF on the local→overlay direction only, and lets overlay→local reach EOF through the pump. | Shutting down the local→overlay writer is what makes the pump's `send` half read `Ok(0)` and exit; only then does the whole `DuplexStream` drop and the copier's overlay→local reader see EOF after draining what the pump had written. Aborting the copy instead would lose that drain. |
| KD5 | §B ends the copy with `Ok(outcome)` (flag set), not `Err`. | Bytes were delivered; from the app's view the hop completed. Attribution is `nya.close = "reaped"`, not `copy_err`. |
| KD6 | §B's *gone* signal is a `tokio_util::sync::CancellationToken` on the shared `StreamCounters`, cancelled from `remove_held_stream` after the `streams` guard is released; the copier receives `token.cancelled_owned()` (a `'static` future), taken before the copy starts. | `tokio-util` is already a dependency (`codec`, `io` features; add `sync`). The token replaces a hand-rolled flag + `Notify` + lost-wakeup pattern with a primitive whose semantics are exactly "fires once, observable late, owned future". `remove_held_stream` is the single point where a stream leaves `streams` (`Inner::drop` is the only bypass and cannot run while a hop holds a `Session`); `StreamCounters` is already shared between `StreamState` and `TunnelStream`; an owned future avoids borrowing `overlay` while `&mut overlay` is live in the copy. `StreamCounters`' "holds no channel, buffer or `Notify`" comment is updated to its intent: holds nothing that can keep the pump alive (a token owns no task or buffer). |
| KD7 | §C adds two `TcpInfo` fields and their per-path series; **no** controller. | The controller's premise ("budget grows past what the pipe carries; the excess queues; the kernel loses") is false in the only production window where loss is measurable: budget < cwnd, `notsent = 0`, `unacked ≪ cwnd` in every lossy step. The remaining question — whether to offer less aggregate load to a shared bottleneck — is not a mechanism gap. The two fields (`bytes_sent`, `delivery_rate_app_limited`) are what was missing to make that reading without a hand-rolled join. |
| KD8 | The G-A ping-cadence baseline is analytic + local repro, not a v0.1.7 series. | No ping counter existed before this design; `nya_pings_total` ships in PR-1 together with A, so the gate is evaluated on the same binary. `Σ 1/probe_interval_for` is exact for an idle path set. |
| KD9 | G2/G4b are re-worded (§D), not re-engineered. | The mechanisms they gate are working; the gates counted app back-pressure and burst pacing as overlay faults. |

---

## Proposed Design

### A. Writer wake target (`crates/nya-core/src/path.rs`, write task)

**Today** (write task, the `ping_due` computation and the timer arm of the `select!`):

```rust
let ping_due = Instant::now() >= next_ping && path_w.is_alive() && !session_w.is_dead()
    && path_w.should_send_ping(path_w.last_rx_ago(), ping_every);
...
_ = tokio::time::sleep_until(next_ping), if !ping_due => {}
```

`should_send_ping` is false while a Pong is outstanding or while the path received a frame within `ping_every`. In both cases `next_ping` (set to `now + ping_every` when the last ping was *sent*) is already in the past; `sleep_until(past)` is `Ready` immediately; the branch is taken; nothing else is ready; the loop re-evaluates and repeats. No `.await` ever returns `Pending`, so the worker never parks. Every alive path on every process spins whenever it is idle *or* receiving data (the second gate).

**Contract.** The write task's timer wakes at the earliest instant at which `should_send_ping` could flip to true, and never at an instant already past:

```
ago      = path_w.last_rx_ago()
pending  = path_w.pending_ping_age()            // Some(age) when a Pong is outstanding
loss_for = session_w.loss_timeout_for(&path_w)  // new accessor = health::loss_timeout(&cfg, path.stable_rtt()),
                                                // the clock maintain passes to expire_stale_pings;
                                                // mirrors the existing down_for / degrade_for

ping_wait =
  if now < next_ping                 → next_ping
  else if let Some(age) = pending    → now + clamp(loss_for − age, maintain_interval, ping_every)
  else if ago < ping_every           → now + (ping_every − ago)            // idle gate
  else                               → now + ping_every                    // !is_alive / session dead / any other gate
```

All gated branches are upper bounds: a Pong arrival, new RX, or path death triggers other `select!` arms (outbound frame, ACK, `Exit::*`) and the *next* iteration recomputes `ping_wait`. `next_ping` itself is not moved, so the cadence after a sent ping is unchanged; the pending-expiry branch wakes within one `maintain_interval` of the instant the gate actually clears, which is what the spinning loop achieved at the cost of a core.

**Implementation.** Compute `ping_every`, `ago`, `pending`, `ping_due`, `ping_wait` once per iteration before the `select!`; replace `sleep_until(next_ping)` with `sleep_until(ping_wait)`. All arithmetic saturating. `nya_pings_total` increments where `write_one` reports `Sent` for a `Frame::Ping` (pings are written directly, not queued). For the unit tests, the write task takes an optional `Arc<AtomicU64>` wake counter through the existing spawn path (`spawn_path_io`), incremented once per timer-arm wake; `None` in production.

**Not changed.** `next_ping = now + ping_every` after a ping is sent; `Exit::Down` polling; ACK batching arm; `ack_wait` notify; `write_one`; the read task (verified: no other unyielding loop in the path tasks).

### B. Copier bounded by the overlay stream's life (`hop.rs`, `stream.rs`, `session/mod.rs`)

**Today.** `DirState` for direction X reaches `done` after EOF read → flush → `poll_shutdown` on the far writer. `CopyTimed::poll` returns `Ready` only when *both* directions are `done`. If the local peer (origin on the server, app on the client) never sends EOF, the local→overlay direction stays `Pending` on `poll_read` with nothing to wake it.

Independently, the session bounds the *stream*: `reap_closed_streams` (every `maintain` tick) removes any stream with `send_fin_sent || recv_fin` whose `close_started_ms` is ≥ `close_linger` (1 s) old. All paths end in `remove_held_stream`, which drops the `StreamState` and with it `inbound_tx`; the pump's `recv` half sees `None`, breaks, and drops its `WriteHalf`. **That alone does not give the copier EOF**: `tokio::io::split` halves share one `DuplexStream`, and the `WriteHalf`'s drop does not shut it down; the copier's overlay→local reader sees EOF only when the *whole* `peer` `DuplexStream` drops, i.e. when the pump's `send` half also exits — and that half is blocked in `r.read()` waiting for the copier to write. Today only the `Inbound::Close` path (`linger_reap_progress_fine` → `w.shutdown()`) gives the copier EOF on that direction; on `Reset`/`None` removal the overlay→local direction is stuck as well. Either way the local→overlay direction is never woken. It, the hop task, the origin/app socket, the `TunnelStream`, the pump task and the hop span live until process exit. The stream-table entry itself is **not** part of the ghost.

**Contract.** A copier outlives the overlay stream it serves by at most the time the local peer takes to accept the bytes already in the overlay→local pipe. When the session removes stream `id`, the copier holding that stream's `TunnelStream`:

1. treats the direction *reading local, writing overlay* as having read EOF, **provided it has not already read EOF** (`!ab.read_done`; a graceful close racing the removal is not relabelled). Bytes still buffered in `DirState` for that direction are discarded and counted in a new `DirCopy::dropped_bytes` (they had no destination);
2. flushes and shuts down the overlay writer for that direction (`DuplexStream::poll_shutdown` is local and never blocks). This is the causal step: the pump's `r.read()` returns `Ok(0)`, `close_send(id)` is a no-op (no stream), the `send` half exits, `join!` completes, the pump drops `peer`;
3. the *reading overlay, writing local* direction now reaches EOF once it has drained what the pump had written — buffered bytes reach the local socket;
4. returns `Ok(CopyOutcome { .., reaped: true })` via the existing both-done path.

The caller (`outbound.rs`, `inbound.rs::copy_with_hop`) then drops both halves as today: the local `TcpStream` closes (FIN, or RST if the peer had unread data pending — the session gave it `close_linger` after our half-close, and the stream is already gone), the `TunnelStream` drops, `reap_stream(id)` is idempotent. No new frame, no new state, no new clock. If step 2's `poll_shutdown` ever returned an error the copy ends with that `Err` as today (`copy_err`), which also drops everything.

**Residual (unchanged from today, now stated).** Step 3 has no bound. If the local peer is alive but *not reading* (origin/app socket send buffer full), `ba` stays `Pending` in `poll_write`; the duplex fills; the pump's `recv` half blocks in `w.write_all`; the `inbound` channel (cap `chan`) fills and `inbound_tx.try_send(Close|Reset)` in `finish_stream`/`linger_reap_progress_fine` fails silently; `peer` is not dropped until the peer drains. This is *not* the hytron ghost class (silent origin, `ba` idle → EOF right after step 2) and it cannot be closed without a clock, which this design deliberately does not add. It is listed under *Risks* with its detection signal, and unit test B(vii) documents it.

**Signal.** `StreamCounters` gains `gone: CancellationToken`. `remove_held_stream` calls `st.gone.cancel()` **after** the `streams` guard has been released (it already is at the end of the `let-else`; the PR keeps the cancel outside any lock). `TunnelStream::gone(&self) -> WaitForCancellationFutureOwned` returns `self.counters.gone.clone().cancelled_owned()` — owned, `'static`, so it is taken *before* `&mut overlay` is borrowed by the copy. `StreamCounters` is already shared between `StreamState` and `TunnelStream` (`try_alloc_local_stream`); nothing new crosses `IncomingStream`. The struct comment's invariant is reworded to its intent: holds nothing that can keep the pump alive.

**Copier.** `copy_bidirectional_timed(a, b)` keeps its signature (used by tests) and delegates to

```rust
pub async fn copy_bidirectional_timed_until<A, B, G>(a: &mut A, b: &mut B, b_gone: G)
    -> io::Result<CopyOutcome>
where G: Future<Output = ()>;
```

whose contract is: when `b_gone` resolves, direction `a→b` is treated as EOF (items 1–2 above). Both call sites do `let gone = overlay.inner().gone();` before the copy and pass it as `b_gone` (`a` is the local socket at both sites). `CopyTimed` gains `gone: Option<Pin<Box<G>>>` and `reaped: bool`; in `poll`, before polling the directions, if `gone` is `Some` and `Ready`: take it (`None`, fused — never polled again), and if `!ab.read_done`: `ab.read_done = true; ab.out.dropped_bytes += cap − pos; ab.pos = ab.cap; reaped = true`. The existing `DirState::poll` then runs flush → shutdown → done for `ab`. If `ab.wait` was `Some((_, Read))` at that moment, `ready()` is not called, so that final read wait is not billed to `read_wait_us` — intended: dead time after the stream is gone is not a limiter.

`CopyOutcome` gains `reaped: bool`; `DirCopy` gains `dropped_bytes: u64`. `HopSample` gains `close: Option<&'static str>` ∈ {`"eof"`, `"reaped"`} recorded as `nya.close` only on `Ok` outcomes (`Err` is already `nya.outcome = copy_err`), and `dropped_bytes: Option<u64>` recorded as `nya.dropped_bytes`. `nya.rx_bytes` is counted by `HopClock` on read and therefore *includes* dropped bytes; the delivered figure is `rx − dropped`. `nya.limiter` attribution is unaffected.

**Outcome shift to expect.** A stream reset by the session while `ab` is mid-transfer ends the copier today with `BrokenPipe` (`nya.outcome = copy_err`). With §B, cancellation happens synchronously inside `finish_stream → remove_held_stream` and will usually win that race, so such hops become `Ok` + `nya.close = reaped` with `nya.dropped_bytes > 0`. `copy_err` counts will drop and `nya_hop_reaped_total` will exceed the ghost rate by that amount; G-B reads the two together. `reaped ∧ dropped_bytes > 0` is also the span signature for *Open Question 1* (the pre-existing 1 s half-close cut), which §B makes visible rather than hiding it in `copy_err`.

### C. Path-loss attribution (`net.rs`, `catalog.rs`, `export.rs`)

**Evidence** (table in *Background*): during every lossy 30 s step on yuusei's four clean paths, budget < kernel cwnd (typically ⅓–½ of it), `tcp_notsent = 0`, `tcp_unacked` 1–17 KB against cwnd 220–1037 KB. Retransmitted bytes reached 40–70 % of delivered in the worst steps, on all four paths simultaneously. This falsifies the working hypothesis of the predecessor's risk note and of rounds 1–3 of this design (that the 3×BDP budget cap lets the overlay queue into the kernel and the kernel loses probing that queue). The overlay is already the limiter of each kernel flow; the loss is what the shared bottleneck does to ~8 MB/s of aggregate offered load. Reducing per-path budgets further would reduce goodput one-for-one with no queue to remove.

**What is missing to read this without a hand-rolled join:**

* a *denominator* for the retransmitted-bytes ratio on the same socket (`tcpi_bytes_sent`, offset 200, Linux ≥ 4.19 — same series as `bytes_retrans` at 208, already parsed);
* the kernel's own statement of who is limiting (`tcpi_delivery_rate_app_limited`, bit 0 of byte 7 — the bitfield byte after `snd_wscale/rcv_wscale`; `tcpi_rto` starts at byte 8): `1` means the kernel's delivery-rate sample was capped by the application (us), which is what the table infers indirectly from `unacked ≪ cwnd`.

**Changes.** `TcpInfo` `+ bytes_sent: u64`, `+ app_limited: bool`. Per-path series `nya_path_tcp_bytes_sent_total` (counter) and `nya_path_tcp_app_limited` (gauge 0/1), exported with the existing 10 s per-path `TCP_INFO` snapshot — no extra `getsockopt`. Hop-span `nya.origin_tcp_*` gains `nya.origin_tcp_bytes_sent` for symmetry (origin-side ratio for the server hop).

**Reading.** Per path and step: `loss = Δbytes_retrans / Δbytes_sent`; `pusher = app_limited ? "overlay" : "kernel"`; with `nya_path_budget_bytes` and `nya_path_tcp_cwnd_bytes` alongside. If a future window shows `budget > cwnd + notsent` with `app_limited = 0` during loss, the queue hypothesis is back on the table and *Alternatives §10* is the starting point. If it shows what v0.1.7 showed, the only overlay-side lever is aggregate offered load, which is a product decision (how much of a shared bottleneck one session may take) to be designed on its own terms.

### D. Acceptance and observability

* **Process self-metrics** (`export.rs`, sampled with the existing 10 s snapshot; Linux-only, absent elsewhere): `nya_process_cpu_seconds_total` (`/proc/self/stat` utime+stime), `nya_process_open_fds` (`/proc/self/fd` entry count), `nya_process_open_fds_baseline` (same count taken once in `main()` after the listener and exporter binds and before any `Session` is constructed — on the client that is before the startup dials begin), `nya_process_rss_bytes` (`/proc/self/statm`). **Ship in PR-1** with A.
* **`nya_pings_total`** counter (§A). **Ship in PR-1.**
* **`nya_hop_reaped_total`** counter (no role label — role is the binary, as with `nya_inbound_*`/`nya_outbound_*`); `nya.close`, `nya.dropped_bytes` span attributes (§B).
* **`nya_path_tcp_bytes_sent_total`**, **`nya_path_tcp_app_limited`**, `nya.origin_tcp_bytes_sent` (§C).
* **Receiver-side window cause** already exists: `nya_zero_window_total{cause=hole|app}` and per-hop `nya.zero_win_hole` / `nya.zero_win_app`. No new sender-side attribute (the sender cannot know the cause).
* **Stall clock.** Catalog note on `stall_ms` / `stall_enter`: semantics changed in v0.1.7 (busy-burst counting removed); do not compare against pre-v0.1.7 windows. Dashboards get a version annotation.
* **G2 re-worded:** "Among large hops whose *receiver* hop span reports `nya.zero_win_hole == 0`, `window_limited_with_room / window_blocks` ≤ 0.1." Hops with `nya.zero_win_app > 0` are app-limited by definition and excluded.
* **G4b re-worded:** "For hops with `nya.max_gap_us ≥ 10 × nya.origin_tcp_min_rtt_us` and `nya.origin_read_wait_us ≥ 50 %` of `nya.copy_us` (burst-paced origin), fan-out during bursts is expected; the gate is `hedges / stream ≤ 1` and `dup_rx / data_rx ≤ 0.1 %`. For hops without that signature (smooth pacing), `nya.paths_used ≤ 2` and hedges only on `path_down`." All from existing `nya.hop` span attributes.

---

## API / Interface Changes

| Surface | Change |
| --- | --- |
| `Session` | `+ loss_timeout_for(&PathState) -> Duration` (= `health::loss_timeout(&cfg, path.stable_rtt())`, mirroring `down_for` / `degrade_for`). |
| Path IO spawn | write task accepts an optional `Arc<AtomicU64>` timer-wake counter (tests only; `None` in production). |
| `net::TcpInfo` | `+ bytes_sent: u64`, `+ app_limited: bool`. |
| `stream::StreamCounters` | `+ gone: CancellationToken`; `TunnelStream::gone() -> WaitForCancellationFutureOwned`. Struct comment updated (KD6). `tokio-util` gains the `sync` feature. |
| `hop::CopyOutcome` | `+ reaped: bool`; `DirCopy + dropped_bytes: u64`. `copy_bidirectional_timed` unchanged; `+ copy_bidirectional_timed_until(a, b, b_gone)`. |
| `HopSample` | `+ close: Option<&'static str>`, `+ dropped_bytes: Option<u64>` (`None` when the copy did not run, as the other counters); span attrs `nya.close` ∈ {`eof`, `reaped`}, `nya.dropped_bytes`, `nya.origin_tcp_bytes_sent`. |
| Metrics catalog | `+ nya_process_cpu_seconds_total`, `nya_process_open_fds`, `nya_process_open_fds_baseline`, `nya_process_rss_bytes`, `nya_pings_total`, `nya_hop_reaped_total`, `nya_path_tcp_bytes_sent_total{path}`, `nya_path_tcp_app_limited{path}`; catalog note on `stall_ms`. |
| `Tuning` / TOML / wire / budget constants | none. |

## Data Model Changes

`CopyTimed`: `+ gone`, `+ reaped`. `DirCopy`: `+ dropped_bytes`. `StreamCounters`: `+ gone`. `TcpInfo`: two fields. No `PathState` changes.

---

## Alternatives Considered

### 1. Event-driven writer wake (Pong `Notify`, RX `Notify`) instead of a recomputed timer
Wakes the writer on every received frame to re-evaluate a ping that is still gated by `next_ping`; under bulk RX that is a wake per frame for nothing. The timer bound is strictly fewer wakes and is the fix that was measured (335 % → 7.5 %). Rejected.

### 2. Uniform `now + ping_every` for every gated case
Simpler than KD2 by one branch; costs up to one `ping_every` of extra delay on the first ping after a probe miss, which today is sent within a `maintain` tick. The exact expiry is one subtraction away. Rejected.

### 3. `tokio::time::interval` with `MissedTickBehavior::Delay` for pings
Changes cadence semantics (`next_ping` is anchored to the *send*, not to a tick) and still fires while gated. Rejected.

### 4. Copier-side half-close linger (round 1 of this design: 8 s no-bytes clock)
Adds a clock that can only disagree with `close_linger`: shorter cuts a stream the session still holds; longer is dead work. Its derivation ("must exceed every session clock") was backwards — the session forgets the stream at 1 s. Also mis-treated a write-blocked direction as silent. Rejected in favour of §B's lifetime coupling.

### 5. Fix the leak in `nya-server/outbound.rs` only (timeout around the origin copy)
Leaves the client-side app hole and duplicates copier state outside it. Rejected.

### 6. `SO_LINGER{0}` / RST on our half-close
Changes semantics for every well-behaved origin (data still in flight from origin to app would be lost). Rejected.

### 7. `select! { copy, overlay.gone() }` at the call sites (abort the copy on *gone*)
Loses bytes the pump already pushed into the `duplex()` that the copier has not yet written to the local socket. §B's in-copier synthetic EOF keeps the drain. Rejected.

### 8. Have the session shut down the pump's `WriteHalf` on every removal path (not only `Close`)
Would give the copier EOF on overlay→local for `Reset`/`None` removals too, but still cannot wake the local→overlay direction blocked on the local socket; the copier-side wake is unavoidable. §B's step 2 achieves the same EOF as a consequence. Rejected as insufficient alone.

### 9. Hand-rolled `AtomicBool + Notify` *gone* signal (rounds 2–3)
Correct with the `enable()` pattern, but `CancellationToken` is the same thing with the lost-wakeup case handled by the library and an owned future built in. Replaced (KD6).

### 10. Loss-gated budget ceiling (rounds 1–3 §C) — **rejected on production data**
Mechanism as reviewed: per ACK round read `TCP_INFO`; a *limited* round with `Δbytes_retrans / Δbytes_sent ≥ 2 %` (BBRv2 `bbr_loss_thresh`) cannot step or launch a probe, rolls back a probe in flight, and pins the ceiling to `bw_now × min_rtt × 2` (BBR `cwnd_gain × BtlBw × RTprop`) until three clean rounds. Reviewed to be consistent with `end_budget_round`'s control flow. Rejected because (a) its premise — budget above cwnd, queue in the socket — is false in every lossy production step (budget < cwnd, `notsent = 0`, `unacked ≪ cwnd`), so the pin could only lower offered load below what the kernel already accepts; (b) at yuusei per-path rates a 13–60 ms round carries 4–20 MSS, so any single retransmit is ≥ 5 % and the 2 % threshold degenerates to "a retransmit happened", which is not the quantity BBRv2 applies it to; (c) "≤ 2 % loss at ≥ 85 % goodput" was a trade, not a fix. Kept here so it is not re-proposed without new data; the §C series are the data.

### 11. Lower `BUDGET_CAP_GAIN` to 2
Reintroduces the measured 75–85 % link utilisation under reverse bulk that motivated 3, on *all* paths; and, per §10(a), the cap is not what is binding during loss. A retune. Rejected.

### 12. Feed loss into loop-fit placement only (mark lossy paths unfit)
When every clean path shares one bottleneck they all go lossy together; unfit-all degenerates to least-bad-fit and total offered load is unchanged. Rejected.

### 13. Aggregate session rate limit
The one lever that would reduce the shared-bottleneck loss. It is a product decision (how much of a bottleneck one session may take, and at whose expense), not a mechanism gap, and LOOP-POLICY does not cover it. Out of scope; §C provides its input data.

---

## Security & Privacy Considerations

None new. `TCP_INFO` is already read. `/proc/self` reads are the process's own. No new wire fields.

---

## Observability

See §D. Verification queries for the rollout gates:

* **G-A:** `rate(nya_process_cpu_seconds_total[10m])` per unit ≤ 0.02 when `nya_streams_held == 0`; `rate(nya_pings_total[10m])` within ±5 % of the local 3×3 repro with A off/on (same binary), and ≤ `Σ_paths 1 / probe_interval_for` (computable from `nya_path_stable_rtt_us` and the `ping_interval_*` config).
* **G-B:** `nya_process_open_fds − nya_streams_held − 2 × count(nya_path_rtt_known) − nya_process_open_fds_baseline` ≈ 0 and flat over 24 h on hytron; `rate(nya_hop_reaped_total)` ≈ ~38/h + former session-reset `copy_err` rate on hytron, ≈ the latter alone elsewhere; per-reason `nya_stream_resets_*_total` and client `unexpected-eof` WARN unchanged; `nya.hop` spans with `nya.close = reaped` end ≈ `close_linger` after the client's FIN. Residual non-reading-peer copies show as the fd formula drifting above 0 with no `nya.hop` end — expected ≈ 0 (not observed in v0.1.5–v0.1.7 data).
* **G-C:** on the next yuusei burst, per path per 10 s: `Δnya_path_tcp_bytes_retrans_total / Δnya_path_tcp_bytes_sent_total`, `nya_path_tcp_app_limited`, `nya_path_budget_bytes`, `nya_path_tcp_cwnd_bytes`, `nya_path_tcp_notsent_bytes` — the *Background* table reproduced from the exporter alone.

---

## Rollout Plan

1. **PR-1 (A + process self-metrics + `nya_pings_total`).** Unit tests + local 3×3 repro (CPU and ping rate before/after on the same binary with A toggled by a test-only env, removed before merge). Deploy to all six units; gate G-A over 1 h. Smallest diff; unblocks the CPU budget on the 1-core HK hosts.
2. **PR-2 (B + C + rest of D).** Unit and integration tests; deploy; gate G-B over 24 h on hytron (fd/RSS flat) and "no new resets/WARN" on all three; G-C read on the next yuusei burst.

Rollback per PR is a redeploy of the previous binary; no state or wire compatibility concerns.

---

## Risks

| Risk | Mitigation |
| --- | --- |
| §A: a gated ping is sent later than today in the worst case (today it is sent the instant the gate opens because the loop is spinning). | Pending-expiry branch: within one `maintain_interval` (5 ms) of the gate clearing. Idle-gate branch: exact. Catch-all: one `ping_every`, only when the path is not alive or the session is dead (no ping would be sent anyway). G-A asserts `nya_pings_total` against the analytic rate. |
| §B: bytes read from the local socket after *gone* are discarded. | They had no destination (stream already removed); counted in `dropped_bytes` and visible on the span. Today those bytes are read into the copier and stuck forever. |
| §B: RST instead of FIN at drop when the local peer had unread data pending. | Only after the session's `close_linger` and only for a stream already gone; today the socket is held open indefinitely instead. |
| §B: `remove_held_stream` is assumed to be the single removal point from `streams`. | Verified in review: `linger_reap_progress_fine`, `residual_d_client`, `finish_stream`, `reap_stream`/`maybe_count_graceful`, `mark_dead → finish_stream` all go through it; `Inner::drop` is the only bypass and cannot run while a hop holds a `Session` (`IncomingStream.session`, `copy_with_hop(&Session)`). The PR adds a debug assertion that `streams.remove` is only called there. |
| §B: `cancel()` called while the `streams` mutex is held would run copier wakers under the lock. | `remove_held_stream` releases the guard at the end of its `let-else` before anything else; the PR keeps `cancel()` after that point and the unit test asserts no lock is held (`try_lock` succeeds inside a waker). |
| §B residual: local peer alive but not reading after *gone* → `ba` blocked in `poll_write`, copier lives until the peer drains (unbounded; unchanged from today). | Not the observed ghost class (silent origin → immediate EOF). Detectable as the G-B fd formula drifting above 0 with no `nya.hop` end. Documented by unit test B(vii). Closing it needs a clock, which is out of scope here and would be a *new* mechanism decision, not a fix to this one. |
| §B: `poll_shutdown` on the overlay writer fails after *gone*. | Copy ends with `Err` → `copy_err` as today; everything drops. Not observed (`DuplexStream` shutdown is infallible). |
| §C: `tcpi_bytes_sent` / `app_limited` absent (kernel < 4.19). | Read as 0/false; series present but flat; attribution falls back to the `unacked ≪ cwnd` inference. Logged once at startup as with P1 socket tuning. |
| §D: `/proc/self/fd` directory scan every 10 s on an 843-fd process. | ~1 ms; only on Linux; only in the snapshot path. |
| Yuusei's shared-bottleneck loss is left as is. | Stated explicitly (KD7, Non-goals, Alt §13). It is not made worse by this design; the data to decide on aggregate rate control ships in PR-2. |

---

## Open Questions

1. **Pre-existing, out of scope:** `reap_closed_streams` resets a half-closed stream with `acked < next` at `close_linger`, i.e. a client that does `shutdown(SHUT_WR)` after its request and then downloads for > 1 s is cut with `Reset(Timeout)`. Protocols that half-close early (some gRPC/streaming clients, `nc -N`) would hit this. After PR-2 the signature is `nya.close = reaped ∧ nya.dropped_bytes > 0` on the server hop; production `nya_stream_resets_timeout_total` and that signature should be read before the next iteration decides whether it is a bug or an accepted contract.
2. §B: should `nya.close = reaped` hops also carry the origin host on the server (as the dial span already does), so hytron's non-FIN origins can be listed? Proposal: no new attribute; join `nya.hop` with `nya.outbound.dial` on `nya.stream_id` as the analysis already did.
3. Aggregate offered load on a shared bottleneck (Alt §13): whether the product wants a per-session ceiling, and at what signal. Not for this loop; needs the §C series first.

---

## Testing

### Unit (`cargo test -p nya-core`)

* **A.** (i) Path whose Pong never arrives, no outbound traffic, `loss_timeout` ≫ `ping_every`: over 20 × `ping_every`, timer-arm wakes ≤ `loss_timeout / ping_every + 2` (the pending branch re-arms at most once per `ping_every`), exactly one ping sent while the gate holds, `next_ping` unchanged. (ii) Path receiving a frame every `ping_every / 4`: timer-arm wakes ≤ 1.34 per `ping_every` (the idle gate re-arms at `¾ ping_every`), no ping sent. (iii) After the Pong arrives, the next ping is sent within `ping_every + maintain_interval` of `next_ping` — cadence preserved. (iv) `!is_alive` path: timer-arm wakes ≤ 1 per `ping_every`. (v) `nya_pings_total` equals pings observed on the wire in (i)–(iii).
* **B (copier, bare `duplex` pairs).** `copy_bidirectional_timed_until(a, b, gone)` with a `CancellationToken`: (i) `b`'s peer sends EOF, `a`'s peer never does, then `cancel()` → returns `Ok` with `reaped = true`, `a→b` shut down, all bytes `b`'s peer wrote before EOF arrive at `a`; (ii) `cancel()` while `a→b` has `pos < cap` buffered → those bytes are in `dropped_bytes`, not `bytes`; (iii) `cancel()` while `b→a` still has undelivered bytes in the duplex → delivered before `Ready`; (iv) both EOF promptly, never cancelled → `reaped = false`, byte counts as before; (v) `cancel()` after `a→b` already read EOF (graceful close racing removal) → `reaped = false`; (vi) `cancel()` before the first poll → `reaped = true` on the first poll; (vii) `cancel()` while `a`'s writer is blocked (`a`'s peer not reading) → copier stays `Pending`, completes once the peer reads — documents the residual; (viii) existing wait-accounting tests unchanged.
* **B (session, real pump).** Extend `half_close_linger_reaps_stream_table` and add a `Reset`-path variant: a `TunnelStream` whose local peer never EOFs, removal via `linger_reap_progress_fine` *and* via `reset_stream(Timeout)` → in both cases `gone()` resolves, the copier returns `reaped = true`, the pump task exits (observed via a `#[cfg(test)]` pump-exit counter on the session, since `spawn_pump` discards its `JoinHandle`), and the bytes the peer sent before removal are delivered. Assert `cancel()` runs with `streams` unlocked (`try_lock().is_ok()` from a waker).
* **C.** `parse_tcp_info` on a captured ≥ 232-byte buffer yields the expected `bytes_sent` (offset 200) and `app_limited` (byte 7 bit 0); a zero-filled 200-byte buffer yields `bytes_sent = 0`, and a buffer shorter than 8 bytes yields `app_limited = false`. Catalog round-trip for the two new series.
* **D.** Catalog round-trip for the new metric names; `HopSample::close`/`dropped_bytes` serialisation; `/proc/self` readers return `Some` on Linux CI; `nya_process_open_fds_baseline` ≤ `nya_process_open_fds`.

### Integration (`nya-e2e`)

* Idle 3×3 topology for 60 s: `nya_process_cpu_seconds_total` delta ≤ 1.2 s per process (2 %); `nya_pings_total` rate within ±5 % of `Σ 1 / probe_interval_for`.
* Origin stub that never FINs after client half-close: hop ends ≈ `close_linger` after the client's FIN with `nya.close = reaped`, `nya_hop_reaped_total` = 1, fd formula returns to 0; the last bytes the origin sent *before* the client's FIN arrive at the client.
* Existing throughput and echo suites unchanged (no controller change).

---

## As built (2026-09-16, implemented directly on `main`, A–D in one change)

Deviations from the text above; the text is left as the design record.

| Design said | Built | Why |
| --- | --- | --- |
| `nya_process_cpu_seconds_total` | `nya_process_cpu_ms_total` (u64 ms, `/proc/self/stat` utime+stime × 10; `USER_HZ` is fixed at 100 for procfs) | The sink is integer-only. G-A reads `rate(nya_process_cpu_ms_total[10m]) ≤ 20` (ms/s). |
| `nya_process_rss_bytes` from `/proc/self/statm` | from `/proc/self/status` `VmRSS` | Page-size independent. |
| Baseline "after the listener and exporter binds" | `procself::mark_fd_baseline()` is the first thing `main()` does after logging init, before any bind, dial or `Session` | Listener + exporter fds are a constant; taking the baseline before them keeps one call site on both binaries. The G-B formula gains a constant offset of the listener count (client: SOCKS listeners; server: 1) plus the exporter (1 if enabled) — still "flat over 24 h". |
| Test-only `Arc<AtomicU64>` wake counter through `spawn_path_io` | `PathState::timer_wakes: AtomicU64`, always present, incremented in the timer arm | No signature change; one relaxed increment per wake is free. Not exported. |
| `tokio-util` gains the `sync` feature | no feature change | `tokio_util::sync` is unconditional in 0.7. |
| `Session::loss_timeout_for` | `pub` on `Session` (steer.rs), also used by `maintain` | Single clock for expiry and for the writer's wait. |
| Wait arithmetic "saturating" | additionally floored: every wait ≥ 1 ms (`ping_interval_min = 0` cannot reintroduce a zero sleep); `clamp(floor, gate)` with `floor = min(maintain_interval, gate)` so a `ping_min` below the 5 ms tick cannot invert the clamp (`Duration::clamp` panics on `min > max`) | Found in self-review. |
| `Inner::drop` "cannot run while a hop holds a `Session`" | `Inner::drop` also cancels every remaining `gone` | Belt-and-braces; costs nothing. |
| G-A "≤ 2 % of one core" | Local 3×3-equivalent (8 paths over TLS loopback, `ping_interval_min_ms = 10`): stock **328 %** → fixed **8 %** per process (≈ 84 ms/s at ~1 000 pings/s each way + pongs). | The 2 % figure assumed production ping spacing; 8 paths × 100 Hz over TLS is ~8 %. The metric now exists to read the production figure directly. |

Verification performed: `cargo test --workspace` (nya-core unit tests incl. 8 new for A/B/C/D), `cargo clippy --workspace --all-targets` (no new warnings), `nya-e2e` `short_matrix` (54 scenarios) and `stream_lifecycle` (5); two `bulk_*` SLA gates and `bulk_bottleneck_lossy_sibling` ("never read loop-unfit") are pre-existing flakes — measured 1/10 fail on this change vs 5/10 on HEAD for the latter, 1/5 vs 1/5 for the throughput gates. Real-process side-by-side (release binaries, same config, stock HEAD vs this change): idle CPU 328 % → 8 % per process; 60 hops whose origin swallows the request and never FINs after the client's close: server fds 29 → 89 → **89** (stock, leak) vs 30 → 90 → **30** (fixed; `nya_hop_reaped_total` = 60, `nya_streams_held` back to 0 within `close_linger` + 1 tick); 4 × 30 MiB downloads 0.17–0.27 s (stock) vs 0.14–0.20 s (fixed).

---

## Review log

### Round 1 (architecture + code-truth review)

| # | Severity | Finding | Disposition |
| --- | --- | --- | --- |
| 1 | BLOCKER | §C pin on `carried × 2` never binds in a limited round (`carried ≈ budget`). | Pin re-derived on `bw_now × min_rtt × 2`; later superseded (round 4). |
| 2 | MAJOR | §C pseudo-code did not fit `end_budget_round` ordering. | Rewritten against the real flow; later superseded (round 4). |
| 3 | MAJOR | §B: session already reaps half-closed streams at `close_linger`; stream-table entry is not part of the ghost; "pump sends Close" false; 8 s derivation inverted. | §B redesigned: no copier clock; copier coupled to `remove_held_stream`. KD3–KD6, Alternatives §4/§7/§8. Pre-existing 1 s half-close reset recorded in Open Questions. |
| 4 | MAJOR | §B no-bytes clock treated a write-blocked direction as silent. | Moot (no clock); synthetic EOF applies only to the local→overlay direction. |
| 5 | MINOR | §A formula fell through to `now` when `!is_alive` / session dead. | Explicit catch-all `now + ping_every`; saturating arithmetic. |
| 6 | MINOR | §A Pong-outstanding branch waited `ping_every`; exact expiry is known. | KD2: `clamp(loss_timeout − pending_age, maintain_interval, ping_every)`. |
| 7 | MINOR | §C: dead `bytes_sent` fallback; ratio definition; first-round clean; atomics. | Applied; later superseded (round 4). |
| 8 | MINOR | Gate queries referenced non-existent metrics. | `nya_pings_total` added; `nya_streams_held`, `count(nya_path_rtt_known)` used. |
| 9 | MINOR | G4b used `app_backlog_max` (receiver drain, not origin burstiness); `budget_floor` is `inflight_bias`. | G4b re-worded on `nya.max_gap_us` / `nya.origin_read_wait_us`. |
| 10 | NIT | Metric naming (`nya_path_*`, no `{role}`), infeasible span attr, redundant `close = err`. | Applied. |
| 11 | NIT | `run_hop` does not exist; `COPY_BUF` 8 KiB; `ping_interval_*`/`all_down_timeout` are `SessionConfig`. | Corrected. |

### Round 2 (re-verification + new §B mechanism)

Round-1 findings confirmed resolved. New §B verified: `StreamCounters` shared as claimed; `remove_held_stream` single removal point (only `Inner::drop` bypasses, unreachable during a hop); dropping `StreamState` drops `inbound_tx`; no double-count.

| # | Severity | Finding | Disposition |
| --- | --- | --- | --- |
| 1 | MINOR | `overlay.gone()` borrowed `overlay` immutably while `&mut overlay` is live. | Owned future (now `cancelled_owned()`), taken before the copy. |
| 2 | MAJOR | "Copier never outlives the stream" overclaims: a local peer alive but not reading leaves `ba` blocked with no bound. | Contract reworded; *Residual* paragraph; Risks row; test B(vii). |
| 3 | MINOR | `loss_timeout_for` accessor does not exist. | Added to API table as a new `Session` method mirroring `down_for`. |
| 4 | MINOR | `StreamCounters` "no `Notify`" invariant; wait pattern under-specified. | Comment reworded; later replaced by `CancellationToken` (round 4). |
| 5 | MINOR | §C step gate contradicted KD12. | Applied; later superseded (round 4). |
| 6 | MINOR | §C pin binds iff `loop_rtt > 2 × min_rtt`; not stated. | Stated; later superseded (round 4). |
| 7–9 | NIT | `rx_bytes` includes dropped; unbilled read wait; export mapping; `copy_err → reaped` shift; G4b attr; resets series names; `Inner::drop`. | All applied. |

### Round 3 (re-verification) — APPROVE WITH NITS

Six nits (KD9 wording, resets series names in two more places, pin lift vs arm, `info` before `is_some()`, KD3 wording, `dropped_bytes` type) applied.

### Round 4 (independent architect review) — ANOTHER ROUND → resolved by data

| # | Severity | Finding | Disposition |
| --- | --- | --- | --- |
| 1 | MAJOR | §C's causal chain does not close (pin ≈ BBR cwnd; budget above cwnd is queue, not wire inflight); per-round sample of 4–20 MSS makes the 2 % threshold degenerate; PR-3 is an experiment, not a fix. Split: ship measurement, defer control. | **Checked against production:** queried `nya_path_budget_bytes`, `nya_path_tcp_cwnd_bytes`, `nya_path_tcp_notsent_bytes`, `nya_path_tcp_unacked_bytes`, `Δnya_path_tcp_bytes_retrans_total` per 30 s step on yuusei's four clean paths. Budget < cwnd, `notsent = 0`, `unacked ≪ cwnd` in **0 of 40** lossy steps was the budget above cwnd. Premise falsified. §C is now attribution fields only (KD7); controller preserved as Alternatives §10; aggregate rate control named as the real (product) question (Alt §13, OQ3). |
| 2 | MAJOR | Rollout order contradicted gates: G-A metrics shipped in PR-2; ±5 % ping baseline had no v0.1.7 series. | PR-1 now carries A + self-metrics + `nya_pings_total`; baseline analytic + local repro (KD8). |
| 3 | MAJOR | §B EOF chain mis-described: `WriteHalf` drop does not shut the duplex; overlay→local EOF is downstream of `ab`'s shutdown → pump `send` exit → `peer` drop. | §B *Today*, *Contract* steps 2–3, KD4 rewritten to the real chain; Alt §8; real-pump session tests added for `Close` and `Reset` removal paths; `poll_shutdown` failure row in Risks. |
| 4 | MAJOR | G-B "baseline" undefined and paths hold 2 fds; G-C join ill-posed. | `nya_process_open_fds_baseline` exported; formula uses `2 × paths`; G-C re-stated as per-path 10 s series (no hop join). |
| 5 | MINOR | Use `CancellationToken` (tokio-util already a dependency) instead of flag + `Notify`. | Adopted (KD6, Alt §9); `sync` feature added. |
| 6 | MINOR | `reaped` under-specified (`!ab.read_done`, fused poll, `dropped_bytes > 0` as OQ1 signature). | All specified; tests B(v)/(vi). |
| 7 | MINOR | Hazards: `cancel()` vs `streams` lock; `tcp_info()` lock on the ACK path. | Risks row + test assertion for the lock; `tcp_info()` no longer called on the ACK path (no controller). |
| 8 | NIT | Line-number anchors rot; pings are written not enqueued; test A(i) bound vacuous; wake-counter hookup; §C counter point. | Anchors replaced by branch/function names; `Sent` for `Frame::Ping`; bound `loss_timeout / ping_every + 2`; counter through `spawn_path_io`; §C counter moot. |

### Round 5 (independent reviewer, re-verification) — APPROVE WITH NITS

All eight round-4 findings confirmed resolved; consistency sweep clean. Nits applied: `tcpi_delivery_rate_app_limited` is byte 7 (not 8); the analytic ping rate is an upper bound (peer pings reset the idle gate), repro on the same binary is the primary G-A comparison; `parse_tcp_info` short-buffer test wording; `open_fds_baseline` capture point pinned in `main()`; pump-exit assertion via a test-only counter (`spawn_pump` discards its `JoinHandle`).

**Loop closed.** Three rounds converged on A and B; the fourth, independent, review challenged C on causal grounds and the challenge was settled by production data rather than by argument. Further rounds would re-review text, not mechanisms. A and B+C+D are ready for implementation as PR-1 and PR-2.
