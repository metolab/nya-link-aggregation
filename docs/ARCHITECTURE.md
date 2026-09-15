# 架构

nya 是一条 overlay 会话：客户端把多条 TCP+TLS 路径接到同一个 session，上面再多路复用若干条应用流。路径是池，不是流的家。每个 offset **只先发一次**（当前最好的活 TCP）；未 ACK 则**换路再发**，禁止原地再打同一条 5-tuple。交互片（≤ `interactive_max`）按发出那条路的 `loss_timeout`（2×RTT）换路；bulk 片只在**那条路静默**（`last_rx_ago ≥ retry_after_bulk`，2×ACK RTT 夹在 loss..down 之间）且片龄也过了这个阈值、或接收端明确丢过（`dropped`）、或到了 `down_timeout × 2^(tries−1)` 的 belt 时才再发——一条健康 TCP 上排队的 bulk 不是丢包。接收端按 offset 重组，先到的一份交付，`STREAM_ACK` 带 SACK 区间让发送端释放乱序已到的片。不把同一包同时打到多条路上。

## Crate

```text
nya-client ──┐
             ├── nya-core ── nya-proto
nya-server ──┘
     ▲
nya-e2e（测试，同时拉 client + server）
```

- **nya-proto**：`u32be length || u8 type || body`，最大 payload 16 KiB。`PROTOCOL_VERSION = 2`，ALPN `nya/2`。TLS exporter 标签 `nya-link-aggregation`。
- **nya-core**：会话、路径 IO、健康时钟、调度、握手、SPKI pin。`Tuning` 不进 TOML。
- **nya-client**：按 `[[links]]` 各开 `connections` 条 TCP+TLS；第一条 `CreateSession`，其余 `JoinSession`。入站是 SOCKS5 CONNECT 或固定目标 forward。
- **nya-server**：TLS 接受 → 握手 → `SessionTable`。`CreateSession` 建会话并 spawn outbound；`JoinSession` 把路径挂到已有会话。
- **nya-e2e**：每条路径前插用户态损伤代理（`packet_wan`：RTT / jitter / loss，可选 `rate_bps` + `queue_bytes` 瓶颈，Reno 拥塞控制、RACK/TLP）；catalog 是短 SLA，`--mixed` 是分 RTT 带的 soak。

## 数据路径

```text
应用 inbound
    │  SOCKS5 / TCP forward
    ▼
Session::open_stream          选最快 class 里一条路径发 Open（一次）
    │
    ▼
STREAM_OPEN / STREAM_DATA / ACK / CLOSE / RESET
    │                         每 offset 重新 pick；超时换路重传
    │
    ▼
PathState 双写队列            ≤ interactive_max 走 urgent，其余 bulk
    │
    ▼
TLS framed IO                 对端 session::handle_frame
    │
    ▼
服务端 IncomingStream         出站：字面 IP 直连；主机名拆 A/AAAA，谁先到谁先连（CAD 20ms）。上一轮真正连上的 origin 地址立刻再试，不等待这一次 lookup。lookup 按 FQDN（尾点），不走 resolv search。
```

对端接受 `STREAM_OPEN` 后分配本地 `TunnelStream`（tokio duplex + 窗口 / 乱序缓冲）。应用读写 duplex；pump 把字节变成带 offset 的 `STREAM_DATA`，对端按 offset 重排。

## 握手与认证

每条路径都是独立 TLS。客户端用 SPKI SHA-256 pin 校验服务端证书，不走系统 CA。

握手绑定 TLS exporter：

1. **CreateSession**：`HMAC-SHA256(psk, "nya-create-v1" || exporter || nonce || user_id)`。服务端核验后发 16 字节 `session_id`。
2. **JoinSession**：`HKDF-SHA256(psk, salt=session_id, info="nya-session-v1")` 得到 session key，再 `HMAC(session_key, "nya-join-v1" || exporter || path_name)`。

PSK 证明「谁能加入这条会话」；pin 证明「TLS 对端是这张证书」。二者缺一不可。

路径名在客户端是 `{link.name}#{i}`，例如 `a#0`、`a#1`。`CreateSession` 带同样的名字；旧客户端省略该字段时服务端才回落到 `init`。

## 路径与健康

每条路径的 TCP socket 在 Linux 上设 `TCP_NOTSENT_LOWAT`（内核只留一小段未发字节，其余排在 overlay 自己的队列里，HOL / 换路能看见）并尽力 `TCP_CONGESTION=bbr`（失败只记一次日志）；`dup()` 出来的 fd 存在 `PathState.tcp_fd`，snapshot 读 `TCP_INFO`（cwnd / unacked / notsent / rtt / retrans）当路径 gauge。

每条路径维护：

- **fast RTT**：近期 EWMA，用于打分和瞬时判断
- **stable RTT**：更慢抬升，给 loss / down 时钟用，避免尖刺拆 TCP
- **min RTT**：10 s 窗口最小值（`WindowedMin`），预算控制器用，不被排队时延抬高
- **ACK RTT / 送达率**：DATA 片带 `delivered` 计数和时间戳，ACK 回来时按 BBR 方式算送达率样本（`bw_filter`）；`ack_rtt` 是 bulk 片的实际 ACK 往返
- **发送预算**（`budget_bytes`）：这条路上允许的 unacked DATA 上限。「长得出吞吐才长」：每个 min-RTT 轮结束比较本轮送达率与上次确认的满带宽，连续两轮 ≥ +15 % 才算一步（×1.5）；3 轮没步就算满管，之后每 4 轮探一次 ×1.25，探到带宽没涨 ≥ 7.5 % 就回去；送达率下沉 ≥ 15 % 连续 3 轮、或 8 轮都没被预算限住，则收缩到 `min(当前, 2 × 本轮带宽 × ACK RTT)`。上限 `3 × bw_max × min_rtt`（`bw_max` 喂的是相邻两轮的较小值，10 s 窗口），下限 `inflight_bias`（64 KiB），硬上限 `chan × 16 KiB`。预算只挡 bulk；bulk 无处可发时等 `budget_wait`，有别的路有余量则溢出到那条（fan-out）。
- **class RTT**：调度用的 class 成员资格；相对 fast 过时偏高时让位。raise 仍是 hold 后一次 7/8；raise store 与 init freeze 都置 unwind permit。完成 init 的生产路径 permit 为真，直到某次 drop store 的 new_us ≤ fast 才清；happy-path freeze（class==fast）不会 catch-up 清 permit，故 `permit && fast < class` 在会话剩余时间绕过 0.25/8 ms 门。fast < class 时每 hold 一次 7/8。EWMA 从尖刺回落到 (class, 2×class] 死区时 permit 保持。仅 poke class 的测试、以及已经 catch-up 清 permit 的路径仍走 0.25/8 ms 门。timeout-stable 仍不是这套时钟。DEGRADED 仍探活（在途 Ping 最多一条）。尖刺时不跟着每 ping 跳 class

超时由 `Tuning` 从 stable RTT 推出来，再夹紧：

| 时钟 | 大致公式 | 作用 |
| --- | --- | --- |
| probe | `clamp(min(fast, stable), ping_min, ping_max)`；未知 RTT 用 `ping_min` | Ping 间隔。`degrade_timeout` 里的 probe 项仍用 stable，不用 `probe_interval_for` |
| loss | `clamp(2×rtt, 20ms, 2s)` | 一次探测 / 发送算丢 |
| degrade | `max(loss, probe+rtt, ping_max)`；未知再抬到 `unknown_degrade_min` (300ms) | 静默后标 degraded。`ping_max` 是「必须已发出 Ping」；Pong 等待靠 in-flight / `probe_miss` |
| down | `max(5×rtt, 320ms) + probe`，上限 5s | 静默后标 down。probe 项用 `assumed_rtt`，不要改成 `min(fast, stable)` |
| failback 同类 | `max(8ms, 0.45×更好路径 RTT)` | 同 class 内要差这么多才迁回 |
| failback 跨 class | 当前 ≥ 更好 × 1.5 + 8ms | 明显更好的 class 才 Upgrade |

路径还有 alive / degraded / down。全部 down 超过 `all_down_timeout` 则拆会话。N≥3 且尚有存活路径时，quiet 为恰好 N−1 **或** quiet≥3 且跨越 ≥2 条 named link、其中至少一条已到 `down_for`，进入 correlated：把已到 `down_for` 的已知 RTT 路径标 degraded、暂缓 `path_failed`（预算仍是 `all_down_timeout`），避免对端短卡或跨 ISP 拥塞被当成独立 5-tuple 死亡而重连风暴。仅 3 条过 degrade、谁都没到 down 不进入。单 named link 静默（H8）仍按 `down_for` 拆。全员静默仍按 `down_for` 拆。客户端链路监督协程按指数退避重连（200ms–2s）。同链路 TCP 相对姐妹 class 已是 backup、且自身 fast 也是 backup、且 class 已冻结满 `stable_up_hold`、再持续这两者 `stable_up_hold` 时，客户端主动拆掉重拨（串行 2s）。class 仍 backup 但 fast 已回到 cliff 以下时不拆，交给 class 7/8 走回。

## 调度

新流：

1. 活着的路径里去掉 backup（class RTT > 最快 × 2 + 20ms）
2. 限制在最快 class（`should_failback(候选, 最好)` 为假的那些）
3. 打分 `class_rtt × load × 1024 + fast_rtt × load`，`load = 1 + inflight/bias + last-send`；同分取最小 `path_id`
4. 交互 DATA 在 last-send 仍是 class、schedulable、loss-fresh 时复用 Open 的 5-tuple（避免 Open 落在 ping-only 瘦 TCP 上撞 200 ms min-RTO）

交互流用更重的 inflight 权重，避免和 bulk 抢同一条连接。

offset 进度（`session::{streams,steer}`，5ms tick）：

- **换路重传**：unacked / StreamOpen / StreamClose 超过 `loss_timeout(min 活 dest 的 fast RTT)` 则避开已试过的 `path_id`、优先不同 `link_key` 再发一次。不是并发双发。不是 2× 那条病 5-tuple。
- **选路跳过静默但 UP 的 TCP**：`last_rx_ago >= loss_timeout(min(fast, class))` 时不当最好路径（不必等 `mark_degraded`）。
- **路径 down**：那条 TCP 上的 unacked / Open / Close **立刻**换到仍活的路上；路径拆/重拨是池卫生，不挡 TTFB。
- **HOL**：same-link bulk vs interactive；last-send 只是诊断和 HOL 放置，不是发送契约。`maybe_failback` 已从 maintain 去掉。Interactive 成员是 `fastest_class_set` 的 live-clock 子集（`should_failback || class_should_drop`），避免 far-band ping 钉在 258 ms extra 上；bulk / Any / HOL 仍用完整 class 集。

HOL 隔离靠「每链路多连接 + bulk 避开交互连接」，不是把流钉死在一条 TCP 上。交互帧（`<= interactive_max` 1500 字节）和控制帧走 urgent；bulk 队列满 **不** `set_congested`。不要靠加大 `chan` 修 TTFB。未知 RTT 的替换 5-tuple 在已有已知、schedulable 姐妹时进不了 fastest class。

## 流控制

- 初始窗口 `128 KiB` 是 **floor**。接收端按 deliver rate × **到达路径** RTT（`last_recv_path`，其次 sticky，再次池 min；下载流的 sticky 只是请求那条路）把 `recv_cap` 调到 `clamp(2 BDP, 128 KiB, 128 KiB × chan)`；`STREAM_ACK.window = recv_cap - buffered_in - recv_buffered`。发送端被我们的窗口边沿卡住（DATA 反复停在上次通告的边沿，`edge_ring`）而应用跟得上时，把 cap 翻倍探 `4 × min_rtt`，送达率没掉 ≥ 10 % 就留下（`recv_cap_probes / kept / reverted`）。不是 TOML 开关，也不改 `Tuning::STANDARD.initial_window`。
- `STREAM_ACK` 是每路径 overwrite register + `Notify`，**不**占 urgent `chan`。writer 每轮最多取 K=8 条直接 `write_one`。urgent 满不能丢掉 ACK，也不能因此 `set_congested`。ACK 可带最多 `MAX_SACK_RANGES` 个乱序已到区间（旧对端解码到累计 ACK 即止）；应用读走字节也会触发 drain + ACK，窗口不会停在 0。
- `STREAM_DATA` 带 offset，接收端 `BTreeMap` 重排。`recv_fin` 之后或 `close_off` 之外再到的重复 DATA 仍回 ACK（`data_dup_rx_bytes` / `ack_after_fin`），发送端才能收尾。
- 未确认数据记在发送路径的 inflight 上；ACK / SACK 时减去，并对小帧采样 RTT（bulk ACK 不当时延）。每条路的 unacked DATA 还受 `budget_bytes`（见「路径与健康」）约束：sticky 满了先溢到同 class 有余量的路，都没有就等 `budget_wait`（`send_budget_blocks`）；被对端窗口挡住而路上有余量计 `send_window_limited_with_room`。Pong/ACK 样本 cap 是 **这条 path** 的 `loss_timeout`，不是池里 `min_alive_fast`（否则 7 ms 同伴会把 60 ms 备份的真实 RTT 丢掉，交互 affinity 钉在慢路上）。
- write-stall（`write_deadline` 20 ms floor）只让 **新** Interactive/Open pick-skip；writer 继续排 bulk。`hold_stream_data` 只对 `!rtt_known()`。已知+stalled 的 DATA 走 bulk；retry 不喷到 write-stalled dest。stall 不拆路。
- 一条 bulk 流尽量钉在一条 5-tuple（`bulk_affinity`，含正在 flush 的 write-stalled dest）。交互 affinity 仍跳过 write-stalled。
- HOL：stall ≥ `close_linger` 的 leftover 不再算 interactive。`hol_place_bulk_fallback` **不**走 `fastest_class_set`（那会在 nsix 空闲时把 stalled soy 藏起来）。
- 服务端出站拨号失败会 `IncomingStream::reset(DialFailed)`，对端收到 `STREAM_RESET`
- 活会话上流表：`counted_close` / 半关闭 linger / hygiene `STREAM_RESET` 会从 `Inner.streams` 摘掉。第一 closer 的 Close 重试停在对端 `recv_fin`、HashMap-gone、或 `close_linger`（`retry_close_from` 同一套），**不用** multiplexed `path.last_rx` 当 Close ACK（Pong ≠ Close 送达）。第二 closer 仍在 `observe_stream_end` 立刻 `forget_close`。Close/Reset 换路**不**走 `pick_retry_path` 的 cycle rung（DATA 仍走）；`push_tried` 只在 Close/Reset **发送成功**时记。`expire_recv_closes` 不在空洞上强制 FIN（`maintain` 上的 belt 只 `try_finish_recv_close`）。**Client** progress-fine linger 且 `!recv_fin` 发 wire `STREAM_RESET`，closer pump 仍是 `Inbound::Close`（Residual D）。**Server** origin-EOF 同类 linger 保持静默（Yuusei hop-RST 修复）。`recv_fin` 已到则两侧都静默摘表。无进度 linger 仍 Reset 换路。`maybe_failback` 不在 `maintain` 里；交互靠每 offset `pick_pref`。neither-FIN hangover 靠 session bounce，不 idle-GC。linger 仍不是产品 `stream_resets_timeout`。

## 可观测性

`Counters` 挂在每个 `Session` 上，进程边缘（入站 / 出站 / 握手 / 重连）走 `ProcessCounters`（始终在 `Inner` 上）。默认每 10s 一条 `nya_core::obs` snapshot；`[obs].metrics_listen` 默认关。info 计分卡带 `mig`/`hol`/`hedge`/`rtx`/`fb_slink`/`picks_unk`/`recycle`/`corr`/`pick_rtt`（最后一次真正发出 StreamOpen 的 dest 的 fast RTT，未知为 0；不是调度输入）；进程边缘 hop p99 与 interval-max `tail=` 也在这条 snapshot 上，**不是**调度输入。决策点（pick / migrate / failback / HOL）仍是结构化 `debug!`。class raise/drop、correlated silence、outlier recycle、unknown-session recreate 走 **info**。热路径（STREAM_DATA / ACK / Ping）不打日志。可选 OTLP 在独立 crate `nya-obs`（只从二进制 `main` 安装）；名字来自 `visit_metrics` 一份 catalog。

线路状态按 `link_key` 汇总（`a#0`/`a#1` → `a`）：up/deg 连接数、RTT 范围、sticky、inflight、队列、rx 新鲜/最旧。`paths=` 可带 ` bak`。迁移原因拆成 speculative / path_down / ensure_sticky / send-blocked；另有 retransmit/hedge、probe_miss、未知 RTT pick。snapshot 带压缩 `streams=`（不进 Prometheus 标签）。

业务计分卡：流完成比、send-unacked ∪ recv-hole stall（进入钟是 `loss_timeout`）、每路径一次 `failover_ms`（`last_rx_ago`）、overlay goodput。换路重传计入 `data_retransmit` / `data_hedge`（跨 `link_key` 为 hedge）；Close 换路计 `close_retry`。限制器归因：`send_budget_blocks` / `send_window_limited_with_room` / `window_blocks`、`data_sacked`、`data_dup_rx_bytes`、`ack_loop_ms`、`recv_cap_max_bytes`；每路径 `budget` / `bw` / `ack_rtt` / `delivered` 与 `TCP_INFO` gauge 进 snapshot `paths=` 和 Prometheus。每个 `nya.hop` span 在 copy 结束时带这条流的 `nya.limiter`（`origin` / `app` / `overlay` / `window` / `budget` / `path` / `none`，由 `copy_bidirectional_timed` 记的每方向读/写等待时间决定，见 OBSERVABILITY）、四个等待时间、`window_blocks` / `budget_blocks` / `recv_cap_max` / `hedges` / `dup_rx_bytes` / `paths_used`、接收侧证据（`recv_hole_max` / `app_backlog_max` / `zero_win_hole` / `zero_win_app` / `hole_us`）以及 server 端 origin socket 的 `TCP_INFO`。流计数器住在 `StreamCounters`（`Arc`，`TunnelStream` 与 `StreamState` 共持），所以流被 reap 后 hop 仍能读到终值。半关闭 linger 计 `stream_reaps_linger`（含 client Residual D Reset），**不是**产品 `stream_resets_timeout`。Soak 看 `(closed - linger) / opened`。e2e 产品门是 **新流 first-byte**、Close-swallowed、以及 `prod_like_bulk_copy`（1 MiB ≪ 10 s overlay cap）。Hytron 下载产品门是 **bounce 之后** hop ≫ 150 KB/s 且 origin ≈ client，不是 ping 1500 ms。见 [OBSERVABILITY.md](OBSERVABILITY.md)。Close/Reset 送达语义见 [design-close-reset-delivery-regression.md](design-close-reset-delivery-regression.md)；bulk goodput 机制见 [design-hytron-bulk-goodput.md](design-hytron-bulk-goodput.md)；每路径发送预算、ACK-clock、bulk 静默 hedge、SACK 见 [design-path-budget-ack-clock.md](design-path-budget-ack-clock.md)。

## 配置分层

运维 TOML（`SessionOpts`）只有四个键：探测预算、路径上限、全 down 放弃。`#[serde(deny_unknown_fields)]`。顶层可选 `[obs]`（snapshot 间隔、metrics 监听、instance_name、嵌套 `[obs.otel]`），stderr 日志级别只走 `RUST_LOG`。OTLP 认证用 `[obs.otel.headers]`，见 [OBSERVABILITY.md](OBSERVABILITY.md)「远程 OTLP」。

算法常数在 `Tuning::STANDARD`：loss/down 倍数、failback 阈值、队列深度、重连退避、交互帧上限。测试里可以 clone 再改；生产路径只有这一张表。

## 测试分层

| 层 | 位置 | 覆盖 |
| --- | --- | --- |
| 单元 | `nya-proto` / `nya-core` 模块内 | 帧编解码、Tuning、握手 duplex、单测调度 |
| 会话 | `nya-core::session` tests | 单路径 echo、多路径 failover |
| 短 matrix | `cargo test -p nya-e2e` | 时延、异构、blackhole、failback、多连接 HOL、prod-like 新流 first-byte、`bulk_*` 瓶颈（50 Mbps / 10 ms / 128 KiB 队列：单流、双流共享、三路 fan-out、bulk 下 ping、健康路零 hedge）… |
| 长 blackhole | `nya-e2e --long` | 30s / 60s / 5m |
| 混合 soak | `nya-e2e --mixed` | near 11–16ms / mid 60–100 / high 120–150 / far 160–200 |

e2e 损伤代理在 TLS 外侧做 stall，不丢 TLS 字节。`bulk_*` 瓶颈场景对 CPU 时序敏感，标 `exclusive`，catalog 里串行跑。CI 跑 fmt、clippy、`--exclude nya-e2e` 的单元测试，以及 `nya-e2e` 的 lib/bin 测试；完整 matrix 留给本地或夜间任务。核多的机器上 `cargo test` 的 16 job 会互相抢 timer，用 `nya-e2e --jobs 4` 跑 release 二进制。

发版流程（tag `v*` → 两个平台二进制 → GitHub Release）见 [RELEASE.md](RELEASE.md)。
