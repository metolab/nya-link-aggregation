# 架构

nya 是一条 overlay 会话：客户端把多条 TCP+TLS 路径接到同一个 session，上面再多路复用若干条应用流。每条应用流粘在一条路径上；路径变差时把流迁走，路径恢复后再迁回来。数据不在多条链路上做条带。

## Crate

```text
nya-client ──┐
             ├── nya-core ── nya-proto
nya-server ──┘
     ▲
nya-e2e（测试，同时拉 client + server）
```

- **nya-proto**：`u32be length || u8 type || body`，最大 payload 16 KiB。ALPN `nya/1`。TLS exporter 标签 `nya-link-aggregation`。
- **nya-core**：会话、路径 IO、健康时钟、调度、握手、SPKI pin。`Tuning` 不进 TOML。
- **nya-client**：按 `[[links]]` 各开 `connections` 条 TCP+TLS；第一条 `CreateSession`，其余 `JoinSession`。入站是 SOCKS5 CONNECT 或固定目标 forward。
- **nya-server**：TLS 接受 → 握手 → `SessionTable`。`CreateSession` 建会话并 spawn outbound；`JoinSession` 把路径挂到已有会话。
- **nya-e2e**：每条路径前插用户态损伤代理；catalog 是短 SLA，`--mixed` 是分 RTT 带的 soak。

## 数据路径

```text
应用 inbound
    │  SOCKS5 / TCP forward
    ▼
Session::open_stream          选最快 class 里一条路径，sticky
    │
    ▼
STREAM_OPEN / STREAM_DATA / ACK / CLOSE / RESET
    │
    ▼
PathState 双写队列            ≤ interactive_max 走 urgent，其余 bulk
    │
    ▼
TLS framed IO                 对端 session::handle_frame
    │
    ▼
服务端 IncomingStream         TcpStream::connect(target) 后双向 copy
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

每条路径维护：

- **fast RTT**：近期 EWMA，用于打分和瞬时判断
- **stable RTT**：更慢抬升，给 loss / down 时钟用，避免尖刺拆 TCP
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

路径还有 alive / degraded / down。全部 down 超过 `all_down_timeout` 则拆会话。N≥3 且恰好 N−1 条路径超过 `degrade_for`（quiet）、其中至少一条已到 `down_for` 时进入 correlated：把已到 `down_for` 的已知 RTT 路径标 degraded、暂缓 `path_failed`（预算仍是 `all_down_timeout`），避免对端短卡时因 `last_rx` 不同步而逐条拆 TCP。仅 3 条过 degrade、谁都没到 down 不进入。全员静默仍按 `down_for` 拆。客户端链路监督协程按指数退避重连（200ms–2s）。同链路 TCP 相对姐妹 class 已是 backup、且自身 fast 也是 backup、且 class 已冻结满 `stable_up_hold`、再持续这两者 `stable_up_hold` 时，客户端主动拆掉重拨（串行 2s）。class 仍 backup 但 fast 已回到 cliff 以下时不拆，交给 class 7/8 走回。

## 调度

新流：

1. 活着的路径里去掉 backup（class RTT > 最快 × 2 + 20ms）
2. 限制在最快 class（`should_failback(候选, 最好)` 为假的那些）
3. 打分 `class_rtt × load × 1024 + fast_rtt × load`，`load = 1 + inflight/bias + sticky`

交互流用更重的 inflight 权重，避免和 bulk 抢同一条连接。

粘滞流的维护（`session::steer`，5ms tick）：

- **speculative migrate**：当前路径变差，先迁到 backup（优先同链路的另一条 TCP，隔离 HOL）
- **failback**：更好路径稳定约 2 RTT 且过了 cooldown 再迁回
- **same-link rebalance**：同一 `link.name` 下两条连接负载差过大时把 bulk 挪到姐妹连接；不会把 bulk 推到更高 class

HOL 隔离靠「每链路多连接 + 姐妹优先 backup」，不是按 ISP 钉死。

## 流控制

- 初始窗口 `128 KiB`，对端用 `STREAM_ACK.window` 通告
- `STREAM_DATA` 带 offset，接收端 `BTreeMap` 重排
- 未确认数据记在发送路径的 inflight 上；ACK 时减去，并对小帧采样 RTT（bulk ACK 不当时延）
- 服务端出站拨号失败会 `IncomingStream::reset(DialFailed)`，对端收到 `STREAM_RESET`

## 可观测性

`Counters` 挂在每个 `Session` 上，进程边缘（入站 / 出站 / 握手 / 重连）走 `ProcessCounters`（始终在 `Inner` 上）。默认每 10s 一条 `nya_core::obs` snapshot；`[obs].metrics_listen` 默认关。info 计分卡带 `mig`/`hol`/`hedge`/`rtx`/`fb_slink`/`picks_unk`/`recycle`/`corr`；决策点（pick / migrate / failback / HOL）仍是结构化 `debug!`。class raise/drop、correlated silence、outlier recycle、unknown-session recreate 走 **info**。热路径（STREAM_DATA / ACK / Ping）不打日志。可选 OTLP 在独立 crate `nya-obs`（只从二进制 `main` 安装）；名字来自 `visit_metrics` 一份 catalog。

线路状态按 `link_key` 汇总（`a#0`/`a#1` → `a`）：up/deg 连接数、RTT 范围、sticky、inflight、队列、rx 新鲜/最旧。`paths=` 可带 ` bak`。迁移原因拆成 speculative / path_down / ensure_sticky / send-blocked；另有 retransmit/hedge、probe_miss、未知 RTT pick。snapshot 带压缩 `streams=`（不进 Prometheus 标签）。

业务计分卡：流完成比、send-unacked ∪ recv-hole stall、每路径一次 `failover_ms`（`last_rx_ago`）、跨 link `failbacks`、overlay goodput（`path.rs::send_frame` / decode）。e2e SLA 仍用应用 ping；overlay p99 只进 notes。见 [OBSERVABILITY.md](OBSERVABILITY.md)。

## 配置分层

运维 TOML（`SessionOpts`）只有四个键：探测预算、路径上限、全 down 放弃。`#[serde(deny_unknown_fields)]`。顶层可选 `[obs]`（snapshot 间隔、metrics 监听、instance_name、嵌套 `[obs.otel]`），stderr 日志级别只走 `RUST_LOG`。OTLP 认证用 `[obs.otel.headers]`，见 [OBSERVABILITY.md](OBSERVABILITY.md)「远程 OTLP」。

算法常数在 `Tuning::STANDARD`：loss/down 倍数、failback 阈值、队列深度、重连退避、交互帧上限。测试里可以 clone 再改；生产路径只有这一张表。

## 测试分层

| 层 | 位置 | 覆盖 |
| --- | --- | --- |
| 单元 | `nya-proto` / `nya-core` 模块内 | 帧编解码、Tuning、握手 duplex、单测调度 |
| 会话 | `nya-core::session` tests | 单路径 echo、多路径 failover |
| 短 matrix | `cargo test -p nya-e2e` | 时延、异构、blackhole、failback、多连接 HOL… |
| 长 blackhole | `nya-e2e --long` | 30s / 60s / 5m |
| 混合 soak | `nya-e2e --mixed` | near 11–16ms / mid 60–100 / high 120–150 / far 160–200 |

e2e 损伤代理在 TLS 外侧做 stall，不丢 TLS 字节。CI 跑 fmt、clippy、`--exclude nya-e2e` 的单元测试，以及 `nya-e2e` 的 lib/bin 测试；完整 matrix 留给本地或夜间任务。

发版流程（tag `v*` → 两个平台二进制 → GitHub Release）见 [RELEASE.md](RELEASE.md)。
