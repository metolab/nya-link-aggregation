# nya-link-aggregation

[![CI](https://github.com/metolab/nya-link-aggregation/actions/workflows/ci.yml/badge.svg)](https://github.com/metolab/nya-link-aggregation/actions/workflows/ci.yml)
[![Build](https://github.com/metolab/nya-link-aggregation/actions/workflows/build.yml/badge.svg)](https://github.com/metolab/nya-link-aggregation/actions/workflows/build.yml)

仓库：<https://github.com/metolab/nya-link-aggregation>

多路径 TCP+TLS overlay：把若干独立链路聚合成一条会话。流粘在最快 RTT class 上，路径故障时切换，恢复后再回切。不把字节打散到多条链路上做条带（不是 MPTCP 那种 stripe）。

适合「同一台出口有多条质量接近的线路、应用层要一条稳定 TCP」的场景。客户端提供 SOCKS5 和 TCP 端口转发。

## 特性

- 每条链路独立 TCP+TLS（可开多个连接，隔离单连接 HOL）
- 流级粘滞（sticky-per-stream），新流落在最快 RTT class
- RTT 自适应的丢包 / 路径 down / failback 时钟
- 交互流量走紧急写队列，bulk 不拖高 ACK 采样
- 服务端 TLS SPKI pin + PSK 握手证明
- 用户态 WAN 损伤 harness 与 SLA matrix（无需 root / tc）

## 架构

```text
inbound  →  session::streams  →  scheduler::pick_path  →  PathState
                │                                           │
                └──── session::steer (migrate / failback)   │
                                                            ▼
                                             urgent / bulk writer
                                                            ▼
                                                  TLS framed IO
```

crate 划分：

| crate | 职责 |
| --- | --- |
| `nya-proto` | 长度前缀帧、握手 / 流控制报文 |
| `nya-core` | 会话、路径健康、调度、TLS pin |
| `nya-client` | 拨号、SOCKS5 / TCP forward |
| `nya-server` | 监听、握手、出站拨号 |
| `nya-e2e` | 损伤代理、场景目录、混合 soak |

细节见 [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)。

## 安装

预编译二进制在 [Releases](https://github.com/metolab/nya-link-aggregation/releases)：Linux x86_64 与 macOS Apple Silicon 的 `nya-client` / `nya-server`。main 上每次推送也会把同样的包传到 [Actions artifacts](https://github.com/metolab/nya-link-aggregation/actions/workflows/build.yml)。打 tag 发版见 [docs/RELEASE.md](docs/RELEASE.md)。

从源码构建需要 Rust stable（edition 2021）：

```bash
git clone https://github.com/metolab/nya-link-aggregation.git
cd nya-link-aggregation
cargo build --release -p nya-server -p nya-client
```

产物在 `target/release/nya-server` 和 `target/release/nya-client`。

## 快速开始

1. 生成自签证书并打印 SPKI pin：

```bash
./target/release/nya-server gen-cert --out certs
```

输出类似：

```text
wrote certs/server.crt and server.key
pinned_spki_sha256 = "…"
```

2. 复制示例配置，改 PSK、pin、监听地址和链路：

```bash
cp examples/server.toml server.toml
cp examples/client.toml client.toml
```

PSK 两端必须一致。客户端 `pinned_spki_sha256` 填上一步打印的值。`[[links]]` 填服务端在各条链路上的地址。

3. 启动：

```bash
./target/release/nya-server --config server.toml
./target/release/nya-client --config client.toml
```

默认客户端在 `127.0.0.1:1080` 提供 SOCKS5。日志级别只用 `RUST_LOG`：未设置时落到 `nya_client=info,nya_core=info,nya_obs=info`（服务端同理；`RUST_LOG` 未点名 `nya_obs` 时仍注入 `nya_obs=info`）。`RUST_LOG=nya_core=debug` 会打出调度决策（pick / migrate / failback / HOL）的结构化字段。每 10 秒一条 `target=nya_core::obs` 的质量 snapshot（计分卡 + paths/links/streams；全量 catalog 在 debug / `/metrics`）。可选 `[obs].metrics_listen = "127.0.0.1:9100"` 提供 Prometheus text（必须是数值 loopback 地址，不要 `localhost` / `0.0.0.0`）。

## 配置

示例见 [`examples/client.toml`](examples/client.toml) 与 [`examples/server.toml`](examples/server.toml)。

`[session]` 只暴露运维表面：探测间隔、路径上限、全 down 放弃计时。未知键是解析错误，没有别名。

| 键 | 含义 | 默认 |
| --- | --- | --- |
| `ping_interval_min_ms` / `ping_interval_max_ms` | 探针对间隔夹紧到路径 RTT | 10 / 50 |
| `all_down_timeout_ms` | 全部路径 down 后拆会话 | 8000 |
| `max_paths` | 单会话路径上限 | 32 |

可选 `[obs]`（缺省即安静的 10s snapshot、不监听 HTTP）：

| 键 | 含义 | 默认 |
| --- | --- | --- |
| `snapshot_interval_ms` | 定期 snapshot 间隔；`0` 关闭 | 10000 |
| `metrics_listen` | Prometheus `/metrics`；空 = 不听 | 空 |
| `instance_name` | 实例名；打开 `[obs.otel]` 时必填 | 空 |

## 远程 OTLP

默认关，不配 `[obs.otel].enabled = true` 就不会连 collector。打开时 **必须** 有非空 `instance_name`（或环境变量 `NYA_INSTANCE_NAME`），否则进程拒绝启动。完整键表、信号开关、PII、span 清单见 [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md)「远程 OTLP」。示例 collector：[`examples/otel-collector.yaml`](examples/otel-collector.yaml)。

`[obs.otel]`（未知键是解析错误）：

| 键 | 含义 | 默认 |
| --- | --- | --- |
| `enabled` | 总开关；`false` 时三信号全关 | `false` |
| `endpoint` | HTTP **基址**，如 `http://127.0.0.1:4318`。**nya-obs** 拼接 `/v1/traces` `/v1/metrics` `/v1/logs` | 空（每路 enabled 信号须有父级、该信号 `endpoint`、或 env） |
| `protocol` | `http/protobuf` 或 `grpc` | `http/protobuf` |
| `gzip` | 压缩 | `true` |
| `timeout_ms` | 每次导出超时；traces/logs shutdown flush | 5000 |
| `export_interval_ms` | **仅 metrics** 推送周期 | 10000 |
| `sample_ratio` | traces 采样，须在 `[0.0, 1.0]` | `1.0` |
| `environment` | Resource `deployment.environment`；空则省略 | 空 |
| `redact_targets` | **仅 OTLP logs**：`host` / `target` 等改成 `*` | `false` |
| `[obs.otel.headers]` | HTTP 头 / gRPC metadata。用来做 **Basic / Bearer** | 空 |

嵌套 `[obs.otel.traces]` / `[obs.otel.metrics]` / `[obs.otel.logs]`：

| 键 | 适用 | 含义 | 默认 |
| --- | --- | --- | --- |
| `enabled` | 三路 | `false` 关这一路；缺省跟随父 `enabled` | 跟随父 |
| `endpoint` | 三路 | 覆盖父 endpoint | 父 / env |
| `level` | **仅 logs** | OTLP 最低级别 `error\|warn\|info\|debug\|trace`。stderr 仍是 `RUST_LOG` | `info` |
| `queue_size` | logs、traces | 内存队列 | 8192 |
| `batch_size` | logs、traces | 单次导出条数，须 `<= queue_size` | 512 |
| `delay_ms` | logs、traces | 定时 flush，最小 10 | 5000 |

`level` 写在 metrics/traces 下、或 `queue_size` 写在 metrics 下，启动失败。gRPC 需要编译 `--features otel-grpc`，否则 `protocol = "grpc"` 启动失败。发行包默认带 OTel SDK，运行时仍默认不导出；`--no-default-features` 编出不含 SDK 的二进制。

HTTP Basic Auth（没有单独的 username/password 键）：

```toml
[obs]
instance_name = "edge-sh-03"

[obs.otel]
enabled = true
endpoint = "http://collector:4318"
protocol = "http/protobuf"

[obs.otel.headers]
Authorization = "Basic dXNlcjpwYXNz"
```

`dXNlcjpwYXNz` 是 `user:pass` 的 Base64（`printf 'user:pass' | base64`）。Bearer：`Authorization = "Bearer <token>"`。头值不会打进业务日志。

环境变量（**非空 TOML 赢**；`OTEL_SDK_DISABLED=true` 或 `1` **永远**关整栈）：

| 变量 | 作用 |
| --- | --- |
| `OTEL_SDK_DISABLED` | `true`/`1` 紧急关闭 |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | TOML `endpoint` 为空时使用 |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | TOML `protocol` 为空时：`http/protobuf` / `http` / `grpc` |
| `OTEL_EXPORTER_OTLP_HEADERS` | 逗号分隔 `k=v`，与 TOML headers 合并，同名 TOML 赢 |
| `OTEL_RESOURCE_ATTRIBUTES` | 追加 Resource；不能覆盖 `service.namespace` / `service.name` / `service.instance.id` / `nya.project` / `nya.instance.name` |
| `NYA_INSTANCE_NAME` | TOML `instance_name` 为空时使用 |
| `NYA_OTEL_LOG_LEVEL` | TOML `[obs.otel.logs].level` 为空时使用 |

不读 `OTEL_SERVICE_NAME`（避免把 `service.name` 打成 `client` 撞车）。Ctrl-C 先停 overlay 再 flush。

过滤（Prometheus 把 `.` 变成 `_`；Tempo 保留点；Loki **必须**在 collector 里把 Resource 做成 stream label，见示例 yaml）：

```promql
nya_failbacks_total{nya_project="nya-link-aggregation", nya_instance_name="edge-sh-03"}
```

```logql
{nya_project="nya-link-aggregation", service_name="nya-client", nya_instance_name="edge-sh-03"}
```

```traceql
{resource.nya.project="nya-link-aggregation" && resource.nya.instance.name="edge-sh-03"}
```

OTLP histogram 是 `_bucket`/`_sum`/`_count` 兼容 series，**不能** `histogram_quantile`；分位数只用 loopback `/metrics`。

健康判定、failback 公式、队列深度等在 `nya_core::Tuning`，**不能**写进 TOML。改算法请改 `Tuning::STANDARD` 并跑 e2e，不要给运维暴露一堆旋钮。

计分卡（`nya_core::obs` snapshot / `/metrics`）用来回答「聚合有没有用」：流 `closed/(closed+reset)`、send-unacked ∪ recv-hole stall、路径静默 `failover_ms`、跨 link `failbacks`（e2e chatter 门仍只看这个）、overlay `bytes_data_* / bytes_ctrl_*`。snapshot 里还有 **线路汇总** `links=`（按 `a`/`b` 把 `a#0`/`a#1` 卷起来：up/deg、RTT 范围、sticky、队列）以及压缩的 `streams=` 粘滞表。决策 why 只在 debug。详细清单见 [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md)。

客户端链路：

```toml
[[links]]
name = "a"
addr = "203.0.113.1:443"
connections = 2   # 该链路上独立的 TCP+TLS 数，默认 2
```

入站：

```toml
[[inbounds]]
type = "socks5"
listen = "127.0.0.1:1080"

[[inbounds]]
type = "forward"
listen = "127.0.0.1:2222"
target = "127.0.0.1:22"
```

## 测试

```bash
# 单元测试（不含 e2e matrix）
cargo test --workspace --exclude nya-e2e

# 短 SLA matrix（并行，约数分钟）
cargo test -p nya-e2e
# 或
cargo run -p nya-e2e --bin nya-e2e -- --jobs 8

# 含 30s / 60s / 5m blackhole
cargo run -p nya-e2e --bin nya-e2e -- --long

# 15 分钟混合 soak（near/mid/high/far）
cargo run -p nya-e2e --bin nya-e2e -- --mixed
cargo run -p nya-e2e --bin nya-e2e -- --mixed --band near
cargo run -p nya-e2e --bin nya-e2e -- --mixed --band mid,high,far --secs 480
```

损伤是每条路径前面的 TCP 代理（时延 / 抖动 / 以 stall 模拟丢包、blackhole、断开）。**不会**在 TLS 上随机丢字节。

## 安全

- 把示例里的 `psk = "change-me"` 换成长随机串；PSK 泄漏等于会话可被加入。
- 客户端必须 pin 服务端证书的 SPKI SHA-256，不要依赖系统 CA。
- 证书与私钥不要提交进仓库（已在 `.gitignore`）。
- 入站默认绑在 loopback；对公网暴露 SOCKS5 等于开放代理。

## License

[MIT](LICENSE)

源码：<https://github.com/metolab/nya-link-aggregation>
