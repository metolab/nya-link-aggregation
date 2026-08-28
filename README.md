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

预编译二进制在 [Releases](https://github.com/metolab/nya-link-aggregation/releases)：`nya-client` 和 `nya-server`，覆盖 Linux / macOS / Windows 的 x86_64 与 Linux / macOS 的 aarch64。main 上每次推送也会把同样的包传到 [Actions artifacts](https://github.com/metolab/nya-link-aggregation/actions/workflows/build.yml)。

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

默认客户端在 `127.0.0.1:1080` 提供 SOCKS5。日志级别用 `RUST_LOG`，例如 `RUST_LOG=nya_core=debug`。

## 配置

示例见 [`examples/client.toml`](examples/client.toml) 与 [`examples/server.toml`](examples/server.toml)。

`[session]` 只暴露运维表面：探测间隔、路径上限、全 down 放弃计时。未知键是解析错误，没有别名。

| 键 | 含义 | 默认 |
| --- | --- | --- |
| `ping_interval_min_ms` / `ping_interval_max_ms` | 探针对间隔夹紧到路径 RTT | 10 / 50 |
| `all_down_timeout_ms` | 全部路径 down 后拆会话 | 8000 |
| `max_paths` | 单会话路径上限 | 32 |

健康判定、failback 公式、队列深度等在 `nya_core::Tuning`，**不能**写进 TOML。改算法请改 `Tuning::STANDARD` 并跑 e2e，不要给运维暴露一堆旋钮。

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
