# 配置示例

- [`server.toml`](server.toml) — 监听、PSK、证书路径、`[session]` 放弃计时
- [`client.toml`](client.toml) — PSK、SPKI pin、链路、入站、`[obs]` / `[obs.otel]` 注释
- [`otel-collector.yaml`](otel-collector.yaml) — 示例 OTLP HTTP :4318 → Prometheus / Loki / Tempo（Loki label 与 Prometheus `resource_to_telemetry_conversion`）

先 `nya-server gen-cert --out certs`，把打印的 `pinned_spki_sha256` 填进客户端。两端 PSK 必须相同，不要用文件里的 `change-me`。

OTLP 默认关。打开时 TOML 里要有 `instance_name`。HTTP Basic/Bearer 写在 `[obs.otel.headers]` 的 `Authorization`，没有单独的用户名密码键。完整键表见仓库 README「远程 OTLP」和 [docs/OBSERVABILITY.md](../docs/OBSERVABILITY.md)。
