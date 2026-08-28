# 配置示例

- [`server.toml`](server.toml) — 监听、PSK、证书路径、`[session]` 放弃计时
- [`client.toml`](client.toml) — PSK、SPKI pin、链路、入站

先 `nya-server gen-cert --out certs`，把打印的 `pinned_spki_sha256` 填进客户端。两端 PSK 必须相同，不要用文件里的 `change-me`。
