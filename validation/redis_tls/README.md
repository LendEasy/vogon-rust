# Redis TLS regression checks

`python3 validation/redis_tls/run.py` generates temporary CA/server certificates,
starts an isolated real Redis over a Unix socket, and exposes two local TLS
terminators. It executes the service's `redis_tls` Rust integration tests with
the first CA trusted. Redis commands and Lua scripts execute in real Redis; no
responses are emulated. Temporary keys and Redis processes are cleaned up.

Requirements: Cargo, OpenSSL CLI, the usual native/Python build dependencies, and
`redis-server` on PATH or `REDIS_SERVER_BIN` pointing to it. Native CI runs this
check without AWS credentials or paid provider requests.

Covered: TLS URL parsing and percent-encoded credentials, authenticated session
creation/token lookup/exclusive ownership, concurrent transfer CAS (exactly one
winner), and both stores rejecting hostname mismatches, an untrusted CA and an
incorrect password. A Rustls provider is initialized by the stores, before HTTP
host construction, matching the actual gateway startup order.

To validate the shipped executable instead of Cargo tests:

```sh
python3 validation/redis_tls/run.py --image vogon-rust:al2023-redis-tls
```

This runs the image's real `--check` against all four TLS cases using host
networking on Linux. The original `vogon-rust:al2023` image fails the trusted
case with `invalid_redis_url`, demonstrating that this check catches the original
packaging defect. This does not validate AWS routing, IAM-token refresh or Redis
cluster support.
