# Local binary acceptance — 2026-10-19

## Redis cluster compatibility build

Current output: `target/al2023-redis-cluster/`, source metadata
`707021a+redis-cluster-working-tree` (not committed). Distinct deployment archive:
`vogon-rust-al2023-redis-cluster-x86_64.tar.gz`. Gateway SHA-256:
`443ad1e2577471f719c67d320e727159a16cfb3e28cf1ae8d09cc4c4b936e3c7`.

Verified locally:

- 53 affected native tests passed; standalone TLS/certificate rejection and
  connection pooling/reconnect tests passed separately.
- A three-primary cluster passed the existing lifecycle/race/expiry suite and
  cluster-specific HTTP/routing tests over both plaintext and TLS (12 tests per
  run, including the shared memory reference cases).
- The old cross-slot operation is rejected; corrected same-slot scripts succeed.
  Concurrent creates from independent clients have exactly one winner; transfer
  values route across all three shards. EVAL ACL denial is classified explicitly.
- Deliberately dropping a mutation reply after Redis executes it returns a failure
  and leaves the mutation counter at one, proving no hidden command replay.
- The actual Amazon Linux runtime image starts against the three-primary TLS
  cluster with `REDIS_CLUSTER_MODE=true`; readiness, HTTP call creation and lookup
  pass, native audio probes succeed, and graceful shutdown exits zero.
- Exported executable checksums pass; maximum required GLIBC symbol version 2.34.
- Scoped Clippy, formatting, Rustdoc links and dependency policy checks pass;
  dependency advisories, bans, licenses and sources require no new exceptions.

The session namespace changes in cluster mode and occupies one slot to preserve
atomicity. Drain existing calls; this is not an active-call migration. Session
write throughput is limited to one shard, while transfer values remain distributed.
No AWS instance, production traffic, failover capacity or live providers were tested.

## Corrected Redis TLS build

The initial binary below lacked Redis TLS support. Replacement output is
`target/al2023-redis-tls/`, with source metadata
`707021a+redis-tls-working-tree` (not yet committed). Runtime image:
`vogon-rust:al2023-redis-tls`.

Both Redis clients now enable Tokio/Rustls with native CA roots and initialize
the crypto provider before connecting. Verified locally:

- 52 service/transfer regression tests passed, including plain Redis behavior;
  scoped Clippy and formatting passed.
- Two Redis TLS tests passed, including authenticated operations for both stores,
  session claims, concurrent transfer CAS and certificate/password rejection.
- The prior image fails the new trusted-TLS check with `invalid_redis_url`.
- The corrected packaged gateway passes trusted TLS startup and rejects wrong
  hostnames, untrusted certificates and incorrect passwords against real Redis.
- Clean-image native audio probes, non-root readiness and shutdown passed again.
- Exported executable checksums passed. The previous archive is preserved.

The TLS test runs locally with generated certificates, not against the user's
AWS instance. CI now includes the real-Redis TLS integration runner.

## Initial build

Gateway source: `707021a`. Build recipe and smoke runner are the accompanying
working-tree additions. The output is local, not a published GitHub release.

Both optimized executables were built natively inside an amd64 Amazon Linux
container synchronized to repository snapshot `2023.10.20260302`. Build metadata:

- Rust 1.99.1; generic x86-64 CPU target.
- glibc `2.34-231.amzn2023.0.3`.
- Python `3.11.14-1.amzn2023.0.4`; NumPy 2.2.6; retained built-in audioop.
- SpeexDSP `1.2.0-3.amzn2023.0.2`; compiled-in verified WebRTC VAD 2.0.10.

Passed checks:

- Both exported executable SHA-256 checksums verified.
- Highest gateway ELF GLIBC symbol requirement: 2.34.
- Clean runtime container resolved every shared library; no build headers or
  Rust compiler were inherited from the builder.
- Native startup probes passed for NumPy/audioop, SpeexDSP and WebRTC VAD.
- Gateway text readiness and recording-directory startup probe passed.
- Gateway and echo HTTP readiness succeeded as UID 10001.
- Gateway SIGTERM and echo SIGINT shutdown both exited zero.

Reproduce the runtime checks with `python3 deploy/al2023/smoke.py` after building
the `runtime` target as `vogon-rust:al2023`. Logs and artifacts are ignored under
`target/al2023-*`. The archive contains installation instructions, executable
checksums, license notices, build metadata and ELF version information.

This verifies packaging and local native execution, not EC2 networking, Redis,
provider credentials, real calls, S3 permissions or production acceptance. No
paid providers or GPT Live were contacted during this build and validation.
