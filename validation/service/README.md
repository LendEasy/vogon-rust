# Module 10 acceptance gate

Run from the monorepo root:

```sh
data-plane/.venv/bin/python vogon-rust/validation/service/run.py
```

Requires the cached Rust toolchain, existing Python environment/native DSP
libraries and permission to bind loopback sockets. Tests start and kill their own
Redis process on a random port; they never alter the shared Redis service.
`REDIS_SERVER_BIN` can override the default
`tools/redis-stable/src/redis-server` executable.

The runner selects the interpreter, writes logs and a source-hash manifest to
`results/`, and fails on any failed command. It never loads deployment `.env`
credentials for calls and never contacts external provider/model services.

## Case matrix

| Layer | Evidence |
|---|---|
| State | All 49 state pairs, sticky terminal states, first timestamps, schema migration and missing required identities. |
| Memory + real Redis | Same lifecycle, tenant/token/SID lookups, claim ownership, 16-way duplicate creation/claim races, concurrent snapshot/provider writes, wrong-owner release, live/terminal expiry and index refresh after claim/snapshot. |
| HTTP | Service auth, invalid requests, tenant scope, duplicate races, public-secret redaction, payload limit, native dial form fields/failure, proxy-prefix signatures and out-of-order callbacks. |
| Status relay | Correct pair bearer and body; unknown SID/status and repeated/backward callbacks do not generate duplicate relays. |
| Text WebSocket | Real caller -> driver -> HTTP Turn API -> output; opaque payload, persisted snapshot, hangup, disconnected pending request, invalid startup, ended-token rejection. |
| Admission/shutdown | Duplicate sockets on the same instance, concurrent calls with blocked StartCall, active/pending caps, input queue overflow, shutdown of active calls and pending handshakes, post-shutdown refusal. |
| Browser voice | Caller PCM continues while eager HTTP waits and while TTS waits for first audio. A changed final suppresses stale eager speech; actual caller receives only the expected audio frame. |
| Phone voice | SID lookup/wrong-mode refusal, connected/start handshake, nested stream SID, media JSON -> native AEC/STT, TTS -> correctly addressed caller media, stop and provider closure. |
| Direct Python comparisons | 40 decoded records, 120 forward state traces, 48 installed Twilio SDK signature cases, six actual Python TwiML route results. |
| Native process vs Python routes | 14 route cases using the real gateway binary and Python FastAPI router; random tokens and wall timestamps normalized, public contract compared. Native bootstrap `--check`, startup and SIGTERM exit. |
| Existing Python tests | `data-plane/tests/test_gateway_service.py`, including a scripted full text call across the application's actual HTTP seam. |
| Static checks | Cargo fmt, crate Clippy with warnings denied, Rustdoc broken-link check. |

Current saved gate: **25 native tests, 228 direct comparisons, 11 Python reference
passes and one skip**. The skip is the existing Python fake-Redis test because
`fakeredis` is not installed; native store tests use a real isolated Redis.
The [manifest](results/manifest.json) identifies the tested source revision.
The [comparison report](results/comparison.json) breaks down direct comparisons.

The existing module-9 gate was also rerun: **133 native passes**, **34 Python
passes / two existing expected failures**, formatting, scoped Clippy and Rustdoc.
See its [manifest](../orchestration/results/manifest.json).

These are correctness/concurrency tests, not throughput or latency benchmarks.
Mock providers gate progress to prove overlap; they do not validate current cloud
provider behavior. The host's default capacity values are not load-test results.
Known behavioral differences and incomplete integrations are in the
[service guide](../../crates/service/README.md#compatibility-decisions-and-remaining-limits).
