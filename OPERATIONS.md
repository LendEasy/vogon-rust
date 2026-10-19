# Runtime operations and dependency retries

Modules 11–13 are wired into the service. This guide describes implemented
behavior, not production acceptance. See [known issues](KNOWN_ISSUES.md) and
[validation](validation/operations/README.md) for the remaining gates.

## Retry configuration

The service validates immutable retry policies at startup. All network boundaries
use exponential backoff; each operation has one retry owner. Native DSP, NumPy,
audioop and local file errors are not retried: repeating an invalid input or
stateful audio operation cannot repair it safely.

```dotenv
GATEWAY_RETRY_ATTEMPTS=3
GATEWAY_RETRY_INITIAL_MS=200
GATEWAY_RETRY_MAX_MS=5000
GATEWAY_RETRY_JITTER=true
# Optional independent override:
GATEWAY_RETRY_STT_ATTEMPTS=2
GATEWAY_RETRY_S3_ATTEMPTS=4
```

`ATTEMPTS` includes the first request. Bounds are 1–16 attempts and
`0 <= INITIAL_MS <= MAX_MS <= 60000`. The dependency names are `TURN`, `FRONT`,
`STT`, `TTS`, `TWILIO`, `REDIS`, `REPORT` and `S3`; each accepts all four suffixes.
Resolution is dependency environment, dependency dotenv, global environment,
global dotenv, then default. Specificity wins over the source of a global value.
Settings are resolved once, not changed by a tenant or a concurrent call.

After failed attempt `n`, starting at zero, the delay ceiling is
`min(INITIAL_MS * 2^n, MAX_MS)`. Full jitter chooses a delay between zero and that
ceiling. Disabling jitter makes the shared Rust policy deterministic. AWS SDK and
Redis ConnectionManager retain their own jitter implementation; `JITTER=false`
does not disable jitter inside those libraries. Existing per-request timeouts and
call shutdown/cancellation still apply; attempts are a maximum, not a promise.
With default settings the two backoffs add at most 600 ms, in addition to request
time. `Retry-After` response headers are not currently interpreted by our policy.

| Boundary | Retry scope | Non-replayed failures / constraints |
|---|---|---|
| Turn API | Network/timeouts, HTTP 408/429/5xx; same serialized body and replay ID | Other HTTP errors and invalid responses. Same-call committing operations remain ordered. The service installs this policy; standalone legacy clients retain their old defaults. |
| Front, Jev, Cerebras, floor judge | Transient transport/status failures using `FRONT` | Authentication, invalid JSON and permanent status failures. Jev's legacy outer retry loop is disabled when the host supplies this policy. |
| Streaming STT/TTS | WebSocket connection/handshake failures, 408/429/5xx; TTS dirty-socket reconnect | Never replay audio into an established stream. A failed active STT stream is not transparently reconstructed. |
| Transfer persona TTS | HTTP synthesis network/429/5xx using `TTS` | No retry of other HTTP failures. Local Say fallback remains available. |
| Twilio dial/transfer REST | Explicit HTTP 429 only | Timeout, disconnect and 5xx can mean a physical call already exists. Never redial automatically after an ambiguous result. Reconciliation remains follow-up work. |
| Redis | Connection establishment/reconnect and safe GET reads | Authentication/command rejection is permanent at the read boundary. Lua CAS/mutations execute once: an acknowledged-lost write must not turn a winner into a duplicate loser. Read attempts can wait for connection recovery; per-operation deadlines bound that wait. |
| Transfer fact reports | Network, 409/429/5xx using `REPORT`; stable report UUID/body | No durable outbox; process failure may still lose a report. |
| Application status relay | Network, 429/5xx using `REPORT`; stable snapshot body | Best effort after budget exhaustion, not durable delivery. |
| S3 | Official AWS SDK standard retry classification/backoff using `S3` | SDK owns retries; no outer retry loop. Auth failures are permanent. Consuming a GET response body is not restarted after a mid-stream failure. |

Dropping a shared-policy future cancels its current request/backoff. Per-call
tasks own speech connections; no global retry counter influences call state or
`CallClock`. A retry does not undo a side effect already accepted remotely.

## AWS SDK S3 storage

`vogon-artifacts` uses native `aws-sdk-s3` and `aws-config`; no Python S3 wrapper.
`Cargo.lock` pins the complete SDK dependency graph. Configure `S3_ENABLED=true`,
`S3_BUCKET` and `AWS_REGION`. Explicit AWS access key/secret/session-token fields
from the shared dotenv are supported; otherwise the enabled SDK default credential
chain resolves credentials, including deployment role credentials. Prefer role
credentials in deployed environments. Never place credentials in an image.

The SDK has a 30-second operation timeout, 10-second attempt timeout and 3-second
connect timeout. Replayable PUT bodies use immutable bytes. Local atomic writes
and S3 upload run independently in parallel; a cloud failure preserves local
success and increments failure diagnostics. Reads try local storage before S3.
Keys are tenant-qualified and restricted to supported artifact names. There is no
background durable upload queue. See [artifact ownership](crates/artifacts/README.md).

## Startup, readiness and metrics

`vogon-gateway --check --env PATH` resolves configuration, connects the session
store, executes native probes and prints text-mode readiness. The native probe
actually imports NumPy/audioop, decodes silence, performs a NumPy reduction, and
processes a frame with native SpeexDSP and VAD. Blocking probes run off Tokio
workers. If recording is enabled, startup creates/removes a private write probe.
The default capability policy refuses a failed native deployment.

| Endpoint | Meaning |
|---|---|
| `/health` | Process liveness and cached capability information. |
| `/ready?mode=text` | Startup succeeded, session store currently readable and admission not stopping. Default mode. |
| `/ready?mode=voice` | Above, plus native success and configured Deepgram key. |
| `/ready?mode=twilio` | Above, plus configured Twilio account/token/from number. |
| `/metrics` | Prometheus text, protected by the existing gateway service-key policy. |

Readiness performs a bounded session-store read, not a state mutation. Native and
startup storage probes are cached. Credential presence does **not** prove remote
provider health, IAM permission or S3 access; readiness makes no paid provider
call and is not a complete deployment acceptance test. An empty service key
disables metrics authentication, consistent with management endpoints.

`vogon-observability` records fixed event categories, active owned sockets, call
capacity, readiness, transfer-report failures, first-TTS-chunk latency, completed
Turn request latency and scalar raw/clean RMS sums. The latter are diagnostics,
not an AEC quality score. Turn latency includes ordering wait and retries.
Labels never contain call IDs, tenant IDs, transcripts or provider bodies.
Process aggregates are shared only for reporting; clocks, event sequences and
speculative generations remain call-local. New operational failure logs use fixed
JSON event/code fields. This does not migrate every existing diagnostic to one
structured logging format or implement distributed tracing.

## Task ownership and shutdown

```text
SIGINT / SIGTERM
  |
  +--> stopping=true --> readiness=503 --> reject new call admission/callback work
  |
  +--> broadcast to all socket owners (including pending handshakes)
  |      |
  |      +--> close intake --> driver/provider/timer teardown --> EndCall
  |      +--> finish recorder --> atomic local write + SDK upload
  |      +--> terminal state / status relay --> release claim / permits
  |
  +--> wait for owned sockets: shutdown budget + 15 seconds
  +--> drain transfer tasks: separate shutdown budget
  +--> HTTP graceful close --> process exit

Call budget expires: abort owned driver, report failure
Transfer drain cancelled/expires: abort retained task handles
```

Default `GATEWAY_SHUTDOWN_TIMEOUT_SEC=210` is a per-phase budget. The service's
two waits are sequential; a fully busy process can require up to roughly
`2 * budget + 15` seconds before HTTP shutdown overhead. Set the supervisor's
termination grace accordingly. In-flight blocking audio encoding cannot be
preempted after `spawn_blocking` starts. Shutdown is bounded best effort; it does
not promise durable recovery after SIGKILL or machine failure.

## Connection ownership and warm-up

| Connection | Owner and lifetime |
| --- | --- |
| STT and TTS WebSockets | One of each per audio call; handshakes start concurrently. Reused across turns, closed at teardown. If either handshake fails, the other future/client is dropped and its socket task is cancelled. |
| Turn API HTTP | Service-owned reqwest pool shared across calls. Each HttpTurnClient retains its own bearer binding, clock epoch, ordering mutex and started state. HTTP/1.1 concurrent requests can use multiple TCP sockets; a pool is not a single socket. |
| Transfer Redis | One lazily initialized ConnectionManager per shared Store, including its clones. Concurrent first use initializes it once. Reads may retry; CAS commands are never automatically replayed after an ambiguous write. Reconnection replaces a lost socket. |
| Call-state Redis | Existing service-owned ConnectionManager, independent of the transfer manager. |
| Front / judge HTTP | Existing shared provider pool. When front generation is enabled and configured, startup attempts GET /v1/models on that pool once, consuming the response to make the connection reusable. The attempt is bounded to two seconds, performs no generation, and failure is logged without failing startup. Readiness does not repeat it. |
| Twilio / transfer reporting / transfer-line REST TTS | Existing host/transfer-manager HTTP pools. |
| S3 | Existing service-owned, lazily initialized AWS SDK client. |

Only network clients/pools are shared. Call clocks, event counters, speculative
revisions, credentials and serialization remain call-local. Idle eviction, peer
closure or concurrent demand can create new HTTP connections. Warming reduces
cold-start work only while the warmed connection remains available; it does not
promise that the next request uses the same socket.

`HttpTurnClient::new` remains available for standalone consumers; the service uses
`with_http` to inject its existing pool with redirects and hidden retries disabled.
The warm-up uses the same pool as real front completions; it does not warm JEV or
external application-model clients. STT/TTS still require independent sessions for
each call and cannot share provider speech state across calls.

Connection tests count accepted TCP connections, prove that another call progresses
while one call's HTTP request is held, check per-call auth/epoch isolation, gate
both speech handshakes before either may complete, and exercise failure cleanup.
The real-Redis test verifies a single connection across 32 racing callers, exactly
one CAS winner, and recovery after the server kills the connection:

```sh
REDIS_TEST_SERVER=/path/to/redis-server cargo test -p vogon-escalation \
  --test redis_pool -- --include-ignored
```

The concurrent echo harness and its measurement limits are documented in
[validation/live/README.md](validation/live/README.md).

## Performance and deployment limits

Input/event collection remains independent of awaited application HTTP and TTS.
Retries sleep asynchronously; they do not allocate one sleeping thread per call.
Call audio processing is serialized per call; independent calls run concurrently.
Keep BLAS/OpenMP worker counts at one unless measured deployment evidence supports
another value. Retained CPython/GIL throughput is still an acceptance item.

Admission defaults are 64 active calls and 128 pending handshakes, with 256 queued
input frames per call. These are limits, **not measured capacity**. Recorder
retention is bounded to 64 MiB encoded audio, four hours of timeline, 8 MiB event
JSON and 720,000 scalar frame rows. Sparse timelines can still expand to about
460 MB per stereo WAV during finalization. Concurrent finalizers multiply that
peak; no separate finalizer concurrency limit is implemented. Provider event/audio
backpressure and production-duration memory tests remain required before sizing.

The Dockerfile pins base-image digests, Rust, Python package versions and the Cargo
lockfile, includes the verified VAD sources during build, and runs as UID 10001.
OS packages still come from apt repositories, so this is not a bit-reproducible
OS build. The build context excludes dotenv, recordings, vendor downloads and
targets. Mount writable recording storage and supply secrets at runtime.

```sh
docker build -t vogon-rust:review .
docker run --rm --env-file /secure/vogon.env vogon-rust:review --check --env /nonexistent
docker run --rm --stop-timeout 450 -p 8002:8002 \
  --env-file /secure/vogon.env -v vogon-recordings:/data/recordings vogon-rust:review
```

Local image acceptance is outstanding: the attempted build exhausted the host's
root filesystem while extracting the builder image. A CI build/start/ready/SIGTERM
workflow is added but has not yet run on GitHub. Native host tests passing does
not establish container acceptance.

For rollout, retain the previous image and configuration, first close admission
and drain old calls, validate the new image's native/store checks, then perform
controlled text and generated-audio canary calls before opening traffic. Use
Redis for multiple workers. Python/Rust transfer state uses different namespaces;
mixed ownership of active calls is not accepted. Roll back by closing admission,
draining, and restoring the previous image/configuration. Preserve artifact data
and Redis state; do not delete them to make a rollout appear healthy. Real PSTN,
production load and earlier live acceptance issues remain explicit release gates.

## Automated gates

Native CI checks scoped formatting/Clippy, workspace tests and Rustdoc. Public
Python references are pinned for differential CI. Dependency policy rejects
advisories and unapproved licenses/sources. Scheduled/manual jobs produce LCOV
coverage and run bounded contract fuzzing under AddressSanitizer. These jobs do
not load deployment secrets, call GPT Live or make paid provider requests.

The current dependency gate intentionally fails on the existing unmaintained
`unsafe-libyaml` transitive dependency of `serde_yaml_ng`; there is no advisory
ignore. Replace/review the YAML dependency in the deferred fixes before release.
