# Module 13 local validation

From the migration-layout parent directory, with the retained interpreter and
native dependencies installed:

```sh
data-plane/.venv/bin/python vogon-rust/validation/operations/run.py --checks --repeat 5
```

The runner never reads dotenv or calls external providers. It runs the locked
workspace tests, repeats operations/recording tests, and checks scoped formatting,
Clippy and Rustdoc links. Logs and a source-hash manifest are written to ignored
`results/`. The two tests whose names begin `live_` remain explicitly excluded.

Latest local run: **207 passed, zero failed, two live tests filtered out**.
Four additional operations/recording runs passed, making five executions of those
tests. Formatting, Clippy, Rustdoc and a separate workspace build passed.

| Boundary | Evidence |
|---|---|
| Retry policy | Paused-time exact exponential delays, cap, jitter range, transient budget, permanent failure and cancellation during backoff. |
| Bootstrap | Global/per-dependency precedence, invalid settings rejected, S3 receives the resolved budget. |
| Turn/front HTTP | Stable Turn body/request ID across 408/429/5xx, configured attempt counts, no multiplicative Jev retry loop. |
| Speech | Local WebSocket handshake fails twice then succeeds; only one audio send; permanent 401 is not retried. |
| Twilio | Explicit 429 retries with identical form; 503 and 401 remain single attempts. No real telephone calls. |
| S3 | Official SDK signs requests to a local S3 protocol fixture; transient PUT reaches attempt three; auth failure is single-attempt; local success survives cloud failure; GET falls back to cloud. |
| Startup/readiness | Real NumPy/audioop/SpeexDSP/VAD execution, requested mode checks, failed storage probe never ready, shutdown readiness false. |
| Parallel ownership | Twenty rounds of 32 concurrent lease owners per repeated run; all released, read probes do not create logical call events. Existing workspace clock-isolation and duplex tests also run. |
| Shutdown | Existing process SIGTERM/driver cleanup plus cancelled transfer-drain test proving owned retry tasks are aborted. |
| Metrics | Eight threads produce exactly 8,000 events with fixed labels; histogram and transcript non-retention assertions. |
| Recording/service parity | 162 exact WAV/timestamp/event comparisons, 80 transfer XML comparisons and 228 service comparisons against the Python reference, run separately. |

Additional local tooling results:

- AddressSanitizer contract fuzzing completed **7,812,094 executions in 121 seconds**
  without a crash. This is a bounded smoke run, not exhaustive proof. It exercises
  contract JSON normalization; it does not fuzz FFI/audio/network code.
- The six selected pure control/retry/metrics packages produced LCOV successfully:
  648/1,384 lines (**46.82%**) and 96/331 functions (**29.00%**). Python differential
  corpora and other workspace packages are outside this coverage run. The workflow
  uploads measurements; it currently enforces no minimum percentage.
- Dependency sources, license and dependency-ban checks pass. Advisory checks
  **fail** on existing `unsafe-libyaml` / RUSTSEC-2024-0449; no suppression is used.
- Container build/smoke acceptance is **unverified**: the attempted local build
  exhausted root-disk capacity while extracting its base image. New GitHub
  workflows have not been exercised on hosted CI yet.

No real S3, PSTN, paid-model or GPT Live tests were run. Earlier live-call findings
remain deferred. These checks demonstrate behavior under controlled schedules,
not production throughput, complete race coverage or Rust-versus-Python speedup.
Operational settings and release limits are in [OPERATIONS.md](../../OPERATIONS.md).
