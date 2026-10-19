# Performance and concurrency boundaries

These tests establish progress, ordering and isolation. They do not establish
capacity, latency improvement over Python, or an advantage over another product.

| Critical step | Implementation and why it matters | Evidence / remaining measurement |
|---|---|---|
| Caller intake during HTTP/TTS | Dedicated input task; collector never awaits the entire API or speech operation. | Gated opening/HTTP/TTS tests, browser interruption and phone STT intake through real local sockets. |
| Parallel calls | Call-local tasks and locks on a multithread Tokio runtime. | Two calls rendezvous on distinct OS threads; eight HTTP eager requests arrive before any reply, and seven complete speech while one remains blocked. |
| Speculative lead time | Eager goes to the existing application server before final EOT. | Python HTTP-seam test checks `reused_speculative=true`, plus live-context matching/changed/resumed tests. Real model latency/savings still need measurement. |
| Discard cost and correctness | Revision changes at event ingestion; worker requests permission before output. Stale work cannot publish speech or end/escalate the call. | Queued changed-final, resumed, duplicate and late-response tests. Server work already committed cannot be undone here. |
| HTTP head-of-line delay | One transaction per call, including retries. Other calls continue. | Ordering is required by the existing server; timeout defaults to 40 seconds per attempt. Server protocol changes are needed to safely overlap same-call mutations. |
| DSP throughput | One audio owner per phone call, blocking work kept off Tokio. | Native AEC/VAD, retained NumPy/audioop. CPython/GIL and native thread-pool contention can limit scaling; no claim of unlimited CPU parallelism. |
| Audio pacing / write latency | One writer and ordered DSP state per call, invalidated output epochs on clear. | Late-audio rejection and marks/clear tests. Network jitter and actual provider/caller latency require live load tests. |
| Filler handoff | Source/hold works during HTTP; normal handoff waits for the active filler. | Deterministic gates prove overlap and no competing desk speech. This natural-completion wait adds latency by policy. |
| Timer cost / isolation | Read-only monotonic Clock; one gated CallClocks progression per call. | 16 parallel clock traces equal isolated traces; 16 competing tick producers do not duplicate elapsed time or thresholds. No mutable global call counters. |
| Admission and memory | Bounded ingress and pending-trigger limit, plus per-call event limit. | Explicit overload test. Module 10 still needs process-wide limits; provider channels need end-to-end queue-budget review. |

Before a capacity claim, measure calls per core, CPU/GIL contention, RSS and queue
depth, p50/p95/p99 caller-to-first-chunk latency, eager hit/discard rates, barge-in
clear latency, provider reconnect behavior and teardown duration at increasing
concurrent call counts. Use recorded audio with controlled delays/loss and the
same provider/application setup for Python and Rust. Separate model/network time
from gateway processing. Avoid using unit-test elapsed time as a benchmark.

## Clock and counter audit

`Clock` contains an immutable `Instant`. Cloning it shares a time coordinate,
not a counter. `now()` has no mutable receiver. Each `Engine::new` owns a new
`CallClocks`, floor, playback state and event sequence. Each driver owns new
request/wire/speculation counters and a new `Hold`; every writer gets its own
output epoch. Call duration subtracts that driver's start time even if several
calls use the same host epoch.

The call-related crates contain no mutable static counters or thread-local call
state. The static `OnceLock` values hold immutable schemas and guard constants;
they are not call variables. Within a call, short locks coordinate its tasks;
removing that coordination would make shared floor/clock state unsafe.
