# Orchestration validation

From the repository root:

```sh
data-plane/.venv/bin/python vogon-rust/validation/orchestration/run.py --sockets --reference --checks
data-plane/.venv/bin/python vogon-rust/validation/orchestration/stress.py --repeat 50
data-plane/.venv/bin/python vogon-rust/validation/orchestration/stress.py --timers --repeat 10
```

Requires loopback socket permission, cached Cargo dependencies, the retained Python
numerical packages and native DSP build inputs. No `.env` keys are loaded by the
Rust fixtures; all STT/TTS/Turn API sockets are local. Python application tests mock
planners/agents and use ASGI. GPT Live and external model tests are excluded.
Without flags, run the pure policy, gated driver, text-session and clock-isolation
tests. A failed run invalidates the previous success manifest. Results and source
hashes are saved in [results/manifest.json](results/manifest.json).
The second command repeats scheduler-sensitive cases and records
[separate stress evidence](results/scheduling-stress.json); it measures no speedup.
The third repeats seven actual heartbeat/socket timer tests and records
[timer stress evidence](results/timer-stress.json). See the
[timer/state-machine audit](../../crates/orchestration/TIMER_VALIDATION.md) for
test layers, state ownership and scheduling limitations.

## Layers and cases

Recorded gate: **133 native tests passed** (35 orchestration plus 98 tests in
the connected modules), **34 Python reference tests passed / 2 existing xfailed**,
and the runner's formatting, Clippy and Rustdoc checks passed. The earlier scheduling stress run
passed **900 test executions across 50 repetitions**. The timer audit passed
**70 timer-path test executions across 10 repetitions**. Existing differential
gates also passed: 1,732 contracts/configuration cases, 2,390 playback cases,
1,828 barge-in cases, 7,048 front comparisons / 6,000 snapshots, 9,780 STT
snapshots and 15,100 clock snapshots. See manifests for the exact tested sources.

| Layer | Cases and assertions |
|---|---|
| Pure inbox | Matching/changed final revisions; resumed invalidation; sequence and callback replay; repeated legitimate phrases; facts versus triggers; sticky machine state; nullable phone history. Exhaustively replay 2,187 seven-event traces twice per callback. |
| Concurrent driver | Input while opening/TTS/HTTP are gated; ordered speech; changed eager/final and resumed stale suppression; duplicates; distinct OS-thread rendezvous; facts-first and terminal receipts; uncancellable terminal request; filler overlap/cancellation; timer/receipt events preserve speech revision and active hold; outage versus contract errors; bounded overload. |
| Lifecycle | Failed start creates no transport/terminal call; provider/config initialization failures still finalize; terminal/artifact timeout; disconnect during request; stale client events/end/escalation; held machine command exception; farewell waits for playout with live input and resumed-turn revalidation. |
| Native HTTP | Start/process/end wire contracts, auth, epoch, response decoding; identical retry body; all 5xx retry versus 4xx non-retry; body/header timeout; empty retry ladder; same-call serialization and independent clients. |
| Real text Session | Configuration/bootstrap, caller frame through engine to API, response and delivery fact, final terminal frame after transport close. |
| Real browser stack | Eight calls with real native HTTP and local STT/TTS sockets; simultaneous eager requests, seven calls progressing while one stalls, matching final submission and changed final; unique speech per call; audio intake while TTS is blocked. Interruption clears speech; bytes deliberately arriving before `Cleared` cannot leak into the next response. |
| Real phone stack | Native audio/AEC worker, streaming STT while TTS is blocked, eager/final flow, null heard history, disconnect joins work and flushes truncated delivery. Transport regression also checks protected machine takeover despite a stale ordinary generation. |
| Clock core | Parallel read-only clock access; 16 call traces versus isolated execution; pause/playout/watchdog isolation; duplicate/backwards ticks; independent offset estimators. |
| Transport clocks | 16 concurrent tick producers on one engine do not double-charge elapsed time or affect another engine; same-named thresholds and event sequences are call-local. |
| Timer transition audit | Seven explicit boundary tests for arming, nested pause/playout, activity reset, ladder/grace, pause/playout watchdogs and delayed ticks. |
| Actual heartbeat and socket timers | Two Session tests require real heartbeat publication without input and check close/isolation; two phone tests verify receipt/pause gating and final/classifier expiry; one real Session/Driver test queues a timer during blocked API work; two socket tests verify outgoing Ping/Pong expiry. |
| Actual Python reference | Existing HTTP triad/idempotency tests, driver contract/lifecycle cases, eager matching/changed/resumed application tests. HTTP seam asserts actual speculative reuse and pre-speculation checkpointing. These supplement Rust integration tests; they are not a direct trace-by-trace differential oracle for every new Rust schedule. |

The combined run also executes existing front, transport, providers, clock and
audio-runtime native tests. Module-1–4 and transport/provider/clock differential
runners remain separate regression gates:

```sh
data-plane/.venv/bin/python vogon-rust/validation/modules/run.py --stage all
data-plane/.venv/bin/python vogon-rust/validation/transport/run.py
```

The Python reference suite has two pre-existing expected failures: retaining
unacknowledged facts after a timeout and positive sequence numbers on synthesized
delivery outcomes. These are disclosed remaining protocol limitations, not
silently counted as passing parity. Local fixtures do not establish real model
quality, microphone/acoustic full-duplex acceptance, network capacity or a
comparative performance advantage. See [performance boundaries](../../crates/orchestration/PERFORMANCE.md).
