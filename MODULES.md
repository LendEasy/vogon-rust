# Vogon Rust: modules 1–13 and implementation checklist

This is the working checklist for the migration. Checked items describe implemented
work; unchecked items describe remaining implementation or acceptance work.
Modules 1–13 have implemented cores, but are not all production-complete.
Modules 11–13 are wired and locally tested; deployment acceptance remains.

Architecture and ownership: [implementation guide](ARCHITECTURE.md).
API documentation: [documentation guide](DOCUMENTATION.md).

The Rust workspace now includes a runnable HTTP/WebSocket host (`vogon-gateway`).
Transfers, recording, retries and operational probes are implemented. Production
readiness remains incomplete; see [operations](OPERATIONS.md).
Generated-audio calls now exercise this host with live Deepgram STT/TTS and a
local echo Turn service: [acceptance report](validation/live/REPORT.md).
This includes browser PCM and locally simulated Twilio media, not PSTN calls.
Earlier live acceptance gaps remain deferred for the next fixes.
An offline parity gate means the listed behavior matches the current Python
implementation on the saved corpus. Text operations use Rust standard Unicode
whitespace; the accepted differences for U+001C–U+001F are recorded separately
from parity cases. It does not mean provider integration,
deployment or every behavior of the owning Python transport has been ported.

| Order | Boundary | Status and parity gate | Remaining integration |
|---|---|---|---|
| Existing | `audio-core` | AEC: 81 pass / 6 known xfail, 46,376 exact frames. Mixer: 20 pass plus 257 differential cases, 24,000 exact frames. Formats: 91 cases. Direct C SpeexDSP/VAD; 12,600 exact runtime acoustic frames including recorded speech and changing paths. | Production/live-call acceptance; existing cold-start and changing-path transition leakage remain. |
| 1 | `contracts`, `configuration` | Implemented; 1,732 differential cases. 33 contract types, 112 config fields. | Service bootstrap supplies parsed dotenv/environment; JSON-safe finite values and i64 integer range are the native boundary. |
| 2 | `audio-runtime` | Implemented; four concurrent calls, 880 exact inbound frames and all generated output chunks; two native queue/lifecycle tests. | Async transport bridge implemented; process admission is wired; production packaging remains. CPython GIL throughput is not measured. |
| 3 | `playback` | Implemented control core; 2,390 differential cases. Queues, splitting, sentence pacing/actions, delivery reports, horizon ledger, mark receipts and drain-clear state. | Default transport/provider wiring, retry and clocks implemented; legacy burst/window paths and full service acceptance remain. |
| 4 | `barge-in` | Implemented decision core; 1,828 differential cases and five native lifecycle tests. Adaptive/provisional gate, semantic fast paths, episode/floor policy, protected/absorb behavior, classifier deadlines/stale results and generation-aware admission. | Effects, retained VAD, open STT feed and timers wired through module 5; module 8 now supplies and wires the remote judge client. Older optional transport window orchestration remains outside this default-controller port. |
| 5 | `transport` | Default Twilio/browser/text adapters implemented: native audio ownership, paced streaming, send/clear ordering, marks, floor callbacks, reconnect/retry and gap ambience. 1,200 Python transport callbacks, 100 wire cases and 960 audio/gate + 960 ambience frames. | Line detection and judge clients are wired; HTTP host and application orchestration are wired; recording remains separate; legacy gated/burst modes are not accepted by the snapshot adapter. Real caller acceptance remains. |
| 6 | `speech-providers` | Deepgram streaming STT/TTS implemented and live-tested. 350 STT traces / 9,780 snapshots, 120 TTS traces, 300 merge + 100 RIFF cases. Eager/resumed/final behavior preserved. | Optional GPT Live implementation exists but testing is deferred at the user’s request. Production outage/concurrency acceptance remains. |
| 7 | `clocks` | Monotonic clock, silence/duration ladders, nested pause/grace and overlap-aware playback watchdog implemented; 100 Python traces / 15,100 snapshots. Provider, floor and gap-ambience timers are wired. Six additional parallel isolation tests. | Reasoning lifecycle is wired through module 9; production timer/load acceptance remains. |
| 8 | `front`: front responses and line detection | Implemented and wired to transport and orchestration: selection/guards, Jev/Cerebras clients, source/canned holds and line detector. 7,048 direct comparisons, 6,000 state snapshots; original combined native gate: 73 pass. | External provider and full-call acceptance remain; exact-word echo limitation is preserved. |
| 9 | `orchestration`: call lifecycle | Core implemented: native Turn API, concurrent input/event collection, ordered response execution, eager/final invalidation, holds and finalization. Local eight-call/full-duplex and clock-isolation tests. Transfer/artifact hooks now installed. | Durable fact acknowledgement and live-call acceptance. |
| 10 | `service`: HTTP service/session management | Implemented host: HTTP/WebSocket routes, auth, admission, dotenv/profile bootstrap, native Twilio REST/signatures and atomic memory/Redis records. Local browser/text/phone integration tests. | Long-call lease renewal/crash recovery, dial reconciliation and production acceptance. |
| 11 | Transfers/escalation | Implemented: native REST, 80 XML comparisons, callback races and real Redis CAS. | Real telephone acceptance, crash recovery and durable report outbox. |
| 12 | Recording/artifacts | Implemented: 162 exact WAV/timestamp/event comparisons; actual-send capture, local storage and AWS SDK S3. | Browser recording, crash recovery, retention and real S3 acceptance. |
| 13 | Observability/deployment | Implemented retries, fixed metrics, native probes, readiness, CI/deployment definitions; 213 native tests and local container smoke pass. | Hosted CI rerun; production shutdown/load and real-call acceptance. |

Only implemented modules have crates. Future names are boundaries to implement,
not empty placeholders counted as completed work. The application remains
behind the existing Turn API; its business logic is not part of this migration.

## Decisions that apply to every module

- Retain NumPy and audioop through PyO3/CPython. Replacing audioop is deferred.
- Use direct C SpeexDSP and WebRTC VAD bindings; preserve the existing DSP behavior.
- Preserve Python behavior unless a difference is explicitly documented and accepted.
- Keep GPT Live tests paused. Default checks must not invoke paid external providers.
- Maintain a separate correctness gate for each module and test the connections
  between modules. Existing numerical benchmarks do not establish gateway throughput.
- Treat unsupported legacy modes as explicit scope decisions. Keep rejecting them
  until they are either implemented and tested or deliberately retired.

## 1. Contracts and configuration

**Status:** Core implemented. **Location:** `crates/contracts`, `crates/configuration`.
**Responsibility:** Define Turn API messages and resolve deployment, tenant and
per-call policy without importing the Python gateway at runtime.

- [x] Port typed messages, event unions, compatibility decoding and settings precedence.
- [x] Generate 33 contract types and 112 configuration fields from the Python reference.
- [x] Document generated type roles and preserve them during regeneration.
- [x] Connect validated settings and call snapshots to service startup and session
  creation in module 10; avoid separate sources of defaults.
- [x] Validate complete StartCall, ProcessEvents and EndCall exchanges through module 9's native HTTP client.
- [ ] Keep schemas and parity cases synchronized with any upstream contract changes.

**Completion gate:** Existing 1,732 differential cases remain passing; real client
serialization and service configuration precedence are covered by integration tests.
Finite JSON values and i64 integers remain the documented native boundary.

## 2. Audio runtime

**Status:** Core and async bridge implemented. **Location:** `crates/audio-runtime`;
shared processing lives in `crates/audio-core`.
**Responsibility:** Own one call's AEC, VAD, resampler, framer and mixer on a
serialized worker, with bounded command admission and deterministic cleanup.

- [x] Implement worker startup, ordered commands, queue-full rejection and draining close.
- [x] Connect the transport through an async bridge that keeps blocking work off Tokio threads.
- [x] Separate output reset from caller clear; feed AEC only dispatched audio.
- [ ] Exercise worker lifecycle and queue pressure under the service's intended call load.
- [x] Connect process-wide call admission to module 10; per-call queue limits alone
  do not bound the number of workers or total memory.
- [ ] Verify interpreter/native-library startup and teardown in the deployment image.
- [ ] Complete real-call AEC acceptance and record the existing cold-start and
  changing-path limitations; improvements require a separate behavior decision.

**Completion gate:** Preserve audio/frame parity, demonstrate no cross-call state
leakage, and prove accepted work drains safely during service shutdown. Existing
four-call/880-frame validation is a correctness check, not a capacity claim.

## 3. Playback

**Status:** Core and default transport integration implemented. **Location:** `crates/playback`.
**Responsibility:** Split and pace speech, track sent versus heard audio, process
marks and clears, and produce delivery reports.

- [x] Port sentence splitting, queues, pacing, delivery reports and heard-horizon tracking.
- [x] Integrate marks, immediate cancellation and reconnect/retry accounting through module 5.
- [x] Connect application speech commands and delivery outcomes to module 9's event loop.
- [ ] Validate complete response sequences with late/duplicate marks, disconnects,
  cancellation and retries while caller input continues.
- [ ] Resolve legacy burst/window playback scope jointly with module 5; these paths
  are not covered by the current default sentence adapter.

**Completion gate:** Preserve 2,390 differential cases and demonstrate that a
completed send is never reported as proof that the caller heard all generated audio.

## 4. Barge-in and floor control

**Status:** Decision core and default transport effects implemented.
**Location:** `crates/barge-in`.
**Responsibility:** Combine acoustic evidence, transcript semantics and playback
policy to grant the caller the floor or continue agent speech.

- [x] Port adaptive/provisional gates, local VAD confirmation and semantic fast paths.
- [x] Implement protected/absorb policy, classifier tokens/deadlines, stale-result
  rejection and generation-aware playback admission.
- [x] Wire floor effects, audio input and heartbeat through the default phone adapter.
- [x] Connect the native semantic judge client from module 8, including token-matched error completion and shutdown cancellation; validated with local HTTP mocks.
- [ ] Verify protected introductions, absorbed utterances and late classifications
  across the complete module 9 call pipeline.
- [ ] Validate genuine interruptions versus residual echo using real calls/replays.

**Completion gate:** Preserve 1,828 differential cases and five lifecycle tests;
prove a stale classification or response cannot cut or replace a newer turn.
Timed-out classifier work must still report completion/error to release its slot.

## 5. Transport adapters

**Status:** Default Twilio, browser PCM and text adapters implemented.
**Location:** `crates/transport`.
**Responsibility:** Translate caller protocols into provider/control events and
deliver audio with correct ordering, pacing, marks and AEC references.

- [x] Implement input routing, wire framing, streaming output, clear priority and mark receipts.
- [x] Connect audio ownership, floor control, clocks, retries and gap ambience.
- [x] Attach authenticated HTTP/WebSocket sockets through module 10's host and Sink.
- [x] Connect module-8 line and judge decisions, protected machine breakthrough, replayed final events and quiet holds.
- [x] Connect application events/front lifecycle to module 9.
- [x] Connect recording/event hooks to module 12.
- [ ] Run actual Twilio and browser microphone acceptance tests, including abrupt
  disconnect, overlapping speech, interrupted output and session cleanup.
- [ ] Decide whether to port legacy gated STT, disabled-controller/window behavior
  and burst/linear16 phone playback; retain explicit rejection until resolved.

**Completion gate:** Preserve existing callback/framing/audio-wiring checks and
verify full live caller flows. Idle callback parity plus native ordering tests
does not cover every possible active-playback event interleaving.

## 6. Streaming speech providers

**Status:** Deepgram STT/TTS implemented and live-smoke-tested.
**Location:** `crates/speech-providers`.
**Responsibility:** Own streaming provider connections, transcript semantics,
segment boundaries, cancellation draining and reconnect lifecycle.

- [x] Implement Flux eager EOT, resumed-text stitching, definitive final callbacks,
  word confidence, onset projection and close fallback.
- [x] Implement streamed TTS segments, independent socket handling, cancellation
  draining and reconnect after a dirty-socket timeout.
- [x] Validate synthetic live Deepgram speech, including eager/resumed/final events.
- [ ] Validate provider outages, slow consumers and concurrent calls under the
  intended deployment load, including bounded recovery and shutdown behavior.
- [ ] Resolve unbounded incoming audio/event buffering with an explicit memory and
  backpressure policy before production; document any departure from Python.
- [x] Verify eager/resumed/final events through the application orchestration in module 9,
  including changed finals, late eager replies and duplicated provider sequences,
  with generated audio and live Deepgram under controlled EOT settings.
- [ ] Accept deployment EOT settings: the saved dotenv uses equal eager/final
  thresholds and finalized the first utterance before the intended continuation.
- [ ] Resume optional GPT Live acceptance only after the user authorizes it;
  the existing implementation is not an accepted production path.

**Completion gate:** Preserve 350 STT traces/9,780 snapshots, 120 TTS traces and
merge/RIFF cases; prove failure cannot look like successful empty synthesis or
deliver stale audio into a later utterance. No GPT Live test is required while paused.

## 7. Clocks and timed behavior

**Status:** Clock core and adapter timers implemented. **Location:** `crates/clocks`.
**Responsibility:** Maintain monotonic timing, silence/duration thresholds,
reasoning pauses, explicit hold grace and outstanding-playback watchdogs.

- [x] Port clock projection, silence ladders, nested pauses and overlap-aware watchdogs.
- [x] Wire provider timers, floor heartbeat and gap-ambience scheduling.
- [x] Implement front-model/canned-filler timing in module 8, including cancellation, natural in-flight completion, truncated speech and repeated hold closure.
- [x] Connect hold and reasoning lifecycle to the module-9 application loop.
- [x] Validate call-local clocks/counters under parallel readers, independent calls and competing tick producers; ignore backwards ticks to prevent double counting.
- [ ] Validate competing timer expirations, disconnect/shutdown and playback
  completion across the full call loop using deterministic injected time.
- [ ] Ensure service shutdown cancels timer owners and produces no late call actions.
- [x] Exercise live phone silence ladders and isolation while another phone call
  speaks; disconnect and SIGTERM checks also require finalized calls and closed providers.
- [ ] Connect browser playback completion to the silence-ladder lifecycle;
  the initial live browser silence case did not fire the requested events.

**Completion gate:** Preserve 100 traces/15,100 snapshots and verify silence
windows resume correctly after successful, failed and cancelled application turns.

## 8. Front responses and line detection

**Status:** Implemented core, native clients and transport integration; offline/local-mock gate passed.
**Location:** [`crates/front`](crates/front/README.md).
**Responsibility:** Generate/select short front responses, classify interruption
intent and detect human versus machine lines using existing gateway policy.
**Python references:** [front selection](../voice-gateway/vogon/gateway/transport/front_pick.py),
[guards](../voice-gateway/vogon/gateway/transport/front_guard.py),
[source](../voice-gateway/vogon/gateway/transport/front_source.py),
[moves](../voice-gateway/vogon/gateway/transport/front_moves.py),
[Jev](../voice-gateway/vogon/gateway/jev.py),
[judge](../voice-gateway/vogon/gateway/transport/barge_judge.py),
[line detector](../voice-gateway/vogon/gateway/transport/line_detector.py).

- [x] Port response selection, guards, rider precedence, confidence thresholds,
  sticky distress, repetition/gap admission and fallback policy into explicit modules.
- [x] Implement settings/artifact bootstrap and native Jev/Cerebras clients,
  preserving wire contracts, typed answers, bounded retries and request deadlines.
- [x] Return judge completion/error using module 4's exact dispatch token; abort
  outstanding decisions during transport shutdown.
- [x] Port and wire line detection: first-turn binding across acoustic episodes,
  coalesced asks, confidence bands, ask-window/quiet/cap timers and ordered verdicts.
- [x] Apply machine breakthrough through protected playback and replay finals with
  the current generation; preserve original episodes across delayed finals/history eviction.
- [x] Implement source/canned filler holds through a `Speaker` interface, implemented
  by Session. Cover early end, natural in-flight completion, truncation, failures,
  duplicate calls, owner drop and stale generation cancellation.
- [x] Add actual Python differential comparisons, local HTTP/WebSocket tests,
  paused-time lifecycle tests, a reproducible runner and a documented case matrix.
- [x] Connect front/hold ownership to module 9's eager/final/resumed Turn API loop.
- [ ] Run external Jev/Cerebras and complete-call acceptance separately; no live
  model calls or GPT Live tests were run in this module.
- [ ] Address the inherited misheard-brand/partial-opening echo-strip limitation
  as a separately tested behavior change; Python records it as an expected failure.

**Dependencies:** Modules 1, 3, 4, 6 and 7.
**Evidence:** [Case matrix and commands](validation/front/README.md),
[parity](validation/front/results/parity.json),
[native and source-hash manifest](validation/front/results/manifest.json).
7,048 policy/protocol comparisons and 6,000 snapshots pass. The combined native
suite has 73 passing tests, with two GPT Live tests filtered out. Python reference
suite: 185 pass, one existing expected failure. Formatting, new-crate Clippy and
Rustdoc link checks pass; earlier module/transport/provider/audio-wiring parity
also passes. The corpus is not a claim of exhaustive network/acoustic coverage.

## 9. Call orchestration

**Status:** Core implemented; local integration and concurrency gate available.
**Location:** [`crates/orchestration`](crates/orchestration/README.md).
**Responsibility:** Coordinate the caller, application Turn API, speech execution,
front responses, finalization and escalation decisions.
**Python references:** [call driver](../voice-gateway/vogon/websocket/handler.py),
[Turn client](../voice-gateway/vogon/websocket/turn_client.py).

- [x] Implement StartCall, ProcessEvents and EndCall client operations with the
  existing request IDs, authentication, retry and error semantics.
- [x] Port event ordering/batching and response execution while keeping caller
  input and provider callbacks independently serviced.
- [x] Implement eager-EOT speculation, resumed-turn cancellation and final commit
  across the application boundary; prevent duplicate turns and stale responses.
- [x] Coordinate front responses, filler holds, floor admission, reasoning pauses,
  silence-policy updates and delivery reports.
- [x] Implement hangup/failure finalization, pending-event drain and dispatch of
  escalation and recording work to modules 11 and 12.
- [x] Verify distinct runtime threads, eight concurrent calls with a blocked peer,
  caller audio during TTS, browser interruption/late bytes, phone AEC intake and
  per-call clock/counter isolation. Test application speculative reuse across the
  real Python HTTP seam with mocked agents.
- [x] Install the module-10 host and validate real caller sockets through the driver.
- [x] Install actual transfer/recording hooks from modules 11/12.
- [ ] Add durable acknowledgement/replay for facts after ambiguous transaction
  failure and a unified positive sequence for synthesized delivery facts.
- [ ] Complete latency/first-chunk analytics parity in module 13 and live-call/load acceptance.

**Dependencies:** Modules 1–8; exposes integration points for 10–12.
**Evidence:** [Case matrix and reproducible gate](validation/orchestration/README.md),
[results](validation/orchestration/results/manifest.json),
[performance-critical steps](crates/orchestration/PERFORMANCE.md).
Combined native gate: 114 pass; scheduler stress: 900 executions pass; Python
reference: 34 pass / two existing expected failures. Static checks pass.
Rust deliberately invalidates stale output earlier than the Python sequential
processor, deduplicates callback replay and suppresses side effects in eager
acknowledgements. Same-call committing HTTP requests remain ordered; independent
calls execute concurrently. Application speculation/business logic remains behind
the existing Turn API. Tests are not a claim of complete schedule coverage or
production throughput; known fact-protocol limitations are documented.

## 10. HTTP service and session management

**Status:** Core implemented; local service/process acceptance passes.
**Location:** [`crates/service`](crates/service/README.md).
**Responsibility:** Host the adapters and orchestrator, authenticate requests,
resolve configuration and own session admission/persistence.
**Python references:** [routes](../voice-gateway/vogon/gateway_service/routes.py),
[startup](../voice-gateway/vogon/gateway_service/main.py),
[session store](../voice-gateway/vogon/gateway_service/session_store.py).

- [x] Implement management HTTP/WebSocket routes, validation, pair-secret auth,
  native Twilio dial/signature/TwiML/status contracts and best-effort status relay.
- [x] Resolve shared dotenv/settings/tenant profiles; save the resolved call snapshot
  before opening transport. No second configuration-default source.
- [x] Wire caller input/Sink to the native driver, keeping input and event work
  independent of blocked HTTP/TTS; preserve eager/final invalidation.
- [x] Implement atomic memory/standalone-Redis creation, tenant/token/SID lookup,
  unique socket claims, retention and monotonic terminal state.
- [x] Bound active calls, pending handshakes, input frames and message size; close
  admission and await active calls during shutdown; test process SIGTERM exit.
- [x] Validate text/browser/phone calls on actual local sockets; use an isolated
  real Redis, all 49 lifecycle transitions, duplicate races and index expiry.
- [x] Replace explicit 501 transfer/artifact routes and `NoHooks` with modules 11/12.
- [ ] Add long-call lease renewal/recovery, ambiguous dial reconciliation and
  production load/rolling-deployment acceptance with module 13.

**Dependencies:** Modules 1, 5 and 9; service lifecycle acceptance continues in 13.
**Evidence:** [Case matrix and reproducible gate](validation/service/README.md),
[manifest](validation/service/results/manifest.json).
25 native tests; 228 direct Python comparisons; Python reference: 11 pass, one
existing skip because `fakeredis` is unavailable (native Redis tests use real Redis).
Formatting, crate Clippy and Rustdoc pass. Module-9 regression: 133 native pass;
34 Python pass / two existing expected failures. No external/GPT Live tests run.
Stricter IDs/URLs, atomic duplicate ownership, sticky terminal states and memory
expiry are intentional [documented deviations](crates/service/README.md#compatibility-decisions-and-remaining-limits),
not assertions of exact equivalence to Python's race behavior.

## 11. Transfers and escalation

**Status:** Implemented with local provider mocks; telephone acceptance remains.
**Location:** [`crates/escalation`](crates/escalation/README.md).
**Responsibility:** Execute transfer requests and reconcile asynchronous provider
callbacks into one consistent escalation outcome.
**Python reference:** [escalation package](../voice-gateway/vogon/gateway_service/escalation).

- [x] Port dial/hold/whisper/keypad flows and generated TwiML/voice responses.
- [x] Implement transfer and per-leg state, timeouts, fallback destinations and cleanup.
- [x] Port callback handling with atomic transitions and duplicate/out-of-order protection.
- [x] Report in-call outcomes and post-call transfer facts through existing contracts.
- [x] Connect escalation to module 9 decisions and module 10 routes/session records.

**Dependencies:** Modules 1, 9 and 10.
**Completion gate:** Python parity for state/TwiML/outcome behavior; callback race,
timeout and duplicate tests; controlled telephone acceptance before production use.

## 12. Recording and artifacts

**Status:** Implemented for the existing Twilio recorder; local/S3 protocol tests pass.
**Location:** [`crates/artifacts`](crates/artifacts/README.md).
**Responsibility:** Preserve accurate caller/agent audio and event timelines,
including what happened to buffered audio when playback was cleared.
**Python references:** [recorder](../voice-gateway/vogon/gateway/audio/audio_recorder.py),
[timeline](../voice-gateway/vogon/gateway/audio/audio_timeline.py),
[artifact store](../voice-gateway/vogon/gateway/audio/artifact_store.py),
[audio event log](../voice-gateway/vogon/gateway/transport/audio_event_log.py).

- [x] Port recording clocks, channel policy and timestamped inbound/outbound capture.
- [x] Integrate actual-send, mark and clear hooks; preserve Python's treatment of
  cleared/unheard output instead of recording all generated TTS as played.
- [x] Generate WAV and event artifacts with the existing metadata and naming conventions.
- [x] Port local/S3 persistence, upload errors and finalization/cleanup behavior.
- [x] Attach recorder lifecycle to call creation and module 9 finalization.

**Dependencies:** Modules 1–3, 5, 7, 9 and 10.
**Completion gate:** Compare WAV samples/lengths and event timelines with Python
for gaps, overlap, partial frames, clears and interrupted calls; test storage failures.

## 13. Observability, development checks and deployment

**Status:** Core implemented; production acceptance remains incomplete.
**Location:** `crates/resilience`, `crates/observability`, service operations,
Dockerfile and `.github/workflows`. See [operations](OPERATIONS.md) and the
[validation gate](validation/operations/README.md).
**Responsibility:** Make the gateway diagnosable, reproducible and operable under load.
**Python references:** [metrics](../voice-gateway/vogon/gateway/metrics),
[capability checks](../voice-gateway/vogon/gateway_service/capabilities.py).

- [x] Add fixed operational metrics for provider, floor, playback, scalar AEC,
  session and application events, plus structured operational error logs.
- [x] Implement native startup probes, writable recording-store checks and bounded
  session-store readiness. Provider readiness checks credential presence only.
- [x] Configure exponential retries for all network boundaries, with cancellation,
  per-dependency overrides and explicit safety exceptions for non-replayable work.
- [x] Define pinned toolchain/runtime inputs and a non-root deployment image.
- [x] Add scoped formatting/Clippy, Rustdoc, native and pinned Python parity CI;
  dependency policy, coverage and contract fuzzing with AddressSanitizer.
- [x] Validate native shutdown/parallel admission and cancellation of transfer drain;
  document phase budgets and current resource limits.
- [x] Replace the unmaintained YAML backend without suppressing the advisory gate;
  document and test leading-zero scalar migration behavior.
- [x] Complete the pinned Docker build and non-root gateway/echo smoke tests.
- [ ] Rerun repaired workflows on hosted CI after publication.
- [ ] Bound provider buffering and concurrent recording finalization; establish
  production resource/admission limits with load and long-duration evidence.
- [ ] Complete deployment smoke tests, real-call acceptance and a documented
  rollout/rollback procedure. Report any performance work separately from correctness.

**Dependencies:** Applies throughout development; production acceptance requires
all deployed modules. Tooling and packaging work can proceed alongside modules 9–12.
**Completion gate:** A clean reproducible build passes automated checks, starts with
explicit capability results, handles intended concurrent load and shuts down without
leaks or lost finalization work. Default CI performs no paid provider calls.

## Execution order and completion rules

Implement **8 → 9 → 10**, then complete transfer and artifact integrations in
**11–12**. Develop module 13's CI/dependency packaging alongside those modules,
and perform its operational acceptance after the service is assembled.
Return to modules 1–7 for the integration and acceptance items listed above;
do not rewrite their already validated cores just to mark a module complete.

For every module, complete its remaining checklist, wire its dependencies, run
its Python parity and native failure/lifecycle checks, document intentional
differences, and record the relevant service acceptance evidence before marking
it production-complete. Deferred GPT Live and unresolved legacy modes must remain
explicitly outside the accepted deployment configuration.

## Validation gate per module

1. Execute the existing Python implementation as the reference, with deterministic
   input/clock/provider fixtures where needed.
2. Compare Rust outputs, emitted actions and relevant state; compare acceptance
   or rejection for invalid inputs. Do not replace these with tests of a copied
   expected algorithm.
3. Save case outcomes and source/binary hashes. Keep runtime invariants that
   have no Python equivalent (bounded admission, worker shutdown) as native tests.
4. Re-run dependencies when a shared implementation changes. Integration gates
   must also prove that the components are connected correctly.

Results: [contracts/configuration](validation/modules/results/contracts.json),
[playback](validation/modules/results/playback.json),
[barge-in/floor](validation/modules/results/barge.json),
[audio runtime](validation/audio/results/runtime.json),
[audio validation](validation/audio/README.md),
[streaming/transport/clock validation](validation/transport/README.md).

Existing numerical benchmarks under `prototypes/` remain historical artifacts
with their own hashes. Live acceptance records end-to-end response timings as
call diagnostics; these include provider/network time and are not numerical
benchmarks, capacity measurements or evidence of a Rust speedup.
