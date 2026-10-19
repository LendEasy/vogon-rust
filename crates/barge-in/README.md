# Barge-in and floor control (module 4)

Native Rust port of the gateway's barge gate, semantic fast paths and default
`FloorController`. Time, transport state, VAD and classifier responses are
injected. This crate has no Python interpreter, provider client or network I/O.
The audio crates continue to retain NumPy, SpeexDSP, audioop and WebRTC VAD.

| Module | Responsibility | Python reference |
|---|---|---|
| `gate` | Energy streaks, TTS grace, adaptive noise floor, provisional M-of-N VAD, timeout/quiet abort, buffered onset and confirmation latch | `transport/barge_gate.py` |
| `semantics` | Normalized echo detection, backchannels, application-supplied safety/floor phrases, final verdict composition and absorb rechecks | `transport/barge_semantics.py`, `textutils.py` |
| `floor` | Acoustic episodes, revision/generation checks, protected/intro policy, repeat/persistent pressure, caller ownership, classifier scheduling and playback admission | `transport/floor_controller.py` |

## Host integration contract

Each call owns a `gate::Gate` and a `floor::Controller`. The controller owns the
mutable floor state and a `Transport` view; synchronize external playback,
audio ID, playout text and policy changes into that view before delivering input.
Clock inputs are monotonic seconds. Settings with the same field names can be
loaded into `floor::Config`; `gate::Config` uses the Python gate's names.

1. Give the gate each frame with its RMS, playout flag and timestamp. Its
   predicate calls the retained VAD only on the same provisional-window branches
   as Python. Frame bytes are opaque and forwarded unchanged; this crate does
   not convert their encoding or sample rate. Preserve the transport's existing
   VAD input convention when connecting module 5.
2. Feed the floor controller frame evidence, including the gate's current
   threshold and `confirmed_now`. Deliver every partial/final transcript and
   explicit turn-resumed notification. **Keep the STT feed open**: the floor
   controller does not use gate buffering to suppress inbound caller audio.
3. Drain ordered `Effect`s into the transport: logs, silence pause/resume, gate
   reset, typing stop, cut telemetry, playback cancellation, immediate clear,
   mark removal, ledger closure and playout-end notification. `CutPlayback`
   cancels an active stream or clears buffered drain audio, matching the
   existing transport method. Streaming cuts emit `SendClear` immediately,
   without waiting for the next TTS provider chunk. The controller also updates
   its transport view (absorb state, gap/typing flags, drain flag, streaming-cut
   end timestamp); reconcile that view with the host state.
4. Call `begin_classification` on the next executor turn and after a completion.
   It supplies a request token, revision/generation/audio-ID key, text, the
   captured heard/unheard split, attempt history and deadline. Run the provider
   outside the frame-processing task. Return its result/error with that token.
   New ambiguous revisions replace pending work; only one physical request can
   be dispatched at a time.
5. At the request deadline call `classifier_deadline`. A timeout becomes an
   uncertain decision, but **retains the request slot**. Return the eventual
   result/error to drain that slot; it is rejected as late before dispatching
   the latest pending revision. Duplicate or obsolete tokens cannot cut audio.
6. A final transcript can return a `FinalWait`. Poll it when its original
   worker finishes or its own deadline expires. Frames and heartbeat ticks
   continue meanwhile. Final waits are independent and idempotent; a later
   worker cannot extend an earlier final's wait.
7. Drive `tick` on the gateway's 50 ms heartbeat. Admission uses
   `allow_playback` and, for a held request, the same `Admission` token through
   `poll_admission`. Poll after floor/protection release, preserving request
   generations. Release epochs retain wakeups even if an event clears again
   before the waiter runs. This preserves Python's asyncio event semantics.
8. Pass playback completion, accepted response generations and machine-line
   takeovers into the corresponding methods. On shutdown, stop inputs, call
   `close`, cancel host timers/waiters and drain an already-running provider.
   Cancelling a waiter does not cancel underlying provider I/O.

Floor history is bounded to 16 episodes. An evicted episode remains alive only
while referenced by a pending request, active classifier or final waiter; it is
reclaimed after those references finish. The host owns admission/final tokens
and must drop them when their call is closed.

## Parity and reproduction

From the monorepo root:

```sh
data-plane/.venv/bin/python vogon-rust/validation/modules/run.py --stage barge
```

The runner builds offline with the lockfile, runs native lifecycle tests and
compares against the actual Python implementations. It saves source and binary
hashes, per-case digests, observed event coverage and native test output in
[`barge.json`](../../validation/modules/results/barge.json).

The current corpus passes **1,828 differential cases and five native lifecycle
tests**. It compares complete gate state, buffered bytes, VAD invocation counts,
floor episodes/state, ordered effects/log details, classifier inputs, final wait
completion and playback admission outcomes. It includes all 16 existing gate
regression tests, with Rust-returned verdicts/state (including the Python Twilio
adaptive-soft-barge integration test), and 16 traces using retained WebRTC VAD
results. The classifier oracle uses controlled asyncio futures and explicit
clock/deadline events; it never calls a live provider. Exact float parsing in
the JSON adapter prevents transport serialization from changing input clocks.

Regression scenarios include echo revisions, protected/intro safety policy,
retracted deferrals, repeated and persistent speech, energy blips after final,
resumed speech invalidating queued replies, immediate streaming and drain cuts,
provider timeout/drain, stale revision/audio/generation results, independent EOT
deadlines, release-before-waiter-resume, machine takeovers and history eviction.

No wrapper performance is measured. This is a validated decision core, not a
runnable Rust gateway. Module 5 still connects the transport effects, retained
VAD, media feed, playback and lifecycle; module 7 supplies runtime clocks/timers;
module 8 supplies the remote judge client. The optional older transport-owned
semantic-window orchestration is not implemented here; its pure verdict/absorb
helpers are included, while this controller ports the current default path.

The native boundary uses finite `f64` clocks and `i64` numeric configuration.
Text trimming, splitting and normalization use Rust standard Unicode whitespace.
U+001C–U+001F are ordinary characters; matching Python on these control
separators is intentionally outside the parity contract. The validation report
records those differences separately from passing parity cases.

Text normalization pins decimal-digit classification to the reference Python
Unicode 13.0.0 table; lowercasing uses Rust's Unicode implementation. The corpus
includes non-ASCII whitespace, quotes, digits and case conversion, but does not
claim exhaustive parity for every Unicode-version difference.

## Source documentation

Each source file documents its responsibility with Rustdoc module comments.
Structs/enums, their fields/variants, and public/private functions document
ownership, units, state changes, outputs and host responsibilities in place.

Build browsable documentation, including private state-machine helpers:

```sh
cargo doc --offline --locked --no-deps --document-private-items -p vogon-barge-in -p vogon-gateway-validation
```

The crate entry is `target/doc/vogon_barge_in/index.html`; the validation adapter
is under `target/doc/vogon_gateway_validation/`. Generated HTML stays in the
ignored build directory. The lifecycle test source documents each invariant it
checks; the saved parity report remains the correctness record.
