# Playback control core

Native, clock-injected logic extracted from the current Python transport. No
Python runtime, audio library, clock read or network operation is required.

| Module | Responsibility |
|---|---|
| `delivery` | Report fields, word-duration fallback, estimated heard prefix and heard fraction |
| `sentences` | Unicode-aware sentence splitting and short-fragment merges |
| `queue` | Priority ordering; a clear discards queued media/marks and precedes retained traffic |
| `ledger` | Bounded playout history, closing the latest open entry and onset-anchored heard horizons |
| `state` | Mark receipts, sentence-played counts, drain expiry, clear events, heard/unheard split and outbound offset |
| `stream` | Incremental sentence frames, tail/mark boundaries, pacing waits, graceful yield and cancellation |

`SentenceStream` emits actions. The transport must execute a media/mark action
and call `acknowledge` only after successful dispatch. `Media.mix` tells the
transport to apply ambience to full frames; short tails retain the existing
unmixed behavior. A `Wait` action must be honored before retrying `next` with the
new clock time. Provider chunks must not be pushed ahead of pending actions.
The caller must initialize the stream only for accepted, nonempty speech.

`PlayoutState` owns receipts separately from generation. After dispatching clear,
call `cleared`, clear the AEC reference via the audio runtime, and notify the
call clocks/floor controller. Terminal mark handling returns a flag for those
notifications. Protected/absorb flags are stored here; deciding whether an
interruption may cut them belongs to the `barge-in` floor controller.

The existing semantics are intentional: a report's negative `audio_ms` fraction
is not lower-clamped, terminal marks close the latest open ledger entry, and a
late mark after clearing is ignored. Retry behavior, network sends, clock
notifications and semantic interruption effects now live in the
[transport adapter](../transport/README.md), composed with the barge-in controller.
Production metrics and full service acceptance remain separate work.

Validation compares production Python functions and actual Twilio sentence-stream
and mark-handling methods with fake provider chunks and explicit time. It covers
normal/short/empty chunks, pacing leads, graceful yield, cancellation, duplicate
and stale marks, clear ordering, Unicode text and heard horizons. See
[module validation](../../validation/modules/README.md).

Text trimming, sentence boundaries and word counts use Rust standard Unicode
whitespace. U+001C–U+001F are ordinary characters, an intentional difference
from Python; the validation report records this separately from parity cases.

## Source documentation

Every module, function, struct, enum and field is documented in its source file.
The comments distinguish send acknowledgements from playback receipts, identify
seconds versus milliseconds and bytes, describe state changes and error behavior,
and specify which actions the host must execute.

Generate browsable documentation, including private helpers, from the workspace:

```sh
cargo doc --offline --locked --no-deps --document-private-items -p vogon-playback
```

Open `target/doc/vogon_playback/index.html`. Generated HTML stays under the
ignored build directory; the source comments are the maintained documentation.
