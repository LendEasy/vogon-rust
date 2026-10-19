# Transport adapters

`Session` connects Twilio, browser PCM and text input/output to the existing Rust
audio, playback, floor and clock modules. The HTTP/WebSocket host supplies a
`wire::Sink`; no HTTP routes, call authentication or outbound telephone dialing
are implemented here.

For ownership, data flow and host integration rules across crates, see the
[architecture guide](../../ARCHITECTURE.md). Source comments and generated API
documentation describe lifecycle and error contracts for individual methods.

| Source | Responsibility |
|---|---|
| `session.rs` | Per-call lifecycle; provider callback pump; input forwarding; immediate cancellation; floor heartbeat and call clocks; gap-only ambience. |
| `session/playback.rs` | One response’s streamed output, packet pacing, reconnect/retry, delivery accounting and cancellation cleanup. |
| `engine.rs` | Pure callback composition: eager/final/resumed and absorbed events, onset/heard/carry-forward annotations, sequence numbers, generation admission, pending final decisions, marks and timer state. |
| `wire.rs` | Twilio JSON/base64 and browser UTF-8 framing; serial output with priority clear and generation rejection; post-send AEC/mark receipts. |
| `audio.rs` | Async bridge to the serialized native audio worker; waiting and explicit shutdown run outside Tokio executor threads. |
| `bin/transport-validation.rs` | Deterministic oracle surface for clock, framing and callback parity. |

Text-only calls can use `Session::text_only` without speech credentials.

Use validated module-1 settings and `resolve_call_snapshot` with
`Config::from_snapshot`. Build speech settings with `SpeechSettings::from_settings`
and apply `for_call`. Pass the same `Clock` to the call and its providers.
`Session::open` returns the session and its ordered event receiver. Forward caller
JSON/binary frames to `text`/`binary`; invoke `send_response` independently so
caller audio continues during output. A `true` input result means hangup/stop.
Always explicitly `close` after stopping response tasks.

Module 8 supplies native semantic clients. Build `front::bootstrap::Clients`
from resolved settings and inject its `judge` and `jev` clients into `Config.judge`
and `Config.line_client`. The Session owns request/completion tasks and returns
judge results using their exact dispatch tokens. Without a judge client, consume
`classifier_request` and return its token through `classifier_result`. The floor
controller retains a timed-out slot until provider completion/error.

`from_snapshot` enables phone line detection from the call policy. The detector
binds the first qualifying STT turn and retains its original acoustic episode.
Machine breakthrough clears protected playback before publishing its verdict and
replayed final with the new generation. `wait_line_quiet` and `release_line_hold`
expose the greeting hold. Shutdown aborts pending judge/line requests. Session
also implements the front `Speaker` trait; module 9 owns source/hold lifecycle.

Phone media reaches STT before floor side effects. During agent playback, drain
or the 250 ms echo cooldown, native AEC cleans it and the retained gate/VAD runs.
The no-AEC fallback suppresses overlapping input and floor frames, matching
Python. The existing gate's opaque mu-law-to-VAD callback convention is preserved;
changing its interpretation remains a separate DSP behavior change.

A successful outgoing media send feeds AEC before the writer accepts a subsequent
clear. Clear invalidates queued media immediately, waits for any in-flight send,
then dispatches the clear and discards references. Failed sends never register
marks or AEC references. This tightens Python's pre-send accounting on failures.
The sink must not synchronously re-enter the same session from its `send` method.

Provider output becomes 160-byte phone packets with sentence marks and terminal
receipt tracking. On a provider failure, the phone path reconnects once, waits
200 ms before recovery, and retries only the unsent sentence suffix with the
existing apology. The watchdog includes both attempts' buffered frames. A failed
provider attempt does not clear audio already handed to the caller. A caller
cancellation interrupts pacing or a stalled provider immediately and rejects stale
queued audio. A dropped response future schedules cancellation cleanup.

Browser audio uses the existing 8192-byte merge threshold and
`u32 little-endian UTF-8 ID byte length + ID + PCM` framing. Text responses are
atomic. Browser delivery reports retain unknown audio duration. Configured gap
ambience uses the retained mixer every 20 ms and stops when TTS begins; it does
not refill the buffer after a response. New-utterance output reset preserves AEC
references for older buffered playback.

This is the default open-feed/floor-controller adapter, not a complete gateway.
Legacy gated feed, disabled-controller window orchestration and burst-mode policy
(including the legacy linear16 phone path)
are not claimed; `from_snapshot` rejects those configurations.
[Module 9](../orchestration/README.md) now owns front/filler holds, Turn API dispatch
and call teardown. The [module-10 host](../service/README.md) supplies HTTP/WebSocket
entry points with local text/browser/phone integration tests. Recording and
production telemetry remain separate migration modules. No real Twilio call or
browser microphone acceptance test has been performed.

Validation and reproducible commands: [transport checks](../../validation/transport/README.md).
