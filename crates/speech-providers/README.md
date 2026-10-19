# Speech providers

Native Tokio/WebSocket clients replace the Python network SDKs. NumPy/audioop
remain in `audio-runtime`; provider network tasks do not acquire the GIL.

| Source | Responsibility |
|---|---|
| `config.rs` | Redacted API-key owner; existing dotenv/environment precedence; phone/browser settings and per-call voice/name hints. |
| `stt.rs` | Flux callbacks, eager/resumed stitching, word confidence, onset projection, legacy hold and close fallback. |
| `tts.rs` | Speak/Flush segment boundaries, first-chunk latency and the existing per-chunk RIFF stripping behavior. |
| `wire.rs` | Independent TLS socket reader/writer, sensitive auth headers, ordered send acknowledgements, Ping/Pong and bounded connection/send waits. |
| `client.rs` | Per-call STT/TTS actors, streaming receivers, graceful close, cancellation drain and complete socket reconnect. |
| `live.rs`, `live_client.rs` | Optional existing GPT Live listener path: background startup, pre-ready frame accounting, quiet-final fallback, private agent context and discarded model audio. Disabled unless explicitly configured. |
| `bin/provider-validation.rs` | Deterministic JSON-lines boundary for Python differential tests. |
| `bin/provider-live.rs` | Explicit, bounded external smoke check with synthetic speech; never automatically invoked by validation. |

`SpeechSettings::load(path)` reads an existing dotenv file without modifying
process environment or writing a second credential file. This checkout has no
`voice-gateway/.env`; the authorized Deepgram check used `data-plane/.env`.
`for_call` applies `persona_voice`, language and tokenized `name_hint`.

Eager EOT emits speculation only. `TurnResumed` retains the preceding text and
cancels a legacy hold; definitive EOT commits stitched text. Remote close emits
pending eager text once. Explicit close cancels legacy hold before draining the
provider's closing messages. Word quality counts definitive EOT words only.

`TurnSequence` in `client.rs` owns a sequence high-water mark for one STT
connection. Replayed or older sequenced `TurnInfo` frames remain visible as raw
provider observations but cannot emit transcript callbacks twice. It resets with
the connection; it never compares counters from different calls. Frames without
a sequence retain the legacy parser behavior. New sequences with identical words
are legitimate new turns. This intentionally corrects the replay behavior exposed
by the [live generated-audio tests](../../validation/live/REPORT.md).
Deepgram defines `sequence_id` as increasing across server messages, independently
of `turn_index`; see its [Flux protocol reference](https://github.com/deepgram/skills/blob/main/skills/api/references/listen.md).

The legacy Python behavior is deliberately preserved: `StartOfTurn` cancels a
held final, while an ordinary `Update` does not. An empty `StartOfTurn` is skipped.
A timer task delivers a held final even if no further socket messages arrive.

TTS has one active utterance per socket. All segment Speak/Flush pairs are sent
upfront. A separate socket task continues receiving and answering pings while a
transport paces playback. Unbounded incoming audio preserves Python's current
receive-queue policy; this is not a memory/concurrency admission limit.

**Intentional failure-handling difference:** a Clear acknowledgement timeout
closes that socket epoch. Reconnect is required before another utterance, so late
audio cannot be mistaken for new speech. Python currently reuses that dirty
socket after its 300 ms drain timeout. Connection errors expose stable categories
and HTTP status, never auth headers or raw response bodies.

Validation: [provider results](../../validation/providers/results/parity.json),
[Deepgram live result](../../validation/providers/results/live.json),
[combined runner](../../validation/transport/README.md).
The live gateway suite also exercises delayed/replayed real provider messages,
speculative response invalidation and concurrent calls. Eager continuation
requires provider configuration that leaves a window before definitive EOT;
equal eager/final thresholds did not exercise that window in the saved dotenv run.

GPT Live testing is paused at the user's request. It is not part of the current
validation runner. No production-readiness claim is made for that optional path.
Its protocol was checked against the official [Live WebSocket reference](https://developers.openai.com/api/reference/resources/live/primary-websocket)
and [connection guide](https://developers.openai.com/api/docs/guides/voice-websockets).
