# Per-call audio runtime

Owns AEC, VAD, audioop resampling, phone framing and optional ambience on a
dedicated worker per call. Native handles are created, used and destroyed on
that worker. Commands are ordered through a bounded queue; full queues reject
explicitly. The worker releases the GIL while waiting for commands.

`AudioRuntime::submit` never waits for audio processing; it returns a response
receiver. The transport's [Audio bridge](../transport/src/audio.rs) awaits that
receiver on blocking executor capacity. `start` and `close` are blocking lifecycle operations: execute
them outside the network executor and without holding the Python GIL. The
validation Python adapter releases the GIL around both operations and waits.

`Tts` generates frames but does not enqueue an AEC reference. Submit `Reference`
only for dispatched audio. `Clear` drops buffered TTS/resampler state and calls
AEC's reference-discard operation. `ResetOutput` resets framing/resampling while
retaining references for older buffered playback. `PhoneInput` combines overlap,
AEC and gate policy; `AmbienceFrame` renders a gap frame without TTS. The command
variants document their byte formats and side effects in the source.
`close` drains accepted work, closes native
handles and joins the worker; repeated close is harmless. Close all runtimes
before interpreter shutdown. Each runtime belongs to one call; it does not
provide cross-call ordering or a global call-admission limit.

Validation runs four concurrent Python callers against independent native workers
and Python reference processors. Inbound bytes/RMS/VAD decisions and all generated
outbound chunks agree; clear/reset, failed initialization, dispatch/reference
separation and accepted-work draining are checked. Two Rust tests cover queue
overflow and rejection after closure.

The acoustic runtime stage also dispatches framed playback, feeds its actual
post-mix/post-resampling reference, and synthesizes a delayed echo return. Gain
steps/ramps, physical delay steps/drift, fractional delays and recorded speech
are checked against Python with separate acoustic quality requirements. Direct
SpeexDSP/VAD calls work even when their Python imports are blocked.

This is correctness validation, not a throughput benchmark. A worker per call
does not eliminate CPython GIL contention. Recorded acoustic fixtures and local
async integration are covered; live-call acceptance and production scheduling
under load remain later integration gates.

```sh
data-plane/.venv/bin/python vogon-rust/validation/audio/run.py --stage runtime
```
