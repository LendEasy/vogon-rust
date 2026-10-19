# Rust audio core

Implements the first three migration stages against the existing Python gateway:
AEC, ambience mixing/source generation, and encoding/framing/sample arithmetic.
The production Python gateway is unchanged. This crate does not yet implement
websockets, provider clients, playout acknowledgements or the call controller.

## Ownership and library calls

| Component | Rust owns | Retained library operations |
|---|---|---|
| `aec.rs` | Reference FIFO, history, GCC windows, delay/slip decisions, filter lifetime, realignment and ERLE counters | NumPy FFT, arrays, least squares and matrix arithmetic; SpeexDSP cancellation; audioop PCM conversion/RMS |
| `mixer.rs` | PCM mixing, clipping, room gain ramps, ducking, typing fades, one-shot cursors | None on the mixing path |
| `sources.rs` | Deterministic generator state, low-pass filters, crossfades, sound banks and asset selection | Python `wave` reader and audioop width/channel/rate conversion |
| `native.rs` | Aligned PCM buffers, validated C inputs, native handle allocation/destruction | Direct SpeexDSP and pinned WebRTC VAD C calls |
| `formats.rs` | Stream buffers, PCM-to-phone conversion state, phone/browser packet boundaries | audioop encoding, decoding, RMS, width/channel conversion and stateful resampling; WebRTC VAD |

AEC calls the installed NumPy implementation through PyO3; no Rust FFT or
least-squares replacement is used. Its NumPy arrays stay in Python, so it does
not need a second typed-array view from the `numpy` crate. The earlier typed
NumPy bridge experiment remains in `prototypes/numpy-bridge`.

`AmbienceMixer::mix` and source synthesis execute native Rust arithmetic.
Direct SpeexDSP and WebRTC VAD owners in `native.rs` need no interpreter.
The complete AEC/framing path still attaches to Python for NumPy and audioop.
See [native library analysis, build dependencies and lifecycle](NATIVE_LIBRARIES.md).
The validation executable registers Rust classes in an embedded Python module;
only the validation scripts import the Python gateway as a comparison oracle.

## Stream contracts

- Use one AEC processor, resampler, phone framer and VAD instance per call/stream.
  Keep frame order serialized. Initialize CPython before using the library
  methods. Native handles free themselves through Rust Drop without Python;
  close the complete audio runtime before interpreter shutdown.
- AEC consumes one queued reference per inbound frame. Reference and near-end
  frames are decoded and padded/truncated to 160 samples. Clearing playout
  discards the reference FIFO and resets alignment slip, preserving the other
  history as in Python. `close()` is idempotent; processing after close fails.
- `PhoneFramer("mulaw")` buffers provider bytes into 160-byte frames.
  `PhoneFramer("linear16")` retains audioop's 24 kHz to 8 kHz filter state and
  encodes to mu-law. The legacy path drops an odd final PCM byte at each chunk
  boundary, matching the current transport.
- `PhoneFramer.push(chunk, mixer)` mixes full frames in linear PCM and re-encodes
  them. `flush_tail()` returns remaining bytes without padding or mixing and
  keeps resampler state. Call `reset()` on cancellation or before a new utterance
  to discard both buffered bytes and resampler history. Sentence boundaries can
  flush tails without resetting a continuing stream.
- `BrowserFramer` emits the whole accumulation once it reaches 8192 bytes,
  including overshoot. Packets contain a little-endian u32 UTF-8 ID byte length,
  the UTF-8 ID and PCM bytes. `reset()` drops pending data.
- `Vad.is_speech()` translates native VAD errors at the validation API boundary.
  `Vad.confirm_frame()` returns false on a frame error, matching the local VAD
  confirmation callback. Native allocation/configuration errors still fail initialization.
- Pass `assets_dir` explicitly to load WAV assets; otherwise `build_mixer` uses
  synthetic sources. This crate does not implicitly locate Python repository
  assets. Sources must have the mixer's rate. Invalid rates, non-finite mixer
  settings and zero/negative fade durations are rejected early.

Framers produce bytes, not delivery confirmations. The implemented
[transport adapter](../transport/README.md) feeds AEC after successful caller
sends, clears references after caller-buffer clear, and owns pacing, marks and
cancellation. Provider Clear is a separate TTS protocol operation.
AEC retains diagnostic state and bounded errors but does not yet emit the
Python gateway's operational log messages/metrics.

## Validation and limits

See [validation instructions and saved results](../../validation/audio/README.md).
The tested environment is Linux x86-64, CPython 3.10.12, NumPy 2.2.6,
vendored WebRTC VAD from py-webrtcvad 2.0.10, system SpeexDSP 1.2rc1 and
Python's audioop. The Python SpeexDSP/VAD extensions are validation oracles only.
`audioop` must be available in the selected Python runtime; the crate does not
substitute another implementation if it is missing. Cargo pins PyO3 0.30.1.

The existing six expected AEC cold-start failures remain: echo can trigger the
barge threshold before initial delay estimation. Passing parity preserves this
limitation. Synthetic acoustic checks are not a live-call acceptance test.
A small recorded-speech fixture now exercises changing echo paths through the
Rust runtime. Local async transport/provider integration is now tested; production
call replay, deployment packaging and scheduling under load remain acceptance
work. No new performance claim is made.
