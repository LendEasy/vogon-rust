# Audio core correctness checks

Run from the monorepo root with the Python environment containing the gateway's
dependencies, NumPy, the Python SpeexDSP/WebRTC VAD oracles, audioop and pytest.
Native builds also require a C compiler, `ar`, `pkg-config` and `libspeexdsp-dev`
(see [library analysis](../../crates/audio-core/NATIVE_LIBRARIES.md)):

```sh
data-plane/.venv/bin/python vogon-rust/validation/audio/run.py --stage all
```

The runner builds `vogon-audio-core` in release mode with Cargo's locked, offline
dependency resolution. It uses Cargo from PATH, or the prototype toolchain at
`/tmp/vogon-rust-toolchain`. It embeds the invoking Python interpreter and runs
each stage in a separate process.
`--stage aec|mixer|formats|runtime|echo_runtime|native` selects a stage;
`--skip-build` reuses the binary. Running replaces that stage's saved results.

## Results

| Stage | Result | Comparison coverage |
|---|---|---|
| AEC | 81 existing tests passed; 6 expected failures | 46,376 byte-identical processed frames; state compared after reference feeds, processing, clears and ERLE resets |
| Mixer and sources | 20 existing tests passed; 257 differential cases passed | 24,000 frames / 5,928,288 samples; generated clips, playback cursors, clipping, ducking, fades, WAV conversion and asset selection |
| Encoding and framing | 91 cases passed | 444 library/result/error comparisons; 4,000 resampler chunks; 3,000 VAD frames; 2,170 phone frames; 80 browser packets |
| Audio runtime | Four concurrent calls matched | 880 inbound frames and all generated output chunks; state isolation, startup failure and shutdown draining |
| Changing echo through runtime | 11 acoustic scenarios, two negative controls and one no-echo double-talk control | 12,600 byte-identical inbound frames; outbound bytes, per-event state, RMS and VAD decisions match |
| Native binding isolation | Passed with Python SpeexDSP/VAD imports blocked | Direct owners and runtime; cleanup, invalid frame behavior, hidden static VAD symbols |

These are different kinds of counts, not an additive test total.

AEC checks pair Python and Rust processors inside the existing acoustic suite.
Every processed output and relevant stream state must match exactly, even in
tests marked as expected failures. Acoustic requirements include delay estimates,
at least 20 dB converged echo reduction in the designated cases, double-talk
preservation, clear behavior and realignment after missing/stalled frames.
One existing test checks Python transport clear wiring rather than Rust AEC
processing. Two extra checks cover interleaved calls/frame lengths and handle
closure. The six expected failures describe the existing cold-start limitation
before the initial delay estimate, not accepted Rust parity mismatches.

Mixer checks use four rates (8/16/24/48 kHz), multiple seeds and layer/configuration
combinations. They compare every output byte and cursor/gain state. WAV checks
exercise widths 1–4, mono/stereo, and rate conversion against Python.

Encoding checks exhaust all 65,536 signed 16-bit input values and 256 mu-law
codes. Resampler checks compare both output and opaque filter state, including
two interleaved streams, empty chunks, reset and invalid-frame errors. VAD checks
compare sequential decisions at all four modes and supported sample rates with
10/20/30 ms frames. Both speech and non-speech decisions occur in the corpus.
The upstream recorded VAD fixture also checks fixed expected decision sequences
in all four modes, independently of the Python extension.

Phone comparisons execute the actual Python `TwilioTransport._do_tts_stream`
with local fake TTS chunks and captured media output, including ambience and
cancellation. Browser comparisons execute `VoiceTransport.send_response` with
captured binary output and UTF-8 IDs. External clients/sends are stubbed; these
are offline framing checks, not network or provider acceptance tests. Sentence
tail behavior is checked separately. Sentence pacing has its own playback-module
validation and is not exercised by this audio runner.

## Changing-path acoustic integration

`echo_runtime_checks.py` runs 18-second cases through the real Rust audio runtime:
TTS chunk framing (25-frame bursts), optional audioop resampling and ambience,
explicit reference dispatch, AEC, PCM/RMS and native VAD. The Python AEC and VAD
are independent reference instances. Every inbound result and relevant AEC
state matches; outbound framing/mixing bytes are compared as well. Generating
TTS frames is checked not to feed the reference until dispatch.

- Fixed 120 ms / gain 0.3 baseline, plus recorded speech.
- Gain steps 0.1 → 0.5 at 6 seconds; gain ramps over seconds 6–10.
- Physical delay steps 120 → 220 ms and 200 → 80 ms at 6 seconds.
- Fractional delay 137.375 ms, including a fractional sample.
- Delay drifting 120 → 140 ms over seconds 6–10, using sample interpolation.
- Recorded speech with gain changes, simultaneous caller speech, and a path
  containing 24-to-8 kHz resampling and ambience mixing.

Fixtures are pinned and attributed in [fixtures/README.md](fixtures/README.md).
They add one short speech recording, not a broad speaker/room corpus.

Every normal case must recover to at least 20 dB echo reduction in seconds
14–18. Actual recovered reductions are approximately 26–33 dB. Double talk
must retain caller gain within 1 dB and correlation within 5% of a no-echo
control. Disabled AEC and an unrelated reference must yield less than 10 dB
reduction; both negative controls detect the broken integration.

**Transition leakage is a separate reported limitation, not a passing quality
claim.** Six cases fall below 15 dB reduction during seconds 6–9. Continuous
delay drift measures about −1.74 dB there, then recovers to about 26 dB. Python
and Rust agree exactly. The recovered-quality gate allows adaptation; it does
not assert acceptable barge-in behavior during every path transition. The six
existing cold-start expected failures also remain unchanged. Network transport,
full barge-controller wiring, multipath and nonlinear echo are outside this suite.

`native_checks.py` uses a fresh interpreter that rejects Python SpeexDSP/VAD
imports. Native owners and runtime must still work. Rust unit tests additionally
exercise repeated native allocation/destruction and invalid buffer/rate rejection
without initializing Python; these are lifecycle/bounds checks, not a leak proof.

Run the three Rust lifecycle/queue tests with the same Python selected at build:

```sh
PYO3_PYTHON=/mnt/disk2/codelexin/data-plane/.venv/bin/python cargo test --release --offline --locked --manifest-path vogon-rust/Cargo.toml -p vogon-audio-core -p vogon-audio-runtime --lib
```

## Artifacts

- [AEC results](results/aec.json) and [pytest XML](results/aec.xml)
- [Mixer results](results/mixer.json) and [pytest XML](results/mixer.xml)
- [Encoding/framing results](results/formats.json)
- [Audio runtime results](results/runtime.json)
- [Changing echo runtime results](results/echo_runtime.json)
- [Native binding isolation](results/native.json)
- [Environment and source hashes](results/environment.json)

The environment artifact lists the stages in the most recent invocation,
toolchain/runtime details, retained module paths/file hashes, source hashes and
the binary hash, native SpeexDSP shared-object hash, C compiler and all vendored
C/fixture hashes. A Python package initializer hash does not fingerprint all of
NumPy or its linked BLAS libraries; use the earlier numerical prototype's
environment report for its benchmarking provenance.

This runner measures correctness only. It does not time wrappers or claim
performance improvements. Existing numerical-only benchmark artifacts remain
under `prototypes/` and have not been replaced by these checks.
