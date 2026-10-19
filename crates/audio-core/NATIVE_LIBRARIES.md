# Native library boundary

The Rust audio path now calls SpeexDSP and WebRTC VAD through C APIs. NumPy and
audioop remain embedded through PyO3. The public Python adapters exist for offline
validation; they do not turn the native DSP handles back into Python objects.

| Dependency | Integration decision | Reason |
|---|---|---|
| SpeexDSP | Direct system `libspeexdsp` | Stable C entry points for creation, rate configuration, cancellation and destruction; same shared library as the Python oracle |
| WebRTC VAD | Direct static C build of the source shipped in py-webrtcvad 2.0.10 | Preserves the existing VAD revision; no Python wrapper or capsule needed |
| NumPy | Retain Python API | Explicit migration decision; FFT, least squares and array calculations remain the reference implementations |
| audioop | Retain Python API | CPython extension, not a separately supported native library API; codec/resampling replacements would change the chosen implementation |
| Python `math` log10/sqrt | Native Rust scalar operations | No Python objects needed; preserve domain errors and verify exact downstream state parity |
| Python `wave` | Retain asset-loader API | Python file-format reader, not a native DSP library; off the per-frame path. Replacing the parser is separate asset-loading work |
| Python set/round | Retain narrowly in AEC alignment | Preserves reference candidate ordering in score ties and diagnostic rounding; these are compatibility operations, not external DSP bindings |

The [Speex wrapper source](https://github.com/xiongyihui/speexdsp-python/blob/master/src/echo_canceller.cpp)
uses `speex_echo_state_init_mc`, sets sample rate, and calls
`speex_echo_cancellation`. Its preprocessing calls are commented out. Rust uses
exactly that configuration: one microphone/speaker, 160 samples, 3200-sample
tail, 8 kHz, no preprocessor. The installed extension's linked symbols and exact
output/state comparison confirm this boundary in the tested environment.

The [WebRTC C API and sources](vendor/README.md) are downloaded separately with
verified provenance and preserved licenses. Rust owns initialization, mode selection and cleanup.
Its safe wrapper checks rate/length before C access and copies bytes into aligned
PCM16 storage. Mutable ownership prevents concurrent use of one adaptive state.
The Python-facing adapter preserves the invalid-frame error name/message and
`confirm_frame`'s false-on-error policy without importing `webrtcvad`.

[audioop's API](https://docs.python.org/3.10/library/audioop.html) accepts Python
bytes and returns Python objects, including opaque resampling state. Calling
CPython extension internals would still depend on Python and would rely on
unsupported implementation details. Extracting its C algorithms or switching
to another resampler is not part of this binding change. The installed system
WebRTC audio-processing library is also not used as an interchangeable VAD:
its code version is not pinned to the gateway's VAD.

## Build and lifecycle

Currently supported: native Linux builds, validated on x86-64. Install a C
compiler, `ar`, `pkg-config`, Python development/linking support and
`libspeexdsp-dev`. `CC` and `AR` may select tool executables. Cross compilation
and other operating systems fail explicitly until their toolchain configuration
is supplied. Rust dependencies remain locked. Run `python tools/fetch_webrtcvad.py` once
from the repository root (or use `--archive` for offline setup) before compiling.
VAD compilation itself needs no network fetch.

SpeexDSP is dynamically linked; deployment must ship the matching library.
Validation records its actual shared-object hash and package version. WebRTC VAD
is linked statically with hidden symbols, keeping the separately loaded Python
oracle independent. Native handles are freed by Rust Drop, even without the
Python interpreter; explicit AEC close remains idempotent. Initialization errors
are propagated, with no fallback to the removed Python bindings.

NumPy/audioop still need CPython. The per-call worker currently holds the GIL
around the complete audio operation, so removing DSP wrappers alone is not a
claim of GIL-free streaming or increased throughput.

## Acceptance checks

Run the audio validation `--stage all` and Rust audio core/runtime unit tests.
The acoustic tests compare native cancellation output and state byte-for-byte
with the existing Python implementation. Runtime tests include gain/delay
changes, a recorded speech fixture, double talk and mixed/resampled playback.
Quality thresholds are independent of parity, with transition leakage reported
separately. Negative controls disable AEC or supply an unrelated reference and
must fail the quality target. A separate fresh-process test blocks imports of
`speexdsp`, `_webrtcvad` and `webrtcvad` while constructing and processing direct
owners and a complete audio runtime.

Known scope limits: the six existing cold-start failures remain; adaptation to
changing paths can leak echo; this is an offline audio-runtime integration test,
not a complete Rust network gateway or a live-call acceptance test. No wrapper
or end-to-end performance measurements were made.
