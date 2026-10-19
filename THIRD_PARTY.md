# Third-party code and libraries

Original Vogon Rust code is Apache-2.0. Dependencies and fixtures retain their
own licenses; the project license does not replace them.

- Python reference implementation: [LendEasy/vogon](https://github.com/LendEasy/vogon),
  Apache-2.0. Contract/configuration declarations and front constants are derived
  from the reference; generator provenance is in `tools/generated-sources.json`.
- WebRTC VAD C code: py-webrtcvad 2.0.10, fetched separately with a pinned hash.
  See [provenance](crates/audio-core/vendor/README.md) and the complete upstream
  [license notice](crates/audio-core/WEBRTC_LICENSE), including WebRTC's BSD terms.
- Recorded test fixtures: unchanged py-webrtcvad files, with their own
  [provenance and license](validation/audio/fixtures/README.md).
- SpeexDSP, CPython/audioop and NumPy: externally installed dependencies.
  Preserve their distribution notices when packaging a runtime image.
- Rust dependencies: versions and checksums are in `Cargo.lock`; their individual
  license metadata remains authoritative for redistributing those components.

No vendored WebRTC sources, private application code or downloaded build binaries
are included in the repository history.
