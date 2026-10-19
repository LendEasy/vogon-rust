# Native source setup

WebRTC VAD sources remain ignored by Git. Before building the audio crates, run
`python tools/fetch_webrtcvad.py` from the repository root. Cargo never downloads
native sources implicitly. For offline setup, supply `--archive PATH`.

The source is unchanged `cbits/webrtc/` from py-webrtcvad **2.0.10**:
https://github.com/wiseman/py-webrtcvad/tree/2.0.10

Archive SHA-256:
`003c7681f7616221f6c4cfb748c9bd2b95a8f5367f16d276c7e984786e510c62`.

The fetcher verifies that hash and preserves the upstream license. The CPython
extension is excluded. `build.rs` compiles the C signal-processing and VAD code
with `WEBRTC_POSIX`; SpeexDSP is provided by the system library. The checked-in
[license notice](../WEBRTC_LICENSE) also covers the separately downloaded source.

This is the same source revision used in the migration tests. Upgrades require
separate parity and acoustic acceptance; publication does not change its behavior.
