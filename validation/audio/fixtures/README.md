# Audio fixtures

Both files are copied byte-for-byte from
[py-webrtcvad 2.0.10](https://github.com/wiseman/py-webrtcvad/tree/2.0.10),
whose project license is included as `LICENSE`.

| Local file | Upstream file | Format | SHA-256 |
|---|---|---|---|
| `webrtc-speech.wav` | `leak-test.wav` | mono PCM16, 8 kHz, 9.375 seconds | `0e415756dd194d0287ac7a949162bafb221e833591c7ba20c33d0eadd3cf7ce9` |
| `webrtc-vad.raw` | `test-audio.raw` | mono PCM16, 8 kHz | `3dbb730b90e0266d78d4fe03f00aba37a83780a6a5240d9084b5c01266194c1e` |

Archive SHA-256:
`003c7681f7616221f6c4cfb748c9bd2b95a8f5367f16d276c7e984786e510c62`.

The acoustic test normalizes the WAV to RMS 3000, clips only to the documented
PCM range, and repeats it to fill 18 seconds. The fixture itself is unchanged.
The raw VAD fixture retains upstream's four fixed expected decision sequences,
providing an oracle independent of executing the Python extension.
Neither fixture is a recording of a production/customer call. This small
corpus adds recorded speech coverage; it does not represent every speaker,
acoustic environment or provider path.
