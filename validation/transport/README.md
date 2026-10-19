# Streaming, transport and clock validation

Run from the monorepo root with the Python environment used for the reference:

```bash
data-plane/.venv/bin/python vogon-rust/validation/transport/run.py --sockets
```

The runner builds offline with the locked dependencies, executes actual Python
implementations as differential oracles, and records source/binary SHA-256 hashes.
`--sockets` enables local loopback WebSocket integration tests; it requires local
network permission. Omitting it still runs all deterministic differential checks.
GPT Live tests and external provider calls are excluded in both cases.

| Gate | Coverage |
|---|---|
| `../providers/parity.py` | 350 Flux traces / 9,780 callback-state snapshots; 300 resumed merges; 100 RIFF cases; 120 streaming TTS segment traces. |
| `parity.py` | 100 clock traces / 15,100 snapshots; 100 browser framing cases; 600 browser and 600 phone callbacks against the real Python transport closures. |
| `audio_checks.py` | 960 phone input/AEC/gate frames and 960 gap-mixer frames, including absent AEC and both gate confirmation modes. |
| Rust socket/integration tests | 21 passed: 7 provider, 12 transport and 2 audio-runtime tests. Local provider streaming, ping handling, disconnects, cancellation drain, callback ordering, priority clear/AEC ordering, mark send failures, stalled-provider cancellation, response-future drop, output reset and reconnect/retry accounting. Two GPT Live tests excluded. |

Saved results: [provider parity](../providers/results/parity.json),
[clock/transport parity](results/parity.json), [audio wiring](results/audio.json),
[native tests](results/native.json), and [source/binary manifest](results/manifest.json).

The phone callback oracle here covers idle/no-playback routing. Protected/absorb,
active overlap and stale-classifier behavior have the existing module-4 differential
suite plus native integration invariants; this is not exhaustive full-transport
interleaving parity.

The authorized external Deepgram smoke has a separate saved
[report](../providers/results/live.json). It generated two short synthetic TTS
sentences, verified multiple streamed chunks, then paced their mu-law audio into
Flux. The continuation scenario observed eager EOT, resumed speech and final
commit. It deliberately adjusted thresholds to reliably exercise resumption; that
configuration is recorded in the report and is not a production default change.
No wrapper throughput measurements or performance claims are made.

The optional GPT Live test is deferred at the user's request. The default runner
will not exercise it or consume API credits. Saved reports from earlier attempts
are historical evidence, not acceptance of that provider path.

For shared audio and prior module regressions:

```bash
data-plane/.venv/bin/python vogon-rust/validation/audio/run.py --stage all
data-plane/.venv/bin/python vogon-rust/validation/modules/run.py --stage all
```
