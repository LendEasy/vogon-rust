# Live generated-audio acceptance

This migration harness retains its original sibling-repository layout. See
[validation prerequisites](../README.md). Raw run output and generated audio are
local and ignored; the public [report](REPORT.md) summarizes earlier runs.
For a standalone demo application use the
[echo Turn service](../../examples/echo-turn-service/README.md).

This opt-in harness runs generated speech through the production Rust service
router, native Driver/Session, live Deepgram Flux STT, a local echo Turn API and
live Deepgram TTS. A separate STT request verifies the audio captured at the caller.
No GPT Live, application model, transfer or external telephone calls are made.
The phone caller implements Twilio media/mark/clear messages locally.

```text
Live TTS --> cached synthetic caller PCM --> 20 ms caller send task
                                                   |
                          browser PCM or phone mu-law WebSocket
                                                   |
                                      Rust service / Driver / Session
                                                   |
                       phone AEC + VAD ----------> live Flux STT
                                                   |
                          eager / resumed / final callbacks
                                                   |
                             local echo Turn API (async HTTP)
                                                   |
                                            live streaming TTS
                                                   |
                                Rust playback / caller socket
                                                   |
                                   independent caller receive task
                                      |                   |
                                captured WAV       phone marks / echo injection
                                      |
                            independent live STT --> transcript checks
```

The provider proxy forwards real bytes in both directions and records event
timing. Selected cases delay, duplicate or close those real streams; they do not
substitute synthesized provider callbacks. HTTP gates let eager/final requests
remain outstanding while the caller continues sending audio.

The credentials are read from an existing dotenv and never copied into reports.
The current workspace has `data-plane/.env`; `voice-gateway/.env` is absent.
A small validation-only Rust launcher injects observation proxy URLs and preseeded
phone records into the same service implementation. It does not add test routes
to the production gateway. The original production binary's process/route parity
remains covered by the module-10 gate.

## Run

From the monorepo root, build `service-live-host` with the existing toolchain and
`PYO3_PYTHON=data-plane/.venv/bin/python`, then:

```sh
data-plane/.venv/bin/python vogon-rust/validation/live/run.py \
  --live --cases all --env data-plane/.env \
  --out vogon-rust/validation/live/results/RUN_NAME
```

This command makes billable live STT/TTS requests. `--cases` also accepts a
comma-separated selection; inspect `run.py` for scenario names. Shutdown runs last
in the full suite because it signals the actual host process. Raw generated audio
is cached by voice/sample rate/text hash with provenance metadata, then replayed
at real-time 20 ms cadence. Reusing input audio does not replace the gateway's live
STT or TTS connections.

The initial eager/resumed suite deliberately sets a 3-second final EOT timeout and
threshold 1.0 to make continuation windows reproducible. Its latency is not a
production-default latency claim. Fixed audio/state test limits and fault actions
are saved in the fixture and provider-event logs.

## Responsibilities

- `infra.py`: live audio generator/verifier, echo endpoint, pass-through provider
  proxy and caller with independent send/receive tasks. Caller audio and proxy
  audio are real provider bytes; event replays are explicitly logged.
- `run.py`: host lifecycle, selection, bounded scenario execution and reports;
  basic voice, multi-turn and eager/resumed cases.
- `scenarios.py`: timers, fault/replay injection, full duplex, concurrent calls,
  phone echo/double-talk, admission and cleanup checks.
- `test_oracle.py`: offline checks that transcript normalization preserves values,
  digit order, names, weekdays and negation.
- `report.py`: aggregate saved results by planned scenario, retaining failed
  prerequisites and linking superseded runs rather than erasing their evidence.
- `crates/service/src/bin/service-live-host.rs`: validation launcher using the
  production service with injected endpoints and known phone identities.

The echo service creates speculative state on eager EOT, discards it on resumed
speech, reuses matching final text and produces one committed reply on final.
It can gate HTTP responses without blocking new socket audio or other calls.
It stores idempotent responses by request ID so HTTP retries can be distinguished
from new turns.

## Evidence and limits

Each run saves scenario results, Turn API exchanges, provider/control timestamps,
caller input/output WAVs, framing and audio chunk evidence. Test audio/transcripts
contain only synthetic fixtures. Secrets and authenticated connection URLs are
excluded. Output WER is checked against the exact text requested for synthesis;
input recognition is checked separately. Numeric words and numeric/currency
formatting are canonicalized without removing the numeric value or currency.

Fault tests interrupt or delay actual live connections or replay actual received
provider events. They are distinguished from unmodified provider observations.
A scenario requiring eager/resumed events fails if those events were not observed;
it never passes solely because its input was sent.

Phone echo energy ratios compare window-average input and forwarded-STT energy;
they are diagnostic measurements, not calibrated ERLE or physical-room acceptance.
Generated-speech and simulated-network checks cannot prove microphone quality,
telephone-network reliability or long-duration production capacity.

See [the acceptance report](REPORT.md) for the saved run matrix, deployment EOT
comparison, corrections and remaining gaps. The final phone rerun validates the
restricted media/mark/clear protocol, four concurrent native audio workers and
repeated phone teardown. Its double-talk check also requires caller transcript
WER <= 0.25; recognizing one expected keyword is insufficient. Provider timing
varies between runs, so a later acoustic pass does not erase an earlier failure.

Regenerate the report after the named runs finish:

```sh
data-plane/.venv/bin/python vogon-rust/validation/live/report.py
data-plane/.venv/bin/python -m unittest discover \
  -s vogon-rust/validation/live -p test_oracle.py
```
