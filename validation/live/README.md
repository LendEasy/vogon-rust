# Live generated-audio acceptance

This migration harness retains its original sibling-repository layout. See
[validation prerequisites](../README.md). Raw run output and generated audio are
local and ignored; the public [report](REPORT.md) summarizes earlier runs.
For a standalone demo application use the
[echo Turn service](../../examples/echo-turn-service/README.md).

## Python ladder Turn service smoke

`ladder_smoke.py` runs the real Python `app.turns.main` service in a separate
process behind the Rust production router/Driver/Session. It reuses the existing
`promise` script from `data-plane/tests/gateway_e2e/ladders_duplex_call.py` verbatim:
identity, availability, DOB/ZIP verification, self-managed payment promise and
closure. The fixture contains a fictional account. It makes live Deepgram and
application reasoning calls, but places no telephone call and runs no GPT Live.

```sh
data-plane/.venv/bin/python vogon-rust/validation/live/ladder_smoke.py \
  --live --env data-plane/.env \
  --out vogon-rust/validation/live/results/python-ladder-UNIQUE_RUN
```

Build `service-live-host` first. The output directory must not already exist.
The phone endpoint sends real-time 20 ms mu-law audio, receives actual streamed
TTS, and acknowledges marks after simulated playout. The observation proxies
forward real HTTP/provider responses unchanged. The Python launcher primes the
existing demo configuration and captures successful checkpoint commits without
replacing reasoning or checkpoint behavior. It seeds the normal application call
record before the call. SQLite, storage and a real Redis child process are isolated
to the run; Redis uses `REDIS_SERVER_BIN`, PATH, or the existing workspace build.
Rust gateway admission uses its memory store. Front fillers and ambience are disabled; EOT
settings inherit the supplied dotenv. No echo injection is performed here.

Assertions require every reply to produce audio and terminal marks, the promise
and closure ladders to be reached, five final caller utterances, one natural
EndCall, no remaining call/socket owners/checkpoints, a completed database record,
one queued post-call job, graceful process shutdown, and independent
STT agreement with each returned speech segment (normalized WER <= 0.25).
Uvicorn's SIGTERM exit convention is accepted only when its lifespan cleanup
record proves call cleanup; queued post-call worker execution is outside this smoke.
Artifacts include `summary.json`, `latency-breakdown.json`, `turn-api.json`, `provider-events.json`,
per-commit checkpoints, audio WAVs, service logs and source/binary hashes. All
run artifacts remain local and ignored.

Latency separates STT endpointing, Python Turn HTTP time, TTS first audio, and the
three observed forwarding intervals through Rust. The latter include local proxy,
socket and scheduler overhead; they are not isolated Rust CPU measurements or a
capacity benchmark. Each conversation has only five sequential turns.

Speculative execution is verified from committed `end_of_turn` ledger rows,
including `reused_speculative`, rather than inferred from eager event delivery.
The harness also requires eager HTTP acknowledgements to contain no speech.
Source hashes matter: the Python implementation changed during the local runs
below, so the first run's lack of engine speculation is not the latest behavior.

### Local evidence, 2026-10-19

| Run | Outcome |
|---|---|
| `python-ladder-20261019-01` | All five conversation turns and six audio checks passed. Overall harness failed because of Uvicorn's SIGTERM convention and incomplete intake/cleanup fixtures. Earlier Python engine did not speculate. |
| `python-ladder-20261019-02` | Preflight failure: Redis was not on PATH; no service call ran. The harness now finds the existing workspace binary. |
| `python-ladder-20261019-03` | Deployment EOT settings split “Yes, now is a good time” into distinct final transcripts “Yes.” and “now is a good time.” The first reply was interrupted. Cleanup passed with real Redis and the seeded call record. |
| `python-ladder-20261019-04` | Controlled EOT smoke passed: five finals, six completed audio segments, natural `caller_farewell`, no leftover call/socket/checkpoint state, database state `done`, one queued post-call job and graceful shutdown. |

The baseline phone query was eager `0.7`, final `0.85`, timeout `1400 ms`.
The controlled run uses `--controlled-eot`: eager `0.3`, final `1.0`, timeout
`3000 ms`. This override does not change the deployment dotenv.

Between baseline and controlled runs, the externally modified Python `api.py`
and `engine_host.py` gained engine speculation. In the controlled run, **all five
committed turns reused speculative results**; eleven eager acknowledgements
contained no speech, and six resumed events were observed. Independent output
transcription WER was 0–4.17%. Rust produced an 8 kHz stereo recording of 108.1 s.
The first run remains evidence of the earlier source version, not a regression
claim against the later implementation.

| Observed mean, five turns | Baseline | Controlled EOT / updated Python |
|---|---:|---:|
| Speech end to first returned audio | 2022 ms | 3422 ms |
| STT endpointing | 602 ms | 3160 ms |
| Final Turn HTTP request/response | 1262 ms | 128 ms |
| TTS request to first provider audio | 155 ms | 132 ms |
| Three Rust forwarding boundaries combined | 2.58 ms | 2.37 ms |

The 128 ms is final-result reuse/commit time: model reasoning ran earlier under
eager execution. Both Python code and EOT configuration changed, so this is not
an isolated speedup comparison. Forwarding intervals include loopback observer
overhead and do not measure total Rust CPU or pre-STT audio processing.

Local `python-ladder-20261019-comparison.json` preserves every run's outcome.
Run 04's `verified-report.json` and `speculation-verification.json` correct the
original harness's stale assumption about Python speculation using committed
ledger evidence; the original `summary.json` remains unchanged. Deployment EOT
reliability and broad speculative race/load acceptance remain open.

## Echo acceptance suite

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
