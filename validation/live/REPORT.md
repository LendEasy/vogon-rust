# Live generated-audio results

Gateway uses native Rust service/transport/orchestration and real Deepgram STT/TTS. The local echo Turn service performs no model calls. Phone media is a local Twilio-protocol simulation, not a telephone-network call. GPT Live was not run.

The generated-audio conversation path works. Full live acceptance remains incomplete: the initial acoustic run produced a false caller turn, the deployment EOT configuration did not expose the required continuation window, and browser silence timers did not fire. The matrix below describes the latest tests, not an erasure of those findings.

## Planned suite after integration fixes

| # | Scenario | Result |
|---|---|---|
| 1 | Startup/authentication | PASS |
| 2 | Text baseline | PASS |
| 3 | Spoken round trip | PASS |
| 4 | Five-turn conversation | PASS |
| 5 | Streaming delivery | PASS |
| 6 | Content correctness | PASS |
| 7 | Long utterance/pauses | PASS |
| 8 | Silence ladder (phone) | PASS |
| 9 | Matching eager/final | PASS |
| 10 | Changed final transcript | PASS |
| 11 | Speech resumed after eager | PASS |
| 12 | Late eager response | PASS |
| 13 | Duplicate provider events | PASS |
| 14 | Input during blocked application | PASS |
| 15 | Barge-in | PASS |
| 16 | Late audio after cancellation | PASS |
| 17 | Parallel calls and timer isolation | PASS |
| 18 | Stalled peer isolation | PASS |
| 19 | Phone media/marks | PASS |
| 20 | Echo and double-talk | PASS |
| 21 | Provider/application faults | PASS |
| 22 | Disconnect/SIGTERM cleanup | PASS |
| 23 | Admission/malformed input | PASS |
| 24 | Repeated lifecycle | PASS |

Planned scenarios: 24 pass, 0 fail, 0 not run. Scenario 17 requires both parallel conversation and live timer-isolation checks to pass.

The phone rerun replaces matching phone scenario results from full-02 and adds phone worker concurrency/cleanup checks; all original evidence remains intact. Its double-talk check requires caller transcript WER <= 0.25. The earlier full-02 check only required the keyword Monday and accepted an agent-speech prefix; that weaker result is not clean-transcript acceptance.

The controlled eager/resumed run uses final EOT confidence 1.0 and a 3000 ms timeout to expose continuation windows. It does not measure deployment-default latency.

Speech correctness uses normalized word error rate (usually <= 0.25), plus scenario-specific content/state assertions. It is not exact transcription: for example, the full-02 multi-turn return-audio recognizer heard “gate” instead of “date” (WER 0.111). Input recognition and captured-output recognition are recorded separately.

## Existing dotenv EOT settings

| Scenario | Result | Detail |
|---|---|---|
| basic-voice | PASSED | First returned audio 694 ms after caller speech ended; output WER 0.000; 14 audio chunks. |
| multi-turn-content | PASSED |  |
| matching-eager | PASSED |  |
| resumed-final | FAILED | timeout: TTS end; captured trace has two finalized utterances and no TurnResumed. The continuation window was not exercised with this configuration. |

Observed STT query: `{"eager_eot_threshold": "0.6", "encoding": "linear16", "eot_threshold": "0.6", "eot_timeout_ms": "5000", "model": "flux-general-en", "sample_rate": "16000"}`.

## Echo injection

| Delay | Gain | Double-talk | Result | Input/STT window energy ratio |
|---|---|---|---|---|
| 60 ms | 0.2 | False | passed: no false caller turn | 12.78 dB |
| 180 ms | 0.5 | False | passed: no false caller turn | 15.26 dB |
| 300 ms | 0.5 | False | passed: no false caller turn | 16.39 dB |
| 120 ms | 0.3 | True | passed: caller WER 0.000 | 0.11 dB |

Energy ratios are window diagnostics, not calibrated ERLE. Pure echo must not create a new caller turn; double-talk must preserve the new caller content. No failed acoustic case is converted into an expected pass.

The initial full-01 run failed pure echo at 60 ms / 0.2 gain. Later runs varied despite fixed lag/gain: generated provider output and scheduling are live. No DSP behavior was changed during this task, so later passes do not establish that the original echo leakage is resolved.

## Defects exposed and corrected

- Provider-sequenced STT replay previously acquired new transport identities and could commit duplicate final turns. A connection-local sequence high-water mark now rejects replay before transcript callbacks; later genuinely repeated words remain valid.
- The host previously passed wall-clock epoch into the Turn client while transport events were already call-relative, producing negative event times. The host now uses zero offset for its per-call Clock. Native and live checks require nonnegative call-relative times.
- Browser control notifications were reaching phone media sockets. The transport now suppresses them for phone mode; a strict live caller and native socket test accept only the supported events. See [Twilio message types](https://www.twilio.com/docs/voice/media-streams/websocket-messages#send-websocket-messages-to-twilio).
- The test oracle initially treated “fifty dollars” and “$50” as different content. Numeric/currency formatting now canonicalizes without removing the value, unit, weekday or names; separate oracle tests protect these distinctions.

## Remaining limits

- Browser silence does not share the phone playout-receipt lifecycle; the initial browser silence attempt failed to produce the requested ladder events. Engine::begin returns before phone clock transitions in browser mode, and phone marks close that lifecycle. Define browser audible-completion semantics and wire its clock transitions before accepting browser silence behavior. Phone ladder acceptance is reported separately above.
- Eager/resumed events depend on provider decisions and configuration. Missing prerequisites are failures in these reports, not silently skipped successes.
- Six repeated browser calls, four repeated phone calls and up to four concurrent calls exercise cleanup/isolation, not long-duration leak freedom or production capacity.
- Echo tests disable the optional remote semantic judge, exposing DSP/floor behavior directly. They do not establish acceptance of every optional classifier configuration.
- Front model calls, transfers, recording generation and deployment packaging remain outside this run. Test WAV capture belongs to the harness, not module 12.

## Offline regression after fixes

- Service: 25 native tests, 228 direct comparisons and 11 Python reference tests passed; one Python reference skipped because fakeredis is absent. Native Redis tests use a private real Redis process.
- Orchestration/transport/providers: 134 native tests passed; 34 Python reference tests passed with two existing expected failures. Two GPT Live-specific tests were filtered out of this gate.
- Rust formatting, scoped Clippy and rustdoc gates passed. Three transcript-oracle unit tests passed.

## Evidence availability

This is a historical summary of the local migration runs. Raw provider traces,
call audio, credentials and machine-specific reports are not published. The
`full-01`, `full-02`, `phone-final` and `deployment-eot` result directories remain
local and ignored. The harness source and strict pass/fail conditions are
published under this directory; it requires the environment documented in
[the validation guide](../README.md).

The first returned audio measurement of 694 ms was one observation, not a latency
percentile, benchmark or Rust speedup claim. Publication does not resolve the
remaining acceptance gaps above.

## Persistent connections and concurrent echo calls — 2026-10-19

Implemented service-wide Turn HTTP pooling with call-local credentials/locks/clock
state, a shared lazy transfer-Redis ConnectionManager, one bounded non-generating
front-provider warm-up, and concurrent STT/TTS handshakes with failure cleanup.
See [OPERATIONS.md](../../OPERATIONS.md#connection-ownership-and-warm-up).

The standalone Rust echo example handled these measured loads on this shared
16-logical-CPU x86_64 host, using debug binaries. Each successful call exchanged
two distinct-reference utterances; live calls used generated input speech, actual
streaming STT/TTS, and independent output transcription. All passing rows below
had zero full-output WER failures at the existing 0.25 threshold.

| Path | Simultaneous calls | Replies | Result | Speech sockets |
| --- | ---: | ---: | --- | --- |
| Text | 1, 32, 128, 512 | 1,346 total | Pass | None |
| Text | 1,024 | 2,048 | Pass | None |
| Browser PCM | 16, 32 | 96 total | Pass; provider/received PCM SHA-256 equal for every call | One STT + one TTS per call |
| Phone mu-law / native audio | 1, 4, 8, 16, 32 | 122 total | Pass | One STT + one TTS per call |
| Phone mu-law / native audio | **45** | **90** | **Pass** | 45 STT + 45 TTS |
| Phone | 46 | No speech sent | Provider boundary: extra TTS handshake returned **HTTP 429** | 46 STT + 45 TTS |
| Phone | 64 | No speech sent | Only 45 TTS connections opened; batch stopped before speech | 64 STT + 45 TTS |

The harness waits for every remote speech connection before sending utterances.
Consequently the failed boundary batches do not claim partial conversation success.
All successful and boundary batches sent EndCall for every opened call, released
provider sockets, and stopped both processes with exit code zero. No PSTN calls,
GPT Live requests, external app reasoning, front generation, ambience or recording
were included. The phone path exercised AEC/native audio but did not inject echo;
this run is not an additional AEC effectiveness benchmark.

At 45 phone calls, mean speech-end-to-first-output was 2.19 s, p95 2.35 s. This
includes the configured 1.8 s STT endpointing window, provider processing, local
observer and scheduling overhead. Mean observed echo HTTP time was 3.70 ms
(p95 23.60 ms). Neither is isolated Rust CPU latency. The input generator's 20 ms
frame cadence had p95 20.72 ms and maximum 52.07 ms; those are send-time gaps,
not measurements of per-frame DSP completion. Phone startup includes the harness's
fixed one-second wait and must not be used as a provider handshake benchmark.
At 1,024 text calls, p95 first output was 334 ms; this includes typing policy and
load-generator/proxy overhead.

**Practical test sizing:** 45 is the largest fully passing live phone batch;
46 hit the current credential/account's observed streaming-TTS quota. Use 32 for
routine concurrent live tests to leave quota headroom; reserve 45 for boundary
checks with no competing provider sessions. Production admission defaults remain
64 active calls / 128 handshakes. Those defaults do not account for this observed
provider limit. Rust's maximum sustained voice capacity remains unmeasured;
release-build soak tests with recording and production features are still needed.
1,024 is a verified short text-call lower bound, not a maximum or a voice capacity.

Evidence (local ignored artifacts):

- `results/echo-concurrent-text-01/report.json` and `echo-concurrent-text-02/report.json`
- `results/echo-concurrent-live-02/report.json` (all browser audio digest checks)
- `results/echo-concurrent-phone-01/report.json` (phone ramp through 32)
- `results/echo-concurrent-phone-boundary/report.json` and `provider-events.json`
  (45 full conversations; explicit TTS 429 at 46)
- `results/echo-concurrent-phone-02/report.json` (64-call setup boundary)
- Each run includes source/binary hashes, raw Turn/provider traces, caller WAVs and
  `process-exits.json`; `reference-audit.json` checks distinct reference numbers.

The first browser ramp, `echo-concurrent-live-01`, failed at 16 because the
independent recognizer rendered “You said” as “USAD” / “Use SAD” in two clips.
Both clips had exactly the provider's byte count and the correct caller content.
The later 16/32 run added per-call SHA-256 verification and passed the original
full-WER threshold without exceptions. Retain this initial failure; it is not
claimed as a clean pass or evidence of Rust saturation.

Focused regression validation: **168 Rust tests passed** across front,
orchestration, escalation, transport, service and echo example, including the
explicit real-Redis connection/reconnect/CAS test. Three Python oracle tests
passed. Formatting checks and the repository's selected-package Clippy command
(`--no-deps --all-targets -- -D warnings`) passed. No secrets were copied and no
production admission limits were changed.
