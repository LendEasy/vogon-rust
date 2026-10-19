# Recording and artifacts (module 12)

One `Capture` belongs to one authenticated call and uses that Driver's `Clock`.
It never advances clocks or shares caller/agent counters with another call.

```text
Twilio start ----------> recording origin
Twilio inbound media --> pair stream timestamp with arrival --> original caller bytes
                         |                                  |
                         +--> DSP / STT                     v
                                                      call-local Recorder
successful Socket send -> agent sample clock -----------> stereo timeline
successful clear ------> trim queued agent samples -----> (caller L / agent R)
mark echo -------------> event log                          |
DSP scalar result -----> frames CSV                         |
floor / EOT / playback -> sparse event log                  |
                                                           v
Driver closes transport -> finish hook -> freeze capture -> blocking WAV encode
                                                           |
                                             +-------------+--------------+
                                             v                            v
                                      atomic local file              AWS SDK S3
                                             |                            |
                                             +------ authenticated GET ---+
                                                      timeline renderer
```

`recorder.rs` owns timestamp projection, bounded packets, clear trimming, event
rebasing and WAV construction. It retains audioop for mu-law decoding. Sample
positions use ties-to-even rounding; gaps are zero PCM and overlapping packets
use the last packet. A timestamp/arrival divergence over 250ms reanchors the caller
clock. Missing or nonfinite timestamps use arrival. The origin is the earliest
stream-start/chunk time. Clear uses dispatch time: Twilio offers no sample-level
clear acknowledgement, so the WAV is an estimate of audible agent playback.

`service/recording.rs` owns `Capture`, its output sample clock and `RecordingSink`.
The decorator records only successfully dispatched packets, including ambience.
Failed media and failed clears cannot create recording side effects. Incoming
original media is captured before AEC. `Observer` carries frame, floor, EOT and
playback diagnostics without changing Turn API sequencing. The finish hook consumes
capture once, runs audioop/WAV assembly on blocking capacity, then awaits storage.
Errors are reported with stable codes and do not prevent other finalization hooks.

`store.rs` owns atomic local replacement and native `aws-sdk-s3` PutObject/GetObject.
Local and S3 writes are independent; either may succeed if the other fails. S3 uses
standard jittered exponential retries, per-attempt and overall operation deadlines,
and the AWS credential chain or explicit credentials from the shared dotenv.
No boto3 or Python gateway package is imported. Configuration reuses
`RECORDINGS_PATH`, `S3_ENABLED`, `S3_BUCKET`, `AWS_REGION`/`AWS_DEFAULT_REGION` and
optional `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`.

`timeline.rs` safely embeds data into the existing Python gateway's standalone
HTML renderer, retained in `timeline.html`. It needs no CDN or Python runtime code.
The optional offline VAD overlay is absent; live scalar DSP diagnostics and floor
logs are present. Sparse event timestamps share the WAV origin.

## Compatibility and resource boundaries

- The existing Python recorder is a Twilio 8kHz stereo recorder. This module ports
  that path. Browser audio recording is not added; browser diagnostic events may
  still be collected. Channel output remains stereo, as in the Python recorder;
  alternative `recording.channels` values do not select a new encoding.
- Policy disabling recording prevents collecting audio/events, an intentional
  improvement over Python collecting audio and skipping only the final save.
- Tenant-qualified keys are `tenants/{tenant}/calls/{call_id}/recording.wav`,
  `audio-events.json` and `frames.csv`. This differs from Python's globally keyed
  flat local files and `calls/{id}` S3 layout, preventing collisions between tenants.
- The HTTP routes require the existing application service key and matching tenant
  session record. Audio timeline generation requires the WAV; absent data is 404.
  Session-record expiry can make retained artifacts inaccessible through these routes.
- Capture bounds are 64MiB encoded audio, four hours of timeline, 8MiB event JSON
  and 720,000 scalar frame rows. Exhaustion sets `recording_truncated` in metadata;
  it does not stop conversational audio. WAV assembly can allocate up to roughly
  460MB for a four-hour sparse timeline; deployment admission must budget for it.
- Local files are renamed atomically, but there is no disk-spooled crash recovery,
  retention janitor or durable upload queue yet. S3 bucket lifecycle/encryption and
  local access permissions are deployment responsibilities. Process shutdown awaits
  recording finalization subject to the configured call deadline.

## Validation

```sh
cargo test --locked -p vogon-artifacts -p vogon-service
cargo build --locked -p vogon-artifacts --bins
python validation/artifacts/compare.py
```

162 direct Python comparisons cover every WAV byte, stream timestamps, origin,
partial clears and event rows. Native tests cover disabled policy, allocation
bounds, HTML escaping, atomic storage, tenant isolation, failed sends, one-shot
finalization and actual phone-host persistence. AWS SDK tests use a local signed
S3 fixture: two transient failures then success, cloud fallback after deleting the
local file, missing keys, and non-retryable authentication failure. No real AWS,
telephone or paid speech call is made. These are correctness checks, not throughput
benchmarks. The [AWS retry documentation](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/retries.html)
describes the SDK's transient-error classification and exponential jitter.
