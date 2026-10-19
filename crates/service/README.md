# HTTP service and session management (module 10)

`vogon-service` supplies a runnable Axum/Tokio gateway host for the existing
orchestrator. Text, browser and Twilio sockets use the same native `Driver` and
`SessionFactory`; application reasoning remains behind the existing Turn API.
Transfers, recording, operational readiness and deployment acceptance are still
modules 11–13. This is an implemented host with local acceptance, not a claim of
production readiness.

## Source responsibilities

| Source | Types and functions | Responsibility |
|---|---|---|
| `main.rs` | `main`, `run` | Own the multithread Tokio runtime, load configuration/store, bind HTTP and coordinate SIGINT/SIGTERM shutdown. |
| `bootstrap.rs` | `Config::{load,resolve,profile}` | Read dotenv without modifying process environment; use module 1's settings/schema and tenant profiles. Explicit environment overrides dotenv. |
| `routes.rs` | `router`, create/inspect, TwiML/status and upgrade handlers; `App`, `Dial`, `Create`, `Failure` | Validate the public HTTP contract, compare service credentials, verify callbacks, persist calls and admit sockets. Request DTOs contain no business policy. Errors omit provider response bodies/secrets. |
| `host.rs` | `Service` | Share immutable clients/config and store; own admission semaphores, registry and shutdown broadcast. Resolve socket identity and run one driver per accepted call. |
| `host.rs` | `Lease`, `SocketSink`, `SnapshotHooks`, local `CallTask` | Release registry/admission on drop; acknowledge actual socket writes; save the resolved snapshot before transport opens; abort a driver if its owner is dropped. |
| `record.rs` | `Record`, `State`, `transition`, `decode`, `valid_id`, `utc_now` | Preserve schema-v1 JSON, ignore future fields, enforce monotonic physical-call state and retain first timestamps. Epoch timestamps are independent of `CallClock`. |
| `store.rs` | `Store`, `Memory`, `Redis`, `Change`, `Ttl`, `Entry` | Atomic creation, tenant-qualified lookup, token/SID indexes, field updates, exclusive claims and expiry. Redis scripts compare complete JSON without Lua reserialization. |
| `twilio.rs` | `signature`, `signed`, `state`, `twiml`, `Dial`, `dial` | Native HMAC validation, callback vocabulary, escaped XML and form-encoded REST dial. No Python Twilio SDK in the running Rust service. |
| `bin/service-validation.rs` | JSON-lines adapter | Expose deterministic record/Twilio functions to the Python oracle; never load deployment credentials. |
| `bin/service-live-host.rs` | Opt-in live validation launcher | Run the same service with localhost observation proxies and seeded synthetic phone records; read existing credentials only after explicit `--live`. |

The service still embeds Python for the previously retained NumPy/audioop audio
routines. SpeexDSP and WebRTC VAD remain direct native integrations.

## Entry, ownership and exit

```text
main: one Tokio multithread runtime
  |
  +-- settings + tenant profiles + Memory/Redis + shared HTTP pools
  |
  +-- POST /v1/tenants/{tenant}/calls
  |      validate/auth --> atomic create --> optional Twilio REST dial
  |                                      --> token / caller WebSocket URL
  |
  +-- WebSocket upgrade --> register Lease (bounded pending handshakes)
         |
         +-- /ws/call: token -> text/browser record
         +-- /ws/twilio: start.callSid -> phone record (10 s handshake limit)
         |
         v
       acquire call permit --> atomic claim(unique socket owner)
         |
         +-- socket reader --bounded Input channel--> Driver::run task
         |                                               |
         |                         StartCall -> save snapshot -> Session
         |                                               |
         |                         independent input / event / API work
         |                         eager EOT -> HTTP await
         |                         final EOT -> collect + invalidate stale work
         |                                               |
         +<-- SocketSink <-- serial writer <-- streaming TTS / controls
         |
         +-- hangup / disconnect / input overflow / shutdown broadcast
                close input --> await Driver teardown + EndCall
                --> terminal record --> status relay --> release claim
                --> drop Lease and call permit
```

Network tasks share runtime workers, not a thread per call. Each phone call still
owns a serialized audio worker. Every driver constructs its own `Clock`, session,
event sequence and counters. The shared registry only counts owned sockets and
never advances any call clock. Host admission and handshake deadlines are separate
from the [per-call heartbeat/timer state machines](../orchestration/CALL_FLOW.md).

Driver event timestamps already use seconds since that call's monotonic origin.
The host therefore supplies a zero timestamp offset to `HttpTurnClient`; persisted
record epochs still use wall time. Subtracting a wall-clock epoch from these
relative events produced negative times and was corrected during live acceptance.
Phone sockets emit media, mark and clear controls only. Browser `server.*`
notifications are suppressed by `Session::client_event` in phone mode.

The caller reader uses bounded `try_send`: a full input queue closes intake and
finalizes the call instead of silently dropping accepted media. Socket sends
acknowledge actual writes. Independent calls have separate API clients/drivers;
a blocked StartCall on A does not prevent B from opening. Within a call the
orchestrator continues its established ordered commit protocol.

## Persisted lifecycle

```text
created ----dial request----> dialing ----callback----> ringing
   |                            |                         |
   +----------------------------+-------------------------+
                                |
                   socket claim / answered callback
                                v
                            connected
                                |
                 disconnect / stop / driver completion
                                v
                              ended

Any nonterminal state -- dial/driver failure --> failed
Any nonterminal state -- canceled callback ---> cancelled

ended / failed / cancelled are terminal.
Repeated and backward callbacks leave state and first timestamps unchanged.
A socket cannot claim a terminal record.
```

Callbacks may skip intermediate states. This store state machine records physical
session lifecycle; it does not replace the transport's listening/playback/silence
state machines or application business state.

Redis preserves the existing `gw:{tenant}:call:{id}`, `gw:idx:token:{token}`,
`gw:idx:sid:{sid}` and `gw:{tenant}:claim:{id}` naming (braces here denote
interpolated values). Record writes use CAS; a provider bind and a concurrent
snapshot cannot overwrite each other. Lookup indexes refresh atomically with the
record. Default live retention is 24 hours; terminal retention is one hour.
Memory mode applies the same expiry policy, pruned on access.

## Routes and configuration

| Route | Implemented behavior |
|---|---|
| `GET /health` | Basic liveness and owned socket count; no provider/native readiness claim. |
| `POST /v1/tenants/{tenant}/calls` | Pair-secret auth, request validation, persisted app binding and optional native REST dial. |
| `GET /v1/tenants/{tenant}/calls/{call_id}` | Public state/timestamps only; excludes tokens, credentials and snapshots. |
| `GET/POST /v1/twilio/twiml?token=...` | Signed URL/form validation, store lookup, stream or hangup XML. |
| `POST /v1/twilio/status` | Signed state transition and bounded best-effort app status relay. |
| `GET /ws/call?token=...` | Text/browser upgrade and exclusive media attachment. |
| `GET /ws/twilio` | Phone start-frame/SID lookup and exclusive attachment. |
| Escalation callbacks | Signed native transfer callbacks and expiring persona audio capabilities (module 11). |
| Artifact paths | Return **501** until module 12 is installed. |

As in Python, management authentication uses the application/gateway pair secret;
tenant identity is data supplied by that trusted service. Empty/whitespace service
key disables management auth. Empty Twilio auth token disables callback signature
checks. Phone WebSocket admission uses the stored SID, matching Python; it does
not add a separate upgrade signature check.

`GATEWAY_PUBLIC_BASE_URL` must contain the externally visible prefix when a proxy
strips one (for example `https://example/gw`). Signed callback URLs and generated
URLs retain it. Proxy forwarding headers are not implicitly trusted.

Existing shared settings resolve through module 1. Host-specific defaults:

| Environment | Default | Purpose |
|---|---|---|
| `GATEWAY_HOST`, `GATEWAY_PORT` | `0.0.0.0`, `8002` | Listener binding. |
| `GATEWAY_MAX_ACTIVE_CALLS` | `64` | Admission permits held through finalization. |
| `GATEWAY_MAX_PENDING_HANDSHAKES` | `128` | Upgrades awaiting identity/claim. |
| `GATEWAY_INPUT_CAPACITY` | `256` | Frames queued to one driver. |
| `GATEWAY_MAX_MESSAGE_BYTES` | `1048576` | HTTP body and caller WebSocket message/frame cap. |
| `GATEWAY_SHUTDOWN_TIMEOUT_SEC` | `210` | Per-driver graceful close budget; service allows another 15 seconds for final store/relay work. |

These are bounds, not measured capacity recommendations. Redis mode requires an
available standalone Redis and fails startup if unavailable; it never silently
falls back to memory. Cluster mode is explicitly unsupported by these multi-key
scripts. `Memory` is a single-process development store; lookup/pruning scans the
map and is not intended as a high-volume distributed backend.

From the monorepo root, with the documented toolchain/native dependencies installed:

```sh
PYO3_PYTHON="$PWD/data-plane/.venv/bin/python" \
PYTHONPATH="$PWD/data-plane/.venv/lib/python3.10/site-packages" \
cargo run --locked --manifest-path vogon-rust/Cargo.toml \
  -p vogon-service --bin vogon-gateway -- --env voice-gateway/.env
```

The default dotenv path is `voice-gateway/.env`, relative to the working directory.
No credential file is copied. `--check` validates bootstrap/store connectivity;
it is not module 13's native capability or provider-readiness check.

## Compatibility decisions and remaining limits

Deliberate differences from Python are exercised by the native tests:

- Atomic duplicate creation and a unique claim owner per socket, including sockets
  on the same instance. Python's instance-level claim permits duplicate attachment.
- Terminal/backward callback protection, single relay per effective transition and
  terminal-token rejection. Python permits regressive state changes.
- Memory expiry matches Redis. Provider IDs cannot be rebound to another call.
- IDs use a bounded ASCII route/index-safe alphabet; app URLs must be HTTP(S),
  without embedded credentials or fragments. Validation errors retain the status
  contract but not FastAPI's detailed Pydantic error-array format.
- Bounded upgrades/calls/input and explicit overload shutdown; management creation
  stops when shutdown begins. XML attributes are escaped.
- Dial failures mark the record failed and return 502 rather than leaving it dialing.

Transfer hooks are installed from module 11. Artifact hooks remain pending, so recording is
not implemented. No long-call lease-renewal/recovery protocol exists: live claims
expire after 24 hours, and process-crash recovery currently waits for expiry.
Rolling Python/Rust ownership compatibility is not accepted; stop existing calls
before switching hosts. Status relays are best effort, not a durable outbox.
An ambiguous dial response or process failure between provider dial and SID bind
still needs reconciliation; do not infer exactly-once dialing from atomic records.
Durable Turn API fact acknowledgement, richer health/metrics and deployment load
acceptance remain in the module checklist.

## Validation

Run `data-plane/.venv/bin/python vogon-rust/validation/service/run.py` from the
monorepo root. It uses loopback servers, a private Redis child and synthetic keys;
no external provider/model requests or GPT Live tests. See the
[case matrix and saved evidence](../../validation/service/README.md).

The separate [live generated-audio suite](../../validation/live/REPORT.md) runs
Deepgram STT/TTS through this router with a local echo Turn service. Its phone
caller simulates Twilio media locally; it does not establish PSTN, real microphone
or production-load acceptance. GPT Live remains untested.
