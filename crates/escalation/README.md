# Transfers and escalation (module 11)

Native Twilio warm transfers preserve the application-owned `EscalationOrder`.
The module owns provider mechanics, not target selection or summary generation.
`Service` installs one manager and a call-bound hook; no business state or call
clock is shared between calls.

```text
application escalation order
  -> prepare four persona renders (parallel, while handoff speech plays)
  -> handoff playback receipt / bounded wait
  -> atomic claim -> caller hold REST update -> agent dial
                                              |
                       +----------------------+-------------------+
                       |                      |                   |
                    whisper                terminal          caller leaves
                 wait audio <=5s          no keypress          cancel/end leg
                       |                      |
                    keypress             next target
                       |                      |
                  conference          exhausted -> voicemail + caller callback
                       |                      |
                 either leg ends -----------+-> one resolved fact

preparing -> holding -> dialing -> ringing -> connected -> finished
                          |           |          |
                          +-----------+          +-> finished
                          |
                   next dialing / fallback -> closing -> finished
```

The media stream may close during the hold redirect. Transfer state therefore
lives beyond Driver teardown. Later signed HTTP callbacks use the manager and
report `escalation_leg` / `escalation_resolved` facts to the same authenticated
application's `/record_transfer` endpoint. Request IDs are stable across retries;
409, 429, network failures and 5xx receive bounded backoff. Exhausted deliveries
increment `report_failures` and emit a credential-free error event. This is bounded
retry delivery, not a crash-recoverable outbox.

## Source responsibilities

- `manager.rs`: `Manager` owns provider pools, renders and report tasks; `Binding`
  identifies the authenticated call/application; `Transfer` owns one order,
  indexed leg, atomic state, audio tokens and fact sequence. `prepare`, `execute`,
  `whisper`, `keypress`, `status` and `caller_gone` implement the lifecycle.
- `store.rs`: `Store` provides expiring opaque values and compare-and-set in memory
  or standalone Redis. CAS includes both state and newly allocated fact numbers.
- `twiml.rs`: pure escaped XML for hold, whisper/gather, conference and fallbacks;
  rendered persona MP3 takes precedence over the configured Twilio Say voice.
- `bin/escalation-validation.rs`: JSON-lines boundary for direct Python XML checks.
- `service/src/extensions.rs`: call-bound orchestration hooks; `service/src/routes.rs`
  verifies callback signatures and serves opaque audio capability URLs.

All provider requests use native asynchronous HTTP. No callback holds a global
lock over I/O. Hold audio waits at most three seconds after playback drains; the
four renders run concurrently with a 30-second per-request limit. Missing/failed
renders use Say. Audio capabilities expire after ten minutes; private hold/whisper
bytes are removed on finalization. Callbacks must include the attempt embedded in
the dial URL and the current agent SID. Empty keypad input does not bridge.

## Deliberate differences and limits

- Callback URLs carry the attempt index, preventing a stale callback from claiming
  a newer dial while its SID is not known yet. Duplicate keypresses do not emit a
  second connection fact. Tombstones prevent duplicate transfer execution.
- Redis uses `gw:rust:esc:*`, separate from Python's incompatible hash encoding.
  Route all callbacks for a transfer to the implementation that created it.
- Conference names use the full caller SID to avoid truncated-name collisions.
- Ambiguous dial errors do not automatically dial another destination. The caller
  receives the fallback; unknown provider-side legs require reconciliation.
- State expires after four hours. Memory mode supports one process only. Redis
  supports callbacks on multiple workers; expiry is an orphan bound, not active
  transfer recovery or live lease renewal.
- Twilio/paid speech acceptance is opt-in. These tests make no external calls;
  real telephone transfer acceptance remains required before production.

Configuration reuses existing `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`,
`TWILIO_PHONE_NUMBER`, `GATEWAY_PUBLIC_BASE_URL`, `GATEWAY_ESCALATION_SAY_VOICE`,
`DEEPGRAM_API_KEY` and the call's persona voice. Public base must be reachable by
Twilio and retain reverse-proxy path prefixes. Callback authentication uses the
existing Twilio signature verifier. Audio URLs use random short-lived tokens.

## Validation

```sh
cargo test --locked -p vogon-escalation -p vogon-service
cargo build --locked -p vogon-escalation --bins
python validation/escalation/compare.py
```

The comparison requires sibling `voice-gateway` and compares XML trees (80 cases),
including escaping, persona audio, voice selection and every TwiML generator.
Native tests cover actual local REST/callback transactions, duplicate/stale events,
callbacks before REST completes, caller departure during dial, independent calls,
report retry identity, fallbacks, voice cleanup, preparation cancellation and
ambiguous dial failures. Service tests cover signatures and real Redis CAS/expiry.
No existing AEC, browser silence or deployment-EOT finding is fixed by this module.
