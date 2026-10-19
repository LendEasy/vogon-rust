# Echo Turn service

A standalone Rust/Axum example of the application's Turn API. It returns
“You said: …” for each finalized caller transcript. The gateway handles streaming
STT, TTS, playback, barge-in and transport; this service needs no provider keys,
Python runtime or model calls.

```text
Caller audio -> Rust gateway -> STT -> echo Turn service
                     ^                       |
                     +---- speak command ----+
                     |
                     +-> TTS -> caller audio
```

## Run

From `vogon-rust`:

```sh
cargo run --locked -p vogon-echo-turn-service
```

Defaults: bind `127.0.0.1:8090`, bearer token `echo-demo`. Override with
`ECHO_BIND` and `ECHO_AUTH_TOKEN`. An empty token is rejected. Stop with Ctrl-C.
The default token is a public development value, intended for a local demo.

## Connect the gateway

Start the gateway using its [existing setup](../../crates/service/README.md).
Create a call whose `app` binding points to this service:

```sh
curl http://127.0.0.1:8002/v1/tenants/demo/calls \
  -H "Authorization: Bearer ${GATEWAY_SERVICE_KEY}" \
  -H 'Content-Type: application/json' \
  -d '{
    "mode": "widget",
    "app": {
      "base_url": "http://127.0.0.1:8090/turn/v1",
      "auth_token": "echo-demo"
    }
  }'
```

Set `GATEWAY_SERVICE_KEY` in your shell to the gateway's configured management
key. This is separate from the echo service token. If you override
`ECHO_AUTH_TOKEN`, use that value in `app.auth_token` too.

Connect your browser/PCM caller to the returned `ws_url`. For a text-only
conversation, use `"mode": "text"` and send
`{"type":"client.message","payload":{"text":"Hello"}}` on that socket.
The demo does not include a microphone UI. For containers or separate hosts,
replace the loopback URL with an address reachable from the gateway.

Keep optional front-model/GPT Live features disabled in your gateway configuration
for an echo-only speech demo. The example's call policy disables the remote
semantic judge, ambience and silence/duration ladders. Speech-provider keys stay
in the gateway's existing dotenv; this service never reads or copies them.

## Try the Turn API directly

These commands need only the echo service, without a gateway or paid API calls:

```sh
curl http://127.0.0.1:8090/turn/v1/start_call \
  -H 'Authorization: Bearer echo-demo' -H 'Content-Type: application/json' \
  -d '{"call_id":"demo-1","mode":"voice"}'

curl http://127.0.0.1:8090/turn/v1/process_events \
  -H 'Authorization: Bearer echo-demo' -H 'Content-Type: application/json' \
  -d '{"call_id":"demo-1","request_id":"turn-1","events":[
    {"kind":"utterance_final","text":"Hello","confidence":1.0,"low_conf_words":[]}
  ]}'

curl http://127.0.0.1:8090/turn/v1/end_call \
  -H 'Authorization: Bearer echo-demo' -H 'Content-Type: application/json' \
  -d '{"call_id":"demo-1","reason":"hangup"}'
```

The final event returns:

```json
{"speak":[{"audio_id":"demo-1-echo-1","text":"You said: Hello"}]}
```

## Behavior and source responsibilities

`src/main.rs` contains the HTTP routes, process entry point and application state:

- `Call` owns one call's prepared reply, audio counter and request replay cache.
  Eager EOT prepares text without emitting speech. Resumed speech discards it.
  Final EOT reuses matching text or creates an answer from the changed transcript.
- `App` owns authentication and the active-call map. Its lock covers short
  in-memory mutations only; handlers do no external work while holding it.
- `start_call` preserves state on retries; `process_events` handles batches in
  order; `end_call` removes the call and accepts repeated finalization.
- `router` exposes those routes and unauthenticated `GET /health`.
  `main` reads the two environment options and owns listener shutdown.

Identical retries with the same request ID return the same audio IDs. Reusing an
ID with a changed body returns 409. Later turns with identical words and new
request IDs are valid. Missing calls return 404. Non-speech facts are ignored.
The gateway remains responsible for rejecting stale provider events/responses.

This illustrates the eager/final protocol with cheap string preparation; it does
not simulate expensive background inference. State and retry caches are held in
memory until EndCall, and disappear on restart. There is no persistence, idle-call
expiry or production admission policy. Use unique call IDs for new conversations.

`src/tests.rs` exercises the HTTP router and validates replies using the gateway's
shared contract codec: speculation/resumption, changed finals, retries, call
isolation, cleanup, silent inputs and authentication.

```sh
cargo test --locked -p vogon-echo-turn-service
```
