# Vogon Rust

Rust implementation of [Vogon](https://github.com/LendEasy/vogon), LendEasy's
full-duplex voice gateway. Streaming speech, barge-in, speculative end-of-turn,
playback and call-local timers run behind an application-facing Turn API.

**Experimental migration:** modules 1–11 have implemented cores. Recording and production acceptance remain incomplete. Read
[known issues](KNOWN_ISSUES.md) and the [module checklist](MODULES.md) before deployment.

## What is implemented

- Tokio/Axum HTTP and WebSocket host for browser PCM, text and Twilio media.
- Streaming Deepgram STT/TTS, eager/resumed/final EOT handling and stale-output rejection.
- Concurrent calls, independent input/output tasks, paced playback and barge-in.
- Per-call clocks, silence ladders on the phone path and graceful call finalization.
- Native SpeexDSP/WebRTC VAD, ambience mixing and retained NumPy/audioop via PyO3.
- Turn API clients, front/floor policy, configuration and memory/Redis session stores.
- Native warm transfers, persona whispers, keypad bridging and atomic callbacks.

Application business logic stays in your Turn service. The
[echo example](examples/echo-turn-service/README.md) replies with what the caller said.

## Build on Linux

Native audio currently supports Linux; x86-64 is the validated platform. Use the
pinned Rust toolchain and CPython 3.10–3.12 (audioop is retained). From this checkout:

```sh
sudo apt-get install build-essential pkg-config libspeexdsp-dev python3-dev python3-venv redis-server
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-runtime.txt
.venv/bin/python tools/fetch_webrtcvad.py
export PYO3_PYTHON="$PWD/.venv/bin/python"
export PYTHONPATH="$($PYO3_PYTHON -c 'import sysconfig; print(sysconfig.get_path("purelib"))')"
cargo build --locked -p vogon-service --bin vogon-gateway
```

Install Rust through rustup if needed; `rust-toolchain.toml` selects the version.
The explicit VAD setup verifies the unchanged 2.0.10 archive before extracting it
into an ignored directory. Cargo never fetches native sources during compilation.
Use `--archive PATH` with the setup script for an offline archive. SpeexDSP is
linked from the system. No Python gateway or private application checkout is
needed to build or run the Rust service.

## Run the echo demo

In one terminal:

```sh
cargo run --locked -p vogon-echo-turn-service
```

In another, after the build environment above:

```sh
cp .env.example .env
cargo run --locked -p vogon-service --bin vogon-gateway -- --env .env
```

The echo service listens on `127.0.0.1:8090`; the gateway listens on
`127.0.0.1:8002`. Text calls need no speech credentials. For voice, set
`DEEPGRAM_API_KEY` in your ignored `.env`. See the
[example's call-creation and WebSocket instructions](examples/echo-turn-service/README.md).
The example does not include a microphone UI. Its public demo tokens are for
local use; configure your own keys before exposing endpoints.

Always pass `--env .env` for this standalone layout: the original binary default
still names `voice-gateway/.env`. GPT Live is not part of current acceptance.

## Test

```sh
export REDIS_SERVER_BIN="$(command -v redis-server)"
cargo test --locked --workspace -- --skip live_
```

Use the same `PYO3_PYTHON` and `PYTHONPATH` as the build. Tests use loopback
HTTP/WebSocket fixtures and an isolated Redis child, not paid providers.
The echo example alone needs neither native audio nor Python:

```sh
cargo test --locked -p vogon-echo-turn-service
```

See [validation scope](validation/README.md) for Python differential runners and
[the live-test summary](validation/live/REPORT.md) for earlier generated-audio
acceptance. Raw logs, credentials, recordings and generated reports are excluded
from Git. Four-call validation is not a production-capacity claim.

## Documentation

| Guide | Contents |
|---|---|
| [MODULES.md](MODULES.md) | Modules 1–13, completed work and remaining acceptance |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Composition, ownership and audio/library boundaries |
| [Call flow](crates/orchestration/CALL_FLOW.md) | ASCII diagrams for eager execution, clocks and timers |
| [Service](crates/service/README.md) | Routes, authentication, sessions and shutdown |
| [Examples](examples/README.md) | Runnable echo Turn service |
| [DOCUMENTATION.md](DOCUMENTATION.md) | API documentation and generated-source maintenance |
| [CONTRIBUTING.md](CONTRIBUTING.md) | DCO, tests and contribution conventions |
| [SECURITY.md](SECURITY.md) | Private reporting and operational boundaries |

Crate READMEs describe each module's types, functions and tests. Some migration
notes retain sibling Python-reference paths; see validation prerequisites.
Numerical prototypes remain local and ignored, outside the public Cargo workspace.

## License

[Apache License 2.0](LICENSE). Third-party libraries and test fixtures retain their
own notices; see [THIRD_PARTY.md](THIRD_PARTY.md).
