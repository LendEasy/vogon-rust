# Validation scope

## Standalone native tests

After the root README's native/Python setup, run from this repository:

```sh
export PYO3_PYTHON="$PWD/.venv/bin/python"
export PYTHONPATH="$($PYO3_PYTHON -c 'import sysconfig; print(sysconfig.get_path("purelib"))')"
export REDIS_SERVER_BIN="$(command -v redis-server)"
cargo test --locked --workspace -- --skip live_
```

Tests use local HTTP/WebSocket fixtures and an isolated Redis child. No provider
credentials are needed. GPT Live-specific tests are excluded. The echo example
can be tested on its own without Python or native audio dependencies:
`cargo test --locked -p vogon-echo-turn-service`.

## Python migration comparisons

The existing `audio`, `modules`, `providers`, `transport`, `front`, `orchestration`
and `service` runners retain their original migration layout. They expect sibling
`voice-gateway` and `vogon-contract` source trees; some orchestration/service
reference tests additionally use the original application's `data-plane` tree.
That application is not part of this public repository and is not needed to build
or run the Rust gateway. These runners are retained as reference/comparison tools,
not advertised as a self-contained public test gate.

Generators likewise require the Python reference trees. Checked-in generated
Rust declarations, schemas and front constants allow ordinary builds without them.

## Live tests and reports

`live/` contains the generated-audio provider harness. It requires explicit
`--live`, credentials and the documented migration environment; it is not a CI
step. The standalone echo Turn service is under `examples/echo-turn-service`.

All `results/` folders, generated audio caches and logs are ignored. Historical
crate documentation may link to local generated results; those paths are present
after the corresponding runner executes in its required environment. Public
Markdown summaries record scope and limitations without publishing raw call
traces, credentials or machine-specific artifacts.
