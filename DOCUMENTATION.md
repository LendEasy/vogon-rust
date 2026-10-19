# Documentation maintenance

Start with [ARCHITECTURE.md](ARCHITECTURE.md) for data flow, ownership, formats and
integration rules. Use [MODULES.md](MODULES.md) for migration status, then the
crate README and source comments for the API you are changing.

The [illustrated call-flow guide](crates/orchestration/CALL_FLOW.md) documents
startup, full-duplex task execution, speculative EOT, clocks and teardown.
Keep its ownership and ordering diagrams synchronized with the driver, transport
and application HTTP/speculation interfaces when those behaviors change.

## Current coverage

All public function and type declarations in the implemented crate source files
have Rustdoc role/behavior comments. Playback and barge-in already include detailed
field and helper documentation. Audio, configuration, providers, transport and
clock, front/line, orchestration and service APIs describe formats, units, side effects and lifecycle constraints.
Selected critical fields and command variants are documented individually.
This is not a claim that every private helper or generated field is documented.

The crate READMEs identify scope, dependencies and validation. The architecture
guide explains composition across crates. The [service guide](crates/service/README.md)
documents module 10, its source/type responsibilities, ASCII lifecycle diagrams,
configuration and compatibility decisions. Modules 11–13 remain in the checklist.

## Build browsable API documentation

From `vogon-rust`, with the cached toolchain/dependencies and an appropriate
Python interpreter selected through `PYO3_PYTHON`:

```sh
cargo doc --offline --locked --no-deps --document-private-items \
  -p vogon-audio-core -p vogon-audio-runtime \
  -p vogon-contracts -p vogon-configuration \
  -p vogon-playback -p vogon-barge-in -p vogon-clocks \
  -p vogon-speech-providers -p vogon-transport -p vogon-front -p vogon-orchestration -p vogon-service
```

Open `target/doc/vogon_transport/index.html` and use the crate navigation.
Generated HTML stays in the ignored target directory. Building documentation
compiles/checks dependencies but does not connect to providers or run GPT Live.
To reject broken API links, set `RUSTDOCFLAGS=-D rustdoc::broken_intra_doc_links`.

## What an API comment should explain

- The responsibility and owner of a type, including whether it is per-call or shared.
- Input/output formats and units when they are not obvious from the type.
- Mutated state, emitted events and the distinction between send and playback completion.
- Failure, cancellation and shutdown behavior; whether accepted work can be withdrawn.
- Compatibility quirks and the reason for an intentional difference from Python.

Keep implementation comments next to the decision they explain. Avoid repeating
function names as descriptions, undocumented changes to units, and claims of
production readiness inferred only from offline parity. When an integration is
implemented, update older READMEs that called it future work.

## Generated declarations

`crates/contracts/src/types.rs` and `crates/configuration/src/models.rs` are
generated. Their role descriptions live in `tools/generate_contracts.py` so
regeneration preserves them. Types, defaults and schemas come from Python, with
one documented runtime-nullability override: eager/final `heard` accepts `None`
because the Python phone transport emits it before any playback history exists.
The generator deliberately requires a role description for newly added types.

From the monorepo root:

```sh
data-plane/.venv/bin/python vogon-rust/tools/generate_contracts.py
```

Review generated declarations, schema JSON and `tools/generated-sources.json`.
For documentation-only edits, verify that declarations and schemas are unchanged
after removing comment lines. For behavioral/schema changes, run the relevant
[module parity checks](validation/modules/README.md).

## Validation documentation

Keep test scope and limitations next to reproduction commands:
[audio](validation/audio/README.md), [modules](validation/modules/README.md),
[transport/providers/clocks](validation/transport/README.md),
[orchestration/full duplex](validation/orchestration/README.md).
External provider checks are separate, explicit operations. Do not rerun them
merely to validate documentation, and do not overwrite historical source hashes
to make an older report appear current.
