# Contracts

Native Rust implementation of the Turn API wire boundary. No Python runtime or
gateway imports are used by this crate.

- `types.rs`: all 33 contract structs and tagged transport/post-call event enums.
- `codec.rs`: recursive normalization, unknown-field removal, defaults, event
  dispatch, typed conversion and nonzero timestamp rebasing.
- `schema.json`: checked-in field/default metadata, generated from the reference.

Use `codec::to_wire(type_name, &value)` for concrete events as well as requests;
it adds their `kind` tags. Use `codec::from_wire::<T>` for typed internal messages.
`codec::normalize` preserves Python's permissive scalar decoding, including
explicit nulls. Typed decoding additionally requires values representable by
the Rust types; Python's dataclass codec does not enforce its annotations.
The supported wire domain is finite JSON numbers, with i64 typed integer fields.

Regenerate declarations after an intentional reference schema change:

```sh
data-plane/.venv/bin/python vogon-rust/tools/generate_contracts.py
```

The generator is a development tool, not a build/runtime dependency. Generated
source hashes live in `tools/generated-sources.json`. Tests independently call
the Python codec rather than using the generated schema as their expected output.
See [module validation](../../validation/modules/README.md).
