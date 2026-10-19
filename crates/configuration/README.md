# Configuration

Native Rust models for deployment `GatewayConfig`, shared `GatewaySettings`,
and `TenantProfile`, plus tenant overlays and call-snapshot resolution.

The model accepts explicit initializer values, a process-environment mapping
and a parsed dotenv mapping. Precedence is initializer > environment > dotenv
> defaults. Environment names are case-insensitive; deployment fields use the
`GATEWAY_` prefix. The speech provider's `SpeechSettings::load` already reads an
explicit dotenv path; general service bootstrap and process initialization remain
pending. Tenant model loading ignores environment/dotenv inputs.

Settings coercion and rejection are compared with installed Pydantic. Validation
error wording is not a Pydantic-compatible API. Rust accepts finite JSON values
and i64 integers; non-finite floats and arbitrary-precision Python integers are
outside this boundary. Unknown settings/profile fields are ignored, matching
the running Python models even where an old docstring says they are rejected.

Tenant snapshots retain the existing precedence and truthiness behavior,
including the default `CallConfig.language` taking precedence over the profile,
and filler being enabled when either a call filler exists or the profile allows
it. No defaults were retuned during the port.

`read_overrides` uses built-ins for missing files and fails on an unreadable or
malformed existing file. `resolved_front_config_path` resolves lexically without
requiring the file to exist. Models/default metadata are regenerated with
`tools/generate_contracts.py`; role comments are maintained in that generator.
The generated settings types contain plain credential strings, so their full
Debug/serialized values must not be logged. `ApiKey` redaction belongs to the
speech-provider boundary. See [validation](../../validation/modules/README.md).
