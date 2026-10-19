# Module parity validation

Run from the monorepo root:

```sh
data-plane/.venv/bin/python vogon-rust/validation/modules/run.py --stage all
```

`--stage contracts` selects contracts/configuration; `--stage playback` selects
playback; `--stage barge` selects barge-in/floor control and its native lifecycle
tests. The runner builds the native `vogon-gateway-validation` binary with
offline locked Cargo dependencies. JSON lines are only a test adapter; runtime
business logic is inside the crates. No Python interpreter is embedded in this
binary, and no wrappers are timed.

| Module | Current result |
|---|---|
| Contracts/configuration | 1,732 parity cases passed |
| Playback | 2,390 parity cases passed |
| Barge-in/floor | 1,828 parity cases and five native lifecycle tests passed |

Contract coverage includes all types, required/omitted fields, explicit nulls,
defaults, unknown fields, tagged unions, nested structures, typed roundtrips,
opaque payloads, clock rebasing and selected permissive scalar behavior.
Configuration coverage includes all 112 fields, coercion and invalid-value
rejection, initializer/environment/dotenv precedence, typed defaults, tenant
overlays, snapshots, auth enablement and path resolution. Error acceptance is
compared; Pydantic error text is not reproduced.

Playback uses the actual Python splitter, delivery reports, queue function,
heard-horizon function and Twilio sentence/mark/clear methods as oracles.
Fake provider chunks and an injected clock make outputs deterministic. Tests
compare media bytes, mark order, pacing waits, sentence counts, heard text and
receipt state. External clients and network sends are stubbed.

Barge-in compares the actual Python gate, semantic functions and floor
controller using an injected clock and controlled asyncio classifier futures.
Checks include complete state, ordered effects/logs, classifier requests and
stale/late rejection, admission holds, protected spans, echo, energy-only blips,
resumed speech and silence release. All 16 existing gate tests execute with
Rust-returned verdicts, and 16 gate traces use retained WebRTC VAD results.
See the [host integration contract](../../crates/barge-in/README.md) for the
explicit timer, request and effect boundaries still to connect in the transport.

Results contain each case's outcome and digest, plus source/binary hashes:
[contracts](results/contracts.json), [playback](results/playback.json),
[barge-in/floor](results/barge.json).
Text operations use Rust standard Unicode whitespace. The parity corpus covers
spaces, tabs, newlines and standard Unicode spaces. U+001C–U+001F are deliberately
not whitespace in Rust: separate checks exercise all four characters in
configuration, sentence boundaries and barge semantics. Reports store their
Python/Rust outputs under `intentional_differences`, outside parity counts.
The old synthetic control-separator parity fixtures now use standard Unicode
spaces; this is an explicit narrowing of the Python compatibility contract.

Passing cases describe the tested finite-JSON domain, not arbitrary Python
objects, unbounded integers or non-finite values. Native queue/lifecycle tests
for `audio-runtime` are run separately with Cargo; its concurrent differential
checks are in `validation/audio/runtime_checks.py`.

The old audio corpus remains a regression gate whenever shared audio code changes:

```sh
data-plane/.venv/bin/python vogon-rust/validation/audio/run.py --stage all
```
