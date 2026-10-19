# Module 8 validation

Run from the monorepo root using the Python environment containing the gateway's
reference dependencies and a cached Rust toolchain:

```sh
data-plane/.venv/bin/python vogon-rust/validation/front/run.py --sockets --checks
```

`--sockets` permits loopback HTTP/WebSocket mocks only. It runs the module-8 tests
and existing transport/provider/clock/audio-runtime tests, excluding `live_` tests.
Without it, only deterministic front lifecycle/line tests run alongside Python
parity. `--checks` adds formatting, Clippy (`-D warnings`, new crate only) and
Rustdoc broken-link checks. No API keys or external requests are needed. The runner
invalidates its success manifest at startup and writes source/binary hashes only
when every selected check succeeds.

[parity.json](results/parity.json) records 7,048 direct policy/protocol cases and
6,000 source/line snapshots: 60 source traces of 30 steps and 120 line traces of
35 steps. The Python oracle imports the existing implementations, including
controlled asynchronous line-detector requests; expected policy is not copied
into the harness. Artifact cases invoke the actual Python YAML loader. Phrase
pools use single entries for exact selection comparisons; multi-entry rotation
invariants are tested natively rather than assuming identical random generators.

[native.json](results/native.json), [manifest.json](results/manifest.json) and
[python-reference.xml](results/python-reference.xml) record the native and Python
reference results. The Python suite has one pre-existing expected failure:
misheard-brand partial echo can reach the instrument. This is preserved and
explicitly tested in Rust, not counted as a fixed defect. The reference tests
and differential corpus establish behavior on these cases, not exhaustive proof
for arbitrary speech, networks or schedules.

## Case matrix

| Area | Cases | Evidence |
|---|---|---|
| Guards | Every retained lexicon, reason order, multiple sentences, lengths, numbers, dates, questions, names/domain boundaries, one-moment exceptions, repeat/openers, Unicode | `parity.py: guards` (734 cases) |
| Move/rider policy | All 14 move labels including unknown × 128 rider combinations × 3 confidence values; missing/empty pools, repeat precedence, desk context | `parity.py: picks` (5,379 cases) |
| Questions/answers | Configured riders, invalid choices, choice/noul/score, missing/wrong kinds, usage, stable probability ties and nonfinite rejection | `parity.py: protocol`, `tests/clients.rs` |
| Artifact loading | Valid, empty and whole-pool rejection, whitespace, beat/tier caps, missing files and invalid structure | 50 actual-loader comparisons; `tests/lifecycle.rs` |
| Echo stripping | Full/partial/no echo, Unicode, consecutive-run boundaries; known misheard-brand limitation | 771 comparisons; `tests/line.rs` |
| Source state | History/prompts, withheld names, admission gap, played ledger, sticky distress, repeat disabled, idempotent prime, stale result rejection, falsey JSON | 1,800 snapshots; `tests/lifecycle.rs` |
| Source timing/ownership | Model versus empty deadlines, deliberate silence, missing client fallback, superseding/cancelling waiters, dropping in-flight HTTP owner | Paused Tokio lifecycle tests; real loopback client/source tests |
| Filler holds | Disabled/duplicate start, early end, primary/continuations/max, IDs, no/zero phrases, two-line source limit, gap, truncation, failed speech, repeated end, cancellation-safe end, owner drop | `tests/lifecycle.rs`; Python `test_call_clocks.py` |
| Line policy | Disabled/short/echo observations, inclusive machine/human boundaries, final during ask, acoustic episode changes, coalescing/duplicate asks, failed/late result, ask-window/extension, quiet/cap/release | 4,200 snapshots; `tests/line.rs` |
| Jev HTTP | Bearer/body shape; every retried status and representative non-retried status; five-attempt/backoff budget; bad JSON/schema; header/body timeout; connection failure; size cap; cancel during request/backoff; one trace | `tests/clients.rs`, loopback fixture |
| Cerebras HTTP | Model/messages/JSON mode/tokens/temperature/effort; null/missing/malformed content; no retry; CONTINUE/YIELD/UNCERTAIN and invalid verdict | `tests/clients.rs` |
| Transport integration | Automatic judge completion; clear before final; protected-opening machine breakthrough; verdict before replayed final; updated generation; missing instrument unknown; shutdown cancellation; original episode after eviction/delayed final; zero-audio retirement | `crates/transport/tests/integration.rs` |
| Regression | Existing native transport/speech/audio lifecycle tests; unchanged module 1–7 differential suites | Runner with `--sockets`; module/transport runners below |

Separate regression runners used during this implementation:

```sh
data-plane/.venv/bin/python vogon-rust/validation/modules/run.py --stage all
data-plane/.venv/bin/python vogon-rust/validation/transport/run.py
```

Those preserve 1,732 contracts/configuration, 2,390 playback and 1,828 barge-in
cases; 9,780 STT snapshots, 120 TTS traces, 15,100 clock snapshots, 1,200 transport
callbacks, 100 wire cases and 960 audio/gate plus 960 ambience frames. They also
check the accepted standard-whitespace differences separately. Audio algorithms
were not changed in module 8; no performance claim is made.
