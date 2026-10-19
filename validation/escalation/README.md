# Module 11 gate

Run `cargo test --locked -p vogon-escalation -p vogon-service`, build
`escalation-validation`, then run `python validation/escalation/compare.py`.
The Python source is loaded from the sibling `voice-gateway` checkout.

Validated locally: 14 native transfer tests, 80 canonical XML comparisons,
27 service tests and 228 service comparisons. The orchestration regression gate
passed 134 native tests and 34 Python tests (two existing expected failures).
Python's transfer suite passed all 76 tests using isolated test dependencies
in `/tmp/vogon-validation-deps`; these are not runtime or repository dependencies.

Native tests use local HTTP provider mocks and an isolated real Redis server.
They test callback-before-REST races, duplicate keypress/terminal callbacks,
stale attempt rejection, late-leg cancellation, concurrent call isolation,
report retry identity, render cleanup, fallbacks and ambiguous dial failure.
Python lifecycle tests and Rust lifecycle tests are separate suites; the direct
differential comparison covers XML generation, not every network interleaving.
Telephone/provider acceptance and crash-recoverable report delivery remain open.
