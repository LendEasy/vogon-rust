# Known issues and acceptance limits

Modules 1–13 have implemented cores. Local tests do not close production acceptance
or the live-call findings deferred during modules 11–13.

- Browser playback completion is not fully connected to silence-ladder timers.
  Phone timer and parallel-call isolation checks pass.
- Equal eager/final EOT thresholds in the tested deployment configuration
  finalized the first utterance before the intended continuation. Controlled
  EOT tests passed; production settings still need acceptance.
- AEC has known cold-start/changing-path leakage. Initial live echo injection
  produced a false caller turn; a later stricter run passed. Reliability remains
  unresolved, and passing byte parity does not prove acoustic quality.
- Provider audio/event buffers need bounded backpressure. Current tests cover
  up to four live-provider calls, not production capacity or long-duration leaks.
- Durable Turn API fact acknowledgement, long-call lease renewal, crash recovery
  and ambiguous outbound dial reconciliation remain incomplete.
- Transfers and phone recording/artifacts are wired, with local protocol and
  Python parity tests. Real telephone transfer acceptance and real AWS permission/
  deployment checks remain. Missing artifacts return 404; implemented routes no
  longer use the former 501 placeholders.
- The dependency policy fails on RUSTSEC-2024-0449: unmaintained `unsafe-libyaml`,
  pulled in by the existing `serde_yaml_ng` front configuration parser. No ignore
  is configured; dependency replacement/review is a deferred release requirement.
- The deployment image and GitHub workflows are added but have not completed
  container/hosted CI acceptance. The local Docker build stopped when extracting
  the builder image exhausted root-disk space.
- Recording retention is bounded, but sparse four-hour timelines can expand to
  roughly 460 MB per WAV. Concurrent blocking finalizers have no separate semaphore;
  production memory sizing and finalizer concurrency limits remain outstanding.
- GPT Live acceptance is deferred. Legacy gated STT and alternate phone playback
  modes are not accepted by the default snapshot adapter.
- Native audio builds currently support Linux without cross compilation.
  NumPy/audioop still run through embedded CPython; the GIL remains relevant.
- Python differential runners retain migration-layout dependencies. Native Cargo
  tests and the echo example run independently; see [validation](validation/README.md).

See [MODULES.md](MODULES.md) for the complete checklist and
[live-test summary](validation/live/REPORT.md) for the observed outcomes.
