# Known issues and acceptance limits

Modules 1–10 have implemented cores. This initial public import preserves their
current behavior; it does not close the remaining migration work.

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
- Transfers (module 11), recording/artifacts (12), production observability,
  packaging and operational acceptance (13) remain pending. Unsupported transfer
  and artifact routes return 501.
- GPT Live acceptance is deferred. Legacy gated STT and alternate phone playback
  modes are not accepted by the default snapshot adapter.
- Native audio builds currently support Linux without cross compilation.
  NumPy/audioop still run through embedded CPython; the GIL remains relevant.
- Python differential runners retain migration-layout dependencies. Native Cargo
  tests and the echo example run independently; see [validation](validation/README.md).

See [MODULES.md](MODULES.md) for the complete checklist and
[live-test summary](validation/live/REPORT.md) for the observed outcomes.
