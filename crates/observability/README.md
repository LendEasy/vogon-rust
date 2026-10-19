# Bounded operational metrics

`Metrics` in `src/lib.rs` owns process-level atomic aggregates and fixed histogram
buckets. `Event` is the closed vocabulary: callers cannot supply unbounded label
values. `observe` classifies diagnostics without retaining payloads, `frame`
records scalar RMS summaries, `turn` records completed request timing, and
`prometheus` renders a snapshot. `log` emits fixed JSON event/error categories.

These aggregates never participate in call decisions or advance `CallClock`.
The service owns one metrics instance; capture/HTTP clients receive shared handles.
The concurrency test verifies 8,000 events across eight threads, fixed histogram
buckets and absence of transcript text. These metrics are not production load
benchmarks or acoustic acceptance measurements. See [operations](../../OPERATIONS.md).
