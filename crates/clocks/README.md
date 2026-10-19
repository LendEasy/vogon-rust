# Clocks

`Clock` owns a monotonic epoch shared by a call's providers and adapters.
`stream_to_monotonic` projects provider word times; `OffsetEstimator` implements
the nonnegative mark-delay EMA.

`call::CallClocks` is a deterministic state machine. It owns duration thresholds,
silence ladders, accumulated idle time, nested pauses, hold grace, overlapping
playout depth and the lost-mark watchdog. It performs no sleeping or I/O.

The transport calls `tick` on a one-second cadence and drives floor deadlines
separately every 50 ms. Ordinary playout pauses silence; real-turn completion may
reset the window. Silence probes preserve accumulated idle time. Caller activity
resets idle, fired silence thresholds and grace. A reasoning pause is capped at
30 seconds; missing playback marks use known audio length plus 10 seconds, or
90 seconds when length is unknown. Overlapping playout retains the longest
outstanding deadline.

The actual Python clock engine matched 100 deterministic randomized traces /
15,100 state snapshots, including threshold events. The asynchronous
front-model/canned-filler hold loop lives in [module 8](../front/README.md),
separately from this pure clock state. [Module 9](../orchestration/README.md)
connects reasoning pause/resume and hold lifetime to the application Turn API loop.

`Clock::now` is read-only. Each call owns its own `CallClocks` and offset counters;
there are no mutable static counters. Cloned host clocks share only an immutable
epoch. Late backwards ticks are ignored so they cannot rewind elapsed accounting.
Six native clock/transport isolation tests cover concurrent reads, 16 independent
call traces, competing tick producers, duplicate ticks, pause/playout/watchdog
isolation and call-local threshold/event sequences. See
[the isolation audit](../orchestration/PERFORMANCE.md#clock-and-counter-audit).
The [clock interface diagram](../orchestration/CALL_FLOW.md#clock-interface-and-isolation)
shows how Tokio wakeups, read-only timestamps and per-call accounting connect.
The [timer inventory and timelines](../orchestration/CALL_FLOW.md#timers-deadlines-and-timelines)
show recurring ticks, EOT/filler/barge deadlines, provider timeouts and shutdown
budgets, including each timer's arm point and cancellation behavior.
