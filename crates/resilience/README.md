# Configurable exponential retries

`Policy` owns a bounded attempt budget and exponential delay calculation;
`Policy::run` applies it only to failures classified transient by the caller.
`Policies` contains immutable settings for each dependency, resolved once by
service bootstrap. Neither type owns a call clock or a shared attempt counter.

`src/lib.rs` documents resolution, jitter and cancellation. Tests verify exact
paused-time delays, caps, permanent failures, cancellation and jitter bounds.
AWS and Redis connection recovery use their native retry implementations with
mapped budgets. See [operations](../../OPERATIONS.md#retry-configuration) for
environment precedence, retry boundaries and non-replayable operations.
