# CPU and memory: Rust gateway and Python gateway

Measured on 2026-11-17. The Rust gateway used 17–32% less CPU per call than the
Python reference gateway (Vogon) and about half the base memory. Both gateways are
cheap once the math-library thread limit is set; without it, either one costs more
than a full core per call.

These are measurements from one development host, not a production-capacity claim.

## What was measured

- **Calls.** Simulated phone calls over the Twilio media protocol: 20 ms mu-law
  frames sent in real time, live Deepgram speech-to-text and text-to-speech, and a
  real Turn service with a tool-calling agent behind it. No telephone call is placed.
- **Gateways.** Each gateway ran as its own process: the Python gateway's service,
  and this repository's `service-live-host` built natively with `--release`.
- **Metric.** The gateway process's CPU time (user plus system, every thread) per
  second, as a percentage of one core, sampled each second from `/proc`. Memory is
  the process's peak resident set size.
- **Settings, the same on both.** `OPENBLAS_NUM_THREADS=1` and `OMP_NUM_THREADS=1`,
  front (filler) model off, ambience off, speech providers reached directly.
- **Host.** 16-core x86-64 Linux. The Turn service, the agent and the simulated
  callers ran on the same machine.

## Under simultaneous calls

Every call played the same seven-line conversation, 90–115 seconds long. Calls
started half a second apart on one gateway process. CPU is taken over the window in
which all calls were live.

| Simultaneous calls | Python: mean / p95 / peak | Rust: mean / p95 / peak | Rust vs Python (mean) |
|---:|---|---|---|
| 1 | 2.5% / 4% / 7% | 1.7% / 3% / 3% | 32% lower |
| 5 | 9.6% / 14% / 21% | 7.1% / 11% / 12% | 25% lower |
| 20 | 28.4% / 38% / 44% | 23.6% / 31% / 34% | 17% lower |

Cost of one call among N (the mean divided by N), and peak memory:

| Simultaneous calls | Python CPU per call | Rust CPU per call | Python peak memory | Rust peak memory |
|---:|---:|---:|---:|---:|
| 1 | 2.51% | 1.71% | 113 MB | 51 MB |
| 5 | 1.91% | 1.43% | 158 MB | 85 MB |
| 20 | 1.42% | 1.18% | 276 MB | 210 MB |

All 56 calls spoke their full script with no error rows in the call log.

- **Both scale better than linearly.** Twenty calls cost about 11 times one call on
  Python and 14 times on Rust.
- **The Python gateway runs on one core.** It has two or three threads and a single
  event loop, so its figures are fractions of the only core it can use. The Rust
  gateway spread across 19 threads at one call and 40 at twenty.
- **Memory.** Each run started a fresh process on both sides. Rust's base is less
  than half of Python's; each added call costs roughly 8 MB on either.

## One call at a time

Five different conversations, run one after another, about ten minutes of call time
per gateway.

| | Python | Rust | Rust vs Python |
|---|---:|---:|---|
| Mean over all call time | 2.58% | 1.72% | 33% lower |
| CPU seconds per minute of call | 1.55 | 1.03 | 33% lower |
| While agent audio plays | 3.48% | 2.34% | 33% lower |
| While the caller speaks | 1.54% | 0.69% | 55% lower |
| While the line is quiet | 1.60% | 1.01% | 37% lower |
| One-second samples: median / p95 / peak | 3% / 4% / 10% | 2% / 3% / 4% | |

## The math-library thread limit

The echo canceller's alignment check is NumPy code, which both gateways run (this
one through PyO3). With the BLAS library's default of one worker thread per core,
the workers spin between 20 ms audio frames. Measured on the same five
conversations without the limit:

| | Python | Rust |
|---|---:|---:|
| Mean over all call time, no limit | 96% | 101% |
| While agent audio plays, no limit | 160% | 159% |
| Mean over all call time, limit of 1 | 2.58% | 1.72% |

The echo canceller alone, fed one frame every 20 ms offline, cost 186% of a core
with the default and 1.2% with one thread. Set both variables to `1` for any
deployment of either gateway; this repository's
[service unit and install script](deploy/al2023/README.md) already do.

## Latency is not reported

Latency figures were removed from this report. The runs used a slower agent model
than production will, and the agent's model time dominates the time from the end of
a caller's line to the first agent audio. Production uses a fast model, so the
numbers measured here do not represent what a real call will look like.

## Not measured

- More than 20 simultaneous calls, or where either gateway saturates.
- The front (filler) model and ambience switched on.
- Long-running processes: memory over hours of calls.
- A deployment host; see the [operations guide](OPERATIONS.md) for the remaining
  acceptance gates.
