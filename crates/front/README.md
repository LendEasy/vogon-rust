# Front responses and line detection

Module 8 implements the gateway's short waiting responses, semantic decision
clients, filler scheduling and first-turn line detector. Domain wording comes
from the existing YAML artifact. Provider calls use native asynchronous Rust HTTP;
this crate does not import the Python gateway or its model SDKs at runtime.

| Source | Types and responsibility |
|---|---|
| `config.rs` | `Artifact` loads YAML and rejects an entire unsafe phrase pool. `Settings` reads the resolved module-1 gateway settings. |
| `bootstrap.rs` | `Clients` creates reusable Jev/Cerebras pools from injected settings and environment lookup. `Identity` supplies allowed prompt identities and the guarded borrower name. |
| `guard.rs` | Ordered shape, length, number, date, domain-word, name, commitment, action, question and repetition checks. `Config` contains explicit exclusions. |
| `moves.rs` | `Read` carries move confidence/rider probabilities; `Pick` selects a tier or reasoned silence. Question/context builders retain the Python protocol. |
| `http.rs` | `Key` redacts credentials. `Http` owns HTTPS pooling, deadlines, bounded JSON responses and sanitized errors. HTTP is allowed only for loopback fixtures. |
| `jev.rs` | `Client` owns bounded retry; `DecisionResult`/`Answer` decode choice, yes/no and score answers. `Trace` is an opt-in, call-local callback, including failure/cancellation. |
| `judge.rs` | `Client` sends JSON-mode Cerebras front and floor requests. Floor classification returns the existing module-4 result type; token validity belongs to the controller. |
| `source.rs` | `State` owns one call's history, phrase rotation, distress latch, admission gap and played ledger. `Source` owns background generation, cancellation and hold deadlines. |
| `hold.rs` | `Hold` schedules source/canned fillers through a `Speaker`. `Timing` resolves source deadlines; `Policy` reuses the Turn API's `FillerPolicy`. `Report` retains delivery count and interruption state. |
| `line.rs` | `Detector` owns deterministic observation/window/quiet state. `Request` carries a dispatch token; ordered `Effect`s apply a machine breakthrough, verdict and optional replayed final. |
| `bin/front-validation.rs` | JSON-lines adapter for differential tests against actual Python implementations. No model requests. |
| `constants.json` | Guard lexicons and prompts exported from Python by `tools/export_front_constants.py`; contains no implementation. |

## Host integration

Resolve gateway and call configuration with module 1. Construct `Clients` with
`Clients::from_settings`, providing the host's environment lookup. It does not load
`.env` itself. Jev key precedence is the configured key, `OPEN_ROUTER_API_KEY`, then
`OPENROUTER_API_KEY`. Cerebras uses the resolved `cerebras_api_key` setting. Key
values never belong in logs, source files or validation reports.

Create transport `Config::from_snapshot`; assign `clients.judge` to `config.judge`
and `clients.jev` to `config.line_client`. The enabled phone detector comes from
`CallConfig.line_detection`. Session dispatches these clients asynchronously and
returns results with their original tokens. Without a judge, the existing
`classifier_request` event/manual completion interface remains available. An
enabled detector without its instrument produces an unknown result on failure.
Browser and text adapters do not run phone line detection.

Resolve the domain artifact path in the service and load `Artifact`. Report load
errors and pass `None` to `Clients::source` to disable fronts. Pass the same host
`Clock` used by the Session. This retains Python's enablement rule: the artifact
must have a prompt and a Cerebras key must exist, including when Jev is selected.
There is no eager models-list warmup request in this port; provider connection
warmup remains a future measured latency optimization.

The module-9 orchestrator must own a `Source` and `Hold` per call. It records caller
and delivered desk context, primes generation, and calls `cancel` when speech
resumes or a turn is superseded. The history and played ledger are different:
selecting/admitting a line does not mark it played. Hold updates the ledger after
the host speech path returns, including truncated delivery, matching Python.

Start a hold with `Hold::begin(policy, source, speaker, Timing::from(&settings), seed)`.
`Session` implements `Speaker`; share it through `Arc` while running holds, then
stop holders and recover exclusive ownership for graceful `Session::close`.
End the hold before sending the desk response: `end` stops waiting/repeats but
allows a currently playing filler to finish. Truncated delivery stops the hold.
Dropping the owner aborts its task; dropping an `end` future retains ownership so
another `end` or owner drop can still clean up. Reasoning pause/resume and Turn API
commit/cancellation remain module 9's responsibility.

## Timing and compatibility

- Jev retries only connection/timeouts and HTTP 408/409/425/429/500/502/503/504,
  with 125/250/500/1000 ms waits: five attempts maximum. Request timeout is per
  attempt, as in Python. Invalid JSON/answer schemas do not retry. The wire body
  contains `model`, `state`, `questions`; timeout is a transport option.
- Cerebras front/judge calls do not retry. Front temperature/max tokens/JSON mode
  and optional reasoning effort retain their original values. Default front
  timeout follows the 900 ms beat deadline, with a 200 ms minimum; judge defaults
  to 700 ms with a 50 ms minimum.
- Default model, intentional-empty and follow-up deadlines are 900/1500/2500 ms.
  Model timeout may use a beat. Deliberate rider silence never uses a beat.
  Sticky distress clears only after a desk answer. A source emits at most its
  first line and one subsequent wait beat; the admission gap can suppress either.
- Line detection binds the first qualifying **STT turn**, retaining its original
  acoustic episode even when history evicts that episode. Revised asks coalesce
  behind one request. Machine classification can finish on an interim; human and
  overlap classifications require final evidence. Python's final-during-flight
  behavior is retained: the original ask can decide before a newer pending text.
- Machine breakthrough cuts even protected playback. Session executes clear
  before publishing the verdict and replayed final, which carries the updated
  playback generation. A late/closed token cannot issue another verdict.
- The opening-end ask window defaults to 2 s, with a 20 s extension for a bound
  unfinished turn. An outstanding request is not retired. The machine hold ends
  on external release, final-plus-700 ms quiet or an 8 s cap. No energy-based
  end-of-message detector is added.

Intentional Rust boundaries: JSON/config types are checked rather than coercing
arbitrary YAML objects to strings; numbers must be finite and fit native contract
ranges. Text uses Rust standard Unicode whitespace, consistent with the accepted
migration decision. Seeded phrase rotation preserves pool exhaustion and boundary
rules, not Python's exact random sequence. HTTP redirects are disabled and reply
bodies are capped at 4 MiB. Traces include malformed-result failures and cancellation
and are explicitly scoped per call; they contain caller text and are not persisted
unless the host opts in.

Cancellation is tightened: a hold captures its generation **before** awaiting a
model, and cancellation invalidates that generation. This closes the Python race
where a stale wait could adopt a later generation. Closing a detector explicitly
invalidates outstanding tokens. These improvements have dedicated lifecycle tests.

The existing exact-word echo-strip limitation remains: a misheard brand inside
an incomplete opening can leave four unmatched words that are sent to the line
instrument. The Python reference records this as a strict expected failure; the
Rust test explicitly records the same behavior. Live acoustic/model quality,
production concurrency and the full application call loop are separate acceptance
work. No GPT Live or external provider tests were run for this module.

Validation, cases and saved evidence: [module-8 tests](../../validation/front/README.md).
