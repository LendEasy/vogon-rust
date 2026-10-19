# Contributing

Contributions use Apache-2.0 and the
[Developer Certificate of Origin 1.1](https://developercertificate.org/), matching
[Vogon's contribution model](https://github.com/LendEasy/vogon/blob/main/CONTRIBUTING.md).
Sign off commits with `git commit -s`.

Start with [build instructions](README.md), [module status](MODULES.md) and
[documentation conventions](DOCUMENTATION.md). Keep one logical module or change
per commit and describe the tests that verify it. Document intentional differences
from Python, including failure semantics and call-local state ownership.

Run the affected native tests, formatting, scoped Clippy and Rustdoc checks.
The standalone native suite does not need the private application repository.
Python differential runners are separate migration tools; see
[validation prerequisites](validation/README.md).

Do not weaken assertions, hide failed acoustic cases or change numerical/DSP
behavior just to make parity pass. Do not commit credentials, call recordings,
transcripts, generated logs, model artifacts or native build products. Live
provider tests must remain opt-in; ordinary CI must not make paid requests.

The publication history imports existing modules in dependency order. It is an
initial import of their tested state, not a reconstruction of development dates.
Modules 11–13 and the published acceptance gaps remain open work.
