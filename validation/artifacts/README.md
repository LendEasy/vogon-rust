# Artifact correctness gate

Build `artifact-validation`, then run `python validation/artifacts/compare.py`
with the sibling Python gateway importable. The script constructs both recorders
from identical deterministic actions and compares raw WAV bytes, projected times,
clear counts, earliest origin and rebased event rows. It directly executes the
Python recorder and event log; expected samples are not copied into Rust tests.

The 162 cases include reordered chunks, gaps, overlaps, partial frames, repeated
clears, missing/invalid timestamps, jitter, drift reanchors, empty recordings and
complete discard. Native tests additionally reject nonfinite clocks and enforce
resource bounds, recording policy, storage failure behavior, signed S3 retry and
call/tenant isolation. Service phone tests prove the actual observer wiring reaches
the saved WAV, frame CSV and playback event log.

Run native tests with local socket access; all network endpoints are local mocks.
AWS credentials in fixtures are dummy strings. No GPT Live or paid provider calls.
