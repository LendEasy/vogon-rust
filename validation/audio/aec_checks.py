"""Run the existing acoustic requirements with Rust/Python paired per frame."""
import hashlib
import json
import logging
from pathlib import Path
import sys

import pytest
import numpy as np
import audioop
import webrtcvad
import vogon_audio_core as rust
from vogon.gateway.transport import aec

ROOT = Path(__file__).resolve().parent
MONOREPO = ROOT.parents[2]
OriginalAEC = aec.AECProcessor
INSTANCES = []
PARITY_FAILURES = []


def state(p):
    return {"delay": p._delay_frames, "estimated": p._delay_estimated, "slip": p._alignment_slip,
            "fifo": len(p._ref_fifo), "history": len(p._shift_history), "near_window": len(p._realign_near),
            "since_check": p._frames_since_realign_check, "gcc_ref": len(p._gcc_ref),
            "gcc_near": len(p._gcc_near), "near_power": p._near_power_sum, "out_power": p._out_power_sum,
            "erle_frames": p._erle_frames, "total_frames": p._total_frames,
            "realignments": [list(x) for x in p.realignments]}


class PairedAEC:
    """Only the test harness delegates to Python. Production state is Rust."""
    def __init__(self):
        self.python = OriginalAEC()
        self.rust = rust.AECProcessor()
        self.frames = 0
        self.feeds = 0
        self.clears = 0
        self.digest = hashlib.sha256()
        INSTANCES.append(self)

    def check_state(self):
        actual = json.loads(self.rust.state_json())
        # The reference catches alignment math-domain errors on silent windows.
        # They are diagnostics, not failed calls: compare resulting state/output.
        actual.pop("errors")
        expected = state(self.python)
        differences = {k: (expected[k], actual[k]) for k in expected if expected[k] != actual[k]}
        if differences: PARITY_FAILURES.append(differences)
        assert not differences, differences

    def feed_reference(self, frame):
        self.python.feed_reference(frame)
        self.rust.feed_reference(frame)
        self.feeds += 1
        self.check_state()

    def process(self, frame):
        expected = self.python.process(frame)
        actual = self.rust.process(frame)
        if actual != expected: PARITY_FAILURES.append(f"AEC output mismatch at frame {self.frames}")
        assert actual == expected, f"AEC output mismatch at frame {self.frames}"
        self.frames += 1
        self.digest.update(actual)
        self.check_state()
        return actual

    def discard_reference(self):
        self.python.discard_reference()
        self.rust.discard_reference()
        self.clears += 1
        self.check_state()

    def log_erle_now(self):
        self.python.log_erle_now()
        self.rust.log_erle_now()
        self.check_state()

    def is_converged(self):
        assert self.python.is_converged() == self.rust.is_converged()
        return self.rust.is_converged()

    def __getattr__(self, name):
        return getattr(self.rust, name)


class Outcomes:
    def __init__(self):
        self.rows = []

    def pytest_runtest_logreport(self, report):
        if report.when == "call" or report.failed or report.skipped:
            self.rows.append({"test": report.nodeid, "phase": report.when, "outcome": report.outcome,
                              "expected_failure": getattr(report, "wasxfail", None),
                              "detail": str(report.longrepr) if report.failed else None})


def main():
    logging.getLogger("vogon.gateway.transport.aec").setLevel(logging.WARNING)
    aec.AECProcessor = PairedAEC
    outcomes = Outcomes()
    results = ROOT / "results"
    results.mkdir(exist_ok=True)
    tests = MONOREPO / "voice-gateway/tests/aec"
    exit_code = pytest.main([str(tests), "-q", "--tb=short", f"--junitxml={results / 'aec.xml'}"], plugins=[outcomes])
    sys.stdout.flush()
    extra = []
    if exit_code == 0:
        # Arbitrary inbound/reference lengths exercise Python's exact pad/clamp
        # semantics. Two calls interleave to expose accidental shared state.
        calls = [PairedAEC(), PairedAEC()]
        for i in range(120):
            for j, call in enumerate(calls):
                size = (0, 1, 159, 160, 161, 320)[(i+j) % 6]
                data = bytes((n * 17 + i + j) % 256 for n in range(size))
                call.feed_reference(data)
                call.process(data)
                if i % 23 == 0: call.discard_reference()
                if i % 11 == 0: call.log_erle_now()
        extra.append({"name": "interleaved_calls_and_frame_padding_truncation", "passed": True})
        p = rust.AECProcessor()
        p.close(); p.close()
        try:
            p.process(b"\xff" * 160)
        except RuntimeError:
            extra.append({"name": "closed_handle_rejected_and_close_idempotent", "passed": True})
        else:
            raise AssertionError("closed native handle accepted")
    doc = {"test_exit_code": int(exit_code), "tests": outcomes.rows, "extra_checks": extra,
           "parity_failures": PARITY_FAILURES,
           "paired_processors": len(INSTANCES), "frames_byte_identical": sum(p.frames for p in INSTANCES),
           "reference_feeds": sum(p.feeds for p in INSTANCES), "clear_events": sum(p.clears for p in INSTANCES),
           "runs": [{"frames": p.frames, "output_sha256": p.digest.hexdigest(), "state": json.loads(p.rust.state_json())} for p in INSTANCES],
           "libraries": {"numpy": np.__version__, "numpy_path": np.__file__, "audioop_path": audioop.__file__,
                         "webrtcvad": webrtcvad.__version__},
           "scope": "Synthetic acoustic requirements plus byte-exact output and per-event state parity; no provider calls or performance measurement"}
    (results / "aec.json").write_text(json.dumps(doc, indent=2))
    for p in INSTANCES: p.rust.close()
    assert not PARITY_FAILURES, PARITY_FAILURES
    if exit_code: raise RuntimeError(f"AEC validation failed: {exit_code}")
    print(f"AEC: {doc['frames_byte_identical']} identical frames across {len(INSTANCES)} paired processors", flush=True)


if __name__ == "__main__":
    main()
