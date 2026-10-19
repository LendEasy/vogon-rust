"""Byte-exact comparisons of native Rust ambience DSP against existing Python."""
import dataclasses
import hashlib
import json
from pathlib import Path
import random
import struct
import sys
import tempfile
import wave

import audioop
import pytest
import vogon_audio_core as rust
from vogon.gateway.transport.ambience import mixer, sources, loader

ROOT = Path(__file__).resolve().parent
MONOREPO = ROOT.parents[2]
ROWS = []


def compare_source(name, python, native):
    assert python.sample_rate == native.sample_rate
    assert len(python.clips) == len(native.clips)
    for i, (p, r) in enumerate(zip(python.clips, native.clips)):
        assert p.pcm == r.pcm, f"source mismatch: {name} clip {i}"
        ROWS.append({"name": f"{name}/{i}", "samples": p.n_samples,
                     "sha256": hashlib.sha256(r.pcm).hexdigest(), "passed": True})


def source_to_rust(source):
    if source is None: return None
    return rust.Source([rust.Clip(c.pcm, c.sample_rate) for c in source.clips], source.sample_rate)


def state(p):
    return {"room_cursor": p._room_cursor, "gain": p._amb_gain, "target": p._amb_target,
            "typing_state": p._typing_state.value, "typing_index": p._typing_clip_idx,
            "typing_pos": p._typing_pos, "typing_gain": p._typing_gain, "typing_target": p._typing_target,
            "shot_index": p._oneshot_clip_idx, "shot_pos": p._oneshot_pos, "shot_active": p._oneshot_active}


def differential():
    frames = samples = 0
    for rate in (8000, 16000, 24000, 48000):
        for seed in (0, 1, -1, 0x51EED):
            compare_source(f"room/{rate}/{seed}", sources.build_synthetic_room_tone(rate, seed), rust.build_synthetic_room_tone(rate, seed))
            compare_source(f"typing/{rate}/{seed}", sources.build_synthetic_typing(rate, seed), rust.build_synthetic_typing(rate, seed))
            compare_source(f"oneshots/{rate}/{seed}", sources.build_synthetic_oneshots(rate, seed), rust.build_synthetic_oneshots(rate, seed))
        rng = random.Random(20261018 + rate)
        def bank(lengths):
            return sources.Source([sources.Clip(struct.pack(f"<{n}h", *[rng.randint(-32768, 32767) for _ in range(n)]), rate) for n in lengths], rate)
        for mode in ("all", "none", "empty", "room", "typing", "shots"):
            room = bank([0 if mode == "empty" else 317]) if mode in ("all", "empty", "room") else None
            typing = bank([0, 1, 200, rate // 2]) if mode in ("all", "empty", "typing") else None
            shots = bank([0, 300, 2000]) if mode in ("all", "empty", "shots") else None
            for cfg in (mixer.AmbienceConfig(), mixer.AmbienceConfig(room_gain=1.7, duck_factor=.17, typing_gain=1.3, oneshot_gain=2.1),
                        mixer.AmbienceConfig(room_gain=0, duck_factor=1, typing_gain=0),
                        mixer.AmbienceConfig(duck_ramp_ms=37, typing_fade_ms=83)):
                p = mixer.AmbienceMixer(rate, room, typing, shots, cfg)
                r = rust.AmbienceMixer(rate, source_to_rust(room), source_to_rust(typing), source_to_rust(shots), rust.AmbienceConfig(**dataclasses.asdict(cfg)))
                digest = hashlib.sha256()
                for i in range(250):
                    if i % 13 == 0: p.set_speaking(i % 26 == 0); r.set_speaking(i % 26 == 0)
                    if i % 17 == 0: p.start_typing(i // 17 - 2); r.start_typing(i // 17 - 2)
                    if i % 19 == 0: p.stop_typing(); r.stop_typing()
                    if i % 23 == 0: p.trigger_oneshot(i // 23 - 2); r.trigger_oneshot(i // 23 - 2)
                    n = (0, 1, 7, 159, 160, 161, rate // 50, 1024)[i % 8]
                    count = max(0, n + (-10, 0, 20)[i % 3])
                    tts = struct.pack(f"<{count}h", *[rng.randint(-32768, 32767) for _ in range(count)]) if i % 4 else None
                    if tts is not None and i % 7 == 0: tts += b"\x01"  # incomplete sample ignored as in reference
                    expected, actual = p.next_frame(n, tts), r.next_frame(n, tts)
                    assert actual == expected, (rate, mode, dataclasses.asdict(cfg), i)
                    assert json.loads(r.state_json()) == state(p), (rate, mode, i, state(p), r.state_json())
                    digest.update(actual)
                    frames += 1; samples += n
                ROWS.append({"name": f"mixer/{rate}/{mode}/{dataclasses.asdict(cfg)}", "frames":250,
                             "sha256":digest.hexdigest(), "passed": True})
    return frames, samples


def wav_checks():
    with tempfile.TemporaryDirectory(prefix="vogon-rust-wav-") as temp:
        root = Path(temp)
        rng = random.Random(7)
        for width in (1, 2, 3, 4):
            for channels in (1, 2):
                for rate in (8000, 24000, 44100):
                    path = root / f"clip-{width}-{channels}-{rate}.wav"
                    pcm16 = struct.pack("<2002h", *[rng.randint(-32768, 32767) for _ in range(2002)])
                    pcm = audioop.lin2lin(pcm16, 2, width)
                    with wave.open(str(path), "wb") as stream:
                        stream.setnchannels(channels); stream.setsampwidth(width); stream.setframerate(rate)
                        stream.writeframes(pcm)
                    for target in (8000, 24000):
                        compare_source(f"wav/{path.name}/{target}", sources.build_wav_source([path], target), rust.build_wav_source([path], target))
        # Explicit asset directory preserves WAV-over-synthetic selection.
        for name in ("room_tone2.wav", "room_tone1.wav", "typing1.wav", "oneshot_1.wav"):
            with wave.open(str(root / name), "wb") as stream:
                stream.setnchannels(1); stream.setsampwidth(2); stream.setframerate(8000)
                stream.writeframes(struct.pack("<317h", *([1234] * 317)))
        old_assets = loader._ASSETS
        try:
            loader._ASSETS = root
            p = loader.build_mixer(8000, enable_oneshots=True)
            r = rust.build_mixer(8000, enable_oneshots=True, assets_dir=root)
            p.start_typing(0); r.start_typing(0)
            p.trigger_oneshot(0); r.trigger_oneshot(0)
            for _ in range(20): assert p.next_frame(160) == r.next_frame(160)
            ROWS.append({"name":"asset_selection_and_mix", "passed":True})
        finally:
            loader._ASSETS = old_assets


class Outcomes:
    def __init__(self): self.rows = []
    def pytest_runtest_logreport(self, report):
        if report.when == "call" or report.failed or report.skipped:
            self.rows.append({"test":report.nodeid, "outcome":report.outcome,
                              "detail":str(report.longrepr) if report.failed else None})


def main():
    frames, samples = differential()
    wav_checks()
    # Run the existing behavioral tests with the Rust classes/builders.
    for name in ("Clip", "Source", "build_synthetic_room_tone", "build_synthetic_typing", "build_synthetic_oneshots", "build_wav_source"):
        setattr(sources, name, getattr(rust, name))
    for name in ("AmbienceMixer", "AmbienceConfig", "TypingState"):
        setattr(mixer, name, getattr(rust, name))
    loader.AmbienceMixer = rust.AmbienceMixer
    loader.AmbienceConfig = rust.AmbienceConfig
    outcomes = Outcomes()
    results = ROOT / "results"; results.mkdir(exist_ok=True)
    code = pytest.main([str(MONOREPO / "voice-gateway/tests/test_ambience_mixer.py"), "-q", "--tb=short",
                        f"--junitxml={results / 'mixer.xml'}"], plugins=[outcomes])
    sys.stdout.flush()
    doc = {"test_exit_code":int(code), "tests":outcomes.rows, "differential_checks":ROWS,
           "frames_byte_identical":frames, "samples_byte_identical":samples,
           "scope":"Native Rust source generation/mixing vs Python; retained wave/audioop asset decoding; no timing"}
    (results / "mixer.json").write_text(json.dumps(doc, indent=2))
    if code: raise RuntimeError(f"Mixer tests failed: {code}")
    print(f"Mixer: {frames} byte-identical frames / {samples} samples, {len(ROWS)} differential cases", flush=True)


if __name__ == "__main__": main()
