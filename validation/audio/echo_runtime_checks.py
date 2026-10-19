"""Deterministic changing acoustic paths through the Rust per-call audio runtime.

Compare with the existing Python AEC, framer arithmetic, mixer and VAD. Measure
absolute acoustic quality independently of parity; no performance measurement.
"""
import audioop
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import sys
import wave

import numpy as np
import webrtcvad
from vogon.gateway.transport.aec import AECProcessor
from vogon.gateway.transport.ambience.loader import build_mixer
from vogon_audio_runtime import AudioRuntime

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parents[2] / 'voice-gateway/tests/aec'))
from echo_path import speech_like
sys.path.insert(0, str(ROOT))
from aec_checks import state

RATE, FRAME, SECONDS = 8000, 160, 18
COUNT = RATE * SECONDS // FRAME


@dataclass(frozen=True)
class Case:
    """Fixed, reproducible acoustic schedule; delays are in milliseconds."""
    name: str
    recorded: bool = False
    gain: str = 'fixed'
    delay: str = 'fixed'
    double_talk: bool = False
    ambience: bool = False
    encoding: str = 'mulaw'
    control: str = ''


CASES = [
    Case('synthetic_fixed'),
    Case('gain_step', gain='step'),
    Case('gain_ramp', gain='ramp'),
    Case('delay_step_up', delay='step_up'),
    Case('delay_step_down', delay='step_down'),
    Case('fractional_delay', delay='fractional'),
    Case('delay_drift', delay='drift'),
    Case('recorded_fixed', recorded=True),
    Case('recorded_gain_step', recorded=True, gain='step'),
    Case('recorded_double_talk', recorded=True, double_talk=True),
    Case('recorded_mixer_linear16', recorded=True, ambience=True, encoding='linear16'),
    Case('disabled_aec_control', control='disabled'),
    Case('wrong_reference_control', control='wrong_reference'),
]


def far_audio(recorded):
    """Load the pinned upstream recording or synthesize fixed-seed speech."""
    if not recorded:
        return speech_like(SECONDS, 103)
    path = ROOT / 'fixtures/webrtc-speech.wav'
    assert hashlib.sha256(path.read_bytes()).hexdigest() == '0e415756dd194d0287ac7a949162bafb221e833591c7ba20c33d0eadd3cf7ce9'
    with wave.open(str(path), 'rb') as wav:
        assert (wav.getnchannels(), wav.getsampwidth(), wav.getframerate()) == (1, 2, RATE)
        raw = np.frombuffer(wav.readframes(wav.getnframes()), dtype='<i2').astype(float)
    raw *= 3000 / np.sqrt(np.mean(raw ** 2))
    return np.resize(np.clip(raw, -32000, 32000).astype('<i2'), RATE * SECONDS).tobytes()


def schedule(case, sample_indices):
    """Sample-resolution gain and physical delay, independent of feed timing."""
    seconds = sample_indices / RATE
    gain = np.full(len(seconds), .3)
    if case.gain == 'step': gain = np.where(seconds < 6, .1, .5)
    if case.gain == 'ramp': gain = .1 + .4 * np.clip((seconds - 6) / 4, 0, 1)
    delay = np.full(len(seconds), 120.)
    if case.delay == 'step_up': delay = np.where(seconds < 6, 120., 220.)
    if case.delay == 'step_down': delay = np.where(seconds < 6, 200., 80.)
    if case.delay == 'fractional': delay[:] = 137.375
    if case.delay == 'drift': delay = 120 + 20 * np.clip((seconds - 6) / 4, 0, 1)
    return gain, delay * RATE / 1000


def erle(raw, clean, start, stop):
    """Power reduction over active echo frames; this is not a double-talk metric."""
    start, stop = int(start * RATE / FRAME), int(stop * RATE / FRAME)
    power = np.mean(raw[start:stop].astype(float) ** 2, axis=1)
    residual = np.mean(clean[start:stop].astype(float) ** 2, axis=1)
    active = power > 50 ** 2
    assert active.any(), 'quality measurement has no active speech'
    return float(10 * np.log10(power[active].mean() / max(residual[active].mean(), 1e-9)))


def retention(output, caller):
    """Caller gain and correlation in the 10–13 second double-talk window."""
    voice = caller[10*RATE:13*RATE].astype(float)
    clean = output.reshape(-1)[10*RATE:13*RATE].astype(float)
    return {'gain_db': float(20*np.log10(abs(np.dot(clean, voice)/np.dot(voice, voice)))),
            'correlation': float(np.corrcoef(clean, voice)[0, 1])}


def run_case(case, no_echo=False):
    """Dispatch framed playback, synthesize its acoustic return, compare each event."""
    config = {'aec': case.control != 'disabled', 'encoding': case.encoding, 'ambience': case.ambience}
    runtime = AudioRuntime(json.dumps(config))
    oracle = AECProcessor() if config['aec'] else None
    oracle_vad = webrtcvad.Vad(3)
    mixer = build_mixer(RATE, enable_oneshots=True) if case.ambience else None
    source = far_audio(case.recorded)
    if case.encoding == 'linear16':
        source = audioop.ratecv(source, 2, 1, RATE, 24000, None)[0]
        # A 3x upsample omits two final samples. Match this as a short stream tail.
        source += bytes(4)
    else: source = audioop.lin2ulaw(source, 2)
    source_frame_bytes = 960 if case.encoding == 'linear16' else FRAME
    source_chunks = [source[i:i+source_frame_bytes*25] for i in range(0, len(source), source_frame_bytes*25)]
    played = np.zeros(RATE * SECONDS)
    raw_frames, clean_frames = [], []
    caller = np.zeros(RATE * SECONDS, dtype=np.int16)
    if case.double_talk:
        caller[10*RATE:13*RATE] = np.frombuffer(speech_like(3, 77, target_rms=3000), dtype=np.int16)
    noise = np.random.default_rng(71).normal(0, 10, RATE*SECONDS)
    wrong = audioop.lin2ulaw(speech_like(SECONDS, 987), 2)
    rate_state = None
    pending = bytearray()
    fed, outbound, state_checks = 0, 0, 0
    digest = hashlib.sha256()

    def call(op, **kwargs):
        return json.loads(runtime.call(json.dumps({'op': op, **kwargs})))

    def check_state():
        nonlocal state_checks
        snapshot = call('snapshot')
        assert snapshot['pending_bytes'] == len(pending), (case.name, 'pending')
        if oracle:
            native = snapshot['aec']
            native.pop('errors')
            assert native == state(oracle), (case.name, 'aec state', native, state(oracle))
        state_checks += 1

    try:
        if mixer:
            mixer.set_speaking(True)
            call('speaking', value=True)
        for index in range(COUNT):
            if index % 25 == 0:
                chunk = source_chunks[index // 25]
                if case.encoding == 'linear16':
                    pcm, rate_state = audioop.ratecv(chunk, 2, 1, 24000, RATE, rate_state)
                    pending.extend(audioop.lin2ulaw(pcm, 2))
                else: pending.extend(chunk)
                expected = []
                while len(pending) >= FRAME:
                    frame = bytes(pending[:FRAME]); del pending[:FRAME]
                    if mixer:
                        frame = audioop.lin2ulaw(mixer.next_frame(FRAME, tts_frame=audioop.ulaw2lin(frame, 2)), 2)
                    expected.append(frame)
                actual = call('tts', data=list(chunk))
                assert actual == [list(frame) for frame in expected], (case.name, index, 'outbound')
                # Framing alone must never enqueue an AEC reference.
                check_state()
                for frame in expected:
                    played[fed*FRAME:(fed+1)*FRAME] = np.frombuffer(audioop.ulaw2lin(frame, 2), dtype=np.int16)
                    reference = wrong[fed*FRAME:(fed+1)*FRAME] if case.control == 'wrong_reference' else frame
                    if oracle: oracle.feed_reference(reference)
                    call('reference', data=list(reference))
                    check_state()
                    fed += 1
                    outbound += 1
            positions = np.arange(index*FRAME, (index+1)*FRAME)
            gain, delay = schedule(case, positions)
            # Fractional sample interpolation models physical path variation, not FIFO slip.
            echo = gain * np.interp(positions-delay, np.arange(len(played)), played, left=0, right=0)
            if no_echo: echo[:] = 0
            near = np.clip(echo + caller[positions] + noise[positions], -32768, 32767).astype(np.int16).tobytes()
            encoded = audioop.lin2ulaw(near, 2)
            expected = oracle.process(encoded) if oracle else encoded
            pcm = audioop.ulaw2lin(expected, 2)
            actual = call('inbound', data=list(encoded))
            assert actual == {'mulaw': list(expected), 'pcm': list(pcm), 'rms': audioop.rms(pcm, 2),
                              'speech': oracle_vad.is_speech(pcm, RATE)}, (case.name, index, 'inbound')
            check_state()
            raw_frames.append(np.frombuffer(audioop.ulaw2lin(encoded, 2), dtype=np.int16))
            clean_frames.append(np.frombuffer(pcm, dtype=np.int16))
            digest.update(expected)
        assert call('flush') == list(pending)
        pending.clear()
        check_state()
        raw, clean = np.array(raw_frames), np.array(clean_frames)
        row = {'name': case.name + ('_no_echo' if no_echo else ''), 'frames': COUNT,
               'outbound_frames': outbound, 'state_checks': state_checks, 'parity_passed': True,
               'sha256': digest.hexdigest(), 'schedule': case.__dict__,
               'erle_db': {} if no_echo else {label: erle(raw, clean, start, stop) for label, start, stop in
                           [('steady_3_6s', 3, 6), ('transition_6_9s', 6, 9), ('recovered_14_18s', 14, 18)]}}
        if case.double_talk: row['caller_retention'] = retention(clean, caller)
        return row
    finally:
        runtime.close()


def main():
    rows = []
    for case in CASES:
        row = run_case(case)
        if case.double_talk:
            control = run_case(case, no_echo=True)
            gain_difference = abs(row['caller_retention']['gain_db']-control['caller_retention']['gain_db'])
            correlation_ratio = row['caller_retention']['correlation']/control['caller_retention']['correlation']
            row['double_talk_control'] = control
            row['caller_gain_difference_db'] = gain_difference
            row['caller_correlation_ratio'] = correlation_ratio
            assert gain_difference <= 1 and correlation_ratio >= .95, row
        # Fixed requirement declared before measurements: recover to 20 dB by 14 s.
        row['required_recovered_erle_db'] = 20
        row['quality_passed'] = row['erle_db']['recovered_14_18s'] >= 20
        # Report transient leakage separately: convergence after a change does
        # not imply caller-safe residual levels during adaptation.
        row['transition_below_15db'] = row['erle_db']['transition_6_9s'] < 15
        if case.control:
            assert row['erle_db']['recovered_14_18s'] < 10, 'negative control did not expose broken cancellation'
            row['negative_control_detected'] = True
        rows.append(row)
        print(f"{case.name}: parity OK, ERLE {row['erle_db']}", flush=True)
    document = {'cases': rows, 'frames_byte_identical': sum(r['frames'] + r.get('double_talk_control', {}).get('frames', 0) for r in rows),
                'quality_failures': [r['name'] for r in rows if not r['quality_passed'] and not r['schedule']['control']],
                'transition_below_15db': [r['name'] for r in rows if r['transition_below_15db'] and not r['schedule']['control']],
                'scope': 'Offline runtime acoustic integration; Python parity plus independent quality thresholds; no network or performance measurement'}
    (ROOT / 'results/echo_runtime.json').write_text(json.dumps(document, indent=2)+'\n')
    assert not document['quality_failures'], document['quality_failures']


if __name__ == '__main__':
    main()
