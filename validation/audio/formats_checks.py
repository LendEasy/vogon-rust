"""Compare retained libraries and Rust framing with the actual Python transports.

No benchmarks: these checks include wrappers and intentionally report no timing.
"""
import asyncio
import audioop
import hashlib
import json
import logging
from pathlib import Path
import random
import struct
from unittest.mock import AsyncMock, patch

import webrtcvad
import vogon_audio_core as rust
from transport_fixtures import bare_twilio_transport
from vogon.gateway.transport.voice_transport import VoiceTransport, _encode_audio_frame
from vogon.gateway.transport.ambience.loader import build_mixer

ROOT = Path(__file__).resolve().parent
ROWS = []
COUNTS = {"library_comparisons": 0, "resampler_chunks": 0, "vad_frames": 0,
          "phone_frames": 0, "browser_packets": 0}


def outcome(fn):
    try:
        return ("ok", fn())
    except Exception as exc:
        return (type(exc).__name__, str(exc))


def equal_call(reference, native):
    assert outcome(reference) == outcome(native)
    COUNTS["library_comparisons"] += 1


def libraries():
    ops = rust.AudioOps()
    ulaw = bytes(range(256))
    pcm = struct.pack("<65536h", *range(-32768, 32768))
    rng = random.Random(18)
    for width in (1, 2, 3, 4):
        expanded = audioop.lin2lin(pcm, 2, width)
        equal_call(lambda: audioop.ulaw2lin(ulaw, width), lambda: ops.ulaw2lin(ulaw, width))
        equal_call(lambda: audioop.lin2ulaw(expanded, width), lambda: ops.lin2ulaw(expanded, width))
        for n in (0, 1, 2, 159, 160, 161, 4096, 65536):
            data = expanded[:n * width]
            equal_call(lambda: audioop.rms(data, width), lambda: ops.rms(data, width))
        for new_width in (1, 2, 3, 4):
            equal_call(lambda: audioop.lin2lin(expanded, width, new_width), lambda: ops.lin2lin(expanded, width, new_width))
        for left, right in ((.5, .5), (1., 1.), (-1., 2.), (0., 0.)):
            equal_call(lambda: audioop.tomono(expanded, width, left, right), lambda: ops.tomono(expanded, width, left, right))
        for _ in range(30):
            data = rng.randbytes(rng.randrange(1000) * width)
            equal_call(lambda: audioop.rms(data, width), lambda: ops.rms(data, width))
            equal_call(lambda: audioop.lin2ulaw(data, width), lambda: ops.lin2ulaw(data, width))
    for width in (0, 2, 5):
        equal_call(lambda: audioop.rms(b"x", width), lambda: ops.rms(b"x", width))
        equal_call(lambda: audioop.lin2ulaw(b"x", width), lambda: ops.lin2ulaw(b"x", width))
        equal_call(lambda: audioop.tomono(b"x", width, .5, .5), lambda: ops.tomono(b"x", width, .5, .5))
    ROWS.append({"name": "audioop exhaustive 16-bit encoding, 256-code decoding, arithmetic and errors", "passed": True})


def resamplers():
    rng = random.Random(2718)
    for source, target in ((24000, 8000), (8000, 24000), (44100, 8000), (16000, 8000), (8000, 8000)):
        for width in (1, 2, 3, 4):
            for channels in (1, 2):
                # Interleave two streams, preserving independent native filter states.
                converters = [rust.RateConverter(source, target, width, channels) for _ in range(2)]
                states = [None, None]
                inputs, outputs = [bytearray(), bytearray()], [bytearray(), bytearray()]
                for i in range(100):
                    k = i % 2
                    data = rng.randbytes(rng.randrange(0, 600) * width * channels)
                    expected, states[k] = audioop.ratecv(data, width, channels, source, target, states[k])
                    actual = converters[k].process(data)
                    assert actual == expected
                    assert converters[k].state == states[k]
                    inputs[k].extend(data); outputs[k].extend(actual)
                    COUNTS["resampler_chunks"] += 1
                for k in range(2):
                    assert bytes(outputs[k]) == audioop.ratecv(bytes(inputs[k]), width, channels, source, target, None)[0]
                    prior = converters[k].state
                    if width * channels > 1:
                        equal_call(lambda: audioop.ratecv(b"x", width, channels, source, target, states[k]),
                                   lambda: converters[k].process(b"x"))
                        assert converters[k].state == prior
                    converters[k].reset()
                    assert converters[k].state is None
                    assert converters[k].process(bytes(inputs[k])) == bytes(outputs[k])
                ROWS.append({"name": f"resample/{source}/{target}/{width}/{channels}", "passed": True})


def vad():
    rng = random.Random(1618)
    positive = negative = 0
    for mode in range(4):
        for rate in (8000, 16000, 32000, 48000):
            p, r = webrtcvad.Vad(mode), rust.Vad(mode, rate)
            for i in range(180):
                n = rate * (10, 20, 30)[i % 3] // 1000
                # Long silence, loud excitation, quiet excitation, then silence:
                # exercise the retained VAD's adaptive state and hangover.
                amplitude = (0, 14000, 200, 0)[(i // 30) % 4]
                data = struct.pack(f"<{n}h", *[rng.randint(-amplitude, amplitude) for _ in range(n)])
                expected = p.is_speech(data, rate)
                assert r.is_speech(data) == expected
                positive += expected; negative += not expected
                COUNTS["vad_frames"] += 1
            for data in (b"", b"x", b"\0" * 100):
                equal_call(lambda: p.is_speech(data, rate), lambda: r.is_speech(data))
                assert r.confirm_frame(data) is False
            ROWS.append({"name": f"vad/{mode}/{rate}", "passed": True})
    assert positive and negative
    assert rust.Vad(2, 12345).confirm_frame(b"\0" * 320) is False
    # Recorded upstream fixture has fixed expected decisions, independent of the
    # Python extension oracle. Detect wrong C versions/modes as well as parity.
    recorded = (ROOT / 'fixtures/webrtc-vad.raw').read_bytes()
    assert hashlib.sha256(recorded).hexdigest() == '3dbb730b90e0266d78d4fe03f00aba37a83780a6a5240d9084b5c01266194c1e'
    expected_modes = ['011110111111111111111111111100', '011110111111111111111111111100',
                      '000000111111111111111111110000', '000000111111111111111100000000']
    for mode, expected_bits in enumerate(expected_modes):
        p, r = webrtcvad.Vad(mode), rust.Vad(mode, 8000)
        bits = ''
        for offset in range(0, len(recorded)-479, 480):
            data = recorded[offset:offset+480]
            expected = p.is_speech(data, 8000)
            assert r.is_speech(data) == expected
            bits += str(int(expected))
            COUNTS['vad_frames'] += 1
        assert bits == expected_bits, (mode, bits)
        # The Python wrapper ignores a trailing incomplete PCM16 sample.
        equal_call(lambda: p.is_speech(bytes(321), 8000), lambda: r.is_speech(bytes(321)))
    equal_call(lambda: webrtcvad.Vad(4), lambda: rust.Vad(4))
    ROWS.append({'name': 'vad recorded fixture, fixed decisions and odd sample tail', 'passed': True})
    COUNTS.update(vad_positive=positive, vad_negative=negative)


class TTS:
    def __init__(self, chunks, transport=None, cancel_at=None):
        self.chunks, self.transport, self.cancel_at = chunks, transport, cancel_at
    async def stream(self, text):
        for i, chunk in enumerate(self.chunks):
            if i == self.cancel_at: self.transport._tts_cancel.set()
            yield chunk, None
    async def cancel(self): pass


async def phones():
    rng = random.Random(123)
    for encoding in ("mulaw", "linear16"):
        for ambience in (False, True):
            for scenario in ("empty", "short", "boundaries", "random", "cancel"):
                sizes = {"empty": [], "short": [1, 17], "boundaries": [0, 1, 159, 160, 161, 319, 320, 321, 960, 1024],
                         "random": [rng.randrange(0, 3000) for _ in range(100)], "cancel": [1000, 17, 1000]}[scenario]
                chunks = [rng.randbytes(n) for n in sizes]
                p_mix = build_mixer(8000, enable_oneshots=True) if ambience else None
                r_mix = rust.build_mixer(8000, enable_oneshots=True) if ambience else None
                for mix in (p_mix, r_mix):
                    if mix:
                        mix.set_speaking(True); mix.start_typing(1); mix.trigger_oneshot(0)
                sent = []
                async def send(frame): sent.append(frame)
                t = bare_twilio_transport(_tts_encoding=encoding, _ambience=p_mix, _floor=None,
                    _send_media_frame=send, _send_mark=AsyncMock(), _send_clear=AsyncMock())
                cancel_at = 2 if scenario == "cancel" else None
                t._tts = TTS(chunks, t, cancel_at)
                count, had_error = await t._do_tts_stream("offline framing check", "test")
                assert not had_error
                assert count == len(sent)
                r = rust.PhoneFramer(encoding)
                actual = []
                for i, chunk in enumerate(chunks):
                    if i == cancel_at:
                        r.reset(); break
                    actual.extend(r.push(chunk, r_mix))
                else:
                    tail = r.flush_tail()
                    if tail: actual.append(tail)
                assert actual == sent, (encoding, ambience, scenario)
                assert r.pending_bytes == 0
                COUNTS["phone_frames"] += len(actual)
                # Reset after completion/cancel must behave like a new utterance.
                r.reset()
                fresh = rust.PhoneFramer(encoding)
                chunk = rng.randbytes(2301)
                assert r.push(chunk) == fresh.push(chunk)
                assert r.flush_tail() == fresh.flush_tail()
                ROWS.append({"name": f"transport/phone/{encoding}/{ambience}/{scenario}", "passed": True,
                             "frames":len(actual), "sha256":hashlib.sha256(b"".join(actual)).hexdigest()})
    # Sentence tails are emitted independently; no merging across marks.
    r = rust.PhoneFramer()
    for chunk in (b"x" * 17, b"y" * 159, b"z" * 161):
        frames = r.push(chunk)
        tail = r.flush_tail()
        assert b"".join(frames) + tail == chunk
        assert r.flush_tail() == b""
    ROWS.append({"name":"phone sentence tails", "passed":True})


async def browsers():
    rng = random.Random(42)
    for audio_id in ("", "resp-7", "聲音-é-🎧"):
        for sizes in ([], [1, 2, 3], [8191, 1, 8193, 17000, 1], [rng.randrange(10000) for _ in range(50)]):
            chunks = [rng.randbytes(n) for n in sizes]
            sent = []
            async def send(frame): sent.append(frame)
            with patch("vogon.gateway.transport.voice_transport.TTSClient"):
                t = VoiceTransport(AsyncMock(), send)
            t._tts = TTS(chunks)
            report = await t.send_response("offline framing check", audio_id)
            assert report.chunks_sent == len(sent)
            r = rust.BrowserFramer(audio_id)
            actual = [packet for chunk in chunks if (packet := r.push(chunk)) is not None]
            if (tail := r.flush_tail()) is not None: actual.append(tail)
            assert actual == sent
            assert r.pending_bytes == 0
            r.push(b"pending"); r.reset()
            assert r.flush_tail() is None
            for chunk in chunks:
                assert rust.encode_audio_frame(audio_id, chunk) == _encode_audio_frame(audio_id, chunk)
            COUNTS["browser_packets"] += len(actual)
            ROWS.append({"name":f"transport/browser/{audio_id}/{len(chunks)}", "passed":True,
                         "packets":len(actual), "sha256":hashlib.sha256(b"".join(actual)).hexdigest()})


def main():
    logging.getLogger().setLevel(logging.ERROR)
    libraries(); resamplers(); vad()
    asyncio.run(phones()); asyncio.run(browsers())
    doc = {"passed": True, "cases": ROWS, "counts": COUNTS,
           "scope":"Offline audioop/WebRTC VAD and actual Python transport output comparison. No timing; no provider/network test."}
    (ROOT / "results/formats.json").write_text(json.dumps(doc, indent=2))
    print(f"Formats: {len(ROWS)} cases passed; {COUNTS}", flush=True)


if __name__ == "__main__": main()
