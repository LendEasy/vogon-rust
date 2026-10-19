"""Prove production construction/processing cannot import Python DSP wrappers.

Runs in a fresh interpreter, separately from differential oracles. NumPy and
audioop remain available by design. No timing or throughput claims.
"""
import importlib.abc
import json
from pathlib import Path
import sys
import subprocess

BLOCKED = ('speexdsp', '_webrtcvad', 'webrtcvad')


class RejectPythonBindings(importlib.abc.MetaPathFinder):
    """Fail the test if Rust tries loading either legacy Python binding."""
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in BLOCKED:
            raise AssertionError(f'Python DSP binding unexpectedly imported: {fullname}')
        return None


def main():
    assert not any(name.split('.')[0] in BLOCKED for name in sys.modules)
    guard = RejectPythonBindings()
    sys.meta_path.insert(0, guard)
    from vogon_audio_core import AECProcessor, Vad
    from vogon_audio_runtime import AudioRuntime
    # Direct objects, invalid-frame path, explicit close and implicit destruction.
    for _ in range(20):
        aec = AECProcessor()
        aec.feed_reference(b'\xff' * 160)
        assert aec.process(b'\xff' * 160) == b'\xff' * 160
        aec.close(); aec.close()
        try: aec.process(b'\xff' * 160)
        except RuntimeError: pass
        else: raise AssertionError('closed Speex owner accepted work')
        unclosed = AECProcessor()
        del unclosed
        vad = Vad()
        assert vad.is_speech(bytes(320)) is False
        assert vad.confirm_frame(b'x') is False
    runtime = AudioRuntime('{}')
    try:
        def call(op, **kwargs): return json.loads(runtime.call(json.dumps({'op': op, **kwargs})))
        frames = call('tts', data=[255]*160)
        assert len(frames) == 1
        call('reference', data=frames[0])
        result = call('inbound', data=[255]*160)
        assert result['rms'] == 0 and result['speech'] is False
    finally:
        runtime.close()
    assert not any(name.split('.')[0] in BLOCKED for name in sys.modules)
    symbols = subprocess.check_output(['nm', '-D', str(Path('/proc/self/exe').resolve())], text=True)
    assert 'WebRtc' not in symbols, 'static VAD symbols could interpose on the Python oracle'
    document = {'passed': True, 'blocked_python_modules': BLOCKED,
                'direct_owners_and_runtime': True, 'lifecycle_iterations': 20,
                'static_vad_symbols_hidden': True,
                'scope': 'Python import isolation and native owner lifecycle, not memory-leak measurement'}
    (Path(__file__).parent / 'results/native.json').write_text(json.dumps(document, indent=2)+'\n')
    print('Native bindings: AEC, VAD and runtime work with Python bindings blocked', flush=True)


if __name__ == '__main__':
    main()
