"""Module 9 reproducible gate. Optional loopback fixtures; never external model calls."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import sysconfig
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parent
WORKSPACE = ROOT.parents[1]
MONO = WORKSPACE.parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sockets', action='store_true', help='Run local HTTP/STT/TTS and existing native integration suites')
    parser.add_argument('--reference', action='store_true', help='Run existing Python driver and real application speculation tests with mocked agents')
    parser.add_argument('--checks', action='store_true', help='Run formatting, Clippy and Rustdoc')
    args = parser.parse_args()
    cargo = shutil.which('cargo') or '/tmp/vogon-rust-toolchain/cargo/bin/cargo'
    env = dict(os.environ, CARGO_HOME=os.environ.get('CARGO_HOME', '/tmp/vogon-rust-toolchain/cargo'), RUSTUP_HOME=os.environ.get('RUSTUP_HOME', '/tmp/vogon-rust-toolchain/rustup'), PYO3_PYTHON=sys.executable, PYTHONDONTWRITEBYTECODE='1', OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1', REDIS_ENABLED='false', AWS_EC2_METADATA_DISABLED='true', PYTEST_DISABLE_PLUGIN_AUTOLOAD='1')
    env['PYTHONPATH'] = os.pathsep.join([sysconfig.get_path('purelib'), str(MONO/'data-plane'), str(MONO/'data-plane/tests'), str(MONO/'voice-gateway'), str(MONO/'vogon-contract/src')])
    results = ROOT/'results'
    results.mkdir(exist_ok=True)
    manifest = {'passed': False, 'local_sockets': args.sockets, 'python_reference_run': args.reference, 'checks_run': args.checks, 'external_model_calls': False, 'gpt_live_tests_run': False}
    target = results/'manifest.json'
    target.write_text(json.dumps(manifest, indent=2)+'\n')

    def run(command, name, extra=None, cwd=MONO):
        with (results/(name+'.log')).open('w') as log:
            subprocess.run(command, cwd=cwd, env=env | (extra or {}), stdout=log, stderr=subprocess.STDOUT, timeout=240, check=True)
        print(name+': passed', flush=True)

    base = ['--offline', '--locked', '--manifest-path', str(WORKSPACE/'Cargo.toml')]
    if args.sockets:
        run([cargo, 'test', *base, '-p', 'vogon-orchestration', '-p', 'vogon-front', '-p', 'vogon-transport', '-p', 'vogon-clocks', '-p', 'vogon-barge-in', '-p', 'vogon-speech-providers', '-p', 'vogon-audio-runtime', '--', '--skip', 'live_'], 'native')
    else:
        run([cargo, 'test', *base, '-p', 'vogon-orchestration', '--test', 'state', '--test', 'concurrency', '--test', 'session', '--test', 'lifecycle'], 'native')
        run([cargo, 'test', *base, '-p', 'vogon-clocks', '-p', 'vogon-transport', '--test', 'isolation', '--test', 'transitions', '--test', 'clock_isolation', '--test', 'heartbeat'], 'clock-isolation')
    logs = ['native'] + ([] if args.sockets else ['clock-isolation'])
    rows = [row for name in logs for row in re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; \d+ measured; (\d+) filtered out', (results/(name+'.log')).read_text())]
    manifest['native'] = {key: sum(int(row[i]) for row in rows) for i, key in enumerate(['passed', 'failed', 'ignored', 'filtered_out'])}
    if args.reference:
        tests = ['tests/test_turn_client.py', 'tests/test_turn_loop_concurrency.py', 'tests/test_handler_harness.py::TestSpeculationAcrossTheHttpSeam', 'tests/test_gateway_turn_contract.py', '../voice-gateway/tests/test_driver_end_call.py']
        run([sys.executable, '-m', 'pytest', *tests, '-q', '--junitxml='+str(results/'python-reference.xml')], 'python-reference', cwd=MONO/'data-plane')
        suites = ET.parse(results/'python-reference.xml').getroot().iter('testsuite')
        suites = list(suites)
        manifest['python_reference'] = {key: sum(int(s.attrib.get(key, 0)) for s in suites) for key in ['tests', 'failures', 'errors', 'skipped']}
    if args.checks:
        run([cargo, 'fmt', '--manifest-path', str(WORKSPACE/'Cargo.toml'), '-p', 'vogon-orchestration', '-p', 'vogon-clocks', '-p', 'vogon-transport', '-p', 'vogon-front', '--', '--check'], 'format')
        run([cargo, 'clippy', *base, '-p', 'vogon-orchestration', '-p', 'vogon-clocks', '--all-targets', '--no-deps', '--', '-D', 'warnings'], 'clippy')
        run([cargo, 'doc', *base, '-p', 'vogon-orchestration', '-p', 'vogon-clocks', '--no-deps', '--document-private-items'], 'rustdoc', {'RUSTDOCFLAGS': '-D rustdoc::broken_intra_doc_links'})
    files = [WORKSPACE/'Cargo.toml', WORKSPACE/'Cargo.lock', WORKSPACE/'tools/generate_contracts.py', Path(__file__)]
    files.extend(ROOT.glob('*.py'))
    for name in ['orchestration', 'clocks', 'transport', 'front', 'contracts', 'configuration', 'playback', 'barge-in', 'speech-providers', 'audio-runtime', 'audio-core']:
        files.extend((WORKSPACE/'crates'/name).rglob('*.rs'))
    files.extend((MONO/'voice-gateway/vogon/websocket').glob('*.py'))
    files.extend((MONO/'data-plane/app/turns').glob('*.py'))
    manifest.update(passed=True, python=sys.version.split()[0], source_sha256={str(p.relative_to(MONO)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(set(files))})
    target.write_text(json.dumps(manifest, indent=2)+'\n')
    print(json.dumps({k: v for k, v in manifest.items() if k != 'source_sha256'}))


if __name__ == '__main__':
    main()
