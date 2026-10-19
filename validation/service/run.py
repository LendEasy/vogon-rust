"""Module 10 gate: local sockets/private Redis, Python reference and direct comparisons. No paid APIs."""
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
    cargo = shutil.which('cargo') or '/tmp/vogon-rust-toolchain/cargo/bin/cargo'
    env = dict(os.environ, CARGO_HOME=os.environ.get('CARGO_HOME', '/tmp/vogon-rust-toolchain/cargo'), RUSTUP_HOME=os.environ.get('RUSTUP_HOME', '/tmp/vogon-rust-toolchain/rustup'), PYO3_PYTHON=sys.executable, PYTHONDONTWRITEBYTECODE='1', OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1', REDIS_ENABLED='false', AWS_EC2_METADATA_DISABLED='true', PYTEST_DISABLE_PLUGIN_AUTOLOAD='1')
    env['PYTHONPATH'] = os.pathsep.join([sysconfig.get_path('purelib'), str(MONO/'data-plane'), str(MONO/'data-plane/tests'), str(MONO/'voice-gateway'), str(MONO/'vogon-contract/src')])
    results = ROOT/'results'
    results.mkdir(exist_ok=True)
    manifest = {'passed': False, 'external_calls': False, 'gpt_live_tests_run': False, 'local_redis': True}
    target = results/'manifest.json'
    target.write_text(json.dumps(manifest, indent=2)+'\n')
    def run(command, name, cwd=MONO, extra=None):
        with (results/(name+'.log')).open('w') as log:
            subprocess.run(command, cwd=cwd, env=env | (extra or {}), stdout=log, stderr=subprocess.STDOUT, timeout=240, check=True)
        print(name+': passed', flush=True)
    base = ['--offline', '--locked', '--manifest-path', str(WORKSPACE/'Cargo.toml')]
    run([cargo, 'test', *base, '-p', 'vogon-service'], 'native')
    run([cargo, 'build', *base, '-p', 'vogon-service', '--bins'], 'build')
    run([sys.executable, str(ROOT/'compare.py')], 'comparison')
    run([sys.executable, '-m', 'pytest', 'tests/test_gateway_service.py', '-q', '--junitxml='+str(results/'python-reference.xml')], 'python-reference', cwd=MONO/'data-plane')
    run([cargo, 'fmt', '--manifest-path', str(WORKSPACE/'Cargo.toml'), '-p', 'vogon-service', '--', '--check'], 'format')
    run([cargo, 'clippy', *base, '-p', 'vogon-service', '--all-targets', '--no-deps', '--', '-D', 'warnings'], 'clippy')
    run([cargo, 'doc', *base, '-p', 'vogon-service', '--no-deps', '--document-private-items'], 'rustdoc', extra={'RUSTDOCFLAGS':'-D rustdoc::broken_intra_doc_links'})
    rows = re.findall(r'test result: ok\. (\d+) passed; (\d+) failed;', (results/'native.log').read_text())
    manifest['native'] = {key: sum(int(row[i]) for row in rows) for i, key in enumerate(['passed', 'failed'])}
    suites = list(ET.parse(results/'python-reference.xml').getroot().iter('testsuite'))
    manifest['python_reference'] = {key: sum(int(s.attrib.get(key, 0)) for s in suites) for key in ['tests', 'failures', 'errors', 'skipped']}
    manifest['comparison'] = json.loads((results/'comparison.json').read_text())
    files = [WORKSPACE/'Cargo.toml',WORKSPACE/'Cargo.lock',WORKSPACE/'crates/service/Cargo.toml',*(ROOT.glob('*.py')),*(WORKSPACE/'crates/service').rglob('*.rs'),*(WORKSPACE/'crates/service').rglob('*.lua'),*(MONO/'voice-gateway/vogon/gateway_service').glob('*.py')]
    manifest.update(passed=True, python=sys.version.split()[0], source_sha256={str(p.relative_to(MONO)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(set(files))})
    target.write_text(json.dumps(manifest, indent=2)+'\n')
    print(json.dumps({k:v for k,v in manifest.items() if k!='source_sha256'}))


if __name__ == '__main__':
    main()
