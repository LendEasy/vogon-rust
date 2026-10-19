"""Repeat gated scheduling tests to expose intermittent races; not a benchmark."""
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

ROOT = Path(__file__).resolve().parent
WORKSPACE = ROOT.parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repeat', type=int, default=50)
    parser.add_argument('--timers', action='store_true', help='Repeat actual Session heartbeats and provider keepalive timers; requires loopback permission')
    args = parser.parse_args()
    if args.repeat < 1:
        parser.error('--repeat must be positive')
    cargo = shutil.which('cargo') or '/tmp/vogon-rust-toolchain/cargo/bin/cargo'
    env = dict(os.environ, CARGO_HOME=os.environ.get('CARGO_HOME', '/tmp/vogon-rust-toolchain/cargo'), RUSTUP_HOME=os.environ.get('RUSTUP_HOME', '/tmp/vogon-rust-toolchain/rustup'), PYO3_PYTHON=sys.executable)
    command = [cargo, 'test', '--offline', '--locked', '--manifest-path', str(WORKSPACE/'Cargo.toml'), '-p', 'vogon-orchestration', '--test', 'concurrency', '--test', 'lifecycle']
    commands = [command]
    if args.timers:
        env.update(PYTHONDONTWRITEBYTECODE='1', OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1')
        env['PYTHONPATH'] = os.pathsep.join([sysconfig.get_path('purelib'), str(WORKSPACE.parent/'voice-gateway'), str(WORKSPACE.parent/'vogon-contract/src')])
        base = [cargo, 'test', '--offline', '--locked', '--manifest-path', str(WORKSPACE/'Cargo.toml')]
        commands = [base + ['-p', 'vogon-orchestration', '-p', 'vogon-transport', '--test', 'heartbeat', '--test', 'integration', '--test', 'session', 'heartbeat'], base + ['-p', 'vogon-speech-providers', '--test', 'keepalive']]
    results = ROOT/'results'
    results.mkdir(exist_ok=True)
    name = 'timer-stress' if args.timers else 'scheduling-stress'
    target = results/(name+'.json')
    target.write_text(json.dumps({'passed': False, 'repetitions': args.repeat})+'\n')
    with (results/(name+'.log')).open('w') as log:
        for iteration in range(args.repeat):
            log.write(f'Run {iteration + 1}\n')
            log.flush()
            for command in commands:
                subprocess.run(command, cwd=WORKSPACE.parent, env=env, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=30)
    counts = re.findall(r'test result: ok\. (\d+) passed', (results/(name+'.log')).read_text())
    names = ['orchestration', 'transport', 'clocks', 'barge-in', 'speech-providers', 'front'] if args.timers else ['orchestration']
    files = [Path(__file__)] + [p for name in names for p in (WORKSPACE/'crates'/name).rglob('*.rs')]
    summary = {'passed': True, 'repetitions': args.repeat, 'total_passed': sum(map(int, counts)), 'timer_tests': args.timers, 'purpose': __doc__, 'source_sha256': {str(p.relative_to(WORKSPACE)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(files)}}
    target.write_text(json.dumps(summary, indent=2)+'\n')
    print(f'{args.repeat} repetitions, {summary["total_passed"]} test executions passed')


if __name__ == '__main__':
    main()
