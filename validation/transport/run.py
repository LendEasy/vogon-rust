"""Reproduce speech/transport parity. GPT Live and external providers are excluded."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import sysconfig

ROOT = Path(__file__).resolve().parent
WORKSPACE = ROOT.parents[1]
MONOREPO = WORKSPACE.parent

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sockets',action='store_true',help='Also run local WebSocket/native lifecycle tests; requires loopback permission')
    args=parser.parse_args()
    cargo=shutil.which('cargo') or '/tmp/vogon-rust-toolchain/cargo/bin/cargo'
    env=dict(os.environ, CARGO_HOME=os.environ.get('CARGO_HOME','/tmp/vogon-rust-toolchain/cargo'),RUSTUP_HOME=os.environ.get('RUSTUP_HOME','/tmp/vogon-rust-toolchain/rustup'),PYO3_PYTHON=sys.executable,RUSTFLAGS='-C target-cpu=native',PYTHONDONTWRITEBYTECODE='1',OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1')
    env['PYTHONPATH']=os.pathsep.join([sysconfig.get_path('purelib'),str(MONOREPO/'voice-gateway'),str(MONOREPO/'vogon-contract/src')])
    def run(command):subprocess.run(command,cwd=MONOREPO,env=env,check=True)
    base=[cargo,'build','--offline','--locked','--manifest-path',str(WORKSPACE/'Cargo.toml')]
    run(base+['-p','vogon-transport','-p','vogon-speech-providers'])
    run(base+['--release','-p','vogon-audio-runtime'])
    run([sys.executable,str(WORKSPACE/'validation/providers/parity.py')])
    run([sys.executable,str(ROOT/'parity.py')])
    run([str(WORKSPACE/'target/release/vogon-runtime-validation'),str(ROOT/'audio_checks.py')])
    if args.sockets:
        command=[cargo,'test','--offline','--locked','--manifest-path',str(WORKSPACE/'Cargo.toml'),'-p','vogon-transport','-p','vogon-speech-providers','-p','vogon-clocks','-p','vogon-audio-runtime','--','--skip','live_']
        with (ROOT/'results/native.log').open('w') as out:subprocess.run(command,cwd=MONOREPO,env=env,stdout=out,stderr=subprocess.STDOUT,check=True)
    files=[WORKSPACE/'Cargo.lock',WORKSPACE/'Cargo.toml']
    for name in ['transport','speech-providers','clocks','audio-runtime','audio-core','playback','barge-in','contracts','configuration']:
        crate=WORKSPACE/'crates'/name;files.extend(crate.rglob('*.rs'));files.append(crate/'Cargo.toml')
    for directory in [ROOT,WORKSPACE/'validation/providers',MONOREPO/'voice-gateway/vogon/gateway/transport',MONOREPO/'voice-gateway/vogon/gateway/pipeline']:
        files.extend(directory.rglob('*.py'))
    for name in ['transport-validation','provider-validation']:files.append(WORKSPACE/'target/debug'/name)
    files.append(WORKSPACE/'target/release/vogon-runtime-validation')
    hashes={str(path.relative_to(MONOREPO)):hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(set(files))}
    manifest={'passed':True,'python':sys.version.split()[0],'rustc':subprocess.check_output([str(Path(cargo).with_name('rustc')),'--version'],env=env,text=True).strip(),'source_and_binary_sha256':hashes,'local_socket_tests_run':args.sockets,'gpt_live_tests_run':False,'external_provider_tests_run':False}
    (ROOT/'results/manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    print('Speech/transport/clock parity passed; source and binary hashes saved.')

if __name__=='__main__':main()
