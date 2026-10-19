"""Reproduce module 8 policy, lifecycle and optional local socket checks. No live APIs."""
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

ROOT=Path(__file__).resolve().parent
WORKSPACE=ROOT.parents[1]
MONO=WORKSPACE.parent
REFERENCE_TESTS=['test_front_guard.py','test_front_complement.py','test_gateway_jev.py','test_call_clocks.py','test_barge_semantics.py','barge_baseline/test_line_detector.py','barge_baseline/test_line_detector_greetings.py']

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sockets',action='store_true',help='Enable local HTTP/WebSocket mocks and module 2/5/6/7 regressions; needs loopback permission')
    parser.add_argument('--checks',action='store_true',help='Check formatting, Clippy for the new crate and Rustdoc links')
    args=parser.parse_args()
    cargo=shutil.which('cargo') or '/tmp/vogon-rust-toolchain/cargo/bin/cargo'
    env=dict(os.environ,CARGO_HOME=os.environ.get('CARGO_HOME','/tmp/vogon-rust-toolchain/cargo'),RUSTUP_HOME=os.environ.get('RUSTUP_HOME','/tmp/vogon-rust-toolchain/rustup'),PYO3_PYTHON=sys.executable,PYTHONDONTWRITEBYTECODE='1',OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1')
    env['PYTHONPATH']=os.pathsep.join([sysconfig.get_path('purelib'),str(MONO/'voice-gateway'),str(MONO/'vogon-contract/src')])
    results=ROOT/'results';results.mkdir(exist_ok=True)
    # Invalidate a previous success manifest before executing a potentially failing run.
    manifest={'passed':False,'local_socket_tests_run':args.sockets,'static_checks_run':args.checks,'external_provider_tests_run':False,'gpt_live_tests_run':False}
    path=results/'manifest.json';path.write_text(json.dumps(manifest,indent=2)+'\n')
    def run(command,name,extra=None):
        with (results/(name+'.log')).open('w') as log:
            subprocess.run(command,cwd=MONO,env=env| (extra or {}),stdout=log,stderr=subprocess.STDOUT,check=True)
        print(name+': passed',flush=True)
    base=['--offline','--locked','--manifest-path',str(WORKSPACE/'Cargo.toml')]
    run([cargo,'build',*base,'-p','vogon-front','-p','vogon-transport'],'build')
    run([sys.executable,str(ROOT/'parity.py')],'parity')
    run([sys.executable,'-m','pytest',*[str(MONO/'voice-gateway/tests'/test) for test in REFERENCE_TESTS],'-q','--junitxml='+str(results/'python-reference.xml')],'python-reference')
    native=[cargo,'test',*base,'-p','vogon-front']
    if args.sockets:native+=['-p','vogon-transport','-p','vogon-speech-providers','-p','vogon-clocks','-p','vogon-audio-runtime','--','--skip','live_']
    else:native+=['--test','lifecycle','--test','line']
    run(native,'native')
    counts=re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; \d+ measured; (\d+) filtered out', (results/'native.log').read_text())
    summary={name:sum(int(row[i]) for row in counts) for i,name in enumerate(['passed','failed','ignored','filtered_out'])}
    (results/'native.json').write_text(json.dumps({'tests':summary,'local_socket_tests_run':args.sockets,'scope':'Native module-8 tests and existing transport/provider/clock/audio-runtime tests when sockets enabled'},indent=2)+'\n')
    xml=ET.parse(results/'python-reference.xml').getroot()
    suites=list(xml.iter('testsuite'));reference={key:sum(int(s.attrib.get(key,0)) for s in suites) for key in ['tests','failures','errors','skipped']}
    manifest['python_reference']=reference
    manifest['native_tests']=summary
    if args.checks:
        run([cargo,'fmt','--manifest-path',str(WORKSPACE/'Cargo.toml'),'-p','vogon-front','-p','vogon-transport','--','--check'],'format')
        run([cargo,'clippy',*base,'-p','vogon-front','--all-targets','--no-deps','--','-D','warnings'],'clippy')
        run([cargo,'doc',*base,'--no-deps','--document-private-items','-p','vogon-front','-p','vogon-transport'],'rustdoc',{'RUSTDOCFLAGS':'-D rustdoc::broken_intra_doc_links'})
    files=[WORKSPACE/'Cargo.toml',WORKSPACE/'Cargo.lock',WORKSPACE/'target/debug/front-validation',WORKSPACE/'target/debug/transport-validation',WORKSPACE/'tools/export_front_constants.py']
    for name in ['front','transport','barge-in','playback','clocks','speech-providers','contracts','configuration','audio-runtime','audio-core']:
        crate=WORKSPACE/'crates'/name;files.extend(crate.rglob('*.rs'));files.append(crate/'Cargo.toml')
    files.extend(ROOT.glob('*.py'));files.append(WORKSPACE/'crates/front/src/constants.json')
    files.extend((MONO/'voice-gateway/vogon/gateway/transport').rglob('*.py'))
    files.extend([MONO/'voice-gateway/vogon/gateway/jev.py',MONO/'voice-gateway/vogon/websocket/line_instruments.py'])
    files.extend(MONO/'voice-gateway/tests'/test for test in REFERENCE_TESTS)
    manifest.update(passed=True,python=sys.version.split()[0],rustc=subprocess.check_output([str(Path(cargo).with_name('rustc')),'--version'],env=env,text=True).strip(),source_and_binary_sha256={str(p.relative_to(MONO)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(set(files))})
    path.write_text(json.dumps(manifest,indent=2)+'\n')
    print(json.dumps({'native':summary,'reference':reference,'no_external_calls':True}))

if __name__=='__main__':main()
