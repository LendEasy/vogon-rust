"""Module 13 offline/local gate. Never uses dotenv or paid providers."""
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
ROOT=Path(__file__).resolve().parents[2]

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--checks',action='store_true')
    parser.add_argument('--repeat',type=int,default=1)
    args=parser.parse_args()
    if not 1<=args.repeat<=100:parser.error('--repeat must be 1..100')
    results=ROOT/'validation/operations/results';results.mkdir(parents=True,exist_ok=True)
    cargo=shutil.which('cargo') or '/tmp/vogon-rust-toolchain/cargo/bin/cargo'
    env=dict(os.environ,PYO3_PYTHON=sys.executable,PYTHONPATH=sysconfig.get_path('purelib'),OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1',AWS_EC2_METADATA_DISABLED='true',REDIS_ENABLED='false')
    if not shutil.which('cargo'):env.update(CARGO_HOME='/tmp/vogon-rust-toolchain/cargo',RUSTUP_HOME='/tmp/vogon-rust-toolchain/rustup')
    manifest={'passed':False,'external_calls':False,'gpt_live':False,'repeat':args.repeat}
    path=results/'manifest.json';path.write_text(json.dumps(manifest,indent=2)+'\n')
    def run(command,name):
        with (results/(name+'.log')).open('w') as log:subprocess.run(command,cwd=ROOT,env=env,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=600)
        print(name+': passed',flush=True)
    run([cargo,'test','--locked','--offline','--workspace','-j','2','--','--skip','live_'],'native')
    for n in range(1,args.repeat):
        run([cargo,'test','--locked','--offline','-p','vogon-service','--test','operations','--test','recording'],'repeat-'+str(n))
    packages=['service','orchestration','clocks','transport','front','speech-providers','escalation','artifacts','resilience','observability']
    select=[part for name in packages for part in ['-p','vogon-'+name]]
    if args.checks:
        run([cargo,'fmt',*select,'--','--check'],'format')
        run([cargo,'clippy','--locked','--offline','--no-deps',*select,'--all-targets','--','-D','warnings'],'clippy')
        env['RUSTDOCFLAGS']='-D rustdoc::broken_intra_doc_links'
        run([cargo,'doc','--locked','--offline','--workspace','--no-deps'],'rustdoc')
    rows=re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; \d+ measured; (\d+) filtered out',(results/'native.log').read_text())
    manifest['native']={name:sum(int(row[i]) for row in rows) for i,name in enumerate(['passed','failed','ignored','filtered_out'])}
    files=[ROOT/'Cargo.toml',ROOT/'Cargo.lock',ROOT/'Dockerfile',ROOT/'deny.toml',*ROOT.glob('.github/workflows/*.yml')]
    files.extend(p for p in ROOT.glob('crates/*/src/**/*.rs'))
    manifest.update(passed=True,source_sha256={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(files)})
    path.write_text(json.dumps(manifest,indent=2)+'\n');print(json.dumps({k:v for k,v in manifest.items() if k!='source_sha256'}))
if __name__=='__main__':main()
