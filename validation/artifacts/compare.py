"""Execute the existing Python recorder, comparing every WAV byte and projected timestamp."""
import base64
import io
import json
from pathlib import Path
import random
import subprocess
import sys
import tempfile
import wave
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT.parent/'voice-gateway'))
from vogon.gateway.audio.audio_recorder import AudioRecorder
from vogon.gateway.transport.audio_event_log import AudioEventLog
rng=random.Random(12013)
cases=[]
for i in range(160):
    origin=rng.choice([0.,10.,1000.])
    actions=[] if i%3==0 else [dict(op='epoch',at=origin)]
    for j in range(rng.randrange(1,24)):
        at=origin+rng.randrange(0,2000)/8000
        actions.append(dict(op=rng.choice(['caller','agent']),at=at,data=base64.b64encode(rng.randbytes(rng.randrange(1,321))).decode()))
        if j%3==0:actions.append(dict(op='clear',at=origin+rng.randrange(0,2000)/8000))
        actions.append(dict(op='timestamp',at=origin+j*.02+rng.choice([0.,.004,.5]),timestamp=rng.choice([str(j*20),j*20,None,'invalid'])))
        actions.append(dict(op='event',at=origin+j*.02,name='mark_echo',detail={'name':str(j)}))
    cases.append(dict(actions=actions))
cases.extend([dict(actions=[]),dict(actions=[dict(op='agent',at=0.,data='/w=='),dict(op='clear',at=0.)])])
result=subprocess.run([str(ROOT/'target/debug/artifact-validation')],input=''.join(json.dumps(c)+'\n' for c in cases),text=True,capture_output=True,check=True)
for index,(case,line) in enumerate(zip(cases,result.stdout.splitlines(),strict=True)):
    r=AudioRecorder(stores=[]);events=AudioEventLog();times=[];clears=[]
    for a in case['actions']:
        op=a['op'];at=a['at']
        if op=='epoch':r.set_stream_epoch(at)
        elif op in ('caller','agent'):getattr(r,'add_'+op+'_chunk')(at,base64.b64decode(a['data']))
        elif op=='timestamp':times.append(r.caller_timestamp({'timestamp':a['timestamp']},at))
        elif op=='clear':clears.append(r.discard_agent_after(at))
        elif op=='event':events.emit(at,a['name'],**a['detail'])
    rust=json.loads(line);wav=r.to_wav_bytes()
    assert (base64.b64decode(rust['wav']) if rust['wav'] else None)==wav,('WAV',index)
    assert rust['times']==times,('timestamps',index)
    assert rust['clears']==clears,('clears',index)
    assert rust['t0']==r.t0,('origin',index)
    if len(events):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'events.json';events.flush(p,r.t0,{})
            assert rust['events']['events']==json.loads(p.read_text())['events'],('events',index)
print(json.dumps({'cases':len(cases),'wav_bytes':'exact','timestamps':'exact','clears':'exact','event_rows':'exact'}))
