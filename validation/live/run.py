"""Explicitly authorized live acceptance; real Deepgram STT/TTS and generated speech.
Run with --live. No GPT, transfer calls, external telephone dialing or app agents.
"""
import argparse
import asyncio
import json
import os
from pathlib import Path
import signal
import sys
import time

from aiohttp import ClientSession, ClientTimeout, web
from infra import MONO, PAIR, Caller, Echo, Proxy, Speech, normalized, port, serve, until, wer


class Suite:
    def __init__(self,out,env_path,default_eot=False):
        self.out=out;out.mkdir(parents=True,exist_ok=True)
        self.default_eot=default_eot
        self.env_path=env_path;self.callers=[];self.results=[];self.echo=Echo();self.process=None

    async def start(self):
        self.client=ClientSession(timeout=ClientTimeout(total=45))
        self.speech=Speech(self.client,self.env_path,self.out);self.proxy=Proxy(self.client)
        app=web.Application();app.router.add_post('/turn/v1/{op}',self.echo.handle);app.router.add_get('/{kind:stt|tts}',self.proxy.handle)
        import hashlib
        source_files=[*(MONO/'vogon-rust/crates').rglob('*.rs'),*Path(__file__).parent.glob('*.py'),MONO/'vogon-rust/target/debug/service-live-host']
        (self.out/'source-hashes.json').write_text(json.dumps({str(p.relative_to(MONO)):hashlib.sha256(p.read_bytes()).hexdigest() for p in source_files},indent=2))
        self.runner,local_port=await serve(app);self.local=f'http://127.0.0.1:{local_port}'
        gateway_port=port();self.base=f'http://127.0.0.1:{gateway_port}'
        phones=[{'tenant':'live','call_id':cid,'session_token':'token-'+cid,'mode':'twilio','provider_call_id':'CA-'+cid,'app_base_url':self.local+'/turn/v1','app_auth_token':'echo-fixture'} for cid in (['phone','silence-phone','clock-a','clock-b','echo-low','echo-mid','echo-high','double-talk']+[f'parallel-phone-{i}' for i in range(4)]+[f'cycle-phone-{i}' for i in range(4)])]
        fixture={'env_path':str(self.env_path),'bind':f'127.0.0.1:{gateway_port}','stt':self.local.replace('http:','ws:')+'/stt','tts':self.local.replace('http:','ws:')+'/tts','phones':phones,'settings':{'front_model_enabled':False,'cerebras_api_key':'','open_router_api_key':'','ambience_enabled':False,'deepgram_stt_model':'flux-general-en','deepgram_tts_model':'aura-2-thalia-en','deepgram_eager_eot_threshold':.3,'deepgram_eot_threshold':1.,'deepgram_eot_timeout_ms':3000,'deepgram_phone_eager_eot_threshold':.3,'deepgram_phone_eot_threshold':1.,'deepgram_phone_eot_timeout_ms':3000,'deepgram_eot_hold_ms':0,'turn_service_timeout_sec':8.}}
        if self.default_eot:
            for key in list(fixture['settings']):
                if 'eot' in key: del fixture['settings'][key]
        fixture_path=self.out/'fixture.json';fixture_path.write_text(json.dumps(fixture,indent=2))
        env=dict(os.environ,PYTHONPATH=str(MONO/'data-plane/.venv/lib/python3.10/site-packages'),OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1',GATEWAY_SESSION_STORE='memory',GATEWAY_TENANT_PROFILES='')
        self.log=(self.out/'gateway.log').open('w')
        self.process=await asyncio.create_subprocess_exec(str(MONO/'vogon-rust/target/debug/service-live-host'),'--live',str(fixture_path),env=env,stdout=self.log,stderr=self.log,cwd=MONO)
        for _ in range(200):
            if self.process.returncode is not None:raise AssertionError('gateway exited before listening; inspect gateway.log')
            try:
                async with self.client.get(self.base+'/health') as r:
                    if r.status==200:return
            except Exception:pass
            await asyncio.sleep(.05)
        raise AssertionError('startup timeout')

    async def call(self,cid,**kwargs):return await Caller(self,cid,**kwargs).open()

    async def verify(self,call,commit):
        aid=commit['audio_id'];data=bytes(call.audio['phone' if call.phone else aid])
        transcript=await self.speech.transcribe(data,8000 if call.phone else 24000)
        score=wer(commit['answer'],transcript)
        evidence={'expected':commit['answer'],'recognized_return_audio':transcript,'output_wer':score,'bytes':len(data)}
        (self.out/(call.cid+'-verification.json')).write_text(json.dumps(evidence,indent=2))
        assert score<=.25,f'returned speech WER {score:.3f}: {transcript}'
        return evidence

    async def test(self,ids,name,fn):
        self.proxy.case=name;before=len(self.callers);began=time.monotonic()
        print('RUN',ids,name,flush=True)
        try:
            detail=await asyncio.wait_for(fn(),180)
            for caller in self.callers[before:]:
                assert not caller.errors, f'caller protocol errors: {caller.errors}'
            ids_created={c.cid for c in self.callers[before:]}
            for exchange in self.echo.rows:
                if exchange['body'].get('call_id') in ids_created:
                    for event in exchange['body'].get('events',[]):
                        for key in ('at','onset_at'):
                            value=event.get(key)
                            if value is not None:
                                assert 0 <= value < 240, f'invalid call-relative {key}: {value}'
            row={'tests':ids,'name':name,'status':'passed','detail':detail}
        except Exception as e:
            row={'tests':ids,'name':name,'status':'failed','error':str(e) if isinstance(e,AssertionError) else type(e).__name__}
        finally:
            self.proxy.duplicate=False;self.proxy.tts_delay=0;self.proxy.drop_next_tts=False
            for event in list(self.echo.final_gates.values())+list(self.echo.eager_gates.values()):event.set()
            for c in self.callers[before:]:
                try:await asyncio.wait_for(c.close(),15)
                except Exception:pass
        row['seconds']=time.monotonic()-began;self.results.append(row);self.save()
        print(row['status'].upper(),ids,row.get('error',''),flush=True)

    def save(self):
        (self.out/'results.json').write_text(json.dumps({'results':self.results,'generated':self.speech.generated,'scope':'Live Deepgram speech through production Rust router/driver; phone caller emulated; injected faults explicitly logged; GPT Live excluded'},indent=2))
        (self.out/'turn-api.json').write_text(json.dumps(self.echo.rows,indent=2))
        (self.out/'provider-events.json').write_text(json.dumps(self.proxy.rows))
        (self.out/'echo-state.json').write_text(json.dumps(self.echo.calls,indent=2))

    async def stop(self):
        for c in self.callers:
            try:await c.close()
            except Exception:pass
        if self.process and self.process.returncode is None:
            self.process.send_signal(signal.SIGTERM)
            try:await asyncio.wait_for(self.process.wait(),35)
            except TimeoutError:self.process.kill();await self.process.wait()
        await self.runner.cleanup();await self.client.close();self.log.close();self.save()

    async def basic(self):
        c=await self.call('basic')
        text='I can pay the bill on Friday.'
        ended=await c.speak(text);commit=await c.reply()
        assert wer(text,commit['text'])<=.15,f'input recognition: {commit["text"]}'
        evidence=await self.verify(c,commit)
        chunks=c.chunks[commit['audio_id']]
        assert len(chunks)>1,'only one caller audio chunk'
        assert chunks[0][0]<next(v['_at'] for v in c.frames if v.get('type')=='server.tts_stream_end')
        evidence.update(chunks=len(chunks),speech_end_to_audio_ms=(chunks[0][0]-ended)*1000,requests=[r['op'] for r in self.echo.rows if r['body']['call_id']==c.cid])
        return evidence

    async def auth(self):
        c=await self.call('auth-live');await c.speak('This is a real voice connection.');await c.reply()
        async with self.client.get(self.base+'/v1/tenants/live/calls/auth-live') as r:assert r.status==401
        async with self.client.get(self.base+'/v1/tenants/other/calls/auth-live',headers=PAIR) as r:assert r.status==404
        ws=await self.client.ws_connect(self.base+'/ws/call?token=invalid')
        v=await ws.receive_json();assert v['payload']['code']=='invalid_token';await ws.close()
        return {'live_roundtrip':True,'unauthorized':401,'wrong_tenant':404,'invalid_token':'rejected'}

    async def text(self):
        c=await self.call('text-baseline',text=True)
        for n,text in enumerate(['hello','second turn','final turn'],1):
            await c.ws.send_json({'type':'client.message','payload':{'text':text}})
            commit=await c.reply(n);assert commit['text']==text
        return {'turns':3,'mode':'text control baseline; no speech provider claimed'}

    async def multi(self):
        c=await self.call('multi')
        texts=['My appointment is on Tuesday.','The amount is fifty dollars.','Can you repeat that question?','Actually I need to change the date.','Please call me tomorrow morning after breakfast.']
        details=[]
        for i,text in enumerate(texts,1):
            await c.speak(text);commit=await c.reply(i)
            score=wer(text,commit['text']);assert score<=.25,f'input recognition {score}: {commit["text"]}'
            details.append(dict(input_wer=score,**await self.verify(c,commit)))
        assert len(self.echo.calls[c.cid]['commits'])==5
        return details

    async def long(self):
        c=await self.call('long')
        text='I am calling about the payment we discussed last week. I would like to make the payment on Friday, but first I need to confirm the total amount and the account details.'
        await c.speak(text);commit=await c.reply();assert wer(text,commit['text'])<=.2
        assert len(self.echo.calls[c.cid]['commits'])==1
        return await self.verify(c,commit)

    async def eager(self):
        c=await self.call('eager-match');await c.speak('Could you help me with my payment?');commit=await c.reply()
        events=self.echo.events(c.cid)
        assert any(e['kind']=='utterance_eager' for e in events),'live eager event not observed'
        assert self.echo.calls[c.cid]['reuses']==1,'matching final did not reuse speculative work'
        assert len(self.echo.calls[c.cid]['commits'])==1
        return dict(reuses=1,**await self.verify(c,commit))

    async def resume(self,delayed=False):
        cid='late-eager' if delayed else 'resumed'
        first='I can pay the bill on Friday.';tail='Actually, I need to pay on Monday instead.'
        await self.speech.generate(first);await self.speech.generate(tail)
        if delayed:self.echo.eager_gates[cid]=asyncio.Event()
        c=await self.call(cid);await c.speak(first)
        await until(lambda:self.echo.events(cid,'utterance_eager'),8,'live eager event')
        await c.speak(tail)
        await until(lambda:any(r.get('value',{}).get('event')=='EndOfTurn' for r in self.proxy.rows if r.get('case')==self.proxy.case and 'monday' in r.get('value',{}).get('transcript','').lower()),10,'provider changed final')
        if delayed:self.echo.eager_gates[cid].set()
        commit=await c.reply();assert 'monday' in commit['text'].lower(),commit['text']
        assert self.echo.events(cid,'utterance_resumed'),'live resumed event not observed'
        assert len(self.echo.calls[cid]['commits'])==1
        assert not any('stale' in aid for aid in c.audio)
        return dict(events=[e['kind'] for e in self.echo.events(cid)],**await self.verify(c,commit))


async def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--live',action='store_true');parser.add_argument('--cases',default='basic');parser.add_argument('--default-eot',action='store_true');parser.add_argument('--env',default='data-plane/.env');parser.add_argument('--out',required=True);args=parser.parse_args()
    if not args.live:parser.error('--live is required')
    suite=Suite(Path(args.out).resolve(),(MONO/args.env).resolve(),args.default_eot)
    await suite.start()
    tests={'basic':([3,5],'basic-voice',suite.basic),'auth':([1],'authentication',suite.auth),'text':([2],'text-baseline',suite.text),'multi':([4,6],'multi-turn-content',suite.multi),'long':([7],'long-utterance',suite.long),'eager':([9],'matching-eager',suite.eager),'resume':([10,11],'resumed-final',suite.resume),'late':([12],'late-eager',lambda:suite.resume(True))}
    import scenarios
    tests.update({
        'silence':([8],'silence-timers',lambda:scenarios.silence(suite)),
        'replay':([13],'provider-replay',lambda:scenarios.replay(suite)),
        'slow':([14],'full-duplex-slow-app',lambda:scenarios.slow(suite)),
        'barge':([15],'barge-in',lambda:scenarios.barge(suite)),
        'stale-audio':([16],'late-provider-audio',lambda:scenarios.barge(suite,True)),
        'clock-isolation':([17],'live-clock-isolation',lambda:scenarios.clock_isolation(suite)),
        'parallel':([17],'parallel-calls',lambda:scenarios.parallel(suite)),
        'stalled':([18],'stalled-peer',lambda:scenarios.parallel(suite,True)),
        'phone':([19],'phone-roundtrip',lambda:scenarios.phone(suite)),
        'acoustic':([20],'echo-double-talk',lambda:scenarios.acoustic(suite)),
        'faults':([21],'network-faults',lambda:scenarios.faults(suite)),
        'admission':([23],'admission-malformed',lambda:scenarios.admission(suite)),
        'cycles':([24],'repeated-lifecycle',lambda:scenarios.cycles(suite)),
        'phone-parallel':([17],'parallel-native-phone-workers',lambda:scenarios.parallel_phone(suite)),
        'phone-cycles':([24],'repeated-native-phone-workers',lambda:scenarios.phone_cycles(suite)),
        'disconnect':([22],'disconnect-and-sigterm',lambda:scenarios.disconnect(suite)),
    })
    try:
        for name in (list(tests) if args.cases=='all' else args.cases.split(',')):
            ids,label,fn=tests[name];await suite.test(ids,label,fn)
    finally:await suite.stop()
    print(json.dumps({'passed':sum(r['status']=='passed' for r in suite.results),'failed':sum(r['status']=='failed' for r in suite.results),'report':str(suite.out/'results.json')}))
    return 1 if any(r['status']!='passed' for r in suite.results) else 0

if __name__=='__main__':sys.exit(asyncio.run(main()))
