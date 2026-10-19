"""Additional live-call scenarios. Faults alter delivery of real provider messages only."""
import asyncio
import json
import math
import signal
import time
from pathlib import Path
from aiohttp import WSMsgType
from infra import PAIR, normalized, until, wer


def resources(pid):
    root=Path('/proc')/str(pid)
    values={}
    for line in (root/'status').read_text().splitlines():
        if line.startswith(('VmRSS:','Threads:')):
            k,v,*_=line.split();values[k.rstrip(':')]=int(v)
    values['fds']=len(list((root/'fd').iterdir()))
    return values


async def silence(s):
    s.echo.options['silence-phone']={'silence':True}
    c=await s.call('silence-phone',phone=True)
    await c.speak('Hello, this is the silence timer test.')
    await c.reply()
    await until(lambda:s.echo.calls[c.cid]['ended']==1,15,'silence termination')
    events=s.echo.events(c.cid,'silence_threshold')
    ids=[v['threshold_id'] for v in events]
    assert ids==['check','exit'],ids
    assert len(s.echo.calls[c.cid]['commits'])==1
    return {'thresholds':events,'ended':1}


async def replay(s):
    s.proxy.duplicate=True
    c=await s.call('replay')
    phrase='Please repeat the word orange.'
    await c.speak(phrase);first=await c.reply()
    await asyncio.sleep(1.)
    assert len(s.echo.calls[c.cid]['commits'])==1,'duplicate first final committed'
    await c.speak(phrase);await c.reply(2)
    await asyncio.sleep(1.)
    assert len(s.echo.calls[c.cid]['commits'])==2,'duplicate or lost second actual utterance'
    faults=[r for r in s.proxy.rows if r.get('case')==s.proxy.case and r.get('fault')=='duplicate_provider_event']
    assert len(faults)>=2,'no genuine provider events replayed'
    return {'commits':2,'replayed_events':len(faults),'audio_check':await s.verify(c,first)}


async def slow(s):
    cid='slow-application';s.echo.final_gates[cid]=asyncio.Event()
    c=await s.call(cid)
    await c.speak('My first question is about Friday.')
    await until(lambda:s.echo.events(cid,'utterance_final'),10,'first final')
    start=time.monotonic()
    await c.speak('My new question is about Monday.')
    await until(lambda:any(r.get('case')==s.proxy.case and r.get('at',0)>start and r.get('value',{}).get('event')=='EndOfTurn' and 'monday' in r['value'].get('transcript','').lower() for r in s.proxy.rows),12,'STT while application blocked')
    assert not c.audio,'audio escaped while application blocked'
    s.echo.final_gates[cid].set()
    await until(lambda:len(s.echo.calls[cid]['commits'])==2,15,'second application response')
    final=await c.reply(2)
    assert len(c.audio)==1 and final['audio_id'] in c.audio,'stale first application response reached caller'
    return {'live_stt_progress_while_http_blocked':True,**await s.verify(c,final)}


async def barge(s,delay=False):
    cid='late-audio' if delay else 'barge'
    s.echo.options[cid]={'long_reply':True}
    if delay:s.proxy.tts_delay=2.5
    await s.speech.generate('Please stop and talk about Monday instead.')
    c=await s.call(cid)
    await c.speak('Please give me a long answer.')
    await until(lambda:s.echo.calls[cid]['commits'],15,'long answer requested')
    first=s.echo.calls[cid]['commits'][0]['audio_id']
    if not delay:await until(lambda:c.audio[first],15,'first audio')
    else:await until(lambda:any(r.get('fault')=='delay_tts_audio' and r.get('case')==s.proxy.case for r in s.proxy.rows),15,'actual provider audio held')
    interrupted_at=time.monotonic()
    await c.speak('Please stop and talk about Monday instead.')
    second=await c.reply(2,timeout=45)
    clears=[v for v in c.frames if v.get('type')=='server.tts_interrupted']
    assert clears,'no interruption control emitted'
    clear_at=clears[0]['_at']
    assert all(t<=clear_at for t,_ in c.chunks[first]),'old audio arrived after clear'
    assert 'monday' in second['text'].lower()
    return {'speech_onset_to_clear_ms':(clear_at-interrupted_at)*1000,'old_chunks':len(c.chunks[first]),'late_provider_audio_injected':delay,**await s.verify(c,second)}


async def parallel(s,stalled=False):
    all_details=[]
    for count in ([4] if stalled else [1,2,4]):
        labels=['apple','banana','orange','purple'][:count]
        calls=await asyncio.gather(*(s.call(f'{"stalled" if stalled else "parallel"}-{count}-{i}') for i in range(count)))
        if stalled:s.echo.final_gates[calls[0].cid]=asyncio.Event()
        await asyncio.gather(*(c.speak(f'The special word for this call is {label}.') for c,label in zip(calls,labels)))
        results=await asyncio.gather(*(c.reply() for c in calls[1 if stalled else 0:]))
        if stalled:
            assert s.echo.events(calls[0].cid,'utterance_final')
            assert not calls[0].audio,'stalled call unexpectedly answered'
            s.echo.final_gates[calls[0].cid].set();results.insert(0,await calls[0].reply())
        for c,label,commit in zip(calls,labels,results):
            assert label in normalized(commit['text']),f'cross-call input {c.cid}: {commit["text"]}'
            assert all(other not in normalized(commit['text']) for other in labels if other!=label)
            seq=[v['seq'] for v in c.frames if 'seq' in v]
            assert seq==list(range(1,len(seq)+1)),f'nonlocal output sequence {seq}'
            check=await s.verify(c,commit)
            all_details.append({'call':c.cid,'count':count,'wire_sequences':seq,'verification':check})
        await asyncio.gather(*(c.close() for c in calls))
    return all_details


async def phone(s):
    c=await s.call('phone',phone=True)
    await c.speak('The payment will be made on Friday.')
    commit=await c.reply();assert 'friday' in commit['text'].lower()
    assert any(v.get('event')=='mark' for v in c.frames)
    return dict(media_frames=len(c.chunks['phone']),**await s.verify(c,commit))


async def acoustic(s):
    evidence=[];failures=[]
    for cid,lag,gain,double in [('echo-low',.06,.2,False),('echo-mid',.18,.5,False),('echo-high',.30,.5,False),('double-talk',.12,.3,True)]:
        s.echo.options[cid]={'long_reply':True}
        c=await s.call(cid,phone=True);c.echo_lag=lag;c.echo_gain=gain
        row={'call':cid,'lag_ms':lag*1000,'gain':gain,'double_talk':double}
        begin=time.monotonic()
        try:
            await c.speak('Please describe the payment process.')
            await until(lambda:c.audio['phone'],20,'phone agent output')
            begin=time.monotonic()
            if double:
                expected='Please stop. I need to pay on Monday instead.'
                await c.speak(expected)
                await until(lambda:len(s.echo.calls[cid]['commits'])>=2,25,'double-talk caller recognition')
                final=s.echo.calls[cid]['commits'][-1]['text']
                row.update(expected_caller=expected,recognized_caller=final,input_wer=wer(expected,final))
                assert 'monday' in final.lower(),final
                assert row['input_wer']<=.25,f'double-talk transcript contamination, WER {row["input_wer"]:.3f}: {final}'
            else:
                await asyncio.sleep(9)
                assert len(s.echo.calls[cid]['commits'])==1,f'{cid}: echo produced a caller turn'
            row['status']='passed'
        except AssertionError as error:
            row.update(status='failed',error=str(error));failures.append(str(error))
        finally:
            end=time.monotonic()
            incoming=[r['rms']**2 for r in c.sent if begin<r['at']<end]
            outgoing=[r['rms']**2 for r in s.proxy.rows if r.get('case')==s.proxy.case and r.get('kind')=='stt' and begin<r.get('at',0)<end and 'rms' in r]
            row['window_energy_reduction_db']=10*math.log10((sum(incoming)/max(1,len(incoming))+1)/(sum(outgoing)/max(1,len(outgoing))+1))
            row['commits']=s.echo.calls[cid]['commits']
            evidence.append(row);await c.close()
            (s.out/'acoustic.json').write_text(json.dumps(evidence,indent=2))
    assert not failures,'; '.join(failures)
    return evidence


async def faults(s):
    # A retryable real HTTP transaction failure must not duplicate committed output.
    s.echo.options['http-fault']={'fail_http':True}
    c=await s.call('http-fault');await c.speak('Please confirm the blue appointment.');commit=await c.reply(timeout=45)
    assert len(s.echo.calls[c.cid]['commits'])==1
    await s.verify(c,commit);await c.close()
    # Close a real TTS connection when its first actual binary frame arrives.
    s.proxy.drop_next_tts=True
    c=await s.call('tts-fault');await c.speak('This sentence checks recovery after a connection failure.')
    await until(lambda:any(r.get('fault')=='provider_disconnect' for r in s.proxy.rows),20,'provider disconnect injected')
    await until(lambda:c.audio or any(v.get('type')=='server.error' for v in c.frames) or s.echo.calls[c.cid]['ended'],30,'TTS recovery or explicit failure')
    outcome='recovered_audio' if c.audio else 'explicit_failure'
    if c.audio:await s.verify(c,await c.reply(timeout=35))
    await c.close()
    # STT failure should close/report rather than silently accepting an unread stream.
    c=await s.call('stt-fault');await c.speak('This is the recognizer failure test.');await c.reply()
    active=[v for v in s.proxy.sockets if v['kind']=='stt' and v['active']]
    assert active
    for connection in active:await connection['remote'].close(code=1011)
    await until(lambda:s.echo.calls[c.cid]['ended'] or any(v.get('type')=='server.error' for v in c.frames),20,'STT failure observed')
    return {'http_retry_commits':1,'tts':outcome,'stt_reported_or_ended':True}


async def disconnect(s):
    evidence=[]
    for phase in ['recognition','speculation','playback']:
        cid='disconnect-'+phase
        if phase=='speculation':s.echo.eager_gates[cid]=asyncio.Event()
        c=await s.call(cid)
        if phase=='recognition':
            task=asyncio.create_task(c.speak('This caller will disconnect before the utterance has finished.'))
            await asyncio.sleep(.5)
        else:
            await c.speak('I would like some help with this question.')
            if phase=='speculation':await until(lambda:s.echo.events(cid,'utterance_eager'),10,'eager request')
            else:await until(lambda:c.audio,20,'playback audio')
        await c.close(abrupt=True)
        if phase=='recognition':task.cancel();await asyncio.gather(task,return_exceptions=True)
        await until(lambda:s.echo.calls[cid]['ended']==1,10,'exactly one EndCall after disconnect')
        evidence.append({'phase':phase,'end_calls':s.echo.calls[cid]['ended']})
    # Actual process signal while two generated-audio calls have open provider sockets.
    calls=await asyncio.gather(s.call('shutdown-a'),s.call('shutdown-b'))
    await asyncio.gather(*(c.speak('We are testing a graceful service shutdown.') for c in calls))
    s.process.send_signal(signal.SIGTERM)
    await asyncio.wait_for(s.process.wait(),35)
    assert s.process.returncode==0
    for c in calls:assert s.echo.calls[c.cid]['ended']==1
    assert not [v for v in s.proxy.sockets if v['active']],'provider socket remains after exit'
    return {'disconnects':evidence,'active_call_sigterm_exit':s.process.returncode}


async def admission(s):
    calls=await asyncio.gather(*(s.call('admission-'+str(i)) for i in range(4)))
    await asyncio.gather(*(c.speak('My admission test is running.') for c in calls))
    await asyncio.gather(*(c.reply() for c in calls))
    async with s.client.post(s.base+'/v1/tenants/live/calls',headers=PAIR,json={'call_id':'over-capacity','mode':'widget','app':{'base_url':s.local+'/turn/v1','auth_token':'echo-fixture'}}) as r:made=await r.json()
    ws=await s.client.ws_connect(made['ws_url']);msg=await ws.receive(timeout=5);assert msg.type==WSMsgType.CLOSE and msg.data==1013;await ws.close()
    # Free one permit so duplicate rejection reaches the ownership check.
    await calls[-1].close();await asyncio.sleep(.1)
    ws=await s.client.ws_connect(calls[0].made['ws_url']);msg=await ws.receive(timeout=5);assert msg.type==WSMsgType.CLOSE and msg.data==4003;await ws.close()
    await calls[1].ws.send_str('{invalid json')
    await until(lambda:s.echo.calls[calls[1].cid]['ended'],10,'invalid frame cleanup')
    await calls[2].ws.send_bytes(bytes(1048577))
    await until(lambda:s.echo.calls[calls[2].cid]['ended'],10,'oversized frame cleanup')
    await calls[0].speak('The healthy call can still say orange.');commit=await calls[0].reply(2)
    assert 'orange' in commit['text'].lower()
    return {'live_calls':4,'over_capacity_close':1013,'duplicate_close':4003,'malformed_and_oversized_cleanup':True,'healthy_call_continued':True}


async def cycles(s):
    samples=[]
    for i in range(6):
        c=await s.call('cycle-'+str(i));await c.speak('This is another complete conversation.');await c.reply();await c.close()
        async def inactive():
            for _ in range(100):
                async with s.client.get(s.base+'/health') as r:
                    if (await r.json())['active_sockets']==0:return
                await asyncio.sleep(.05)
            raise AssertionError('active calls leaked')
        await inactive();await asyncio.sleep(.2);samples.append(resources(s.process.pid))
    first,last=samples[1],samples[-1]
    assert last['fds']<=first['fds']+2,samples
    assert last['Threads']<=first['Threads']+2,samples
    assert last['VmRSS']-first['VmRSS']<65536,samples
    return {'samples':samples,'rss_units':'KiB','scope':'bounded six-call accumulation check; not a long-duration leak proof'}

async def clock_isolation(s):
    """A quiet phone's ladder fires while another phone keeps producing real speech."""
    s.echo.options['clock-a']={'silence':True}
    a=await s.call('clock-a',phone=True)
    await a.speak('Hello, this is the quiet call.')
    await a.reply()
    b=await s.call('clock-b',phone=True)
    task=asyncio.create_task(b.speak('This second call is still speaking about its own appointment and its own payment. Please keep listening while I describe the details of this independent conversation.'))
    await until(lambda:s.echo.calls[a.cid]['ended']==1,15,'quiet call timer despite active peer')
    assert s.echo.calls[b.cid]['ended']==0,'quiet peer ended the speaking call'
    assert s.echo.events(a.cid,'silence_threshold')
    assert not s.echo.events(b.cid,'silence_threshold'),'timer policy leaked to peer'
    await task
    await b.reply()
    return {'quiet_call_rungs':[e['threshold_id'] for e in s.echo.events(a.cid,'silence_threshold')],'speaking_peer_completed':True}

async def parallel_phone(s):
    """Four real speech sessions also exercise independent native audio workers."""
    labels=['apple','banana','orange','purple']
    calls=await asyncio.gather(*(s.call('parallel-phone-'+str(i),phone=True) for i in range(4)))
    await asyncio.gather(*(c.speak('The special word for this call is '+label+'.') for c,label in zip(calls,labels)))
    commits=await asyncio.gather(*(c.reply() for c in calls))
    evidence=[]
    for c,label,commit in zip(calls,labels,commits):
        assert label in normalized(commit['text']),commit['text']
        assert all(other not in normalized(commit['text']) for other in labels if other!=label)
        assert not c.errors,c.errors
        evidence.append({'call':c.cid,**await s.verify(c,commit)})
    return evidence


async def phone_cycles(s):
    """Native worker/thread lifecycle, beyond the browser-only cycle test."""
    samples=[]
    for i in range(4):
        c=await s.call('cycle-phone-'+str(i),phone=True)
        await c.speak('This phone conversation is now complete.');await c.reply();await c.close()
        await asyncio.sleep(.4)
        samples.append(resources(s.process.pid))
    first,last=samples[1],samples[-1]
    assert last['fds']<=first['fds']+2,samples
    assert last['Threads']<=first['Threads']+2,samples
    assert last['VmRSS']-first['VmRSS']<65536,samples
    return {'samples':samples,'rss_units':'KiB','phone_workers_created_and_closed':4}
