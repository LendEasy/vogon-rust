"""Actual Python clock, browser callback and binary framing differential tests."""
import asyncio
import dataclasses
import json
import logging
from pathlib import Path
import random
import subprocess
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from vogon.gateway.transport.clocks import CallClocks
from vogon.gateway.transport.voice_transport import VoiceTransport, _encode_audio_frame

ROOT=Path(__file__).resolve().parent
BINARY=ROOT.parents[1]/'target/debug/transport-validation'

class Native:
    def __init__(self): self.p=subprocess.Popen([str(BINARY)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True)
    def call(self,v):
        self.p.stdin.write(json.dumps(v)+'\n');self.p.stdin.flush();return json.loads(self.p.stdout.readline())
    def close(self): self.p.stdin.close();assert self.p.wait(timeout=5)==0

FIELDS={'ladder_name':'_ladder_name','start_t':'_start_t','last_tick_t':'_last_tick_t','dur_fired':'_dur_fired','sil_armed':'_sil_armed','sil_idle':'_sil_idle','sil_fired':'_sil_fired','pause_depth':'_sil_pause_depth','paused_since':'_sil_paused_since','grace':'_sil_grace_sec','playing_depth':'_playing_depth','playing_since':'_playing_since','audio_ms':'_playout_audio_ms','watch_committed':'_watch_committed','playing_is_turn':'_playing_is_turn'}
def clocks(case):
    cfg=SimpleNamespace(**{k:[SimpleNamespace(**x) for x in value] if isinstance(value,list) else value for k,value in case['config'].items()})
    events=[];clock=CallClocks(cfg,events.append,AsyncMock());clock._start_t=clock._last_tick_t=case['epoch'];rows=[]
    for e in case['events']:
        events.clear();op=e['op']
        if op=='tick':clock.tick(e['at'])
        elif op=='activity':clock.note_caller_activity()
        elif op=='pause':clock.pause_silence()
        elif op=='resume':clock.resume_silence()
        elif op=='start':clock.note_playout_start(e['turn'])
        elif op=='sent':clock.note_playout_sent(e['ms'])
        elif op=='end':clock.note_playout_end(e['reset'])
        elif op=='ladder':clock.select_ladder(e['name'])
        elif op=='grace':clock.grant_silence_grace(e['seconds'])
        state={k:sorted(v) if isinstance(v,set) else v for k,attr in FIELDS.items() for v in [getattr(clock,attr)]}
        result=[]
        for event in events:
            row=dataclasses.asdict(event);row['kind']='duration_threshold' if type(event).__name__=='DurationThreshold' else 'silence_threshold';result.append(row)
        rows.append(dict(events=result,state=state))
    return rows

async def browser(case):
    callbacks={};now=[0.0];wire=[]
    class STT:
        def __init__(self,**kw):callbacks.update(kw)
        async def start(self):return 0
    async def send(kind,payload):wire.append(dict(type=kind,payload=payload))
    with patch('vogon.gateway.transport.voice_transport.STTSession',STT),patch('vogon.gateway.transport.voice_transport.TTSClient') as tts,patch('vogon.gateway.transport.voice_transport.time.monotonic',side_effect=lambda:now[0]):
        tts.return_value.open=AsyncMock();transport=VoiceTransport(send,AsyncMock());await transport.start();rows=[]
        for e in case['events']:
            now[0]=e['at'];wire.clear();event=e['event'];kind=event['kind']
            if kind=='resumed':await callbacks['on_turn_resumed']()
            elif kind=='interim':await callbacks['on_interim'](event['text'],event['confidence'])
            else:await callbacks['on_eager_end' if kind=='eager' else 'on_final'](event['text'],event['confidence'],event['elapsed_ms'],event['low_conf_words'])
            emitted=[]
            while not transport.utterance_queue.empty():
                value=transport.utterance_queue.get_nowait();row=dataclasses.asdict(value);row['kind']={'UtteranceEager':'utterance_eager','UtteranceFinal':'utterance_final','UtteranceResumed':'utterance_resumed'}[type(value).__name__];emitted.append(row)
            rows.append(dict(events=emitted,wire=wire.copy()))
        return rows

async def phone(case):
    from vogon.gateway.transport.twilio_transport import TwilioTransport
    from vogon.gateway.settings import settings
    callbacks={};now=[0.0];current_onset=[None]
    class STT:
        def __init__(self,**kw):callbacks.update(kw)
        async def start(self):return 0.
        def last_onset_monotonic(self):return current_onset[0]
    with patch('vogon.gateway.transport.twilio_transport.STTSession',STT),patch('vogon.gateway.transport.twilio_transport.TTSClient') as tts,patch('vogon.gateway.transport.twilio_transport.create_aec',return_value=None),patch('vogon.gateway.transport.twilio_transport.time.monotonic',side_effect=lambda:now[0]),patch.object(settings,'ambience_enabled',False):
        tts.return_value.open=AsyncMock();transport=TwilioTransport(None,AsyncMock(),start_event={'streamSid':'test'})
        transport._floor.start=lambda:None
        await transport.start();rows=[]
        for e in case['events']:
            now[0]=e['at'];current_onset[0]=e.get('onset');event=e['event'];kind=event['kind']
            if kind=='resumed':await callbacks['on_turn_resumed']()
            elif kind=='interim':await callbacks['on_interim'](event['text'],event['confidence'])
            else:await callbacks['on_eager_end' if kind=='eager' else 'on_final'](event['text'],event['confidence'],event['elapsed_ms'],event['low_conf_words'])
            emitted=[]
            while not transport.utterance_queue.empty():
                value=transport.utterance_queue.get_nowait();row=dataclasses.asdict(value);row['kind']=value.kind;emitted.append(row)
            rows.append(dict(events=emitted,wire=[]))
        await transport._floor.close()
        return rows


def main():
    logging.disable(logging.CRITICAL);native=Native();rng=random.Random(985);snapshots=0
    cfg={'silence_thresholds':[{'id':'warn','after_sec':8.},{'id':'exit','after_sec':20.}], 'opening_silence_thresholds':[{'id':'probe','after_sec':3.},{'id':'exit','after_sec':7.}], 'duration_thresholds':[{'id':'cap','after_sec':300.}], 'silence_ladder':'default'}
    try:
        for index in range(100):
            now=10.;steps=[dict(op='end',reset=True)]
            for _ in range(150):
                op=rng.choice(['tick']*5+['activity','pause','resume','start','sent','end','ladder','grace']);now+=rng.choice([.25,1,5,31,95])
                steps.append(dict(op=op,at=now,turn=rng.choice([True,False]),ms=rng.choice([0.,1000.,120000.]),reset=rng.choice([True,False]),name=rng.choice(['default','opening','missing']),seconds=rng.choice([-2.,0.,15.,60.])))
            case=dict(op='clocks',epoch=10.,config=cfg,events=steps);actual=native.call(case);expected=clocks(case)
            for i,(a,e) in enumerate(zip(actual,expected)):
                a['state']={k:a['state'][k] for k in FIELDS}
                assert a==e,(index,i,steps[i],a,e)
            snapshots+=len(steps)
        for i in range(100):
            raw=rng.randbytes(rng.randrange(300));name=rng.choice(['audio','你好','é-123',''])
            assert bytes(native.call(dict(op='browser_audio',id=name,data=list(raw))))==_encode_audio_frame(name,raw)
        for i in range(30):
            events=[]
            for j in range(20):
                kind=rng.choice(['interim','eager','resumed','final']);event=dict(kind=kind)
                if kind!='resumed':event.update(text=rng.choice(['hello','I can pay tomorrow','こんにちは']),confidence=.9)
                if kind in ['eager','final']:event.update(elapsed_ms=250.,low_conf_words=['tomorrow'])
                events.append(dict(at=10.+j,event=event))
            case=dict(mode='browser',events=events);a=native.call(case);e=asyncio.run(browser(case));assert a==e,(i,a,e)
        for i in range(30):
            events=[]
            for j in range(20):
                kind=rng.choice(['interim','eager','resumed','final']);event=dict(kind=kind)
                if kind!='resumed':event.update(text=rng.choice(['hello','I can pay tomorrow','こんにちは']),confidence=.9)
                if kind in ['eager','final']:event.update(elapsed_ms=250.,low_conf_words=['tomorrow'])
                events.append(dict(at=10.+j,event=event,onset=9.+j if kind in ['eager','final'] else None))
            case=dict(mode='twilio',events=events);a=native.call(case);e=asyncio.run(phone(case))
            for j,(actual,expected) in enumerate(zip(a,e)):assert actual==expected,(i,j,actual,expected)
    finally:native.close()
    result=dict(passed=True,clock_traces=100,clock_snapshots=snapshots,browser_framing=100,browser_callback_traces=30,browser_callbacks=600,phone_callback_traces=30,phone_callbacks=600)
    (ROOT/'results/parity.json').write_text(json.dumps(result,indent=2)+'\n');print(result)
if __name__=='__main__':main()
