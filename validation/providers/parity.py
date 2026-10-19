"""Compare Rust speech state with actual Python STT/TTS classes on injected time."""
import asyncio
import json
from pathlib import Path
import random
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from deepgram.listen.v2.types import ListenV2TurnInfo
from vogon.gateway.pipeline.stt import STTSession, merge_resumed_transcripts
from vogon.gateway.pipeline.tts import _strip_wav_header, TTSClient
from vogon.gateway.settings import settings

ROOT = Path(__file__).resolve().parent
WORKSPACE = ROOT.parents[1]
BINARY = WORKSPACE / 'target/debug/provider-validation'


class Native:
    def __init__(self):
        self.process = subprocess.Popen([str(BINARY)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    def call(self, value):
        self.process.stdin.write(json.dumps(value)+'\n'); self.process.stdin.flush()
        line = self.process.stdout.readline()
        assert line, 'native validator exited'
        return json.loads(line)
    def close(self):
        self.process.stdin.close()
        assert self.process.wait(timeout=5) == 0


async def oracle(case):
    now = [case['epoch']]
    callbacks, timers = [], []
    original_sleep = asyncio.sleep
    config = case['config']
    async def interim(text, confidence): callbacks.append(dict(kind='interim', text=text, confidence=confidence))
    async def final(text, confidence, elapsed_ms, low_conf_words): callbacks.append(dict(kind='final', text=text, confidence=confidence, elapsed_ms=elapsed_ms, low_conf_words=low_conf_words))
    async def eager(text, confidence, elapsed_ms, low_conf_words): callbacks.append(dict(kind='eager', text=text, confidence=confidence, elapsed_ms=elapsed_ms, low_conf_words=low_conf_words))
    async def resumed(): callbacks.append(dict(kind='resumed'))
    async def sleep(seconds):
        future = asyncio.get_running_loop().create_future()
        timers.append((now[0]+seconds, future))
        await future
    rows=[]
    with patch('vogon.gateway.pipeline.stt.time.monotonic', side_effect=lambda: now[0]), \
         patch('vogon.gateway.pipeline.stt.asyncio.sleep', side_effect=sleep), \
         patch.object(settings, 'deepgram_eot_hold_ms', config.get('hold_ms', 250)), \
         patch.object(settings, 'stt_word_conf_threshold', config.get('word_conf_threshold', .75)):
        session=STTSession(on_interim=interim,on_final=final,
            eager_eot_threshold=config.get('eager_eot_threshold'),
            on_eager_end=eager if config.get('eager_callback',True) else None,
            on_turn_resumed=resumed if config.get('resumed_callback',True) else None)
        for step in case['events']:
            now[0]=step['at']; callbacks.clear()
            if step.get('op')=='tick':
                for deadline, future in timers:
                    if deadline<=now[0] and not future.done(): future.set_result(None)
            elif step.get('op')=='close':
                if step.get('requested'): session._cancel_eot_hold()
                session._on_close(None)
            else:
                message=MagicMock(spec=ListenV2TurnInfo)
                row=step['message']; message.event=row['event'];message.transcript=row.get('transcript')
                message.end_of_turn_confidence=.9
                message.words=[SimpleNamespace(word=w['word'],confidence=w['confidence'],start=w.get('start')) for w in row.get('words',[])]
                session._on_message(message)
            await original_sleep(0); await original_sleep(0)
            deadline=next((at for at, future in reversed(timers) if not future.done()),None)
            rows.append({'emitted':callbacks.copy(),'state':{'pending_text':session._pending_text,
                'pending_confidence':session._pending_confidence,'resumed_prefix':session._resumed_prefix,
                'onset':session.last_onset_monotonic(),'deadline':deadline,'quality':session.quality_stats(),'eager_mode':session._eager_mode}})
        session._cancel_eot_hold()
        await original_sleep(0)
    return rows


def turn(at,event,text='hello',words=None):
    return {'at':at,'message':{'event':event,'transcript':text,'words':words if words is not None else [{'word':text,'confidence':.9,'start':.25}]}}


def traces():
    groups=[
        [turn(1,'EagerEndOfTurn','Can I pay it'),turn(1.1,'TurnResumed',''),turn(1.2,'Update','on the phone?'),turn(2,'EndOfTurn','on the phone?'),{'op':'tick','at':3}],
        [turn(1,'EagerEndOfTurn','Can I pay it'),turn(1.1,'TurnResumed',''),turn(1.2,'EagerEndOfTurn','on the phone?'),turn(1.3,'TurnResumed',''),turn(2,'EndOfTurn','tomorrow'),{'op':'tick','at':3}],
        [turn(1,'EagerEndOfTurn','hello'),{'op':'close','at':2},{'op':'close','at':3}],
        [turn(1,'EagerEndOfTurn','hello'),turn(1.1,'TurnResumed',''),turn(2,'EndOfTurn','',[]),{'op':'tick','at':3}],
        [turn(1,'EndOfTurn','held'),turn(1.1,'Update','continues'),{'op':'tick','at':2}],
        [turn(1,'EndOfTurn','held'),turn(1.1,'StartOfTurn','continues'),{'op':'tick','at':2}],
        [turn(1,'EndOfTurn','held'),turn(1.1,'StartOfTurn','',[]),{'op':'tick','at':2}],
        [turn(1,'EndOfTurn','held'),{'op':'close','requested':True,'at':1.1},{'op':'tick','at':2}],
        [turn(1,'EndOfTurn','held'),{'op':'close','requested':False,'at':1.1},{'op':'tick','at':2}],
        [turn(1,'EndOfTurn','hello',[{'word':'hello','confidence':.5,'start':0}]),turn(2,'EndOfTurn','there',[]),{'op':'tick','at':3}],
    ]
    rng=random.Random(731)
    for _ in range(60):
        steps=[]
        for index in range(30):
            at=1+index*.25
            if rng.random()<.25: steps.append({'op':'tick','at':at});continue
            event=rng.choice(['Update','StartOfTurn','EagerEndOfTurn','TurnResumed','EndOfTurn'])
            text=rng.choice(['','Can I pay it','it on the phone?','on the phone?','HELLO, there!','yes','こんにちは'])
            words=[{'word':w,'confidence':rng.choice([.5,.75,.9,1.0]),'start':rng.choice([None,0,.25])} for w in text.split()]
            steps.append(turn(at,event,text,words))
        steps.append({'op':'tick','at':10});steps.append({'op':'close','at':11})
        groups.append(steps)
    for config in ({},{'eager_eot_threshold':.7},{'eager_eot_threshold':.95,'eot_threshold':.85},
                   {'eager_eot_threshold':.7,'eager_callback':False},{'eager_eot_threshold':.7,'resumed_callback':False}):
        for steps in groups: yield {'config':config,'epoch':0.,'events':steps}


async def tts_oracle(case):
    from deepgram.speak.v1.types import SpeakV1Flushed
    now=[case['epoch']];sent=[]
    class Socket:
        async def send_text(self,text):sent.append(('Speak',text.text))
        async def send_flush(self):sent.append(('Flush',None))
        def __aiter__(self):return self.messages()
        async def messages(self):
            for event in case['events']:
                now[0]=event['at']
                if 'data' in event:yield bytes(event['data'])
                elif event['type']=='Flushed':yield MagicMock(spec=SpeakV1Flushed,sequence_id=1)
                else:yield SimpleNamespace(type=event['type'])
    with patch('vogon.gateway.pipeline.tts.time.monotonic',side_effect=lambda:now[0]):
        client=TTSClient();client._ws_client=Socket()
        rows=[dict(segment=segment,data=list(data),first_chunk_ms=latency) async for segment,data,latency in client.stream_segments(['segment']*case['segments'])]
    assert sent==[item for _ in range(case['segments']) for item in [('Speak','segment'),('Flush',None)]]
    return rows


async def live_oracle(case):
    from vogon.gateway.pipeline.live_session import LiveSession
    now=[case['epoch']];emitted=[];deadline=[None]
    async def interim(text,confidence):emitted.append(dict(kind='interim',text=text,confidence=confidence))
    async def final(text,confidence,elapsed_ms,low_conf_words):emitted.append(dict(kind='final',text=text,confidence=confidence,elapsed_ms=elapsed_ms,low_conf_words=low_conf_words))
    def arm():deadline[0]=now[0]+case['config']['final_quiet_ms']/1000
    async def no_sleep(_):pass
    rows=[]
    with patch('vogon.gateway.pipeline.live_session.time.monotonic',side_effect=lambda:now[0]),patch('vogon.gateway.pipeline.live_session.asyncio.sleep',side_effect=no_sleep):
        client=LiveSession(interim,final);client._arm_quiet=arm
        for e in case['events']:
            now[0]=e['at'];emitted.clear()
            if e.get('op')=='tick':
                if deadline[0] is not None and now[0]>=deadline[0]:deadline[0]=None;await client._quiet_final()
            else:
                message=e['message'];kind=message['type']
                if kind=='session.input_transcript.delta':await client._on_delta(SimpleNamespace(**message))
                elif kind=='session.delegation.created':deadline[0]=None;await client._on_delegation(SimpleNamespace(**message))
            rows.append(dict(emitted=emitted.copy(),onset=client.last_onset_monotonic(),quality=client.quality_stats()))
    return rows


def main():
    native=Native(); count=0; events=0
    try:
        for case in traces():
            expected=asyncio.run(oracle(case));actual=native.call(case)['rows']
            if actual!=expected:
                for index,(a,e) in enumerate(zip(actual,expected)):
                    if a!=e: raise AssertionError((count,index,case['events'][index],a,e))
            count+=1;events+=len(case['events'])
        rng=random.Random(8)
        pieces=['','hello','Hello, there','there friend','hello there friend','can I pay','I pay tomorrow','שלום','ÉCOLE','a  b','a\u2003b']
        for _ in range(300):
            a,b=rng.choice(pieces),rng.choice(pieces)
            assert native.call({'op':'merge','prefix':a,'text':b})==merge_resumed_transcripts(a,b)
        for _ in range(100):
            raw=rng.randbytes(rng.randrange(100))
            if rng.random()<.5: raw=b'RIFF'+bytes(4)+b'WAVE'+b'data'+len(raw).to_bytes(4,'little')+raw
            assert bytes(native.call({'op':'strip','data':list(raw)}))==_strip_wav_header(raw)
        for index in range(120):
            segments=rng.randrange(1,5);steps=[];at=10.
            for segment in range(segments):
                for _ in range(rng.randrange(1,5)):
                    at+=.125;raw=rng.randbytes(rng.randrange(0,100))
                    if rng.random()<.5:raw=b'RIFF'+bytes(4)+b'WAVE'+b'data'+len(raw).to_bytes(4,'little')+raw
                    steps.extend([{'at':at,'type':'Metadata'},{'at':at,'data':list(raw)}])
                steps.append({'at':at,'type':'Flushed'})
            case={'op':'tts','segments':segments,'epoch':10.,'events':steps}
            expected=asyncio.run(tts_oracle(case));actual=[r['chunk'] for r in native.call(case) if r['chunk'] is not None]
            assert actual==expected,(index,actual,expected)
        for index in range(80 if "--include-live-listener" in sys.argv else 0):
            steps=[];at=10.
            for _ in range(40):
                at+=rng.choice([.125,.25,1.]);kind=rng.choice(['delta','delta','delegate','tick','output'])
                if kind=='tick':steps.append({'op':'tick','at':at})
                else:
                    message={'type':{'delta':'session.input_transcript.delta','delegate':'session.delegation.created','output':'session.output_audio.delta'}[kind]}
                    if kind=='delta':message.update(delta=rng.choice(['','hello',' there','I can pay']),start_ms=rng.choice([None,0.,125.,500.]))
                    steps.append({'at':at,'message':message})
            case={'op':'live','config':{'final_quiet_ms':250},'epoch':10.,'events':steps}
            actual=native.call(case);expected=asyncio.run(live_oracle(case));assert actual==expected,(index,actual,expected)
    finally: native.close()
    result={'passed':True,'stt_traces':count,'stt_events_and_snapshots':events,'merge_cases':300,'wav_cases':100,'tts_segment_traces':120,'live_listener_traces':80 if '--include-live-listener' in sys.argv else 0,'live_listener_snapshots':3200 if '--include-live-listener' in sys.argv else 0,'live_listener_skipped':'--include-live-listener' not in sys.argv,
            'scope':'Actual Python STT callbacks/state with deterministic clock and hold scheduling; no network'}
    (ROOT/'results').mkdir(exist_ok=True)
    (ROOT/'results/parity.json').write_text(json.dumps(result,indent=2)+'\n')
    print(result)


if __name__=='__main__': main()
