"""Replay native playback decisions against the production Python functions."""
import asyncio
import dataclasses
import itertools
import json
import random
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from vogon.gateway.pipeline.sentence_split import split_sentences
from vogon.gateway.transport.delivery import DeliveryReport, estimate_total_ms, estimate_heard_text
from vogon.gateway.transport.outbound_queue import enqueue_twilio
from vogon.gateway.transport import twilio_transport
from transport_fixtures import bare_twilio_transport


def run(h):
    rng=random.Random(20261019)
    texts=['',' ', 'Okay. Let me pull up the details on your account now.',
           'Your past-due balance is $201.42. Are you able to take care of that today?',
           '聲音測試。 Another line! End?', 'a.\u2003 b!\u00a0 c?',
           'First sentence here is long enough. Second sentence is also long enough! Last sentence asks a question?']
    for _ in range(200):
        texts.append(''.join(rng.choice(['hello','聲音','🎧','.','!','?', ' ', '\t','\n','\u00a0','1.23']) for _ in range(rng.randrange(100))))
    for i,text in enumerate(texts):
        for minimum in (-1,0,1,25,100):
            h.check(f'sentences/{i}/{minimum}',{'op':'sentences','text':text,'min_chars':minimum},lambda:split_sentences(text,minimum))
        for audio in (None,-10,0,125,99999):
            report=DeliveryReport('a','complete',audio_ms=audio,est_total_ms=estimate_total_ms(text))
            h.check(f'delivery/{i}/{audio}',{'op':'delivery','text':text,'report':dataclasses.asdict(report)},lambda:{
                'total':estimate_total_ms(text),'heard':estimate_heard_text(text,audio,report.est_total_ms),
                'fraction':report.heard_fraction,'report':dataclasses.asdict(report)})
    for i in range(100):
        entries=[]
        for k in range(rng.randrange(1,20)):
            start=1.0+k
            entries.append({'audio_id':f'a{k}','text':rng.choice(texts),'start_t':start,'end_t':rng.choice([None,start+.5,start+2.0])})
        onset=rng.choice([0,1,1.5,3.75,30])
        transport=bare_twilio_transport(_delivery_ledger=entries)
        def expected():
            horizon=transport._compute_horizon(onset)
            return dataclasses.asdict(horizon) if horizon else None
        h.check(f'heard_horizon/{i}',{'op':'horizon','entries':entries,'onset':onset},expected)
    for i in range(100):
        queue=asyncio.PriorityQueue()
        entries=[]
        for seq in range(10):
            item=rng.choice([None,['binary',[0,1,2]],['text',json.dumps({'event':rng.choice(['media','mark','clear','other'])})]])
            priority=rng.choice([-1,0,1])
            entries.append({'priority':priority,'sequence':seq,'item':item})
            queue.put_nowait((priority,seq,item))
        raws=[json.dumps({'event':rng.choice(['media','mark','clear','other']),'index':n}) for n in range(30)]
        counter=itertools.count(10)
        def expected():
            discarded=[enqueue_twilio(queue,counter,raw) for raw in raws]
            ordered=[]
            while not queue.empty():
                priority,seq,item=queue.get_nowait()
                ordered.append({'priority':priority,'sequence':seq,'item':list(item) if item else item})
            return {'discarded':discarded,'entries':ordered}
        h.check(f'queue/{i}',{'op':'queue','entries':entries,'sequence':10,'raws':raws},expected)
    sentence_streams(h,rng)
    mark_receipts(h,rng)
    for codepoint in range(0x1c, 0x20):
        text = f'First.{chr(codepoint)}Second.'
        h.intentional_difference(f'sentences/control_separator/U+{codepoint:04X}',
            {'op':'sentences','text':text,'min_chars':1}, [text],
            lambda:split_sentences(text,1))


def mark_receipts(h,rng):
    from vogon_contract.codec import to_wire
    for i in range(100):
        text='First sentence here is long enough. Second sentence is also long enough!'
        entries=[{'audio_id':'old','text':'Previous line.','start_t':1.0,'end_t':2.0},
                 {'audio_id':'a','text':text,'start_t':10.0,'end_t':None}]
        pending={'tts_s0_a':[10.0,'First sentence'], 'tts_s1_a':[10.2,'Second sentence'], 'tts_end_a':[10.3,'All sentences']}
        sentences=split_sentences(text) if i%2 else []
        events=[{'action':'observe','now':100.0}]
        events += [{'action':rng.choice(['mark','clear','close','observe']),
                    'name':rng.choice(['tts_s0_a','tts_s1_a','tts_end_a','unknown']),
                    'now':10.4+k*.375} for k in range(15)]
        async def expected():
            facts=[]
            t=bare_twilio_transport(_delivery_ledger=json.loads(json.dumps(entries)),_pending_marks=dict(pending),
                _last_audio_id='a',_last_speak_text=text,_tts_started_at=10.0,_tts_ended_at=0.0,
                _current_sentences=sentences,_floor=None,_line=None,_clocks=None,
                _enqueue_event=lambda ev:facts.append(to_wire(ev)),_send_clear=AsyncMock())
            states=[]
            for ev in events:
                now=ev['now']
                with patch.object(twilio_transport,'time',SimpleNamespace(monotonic=lambda:now)):
                    if ev['action']=='mark': await t.handle_text_frame({'event':'mark','mark':{'name':ev['name']}})
                    elif ev['action']=='clear': await t._clear_buffered_playback()
                    elif ev['action']=='close': t._close_ledger(now)
                    state={'pending':t._pending_marks,'played':t._sentences_played,'ended_at':t._tts_ended_at,
                           'ledger':t._delivery_ledger,'events':facts,'draining':t._playback_draining,
                           'offset':t._outbound_offset.value(),'heard':t._playout_heard_split()}
                    states.append(json.loads(json.dumps(state)))
            return states
        h.check(f'mark_receipts/{i}',{'op':'playout','text':text,'audio_id':'a','started_at':10.0,
            'entries':entries,'pending':pending,'sentences':sentences,'events':events},lambda:asyncio.run(expected()))


def sentence_streams(h,rng):
    text='First sentence here is long enough. Second sentence is also long enough! Last sentence asks a question?'
    for scenario in ('normal','tail','empty_chunks','yield','cancel'):
        for lead in (0,20,500,2000):
            chunks=[]
            for segment in range(3):
                for i in range(4):
                    size=0 if scenario=='empty_chunks' else (17 if scenario=='tail' else rng.randrange(1,1700))
                    chunks.append({'segment':segment,'data':list(rng.randbytes(size)),
                                   'yield':scenario=='yield' and segment==0 and i==2,
                                   'cancel':scenario=='cancel' and segment==1 and i==2})
            async def reference():
                now=10.0
                output=[]; waits=[]
                async def sleep(seconds):
                    nonlocal now
                    waits.append(seconds); now+=seconds
                async def send(frame): output.append(['media',list(frame)])
                async def mark(name): output.append(['mark',name])
                async def clear(): output.append(['clear'])
                transport=bare_twilio_transport(_floor=None,_ambience=None,_send_media_frame=send,_send_mark=mark,_send_clear=clear)
                class TTS:
                    async def stream_segments(self,sentences):
                        nonlocal now
                        for row in chunks:
                            now+=.005
                            if row['yield']: transport._graceful_yield=True
                            if row['cancel']: transport._tts_cancel.set()
                            yield row['segment'],bytes(row['data']),None
                    async def cancel(self): output.append(['cancel'])
                transport._tts=TTS()
                with patch.object(twilio_transport,'time',SimpleNamespace(monotonic=lambda:now)), \
                     patch.object(twilio_transport,'asyncio',SimpleNamespace(sleep=sleep)), \
                     patch.object(twilio_transport.settings,'tts_buffer_lead_ms',lead):
                    count,error,total,sent,heard=await transport._do_tts_stream_sentences(text,'a')
                assert not error
                return {'output':output,'waits':waits,'chunks_sent':count,'sentences_total':total,'sentences_sent':sent,'heard_text':heard}
            request={'op':'sentence_stream','text':text,'audio_id':'a','lead_ms':lead,'chunks':chunks}
            h.check(f'sentence_stream/{scenario}/{lead}',request,lambda:asyncio.run(reference()))
