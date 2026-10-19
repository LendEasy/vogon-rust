"""Live test infrastructure: generated audio, echo API, provider proxy and caller.
No production credentials are persisted in artifacts. All transcripts are synthetic.
"""
import asyncio
import audioop
import base64
from collections import defaultdict, deque
import hashlib
import json
import math
import os
from pathlib import Path
import re
import signal
import socket
import struct
import time
import wave

from aiohttp import ClientSession, ClientTimeout, WSMsgType, web
from dotenv import dotenv_values

MONO = Path(__file__).resolve().parents[3]
PAIR = {'Authorization': 'Bearer live-fixture-pair'}


def port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def normalized(text):
    """Compare spoken number values while retaining currency/date/name content."""
    text = re.sub(r'\$(\d+(?:\.\d+)?)', r'\1 dollars', text.lower())
    tokens = re.findall(r"[a-z0-9]+", text)
    units = dict(zip('zero one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen seventeen eighteen nineteen'.split(), range(20)))
    units.update(dict(zip('twenty thirty forty fifty sixty seventy eighty ninety'.split(), range(20,100,10))))
    result = []; index = 0
    while index < len(tokens):
        token = tokens[index]
        if token in units:
            value = units[token]
            # Combine only a grammatical tens-plus-unit pair. A sequence such as
            # "one two" must remain two digits, never become the arithmetic sum.
            if value >= 20 and index+1 < len(tokens) and 0 < units.get(tokens[index+1], -1) < 10:
                value += units[tokens[index+1]]; index += 1
            result.append(str(value))
        else:
            result.append(token)
        index += 1
    return result


def wer(reference, hypothesis):
    a, b = normalized(reference), normalized(hypothesis)
    previous = list(range(len(b)+1))
    for i, x in enumerate(a, 1):
        current = [i]
        for j, y in enumerate(b, 1):
            current.append(min(previous[j]+1, current[-1]+1, previous[j-1]+(x != y)))
        previous = current
    return previous[-1]/max(1, len(a))


def wav(path, data, rate):
    with wave.open(str(path), 'wb') as w:
        w.setnchannels(1); w.setsampwidth(2); w.setframerate(rate); w.writeframes(data)


async def until(predicate, seconds=30, label='condition'):
    end = time.monotonic()+seconds
    while not predicate():
        if time.monotonic() > end:
            raise AssertionError(f'timeout: {label}')
        await asyncio.sleep(.02)
    return predicate()


class Speech:
    """Generate/cache actual provider audio and independently transcribe received audio."""
    def __init__(self, client, env_path, out):
        values = dotenv_values(env_path)
        self.key = os.environ.get('DEEPGRAM_API_KEY') or values.get('DEEPGRAM_API_KEY')
        assert self.key, 'Deepgram credentials missing'
        self.client, self.out = client, out
        self.cache = MONO/'vogon-rust/validation/live/audio-cache'
        self.cache.mkdir(exist_ok=True)
        self.generated = []

    async def generate(self, text):
        identity = hashlib.sha256(('aura-2-thalia-en|16000|'+text).encode()).hexdigest()[:20]
        path = self.cache/(identity+'.pcm')
        if not path.exists():
            async with self.client.post('https://api.deepgram.com/v1/speak', params={'model':'aura-2-thalia-en','encoding':'linear16','sample_rate':'16000','container':'none'}, headers={'Authorization':'Token '+self.key},json={'text':text}) as r:
                assert r.status == 200, f'input TTS HTTP {r.status}'
                data = await r.read()
            assert data and len(data)%2 == 0 and not data.startswith(b'RIFF'), 'invalid generated PCM'
            path.write_bytes(data)
            path.with_suffix('.json').write_text(json.dumps({'text':text,'model':'aura-2-thalia-en','sample_rate':16000,'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()},indent=2))
        data = path.read_bytes()
        self.generated.append({'text':text,'file':str(path.relative_to(MONO)),'seconds':len(data)/32000})
        return data

    async def transcribe(self, data, rate):
        assert data, 'no returned audio'
        async with self.client.post('https://api.deepgram.com/v1/listen', params={'model':'nova-3','encoding':'linear16','sample_rate':str(rate),'channels':'1','smart_format':'true'},headers={'Authorization':'Token '+self.key,'Content-Type':'application/octet-stream'},data=data) as r:
            assert r.status == 200, f'verification STT HTTP {r.status}'
            result = await r.json()
        return result['results']['channels'][0]['alternatives'][0]['transcript']


class Echo:
    """Per-call speculative cache and controlled response gates; no model calls."""
    def __init__(self):
        self.rows = []
        self.calls = {}
        self.options = {}
        self.final_gates = {}
        self.eager_gates = {}

    def events(self, cid, kind=None):
        return [e for row in self.rows if row['body'].get('call_id') == cid for e in row['body'].get('events',[]) if kind is None or e.get('kind') == kind]

    async def handle(self, request):
        assert request.headers.get('Authorization') == 'Bearer echo-fixture', 'Turn API binding auth missing'
        body = await request.json(); cid = body['call_id']; op = request.match_info['op']
        row = {'at':time.monotonic(),'op':op,'body':body}; self.rows.append(row)
        options = self.options.get(cid,{})
        if op == 'start_call':
            self.calls[cid] = {'prepared':None,'builds':0,'reuses':0,'commits':[],'responses':{},'ended':0}
            config = {'persona_voice':'aura-2-thalia-en','language':'en','ambience':{'enabled':False},'barge_policy':{'semantic_judge':False},'silence_thresholds':[], 'opening_silence_thresholds':[], 'duration_thresholds':[]}
            if options.get('silence'):
                thresholds = [{'id':'check','after_sec':2.},{'id':'exit','after_sec':5.}]
                config.update(silence_thresholds=thresholds, opening_silence_thresholds=thresholds)
            return web.json_response({'call_config':config})
        state = self.calls[cid]
        if op == 'end_call':
            state['ended'] += 1
            return web.json_response({})
        rid = body['request_id']
        if rid in state['responses']:
            row['retry'] = True
            return web.json_response(state['responses'][rid])
        if options.get('fail_http') and not options.get('failed_once'):
            options['failed_once'] = True
            return web.json_response({'error':'injected retryable failure'},status=503)
        event = body['events'][-1]; kind = event['kind']; text = event.get('text','')
        response = {}
        if kind == 'utterance_eager':
            state['builds'] += 1
            state['prepared'] = (text, 'You said: '+text)
            if cid in self.eager_gates:
                await self.eager_gates[cid].wait()
                response = {'speak':[{'audio_id':cid+'-stale','text':'This stale answer must never be spoken.'}]}
        elif kind == 'utterance_resumed':
            state['prepared'] = None
        elif kind == 'utterance_final':
            if cid in self.final_gates:
                await self.final_gates[cid].wait()
            prepared = state['prepared']
            if prepared and prepared[0] == text:
                state['reuses'] += 1; answer = prepared[1]
            else:
                state['builds'] += 1; answer = 'You said: '+text
            state['prepared'] = None
            if options.get('long_reply') and not state['commits']:
                answer = ('This is a deliberately long reply so you can interrupt me while I am still speaking. '*5)
            audio_id = cid+'-reply-'+str(len(state['commits'])+1)
            state['commits'].append({'text':text,'answer':answer,'audio_id':audio_id,'at':time.monotonic()})
            response = {'speak':[{'audio_id':audio_id,'text':answer}]}
        elif kind == 'silence_threshold' and event['threshold_id'] == 'exit':
            response = {'action':'end_call','end_reason':'silence_test'}
        state['responses'][rid] = response
        row['responded_at'] = time.monotonic()
        return web.json_response(response)


class Proxy:
    """Forward actual provider streams. Fault actions and their exact delivery are logged."""
    def __init__(self, client):
        self.client = client
        self.case = 'bootstrap'
        self.rows = []
        self.sockets = []
        self.duplicate = False
        self.tts_delay = 0.
        self.drop_next_tts = False
        self.delayed = set()

    async def handle(self, request):
        kind = request.match_info['kind']
        case = self.case
        upstream = 'wss://api.deepgram.com/'+('v2/listen' if kind == 'stt' else 'v1/speak')
        downstream = web.WebSocketResponse(autoping=True)
        await downstream.prepare(request)
        try:
            remote = await self.client.ws_connect(upstream+'?'+request.query_string, headers={'Authorization':request.headers['Authorization']},autoping=True)
        except Exception as e:
            self.rows.append({'case':case,'kind':kind,'error':type(e).__name__,'status':getattr(e,'status',None)})
            await downstream.close(); return downstream
        sid = len(self.sockets)
        self.rows.append({'at':time.monotonic(),'case':case,'socket':sid,'kind':kind,'connection_query':dict(request.query)})
        entry = {'id':sid,'case':case,'kind':kind,'remote':remote,'downstream':downstream,'active':True}
        received_audio = hashlib.sha256()
        self.sockets.append(entry)
        async def forward(source, target, direction):
            async for msg in source:
                now = time.monotonic()
                if msg.type == WSMsgType.TEXT:
                    value = json.loads(msg.data)
                    self.rows.append({'at':now,'case':case,'socket':sid,'kind':kind,'direction':direction,'value':value})
                    await target.send_str(msg.data)
                    if direction=='receive' and kind=='stt' and self.duplicate and value.get('event') in ('EagerEndOfTurn','EndOfTurn'):
                        await target.send_str(msg.data)
                        self.rows.append({'at':time.monotonic(),'case':case,'socket':sid,'fault':'duplicate_provider_event','event':value['event']})
                elif msg.type == WSMsgType.BINARY:
                    if kind == 'tts' and direction == 'receive':
                        received_audio.update(msg.data)
                    row={'at':now,'case':case,'socket':sid,'kind':kind,'direction':direction,'bytes':len(msg.data)}
                    if kind=='stt':
                        pcm=audioop.ulaw2lin(msg.data,2) if request.query.get('encoding')=='mulaw' else msg.data
                        row['rms']=audioop.rms(pcm,2)
                    self.rows.append(row)
                    if kind=='tts' and direction=='receive' and self.drop_next_tts:
                        self.drop_next_tts=False;row['fault']='provider_disconnect';await source.close();return
                    if kind=='tts' and direction=='receive' and self.tts_delay and sid not in self.delayed:
                        self.delayed.add(sid);row['fault']='delay_tts_audio';await asyncio.sleep(self.tts_delay)
                    await target.send_bytes(msg.data)
                elif msg.type in (WSMsgType.ERROR,WSMsgType.CLOSE,WSMsgType.CLOSED):
                    return
        tasks=[asyncio.create_task(forward(downstream,remote,'send')),asyncio.create_task(forward(remote,downstream,'receive'))]
        try:
            _,pending=await asyncio.wait(tasks,return_when=asyncio.FIRST_COMPLETED)
            for task in pending: task.cancel()
            await asyncio.gather(*tasks,return_exceptions=True)
        finally:
            await remote.close();await downstream.close();entry['active']=False
            entry['audio_sha256'] = received_audio.hexdigest()
        return downstream


class Caller:
    """Continuous 20-ms caller media, independent receive loop and explicit artifacts."""
    def __init__(self, suite, cid, phone=False, text=False):
        self.suite=suite;self.cid=cid;self.phone=phone;self.text=text
        self.frames=[];self.audio=defaultdict(bytearray);self.chunks=defaultdict(list)
        self.queue=deque();self.echo=deque();self.echo_gain=0.;self.echo_lag=.12
        self.running=True;self.tasks=[];self.sent=[];self.input=bytearray();self.playout_until=0.
        self.errors=[]

    async def open(self):
        s=self.suite
        if self.phone:
            self.ws=await s.client.ws_connect(s.base+'/ws/twilio')
            await self.ws.send_json({'event':'connected'})
            await self.ws.send_json({'event':'start','start':{'callSid':'CA-'+self.cid,'streamSid':'MZ-'+self.cid}})
        else:
            async with s.client.post(s.base+'/v1/tenants/live/calls',headers=PAIR,json={'call_id':self.cid,'mode':'text' if self.text else 'widget','app':{'base_url':s.local+'/turn/v1','auth_token':'echo-fixture'},'app_payload':{'fixture':self.cid}}) as r:
                assert r.status==200,f'create HTTP {r.status}'
                self.made=await r.json()
            self.ws=await s.client.ws_connect(self.made['ws_url'])
        self.tasks.append(asyncio.create_task(self.receive()))
        if not self.text:self.tasks.append(asyncio.create_task(self.pump()))
        if self.phone:
            await until(lambda:self.cid in s.echo.calls,15,'phone StartCall')
            await asyncio.sleep(1.)
        else:
            await until(lambda:any(v.get('type')=='server.session_ready' for v in self.frames),15,'session ready')
        s.callers.append(self)
        return self

    async def receive(self):
        try:
            async for msg in self.ws:
                now=time.monotonic()
                if msg.type==WSMsgType.BINARY:
                    n=struct.unpack('<I',msg.data[:4])[0]
                    assert 0<n<=len(msg.data)-4,'invalid browser audio framing'
                    audio_id=msg.data[4:4+n].decode();pcm=msg.data[4+n:]
                    assert len(pcm)%2==0,'unaligned PCM';self.audio[audio_id].extend(pcm);self.chunks[audio_id].append((now,len(pcm)))
                elif msg.type==WSMsgType.TEXT:
                    v=json.loads(msg.data);v['_at']=now;self.frames.append(v)
                    if self.phone:
                        assert v.get('event') in ('media','mark','clear'), 'unsupported Twilio message type'
                    if v.get('event')=='media':
                        assert v['streamSid']=='MZ-'+self.cid
                        pcm=audioop.ulaw2lin(base64.b64decode(v['media']['payload']),2)
                        self.audio['phone'].extend(pcm);self.chunks['phone'].append((now,len(pcm)))
                        self.playout_until=max(now,self.playout_until)+len(pcm)/16000
                        if self.echo_gain:
                            for i in range(0,len(pcm),320):self.echo.append((now+self.echo_lag+i/16000,audioop.mul(pcm[i:i+320],2,self.echo_gain)))
                    if v.get('event')=='mark':
                        async def mark(value,deadline):
                            await asyncio.sleep(max(0,deadline-time.monotonic()))
                            if not self.ws.closed:await self.ws.send_json({'event':'mark','streamSid':'MZ-'+self.cid,'mark':value['mark']})
                        self.tasks.append(asyncio.create_task(mark(v,self.playout_until)))
                    if v.get('event')=='clear':self.echo.clear();self.playout_until=now
                elif msg.type==WSMsgType.ERROR:break
        except Exception as e:self.errors.append(str(e) if isinstance(e,AssertionError) else type(e).__name__)

    async def pump(self):
        size=320 if self.phone else 640
        deadline=time.monotonic()
        try:
            while self.running and not self.ws.closed:
                pcm,done=self.queue.popleft() if self.queue else (bytes(size),None)
                if self.phone and self.echo and self.echo[0][0]<=time.monotonic():
                    _,echo=self.echo.popleft();pcm=audioop.add(pcm,echo.ljust(size,b'\0'),2)
                self.input.extend(pcm)
                self.sent.append({'at':time.monotonic(),'rms':audioop.rms(pcm,2)})
                if self.phone:await self.ws.send_json({'event':'media','streamSid':'MZ-'+self.cid,'media':{'payload':base64.b64encode(audioop.lin2ulaw(pcm,2)).decode()}})
                else:await self.ws.send_bytes(pcm)
                if done and not done.done():done.set_result(time.monotonic())
                deadline+=.02
                await asyncio.sleep(max(0,deadline-time.monotonic()))
        except Exception as e:
            if self.running and not self.ws.closed:self.errors.append(type(e).__name__)

    async def speak(self,text):
        data=await self.suite.speech.generate(text)
        if self.phone:data,_=audioop.ratecv(data,2,1,16000,8000,None)
        size=320 if self.phone else 640
        future=asyncio.get_running_loop().create_future()
        parts=[data[i:i+size].ljust(size,b'\0') for i in range(0,len(data),size)]
        for i,chunk in enumerate(parts):self.queue.append((chunk,future if i==len(parts)-1 else None))
        return await asyncio.wait_for(future,30)

    async def reply(self,index=1,timeout=35):
        state=await until(lambda:self.suite.echo.calls.get(self.cid),timeout,'call state')
        await until(lambda:len(state['commits'])>=index,timeout,'committed echo reply')
        commit=state['commits'][index-1];aid=commit['audio_id']
        if self.phone:
            await until(lambda:any(v.get('event')=='mark' and v.get('mark',{}).get('name')=='tts_end_'+aid for v in self.frames),timeout,'terminal phone mark')
            await asyncio.sleep(max(0,self.playout_until-time.monotonic())+.2)
        elif self.text:await until(lambda:any(v.get('type')=='server.message' and v.get('payload',{}).get('audio_id')==aid for v in self.frames),timeout,'text reply')
        else:await until(lambda:any(v.get('type')=='server.tts_stream_end' and v.get('payload',{}).get('audio_id')==aid for v in self.frames),timeout,'TTS end')
        return commit

    async def close(self,abrupt=False):
        if not self.ws.closed:
            if not abrupt:
                await self.ws.send_json({'event':'stop'} if self.phone else {'type':'client.hangup'})
                try:await until(lambda:self.suite.echo.calls.get(self.cid,{}).get('ended'),8,'EndCall')
                except AssertionError:pass
            await self.ws.close()
        self.running=False
        for t in self.tasks:t.cancel()
        await asyncio.gather(*self.tasks,return_exceptions=True)
        self.save()

    def save(self):
        out=self.suite.out/self.cid;out.mkdir(exist_ok=True)
        (out/'frames.json').write_text(json.dumps(self.frames,indent=2))
        (out/'input-timing.json').write_text(json.dumps(self.sent))
        (out/'output-timing.json').write_text(json.dumps(dict(self.chunks)))
        if self.input:wav(out/'caller.wav',bytes(self.input),8000 if self.phone else 16000)
        for aid,pcm in self.audio.items():wav(out/(aid+'.wav'),bytes(pcm),8000 if self.phone else 24000)


async def serve(app):
    runner=web.AppRunner(app,access_log=None);await runner.setup()
    listener=web.TCPSite(runner,'127.0.0.1',0);await listener.start()
    return runner,listener._server.sockets[0].getsockname()[1]
