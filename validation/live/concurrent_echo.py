"""Concurrent production gateway calls against the standalone Rust echo example.

--live uses real streaming Deepgram STT/TTS and generated caller audio; otherwise
text calls measure gateway/Turn concurrency without provider limits. The observing
proxy forwards unchanged Turn requests/responses to the actual example binary.
Results are bounded observations of this host/build, not a production capacity SLA.
"""
import argparse
import asyncio
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import signal
import time

from aiohttp import ClientSession, ClientTimeout, TCPConnector, web
from infra import MONO, Caller, Proxy, Speech, port, serve, until, wer, normalized


class ObservedEcho:
    """Observe the real echo service; never implement replies or speculative decisions."""
    def __init__(self, client, upstream):
        self.client, self.upstream = client, upstream
        self.calls, self.rows, self.transports = {}, [], set()

    async def handle(self, request):
        body = await request.json()
        cid, op = body['call_id'], request.match_info['op']
        self.transports.add(request.transport)
        row = {'at': time.monotonic(), 'op': op, 'body': body,
               'connection': id(request.transport)}
        self.rows.append(row)
        async with self.client.post(self.upstream + '/' + op, json=body,
                                    headers={'Authorization': request.headers.get('Authorization', '')}) as response:
            data = await response.read()
            row.update(status=response.status, end=time.monotonic())
            if response.status == 200:
                value = json.loads(data)
                row['response'] = value
                state = self.calls.setdefault(cid, {'commits': [], 'ended': False})
                if op == 'end_call':
                    state['ended'] = True
                for speech in value.get('speak', []):
                    assert speech['audio_id'].startswith(cid + '-echo-'), 'cross-call audio ID'
                    if not any(c['audio_id'] == speech['audio_id'] for c in state['commits']):
                        finals = [e for e in body.get('events', []) if e.get('kind') == 'utterance_final']
                        state['commits'].append({'audio_id': speech['audio_id'], 'answer': speech['text'],
                                                 'text': finals[-1]['text'] if finals else '',
                                                 'at': row['at']})
            return web.Response(body=data, status=response.status, content_type='application/json')


def percentiles(values):
    """Summarize observed milliseconds without extrapolating a capacity ceiling."""
    if not values:
        return {}
    values = sorted(values)
    return {'mean': sum(values)/len(values), 'p50': values[int((len(values)-1)*.5)],
            'p95': values[int((len(values)-1)*.95)], 'max': values[-1]}


class Suite:
    """Own subprocesses, callers, observer and bounded batches; always tear everything down."""
    def __init__(self, args):
        self.args = args
        self.out = Path(args.out).resolve()
        self.out.mkdir(parents=True, exist_ok=True)
        self.callers, self.processes, self.logs, self.results = [], [], [], []

    async def launch(self, name, *command, env):
        log = (self.out / (name + '.log')).open('w')
        self.logs.append(log)
        process = await asyncio.create_subprocess_exec(*command, env=env, cwd=MONO, stdout=log, stderr=log)
        self.processes.append((name, process))
        return process

    async def ready(self, base, process):
        for _ in range(300):
            assert process.returncode is None, 'process exited; inspect logs'
            try:
                async with self.client.get(base + '/health') as response:
                    if response.status == 200:
                        return
            except OSError:
                pass
            await asyncio.sleep(.05)
        raise AssertionError('startup timeout')

    async def start(self):
        self.client = ClientSession(connector=TCPConnector(limit=0), timeout=ClientTimeout(total=45))
        self.speech = Speech(self.client, (MONO/self.args.env).resolve(), self.out) if self.args.live else None
        echo_base = f'http://127.0.0.1:{port()}'
        process = await self.launch('echo', str(MONO/'vogon-rust/target/debug/vogon-echo-turn-service'),
                                    env=dict(os.environ, ECHO_BIND=echo_base.removeprefix('http://'), ECHO_AUTH_TOKEN='echo-fixture'))
        await self.ready(echo_base, process)
        self.echo = ObservedEcho(self.client, echo_base + '/turn/v1')
        self.proxy = Proxy(self.client)
        app = web.Application()
        app.router.add_post('/turn/v1/{op}', self.echo.handle)
        app.router.add_get('/{kind:stt|tts}', self.proxy.handle)
        self.runner, proxy_port = await serve(app)
        self.local = f'http://127.0.0.1:{proxy_port}'
        gateway_port = port()
        self.base = f'http://127.0.0.1:{gateway_port}'
        fixture = {'env_path': str((MONO/self.args.env).resolve()), 'bind': f'127.0.0.1:{gateway_port}',
                   'stt': self.local.replace('http:', 'ws:')+'/stt', 'tts': self.local.replace('http:', 'ws:')+'/tts',
                   'phones': [{'tenant':'live', 'call_id':f'phone-{n}-{i}', 'session_token':f'token-{n}-{i}', 'mode':'twilio', 'provider_call_id':f'CA-phone-{n}-{i}', 'app_base_url':self.local+'/turn/v1', 'app_auth_token':'echo-fixture'} for n in self.args.levels for i in range(n)] if self.args.phone else [], 'max_active': max(self.args.levels), 'max_handshakes': max(self.args.levels),
                   'settings': {'front_model_enabled': False, 'cerebras_api_key': '', 'open_router_api_key': '',
                                'ambience_enabled': False, 'recording_enabled': False,
                                'deepgram_stt_model': 'flux-general-en', 'deepgram_tts_model': 'aura-2-thalia-en',
                                'deepgram_eager_eot_threshold': .3, 'deepgram_eot_threshold': 1.,
                                'deepgram_eot_timeout_ms': 1800, 'deepgram_eot_hold_ms': 0,
                                'deepgram_phone_eager_eot_threshold': .3, 'deepgram_phone_eot_threshold': 1., 'deepgram_phone_eot_timeout_ms':1800,
                                'turn_service_timeout_sec': 15.}}
        path = self.out/'fixture.json'
        path.write_text(json.dumps(fixture, indent=2))
        env = dict(os.environ, PYTHONPATH=str(MONO/'data-plane/.venv/lib/python3.10/site-packages'),
                   OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1', GATEWAY_SESSION_STORE='memory', GATEWAY_TENANT_PROFILES='')
        process = await self.launch('gateway', str(MONO/'vogon-rust/target/debug/service-live-host'), '--live', str(path), env=env)
        await self.ready(self.base, process)
        files = [*Path(MONO/'vogon-rust/crates').rglob('*.rs'), *Path(MONO/'vogon-rust/examples/echo-turn-service').rglob('*.rs'), Path(__file__), Path(__file__).with_name('infra.py'),
                 MONO/'vogon-rust/target/debug/service-live-host', MONO/'vogon-rust/target/debug/vogon-echo-turn-service']
        (self.out/'source-hashes.json').write_text(json.dumps({str(p.relative_to(MONO)): hashlib.sha256(p.read_bytes()).hexdigest() for p in files}, indent=2))

    async def batch(self, count):
        """Synchronize callers, verify two turns each, and record cleanup even on failure."""
        name = f'{"phone" if self.args.phone else "voice" if self.args.live else "text"}-{count}'
        self.proxy.case = name
        callers = [Caller(self, f'{name}-{i}', text=not self.args.live, phone=self.args.phone) for i in range(count)]
        row = {'concurrent_calls': count, 'case': name, 'errors': [], 'turns': [], 'build': 'debug'}
        self.results.append(row)
        began = time.monotonic()
        texts = {c.cid: [f'Please check reference number {i+1}.', f'Thank you for checking reference number {i+1}.'] for i,c in enumerate(callers)}
        if self.args.live:
            gate = asyncio.Semaphore(4)
            async def generate(text):
                async with gate:
                    await self.speech.generate(text)
            try:
                await asyncio.gather(*(generate(t) for ts in texts.values() for t in ts))
            except Exception as error:
                row.update(passed=False, phase='input_generation')
                row['errors'].append(f'{type(error).__name__}: {error}')
                self.save()
                print(json.dumps(row), flush=True)
                return False
        began = time.monotonic()
        starts = []
        async def open_call(c):
            started = time.monotonic()
            await c.open()
            starts.append((time.monotonic()-started)*1000)
        try:
            opened = await asyncio.wait_for(asyncio.gather(*(open_call(c) for c in callers), return_exceptions=True), 45)
            errors = [f'{c.cid}: {type(r).__name__}: {r}' for c,r in zip(callers,opened) if isinstance(r,BaseException)]
            if errors:
                raise AssertionError('; '.join(errors))
            if self.args.live:
                await until(lambda: sum(r.get('case') == name and 'connection_query' in r for r in self.proxy.rows) >= 2*count, 25, 'all remote speech connections ready')
            row['all_ready_ms'] = (time.monotonic()-began)*1000
            row['simultaneously_ready'] = len(callers)
            row['session_ready_ms'] = percentiles(starts)
            # All sessions are ready before synchronized turns begin; input pumping and
            # output reading remain independent throughout each live conversation.
            for turn in range(2):
                async def exchange(c):
                    text = texts[c.cid][turn]
                    audio_offset = len(c.audio['phone']) if self.args.phone else 0
                    chunk_offset = len(c.chunks['phone']) if self.args.phone else 0
                    sent = time.monotonic()
                    if self.args.live:
                        await c.speak(text)
                    else:
                        await c.ws.send_json({'type': 'client.message', 'payload': {'text': text}})
                    speech_end = time.monotonic()
                    commit = await c.reply(turn+1, timeout=45)
                    assert wer(text,commit['text']) <= .25, f'input mismatch: {commit["text"]}'
                    assert normalized(text)[-1] == normalized(commit['text'])[-1], 'input reference number changed'
                    assert commit['answer'] == 'You said: ' + commit['text'], 'wrong echo response'
                    assert not c.errors, str(c.errors)
                    chunks = c.chunks['phone'][chunk_offset:] if self.args.phone else c.chunks.get(commit['audio_id'], [])
                    first = chunks[0][0] if chunks else next((f['_at'] for f in c.frames if f.get('type')=='server.message' and f.get('payload',{}).get('audio_id')==commit['audio_id']), None)
                    assert first is not None, 'no output'
                    return {'cid': c.cid, 'turn': turn+1, 'expected': text, 'transcript': commit['text'],
                            'answer': commit['answer'], 'audio_id': commit['audio_id'], 'audio_start': audio_offset, 'audio_end': len(c.audio['phone']) if self.args.phone else 0,
                            'speech_end_to_first_output_ms': (first-speech_end)*1000,
                            'round_trip_ms': (time.monotonic()-sent)*1000}
                results = await asyncio.wait_for(asyncio.gather(*(exchange(c) for c in callers),return_exceptions=True), 70)
                for c, result in zip(callers, results):
                    if isinstance(result, BaseException):
                        row['errors'].append(f'{c.cid}: {type(result).__name__}: {result}')
                    else:
                        row['turns'].append(result)
                if row['errors']:
                    break
            for c in callers:
                assert len(self.echo.calls[c.cid]['commits']) == 2, f'{c.cid}: missing/duplicate turn'
                terminal = [f for f in c.frames if (f.get('event') == 'mark' and f.get('mark',{}).get('name','').startswith('tts_end_'))] if self.args.phone else [f for f in c.frames if f.get('type') == ('server.tts_stream_end' if self.args.live else 'server.message')]
                assert len(terminal) == 2, f'{c.cid}: duplicate/missing delivery'
            if self.args.live:
                gate = asyncio.Semaphore(4)
                async def verify(turn):
                    c = next(c for c in callers if c.cid == turn['cid'])
                    async with gate:
                        audio = bytes(c.audio['phone'][turn['audio_start']:turn['audio_end']]) if self.args.phone else bytes(c.audio[turn['audio_id']])
                        recognized = await self.speech.transcribe(audio, 8000 if self.args.phone else 24000)
                    score = wer(turn['answer'], recognized)
                    turn.update(output_transcript=recognized, output_wer=score)
                    # Keep the full WER. Exact caller-content recognition is a separate
                    # check: Nova sometimes renders the fixed 'You said' prefix as 'USAD'.
                    expected_tokens, heard_tokens = normalized(turn['transcript']), normalized(recognized)
                    turn['caller_content_exact'] = bool(expected_tokens) and heard_tokens[-len(expected_tokens):] == expected_tokens
                    turn['strict_output_passed'] = score <= .25
                    assert heard_tokens and heard_tokens[-1] == normalized(turn['expected'])[-1], f'{c.cid}: output reference number changed'
                    assert score <= .25, f'{c.cid}: output WER {score}'
                checks = await asyncio.gather(*(verify(t) for t in row['turns']),return_exceptions=True)
                row['errors'] += [str(e) for e in checks if isinstance(e,BaseException)]
        except Exception as e:
            row['errors'].append(f'{type(e).__name__}: {e}')
        finally:
            # Close even partially opened callers; Caller.close owns pumps/readers.
            async def close_call(c):
                if not hasattr(c, 'ws'):
                    return
                try:
                    await asyncio.wait_for(c.close(), 12)
                except Exception as e:
                    row['errors'].append(f'cleanup {c.cid}: {type(e).__name__}')
                    c.running = False
                    for task in c.tasks:
                        task.cancel()
                    await asyncio.gather(*c.tasks, return_exceptions=True)
                    await c.ws.close()
            await asyncio.gather(*(close_call(c) for c in callers))
            ids = {c.cid for c in callers}
            ended = sum(self.echo.calls.get(cid,{}).get('ended',False) for cid in ids)
            row['ended_calls'] = ended
            if ended != count:
                row['errors'].append(f'EndCall count {ended}/{count}')
            requests = [r for r in self.echo.rows if r['body']['call_id'] in ids]
            row['turn_http_ms'] = percentiles([(r['end']-r['at'])*1000 for r in requests if 'end' in r])
            row['turn_tcp_connections'] = len({r['connection'] for r in requests})
            row['first_output_ms'] = percentiles([t['speech_end_to_first_output_ms'] for t in row['turns']])
            opens = [r for r in self.proxy.rows if r.get('case') == name and 'connection_query' in r]
            row['provider_connections'] = dict(Counter(r['kind'] for r in opens))
            if self.args.live and row['provider_connections'] != {'stt':count, 'tts':count}:
                row['errors'].append('provider reconnect or missing connection; inspect trace')
            if self.args.live:
                try:
                    await until(lambda: not any(s['active'] and s['case'] == name for s in self.proxy.sockets), 5, 'provider socket cleanup')
                except AssertionError as error:
                    row['errors'].append(str(error))
                if not self.args.phone:
                    integrity = []
                    for c in callers:
                        turns = [t for t in row['turns'] if t['cid'] == c.cid]
                        if not turns:
                            continue
                        socket = next((r['socket'] for r in self.proxy.rows if r.get('case') == name and r.get('kind') == 'tts' and r.get('value',{}).get('text') == turns[0]['answer']), None)
                        provider = next((s for s in self.proxy.sockets if s['id'] == socket), {})
                        delivered = hashlib.sha256(b''.join(bytes(c.audio[t['audio_id']]) for t in turns)).hexdigest()
                        exact = delivered == provider.get('audio_sha256')
                        integrity.append({'cid': c.cid, 'exact_provider_audio': exact, 'provider_sha256': provider.get('audio_sha256'), 'delivered_sha256': delivered})
                        if not exact:
                            row['errors'].append(f'{c.cid}: provider/delivered audio digest mismatch')
                    row['audio_integrity'] = integrity
                row['strict_output_failures'] = sum(not t.get('strict_output_passed',True) for t in row['turns'])
                row['media_frame_gap_ms'] = percentiles([(b['at']-a['at'])*1000 for c in callers for a,b in zip(c.sent,c.sent[1:])])
            row['seconds'] = time.monotonic()-began
            row['passed'] = not row['errors']
            self.save()
            print(json.dumps({k:v for k,v in row.items() if k!='turns'}),flush=True)
        return row['passed']

    def save(self):
        """Persist redacted evidence after each batch, including failed batches."""
        report = {'scope': 'Production Rust gateway + actual Rust echo example; local observing proxy; debug build; no GPT or PSTN',
                  'live_speech': self.args.live, 'phone': self.args.phone, 'results': self.results,
                  'highest_passing_concurrency': max((r['concurrent_calls'] for r in self.results if r.get('passed')),default=0)}
        (self.out/'report.json').write_text(json.dumps(report,indent=2))
        (self.out/'turn-api.json').write_text(json.dumps(self.echo.rows,indent=2))
        (self.out/'provider-events.json').write_text(json.dumps(self.proxy.rows,indent=2))

    async def close(self):
        """Stop only this harness's subprocesses and release all proxy resources."""
        for name, process in reversed(self.processes):
            if process.returncode is None:
                process.send_signal(signal.SIGINT)
                try:
                    await asyncio.wait_for(process.wait(),40)
                except asyncio.TimeoutError:
                    process.kill()
                    await process.wait()
        if hasattr(self,'runner'):
            await self.runner.cleanup()
        if hasattr(self,'client'):
            await self.client.close()
        for log in self.logs:
            log.close()
        (self.out/'process-exits.json').write_text(json.dumps({name:p.returncode for name,p in self.processes},indent=2))


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--live',action='store_true')
    parser.add_argument('--phone',action='store_true')
    parser.add_argument('--levels',default='1,4,8,16,32')
    parser.add_argument('--env',default='data-plane/.env')
    parser.add_argument('--out',required=True)
    args = parser.parse_args()
    if args.phone and not args.live:
        parser.error('--phone requires --live')
    args.levels = [int(n) for n in args.levels.split(',')]
    if not args.levels or any(n<1 or n>1024 for n in args.levels):
        parser.error('levels must be between 1 and 1024')
    suite = Suite(args)
    try:
        await suite.start()
        for count in args.levels:
            if not await suite.batch(count):
                break
    finally:
        await suite.close()

if __name__ == '__main__':
    asyncio.run(main())
