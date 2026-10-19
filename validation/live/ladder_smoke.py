"""Opt-in real Python ladder Turn service + Rust gateway + live STT/TTS smoke.

Reuse the existing promise script verbatim. Only the telephone endpoint is
simulated; HTTP and speech proxies observe real responses without changing them.
All outputs are synthetic demo data in an ignored, isolated results directory.
"""
import argparse
import ast
import asyncio
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import signal
import shutil
import sys
import sysconfig
import time

from aiohttp import ClientSession, ClientTimeout, web
from infra import Caller, MONO, PAIR, Proxy, Speech, port, serve, until, wav, wer

SOURCE = MONO / 'data-plane/tests/gateway_e2e/ladders_duplex_call.py'


def existing_script():
    """Read only the literal GATE and promise sequence; never execute the old host."""
    tree = ast.parse(SOURCE.read_text())
    values = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign) or not isinstance(node.targets[0], ast.Name):
            continue
        name = node.targets[0].id
        if name == 'GATE':
            values['GATE'] = ast.literal_eval(node.value)
        if name == 'SCRIPTS':
            for key, value in zip(node.value.keys, node.value.values):
                if ast.literal_eval(key) == 'promise':
                    assert isinstance(value, ast.BinOp) and isinstance(value.op, ast.Add)
                    return values[value.left.id] + ast.literal_eval(value.right)
    raise ValueError('Existing promise script not found')


def speculation_evidence(out):
    """Read committed EOT records; do not infer speculative support from event delivery."""
    committed = {}
    for path in sorted((Path(out) / 'checkpoints').glob('*.json')):
        state = json.loads(path.read_text())
        for row in state.get('log_tail', []):
            if row.get('event_type') == 'end_of_turn':
                detail = row.get('detail') or {}
                key = (row.get('turn'), detail.get('final_text'))
                committed[key] = {'turn': key[0], 'text': key[1],
                                  'reused_speculative': detail.get('reused_speculative', False),
                                  'fallback_reason': detail.get('fallback_reason')}
    rows = sorted(committed.values(), key=lambda r: r['turn'])
    return {'committed_turns': rows, 'reused_count': sum(r['reused_speculative'] for r in rows),
            'scope': 'Committed checkpoint ledger evidence for this source version; not just eager event counts.'}


class TurnProxy:
    """Record real HTTP exchanges; keep only enough call state for the shared caller."""
    def __init__(self, client, upstream):
        self.client, self.upstream = client, upstream
        self.rows, self.calls = [], {}

    async def handle(self, request):
        body = await request.json()
        op, cid = request.match_info['op'], body['call_id']
        row = {'op': op, 'body': body, 'at': time.monotonic()}
        self.rows.append(row)
        async with self.client.post(
            self.upstream + '/turn/v1/' + op, json=body,
            headers={'Authorization': request.headers.get('Authorization', '')},
        ) as response:
            raw = await response.read()
            row.update(status=response.status, response=json.loads(raw),
                       responded_at=time.monotonic())
            if response.status == 200:
                state = self.calls.setdefault(cid, {'ended': 0})
                if op == 'end_call':
                    state['ended'] += 1
            return web.Response(body=raw, status=response.status, content_type='application/json')


class Smoke:
    """Own both service processes, network observers and one simulated phone caller."""
    def __init__(self, args):
        self.out = Path(args.out).resolve()
        self.out.mkdir(parents=True, exist_ok=False)
        (self.out / 'checkpoints').mkdir()
        self.env_path = Path(args.env).resolve()
        self.controlled_eot = args.controlled_eot
        self.cid = 'ladder-' + str(int(time.time()))
        self.callers, self.processes, self.logs = [], [], []
        self.segments, self.turns = [], []
        self.result = {'passed': False, 'script': 'promise', 'source': str(SOURCE.relative_to(MONO)),
                       'call_id': self.cid, 'gpt_live': False, 'real_telephone_call': False,
                       'controlled_eot': self.controlled_eot,
                       'python_ladder_speculation': None,
                       'speculation_note': 'Determined from committed EOT evidence after the call.'}
        self.client = self.runner = None

    async def launch(self, command, env, name, cwd):
        log = (self.out / (name + '.log')).open('w')
        self.logs.append(log)
        process = await asyncio.create_subprocess_exec(
            *command, env=env, cwd=cwd, stdout=log, stderr=log
        )
        self.processes.append((name, process))
        return process

    async def healthy(self, url, process, headers=None):
        for _ in range(600):
            assert process.returncode is None, 'service exited during startup; inspect local log'
            try:
                async with self.client.get(url, headers=headers) as response:
                    if response.status == 200:
                        return await response.json()
            except (OSError, asyncio.TimeoutError):
                pass
            await asyncio.sleep(.1)
        raise AssertionError('service startup timeout')

    async def start(self):
        self.client = ClientSession(timeout=ClientTimeout(total=90))
        self.speech = Speech(self.client, self.env_path, self.out)
        self.script = existing_script()
        for text in self.script:
            await self.speech.generate(text)
        turn_port, gateway_port, redis_port = port(), port(), port()
        redis_binary = (os.environ.get('REDIS_SERVER_BIN') or shutil.which('redis-server')
                        or str(MONO / 'tools/redis-stable/src/redis-server'))
        assert Path(redis_binary).is_file(), 'redis-server is required for an isolated application session store'
        redis_process = await self.launch(
            [redis_binary, '--bind', '127.0.0.1', '--port', str(redis_port),
             '--save', '', '--appendonly', 'no', '--dir', str(self.out)],
            dict(os.environ), 'redis', self.out)
        import redis.asyncio
        redis_probe = redis.asyncio.from_url(f'redis://127.0.0.1:{redis_port}')
        try:
            for attempt in range(100):
                assert redis_process.returncode is None, 'isolated Redis exited'
                try:
                    if await redis_probe.ping():
                        break
                except redis.exceptions.ConnectionError:
                    if attempt == 99:
                        raise
                    await asyncio.sleep(.05)
        finally:
            await redis_probe.aclose()
        self.turn_base = f'http://127.0.0.1:{turn_port}'
        self.base = f'http://127.0.0.1:{gateway_port}'
        self.echo = TurnProxy(self.client, self.turn_base)
        self.proxy = Proxy(self.client)
        self.proxy.case = 'python-ladder-promise'
        app = web.Application()
        app.router.add_post('/turn/v1/{op}', self.echo.handle)
        app.router.add_get('/{kind:stt|tts}', self.proxy.handle)
        self.runner, local_port = await serve(app)
        self.local = f'http://127.0.0.1:{local_port}'
        token = self.cid + '-demo-config'
        fixture = {'env_path': str(self.env_path), 'out': str(self.out),
                   'call_id': self.cid, 'session_token': token, 'turn_port': turn_port}
        turn_fixture = self.out / 'python-fixture.json'
        turn_fixture.write_text(json.dumps(fixture, indent=2))
        storage = self.out / 'python-storage'
        storage.mkdir()
        env = dict(os.environ, REDIS_ENABLED='true', REDIS_CLUSTER_MODE='false',
                   REDIS_URL=f'redis://127.0.0.1:{redis_port}', S3_ENABLED='false',
                   DATABASE_URL='sqlite:///' + str(self.out / 'python.sqlite'),
                   STORAGE_BASE_PATH=str(storage), TRANSCRIPTS_PATH=str(storage / 'transcripts'),
                   RECORDINGS_PATH=str(storage / 'recordings'), CALL_LOGS_PATH=str(storage / 'call_logs'),
                   TURN_SERVICE_API_KEY='ladder-smoke-pair', CONFIG_CACHE_TTL_SEC='600',
                   AMBIENCE_ENABLED='false', FRONT_MODEL_ENABLED='false',
                   PYTHONDONTWRITEBYTECODE='1', OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1')
        turn = await self.launch(
            [sys.executable, str(Path(__file__).with_name('python_turn_host.py')),
             '--fixture', str(turn_fixture)], env, 'python-turn', MONO / 'data-plane')
        self.result['python_startup'] = await self.healthy(self.turn_base + '/health', turn)
        fixture = {'env_path': str(self.env_path), 'bind': f'127.0.0.1:{gateway_port}',
                   'stt': self.local.replace('http:', 'ws:') + '/stt',
                   'tts': self.local.replace('http:', 'ws:') + '/tts',
                   'settings': {'front_model_enabled': False, 'ambience_enabled': False,
                                'deepgram_stt_model': 'flux-general-en',
                                'turn_service_timeout_sec': 60,
                                'recordings_path': str(self.out / 'rust-recordings')},
                   'phones': [{'tenant': 'live', 'call_id': self.cid,
                               'session_token': 'token-' + self.cid, 'mode': 'twilio',
                               'provider_call_id': 'CA-' + self.cid,
                               'app_base_url': self.local + '/turn/v1',
                               'app_auth_token': 'ladder-smoke-pair',
                               'app_payload': {'owner_id': 'e2e-ladders',
                                               'workflow_id': 'collections_ladders',
                                               'session_token': token},
                               'route': 'collections_ladders'}]}
        rust_fixture = self.out / 'rust-fixture.json'
        if self.controlled_eot:
            fixture['settings'].update(deepgram_phone_eager_eot_threshold=.3,
                                       deepgram_phone_eot_threshold=1.,
                                       deepgram_phone_eot_timeout_ms=3000,
                                       deepgram_eot_hold_ms=0)
        rust_fixture.write_text(json.dumps(fixture, indent=2))
        env.update(PYTHONPATH=sysconfig.get_path('purelib'), GATEWAY_SESSION_STORE='memory',
                   GATEWAY_TENANT_PROFILES='', AWS_EC2_METADATA_DISABLED='true')
        gateway = await self.launch(
            [str(MONO / 'vogon-rust/target/debug/service-live-host'), '--live', str(rust_fixture)],
            env, 'rust-gateway', MONO)
        self.result['rust_startup'] = await self.healthy(self.base + '/ready?mode=voice', gateway)
        sources = [SOURCE, Path(__file__), Path(__file__).with_name('python_turn_host.py'),
                   MONO / 'vogon-rust/target/debug/service-live-host',
                   MONO / 'vogon-rust/Cargo.lock',
                   MONO / 'data-plane/app/workflows/definitions/collections_ladders.json']
        sources += list((MONO / 'data-plane/app/turns').glob('*.py'))
        (self.out / 'source-hashes.json').write_text(json.dumps({
            str(p.relative_to(MONO)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sources
        }, indent=2))

    async def heard(self, caller, row, offset, label):
        """Require every returned speech command's terminal mark and actual playout."""
        commands = row['response'].get('speak', [])
        assert commands, f'no speech in {label}'
        for command in commands:
            def complete():
                truncated = any(e.get('kind') == 'delivery_outcome' and
                                e.get('audio_id') == command['audio_id'] and
                                e.get('status') == 'truncated'
                                for r in self.echo.rows for e in r['body'].get('events', []))
                assert not truncated, f'{label} speech was truncated before completion'
                return any(f.get('event') == 'mark' and
                           f.get('mark', {}).get('name') == 'tts_end_' + command['audio_id']
                           for f in caller.frames)
            await until(complete, 65, f'{label} terminal mark')
        await asyncio.sleep(max(0, caller.playout_until - time.monotonic()) + .8)
        data = bytes(caller.audio['phone'][offset:])
        assert data, f'{label} has no returned audio'
        expected = ' '.join(c['text'] for c in commands)
        path = self.out / (label + '.wav')
        wav(path, data, 8000)
        self.segments.append({'label': label, 'expected': expected, 'bytes': len(data),
                              'pcm': data, 'audio_ids': [c['audio_id'] for c in commands]})
        print('AGENT', label, expected, flush=True)
        return expected

    async def conversation(self):
        caller = await Caller(self, self.cid, phone=True).open()
        self.caller = caller
        opening = await until(lambda: next((r for r in self.echo.rows if
                              r['op'] == 'start_call' and r.get('status') == 200), None),
                              60, 'Python StartCall')
        await self.heard(caller, opening, 0, 'opening')
        for index, text in enumerate(self.script, 1):
            before, offset = len(self.echo.rows), len(caller.audio['phone'])
            print('CALLER', index, text, flush=True)
            speech_end = await caller.speak(text)
            row = await until(lambda: next((r for r in self.echo.rows[before:] if
                              r.get('status') == 200 and r['op'] == 'process_events' and
                              r.get('response', {}).get('speak') and
                              not any(e.get('kind') == 'utterance_eager'
                                      for e in r['body'].get('events', []))), None),
                              80, f'Python reply {index}')
            answer = await self.heard(caller, row, offset, f'turn-{index}')
            received = [t for t, _ in caller.chunks['phone'] if t >= speech_end]
            self.turns.append({'index': index, 'caller': text, 'agent': answer,
                               'speech_end': speech_end,
                               'first_audio_ms': round((received[0] - speech_end) * 1000)
                               if received else None,
                               'trigger': row['body'].get('events', [])[-1]})
            self.save()
        await until(lambda: self.echo.calls[self.cid]['ended'] == 1, 60, 'natural EndCall')
        await until(lambda: caller.ws.closed, 15, 'closed caller socket')
        async with self.client.get(self.base + '/health') as response:
            health = await response.json()
        assert health['active_sockets'] == 0, 'Rust socket owner leaked'
        async with self.client.get(self.turn_base + '/health') as response:
            health = await response.json()
        assert health['live_calls'] == 0, 'Python call registry leaked'
        assert not caller.errors, caller.errors
        assert all(r.get('status') == 200 for r in self.echo.rows), 'Turn HTTP error'
        end = [r for r in self.echo.rows if r['op'] == 'end_call']
        assert len(end) == 1, 'duplicate EndCall'
        self.result['end_reason'] = end[0]['body']['reason']
        self.result['conversation_passed'] = True
        self.result['wire_events'] = dict(Counter(e['kind'] for r in self.echo.rows
                                                for e in r['body'].get('events', [])))
        for segment in self.segments:
            actual = await self.speech.transcribe(segment['pcm'], 8000)
            segment['recognized'], segment['wer'] = actual, wer(segment['expected'], actual)
            assert segment['wer'] <= .25, f"returned audio mismatch {segment['label']}: {actual}"
        rows = [json.loads(p.read_text()) for p in sorted((self.out / 'checkpoints').glob('*.json'))]
        states = [(r.get('handler_state') or {}).get('engine_state') or {} for r in rows]
        edges = [e for state in states for e in (state.get('runtime') or {}).get('edge_rows', [])]
        destinations = {e.get('to_ladder') for e in edges}
        self.result['ladder_destinations'] = sorted(x for x in destinations if x)
        assert 'promise_counter' in destinations, 'promise ladder was not reached'
        assert 'call_closure' in destinations, 'closure ladder was not reached'
        events = [e for r in self.echo.rows for e in r['body'].get('events', [])]
        finals = [e for e in events if e['kind'] == 'utterance_final']
        self.result['recognized_caller'] = [e['text'] for e in finals]
        assert len(finals) == len(self.script), 'missing or duplicate final caller turns'
        assert any('friday' in e['text'].lower() for e in finals), 'promise date lost in STT'
        speculation = speculation_evidence(self.out)
        assert len(speculation['committed_turns']) == len(self.script), 'unexpected committed EOT count'
        self.result['speculation'] = speculation
        self.result['python_ladder_speculation'] = speculation['reused_count'] > 0
        eager_rows = [r for r in self.echo.rows if any(e.get('kind') == 'utterance_eager'
                      for e in r['body'].get('events', []))]
        assert all(not r.get('response', {}).get('speak') for r in eager_rows), \
            'eager acknowledgement incorrectly emitted speech'
        self.latency_breakdown()
        self.result['passed'] = True

    def latency_breakdown(self):
        """Measure observed boundaries on the harness clock, excluding remote waits from forwarding."""
        rows = []
        for turn in self.turns:
            final = turn['trigger']['text']
            request = next(r for r in self.echo.rows if r['op'] == 'process_events' and any(
                e.get('kind') == 'utterance_final' and e.get('text') == final
                for e in r['body'].get('events', [])))
            stt = next(r['at'] for r in self.proxy.rows if
                       r.get('value', {}).get('event') == 'EndOfTurn' and
                       r['value'].get('transcript') == final)
            sent = next(r['at'] for r in self.proxy.rows if r['at'] >= request['responded_at'] and
                        r['kind'] == 'tts' and r.get('direction') == 'send' and
                        r.get('value', {}).get('type') == 'Speak')
            received = next(r['at'] for r in self.proxy.rows if r['at'] >= sent and
                            r['kind'] == 'tts' and r.get('direction') == 'receive' and 'bytes' in r)
            caller = next(t for t, _ in self.caller.chunks['phone'] if t >= turn['speech_end'])
            row = {'turn': turn['index'], 'stt_endpointing_ms': (stt - turn['speech_end']) * 1000,
                   'stt_final_to_turn_request_ms': (request['at'] - stt) * 1000,
                   'python_turn_ms': (request['responded_at'] - request['at']) * 1000,
                   'turn_reply_to_tts_speak_ms': (sent - request['responded_at']) * 1000,
                   'tts_first_audio_ms': (received - sent) * 1000,
                   'tts_audio_to_caller_ms': (caller - received) * 1000,
                   'end_to_end_ms': (caller - turn['speech_end']) * 1000}
            rows.append({k: round(v, 3) if isinstance(v, float) else v for k, v in row.items()})
        result = {'scope': 'Observed boundaries include local proxy/socket scheduling, not isolated Rust CPU time; five sequential turns, no load benchmark.',
                  'turns': rows}
        (self.out / 'latency-breakdown.json').write_text(json.dumps(result, indent=2))
        self.result['latency'] = result

    def save(self):
        self.result['turns'] = self.turns
        self.result['returned_audio'] = [{k: v for k, v in s.items() if k != 'pcm'} for s in self.segments]
        (self.out / 'summary.json').write_text(json.dumps(self.result, indent=2))
        if hasattr(self, 'echo'):
            (self.out / 'turn-api.json').write_text(json.dumps(self.echo.rows, indent=2))
            (self.out / 'provider-events.json').write_text(json.dumps(self.proxy.rows))

    async def stop(self):
        for caller in self.callers:
            await caller.close()
        for name, process in reversed(self.processes):
            if process.returncode is None:
                process.send_signal(signal.SIGTERM)
                try:
                    await asyncio.wait_for(process.wait(), 40)
                except asyncio.TimeoutError:
                    process.kill()
                    await process.wait()
            self.result[name + '_exit'] = process.returncode
            # Uvicorn completes lifespan shutdown then re-raises the original SIGTERM.
            # Accept that convention only with independently saved successful cleanup.
            allowed = (0, -signal.SIGTERM) if name == 'python-turn' else (0,)
            if process.returncode not in allowed:
                self.result['passed'] = False
        if self.runner:
            await self.runner.cleanup()
        if self.client:
            await self.client.close()
        for log in self.logs:
            log.close()
        cleanup = self.out / 'python-cleanup.json'
        if cleanup.exists():
            self.result['python_cleanup'] = json.loads(cleanup.read_text())
            if self.result['python_cleanup']['checkpoint_remaining'] or self.result['python_cleanup']['registry_remaining']:
                self.result['passed'] = False
            if self.result['python_cleanup']['post_call_jobs'] != 1:
                self.result['passed'] = False
            if self.result['python_cleanup']['record_state'] != 'done':
                self.result['passed'] = False
        else:
            self.result['passed'] = False
        self.save()


async def run(args):
    smoke = Smoke(args)
    try:
        await smoke.start()
        await asyncio.wait_for(smoke.conversation(), 420)
    except Exception as error:
        smoke.result['error'] = str(error) if isinstance(error, AssertionError) else type(error).__name__
        print('FAILED', smoke.result['error'], flush=True)
    finally:
        await smoke.stop()
    print(json.dumps({k: v for k, v in smoke.result.items() if k not in ['turns', 'returned_audio']}, indent=2))
    return smoke.result['passed']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--live', action='store_true', required=True)
    parser.add_argument('--env', default=str(MONO / 'data-plane/.env'))
    parser.add_argument('--controlled-eot', action='store_true',
                        help='Use the existing live-suite .3 eager / 1.0 final / 3000ms settings')
    parser.add_argument('--out', required=True)
    args = parser.parse_args()
    sys.exit(0 if asyncio.run(run(args)) else 1)


if __name__ == '__main__':
    main()
