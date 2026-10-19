"""Direct comparisons against Python records, the installed Twilio SDK and real TwiML route."""
import asyncio
from dataclasses import asdict
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.datastructures import FormData
from twilio.request_validator import RequestValidator
from vogon.gateway_service import routes
from vogon.gateway_service.session_store import CallSessionRecord, MemorySessionStore

ROOT = Path(__file__).resolve().parent
WORKSPACE = ROOT.parents[1]


def main():
    cases = []
    counts = {}

    def add(group, payload, expected):
        counts[group] = counts.get(group, 0) + 1
        cases.append((payload, expected))

    for n in range(40):
        raw = dict(tenant='acme', call_id=f'call-{n}', session_token=f'token-{n}', mode=['text', 'widget', 'twilio'][n % 3])
        if n % 2:
            raw.update(schema_version=99, unknown_future_field={'x': []}, app_payload={'items': [], 'unicode': 'नमस्ते'}, created_at_utc=12.5)
        if n % 3:
            raw.update(call_config={'empty_list': [], 'empty_object': {}, 'flag': False}, route='opaque')
        record = CallSessionRecord.from_json(json.dumps(raw))
        add('record_decode', {'op': 'record', 'record': raw}, asdict(record))
        for terminal in ('ended', 'failed', 'cancelled'):
            steps = [{'state': state, 'now': float(20 + i)} for i, state in enumerate(('dialing', 'ringing', 'connected', 'connected', terminal))]
            record = CallSessionRecord.from_json(json.dumps(raw))
            store = MemorySessionStore()
            async def transitions():
                await store.create(record)
                for step in steps:
                    with patch('vogon.gateway_service.session_store.time.time', return_value=step['now']):
                        await store.transition(record.tenant, record.call_id, step['state'])
            # Avoid comparing clocks across processes; record migration itself retains zero.
            with patch('vogon.gateway_service.session_store.time.time', return_value=10.0):
                asyncio.run(transitions())
            created = dict(raw, created_at_utc=raw.get('created_at_utc') or 10.0)
            add('forward_transitions', {'op': 'record', 'record': created, 'steps': steps}, asdict(record))

    token = 'fixture-secret'
    validator = RequestValidator(token)
    for url in ('https://public.example/gw/v1/twilio/status', 'https://public.example:443/gw/v1/twilio/twiml?token=a%2Bb', 'http://127.0.0.1:8002/v1/twilio/status', 'https://public.example:8443/gw/v1/twilio/status'):
        for params in ([], [('CallSid', 'CA1'), ('CallStatus', 'completed')], [('Z', 'two'), ('A', 'नमस्ते'), ('Z', 'one'), ('Z', 'one')]):
            form = FormData(params)
            signature = validator.compute_signature(url, form)
            for header in (signature, 'bad', '', validator.compute_signature(url.replace(':443', '').replace(':8443', '').replace(':8002', ''), form)):
                add('twilio_sdk_signature', {'op': 'signature', 'token': token, 'url': url, 'params': params, 'header': header}, {'signature': signature, 'valid': validator.validate(url, form, header)})

    app = FastAPI()
    app.include_router(routes.router)
    for base in ('https://public.example/gw', 'http://localhost:8002', ''):
        store = MemorySessionStore()
        asyncio.run(store.create(CallSessionRecord('acme', 'phone', 'good', 'twilio')))
        with patch.object(routes, 'store', return_value=store), patch.object(routes, 'gateway_config', return_value=SimpleNamespace(public_base_url=base)), patch.object(routes, 'settings', SimpleNamespace(twilio_auth_token='')):
            with TestClient(app) as client:
                for valid in (True, False):
                    response = client.get('/v1/twilio/twiml', params={'token': 'good' if valid else 'unknown'})
                    assert response.status_code == 200
                    add('twiml_route', {'op': 'twiml', 'base': base, 'valid': valid}, response.text)

    process = subprocess.run([str(WORKSPACE/'target/debug/service-validation')], input=''.join(json.dumps(v, ensure_ascii=False)+'\n' for v, _ in cases), text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
    actual = [json.loads(line) for line in process.stdout.splitlines()]
    assert len(actual) == len(cases)
    for i, ((request, expected), received) in enumerate(zip(cases, actual)):
        assert received == expected, (i, request, expected, received)
    from host import check
    route_count = check(WORKSPACE)
    counts['native_process_route_parity'] = route_count
    report = {'passed': len(cases) + route_count, 'groups': counts, 'external_calls': False, 'deliberate_differences': ['terminal/regressive transitions', 'duplicate create and unique socket ownership', 'memory expiry', 'stricter identifier/URL validation', 'escaped XML attributes']}
    (ROOT/'results/comparison.json').write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps(report))


if __name__ == '__main__':
    main()
