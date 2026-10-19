"""Compare actual native HTTP process with Python routes, then exercise SIGTERM exit."""
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient
import httpx
from vogon.gateway_service import routes
from vogon.gateway_service.session_store import MemorySessionStore


def check(workspace: Path):
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        port = listener.getsockname()[1]
    env = dict(os.environ, GATEWAY_HOST='127.0.0.1', GATEWAY_PORT=str(port), GATEWAY_SESSION_STORE='memory', GATEWAY_SERVICE_KEY='local-pair', GATEWAY_PUBLIC_BASE_URL='https://public.example/gw', FRONT_MODEL_ENABLED='false', TWILIO_AUTH_TOKEN='')
    config = SimpleNamespace(service_key='local-pair', auth_enabled=True, public_base_url='https://public.example/gw')
    app = FastAPI()
    app.include_router(routes.router)
    cases = [
        ('POST', '/v1/tenants/acme/calls', {}, {}),
        ('POST', '/v1/tenants/acme/calls', {'mode':'bad', 'app':{'base_url':'http://app'}}, {'Authorization':'Bearer local-pair'}),
        ('POST', '/v1/tenants/acme/calls', {'mode':'twilio', 'app':{'base_url':'http://app'}}, {'Authorization':'Bearer local-pair'}),
        ('POST', '/v1/tenants/acme/calls', {'mode':'text', 'app':{'base_url':'http://app'}, 'call_id':'../bad'}, {'Authorization':'Bearer local-pair'}),
    ]
    # FastAPI validates malformed bodies before authorization; compare status only for
    # valid-body authentication cases and leave validation-body formatting out of parity.
    cases[0] = ('POST','/v1/tenants/acme/calls',{'mode':'text','app':{'base_url':'http://app'}},{})
    for mode in ('text', 'widget'):
        body = {'mode':mode, 'call_id':f'compare-{mode}', 'app':{'base_url':'http://app/turn/v1','auth_token':'private'}, 'app_payload':{'items':[]}, 'route':'opaque'}
        cases.extend([
            ('POST','/v1/tenants/acme/calls',body,{'Authorization':'Bearer local-pair'}),
            ('POST','/v1/tenants/acme/calls',body,{'Authorization':'Bearer local-pair'}),
            ('GET',f'/v1/tenants/acme/calls/compare-{mode}',None,{'Authorization':'Bearer local-pair'}),
            ('GET',f'/v1/tenants/other/calls/compare-{mode}',None,{'Authorization':'Bearer local-pair'}),
        ])
    cases.extend([
        ('POST','/v1/twilio/status',None,{}),
        ('GET','/v1/twilio/twiml?token=unknown',None,{}),
    ])
    def normalized(response):
        if 'json' not in response.headers.get('content-type',''):
            return response.text
        v = response.json()
        if 'session_token' in v:
            v['ws_url'] = v['ws_url'].replace(v['session_token'], '<token>')
            v['session_token'] = '<token>'
        for k in ('created_at_utc','connected_at_utc','ended_at_utc'):
            if k in v:
                v[k] = 1 if v[k] > 0 else 0
        return v
    with tempfile.NamedTemporaryFile(mode='w', suffix='.env') as file, tempfile.TemporaryFile(mode='w+') as logs:
        file.flush()
        command = [str(workspace/'target/debug/vogon-gateway'), '--env', file.name]
        subprocess.run([*command, '--check'],env=env,stdout=logs,stderr=logs,check=True,timeout=15)
        process = subprocess.Popen(command, env=env, stdout=logs, stderr=logs)
        try:
            with httpx.Client(base_url=f'http://127.0.0.1:{port}', timeout=5) as native:
                deadline = time.monotonic()+10
                while True:
                    assert process.poll() is None, 'native process exited before readiness'
                    try:
                        if native.get('/health').status_code == 200:
                            break
                    except httpx.ConnectError:
                        pass
                    assert time.monotonic()<deadline, 'native startup timeout'
                    time.sleep(.02)
                with patch.object(routes,'store',return_value=MemorySessionStore()), patch.object(routes,'gateway_config',return_value=config), patch.object(routes,'settings',SimpleNamespace(twilio_auth_token='')):
                    with TestClient(app) as reference:
                        for method,path,body,headers in cases:
                            kwargs = {'headers':headers}
                            if body is not None:
                                kwargs['json'] = body
                            actual = native.request(method,path,**kwargs)
                            expected = reference.request(method,path,**kwargs)
                            assert actual.status_code == expected.status_code, (path,actual.text,expected.text)
                            assert normalized(actual) == normalized(expected), (path,actual.text,expected.text)
            process.send_signal(signal.SIGTERM)
            assert process.wait(timeout=10) == 0, 'native process shutdown failed'
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
    return len(cases)
