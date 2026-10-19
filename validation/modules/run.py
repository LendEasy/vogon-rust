"""Native Rust module parity against the current Python implementation. No timing."""
import argparse
import dataclasses as dc
import hashlib
import json
import os
from pathlib import Path
import random
import shutil
import subprocess
import sys
import tempfile
import typing as t
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent
WORKSPACE = ROOT.parents[1]
MONO = WORKSPACE.parent
sys.path[:0] = [str(MONO / 'vogon-contract/src'), str(MONO / 'voice-gateway'), str(MONO / 'voice-gateway/tests')]
from vogon_contract import types, codec
from vogon.gateway.settings import GatewaySettings
from vogon.gateway_service import config


class Harness:
    def __init__(self, binary):
        self.child = subprocess.Popen([str(binary)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        self.rows = []
        self.differences = []
    def call(self, request):
        self.child.stdin.write(json.dumps(request, ensure_ascii=False, allow_nan=False) + '\n')
        self.child.stdin.flush()
        line = self.child.stdout.readline()
        assert line, f'Rust exited unexpectedly: {self.child.poll()}'
        return json.loads(line)
    def check(self, name, request, reference):
        try:
            expected = reference()
        except Exception as error:
            expected = None
            error_type = type(error).__name__
        else:
            error_type = None
        actual = self.call(request)
        if error_type:
            assert 'error' in actual, (name, error_type, actual)
            if request['op'] in ('normalize', 'typed'): assert actual['error'] == error_type, (name, error_type, actual)
        else:
            assert actual == {'ok':expected}, (name, request, expected, actual)
        self.rows.append({'case':name, 'passed':True, 'expected_error':error_type,
                          'result_sha256':hashlib.sha256(json.dumps(actual.get('ok'), sort_keys=True, ensure_ascii=False).encode()).hexdigest()})
    def intentional_difference(self, name, request, expected_rust, reference):
        """Verify an approved behavior change separately from Python parity."""
        python_result = reference()
        actual = self.call(request)
        assert actual == {'ok':expected_rust}, (name, expected_rust, actual)
        assert expected_rust != python_result, (name, 'expected a documented difference')
        self.differences.append({'case':name, 'passed':True,
            'reason':'Rust standard Unicode whitespace; U+001C–U+001F are ordinary characters',
            'request':request, 'rust':expected_rust, 'python':python_result})
    def close(self):
        self.child.stdin.close()
        assert self.child.wait(timeout=10) == 0


RNG = random.Random(20261018)
CLASSES = {name:cls for name,cls in vars(types).items() if isinstance(cls,type) and dc.is_dataclass(cls)}


def sample(h, full=False):
    origin, args = t.get_origin(h), t.get_args(h)
    if isinstance(h, (str, t.ForwardRef)): return sample(getattr(types, h if isinstance(h,str) else h.__forward_arg__), full)
    if args and origin is not t.Literal and type(None) in args:
        return sample(next(a for a in args if a is not type(None)), full) if full else None
    if origin is t.Union: return sample(RNG.choice(args), full)
    if origin is t.Literal: return RNG.choice(args)
    if origin is list: return [sample(args[0], full) for _ in range(2)]
    if dc.is_dataclass(h): return instance(h, full)
    if h is str: return '聲音-é sample . ! ?'
    if h is float: return RNG.choice([0.0, -2.5, 0.875, 12345.125])
    if h is int: return RNG.choice([0, 1, -7, 300, 2**53+1])
    if h is bool: return RNG.choice([True, False])
    if h is list: return [{'event_name':'test', 'metadata':{'unicode':'聲音'}}]
    return {'opaque':[1, None, False, {'nested':'unchanged'}]}


def instance(cls, full=False):
    hints = t.get_type_hints(cls, vars(types))
    return cls(**{f.name:sample(hints[f.name], full) for f in dc.fields(cls)
                  if full or (f.default is dc.MISSING and f.default_factory is dc.MISSING)})


def contracts(h):
    for name, cls in CLASSES.items():
        for full in (False, True):
            data = codec.to_wire(instance(cls, full))
            for op in ('normalize', 'typed'):
                h.check(f'{op}/{name}/{full}', {'op':op, 'type':name, 'data':data},
                        lambda:codec.to_wire(codec.from_wire(cls, data)))
            data['future_field'] = {'opaque':True}
            h.check(f'unknown/{name}/{full}', {'op':'normalize','type':name,'data':data},
                    lambda:codec.to_wire(codec.from_wire(cls,data)))
        minimal = {f.name:codec.to_wire(getattr(instance(cls), f.name)) for f in dc.fields(cls)
                   if f.default is dc.MISSING and f.default_factory is dc.MISSING}
        h.check(f'defaults/{name}', {'op':'normalize','type':name,'data':minimal}, lambda:codec.to_wire(codec.from_wire(cls,minimal)))
        for f in dc.fields(cls):
            data = codec.to_wire(instance(cls, True)); data.pop(f.name)
            h.check(f'omitted/{name}/{f.name}', {'op':'normalize','type':name,'data':data}, lambda:codec.to_wire(codec.from_wire(cls,data)))
            data = codec.to_wire(instance(cls, True)); data[f.name] = None
            h.check(f'explicit_null/{name}/{f.name}', {'op':'normalize','type':name,'data':data}, lambda:codec.to_wire(codec.from_wire(cls,data)))
    for group, entries, fn in [('TransportEvent',types.EVENT_TYPES,codec.event_from_wire),
                                ('PostCallFact',types.POST_CALL_FACT_TYPES,codec.post_call_fact_from_wire)]:
        for cls in entries:
            data = codec.to_wire(instance(cls, True))
            h.check(f'tag/{group}/{cls.kind}', {'op':'normalize','type':group,'data':data}, lambda:codec.to_wire(fn(data)))
        for data in ({}, {'kind':'future_kind'}, {'kind':None}):
            h.check(f'bad_tag/{group}/{data}', {'op':'normalize','type':group,'data':data}, lambda:codec.to_wire(fn(data)))
    for offset in (0.0, -12345.125, 0.125, 12345.125):
        events = [instance(cls, True) for cls in types.EVENT_TYPES]
        h.check(f'clock_rebase/{offset}', {'op':'shift','events':codec.to_wire(events),'offset':offset},
                lambda:codec.to_wire(codec.shift_event_times(events, offset)))
    # Exercise documented permissiveness, instead of assuming type annotations validate.
    for field, value in [('text',42), ('confidence','0.7'), ('low_conf_words','abc'), ('heard',None), ('playback_generation','legacy')]:
        data = codec.to_wire(instance(types.UtteranceFinal)); data[field] = value
        h.check(f'permissive/{field}', {'op':'normalize','type':'UtteranceFinal','data':data},
                lambda:codec.to_wire(codec.from_wire(types.UtteranceFinal,data)))


def configurations(h):
    classes = {'settings':GatewaySettings, 'gateway':config.GatewayConfig, 'tenant':config.TenantProfile}
    with patch.dict(os.environ, {}, clear=True):
        for model, cls in classes.items():
            def reference(initial, env=None, dotenv=None):
                if model == 'tenant': return cls(**initial).model_dump()
                with patch.dict(os.environ, env or {}, clear=True):
                    if dotenv:
                        with tempfile.NamedTemporaryFile(mode='w',suffix='.env') as stream:
                            stream.write('\n'.join(f'{k}={v}' for k,v in dotenv.items())); stream.flush()
                            return cls(_env_file=stream.name, **initial).model_dump()
                    return cls(_env_file=None, **initial).model_dump()
            h.check(f'configuration/{model}/defaults', {'op':'settings','model':model}, lambda:reference({}))
            h.check(f'configuration/{model}/typed_defaults', {'op':'settings_typed','model':model}, lambda:reference({}))
            for name, field in cls.model_fields.items():
                typ = field.annotation
                values = {str:['', 'changed', None, 42], bool:[True,False,1,0,'true','OFF','yes','n','bad',None],
                          int:[0,12,-3, '1000','1.0','1e3','1_000','1__0',2.5,None],
                          float:[0,12,-3.125, '1e3','1_000','bad',None]}.get(typ, [[], ['hello'], [12], None])
                for value in values:
                    initial = {name:value, 'future_setting':'ignored'}
                    h.check(f'configuration/{model}/{name}/{value!r}', {'op':'settings','model':model,'initial':initial}, lambda:reference(initial))
                if model != 'tenant':
                    prefix = 'GATEWAY_' if model == 'gateway' else ''
                    key = prefix + name.upper()
                    value = 'yes' if typ is bool else '123' if typ in (int,float) else 'env-value'
                    env = {key:value}; dotenv = {key:'0' if typ in (bool,int,float) else 'file-value'}
                    h.check(f'env/{model}/{name}', {'op':'settings','model':model,'env':env,'dotenv':dotenv}, lambda:reference({},env,dotenv))
                    initial = {name:field.default}
                    h.check(f'initial_precedence/{model}/{name}', {'op':'settings','model':model,'initial':initial,'env':env}, lambda:reference(initial,env))
        for i in range(100):
            profile = config.TenantProfile(language=RNG.choice(['en-US','fr','']), filler_enabled=RNG.choice([True,False]),
                recording_enabled=RNG.choice([True,False]), ambience_enabled=RNG.choice([True,False]),
                safety_phrases=['stop'], floor_request_phrases=['wait'])
            call = types.CallConfig(persona_voice='', language=RNG.choice(['en-US','fr','']),
                ambience=RNG.choice([None,{}, {'enabled':False}, {'enabled':True}]),
                recording=RNG.choice([None,types.RecordingPolicy(False),types.RecordingPolicy(True,'mono')]),
                filler=RNG.choice([None,types.FillerPolicy(100,200,2)]),
                barge_policy=RNG.choice([None,types.BargePolicy([],[],False),types.BargePolicy(['halt'],['listen'])]))
            h.check(f'snapshot/{i}', {'op':'snapshot','profile':profile.model_dump(),'call':codec.to_wire(call)},
                    lambda:config.resolve_call_snapshot(profile,call))
        shared = GatewaySettings(_env_file=None)
        for overrides in ({}, {'tenant':{}}, {'tenant':{'language':'fr','unknown':'ignored','recording_enabled':False}}):
            with tempfile.NamedTemporaryFile(mode='w',suffix='.json') as stream:
                json.dump(overrides,stream); stream.flush()
                cfg = config.GatewayConfig(_env_file=None,tenant_profiles=stream.name)
                with patch.object(config,'shared_settings',shared):
                    h.check(f'tenant/{overrides}', {'op':'tenant','tenant':'tenant','shared':shared.model_dump(),'overrides':overrides},
                            lambda:config.tenant_profile('tenant',cfg).model_dump())
        for key in ('',' ','\u2003',' secret '):
            h.check(f'auth/{key!r}', {'op':'auth','data':{'service_key':key}}, lambda:config.GatewayConfig(_env_file=None, service_key=key).auth_enabled)
        for codepoint in range(0x1c, 0x20):
            key = chr(codepoint)
            h.intentional_difference(f'auth/control_separator/U+{codepoint:04X}',
                {'op':'auth','data':{'service_key':key}}, True,
                lambda:config.GatewayConfig(_env_file=None, service_key=key).auth_enabled)
        for path in ('','conf/front.yaml','../front.yaml','/tmp/../front.yaml','./a/../b','//tmp/front.yaml','///tmp/front.yaml'):
            h.check(f'path/{path}', {'op':'path','path':path,'cwd':os.getcwd()}, lambda:GatewaySettings(_env_file=None, front_config_path=path).resolved_front_config_path())


def build():
    env = dict(os.environ)
    cargo = shutil.which('cargo')
    if cargo is None:
        cargo = '/tmp/vogon-rust-toolchain/cargo/bin/cargo'
        env.update(CARGO_HOME='/tmp/vogon-rust-toolchain/cargo',RUSTUP_HOME='/tmp/vogon-rust-toolchain/rustup')
    subprocess.run([cargo,'build','--release','--offline','--locked','-p','vogon-gateway-validation'],cwd=WORKSPACE,env=env,check=True)
    return WORKSPACE/'target/release/vogon-gateway-validation'


def native_barge_tests():
    env=dict(os.environ)
    cargo=shutil.which('cargo')
    if cargo is None:
        cargo='/tmp/vogon-rust-toolchain/cargo/bin/cargo'
        env.update(CARGO_HOME='/tmp/vogon-rust-toolchain/cargo',RUSTUP_HOME='/tmp/vogon-rust-toolchain/rustup')
    command=[cargo,'test','--offline','--locked','-p','vogon-barge-in']
    result=subprocess.run(command,cwd=WORKSPACE,env=env,check=True,text=True,capture_output=True)
    print('barge: native lifecycle tests passed',flush=True)
    return {'passed':True,'command':command,'output':result.stdout}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--stage', choices=['contracts','playback','barge','all'], default='all')
    args=parser.parse_args()
    binary=build()
    stages=['contracts','playback','barge'] if args.stage=='all' else [args.stage]
    for stage in stages:
        h=Harness(binary)
        try:
            if stage=='contracts': contracts(h); configurations(h)
            elif stage=='playback':
                from playback_checks import run
                run(h)
            else:
                from barge_checks import run
                h.native_tests=native_barge_tests()
                run(h)
        finally:
            h.close()
        save(stage,h,binary)


def save(stage,h,binary):
    paths = [*sorted((WORKSPACE/'crates/contracts').rglob('*.rs')), *sorted((WORKSPACE/'crates/configuration').rglob('*.rs')),
             *sorted((WORKSPACE/'crates').glob('*/src/schema.json')), WORKSPACE/'Cargo.lock', Path(__file__),
             MONO/'vogon-contract/src/vogon_contract/types.py', MONO/'vogon-contract/src/vogon_contract/codec.py',
             MONO/'voice-gateway/vogon/gateway/settings.py', MONO/'voice-gateway/vogon/gateway_service/config.py']
    report={'passed':True, 'count':len(h.rows), 'cases':h.rows, 'python':sys.version,
            'intentional_difference_count':len(h.differences), 'intentional_differences':h.differences,
            'source_sha256':{str(p.relative_to(MONO)):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths},
            'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),
            'scope':'Offline differential correctness; no wrapper timing, network or provider calls'}
    paths += [*sorted((WORKSPACE/'crates/playback').rglob('*.rs')),ROOT/'playback_checks.py',
              WORKSPACE/'crates/gateway-validation/src/main.rs',WORKSPACE/'Cargo.toml',
              *sorted((WORKSPACE/'crates').glob('*/Cargo.toml')),
              MONO/'voice-gateway/vogon/gateway/transport/twilio_transport.py',
              MONO/'voice-gateway/vogon/gateway/transport/delivery.py',
              MONO/'voice-gateway/vogon/gateway/transport/outbound_queue.py',
              MONO/'voice-gateway/vogon/gateway/pipeline/sentence_split.py']
    paths += [*sorted((WORKSPACE/'crates/barge-in').rglob('*.rs')), ROOT/'barge_checks.py',
              WORKSPACE/'crates/gateway-validation/src/barge.rs',
              *[MONO/'voice-gateway/vogon/gateway'/p for p in ('transport/barge_gate.py', 'transport/barge_semantics.py', 'transport/floor_controller.py', 'textutils.py')], MONO/'voice-gateway/tests/test_barge_gate.py']
    if hasattr(h,'barge_coverage'): report['coverage']=h.barge_coverage
    if hasattr(h,'native_tests'): report['native_tests']=h.native_tests
    report['source_sha256']={str(p.relative_to(MONO)):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    (ROOT/'results').mkdir(parents=True, exist_ok=True)
    (ROOT/f'results/{stage}.json').write_text(json.dumps(report,indent=2,ensure_ascii=False)+'\n')
    print(f'{stage}: {len(h.rows)} parity cases passed',flush=True)
    if h.differences:
        print(f'{stage}: {len(h.differences)} intentional whitespace differences verified separately',flush=True)


if __name__=='__main__': main()
