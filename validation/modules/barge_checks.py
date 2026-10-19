"""Exact differential traces against production gate/semantics/FloorController.

The Python controller runs on asyncio with a controlled executor and timeout
futures. No replacement decision algorithm, real network, sleeps or timing
measurements. Transport effects and classifier inputs are part of the oracle.
"""
import asyncio
import copy
import dataclasses as dc
import itertools
import json
import random
from types import SimpleNamespace
from unittest.mock import patch

from vogon.gateway.transport.barge_gate import BargeGate, BargeGateConfig
from vogon.gateway.transport import barge_semantics as sem, floor_controller as fc
from vogon.gateway.textutils import normalize

FLOOR_DEFAULTS = dict(barge_episode_gap_ms=350, barge_repeat_speech_ms=240,
    barge_persistent_speech_ms=1200, barge_attempt_history_ms=10000,
    barge_floor_silence_ms=500, semantic_barge_window_ms=900,
    semantic_barge_llm_timeout_ms=700, semantic_barge_backchannel_max_tokens=3,
    semantic_barge_substantive_min_tokens=6)
TRANSPORT_DEFAULTS = dict(tts_playing=False, playback_draining=False, last_audio_id=None,
    last_speak_text='', protected_playout=False, absorb_playout=False,
    safety_phrases=[], floor_phrases=[], semantic_judge=False, heard='', unheard='',
    absorbing_utterance=False, absorb_detail=None, gap_active=False,
    typing_arm_deadline=0, ambience=False, tts_ended_at=0, candidate_raw_rms=0, candidate_clean_rms=0)
ACTIVE = dict(tts_playing=True, last_audio_id='reply-1',
    last_speak_text='Let me explain your available repayment options.',
    heard='Let me explain', unheard='your available repayment options.',
    safety_phrases=['do not call'], floor_phrases=['stop','let me talk'],
    ambience=True, candidate_raw_rms=3100, candidate_clean_rms=2800)


def gate_reference(request):
    callback_calls = 0
    current = None
    def vad(frame):
        nonlocal callback_calls
        callback_calls += 1
        assert frame == bytes(current['frame'])
        return current.get('vad', False)
    g = BargeGate(BargeGateConfig(**request.get('config', {})),
                  vad if request.get('vad_available', True) else None)
    results = []
    for current in request['actions']:
        verdict = None
        if current['action'] == 'start': g.on_tts_start(current['now'])
        elif current['action'] == 'reset': g.reset()
        else:
            v = g.on_frame(current['now'], current['rms'], current.get('playing', False), bytes(current['frame']))
            verdict = dc.asdict(v)
            verdict['forward_frames'] = [list(f) for f in v.forward_frames]
        state = {k.lstrip('_'):v for k,v in vars(g).items() if k != '_confirm_frame'}
        state['cfg'] = dc.asdict(g.cfg)
        state['state'] = g.state.value
        state['prov_buffer'] = [list(f) for f in g._prov_buffer]
        results.append(copy.deepcopy(dict(gate=state, threshold=g.threshold, in_progress=g.in_progress,
                            verdict=verdict, vad_calls=callback_calls)))
    return json.loads(json.dumps(results))


class FloorOracle:
    def __init__(self, request):
        self.now = request.get('now', 100.0)
        self.config = FLOOR_DEFAULTS | request.get('config', {})
        self.effects = []
        self.requests = []
        self.providers = []
        self.waiters = []
        self.finals = []
        self.admissions = []
        self.t = SimpleNamespace()
        self.update_transport(TRANSPORT_DEFAULTS | request.get('transport', {}))
        self.t._audio_log = SimpleNamespace(emit=lambda now,event,**detail:
            self.effect('log', now=now, event=event, detail=detail))
        self.t.pause_silence = lambda:self.effect('pause_silence')
        self.t.resume_silence = lambda:self.effect('resume_silence')
        self.t._barge_gate = SimpleNamespace(reset=lambda:self.effect('reset_gate'))
        self.t._playout_heard_split = lambda:(self.t._heard, self.t._unheard)
        self.t._emit_barge_cut_telemetry = lambda raw,clean:self.effect('cut_telemetry',raw_rms=raw,clean_rms=clean)
        async def cut():
            self.effect('cut_playback')
            if not self.t._tts_playing: self.t._playback_draining = False
        async def clear(): self.effect('send_clear')
        self.t._cut_playback = cut
        self.t._send_clear = clear
        def clear_marks():
            self.effect('clear_pending_marks')
            self.t._playback_draining = False
        self.t._pending_marks = SimpleNamespace(clear=clear_marks)
        self.t._close_ledger = lambda now:self.effect('close_ledger',now=now)
        self.t._note_playout_end = lambda reset_window=False:self.effect('note_playout_end',reset_window=reset_window)
        self.c = fc.FloorController(self.t, lambda:self.now)

    def effect(self, kind, **fields): self.effects.append(dict(kind=kind, **fields))

    def update_transport(self, values):
        for k,v in values.items():
            if k == 'ambience':
                v = SimpleNamespace(stop_typing=lambda:self.effect('stop_typing')) if v else None
            setattr(self.t, '_'+k, copy.deepcopy(v))

    def transport(self):
        d = {k:getattr(self.t, '_'+k) for k in TRANSPORT_DEFAULTS}
        d['ambience'] = d['ambience'] is not None
        return d

    def executor(self, executor, fn, text, heard, unheard, agent_line, attempts):
        assert fn is fc.classify_floor
        f = asyncio.get_running_loop().create_future()
        self.providers.append(f)
        self.requests.append(dict(token=len(self.providers), key=self.c.requested, text=text,
            heard=heard, unheard=unheard, agent_line=agent_line, attempts=attempts,
            deadline=self.now+self.config['semantic_barge_llm_timeout_ms']/1000))
        return f

    async def wait_for(self, awaitable, timeout):
        task = asyncio.current_task()
        kind = 'classifier' if task is self.c.worker else 'final'
        alarm = asyncio.get_running_loop().create_future()
        waiter = (kind, task, self.now+timeout, alarm)
        self.waiters.append(waiter)
        try:
            done, _ = await asyncio.wait([awaitable, alarm], return_when=asyncio.FIRST_COMPLETED)
            if alarm in done:
                awaitable.cancel()  # shield wrapper, never the underlying worker/provider
                raise asyncio.TimeoutError
            return awaitable.result()
        finally:
            self.waiters.remove(waiter)
            if not alarm.done(): alarm.cancel()

    async def settle(self):
        # Bounded event-loop turns, no wall-clock sleeps. Enough for shield,
        # wait_for, worker and final/admission continuations to settle.
        for _ in range(20): await asyncio.sleep(0)

    def snapshot(self):
        c = self.c
        d = dict(episodes=[dc.asdict(e) for e in c.episodes],
            current=c.current.id if c.current else None,
            **{k:getattr(c,k) for k in ('serial','generation','caller_floor','awaiting_reply',
                'last_speech','final_seen','requested','accepted_generation','pending_text')},
            released=c.released.is_set(), protection_released=c.protection_released.is_set(),
            pending=c.pending[1] if c.pending else None,
            worker_active=c.worker is not None and not c.worker.done(),
            transport=self.transport(), effects=self.effects, requests=self.requests,
            finals=[t.done() for t in self.finals],
            admissions=['waiting' if not t.done() else 'allowed' if t.result() else 'rejected' for t in self.admissions])
        self.effects = []
        return json.loads(json.dumps(d))

    async def run(self, request):
        async def heartbeat_sleep(seconds):
            await asyncio.get_running_loop().create_future()  # tests drive heartbeat ticks explicitly
        facade = SimpleNamespace(**{k:getattr(asyncio,k) for k in dir(asyncio) if k not in ('wait_for','sleep')},wait_for=self.wait_for,sleep=heartbeat_sleep)
        out = []
        loop = asyncio.get_running_loop()
        with patch.object(fc,'settings',SimpleNamespace(**self.config)), patch.object(fc,'asyncio',facade), patch.object(loop,'run_in_executor',self.executor):
            try:
                for a in request['actions']:
                    self.now = a.get('now',self.now)
                    self.update_transport(a.get('transport',{}))
                    op = a['action']
                    if op=='start': self.c.start()
                    elif op=='frame': await self.c.frame(a.get('rms',3000), a.get('threshold',1200), a.get('duration',20), a.get('confirmed',False))
                    elif op=='transcript':
                        t = asyncio.create_task(self.c.transcript(a.get('text',''), a.get('final',False)))
                        self.finals.append(t)
                        await asyncio.sleep(0)  # execute transcript up to its optional EOT wait
                    elif op=='tick': await self.c.tick()
                    elif op=='resume': self.c.resume()
                    elif op=='finished': await self.c.playback_finished()
                    elif op=='machine': self.c.take_floor_from_machine(a.get('audio_id'))
                    elif op=='accept': self.c.accepted_generation=a.get('generation')
                    elif op=='allow':
                        self.admissions.append(asyncio.create_task(self.c.allow_playback(a.get('generation'), a.get('wait',False), a.get('audio_id'))))
                        await asyncio.sleep(0)
                    elif op in ('result','error'):
                        f = self.providers[a.get('token',len(self.providers))-1]
                        assert not f.done(), 'fixture attempted to resolve a finished provider'
                        if op=='error': f.set_exception(RuntimeError('fixture failure'))
                        else: f.set_result(a['decision'])
                    elif op in ('classifier_timeout','final_timeout'):
                        found = False
                        for kind,task,deadline,alarm in list(self.waiters):
                            target = kind=='classifier' if op=='classifier_timeout' else task is self.finals[a['index']]
                            if target:
                                assert self.now>=deadline, (self.now,deadline)
                                alarm.set_result(None)
                                found=True
                        assert found, (op,self.waiters)
                    elif op=='close': await self.c.close()
                    elif op not in ('pump','context'): raise ValueError(op)
                    if a.get('pump',True): await self.settle()
                    for t in self.finals:
                        if t.done(): t.result()  # fail on oracle exceptions, never silently compare errors
                    out.append(self.snapshot())
            finally:
                await self.c.close()
                for t in self.finals+self.admissions:
                    if not t.done(): t.cancel()
                await asyncio.gather(*self.finals,*self.admissions,return_exceptions=True)
                for f in self.providers:
                    if not f.done(): f.cancel()
        return out


def check(h, name, request, expected):
    # Fail with first differing step/field rather than dumping an entire trace.
    actual = h.call(request)
    if actual != {'ok':expected}:
        value = actual.get('ok')
        if isinstance(value,list) and isinstance(expected,list):
            for i,(a,e) in enumerate(itertools.zip_longest(value,expected)):
                if a!=e:
                    diffs = {k:{'python':e.get(k),'rust':a.get(k)} for k in e.keys()|a.keys() if e.get(k)!=a.get(k)} if isinstance(a,dict) and isinstance(e,dict) else (a,e)
                    raise AssertionError(json.dumps({'case':name,'step':i,'action':request.get('actions',[{}])[i], 'difference':diffs},ensure_ascii=False,indent=2))
        raise AssertionError((name,request,expected,actual))
    h.check(name,request,lambda:expected)
    if not hasattr(h,'barge_coverage'):
        h.barge_coverage={'semantic_cases':0,'gate_traces':0,'gate_steps':0,'floor_traces':0,'floor_steps':0,'classifier_requests':0,'events':{},'existing_gate_tests':0,'retained_vad_traces':0}
    c=h.barge_coverage
    if request['op']=='barge_semantics': c['semantic_cases']+=1
    elif request['op']=='barge_gate':
        c['gate_traces']+=1; c['gate_steps']+=len(expected)
        c['existing_gate_tests']+=int('/existing_gate/' in name)
        c['retained_vad_traces']+=int('/retained_vad/' in name)
    else:
        c['floor_traces']+=1; c['floor_steps']+=len(expected)
        c['classifier_requests']+=len(expected[-1]['requests'])
        for step in expected:
            for effect in step['effects']:
                if effect['kind']=='log':
                    key=effect['event']+'/'+effect['detail']['reason']
                    c['events'][key]=c['events'].get(key,0)+1


def run(h):
    from vogon.gateway.settings import GatewaySettings
    assert {k:GatewaySettings.model_fields[k].default for k in FLOOR_DEFAULTS} == FLOOR_DEFAULTS
    rng = random.Random(20261020)
    texts = ['', ' \u2003 ', 'okay', 'okay?', 'right yeah!', 'got it', 'sounds good', 'uh-huh',
        'I need to change my payment arrangement', 'What?', 'Do not call me', 'Stop', 'stopped',
        'nonstop', 'let me talk', 'Let me', 'Let me explain your available', 'unknown phrase',
        '.,!?', 'I SEE', 'İ Σ ΟΣ', '1,200,000.?!', '١,٢٠٠!', '１,２３４?',
        '“That’s 1,200.30!”', 'one\u2003two\u00a0three']
    for _ in range(300):
        texts.append(' '.join(rng.choices(['okay','yes','stop','stopper','wait','help','what','\u00a0','word','got','it','?','‘','—','1,000','２,４'],k=rng.randrange(9))))
    policies = [{}, {'safety_phrases':['do not call'], 'floor_phrases':['stop','let me talk'], 'playout_text':ACTIVE['last_speak_text']},
        {'safety_phrases':['STOP'], 'floor_phrases':['hold on',''], 'playout_text':'Please hold on while I find 1,200.'},
        {'backchannel_max_tokens':1,'substantive_min_tokens':4}]
    for i,(text,policy) in enumerate(itertools.product(texts,policies)):
        llm = rng.choice([None,'backchannel','answer_to_heard','safety','floor_request','substantive_interrupt','invalid'])
        absorb = rng.choice(['echo','intro_absorb','backchannel','no_text'])
        min_words = rng.choice([0,2,4])
        hv = sem.heuristic_verdict(text,**policy)
        expected = dict(normalized=normalize(text),heuristic=hv,resolved=list(sem.resolve_final(hv,llm)),
            swallow=sem.should_swallow_final(text,**policy),swallow_absorbed=sem.should_swallow_absorbed_final(absorb,text,**policy),
            echo=sem.is_echo_of(text,policy.get('playout_text',''),min_words=min_words),absorb=sem.is_absorb(hv))
        check(h,f'barge/semantics/{i}',dict(op='barge_semantics',text=text,policy=policy,llm=llm,absorb=absorb,min_words=min_words),expected)
    run_gates(h,rng)
    run_floors(h,rng)
    run_existing_gate_tests(h)
    run_retained_vad_traces(h)
    for codepoint in range(0x1c, 0x20):
        text = f'okay{chr(codepoint)}yes'
        expected_rust = dict(normalized=text, heuristic='ambiguous',
            resolved=['timeout_absorb','window_expiry'], swallow=True,
            swallow_absorbed=True, echo=False, absorb=False)
        def reference():
            hv = sem.heuristic_verdict(text)
            return dict(normalized=normalize(text), heuristic=hv,
                resolved=list(sem.resolve_final(hv,None)), swallow=sem.should_swallow_final(text),
                swallow_absorbed=sem.should_swallow_absorbed_final('backchannel',text),
                echo=sem.is_echo_of(text,''), absorb=sem.is_absorb(hv))
        h.intentional_difference(f'barge/control_separator/U+{codepoint:04X}',
            dict(op='barge_semantics',text=text,absorb='backchannel'), expected_rust, reference)


def run_gates(h,rng):
    configs = [ {}, {'consecutive_frames':1,'grace_sec':0},
        {'adaptive_enabled':True,'adaptive_warmup_frames':5},
        {'confirm':'local_vad','grace_sec':0},
        {'confirm':'local_vad','adaptive_enabled':True,'adaptive_warmup_frames':3},
        {'confirm':'local_vad','confirm_frames_m':1,'confirm_frames_n':1},
        {'confirm':'local_vad','confirm_frames_m':5,'confirm_frames_n':3,'confirm_timeout_sec':0.05},
        {'confirm':'local_vad','confirm_frames_n':0},
        {'confirm':'local_vad','confirm_frames_n':-2},
    ]
    for i in range(180):
        config=configs[i%len(configs)]
        now=10.0
        actions=[dict(action='start',now=now)]
        for n in range(200):
            now += rng.choice([.02,.02,.02,.001,.1,.5])
            action=rng.choices(['frame','reset','start'],[94,4,2])[0]
            actions.append(dict(action=action, now=now,rms=rng.choice([0,100,500,600,1200,1201,2000,3000]),
                playing=rng.random()<.9,frame=[n%256,0,255,n%256],vad=rng.random()<.5))
        req=dict(op='barge_gate',config=config,vad_available=i%7!=0,actions=actions)
        check(h,f'barge/gate/random/{i}',req,gate_reference(req))
    # Deterministic regression windows, including confirmation on the timeout
    # frame (confirmation wins), energy abort, grace and reset/latch behavior.
    for flags in itertools.product([False,True],repeat=7):
        actions=[dict(action='start',now=0)]
        for n,flag in enumerate(flags):
            actions.append(dict(action='frame',now=(n+1)*.02,rms=3000 if n<4 else 100,
                playing=True,frame=[n,0],vad=flag))
        req=dict(op='barge_gate',config=dict(confirm='local_vad',consecutive_frames=1,grace_sec=0,
            confirm_timeout_sec=.1),actions=actions)
        check(h,f'barge/gate/window/{flags}',req,gate_reference(req))


def frame(now=None, rms=3000, confirmed=True, duration=20):
    return dict(action='frame',**({'now':now} if now is not None else {}),rms=rms,confirmed=confirmed,duration=duration)
def text(value, final=False, **kw): return dict(action='transcript',text=value,final=final,**kw)

def run_floors(h,rng):
    traces=[]
    def add(name, actions, transport=None, config=None):
        traces.append((name,dict(op='barge_floor',transport=ACTIVE|(transport or {}),config=config or {},actions=actions)))
    add('policy_and_gate',[dict(action='start'),text('okay'),frame(),text('okay I need another payment arrangement please'),text('okay I need another payment arrangement please',True),
        dict(action='context',transport=dict(tts_playing=False)),dict(action='tick',now=100.6),dict(action='allow',generation=1,audio_id='next')])
    for protected,absorb in itertools.product([False,True],repeat=2):
        for utterance in ['okay','short fragment','stop','do not call','I want to discuss another payment arrangement']:
            add(f'policy/{protected}/{absorb}/{utterance}',[frame(),text(utterance),text(utterance,True),dict(action='allow',generation=0,wait=False),
                dict(action='allow',generation=0,wait=True),dict(action='context',transport={'tts_playing':False,'protected_playout':False}),dict(action='finished'),dict(action='tick',now=101)],
                dict(protected_playout=protected,absorb_playout=absorb))
    add('echo_revision',[frame(),text('Let me'),text('Let me explain your available'),text('wait a moment'),frame(),text('stop')],dict(semantic_judge=True))
    add('final_short_echo_can_yield',[frame(),text('Let me'),text('Let me',True),dict(action='result',decision='YIELD'),dict(action='tick',now=101)],dict(semantic_judge=True))
    add('protected_retract',[frame(),text('I need to change my payment arrangement'),text('okay',True),dict(action='finished')],dict(protected_playout=True))
    add('deferred_without_generation_increment',[frame(),text('stop',True),dict(action='context',transport=dict(tts_playing=False)),dict(action='finished'),dict(action='tick',now=100.6)],dict(protected_playout=True))
    add('energy_blip_after_final',[text('hello',True),dict(action='context',transport=dict(tts_playing=False)),frame(100.4,rms=0),frame(100.5),frame(101.1,rms=0),dict(action='allow',generation=0)],dict(tts_playing=False))
    add('energy_keeps_final_release',[dict(action='resume'),frame(),text('hello',True),frame(100.25),dict(action='tick',now=100.8),dict(action='allow',generation=1)],dict(tts_playing=False))
    add('resumed_words_invalidate',[dict(action='resume'),frame(),text('hello',True),frame(100.25),text('one more thing'),dict(action='allow',generation=1),text('one more thing',True),dict(action='tick',now=101)],dict(tts_playing=False))
    add('delayed_partial_binds_new_episode',[frame(),text('okay'),frame(100.5,rms=0),text('short fragment'),frame(100.6),text('updated fragment')])
    add('pressure',[frame(),text('short fragment')]+[frame(100+n*.02) for n in range(1,65)])
    add('repeat_failed',[frame(),text('short fragment'),dict(action='tick',now=100.91),frame(101,rms=0)]+[frame(101.1+n*.02) for n in range(14)])
    add('history_expired',[frame(),text('short fragment'),dict(action='tick',now=100.91),frame(101,rms=0)]+[frame(112+n*.02) for n in range(14)])
    add('machine_takes_floor',[dict(action='resume'),dict(action='allow',generation=1,wait=True),dict(action='machine',audio_id='voicemail'),dict(action='allow',generation=1)],dict(tts_playing=False))
    for generation in [None,0,1,2]:
        add(f'admission_hold/{generation}',[dict(action='resume'),frame(),dict(action='allow',generation=generation,wait=True),text('hello',True),dict(action='tick',now=101)],dict(tts_playing=False))
    add('admission_stale_after_hold',[dict(action='resume'),frame(),text('hello',True),dict(action='allow',generation=1,wait=True),text('hello again'),text('hello again',True),dict(action='tick',now=101)],dict(tts_playing=False))
    for verdict in ['CONTINUE','YIELD','UNCERTAIN','not a verdict']:
        add(f'classifier/{verdict}',[frame(),text('short fragment'),dict(action='result',decision=verdict,now=100.125),text('okay',True)],dict(semantic_judge=True))
    add('classifier_error',[frame(),text('short fragment'),dict(action='error')],dict(semantic_judge=True))
    add('classifier_stale_revision',[frame(),text('short fragment'),text('different fragment'),dict(action='result',decision='YIELD'),dict(action='result',decision='CONTINUE')],dict(semantic_judge=True))
    add('classifier_latest_pending',[frame(),text('short fragment'),text('second fragment'),text('third fragment'),text('fourth fragment'),dict(action='result',decision='YIELD'),dict(action='result',decision='UNCERTAIN')],dict(semantic_judge=True))
    add('classifier_stale_audio',[frame(),text('short fragment'),dict(action='context',transport=dict(last_audio_id='reply-2')),dict(action='result',decision='YIELD')],dict(semantic_judge=True))
    add('classifier_playback_ended',[frame(),text('short fragment'),dict(action='context',transport=dict(tts_playing=False)),dict(action='result',decision='YIELD')],dict(semantic_judge=True))
    add('classifier_safety_wins',[frame(),text('short fragment'),text('do not call'),dict(action='result',decision='CONTINUE')],dict(semantic_judge=True))
    add('classifier_deadline_drains',[frame(),text('short fragment'),dict(action='classifier_timeout',now=100.71),text('latest fragment'),dict(action='result',decision='YIELD',now=101),dict(action='result',decision='CONTINUE')],dict(semantic_judge=True))
    add('classifier_deadline_audio_continues',[frame(),text('short fragment'),dict(action='classifier_timeout',now=100.71)]+[frame(100.72+n*.02) for n in range(65)]+[dict(action='result',decision='YIELD')],dict(semantic_judge=True))
    add('final_deadline_rejects_late_cut',[frame(),text('short fragment',True),dict(action='final_timeout',index=0,now=100.71),dict(action='result',decision='YIELD')],dict(semantic_judge=True))
    add('final_waits_for_worker_drain',[frame(),text('short fragment'),text('short fragment',True,now=100.1),dict(action='classifier_timeout',now=100.71),dict(action='final_timeout',index=1,now=100.81),dict(action='result',decision='YIELD')],dict(semantic_judge=True))
    add('final_classifier_resolves',[frame(),text('short fragment',True),dict(action='result',decision='YIELD',now=100.2)],dict(semantic_judge=True))
    add('drain_cut',[frame(),text('stop',True)],dict(tts_playing=False,playback_draining=True))
    add('history_clear_only_accepted',[frame(),text('short fragment'),dict(action='tick',now=101),dict(action='finished'),dict(action='accept',generation=0),dict(action='finished')])
    add('bounded_episodes',[a for i in range(30) for a in [frame(100+i),text('short fragment'),frame(100+i+.5,rms=0)]])
    add('close_inflight',[frame(),text('short fragment'),dict(action='close')],dict(semantic_judge=True))
    add('release_pulse_before_admission_resumes',[dict(action='resume'),frame(),dict(action='allow',generation=1,wait=True),text('hello',True),dict(action='tick',now=101,pump=False),dict(action='resume',pump=False),dict(action='pump')],dict(tts_playing=False))
    add('provider_retains_evicted_episode',[frame(),text('short fragment')]+[a for i in range(22) for a in [frame(101+i,rms=0),frame(101+i+.5)]]+[dict(action='classifier_timeout',now=125),dict(action='result',decision='YIELD')],dict(semantic_judge=True))
    add('two_finals_independent_deadlines',[frame(),text('short fragment',True),text('short fragment',True,now=100.1),dict(action='final_timeout',index=0,now=100.71),dict(action='final_timeout',index=1,now=100.81),dict(action='result',decision='YIELD')],dict(semantic_judge=True))
    add('final_waits_for_pending_revision',[frame(),text('short fragment',True),text('different fragment'),dict(action='result',decision='YIELD'),dict(action='result',decision='CONTINUE')],dict(semantic_judge=True))
    add('pending_uses_dispatch_text_and_original_key',[frame(),text('short fragment'),text('second fragment'),text('okay'),dict(action='result',decision='YIELD'),dict(action='result',decision='YIELD')],dict(semantic_judge=True))
    add('stale_timeout_still_drains',[frame(),text('short fragment'),text('different fragment'),dict(action='classifier_timeout',now=100.71),text('latest fragment'),dict(action='error',now=100.75),dict(action='result',decision='CONTINUE')],dict(semantic_judge=True))
    add('deferred_latest_episode',[frame(),text('stop'),frame(100.5,rms=0),frame(100.6),text('another fragment',True),dict(action='allow',generation=0,wait=True),dict(action='context',transport=dict(tts_playing=False)),dict(action='finished'),dict(action='tick',now=101.2)],dict(protected_playout=True))
    for n in range(120):
        now=100.0
        actions=[dict(action='start')]
        for k in range(60):
            now+=rng.choice([.02,.02,.1,.4,1.0])
            op=rng.choice(['frame','frame','transcript','transcript','tick','allow','finished','context','machine','resume','accept'])
            if op=='frame': a=frame(rms=rng.choice([0,1200,1201,3000]),confirmed=rng.choice([True,False]),duration=rng.choice([10,20,100,250]))
            elif op=='transcript': a=text(rng.choice(['','okay','short fragment','Let me','Let me explain your available','stop','do not call','I have another question about my account']),rng.choice([False,True]))
            elif op=='allow': a=dict(action=op,generation=rng.choice([None,0,1,2,3,4]),wait=rng.choice([True,False]),audio_id='next')
            elif op=='context': a=dict(action=op,transport=dict(tts_playing=rng.choice([False,True]),playback_draining=rng.choice([False,True]),protected_playout=rng.choice([False,True]),absorb_playout=rng.choice([False,True])))
            elif op=='accept': a=dict(action=op,generation=rng.choice([None,0,1,2,3,4]))
            else: a=dict(action=op)
            a['now']=now;actions.append(a)
        add(f'random/{n}',actions,config=dict(barge_persistent_speech_ms=rng.choice([100,400,1200]),barge_repeat_speech_ms=rng.choice([40,240]),semantic_barge_window_ms=rng.choice([100,900])))
    for name,req in traces:
        expected=asyncio.run(FloorOracle(req).run(req))
        check(h,'barge/floor/'+name,req,expected)


def run_existing_gate_tests(h):
    """Run unchanged Python regression assertions with Rust-returned verdicts.

    The Python gate observes each input in parallel only to obtain injected VAD
    results and the exact reference snapshot. Assertions read the Rust state.
    """
    import inspect
    import pytest
    import test_barge_gate as tests
    from vogon.gateway.transport import twilio_transport
    from vogon.gateway.transport.barge_gate import FrameVerdict, GateState

    class PairedGate:
        def __init__(self, cfg, confirm_frame=None):
            self.request = dict(op='barge_gate',config=dc.asdict(cfg),
                                vad_available=confirm_frame is not None,actions=[])
            def confirm(frame):
                value=bool(confirm_frame(frame))
                self.request['actions'][-1]['vad']=value
                return value
            self.reference = BargeGate(copy.deepcopy(cfg),confirm if confirm_frame else None)
            self.last = None
            self.reset()
            instances.append(self)
        def execute(self, a):
            self.request['actions'].append(a)
            if a['action']=='frame':
                self.reference.on_frame(a['now'],a['rms'],a['playing'],bytes(a['frame']))
            elif a['action']=='start': self.reference.on_tts_start(a['now'])
            else: self.reference.reset()
            expected=gate_reference(self.request)
            actual=h.call(self.request)
            assert actual=={'ok':expected}, test_name
            self.last=actual['ok'][-1]
        def on_frame(self,now,clean_rms,tts_playing,frame):
            self.execute(dict(action='frame',now=now,rms=clean_rms,playing=tts_playing,frame=list(frame)))
            d=copy.deepcopy(self.last['verdict'])
            d['forward_frames']=[bytes(f) for f in d['forward_frames']]
            return FrameVerdict(**d)
        def on_tts_start(self,now): self.execute(dict(action='start',now=now))
        def reset(self): self.execute(dict(action='reset'))
        @property
        def threshold(self): return self.last['threshold']
        @property
        def in_progress(self): return self.last['in_progress']
        @property
        def state(self): return GateState(self.last['gate']['state'])
        @property
        def streak(self): return self.last['gate']['streak']
        @property
        def noise_floor(self): return self.last['gate']['floor']
        @property
        def cfg(self): return BargeGateConfig(**self.last['gate']['cfg'])

    for test_name,test in vars(tests).items():
        if not test_name.startswith('test_') or not callable(test): continue
        instances=[]
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(tests,'BargeGate',PairedGate)
            mp.setattr(twilio_transport,'BargeGate',PairedGate)
            # Production env tuning must not change the regression fixture.
            for field,value in dict(barge_in_confirm='off',barge_in_adaptive_enabled=False,
                barge_in_rms_threshold=1200,barge_in_consecutive_frames=3,barge_in_grace_ms=100,
                barge_in_adaptive_min=600,barge_in_adaptive_max=2600,barge_in_adaptive_margin=7.0,
                barge_in_adaptive_alpha=.05,barge_in_adaptive_warmup_frames=50).items():
                mp.setattr(tests.settings,field,value)
            test(**{k:mp for k in inspect.signature(test).parameters})
        for i,g in enumerate(instances):
            check(h,f'barge/existing_gate/{test_name}/{i}',g.request,gate_reference(g.request))


def run_retained_vad_traces(h):
    import math
    import struct
    from vogon.gateway.transport.barge_gate import make_confirm_frame
    rng=random.Random(42)
    for mode in range(4):
        for scenario in ('silence','tone','noise','alternating'):
            vad=make_confirm_frame('local_vad',mode,8000)
            assert vad is not None, 'retained WebRTC VAD must be installed for this gate'
            config=dict(confirm='local_vad',grace_sec=0,consecutive_frames=1)
            actions=[]
            current=None
            def confirm(data):
                current['vad']=bool(vad(data))
                return current['vad']
            ref=BargeGate(BargeGateConfig(**config),confirm)
            for i in range(80):
                kind=scenario if scenario!='alternating' else ['silence','tone','noise'][(i//8)%3]
                samples=[0 if kind=='silence' else int(6000*math.sin(2*math.pi*220*(i*160+j)/8000)) if kind=='tone' else rng.randint(-6000,6000) for j in range(160)]
                data=struct.pack('<160h',*samples)
                current=dict(action='frame',now=10+i*.02,rms=0 if kind=='silence' else 4000,playing=True,frame=list(data))
                actions.append(current)
                ref.on_frame(current['now'],current['rms'],True,data)
                if i%13==12:
                    actions.append(dict(action='reset'))
                    ref.reset()
            request=dict(op='barge_gate',config=config,actions=actions)
            check(h,f'barge/retained_vad/{mode}/{scenario}',request,gate_reference(request))
