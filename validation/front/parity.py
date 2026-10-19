"""Module-8 differential checks against actual Python policies. No network or secrets."""
import asyncio
import dataclasses as dc
import hashlib
import itertools
import json
import logging
from pathlib import Path
import random
import subprocess
import sys
import tempfile
from types import SimpleNamespace as NS
from unittest.mock import patch

ROOT=Path(__file__).resolve().parent
WORKSPACE=ROOT.parents[1];MONO=WORKSPACE.parent
sys.path[:0]=[str(MONO/'voice-gateway'),str(MONO/'vogon-contract/src')]
from vogon.gateway.transport import front_guard as guard,front_moves as moves,front_pick as pick,front_source as source,line_detector as line
from vogon.gateway import jev
from vogon_contract import LineDetection
logging.disable(logging.CRITICAL)
RNG=random.Random(81)
COUNTS={};ROWS=[]
CHILD=subprocess.Popen([str(WORKSPACE/'target/debug/front-validation')],stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True)
def check(group,request,expected):
    CHILD.stdin.write(json.dumps(request,ensure_ascii=False)+'\n');CHILD.stdin.flush()
    actual=json.loads(CHILD.stdout.readline())
    expected=json.loads(json.dumps(expected))
    assert actual=={'ok':expected},(group,request,expected,actual)
    COUNTS[group]=COUNTS.get(group,0)+1
    ROWS.append(hashlib.sha256(json.dumps(actual,sort_keys=True).encode()).hexdigest())
def error(group,request):
    CHILD.stdin.write(json.dumps(request)+'\n');CHILD.stdin.flush();actual=json.loads(CHILD.stdout.readline());assert 'error' in actual,(group,actual)
    COUNTS[group]=COUNTS.get(group,0)+1

def guards():
    texts=['','   ','One moment.','One moments','one seconding','one sec.','one second','one-second','one\tsecond','just ONE thing','No problem.','Go ahead.','I am taking care of it.','Let me check. Another line.','One moment.\n','Did you say Alex?','checking 10%','$0.5','May I see it','année','éyesé','_yes_','yes\u0301','One moment.\u00a0Looking.','İYES','checking loan+s','noting that authorization.']
    texts += [f'Before {w} after.' for w in guard.COMMITMENT+guard.ACTION_CLAIMS+guard.SPELLED_NUMBERS+guard.CALENDAR]
    tokens=['One','moment.','checking','the','details','for','you','now','Alex','yes','no','\n','12','$','two','tomorrow','?','loan','loans','é','聲音','okay']
    texts += [' '.join(RNG.choices(tokens,k=RNG.randrange(1,15))) for _ in range(600)]
    for text in texts:
        cfg={'max_words':RNG.choice([0,4,8,12]),'must_not_say':['loan','loan+','credit','rate'],'forbidden_names':['Alex','John Smith'],'earlier':RNG.choice([[],['Let me check those details now.'],[text]])}
        check('guard',{'op':'guard','text':text,'config':cfg},{'trips':guard.guard(text,**cfg),'normalized':guard.normalize(text),'sentences':guard.sentences(text)})
def picks():
    tiers={'repeat':['Let me try again.'],'courtesy':['Thanks for your time.'],'lookup':['Let me check that for you.'],'beat':['One moment.']}
    for movement,bits,confidence in itertools.product(moves.MOVES+["UNRECOGNIZED"] if isinstance(moves.MOVES,list) else (*moves.MOVES,'UNRECOGNIZED'),range(128),[0.44,0.45,0.99]):
        riders={name:float(bool(bits&(1<<i))) for i,name in enumerate(moves.RIDERS)}
        read=moves.FrontRead(move=movement,confidence=confidence,riders=riders)
        pool=tiers if bits%3 else {k:v for k,v in tiers.items() if k not in ('lookup','courtesy')}
        spoken=bits%5!=0
        check('pick',{'op':'pick','read':dc.asdict(read),'tiers':pool,'has_spoken':spoken},dc.asdict(pick.pick(read,pool,lambda k,p:p[0],has_spoken=spoken)))
    for pool in [{},{'beat':[]},{'repeat':[]}]:
        read=moves.FrontRead(move='FIGURE',confidence=1)
        check('pick',{'op':'pick','read':dc.asdict(read),'tiers':pool},dc.asdict(pick.pick(read,pool,lambda k,p:p[0])))
def protocol():
    for count in range(8):
        riders={k:{'ask':f'ask {k}','yes_when':'yes','no_when':'no'} for k in moves.RIDERS[:count]}
        criteria={'REFUSAL':'decline','UNSURE':'unsure'}
        check('questions',{'op':'questions','criteria':criteria,'riders':riders,'question':''},moves.build_questions(criteria,riders))
    for criteria in [{},{'one':'one'}]:error('invalid_questions',{'op':'questions','criteria':criteria,'riders':{}})
    for i in range(100):
        raw={'answers':{'move':{'type':'choice','choice':'FIGURE','probabilities':{'FIGURE':.8,'UNSURE':.2},'confidence':RNG.choice([None,.9])},'yes':{'type':'noul','noul':RNG.choice([True,False,.5])},'scale':{'type':'score','score':RNG.randrange(5),'confidence':.8}},'usage':{'input_tokens':i,'output_tokens':3,'cost':.001},'model':'fixture','id':str(i)}
        result=jev._parse(NS(**{**raw,'answers':{k:NS(**v) for k,v in raw['answers'].items()},'usage':NS(**raw['usage'])}),1.25)
        expected=dc.asdict(result)
        for name,a in raw['answers'].items():
            expected['answers'][name]['type']=a['type']
            if a['type']=='noul':expected['answers'][name]['noul']=expected['answers'][name].pop('probability')
        check('answers',{'op':'parse','raw':raw},expected)
    for a in [{'type':'unknown'},{'type':'noul','noul':'not-a-number'},{'type':'choice','choice':'x','probabilities':[]},{'type':'score','score':None}]:error('invalid_answers',{'op':'parse','raw':{'answers':{'q':a}}})
def artifacts():
    phrases=['One moment.','Still here.','Let me check that for you.','Thanks for your time.','Got it.','Pay $10.','May I ask?','   ']
    with tempfile.TemporaryDirectory(prefix='vogon-front-artifacts-') as folder:
        path=Path(folder)/'front.yaml'
        for i in range(50):
            value={'prompt':'  You are {agent_name}.  ','beats':{k:RNG.sample(phrases,RNG.randrange(4)) for k in ['hold','wait']},'tiers':{k:RNG.sample(phrases,RNG.randrange(4)) for k in ['beat','lookup','repeat']},'must_not_say':['loan'],'move_criteria':{'ASSENT':'agree','REFUSAL':'decline'},'move_riders':{},'move_question':'  Which move?  '}
            path.write_text(json.dumps(value))
            obj=source.FrontConfig.load(str(path))
            expected={k:getattr(obj,k) for k in ['prompt','beats','tiers','must_not_say','move_criteria','move_riders','move_question']}
            expected['rejected']=[f'{family}:{kind}' for family in ['beats','tiers'] for kind in sorted(value[family]) if kind not in expected[family]]
            check('artifact',{'op':'artifact','value':value},expected)

def echo():
    ours='Hello this is Maya calling on a recorded line'
    texts=['',ours,'Hi you have reached Alex',"Hi this is Maya calling you've reached Alex on a recorded line leave a message",'HELLO, this IS Maya... hello this is Maya','just hello this','聲音 é other']
    texts+=[' '.join(RNG.choices(ours.split()+['leave','message','Alex','beep'],k=RNG.randrange(1,30))) for _ in range(250)]
    for text in texts:
        for min_run in [1,3,5]:check('echo',{'op':'echo','text':text,'ours':ours,'min_run':min_run},line.strip_echo(text,ours,min_run))
async def sources():
    artifact={'prompt':'You are {agent_name} for {lender_name}.','beats':{'hold':['One moment.'],'wait':['Still checking.']},'must_not_say':['loan'],'tiers':{'repeat':['Let me try again.'],'lookup':['Let me check that for you.'],'beat':['One moment.'],'courtesy':['Thanks for your time.']},'move_criteria':{'FIGURE':'figure','UNSURE':'unclear'},'move_riders':{},'move_question':''}
    for trial in range(60):
        cfg={'engine':'jev','model_effort':'none','max_words':12,'min_gap_sec':6.,'move_floor':.45,'repeat_enabled':trial%2==0,'beat_after_ms':900,'beat_after_empty_ms':1500,'wait_again_after_ms':2500}
        obj=source.FrontModelSource(source.FrontConfig(**artifact),agent_name='Maya',lender_name='' if trial%2 else 'Example',borrower_first_name='Alex')
        now=[100.];steps=[];expected=[]
        def snapshot():return {'system':obj._system,'history':obj._history,'lines_played':obj.lines_played,'metrics':obj.metrics.copy(),'distress':obj._distress_latch,'deliberate_silence':obj._deliberate_silence,'last_line_t':obj._last_line_t,'n':obj._n}
        with patch.multiple(source.settings,**{'front_'+k:v for k,v in cfg.items()}),patch.object(source.time,'monotonic',lambda:now[0]),patch.object(source.random,'sample',lambda p,n:list(p)):
            for i in range(30):
                op=RNG.choice(['borrower','agent','played','read','admit','beat','message']);text=RNG.choice(['One moment.','Let me check that for you.','Hello Alex.','The desk answered.','new caller text'])+(' '+str(i) if op=='read' else '')
                step={'op':op,'text':text};result=None
                if op=='borrower':obj.note_borrower(text)
                elif op=='agent':obj.note_agent(text)
                elif op=='played':obj.note_played(text)
                elif op=='read':
                    read=moves.FrontRead(move=RNG.choice(moves.MOVES),confidence=RNG.choice([.3,.9]),riders={RNG.choice(moves.RIDERS):1.})
                    step['read']=dc.asdict(read);obj._deliberate_silence=False
                    async def fake(*args,**kwargs):return read
                    with patch.object(source.front_moves,'read_async',fake):result=await obj._read_and_pick(text)
                elif op=='admit':now[0]+=RNG.choice([0.,2.,6.,10.]);step['now']=now[0];result=obj._admit(text)
                elif op=='beat':step['kind']=RNG.choice(['hold','wait','missing']);result=obj.beat(step['kind'])
                else:result=obj._user_message(text)
                steps.append(step);expected.append(json.loads(json.dumps({'result':result,'state':snapshot()})))
        check('source_traces',{'op':'source','artifact':artifact,'settings':cfg,'agent':'Maya','lender':'' if trial%2 else 'Example','borrower':'Alex','steps':steps},expected)
async def lines():
    for trial in range(120):
        cfg={'enabled':trial%9!=0,'min_words':RNG.choice([2,4]),'machine_p':.6,'human_p':.3,'ask_window_s':2.,'quiet_ms':700.,'cap_s':8.}
        now=[100.];requests=[];futures=[];effects=[]
        async def ask(state):
            requests.append(state);future=asyncio.get_running_loop().create_future();futures.append(future);return await future
        class Queue:
            async def put(self,event):
                v=dc.asdict(event);v['kind']=event.kind;v.pop('playback_generation',None)
                effects.append(v)
        async def decide(ep,*args,**kw):effects.append({'breakthrough':ep.id})
        transport=NS(_cfg=NS(name_hint='Alex'),_last_speak_text='Hello this is Maya on a recorded line',_utterance_queue=Queue(),_floor=NS(decide=decide),playback_generation=lambda:0)
        obj=line.LineDetector(transport,LineDetection(**cfg),now=lambda:now[0],ask=ask)
        steps=[];expected=[]
        def snap():return {k:getattr(obj,k) for k in ['closed','bound','onset_before_audio','final_seen','final_t','last_text_t','asked_text','pending','last_p','last_latency_ms','asks','failures','first_audio_t','opening_ended_t','retire_at']}|{'episode':obj.episode.id if obj.episode else None,'verdict':(dc.asdict(obj.verdict)|{'kind':'line_verdict'}) if obj.verdict else None,'worker_active':bool(obj.worker and not obj.worker.done())}
        def quiet():
            if not obj.verdict:return 'no_verdict'
            if obj._release.is_set():return 'released'
            if now[0]>=obj.verdict.at+obj.cfg.cap_s:return 'cap'
            if obj.final_seen and now[0]-max(obj.last_text_t or 0.,obj.final_t or 0.)>=obj.cfg.quiet_ms/1000.:return 'quiet'
            return None
        for i in range(35):
            now[0]+=RNG.choice([0.,.1,.7,2.,10.]);op='start' if i==0 else 'end' if i==3 else RNG.choice(['observe','observe','complete','tick','release'])
            step={'op':op,'now':now[0]}
            if op=='start':obj.note_playout_start()
            elif op=='end':obj.note_playout_end()
            elif op=='observe':
                ep={'id':i+1,'onset':now[0]-.1,'text':RNG.choice(['Hello','you have reached Alex leave a message','I am driving please call later','Hello this is Maya on a recorded line','changed words for the first greeting']),'echo':trial%7==0,'possible_echo':trial%11==0};final=RNG.random()<.25;step.update(episode=ep,final=final,ours=transport._last_speak_text);await obj.observe(NS(**ep),final)
            elif op=='complete':
                p=RNG.choice([None,.1,.3,.5,.6,.9]);step['p']=p
                if futures:
                    future=futures.pop(0)
                    if not future.done():
                        if p is None:future.set_exception(RuntimeError('fixture failure'))
                        else:future.set_result(p)
            elif op=='tick':await obj.tick()
            else:obj.release_hold()
            for _ in range(5):await asyncio.sleep(0)
            request=requests.pop(0) if requests else None
            steps.append(step);expected.append(json.loads(json.dumps({'state':snap(),'request':request,'effects':effects.copy(),'quiet':quiet()})));effects.clear()
        check('line_traces',{'op':'line','config':cfg,'holder':'Alex','steps':steps},expected)
        await obj.close()
async def main():
    guards();picks();protocol();echo();await sources();await lines();artifacts()
    CHILD.stdin.close();assert CHILD.wait(timeout=10)==0
    out={'passed':True,'counts':COUNTS,'snapshot_counts':{'source':60*30,'line':120*35},'case_digest':hashlib.sha256(''.join(ROWS).encode()).hexdigest(),'scope':'Actual Python policy implementations; injected model answers and clocks; no external API calls'}
    (ROOT/'results/parity.json').write_text(json.dumps(out,indent=2)+'\n');print(out)
if __name__=='__main__':asyncio.run(main())
