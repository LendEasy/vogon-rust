"""Copy protocol prompts/lexicons from Python, never its decision algorithms."""
import ast
import hashlib
import json
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
REF=ROOT.parent/'voice-gateway/vogon/gateway/transport'
def assignments(path):
    tree=ast.parse(path.read_text())
    return tree,{n.targets[0].id:n.value for n in tree.body if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name)}
guard,values=assignments(REF/'front_guard.py')
out={name.lower():ast.literal_eval(values[name]) for name in ['COMMITMENT','ACTION_CLAIMS','SPELLED_NUMBERS','CALENDAR']}
_,moves=assignments(REF/'front_moves.py')
out['move_question']=ast.literal_eval(moves['DEFAULT_MOVE_QUESTION'])
_,line=assignments(REF/'line_detector.py')
call=line['RECORDING_QUESTION'];kw={x.arg:ast.literal_eval(x.value) for x in call.keywords}
out['recording_question']={'type':'noul','instructions':ast.literal_eval(call.args[0]),'criteria':{'true':kw['yes'],'false':kw['no']}}
fn=next(n for n in ast.parse((REF/'barge_semantics.py').read_text()).body if isinstance(n,ast.FunctionDef) and n.name=='classify_floor')
out['floor_system']=ast.literal_eval(next(n.value for n in fn.body if isinstance(n,ast.Assign) and n.targets[0].id=='system'))
(ROOT/'crates/front/src/constants.json').write_text(json.dumps(out,indent=2,ensure_ascii=False)+'\n')
(ROOT/'validation/front/results/constants-sources.json').write_text(json.dumps({str(p.relative_to(ROOT.parent)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [REF/'front_guard.py',REF/'front_moves.py',REF/'line_detector.py',REF/'barge_semantics.py']},indent=2)+'\n')
