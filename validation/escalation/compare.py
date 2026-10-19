"""Compare generated XML trees directly with the Python reference, without providers."""
import importlib.util
import json
from pathlib import Path
import subprocess
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[2]
PYTHON = ROOT.parent / 'voice-gateway/vogon/gateway_service/escalation/twiml.py'
spec = importlib.util.spec_from_file_location('reference_twiml', PYTHON)
reference = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reference)

def canonical(text):
    def node(e):
        return e.tag, sorted(e.attrib.items()), e.text or '', [node(c) for c in e]
    return node(ET.fromstring(text))

cases=[]; expected=[]
for text in ['', 'Hello', 'Account <42> & "terms"', 'Café — नमस्ते']:
    for audio in [None, 'https://example.invalid/voice?a=1&b=2']:
        for voice in ['Polly.Joanna-Neural', 'custom<&voice']:
            for op in ['hold','whisper','connect','voicemail','callback']:
                row=dict(op=op,text=text,room='esc_room<&>',music='https://example.invalid/music?a=1&b=2',action='https://example.invalid/key?a=1&b=2',voice=voice,audio=audio)
                cases.append(row)
                if op=='hold': result=reference.hold_twiml(text,row['room'],row['music'],voice,audio)
                elif op=='whisper': result=reference.whisper_twiml(text,row['action'],voice,audio)
                elif op=='connect': result=reference.agent_connect_twiml(row['room'])
                elif op=='voicemail': result=reference.voicemail_twiml(text,voice,audio)
                else: result=reference.callback_twiml(text,voice,audio)
                expected.append(result)
process=subprocess.run([str(ROOT/'target/debug/escalation-validation')],input=''.join(json.dumps(r)+'\n' for r in cases),text=True,capture_output=True,check=True)
actual=[json.loads(line)['xml'] for line in process.stdout.splitlines()]
assert len(actual)==len(expected)
for row,a,b in zip(cases,actual,expected): assert canonical(a)==canonical(b),row
print(json.dumps({'twiml_comparisons':len(cases),'passed':True,'external_calls':False}))
