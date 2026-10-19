"""Compare new native transport/audio commands to the actual Python DSP and gate."""
import audioop
import dataclasses
import json
from pathlib import Path
import random
from vogon.gateway.transport.aec import AECProcessor
from vogon.gateway.transport.barge_gate import BargeGate, BargeGateConfig, make_confirm_frame
from vogon.gateway.transport.ambience.loader import build_mixer
from vogon_audio_runtime import AudioRuntime

def main():
    rng=random.Random(428);frames=0
    for aec_enabled in [False,True]:
        for mode in ['off','local_vad']:
            cfg=BargeGateConfig(confirm=mode,adaptive_enabled=True)
            gate=BargeGate(cfg,confirm_frame=make_confirm_frame(mode,3))
            aec=AECProcessor() if aec_enabled else None
            native=AudioRuntime(json.dumps({'aec':aec_enabled,'barge_gate':dataclasses.asdict(cfg),'ambience':True}))
            mixer=build_mixer(8000,enable_oneshots=True)
            def call(op,**kw):return json.loads(native.call(json.dumps(dict(op=op,**kw))))
            try:
                for index in range(240):
                    now=10.+index*.02;active=index%50<35;playing=index%50<20
                    if index%50==0:gate.on_tts_start(now);call('gate_start',now=now)
                    if index%31==0:
                        if aec:aec.discard_reference()
                        call('clear')
                    reference=rng.randbytes(160)
                    if aec:aec.feed_reference(reference)
                    call('reference',data=list(reference))
                    raw=rng.randbytes(rng.choice([80,160,320]));raw_rms=audioop.rms(audioop.ulaw2lin(raw,2),2)
                    clean=aec.process(raw) if active and aec else None if active else raw
                    clean_rms=audioop.rms(audioop.ulaw2lin(clean,2),2) if clean is not None else raw_rms
                    verdict=None
                    if not active:gate.reset()
                    elif clean is not None:
                        v=gate.on_frame(now=now,clean_rms=clean_rms,tts_playing=playing,frame=clean)
                        verdict=dataclasses.asdict(v);verdict['forward_frames']=[list(x) for x in v.forward_frames];verdict['events']=[[name,detail] for name,detail in v.events]
                    expected=dict(stt=list(clean) if clean is not None else None,raw_rms=raw_rms,clean_rms=clean_rms,verdict=verdict,threshold=gate.threshold)
                    assert call('phone_input',data=list(raw),now=now,tts_playing=playing,active_audio=active)==expected,(aec_enabled,mode,index)
                    if index%20==0:call('start_typing',index=index//20);mixer.start_typing(index//20)
                    if index%27==0:call('stop_typing');mixer.stop_typing()
                    expected_audio=audioop.lin2ulaw(mixer.next_frame(160),2)
                    assert bytes(call('ambience_frame'))==expected_audio
                    frames+=1
            finally:native.close()
    result=dict(passed=True,phone_input_frames=frames,ambience_gap_frames=frames,confirm_modes=['off','local_vad'],aec_modes=[False,True])
    (Path(__file__).parent/'results/audio.json').write_text(json.dumps(result,indent=2)+'\n');print(result)
main()
