"""Compare concurrent Rust audio owners with independent Python call state."""
import audioop
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import random

import webrtcvad
from vogon.gateway.transport.aec import AECProcessor
from vogon.gateway.transport.ambience.loader import build_mixer
from vogon_audio_runtime import AudioRuntime

ROOT = Path(__file__).resolve().parent


def scenario(seed, encoding, ambience):
    rng = random.Random(seed)
    original = AECProcessor()
    vad = webrtcvad.Vad(3)
    mixer = build_mixer(8000, enable_oneshots=True) if ambience else None
    runtime = AudioRuntime(json.dumps({'encoding':encoding,'ambience':ambience}))
    def call(op, **data): return json.loads(runtime.call(json.dumps({'op':op, **data})))
    pending = bytearray()
    rate_state = None
    digest = hashlib.sha256()
    frames = commands = 0
    try:
        for i in range(220):
            if i % 13 == 0:
                call('speaking',value=i%26==0)
                if mixer: mixer.set_speaking(i%26==0)
            if i % 31 == 0:
                call('start_typing',index=i%4)
                if mixer: mixer.start_typing(i%4)
            if i % 37 == 0:
                call('stop_typing')
                if mixer: mixer.stop_typing()
            if i % 41 == 0:
                call('oneshot',index=i%2)
                if mixer: mixer.trigger_oneshot(i%2)
            chunk = rng.randbytes(rng.randrange(1,1800))
            if encoding == 'linear16':
                even = chunk[:len(chunk)//2*2]
                if even:
                    pcm, rate_state = audioop.ratecv(even,2,1,24000,8000,rate_state)
                    pending.extend(audioop.lin2ulaw(pcm,2))
            else: pending.extend(chunk)
            expected=[]
            while len(pending)>=160:
                frame=bytes(pending[:160]); del pending[:160]
                if mixer: frame=audioop.lin2ulaw(mixer.next_frame(160,tts_frame=audioop.ulaw2lin(frame,2)),2)
                expected.append(list(frame))
            actual=call('tts',data=list(chunk))
            assert actual==expected, (seed,i,'tts')
            # Deliberately dispatch only some produced audio. Generation alone
            # must not feed the reference FIFO.
            for frame in expected[:1]:
                original.feed_reference(bytes(frame)); call('reference',data=frame)
            near=rng.randbytes((0,1,159,160,161,320)[i%6])
            clean=original.process(near)
            pcm=audioop.ulaw2lin(clean,2)
            try: speech=vad.is_speech(pcm,8000)
            except Exception: speech=False
            expected_frame={'mulaw':list(clean),'pcm':list(pcm),'rms':audioop.rms(pcm,2),'speech':speech}
            assert call('inbound',data=list(near))==expected_frame, (seed,i,'inbound')
            digest.update(clean); frames+=1
            if i%29==0:
                assert call('flush')==list(pending)
                pending.clear()
            if i%43==0:
                call('clear'); original.discard_reference(); pending.clear(); rate_state=None
            state=call('snapshot')
            assert state['pending_bytes']==len(pending)
            assert state['aec']['fifo']==len(original._ref_fifo)
            assert state['aec']['total_frames']==original._total_frames
            assert state['aec']['delay']==original._delay_frames
            assert state['aec']['slip']==original._alignment_slip
            commands+=1
        assert call('flush')==list(pending)
    finally:
        runtime.close(); runtime.close()
    try: call('snapshot')
    except RuntimeError: pass
    else: raise AssertionError('closed runtime accepted command')
    return {'seed':seed,'encoding':encoding,'ambience':ambience,'frames':frames,'state_checks':commands,
            'sha256':digest.hexdigest(),'passed':True}


def main():
    cases=[(i+125,encoding,ambience) for i,(encoding,ambience) in enumerate([
        ('mulaw',False),('mulaw',True),('linear16',False),('linear16',True)])]
    with ThreadPoolExecutor(max_workers=4) as executor:
        rows=list(executor.map(lambda case:scenario(*case),cases))
    for config in ({'queue_capacity':0},{'encoding':'invalid'},{'vad_mode':9}):
        try: AudioRuntime(json.dumps(config))
        except RuntimeError: pass
        else: raise AssertionError(f'invalid runtime initialized: {config}')
    runtime=AudioRuntime('{"aec":false}')
    responses=json.loads(runtime.drain_and_close(json.dumps([
        {'op':'tts','data':[255]*17},{'op':'snapshot'},{'op':'flush'},{'op':'snapshot'}])))
    assert responses[0]==[] and responses[1]['pending_bytes']==17
    assert responses[2]==[255]*17 and responses[3]['pending_bytes']==0
    runtime.close()
    doc={'passed':True,'concurrent_calls':4,'frames_byte_identical':sum(r['frames'] for r in rows),'cases':rows,
         'accepted_work_drained_before_close':True,
         'scope':'Four concurrent callers; serialized per-call workers; no concurrency performance claim. Startup failure, close, ordering and dispatch-reference separation checked.'}
    (ROOT/'results/runtime.json').write_text(json.dumps(doc,indent=2)+'\n')
    print(f'Audio runtime: 4 concurrent calls, {doc["frames_byte_identical"]} byte-identical inbound frames, all outbound chunks match',flush=True)


if __name__=='__main__': main()
