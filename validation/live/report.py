"""Summarize completed live runs without merging failures into passes."""
import json
from pathlib import Path

ROOT=Path(__file__).resolve().parent
NAMES={1:'Startup/authentication',2:'Text baseline',3:'Spoken round trip',4:'Five-turn conversation',5:'Streaming delivery',6:'Content correctness',7:'Long utterance/pauses',8:'Silence ladder (phone)',9:'Matching eager/final',10:'Changed final transcript',11:'Speech resumed after eager',12:'Late eager response',13:'Duplicate provider events',14:'Input during blocked application',15:'Barge-in',16:'Late audio after cancellation',17:'Parallel calls and timer isolation',18:'Stalled peer isolation',19:'Phone media/marks',20:'Echo and double-talk',21:'Provider/application faults',22:'Disconnect/SIGTERM cleanup',23:'Admission/malformed input',24:'Repeated lifecycle'}

def main():
    full=json.loads((ROOT/'results/full-02/results.json').read_text())
    natural=json.loads((ROOT/'results/deployment-eot/results.json').read_text())
    phone_path=ROOT/'results/phone-final/results.json'
    if phone_path.exists():
        replacements=json.loads(phone_path.read_text())['results']
        replaced_names={r['name'] for r in replacements}
        full['results']=[r for r in full['results'] if r['name'] not in replaced_names]+replacements
    lines=['# Live generated-audio results','',
        'Gateway uses native Rust service/transport/orchestration and real Deepgram STT/TTS. The local echo Turn service performs no model calls. Phone media is a local Twilio-protocol simulation, not a telephone-network call. GPT Live was not run.','',
        'The generated-audio conversation path works. Full live acceptance remains incomplete: the initial acoustic run produced a false caller turn, the deployment EOT configuration did not expose the required continuation window, and browser silence timers did not fire. The matrix below describes the latest tests, not an erasure of those findings.','',
        '## Planned suite after integration fixes','',
        '| # | Scenario | Result |','|---|---|---|']
    statuses={}
    for number,name in NAMES.items():
        rows=[r for r in full['results'] if number in r['tests']]
        result='NOT RUN' if not rows else 'PASS' if all(r['status']=='passed' for r in rows) else 'FAIL'
        statuses[number]=result
        errors='; '.join(r.get('error','') for r in rows if r['status']!='passed')
        lines.append(f'| {number} | {name} | {result}{": "+errors if errors else ""} |')
    lines+=['',f'Planned scenarios: {sum(v=="PASS" for v in statuses.values())} pass, {sum(v=="FAIL" for v in statuses.values())} fail, {sum(v=="NOT RUN" for v in statuses.values())} not run. Scenario 17 requires both parallel conversation and live timer-isolation checks to pass.','',
        'The phone rerun replaces matching phone scenario results from full-02 and adds phone worker concurrency/cleanup checks; all original evidence remains intact. Its double-talk check requires caller transcript WER <= 0.25. The earlier full-02 check only required the keyword Monday and accepted an agent-speech prefix; that weaker result is not clean-transcript acceptance.', '', 'The controlled eager/resumed run uses final EOT confidence 1.0 and a 3000 ms timeout to expose continuation windows. It does not measure deployment-default latency.','',
        'Speech correctness uses normalized word error rate (usually <= 0.25), plus scenario-specific content/state assertions. It is not exact transcription: for example, the full-02 multi-turn return-audio recognizer heard “gate” instead of “date” (WER 0.111). Input recognition and captured-output recognition are recorded separately.','',
        '## Existing dotenv EOT settings','',
        '| Scenario | Result | Detail |','|---|---|---|']
    for r in natural['results']:
        detail=r.get('error','')
        if r['name']=='resumed-final' and r['status']!='passed':
            detail+='; captured trace has two finalized utterances and no TurnResumed. The continuation window was not exercised with this configuration.'
        if r['name']=='basic-voice' and r['status']=='passed':
            d=r['detail'];detail=f'First returned audio {d["speech_end_to_audio_ms"]:.0f} ms after caller speech ended; output WER {d["output_wer"]:.3f}; {d["chunks"]} audio chunks.'
        lines.append(f'| {r["name"]} | {r["status"].upper()} | {detail} |')
    events=json.loads((ROOT/'results/deployment-eot/provider-events.json').read_text())
    queries=[r['connection_query'] for r in events if r.get('kind')=='stt' and 'connection_query' in r]
    if queries:lines+=['',f'Observed STT query: `{json.dumps(queries[0],sort_keys=True)}`.']
    acoustic=(ROOT/'results/phone-final/acoustic.json') if (ROOT/'results/phone-final/acoustic.json').exists() else ROOT/'results/full-02/acoustic.json'
    if acoustic.exists():
        lines+=['','## Echo injection','', '| Delay | Gain | Double-talk | Result | Input/STT window energy ratio |','|---|---|---|---|---|']
        for r in json.loads(acoustic.read_text()):
            detail=r.get('error') or (f'caller WER {r["input_wer"]:.3f}' if 'input_wer' in r else 'caller recognized' if r['double_talk'] else 'no false caller turn')
            lines.append(f'| {r["lag_ms"]:.0f} ms | {r["gain"]} | {r["double_talk"]} | {r["status"]}: {detail} | {r["window_energy_reduction_db"]:.2f} dB |')
        lines+=['','Energy ratios are window diagnostics, not calibrated ERLE. Pure echo must not create a new caller turn; double-talk must preserve the new caller content. No failed acoustic case is converted into an expected pass.', '',
            'The initial full-01 run failed pure echo at 60 ms / 0.2 gain. Later runs varied despite fixed lag/gain: generated provider output and scheduling are live. No DSP behavior was changed during this task, so later passes do not establish that the original echo leakage is resolved.']
    lines+=['','## Defects exposed and corrected','',
        '- Provider-sequenced STT replay previously acquired new transport identities and could commit duplicate final turns. A connection-local sequence high-water mark now rejects replay before transcript callbacks; later genuinely repeated words remain valid.',
        '- The host previously passed wall-clock epoch into the Turn client while transport events were already call-relative, producing negative event times. The host now uses zero offset for its per-call Clock. Native and live checks require nonnegative call-relative times.',
        '- Browser control notifications were reaching phone media sockets. The transport now suppresses them for phone mode; a strict live caller and native socket test accept only the supported events. See [Twilio message types](https://www.twilio.com/docs/voice/media-streams/websocket-messages#send-websocket-messages-to-twilio).',
        '- The test oracle initially treated “fifty dollars” and “$50” as different content. Numeric/currency formatting now canonicalizes without removing the value, unit, weekday or names; separate oracle tests protect these distinctions.','',
        '## Remaining limits','',
        '- Browser silence does not share the phone playout-receipt lifecycle; the initial browser silence attempt failed to produce the requested ladder events. Engine::begin returns before phone clock transitions in browser mode, and phone marks close that lifecycle. Define browser audible-completion semantics and wire its clock transitions before accepting browser silence behavior. Phone ladder acceptance is reported separately above.',
        '- Eager/resumed events depend on provider decisions and configuration. Missing prerequisites are failures in these reports, not silently skipped successes.',
        '- Six repeated browser calls, four repeated phone calls and up to four concurrent calls exercise cleanup/isolation, not long-duration leak freedom or production capacity.',
        '- Echo tests disable the optional remote semantic judge, exposing DSP/floor behavior directly. They do not establish acceptance of every optional classifier configuration.',
        '- Front model calls, transfers, recording generation and deployment packaging remain outside this run. Test WAV capture belongs to the harness, not module 12.','',
        '## Offline regression after fixes','',
        '- Service: 25 native tests, 228 direct comparisons and 11 Python reference tests passed; one Python reference skipped because fakeredis is absent. Native Redis tests use a private real Redis process.',
        '- Orchestration/transport/providers: 134 native tests passed; 34 Python reference tests passed with two existing expected failures. Two GPT Live-specific tests were filtered out of this gate.',
        '- Rust formatting, scoped Clippy and rustdoc gates passed. Three transcript-oracle unit tests passed.','',
        '## Evidence','',
        '- [Corrected suite](results/full-02/results.json), [original corrected-run echo detail](results/full-02/acoustic.json), [source hashes](results/full-02/source-hashes.json).',
        '- [Phone rerun after protocol fix](results/phone-final/results.json), [strict acoustic checks](results/phone-final/acoustic.json), [all Rust source and tested binary hashes](results/phone-final/source-hashes.json), including four concurrent native audio workers and repeated phone-worker cleanup.',
        '- [Existing dotenv settings run](results/deployment-eot/results.json).',
        '- Listen to a complete sample: [generated caller](results/deployment-eot/basic/caller.wav), [captured echoed reply](results/deployment-eot/basic/basic-reply-1.wav). The 694 ms first-audio result is one observation, not a latency percentile or benchmark.',
        '- [Initial run, including original failures](results/full-01/results.json).',
        '- [Native service regression](../service/results/manifest.json), [orchestration/provider regression](../orchestration/results/manifest.json).',
        '- Per-call folders hold generated caller and captured response WAVs, client frames and input/output timing. Large audio files remain local and ignored by Git.','']
    (ROOT/'REPORT.md').write_text('\n'.join(lines))
    (ROOT/'results/summary.json').write_text(json.dumps({'scenarios':statuses,'latest_suite_all_passed':all(v=='PASS' for v in statuses.values()),'deployment_eot_all_passed':all(r['status']=='passed' for r in natural['results']),'live_acceptance_complete':False,'remaining_findings':['Echo reliability not established; initial false caller turn and earlier contaminated double-talk transcript','Deployment EOT continuation window not exercised','Browser silence lifecycle incomplete']},indent=2)+'\n')
    print('Report written:',ROOT/'REPORT.md')

if __name__=='__main__':main()
