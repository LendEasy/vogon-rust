//! Bounded live smoke: synthetic TTS into Flux, including eager/resumed turn flow.
//! Reads the existing gateway dotenv; never prints keys or raw connection errors.
use serde_json::{Value, json};
use tokio::{
    sync::mpsc,
    time::{Duration, sleep, timeout},
};
use vogon_clocks::Clock;
use vogon_speech_providers::{
    Error, Result,
    client::{SttClient, SttOutput, TtsClient, TtsOutput},
    config::SpeechSettings,
    stt::{SttConfig, Transcript},
};

async fn synthesize(client: &TtsClient, text: &str) -> Result<(Vec<u8>, usize)> {
    let mut output = client.stream(text.into()).await?;
    let mut audio = vec![];
    let mut chunks = 0;
    loop {
        match timeout(Duration::from_secs(15), output.recv())
            .await
            .map_err(|_| Error::new("live_tts_timeout"))?
        {
            Some(TtsOutput::Audio { chunk }) => {
                audio.extend(chunk.data);
                chunks += 1;
            }
            Some(TtsOutput::Finished) => return Ok((audio, chunks)),
            Some(TtsOutput::Failed { error }) => return Err(error),
            _ => return Err(Error::new("live_tts_incomplete")),
        }
    }
}
fn drain(events: &mut mpsc::UnboundedReceiver<SttOutput>, rows: &mut Vec<Value>) -> bool {
    let mut eager = false;
    while let Ok(event) = events.try_recv() {
        if let SttOutput::Transcript {
            event: Transcript::Eager { .. },
            ..
        } = &event
        {
            eager = true;
        }
        rows.push(serde_json::to_value(event).unwrap());
    }
    eager
}
async fn transcribe(
    settings: &SpeechSettings,
    first: &[u8],
    continuation: Option<&[u8]>,
    clock: Clock,
) -> Result<Value> {
    let config = if continuation.is_some() {
        SttConfig {
            eager_eot_threshold: Some(0.3),
            eot_threshold: 1.0,
            eot_timeout_ms: 3000,
            ..settings.phone.clone()
        }
    } else {
        settings.phone.clone()
    };
    let query = config.query();
    let (mut stt, mut events) = SttClient::open(config, settings.key.clone(), clock).await?;
    let mut rows = vec![];
    let outcome=async{
        for frame in first.chunks(160){stt.input.send_audio(frame.to_vec()).await?;sleep(Duration::from_millis(20)).await;drain(&mut events,&mut rows);}
        if let Some(tail)=continuation{
            let mut got_eager=rows.iter().any(|r|r["type"]=="transcript"&&r["event"]["kind"]=="eager");
            for _ in 0..125{
                if got_eager{break;}
                stt.input.send_audio(vec![255;160]).await?;sleep(Duration::from_millis(20)).await;got_eager|=drain(&mut events,&mut rows);
            }
            if !got_eager{return Err(Error::new("live_eager_not_observed"));}
            for frame in tail.chunks(160){stt.input.send_audio(frame.to_vec()).await?;sleep(Duration::from_millis(20)).await;drain(&mut events,&mut rows);}
        }
        for _ in 0..200{
            stt.input.send_audio(vec![255;160]).await?;sleep(Duration::from_millis(20)).await;drain(&mut events,&mut rows);
            if rows.iter().any(|r|r["type"]=="transcript"&&r["event"]["kind"]=="final"){break;}
        }
        let callbacks:Vec<_>=rows.iter().filter(|r|r["type"]=="transcript").collect();
        let finals=callbacks.iter().filter(|r|r["event"]["kind"]=="final").count();
        let resumed=callbacks.iter().any(|r|r["event"]["kind"]=="resumed");
        if finals==0{return Err(Error::new("live_final_not_observed"));}
        if continuation.is_some()&&!resumed{return Err(Error::new("live_resume_not_observed"));}
        if rows.iter().any(|r|r["type"]=="error"){return Err(Error::new("live_stt_reported_error"));}
        Ok(json!({"passed":true,"query":query,"events":rows,"finals":finals,"resumed":resumed,"state":stt.snapshot().await?}))
    }.await;
    stt.close().await;
    outcome
}
async fn live_listener(path: &str, audio: &[u8], clock: Clock) -> Result<Value> {
    use vogon_speech_providers::{live::LiveConfig, live_client::LiveClient};
    let (config, key) = LiveConfig::load(std::path::Path::new(path))?;
    let (mut client, mut events) = LiveClient::open(config, key, clock).await?;
    let result = async {
        for _ in 0..120 {
            while let Ok(event) = events.try_recv() {
                if let SttOutput::Error { error } = event {
                    return Err(error);
                }
            }
            if client.input.is_ready() {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
        if !client.input.is_ready() {
            return Err(Error::new("live_listener_not_ready"));
        }
        let mut rows = vec![];
        for frame in audio.chunks(160) {
            client.input.send_audio(frame.to_vec()).await?;
            sleep(Duration::from_millis(20)).await;
            drain(&mut events, &mut rows);
        }
        for _ in 0..200 {
            client.input.send_audio(vec![255; 160]).await?;
            sleep(Duration::from_millis(20)).await;
            drain(&mut events, &mut rows);
            if rows
                .iter()
                .any(|r| r["type"] == "transcript" && r["event"]["kind"] == "final")
            {
                break;
            }
        }
        if !rows
            .iter()
            .any(|r| r["type"] == "transcript" && r["event"]["kind"] == "final")
        {
            return Err(Error::new("live_listener_final_missing"));
        }
        if rows.iter().any(|r| r["type"] == "error") {
            return Err(Error::new("live_listener_error"));
        }
        client
            .input
            .note_agent_said("Thank you. This completes the synthetic streaming check.".into())
            .await;
        Ok(rows)
    }
    .await;
    client.close().await;
    match result {
        Ok(mut rows) => {
            drain(&mut events, &mut rows);
            Ok(
                json!({"passed":true,"events":rows,"scope":"Live GPT listener with synthetic Deepgram TTS; model audio discarded"}),
            )
        }
        Err(error) => Err(error),
    }
}
#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let env_path = args.get(1).ok_or_else(|| Error::new("expected_env_path"))?;
    let out = args
        .get(2)
        .ok_or_else(|| Error::new("expected_report_path"))?;
    let settings = SpeechSettings::load(std::path::Path::new(env_path))?;
    let clock = Clock::default();
    let mut tts = TtsClient::open(
        vogon_speech_providers::tts::TtsConfig {
            encoding: "mulaw".into(),
            sample_rate: 8000,
            ..settings.tts.clone()
        },
        settings.key.clone(),
        clock.clone(),
    )
    .await?;
    let result=async{
        let(first,chunks1)=synthesize(&tts,"I can pay the bill on Friday.").await?;
        if args.get(3).is_some_and(|v|v=="--live-listener"){return live_listener(env_path,&first,clock.clone()).await;}
        let(second,chunks2)=synthesize(&tts,"Actually, I need to pay on Monday instead.").await?;
        if chunks1<2||chunks2<2{return Err(Error::new("tts_did_not_stream_multiple_chunks"));}
        let baseline=transcribe(&settings,&first,None,clock.clone()).await?;
        let resumed=transcribe(&settings,&first,Some(&second),clock.clone()).await?;
        Ok(json!({"passed":true,"tts_chunks":[chunks1,chunks2],"tts_bytes":[first.len(),second.len()],"baseline":baseline,"resume_scenario":resumed,
            "scope":"Live Deepgram WebSocket TTS and Flux with synthetic test sentences only; no performance benchmark"}))
    }.await;
    tts.close().await;
    match result {
        Ok(report) => {
            std::fs::write(out, serde_json::to_string_pretty(&report).unwrap())
                .map_err(|_| Error::new("report_write_failed"))?;
            println!("Live speech-provider scenario: passed");
            Ok(())
        }
        Err(error) => {
            std::fs::write(out, json!({"passed":false,"error":error}).to_string()).ok();
            Err(error)
        }
    }
}
