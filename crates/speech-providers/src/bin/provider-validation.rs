//! JSON-lines adapter for deterministic comparisons against the Python providers.
use serde_json::{Value, json};
use std::io::{self, BufRead};
use vogon_speech_providers::{
    stt::{SttConfig, SttState, TurnInfo, merge_resumed_transcripts},
    tts::{TtsState, strip_wav_header},
};
fn run(v: Value) -> Value {
    match v["op"].as_str().unwrap_or("stt") {
        "live" => {
            let mut state = vogon_speech_providers::live::LiveState::new(
                serde_json::from_value(v["config"].clone()).unwrap(),
                v["epoch"].as_f64().unwrap(),
            );
            let mut rows = vec![];
            for e in v["events"].as_array().unwrap() {
                let now = e["at"].as_f64().unwrap();
                let emitted = if e["op"] == "tick" {
                    state.tick(now)
                } else {
                    state.message(&e["message"], now)
                };
                rows.push(json!({"emitted":emitted,"onset":state.onset_monotonic(),"quality":state.quality(0,None)}));
            }
            json!(rows)
        }
        "merge" => json!(merge_resumed_transcripts(
            v["prefix"].as_str().unwrap(),
            v["text"].as_str().unwrap()
        )),
        "strip" => json!(strip_wav_header(
            &serde_json::from_value::<Vec<u8>>(v["data"].clone()).unwrap()
        )),
        "tts" => {
            let mut state = TtsState::new(
                v["segments"].as_u64().unwrap() as usize,
                v["epoch"].as_f64().unwrap(),
            );
            let mut rows = vec![];
            for event in v["events"].as_array().unwrap() {
                let chunk = if event["data"].is_array() {
                    state.audio(
                        &serde_json::from_value::<Vec<u8>>(event["data"].clone()).unwrap(),
                        event["at"].as_f64().unwrap(),
                    )
                } else {
                    state.control(event["type"].as_str().unwrap());
                    None
                };
                rows.push(json!({"chunk":chunk,"done":state.done(),"flushed":state.flushed}));
            }
            json!(rows)
        }
        _ => {
            let config: SttConfig = serde_json::from_value(v["config"].clone()).unwrap();
            let query = config.query();
            let mut state = SttState::new(config, v["epoch"].as_f64().unwrap());
            let mut rows = vec![];
            for event in v["events"].as_array().unwrap() {
                let at = event["at"].as_f64().unwrap();
                let emitted = match event["op"].as_str().unwrap_or("message") {
                    "tick" => state.tick(at),
                    "close" => state.closed(at, event["requested"].as_bool().unwrap_or(false)),
                    _ => state.message(
                        serde_json::from_value::<TurnInfo>(event["message"].clone()).unwrap(),
                        at,
                    ),
                };
                rows.push(json!({"emitted":emitted,"state":state.snapshot()}));
            }
            json!({"query":query,"rows":rows})
        }
    }
}
fn main() {
    for line in io::stdin().lock().lines() {
        let line = line.unwrap();
        let value = serde_json::from_str(&line).unwrap();
        println!("{}", run(value));
    }
}
