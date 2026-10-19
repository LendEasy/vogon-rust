//! JSON-lines differential boundary for the Python recorder; no live provider calls.
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use std::io::{self, BufRead};
use vogon_artifacts::recorder::Recorder;
fn main() {
    for line in io::stdin().lock().lines() {
        let line = line.unwrap();
        let v: Value = serde_json::from_str(&line).unwrap();
        let mut r = Recorder::default();
        let mut times = vec![];
        let mut clears = vec![];
        for action in v["actions"].as_array().unwrap() {
            let at = action["at"].as_f64().unwrap_or(0.);
            match action["op"].as_str().unwrap() {
                "epoch" => r.epoch(at),
                "caller" | "agent" => r.add(
                    action["op"] == "caller",
                    at,
                    &STANDARD.decode(action["data"].as_str().unwrap()).unwrap(),
                ),
                "timestamp" => times.push(r.caller_timestamp(&action["timestamp"], at)),
                "clear" => clears.push(r.clear(at)),
                "event" => r.event(
                    at,
                    action["name"].as_str().unwrap(),
                    action["detail"].clone(),
                ),
                _ => panic!("invalid action"),
            }
        }
        let wav = r.wav().unwrap().map(|b| STANDARD.encode(b));
        println!(
            "{}",
            json!({"wav":wav,"times":times,"clears":clears,"t0":r.t0,"events":r.event_json(json!({}))})
        );
    }
}
