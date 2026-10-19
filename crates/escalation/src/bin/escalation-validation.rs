//! JSON-lines boundary for direct Python TwiML comparisons; no network operations.
use std::io::{self, BufRead};
use vogon_escalation::twiml;
fn main() {
    for line in io::stdin().lock().lines() {
        let v: serde_json::Value = serde_json::from_str(&line.unwrap()).unwrap();
        let s = |k: &str| v[k].as_str().unwrap_or("");
        let audio = v["audio"].as_str();
        let voice = v["voice"].as_str().unwrap_or(twiml::VOICE);
        let xml = match s("op") {
            "hold" => twiml::hold(s("text"), s("room"), s("music"), voice, audio),
            "whisper" => twiml::whisper(s("text"), s("action"), voice, audio),
            "connect" => twiml::connect(s("room")),
            _ => twiml::goodbye(s("text"), voice, audio),
        };
        println!("{}", serde_json::json!({"xml":xml}));
    }
}
