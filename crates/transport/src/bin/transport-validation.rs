//! JSON-lines oracle boundary for clocks, callback routing and browser wire bytes.
use serde_json::{Value, json};
use std::io::{self, BufRead};
use vogon_clocks::call::CallClocks;
use vogon_transport::{
    engine::{Action, Engine, Mode},
    wire,
};
fn run(v: Value) -> Value {
    match v["op"].as_str().unwrap_or("engine") {
        "browser_audio" => match wire::browser_audio(
            v["id"].as_str().unwrap(),
            &serde_json::from_value::<Vec<u8>>(v["data"].clone()).unwrap(),
        ) {
            wire::Frame::Binary(bytes) => json!(bytes),
            _ => unreachable!(),
        },
        "clocks" => {
            let mut clock = CallClocks::new(
                serde_json::from_value(v["config"].clone()).unwrap(),
                v["epoch"].as_f64().unwrap(),
            );
            let mut rows = vec![];
            for e in v["events"].as_array().unwrap() {
                let events = match e["op"].as_str().unwrap() {
                    "tick" => clock.tick(e["at"].as_f64().unwrap()),
                    "activity" => {
                        clock.caller_activity();
                        vec![]
                    }
                    "pause" => {
                        clock.pause_silence();
                        vec![]
                    }
                    "resume" => {
                        clock.resume_silence();
                        vec![]
                    }
                    "start" => {
                        clock.playout_start(e["turn"].as_bool().unwrap());
                        vec![]
                    }
                    "sent" => {
                        clock.playout_sent(e["ms"].as_f64().unwrap());
                        vec![]
                    }
                    "end" => {
                        clock.playout_end(e["reset"].as_bool().unwrap());
                        vec![]
                    }
                    "ladder" => {
                        clock.select_ladder(e["name"].as_str().unwrap());
                        vec![]
                    }
                    "grace" => {
                        clock.grant_grace(e["seconds"].as_f64().unwrap());
                        vec![]
                    }
                    _ => panic!("unknown operation"),
                };
                rows.push(json!({"events":events,"state":clock}));
            }
            json!(rows)
        }
        _ => {
            let mode: Mode = serde_json::from_value(v["mode"].clone()).unwrap();
            let mut engine = Engine::new(
                mode,
                Default::default(),
                Default::default(),
                Default::default(),
                0.0,
            );
            let mut rows = vec![];
            for event in v["events"].as_array().unwrap() {
                engine.transcript(
                    serde_json::from_value(event["event"].clone()).unwrap(),
                    event["onset"].as_f64(),
                    event["at"].as_f64().unwrap(),
                );
                let wire: Vec<Value> = std::mem::take(&mut engine.actions)
                    .into_iter()
                    .filter_map(|a| {
                        if let Action::Browser(value) = a {
                            Some(value)
                        } else {
                            None
                        }
                    })
                    .collect();
                rows.push(json!({"events":std::mem::take(&mut engine.events),"wire":wire}));
            }
            json!(rows)
        }
    }
}
fn main() {
    for line in io::stdin().lock().lines() {
        let v = serde_json::from_str(&line.unwrap()).unwrap();
        println!("{}", run(v));
    }
}
