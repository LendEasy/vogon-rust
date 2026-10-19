//! JSON-lines oracle boundary for module-8 differential tests; no network calls.
use serde_json::{Value, json};
use std::io::{self, BufRead};
use vogon_front::{Error, Result, config, guard, jev, line, moves, source};
fn field<T: serde::de::DeserializeOwned>(v: &Value, key: &str) -> Result<T> {
    serde_json::from_value(v[key].clone()).map_err(|_| Error::new("invalid_validation_input"))
}
fn run(v: Value) -> Result<Value> {
    Ok(match v["op"].as_str().unwrap_or("") {
        "guard" => {
            json!({"trips":guard::check(v["text"].as_str().unwrap_or(""),&field(&v,"config")?),"normalized":guard::normalize(v["text"].as_str().unwrap_or("")),"sentences":guard::sentences(v["text"].as_str().unwrap_or(""))})
        }
        "pick" => serde_json::to_value(moves::pick(
            &field(&v, "read")?,
            &field(&v, "tiers")?,
            |_, p| p[0].clone(),
            v["has_spoken"].as_bool().unwrap_or(true),
            v["move_floor"].as_f64().unwrap_or(0.45),
        ))
        .unwrap(),
        "questions" => moves::questions(
            &v["criteria"],
            &v["riders"],
            v["question"].as_str().unwrap_or(""),
        )?,
        "parse" => serde_json::to_value(jev::DecisionResult::parse(&v["raw"], 1.25)?).unwrap(),
        "artifact" => serde_json::to_value(config::Artifact::from_value(&v["value"], 12)?).unwrap(),
        "echo" => json!(line::strip_echo(
            v["text"].as_str().unwrap(),
            v["ours"].as_str().unwrap(),
            v["min_run"].as_u64().unwrap_or(3) as usize
        )),
        "source" => {
            let mut s = source::State::new(
                field(&v, "artifact")?,
                field(&v, "settings")?,
                v["agent"].as_str().unwrap_or(""),
                v["lender"].as_str().unwrap_or(""),
                v["borrower"].as_str().unwrap_or(""),
                5,
            );
            let mut rows = vec![];
            for op in v["steps"].as_array().unwrap() {
                let mut result = Value::Null;
                match op["op"].as_str().unwrap() {
                    "borrower" => s.note_borrower(op["text"].as_str().unwrap()),
                    "agent" => s.note_agent(op["text"].as_str().unwrap(), false),
                    "played" => s.note_played(op["text"].as_str().unwrap()),
                    "read" => {
                        let generation = s.prime(op["text"].as_str().unwrap()).unwrap();
                        s.complete_read(generation, field(op, "read")?);
                        result = json!(s.result);
                    }
                    "admit" => {
                        result = json!(
                            s.admit(op["text"].as_str().unwrap(), op["now"].as_f64().unwrap())
                        )
                    }
                    "beat" => result = json!(s.beat(op["kind"].as_str().unwrap())),
                    "message" => result = json!(s.user_message(op["text"].as_str().unwrap())),
                    _ => return Err(Error::new("invalid_source_op")),
                }
                rows.push(json!({"result":result,"state":s.snapshot()}));
            }
            json!(rows)
        }
        "line" => {
            let mut detector = line::Detector::new(
                field(&v, "config")?,
                v["holder"].as_str().unwrap_or("").into(),
            );
            let mut rows = vec![];
            let mut token = 0;
            for op in v["steps"].as_array().unwrap() {
                let now = op["now"].as_f64().unwrap();
                match op["op"].as_str().unwrap() {
                    "start" => detector.playout_start(now),
                    "end" => detector.playout_end(now),
                    "observe" => detector.observe(
                        serde_json::from_value(op["episode"].clone()).ok(),
                        op["final"].as_bool().unwrap_or(false),
                        op["ours"].as_str().unwrap_or(""),
                        now,
                    ),
                    "complete" => {
                        detector.complete(token, op["p"].as_f64(), now);
                    }
                    "tick" => detector.tick(now),
                    "release" => detector.released = true,
                    _ => return Err(Error::new("invalid_line_op")),
                }
                let request = detector.begin_request(now);
                if let Some(r) = &request {
                    token = r.token;
                }
                let effects = detector
                    .drain_effects()
                    .into_iter()
                    .map(|e| match e {
                        line::Effect::Breakthrough { episode } => json!({"breakthrough":episode}),
                        line::Effect::Event(v) => v,
                    })
                    .collect::<Vec<_>>();
                rows.push(json!({"state":detector.snapshot(),"request":request.map(|r|r.state),"effects":effects,"quiet":detector.quiet_reason(now)}));
            }
            json!(rows)
        }
        _ => return Err(Error::new("unknown_operation")),
    })
}
fn main() {
    for line in io::stdin().lock().lines() {
        match line {
            Ok(line) => {
                let out = serde_json::from_str(&line)
                    .map_err(|_| Error::new("invalid_json"))
                    .and_then(run);
                println!(
                    "{}",
                    match out {
                        Ok(v) => json!({"ok":v}),
                        Err(e) => json!({"error":e.code}),
                    }
                );
            }
            Err(_) => break,
        }
    }
}
