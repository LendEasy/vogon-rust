//! JSON-lines adapter for direct Python/Rust record and Twilio SDK comparisons.
//! Test inputs only; never load deployment settings or contact external providers.
use serde_json::{Value, json};
use std::io::{self, BufRead};
use vogon_service::{
    record::{Record, State},
    twilio,
};
fn evaluate(v: Value) -> Value {
    match v["op"].as_str().unwrap() {
        "record" => {
            let mut r = Record::decode(&v["record"].to_string()).unwrap();
            if let Some(steps) = v["steps"].as_array() {
                for step in steps {
                    let state: State = serde_json::from_value(step["state"].clone()).unwrap();
                    r.transition(state, step["now"].as_f64().unwrap());
                }
            }
            json!(r)
        }
        "signature" => {
            let params: Vec<(String, String)> =
                serde_json::from_value(v["params"].clone()).unwrap();
            let token = v["token"].as_str().unwrap();
            let url = v["url"].as_str().unwrap();
            json!({"signature":twilio::signature(token,url,&params),"valid":twilio::signed(token,url,&params,v["header"].as_str().unwrap_or(""))})
        }
        "twiml" => json!(twilio::twiml(
            v["base"].as_str().unwrap(),
            v["valid"].as_bool().unwrap()
        )),
        _ => panic!("unsupported validation operation"),
    }
}
fn main() {
    for line in io::stdin().lock().lines() {
        println!(
            "{}",
            evaluate(serde_json::from_str(&line.unwrap()).unwrap())
        );
    }
}
