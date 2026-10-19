//! JSON-lines adapter for offline differential tests. No Python in this binary.
//!
//! `execute` routes one fixture request to native contract/configuration/playback
//! code or the barge adapter. `main` owns line-oriented input, response envelopes
//! and flushing. Domain state/types live in the workspace crates; this executable
//! supplies deterministic fixtures and serializes observations for the Python oracle.
/// Barge-in trace replay and snapshots, isolated from the general operation router.
mod barge;
use std::io::{self, BufRead, Write};
use serde_json::{json, Value};
use vogon_contracts::codec::{self, Error};
use vogon_configuration as config;
use vogon_playback::{delivery, sentences, ledger, queue, stream};

/// Run one offline playback fixture and return its native observations.
///
/// Covers sentence splitting, delivery estimates, queue ordering, horizon and
/// playout state, and a sentence-stream action loop with an injected clock.
/// The nested drain closure records media/mark/clear/cancel actions and
/// acknowledges them locally; it performs no provider or socket operations.
fn playback(v: &Value) -> Result<Value, Error> {
    let text=v["text"].as_str().unwrap_or("");
    match v["op"].as_str().unwrap_or("") {
        "sentences" => Ok(json!(sentences::split_sentences(text,v["min_chars"].as_i64().unwrap_or(25)))),
        "delivery" => {
            let report:delivery::DeliveryReport=serde_json::from_value(v["report"].clone()).map_err(|e|Error::type_error(e.to_string()))?;
            Ok(json!({"total":delivery::estimate_total_ms(text),"heard":delivery::estimate_heard_text(text,report.audio_ms,report.est_total_ms),
                "fraction":report.heard_fraction(),"report":report}))
        },
        "horizon" => {
            let entries=serde_json::from_value(v["entries"].clone()).map_err(|e|Error::type_error(e.to_string()))?;
            Ok(json!(ledger::Ledger {entries}.horizon(v["onset"].as_f64().unwrap_or(0.0))))
        },
        "queue" => {
            let entries=serde_json::from_value(v["entries"].clone()).map_err(|e|Error::type_error(e.to_string()))?;
            let mut q=queue::OutboundQueue::with_entries(entries,v["sequence"].as_u64().unwrap_or(0));
            let mut discarded=Vec::new();
            for raw in v["raws"].as_array().unwrap() { discarded.push(q.enqueue_twilio(raw.as_str().unwrap()).map_err(|e|Error::value_error(e.to_string()))?); }
            Ok(json!({"discarded":discarded,"entries":q.ordered()}))
        },
        "playout" => {
            let mut s=vogon_playback::state::PlayoutState::default();
            s.ledger.entries=serde_json::from_value(v["entries"].clone()).map_err(|e|Error::type_error(e.to_string()))?;
            s.pending_marks=serde_json::from_value(v["pending"].clone()).map_err(|e|Error::type_error(e.to_string()))?;
            s.last_audio_id=v["audio_id"].as_str().unwrap_or("").to_owned();s.text=text.to_owned();
            s.started_at=v["started_at"].as_f64().unwrap_or(0.0);
            s.sentences=serde_json::from_value(v["sentences"].clone()).map_err(|e|Error::type_error(e.to_string()))?;
            let mut states=Vec::new();
            for ev in v["events"].as_array().unwrap() {
                let now=ev["now"].as_f64().unwrap();
                match ev["action"].as_str().unwrap() {
                    "mark"=>{s.mark_echo(ev["name"].as_str().unwrap(),now);},
                    "clear"=>s.cleared(now), "close"=>s.close_ledger(now), _=>{},
                }
                states.push(s.snapshot(now));
            }
            Ok(json!(states))
        },
        "sentence_stream" => {
            let mut s=stream::SentenceStream::new(text.to_owned(),v["audio_id"].as_str().unwrap_or("a").to_owned(),v["lead_ms"].as_f64().unwrap_or(500.0));
            let mut now=10.0; let mut output=Vec::new(); let mut waits=Vec::new();
            let drain=|s:&mut stream::SentenceStream,now:&mut f64,output:&mut Vec<Value>,waits:&mut Vec<f64>| {
                while let Some(action)=s.next(*now) {
                    match action {
                        stream::Action::Wait {seconds}=>{waits.push(seconds);*now+=seconds;},
                        stream::Action::Media {data,..}=>{output.push(json!(["media",data]));s.acknowledge();},
                        stream::Action::Mark {name,..}=>{output.push(json!(["mark",name]));s.acknowledge();},
                        stream::Action::CancelProvider=>{output.push(json!(["cancel"]));s.acknowledge();},
                        stream::Action::Clear=>{output.push(json!(["clear"]));s.acknowledge();},
                    }
                }
            };
            for input in v["chunks"].as_array().unwrap() {
                now+=0.005;
                if input["yield"]==true {s.request_yield();}
                if input["cancel"]==true {s.cancel();drain(&mut s,&mut now,&mut output,&mut waits);break;}
                let data:Vec<u8>=serde_json::from_value(input["data"].clone()).map_err(|e|Error::type_error(e.to_string()))?;
                s.push(input["segment"].as_u64().unwrap() as usize,&data).map_err(Error::value_error)?;
                drain(&mut s,&mut now,&mut output,&mut waits);
                if s.yielded {break;}
            }
            s.finish().map_err(Error::value_error)?;drain(&mut s,&mut now,&mut output,&mut waits);
            Ok(json!({"output":output,"waits":waits,"chunks_sent":s.chunks_sent,"sentences_sent":s.sentences_sent,"heard_text":s.heard_text(),"sentences_total":s.sentences.len()}))
        },
        _=>Err(Error::value_error("unknown playback operation")),
    }
}

/// Route one JSON fixture by operation name to its owning native module.
///
/// Missing optional configuration objects use empty objects. Decode/domain
/// errors are returned for protocol wrapping; malformed trusted fixtures may
/// still panic in the operation-specific adapters.
fn execute(v: &Value) -> Result<Value, Error> {
    let empty = json!({});
    let object = |name: &str| v.get(name).unwrap_or(&empty);
    match v["op"].as_str().unwrap_or("") {
        "barge_gate" | "barge_semantics" | "barge_floor" => barge::run(v),
        "normalize" => codec::normalize(v["type"].as_str().unwrap_or(""), &v["data"]),
        "typed" => vogon_contracts::types::typed_roundtrip(v["type"].as_str().unwrap_or(""), &v["data"]),
        "shift" => codec::shift_event_times(v["events"].as_array().ok_or_else(|| Error::type_error("events"))?, v["offset"].as_f64().unwrap_or(0.0)).map(Value::Array),
        "settings" => config::settings(v["model"].as_str().unwrap_or("settings"), object("initial"), object("env"), object("dotenv")),
        "settings_typed" => {
            let value=match v["model"].as_str().unwrap_or("settings") {
                "gateway"=>serde_json::to_value(config::GatewayConfig::load(object("initial"),object("env"),object("dotenv"))?),
                "tenant"=>serde_json::to_value(config::TenantProfile::load(object("initial"),object("env"),object("dotenv"))?),
                _=>serde_json::to_value(config::GatewaySettings::load(object("initial"),object("env"),object("dotenv"))?),
            };
            value.map_err(|e|Error::value_error(e.to_string()))
        },
        "tenant" => config::tenant_profile(v["tenant"].as_str().unwrap_or(""), object("shared"), object("overrides")),
        "snapshot" => config::resolve_call_snapshot(&v["profile"], &v["call"]),
        "auth" => Ok(json!(config::auth_enabled(&v["data"]))),
        "path" => Ok(json!(config::resolved_front_config_path(v["path"].as_str().unwrap_or(""), std::path::Path::new(v["cwd"].as_str().unwrap_or("/"))))),
        "sentences" | "delivery" | "horizon" | "queue" | "sentence_stream" | "playout" => playback(v),
        _ => Err(Error::value_error("unknown operation")),
    }
}

/// Read one request per stdin line, execute it and flush one response to stdout.
///
/// Successful results use an `ok` envelope; parse/domain failures use `error`
/// and `message`. I/O failures terminate the process through the returned error.
/// Per-line flushing allows the Python harness to synchronously compare each result.
fn main() -> io::Result<()> {
    let mut out = io::BufWriter::new(io::stdout().lock());
    for line in io::stdin().lock().lines() {
        let result = serde_json::from_str::<Value>(&line?).map_err(|e| Error::value_error(e.to_string())).and_then(|v| execute(&v));
        let row = match result { Ok(value) => json!({"ok":value}), Err(error) => json!({"error":error.category,"message":error.message}) };
        writeln!(out, "{row}")?;
        out.flush()?;
    }
    Ok(())
}
