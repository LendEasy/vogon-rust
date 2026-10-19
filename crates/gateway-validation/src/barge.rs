//! JSON fixture adapter for offline barge-in differential validation.
//!
//! This file owns trace replay and observable snapshots, not policy algorithms.
//! Gate, semantic and floor decisions execute inside `vogon_barge_in`; Python
//! runs separately as the oracle. Provider outcomes, clocks and VAD decisions
//! are fixture inputs, so this adapter makes no network or retained-library calls.
//! Inputs are trusted test fixtures: malformed required scalar/index fields may
//! panic; returned errors cover decoding and unknown operation/action names.

use serde_json::{json, Value};
use vogon_contracts::codec::Error;
use vogon_barge_in::{gate, semantics, floor::{self, ClassifierResult, AdmissionResult}};
/// Decode a typed fixture value and translate serde failures into the harness error format.
fn decode<T:serde::de::DeserializeOwned>(v:Value)->Result<T,Error> {serde_json::from_value(v).map_err(|e|Error::value_error(e.to_string()))}
/// Clone a named configuration object, supplying an empty object when the field is absent.
fn obj(v:&Value,k:&str)->Value {v.get(k).cloned().unwrap_or(json!({}))}
/// Read a floating-point fixture field or use the operation-specific fallback.
fn num(v:&Value,k:&str,default:f64)->f64 {v[k].as_f64().unwrap_or(default)}
/// Read a Boolean fixture field; absent/non-Boolean values default to false.
fn boolean(v:&Value,k:&str)->bool {v[k].as_bool().unwrap_or(false)}
/// Borrow a string fixture field, using an empty string when it is absent or not text.
fn string<'a>(v:&'a Value,k:&str)->&'a str {v[k].as_str().unwrap_or("")}
/// Copy an optional string fixture field; missing/null/non-string values become None.
fn optional(v:&Value,k:&str)->Option<String> {v[k].as_str().map(str::to_owned)}

/// Dispatch a semantic query, gate trace or floor trace into the native crate.
///
/// Return only observable data: verdicts, state, events, buffered bytes and
/// callback counts. The caller wraps this value in the JSON-lines protocol.
/// VAD flags are injected, but the native gate decides when to invoke them.
pub fn run(v:&Value)->Result<Value,Error> {
    match string(v,"op") {
        "barge_semantics"=> {
            let p:semantics::Policy=decode(obj(v,"policy"))?;let text=string(v,"text");
            let h=semantics::heuristic(text,&p);
            Ok(json!({"normalized":semantics::normalize(text),"heuristic":h,
                "resolved":semantics::resolve_final(h,v["llm"].as_str()),
                "swallow":semantics::should_swallow_final(text,&p),
                "swallow_absorbed":semantics::should_swallow_absorbed_final(string(v,"absorb"),text,&p),
                "echo":semantics::is_echo_of(text,&p.playout_text,v["min_words"].as_u64().unwrap_or(4) as usize),
                "absorb":semantics::ABSORB.contains(&h)}))
        },
        "barge_gate"=> {
            let available=v["vad_available"].as_bool().unwrap_or(true);
            let mut g=gate::Gate::new(decode(obj(v,"config"))?,available);let mut out=Vec::new();let mut calls=0;
            for a in v["actions"].as_array().ok_or_else(||Error::value_error("actions"))? {
                let mut verdict=None;
                match string(a,"action") {
                    "reset"=>g.reset(),"start"=>g.on_tts_start(num(a,"now",0.0)),
                    "frame"=>{let frame:Vec<u8>=decode(a["frame"].clone())?;
                        verdict=Some(g.on_frame(num(a,"now",0.0),a["rms"].as_i64().unwrap(),boolean(a,"playing"),&frame,|_|{
                            if available {calls+=1;boolean(a,"vad")} else {false}
                        }));},
                    _=>return Err(Error::value_error("gate action")),
                }
                out.push(json!({"gate":g,"threshold":g.threshold(),"in_progress":g.in_progress(),"verdict":verdict,"vad_calls":calls}));
            }
            Ok(json!(out))
        },
        "barge_floor"=>floor_trace(v),
        _=>Err(Error::value_error("barge operation")),
    }
}
/// Replay ordered host events through a fresh controller with explicit time.
///
/// Own final/admission continuation tokens, collect classifier requests, and
/// record state plus ordered effects after every event. Unless `pump` is false,
/// advance dispatch and eligible continuations to model an executor turn; the
/// fixture can defer pumping to exercise release/resumption races. No provider
/// work or actual transport side effects are executed.
fn floor_trace(v:&Value)->Result<Value,Error> {
    let mut c=floor::Controller::new(decode(obj(v,"config"))?,decode(obj(v,"transport"))?,num(v,"now",100.0));
    let mut finals:Vec<Option<floor::FinalWait>>=vec![];
    let mut admissions:Vec<(floor::Admission,AdmissionResult)>=vec![];
    let mut requests:Vec<floor::ClassifierRequest>=vec![];let mut out=vec![];
    for a in v["actions"].as_array().ok_or_else(||Error::value_error("actions"))? {
        c.now=num(a,"now",c.now);
        if let Some(patch)=a["transport"].as_object() {
            let mut t=serde_json::to_value(&c.transport).unwrap();t.as_object_mut().unwrap().extend(patch.clone());c.transport=decode(t)?;
        }
        match string(a,"action") {
            "start"=>c.start(), "frame"=>c.frame(a["rms"].as_i64().unwrap_or(3000),a["threshold"].as_i64().unwrap_or(1200),num(a,"duration",20.0),boolean(a,"confirmed")),
            "transcript"=> {finals.push(c.transcript(string(a,"text"),boolean(a,"final")));},
            "tick"=>c.tick(), "resume"=>c.resume(), "finished"=>c.playback_finished(),
            "machine"=>c.take_floor_from_machine(optional(a,"audio_id")),
            "accept"=>c.accepted_generation=a["generation"].as_u64(),
            "allow"=>admissions.push(c.allow_playback(a["generation"].as_u64(),boolean(a,"wait"),optional(a,"audio_id"))),
            "result"|"error"=> {
                let token=a["token"].as_u64().unwrap_or_else(||requests.last().map_or(0,|r|r.token));
                let result=if string(a,"action")=="error" {ClassifierResult::Error} else {ClassifierResult::Verdict(string(a,"decision").into())};
                c.classifier_result(token,result);
            },
            "classifier_timeout"=>c.classifier_deadline(),
            "final_timeout"=> {let index=a["index"].as_u64().unwrap() as usize;
                if let Some(w)=finals.get(index).and_then(Option::as_ref) {if c.poll_final(w) {finals[index]=None;}}},
            "close"=>c.close(), "pump"|"context"=>{},
            _=>return Err(Error::value_error("floor action")),
        }
        if a["pump"]!=false {
            if let Some(r)=c.begin_classification() {requests.push(r);}
            if !c.worker_active() {for w in &mut finals {if let Some(token)=w {if c.poll_final(token) {*w=None;}}}}
            for (admission,status) in &mut admissions {if *status==AdmissionResult::Waiting {*status=c.poll_admission(admission);}}
        }
        let effects=c.drain_effects();
        out.push(json!({"episodes":c.episodes().collect::<Vec<_>>(),"current":c.current,"serial":c.serial,
            "generation":c.generation,"caller_floor":c.caller_floor,"awaiting_reply":c.awaiting_reply,
            "last_speech":c.last_speech,"final_seen":c.final_seen,"released":c.released,"protection_released":c.protection_released,
            "pending":c.pending_key(),"requested":c.requested,"accepted_generation":c.accepted_generation,
            "pending_text":c.pending_text,"worker_active":c.worker_active(),"transport":c.transport,
            "effects":effects,"requests":requests,"finals":finals.iter().map(Option::is_none).collect::<Vec<_>>(),
            "admissions":admissions.iter().map(|(_,s)|s).collect::<Vec<_>>()}));
    }
    Ok(json!(out))
}
