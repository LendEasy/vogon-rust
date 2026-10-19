//! Mark receipts and buffered-playback clearing, separate from stream generation.
//!
//! [`PlayoutState`](crate::state::PlayoutState) owns the host's pending mark map,
//! sentence receipts, ledger, emitted facts and echo-latency estimate. The host
//! registers sent marks and feeds their echoes/clear dispatches into this state.
//! Receipt confirmation is distinct from [`crate::stream::SentenceStream`]'s
//! acknowledged sends. This module neither sends packets nor arbitrates the floor.
use std::collections::BTreeMap;
use serde_json::{json, Value};
use crate::{ledger::Ledger, delivery::{estimate_total_ms,estimate_heard_text}};

/// Per-call accounting for sent marks, received playback confirmations and clears.
///
/// The host supplies the current text, sentence plan and pending mark entries,
/// then invokes receipt methods with monotonic seconds. Events accumulate for
/// the host to publish. Protected/absorb flags describe playout; the barge-in
/// controller decides whether caller speech may interrupt it.
#[derive(Default)]
pub struct PlayoutState {
    /// Playout history updated when an attempt begins or its latest open entry ends.
    pub ledger:Ledger,
    /// Mark name to `(sent_at_seconds, text_snippet)` mapping populated by the host.
    pub pending_marks:BTreeMap<String,(f64,String)>,
    /// Current sentence plan; when populated, heard splitting uses echoed sentence count.
    pub sentences:Vec<String>,
    /// Number of recognized sentence-mark echoes for the current playout.
    pub sentences_played:usize,
    /// Full intended text used when sentence receipt accounting is unavailable.
    pub text:String,
    /// Current/latest response identity used for drain-clear facts.
    pub last_audio_id:String,
    /// Current attempt start in monotonic seconds; zero means no timing anchor.
    pub started_at:f64,
    /// Most recent terminal echo/clear time in monotonic seconds.
    pub ended_at:f64,
    /// Whether the current playout carries protected-span policy.
    pub protected:bool,
    /// Whether the protected playout carries absorb policy; begin enables this only with protection.
    pub absorb:bool,
    /// Accumulated playout-finished/drain-cleared JSON facts awaiting host publication.
    pub events:Vec<Value>,
    /// Smoothed nonnegative mark round-trip/playout delay in seconds, absent until observed.
    offset:Option<f64>,
}
impl PlayoutState {
    /// Open a ledger entry and initialize current text, identity, sentence count and policy.
    ///
    /// Set `absorb` only if the playout is also protected. Pending marks, accumulated
    /// events, the prior end timestamp and the smoothed offset survive this call;
    /// the host manages their lifecycle and populates the new sentence plan.
    pub fn begin(&mut self,audio_id:String,text:String,now:f64,protected:bool,absorb:bool) {
        self.ledger.begin(audio_id.clone(),text.clone(),now);
        self.last_audio_id=audio_id;self.text=text;self.started_at=now;
        self.sentences.clear();self.sentences_played=0;self.protected=protected;self.absorb=protected&&absorb;
    }
    /// Report whether the oldest pending mark is less than 90 seconds old at `now`.
    ///
    /// This is a buffered-playback fallback indicator, independent of active TTS
    /// streaming. Expired marks are not removed by this read-only query.
    pub fn draining(&self,now:f64)->bool {
        self.pending_marks.values().map(|(t,_)|*t).min_by(f64::total_cmp).is_some_and(|oldest|now-oldest<90.0)
    }
    /// Close the latest open ledger entry and append a `playout_finished` fact if one exists.
    ///
    /// The fact uses `now` and sequence zero; publication/sequence coordination
    /// belongs to the host. Existing closed entries produce no additional fact.
    pub fn close_ledger(&mut self,now:f64) {
        if let Some(id)=self.ledger.close(now) {self.events.push(json!({"kind":"playout_finished","audio_id":id,"seq":0,"at":now}));}
    }
    /// Consume a known mark echo and return whether it is a terminal notification.
    ///
    /// Unknown/duplicate marks return false. A known name beginning `tts_s` advances
    /// the sentence-receipt count and returns false. Other known marks set the end
    /// time, close the latest open ledger entry and return true so the host can
    /// notify clocks/floor control. Nonnegative elapsed send-to-echo time updates
    /// the offset using a 0.3 new-sample weight; negative samples are ignored.
    pub fn mark_echo(&mut self,name:&str,now:f64)->bool {
        let Some((sent,_))=self.pending_marks.remove(name) else {return false;};
        if name.starts_with("tts_s") {self.sentences_played+=1;return false;}
        self.ended_at=now;self.close_ledger(now);
        // Preserve Python's ms-then-seconds expression and its floating rounding.
        let seconds=((now-sent)*1000.0)/1000.0;
        if seconds>=0.0 {self.offset=Some(self.offset.map_or(seconds,|old|0.3*seconds+(1.0-0.3)*old));}
        true
    }
    /// Account for a host-dispatched clear at `now`.
    ///
    /// Remove all pending marks, record the end time, close the latest open ledger
    /// entry and append `drain_cleared` for a nonempty latest audio identity. Later
    /// echoes for removed marks are ignored. This method sends no clear packet and
    /// does not clear AEC reference audio or notify clocks; those are host actions.
    pub fn cleared(&mut self,now:f64) {
        self.pending_marks.clear();self.ended_at=now;self.close_ledger(now);
        if !self.last_audio_id.is_empty() {self.events.push(json!({"kind":"drain_cleared","audio_id":self.last_audio_id,"seq":0,"at":now}));}
    }
    /// Return the current `(heard_text, unheard_text)` estimate without mutating state.
    ///
    /// When sentences are available, split at the echoed sentence count, clamped
    /// to the plan length. Otherwise estimate a whole-word prefix from elapsed time
    /// since `started_at` and the text-duration fallback. Empty text yields two
    /// empty strings. The fallback suffix is sliced by prefix character count,
    /// preserving the existing transport behavior.
    pub fn heard_split(&self,now:f64)->(String,String) {
        if self.text.is_empty() {return (String::new(),String::new());}
        if !self.sentences.is_empty() {
            let k=self.sentences_played.min(self.sentences.len());return (self.sentences[..k].join(" "),self.sentences[k..].join(" "));
        }
        let elapsed=if self.started_at!=0.0 {(now-self.started_at)*1000.0} else {0.0};
        let heard=estimate_heard_text(&self.text,Some(elapsed),estimate_total_ms(&self.text));
        let unheard=self.text.chars().skip(heard.chars().count()).collect::<String>().trim().to_owned();
        (heard,unheard)
    }
    /// Serialize receipt state and derived drain/offset/heard observations for inspection.
    ///
    /// `now` affects only derived observations. Missing offset is reported as zero;
    /// this method neither drains events nor changes marks, history or counters.
    pub fn snapshot(&self,now:f64)->Value {
        json!({"pending":self.pending_marks,"played":self.sentences_played,"ended_at":self.ended_at,
            "ledger":self.ledger.entries,"events":self.events,"draining":self.draining(now),"offset":self.offset.unwrap_or(0.0),"heard":self.heard_split(now)})
    }
}
