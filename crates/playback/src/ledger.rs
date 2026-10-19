//! Playout history for reconstructing what the caller may have heard at onset.
//!
//! [`Ledger`](crate::ledger::Ledger) records text plus host-supplied start/end
//! times. Its horizon query combines completed entries with a proportional
//! estimate for overlapping audio; it does not inspect samples or send receipts.
//! All timestamps use one call's monotonic-seconds clock domain.

use serde::{Deserialize, Serialize};
use vogon_contracts::HeardHorizon;
use crate::delivery::{estimate_heard_text, estimate_total_ms};

/// One agent playout recorded for later caller-onset context.
///
/// An absent end means the host has not closed this entry; it does not guarantee
/// that a provider is still generating audio or that the caller can hear it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    /// Identity linking this playout to response/transport events.
    pub audio_id:String,
    /// Full intended agent text used for heard/unheard estimates.
    pub text:String,
    /// Host-supplied playout start in monotonic seconds.
    pub start_t:f64,
    /// Host-supplied completion or clear time in monotonic seconds, when known.
    pub end_t:Option<f64>
}

/// Insertion-ordered playout history owned by a call.
///
/// Normal insertion through `begin` keeps up to 20 entries. Directly replacing
/// `entries` is supported for restoration/fixtures; callers then own ordering
/// and size validity.
#[derive(Default)]
pub struct Ledger {
    /// Playout records in insertion order, used to reconstruct onset context.
    pub entries: Vec<Entry>
}
impl Ledger {
    /// Append an open playout at `now` and remove the oldest entry if the length exceeds 20.
    ///
    /// Existing open entries are not automatically closed; the host controls
    /// completion timing and must keep all timestamps in the same clock domain.
    pub fn begin(&mut self, audio_id: String, text: String, now: f64) {
        self.entries.push(Entry {audio_id,text,start_t:now,end_t:None});
        if self.entries.len() > 20 { self.entries.remove(0); }
    }
    /// Close the latest still-open entry at `now` and return its audio identity.
    ///
    /// Return None when no open entry exists. Closing selects by reverse insertion
    /// order, not by a supplied audio ID; callers preserve the transport's ordering.
    pub fn close(&mut self, now: f64) -> Option<String> {
        let entry = self.entries.iter_mut().rev().find(|e| e.end_t.is_none())?;
        entry.end_t = Some(now); Some(entry.audio_id.clone())
    }
    /// Build heard/unheard context at caller `onset` in monotonic seconds.
    ///
    /// Return None for the zero-onset sentinel or an empty ledger. Entries ending
    /// by onset count as completed; entries starting after onset contribute unheard
    /// text. Overlapping entries use the word-duration fallback for a heard prefix.
    /// Completed identities are sorted by end time; insertion order governs the
    /// remaining context, with the last overlapping entry supplying `in_flight`.
    /// No receipt or audio evidence is inferred beyond the stored times.
    pub fn horizon(&self, onset: f64) -> Option<HeardHorizon> {
        if onset == 0.0 || self.entries.is_empty() { return None; }
        let mut completed = Vec::new();
        let mut last_heard = String::new(); let mut unheard = String::new();
        let mut in_flight = None; let mut prefix = String::new();
        for entry in &self.entries {
            if entry.start_t > onset {
                unheard = format!("{unheard} {}",entry.text).trim().to_owned(); continue;
            }
            if let Some(end) = entry.end_t && end <= onset {
                completed.push((end,entry.audio_id.clone())); last_heard = entry.text.clone(); continue;
            }
            in_flight = Some(entry.audio_id.clone());
            prefix = estimate_heard_text(&entry.text, Some(((onset-entry.start_t)*1000.0).max(0.0)),estimate_total_ms(&entry.text));
            if !prefix.is_empty() { last_heard = prefix.clone(); }
            let remainder = if prefix.is_empty() { entry.text.clone() }
                else { entry.text.chars().skip(prefix.chars().count()).collect::<String>().trim().to_owned() };
            unheard = format!("{remainder} {unheard}").trim().to_owned();
        }
        completed.sort_by(|a,b| a.0.total_cmp(&b.0));
        Some(HeardHorizon {completed:completed.into_iter().map(|(_,id)|id).collect(),in_flight,
            in_flight_heard_text:prefix,last_heard_text:last_heard,unheard_text:unheard})
    }
}
