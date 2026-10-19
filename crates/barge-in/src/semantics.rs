//! Native semantic fast paths ported from Python; remote classification is injected by
//! the host. No application-specific safety or floor vocabulary is supplied.
//!
//! [`Policy`](crate::semantics::Policy) is stateless call policy. Normalization supports echo comparisons;
//! [`heuristic`](crate::semantics::heuristic) resolves clear utterances; final/absorb helpers preserve the
//! older semantic-window composition rules. [`crate::floor::Controller`] applies
//! these text judgments together with acoustic evidence and playback state.
use serde::{Deserialize, Serialize};
/// Pinned Python decimal-digit ranges used by normalization; no runtime Unicode lookup.
mod unicode { include!("unicode_decimal.rs"); }

/// Normalize transcript text for substring comparisons using Rust whitespace rules.
///
/// Lowercase, straighten smart quotes, remove commas between decimal digits,
/// collapse standard Unicode whitespace and strip trailing terminal punctuation. Internal
/// decimal points remain intact. Decimal ranges are pinned to Python Unicode
/// 13.0.0; lowercasing follows Rust Unicode tables, so exhaustive cross-version
/// Unicode identity is not guaranteed.
pub fn normalize(text:&str)->String {
    let chars:Vec<char>=text.to_lowercase().chars().map(|c|match c {
        '‘'|'’'|'‚'|'‛'|'′'|'`'=>'\'', '“'|'”'|'„'|'‟'|'″'=>'"', c=>c}).collect();
    let digits=|c:char|unicode::DECIMAL.iter().any(|&(lo,hi)|lo<=c as u32 && c as u32<=hi);
    let s:String=chars.iter().enumerate().filter_map(|(i,&c)| {
        if c==',' && i>0 && i+1<chars.len() && digits(chars[i-1]) && digits(chars[i+1]) {None} else {Some(c)}
    }).collect();
    let s=s.split_whitespace().collect::<Vec<_>>().join(" ");
    s.trim_end_matches(['.','!','?']).trim().into()
}
/// Test normalized substring membership; an empty normalized needle matches, as in Python.
pub fn contains(text:&str, needle:&str)->bool {normalize(text).contains(&normalize(needle))}
/// Text-only policy supplied by the application and current playout.
///
/// This value owns no transcript history or classifier. Safety phrases are
/// matched as supplied against lowercased text, while floor phrases are trimmed
/// and lowercased before token matching. Empty application phrase lists add no
/// domain-specific vocabulary.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct Policy {
    /// Maximum token count eligible for the cooperative backchannel fast path.
    pub backchannel_max_tokens:i64,
    /// Token count at which speech is substantive regardless of floor phrases.
    pub substantive_min_tokens:i64,
    /// Application-provided substring phrases that take precedence over other text rules.
    pub safety_phrases:Vec<String>,
    /// Application-provided demands for the floor, matched as contiguous token sequences.
    pub floor_phrases:Vec<String>,
    /// Agent line used to veto floor phrases that may be leaked agent speech.
    pub playout_text:String,
}
impl Default for Policy {
    /// Use the reference token limits (three/six), empty application phrase lists and no playout.
    fn default()->Self {Self {backchannel_max_tokens:3,substantive_min_tokens:6,safety_phrases:vec![],floor_phrases:vec![],playout_text:String::new()}}
}
/// Conservative cooperative tokens shared with the Python fast path; not a general speech classifier.
const BACKCHANNEL:&[&str]=&["yeah","yes","yep","yup","ok","okay","kay","right","sure","uh-huh","uhhuh","mhm","mm-hmm","mmhmm","hmm","uh","um","got","it","gotcha","alright","fine","correct","true","i","see","oh","ah","understood","sounds","good"];
/// Legacy semantic verdicts that keep agent audio playing and absorb the turn.
pub const ABSORB:&[&str]=&["backchannel","no_text","timeout_absorb","intro_absorb"];
/// Legacy semantic verdicts that permit interrupting agent playout.
pub const CUT:&[&str]=&["answer_to_heard","substantive_interrupt","safety","floor_request"];
/// Heuristic verdicts that resolve immediately without waiting for a remote judge.
pub const DECISIVE:&[&str]=&["backchannel","safety","substantive_interrupt","floor_request"];
/// Classify text without I/O or state mutation.
///
/// Order matters: empty text, safety phrases, backchannels, substantive length
/// or a trailing question mark, then token-matched floor phrases. A floor
/// phrase appearing in agent playout is vetoed as potential echo. The remaining
/// short, unclear text returns `ambiguous` for further arbitration.
pub fn heuristic(text:&str,p:&Policy)->&'static str {
    let stripped=text.trim().to_lowercase();
    if stripped.is_empty() {return "no_text";}
    if p.safety_phrases.iter().any(|s|stripped.contains(s)) {return "safety";}
    let tokens:Vec<&str>=stripped.split_whitespace().map(|t|t.trim_matches(['.',',','!','?','\'','’','-'])).filter(|t|!t.is_empty()).collect();
    if tokens.is_empty() {return "no_text";}
    if tokens.len() as i64<=p.backchannel_max_tokens && tokens.iter().all(|t|BACKCHANNEL.contains(t)) {return "backchannel";}
    if tokens.len() as i64>=p.substantive_min_tokens || stripped.ends_with('?') {return "substantive_interrupt";}
    for phrase in &p.floor_phrases {
        let phrase=phrase.trim().to_lowercase();
        let want:Vec<&str>=phrase.split_whitespace().collect();
        if !want.is_empty() && tokens.windows(want.len()).any(|w|w==want) &&
            (p.playout_text.is_empty() || !contains(&p.playout_text,&phrase)) {return "floor_request";}
    }
    "ambiguous"
}
/// Compose a legacy window verdict and its source label.
///
/// A decisive heuristic wins; otherwise a recognized judge verdict is used.
/// With no usable judge answer, empty text remains `no_text` and ambiguity
/// becomes `timeout_absorb`. Returned verdict strings borrow from the inputs
/// or static literals; this function does not launch a classifier.
pub fn resolve_final<'a>(heuristic:&'a str,llm:Option<&'a str>)->(&'a str,&'static str) {
    if DECISIVE.contains(&heuristic) {return (heuristic,"heuristic");}
    if let Some(v)=llm {if CUT.contains(&v)||v=="backchannel" {return (v,"llm");}}
    if heuristic=="no_text" {("no_text","window_expiry")} else {("timeout_absorb","window_expiry")}
}
/// Recheck an absorbed utterance after its final transcript arrives.
///
/// Return true only for backchannel, empty or ambiguous text; speech that
/// grows into a substantive interruption or policy phrase must reach the turn handler.
pub fn should_swallow_final(text:&str,p:&Policy)->bool {matches!(heuristic(text,p),"backchannel"|"no_text"|"ambiguous")}
/// Apply the prior absorb reason before rechecking the final transcript.
///
/// `echo` and `intro_absorb` remain unconditionally swallowed for legacy parity;
/// other reasons delegate to the normal final-text heuristic.
pub fn should_swallow_absorbed_final(verdict:&str,text:&str,p:&Policy)->bool {
    matches!(verdict,"echo"|"intro_absorb")||should_swallow_final(text,p)
}
/// Return whether a nonempty candidate is a normalized substring of agent speech
/// and contains at least `min_words` whitespace-separated words.
///
/// The word floor protects short genuine replies; the controller uses four
/// words for echo and two for the tentative partial-echo check.
pub fn is_echo_of(candidate:&str,speak:&str,min_words:usize)->bool {
    !candidate.is_empty() && !speak.is_empty() && candidate.split_whitespace().count()>=min_words && contains(speak,candidate)
}
