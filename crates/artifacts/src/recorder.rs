//! Timestamp projection, sample-accurate clear trimming and stereo WAV assembly.
use crate::{Error, Result};
use pyo3::{Python, types::PyBytesMethods};
use serde_json::{Value, json};
use vogon_audio_core::formats::AudioOps;

/// One original mu-law packet and its monotonic playback/capture position.
#[derive(Clone)]
struct Chunk {
    at: f64,
    data: Vec<u8>,
}
/// Bounded call-local capture. WAV construction runs on a blocking worker at teardown.
/// Disabled policy prevents capture itself, not just persistence.
pub struct Recorder {
    pub enabled: bool,
    pub t0: Option<f64>,
    caller: Vec<Chunk>,
    agent: Vec<Chunk>,
    epoch: Option<f64>,
    anchor: Option<(f64, f64)>,
    pub stream_used: usize,
    pub stream_missing: usize,
    pub reanchors: usize,
    pub max_divergence: f64,
    bytes: usize,
    pub max_bytes: usize,
    pub max_seconds: f64,
    pub truncated: bool,
    event_bytes: usize,
    events: Vec<(f64, String, Value)>,
    frames: Vec<(f64, [i64; 6])>,
}
impl Default for Recorder {
    fn default() -> Self {
        Self {
            enabled: true,
            t0: None,
            caller: vec![],
            agent: vec![],
            epoch: None,
            anchor: None,
            stream_used: 0,
            stream_missing: 0,
            reanchors: 0,
            max_divergence: 0.,
            bytes: 0,
            max_bytes: 64 * 1024 * 1024,
            max_seconds: 4. * 3600.,
            truncated: false,
            event_bytes: 0,
            events: vec![],
            frames: vec![],
        }
    }
}
impl Recorder {
    /// Establish the timeline origin without assuming start-event time places audio.
    pub fn epoch(&mut self, at: f64) {
        if at.is_finite() {
            self.epoch.get_or_insert(at);
            self.note(at);
        }
    }
    fn note(&mut self, at: f64) {
        self.t0 = Some(self.t0.map_or(at, |t| t.min(at)));
    }
    /// Pair a packet's stream timestamp with its own arrival; reanchor beyond 250ms drift.
    pub fn caller_timestamp(&mut self, timestamp: &Value, arrival: f64) -> f64 {
        let ms = timestamp
            .as_f64()
            .or_else(|| timestamp.as_str().and_then(|s| s.parse().ok()))
            .filter(|v: &f64| v.is_finite());
        let Some(ms) = ms else {
            self.stream_missing += 1;
            return arrival;
        };
        let (mono, base) = *self.anchor.get_or_insert((arrival, ms));
        self.stream_used += 1;
        let placed = mono + (ms - base) / 1000.;
        let drift = placed - arrival;
        if drift.abs() > self.max_divergence.abs() {
            self.max_divergence = drift;
        }
        if drift.abs() > 0.25 {
            self.reanchors += 1;
            self.anchor = Some((arrival, ms));
            arrival
        } else {
            placed
        }
    }
    /// Capture original bytes; overlap resolution is last-packet-wins, matching Python.
    pub fn add(&mut self, caller: bool, at: f64, data: &[u8]) {
        if !self.enabled || data.is_empty() {
            return;
        }
        if !at.is_finite()
            || data.len() > self.max_bytes.saturating_sub(self.bytes)
            || self
                .t0
                .is_some_and(|t| (at - t).abs() + data.len() as f64 / 8000. > self.max_seconds)
        {
            self.truncated = true;
            return;
        }
        self.note(at);
        self.bytes += data.len();
        let chunk = Chunk {
            at,
            data: data.to_vec(),
        };
        if caller {
            self.caller.push(chunk);
        } else {
            self.agent.push(chunk);
        }
    }
    /// Discard agent samples estimated to be queued when a clear successfully dispatches.
    pub fn clear(&mut self, at: f64) -> usize {
        if !at.is_finite() {
            return 0;
        }
        let mut discarded = 0;
        for c in &mut self.agent {
            let n = ((at - c.at) * 8000.).round_ties_even().max(0.) as usize;
            let n = n.min(c.data.len());
            discarded += c.data.len() - n;
            c.data.truncate(n);
        }
        self.agent.retain(|c| !c.data.is_empty());
        self.bytes -= discarded;
        discarded
    }
    /// Append bounded diagnostic events, with no network or file work on the audio path.
    pub fn event(&mut self, at: f64, name: &str, detail: Value) {
        if !self.enabled {
            return;
        }
        let size = detail.to_string().len() + name.len();
        if !at.is_finite()
            || self.events.len() >= 100_000
            || size > 16_384
            || self.event_bytes + size > 8 * 1024 * 1024
        {
            self.truncated = true;
            return;
        }
        self.event_bytes += size;
        self.events.push((at, name.into(), detail));
    }
    /// Keep only scalar frame diagnostics, never duplicate processed audio in JSON.
    pub fn frame(&mut self, at: f64, data: Value) {
        if !self.enabled {
            return;
        }
        if !at.is_finite() || self.frames.len() >= 720_000 {
            self.truncated = true;
            return;
        }
        self.frames.push((
            at,
            [
                "raw_rms",
                "clean_rms",
                "tts_playing",
                "gap_active",
                "streak",
                "threshold",
            ]
            .map(|k| data[k].as_i64().unwrap_or(0)),
        ));
    }
    /// Build the Python-compatible {meta,events} timeline using the WAV origin.
    pub fn event_json(&self, mut meta: Value) -> Value {
        meta["recording_truncated"] = json!(self.truncated);
        meta["stream_clock"] = json!({"used":self.stream_used,"missing":self.stream_missing,"reanchors":self.reanchors,"max_divergence":self.max_divergence});
        let base = self
            .t0
            .or_else(|| self.events.first().map(|e| e.0))
            .unwrap_or(0.);
        let rows: Vec<_> = self
            .events
            .iter()
            .map(|(at, name, detail)| {
                let mut row = json!({"t":round_millis(at-base),"event":name});
                if let Some(d) = detail.as_object() {
                    for (k, v) in d {
                        if !matches!(k.as_str(), "t" | "event") {
                            row[k] = v.clone();
                        }
                    }
                }
                row
            })
            .collect();
        json!({"meta":meta,"events":rows})
    }
    /// Export the existing diagnostic CSV field names, aligned to the recording.
    pub fn frames_csv(&self) -> Option<Vec<u8>> {
        if self.frames.is_empty() {
            return None;
        }
        let base = self.t0.unwrap_or(self.frames[0].0);
        let mut out =
            String::from("t_rel_s,raw_rms,clean_rms,tts_playing,gap_active,streak,threshold\n");
        for (at, v) in &self.frames {
            out.push_str(&format!(
                "{:.6},{},{},{},{},{},{}\n",
                at - base,
                v[0],
                v[1],
                v[2],
                v[3],
                v[4],
                v[5]
            ));
        }
        Some(out.into_bytes())
    }
    /// Decode with retained audioop; interleave caller left / agent right, exactly 8kHz.
    pub fn wav(&self) -> Result<Option<Vec<u8>>> {
        if !self.enabled || (self.caller.is_empty() && self.agent.is_empty()) {
            return Ok(None);
        }
        let base = self.t0.unwrap_or(0.);
        let n = self
            .caller
            .iter()
            .chain(&self.agent)
            .map(|c| ((c.at - base) * 8000.).round_ties_even().max(0.) as usize + c.data.len())
            .max()
            .unwrap_or(0);
        if n == 0 {
            return Ok(None);
        }
        if n as f64 > self.max_seconds * 8000. || n > (u32::MAX as usize - 36) / 4 {
            return Err(Error("recording_timeline_limit"));
        }
        let mut pcm = vec![0u8; n * 4];
        Python::attach(|py| -> pyo3::PyResult<()> {
            let ops = AudioOps::new(py)?;
            for (channel, chunks) in [&self.caller, &self.agent].iter().enumerate() {
                for c in *chunks {
                    let offset = ((c.at - base) * 8000.).round_ties_even();
                    if offset < 0. {
                        continue;
                    }
                    let decoded = ops.ulaw2lin(py, &c.data, 2)?;
                    for (i, sample) in decoded.as_bytes().as_chunks::<2>().0.iter().enumerate() {
                        let idx = (offset as usize + i) * 4 + channel * 2;
                        if idx + 2 <= pcm.len() {
                            pcm[idx..idx + 2].copy_from_slice(sample);
                        }
                    }
                }
            }
            Ok(())
        })
        .map_err(|_| Error("recording_decode_failed"))?;
        let mut wav = Vec::with_capacity(44 + pcm.len());
        wav.extend_from_slice(b"RIFF");
        wav.extend_from_slice(&(36 + pcm.len() as u32).to_le_bytes());
        wav.extend_from_slice(b"WAVEfmt ");
        wav.extend_from_slice(&16u32.to_le_bytes());
        wav.extend_from_slice(&1u16.to_le_bytes());
        wav.extend_from_slice(&2u16.to_le_bytes());
        wav.extend_from_slice(&8000u32.to_le_bytes());
        wav.extend_from_slice(&32000u32.to_le_bytes());
        wav.extend_from_slice(&4u16.to_le_bytes());
        wav.extend_from_slice(&16u16.to_le_bytes());
        wav.extend_from_slice(b"data");
        wav.extend_from_slice(&(pcm.len() as u32).to_le_bytes());
        wav.extend_from_slice(&pcm);
        Ok(Some(wav))
    }
}
/// Python's decimal round uses the represented binary float, not a scaled intermediate.
fn round_millis(value: f64) -> f64 {
    format!("{value:.3}").parse().unwrap_or(value)
}
