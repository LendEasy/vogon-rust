//! Stream-local framing, retained audioop, and direct native WebRTC VAD.
//! Framing produces bytes; delivery, pacing and cancellation belong to transport.
use crate::mixer::AmbienceMixer;
use crate::native::VoiceDetector;

// Match the Python-facing error name without importing its extension.
pyo3::create_exception!(webrtcvad, Error, pyo3::exceptions::PyException);
use pyo3::prelude::*;
use pyo3::types::PyBytes;

#[pyclass]
/// Retained audioop operations used at encoding and PCM conversion boundaries.
/// Methods require an attached Python interpreter and propagate library validation errors.
pub struct AudioOps {
    module: Py<PyModule>,
}

#[pymethods]
impl AudioOps {
    #[new]
    /// Import audioop for subsequent calls; no replacement implementation is substituted.
    pub fn new(py: Python<'_>) -> PyResult<Self> {
        Ok(Self {
            module: py.import("audioop")?.unbind(),
        })
    }
    #[pyo3(signature=(data, width=2))]
    /// Decode mu-law bytes to PCM with the requested sample width in bytes.
    pub fn ulaw2lin<'py>(
        &self,
        py: Python<'py>,
        data: &[u8],
        width: usize,
    ) -> PyResult<Bound<'py, PyBytes>> {
        self.module
            .bind(py)
            .getattr("ulaw2lin")?
            .call1((PyBytes::new(py, data), width))?
            .cast_into()
            .map_err(Into::into)
    }
    #[pyo3(signature=(data, width=2))]
    /// Encode PCM of the supplied sample width in bytes to mu-law.
    pub fn lin2ulaw<'py>(
        &self,
        py: Python<'py>,
        data: &[u8],
        width: usize,
    ) -> PyResult<Bound<'py, PyBytes>> {
        self.module
            .bind(py)
            .getattr("lin2ulaw")?
            .call1((PyBytes::new(py, data), width))?
            .cast_into()
            .map_err(Into::into)
    }
    #[pyo3(signature=(data, width=2))]
    /// Return audioop RMS for PCM samples of the supplied byte width.
    pub fn rms(&self, py: Python<'_>, data: &[u8], width: usize) -> PyResult<i64> {
        self.module
            .bind(py)
            .getattr("rms")?
            .call1((PyBytes::new(py, data), width))?
            .extract()
    }
    /// Convert PCM sample width while retaining audioop truncation and sign semantics.
    pub fn lin2lin<'py>(
        &self,
        py: Python<'py>,
        data: &[u8],
        width: usize,
        new_width: usize,
    ) -> PyResult<Bound<'py, PyBytes>> {
        self.module
            .bind(py)
            .getattr("lin2lin")?
            .call1((PyBytes::new(py, data), width, new_width))?
            .cast_into()
            .map_err(Into::into)
    }
    /// Combine interleaved stereo PCM using the supplied left/right gains.
    pub fn tomono<'py>(
        &self,
        py: Python<'py>,
        data: &[u8],
        width: usize,
        left: f64,
        right: f64,
    ) -> PyResult<Bound<'py, PyBytes>> {
        self.module
            .bind(py)
            .getattr("tomono")?
            .call1((PyBytes::new(py, data), width, left, right))?
            .cast_into()
            .map_err(Into::into)
    }
}

/// One converter per stream. The opaque audioop filter state must never be shared.
#[pyclass]
pub struct RateConverter {
    module: Py<PyModule>,
    state: Py<PyAny>,
    width: usize,
    channels: usize,
    source_rate: u32,
    target_rate: u32,
}

#[pymethods]
impl RateConverter {
    #[new]
    #[pyo3(signature=(source_rate, target_rate, width=2, channels=1))]
    /// Create a stream-local resampler; reject zero rates/channels and unsupported widths.
    pub fn new(
        py: Python<'_>,
        source_rate: u32,
        target_rate: u32,
        width: usize,
        channels: usize,
    ) -> PyResult<Self> {
        if source_rate == 0 || target_rate == 0 || channels == 0 || !(1..=4).contains(&width) {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "invalid resampler format",
            ));
        }
        Ok(Self {
            module: py.import("audioop")?.unbind(),
            state: py.None(),
            width,
            channels,
            source_rate,
            target_rate,
        })
    }
    /// Resample this chunk and retain the opaque filter state for the next chunk.
    pub fn process<'py>(&mut self, py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyBytes>> {
        let result = self.module.bind(py).getattr("ratecv")?.call1((
            PyBytes::new(py, data),
            self.width,
            self.channels,
            self.source_rate,
            self.target_rate,
            self.state.bind(py),
        ))?;
        let output = result.get_item(0)?.cast_into::<PyBytes>()?;
        self.state = result.get_item(1)?.unbind();
        Ok(output)
    }
    /// Discard filter history before an independent stream or utterance.
    pub fn reset(&mut self, py: Python<'_>) {
        self.state = py.None();
    }
    #[getter]
    /// Clone the opaque Python filter-state handle for diagnostics and parity checks.
    pub fn state(&self, py: Python<'_>) -> Py<PyAny> {
        self.state.clone_ref(py)
    }
}

#[pyclass]
/// Buffers provider chunks into 160-byte, 8 kHz mu-law phone frames.
/// The optional legacy linear16 converter is stream-local; this type does no pacing.
pub struct PhoneFramer {
    ops: AudioOps,
    converter: Option<RateConverter>,
    pending: Vec<u8>,
}

#[pymethods]
impl PhoneFramer {
    #[new]
    #[pyo3(signature=(encoding="mulaw"))]
    /// Select mu-law passthrough or 24 kHz linear16 conversion; reject other encodings.
    pub fn new(py: Python<'_>, encoding: &str) -> PyResult<Self> {
        let converter = match encoding {
            "mulaw" => None,
            "linear16" => Some(RateConverter::new(py, 24000, 8000, 2, 1)?),
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "expected mulaw or linear16",
                ));
            }
        };
        Ok(Self {
            ops: AudioOps::new(py)?,
            converter,
            pending: Vec::new(),
        })
    }
    #[pyo3(signature=(chunk, mixer=None))]
    /// Produce complete phone frames, optionally mixing ambience, and retain the short tail.
    pub fn push<'py>(
        &mut self,
        py: Python<'py>,
        chunk: &[u8],
        mut mixer: Option<PyRefMut<'py, AmbienceMixer>>,
    ) -> PyResult<Vec<Py<PyBytes>>> {
        Ok(self
            .push_native(py, chunk, mixer.as_deref_mut())?
            .iter()
            .map(|frame| PyBytes::new(py, frame).unbind())
            .collect())
    }
    /// Sentence/stream tail: no padding or ambience, matching TwilioTransport.
    /// Does not reset the resampler; reset explicitly before the next utterance.
    pub fn flush_tail<'py>(&mut self, py: Python<'py>) -> Bound<'py, PyBytes> {
        let bytes = PyBytes::new(py, &self.pending);
        self.pending.clear();
        bytes
    }
    /// Cancellation/new utterance: discard pending data and filter history.
    pub fn reset(&mut self, py: Python<'_>) {
        self.pending.clear();
        if let Some(converter) = &mut self.converter {
            converter.reset(py);
        }
    }
    #[getter]
    /// Return buffered mu-law bytes that have not formed a complete phone frame.
    pub fn pending_bytes(&self) -> usize {
        self.pending.len()
    }
}

impl PhoneFramer {
    /// Native owner API; Python exposure above is only an adapter.
    pub fn push_native(
        &mut self,
        py: Python<'_>,
        chunk: &[u8],
        mut mixer: Option<&mut AmbienceMixer>,
    ) -> PyResult<Vec<Vec<u8>>> {
        if let Some(converter) = &mut self.converter {
            // Existing legacy transport drops a partial sample per provider chunk.
            let aligned = &chunk[..chunk.len() / 2 * 2];
            if aligned.is_empty() {
                return Ok(Vec::new());
            }
            let pcm = converter.process(py, aligned)?;
            self.pending
                .extend_from_slice(self.ops.lin2ulaw(py, pcm.as_bytes(), 2)?.as_bytes());
        } else {
            self.pending.extend_from_slice(chunk);
        }
        let count = self.pending.len() / 160;
        let mut frames = Vec::with_capacity(count);
        for frame in self.pending[..count * 160].chunks_exact(160) {
            let output = if let Some(mixer) = mixer.as_deref_mut() {
                let pcm = self.ops.ulaw2lin(py, frame, 2)?;
                let mixed = mixer.mix(160, Some(pcm.as_bytes()));
                self.ops.lin2ulaw(py, &mixed, 2)?
            } else {
                PyBytes::new(py, frame)
            };
            frames.push(output.as_bytes().to_vec());
        }
        self.pending.drain(..count * 160);
        Ok(frames)
    }
}

fn browser_packet(audio_id: &str, data: &[u8]) -> PyResult<Vec<u8>> {
    let len = u32::try_from(audio_id.len())
        .map_err(|_| pyo3::exceptions::PyOverflowError::new_err("audio ID exceeds u32"))?;
    let mut packet = Vec::with_capacity(4 + audio_id.len() + data.len());
    packet.extend_from_slice(&len.to_le_bytes());
    packet.extend_from_slice(audio_id.as_bytes());
    packet.extend_from_slice(data);
    Ok(packet)
}

#[pyfunction]
/// Prefix PCM bytes with a little-endian u32 UTF-8 audio-ID byte length and the ID.
/// Reject IDs whose byte length cannot fit the wire header.
pub fn encode_audio_frame<'py>(
    py: Python<'py>,
    audio_id: &str,
    audio_bytes: &[u8],
) -> PyResult<Bound<'py, PyBytes>> {
    Ok(PyBytes::new(py, &browser_packet(audio_id, audio_bytes)?))
}

#[pyclass]
/// Accumulates browser PCM until 8192 bytes, then emits the whole buffer with an ID header.
pub struct BrowserFramer {
    audio_id: String,
    pending: Vec<u8>,
}

#[pymethods]
impl BrowserFramer {
    #[new]
    /// Start an empty browser buffer for one audio ID; no encoding conversion is performed.
    pub fn new(audio_id: String) -> Self {
        Self {
            audio_id,
            pending: Vec::new(),
        }
    }
    /// Append PCM and emit the complete accumulation once the merge threshold is reached.
    pub fn push<'py>(
        &mut self,
        py: Python<'py>,
        chunk: &[u8],
    ) -> PyResult<Option<Bound<'py, PyBytes>>> {
        self.pending.extend_from_slice(chunk);
        if self.pending.len() >= 8192 {
            self.flush_tail(py)
        } else {
            Ok(None)
        }
    }
    /// Emit remaining PCM, including its ID header, or None when the buffer is empty.
    pub fn flush_tail<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        if self.pending.is_empty() {
            return Ok(None);
        }
        // Browser transport emits the whole accumulation, including overshoot.
        let packet = browser_packet(&self.audio_id, &self.pending)?;
        self.pending.clear();
        Ok(Some(PyBytes::new(py, &packet)))
    }
    /// Discard pending PCM without changing the audio ID.
    pub fn reset(&mut self) {
        self.pending.clear();
    }
    #[getter]
    /// Return pending PCM byte count, excluding the browser packet header.
    pub fn pending_bytes(&self) -> usize {
        self.pending.len()
    }
}

/// Python validation adapter for a directly owned C VAD state. Runtime calls use
/// the same owner on their per-call worker; there is no Python VAD object.
#[pyclass(unsendable)]
pub struct Vad {
    vad: VoiceDetector,
    sample_rate: u32,
}

#[pymethods]
impl Vad {
    /// Create a detector; preserve the existing Python-facing constructor API.
    #[new]
    #[pyo3(signature=(aggressiveness=2, sample_rate=8000))]
    pub fn new(_py: Python<'_>, aggressiveness: u8, sample_rate: u32) -> PyResult<Self> {
        if aggressiveness > 3 {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "{aggressiveness} is an invalid mode, must be 0-3"
            )));
        }
        let vad = VoiceDetector::new(aggressiveness)
            .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
        Ok(Self { vad, sample_rate })
    }
    /// Classify a PCM16 frame, updating this stream's adaptive state.
    pub fn is_speech(&mut self, _py: Python<'_>, pcm: &[u8]) -> PyResult<bool> {
        self.vad
            .process(pcm, self.sample_rate)
            .map_err(Error::new_err)
    }
    /// Preserve barge_gate.make_confirm_frame's fail-closed frame policy.
    pub fn confirm_frame(&mut self, py: Python<'_>, pcm: &[u8]) -> bool {
        self.is_speech(py, pcm).unwrap_or(false)
    }
}
