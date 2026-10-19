//! Direct state-machine port of voice-gateway's AECProcessor.
//! FIFO/history ordering, error fallback and reset timing are part of parity.
use crate::libraries::{Libraries, dot};
use crate::native::EchoCanceller;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PySet, PySlice};
use serde_json::json;
use std::collections::VecDeque;

const FRAME_BYTES: usize = 320;
const HISTORY_FRAMES: usize = 50;
const GCC_BYTES: usize = 9600;
const WINDOW: usize = 10;
const SILENCE: [u8; FRAME_BYTES] = [0; FRAME_BYTES];

/// Per-call object. Python access is restricted to the creating thread, matching
/// the reference's single-threaded contract. Do not share mutable stream state.
#[pyclass(name = "AECProcessor", unsendable)]
pub struct AecProcessor {
    libs: Libraries,
    ec: Option<EchoCanceller>,
    reference: VecDeque<Vec<u8>>,
    history: VecDeque<Vec<u8>>,
    near_window: VecDeque<Vec<u8>>,
    delay: i64,
    estimated: bool,
    slip: i64,
    since_check: usize,
    realignments: Vec<(usize, i64, f64, f64)>,
    gcc_ref: Vec<u8>,
    gcc_near: Vec<u8>,
    near_power: f64,
    out_power: f64,
    erle_frames: usize,
    total_frames: usize,
    errors: Vec<String>,
}

fn append(queue: &mut VecDeque<Vec<u8>>, frame: Vec<u8>, limit: usize) {
    if queue.len() == limit {
        queue.pop_front();
    }
    queue.push_back(frame);
}

#[pymethods]
impl AecProcessor {
    #[new]
    /// Import retained NumPy/audioop and allocate the direct C echo canceller for one call.
    /// Initialization fails when any required runtime dependency is unavailable.
    pub fn new(py: Python<'_>) -> PyResult<Self> {
        let libs = Libraries::new(py)?;
        let ec = EchoCanceller::new().map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
        Ok(Self {
            libs,
            ec: Some(ec),
            reference: VecDeque::new(),
            history: VecDeque::new(),
            near_window: VecDeque::new(),
            delay: 0,
            estimated: false,
            slip: 0,
            since_check: 0,
            realignments: vec![],
            gcc_ref: vec![],
            gcc_near: vec![],
            near_power: 0.0,
            out_power: 0.0,
            erle_frames: 0,
            total_frames: 0,
            errors: vec![],
        })
    }

    /// Queue successfully dispatched mu-law audio, decoded to one 160-sample PCM frame.
    /// Decode failures are recorded and do not enqueue a reference.
    pub fn feed_reference(&mut self, py: Python<'_>, data: &[u8]) {
        match self.libs.frame(py, data) {
            Ok(pcm) => self.reference.push_back(pcm),
            Err(e) => self.note_error("decode reference", e),
        }
    }

    /// Consume one inbound mu-law frame and the next reference, returning cleaned mu-law.
    /// Processing after close fails; internal processing fallbacks preserve reference behavior.
    pub fn process<'py>(&mut self, py: Python<'py>, near: &[u8]) -> PyResult<Bound<'py, PyBytes>> {
        if self.ec.is_none() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "AEC processor is closed",
            ));
        }
        let output = self.process_frame(py, near);
        Ok(PyBytes::new(py, &output))
    }

    /// Discard queued references and reset alignment slip while retaining filter/history state.
    pub fn discard_reference(&mut self) {
        self.reference.clear();
        self.slip = 0;
    }

    /// Report whether initial delay estimation completed, not an acoustic-quality guarantee.
    pub fn is_converged(&self) -> bool {
        self.estimated
    }
    /// Apply the existing ERLE accumulator reset threshold; no production log is emitted.
    pub fn log_erle_now(&mut self) {
        self.reset_erle_if_ready();
    }

    /// Deterministic cleanup. Dropping the Rust owner frees C state without Python.
    pub fn close(&mut self, _py: Python<'_>) -> PyResult<()> {
        self.ec.take();
        Ok(())
    }

    #[getter(_delay_frames)]
    fn delay_frames(&self) -> i64 {
        self.delay
    }
    #[getter(_alignment_slip)]
    fn alignment_slip(&self) -> i64 {
        self.slip
    }
    #[getter]
    fn realignments(&self) -> Vec<(usize, i64, f64, f64)> {
        self.realignments.clone()
    }

    /// Validation/diagnostic surface; not used to drive stream decisions.
    pub fn state_json(&self) -> String {
        json!({"delay":self.delay, "estimated":self.estimated, "slip":self.slip,
            "fifo":self.reference.len(), "history":self.history.len(),
            "near_window":self.near_window.len(), "since_check":self.since_check,
            "gcc_ref":self.gcc_ref.len(), "gcc_near":self.gcc_near.len(),
            "near_power":self.near_power, "out_power":self.out_power,
            "erle_frames":self.erle_frames, "total_frames":self.total_frames,
            "realignments":self.realignments, "errors":self.errors})
        .to_string()
    }
}

impl AecProcessor {
    fn note_error(&mut self, operation: &str, error: PyErr) {
        // Bounded diagnostics: malformed inputs cannot grow this indefinitely.
        if self.errors.len() == 16 {
            self.errors.remove(0);
        }
        self.errors.push(format!("{operation}: {error}"));
    }

    fn process_frame(&mut self, py: Python<'_>, near: &[u8]) -> Vec<u8> {
        self.total_frames += 1;
        let current = self
            .reference
            .pop_front()
            .unwrap_or_else(|| SILENCE.to_vec());
        append(&mut self.history, current.clone(), HISTORY_FRAMES);
        let pcm = match self.libs.frame(py, near) {
            Ok(pcm) => pcm,
            Err(e) => {
                self.note_error("decode near", e);
                return near.to_vec();
            }
        };
        if !self.estimated
            && (!self.gcc_ref.is_empty() || self.libs.rms(py, &current).unwrap_or(0) >= 200)
        {
            self.gcc_ref.extend(&current);
            self.gcc_near.extend(&pcm);
            if self.gcc_ref.len() >= GCC_BYTES && self.gcc_near.len() >= GCC_BYTES {
                if let Err(e) = self.gcc(py) {
                    self.delay = 9;
                    self.estimated = true;
                    self.note_error("GCC fallback", e);
                }
                self.gcc_ref.clear();
                self.gcc_near.clear();
            }
        }
        let reference = self.shifted_reference().to_vec();
        let clean = match self.ec.as_mut().unwrap().process(&pcm, &reference) {
            Ok(clean) => clean,
            Err(e) => {
                self.note_error(
                    "Speex process",
                    pyo3::exceptions::PyRuntimeError::new_err(e),
                );
                return near.to_vec();
            }
        };
        if let (Ok(n), Ok(o)) = (self.libs.rms(py, &pcm), self.libs.rms(py, &clean)) {
            self.near_power += (n * n) as f64;
            self.out_power += (o * o) as f64;
            self.erle_frames += 1;
        }
        if self.erle_frames > 0 && self.erle_frames % 50 == 0 {
            self.reset_erle_if_ready();
        }
        append(&mut self.near_window, pcm, WINDOW);
        self.since_check += 1;
        if self.estimated && self.since_check >= 5 && self.near_window.len() == WINDOW {
            self.since_check = 0;
            if let Err(e) = self.check_alignment(py) {
                self.note_error("alignment", e);
            }
        }
        match self.libs.encode(py, &clean) {
            Ok(output) => output,
            Err(e) => {
                self.note_error("encode output", e);
                near.to_vec()
            }
        }
    }

    fn reset_erle_if_ready(&mut self) {
        if self.erle_frames < 5 || self.out_power <= 0.0 {
            return;
        }
        self.near_power = 0.0;
        self.out_power = 0.0;
        self.erle_frames = 0;
    }

    fn shifted_reference(&self) -> &[u8] {
        let lag = self.delay + self.slip;
        if lag < 0 || lag as usize >= self.history.len() {
            &SILENCE
        } else {
            &self.history[self.history.len() - lag as usize - 1]
        }
    }

    fn reference_run<'py>(&self, py: Python<'py>, lag: i64) -> PyResult<Option<Bound<'py, PyAny>>> {
        if lag < 0 || self.history.len() < WINDOW + 1 + lag as usize {
            return Ok(None);
        }
        let end = self.history.len() - lag as usize;
        let bytes: Vec<u8> = self
            .history
            .range(end - WINDOW - 1..end)
            .flatten()
            .copied()
            .collect();
        let samples = self.libs.samples(py, &bytes, "float64")?;
        Ok(Some(samples.get_item(PySlice::new(
            py,
            145,
            isize::MAX,
            1,
        ))?))
    }

    fn trial(&self, py: Python<'_>, near: &Bound<'_, PyAny>, lag: i64) -> PyResult<Option<f64>> {
        let Some(reference) = self.reference_run(py, lag)? else {
            return Ok(None);
        };
        if dot(&reference, &reference)? < 1.0 {
            return Ok(None);
        }
        let np = self.libs.numpy.bind(py);
        let matrix = np
            .getattr("lib")?
            .getattr("stride_tricks")?
            .getattr("sliding_window_view")?
            .call1((&reference, 16))?
            .get_item((PySlice::full(py), PySlice::new(py, -1, isize::MIN, -1)))?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("rcond", py.None())?;
        let fit = np
            .getattr("linalg")?
            .getattr("lstsq")?
            .call((&matrix, near), Some(&kwargs))?
            .get_item(0)?;
        let prediction = matrix.call_method1("__matmul__", (&fit,))?;
        let residual = near.call_method1("__sub__", (&prediction,))?;
        Ok(Some(
            10.0 * self
                .libs
                .log10(py, dot(near, near)? / dot(&residual, &residual)?.max(1e-9))?,
        ))
    }

    fn correlation(
        &self,
        py: Python<'_>,
        near: &Bound<'_, PyAny>,
        current: i64,
    ) -> PyResult<Option<i64>> {
        let mut best = None;
        let mut strength = 0.0;
        for offset in -8..=8 {
            let Some(reference) = self.reference_run(py, current + offset)? else {
                continue;
            };
            let aligned = reference.get_item(PySlice::new(py, 15, isize::MAX, 1))?;
            let norm = self
                .libs
                .sqrt(py, dot(&aligned, &aligned)? * dot(near, near)?)?;
            if norm > 0.0 {
                let correlation = dot(&aligned, near)?.abs() / norm;
                if correlation > strength {
                    best = Some(current + offset);
                    strength = correlation;
                }
            }
        }
        Ok(best)
    }

    fn check_alignment(&mut self, py: Python<'_>) -> PyResult<()> {
        let current = self.delay + self.slip;
        let Some(reference) = self.reference_run(py, current)? else {
            return Ok(());
        };
        let np = self.libs.numpy.bind(py);
        let aligned = reference.get_item(PySlice::new(py, 15, isize::MAX, 1))?;
        let squared = aligned.call_method1("__pow__", (2,))?;
        let mean = np.getattr("mean")?.call1((&squared,))?;
        if np.getattr("sqrt")?.call1((&mean,))?.extract::<f64>()? < 200.0 {
            return Ok(());
        }
        let bytes: Vec<u8> = self.near_window.iter().flatten().copied().collect();
        let near = self.libs.samples(py, &bytes, "float64")?;
        // Preserve Python set iteration in exact-score ties, not Rust hash order.
        let candidates = PySet::new(
            py,
            [
                Some(current - 1),
                Some(current),
                Some(current + 1),
                self.correlation(py, &near, current)?,
            ],
        )?;
        let none = PySet::new(py, [py.None()])?;
        let candidates = candidates.call_method1("__sub__", (&none,))?;
        let mut scores = vec![];
        for lag in candidates.try_iter()? {
            let lag: i64 = lag?.extract()?;
            if let Some(score) = self.trial(py, &near, lag)? {
                scores.push((lag, score));
            }
        }
        let Some(&(_, old_score)) = scores.iter().find(|s| s.0 == current) else {
            return Ok(());
        };
        let mut best = scores[0];
        for &candidate in &scores {
            if candidate.1 > best.1 {
                best = candidate;
            }
        }
        if best.0 == current || best.1 < 6.0 || best.1 < old_score + 3.0 {
            return Ok(());
        }
        let shift = best.0 - current;
        self.slip += shift;
        let round = py.import("builtins")?.getattr("round")?;
        self.realignments.push((
            self.total_frames,
            shift,
            round.call1((old_score, 1))?.extract()?,
            round.call1((best.1, 1))?.extract()?,
        ));
        Ok(())
    }

    fn gcc(&mut self, py: Python<'_>) -> PyResult<()> {
        let n = self.gcc_ref.len().min(self.gcc_near.len()).min(GCC_BYTES);
        let reference = self.libs.samples(py, &self.gcc_ref[..n], "float32")?;
        let near = self.libs.samples(py, &self.gcc_near[..n], "float32")?;
        let np = self.libs.numpy.bind(py);
        let fft = np.getattr("fft")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("n", n)?;
        let a = fft.getattr("rfft")?.call((&near,), Some(&kwargs))?;
        let b = fft.getattr("rfft")?.call((&reference,), Some(&kwargs))?;
        let cross = a.call_method1("__mul__", (np.getattr("conj")?.call1((&b,))?,))?;
        let denominator = np.getattr("abs")?.call1((&cross,))?;
        let small = denominator.call_method1("__lt__", (1e-10,))?;
        denominator.set_item(&small, 1e-10)?;
        let normalized = cross.call_method1("__truediv__", (&denominator,))?;
        let phat = fft.getattr("irfft")?.call((&normalized,), Some(&kwargs))?;
        let window = np
            .getattr("abs")?
            .call1((phat.get_item(PySlice::new(py, 0, 4000, 1))?,))?;
        let delay: usize = np.getattr("argmax")?.call1((&window,))?.extract()?;
        let mean: f64 = window.call_method0("mean")?.extract()?;
        let ratio = if mean > 0.0 {
            window.get_item(delay)?.extract::<f64>()? / mean
        } else {
            0.0
        };
        if ratio < 12.0 {
            return Ok(());
        }
        self.delay = (delay / 160) as i64;
        self.estimated = true;
        if self.delay > 0 {
            let replacement =
                EchoCanceller::new().map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
            self.close(py)?;
            self.ec = Some(replacement);
        }
        Ok(())
    }
}
