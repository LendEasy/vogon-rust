//! Retained NumPy/audioop boundary for AEC numerical and sample operations.
//! These helpers own Python imports, not the Python gateway implementation.
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

/// Imported once per processor. No gateway Python implementation is imported.
pub struct Libraries {
    pub numpy: Py<PyModule>,
    pub audioop: Py<PyModule>,
}

impl Libraries {
    /// Import NumPy and audioop once for this processor; missing modules fail startup.
    pub fn new(py: Python<'_>) -> PyResult<Self> {
        Ok(Self {
            numpy: py.import("numpy")?.unbind(),
            audioop: py.import("audioop")?.unbind(),
        })
    }

    /// Decode 8-bit mu-law to signed 16-bit PCM using retained audioop.
    pub fn decode(&self, py: Python<'_>, data: &[u8]) -> PyResult<Vec<u8>> {
        self.audioop
            .bind(py)
            .getattr("ulaw2lin")?
            .call1((PyBytes::new(py, data), 2))?
            .extract()
    }

    /// Decode and pad or truncate to 320 PCM bytes, one 20 ms frame at 8 kHz.
    pub fn frame(&self, py: Python<'_>, data: &[u8]) -> PyResult<Vec<u8>> {
        let mut pcm = self.decode(py, data)?;
        pcm.resize(320, 0);
        Ok(pcm)
    }

    /// Encode signed 16-bit PCM to mu-law using retained audioop.
    pub fn encode(&self, py: Python<'_>, pcm: &[u8]) -> PyResult<Vec<u8>> {
        self.audioop
            .bind(py)
            .getattr("lin2ulaw")?
            .call1((PyBytes::new(py, pcm), 2))?
            .extract()
    }

    /// Compute integer RMS of signed 16-bit PCM with audioop semantics.
    pub fn rms(&self, py: Python<'_>, pcm: &[u8]) -> PyResult<i64> {
        self.audioop
            .bind(py)
            .getattr("rms")?
            .call1((PyBytes::new(py, pcm), 2))?
            .extract()
    }

    /// Interpret PCM as NumPy int16 and convert to the requested NumPy dtype.
    /// The returned array remains Python-owned; callers must hold the interpreter attachment.
    pub fn samples<'py>(
        &self,
        py: Python<'py>,
        pcm: &[u8],
        dtype: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let np = self.numpy.bind(py);
        let kwargs = PyDict::new(py);
        kwargs.set_item("dtype", np.getattr("int16")?)?;
        np.getattr("frombuffer")?
            .call((PyBytes::new(py, pcm),), Some(&kwargs))?
            .call_method1("astype", (np.getattr(dtype)?,))
    }

    /// Native scalar math, preserving Python's domain error fallback for silence.
    pub fn log10(&self, _py: Python<'_>, value: f64) -> PyResult<f64> {
        if value <= 0.0 {
            return Err(pyo3::exceptions::PyValueError::new_err("math domain error"));
        }
        Ok(value.log10())
    }

    /// Native square root with the existing error policy for negative inputs.
    pub fn sqrt(&self, _py: Python<'_>, value: f64) -> PyResult<f64> {
        if value < 0.0 {
            return Err(pyo3::exceptions::PyValueError::new_err("math domain error"));
        }
        Ok(value.sqrt())
    }
}

/// Perform NumPy-compatible matrix multiplication and extract a scalar result.
pub fn dot(a: &Bound<'_, PyAny>, b: &Bound<'_, PyAny>) -> PyResult<f64> {
    a.call_method1("__matmul__", (b,))?.extract()
}
