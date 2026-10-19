//! Deterministic ambience sources. Generation is native Rust; WAV/audioop
//! loading retains the existing library behavior, including conversion order.
use crate::mixer::{AmbienceConfig, AmbienceMixer};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::path::{Path, PathBuf};

/// Saturate an integer sample to the signed PCM16 range.
pub(crate) fn clamp(x: i64) -> i16 {
    x.clamp(-32768, 32767) as i16
}
/// Encode samples as little-endian PCM16 bytes.
pub(crate) fn pack(samples: &[i16]) -> Vec<u8> {
    samples.iter().flat_map(|s| s.to_le_bytes()).collect()
}
/// Decode complete little-endian PCM16 samples, ignoring an odd trailing byte.
pub(crate) fn unpack(pcm: &[u8]) -> Vec<i16> {
    pcm.chunks_exact(2)
        .map(|s| i16::from_le_bytes([s[0], s[1]]))
        .collect()
}

#[derive(Clone)]
#[pyclass(from_py_object)]
/// Owned PCM16 clip with its declared sample rate; constructors do not resample it.
pub struct Clip {
    pub(crate) pcm: Vec<u8>,
    #[pyo3(get)]
    pub sample_rate: u32,
}

#[pymethods]
impl Clip {
    #[new]
    /// Copy PCM bytes and retain the declared sample rate without format validation.
    pub fn new(pcm: &[u8], sample_rate: u32) -> Self {
        Self {
            pcm: pcm.to_vec(),
            sample_rate,
        }
    }
    #[getter]
    fn pcm<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.pcm)
    }
    #[getter]
    /// Count complete 16-bit samples; an odd trailing byte does not count.
    pub fn n_samples(&self) -> usize {
        self.pcm.len() / 2
    }
}

#[derive(Clone)]
#[pyclass(from_py_object)]
/// Nonempty bank of clips sharing one sample rate, used by a mixer layer.
pub struct Source {
    #[pyo3(get)]
    pub clips: Vec<Clip>,
    #[pyo3(get)]
    pub sample_rate: u32,
}

#[pymethods]
impl Source {
    #[new]
    /// Validate a nonempty clip bank whose declared rates match the source rate.
    pub fn new(clips: Vec<Clip>, sample_rate: u32) -> PyResult<Self> {
        if clips.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Source requires at least one clip",
            ));
        }
        if clips.iter().any(|c| c.sample_rate != sample_rate) {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "all clips must share the source sample rate",
            ));
        }
        Ok(Self { clips, sample_rate })
    }
    fn __len__(&self) -> usize {
        self.clips.len()
    }
    /// Clone a clip using cyclic indexing, including negative indices.
    pub fn clip(&self, index: i64) -> Clip {
        self.clips[index.rem_euclid(self.clips.len() as i64) as usize].clone()
    }
}

struct Lcg(u32);
impl Lcg {
    fn next(&mut self) -> u32 {
        self.0 = self.0.wrapping_mul(1664525).wrapping_add(1013904223);
        self.0
    }
    fn unit(&mut self) -> f64 {
        (self.next() as f64 / u32::MAX as f64) * 2.0 - 1.0
    }
}

fn lowpass(samples: &[i16], alpha: f64) -> Vec<i16> {
    let mut previous = 0.0;
    samples
        .iter()
        .map(|&s| {
            previous = previous + alpha * (s as f64 - previous);
            clamp(previous as i64)
        })
        .collect()
}

fn source(clips: Vec<Vec<i16>>, rate: u32) -> PyResult<Source> {
    Source::new(
        clips
            .iter()
            .map(|s| Clip {
                pcm: pack(s),
                sample_rate: rate,
            })
            .collect(),
        rate,
    )
}

fn rate_check(rate: u32) -> PyResult<()> {
    if rate == 0 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "sample rate must be positive",
        ));
    }
    Ok(())
}

#[pyfunction]
#[pyo3(signature = (target_rate, seed=0x51EED))]
/// Generate four seconds of seeded, low-pass noise with a crossfaded loop boundary.
pub fn build_synthetic_room_tone(target_rate: u32, seed: i64) -> PyResult<Source> {
    rate_check(target_rate)?;
    let n = target_rate as usize * 4;
    let mut rng = Lcg(seed as u32);
    let peak = 0.06 * 32767.0;
    let raw: Vec<i16> = (0..n).map(|_| (rng.unit() * peak) as i16).collect();
    let mut shaped = lowpass(&raw, 0.06);
    let crossfade = ((target_rate as f64 * 0.03) as usize).min(n / 4);
    for i in 0..crossfade {
        let weight = i as f64 / crossfade as f64;
        shaped[n - crossfade + i] = clamp(
            (shaped[n - crossfade + i] as f64 * (1.0 - weight) + shaped[i] as f64 * weight) as i64,
        );
    }
    source(vec![shaped], target_rate)
}

#[pyfunction]
#[pyo3(signature = (target_rate, seed_base=0x7A11, n=4))]
/// Generate a seeded bank of typing bursts and gaps; zero clips is rejected.
pub fn build_synthetic_typing(target_rate: u32, seed_base: i64, n: usize) -> PyResult<Source> {
    rate_check(target_rate)?;
    let mut clips = vec![];
    for index in 0..n {
        let mut rng = Lcg((seed_base as u32).wrapping_add(index as u32));
        let keys = 7 + rng.next() % 6;
        let peak = 0.5 * 32767.0;
        let mut samples = vec![];
        for _ in 0..keys {
            let gap = (target_rate as f64 * (0.06 + 0.10 * ((rng.next() % 1000) as f64 / 1000.0)))
                as usize;
            samples.resize(samples.len() + gap, 0);
            let duration = (target_rate as f64
                * (0.025 + 0.020 * ((rng.next() % 1000) as f64 / 1000.0)))
                as usize;
            for i in 0..duration {
                let envelope = (-(i as f64) / (duration as f64 * 0.35)).exp();
                samples.push(clamp((rng.unit() * peak * envelope) as i64));
            }
        }
        clips.push(samples);
    }
    source(clips, target_rate)
}

#[pyfunction]
#[pyo3(signature = (target_rate, seed_base=0x0EE))]
/// Generate deterministic ring and creak clips at a positive target rate.
pub fn build_synthetic_oneshots(target_rate: u32, seed_base: i64) -> PyResult<Source> {
    rate_check(target_rate)?;
    let rate = target_rate as f64;
    let mut rng = Lcg(seed_base as u32);
    let burst = (rate * 0.35) as usize;
    let gap = (rate * 0.20) as usize;
    let peak = 0.18 * 32767.0;
    let mut ring = vec![];
    for _ in 0..2 {
        for i in 0..burst {
            let t = i as f64 / rate;
            let carrier = (2.0 * std::f64::consts::PI * 480.0 * t).sin();
            let warble = 0.5 + 0.5 * (2.0 * std::f64::consts::PI * 20.0 * t).sin();
            let envelope = (std::f64::consts::PI * i as f64 / burst as f64).sin();
            ring.push(clamp((carrier * warble * envelope * peak) as i64));
        }
        ring.resize(ring.len() + gap, 0);
    }
    let mut ring = lowpass(&ring, 0.25);
    for s in &mut ring {
        *s = clamp(*s as i64 + (rng.unit() * peak * 0.02) as i64);
    }
    let mut rng = Lcg((seed_base as u32).wrapping_add(1));
    let n = (rate * 0.4) as usize;
    let raw: Vec<i16> = (0..n)
        .map(|_| (rng.unit() * (0.30 * 32767.0)) as i16)
        .collect();
    let mut creak = lowpass(&raw, 0.02);
    for (i, s) in creak.iter_mut().enumerate() {
        *s = clamp((*s as f64 * (std::f64::consts::PI * i as f64 / n as f64).sin()) as i64);
    }
    source(vec![ring, creak], target_rate)
}

#[pyfunction]
/// Read WAV assets, then convert width, stereo-to-mono and sample rate in that order.
/// Uses Python wave/audioop; unsupported channels, invalid rates and I/O failures propagate.
pub fn build_wav_source(py: Python<'_>, paths: Vec<PathBuf>, target_rate: u32) -> PyResult<Source> {
    rate_check(target_rate)?;
    let wave = py.import("wave")?;
    let audioop = py.import("audioop")?;
    let mut clips = vec![];
    for path in paths {
        let file = wave
            .getattr("open")?
            .call1((path.to_string_lossy().as_ref(), "rb"))?;
        let loaded = (|| -> PyResult<(u32, usize, u32, Py<PyAny>)> {
            let channels = file.call_method0("getnchannels")?.extract()?;
            let width = file.call_method0("getsampwidth")?.extract()?;
            let rate = file.call_method0("getframerate")?.extract()?;
            let pcm = file.call_method1("readframes", (file.call_method0("getnframes")?,))?;
            Ok((channels, width, rate, pcm.unbind()))
        })();
        file.call_method0("close")?;
        let (channels, width, rate, pcm) = loaded?;
        let mut pcm = pcm.into_bound(py);
        if width != 2 {
            pcm = audioop.getattr("lin2lin")?.call1((&pcm, width, 2))?;
        }
        if channels == 2 {
            pcm = audioop.getattr("tomono")?.call1((&pcm, 2, 0.5, 0.5))?;
        } else if channels != 1 {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "unsupported channel count {channels}"
            )));
        }
        if rate != target_rate {
            pcm = audioop
                .getattr("ratecv")?
                .call1((&pcm, 2, 1, rate, target_rate, py.None()))?
                .get_item(0)?;
        }
        clips.push(Clip {
            pcm: pcm.extract()?,
            sample_rate: target_rate,
        });
    }
    Source::new(clips, target_rate)
}

fn wavs(directory: &Path, prefix: &str) -> Vec<PathBuf> {
    let mut paths: Vec<_> = std::fs::read_dir(directory)
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with(prefix))
                && p.extension().is_some_and(|e| e == "wav")
        })
        .collect();
    paths.sort();
    paths
}

#[pyfunction]
#[pyo3(signature = (sample_rate, *, enable_room=true, enable_typing=true, enable_oneshots=false, config=None, assets_dir=None))]
/// Build enabled layers from matching WAV assets or deterministic synthetic fallbacks.
/// Asset discovery is limited to the explicit directory; disabled layers are not loaded.
pub fn build_mixer(
    py: Python<'_>,
    sample_rate: u32,
    enable_room: bool,
    enable_typing: bool,
    enable_oneshots: bool,
    config: Option<AmbienceConfig>,
    assets_dir: Option<PathBuf>,
) -> PyResult<AmbienceMixer> {
    let layer = |enabled: bool, prefix: &str| -> PyResult<Option<Source>> {
        if !enabled {
            return Ok(None);
        }
        let files = assets_dir
            .as_ref()
            .map(|dir| wavs(dir, prefix))
            .unwrap_or_default();
        if !files.is_empty() {
            return Ok(Some(build_wav_source(py, files, sample_rate)?));
        }
        Ok(Some(match prefix {
            "room_tone" => build_synthetic_room_tone(sample_rate, 0x51EED)?,
            "typing" => build_synthetic_typing(sample_rate, 0x7A11, 4)?,
            _ => build_synthetic_oneshots(sample_rate, 0x0EE)?,
        }))
    };
    AmbienceMixer::new(
        sample_rate,
        layer(enable_room, "room_tone")?,
        layer(enable_typing, "typing")?,
        layer(enable_oneshots, "oneshot_")?,
        config,
    )
}
