//! Direct C-library ownership. No Python objects or interpreter are used here.
//!
//! Each handle owns one call's adaptive state. Mutable methods serialize access;
//! neither handle is shared across threads. Input bytes are copied to aligned
//! i16 arrays before C sees them, avoiding alignment and short-buffer UB.
use std::{
    ffi::{c_int, c_void},
    ptr::NonNull,
};

unsafe extern "C" {
    fn speex_echo_state_init_mc(
        frame: c_int,
        tail: c_int,
        mics: c_int,
        speakers: c_int,
    ) -> *mut c_void;
    fn speex_echo_ctl(state: *mut c_void, request: c_int, value: *mut c_void) -> c_int;
    fn speex_echo_cancellation(
        state: *mut c_void,
        near: *const i16,
        far: *const i16,
        out: *mut i16,
    );
    fn speex_echo_state_destroy(state: *mut c_void);
    fn WebRtcVad_Create(state: *mut *mut c_void) -> c_int;
    fn WebRtcVad_Init(state: *mut c_void) -> c_int;
    fn WebRtcVad_set_mode(state: *mut c_void, mode: c_int) -> c_int;
    fn WebRtcVad_Process(state: *mut c_void, rate: c_int, pcm: *const i16, samples: c_int)
    -> c_int;
    fn WebRtcVad_ValidRateAndFrameLength(rate: c_int, samples: c_int) -> c_int;
    fn WebRtcVad_Free(state: *mut c_void);
}

/// Single-channel, 8 kHz Speex MDF canceller with 20 ms frames and a 400 ms tail.
/// Matches speexdsp-python's init_mc + sample-rate ctl; preprocessing is disabled
/// in that wrapper and remains disabled here. Drop frees the native state once.
pub struct EchoCanceller {
    state: NonNull<c_void>,
}

impl EchoCanceller {
    /// Allocate and configure the C state, cleaning up if configuration fails.
    pub fn new() -> Result<Self, &'static str> {
        // SAFETY: fixed positive dimensions match every input buffer below.
        let state = NonNull::new(unsafe { speex_echo_state_init_mc(160, 3200, 1, 1) })
            .ok_or("SpeexDSP allocation failed")?;
        let owner = Self { state };
        let mut rate: c_int = 8000;
        // SPEEX_ECHO_SET_SAMPLING_RATE = 24 in speex_echo.h; ctl reads one int.
        let status =
            unsafe { speex_echo_ctl(state.as_ptr(), 24, (&mut rate as *mut c_int).cast()) };
        if status != 0 {
            return Err("SpeexDSP sample-rate configuration failed");
        }
        Ok(owner)
    }

    /// Cancel one complete PCM16 frame. Reject bad lengths before entering C.
    pub fn process(&mut self, near: &[u8], far: &[u8]) -> Result<Vec<u8>, &'static str> {
        if near.len() != 320 || far.len() != 320 {
            return Err("SpeexDSP requires 160 PCM16 samples per frame");
        }
        let samples = |bytes: &[u8]| -> [i16; 160] {
            std::array::from_fn(|i| i16::from_ne_bytes([bytes[2 * i], bytes[2 * i + 1]]))
        };
        let near = samples(near);
        let far = samples(far);
        let mut output = [0i16; 160];
        // SAFETY: live exclusively owned state; all arrays match configured size.
        unsafe {
            speex_echo_cancellation(
                self.state.as_ptr(),
                near.as_ptr(),
                far.as_ptr(),
                output.as_mut_ptr(),
            );
        }
        Ok(output
            .iter()
            .flat_map(|sample| sample.to_ne_bytes())
            .collect())
    }
}

impl Drop for EchoCanceller {
    fn drop(&mut self) {
        // SAFETY: this owner alone frees the successful allocation.
        unsafe {
            speex_echo_state_destroy(self.state.as_ptr());
        }
    }
}

/// One stream's WebRTC VAD adaptive state, using the C code from py-webrtcvad 2.0.10.
pub struct VoiceDetector {
    state: NonNull<c_void>,
}

impl VoiceDetector {
    /// Initialize a new detector in mode 0..=3. Failure releases allocated state.
    pub fn new(mode: u8) -> Result<Self, &'static str> {
        if mode > 3 {
            return Err("VAD mode must be 0-3");
        }
        let mut raw = std::ptr::null_mut();
        // SAFETY: Create writes one owned handle to a valid out pointer.
        if unsafe { WebRtcVad_Create(&mut raw) } != 0 {
            return Err("WebRTC VAD allocation failed");
        }
        let owner = Self {
            state: NonNull::new(raw).ok_or("WebRTC VAD returned null")?,
        };
        // SAFETY: both functions receive the freshly allocated live handle.
        if unsafe { WebRtcVad_Init(raw) } != 0
            || unsafe { WebRtcVad_set_mode(raw, mode.into()) } != 0
        {
            return Err("WebRTC VAD initialization failed");
        }
        Ok(owner)
    }

    /// Classify 10/20/30 ms PCM16 at a supported rate, preserving native hangover.
    /// Like the Python API, a final incomplete sample is ignored.
    pub fn process(&mut self, pcm: &[u8], sample_rate: u32) -> Result<bool, &'static str> {
        let rate = c_int::try_from(sample_rate).map_err(|_| "Error while processing frame")?;
        let count = c_int::try_from(pcm.len() / 2).map_err(|_| "Error while processing frame")?;
        // SAFETY: validation uses integers only, before allocating or accessing C buffers.
        if unsafe { WebRtcVad_ValidRateAndFrameLength(rate, count) } != 0 {
            return Err("Error while processing frame");
        }
        let samples: Vec<i16> = pcm
            .chunks_exact(2)
            .map(|s| i16::from_ne_bytes([s[0], s[1]]))
            .collect();
        // SAFETY: length/rate were checked and &mut self prevents simultaneous mutation.
        match unsafe { WebRtcVad_Process(self.state.as_ptr(), rate, samples.as_ptr(), count) } {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err("Error while processing frame"),
        }
    }
}

impl Drop for VoiceDetector {
    fn drop(&mut self) {
        // SAFETY: unique owner of a successfully allocated handle.
        unsafe {
            WebRtcVad_Free(self.state.as_ptr());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reject malformed inputs before C and repeatedly allocate/free without Python.
    #[test]
    fn native_lifecycle_and_bounds() {
        for _ in 0..100 {
            let mut echo = EchoCanceller::new().unwrap();
            assert!(echo.process(&[0; 319], &[0; 320]).is_err());
            assert!(echo.process(&[0; 320], &[0; 321]).is_err());
            assert_eq!(echo.process(&[0; 320], &[0; 320]).unwrap(), [0; 320]);
            for mode in 0..4 {
                let mut vad = VoiceDetector::new(mode).unwrap();
                assert!(!vad.process(&[0; 320], 8000).unwrap());
                assert!(vad.process(&[], 8000).is_err());
                assert!(vad.process(&[0; 320], 12345).is_err());
            }
        }
        assert!(VoiceDetector::new(4).is_err());
    }
}
