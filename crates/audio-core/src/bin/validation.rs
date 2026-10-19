//! Embed the chosen Python runtime and register the Rust classes for testing.
use pyo3::prelude::*;
use std::path::Path;
use vogon_audio_core::vogon_audio_core as audio_module;

fn main() -> PyResult<()> {
    pyo3::append_to_inittab!(audio_module);
    let args: Vec<String> = std::env::args().collect();
    assert!(
        args.len() >= 2,
        "usage: vogon-audio-validation SCRIPT [arguments]"
    );
    Python::attach(|py| {
        let sys = py.import("sys")?;
        sys.setattr("argv", &args[1..])?;
        let script = Path::new(&args[1]).canonicalize().unwrap();
        sys.getattr("path")?
            .call_method1("insert", (0, script.parent().unwrap().to_str().unwrap()))?;
        let kwargs = pyo3::types::PyDict::new(py);
        kwargs.set_item("run_name", "__main__")?;
        py.import("runpy")?
            .getattr("run_path")?
            .call((script.to_str().unwrap(),), Some(&kwargs))?;
        Ok(())
    })
}
