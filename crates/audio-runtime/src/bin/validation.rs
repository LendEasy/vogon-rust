use pyo3::prelude::*;
use vogon_audio_core::vogon_audio_core as core_module;
use vogon_audio_runtime::vogon_audio_runtime as runtime_module;

fn main() -> PyResult<()> {
    pyo3::append_to_inittab!(runtime_module);
    pyo3::append_to_inittab!(core_module);
    let path = std::env::args().nth(1).expect("validation script path");
    Python::attach(|py| {
        let kwargs = pyo3::types::PyDict::new(py);
        kwargs.set_item("run_name", "__main__")?;
        py.import("runpy")?
            .getattr("run_path")?
            .call((path,), Some(&kwargs))?;
        Ok(())
    })
}
