//! Build the pinned WebRTC C sources without their CPython extension.
//! SpeexDSP is supplied by the system, matching the Python oracle's shared library.
use std::{env, fs, path::PathBuf, process::Command};

/// Run one tool, failing the build rather than silently falling back to Python.
fn run(command: &mut Command) {
    assert!(
        command
            .status()
            .expect("launch native build tool")
            .success(),
        "native build failed: {command:?}"
    );
}

fn main() {
    assert_eq!(
        env::var("CARGO_CFG_TARGET_OS").unwrap(),
        "linux",
        "native audio build currently supports Linux"
    );
    assert_eq!(
        env::var("HOST").unwrap(),
        env::var("TARGET").unwrap(),
        "cross compilation requires a target C toolchain configuration"
    );
    let root = PathBuf::from("vendor/webrtcvad");
    let out = PathBuf::from(env::var_os("OUT_DIR").unwrap());
    println!("cargo:rerun-if-changed=vendor/webrtcvad");
    println!("cargo:rerun-if-env-changed=CC");
    println!("cargo:rerun-if-env-changed=AR");
    let mut objects = Vec::new();
    for directory in ["signal_processing", "vad"] {
        let mut sources: Vec<_> = fs::read_dir(root.join("webrtc/common_audio").join(directory))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "c"))
            .collect();
        sources.sort();
        for source in sources {
            let object = out.join(source.file_stem().unwrap()).with_extension("o");
            run(
                Command::new(env::var_os("CC").unwrap_or_else(|| "cc".into()))
                    .args([
                        "-c",
                        "-O3",
                        "-fPIC",
                        "-fwrapv",
                        "-fno-strict-aliasing",
                        "-fvisibility=hidden",
                        "-DWEBRTC_POSIX",
                        "-I",
                    ])
                    .arg(&root)
                    .arg(&source)
                    .arg("-o")
                    .arg(&object),
            );
            objects.push(object);
        }
    }
    run(
        Command::new(env::var_os("AR").unwrap_or_else(|| "ar".into()))
            .arg("crs")
            .arg(out.join("libvogon_webrtcvad.a"))
            .args(objects),
    );
    println!("cargo:rustc-link-search=native={}", out.display());
    println!("cargo:rustc-link-lib=static=vogon_webrtcvad");
    println!("cargo:rustc-link-lib=speexdsp");
    println!("cargo:rustc-link-lib=m");
    println!("cargo:rustc-link-lib=pthread");
}
