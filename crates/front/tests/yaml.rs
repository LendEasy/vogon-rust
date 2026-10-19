//! Frozen outputs captured from serde_yaml_ng 0.10.0 before replacing its backend.
//! This corpus checks the real file-loading boundary without retaining the old dependency.
use serde_json::{Value, json};
use vogon_front::config::Artifact;

#[test]
fn yaml_artifact_loading_matches_reference_and_explicit_migration_cases() {
    let cases: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/yaml_compatibility.json")).unwrap();
    let path = std::env::temp_dir().join(format!(
        "vogon-yaml-compatibility-{}.yaml",
        std::process::id()
    ));
    let mut differences = Vec::new();
    for case in cases {
        std::fs::write(&path, case["yaml"].as_str().unwrap()).unwrap();
        let actual = Artifact::load(&path, 12)
            .map(|a| serde_json::to_value(a).unwrap())
            .map_err(|e| e.code);
        let reference = case.get("expected").unwrap_or(&case["reference"]);
        let expected = if reference.get("error").is_some() {
            Err("invalid_front_yaml".to_owned())
        } else {
            Artifact::from_value(&reference["ok"], 12)
                .map(|a| json!(a))
                .map_err(|e| e.code)
        };
        if actual != expected {
            differences.push(format!(
                "{}: actual={actual:?}, expected={expected:?}",
                case["name"]
            ));
        }
    }
    std::fs::remove_file(path).unwrap();
    assert!(differences.is_empty(), "{}", differences.join("\n"));
}
