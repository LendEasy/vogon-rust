#![no_main]
//! Exercise untrusted contract JSON without sockets, credentials or Python.
use libfuzzer_sys::fuzz_target;
fuzz_target!(|data: &[u8]| {
    if data.len() > 65536 {
        return;
    }
    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(data) {
        for name in [
            "StartCallRequest",
            "ProcessEventsRequest",
            "EndCallRequest",
            "CallConfig",
            "EscalationOrder",
        ] {
            if let Ok(normal) = vogon_contracts::codec::normalize(name, &value) {
                let bytes = serde_json::to_vec(&normal).unwrap();
                let decoded = serde_json::from_slice::<serde_json::Value>(&bytes).unwrap();
                assert_eq!(normal, decoded);
            }
        }
    }
});
