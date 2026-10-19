//! Existing Vogon timeline UI, populated natively with escaped artifact data.
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
/// Render a standalone timeline with embedded WAV; JSON cannot break out of the script.
pub fn render(call: &str, wav: &[u8], events: &Value, frames: Option<&[u8]>) -> String {
    let frames = frames
        .and_then(|b| std::str::from_utf8(b).ok())
        .map(|text| {
            let mut cols =
                json!({"t":[],"raw":[],"clean":[],"tts":[],"gap":[],"streak":[],"thr":[]});
            for row in text.lines().skip(1) {
                let cells: Vec<_> = row.split(',').collect();
                if cells.len() != 7 {
                    continue;
                }
                for (i, key) in ["t", "raw", "clean", "tts", "gap", "streak", "thr"]
                    .iter()
                    .enumerate()
                {
                    cols[key].as_array_mut().unwrap().push(
                        cells[i]
                            .parse::<f64>()
                            .ok()
                            .map_or(Value::Null, |v| json!(v)),
                    );
                }
            }
            cols
        });
    let data = json!({"callId":call,"meta":events["meta"],"events":events["events"].as_array().cloned().unwrap_or_default(),"frames":frames,"vad":null,"frameMs":20});
    let title = format!("Audio timeline — {call}")
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#x27;");
    include_str!("timeline.html")
        .replace("__TITLE__", &title)
        .replace("__AUDIO_B64__", &STANDARD.encode(wav))
        .replace(
            "__DATA_JSON__",
            &data
                .to_string()
                .replace('<', "\\u003c")
                .replace('>', "\\u003e")
                .replace('&', "\\u0026"),
        )
}
