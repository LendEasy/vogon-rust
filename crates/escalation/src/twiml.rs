//! Pure TwiML generators. Persona audio takes precedence over the configured Say voice.
pub const VOICE: &str = "Polly.Joanna-Neural";
pub const PROMPT: &str = "Press any key when you are ready to connect the caller.";
pub const HOLD: &str = "Please hold while we connect you to a specialist.";
pub const CALLBACK: &str =
    "Our specialist is unavailable right now. Someone will call you back shortly.";

/// Escape text and attributes before embedding configuration in XML.
pub fn xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
/// Wrap a valid response body with the common declaration.
fn response(body: String) -> String {
    format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Response>{body}</Response>")
}
/// Select rendered persona audio or the existing Say fallback.
fn speak(text: &str, voice: &str, audio: Option<&str>) -> String {
    match audio {
        Some(url) => format!("<Play>{}</Play>", xml(url)),
        None => format!("<Say voice=\"{}\">{}</Say>", xml(voice), xml(text)),
    }
}
/// Hold the caller in a conference until an agent joins.
pub fn hold(text: &str, room: &str, music: &str, voice: &str, audio: Option<&str>) -> String {
    let wait = if music.is_empty() {
        String::new()
    } else {
        format!(" waitUrl=\"{}\"", xml(music))
    };
    response(format!(
        "{}<Dial><Conference startConferenceOnEnter=\"false\" endConferenceOnExit=\"true\" beep=\"false\" statusCallbackEvent=\"join leave\"{wait}>{}</Conference></Dial>",
        speak(text, voice, audio),
        xml(room)
    ))
}
/// Whisper a summary to the agent, then gather one key before connecting.
pub fn whisper(summary: &str, action: &str, voice: &str, audio: Option<&str>) -> String {
    response(format!(
        "<Gather numDigits=\"1\" action=\"{}\" method=\"POST\" timeout=\"60\">{}</Gather><Hangup/>",
        xml(action),
        speak(&format!("{summary} {PROMPT}"), voice, audio)
    ))
}
/// Join the agent to the caller's conference.
pub fn connect(room: &str) -> String {
    response(format!(
        "<Dial><Conference startConferenceOnEnter=\"true\" endConferenceOnExit=\"true\" beep=\"false\">{}</Conference></Dial>",
        xml(room)
    ))
}
/// Speak voicemail, callback or a stale-leg notice and hang up.
pub fn goodbye(text: &str, voice: &str, audio: Option<&str>) -> String {
    response(format!("{}<Hangup/>", speak(text, voice, audio)))
}
