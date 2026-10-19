//! Transfer execution and callbacks. No lock spans provider I/O. Every callback
//! transition is an atomic CAS; each dial URL identifies its exact attempt.
use crate::{Error, Result, Store, twiml, utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::task::JoinHandle;
use vogon_contracts::EscalationOrder;

const TTL: Duration = Duration::from_secs(4 * 3600);
/// Provider settings owned by the service; intentionally not Debug/Serialize.
#[derive(Clone)]
pub struct Config {
    pub endpoint: String,
    pub account: String,
    pub token: String,
    pub from: String,
    pub public_base: String,
    pub say_voice: String,
    pub tts_endpoint: String,
    pub tts_key: String,
    pub timeout: Duration,
    pub retry_delays: Vec<Duration>,
}
/// One transfer's authenticated application destination and physical caller identity.
#[derive(Clone, Serialize, Deserialize)]
pub struct Binding {
    pub call_id: String,
    pub caller_sid: String,
    pub report_url: String,
    pub report_token: String,
}
/// Persisted state survives the media socket and can be advanced on another worker.
#[derive(Clone, Serialize, Deserialize)]
struct Transfer {
    binding: Binding,
    order: EscalationOrder,
    room: String,
    phase: String,
    attempt: usize,
    leg: String,
    answered: bool,
    counted: Vec<String>,
    hold_at: f64,
    connected_at: f64,
    facts: Vec<Value>,
    audio: BTreeMap<String, String>,
    rendering: Vec<String>,
    failed_audio: BTreeMap<String, String>,
}
impl Transfer {
    /// Allocate transfer-local sequence numbers atomically with the transition.
    fn fact(&mut self, event: &str, detail: &str) {
        self.facts.push(json!({"kind":"escalation_leg","escalation_id":self.order.escalation_id,"seq":self.facts.len()+1,"event":event,"target":self.order.targets.get(self.attempt).map(|t|t.label.as_str()).unwrap_or(""),"attempt":self.attempt,"agent_call_sid":self.leg,"detail":detail,"at_utc":utc()}));
    }
    /// A callback itself proves an agent leg exists, even before REST returns its SID.
    fn bind(&mut self, leg: &str) {
        self.leg = leg.into();
        if !self.counted.iter().any(|s| s == leg) {
            self.counted.push(leg.into());
            self.fact("dialled", "");
        }
    }
    /// Terminal tombstones prevent duplicate resolutions or repeated execution.
    fn resolve(&mut self, result: &str, detail: &str) {
        let now = utc();
        let detail = if self.failed_audio.is_empty() {
            detail.to_owned()
        } else {
            format!("{detail}; voice: twilio <Say> fallback")
        };
        self.facts.push(json!({"kind":"escalation_resolved","escalation_id":self.order.escalation_id,"seq":self.facts.len()+1,"result":result,"connected_target":if self.connected_at>0. {self.order.targets[self.attempt].label.as_str()}else{""},"attempts":self.counted.len(),"hold_sec":if self.hold_at>0.{((if self.connected_at>0.{self.connected_at}else{now})-self.hold_at).max(0.)}else{0.},"talk_sec":if self.connected_at>0.{(now-self.connected_at).max(0.)}else{0.},"detail":detail,"at_utc":now}));
        self.phase = "finished".into();
    }
}
/// Shared network pools/store and owned background render/report tasks. All call
/// state/counters are in Transfer. Failed report delivery is observable and bounded.
pub struct Manager {
    pub config: Config,
    pub store: Store,
    http: reqwest::Client,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    pub report_failures: AtomicU64,
}
impl Manager {
    /// Create without dialing or making paid requests.
    pub fn new(config: Config, store: Store) -> Result<Arc<Self>> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .map_err(|_| Error("transfer_http_init"))?;
        Ok(Arc::new(Self {
            config,
            store,
            http,
            tasks: Mutex::new(vec![]),
            report_failures: AtomicU64::new(0),
        }))
    }
    /// Own each background task until completion or explicit shutdown drain.
    fn spawn(&self, task: impl std::future::Future<Output = ()> + Send + 'static) {
        let mut tasks = self.tasks.lock().unwrap();
        tasks.retain(|t| !t.is_finished());
        tasks.push(tokio::spawn(task));
    }
    /// Await pending reports/renders; callers impose the process shutdown deadline.
    pub async fn drain(&self) {
        loop {
            let tasks = std::mem::take(&mut *self.tasks.lock().unwrap());
            if tasks.is_empty() {
                break;
            }
            for t in tasks {
                let _ = t.await;
            }
        }
    }
    /// Namespace separate from Python's incompatible hash representation.
    fn key(sid: &str) -> String {
        format!("gw:rust:esc:{sid}")
    }
    async fn get(&self, sid: &str) -> Result<Option<Transfer>> {
        self.store
            .get(&Self::key(sid))
            .await?
            .map(|v| serde_json::from_slice(&v).map_err(|_| Error("invalid_transfer_state")))
            .transpose()
    }
    /// Atomically mutate and enqueue only the facts from the winning transition.
    async fn change(
        self: &Arc<Self>,
        sid: &str,
        f: impl Fn(&mut Transfer) -> bool,
    ) -> Result<Option<Transfer>> {
        for _ in 0..64 {
            let Some(raw) = self.store.get(&Self::key(sid)).await? else {
                return Ok(None);
            };
            let mut state: Transfer =
                serde_json::from_slice(&raw).map_err(|_| Error("invalid_transfer_state"))?;
            let first = state.facts.len();
            if !f(&mut state) {
                return Ok(None);
            }
            let next = serde_json::to_vec(&state).map_err(|_| Error("invalid_transfer_state"))?;
            if self
                .store
                .cas(&Self::key(sid), Some(&raw), Some(&next), TTL)
                .await?
            {
                for fact in &state.facts[first..] {
                    self.report(state.binding.clone(), fact.clone());
                }
                if state.phase == "finished" {
                    for (line, token) in &state.audio {
                        if matches!(line.as_str(), "voicemail" | "callback") {
                            continue;
                        }
                        let key = format!("gw:rust:esc:audio:{token}");
                        if let Ok(Some(bytes)) = self.store.get(&key).await {
                            let _ = self.store.cas(&key, Some(&bytes), None, TTL).await;
                        }
                    }
                }
                return Ok(Some(state));
            }
        }
        Err(Error("transfer_contention"))
    }
    /// One stable request ID across bounded post-call retries; 409 means finalizing.
    fn report(self: &Arc<Self>, binding: Binding, fact: Value) {
        if binding.report_url.is_empty() || fact["escalation_id"] == "" {
            return;
        }
        let this = self.clone();
        self.spawn(async move {
            let body=json!({"call_id":binding.call_id,"request_id":uuid::Uuid::new_v4().simple().to_string(),"events":[fact]});
            for delay in std::iter::once(Duration::ZERO).chain(this.config.retry_delays.iter().copied()) {
                tokio::time::sleep(delay).await;
                let response=this.http.post(format!("{}/record_transfer",binding.report_url.trim_end_matches('/'))).bearer_auth(&binding.report_token).json(&body).timeout(Duration::from_secs(5)).send().await;
                if let Ok(r)=response {
                    if r.status().is_success(){return;}
                    if r.status().as_u16()<500 && !matches!(r.status().as_u16(),409|429){break;}
                }
            }
            this.report_failures.fetch_add(1,Ordering::Relaxed);
            eprintln!("{{\"event\":\"transfer_fact_delivery_failed\"}}");
        });
    }
    /// Bounded native REST call. Never retries an ambiguous dial automatically.
    async fn rest(&self, sid: Option<&str>, form: Vec<(&str, String)>) -> Result<Value> {
        if self.config.account.is_empty()
            || self.config.token.is_empty()
            || self.config.from.is_empty()
        {
            return Err(Error("twilio_not_configured"));
        }
        let path = sid.map(|s| format!("/{s}")).unwrap_or_default();
        let response = self
            .http
            .post(format!(
                "{}/Accounts/{}/Calls{path}.json",
                self.config.endpoint.trim_end_matches('/'),
                self.config.account
            ))
            .basic_auth(&self.config.account, Some(&self.config.token))
            .form(&form)
            .timeout(self.config.timeout)
            .send()
            .await
            .map_err(|_| Error("transfer_rest_ambiguous"))?;
        if !response.status().is_success() {
            return Err(Error(if response.status().is_server_error() {
                "transfer_rest_ambiguous"
            } else {
                "transfer_rest_rejected"
            }));
        }
        response
            .json()
            .await
            .map_err(|_| Error("transfer_rest_ambiguous"))
    }
    /// Unique attempt URLs reject callbacks from a previous in-flight dial.
    fn callback(&self, kind: &str, sid: &str, attempt: usize) -> String {
        format!(
            "{}/v1/escalation/{kind}/{sid}?attempt={attempt}",
            self.config.public_base.trim_end_matches('/')
        )
    }
    fn audio_url(&self, s: &Transfer, line: &str) -> Option<String> {
        s.audio.get(line).map(|t| {
            format!(
                "{}/v1/escalation/audio/{t}.mp3",
                self.config.public_base.trim_end_matches('/')
            )
        })
    }
    /// Start persona renders before handoff playback; this makes no Twilio request.
    pub async fn prepare(
        self: &Arc<Self>,
        binding: Binding,
        order: EscalationOrder,
        voice: String,
    ) -> Result<()> {
        if order.targets.is_empty() {
            return Ok(());
        }
        if !safe_id(&binding.caller_sid)
            || !order.agent_timeout_sec.is_finite()
            || !(1.0..=600.0).contains(&order.agent_timeout_sec)
        {
            return Err(Error("invalid_transfer_order"));
        }
        if self.config.public_base.is_empty() {
            return Err(Error("transfer_public_url_required"));
        }
        let sid = binding.caller_sid.clone();
        let state = Transfer {
            room: format!("esc_{sid}"),
            binding,
            order,
            phase: "preparing".into(),
            attempt: 0,
            leg: String::new(),
            answered: false,
            counted: vec![],
            hold_at: 0.,
            connected_at: 0.,
            facts: vec![],
            audio: BTreeMap::new(),
            rendering: vec![
                "hold".into(),
                "whisper".into(),
                "voicemail".into(),
                "callback".into(),
            ],
            failed_audio: BTreeMap::new(),
        };
        if !self
            .store
            .cas(
                &Self::key(&sid),
                None,
                Some(&serde_json::to_vec(&state).unwrap()),
                TTL,
            )
            .await?
        {
            let old = self.get(&sid).await?.ok_or(Error("transfer_disappeared"))?;
            if old.order != state.order {
                return Err(Error("transfer_already_exists"));
            }
            return Ok(());
        }
        self.render(&state, voice);
        Ok(())
    }
    /// Redirect only after handoff receipts; one CAS owner performs the hold/dial.
    pub async fn execute(
        self: &Arc<Self>,
        binding: Binding,
        order: EscalationOrder,
        voice: String,
    ) -> Result<&'static str> {
        if order.targets.is_empty() {
            return Ok("no_targets");
        }
        let sid = binding.caller_sid.clone();
        self.prepare(binding, order, voice).await?;
        if self
            .change(&sid, |s| {
                if s.phase != "preparing" {
                    return false;
                }
                s.phase = "holding".into();
                true
            })
            .await?
            .is_none()
        {
            return Ok(
                if self
                    .get(&sid)
                    .await?
                    .is_some_and(|s| matches!(s.phase.as_str(), "ringing" | "connected"))
                {
                    "initiated"
                } else {
                    "failed"
                },
            );
        }
        let state = self
            .wait_audio(&sid, "hold", Duration::from_secs(3))
            .await?
            .ok_or(Error("transfer_disappeared"))?;
        if state.phase != "holding" {
            return Ok("failed");
        }
        let hold = twiml::hold(
            default(&state.order.hold_message, twiml::HOLD),
            &state.room,
            &state.order.hold_music_url,
            &self.config.say_voice,
            self.audio_url(&state, "hold").as_deref(),
        );
        if self.rest(Some(&sid), vec![("Twiml", hold)]).await.is_err() {
            self.change(&sid, |s| {
                if s.phase != "holding" {
                    return false;
                }
                s.resolve("failed", "hold redirect failed");
                true
            })
            .await?;
            return Ok("failed");
        }
        if self
            .change(&sid, |s| {
                if s.phase != "holding" {
                    return false;
                }
                s.phase = "dialing".into();
                s.hold_at = utc();
                true
            })
            .await?
            .is_none()
        {
            return Ok("failed");
        }
        self.dial(&sid).await
    }
    /// Try subsequent destinations on explicit REST rejection; no global call lock.
    async fn dial(self: &Arc<Self>, sid: &str) -> Result<&'static str> {
        loop {
            let Some(s) = self.get(sid).await? else {
                return Ok("failed");
            };
            if s.phase != "dialing" {
                return Ok(if s.phase == "finished" {
                    "failed"
                } else {
                    "initiated"
                });
            }
            let idx = s.attempt;
            let result = self
                .rest(
                    None,
                    vec![
                        ("To", s.order.targets[idx].number.clone()),
                        ("From", self.config.from.clone()),
                        ("Url", self.callback("whisper", sid, idx)),
                        ("Method", "GET".into()),
                        ("StatusCallback", self.callback("agent-status", sid, idx)),
                        ("StatusCallbackMethod", "POST".into()),
                        ("StatusCallbackEvent", "completed".into()),
                        ("Timeout", (s.order.agent_timeout_sec as u64).to_string()),
                    ],
                )
                .await;
            if result
                .as_ref()
                .is_err_and(|e| e.0 == "transfer_rest_ambiguous")
                || result
                    .as_ref()
                    .is_ok_and(|v| !v["sid"].as_str().is_some_and(safe_id))
            {
                self.change(sid, |s| {
                    if s.phase != "dialing" || s.attempt != idx {
                        return false;
                    }
                    s.fact(
                        "dial_failed",
                        "ambiguous provider result; no automatic redial",
                    );
                    s.phase = "fallback".into();
                    true
                })
                .await?;
                self.finish_callback(sid, false).await?;
                return Ok("failed");
            }
            let leg = result
                .ok()
                .and_then(|v| v["sid"].as_str().filter(|s| safe_id(s)).map(str::to_owned));
            if let Some(leg) = leg {
                let placed = self
                    .change(sid, |s| {
                        if s.attempt != idx
                            || s.phase == "finished"
                            || (!s.leg.is_empty() && s.leg != leg)
                        {
                            return false;
                        }
                        s.leg = leg.clone();
                        if s.phase == "dialing" {
                            s.phase = "ringing".into();
                        }
                        if !s.counted.contains(&leg) {
                            s.counted.push(leg.clone());
                            s.fact("dialled", "");
                        }
                        true
                    })
                    .await?;
                if placed.is_none() {
                    let _ = self
                        .rest(Some(&leg), vec![("Status", "canceled".into())])
                        .await;
                }
                return Ok(
                    if placed.is_some()
                        || self
                            .get(sid)
                            .await?
                            .is_some_and(|s| matches!(s.phase.as_str(), "ringing" | "connected"))
                    {
                        "initiated"
                    } else {
                        "failed"
                    },
                );
            }
            let next = self
                .change(sid, |s| {
                    if s.phase != "dialing" || s.attempt != idx {
                        return false;
                    }
                    s.fact("dial_failed", "provider dial failed");
                    if idx + 1 < s.order.targets.len() {
                        s.attempt += 1;
                    } else {
                        s.phase = "fallback".into();
                    }
                    true
                })
                .await?;
            let Some(next) = next else {
                return Ok("failed");
            };
            if next.phase == "fallback" {
                self.finish_callback(sid, false).await?;
                return Ok("failed");
            }
        }
    }
    /// Bind an early answered callback to the indexed attempt, then await persona audio.
    pub async fn whisper(self: &Arc<Self>, sid: &str, leg: &str, attempt: usize) -> Result<String> {
        let accepted = self
            .change(sid, |s| {
                if !current(s, leg, attempt) {
                    return false;
                }
                if s.answered {
                    return false;
                }
                s.bind(leg);
                s.answered = true;
                s.fact("answered", "");
                true
            })
            .await?;
        let state = if accepted.is_some() {
            self.wait_audio(sid, "whisper", Duration::from_secs(5))
                .await?
        } else {
            self.get(sid).await?
        };
        let Some(s) = state.filter(|s| current(s, leg, attempt)) else {
            return Ok(self.gone());
        };
        Ok(twiml::whisper(
            default(&s.order.whisper_text, "Incoming transfer."),
            &self.callback("agent-keypress", sid, attempt),
            &self.config.say_voice,
            self.audio_url(&s, "whisper").as_deref(),
        ))
    }
    /// A duplicate keypress returns the same bridge without issuing another fact.
    pub async fn keypress(
        self: &Arc<Self>,
        sid: &str,
        leg: &str,
        attempt: usize,
        digits: &str,
    ) -> Result<String> {
        if digits.is_empty() {
            return Ok(self.gone());
        }
        let state = self
            .change(sid, |s| {
                if !current(s, leg, attempt) || s.phase == "connected" {
                    return false;
                }
                s.bind(leg);
                s.phase = "connected".into();
                s.connected_at = utc();
                s.fact("connected", "");
                true
            })
            .await?;
        let s = if state.is_some() {
            state
        } else {
            self.get(sid).await?
        };
        Ok(
            s.filter(|s| s.phase == "connected" && s.leg == leg && s.attempt == attempt)
                .map(|s| twiml::connect(&s.room))
                .unwrap_or_else(|| self.gone()),
        )
    }
    /// Consume one terminal agent callback; CAS chooses one next-leg/fallback owner.
    pub async fn status(
        self: &Arc<Self>,
        sid: &str,
        leg: &str,
        attempt: usize,
        status: &str,
    ) -> Result<bool> {
        if !matches!(
            status,
            "completed" | "busy" | "no-answer" | "failed" | "canceled"
        ) {
            return Ok(false);
        }
        let next = self
            .change(sid, |s| {
                if !current(s, leg, attempt) {
                    return false;
                }
                s.bind(leg);
                if s.phase == "connected" {
                    s.resolve("connected", "agent hung up");
                    return true;
                }
                s.fact(
                    match status {
                        "no-answer" => "no_answer",
                        "completed" => "no_keypress",
                        x => x,
                    },
                    "",
                );
                if s.attempt + 1 < s.order.targets.len() {
                    s.attempt += 1;
                    s.phase = "dialing".into();
                    s.leg.clear();
                    s.answered = false;
                } else {
                    s.phase = "fallback".into();
                }
                true
            })
            .await?;
        let Some(next) = next else { return Ok(false) };
        if next.phase == "dialing" {
            self.dial(sid).await?;
        } else if next.phase == "fallback" {
            self.finish_callback(sid, true).await?;
        }
        Ok(true)
    }
    /// Discard unused renders when a handoff was canceled before provider redirection.
    pub async fn discard_prepared(self: &Arc<Self>, sid: &str) -> Result<()> {
        self.change(sid, |s| {
            if s.phase != "preparing" {
                return false;
            }
            s.phase = "finished".into();
            true
        })
        .await?;
        Ok(())
    }
    /// Caller departure cancels a ringing leg or ends an answered unbridged leg.
    pub async fn caller_gone(self: &Arc<Self>, sid: &str) -> Result<()> {
        let changed = self
            .change(sid, |s| {
                if s.phase == "finished" {
                    return false;
                }
                let result = if s.phase == "connected" {
                    "connected"
                } else {
                    "caller_abandoned"
                };
                s.resolve(result, "caller hung up");
                true
            })
            .await?;
        if let Some(s) = changed
            && s.connected_at == 0.
            && !s.leg.is_empty()
        {
            let _ = self
                .rest(
                    Some(&s.leg),
                    vec![(
                        "Status",
                        if s.answered { "completed" } else { "canceled" }.into(),
                    )],
                )
                .await;
        }
        Ok(())
    }
    /// Complete the fallback once; retained audio expires after Twilio can fetch it.
    async fn finish_callback(self: &Arc<Self>, sid: &str, voicemail: bool) -> Result<()> {
        let Some(s) = self
            .change(sid, |s| {
                if s.phase != "fallback" {
                    return false;
                }
                s.phase = "closing".into();
                true
            })
            .await?
        else {
            return Ok(());
        };
        if voicemail {
            let result = self
                .rest(
                    None,
                    vec![
                        ("To", s.order.targets.last().unwrap().number.clone()),
                        ("From", self.config.from.clone()),
                        (
                            "Twiml",
                            twiml::goodbye(
                                &s.order.voicemail_script,
                                &self.config.say_voice,
                                self.audio_url(&s, "voicemail").as_deref(),
                            ),
                        ),
                        ("MachineDetection", "DetectMessageEnd".into()),
                    ],
                )
                .await;
            self.change(sid, |s| {
                if s.phase != "closing" {
                    return false;
                }
                s.fact(
                    if result.is_ok() {
                        "voicemail_left"
                    } else {
                        "voicemail_failed"
                    },
                    "",
                );
                true
            })
            .await?;
        }
        if let Some(s) = self
            .change(sid, |s| {
                if s.phase != "closing" {
                    return false;
                }
                s.resolve(
                    if voicemail {
                        "fallback_callback"
                    } else {
                        "failed"
                    },
                    "",
                );
                true
            })
            .await?
        {
            let _ = self
                .rest(
                    Some(sid),
                    vec![(
                        "Twiml",
                        twiml::goodbye(
                            default(&s.order.callback_message, twiml::CALLBACK),
                            &self.config.say_voice,
                            self.audio_url(&s, "callback").as_deref(),
                        ),
                    )],
                )
                .await;
        }
        Ok(())
    }
    fn gone(&self) -> String {
        twiml::goodbye(
            "The caller is no longer waiting. Goodbye.",
            &self.config.say_voice,
            None,
        )
    }
    /// Render independent transfer lines concurrently; absent credentials use Say.
    fn render(self: &Arc<Self>, s: &Transfer, voice: String) {
        let lines = [
            (
                "hold",
                default(&s.order.hold_message, twiml::HOLD).to_owned(),
            ),
            (
                "whisper",
                format!(
                    "{} {}",
                    default(&s.order.whisper_text, "Incoming transfer."),
                    twiml::PROMPT
                ),
            ),
            ("voicemail", s.order.voicemail_script.clone()),
            (
                "callback",
                default(&s.order.callback_message, twiml::CALLBACK).to_owned(),
            ),
        ];
        for (line, text) in lines {
            let this = self.clone();
            let sid = s.binding.caller_sid.clone();
            let voice = voice.clone();
            self.spawn(async move {
                let mut rendered: Result<Vec<u8>> = async {
                    if text.trim().is_empty() || voice.is_empty() || this.config.tts_key.is_empty()
                    {
                        return Err(Error("voice_unavailable"));
                    }
                    let mut response = this
                        .http
                        .post(&this.config.tts_endpoint)
                        .query(&[("model", voice.as_str()), ("encoding", "mp3")])
                        .header("Authorization", format!("Token {}", this.config.tts_key))
                        .json(&json!({"text":text}))
                        .timeout(Duration::from_secs(30))
                        .send()
                        .await
                        .map_err(|_| Error("voice_render_failed"))?;
                    if !response.status().is_success() {
                        return Err(Error("voice_render_failed"));
                    }
                    let mut bytes = Vec::new();
                    while let Some(chunk) = response
                        .chunk()
                        .await
                        .map_err(|_| Error("voice_render_failed"))?
                    {
                        if bytes.len() + chunk.len() > 8 * 1024 * 1024 {
                            return Err(Error("invalid_voice_audio"));
                        }
                        bytes.extend_from_slice(&chunk);
                    }
                    if bytes.is_empty() {
                        return Err(Error("invalid_voice_audio"));
                    }
                    Ok(bytes)
                }
                .await;
                let token = uuid::Uuid::new_v4().simple().to_string();
                let key = format!("gw:rust:esc:audio:{token}");
                if let Ok(bytes) = &rendered
                    && !matches!(
                        this.store
                            .cas(&key, None, Some(bytes), Duration::from_secs(600))
                            .await,
                        Ok(true)
                    )
                {
                    rendered = Err(Error("voice_store_failed"));
                }
                let result = this
                    .change(&sid, |s| {
                        if s.phase == "finished" && !matches!(line, "callback" | "voicemail") {
                            return false;
                        }
                        s.rendering.retain(|n| n != line);
                        if rendered.is_ok() {
                            s.audio.insert(line.into(), token.clone());
                        } else {
                            s.failed_audio
                                .insert(line.into(), "render unavailable".into());
                        }
                        true
                    })
                    .await;
                if !matches!(&result, Ok(Some(_)))
                    && let Ok(Some(bytes)) = this.store.get(&key).await
                {
                    let _ = this.store.cas(&key, Some(&bytes), None, TTL).await;
                }
                if result.is_err() {
                    eprintln!("{{\"event\":\"transfer_voice_store_failed\"}}");
                }
            });
        }
    }
    async fn wait_audio(&self, sid: &str, line: &str, limit: Duration) -> Result<Option<Transfer>> {
        let end = tokio::time::Instant::now() + limit;
        loop {
            let s = self.get(sid).await?;
            if s.as_ref().is_none_or(|s| {
                s.phase == "finished"
                    || s.audio.contains_key(line)
                    || !s.rendering.iter().any(|n| n == line)
            }) || tokio::time::Instant::now() >= end
            {
                return Ok(s);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    /// Unguessable MP3 capability URL; no caller identity is accepted as an audio key.
    pub async fn audio(&self, token: &str) -> Result<Option<Vec<u8>>> {
        if token.len() != 32 || !token.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Ok(None);
        }
        self.store.get(&format!("gw:rust:esc:audio:{token}")).await
    }
}
/// Only provider-safe path identifiers may reach REST URLs.
fn safe_id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}
fn default<'a>(s: &'a str, fallback: &'a str) -> &'a str {
    if s.is_empty() { fallback } else { s }
}
/// Strict indexed matching, including callbacks racing a REST dial response.
fn current(s: &Transfer, leg: &str, attempt: usize) -> bool {
    safe_id(leg)
        && s.attempt == attempt
        && matches!(s.phase.as_str(), "dialing" | "ringing" | "connected")
        && (s.leg.is_empty() || s.leg == leg)
}
