//! Streaming response execution, paced output, retry and cancellation cleanup.
//! The parent session owns sockets and state; this module owns one response attempt.
use super::*;

/// If the host drops a response future, release its provider/audio ownership.
struct ResponseCleanup {
    inner: std::sync::Weak<Inner>,
    epoch: u64,
    armed: bool,
}
impl Drop for ResponseCleanup {
    fn drop(&mut self) {
        if self.armed
            && let Some(inner) = self.inner.upgrade()
        {
            let epoch = self.epoch;
            tokio::spawn(async move {
                if inner.writer.epoch() == epoch {
                    let _ = inner.interrupt().await;
                }
            });
        }
    }
}
impl Inner {
    /// Compose provider output, phone packet pacing or browser buffering and delivery reports.
    pub(super) async fn respond(
        self: &Arc<Self>,
        response: Response,
        held: bool,
    ) -> Result<Option<DeliveryReport>> {
        if response.text.trim().is_empty() {
            return Ok(None);
        }
        let _response = self
            .response_lock
            .try_lock()
            .map_err(|_| Error::new("playback_busy"))?;
        if held {
            let mut state = self.state.lock().await;
            state.sync(self.clock.now());
            if let Some(floor) = &mut state.floor {
                floor.take_floor_from_machine(Some(response.audio_id.clone()));
            }
            state.collect();
            drop(state);
            self.dispatch().await?;
        } else {
            self.admit(&response).await?;
        }
        let mut report = DeliveryReport {
            audio_id: response.audio_id.clone(),
            status: "complete".into(),
            chunks_sent: 0,
            audio_ms: None,
            est_total_ms: estimate_total_ms(&response.text),
            mark_pending: false,
            sentences_total: 0,
            sentences_sent: 0,
            heard_text: String::new(),
            first_chunk_ms: None,
            first_chunk_t: None,
        };
        if self.config.mode == Mode::Text {
            let now = self.clock.now();
            self.writer.send(None,Frame::Json(json!({"type":"server.message","payload":{"text":response.text,"audio_id":response.audio_id}})),Receipt::None).await?;
            report.chunks_sent = 1;
            report.first_chunk_t = Some(now);
            report.est_total_ms = 0.0;
            return Ok(Some(report));
        }
        let mut plan = SentenceStream::new(
            response.text.clone(),
            response.audio_id.clone(),
            self.config.lead_ms,
        );
        let phone = self.config.mode == Mode::Twilio;
        let mut attempt = 0;
        let (mut output, mut cancellation, epoch, sid, mut cleanup) = {
            let _cancel = self.provider_start_guard().await;
            let mut tts = self.tts.lock().await;
            let tts = tts.as_mut().ok_or_else(|| Error::new("tts_closed"))?;
            if !tts.is_open() && !tts.reconnect().await {
                return Err(Error::new("tts_reconnect_failed"));
            }
            let mut state = self.state.lock().await;
            let sid = state.stream_sid.clone().unwrap_or_default();
            if phone && sid.is_empty() {
                return Err(Error::new("stream_not_started"));
            }
            state.begin(
                response.audio_id.clone(),
                response.text.clone(),
                response.protected,
                response.absorb,
                self.clock.now(),
            );
            state.playback.sentences = if phone {
                plan.sentences.clone()
            } else {
                vec![]
            };
            let epoch = self.writer.invalidate();
            let cleanup = ResponseCleanup {
                inner: Arc::downgrade(self),
                epoch,
                armed: true,
            };
            let cancellation = self.cancel.subscribe();
            drop(state);
            let segments = if phone {
                plan.sentences.clone()
            } else {
                vec![response.text.clone()]
            };
            let mut output = tts.stream_segments(segments.clone()).await;
            if phone && output.is_err() {
                time::sleep(Duration::from_millis(200)).await;
                if self.writer.epoch() != epoch {
                    return Err(Error::new("playback_cancelled"));
                }
                if tts.reconnect().await {
                    if self.writer.epoch() != epoch {
                        return Err(Error::new("playback_cancelled"));
                    }
                    output = tts.stream_segments(segments).await;
                    attempt = 1;
                }
            }
            (output?, cancellation, epoch, sid, cleanup)
        };
        // A clear may arrive while the initial Speak/Flush is being acknowledged.
        // Route that cancellation through normal delivery accounting as well.
        let setup: Result<()> = async {
        let _cancel = self.provider_start_guard().await;
        if self.writer.epoch() != epoch {
            return Err(Error::new("playback_cancelled"));
        }
        if let Some(audio) = &self.audio {
            audio.call(Command::ResetOutput).await?;
            audio.call(Command::StopTyping).await?;
            audio.call(Command::Speaking { value: true }).await?;
            audio
                .call(Command::GateStart {
                    now: self.clock.now(),
                })
                .await?;
        }
        if !phone {
            self.writer.send(Some(epoch),Frame::Json(json!({"type":"server.tts_stream_start","payload":{"audio_id":response.audio_id}})),Receipt::None).await?;
        }
        Ok(())
        }.await;
        let mut setup = Some(setup);
        let mut browser_pending = vec![];
        let mut segment = 0;
        let mut buffered_chunks = 0;
        let result = loop {
            if let Some(Err(error)) = setup.take() {
                break Err(error);
            }
            let result: Result<()> = async {
                loop {
                    let event = tokio::select! {
                        biased;
                        _ = cancellation.changed() => return Err(Error::new("playback_cancelled")),
                        event = output.recv() => event.ok_or_else(|| Error::new("tts_disconnected"))?,
                    };
                    match event {
                        TtsOutput::Audio { chunk } => {
                            if report.first_chunk_ms.is_none() {
                                report.first_chunk_ms = chunk.first_chunk_ms;
                            }
                            if phone {
                                if chunk.segment != segment {
                                    let tail: Vec<u8> = serde_json::from_value(
                                        self.audio.as_ref().unwrap().call(Command::Flush).await?
                                    ).map_err(|_| Error::new("invalid_audio_result"))?;
                                    if !tail.is_empty() {
                                        plan.push(segment, &tail).map_err(Error::new)?;
                                        self.dispatch_plan(&mut plan, epoch, &sid, &mut cancellation, &mut report).await?;
                                    }
                                    segment = chunk.segment;
                                }
                                let frames: Vec<Vec<u8>> = serde_json::from_value(
                                    self.audio.as_ref().unwrap().call(Command::Tts { data: chunk.data }).await?
                                ).map_err(|_| Error::new("invalid_audio_result"))?;
                                // Empty chunks still preserve sentence transitions.
                                plan.push(segment, &frames.concat()).map_err(Error::new)?;
                                self.dispatch_plan(&mut plan, epoch, &sid, &mut cancellation, &mut report).await?;
                            } else {
                                browser_pending.extend_from_slice(&chunk.data);
                                if browser_pending.len() >= 8192 {
                                    self.send_browser(epoch, &response.audio_id, &browser_pending, &mut report).await?;
                                    browser_pending.clear();
                                }
                            }
                        }
                        TtsOutput::Finished => {
                            if phone {
                                let tail: Vec<u8> = serde_json::from_value(
                                    self.audio.as_ref().unwrap().call(Command::Flush).await?
                                ).map_err(|_| Error::new("invalid_audio_result"))?;
                                if !tail.is_empty() {
                                    plan.push(segment, &tail).map_err(Error::new)?;
                                    self.dispatch_plan(&mut plan, epoch, &sid, &mut cancellation, &mut report).await?;
                                }
                                plan.finish().map_err(Error::new)?;
                                self.dispatch_plan(&mut plan, epoch, &sid, &mut cancellation, &mut report).await?;
                            } else {
                                if !browser_pending.is_empty() {
                                    self.send_browser(epoch, &response.audio_id, &browser_pending, &mut report).await?;
                                }
                                let end = json!({"type":"server.tts_stream_end","payload":{"audio_id":response.audio_id}});
                                self.writer.send(Some(epoch), Frame::Json(end), Receipt::None).await?;
                            }
                            return Ok(());
                        }
                        TtsOutput::Cancelled => return Err(Error::new("playback_cancelled")),
                        TtsOutput::Failed { error } => return Err(error),
                    }
                }
            }.await;
            buffered_chunks += plan.chunks_sent;
            let retryable = phone
                && attempt == 0
                && result.as_ref().err().is_some_and(|error| {
                    error.code.starts_with("tts_") || error.code.starts_with("provider_")
                });
            if !retryable {
                break result;
            }
            tokio::select! {biased;_=cancellation.changed()=>break Err(Error::new("playback_cancelled")),_=time::sleep(Duration::from_millis(200))=>{}}
            let _cancel = self.provider_start_guard().await;
            if self.writer.epoch() != epoch {
                break Err(Error::new("playback_cancelled"));
            }
            let mut provider = self.tts.lock().await;
            let provider = provider.as_mut().unwrap();
            if !provider.reconnect().await {
                break result;
            }
            if self.writer.epoch() != epoch {
                break Err(Error::new("playback_cancelled"));
            }
            let retry_text = if plan.chunks_sent == 0 {
                response.text.clone()
            } else {
                let remainder =
                    plan.sentences[plan.sentences_sent.min(plan.sentences.len())..].join(" ");
                if remainder.trim().is_empty() {
                    break Ok(());
                }
                format!("Sorry about that. {}", remainder)
            };
            let mut retry_plan =
                SentenceStream::new(retry_text, response.audio_id.clone(), self.config.lead_ms);
            match provider.stream_segments(retry_plan.sentences.clone()).await {
                Ok(next) => output = next,
                Err(error) => {
                    plan = retry_plan;
                    break Err(error);
                }
            }
            self.audio
                .as_ref()
                .unwrap()
                .call(Command::ResetOutput)
                .await?;
            self.state.lock().await.playback.sentences = retry_plan.sentences.clone();
            std::mem::swap(&mut plan, &mut retry_plan);
            segment = 0;
            attempt += 1;
        };
        if let Err(error) = result {
            report.status = if error.code == "playback_cancelled" {
                "truncated"
            } else {
                "failed"
            }
            .into();
            if error.code != "playback_cancelled" {
                let _ = self
                    .events
                    .send(json!({"kind":"provider_error","provider":"tts","error":error}));
                // Retain already buffered phone audio on provider failure, as Python does.
                // A dead caller socket still fails the response explicitly.
                if !phone {
                    self.writer.send(None,Frame::Json(json!({"type":"server.error","payload":{"code":"tts_error","message":"Voice synthesis failed","fatal":false}})),Receipt::None).await?;
                }
            }
        }
        if let Some(audio) = &self.audio {
            audio.call(Command::Speaking { value: false }).await?;
        }
        {
            let mut state = self.state.lock().await;
            state.playing = false;
            if phone && report.status != "truncated" {
                if buffered_chunks > 0 {
                    state.clocks.playout_sent(buffered_chunks as f64 * 20.0);
                } else {
                    state.playback.ended_at = self.clock.now();
                    state.playback.close_ledger(self.clock.now());
                    state.clocks.playout_end(is_turn_audio(&response.audio_id));
                    if let Some(line) = &mut state.line {
                        line.playout_end(self.clock.now());
                    }
                }
            }
            report.mark_pending = phone
                && state
                    .playback
                    .pending_marks
                    .contains_key(&format!("tts_end_{}", response.audio_id));
            report.audio_ms = if phone {
                Some(plan.chunks_sent as f64 * 20.0)
            } else {
                None
            };
            state.collect();
        }
        if phone {
            report.chunks_sent = plan.chunks_sent;
            report.sentences_total = plan.sentences.len();
            report.sentences_sent = plan.sentences_sent;
            report.heard_text = plan.heard_text();
        }
        if let Some(ListenerInput::Live(input)) = &self.stt {
            input
                .note_agent_said(if !report.heard_text.is_empty() {
                    report.heard_text.clone()
                } else if report.status == "complete" {
                    response.text.clone()
                } else {
                    String::new()
                })
                .await;
        }
        self.dispatch().await?;
        cleanup.armed = false;
        Ok(Some(report))
    }
    /// Acknowledgement follows real socket completion; failed sends never advance counters.
    async fn send_browser(
        &self,
        epoch: u64,
        id: &str,
        bytes: &[u8],
        report: &mut DeliveryReport,
    ) -> Result<()> {
        self.writer
            .send(Some(epoch), wire::browser_audio(id, bytes), Receipt::None)
            .await?;
        report.chunks_sent += 1;
        report.first_chunk_t.get_or_insert(self.clock.now());
        Ok(())
    }
    /// Execute the existing sentence plan with cancellation interrupting pacing sleeps.
    async fn dispatch_plan(
        &self,
        plan: &mut SentenceStream,
        epoch: u64,
        sid: &str,
        cancellation: &mut watch::Receiver<u64>,
        report: &mut DeliveryReport,
    ) -> Result<()> {
        while let Some(action) = plan.next(self.clock.now()) {
            if cancellation.has_changed().unwrap_or(true) {
                return Err(Error::new("playback_cancelled"));
            }
            match action {
                StreamAction::Wait { seconds } => {
                    tokio::select! {biased;_=cancellation.changed()=>return Err(Error::new("playback_cancelled")),_=time::sleep(Duration::from_secs_f64(seconds.max(0.0)))=>{}}
                    continue;
                }
                StreamAction::Media { data, .. } => {
                    self.writer
                        .send(Some(epoch), wire::media(sid, &data), Receipt::Media(data))
                        .await?;
                    report.first_chunk_t.get_or_insert(self.clock.now());
                }
                StreamAction::Mark { name, snippet } => {
                    self.writer
                        .send(
                            Some(epoch),
                            Frame::Json(
                                json!({"event":"mark","streamSid":sid,"mark":{"name":name}}),
                            ),
                            Receipt::Mark { name, snippet },
                        )
                        .await?
                }
                StreamAction::CancelProvider | StreamAction::Clear => {
                    return Err(Error::new("playback_cancelled"));
                }
            }
            plan.acknowledge();
        }
        Ok(())
    }
}
