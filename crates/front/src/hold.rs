//! Filler hold scheduling, independent of the future application Turn API loop.
//! End waits for in-flight speech naturally; shutdown aborts owned waiting work.
use crate::{Result, source::Source};
use rand::{Rng, SeedableRng, seq::SliceRandom};
use serde::Serialize;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{self, Duration},
};
use vogon_playback::delivery::DeliveryReport;
/// Host speech path; it must publish the same delivery facts as ordinary responses.
pub trait Speaker: Send + Sync + 'static {
    /// Send one filler. The hold waits for completion without cutting it mid-sentence.
    fn speak(
        &self,
        text: String,
        audio_id: String,
    ) -> Pin<Box<dyn Future<Output = Result<Option<DeliveryReport>>> + Send + '_>>;
}
/// Existing Turn API canned filler policy; all durations are milliseconds.
pub use vogon_contracts::types::FillerPolicy as Policy;
/// Source timing resolved once for a call; canned holds use their own policy durations.
#[derive(Clone, Copy, Debug)]
pub struct Timing {
    pub beat_ms: u64,
    pub empty_ms: Option<u64>,
    pub again_ms: u64,
}
impl From<&crate::config::Settings> for Timing {
    /// Retain the gateway's model, intentional-empty and follow-up deadlines.
    fn from(settings: &crate::config::Settings) -> Self {
        Self {
            beat_ms: settings.beat_after_ms,
            empty_ms: Some(settings.beat_after_empty_ms),
            again_ms: settings.wait_again_after_ms,
        }
    }
}
impl Default for Timing {
    fn default() -> Self {
        Self::from(&crate::config::Settings::default())
    }
}
/// Delivery facts the orchestrator needs when deciding whether to execute the desk answer.
#[derive(Clone, Debug, Default, Serialize, PartialEq)]
pub struct Report {
    pub fillers_played: u64,
    pub interrupted: bool,
}
/// One active reasoning hold with a call-local monotonically increasing canned ID counter.
pub struct Hold {
    task: Option<JoinHandle<(Report, Result<()>)>>,
    end: Option<watch::Sender<bool>>,
    count: Arc<AtomicU64>,
    last_report: Report,
}
impl Default for Hold {
    fn default() -> Self {
        Self {
            task: None,
            end: None,
            count: Arc::new(AtomicU64::new(0)),
            last_report: Report::default(),
        }
    }
}
impl Drop for Hold {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
async fn wait(seconds: f64, end: &mut watch::Receiver<bool>) -> bool {
    if *end.borrow() {
        return false;
    }
    tokio::select! {biased;_=end.changed()=>false,_=time::sleep(Duration::from_secs_f64(seconds.max(0.)))=>true}
}
impl Hold {
    /// Start once; an already-open hold is reused. A source supersedes canned policy.
    pub fn begin(
        &mut self,
        policy: Option<Policy>,
        source: Option<Arc<Source>>,
        speaker: Arc<dyn Speaker>,
        timing: Timing,
        seed: u64,
    ) -> bool {
        if self.task.is_some() {
            return true;
        }
        if policy.is_none() && source.is_none() {
            return false;
        }
        let Timing {
            beat_ms,
            empty_ms,
            again_ms,
        } = timing;
        let (tx, mut end) = watch::channel(false);
        self.end = Some(tx);
        let counter = self.count.clone();
        self.last_report = Report::default();
        self.task = Some(tokio::spawn(async move {
            let mut report = Report::default();
            let result=async {
            if let Some(source) = source {
                counter.fetch_add(1, Ordering::Relaxed);
                let first = tokio::select! {biased;_=end.changed()=>return Ok(()),line=source.hold_line(beat_ms as f64/1000.,empty_ms.map(|n|n as f64/1000.))=>line};
                if first.0.is_empty() || *end.borrow() {
                    return Ok(());
                }
                for attempt in 0..2 {
                    let (text, id) = if attempt == 0 {
                        first.clone()
                    } else {
                        if !wait(again_ms as f64 / 1000., &mut end).await {
                            return Ok(());
                        }
                        source.wait_line().await
                    };
                    if text.is_empty() || *end.borrow() {
                        return Ok(());
                    }
                    let dr = speaker.speak(text.clone(), id).await?;
                    report.fillers_played += 1;
                    source.note_played(&text).await;
                    if dr.is_some_and(|dr| dr.status == "truncated") {
                        report.interrupted = true;
                        return Ok(());
                    }
                }
            } else if let Some(policy) = policy {
                if !wait(policy.first_after_ms / 1000., &mut end).await {
                    return Ok(());
                }
                let count = counter.fetch_add(1, Ordering::Relaxed) + 1;
                let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
                let mut pool = if policy.continuations.is_empty() {
                    policy.phrases.clone()
                } else {
                    policy.continuations.clone()
                };
                let mut queue = vec![];
                for index in 0..policy.max.max(0) {
                    if *end.borrow() {
                        break;
                    }
                    let phrase = if index == 0 && !policy.phrases.is_empty() {
                        policy.phrases[rng.random_range(0..policy.phrases.len())].clone()
                    } else {
                        if queue.is_empty() {
                            pool.shuffle(&mut rng);
                            queue = pool.clone();
                        }
                        if queue.is_empty() {
                            break;
                        }
                        queue.remove(0)
                    };
                    let dr = speaker
                        .speak(
                            phrase,
                            format!("filler-{count}-{}", report.fillers_played + 1),
                        )
                        .await?;
                    report.fillers_played += 1;
                    if dr.is_some_and(|dr| dr.status == "truncated") {
                        report.interrupted = true;
                        break;
                    }
                    if !wait(policy.interval_ms / 1000., &mut end).await {
                        break;
                    }
                }
            }
            Ok(())
            }.await;
            (report, result)
        }));
        true
    }
    /// Abort and join a hold during call shutdown, including an in-flight speech future.
    /// Ordinary turn completion uses end so current speech can finish naturally.
    pub async fn cancel(&mut self) {
        self.end.take();
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }
    /// Stop waiting/repeats; allow any speech already in progress to finish before returning.
    pub async fn end(&mut self) -> Result<Report> {
        if let Some(end) = self.end.take() {
            end.send_replace(true);
        }
        if let Some(task) = self.task.as_mut() {
            let outcome = task.await;
            self.task = None;
            let (report, result) = outcome.map_err(|_| crate::Error::new("hold_task_failed"))?;
            self.last_report = report;
            result?;
        }
        Ok(self.last_report.clone())
    }
}
