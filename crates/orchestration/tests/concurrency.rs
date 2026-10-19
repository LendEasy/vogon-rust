//! Gated tests establish progress/order while peers are blocked; no speedup is inferred.
mod support;
use serde_json::json;
use std::sync::{Arc, atomic::Ordering};
use support::*;
use vogon_contracts::{EndCallResponse, ProcessEventsResponse};
use vogon_orchestration::{Error, port::Input};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminal_speech_waits_for_playout_while_input_remains_live() {
    for resume in [false, true] {
        let mut h = basic();
        let (gate, mut waiting) = tokio::sync::mpsc::unbounded_channel();
        *h.port.heard_gate.lock().unwrap() = Some(gate);
        h.events
            .send(event("utterance_final", "goodbye", 1))
            .unwrap();
        let request = recv(&mut h.requests).await;
        let mut final_response = response("Goodbye");
        final_response.action = "end_call".into();
        final_response.end_reason = Some("complete".into());
        request.reply.send(Ok(final_response)).unwrap();
        played(recv(&mut h.speech).await, "complete");
        let heard = recv(&mut waiting).await;
        h.input.send(Input::Binary(vec![0; 160])).await.unwrap();
        until(|| h.port.input_frames.load(Ordering::SeqCst) == 1).await;
        assert!(h.ends.try_recv().is_err());
        assert_eq!(h.port.closed.load(Ordering::SeqCst), 0);
        if resume {
            h.events.send(event("utterance_resumed", "", 2)).unwrap();
        }
        heard.send(true).unwrap();
        if resume {
            let r = recv(&mut h.requests).await;
            assert_eq!(
                json!(r.request.events.last().unwrap())["kind"],
                "utterance_resumed"
            );
            r.reply.send(Ok(empty())).unwrap();
            assert!(h.ends.try_recv().is_err());
            assert!(end(h).await.errors.is_empty());
        } else {
            let e = recv(&mut h.ends).await;
            assert_eq!(e.request.reason, "complete");
            e.reply.send(Ok(EndCallResponse {})).unwrap();
            assert!(h.task.await.unwrap().errors.is_empty());
        }
    }
}

#[tokio::test(start_paused = true)]
async fn clock_events_and_receipts_do_not_change_speech_revision_or_cancel_reasoning_hold() {
    let mut h = start(
        json!([]),
        json!({"persona_voice":"test","filler":{"phrases":["Please wait"],"first_after_ms":10,"interval_ms":100,"max":1}}),
        None,
        |_| {},
    );
    h.events
        .send(event("utterance_final", "question", 1))
        .unwrap();
    let pending = recv(&mut h.requests).await;
    h.events
        .send(json!({"kind":"drain_cleared","audio_id":"earlier","at":2.,"seq":2}))
        .unwrap();
    h.events.send(json!({"kind":"duration_threshold","threshold_id":"limit","elapsed_sec":10.,"at":10.,"seq":3})).unwrap();
    let filler = recv(&mut h.speech).await;
    assert_eq!(filler.command.text, "Please wait");
    assert_eq!(h.port.paused.load(Ordering::SeqCst), 1);
    played(filler, "complete");
    pending.reply.send(Ok(empty())).unwrap();
    let duration = recv(&mut h.requests).await;
    assert_eq!(
        json!(duration.request.events.last().unwrap())["kind"],
        "duration_threshold"
    );
    assert!(
        json!(duration.request.events)
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["kind"] == "drain_cleared")
    );
    duration
        .reply
        .send(Ok(serde_json::from_value(
            json!({"action":"end_call","end_reason":"duration_limit"}),
        )
        .unwrap()))
        .unwrap();
    let ending = recv(&mut h.ends).await;
    assert_eq!(ending.request.reason, "duration_limit");
    ending.reply.send(Ok(EndCallResponse {})).unwrap();
    let report = h.task.await.unwrap();
    assert!(report.errors.is_empty());
    assert_eq!(report.requests, 2);
    assert_eq!(report.stale_responses, 0);
    assert_eq!(h.port.paused.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eager_request_listens_to_changed_final_and_discards_all_speculative_actions() {
    let mut h = basic();
    h.events
        .send(event("utterance_eager", "Can I pay", 1))
        .unwrap();
    let eager = recv(&mut h.requests).await;
    h.input.send(Input::Binary(vec![0; 160])).await.unwrap();
    h.events
        .send(event("utterance_final", "Can I pay tomorrow instead", 2))
        .unwrap();
    until(|| h.port.input_frames.load(Ordering::SeqCst) == 1).await;
    let mut stale = response("MUST NOT SPEAK");
    stale.action = "end_call".into();
    stale.end_reason = Some("wrong_end".into());
    eager.reply.send(Ok(stale)).unwrap();
    let final_request = recv(&mut h.requests).await;
    assert_eq!(
        json!(final_request.request.events.last().unwrap())["text"],
        "Can I pay tomorrow instead"
    );
    assert_eq!(final_request.request.request_id, "call-2");
    assert!(h.speech.try_recv().is_err());
    assert!(h.ends.try_recv().is_err());
    final_request
        .reply
        .send(Ok(response("authoritative answer")))
        .unwrap();
    played(recv(&mut h.speech).await, "complete");
    let report = end(h).await;
    assert_eq!(report.requests, 2);
    assert_eq!(report.speculative_responses, 1);
    assert_eq!(report.stale_responses, 1);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn matching_final_still_commits_once_and_duplicate_eager_final_callbacks_are_ignored() {
    let mut h = basic();
    let eager = event("utterance_eager", "same words", 1);
    h.events.send(eager.clone()).unwrap();
    let first = recv(&mut h.requests).await;
    h.events.send(eager).unwrap();
    let final_event = event("utterance_final", "same words", 2);
    h.events.send(final_event.clone()).unwrap();
    h.events.send(final_event).unwrap();
    first
        .reply
        .send(Ok(response("never authoritative")))
        .unwrap();
    let final_request = recv(&mut h.requests).await;
    final_request
        .reply
        .send(Ok(response("only final")))
        .unwrap();
    played(recv(&mut h.speech).await, "complete");
    let r = end(h).await;
    assert_eq!(r.requests, 2);
    assert_eq!(r.duplicate_events, 2);
    assert_eq!(r.stale_responses, 0);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resumed_event_invalidates_inflight_final_response_without_cancelling_its_transaction() {
    let mut h = basic();
    h.events.send(event("utterance_final", "old", 1)).unwrap();
    let old = recv(&mut h.requests).await;
    h.events.send(event("utterance_resumed", "", 2)).unwrap();
    h.events
        .send(event("utterance_final", "old with more words", 3))
        .unwrap();
    old.reply.send(Ok(response("stale"))).unwrap();
    let resume = recv(&mut h.requests).await;
    assert_eq!(
        json!(resume.request.events.last().unwrap())["kind"],
        "utterance_resumed"
    );
    assert!(
        json!(resume.request.events)
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["kind"] == "delivery_outcome"
                && e["audio_id"] == "stale"
                && e["status"] == "truncated")
    );
    resume.reply.send(Ok(empty())).unwrap();
    let final_request = recv(&mut h.requests).await;
    final_request.reply.send(Ok(response("new"))).unwrap();
    played(recv(&mut h.speech).await, "complete");
    assert_eq!(end(h).await.stale_responses, 1);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn frames_and_events_progress_during_opening_and_slow_tts_but_speech_never_overlaps() {
    let mut h = start(
        json!([{"audio_id":"opening","text":"hello"}]),
        json!({"persona_voice":"test"}),
        None,
        |_| {},
    );
    let opening = recv(&mut h.speech).await;
    h.input.send(Input::Binary(vec![1])).await.unwrap();
    h.events
        .send(event("utterance_final", "caller", 1))
        .unwrap();
    until(|| h.port.input_frames.load(Ordering::SeqCst) == 1).await;
    assert!(h.requests.try_recv().is_err());
    played(opening, "complete");
    let req = recv(&mut h.requests).await;
    assert_eq!(
        json!(req.request.events.first().unwrap())["kind"],
        "delivery_outcome"
    );
    req.reply.send(Ok(response("answer"))).unwrap();
    let answer = recv(&mut h.speech).await;
    h.input.send(Input::Binary(vec![2])).await.unwrap();
    h.events
        .send(event("utterance_final", "later caller", 2))
        .unwrap();
    until(|| h.port.input_frames.load(Ordering::SeqCst) == 2).await;
    assert!(h.requests.try_recv().is_err());
    played(answer, "truncated");
    let next = recv(&mut h.requests).await;
    assert_eq!(json!(next.request.events[0])["status"], "truncated");
    next.reply.send(Ok(empty())).unwrap();
    assert!(end(h).await.errors.is_empty());
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_calls_execute_on_different_runtime_threads_without_global_lock() {
    let probe = Arc::new(Probe::new());
    let mut a = start(
        json!([]),
        json!({"persona_voice":"x"}),
        Some(probe.clone()),
        |_| {},
    );
    let mut b = start(
        json!([]),
        json!({"persona_voice":"x"}),
        Some(probe.clone()),
        |_| {},
    );
    a.events.send(event("utterance_final", "a", 1)).unwrap();
    b.events.send(event("utterance_final", "b", 1)).unwrap();
    let ar = recv(&mut a.requests).await;
    let br = recv(&mut b.requests).await;
    assert_eq!(probe.arrived.lock().unwrap().len(), 2);
    ar.reply.send(Ok(empty())).unwrap();
    br.reply.send(Ok(empty())).unwrap();
    let (a, b) = tokio::join!(end(a), end(b));
    assert_eq!((a.requests, b.requests), (1, 1));
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn facts_before_trigger_and_terminal_flush_include_late_receipts() {
    let mut h = basic();
    h.events
        .send(json!({"kind":"line_verdict","answered_by":"human","probability":0.2,"source":"jev"}))
        .unwrap();
    h.events.send(event("utterance_final", "hello", 1)).unwrap();
    let req = recv(&mut h.requests).await;
    assert_eq!(json!(req.request.events[0])["kind"], "line_verdict");
    req.reply.send(Ok(response("answer"))).unwrap();
    played(recv(&mut h.speech).await, "complete");
    h.events
        .send(json!({"kind":"playout_finished","audio_id":"answer","at":3.}))
        .unwrap();
    h.input
        .send(Input::Json(json!({"hangup":true})))
        .await
        .unwrap();
    let ending = recv(&mut h.ends).await;
    let facts = json!(ending.request.events);
    assert!(
        facts
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["kind"] == "playout_finished")
    );
    assert!(
        facts
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["kind"] == "delivery_outcome")
    );
    ending.reply.send(Ok(EndCallResponse {})).unwrap();
    assert!(h.task.await.unwrap().end_acknowledged);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hangup_cannot_cancel_end_call_and_end_call_without_speech_is_executed_once() {
    let mut h = basic();
    h.events.send(event("utterance_final", "done", 1)).unwrap();
    recv(&mut h.requests)
        .await
        .reply
        .send(Ok(serde_json::from_value::<ProcessEventsResponse>(
            json!({"action":"end_call","end_reason":"completed"}),
        )
        .unwrap()))
        .unwrap();
    let ending = recv(&mut h.ends).await;
    let _ = h.input.send(Input::Disconnect).await;
    assert!(!h.task.is_finished());
    ending.reply.send(Ok(EndCallResponse {})).unwrap();
    let report = h.task.await.unwrap();
    assert_eq!(report.reason, "completed");
    assert!(report.end_acknowledged);
    assert!(h.ends.try_recv().is_err());
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filler_runs_during_slow_api_and_response_waits_for_current_filler() {
    let mut h = start(
        json!([]),
        json!({"persona_voice":"x","filler":{"first_after_ms":0,"interval_ms":10000,"max":1,"phrases":["Waiting"]}}),
        None,
        |_| {},
    );
    h.events
        .send(event("utterance_final", "question", 1))
        .unwrap();
    let request = recv(&mut h.requests).await;
    let filler = recv(&mut h.speech).await;
    assert!(filler.command.audio_id.starts_with("filler-"));
    assert_eq!(h.port.paused.load(Ordering::SeqCst), 1);
    request.reply.send(Ok(response("real answer"))).unwrap();
    assert!(h.speech.try_recv().is_err());
    played(filler, "complete");
    played(recv(&mut h.speech).await, "complete");
    assert!(end(h).await.errors.is_empty());
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disconnect_aborts_and_joins_filler_and_preserves_cancelled_delivery_fact() {
    let mut h = start(
        json!([]),
        json!({"persona_voice":"x","filler":{"first_after_ms":0,"interval_ms":1000,"max":2,"phrases":["Waiting"]}}),
        None,
        |_| {},
    );
    h.events
        .send(event("utterance_final", "question", 1))
        .unwrap();
    let _request = recv(&mut h.requests).await;
    let filler = recv(&mut h.speech).await;
    h.input.send(Input::Disconnect).await.unwrap();
    let ending = recv(&mut h.ends).await;
    assert_eq!(h.port.active_speech.load(Ordering::SeqCst), 0);
    assert!(
        json!(ending.request.events)
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["audio_id"] == filler.command.audio_id && e["status"] == "truncated")
    );
    assert!(filler.reply.is_closed());
    ending.reply.send(Ok(EndCallResponse {})).unwrap();
    assert!(h.task.await.unwrap().errors.is_empty());
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outage_uses_farewell_and_contract_error_does_not() {
    for outage in [false, true] {
        let mut h = basic();
        h.events
            .send(event("utterance_final", "question", 1))
            .unwrap();
        let req = recv(&mut h.requests).await;
        req.reply
            .send(Err(Error::new(if outage {
                "turn_service_unavailable"
            } else {
                "invalid_turn_contract"
            })))
            .unwrap();
        if outage {
            let speech = recv(&mut h.speech).await;
            assert_eq!(speech.command.audio_id, "trouble-1");
            played(speech, "complete");
        }
        let ending = recv(&mut h.ends).await;
        assert_eq!(
            ending.request.reason,
            if outage {
                "turn_unreachable"
            } else {
                "abnormal_exit"
            }
        );
        ending.reply.send(Ok(EndCallResponse {})).unwrap();
        h.task.await.unwrap();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_pending_work_terminates_explicitly_instead_of_silently_dropping_turns() {
    let mut h = start(json!([]), json!({"persona_voice":"x"}), None, |c| {
        c.max_queued_triggers = 2
    });
    h.events.send(event("utterance_final", "first", 1)).unwrap();
    let _blocked = recv(&mut h.requests).await;
    for i in 2..5 {
        h.events
            .send(event("utterance_final", &format!("text{i}"), i))
            .unwrap();
    }
    let end = recv(&mut h.ends).await;
    assert_eq!(end.request.reason, "overloaded");
    end.reply.send(Ok(EndCallResponse {})).unwrap();
    assert!(
        h.task
            .await
            .unwrap()
            .errors
            .contains(&"orchestration_capacity_exceeded".into())
    );
}
