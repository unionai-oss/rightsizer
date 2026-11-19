//! Memo, single-flight, limiter, attempts and ladders, groups, outcomes.

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::*;
use rightsizer_core::*;
use rightsizer_engine::{Config, ContextInput, DecideRequest};
use rightsizer_history::HistoryStore;

#[tokio::test]
async fn memo_hit_then_invalidated_by_an_outcome() {
    let r = rig(1.0).await;
    let req = || seed_req(prior_gib(8, 2), ladder_platform(), Mode::Recommend);
    let d1 = r.engine.decide(req()).await;
    assert!(matches!(d1.source, DecisionSource::Estimator { .. }));
    let d2 = r.engine.decide(req()).await;
    assert!(matches!(d2.source, DecisionSource::Memo { .. }), "{:?}", d2.source);
    assert_eq!(d1.applied, d2.applied);
    assert_eq!(r.fixed.calls.load(Ordering::SeqCst), 1);
    r.engine.outcome(success("i1", t0(), 0.9, 8)).await.unwrap();
    let d3 = r.engine.decide(req()).await;
    assert!(matches!(d3.source, DecisionSource::Estimator { .. }));
    assert_eq!(r.fixed.calls.load(Ordering::SeqCst), 2);
    // A different feature bucket is a different memo entry.
    let mut other = req();
    if let ContextInput::Seed { seed, .. } = &mut other.context {
        seed.inputs.push(InputRef { name: "x".into(), uri: None, bytes: Some(50 << 30), format: None, scalar: None });
    }
    r.engine.decide(other).await;
    assert_eq!(r.fixed.calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn single_flight_shares_one_chain_call() {
    let fixed = Arc::new(Fixed::slow(1.0, Duration::from_millis(80)));
    let r = rig_with(Config::default(), fixed.clone(), fixed.clone(), true).await;
    let mut hs = Vec::new();
    for _ in 0..16 {
        let e = r.engine.clone();
        hs.push(tokio::spawn(async move { e.decide(seed_req(prior_gib(8, 2), ladder_platform(), Mode::Recommend)).await }));
    }
    let ds: Vec<Decision> = futures::future::join_all(hs).await.into_iter().map(|h| h.unwrap()).collect();
    assert_eq!(fixed.calls.load(Ordering::SeqCst), 1);
    assert!(ds.iter().all(|d| d.applied == ds[0].applied));
}

#[tokio::test]
async fn limiter_bounds_distinct_decisions() {
    let mut cfg = Config::default();
    cfg.decision.max_concurrent_decisions = 2;
    let fixed = Arc::new(Fixed::slow(1.0, Duration::from_millis(60)));
    let r = rig_with(cfg, fixed.clone(), fixed.clone(), true).await;
    let t = std::time::Instant::now();
    let mut hs = Vec::new();
    for i in 0..4u64 {
        let e = r.engine.clone();
        hs.push(tokio::spawn(async move {
            let mut req = seed_req(prior_gib(8, 2), ladder_platform(), Mode::Recommend);
            if let ContextInput::Seed { seed, .. } = &mut req.context {
                seed.inputs.push(InputRef { name: "x".into(), uri: None, bytes: Some((1 + i * 3) << 30), format: None, scalar: None });
            }
            e.decide(req).await
        }));
    }
    for h in hs {
        h.await.unwrap();
    }
    assert_eq!(fixed.calls.load(Ordering::SeqCst), 4);
    assert!(t.elapsed() >= Duration::from_millis(115), "4 distinct decisions, 2 at a time: {:?}", t.elapsed());
}

fn attempt(k: u32) -> AttemptContext {
    AttemptContext { v: 1, invocation_id: "inv-42".into(), attempt: k, max_attempts: Some(3) }
}

#[tokio::test]
async fn attempt_record_is_written_before_a_reduced_attempt_and_ladders_up() {
    let r = rig(1.0).await;
    let req = || seed_req(prior_gib(8, 2), ladder_platform(), Mode::Enforce);
    let d1 = r.engine.attempt_decide(req(), attempt(1)).await;
    let a1 = d1.applied.requests.memory.unwrap();
    assert!(a1 < Bytes::gib(8));
    let rec = r.store.attempt_get(&key(), "inv-42").await.unwrap().expect("attempt record");
    assert!(rec.reduced);
    assert_eq!(rec.applied.memory, Some(a1));
    assert_eq!(rec.origin_prior.memory, Some(Bytes::gib(8)));
    assert!(rec.lease.as_ref().is_some_and(|l| l.closed_at.is_none()), "Caller ladder: lease open");
    // Attempt 2: step 1 of attempt 1 (×2), at least.
    let d2 = r.engine.attempt_decide(req(), attempt(2)).await;
    assert!(d2.applied.requests.memory.unwrap() >= Bytes(a1.0 * 2), "{:?} vs {:?}", d2.applied, a1);
    // Attempt 3 (= max_steps): at least the final floor (the origin prior).
    let d3 = r.engine.attempt_decide(req(), attempt(3)).await;
    assert!(d3.applied.requests.memory.unwrap() >= Bytes::gib(8));
    // Attempts beyond max_attempts stay at the last step.
    let d9 = r.engine.attempt_decide(req(), attempt(9)).await;
    assert_eq!(d9.applied.requests.memory, d3.applied.requests.memory);
}

#[tokio::test]
async fn attempt_not_reduced_when_the_record_cannot_be_written() {
    let r = rig(1.0).await;
    r.store.attempts_down.store(true, Ordering::SeqCst);
    let d = r.engine.attempt_decide(seed_req(prior_gib(8, 2), ladder_platform(), Mode::Enforce), attempt(1)).await;
    assert_eq!(d.applied.requests.memory, Some(Bytes::gib(8)), "{:?}", d.explanation);
    assert!(d.explanation.iter().any(|e| e.contains("attempt record could not be written")));
}

#[tokio::test]
async fn retry_without_a_record_is_never_below_the_prior() {
    let r = rig(1.0).await;
    let d = r.engine.attempt_decide(seed_req(prior_gib(8, 2), ladder_platform(), Mode::Enforce), attempt(2)).await;
    assert!(d.applied.requests.memory.unwrap() >= Bytes::gib(8));
    // An increase on attempt 1 is never undone.
    let r = rig(20.0).await;
    let d1 = r.engine.attempt_decide(seed_req(prior_gib(8, 2), ladder_platform(), Mode::Enforce), attempt(1)).await;
    assert!(d1.applied.requests.memory.unwrap() > Bytes::gib(8));
    let rec = r.store.attempt_get(&key(), "inv-42").await.unwrap().unwrap();
    assert!(!rec.reduced);
    let d2 = r.engine.attempt_decide(seed_req(prior_gib(8, 2), ladder_platform(), Mode::Enforce), attempt(2)).await;
    assert!(d2.applied.requests.memory >= d1.applied.requests.memory);
}

#[tokio::test]
async fn lease_is_closed_when_the_failure_is_classified() {
    let r = rig(1.0).await;
    let d = r.engine.attempt_decide(seed_req(prior_gib(8, 2), ladder_platform(), Mode::Enforce), attempt(1)).await;
    let mut oom = success("inv-42", t0(), 1.0, 1);
    oom.decision_id = Some(d.id);
    oom.requested = Some(d.applied.clone());
    oom.status = RunStatus::OomKilled;
    let rep = r.engine.outcome(oom).await.unwrap();
    assert_eq!(rep.attribution.as_deref(), Some("caused"));
    assert!(rep.tripped.is_empty(), "the lease was live: the ladder may still recover it");
    assert!(rep.lease_closed);
    let rec = r.store.attempt_get(&key(), "inv-42").await.unwrap().unwrap();
    assert_eq!(rec.lease.unwrap().outcome, Some(RunStatus::OomKilled));
}

#[tokio::test]
async fn group_is_sized_once_from_the_union_of_histories() {
    let r = rig(1.0).await;
    let member = |unit: &str| {
        let mut q = seed_req(prior_gib(8, 2), ladder_platform(), Mode::Recommend);
        if let ContextInput::Seed { unit: u, group, .. } = &mut q.context {
            *u = UnitId(unit.into());
            *group = Some(GroupId("workers".into()));
        }
        q
    };
    let ds = r.engine.decide_group(vec![member("w0"), member("w1"), member("w2")]).await;
    assert_eq!(ds.len(), 3);
    assert!(ds.iter().all(|d| d.applied == ds[0].applied && d.id == ds[0].id));
    assert_eq!(ds.iter().map(|d| d.unit.0.as_str()).collect::<Vec<_>>(), vec!["w0", "w1", "w2"]);
    assert!(ds.iter().all(|d| d.group == Some(GroupId("workers".into()))));
    let logged = r.store.decisions(&Default::default()).await.unwrap();
    assert_eq!(logged.len(), 1, "one decision per group");
}

#[tokio::test]
async fn full_context_requests_decide_without_the_store() {
    let r = rig(1.0).await;
    r.store.set_down(true);
    let ctx = WorkloadContext {
        v: 1,
        key: key(),
        unit: UnitId("main".into()),
        platform: ladder_platform(),
        code: None,
        inputs: vec![],
        manifest: Default::default(),
        prior: prior_gib(8, 2),
        history: vec![],
        labels: labels(),
    };
    let mut req = DecideRequest::for_context(ctx);
    req.policy.mode = Some(Mode::Recommend);
    let d = r.engine.decide(req).await;
    assert_eq!(d.mode, Mode::Recommend);
    assert!(d.recommended.requests.memory < Some(Bytes::gib(8)));
}

#[tokio::test]
async fn metrics_cover_spec_names() {
    let r = rig(1.0).await;
    r.engine.decide(seed_req(prior_gib(8, 2), platform(), Mode::Enforce)).await;
    let text = r.engine.metrics().render();
    for m in [
        "rightsizer_decisions_total",
        "rightsizer_decision_latency_seconds",
        "rightsizer_held_total",
        "rightsizer_outcome_join_ratio",
    ] {
        assert!(text.contains(m), "missing {m}");
    }
}
