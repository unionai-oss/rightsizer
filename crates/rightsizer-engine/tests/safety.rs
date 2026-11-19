//! The safety contract (SPEC §2.3) as named tests, at the engine level.

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::*;
use rightsizer_core::breaker::{BreakerScope, TripReason};
use rightsizer_core::*;
use rightsizer_engine::{Config, Engine};
use rightsizer_estimators::{Estimator, EstimatorChain};
use rightsizer_history::{DecisionFilter, HistoryStore, MemoryStore};

fn is_prior(d: &Decision) -> bool {
    d.applied == d.prior.spec
}

fn reason(d: &Decision) -> Option<PriorReason> {
    match &d.source {
        DecisionSource::Prior { reason, .. } => Some(*reason),
        _ => None,
    }
}

#[tokio::test]
async fn s1_inert_on_estimator_panic() {
    let fixed = Arc::new(Fixed::new(1.0, 1.0));
    let r = rig_with(Config::default(), Arc::new(Panicker(info("boom"))), fixed, true).await;
    let d = r.engine.decide(seed_req(prior_gib(8, 2), platform(), Mode::Enforce)).await;
    assert!(is_prior(&d));
    assert_eq!(reason(&d), Some(PriorReason::Panic));
    // The engine still works afterwards.
    let d2 = r.engine.decide(seed_req(prior_gib(8, 2), platform(), Mode::Enforce)).await;
    assert_eq!(reason(&d2), Some(PriorReason::Panic));
    assert!(r.engine.metrics().render().contains("rightsizer_errors_total{reason=\"panic\",stage=\"decide\"} 2"));
}

#[tokio::test]
async fn s1_inert_on_estimator_error_and_decline() {
    struct Fails(rightsizer_estimators::EstimatorInfo);
    #[async_trait::async_trait]
    impl Estimator for Fails {
        fn info(&self) -> &rightsizer_estimators::EstimatorInfo {
            &self.0
        }
        async fn estimate(&self, _: &WorkloadContext) -> Result<Estimate, rightsizer_estimators::EstimateError> {
            Err(rightsizer_estimators::EstimateError::Failed { message: "down".into(), retryable: true })
        }
    }
    let fixed = Arc::new(Fixed::new(1.0, 1.0));
    let r = rig_with(Config::default(), Arc::new(Fails(info("f"))), fixed, true).await;
    let d = r.engine.decide(seed_req(prior_gib(8, 2), platform(), Mode::Enforce)).await;
    assert!(is_prior(&d));
    assert_eq!(reason(&d), Some(PriorReason::EstimatorError));
}

#[tokio::test]
async fn s1_inert_on_store_unreachable_after_ttl() {
    let r = rig(1.0).await;
    let mut prior_hist = Vec::new();
    for i in 0..4 {
        let s = success(&format!("h{i}"), t0() - chrono::Duration::hours(10 - i), 0.8, 8);
        r.store.put_run(&s).await.unwrap();
        prior_hist.push(s);
    }
    let req = || seed_req(prior_gib(8, 2), platform(), Mode::Enforce);
    let d = r.engine.decide(req()).await;
    assert_eq!(d.mode, Mode::Enforce);
    assert!(d.applied.requests.memory < d.prior.spec.requests.memory, "evidence allows a reduction: {:?}", d.explanation);

    // Store down, within the cache TTL: cached history still decides.
    r.store.set_down(true);
    r.clock.advance(Duration::from_secs(300));
    let d = r.engine.decide(req()).await;
    assert_eq!(d.mode, Mode::Enforce, "{:?}", d.explanation);

    // Past the TTL: demoted, reductions blocked, the submission untouched.
    r.clock.advance(Duration::from_secs(400));
    let d = r.engine.decide(req()).await;
    assert_eq!(d.mode, Mode::Recommend, "{:?}", d.explanation);
    assert_eq!(d.applied.requests.memory, d.prior.spec.requests.memory);
    assert!(d.explanation.iter().any(|e| e.contains("history unavailable")));
    // Decisions were buffered, not lost; they flush when the store returns.
    assert!(r.engine.view().buffered() > 0);
    r.store.set_down(false);
    assert!(r.engine.flush().await > 0);
    assert_eq!(r.engine.view().buffered(), 0);
}

#[tokio::test]
async fn s1_store_unreachable_for_a_never_cached_key() {
    let r = rig(1.0).await;
    r.store.set_down(true);
    let d = r.engine.decide(seed_req(prior_gib(8, 2), ladder_platform(), Mode::Enforce)).await;
    assert_eq!(d.mode, Mode::Recommend);
    assert!(is_prior(&d) || d.applied.requests.memory >= d.prior.spec.requests.memory);
}

#[tokio::test]
async fn s1_budget_expiry_returns_prior() {
    let fixed = Arc::new(Fixed::slow(1.0, Duration::from_millis(500)));
    let r = rig_with(Config::default(), fixed.clone(), fixed, true).await;
    let mut req = seed_req(prior_gib(8, 2), ladder_platform(), Mode::Enforce);
    req.budget_ms = Some(30);
    let t = std::time::Instant::now();
    let d = r.engine.decide(req).await;
    assert!(t.elapsed() < Duration::from_millis(300));
    assert!(is_prior(&d));
    assert_eq!(reason(&d), Some(PriorReason::Budget));
    assert!(r.engine.metrics().render().contains("rightsizer_budget_expired_total{platform=\"k8s\"} 1"));
}

#[tokio::test]
async fn s1_config_error_is_inert_unless_strict() {
    let mut cfg = Config::default();
    cfg.decision.need_headroom = 0.5;
    let e = Engine::builder(cfg.clone()).store(Arc::new(MemoryStore::new())).build().await.unwrap();
    assert!(e.config_error().is_some());
    let d = e.decide(seed_req(prior_gib(8, 2), platform(), Mode::Enforce)).await;
    assert!(is_prior(&d));
    assert_eq!(reason(&d), Some(PriorReason::ConfigError));
    cfg.strict = true;
    assert!(Engine::builder(cfg).store(Arc::new(MemoryStore::new())).build().await.is_err());
}

#[tokio::test]
async fn s3_reduction_needs_ladder_or_evidence() {
    // No ladder, no history: the reduction is recommended, not applied.
    let r = rig(1.0).await;
    let d = r.engine.decide(seed_req(prior_gib(8, 2), platform(), Mode::Enforce)).await;
    assert_eq!(d.mode, Mode::Enforce);
    assert!(d.recommended.requests.memory < d.prior.spec.requests.memory);
    assert_eq!(d.applied.requests.memory, d.prior.spec.requests.memory);
    assert!(d.held.iter().any(|h| h.axis == Axis::Memory && h.reason == HoldReason::S3NotMet), "{:?}", d.held);

    // (a) A Caller ladder that reaches the prior permits it.
    let r = rig(1.0).await;
    let d = r.engine.decide(seed_req(prior_gib(8, 2), ladder_platform(), Mode::Enforce)).await;
    assert!(d.applied.requests.memory < d.prior.spec.requests.memory, "{:?}", d.explanation);

    // (b) Three measured, trusted successes that fit permit it.
    let r = rig(1.0).await;
    for i in 0..3 {
        r.store.put_run(&success(&format!("s{i}"), t0() - chrono::Duration::hours(5 - i), 0.9, 8)).await.unwrap();
    }
    let d = r.engine.decide(seed_req(prior_gib(8, 2), platform(), Mode::Enforce)).await;
    assert!(d.applied.requests.memory < d.prior.spec.requests.memory, "{:?}", d.explanation);

    // Unverified self-reports are not evidence.
    let r = rig(1.0).await;
    for i in 0..3 {
        let mut s = success(&format!("u{i}"), t0() - chrono::Duration::hours(5 - i), 0.9, 8);
        s.trust = Trust::Unverified;
        r.store.put_run(&s).await.unwrap();
    }
    let d = r.engine.decide(seed_req(prior_gib(8, 2), platform(), Mode::Enforce)).await;
    assert_eq!(d.applied.requests.memory, d.prior.spec.requests.memory);
}

#[tokio::test]
async fn s5_breaker_demotes_to_recommend() {
    let r = rig(1.0).await;
    r.engine.breaker_trip(BreakerScope::Workload { key: key() }, Some("test".into())).await.unwrap();
    let d = r.engine.decide(seed_req(prior_gib(8, 2), ladder_platform(), Mode::Enforce)).await;
    assert_eq!(d.mode, Mode::Recommend);
    assert!(d.explanation.iter().any(|e| e.contains("per-workload breaker")));
    // The fleet breaker too.
    let r = rig(1.0).await;
    r.engine.breaker_trip(BreakerScope::Fleet, None).await.unwrap();
    let d = r.engine.decide(seed_req(prior_gib(8, 2), ladder_platform(), Mode::Enforce)).await;
    assert_eq!(d.mode, Mode::Recommend);
    // A reset closes it (after the breaker cache).
    r.clock.advance(Duration::from_secs(1));
    r.engine.breaker_reset(BreakerScope::Fleet, Some("op".into())).await.unwrap();
    r.clock.advance(Duration::from_secs(1));
    let d = r.engine.decide(seed_req(prior_gib(8, 2), ladder_platform(), Mode::Enforce)).await;
    assert_eq!(d.mode, Mode::Enforce);
}

#[tokio::test]
async fn s5_caused_unrecovered_failure_trips_the_workload_breaker() {
    let r = rig(1.0).await;
    let d = r.engine.decide(seed_req(prior_gib(8, 2), ladder_platform(), Mode::Enforce)).await;
    assert_eq!(d.mode, Mode::Enforce);
    let applied = d.applied.requests.memory.unwrap();
    assert!(applied < Bytes::gib(8));
    // The enforced attempt OOMs and nothing recovers it.
    let mut oom = success("inv-1", t0(), applied.as_gib(), 1);
    oom.decision_id = Some(d.id);
    oom.requested = Some(d.applied.clone());
    oom.status = RunStatus::OomKilled;
    oom.memory_peak = Some(Measurement { value: applied.0 as f64, censor: Censor::AtLeast, resolution_s: None });
    let rep = r.engine.outcome(oom).await.unwrap();
    assert!(rep.joined);
    assert_eq!(rep.attribution.as_deref(), Some("caused"));
    assert_eq!(rep.tripped, vec![BreakerScope::Workload { key: key() }]);
    let objs = r.store.breaker_get(&BreakerScope::Workload { key: key() }).await.unwrap();
    assert_eq!(objs.trips[0].reason, TripReason::UnrecoveredFailure);
    let d2 = r.engine.decide(seed_req(prior_gib(8, 2), ladder_platform(), Mode::Enforce)).await;
    assert_eq!(d2.mode, Mode::Recommend);
    // A failure at the prior is not caused.
    let mut at_prior = success("inv-2", t0(), 8.0, 8);
    at_prior.status = RunStatus::OomKilled;
    let rep = r.engine.outcome(at_prior).await.unwrap();
    assert_eq!(rep.attribution.as_deref(), Some("not_caused"));
    assert!(rep.tripped.is_empty());
}

#[tokio::test]
async fn breaker_or_control_unreadable_blocks_reductions() {
    let r = rig(1.0).await;
    // Warm the history cache, then lose the store: control and breakers become unreadable.
    r.engine.decide(seed_req(prior_gib(8, 2), ladder_platform(), Mode::Enforce)).await;
    r.store.set_down(true);
    r.clock.advance(Duration::from_secs(120));
    let d = r.engine.decide(seed_req(prior_gib(8, 2), ladder_platform(), Mode::Enforce)).await;
    assert_eq!(d.applied.requests.memory, d.prior.spec.requests.memory);
    assert!(d.held.iter().any(|h| h.reason == HoldReason::Breaker), "{:?}", d.held);
}

#[tokio::test]
async fn mode_ceiling_caps_everything() {
    let r = rig(1.0).await;
    r.engine.control_set(Mode::Observe, Some("op".into())).await.unwrap();
    let d = r.engine.decide(seed_req(prior_gib(8, 2), ladder_platform(), Mode::Enforce)).await;
    assert_eq!(d.mode, Mode::Observe);
    assert!(is_prior(&d));
    assert_eq!(reason(&d), Some(PriorReason::Mode));
    assert_eq!(r.fixed.calls.load(Ordering::SeqCst), 0, "observe does not estimate");
    r.clock.advance(Duration::from_secs(61));
    r.engine.control_set(Mode::Recommend, None).await.unwrap();
    let d = r.engine.decide(seed_req(prior_gib(8, 2), ladder_platform(), Mode::Enforce)).await;
    assert_eq!(d.mode, Mode::Recommend);
    assert!(d.explanation.iter().any(|e| e.contains("mode_ceiling")));
    r.engine.control_set(Mode::Off, None).await.unwrap();
    let d = r.engine.decide(seed_req(prior_gib(8, 2), ladder_platform(), Mode::Recommend)).await;
    assert_eq!(d.mode, Mode::Off);
    assert!(is_prior(&d));
}

#[tokio::test]
async fn enforce_demoted_on_non_durable_store() {
    let fixed = Arc::new(Fixed::new(1.0, 1.0));
    let r = rig_with(Config::default(), fixed.clone(), fixed, false).await;
    let d = r.engine.decide(seed_req(prior_gib(8, 2), ladder_platform(), Mode::Enforce)).await;
    assert_eq!(d.mode, Mode::Recommend);
    assert!(d.explanation.iter().any(|e| e.contains("not durable")));
}

#[tokio::test]
async fn unclassifiable_surface_recommends_reductions_only() {
    let r = rig(1.0).await;
    let mut req = seed_req(prior_gib(8, 2), ladder_platform(), Mode::Enforce);
    req.policy.can_classify = Some(false);
    let d = r.engine.decide(req).await;
    assert_eq!(d.mode, Mode::Enforce);
    assert!(d.recommended.requests.memory < d.prior.spec.requests.memory);
    assert_eq!(d.applied.requests.memory, d.prior.spec.requests.memory);
    assert!(d.held.iter().any(|h| h.reason == HoldReason::Unclassifiable), "{:?}", d.held);
}

#[tokio::test]
async fn max_mode_caps_the_surface() {
    let r = rig(1.0).await;
    let mut req = seed_req(prior_gib(8, 2), ladder_platform(), Mode::Enforce);
    req.policy.max_mode = Some(Mode::Recommend);
    assert_eq!(r.engine.decide(req).await.mode, Mode::Recommend);
}

#[tokio::test]
async fn off_reads_nothing_and_records_nothing() {
    let r = rig(1.0).await;
    r.store.set_down(true);
    let d = r.engine.decide(seed_req(prior_gib(8, 2), ladder_platform(), Mode::Off)).await;
    assert_eq!(d.mode, Mode::Off);
    assert!(is_prior(&d));
    assert_eq!(r.engine.view().buffered(), 0);
    assert_eq!(r.fixed.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn dry_run_is_not_recorded_and_real_runs_are() {
    let r = rig(1.0).await;
    let mut req = seed_req(prior_gib(8, 2), ladder_platform(), Mode::Recommend);
    req.dry_run = true;
    r.engine.decide(req.clone()).await;
    assert!(r.store.decisions(&DecisionFilter::default()).await.unwrap().is_empty());
    req.dry_run = false;
    let d = r.engine.decide(req).await;
    let logged = r.store.decisions(&DecisionFilter::default()).await.unwrap();
    assert_eq!(logged.len(), 1);
    assert_eq!(logged[0].id, d.id);
}

#[tokio::test]
async fn estimator_version_breaker_skips_that_estimator() {
    let fixed = Arc::new(Fixed::new(1.0, 1.0));
    let store = Arc::new(Flaky::new());
    let chain = EstimatorChain::new(vec![fixed.clone(), Arc::new(rightsizer_estimators::PriorEstimator::default())]);
    let e = Engine::builder(Config::default()).store(store.clone()).chain(chain).build().await.unwrap();
    e.breaker_trip(BreakerScope::EstimatorVersion { id: "fixed".into(), version: "1".into() }, None).await.unwrap();
    let d = e.decide(seed_req(prior_gib(8, 2), ladder_platform(), Mode::Enforce)).await;
    assert_eq!(fixed.calls.load(Ordering::SeqCst), 0);
    assert!(is_prior(&d));
    assert!(d.explanation.iter().any(|x| x.contains("skipped by an open estimator breaker")));
}
