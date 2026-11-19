//! Cross-crate integration fixes: features context building inside the
//! engine, the unified memo, the history cache on the injected clock, key
//! listing, staged bytes, breaker scope paths, and the lease-expiry sweep.

mod common;

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use common::*;
use rightsizer_core::*;
use rightsizer_engine::{ContextInput, DecideRequest};
use rightsizer_estimators::{EstimateError, Estimator, EstimatorInfo};
use rightsizer_history::HistoryStore;

/// Records the inputs of every context it sees; answers like [`Fixed`].
struct Capture {
    info: EstimatorInfo,
    seen: Mutex<Vec<Vec<InputMeta>>>,
    inner: Fixed,
}

impl Capture {
    fn new() -> Self {
        Capture { info: info("capture"), seen: Mutex::new(Vec::new()), inner: Fixed::new(1.0, 1.0) }
    }
}

#[async_trait]
impl Estimator for Capture {
    fn info(&self) -> &EstimatorInfo {
        &self.info
    }
    async fn estimate(&self, ctx: &WorkloadContext) -> Result<Estimate, EstimateError> {
        self.seen.lock().unwrap().push(ctx.inputs.clone());
        self.inner.estimate(ctx).await
    }
}

fn with_input(mut req: DecideRequest, uri: &str, hash_only: bool) -> DecideRequest {
    if let ContextInput::Seed { seed, .. } = &mut req.context {
        seed.inputs.push(InputRef { name: "data".into(), uri: Some(uri.into()), bytes: None, format: None, scalar: None });
        if hash_only {
            seed.manifest.extra.insert("inputs_hash_only".into(), ScalarValue::Bool(true));
        }
    }
    req
}

#[test]
fn decide_future_is_send() {
    fn assert_send<T: Send>(_: T) {}
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let r = rt.block_on(rig(1.0));
    assert_send(r.engine.decide(seed_req(prior_gib(8, 2), platform(), Mode::Recommend)));
    assert_send(r.engine.decide_group(vec![]));
}

#[tokio::test]
async fn seed_inputs_are_read_through_features() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data.bin");
    std::fs::write(&path, vec![7u8; 12_345]).unwrap();
    let cap = Arc::new(Capture::new());
    let fixed = Arc::new(Fixed::new(1.0, 1.0));
    let r = rig_with(rightsizer_engine::Config::default(), cap.clone(), fixed, true).await;
    let req = with_input(seed_req(prior_gib(8, 2), platform(), Mode::Recommend), path.to_str().unwrap(), false);
    r.engine.decide(req).await;
    let seen = cap.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0][0].bytes, Some(12_345), "size read from the file (HEAD)");
    assert!(seen[0][0].uri_hash.as_deref().is_some_and(|h| h.starts_with("sha256:")));
}

#[tokio::test]
async fn admission_path_hashes_inputs_without_reading() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data.bin");
    std::fs::write(&path, vec![7u8; 12_345]).unwrap();
    let cap = Arc::new(Capture::new());
    let fixed = Arc::new(Fixed::new(1.0, 1.0));
    let r = rig_with(rightsizer_engine::Config::default(), cap.clone(), fixed, true).await;
    let req = with_input(seed_req(prior_gib(8, 2), platform(), Mode::Recommend), path.to_str().unwrap(), true);
    r.engine.decide(req).await;
    let seen = cap.seen.lock().unwrap().clone();
    assert_eq!(seen[0][0].bytes, None, "inputs_hash_only: never read inline");
    assert!(seen[0][0].uri_hash.is_some(), "but still hashed");
}

/// Staged tmpfs bytes are counted once: history peaks include the staging
/// (recorded as `staged_bytes`), the estimators subtract it, and the
/// decision adds the current `memory_backed_bytes` back.
#[tokio::test]
async fn staged_bytes_are_not_counted_twice() {
    async fn decide_with(staged_in_record: Option<u64>, staged_now: Option<u64>) -> Decision {
        let est: Arc<dyn Estimator> = Arc::new(rightsizer_estimators::HistoryPeak::default());
        let fixed = Arc::new(Fixed::new(1.0, 1.0));
        let r = rig_with(rightsizer_engine::Config::default(), est, fixed, true).await;
        for i in 0..5 {
            let mut rec = success(&format!("i{i}"), t0() - chrono::Duration::hours(i + 1), 6.0, 16);
            rec.staged_bytes = staged_in_record.map(Bytes::gib);
            r.store.inner.put_run(&rec).await.unwrap();
        }
        let mut req = seed_req(prior_gib(16, 2), platform(), Mode::Recommend);
        if let ContextInput::Seed { seed, .. } = &mut req.context {
            seed.manifest.memory_backed_bytes = staged_now.map(Bytes::gib);
        }
        req.dry_run = true;
        r.engine.decide(req).await
    }
    let mem = |d: &Decision| d.recommended.requests.memory.unwrap().0 as f64 / Bytes::GIB as f64;
    // No staging anywhere: ~6 GiB × margin.
    let plain = decide_with(None, None).await;
    // 4 GiB of the 6 GiB peak was staging, recorded on the runs and declared now.
    let staged = decide_with(Some(4), Some(4)).await;
    // Same, but the runs predate `staged_bytes`: the current staging is assumed.
    let unrecorded = decide_with(None, Some(4)).await;
    let grown = decide_with(Some(1), Some(4)).await;
    // Need never below the observed container peak (6 GiB).
    assert!(mem(&staged) >= 6.0, "{}", mem(&staged));
    // Counted once: the 4 GiB of staging is fixed (no estimation spread), so
    // the staged unit needs no more than the unstaged 6 GiB demand. Counted
    // twice (the old behaviour) it would need ≥ (6 × 1.15 + 4) × 1.05 ≈ 11.4 GiB.
    assert!(mem(&staged) <= mem(&plain), "staged {} vs plain {}", mem(&staged), mem(&plain));
    assert!(mem(&staged) < 11.4);
    // Runs that predate `staged_bytes` assume the current staging: same answer.
    assert_eq!(mem(&unrecorded), mem(&staged));
    // Staging that grew since the runs (1 → 4 GiB) is added in full.
    assert!(mem(&grown) > mem(&staged), "{} vs {}", mem(&grown), mem(&staged));
}

fn attempt_ctx(n: u32) -> AttemptContext {
    AttemptContext { v: 1, invocation_id: "inv-7".into(), attempt: n, max_attempts: Some(3) }
}

/// A reduced attempt behind a Caller ladder whose failure arrives
/// unclassified (inferred) and whose escalation never reports: the lease
/// expires open, and the sweep treats the failure as unrecovered.
#[tokio::test]
async fn sweep_trips_on_leases_that_expire_open() {
    let r = rig(1.0).await;
    let d = r.engine.attempt_decide(seed_req(prior_gib(8, 2), ladder_platform(), Mode::Enforce), attempt_ctx(1)).await;
    assert_eq!(d.mode, Mode::Enforce);
    let att = r.store.attempt_get(&key(), "inv-7").await.unwrap().unwrap();
    assert!(att.reduced && att.lease.as_ref().is_some_and(|l| l.closed_at.is_none()), "lease opened: {att:?}");
    let mut fail = success("inv-7", t0(), 1.0, 1);
    fail.decision_id = Some(d.id);
    fail.requested = Some(d.applied.clone());
    fail.status = RunStatus::OomKilled;
    fail.status_confidence = Confidence::Inferred; // unclassified: the lease stays open
    fail.duration_s = Some(60.0);
    let rep = r.engine.outcome(fail).await.unwrap();
    assert!(rep.tripped.is_empty() && !rep.lease_closed, "{rep:?}");

    // Within lease_ttl (15 min past the failure's end): nothing yet.
    r.clock.advance(std::time::Duration::from_secs(10 * 60));
    let s = r.engine.sweep(false).await.unwrap();
    assert_eq!((s.expired_leases, s.tripped.len()), (0, 0), "{s:?}");

    // Past it: unrecovered → the per-workload breaker trips, the lease closes.
    r.clock.advance(std::time::Duration::from_secs(7 * 60));
    let dry = r.engine.sweep(true).await.unwrap();
    assert_eq!(dry.tripped, vec![rightsizer_core::breaker::BreakerScope::Workload { key: key() }]);
    assert!(!r.store.breaker_get(&rightsizer_core::breaker::BreakerScope::Workload { key: key() }).await.unwrap().is_open(), "dry run writes nothing");
    let s = r.engine.sweep(false).await.unwrap();
    assert_eq!((s.expired_leases, s.leases_closed), (1, 1), "{s:?}");
    assert_eq!(s.tripped, vec![rightsizer_core::breaker::BreakerScope::Workload { key: key() }]);
    let b = r.store.breaker_get(&rightsizer_core::breaker::BreakerScope::Workload { key: key() }).await.unwrap();
    assert!(b.is_open());
    assert_eq!(b.trips[0].reason, rightsizer_core::breaker::TripReason::UnrecoveredFailure);
    let att = r.store.attempt_get(&key(), "inv-7").await.unwrap().unwrap();
    assert_eq!(att.lease.unwrap().outcome, Some(RunStatus::OomKilled));
    // Swept once.
    assert_eq!(r.engine.sweep(false).await.unwrap().expired_leases, 0);
}

#[tokio::test]
async fn sweep_skips_recovered_invocations() {
    let r = rig(1.0).await;
    let d = r.engine.attempt_decide(seed_req(prior_gib(8, 2), ladder_platform(), Mode::Enforce), attempt_ctx(1)).await;
    let mut fail = success("inv-7", t0(), 1.0, 1);
    fail.decision_id = Some(d.id);
    fail.status = RunStatus::OomKilled;
    fail.status_confidence = Confidence::Inferred;
    r.engine.outcome(fail).await.unwrap();
    let mut ok = success("inv-7", t0() + chrono::Duration::minutes(2), 1.5, 2);
    ok.attempt = 2;
    ok.status_confidence = Confidence::Inferred; // not classified either: lease left open
    ok.status = RunStatus::Succeeded;
    r.store.inner.put_run(&ok).await.unwrap();
    r.clock.advance(std::time::Duration::from_secs(3600));
    let s = r.engine.sweep(false).await.unwrap();
    assert_eq!((s.expired_leases, s.tripped.len()), (0, 0), "{s:?}");
}

#[tokio::test]
async fn keys_come_from_the_store() {
    let r = rig(1.0).await;
    r.store.inner.put_run(&success("a", t0(), 1.0, 2)).await.unwrap();
    let mut other = success("b", t0(), 1.0, 2);
    other.key = WorkloadKey::parse("k8s://prod/ml/job/other").unwrap();
    r.store.inner.put_run(&other).await.unwrap();
    let ks: Vec<String> = r.engine.keys().await.unwrap().iter().map(|k| k.to_string()).collect();
    assert_eq!(ks, vec!["k8s://prod/ml/job/other".to_string(), key().to_string()]);
}
