//! Shadow / audit replay (SPEC §12.2, §12.3).

mod common;

use std::sync::Arc;

use chrono::Duration as CDuration;
use common::*;
use rightsizer_core::*;
use rightsizer_engine::replay::{Fit, Readiness, ReplayOptions};
use rightsizer_engine::{Config, Engine, report};
use rightsizer_estimators::{EstimateError, Estimator, EstimatorChain, EstimatorInfo};
use rightsizer_history::MemoryStore;

/// Echoes the largest history peak as a point estimate (so a decision depends on what it saw).
struct HistMax(EstimatorInfo);

#[async_trait::async_trait]
impl Estimator for HistMax {
    fn info(&self) -> &EstimatorInfo {
        &self.0
    }
    async fn estimate(&self, ctx: &WorkloadContext) -> Result<Estimate, EstimateError> {
        let m = ctx.history.iter().filter_map(|r| r.memory_peak).map(|m| m.value).fold(0.0, f64::max);
        if m <= 0.0 {
            return Err(EstimateError::Insufficient("no history".into()));
        }
        let mut e = Estimate::empty(Provenance { estimator: "histmax".into(), version: "1".into(), grid: "rs-grid-v1".into(), features: String::new(), latency_ms: 0 });
        e.memory = Some(AxisEstimate::Point { value: m });
        e.cpu_mean = Some(AxisEstimate::Point { value: 1.0 });
        Ok(e)
    }
}

async fn engine(est: Arc<dyn Estimator>) -> Engine {
    Engine::builder(Config::default()).store(Arc::new(MemoryStore::new())).chain(EstimatorChain::new(vec![est])).build().await.unwrap()
}

fn run(inv: &str, start_s: i64, dur_s: f64, peak_gib: f64) -> RunRecord {
    let mut r = success(inv, t0() + CDuration::seconds(start_s), peak_gib, 8);
    r.duration_s = Some(dur_s);
    r
}

#[tokio::test]
async fn counterfactual_uses_only_runs_completed_before() {
    let e = engine(Arc::new(HistMax(info("histmax")))).await;
    let runs = vec![
        run("a", 0, 100.0, 2.0),  // ends at 100
        run("b", 50, 10.0, 1.0),  // starts before a completes
        run("c", 200, 10.0, 1.0), // after both
    ];
    let opts = ReplayOptions { per_run: true, ..Default::default() };
    let rep = e.replay(runs, &opts).await;
    let k = &rep.keys[0];
    let seen: Vec<(String, usize)> = k.per_run.iter().map(|r| (r.invocation_id.clone(), r.history)).collect();
    assert_eq!(seen, vec![("a".into(), 0), ("b".into(), 0), ("c".into(), 2)]);
    // a and b had nothing to learn from: kept the prior.
    assert_eq!(k.per_run[0].decided_memory, Some(Bytes::gib(8).0));
    assert_eq!(k.per_run[1].decided_memory, Some(Bytes::gib(8).0));
    assert!(k.per_run[2].recommended_memory < Some(Bytes::gib(8).0));
}

#[tokio::test]
async fn audit_equals_shadow_on_the_same_snapshot() {
    let runs: Vec<RunRecord> = (0..12).map(|i| run(&format!("r{i}"), i * 7200, 3600.0, 0.8 + 0.01 * i as f64)).collect();
    let opts = ReplayOptions { per_run: true, ..Default::default() };
    let shadow = engine(Arc::new(HistMax(info("histmax")))).await.replay(runs.clone(), &opts).await;
    let mut shuffled = runs.clone();
    shuffled.reverse();
    let audit = engine(Arc::new(HistMax(info("histmax")))).await.replay(shuffled, &opts).await;
    assert_eq!(shadow, audit);
    // Reductions are recommended, and with 3+ measured fitting runs S3(b) holds.
    let k = &shadow.keys[0];
    assert!(k.decided_memory_p50 < Some(Bytes::gib(8).0), "{k:?}");
    assert_eq!(k.readiness, Readiness::Ready);
    assert_eq!(k.would_fail, 0);
    assert!(k.would_save.gib_h > 0.0);
    let ci = k.would_save.gib_h_ci;
    assert!(ci.lo <= k.would_save.gib_h && k.would_save.gib_h <= ci.hi + 1e-9, "{ci:?}");
}

#[tokio::test]
async fn would_fail_is_censoring_aware_and_trips_the_simulated_breaker() {
    let e = engine(Arc::new(Fixed::new(1.0, 1.0))).await;
    let mut runs: Vec<RunRecord> = (0..5).map(|i| run(&format!("ok{i}"), i * 7200, 3600.0, 0.9)).collect();
    // An OOM at the prior: the peak is only known to be ≥ 8 GiB... but 8 GiB exceeds any reduction: fail.
    let mut oom = run("oom", 5 * 7200, 600.0, 8.0);
    oom.status = RunStatus::OomKilled;
    oom.memory_peak = Some(Measurement { value: 8.0 * Bytes::GIB as f64, censor: Censor::AtLeast, resolution_s: None });
    runs.push(oom);
    // A censored success: AtLeast 0.5 GiB fits nothing for sure.
    let mut cens = run("cens", 6 * 7200, 3600.0, 0.5);
    cens.memory_peak = Some(Measurement { value: 0.5 * Bytes::GIB as f64, censor: Censor::AtLeast, resolution_s: None });
    runs.push(cens);
    runs.push(run("after", 7 * 7200, 3600.0, 0.9));
    let rep = e.replay(runs, &ReplayOptions { per_run: true, ..Default::default() }).await;
    let k = &rep.keys[0];
    let fits: Vec<Fit> = k.per_run.iter().map(|r| r.fit).collect();
    assert_eq!(fits[5], Fit::Fail);
    assert_eq!(fits[6], Fit::Unknown, "AtLeast below the decision is unknown, not fit");
    assert_eq!(k.would_fail, 1);
    assert_eq!(k.unknown, 1);
    assert!(k.per_run[7].breaker_open, "the would-fail run stops later would-saves");
    assert_eq!(k.per_run[7].saved_gib_h, 0.0);
    assert!(k.per_run[5].saved_dollars < 0.0, "a failure without a ladder costs the burnt attempt");
    let text = report::text(&rep);
    assert!(text.contains("1 / 8 (?)"), "{text}");
}

#[tokio::test]
async fn readiness_ladder_only_and_not_ready() {
    let e = engine(Arc::new(Fixed::new(1.0, 1.0))).await;
    let mut unmeasured = run("u", 0, 3600.0, 0.9);
    unmeasured.memory_peak = None;
    let rep = e.replay(vec![unmeasured.clone()], &ReplayOptions::default()).await;
    assert_eq!(rep.keys[0].readiness, Readiness::NotReady { more_runs: 3 });
    let opts = ReplayOptions { platform: Some(ladder_platform()), ..Default::default() };
    let rep = e.replay(vec![unmeasured], &opts).await;
    assert_eq!(rep.keys[0].readiness, Readiness::LadderOnly);
}

#[tokio::test]
async fn bootstrap_is_seeded_and_html_is_self_contained() {
    let e = engine(Arc::new(HistMax(info("histmax")))).await;
    let runs: Vec<RunRecord> = (0..20).map(|i| run(&format!("r{i}"), i * 7200, 1800.0 + 100.0 * i as f64, 0.5 + 0.05 * (i % 5) as f64)).collect();
    let a = e.replay(runs.clone(), &ReplayOptions::default()).await;
    let b = e.replay(runs.clone(), &ReplayOptions::default()).await;
    assert_eq!(a.total, b.total);
    let c = e.replay(runs, &ReplayOptions { seed: 7, ..Default::default() }).await;
    assert_eq!(a.total.gib_h, c.total.gib_h);
    let html = report::html(&a, "rsz shadow");
    assert!(html.starts_with("<!doctype html>"));
    assert!(!html.contains("http://") && !html.contains("https://") && !html.contains("<script") && !html.contains("<link"));
    assert!(html.contains("k8s://prod/ml/job/train"));
}
