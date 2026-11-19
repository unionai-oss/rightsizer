//! Engine-level decide latency: a memo hit end to end (seed → cached
//! history → mode/breaker resolution → memo → decision rule → decision log)
//! on a `MemoryStore`. `cargo bench -p rightsizer-engine --bench engine`.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use criterion::{Criterion, criterion_group, criterion_main};
use rightsizer_core::*;
use rightsizer_engine::{Config, ContextInput, DecideRequest, Engine, Policy};
use rightsizer_estimators::{EstimateAxis, EstimateError, Estimator, EstimatorChain, EstimatorInfo, Needs};
use rightsizer_history::{HistoryStore, MemoryStore};

struct Fixed(EstimatorInfo);

#[async_trait]
impl Estimator for Fixed {
    fn info(&self) -> &EstimatorInfo {
        &self.0
    }
    async fn estimate(&self, _: &WorkloadContext) -> Result<Estimate, EstimateError> {
        let mut e = Estimate::empty(Provenance { estimator: "fixed".into(), version: "1".into(), grid: "rs-grid-v1".into(), features: String::new(), latency_ms: 0 });
        e.memory = Some(AxisEstimate::Quantiles { q: vec![(0.5, 1.5e9), (0.9, 2.5e9), (0.99, 4e9)] });
        e.cpu_mean = Some(AxisEstimate::Point { value: 1.2 });
        Ok(e)
    }
}

fn record(i: usize) -> RunRecord {
    RunRecord {
        v: 1,
        key: WorkloadKey::parse("k8s://prod/ml/job/train").unwrap(),
        cluster: None,
        invocation_id: format!("inv-{i}"),
        attempt: 1,
        decision_id: None,
        started_at: DateTime::<Utc>::from_timestamp(1_800_000_000 + i as i64 * 3600, 0).unwrap(),
        inputs: vec![InputMeta::blob("data", 2_000_000_000 + i as u64 * 1_000_000)],
        code_digest: Some("sha256:abc".into()),
        requested: Some(ResourceSpec { requests: Requests { memory: Some(Bytes::gib(8)), cpu: Some(MilliCpu::cores(2)), ..Default::default() }, limits: Limits::default() }),
        origin_prior: None,
        status: RunStatus::Succeeded,
        status_confidence: Confidence::Observed,
        memory_peak: Some(Measurement::exact(1.6e9 + i as f64 * 1e7)),
        cpu_mean: Some(Measurement::exact(1.1)),
        cpu_peak_window: None,
        cpu_throttled_frac: None,
        gpu_memory_peak: None,
        gpu_util_mean: None,
        t_peak_frac: Some(0.6),
        duration_s: Some(1800.0),
        source: OutcomeSource::Watcher,
        trust: Trust::Verified,
    }
}

fn bench(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let store = Arc::new(MemoryStore::new());
    let engine = rt.block_on(async {
        for i in 0..32 {
            store.put_run(&record(i)).await.unwrap();
        }
        let chain = EstimatorChain::new(vec![Arc::new(Fixed(EstimatorInfo {
            id: "fixed".into(),
            version: "1".into(),
            grids: vec![],
            axes: vec![EstimateAxis::Memory],
            kind: EstimateKind::Demand,
            needs: Needs::default(),
            features: String::new(),
            novelty: None,
            max_batch: None,
        }))]);
        Engine::builder(Config::default()).store(store.clone()).chain(chain).build().await.unwrap()
    });
    let mut platform = PlatformRef::new("k8s");
    platform.retry.ladder = Some(Ladder::caller());
    let req = |dry_run| DecideRequest {
        context: ContextInput::Seed {
            key: WorkloadKey::parse("k8s://prod/ml/job/train").unwrap(),
            unit: UnitId("main".into()),
            group: None,
            seed: ContextSeed { inputs: vec![InputRef { name: "data".into(), uri: None, bytes: Some(2_100_000_000), format: None, scalar: None }], ..Default::default() },
            prior: Prior::from_requests(Requests { memory: Some(Bytes::gib(8)), cpu: Some(MilliCpu::cores(2)), ..Default::default() }),
            platform: platform.clone(),
        },
        constraints: Constraints::default(),
        state: PlatformState::default(),
        policy: Policy { mode: Some(Mode::Enforce), ..Default::default() },
        budget_ms: Some(2000),
        dry_run,
    };
    // Warm the history cache and the memo.
    let d = rt.block_on(engine.decide(req(true)));
    assert!(matches!(d.source, DecisionSource::Estimator { .. }));
    let d = rt.block_on(engine.decide(req(true)));
    assert!(matches!(d.source, DecisionSource::Memo { .. }));
    c.bench_function("decide/memo-hit/dry-run", |b| b.iter(|| rt.block_on(engine.decide(req(true)))));
    c.bench_function("decide/memo-hit/recorded", |b| b.iter(|| rt.block_on(engine.decide(req(false)))));
}

criterion_group!(benches, bench);
criterion_main!(benches);
