//! Shared fixtures for the engine's integration tests.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rightsizer_core::breaker::{BreakerObjects, BreakerScope, Control, Reset, Trip};
use rightsizer_core::*;
use rightsizer_engine::{Config, ContextInput, DecideRequest, Engine, ManualClock, Policy};
use rightsizer_estimators::{EstimateAxis, EstimateError, Estimator, EstimatorChain, EstimatorInfo, Needs};
use rightsizer_history::{CompactOptions, CompactReport, DecisionFilter, HistoryStore, MemoryStore, PutOutcome, StoreError, StoreInfo};
use ulid::Ulid;

pub fn t0() -> DateTime<Utc> {
    DateTime::from_timestamp(1_800_000_000, 0).unwrap()
}

pub fn key() -> WorkloadKey {
    WorkloadKey::parse("k8s://prod/ml/job/train").unwrap()
}

pub fn info(id: &str) -> EstimatorInfo {
    EstimatorInfo {
        id: id.into(),
        version: "1".into(),
        grids: vec![],
        axes: vec![EstimateAxis::Memory, EstimateAxis::CpuMean],
        kind: EstimateKind::Demand,
        needs: Needs::default(),
        features: String::new(),
        novelty: None,
        max_batch: None,
    }
}

/// A point estimate of `mem_gib` and `cpu` cores; counts calls; optional delay.
pub struct Fixed {
    pub info: EstimatorInfo,
    pub mem_gib: f64,
    pub cpu: f64,
    pub calls: AtomicUsize,
    pub delay: Duration,
}

impl Fixed {
    pub fn new(mem_gib: f64, cpu: f64) -> Self {
        Fixed { info: info("fixed"), mem_gib, cpu, calls: AtomicUsize::new(0), delay: Duration::ZERO }
    }
    pub fn slow(mem_gib: f64, delay: Duration) -> Self {
        Fixed { delay, ..Fixed::new(mem_gib, 1.0) }
    }
}

#[async_trait]
impl Estimator for Fixed {
    fn info(&self) -> &EstimatorInfo {
        &self.info
    }
    async fn estimate(&self, _ctx: &WorkloadContext) -> Result<Estimate, EstimateError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        let mut e = Estimate::empty(Provenance {
            estimator: self.info.id.clone(),
            version: self.info.version.clone(),
            grid: "rs-grid-v1".into(),
            features: String::new(),
            latency_ms: 0,
        });
        e.memory = Some(AxisEstimate::Point { value: self.mem_gib * Bytes::GIB as f64 });
        e.cpu_mean = Some(AxisEstimate::Point { value: self.cpu });
        Ok(e)
    }
}

/// Panics on every call.
pub struct Panicker(pub EstimatorInfo);

#[async_trait]
impl Estimator for Panicker {
    fn info(&self) -> &EstimatorInfo {
        &self.0
    }
    async fn estimate(&self, _ctx: &WorkloadContext) -> Result<Estimate, EstimateError> {
        panic!("estimator exploded");
    }
}

/// A store whose reads and writes can be switched off; durable unless told otherwise.
pub struct Flaky {
    pub inner: MemoryStore,
    pub down: AtomicBool,
    pub attempts_down: AtomicBool,
    pub durable: bool,
}

impl Flaky {
    pub fn new() -> Self {
        Flaky { inner: MemoryStore::new(), down: AtomicBool::new(false), attempts_down: AtomicBool::new(false), durable: true }
    }
    pub fn set_down(&self, d: bool) {
        self.down.store(d, Ordering::SeqCst);
    }
    fn check(&self) -> Result<(), StoreError> {
        if self.down.load(Ordering::SeqCst) { Err(StoreError::Unreachable("test: store down".into())) } else { Ok(()) }
    }
}

#[async_trait]
impl HistoryStore for Flaky {
    fn info(&self) -> StoreInfo {
        StoreInfo { backend: rightsizer_history::Backend::Memory, uri: "flaky://".into(), durable_shared: self.durable }
    }
    async fn put_run(&self, rec: &RunRecord) -> Result<PutOutcome, StoreError> {
        self.check()?;
        self.inner.put_run(rec).await
    }
    async fn put_decision(&self, d: &Decision) -> Result<(), StoreError> {
        self.check()?;
        self.inner.put_decision(d).await
    }
    async fn runs(&self, key: &WorkloadKey, limit: usize) -> Result<Vec<RunRecord>, StoreError> {
        self.check()?;
        self.inner.runs(key, limit).await
    }
    async fn decisions(&self, f: &DecisionFilter) -> Result<Vec<Decision>, StoreError> {
        self.check()?;
        self.inner.decisions(f).await
    }
    async fn decision(&self, id: Ulid) -> Result<Option<Decision>, StoreError> {
        self.check()?;
        self.inner.decision(id).await
    }
    async fn breaker_get(&self, s: &BreakerScope) -> Result<BreakerObjects, StoreError> {
        self.check()?;
        self.inner.breaker_get(s).await
    }
    async fn breaker_list(&self) -> Result<Vec<(BreakerScope, BreakerObjects)>, StoreError> {
        self.check()?;
        self.inner.breaker_list().await
    }
    async fn breaker_trip(&self, t: &Trip) -> Result<(), StoreError> {
        self.check()?;
        self.inner.breaker_trip(t).await
    }
    async fn breaker_reset(&self, r: &Reset) -> Result<(), StoreError> {
        self.check()?;
        self.inner.breaker_reset(r).await
    }
    async fn control_get(&self) -> Result<Option<Control>, StoreError> {
        self.check()?;
        self.inner.control_get().await
    }
    async fn control_set(&self, c: &Control) -> Result<(), StoreError> {
        self.check()?;
        self.inner.control_set(c).await
    }
    async fn attempt_get(&self, k: &WorkloadKey, i: &str) -> Result<Option<AttemptRecord>, StoreError> {
        self.check()?;
        self.inner.attempt_get(k, i).await
    }
    async fn attempt_put_if_absent(&self, r: &AttemptRecord) -> Result<bool, StoreError> {
        self.check()?;
        if self.attempts_down.load(Ordering::SeqCst) {
            return Err(StoreError::Unreachable("test: attempts down".into()));
        }
        self.inner.attempt_put_if_absent(r).await
    }
    async fn attempt_close_lease(&self, k: &WorkloadKey, i: &str, at: DateTime<Utc>, s: RunStatus) -> Result<(), StoreError> {
        self.check()?;
        self.inner.attempt_close_lease(k, i, at, s).await
    }
    async fn compact(&self, o: &CompactOptions) -> Result<CompactReport, StoreError> {
        self.check()?;
        self.inner.compact(o).await
    }
}

pub fn prior_gib(mem: u64, cpu: u32) -> Prior {
    Prior::from_requests(Requests { memory: Some(Bytes::gib(mem)), cpu: Some(MilliCpu::cores(cpu)), ..Default::default() })
}

pub fn platform() -> PlatformRef {
    PlatformRef::new("k8s")
}

pub fn ladder_platform() -> PlatformRef {
    let mut p = platform();
    p.retry.ladder = Some(Ladder::caller());
    p
}

pub fn seed_req(prior: Prior, platform: PlatformRef, mode: Mode) -> DecideRequest {
    DecideRequest {
        context: ContextInput::Seed { key: key(), unit: UnitId("main".into()), group: None, seed: ContextSeed::default(), prior, platform },
        constraints: Constraints::default(),
        state: PlatformState::default(),
        policy: Policy { mode: Some(mode), ..Default::default() },
        budget_ms: Some(2000),
        dry_run: false,
    }
}

/// A measured, trusted success at `peak_gib` with `req_gib` requested.
pub fn success(inv: &str, at: DateTime<Utc>, peak_gib: f64, req_gib: u64) -> RunRecord {
    RunRecord {
        v: 1,
        key: key(),
        cluster: None,
        invocation_id: inv.into(),
        attempt: 1,
        decision_id: None,
        started_at: at,
        inputs: vec![],
        code_digest: None,
        requested: Some(ResourceSpec {
            requests: Requests { memory: Some(Bytes::gib(req_gib)), cpu: Some(MilliCpu::cores(2)), ..Default::default() },
            limits: Limits::default(),
        }),
        origin_prior: None,
        status: RunStatus::Succeeded,
        status_confidence: Confidence::Observed,
        memory_peak: Some(Measurement::exact(peak_gib * Bytes::GIB as f64)),
        cpu_mean: Some(Measurement::exact(1.0)),
        cpu_peak_window: None,
        cpu_throttled_frac: None,
        gpu_memory_peak: None,
        gpu_util_mean: None,
        t_peak_frac: Some(0.5),
        duration_s: Some(3600.0),
        source: OutcomeSource::Watcher,
        trust: Trust::Verified,
    }
}

pub struct Rig {
    pub engine: Engine,
    pub store: Arc<Flaky>,
    pub clock: Arc<ManualClock>,
    pub fixed: Arc<Fixed>,
}

pub async fn rig_with(config: Config, est: Arc<dyn Estimator>, fixed: Arc<Fixed>, durable: bool) -> Rig {
    let store = Arc::new(Flaky { durable, ..Flaky::new() });
    let clock = Arc::new(ManualClock::new(t0()));
    let engine = Engine::builder(config)
        .store(store.clone())
        .chain(EstimatorChain::new(vec![est]))
        .clock(clock.clone())
        .build()
        .await
        .unwrap();
    Rig { engine, store, clock, fixed }
}

/// An engine over a durable flaky store with a fixed estimator.
pub async fn rig(mem_gib: f64) -> Rig {
    let fixed = Arc::new(Fixed::new(mem_gib, 1.0));
    rig_with(Config::default(), fixed.clone(), fixed, true).await
}

pub fn labels() -> BTreeMap<String, String> {
    BTreeMap::new()
}
