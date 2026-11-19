//! Adversarial review of the engine: groups, ladders, memo keys, replay.

mod common;

use std::sync::Arc;

use async_trait::async_trait;
use common::*;
use rightsizer_core::*;
use rightsizer_engine::{Config, ContextInput, ReplayOptions};
use rightsizer_estimators::{EstimateError, Estimator, EstimatorInfo};
use rightsizer_history::HistoryStore;

fn attempt(k: u32) -> AttemptContext {
    AttemptContext { v: 1, invocation_id: "inv-42".into(), attempt: k, max_attempts: Some(3) }
}

fn member(unit: &str, prior: Prior, platform: PlatformRef) -> rightsizer_engine::DecideRequest {
    let mut q = seed_req(prior, platform, Mode::Enforce);
    if let ContextInput::Seed { unit: u, group, .. } = &mut q.context {
        *u = UnitId(unit.into());
        *group = Some(GroupId("g".into()));
    }
    q
}

#[tokio::test]
async fn group_never_overrides_a_members_pin_or_prior() {
    // Member a pins CPU at 8 cores; member b has a bigger memory prior and
    // small CPU. The group decision must keep a's pinned CPU, and each
    // member's decision must carry its own prior (attribution uses it).
    let r = rig(1.0).await;
    let mut a = member("a", prior_gib(8, 8), platform());
    a.constraints.pinned = AxisSet::from(Axis::Cpu);
    let b = member("b", prior_gib(64, 1), platform());
    let ds = r.engine.decide_group(vec![a, b]).await;
    assert_eq!(ds.len(), 2);
    let (da, db) = (&ds[0], &ds[1]);
    assert_eq!(da.applied.requests.cpu, Some(MilliCpu::cores(8)), "a's pinned CPU changed: {:?}", da.explanation);
    assert_eq!(da.prior, prior_gib(8, 8), "a's decision carries b's prior");
    assert_eq!(db.prior, prior_gib(64, 1));
    // Memory: never below either member's own decision (demand = max).
    for d in &ds {
        let p = d.prior.spec.requests.memory.unwrap();
        assert!(d.applied.requests.memory.unwrap() >= p, "{}: {:?} below prior {p} without S3", d.unit.0, d.applied.requests.memory);
    }
}

fn catalog_state() -> PlatformState {
    let shape = |name: &str, cpu: u32, mem: u64, price: f64| Shape {
        name: name.into(),
        cpu: MilliCpu::cores(cpu),
        memory: Bytes::gib(mem),
        reserved: Bytes::gib(1),
        gpu: None,
        price: [("training".to_string(), price)].into_iter().collect(),
        family: "m5".into(),
    };
    PlatformState {
        request_model: RequestModel::Catalog {
            shapes: vec![
                shape("ml.m5.large", 2, 8, 0.115),
                shape("ml.m5.xlarge", 4, 16, 0.23),
                shape("ml.m5.2xlarge", 8, 32, 0.461),
                shape("ml.m5.4xlarge", 16, 64, 0.922),
                shape("ml.m5.12xlarge", 48, 192, 2.765),
            ],
        },
        price: PriceModel::Instance { usage: Some("training".into()) },
        ..Default::default()
    }
}

#[tokio::test]
async fn catalog_ladder_step_moves_the_rendered_shape() {
    // On catalog platforms the adapter renders `shape`; a ladder step that
    // raises memory but keeps the shape name does not move up at all.
    let r = rig(1.0).await;
    let req = || {
        let mut q = seed_req(prior_gib(60, 16), ladder_platform(), Mode::Enforce);
        q.state = catalog_state();
        q
    };
    let d1 = r.engine.attempt_decide(req(), attempt(1)).await;
    let a1 = d1.applied.requests.memory.unwrap();
    assert!(a1 < Bytes::gib(60), "attempt 1 is reduced: {:?}", d1.explanation);
    let usable = |d: &Decision| {
        let RequestModel::Catalog { shapes } = &catalog_state().request_model else { unreachable!() };
        shapes.iter().find(|s| Some(&s.name) == d.shape.as_ref()).map(|s| s.usable())
    };
    for k in 2..=3 {
        let d = r.engine.attempt_decide(req(), attempt(k)).await;
        let m = d.applied.requests.memory.unwrap();
        assert!(m > a1, "attempt {k} moves up");
        let u = usable(&d).expect("a shape is rendered");
        assert!(u >= m, "attempt {k}: shape {:?} (usable {u}) does not hold the stepped memory {m}", d.shape);
    }
}

#[tokio::test]
async fn ladder_step_is_clamped_to_the_ceiling() {
    // S4: a ladder step above the largest node is unschedulable (a silent
    // Pending); it is clamped to the ceiling, and never below the prior.
    let r = rig(1.0).await;
    let mut platform = ladder_platform();
    platform.retry.ladder = Some(Ladder { memory_factor: 3.0, ..Ladder::caller() });
    let req = || {
        let mut q = seed_req(prior_gib(64, 2), platform.clone(), Mode::Enforce);
        q.state.shapes = vec![NodeShape { name: None, cpu: MilliCpu::cores(16), memory: Bytes::gib(64), gpu: None, price_per_hour: 3.0, host_per_gpu: None }];
        q
    };
    let d1 = r.engine.attempt_decide(req(), attempt(1)).await;
    assert!(d1.applied.requests.memory.unwrap() < Bytes::gib(64), "{:?}", d1.explanation);
    let d2 = r.engine.attempt_decide(req(), attempt(2)).await;
    let m = d2.applied.requests.memory.unwrap();
    assert!(m <= Bytes::gib(64), "attempt 2 asks {m}, above the largest node (64Gi)");
    assert!(m >= Bytes::gib(64), "attempt 2 at least the prior when the step is clamped: {m}");
}

#[tokio::test]
async fn orchestrator_ladder_failure_is_not_tripped_before_escalation() {
    // Escalate-on-retry: attempt 1's OOM is recovered by attempt 2; the
    // per-workload breaker must not trip on attempt 1's outcome (it would
    // demote attempt 2 to recommend before the ladder runs).
    let r = rig(1.0).await;
    let mut platform = platform();
    platform.retry.ladder = Some(Ladder { driver: LadderDriver::Orchestrator, ..Ladder::caller() });
    let req = seed_req(prior_gib(8, 2), platform, Mode::Enforce);
    let d1 = r.engine.attempt_decide(req, attempt(1)).await;
    assert!(d1.applied.requests.memory.unwrap() < Bytes::gib(8), "{:?}", d1.explanation);
    let mut oom = success("inv-42", t0(), 1.0, 1);
    oom.decision_id = Some(d1.id);
    oom.requested = Some(d1.applied.clone());
    oom.status = RunStatus::OomKilled;
    let rep = r.engine.outcome(oom).await.unwrap();
    assert_eq!(rep.attribution.as_deref(), Some("caused"));
    assert!(rep.tripped.is_empty(), "tripped before the orchestrator retried: {:?}", rep.tripped);
}

/// An estimator whose answer depends on the prior and on the history (as
/// `rightsizer-trees/1`'s features do).
struct PriorEcho(EstimatorInfo);

#[async_trait]
impl Estimator for PriorEcho {
    fn info(&self) -> &EstimatorInfo {
        &self.0
    }
    async fn estimate(&self, ctx: &WorkloadContext) -> Result<Estimate, EstimateError> {
        let mut e = Estimate::empty(Provenance { estimator: "echo".into(), version: "1".into(), grid: "rs-grid-v1".into(), ..Default::default() });
        let m = ctx.prior.spec.requests.memory.map(|b| b.0 as f64).unwrap_or(0.0) / 2.0 + ctx.history.len() as f64 * Bytes::GIB as f64;
        e.memory = Some(AxisEstimate::Point { value: m });
        e.cpu_mean = Some(AxisEstimate::Point { value: 1.0 });
        Ok(e)
    }
}

#[tokio::test]
async fn memo_does_not_serve_an_estimate_computed_for_another_prior_or_history() {
    let fixed = Arc::new(Fixed::new(1.0, 1.0));
    let r = rig_with(Config::default(), Arc::new(PriorEcho(info("echo"))), fixed, true).await;
    let mem_of = |d: &Decision| match &d.estimate.as_ref().unwrap().memory {
        Some(AxisEstimate::Cells { probs, .. }) => {
            let g = Grid::rs_grid_v1();
            let i = probs.iter().enumerate().max_by(|a, b| a.1.total_cmp(b.1)).unwrap().0;
            g.memory[i]
        }
        other => panic!("{other:?}"),
    };
    let d8 = r.engine.decide(seed_req(prior_gib(8, 2), platform(), Mode::Recommend)).await;
    let d64 = r.engine.decide(seed_req(prior_gib(64, 2), platform(), Mode::Recommend)).await;
    assert!(mem_of(&d64) > mem_of(&d8), "the prior changed (8 → 64 Gi) but the memo served the old estimate: {:?}", d64.source);
    // New history written by another process (no local outcome to invalidate the memo).
    for i in 0..40 {
        r.store.inner.put_run(&success(&format!("other-{i}"), t0(), 1.0, 8)).await.unwrap();
    }
    r.engine.view().invalidate(&key());
    let d_hist = r.engine.decide(seed_req(prior_gib(64, 2), platform(), Mode::Recommend)).await;
    assert!(mem_of(&d_hist) > mem_of(&d64), "history changed but the memo served the old estimate: {:?}", d_hist.source);
}

#[tokio::test]
async fn replay_never_uses_a_run_that_had_not_finished() {
    // A run with unknown duration that started a minute earlier had not
    // necessarily finished: its outcome is the future for the next run.
    let r = rig(1.0).await;
    let mut a = success("a", t0(), 6.0, 8);
    a.duration_s = None;
    let b = success("b", t0() + chrono::Duration::seconds(60), 6.0, 8);
    let rep = r.engine.replay(vec![a, b], &ReplayOptions { per_run: true, bootstrap: 0, ..Default::default() }).await;
    let rows = &rep.keys[0].per_run;
    let rb = rows.iter().find(|x| x.invocation_id == "b").unwrap();
    assert_eq!(rb.history, 0, "b saw a's outcome although a may still have been running");
}

#[tokio::test]
async fn exploration_is_a_per_decision_draw_not_a_per_key_constant() {
    // With ε = 0.05 about 5% of decisions explore. A seed fixed per key
    // makes one key explore on every run (or never), and the logged
    // propensity ε is then false for off-policy learning.
    let r = rig(3.0).await;
    let mut explored = 0;
    let n = 400;
    for _ in 0..n {
        let mut q = seed_req(prior_gib(1, 1), platform(), Mode::Recommend);
        q.policy.exploration = Some(0.05);
        q.dry_run = true;
        let d = r.engine.decide(q).await;
        assert!(d.propensity == 0.05 || d.propensity == 0.95, "{}", d.propensity);
        if d.propensity == 0.05 {
            explored += 1;
        }
        r.clock.advance(std::time::Duration::from_millis(1));
    }
    assert!((1..n / 4).contains(&explored), "{explored} of {n} decisions explored");
}
