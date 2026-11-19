//! Decision-rule tests. Safety tests are named after the contract clause.

use std::collections::BTreeMap;

use chrono::DateTime;
use proptest::prelude::*;

use super::*;
use crate::types::{
    AxisEstimate, AxisSet, Censor, Combo, Confidence, Estimate, InputMeta, Ladder, Limits, ManifestFeatures,
    Measurement, NodeShape, OutcomeSource, Parallelism, PlatformRef, PlatformRetry, Provenance, QosClass, Quota,
    RunRecord, RunStatus, Trust, UnitId, WorkloadKey,
};

const GIB: f64 = Bytes::GIB as f64;

fn ctx(mem_gib: Option<f64>, cpu: Option<f64>) -> WorkloadContext {
    WorkloadContext {
        v: 1,
        key: WorkloadKey::parse("k8s://prod/ml/job/train/main").unwrap(),
        unit: UnitId("main".into()),
        platform: PlatformRef::new("k8s"),
        code: Some(crate::types::CodeInfo { digest: "d1".into(), tokens: vec![], source: None }),
        inputs: vec![],
        manifest: ManifestFeatures::default(),
        prior: Prior::from_requests(Requests {
            memory: mem_gib.map(Bytes::from_gib_f64),
            cpu: cpu.map(MilliCpu::from_cores_f64),
            ..Default::default()
        }),
        history: vec![],
        labels: BTreeMap::new(),
    }
}

fn est(mem_gib: f64, cpu: f64) -> Estimate {
    Estimate {
        memory: Some(AxisEstimate::Point { value: mem_gib * GIB }),
        cpu_mean: Some(AxisEstimate::Point { value: cpu }),
        ..Estimate::empty(Provenance { estimator: "rules-sized".into(), version: "1".into(), grid: "rs-grid-v1".into(), features: "features-v1".into(), latency_ms: 0 })
    }
}

fn success(peak_gib: f64, req_gib: f64) -> RunRecord {
    RunRecord {
        v: 1,
        key: WorkloadKey::parse("k8s://prod/ml/job/train/main").unwrap(),
        cluster: None,
        invocation_id: format!("inv-{peak_gib}-{req_gib}"),
        attempt: 1,
        decision_id: None,
        started_at: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
        inputs: vec![],
        code_digest: Some("d1".into()),
        requested: Some(ResourceSpec {
            requests: Requests { memory: Some(Bytes::from_gib_f64(req_gib)), ..Default::default() },
            ..Default::default()
        }),
        origin_prior: None,
        status: RunStatus::Succeeded,
        status_confidence: Confidence::Observed,
        memory_peak: Some(Measurement { value: peak_gib * GIB, censor: Censor::Exact, resolution_s: None }),
        cpu_mean: None,
        cpu_peak_window: None,
        cpu_throttled_frac: None,
        gpu_memory_peak: None,
        gpu_util_mean: None,
        t_peak_frac: Some(0.5),
        duration_s: Some(600.0),
        source: OutcomeSource::Watcher,
        trust: Trust::Verified,
    }
}

fn ladder() -> PlatformRetry {
    PlatformRetry { same_size: 0, ladder: Some(Ladder::caller()) }
}

struct Fx {
    grid: Grid,
    state: PlatformState,
    constraints: Constraints,
    params: DecisionParams,
}

impl Fx {
    fn new() -> Self {
        Fx { grid: Grid::rs_grid_v1(), state: PlatformState::default(), constraints: Constraints::default(), params: DecisionParams::default() }
    }
    fn decide(&self, c: &WorkloadContext, e: Option<&Estimate>) -> Decision {
        self.decide_mode(c, e, Mode::Enforce)
    }
    fn decide_mode(&self, c: &WorkloadContext, e: Option<&Estimate>, mode: Mode) -> Decision {
        let inp = DecisionInput::new(c, e, &self.grid, &self.state, &self.constraints, &self.params, mode);
        decide(&inp)
    }
}

fn mem(d: &ResourceSpec) -> Option<Bytes> {
    d.requests.memory
}

#[test]
fn no_estimate_keeps_the_prior() {
    let fx = Fx::new();
    let c = ctx(Some(4.0), Some(1.0));
    let d = fx.decide(&c, None);
    assert_eq!(d.applied, c.prior.spec);
    assert_eq!(d.recommended, c.prior.spec);
    assert!(matches!(d.source, DecisionSource::Prior { reason: PriorReason::Insufficient, .. }));
    assert_eq!(d.propensity, 1.0);
}

#[test]
fn invalid_estimate_keeps_the_prior() {
    let fx = Fx::new();
    let c = ctx(Some(4.0), Some(1.0));
    let mut e = est(2.0, 1.0);
    e.memory = Some(AxisEstimate::Cells { grid: "rs-grid-v1".into(), probs: vec![0.0; 27] });
    let d = fx.decide(&c, Some(&e));
    assert!(matches!(d.source, DecisionSource::Prior { reason: PriorReason::InvalidEstimate, .. }));
    assert_eq!(d.applied, c.prior.spec);
}

#[test]
fn prior_estimator_keeps_the_prior() {
    let fx = Fx::new();
    let c = ctx(Some(4.0), Some(1.0));
    let mut e = est(2.0, 1.0);
    e.provenance.estimator = "prior".into();
    let d = fx.decide(&c, Some(&e));
    assert_eq!(d.applied, c.prior.spec);
    assert!(matches!(d.source, DecisionSource::Estimator { ref id, .. } if id == "prior"));
}

#[test]
fn increases_need_no_evidence() {
    let fx = Fx::new();
    let c = ctx(Some(1.0), Some(1.0));
    let d = fx.decide(&c, Some(&est(6.0, 1.0)));
    let m = mem(&d.applied).unwrap();
    assert!(m.0 as f64 >= 6.0 * GIB * 1.05, "{m}");
    assert!(m <= Bytes::gib(16), "{m}");
    assert!(d.held.is_empty(), "{:?}", d.held);
    assert!(d.expected_cost.is_some() && d.p_fail.unwrap() < 0.2);
}

#[test]
fn s3_reduction_held_without_evidence() {
    let fx = Fx::new();
    let c = ctx(Some(64.0), Some(2.0));
    let d = fx.decide(&c, Some(&est(2.0, 1.0)));
    assert_eq!(mem(&d.applied), Some(Bytes::gib(64)));
    assert!(mem(&d.recommended).unwrap() < Bytes::gib(64), "the reduction is still recommended");
    let h = d.held.iter().find(|h| h.axis == Axis::Memory).expect("memory held");
    assert_eq!(h.reason, HoldReason::S3NotMet);
}

#[test]
fn s3a_ladder_permits_reduction_within_trust_region() {
    let fx = Fx::new();
    let mut c = ctx(Some(64.0), Some(2.0));
    c.platform.retry = ladder();
    let d = fx.decide(&c, Some(&est(2.0, 1.0)));
    let m = mem(&d.applied).unwrap();
    assert!(m < Bytes::gib(64));
    // Anchor = prior 64Gi; two cells down = 32Gi.
    assert_eq!(m, Bytes::gib(32));
    assert!(!d.is_held(Axis::Memory));
}

#[test]
fn s3a_fails_when_final_floor_exceeds_ceiling() {
    let mut fx = Fx::new();
    fx.constraints.ceiling.memory = Some(Bytes::gib(32));
    let mut c = ctx(Some(64.0), Some(2.0));
    c.platform.retry = ladder();
    let d = fx.decide(&c, Some(&est(2.0, 1.0)));
    // Ladder cannot reach the 64Gi prior within a 32Gi ceiling: no S3(a); no evidence: held.
    assert_eq!(mem(&d.applied), Some(Bytes::gib(64)));
    assert!(d.is_held(Axis::Memory));
}

#[test]
fn s3a_orchestrator_ladder_needs_known_attempts() {
    let fx = Fx::new();
    let mut c = ctx(Some(64.0), Some(2.0));
    c.platform.retry = PlatformRetry { same_size: 0, ladder: Some(Ladder { driver: crate::types::LadderDriver::Orchestrator, ..Ladder::caller() }) };
    let e = est(2.0, 1.0);
    let d = fx.decide(&c, Some(&e));
    assert_eq!(mem(&d.applied), Some(Bytes::gib(64)), "max_attempts unknown: no ladder");
    let a = AttemptContext { v: 1, invocation_id: "i".into(), attempt: 1, max_attempts: Some(3) };
    let mut inp = DecisionInput::new(&c, Some(&e), &fx.grid, &fx.state, &fx.constraints, &fx.params, Mode::Enforce);
    inp.attempt = Some(&a);
    assert!(mem(&decide(&inp).applied).unwrap() < Bytes::gib(64));
}

#[test]
fn s3b_evidence_permits_reduction_above_floor() {
    let fx = Fx::new();
    let mut c = ctx(Some(64.0), Some(2.0));
    c.history = (0..3).map(|_| success(2.0, 8.0)).collect();
    let d = fx.decide(&c, Some(&est(2.0, 1.0)));
    let m = mem(&d.applied).unwrap();
    // Anchor 8Gi (smallest requested success) → trust region ≥ 4Gi; floor 2.1Gi.
    assert!(m < Bytes::gib(64) && m >= Bytes::gib(4), "{m}");
    assert!(!d.is_held(Axis::Memory));
}

#[test]
fn s3_reduction_held_with_unverified_history() {
    let fx = Fx::new();
    let mut c = ctx(Some(64.0), Some(2.0));
    c.history = (0..5)
        .map(|_| {
            let mut r = success(2.0, 8.0);
            r.trust = Trust::Unverified;
            r
        })
        .collect();
    let d = fx.decide(&c, Some(&est(2.0, 1.0)));
    assert_eq!(mem(&d.applied), Some(Bytes::gib(64)));
    let mut fx2 = Fx::new();
    fx2.params.constraints.allow_unverified = true;
    let d2 = fx2.decide(&c, Some(&est(2.0, 1.0)));
    assert!(mem(&d2.applied).unwrap() < Bytes::gib(64), "allow_unverified_for_reductions opts in");
}

#[test]
fn s3_unenforced_memory_needs_evidence_even_with_a_ladder() {
    let fx = Fx::new();
    let mut c = ctx(Some(64.0), Some(2.0));
    c.platform.retry = ladder();
    c.platform.memory_enforced = false;
    let d = fx.decide(&c, Some(&est(2.0, 1.0)));
    assert_eq!(mem(&d.applied), Some(Bytes::gib(64)));
    assert_eq!(d.held.iter().find(|h| h.axis == Axis::Memory).unwrap().reason, HoldReason::NotEnforced);
}

#[test]
fn s5_breaker_blocks_reductions() {
    let mut fx = Fx::new();
    fx.params.reductions_blocked = Some(HoldReason::Breaker);
    let mut c = ctx(Some(64.0), Some(2.0));
    c.platform.retry = ladder();
    let d = fx.decide(&c, Some(&est(2.0, 1.0)));
    assert_eq!(mem(&d.applied), Some(Bytes::gib(64)));
    assert_eq!(d.held.iter().find(|h| h.axis == Axis::Memory).unwrap().reason, HoldReason::Breaker);
    // Increases still go through.
    let up = fx.decide(&ctx(Some(1.0), Some(1.0)), Some(&est(6.0, 1.0)));
    assert!(mem(&up.applied).unwrap() > Bytes::gib(1));
}

#[test]
fn s5_unclassifiable_surface_recommends_reductions_only() {
    let mut fx = Fx::new();
    fx.params.can_classify = false;
    let mut c = ctx(Some(64.0), Some(2.0));
    c.platform.retry = ladder();
    let d = fx.decide(&c, Some(&est(2.0, 1.0)));
    assert_eq!(mem(&d.applied), Some(Bytes::gib(64)));
    assert_eq!(d.held.iter().find(|h| h.axis == Axis::Memory).unwrap().reason, HoldReason::Unclassifiable);
}

#[test]
fn s2_never_adds_limit() {
    let fx = Fx::new();
    let c = ctx(Some(1.0), Some(0.5));
    let d = fx.decide(&c, Some(&est(6.0, 3.0)));
    assert!(d.applied.limits.is_empty());
    assert!(d.recommended.limits.is_empty());
}

#[test]
fn s2_preserves_limit_ratio() {
    let fx = Fx::new();
    let mut c = ctx(Some(2.0), Some(1.0));
    c.prior.spec.limits = Limits { memory: Some(Bytes::gib(4)), cpu: Some(MilliCpu(2000)), ephemeral_storage: None };
    let d = fx.decide(&c, Some(&est(6.0, 3.0)));
    let r = &d.applied.requests;
    let l = &d.applied.limits;
    assert_eq!(l.memory.unwrap().0, r.memory.unwrap().0 * 2);
    assert_eq!(l.cpu.unwrap().0, r.cpu.unwrap().0 * 2);
}

#[test]
fn s2_guaranteed_keeps_limits_equal_requests_and_whole_cpus() {
    let fx = Fx::new();
    let mut c = ctx(Some(2.0), Some(2.0));
    c.prior.spec.limits = Limits { memory: Some(Bytes::gib(2)), cpu: Some(MilliCpu(2000)), ephemeral_storage: None };
    c.prior.qos = Some(QosClass::Guaranteed);
    let d = fx.decide(&c, Some(&est(6.0, 2.6)));
    let r = &d.applied.requests;
    assert_eq!(d.applied.limits.memory, r.memory);
    assert_eq!(d.applied.limits.cpu, r.cpu);
    assert!(r.cpu.unwrap().is_whole(), "{:?}", r.cpu);
}

#[test]
fn s2_gpu_count_never_changes() {
    let mut fx = Fx::new();
    fx.params.constraints.allow_gpu_type_change = true;
    fx.constraints.allowed_shapes = Some(vec!["L4-24G:4".parse().unwrap(), "H100-80G:1".parse().unwrap()]);
    let mut c = ctx(Some(32.0), Some(8.0));
    c.prior.spec.requests.gpu = Some("A100-80G:4".parse().unwrap());
    let mut e = est(16.0, 4.0);
    e.gpu_memory = Some(AxisEstimate::Point { value: 10.0 * GIB });
    let d = fx.decide(&c, Some(&e));
    assert_eq!(d.applied.requests.gpu.unwrap().count, 4);
    assert_eq!(d.recommended.requests.gpu.unwrap().count, 4);
    assert!(d.is_held(Axis::GpuCount));
}

#[test]
fn gpu_type_held_by_default_and_unknown_never_changes() {
    let fx = Fx::new();
    let mut c = ctx(Some(32.0), Some(8.0));
    c.prior.spec.requests.gpu = Some("unknown-gpu:1".parse().unwrap());
    let mut e = est(16.0, 4.0);
    e.gpu_memory = Some(AxisEstimate::Point { value: 10.0 * GIB });
    let d = fx.decide(&c, Some(&e));
    assert_eq!(d.applied.requests.gpu, c.prior.spec.requests.gpu);
    assert_eq!(d.held.iter().find(|h| h.axis == Axis::GpuType).unwrap().reason, HoldReason::UnknownGpu);
}

#[test]
fn pinned_axes_equal_prior() {
    let mut fx = Fx::new();
    fx.constraints.pinned = AxisSet::from(Axis::Memory);
    let c = ctx(Some(1.0), Some(1.0));
    let d = fx.decide(&c, Some(&est(6.0, 3.0)));
    assert_eq!(mem(&d.applied), Some(Bytes::gib(1)));
    assert_eq!(mem(&d.recommended), Some(Bytes::gib(1)), "pins bind recommended too");
    assert!(d.applied.requests.cpu.unwrap() > MilliCpu(1000), "cpu still free");
}

#[test]
fn ephemeral_storage_and_shm_held() {
    let fx = Fx::new();
    let mut c = ctx(Some(1.0), Some(1.0));
    c.prior.spec.requests.ephemeral_storage = Some(Bytes::gib(10));
    c.prior.spec.requests.shm = Some(Bytes::gib(1));
    let d = fx.decide(&c, Some(&est(6.0, 3.0)));
    assert_eq!(d.applied.requests.ephemeral_storage, Some(Bytes::gib(10)));
    assert_eq!(d.applied.requests.shm, Some(Bytes::gib(1)));
    assert!(d.is_held(Axis::EphemeralStorage) && d.is_held(Axis::Shm));
}

#[test]
fn s4_increase_clamped_to_ceiling() {
    let mut fx = Fx::new();
    fx.state.shapes = vec![NodeShape { name: Some("m6i.4xlarge".into()), cpu: MilliCpu::cores(16), memory: Bytes::gib(16), gpu: None, price_per_hour: 0.768, host_per_gpu: None }];
    let c = ctx(Some(4.0), Some(1.0));
    let d = fx.decide(&c, Some(&est(32.0, 1.0)));
    assert_eq!(mem(&d.applied), Some(Bytes::gib(16)));
    assert!(mem(&d.recommended).unwrap() <= Bytes::gib(16));
    assert!(d.advisories.iter().any(|a| matches!(a, Advisory::Clamped { axis: Axis::Memory, .. })), "{:?}", d.advisories);
}

#[test]
fn quota_bounds_the_ceiling() {
    let mut fx = Fx::new();
    fx.state.quota = Some(Quota { memory: Some(Bytes::gib(8)), ..Default::default() });
    let d = fx.decide(&ctx(Some(4.0), Some(1.0)), Some(&est(32.0, 1.0)));
    assert_eq!(mem(&d.applied), Some(Bytes::gib(8)));
}

#[test]
fn infeasible_floor_uses_the_ceiling() {
    let mut fx = Fx::new();
    fx.constraints.ceiling.memory = Some(Bytes::gib(8));
    let mut c = ctx(Some(4.0), Some(1.0));
    c.history = vec![success(20.0, 32.0)];
    let d = fx.decide(&c, Some(&est(2.0, 1.0)));
    assert_eq!(mem(&d.applied), Some(Bytes::gib(8)));
    assert!(d.infeasible_floor());
}

#[test]
fn size_scaled_floor_binds_applied() {
    let fx = Fx::new();
    let mut c = ctx(Some(4.0), Some(1.0));
    // A large same-digest peak: the floor is 6 × 1.05 Gi even though the estimate says 1 Gi.
    c.history = vec![success(6.0, 8.0)];
    let d = fx.decide(&c, Some(&est(1.0, 1.0)));
    assert!(mem(&d.applied).unwrap().0 as f64 >= 6.0 * 1.05 * GIB);
}

#[test]
fn no_prior_is_recommend_only() {
    let fx = Fx::new();
    let c = ctx(None, None);
    let d = fx.decide(&c, Some(&est(3.0, 1.0)));
    assert_eq!(d.applied.requests.memory, None);
    assert_eq!(d.applied.requests.cpu, None);
    assert!(d.recommended.requests.memory.is_some());
    assert!(d.held.iter().any(|h| h.reason == HoldReason::NoPrior));
    let mut fx2 = Fx::new();
    fx2.params.constraints.enforce_without_prior = true;
    let d2 = fx2.decide(&c, Some(&est(3.0, 1.0)));
    assert!(d2.applied.requests.memory.is_some());
    assert!(d2.applied.limits.is_empty(), "even then, no limit is added");
}

#[test]
fn below_enforce_applied_is_still_computed() {
    let fx = Fx::new();
    let c = ctx(Some(1.0), Some(1.0));
    let e = est(6.0, 1.0);
    let a = fx.decide_mode(&c, Some(&e), Mode::Enforce);
    let r = fx.decide_mode(&c, Some(&e), Mode::Recommend);
    assert_eq!(a.applied, r.applied);
    assert_eq!(r.mode, Mode::Recommend);
    assert!(r.explanation.iter().any(|l| l.contains("would apply")));
}

#[test]
fn exploration_is_deterministic_and_logs_propensity() {
    let mut fx = Fx::new();
    fx.params.exploration = 0.05;
    let c = ctx(Some(1.0), Some(1.0));
    let e = est(6.0, 1.0);
    let base = {
        let f = Fx::new();
        f.decide(&c, Some(&e))
    };
    let mut explored = None;
    for seed in 0..500u64 {
        fx.params.seed = seed;
        let a = fx.decide(&c, Some(&e));
        let b = fx.decide(&c, Some(&e));
        assert_eq!(a.applied, b.applied, "same seed, same decision");
        assert!(a.propensity == 0.05 || a.propensity == 0.95);
        if a.propensity == 0.05 {
            explored = Some(a);
            break;
        }
    }
    let x = explored.expect("some seed explores");
    assert!(mem(&x.applied).unwrap() > mem(&base.applied).unwrap(), "never below the decision");
}

#[test]
fn action_estimates_decode_by_argmax() {
    let fx = Fx::new();
    let c = ctx(Some(1.0), Some(1.0));
    let mut probs = vec![0.0f32; 27];
    probs[8] = 0.6; // 4Gi
    probs[12] = 0.4;
    let mut e = est(6.0, 1.0);
    e.kind = EstimateKind::Action;
    e.memory = Some(AxisEstimate::Cells { grid: "rs-grid-v1".into(), probs });
    let d = fx.decide(&c, Some(&e));
    assert_eq!(mem(&d.applied), Some(Bytes::gib(4)));
    assert!(d.expected_cost.is_none());
}

#[test]
fn catalog_renders_a_shape() {
    let mut fx = Fx::new();
    let shape = |name: &str, cpu: u32, mem: u64, price: f64| crate::types::Shape {
        name: name.into(),
        cpu: MilliCpu::cores(cpu),
        memory: Bytes::gib(mem),
        reserved: Bytes::gib(1),
        gpu: None,
        price: [("training".to_string(), price)].into_iter().collect(),
        family: "m5".into(),
    };
    fx.state.request_model = RequestModel::Catalog {
        shapes: vec![shape("ml.m5.large", 2, 8, 0.115), shape("ml.m5.xlarge", 4, 16, 0.23), shape("ml.m5.2xlarge", 8, 32, 0.461)],
    };
    fx.state.price = crate::types::PriceModel::Instance { usage: Some("training".into()) };
    fx.params.risk = Some(RiskProfile::LEAN);
    let c = ctx(Some(4.0), Some(2.0));
    let d = fx.decide(&c, Some(&est(7.0, 2.0)));
    assert_eq!(d.shape.as_deref(), Some("ml.m5.xlarge"), "{:?}", d.explanation);
    assert_eq!(mem(&d.applied), Some(Bytes::gib(15)));
}

#[test]
fn combos_snap_to_a_legal_pair() {
    let mut fx = Fx::new();
    fx.state.request_model = RequestModel::Combos {
        combos: vec![
            Combo { cpu: MilliCpu(250), memory: vec![Bytes::mib(512), Bytes::gib(1), Bytes::gib(2)] },
            Combo { cpu: MilliCpu(1000), memory: (2..=8).map(Bytes::gib).collect() },
        ],
    };
    fx.state.price = crate::types::PriceModel::Linear { rates: crate::types::Rates { cpu_core_hr: 0.04, mem_gib_hr: 0.0044, gpu_hr: BTreeMap::new() } };
    let c = ctx(Some(1.0), Some(0.25));
    let d = fx.decide(&c, Some(&est(3.0, 0.2)));
    let r = &d.applied.requests;
    assert_eq!(r.cpu, Some(MilliCpu(1000)));
    assert!(r.memory.unwrap() >= Bytes::gib(4) && r.memory.unwrap().0.is_multiple_of(Bytes::GIB));
}

#[test]
fn inexact_snap_is_recommend_only() {
    let mut fx = Fx::new();
    fx.state.request_model = RequestModel::Snap { shapes: vec![], exact: false, instance_bounded: false };
    let c = ctx(Some(4.0), Some(1.0));
    let d = fx.decide(&c, Some(&est(8.0, 2.0)));
    assert_eq!(d.applied.requests, c.prior.spec.requests);
    assert!(d.held.iter().any(|h| h.reason == HoldReason::InexactSnap));
}

#[test]
fn advisories_prior_quality_gpu_and_walltime() {
    let mut fx = Fx::new();
    fx.state.shapes = vec![
        NodeShape { name: Some("p4d".into()), cpu: MilliCpu::cores(96), memory: Bytes::gib(1152), gpu: Some("A100-40G:8".parse().unwrap()), price_per_hour: 32.0, host_per_gpu: None },
        NodeShape { name: Some("g6".into()), cpu: MilliCpu::cores(8), memory: Bytes::gib(32), gpu: Some("L4-24G:1".parse().unwrap()), price_per_hour: 0.98, host_per_gpu: None },
    ];
    let mut c = ctx(Some(512.0), Some(64.0));
    c.prior.spec.requests.gpu = Some("A100-40G:8".parse().unwrap());
    c.platform.walltime_s = Some(600);
    let mut r = success(2.0, 8.0);
    r.gpu_util_mean = Some(0.1);
    c.history = vec![r];
    let mut e = est(4.0, 2.0);
    e.gpu_memory = Some(AxisEstimate::Point { value: 6.0 * GIB });
    e.duration_s = Some(AxisEstimate::Point { value: 1200.0 });
    let d = fx.decide(&c, Some(&e));
    let has = |f: &dyn Fn(&Advisory) -> bool| d.advisories.iter().any(f);
    assert!(has(&|a| matches!(a, Advisory::PriorOversized { axis: Axis::Memory, .. })), "{:?}", d.advisories);
    assert!(has(&|a| matches!(a, Advisory::GpuLowerCount { fits_count: 1, .. })));
    assert!(has(&|a| matches!(a, Advisory::GpuCheaperShape { .. })));
    assert!(has(&|a| matches!(a, Advisory::GpuLowUtil { .. })));
    assert!(has(&|a| matches!(a, Advisory::WalltimeTimeout { .. })));
    let mut big = est(4.0, 2.0);
    big.gpu_memory = Some(AxisEstimate::Point { value: 60.0 * GIB });
    let d = fx.decide(&c, Some(&big));
    assert!(d.advisories.iter().any(|a| matches!(a, Advisory::GpuOomPredicted { .. })));
}

#[test]
fn decision_json_round_trips() {
    let fx = Fx::new();
    let d = fx.decide(&ctx(Some(4.0), Some(1.0)), Some(&est(6.0, 2.0)));
    let j = serde_json::to_string(&d).unwrap();
    let back: Decision = serde_json::from_str(&j).unwrap();
    assert_eq!(back, d);
    let v: serde_json::Value = serde_json::from_str(&j).unwrap();
    assert_eq!(v["v"], 1);
    assert_eq!(v["source"]["kind"], "estimator");
    assert_eq!(v["key"], "k8s://prod/ml/job/train/main");
}

#[test]
fn context_digest_is_stable() {
    let fx = Fx::new();
    let c = ctx(Some(4.0), Some(1.0));
    let a = fx.decide(&c, Some(&est(6.0, 2.0)));
    let b = fx.decide(&c, Some(&est(6.0, 2.0)));
    assert_eq!(a.context_digest, b.context_digest);
    assert!(a.context_digest.starts_with("sha256:"));
    let mut c2 = c.clone();
    c2.inputs.push(InputMeta::blob("x", 1));
    assert_ne!(fx.decide(&c2, Some(&est(6.0, 2.0))).context_digest, a.context_digest);
}

#[test]
fn gpu_shapes_hold_host_resources_at_per_gpu_share() {
    let mut fx = Fx::new();
    fx.state.shapes = vec![NodeShape { name: Some("g5.12xlarge".into()), cpu: MilliCpu::cores(48), memory: Bytes::gib(192), gpu: Some("A10G-24G:4".parse().unwrap()), price_per_hour: 5.672, host_per_gpu: None }];
    let mut c = ctx(Some(8.0), Some(4.0));
    c.prior.spec.requests.gpu = Some("A10G-24G:1".parse().unwrap());
    let mut e = est(20.0, 6.0);
    e.gpu_memory = Some(AxisEstimate::Point { value: 10.0 * GIB });
    e.parallelism = Some(Parallelism::default());
    let d = fx.decide(&c, Some(&e));
    // Within the per-GPU share (12 cores / 48Gi) host resources are free: no reason to under-ask.
    assert!(mem(&d.applied).unwrap().0 as f64 >= 20.0 * GIB);
    assert!(d.applied.requests.cpu.unwrap() >= MilliCpu::cores(6));
}

// ── property tests ──────────────────────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig { cases: 48, ..ProptestConfig::default() })]

    #[test]
    fn decision_monotonic_in_demand(a in 0.3f64..200.0, b in 0.3f64..200.0, cpu in 0.2f64..16.0) {
        let (lo, hi) = if a < b { (a, b) } else { (b, a) };
        let fx = Fx::new();
        let mut c = ctx(Some(64.0), Some(4.0));
        c.platform.retry = ladder();
        let d_lo = fx.decide(&c, Some(&est(lo, cpu)));
        let d_hi = fx.decide(&c, Some(&est(hi, cpu)));
        prop_assert!(mem(&d_lo.applied) <= mem(&d_hi.applied), "{lo} → {:?}, {hi} → {:?}", mem(&d_lo.applied), mem(&d_hi.applied));
        prop_assert!(mem(&d_lo.recommended) <= mem(&d_hi.recommended));
    }

    #[test]
    fn never_below_floor(peak in 0.5f64..100.0, demand in 0.3f64..50.0, prior in 1.0f64..256.0) {
        let fx = Fx::new();
        let mut c = ctx(Some(prior), Some(2.0));
        c.platform.retry = ladder();
        c.history = (0..3).map(|_| success(peak, prior)).collect();
        let d = fx.decide(&c, Some(&est(demand, 1.0)));
        let floor = Bytes::from_f64(peak * 1.05 * GIB).min(Bytes::gib(2048));
        prop_assert!(mem(&d.applied).unwrap() >= floor, "applied {:?} floor {floor}", mem(&d.applied));
    }

    #[test]
    fn never_above_ceiling(ceil_gib in 1u64..512, demand in 0.3f64..1000.0, prior in 0.5f64..8.0, cpu_ceiling in 1u32..64) {
        let mut fx = Fx::new();
        fx.constraints.ceiling = Requests { memory: Some(Bytes::gib(ceil_gib)), cpu: Some(MilliCpu::cores(cpu_ceiling)), ..Default::default() };
        let c = ctx(Some(prior.min(ceil_gib as f64)), Some(1.0f64.min(cpu_ceiling as f64)));
        let d = fx.decide(&c, Some(&est(demand, 100.0)));
        prop_assert!(mem(&d.applied).unwrap() <= Bytes::gib(ceil_gib));
        prop_assert!(mem(&d.recommended).unwrap() <= Bytes::gib(ceil_gib));
        prop_assert!(d.applied.requests.cpu.unwrap() <= MilliCpu::cores(cpu_ceiling));
        prop_assert!(d.recommended.requests.cpu.unwrap() <= MilliCpu::cores(cpu_ceiling));
    }

    #[test]
    fn s2_never_adds_limit_property(has_mem_limit in any::<bool>(), has_cpu_limit in any::<bool>(), demand in 0.3f64..100.0, cpu in 0.1f64..32.0, ladder_on in any::<bool>()) {
        let fx = Fx::new();
        let mut c = ctx(Some(8.0), Some(2.0));
        if ladder_on { c.platform.retry = ladder(); }
        if has_mem_limit { c.prior.spec.limits.memory = Some(Bytes::gib(12)); }
        if has_cpu_limit { c.prior.spec.limits.cpu = Some(MilliCpu(3000)); }
        let d = fx.decide(&c, Some(&est(demand, cpu)));
        for spec in [&d.applied, &d.recommended] {
            prop_assert_eq!(spec.limits.memory.is_some(), has_mem_limit);
            prop_assert_eq!(spec.limits.cpu.is_some(), has_cpu_limit);
            if let (Some(l), Some(r)) = (spec.limits.memory, spec.requests.memory) { prop_assert!(l >= r); }
            if let (Some(l), Some(r)) = (spec.limits.cpu, spec.requests.cpu) { prop_assert!(l >= r); }
        }
    }

    #[test]
    fn held_axes_equal_prior(pin_mem in any::<bool>(), pin_cpu in any::<bool>(), demand in 0.3f64..100.0, cpu in 0.1f64..32.0) {
        let mut fx = Fx::new();
        let mut pins = AxisSet::empty();
        if pin_mem { pins.insert(Axis::Memory); }
        if pin_cpu { pins.insert(Axis::Cpu); }
        fx.constraints.pinned = pins;
        let mut c = ctx(Some(8.0), Some(2.0));
        c.platform.retry = ladder();
        c.prior.spec.requests.ephemeral_storage = Some(Bytes::gib(20));
        let d = fx.decide(&c, Some(&est(demand, cpu)));
        for h in &d.held {
            let (a, p) = (&d.applied.requests, &c.prior.spec.requests);
            match h.axis {
                Axis::Memory => prop_assert_eq!(a.memory, p.memory),
                Axis::Cpu => prop_assert_eq!(a.cpu, p.cpu),
                Axis::GpuCount | Axis::GpuType => prop_assert_eq!(a.gpu, p.gpu),
                Axis::EphemeralStorage => prop_assert_eq!(a.ephemeral_storage, p.ephemeral_storage),
                Axis::Shm => prop_assert_eq!(a.shm, p.shm),
            }
        }
        prop_assert!(d.is_held(Axis::EphemeralStorage));
        if pin_mem { prop_assert_eq!(mem(&d.applied), Some(Bytes::gib(8))); }
    }

    #[test]
    fn gpu_count_never_changes_property(count in 1u32..9, gpu_gib in 0.0f64..600.0, demand in 0.3f64..200.0, allow in any::<bool>()) {
        let mut fx = Fx::new();
        fx.params.constraints.allow_gpu_type_change = allow;
        fx.constraints.allowed_shapes = Some(vec![GpuShape { accel: AcceleratorId::H100_80G, count }, GpuShape { accel: AcceleratorId::L4_24G, count: 1 }]);
        let mut c = ctx(Some(64.0), Some(8.0));
        c.prior.spec.requests.gpu = Some(GpuShape { accel: AcceleratorId::A100_80G, count });
        let mut e = est(demand, 4.0);
        e.gpu_memory = Some(AxisEstimate::Point { value: gpu_gib * GIB });
        let d = fx.decide(&c, Some(&e));
        prop_assert_eq!(d.applied.requests.gpu.map(|g| g.count), Some(count));
        prop_assert_eq!(d.recommended.requests.gpu.map(|g| g.count), Some(count));
        if !allow { prop_assert_eq!(d.applied.requests.gpu, c.prior.spec.requests.gpu); }
    }

    #[test]
    fn s3_reductions_absent_when_s3_fails(prior in 0.5f64..512.0, demand in 0.1f64..512.0, same_size in 0u32..4, n_hist in 0usize..3) {
        let fx = Fx::new();
        let mut c = ctx(Some(prior), Some(2.0));
        c.platform.retry.same_size = same_size;
        // Fewer than min_history successes: S3(b) cannot hold; no ladder: S3(a) cannot.
        c.history = (0..n_hist).map(|_| success(demand * 0.5, prior)).collect();
        let d = fx.decide(&c, Some(&est(demand, 1.0)));
        prop_assert!(mem(&d.applied).unwrap() >= Bytes::from_gib_f64(prior));
    }
}
