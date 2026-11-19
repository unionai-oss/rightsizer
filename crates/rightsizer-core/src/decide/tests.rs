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
        staged_bytes: None,
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
fn s4_clamp_reason_names_missing_platform_state() {
    // An adapter without node shapes (Ray without autoscaler node types or
    // `[ray] shapes`) holds increases with a ceiling at the prior and says why.
    let mut fx = Fx::new();
    fx.constraints.ceiling = Requests { memory: Some(Bytes::gib(4)), ..Default::default() };
    fx.constraints.ceiling_reason = Some("no_platform_state".into());
    let c = ctx(Some(4.0), Some(1.0));
    let d = fx.decide(&c, Some(&est(32.0, 1.0)));
    assert_eq!(mem(&d.applied), Some(Bytes::gib(4)));
    assert!(
        d.advisories.iter().any(|a| matches!(a, Advisory::Clamped { axis: Axis::Memory, reason } if reason == "no_platform_state")),
        "{:?}",
        d.advisories
    );
    // Without a reason the user ceiling is reported as "user".
    fx.constraints.ceiling_reason = None;
    let d = fx.decide(&c, Some(&est(32.0, 1.0)));
    assert!(d.advisories.iter().any(|a| matches!(a, Advisory::Clamped { reason, .. } if reason == "user")), "{:?}", d.advisories);
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

// ── adversarial review: invariants over random contexts ────────────────

mod adversarial {
    use super::*;

    /// An independent reading of SPEC §2.3 S3(b) (not `safety::s3b_evidence`).
    fn oracle_s3b(c: &WorkloadContext, proposed: Bytes, min_history: u32, allow_unverified: bool) -> bool {
        let digest = c.code.as_ref().map(|x| x.digest.as_str());
        let b_now = c.input_bytes();
        let mut w = 0.0;
        for r in &c.history {
            let Some(m) = r.memory_peak else { continue };
            if r.status != RunStatus::Succeeded
                || r.status_confidence != Confidence::Observed
                || !(allow_unverified || r.trust >= Trust::Authenticated)
                || !matches!(m.censor, Censor::Exact | Censor::AtMost)
                || !m.value.is_finite()
            {
                continue;
            }
            let other = r.code_digest.as_deref() != digest;
            let res = m.resolution_s.unwrap_or(0.0);
            let coarse = res >= 10.0;
            let h = match (coarse, res > 1.0, other) {
                (true, _, false) => 1.5,
                (true, _, true) => 1.75,
                (false, true, false) => 1.25,
                (false, true, true) => 1.5,
                (false, false, false) => 1.05,
                (false, false, true) => 1.25,
            };
            let scale = match (b_now, r.input_bytes()) {
                (Some(n), Some(hb)) if hb > 0 => (n as f64 / hb as f64).max(1.0),
                _ => 1.0,
            };
            if m.value * scale * h <= proposed.0 as f64 {
                w += if coarse { 0.5 } else { 1.0 };
            }
        }
        w + 1e-9 >= min_history as f64
    }

    /// An independent reading of S3(a).
    fn oracle_s3a(c: &WorkloadContext, ceiling: Bytes, attempt: Option<&AttemptContext>, allow_unverified: bool) -> bool {
        let Some(l) = &c.platform.retry.ladder else { return false };
        if !c.platform.memory_enforced || l.max_steps == 0 || l.memory_factor.is_nan() || l.memory_factor <= 1.0 {
            return false;
        }
        if l.driver == crate::types::LadderDriver::Orchestrator
            && !attempt.and_then(|a| a.max_attempts).is_some_and(|m| m.saturating_sub(1) >= l.max_steps)
        {
            return false;
        }
        let largest = c
            .history
            .iter()
            .filter(|r| r.status == RunStatus::Succeeded && (allow_unverified || r.trust >= Trust::Authenticated))
            .filter_map(|r| r.requested_memory())
            .max()
            .map(Bytes);
        let ff = [c.prior.spec.requests.memory, largest, l.final_floor.memory].into_iter().flatten().max();
        ff.is_none_or(|f| f <= ceiling)
    }

    #[derive(Clone, Debug)]
    struct RecSpec {
        peak_gib: f64,
        req_gib: f64,
        status: u8,
        confidence_observed: bool,
        censor: u8,
        resolution: u8,
        trust: u8,
        other_digest: bool,
        input_gib: Option<f64>,
    }

    fn rec_strategy() -> impl Strategy<Value = RecSpec> {
        (
            0.0f64..96.0,
            0.25f64..128.0,
            0u8..6,
            prop::bool::weighted(0.8),
            0u8..3,
            0u8..4,
            0u8..3,
            any::<bool>(),
            prop::option::of(0.01f64..64.0),
        )
            .prop_map(|(peak_gib, req_gib, status, confidence_observed, censor, resolution, trust, other_digest, input_gib)| RecSpec {
                peak_gib,
                req_gib,
                status,
                confidence_observed,
                censor,
                resolution,
                trust,
                other_digest,
                input_gib,
            })
    }

    fn build_rec(i: usize, s: &RecSpec) -> RunRecord {
        let mut r = success(s.peak_gib, s.req_gib);
        r.invocation_id = format!("inv-{i}");
        r.started_at = DateTime::from_timestamp(1_700_000_000 - i as i64 * 3600, 0).unwrap();
        r.status = match s.status {
            0..=2 => RunStatus::Succeeded,
            3 => RunStatus::OomKilled,
            4 => RunStatus::Failed,
            _ => RunStatus::Unknown,
        };
        r.status_confidence = if s.confidence_observed { Confidence::Observed } else { Confidence::Inferred };
        let censor = [Censor::Exact, Censor::AtMost, Censor::AtLeast][s.censor as usize];
        let resolution = [None, Some(0.5), Some(5.0), Some(60.0)][s.resolution as usize];
        r.memory_peak = Some(Measurement { value: s.peak_gib * GIB, censor, resolution_s: resolution });
        r.trust = [Trust::Unverified, Trust::Authenticated, Trust::Verified][s.trust as usize];
        if s.other_digest {
            r.code_digest = Some("d0".into());
        }
        if let Some(g) = s.input_gib {
            r.inputs = vec![InputMeta::blob("in", Bytes::from_gib_f64(g).0)];
        }
        r
    }

    fn est_strategy() -> impl Strategy<Value = Estimate> {
        let mem = prop_oneof![
            (0.0f64..300.0).prop_map(|g| AxisEstimate::Point { value: g * GIB }),
            (0usize..27).prop_map(|i| {
                let mut p = vec![0.0f32; 27];
                p[i] = 1.0;
                AxisEstimate::Cells { grid: "rs-grid-v1".into(), probs: p }
            }),
            prop::collection::vec((0.01f32..0.99, 0.0f64..200.0), 1..5)
                .prop_map(|q| AxisEstimate::Quantiles { q: q.into_iter().map(|(l, v)| (l, v * GIB)).collect() }),
            (0.0f64..200.0).prop_map(|v| AxisEstimate::Quantiles { q: vec![(0.1, v * GIB), (0.5, v * GIB), (0.9, v * GIB)] }),
        ];
        let cpu = prop_oneof![(0.0f64..64.0).prop_map(|v| AxisEstimate::Point { value: v }),];
        let dur = prop::option::of(prop_oneof![
            (0.0f64..100_000.0).prop_map(|v| AxisEstimate::Point { value: v }),
            Just(AxisEstimate::Quantiles { q: vec![(0.1, 0.0), (0.5, 0.0), (0.9, 0.0)] }),
        ]);
        (prop::option::of(mem), prop::option::of(cpu), dur, prop::option::of(0.0f32..1.0), prop::bool::weighted(0.2)).prop_map(|(m, c, d, t, action)| {
            let mut e = est(1.0, 1.0);
            if action {
                e.kind = EstimateKind::Action;
            }
            e.memory = m;
            e.cpu_mean = c;
            e.duration_s = d;
            e.t_peak_frac = t;
            e
        })
    }

    #[derive(Clone, Debug)]
    struct World {
        prior_gib: f64,
        prior_cpu: f64,
        mem_limit_ratio: Option<f64>,
        cpu_limit: bool,
        guaranteed: bool,
        history: Vec<RecSpec>,
        ladder: u8,
        max_attempts: Option<u32>,
        memory_enforced: bool,
        blocked: bool,
        can_classify: bool,
        ceiling_gib: Option<f64>,
        cpu_ceiling_m: Option<u32>,
        input_gib: Option<f64>,
        allow_unverified: bool,
        walltime: Option<u64>,
        zero_price: bool,
        model: u8,
    }

    fn world_strategy() -> impl Strategy<Value = World> {
        (
            (0.25f64..256.0, 0.1f64..32.0, prop::option::of(1.0f64..4.0), any::<bool>(), prop::bool::weighted(0.2)),
            prop::collection::vec(rec_strategy(), 0..9),
            (0u8..3, prop::option::of(1u32..5), prop::bool::weighted(0.8), prop::bool::weighted(0.15), prop::bool::weighted(0.9)),
            (prop::option::of(0.5f64..512.0), prop::option::of(100u32..64_000), prop::option::of(0.01f64..64.0)),
            (prop::bool::weighted(0.2), prop::option::of(1u64..100_000), prop::bool::weighted(0.1), 0u8..4),
        )
            .prop_map(|(p, history, l, c, x)| World {
                prior_gib: p.0,
                prior_cpu: p.1,
                mem_limit_ratio: p.2,
                cpu_limit: p.3,
                guaranteed: p.4,
                history,
                ladder: l.0,
                max_attempts: l.1,
                memory_enforced: l.2,
                blocked: l.3,
                can_classify: l.4,
                ceiling_gib: c.0,
                cpu_ceiling_m: c.1,
                input_gib: c.2,
                allow_unverified: x.0,
                walltime: x.1,
                zero_price: x.2,
                model: x.3,
            })
    }

    fn setup(w: &World) -> (Fx, WorkloadContext, Option<AttemptContext>) {
        let mut fx = Fx::new();
        fx.params.reductions_blocked = w.blocked.then_some(HoldReason::Breaker);
        fx.params.can_classify = w.can_classify;
        fx.params.constraints.allow_unverified = w.allow_unverified;
        fx.constraints.ceiling = Requests {
            memory: w.ceiling_gib.map(Bytes::from_gib_f64),
            cpu: w.cpu_ceiling_m.map(MilliCpu),
            ..Default::default()
        };
        if w.zero_price {
            fx.state.price = crate::types::PriceModel::Linear { rates: crate::types::Rates { cpu_core_hr: 0.0, mem_gib_hr: 0.0, gpu_hr: BTreeMap::new() } };
        }
        match w.model {
            1 => {
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
                    shapes: (1..9).map(|k| shape(&format!("s{k}"), 1 << (k / 2), 2u64 << k, 0.05 * (1 << k) as f64)).collect(),
                };
                fx.state.price = crate::types::PriceModel::Instance { usage: Some("training".into()) };
            }
            2 => {
                fx.state.request_model = RequestModel::Combos {
                    combos: vec![
                        Combo { cpu: MilliCpu(250), memory: vec![Bytes::mib(512), Bytes::gib(1), Bytes::gib(2)] },
                        Combo { cpu: MilliCpu(1000), memory: (2..=8).map(Bytes::gib).collect() },
                        Combo { cpu: MilliCpu(4000), memory: (8..=30).map(Bytes::gib).collect() },
                        Combo { cpu: MilliCpu(16000), memory: (32..=120).step_by(8).map(Bytes::gib).collect() },
                    ],
                };
                fx.state.price = crate::types::PriceModel::Linear { rates: crate::types::Rates { cpu_core_hr: 0.04, mem_gib_hr: 0.0044, gpu_hr: BTreeMap::new() } };
            }
            3 => {
                fx.state.shapes = vec![
                    NodeShape { name: None, cpu: MilliCpu::cores(8), memory: Bytes::gib(32), gpu: None, price_per_hour: 0.4, host_per_gpu: None },
                    NodeShape { name: None, cpu: MilliCpu::cores(64), memory: Bytes::gib(256), gpu: None, price_per_hour: 3.2, host_per_gpu: None },
                ];
            }
            _ => {}
        }
        let mut c = ctx(Some(w.prior_gib), Some(w.prior_cpu));
        if w.guaranteed {
            c.prior.spec.requests.cpu = Some(MilliCpu::from_cores_f64(w.prior_cpu).ceil_whole());
            c.prior.spec.limits = Limits { memory: c.prior.spec.requests.memory, cpu: c.prior.spec.requests.cpu, ephemeral_storage: None };
            c.prior.qos = Some(QosClass::Guaranteed);
        } else {
            if let Some(r) = w.mem_limit_ratio {
                c.prior.spec.limits.memory = Some(Bytes::from_gib_f64(w.prior_gib * r));
            }
            if w.cpu_limit {
                c.prior.spec.limits.cpu = Some(MilliCpu::from_cores_f64(w.prior_cpu * 2.0));
            }
        }
        c.history = w.history.iter().enumerate().map(|(i, s)| build_rec(i, s)).collect();
        if let Some(g) = w.input_gib {
            c.inputs = vec![InputMeta::blob("in", Bytes::from_gib_f64(g).0)];
        }
        c.platform.memory_enforced = w.memory_enforced;
        c.platform.walltime_s = w.walltime;
        c.platform.retry.ladder = match w.ladder {
            1 => Some(Ladder::caller()),
            2 => Some(Ladder { driver: crate::types::LadderDriver::Orchestrator, ..Ladder::caller() }),
            _ => None,
        };
        let at = w.max_attempts.map(|m| AttemptContext { v: 1, invocation_id: "now".into(), attempt: 1, max_attempts: Some(m) });
        (fx, c, at)
    }

    fn run(fx: &Fx, c: &WorkloadContext, e: &Estimate, at: Option<&AttemptContext>) -> Decision {
        let mut inp = DecisionInput::new(c, Some(e), &fx.grid, &fx.state, &fx.constraints, &fx.params, Mode::Enforce);
        inp.attempt = at;
        decide(&inp)
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 384, ..ProptestConfig::default() })]

        /// S2/S3/S4 and numerical sanity over random worlds.
        #[test]
        fn adversarial_invariants(w in world_strategy(), e in est_strategy()) {
            let (fx, c, at) = setup(&w);
            let d = run(&fx, &c, &e, at.as_ref());
            let pr = &c.prior.spec.requests;
            let a = &d.applied.requests;
            let (ceil, ..) = crate::constraints::ceiling(&fx.state, &fx.constraints, pr.gpu, &fx.grid);
            if let Some(x) = d.expected_cost { prop_assert!(x.is_finite(), "expected cost {x}"); }
            prop_assert!(d.applied.requests.gpu == pr.gpu && d.recommended.requests.gpu.map(|g| g.count) == pr.gpu.map(|g| g.count));
            if let Some(p) = d.p_fail { prop_assert!(p.is_finite() && (0.0..=1.0 + 1e-6).contains(&p), "p_fail {p}"); }
            // S3: a reduction below the origin prior needs (a) or (b), and no block.
            if let (Some(am), Some(pm)) = (a.memory, pr.memory) && am < pm {
                prop_assert!(!w.blocked && w.can_classify, "reduction while blocked: {:?}", d.explanation);
                let s3a = oracle_s3a(&c, ceil, at.as_ref(), w.allow_unverified);
                let s3b = oracle_s3b(&c, am, fx.params.min_history, w.allow_unverified);
                prop_assert!(s3a || s3b, "reduction {} → {} without S3: {:#?}", pm, am, d.explanation);
                if !w.memory_enforced { prop_assert!(s3b, "not enforced: only (b) applies"); }
            }
            // S4: an increase stays within the ceiling.
            if let (Some(am), Some(pm)) = (a.memory, pr.memory) && am > pm {
                prop_assert!(am <= ceil, "increase {am} above ceiling {ceil}");
            }
            // S2: limits.
            for spec in [&d.applied, &d.recommended] {
                prop_assert_eq!(spec.limits.memory.is_some(), c.prior.spec.limits.memory.is_some());
                prop_assert_eq!(spec.limits.cpu.is_some(), c.prior.spec.limits.cpu.is_some());
                if let (Some(l), Some(r)) = (spec.limits.memory, spec.requests.memory) { prop_assert!(l >= r); }
                if let (Some(l), Some(r)) = (spec.limits.cpu, spec.requests.cpu) { prop_assert!(l >= r); }
                if w.guaranteed {
                    prop_assert_eq!(spec.limits.memory, spec.requests.memory);
                    prop_assert_eq!(spec.limits.cpu, spec.requests.cpu);
                    prop_assert!(spec.requests.cpu.is_none_or(|x| x.is_whole()), "guaranteed whole CPUs: {:?}", spec.requests.cpu);
                }
            }
            // Exploration off: propensity 1.
            prop_assert_eq!(d.propensity, 1.0);
        }

        /// The size-scaled floor binds `applied` and `recommended` (within the
        /// ceiling): history shows the workload needs it.
        #[test]
        fn adversarial_floor_binds_both(w in world_strategy(), e in est_strategy()) {
            let (fx, c, at) = setup(&w);
            let d = run(&fx, &c, &e, at.as_ref());
            let pr = &c.prior.spec.requests;
            let digest = c.code.as_ref().map(|x| x.digest.as_str());
            let b = crate::constraints::bounds(&c.prior, &fx.constraints, &fx.state, &fx.grid, &c.history, digest, c.input_bytes(), &fx.params.constraints);
            if let Some(f) = b.mem_floor {
                if let Some(rm) = d.recommended.requests.memory && e.memory.is_some() && d.recommended.requests != *pr {
                    prop_assert!(rm >= f, "recommended {rm} below the floor {f}: {:#?}", d.explanation);
                }
                if let Some(am) = d.applied.requests.memory && pr.memory.is_some_and(|p| p < f) && e.memory.is_some() && d.applied.requests != *pr {
                    prop_assert!(am >= f, "applied {am} below the floor {f} (prior {:?}): {:#?}", pr.memory, d.explanation);
                }
            }
        }
    }

    #[test]
    fn no_platform_state_floor_above_prior_is_clamped_not_below_floor() {
        // Reported: an adapter without node shapes sets the ceiling to the
        // prior (`no_platform_state`); history's floor is above it. The
        // increase the floor demands is clamped (S4: counted and explained),
        // and `recommended` must not fall below the floor.
        let mut fx = Fx::new();
        fx.constraints.ceiling = Requests { memory: Some(Bytes::gib(4)), ..Default::default() };
        fx.constraints.ceiling_reason = Some("no_platform_state".into());
        let mut c = ctx(Some(4.0), Some(1.0));
        c.history = vec![success(6.0, 8.0)];
        let d = fx.decide(&c, Some(&est(1.0, 1.0)));
        assert_eq!(mem(&d.applied), Some(Bytes::gib(4)));
        assert!(mem(&d.recommended).unwrap() >= Bytes::gib(4), "recommended {:?} below the (clamped) floor", mem(&d.recommended));
        assert!(
            d.advisories.iter().any(|a| matches!(a, Advisory::Clamped { axis: Axis::Memory, reason } if reason == "no_platform_state")),
            "{:?}",
            d.advisories
        );
        assert!(!d.infeasible_floor(), "the floor is not infeasible on the platform, only unknown: {:?}", d.advisories);
        // A real ceiling (a node shape) still flags infeasible_floor, and is counted as a clamp.
        let mut fx = Fx::new();
        fx.state.shapes = vec![NodeShape { name: None, cpu: MilliCpu::cores(4), memory: Bytes::gib(4), gpu: None, price_per_hour: 0.2, host_per_gpu: None }];
        let d = fx.decide(&c, Some(&est(1.0, 1.0)));
        assert!(d.infeasible_floor());
        assert!(d.advisories.iter().any(|a| matches!(a, Advisory::Clamped { axis: Axis::Memory, .. })), "{:?}", d.advisories);
        assert!(mem(&d.recommended).unwrap() >= Bytes::gib(4));
    }

    #[test]
    fn guaranteed_whole_cpus_with_fractional_cpu_ceiling() {
        let mut fx = Fx::new();
        fx.state.quota = Some(Quota { cpu: Some(MilliCpu(3500)), ..Default::default() });
        let mut c = ctx(Some(4.0), Some(2.0));
        c.prior.spec.limits = Limits { memory: Some(Bytes::gib(4)), cpu: Some(MilliCpu(2000)), ephemeral_storage: None };
        c.prior.qos = Some(QosClass::Guaranteed);
        let d = fx.decide(&c, Some(&est(4.0, 6.0)));
        for s in [&d.applied, &d.recommended] {
            let cpu = s.requests.cpu.unwrap();
            assert!(cpu.is_whole(), "{cpu:?} is not whole: the static CPU manager needs whole CPUs");
            assert!(cpu <= MilliCpu(3500));
        }
    }

    #[test]
    fn cpu_granularity_whole_cpus_for_slurm_like_platforms() {
        // Slurm renders `--cpus-per-task` as an integer: a 500m decision would
        // be rendered as 1 CPU (or rejected), so the decision must already be whole.
        let mut fx = Fx::new();
        fx.state.cpu_granularity = Some(MilliCpu(1000));
        fx.state.quota = Some(Quota { cpu: Some(MilliCpu(3500)), ..Default::default() });
        let c = ctx(Some(4.0), Some(2.0));
        for cores in [0.2, 0.45, 1.3, 2.6, 9.0] {
            let d = fx.decide(&c, Some(&est(3.0, cores)));
            for s in [&d.applied, &d.recommended] {
                let cpu = s.requests.cpu.unwrap();
                assert!(cpu.is_whole() && cpu <= MilliCpu(3500), "{cores} cores → {cpu:?}");
            }
        }
        // Without granularity the same platform decides sub-core CPUs.
        fx.state.cpu_granularity = None;
        let d = fx.decide(&c, Some(&est(3.0, 0.2)));
        assert!(!d.applied.requests.cpu.unwrap().is_whole(), "{:?}", d.applied.requests.cpu);
        // JSON: absent by default, round-trips when set.
        let j = serde_json::to_value(PlatformState::default()).unwrap();
        assert!(j.get("cpu_granularity").is_none());
        let st: PlatformState = serde_json::from_str(r#"{"cpu_granularity": 1000}"#).unwrap();
        assert_eq!(st.cpu_granularity, Some(MilliCpu(1000)));
    }

    #[test]
    fn catalog_kept_prior_never_renders_a_smaller_shape() {
        // The catalog has no shape that holds the 64Gi prior (e.g. the
        // user's instance type is not in the refreshed catalog). Keeping the
        // prior must not render the largest listed shape (31Gi usable): that
        // is a reduction S3 never checked.
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
        fx.state.request_model = RequestModel::Catalog { shapes: vec![shape("ml.m5.xlarge", 4, 16, 0.23), shape("ml.m5.2xlarge", 8, 32, 0.461)] };
        fx.state.price = crate::types::PriceModel::Instance { usage: Some("training".into()) };
        let c = ctx(Some(64.0), Some(8.0));
        for demand in [2.0, 20.0, 60.0] {
            let d = fx.decide(&c, Some(&est(demand, 2.0)));
            if let Some(name) = &d.shape {
                let RequestModel::Catalog { shapes } = &fx.state.request_model else { unreachable!() };
                let s = shapes.iter().find(|s| &s.name == name).unwrap();
                assert!(s.usable() >= Bytes::gib(64), "demand {demand}: renders {name} ({}) for a 64Gi prior without S3: {:?}", s.usable(), d.explanation);
            }
            assert!(mem(&d.applied).unwrap() >= Bytes::gib(64));
        }
    }

    fn gpu_ctx() -> (Fx, WorkloadContext, Estimate) {
        let mut fx = Fx::new();
        fx.params.constraints.allow_gpu_type_change = true;
        fx.params.risk = Some(RiskProfile::LEAN);
        fx.constraints.allowed_shapes = Some(vec!["L4-24G:1".parse().unwrap()]);
        let mut c = ctx(Some(16.0), Some(4.0));
        c.prior.spec.requests.gpu = Some("A100-80G:1".parse().unwrap());
        let mut e = est(8.0, 2.0);
        e.gpu_memory = Some(AxisEstimate::Point { value: 6.0 * GIB });
        (fx, c, e)
    }

    fn gpu_rec(i: usize, gpu_peak_gib: f64) -> RunRecord {
        let mut r = success(8.0, 16.0);
        r.invocation_id = format!("g{i}");
        r.gpu_memory_peak = Some(Measurement::exact(gpu_peak_gib * GIB));
        r
    }

    #[test]
    fn gpu_type_reduction_uses_s3b_headroom_for_other_digest() {
        // 21 GiB peaks × 1.05 = 22.05 fits under an L4's ~22 GiB? No — but
        // from another digest the headroom is 1.25 (26.25 GiB): never fits.
        let (fx, mut c, e) = gpu_ctx();
        c.history = (0..3)
            .map(|i| {
                let mut r = gpu_rec(i, 18.0);
                r.code_digest = Some("other".into());
                r
            })
            .collect();
        let d = fx.decide(&c, Some(&e));
        assert_eq!(d.applied.requests.gpu, c.prior.spec.requests.gpu, "18 GiB × 1.25 (other digest) does not fit an L4: {:?}", d.explanation);
    }

    #[test]
    fn gpu_type_reduction_needs_double_coarse_evidence() {
        let (fx, mut c, e) = gpu_ctx();
        c.history = (0..3)
            .map(|i| {
                let mut r = gpu_rec(i, 6.0);
                r.gpu_memory_peak.as_mut().unwrap().resolution_s = Some(60.0);
                r
            })
            .collect();
        let d = fx.decide(&c, Some(&e));
        assert_eq!(d.applied.requests.gpu, c.prior.spec.requests.gpu, "3 coarse peaks count 1.5 < 3");
    }

    #[test]
    fn gpu_type_reduction_ignores_lower_bound_peaks_and_unclassifiable_surfaces() {
        let (mut fx, mut c, e) = gpu_ctx();
        c.history = (0..3)
            .map(|i| {
                let mut r = gpu_rec(i, 6.0);
                r.gpu_memory_peak.as_mut().unwrap().censor = Censor::AtLeast;
                r
            })
            .collect();
        let d = fx.decide(&c, Some(&e));
        assert_eq!(d.applied.requests.gpu, c.prior.spec.requests.gpu, "AtLeast GPU peaks are not evidence");
        c.history = (0..3).map(|i| gpu_rec(i, 6.0)).collect();
        let ok = fx.decide(&c, Some(&e));
        assert_eq!(ok.applied.requests.gpu, c.prior.spec.requests.gpu, "A100-80G → L4 is more than the trust region's 2 steps (half)");
        // Control: A100-40G → L4 is within the trust region, and exact evidence permits it.
        c.prior.spec.requests.gpu = Some("A100-40G:1".parse().unwrap());
        let ok = fx.decide(&c, Some(&e));
        assert_ne!(ok.applied.requests.gpu, c.prior.spec.requests.gpu, "control: exact evidence permits the smaller device");
        fx.params.can_classify = false;
        let d = fx.decide(&c, Some(&e));
        assert_eq!(d.applied.requests.gpu, c.prior.spec.requests.gpu, "unclassifiable surface: no incompressible reductions");
    }
}
