//! Shared fixtures for the history integration tests.
#![allow(dead_code)]

use chrono::{DateTime, Duration, Utc};
use rightsizer_core::breaker::{BreakerScope, Control, Reset, Trip, TripReason};
use rightsizer_core::{
    AttemptRecord, Confidence, Decision, DecisionSource, Lease, Measurement, Mode, OutcomeSource, Prior, ResourceSpec, RunRecord,
    RunStatus, Trust, UnitId, WorkloadKey,
};
use ulid::Ulid;

pub fn key(s: &str) -> WorkloadKey {
    WorkloadKey::parse(s).unwrap()
}

pub fn k1() -> WorkloadKey {
    key("k8s://c1/ns/job/train")
}

pub fn k2() -> WorkloadKey {
    key("flyte://acme/proj/dev/env.train")
}

pub fn t(secs: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(1_700_000_000 + secs, 0).unwrap()
}

pub fn rec(k: &WorkloadKey, inv: &str, attempt: u32, started_at: DateTime<Utc>) -> RunRecord {
    RunRecord {
        v: 1,
        key: k.clone(),
        cluster: None,
        invocation_id: inv.into(),
        attempt,
        decision_id: None,
        started_at,
        inputs: vec![],
        code_digest: Some("sha256:abc".into()),
        requested: None,
        origin_prior: None,
        status: RunStatus::Succeeded,
        status_confidence: Confidence::Observed,
        memory_peak: Some(Measurement::exact(1.5e9)),
        cpu_mean: Some(Measurement::exact(0.5)),
        cpu_peak_window: None,
        cpu_throttled_frac: None,
        gpu_memory_peak: None,
        gpu_util_mean: None,
        t_peak_frac: None,
        duration_s: Some(12.0),
        staged_bytes: None,
        source: OutcomeSource::Watcher,
        trust: Trust::Verified,
    }
}

pub fn decision(k: &WorkloadKey, at: DateTime<Utc>, mode: Mode, est: &str) -> Decision {
    Decision {
        v: 1,
        id: Ulid::from_parts(at.timestamp_millis() as u64, rand_u128()),
        key: k.clone(),
        unit: UnitId("main".into()),
        group: None,
        mode,
        recommended: ResourceSpec::default(),
        applied: ResourceSpec::default(),
        shape: None,
        prior: Prior::default(),
        held: vec![],
        source: DecisionSource::Estimator { id: est.into(), version: "1".into() },
        estimate: None,
        propensity: 1.0,
        expected_cost: Some(0.1),
        p_fail: Some(0.01),
        advisories: vec![],
        explanation: vec![],
        context_digest: "d".into(),
    }
}

pub fn rand_u128() -> u128 {
    Ulid::generate().random()
}

pub fn trip(scope: BreakerScope, at: DateTime<Utc>) -> Trip {
    Trip { v: 1, id: Ulid::from_parts(at.timestamp_millis() as u64, rand_u128()), scope, at, reason: TripReason::Manual, detail: None }
}

pub fn reset(scope: BreakerScope, at: DateTime<Utc>) -> Reset {
    Reset { v: 1, scope, at, by: Some("op".into()) }
}

pub fn control(mode: Mode) -> Control {
    Control { v: 1, mode_ceiling: mode, updated_at: Utc::now(), by: Some("op".into()) }
}

pub fn attempt(k: &WorkloadKey, inv: &str, written_at: DateTime<Utc>) -> AttemptRecord {
    AttemptRecord {
        v: 1,
        key: k.clone(),
        invocation_id: inv.into(),
        origin_prior: Default::default(),
        applied: Default::default(),
        decision_id: None,
        reduced: true,
        written_at,
        lease: Some(Lease { opened_at: written_at, closed_at: None, outcome: None }),
    }
}

pub fn invs(v: &[RunRecord]) -> Vec<String> {
    v.iter().map(|r| format!("{}#{}", r.invocation_id, r.attempt)).collect()
}

pub fn since_now(d: Duration) -> DateTime<Utc> {
    Utc::now() + d
}
