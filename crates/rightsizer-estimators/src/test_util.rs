//! Test fixtures shared by the unit tests.

use std::collections::BTreeMap;

use rightsizer_core::*;

pub fn gib(x: f64) -> f64 {
    x * Bytes::GIB as f64
}

pub fn key() -> WorkloadKey {
    WorkloadKey::parse("k8s://c/ns/job/x").unwrap()
}

pub fn ctx_with(inputs: Vec<InputMeta>, history: Vec<RunRecord>) -> WorkloadContext {
    WorkloadContext {
        v: 1,
        key: key(),
        unit: UnitId("main".into()),
        platform: PlatformRef::new("k8s"),
        code: None,
        inputs,
        manifest: ManifestFeatures::default(),
        prior: Prior::default(),
        history,
        labels: BTreeMap::new(),
    }
}

/// A verified record with an optional memory peak and total input bytes.
pub fn record(id: &str, status: RunStatus, peak: Option<Measurement>, input_bytes: Option<u64>) -> RunRecord {
    RunRecord {
        v: 1,
        key: key(),
        cluster: None,
        invocation_id: id.into(),
        attempt: 1,
        decision_id: None,
        started_at: chrono::DateTime::parse_from_rfc3339("2026-11-01T00:00:00Z").unwrap().with_timezone(&chrono::Utc),
        inputs: input_bytes.map(|b| vec![InputMeta::blob("x", b)]).unwrap_or_default(),
        code_digest: None,
        requested: None,
        origin_prior: None,
        status,
        status_confidence: Confidence::Observed,
        memory_peak: peak,
        cpu_mean: None,
        cpu_peak_window: None,
        cpu_throttled_frac: None,
        gpu_memory_peak: None,
        gpu_util_mean: None,
        t_peak_frac: None,
        duration_s: None,
        staged_bytes: None,
        source: OutcomeSource::Watcher,
        trust: Trust::Verified,
    }
}
