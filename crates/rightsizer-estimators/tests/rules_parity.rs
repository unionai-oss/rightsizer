//! `rules-sized` vs the tuner's Python (`bench_all/arms.py` `_sized_one`,
//! `bench/arms.py` `_size_one`) on hand-built cases. Expected numbers come
//! from running the tuner's code: see `tests/rules_parity_gen.py`.
//!
//! The estimator emits demand; the tuner's arm emits a cell. The bridge is
//! the decision rule's need: `(demand + overhead) × 1.05` (`need_headroom`,
//! the tuner's `LABEL_HEADROOM`). The bench power law was fit to the
//! requirement itself, so it is compared as is.

use std::collections::{BTreeMap, HashMap};

use rightsizer_core::grid::Grid;
use rightsizer_core::*;
use rightsizer_estimators::rules_sized::{ColdStart, MemoryBasis, RulesSized, RulesSizedParams};
use serde::Deserialize;

const MIB: f64 = 1048576.0;
const NEED_HEADROOM: f64 = 1.05;

#[derive(Deserialize)]
struct Hist {
    container_peak_mib: f64,
    input_bytes: u64,
    mean_cores: f64,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    kind: String,
    #[serde(default)]
    platform: Option<String>,
    #[serde(default)]
    overhead_mib: f64,
    input_bytes: u64,
    #[serde(default)]
    history: Vec<Hist>,
    #[serde(default)]
    margin: Option<f64>,
    #[serde(default)]
    base_mib: Option<f64>,
    #[serde(default)]
    slope: Option<f64>,
    #[serde(default)]
    cpu_margin: Option<f64>,
    #[serde(default)]
    format: Option<String>,
    #[serde(default)]
    a: Option<f64>,
    #[serde(default)]
    b: Option<f64>,
    tuner_workload_mib: f64,
    tuner_need_mib: f64,
    tuner_mem_cell: usize,
    #[serde(default)]
    tuner_cpu_cell: Option<usize>,
    deviation: Option<String>,
}

#[derive(Deserialize)]
struct File {
    cases: Vec<Case>,
}

fn key() -> WorkloadKey {
    WorkloadKey::parse("k8s://c/ns/job/x").unwrap()
}

fn record(i: usize, h: &Hist) -> RunRecord {
    RunRecord {
        v: 1,
        key: key(),
        cluster: None,
        invocation_id: format!("r{i}"),
        attempt: 1,
        decision_id: None,
        started_at: chrono::DateTime::parse_from_rfc3339("2026-11-01T00:00:00Z").unwrap().with_timezone(&chrono::Utc),
        inputs: vec![InputMeta::blob("a", h.input_bytes)],
        code_digest: None,
        requested: None,
        origin_prior: None,
        status: RunStatus::Succeeded,
        status_confidence: Confidence::Observed,
        memory_peak: Some(Measurement::exact(h.container_peak_mib * MIB)),
        cpu_mean: Some(Measurement::exact(h.mean_cores)),
        cpu_peak_window: None,
        cpu_throttled_frac: None,
        gpu_memory_peak: None,
        gpu_util_mean: None,
        t_peak_frac: None,
        duration_s: None,
        source: OutcomeSource::Watcher,
        trust: Trust::Verified,
    }
}

fn format(s: &str) -> InputFormat {
    serde_json::from_value(serde_json::Value::String(s.into())).unwrap()
}

#[test]
fn rules_sized_matches_the_tuner() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/rules_sized_parity.json");
    let file: File = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let grid = Grid::rs_grid_v1();
    assert!(file.cases.len() >= 8);
    for c in &file.cases {
        let mut input = InputMeta::blob("a", c.input_bytes);
        let mut params = RulesSizedParams { cold: HashMap::new(), ..Default::default() };
        if c.kind == "power" {
            let f = format(c.format.as_deref().unwrap());
            input.format = f;
            params.cold.insert(f, ColdStart::power_law(c.a.unwrap(), c.b.unwrap()));
        } else {
            params.cold_default = ColdStart::linear(c.base_mib.unwrap(), c.slope.unwrap());
            params.margin = c.margin.unwrap();
            params.cpu_margin = c.cpu_margin.unwrap();
        }
        let mut platform = PlatformRef::new(c.platform.as_deref().unwrap_or("k8s"));
        if c.overhead_mib > 0.0 {
            platform.overhead = OverheadModel { memory_mib: vec![(0.5, c.overhead_mib)], cpu_millis: 0 };
        }
        // Newest first: the tuner's lists are oldest first.
        let history: Vec<RunRecord> = c.history.iter().enumerate().map(|(i, h)| record(i, h)).rev().collect();
        let ctx = WorkloadContext {
            v: 1,
            key: key(),
            unit: UnitId("main".into()),
            platform,
            code: None,
            inputs: vec![input],
            manifest: ManifestFeatures::default(),
            prior: Prior::default(),
            history,
            labels: BTreeMap::new(),
        };
        let est = RulesSized::new(params);
        let center = match est.memory_basis(&ctx).unwrap() {
            MemoryBasis::Warm { center, .. } | MemoryBasis::Cold { center, .. } => center / MIB,
        };
        let need = if c.kind == "power" { center } else { (center + c.overhead_mib) * NEED_HEADROOM };
        let cell = grid.memory_cell(Bytes::from_mib_f64(need));
        match &c.deviation {
            None => {
                assert!((need / c.tuner_need_mib - 1.0).abs() < 1e-9, "{}: need {need} vs tuner {}", c.name, c.tuner_need_mib);
                assert_eq!(cell, c.tuner_mem_cell, "{}", c.name);
            }
            Some(why) => {
                // The documented deviation: never scale below the anchor run.
                assert!(center > c.tuner_workload_mib, "{} ({why})", c.name);
                assert!(cell >= c.tuner_mem_cell, "{}", c.name);
            }
        }
        if let Some(want) = c.tuner_cpu_cell {
            let cores = est.cpu_basis(&ctx);
            assert_eq!(grid.cpu_cell(MilliCpu::from_cores_f64(cores)), want, "{}: cpu {cores}", c.name);
        }
    }
}
