//! Rust-produced wire fixtures for the cross-language tests
//! (ARCHITECTURE.md §4, `tests/fixtures/wire/`).
//!
//! `RIGHTSIZER_WRITE_FIXTURES=1 cargo test -p rightsizer-py --test wire_fixtures`
//! (re)writes them; otherwise the test checks that every fixture on disk
//! still deserializes and re-serializes to the same JSON (so a serde change
//! that breaks Python's view of the wire fails here first). Decisions carry
//! fresh ids, so they are compared structurally, not regenerated.

use std::collections::BTreeMap;
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use rightsizer_core::*;
use rightsizer_engine::{ContextInput, DecideRequest, Policy};
use serde::Serialize;
use serde::de::DeserializeOwned;

fn dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/wire")
}

fn t(s: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(1_800_000_000 + s, 0).unwrap()
}

fn key() -> WorkloadKey {
    WorkloadKey::parse("flyte://acme/proj/dev/env.train").unwrap()
}

fn parquet(bytes: u64) -> InputMeta {
    InputMeta {
        name: "data".into(),
        uri_hash: Some("sha256:0f".into()),
        format: InputFormat::Parquet,
        bytes: Some(bytes),
        rows: Some(1_000_000),
        columns: Some(ColumnsInfo { total: 12, numeric: 9, string: 2, nested: 1, other: 0, names: Some(vec!["a".into(), "b".into()]) }),
        shape: None,
        dtype: None,
        params: None,
        scalar: None,
    }
}

fn record(i: u32, status: RunStatus, peak_gib: f64) -> RunRecord {
    RunRecord {
        v: 1,
        key: key(),
        cluster: i.is_multiple_of(2).then(|| "us-east-1".to_string()),
        invocation_id: format!("inv-{i}"),
        attempt: 1,
        decision_id: Some(ulid::Ulid::from_parts(1_800_000_000_000 + i as u64, 7)),
        started_at: t(i as i64 * 3600),
        inputs: vec![
            parquet(8 << 30),
            InputMeta { format: InputFormat::Scalar, bytes: None, scalar: Some(ScalarValue::Int(3)), ..InputMeta::blob("epochs", 0) },
        ],
        code_digest: Some("sha256:abc".into()),
        requested: Some(ResourceSpec {
            requests: Requests { cpu: Some(MilliCpu(8000)), memory: Some(Bytes::gib(64)), gpu: Some("A100-80G:1".parse().unwrap()), ..Default::default() },
            limits: Limits { memory: Some(Bytes::gib(64)), ..Default::default() },
        }),
        origin_prior: Some(Requests { cpu: Some(MilliCpu(8000)), memory: Some(Bytes::gib(64)), ..Default::default() }),
        status,
        status_confidence: Confidence::Observed,
        memory_peak: Some(Measurement {
            value: peak_gib * Bytes::GIB as f64,
            censor: if status == RunStatus::OomKilled { Censor::AtLeast } else { Censor::Exact },
            resolution_s: i.is_multiple_of(3).then_some(1.0),
        }),
        cpu_mean: Some(Measurement { value: 3.5, censor: Censor::Exact, resolution_s: None }),
        cpu_peak_window: Some(Measurement { value: 6.25, censor: Censor::AtLeast, resolution_s: Some(1.0) }),
        cpu_throttled_frac: Some(0.0625),
        gpu_memory_peak: Some(Measurement { value: 30.0 * Bytes::GIB as f64, censor: Censor::Exact, resolution_s: None }),
        gpu_util_mean: Some(0.5),
        t_peak_frac: Some(0.75),
        duration_s: Some(1800.0 + i as f64),
        staged_bytes: (i == 1).then_some(Bytes(256 << 20)),
        source: if i.is_multiple_of(2) { OutcomeSource::Cgroup } else { OutcomeSource::Importer { platform: "flyte".into() } },
        trust: if i.is_multiple_of(2) { Trust::Authenticated } else { Trust::Unverified },
    }
}

fn estimates() -> Vec<(&'static str, Estimate)> {
    let prov = |id: &str| Provenance { estimator: id.into(), version: "3".into(), grid: "rs-grid-v1".into(), features: "features-v1".into(), latency_ms: 4 };
    let mut cells = Estimate::empty(prov("rules-sized"));
    let mut p = vec![0.0f32; 22];
    p[8] = 0.25;
    p[9] = 0.5;
    p[10] = 0.25;
    cells.memory = Some(AxisEstimate::Cells { grid: "rs-grid-v1".into(), probs: p });
    cells.cpu_mean = Some(AxisEstimate::Quantiles { q: vec![(0.1, 1.5), (0.5, 2.0), (0.9, 3.25)] });
    cells.duration_s = Some(AxisEstimate::Point { value: 1800.0 });
    cells.t_peak_frac = Some(0.5);
    let mut q = Estimate::empty(prov("gbt"));
    q.kind = EstimateKind::Action;
    q.memory = Some(AxisEstimate::Quantiles { q: vec![(0.5, 4.0 * Bytes::GIB as f64), (0.95, 6.5 * Bytes::GIB as f64)] });
    q.gpu_memory = Some(AxisEstimate::Point { value: 20.0 * Bytes::GIB as f64 });
    q.cpu_peak = Some(AxisEstimate::Point { value: 4.0 });
    q.parallelism = Some(Parallelism { mode: ParallelismMode::Sharded, imbalance: 1.5 });
    q.novelty = Some(0.125);
    vec![("estimate_cells", cells), ("estimate_action", q)]
}

fn decide_requests() -> Vec<(&'static str, DecideRequest)> {
    let prior = Prior {
        spec: ResourceSpec {
            requests: Requests { cpu: Some(MilliCpu(8000)), memory: Some(Bytes::gib(64)), gpu: Some("A100-80G:1".parse().unwrap()), ..Default::default() },
            limits: Limits { memory: Some(Bytes::gib(64)), ..Default::default() },
        },
        defaulted: AxisSet::empty(),
        qos: None,
    };
    let history: Vec<RunRecord> = (0..8).map(|i| record(i, RunStatus::Succeeded, 20.0 + i as f64 * 0.5)).collect();
    let mut platform = PlatformRef::new("flyte");
    platform.overhead = OverheadModel { memory_mib: vec![(0.5, 70.0), (0.9, 242.0)], cpu_millis: 0 };
    platform.walltime_s = Some(7200);
    let ctx = WorkloadContext {
        v: 1,
        key: key(),
        unit: UnitId("main".into()),
        platform,
        code: Some(rightsizer_features::code_info("import pandas as pd\ndf = pd.read_parquet(p)\n", None, false)),
        inputs: vec![parquet(8 << 30)],
        manifest: Default::default(),
        prior: prior.clone(),
        history,
        labels: BTreeMap::from([("team".into(), "ml".into())]),
    };
    let mut enforce = DecideRequest::for_context(ctx.clone());
    enforce.policy = Policy { mode: Some(Mode::Enforce), ..Default::default() };
    enforce.constraints.pinned = rightsizer_core::parse_axis_name("gpu").unwrap();
    enforce.dry_run = true;
    let seed = DecideRequest {
        context: ContextInput::Seed {
            key: WorkloadKey::parse("python://alice/etl").unwrap(),
            unit: UnitId("main".into()),
            group: None,
            seed: ContextSeed {
                inputs: vec![InputRef { name: "x".into(), uri: None, bytes: Some(3 << 30), format: Some(InputFormat::Csv), scalar: None }],
                ..Default::default()
            },
            prior: Prior::from_requests(Requests { cpu: Some(MilliCpu(2000)), memory: Some(Bytes::gib(8)), ..Default::default() }),
            platform: PlatformRef::new("python"),
        },
        dry_run: true,
        ..DecideRequest::for_context(ctx.clone())
    };
    let mut off = seed.clone();
    off.policy.mode = Some(Mode::Off);
    vec![("decision_enforce", enforce), ("decision_seed", seed), ("decision_off", off)]
}

fn write<T: Serialize>(name: &str, v: &T) {
    let mut s = serde_json::to_string_pretty(v).unwrap();
    s.push('\n');
    std::fs::write(dir().join(format!("{name}.json")), s).unwrap();
}

fn check_round_trip<T: Serialize + DeserializeOwned>(name: &str) {
    let path = dir().join(format!("{name}.json"));
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e} (RIGHTSIZER_WRITE_FIXTURES=1 writes it)", path.display()));
    let orig: serde_json::Value = serde_json::from_str(&text).unwrap();
    let parsed: T = serde_json::from_str(&text).unwrap_or_else(|e| panic!("{name}: {e}"));
    // Through text, as Python sees it (f32 fields print in their shortest form).
    let back: serde_json::Value = serde_json::from_str(&serde_json::to_string(&parsed).unwrap()).unwrap();
    assert_eq!(back, orig, "{name}: re-serialized JSON differs");
}

#[test]
fn wire_fixtures() {
    let write_mode = std::env::var("RIGHTSIZER_WRITE_FIXTURES").is_ok_and(|v| v == "1");
    if write_mode {
        std::fs::create_dir_all(dir()).unwrap();
        let recs = [record(0, RunStatus::Succeeded, 21.0), record(1, RunStatus::OomKilled, 63.5), record(2, RunStatus::Timeout, 12.0)];
        for (i, r) in recs.iter().enumerate() {
            write(&format!("run_record_{i}"), r);
        }
        let mut minimal = record(3, RunStatus::Succeeded, 1.0);
        minimal.cluster = None;
        minimal.decision_id = None;
        minimal.inputs.clear();
        minimal.code_digest = None;
        minimal.requested = None;
        minimal.origin_prior = None;
        minimal.cpu_mean = None;
        minimal.cpu_peak_window = None;
        minimal.cpu_throttled_frac = None;
        minimal.gpu_memory_peak = None;
        minimal.gpu_util_mean = None;
        minimal.t_peak_frac = None;
        minimal.duration_s = None;
        minimal.staged_bytes = None;
        minimal.source = OutcomeSource::SelfReport;
        write("run_record_minimal", &minimal);
        for (name, e) in estimates() {
            write(name, &e);
        }
        let config = _core::api::load_config(Some("{}"), Some("memory://"), false).unwrap();
        let engine = _core::runtime::block_on(_core::api::build_engine(config, rightsizer_history::Role::Client, vec![])).unwrap().unwrap();
        for (name, req) in decide_requests() {
            write(&format!("{name}_request"), &req);
            let e = engine.clone();
            let d = _core::runtime::block_on(async move { e.decide(req).await }).unwrap();
            write(name, &d);
        }
        let fallback = _core::api::invalid_request_decision("{}", "fixture");
        write("decision_invalid", &fallback);
    }
    for n in ["run_record_0", "run_record_1", "run_record_2", "run_record_minimal"] {
        check_round_trip::<RunRecord>(n);
    }
    for n in ["estimate_cells", "estimate_action"] {
        check_round_trip::<Estimate>(n);
    }
    for n in ["decision_enforce", "decision_seed", "decision_off", "decision_invalid"] {
        check_round_trip::<Decision>(n);
        if n != "decision_invalid" {
            check_round_trip::<DecideRequest>(&format!("{n}_request"));
        }
    }
    // The enforce fixture exercises an estimate, holds and the explanation.
    let d: Decision = serde_json::from_str(&std::fs::read_to_string(dir().join("decision_enforce.json")).unwrap()).unwrap();
    assert!(d.estimate.is_some(), "decision_enforce carries its estimate");
    assert!(!d.held.is_empty(), "decision_enforce has holds (gpu pinned)");
}
