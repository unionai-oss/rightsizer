//! Property tests: every built-in estimate is a valid distribution on
//! rs-grid-v1, and `rules-sized` is monotone in input bytes.

use std::collections::BTreeMap;

use proptest::prelude::*;
use rightsizer_core::grid::Grid;
use rightsizer_core::*;
use rightsizer_estimators::{Estimator, HistoryPeak, PriorEstimator, RulesSized};

fn key() -> WorkloadKey {
    WorkloadKey::parse("k8s://c/ns/job/x").unwrap()
}

const FORMATS: [InputFormat; 6] =
    [InputFormat::Parquet, InputFormat::Csv, InputFormat::Jsonl, InputFormat::Npy, InputFormat::Blob, InputFormat::Safetensors];
const STATUSES: [RunStatus; 5] = [RunStatus::Succeeded, RunStatus::OomKilled, RunStatus::Failed, RunStatus::Cached, RunStatus::GpuOom];
const CENSORS: [Censor; 3] = [Censor::Exact, Censor::AtLeast, Censor::AtMost];

#[derive(Clone, Debug)]
struct H {
    status: usize,
    censor: usize,
    peak: f64,
    bytes: Option<u64>,
    cpu: Option<f64>,
    gpu: Option<f64>,
    dur: Option<f64>,
}

fn hist() -> impl Strategy<Value = H> {
    (
        0..STATUSES.len(),
        0..CENSORS.len(),
        1e6f64..5e12,
        proptest::option::of(1u64..1u64 << 40),
        proptest::option::of(0.01f64..200.0),
        proptest::option::of(1e8f64..1e12),
        proptest::option::of(1.0f64..1e5),
    )
        .prop_map(|(status, censor, peak, bytes, cpu, gpu, dur)| H { status, censor, peak, bytes, cpu, gpu, dur })
}

fn record(i: usize, h: &H) -> RunRecord {
    let m = |v: f64| Measurement { value: v, censor: CENSORS[h.censor], resolution_s: None };
    RunRecord {
        v: 1,
        key: key(),
        cluster: None,
        invocation_id: format!("r{i}"),
        attempt: 1,
        decision_id: None,
        started_at: chrono::DateTime::parse_from_rfc3339("2026-11-01T00:00:00Z").unwrap().with_timezone(&chrono::Utc),
        inputs: h.bytes.map(|b| vec![InputMeta::blob("a", b)]).unwrap_or_default(),
        code_digest: None,
        requested: None,
        origin_prior: None,
        status: STATUSES[h.status],
        status_confidence: Confidence::Observed,
        memory_peak: Some(m(h.peak)),
        cpu_mean: h.cpu.map(m),
        cpu_peak_window: h.cpu.map(|c| m(c * 1.5)),
        cpu_throttled_frac: None,
        gpu_memory_peak: h.gpu.map(m),
        gpu_util_mean: None,
        t_peak_frac: Some(0.5),
        duration_s: h.dur,
        staged_bytes: None,
        source: OutcomeSource::Watcher,
        trust: Trust::Unverified,
    }
}

fn ctx(inputs: Vec<InputMeta>, history: Vec<RunRecord>, overhead_mib: f64) -> WorkloadContext {
    let mut platform = PlatformRef::new("k8s");
    platform.overhead = OverheadModel { memory_mib: vec![(0.5, overhead_mib), (0.9, overhead_mib * 3.0)], cpu_millis: 0 };
    WorkloadContext {
        v: 1,
        key: key(),
        unit: UnitId("main".into()),
        platform,
        code: None,
        inputs,
        manifest: ManifestFeatures::default(),
        prior: Prior::from_requests(Requests { memory: Some(Bytes::gib(4)), cpu: Some(MilliCpu(1500)), ..Default::default() }),
        history,
        labels: BTreeMap::new(),
    }
}

fn check_axis(e: &Option<AxisEstimate>, n: usize) -> Result<(), TestCaseError> {
    match e {
        None => Ok(()),
        Some(AxisEstimate::Cells { grid, probs }) => {
            prop_assert_eq!(grid.as_str(), "rs-grid-v1");
            prop_assert_eq!(probs.len(), n);
            prop_assert!(probs.iter().all(|p| p.is_finite() && *p >= 0.0), "{:?}", probs);
            let s: f32 = probs.iter().sum();
            prop_assert!((s - 1.0).abs() < 1e-5, "sum {}", s);
            Ok(())
        }
        Some(AxisEstimate::Quantiles { q }) => {
            prop_assert!(!q.is_empty() && q.iter().all(|(l, v)| l.is_finite() && v.is_finite() && *v >= 0.0));
            Ok(())
        }
        Some(AxisEstimate::Point { value }) => {
            prop_assert!(value.is_finite() && *value > 0.0);
            Ok(())
        }
    }
}

fn check(e: &Estimate) -> Result<(), TestCaseError> {
    let g = Grid::rs_grid_v1();
    check_axis(&e.memory, g.memory.len())?;
    check_axis(&e.cpu_mean, g.cpu.len())?;
    check_axis(&e.cpu_peak, g.cpu.len())?;
    check_axis(&e.gpu_memory, g.gpu_memory.len())?;
    check_axis(&e.duration_s, 0)?;
    if let Some(t) = e.t_peak_frac {
        prop_assert!((0.0..=1.0).contains(&t));
    }
    Ok(())
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
}

fn cum(e: &Option<AxisEstimate>) -> Vec<f64> {
    let Some(AxisEstimate::Cells { probs, .. }) = e else { panic!("cells") };
    let mut acc = 0.0;
    probs
        .iter()
        .map(|p| {
            acc += *p as f64;
            acc
        })
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    #[test]
    fn estimates_are_valid_distributions(
        sizes in proptest::collection::vec((0..FORMATS.len(), proptest::option::of(0u64..1u64 << 42)), 0..4),
        history in proptest::collection::vec(hist(), 0..8),
        overhead in 0.0f64..300.0,
    ) {
        let inputs = sizes.iter().enumerate().map(|(i, (f, b))| {
            let mut m = InputMeta::blob(format!("i{i}"), 0);
            m.format = FORMATS[*f];
            m.bytes = *b;
            m
        }).collect();
        let history = history.iter().enumerate().map(|(i, h)| record(i, h)).collect();
        let c = ctx(inputs, history, overhead);
        let rt = rt();
        let ests: [&dyn Estimator; 3] = [&RulesSized::default(), &HistoryPeak::default(), &PriorEstimator::default()];
        for est in ests {
            match rt.block_on(est.estimate(&c)) {
                Ok(e) => check(&e)?,
                Err(err) => prop_assert!(err.is_insufficient(), "{}: {err}", est.info().id),
            }
        }
    }

    #[test]
    fn rules_sized_is_monotone_in_input_bytes(
        f in 0..FORMATS.len(),
        a in 1u64..1u64 << 41,
        b in 1u64..1u64 << 41,
        history in proptest::collection::vec(hist(), 0..5),
    ) {
        let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
        let history: Vec<RunRecord> = history.iter().enumerate().map(|(i, h)| record(i, h)).collect();
        let mk = |bytes: u64| {
            let mut m = InputMeta::blob("x", bytes);
            m.format = FORMATS[f];
            ctx(vec![m], history.clone(), 70.0)
        };
        let est = RulesSized::default();
        let rt = rt();
        let (Ok(el), Ok(eh)) = (rt.block_on(est.estimate(&mk(lo))), rt.block_on(est.estimate(&mk(hi)))) else {
            return Err(TestCaseError::fail("rules-sized declined with input bytes"));
        };
        // With history the anchor may change between sizes, so monotonicity
        // is asserted for cold starts and for a single fixed anchor.
        let warm_anchors = history.iter().filter(|r| r.status != RunStatus::Cached).count();
        if warm_anchors <= 1 {
            let (cl, ch) = (cum(&el.memory), cum(&eh.memory));
            for (x, y) in cl.iter().zip(&ch) {
                prop_assert!(*y <= *x + 1e-5, "P(D ≤ v) must not grow with input bytes: {cl:?} vs {ch:?}");
            }
        }
    }
}
