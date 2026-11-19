//! Per-decision latency on `rs-grid-v1` (SPEC §7.2: budget < 5 ms).

use std::collections::BTreeMap;
use std::hint::black_box;

use chrono::DateTime;
use criterion::{Criterion, criterion_group, criterion_main};
use rightsizer_core::*;

const GIB: f64 = Bytes::GIB as f64;

fn record(i: usize) -> RunRecord {
    RunRecord {
        v: 1,
        key: WorkloadKey::parse("k8s://prod/ml/job/train/main").unwrap(),
        cluster: Some("prod".into()),
        invocation_id: format!("inv-{i}"),
        attempt: 1,
        decision_id: None,
        started_at: DateTime::from_timestamp(1_700_000_000 + i as i64 * 3600, 0).unwrap(),
        inputs: vec![InputMeta::blob("train", 10_000_000_000 + i as u64 * 1_000_000)],
        code_digest: Some(if i.is_multiple_of(3) { "old".into() } else { "d1".into() }),
        requested: Some(ResourceSpec {
            requests: Requests { memory: Some(Bytes::gib(48)), cpu: Some(MilliCpu::cores(8)), ..Default::default() },
            ..Default::default()
        }),
        origin_prior: None,
        status: if i % 11 == 5 { RunStatus::OomKilled } else { RunStatus::Succeeded },
        status_confidence: Confidence::Observed,
        memory_peak: Some(Measurement { value: (18.0 + (i % 7) as f64) * GIB, censor: Censor::Exact, resolution_s: Some(15.0) }),
        cpu_mean: Some(Measurement::exact(3.5)),
        cpu_peak_window: None,
        cpu_throttled_frac: None,
        gpu_memory_peak: None,
        gpu_util_mean: None,
        t_peak_frac: Some(0.6),
        duration_s: Some(1800.0 + i as f64 * 10.0),
        staged_bytes: None,
        source: OutcomeSource::Watcher,
        trust: Trust::Verified,
    }
}

fn context(gpu: bool) -> WorkloadContext {
    let mut platform = PlatformRef::new("k8s");
    platform.overhead = OverheadModel { memory_mib: vec![(0.5, 70.0), (0.9, 242.0)], cpu_millis: 50 };
    platform.retry = PlatformRetry { same_size: 1, ladder: Some(Ladder::caller()) };
    platform.walltime_s = Some(6 * 3600);
    let mut prior = Requests { memory: Some(Bytes::gib(64)), cpu: Some(MilliCpu::cores(8)), ..Default::default() };
    if gpu {
        prior.gpu = Some("A100-80G:1".parse().unwrap());
    }
    let mut p = Prior::from_requests(prior);
    p.spec.limits.memory = Some(Bytes::gib(64));
    WorkloadContext {
        v: 1,
        key: WorkloadKey::parse("k8s://prod/ml/job/train/main").unwrap(),
        unit: UnitId("main".into()),
        platform,
        code: Some(CodeInfo { digest: "d1".into(), tokens: vec!["pandas".into()], source: None }),
        inputs: vec![InputMeta::blob("train", 12_000_000_000)],
        manifest: ManifestFeatures::default(),
        prior: p,
        history: (0..32).map(record).collect(),
        labels: BTreeMap::new(),
    }
}

fn estimate(gpu: bool) -> Estimate {
    let mut e = Estimate::empty(Provenance { estimator: "rules-sized".into(), version: "1".into(), grid: "rs-grid-v1".into(), features: "features-v1".into(), latency_ms: 1 });
    e.memory = Some(AxisEstimate::Quantiles { q: vec![(0.1, 14.0 * GIB), (0.5, 20.0 * GIB), (0.9, 27.0 * GIB), (0.99, 33.0 * GIB)] });
    e.cpu_mean = Some(AxisEstimate::Quantiles { q: vec![(0.1, 2.0), (0.5, 3.5), (0.9, 5.0)] });
    e.duration_s = Some(AxisEstimate::Point { value: 1900.0 });
    if gpu {
        e.gpu_memory = Some(AxisEstimate::Point { value: 30.0 * GIB });
    }
    e
}

fn state() -> PlatformState {
    let node = |cpu: u32, mem: u64, price: f64, gpu: Option<&str>| NodeShape {
        name: None,
        cpu: MilliCpu::cores(cpu),
        memory: Bytes::gib(mem),
        gpu: gpu.map(|g| g.parse().unwrap()),
        price_per_hour: price,
        host_per_gpu: None,
    };
    PlatformState {
        shapes: vec![
            node(4, 16, 0.192, None),
            node(8, 32, 0.384, None),
            node(16, 64, 0.768, None),
            node(32, 128, 1.536, None),
            node(64, 256, 3.072, None),
            node(8, 64, 0.504, None),
            node(16, 128, 1.008, None),
            node(32, 256, 2.016, None),
            node(12, 85, 4.1, Some("A100-80G:1")),
            node(96, 1152, 32.77, Some("A100-80G:8")),
        ],
        ..Default::default()
    }
}

fn bench(c: &mut Criterion) {
    let grid = Grid::rs_grid_v1();
    let st = state();
    let cons = Constraints::default();
    let params = DecisionParams::default();
    for gpu in [false, true] {
        let ctx = context(gpu);
        let est = estimate(gpu);
        let digest = rightsizer_core::digest::context_digest(&ctx);
        let name = if gpu { "decide/rs-grid-v1/gpu" } else { "decide/rs-grid-v1/cpu" };
        c.bench_function(name, |b| {
            b.iter(|| {
                let mut inp = DecisionInput::new(&ctx, Some(&est), &grid, &st, &cons, &params, Mode::Enforce);
                inp.context_digest = Some(&digest);
                black_box(decide(black_box(&inp)))
            })
        });
        c.bench_function(&format!("{name}/with-digest"), |b| {
            b.iter(|| {
                let inp = DecisionInput::new(&ctx, Some(&est), &grid, &st, &cons, &params, Mode::Enforce);
                black_box(decide(black_box(&inp)))
            })
        });
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
