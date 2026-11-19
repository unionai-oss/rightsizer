//! The `history-peak` estimator (SPEC §6.2; the tuner's `vpa` arm,
//! `tuner:bench/arms.py` `_vpa_one`, with Kubernetes VPA's OOM rule).
//!
//! - **Records**: the last `k` (10) learnable same-key records with a
//!   measurement, restricted to the context's code digest when any has it.
//!   `Cached` runs are excluded; `Unverified` records are used (trust gates
//!   floors, anchors and S3(b) only, SPEC §8.2).
//! - **Memory**: the censored max of demand (container peak − the overhead
//!   median, SPEC §5.2) × `margin` (1.15, VPA's default
//!   `recommendation-margin-fraction`). A lower bound (`AtLeast`, and every
//!   `OomKilled` peak) enters the max as `max(v × 1.2, v + 100Mi)` (VPA's OOM
//!   bump) and no mass is placed below `v` (SPEC §5.7). No input scaling: that
//!   is `rules-sized`'s job.
//! - **CPU** (`cpu_mean`, `cpu_peak`), **GPU memory**: the same censored max,
//!   CPU lower bounds (throttled runs) bumped ×1.2.
//! - **Duration**: p10/p50/p90 of successful durations (a point with fewer
//!   than three); **`t_peak_frac`**: the median.
//!
//! Output: `Cells` on rs-grid-v1 from a narrow log-normal (σ 0.1) around the
//! center, truncated at the largest lower bound.
//!
//! Declines (`Insufficient`) when no learnable record has a memory, CPU or
//! GPU measurement.

use async_trait::async_trait;
use rightsizer_core::grid::Grid;
use rightsizer_core::{Estimate, EstimateKind, Provenance, RunRecord, WorkloadContext};

use crate::common::{
    Bump, MIB, Resource, censored_max, cpu_mean_obs, cpu_peak_obs, duration_estimate, durations, gpu_obs, lognormal_cells, memory_demand_obs, staged_now,
    prefer_same_digest, t_peak_median,
};
use crate::{EstimateAxis, EstimateError, Estimator, EstimatorInfo, Needs};

/// `history-peak` parameters.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct HistoryPeakParams {
    /// Records considered, newest first (K).
    pub k: usize,
    /// Margin on the max (VPA: 1.15).
    pub margin: f64,
    /// Margin on CPU.
    pub cpu_margin: f64,
    /// σ of the emitted log-normal.
    pub sigma: f64,
    /// Bump factor for lower bounds (1.2).
    pub bump_factor: f64,
    /// Additive bump, MiB (100).
    pub bump_add_mib: f64,
}

impl Default for HistoryPeakParams {
    fn default() -> Self {
        HistoryPeakParams { k: 10, margin: 1.15, cpu_margin: 1.0, sigma: 0.1, bump_factor: 1.2, bump_add_mib: 100.0 }
    }
}

/// The `history-peak` estimator.
pub struct HistoryPeak {
    info: EstimatorInfo,
    params: HistoryPeakParams,
    grid: Grid,
}

impl Default for HistoryPeak {
    fn default() -> Self {
        Self::new(HistoryPeakParams::default())
    }
}

impl HistoryPeak {
    /// Id.
    pub const ID: &'static str = "history-peak";
    /// Version.
    pub const VERSION: &'static str = "1";

    /// With `params`, on rs-grid-v1.
    pub fn new(params: HistoryPeakParams) -> Self {
        let grid = Grid::rs_grid_v1();
        HistoryPeak {
            info: EstimatorInfo {
                id: Self::ID.into(),
                version: Self::VERSION.into(),
                grids: vec![grid.id.clone()],
                axes: vec![
                    EstimateAxis::Memory,
                    EstimateAxis::CpuMean,
                    EstimateAxis::CpuPeak,
                    EstimateAxis::GpuMemory,
                    EstimateAxis::Duration,
                ],
                kind: EstimateKind::Demand,
                needs: Needs { code: false, history: true, input_bytes: false },
                features: String::new(),
                novelty: None,
                max_batch: None,
            },
            params,
            grid,
        }
    }

    /// Parameters.
    pub fn params(&self) -> &HistoryPeakParams {
        &self.params
    }

    fn records<'a>(&self, ctx: &'a WorkloadContext) -> Vec<&'a RunRecord> {
        let o = ctx.platform.overhead.median_bytes();
        let st = staged_now(ctx);
        let digest = ctx.code.as_ref().map(|c| c.digest.as_str());
        prefer_same_digest(&ctx.history, digest, self.params.k, |r| {
            memory_demand_obs(r, o, st).is_some() || cpu_mean_obs(r, 0.0).is_some() || cpu_peak_obs(r, 0.0).is_some() || gpu_obs(r).is_some()
        })
    }
}

#[async_trait]
impl Estimator for HistoryPeak {
    fn info(&self) -> &EstimatorInfo {
        &self.info
    }

    async fn estimate(&self, ctx: &WorkloadContext) -> Result<Estimate, EstimateError> {
        let started = std::time::Instant::now();
        let recs = self.records(ctx);
        if recs.is_empty() {
            return Err(EstimateError::Insufficient("history-peak: no history with a measurement".into()));
        }
        let p = &self.params;
        let g = &self.grid;
        let bump = Bump { factor: p.bump_factor, add_bytes: p.bump_add_mib * MIB };
        let o = ctx.platform.overhead.median_bytes();
        let st = staged_now(ctx);
        let cpu_o = ctx.platform.overhead.cpu_millis as f64 / 1000.0;
        let mut e = Estimate::empty(Provenance {
            estimator: self.info.id.clone(),
            version: self.info.version.clone(),
            grid: g.id.clone(),
            features: String::new(),
            latency_ms: 0,
        });
        e.memory = censored_max(recs.iter().filter_map(|r| memory_demand_obs(r, o, st)), Resource::Memory, &bump)
            .map(|c| lognormal_cells(g, Resource::Memory, c.center * p.margin, p.sigma, c.lower));
        e.cpu_mean = censored_max(recs.iter().filter_map(|r| cpu_mean_obs(r, cpu_o)), Resource::Cpu, &bump)
            .map(|c| lognormal_cells(g, Resource::Cpu, c.center * p.cpu_margin, p.sigma, c.lower));
        e.cpu_peak = censored_max(recs.iter().filter_map(|r| cpu_peak_obs(r, cpu_o)), Resource::Cpu, &bump)
            .map(|c| lognormal_cells(g, Resource::Cpu, c.center * p.cpu_margin, p.sigma, c.lower));
        e.gpu_memory = censored_max(recs.iter().filter_map(|r| gpu_obs(r)), Resource::GpuMemory, &bump)
            .map(|c| lognormal_cells(g, Resource::GpuMemory, c.center * p.margin, p.sigma, c.lower));
        e.duration_s = duration_estimate(&durations(&recs, |_| 1.0));
        e.t_peak_frac = t_peak_median(&recs);
        e.provenance.latency_ms = started.elapsed().as_millis().min(u32::MAX as u128) as u32;
        Ok(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::*;
    use rightsizer_core::grid::GridAxis;
    use rightsizer_core::normalize::normalize_axis;
    use rightsizer_core::*;

    fn median_of(e: &AxisEstimate, axis: GridAxis) -> f64 {
        normalize_axis(e, &Grid::rs_grid_v1(), axis, 0.35, "x").unwrap().median()
    }

    #[tokio::test]
    async fn declines_without_history() {
        let ctx = ctx_with(vec![InputMeta::blob("x", 1 << 30)], vec![]);
        assert!(HistoryPeak::default().estimate(&ctx).await.unwrap_err().is_insufficient());
        let cached = record("c", RunStatus::Cached, Some(Measurement::exact(gib(1.0))), None);
        let ctx = ctx_with(vec![], vec![cached]);
        assert!(HistoryPeak::default().estimate(&ctx).await.unwrap_err().is_insufficient());
    }

    #[tokio::test]
    async fn max_times_margin() {
        let recs = vec![
            record("a", RunStatus::Succeeded, Some(Measurement::exact(gib(1.0))), None),
            record("b", RunStatus::Succeeded, Some(Measurement::exact(gib(2.0))), None),
            record("c", RunStatus::Succeeded, Some(Measurement::exact(gib(1.5))), None),
        ];
        let e = HistoryPeak::default().estimate(&ctx_with(vec![], recs)).await.unwrap();
        let m = median_of(e.memory.as_ref().unwrap(), GridAxis::Memory);
        // Cells are coarse (2Gi..3Gi): the median read back from cells is within 10%.
        assert!((m / (gib(2.0) * 1.15) - 1.0).abs() < 0.1, "{}", m / gib(1.0));
        assert_eq!(e.provenance.estimator, "history-peak");
    }

    #[tokio::test]
    async fn oom_bump_max_of_ratio_and_additive() {
        // Small OOM: +100Mi wins over ×1.2.
        let small =
            record("s", RunStatus::OomKilled, Some(Measurement { value: 300.0 * MIB, censor: Censor::AtLeast, resolution_s: None }), None);
        let hp = HistoryPeak::new(HistoryPeakParams { margin: 1.0, ..Default::default() });
        let e = hp.estimate(&ctx_with(vec![], vec![small])).await.unwrap();
        let m = median_of(e.memory.as_ref().unwrap(), GridAxis::Memory);
        assert!((m / (400.0 * MIB) - 1.0).abs() < 0.05, "{}", m / MIB);
        // Large OOM: ×1.2 wins.
        let big = record("b", RunStatus::OomKilled, Some(Measurement::exact(gib(4.0))), None);
        let e = hp.estimate(&ctx_with(vec![], vec![big])).await.unwrap();
        let AxisEstimate::Cells { probs, .. } = e.memory.as_ref().unwrap() else { panic!() };
        // No mass at or below 4Gi (cell 8).
        assert!(probs[..=8].iter().all(|p| *p == 0.0));
        let m = median_of(e.memory.as_ref().unwrap(), GridAxis::Memory);
        assert!((m / (gib(4.8)) - 1.0).abs() < 0.05);
    }

    #[tokio::test]
    async fn lower_bound_above_the_exact_max_places_mass_above_it() {
        let recs = vec![
            record("a", RunStatus::Succeeded, Some(Measurement::exact(gib(1.0))), None),
            record("b", RunStatus::Succeeded, Some(Measurement { value: gib(6.0), censor: Censor::AtLeast, resolution_s: None }), None),
        ];
        let e = HistoryPeak::default().estimate(&ctx_with(vec![], recs)).await.unwrap();
        let d = normalize_axis(e.memory.as_ref().unwrap(), &Grid::rs_grid_v1(), GridAxis::Memory, 0.35, "m").unwrap();
        assert!(d.cdf(gib(6.0)) < 1e-6, "P(D ≤ 6Gi) = {}", d.cdf(gib(6.0)));
    }

    #[tokio::test]
    async fn digest_preference_and_last_k() {
        let mut old = record("old", RunStatus::Succeeded, Some(Measurement::exact(gib(30.0))), None);
        old.code_digest = Some("sha256:a".into());
        let mut new = record("new", RunStatus::Succeeded, Some(Measurement::exact(gib(1.0))), None);
        new.code_digest = Some("sha256:b".into());
        let mut ctx = ctx_with(vec![], vec![new.clone(), old.clone()]);
        ctx.code = Some(CodeInfo { digest: "sha256:b".into(), tokens: vec![], source: None });
        let e = HistoryPeak::default().estimate(&ctx).await.unwrap();
        assert!(median_of(e.memory.as_ref().unwrap(), GridAxis::Memory) < gib(2.0));
        // No same-digest record: all records count.
        ctx.code = Some(CodeInfo { digest: "sha256:c".into(), tokens: vec![], source: None });
        let e = HistoryPeak::default().estimate(&ctx).await.unwrap();
        assert!(median_of(e.memory.as_ref().unwrap(), GridAxis::Memory) > gib(30.0));
        // K = 1: only the newest.
        let hp = HistoryPeak::new(HistoryPeakParams { k: 1, ..Default::default() });
        let e = hp.estimate(&ctx_with(vec![], vec![new, old])).await.unwrap();
        assert!(median_of(e.memory.as_ref().unwrap(), GridAxis::Memory) < gib(2.0));
    }

    #[tokio::test]
    async fn overhead_and_gpu_and_cpu() {
        let mut r = record("a", RunStatus::Succeeded, Some(Measurement::exact(gib(2.0) + 70.0 * MIB)), None);
        r.cpu_mean = Some(Measurement::exact(0.5));
        r.cpu_peak_window = Some(Measurement::exact(2.0));
        r.gpu_memory_peak = Some(Measurement::exact(gib(10.0)));
        let mut g = record("g", RunStatus::GpuOom, None, None);
        g.gpu_memory_peak = Some(Measurement::exact(gib(22.0)));
        let mut ctx = ctx_with(vec![], vec![r, g]);
        ctx.platform.overhead = OverheadModel { memory_mib: vec![(0.5, 70.0)], cpu_millis: 0 };
        let hp = HistoryPeak::new(HistoryPeakParams { margin: 1.0, ..Default::default() });
        let e = hp.estimate(&ctx).await.unwrap();
        let m = median_of(e.memory.as_ref().unwrap(), GridAxis::Memory);
        assert!((m / gib(2.0) - 1.0).abs() < 0.03);
        let gm = normalize_axis(e.gpu_memory.as_ref().unwrap(), &Grid::rs_grid_v1(), GridAxis::GpuMemory, 0.35, "g").unwrap();
        // 22Gi is a cell edge: no mass at or below it.
        assert!(gm.cdf(gib(22.0)) < 1e-6, "GPU OOM is a lower bound");
        assert!((median_of(e.cpu_mean.as_ref().unwrap(), GridAxis::Cpu) - 0.5).abs() < 0.03);
        assert!((median_of(e.cpu_peak.as_ref().unwrap(), GridAxis::Cpu) - 2.0).abs() < 0.1);
    }

    #[tokio::test]
    async fn duration_quantiles() {
        let recs: Vec<RunRecord> = (1..=5)
            .map(|i| {
                let mut r = record(&format!("r{i}"), RunStatus::Succeeded, Some(Measurement::exact(gib(1.0))), None);
                r.duration_s = Some(i as f64 * 10.0);
                r
            })
            .collect();
        let e = HistoryPeak::default().estimate(&ctx_with(vec![], recs)).await.unwrap();
        let Some(AxisEstimate::Quantiles { q }) = e.duration_s else { panic!() };
        assert_eq!(q.len(), 3);
        assert_eq!(q[1], (0.5, 30.0));
    }
}
