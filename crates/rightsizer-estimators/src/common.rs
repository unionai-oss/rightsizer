//! Helpers shared by the history-based estimators: censored observations on
//! the demand basis (SPEC §5.2, §5.7), distributions on grid cells, the VPA
//! bump, and duration / `t_peak_frac` from history.

use rightsizer_core::grid::{Grid, GridAxis, covering};
use rightsizer_core::normalize::cells_from_cdf;
use rightsizer_core::stats::{median, norm_cdf, quantile};
use rightsizer_core::{AxisEstimate, Bytes, Censor, Measurement, RunRecord, RunStatus};

/// One MiB as `f64`.
pub(crate) const MIB: f64 = Bytes::MIB as f64;
/// Smallest demand an estimator emits (bytes): a peak at or below the overhead
/// median still means *some* demand.
pub(crate) const MIN_MEMORY_DEMAND: f64 = 16.0 * MIB;
/// Smallest CPU demand an estimator emits (cores).
pub(crate) const MIN_CPU_DEMAND: f64 = 0.01;

/// A censored observation, already on the demand basis.
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct Obs {
    /// Value (bytes or cores).
    pub value: f64,
    /// Censoring.
    pub censor: Censor,
}

impl Obs {
    /// Whether the true value is at least `value` (a lower bound).
    pub fn is_lower_bound(&self) -> bool {
        self.censor == Censor::AtLeast
    }
}

/// The resource an observation is on; picks the bump rule.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Resource {
    /// Host memory, bytes.
    Memory,
    /// CPU, cores.
    Cpu,
    /// GPU memory, bytes.
    GpuMemory,
}

impl Resource {
    pub(crate) fn grid_axis(self) -> GridAxis {
        match self {
            Resource::Memory => GridAxis::Memory,
            Resource::Cpu => GridAxis::Cpu,
            Resource::GpuMemory => GridAxis::GpuMemory,
        }
    }
    fn min(self) -> f64 {
        match self {
            Resource::Memory | Resource::GpuMemory => MIN_MEMORY_DEMAND,
            Resource::Cpu => MIN_CPU_DEMAND,
        }
    }
}

/// The VPA rule for a value that was a lower bound (SPEC §6.2):
/// memory `max(v × factor, v + add)`, CPU `v × factor`.
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct Bump {
    /// Multiplier (1.2).
    pub factor: f64,
    /// Additive bytes for memory axes (100 MiB).
    pub add_bytes: f64,
}

impl Default for Bump {
    fn default() -> Self {
        Bump { factor: 1.2, add_bytes: 100.0 * MIB }
    }
}

impl Bump {
    /// Apply to `v` on `res`.
    pub fn apply(&self, v: f64, res: Resource) -> f64 {
        match res {
            Resource::Memory | Resource::GpuMemory => (v * self.factor).max(v + self.add_bytes),
            Resource::Cpu => v * self.factor,
        }
    }
}

/// Whether a record can be learned from at all (SPEC §5.7: `Cached` runs are
/// excluded). Trust is *not* checked: `Unverified` records are usable for
/// estimation; trust gates floors, anchors and S3(b) only (SPEC §8.2).
pub fn learnable(r: &RunRecord) -> bool {
    r.status != RunStatus::Cached
}

fn finite_pos(m: &Measurement) -> bool {
    m.value.is_finite() && m.value >= 0.0
}

/// Memory demand of a record: container peak − the overhead median (SPEC
/// §5.2), floored at 16 MiB, staging **not** removed (the `features-v1`
/// history features keep the tuner's container-peak semantics; estimators
/// use [`memory_demand_obs`]). OOM-killed runs are always lower bounds, whatever
/// the importer wrote.
pub fn memory_obs(r: &RunRecord, overhead_median: f64) -> Option<Obs> {
    if !learnable(r) {
        return None;
    }
    let m = r.memory_peak.filter(finite_pos)?;
    let censor = if r.status == RunStatus::OomKilled { Censor::AtLeast } else { m.censor };
    Some(Obs { value: (m.value - overhead_median.max(0.0)).max(MIN_MEMORY_DEMAND), censor })
}

/// Memory-backed (tmpfs) staging bytes of the context being decided: its
/// `manifest.memory_backed_bytes` (0 when none). The decision adds this to
/// demand (`need = (demand + overhead + staged) × headroom`, SPEC §5.2).
pub fn staged_now(ctx: &rightsizer_core::WorkloadContext) -> f64 {
    ctx.manifest.memory_backed_bytes.map(|b| b.0 as f64).unwrap_or(0.0)
}

/// Staging bytes counted in a record's container peak: the record's own
/// `staged_bytes`, else `fallback` (the current context's [`staged_now`]).
/// The fallback is safe: the decision adds the current staging back, so
/// `need ≥ peak` whatever the run staged; only a recorded value is exact
/// when staging changed between runs.
pub fn staged_of(r: &RunRecord, fallback: f64) -> f64 {
    r.staged_bytes.map(|b| b.0 as f64).unwrap_or(fallback).max(0.0)
}

/// Memory demand as the estimators learn it (SPEC §5.2): container peak −
/// overhead median − staged bytes ([`staged_of`]), floored at 16 MiB, so
/// the decision's `+ memory_backed_bytes` does not count staging twice.
/// Censoring as [`memory_obs`].
pub fn memory_demand_obs(r: &RunRecord, overhead_median: f64, staged_fallback: f64) -> Option<Obs> {
    let staged = staged_of(r, staged_fallback);
    memory_obs(r, overhead_median).map(|o| Obs { value: (o.value - staged).max(MIN_MEMORY_DEMAND), ..o })
}

/// GPU memory demand (busiest device × nothing: the record holds the busiest
/// device; total = busiest × count is the adapter's job). `GpuOom` is a lower
/// bound.
pub fn gpu_obs(r: &RunRecord) -> Option<Obs> {
    if !learnable(r) {
        return None;
    }
    let m = r.gpu_memory_peak.filter(finite_pos)?;
    let censor = if r.status == RunStatus::GpuOom { Censor::AtLeast } else { m.censor };
    let devices = r.requested.as_ref().and_then(|s| s.requests.gpu).map(|g| g.count.max(1)).unwrap_or(1) as f64;
    Some(Obs { value: (m.value * devices).max(MIN_MEMORY_DEMAND), censor })
}

/// Mean-CPU demand of a record (cores), minus the fixed CPU overhead.
pub fn cpu_mean_obs(r: &RunRecord, overhead_cores: f64) -> Option<Obs> {
    if !learnable(r) {
        return None;
    }
    let m = r.cpu_mean.filter(finite_pos)?;
    Some(Obs { value: (m.value - overhead_cores).max(MIN_CPU_DEMAND), censor: m.censor })
}

/// Busiest-window CPU of a record (cores).
pub fn cpu_peak_obs(r: &RunRecord, overhead_cores: f64) -> Option<Obs> {
    if !learnable(r) {
        return None;
    }
    let m = r.cpu_peak_window.filter(finite_pos)?;
    Some(Obs { value: (m.value - overhead_cores).max(MIN_CPU_DEMAND), censor: m.censor })
}

/// A censored maximum: the center and the lower bound below which no mass may
/// sit (SPEC §5.7: an `AtLeast` value shifts mass upward and is never a point).
///
/// - `Exact` / `AtMost` values enter the max as they are (`AtMost` is a
///   conservative upper bound of the truth);
/// - an `AtLeast` value `v` enters as `bump(v)` and sets the lower bound to `v`.
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct CensoredMax {
    /// The max after bumps.
    pub center: f64,
    /// Largest lower bound seen (0 when none).
    pub lower: f64,
    /// Whether the max came from a bumped lower bound.
    pub bumped: bool,
}

/// Censored max over observations; `None` when empty.
pub fn censored_max(obs: impl IntoIterator<Item = Obs>, res: Resource, bump: &Bump) -> Option<CensoredMax> {
    let mut out: Option<CensoredMax> = None;
    for o in obs {
        let (v, lower, bumped) = if o.is_lower_bound() { (bump.apply(o.value, res), o.value, true) } else { (o.value, 0.0, false) };
        out = Some(match out {
            None => CensoredMax { center: v, lower, bumped },
            Some(c) => {
                CensoredMax { center: c.center.max(v), lower: c.lower.max(lower), bumped: if v > c.center { bumped } else { c.bumped } }
            }
        });
    }
    out
}

/// A log-normal with median `center` and σ, truncated below at `lower`
/// (no mass under a lower bound), as `Cells` on `grid`'s axis. The center is
/// raised to at least the bound and the resource minimum.
pub fn lognormal_cells(grid: &Grid, res: Resource, center: f64, sigma: f64, lower: f64) -> AxisEstimate {
    let axis = res.grid_axis();
    let values = grid.values(axis);
    let zero_cell = axis == GridAxis::GpuMemory && grid.has_gpu_none();
    let lower = if lower.is_finite() { lower.max(0.0) } else { 0.0 };
    let center = if center.is_finite() { center.max(res.min()).max(lower) } else { res.min().max(lower) };
    let sigma = if sigma.is_finite() && sigma > 1e-6 { sigma } else { 1e-6 };
    let base = |x: f64| if x <= 0.0 { 0.0 } else { norm_cdf((x / center).ln() / sigma) };
    let f_lower = base(lower);
    let probs = if lower > 0.0 && 1.0 - f_lower > 1e-9 {
        cells_from_cdf(&values, zero_cell, |x| if x < lower { 0.0 } else { ((base(x) - f_lower) / (1.0 - f_lower)).clamp(0.0, 1.0) })
    } else if lower > 0.0 {
        // The bound sits far in the tail: everything just above it.
        let mut p = vec![0.0; values.len()];
        let (i, _) = covering(&values, lower * (1.0 + 1e-9));
        p[i] = 1.0;
        p
    } else {
        cells_from_cdf(&values, zero_cell, base)
    };
    cells(grid, probs)
}

/// `Cells` from `f64` probabilities, renormalized in `f32`.
pub(crate) fn cells(grid: &Grid, probs: Vec<f64>) -> AxisEstimate {
    let total: f64 = probs.iter().filter(|p| p.is_finite() && **p > 0.0).sum();
    let mut out: Vec<f32> = probs.iter().map(|&p| if p.is_finite() && p > 0.0 && total > 0.0 { (p / total) as f32 } else { 0.0 }).collect();
    // Put any f32 rounding residue on the most probable cell so the sum is 1.
    let s: f32 = out.iter().sum();
    if let Some((i, _)) = out.iter().enumerate().max_by(|a, b| a.1.total_cmp(b.1)) {
        out[i] = (out[i] + (1.0 - s)).max(0.0);
    }
    AxisEstimate::Cells { grid: grid.id.clone(), probs: out }
}

/// Durations (seconds) of successful learnable records, each multiplied by
/// `factor(record)`.
pub fn durations(history: &[&RunRecord], factor: impl Fn(&RunRecord) -> f64) -> Vec<f64> {
    history
        .iter()
        .filter(|r| r.status == RunStatus::Succeeded)
        .filter_map(|r| r.duration_s.filter(|d| d.is_finite() && *d > 0.0).map(|d| d * factor(r)))
        .collect()
}

/// Duration estimate from history durations: `Quantiles` (p10, p50, p90)
/// with ≥ 3 records, a `Point` at the median with 1–2, `None` without.
pub fn duration_estimate(ds: &[f64]) -> Option<AxisEstimate> {
    match ds.len() {
        0 => None,
        1 | 2 => median(ds).map(|m| AxisEstimate::Point { value: m }),
        _ => {
            let q = [0.1f32, 0.5, 0.9].iter().filter_map(|&p| quantile(ds, p as f64).map(|v| (p, v))).collect();
            Some(AxisEstimate::Quantiles { q })
        }
    }
}

/// Median `t_peak_frac` of learnable records.
pub fn t_peak_median(history: &[&RunRecord]) -> Option<f32> {
    let v: Vec<f64> = history.iter().filter_map(|r| r.t_peak_frac).filter(|t| t.is_finite()).map(|t| t.clamp(0.0, 1.0) as f64).collect();
    median(&v).map(|m| m as f32)
}

/// The records to learn from: learnable ones, restricted to the context's
/// code digest when any record has it (SPEC §8.5: same digest preferred).
/// Newest first, at most `k`.
pub fn prefer_same_digest<'a>(
    history: &'a [RunRecord],
    digest: Option<&str>,
    k: usize,
    has_data: impl Fn(&RunRecord) -> bool,
) -> Vec<&'a RunRecord> {
    let usable: Vec<&RunRecord> = history.iter().filter(|r| learnable(r) && has_data(r)).collect();
    let same: Vec<&RunRecord> = match digest {
        Some(d) => usable.iter().copied().filter(|r| r.code_digest.as_deref() == Some(d)).collect(),
        None => Vec::new(),
    };
    let mut chosen = if same.is_empty() { usable } else { same };
    chosen.truncate(k.max(1));
    chosen
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sum(e: &AxisEstimate) -> f32 {
        match e {
            AxisEstimate::Cells { probs, .. } => probs.iter().sum(),
            _ => panic!(),
        }
    }

    #[test]
    fn bump_is_vpa() {
        let b = Bump::default();
        assert_eq!(b.apply(100.0 * MIB, Resource::Memory), 200.0 * MIB);
        assert_eq!(b.apply(1000.0 * MIB, Resource::Memory), 1200.0 * MIB);
        assert!((b.apply(2.0, Resource::Cpu) - 2.4).abs() < 1e-12);
    }

    #[test]
    fn censored_max_bumps_lower_bounds() {
        let o = [Obs { value: 1000.0 * MIB, censor: Censor::Exact }, Obs { value: 900.0 * MIB, censor: Censor::AtLeast }];
        let c = censored_max(o, Resource::Memory, &Bump::default()).unwrap();
        assert_eq!(c.center, 1080.0 * MIB);
        assert_eq!(c.lower, 900.0 * MIB);
        assert!(c.bumped);
        assert!(censored_max([], Resource::Memory, &Bump::default()).is_none());
    }

    #[test]
    fn truncated_cells_have_no_mass_below_bound() {
        let g = Grid::rs_grid_v1();
        let e = lognormal_cells(&g, Resource::Memory, 3.0 * 1024.0 * MIB, 0.3, 2.5 * 1024.0 * MIB);
        assert!((sum(&e) - 1.0).abs() < 1e-6);
        let AxisEstimate::Cells { probs, .. } = &e else { panic!() };
        // 2.5Gi lies in cell 7 (2Gi, 3Gi]: cells 0..=6 empty.
        assert!(probs[..7].iter().all(|p| *p == 0.0));
        // A bound far above the center: everything lands just above the bound.
        let e = lognormal_cells(&g, Resource::Memory, 1024.0 * MIB, 0.01, 40.0 * 1024.0 * MIB);
        let AxisEstimate::Cells { probs, .. } = &e else { panic!() };
        assert!(probs[..15].iter().all(|p| *p == 0.0));
    }
}
