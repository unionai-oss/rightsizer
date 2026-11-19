//! Normalization of estimates to grid cells, applied before deciding (SPEC §5.5).
//!
//! - **Cells** on another grid are rebinned conservatively ([`crate::grid::rebin`]).
//! - **Quantiles**: crossed quantiles are sorted; the CDF is interpolated
//!   log-linearly between points; mass below `q_min` goes to `q_min`'s
//!   covering cell; above `q_max` a log-normal tail fitted to the top two
//!   points, with the remainder at the top cell.
//! - **Point**: a log-normal centred on the value with σ = `point_sigma` (0.35).
//! - **Duration** missing → median of same-key history (log-normal around it),
//!   else 1 h with timeout modeling disabled. **`t_peak_frac`** missing → the
//!   history median, else 0.5.
//!
//! Within a cell, mass is spread log-uniformly over `(lower, upper]`; cell 0's
//! lower edge is half its value (GPU axis: index 0 is a point mass at 0).

use crate::grid::{Grid, GridAxis, rebin};
use crate::stats::{median, norm_cdf, norm_ppf};
use crate::types::{AxisEstimate, Estimate, EstimateKind, Parallelism, RunRecord, RunStatus};

/// Default σ of the log-normal put around a `Point` estimate.
pub const POINT_SIGMA: f64 = 0.35;
/// Duration assumed when neither estimate nor history has one (seconds).
pub const DEFAULT_DURATION_S: f64 = 3600.0;
/// `t_peak_frac` assumed when neither estimate nor history has one.
pub const DEFAULT_T_PEAK_FRAC: f64 = 0.5;
/// Quantile levels of the duration scenarios (each weighted equally).
pub const DURATION_LEVELS: [f64; 5] = [0.1, 0.3, 0.5, 0.7, 0.9];

/// Errors normalizing an estimate.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum NormalizeError {
    /// Probabilities that are all zero, negative or non-finite.
    #[error("{axis}: probabilities are empty, negative or not finite")]
    BadProbs {
        /// Axis name.
        axis: &'static str,
    },
    /// Quantiles or a point with non-finite values.
    #[error("{axis}: non-finite values")]
    NonFinite {
        /// Axis name.
        axis: &'static str,
    },
    /// An empty quantile list.
    #[error("{axis}: no quantile points")]
    NoPoints {
        /// Axis name.
        axis: &'static str,
    },
}

/// A distribution over one grid axis's cells.
#[derive(Clone, Debug, PartialEq)]
pub struct CellDist {
    /// Cell upper edges (natural units).
    pub values: Vec<f64>,
    /// Per-cell probabilities, summing to 1.
    pub probs: Vec<f64>,
    /// Whether cell 0 is a point mass at 0 (GPU axis "none").
    pub zero_cell: bool,
    cum: Vec<f64>,
}

impl CellDist {
    /// Build from values and (unnormalized) probabilities.
    pub fn new(values: Vec<f64>, probs: Vec<f64>, zero_cell: bool, axis: &'static str) -> Result<Self, NormalizeError> {
        let mut probs: Vec<f64> = probs.into_iter().map(|p| if p.is_finite() && p > 0.0 { p } else { 0.0 }).collect();
        probs.resize(values.len(), 0.0);
        let total: f64 = probs.iter().sum();
        if values.is_empty() || total.is_nan() || total <= 0.0 {
            return Err(NormalizeError::BadProbs { axis });
        }
        for p in &mut probs {
            *p /= total;
        }
        let mut cum = Vec::with_capacity(probs.len());
        let mut acc = 0.0;
        for p in &probs {
            acc += p;
            cum.push(acc);
        }
        Ok(CellDist { values, probs, zero_cell, cum })
    }

    /// A point mass on cell `idx`.
    pub fn degenerate(values: Vec<f64>, idx: usize, zero_cell: bool) -> Self {
        let mut probs = vec![0.0; values.len()];
        let i = idx.min(values.len().saturating_sub(1));
        probs[i] = 1.0;
        CellDist::new(values, probs, zero_cell, "degenerate").expect("non-empty")
    }

    fn lower(&self, j: usize) -> f64 {
        if j == 0 {
            if self.zero_cell { 0.0 } else { self.values[0] * 0.5 }
        } else if j == 1 && self.zero_cell {
            self.values[1] * 0.5
        } else {
            self.values[j - 1]
        }
    }

    /// `P(D ≤ x)`, log-uniform within cells.
    pub fn cdf(&self, x: f64) -> f64 {
        if x.is_nan() {
            return 0.0;
        }
        let n = self.values.len();
        if self.zero_cell && x >= 0.0 && x < self.lower(1) {
            return self.cum[0];
        }
        if x >= self.values[n - 1] {
            return 1.0;
        }
        // first cell whose upper edge ≥ x
        let j = self.values.partition_point(|&v| v < x);
        if self.zero_cell && j == 0 {
            return if x >= 0.0 { self.cum[0] } else { 0.0 };
        }
        let lo = self.lower(j);
        let hi = self.values[j];
        let before = if j == 0 { 0.0 } else { self.cum[j - 1] };
        if x <= lo {
            return before;
        }
        let frac = if hi > lo && lo > 0.0 { (x / lo).ln() / (hi / lo).ln() } else { 1.0 };
        (before + self.probs[j] * frac.clamp(0.0, 1.0)).min(1.0)
    }

    /// `P(D > x)`.
    pub fn sf(&self, x: f64) -> f64 {
        (1.0 - self.cdf(x)).max(0.0)
    }

    /// The `p`-quantile (inverse of [`cdf`](Self::cdf)).
    pub fn quantile(&self, p: f64) -> f64 {
        let p = p.clamp(0.0, 1.0);
        let j = self.cum.partition_point(|&c| c < p - 1e-15).min(self.values.len() - 1);
        if self.zero_cell && j == 0 {
            return 0.0;
        }
        let before = if j == 0 { 0.0 } else { self.cum[j - 1] };
        let lo = self.lower(j);
        let hi = self.values[j];
        if self.probs[j] <= 0.0 || lo <= 0.0 {
            return hi;
        }
        let frac = ((p - before) / self.probs[j]).clamp(0.0, 1.0);
        lo * (hi / lo).powf(frac)
    }

    /// Median.
    pub fn median(&self) -> f64 {
        self.quantile(0.5)
    }

    /// A representative demand value per cell (geometric midpoint of its
    /// interval; 0 for the GPU "none" cell).
    pub fn representative(&self, j: usize) -> f64 {
        let lo = self.lower(j);
        let hi = self.values[j];
        if lo <= 0.0 { if self.zero_cell && j == 0 { 0.0 } else { hi } } else { (lo * hi).sqrt() }
    }

    /// `E[f(D)]` using cell representatives.
    pub fn expect(&self, f: impl Fn(f64) -> f64) -> f64 {
        self.probs.iter().enumerate().filter(|(_, p)| **p > 0.0).map(|(j, p)| p * f(self.representative(j))).sum()
    }

    /// Index of the most probable cell (ties → smaller cell).
    pub fn argmax(&self) -> usize {
        let mut best = 0;
        for (i, p) in self.probs.iter().enumerate() {
            if *p > self.probs[best] {
                best = i;
            }
        }
        best
    }
}

/// A continuous CDF built from quantile points (SPEC §5.5).
#[derive(Clone, Debug, PartialEq)]
pub struct QuantileCdf {
    pts: Vec<(f64, f64)>,
    tail: Option<(f64, f64)>,
}

impl QuantileCdf {
    /// From `(level, value)` pairs; crossed quantiles are sorted (levels and
    /// values each ascending). Values ≤ 0 are a point mass at zero.
    pub fn new(q: &[(f32, f64)], axis: &'static str) -> Result<Self, NormalizeError> {
        if q.is_empty() {
            return Err(NormalizeError::NoPoints { axis });
        }
        if q.iter().any(|(p, v)| !p.is_finite() || !v.is_finite()) {
            return Err(NormalizeError::NonFinite { axis });
        }
        let mut levels: Vec<f64> = q.iter().map(|(p, _)| (*p as f64).clamp(1e-6, 1.0 - 1e-6)).collect();
        let mut values: Vec<f64> = q.iter().map(|(_, v)| v.max(0.0)).collect();
        levels.sort_by(|a, b| a.total_cmp(b));
        values.sort_by(|a, b| a.total_cmp(b));
        let mut pts: Vec<(f64, f64)> = Vec::with_capacity(levels.len());
        for (p, v) in levels.into_iter().zip(values) {
            match pts.last_mut() {
                Some(last) if (last.0 - p).abs() < 1e-12 => last.1 = last.1.max(v),
                _ => pts.push((p, v)),
            }
        }
        let tail = if pts.len() >= 2 {
            let (p1, x1) = pts[pts.len() - 2];
            let (p2, x2) = pts[pts.len() - 1];
            let (z1, z2) = (norm_ppf(p1), norm_ppf(p2));
            if x1 > 0.0 && x2 > x1 && z2 > z1 {
                let sigma = (x2.ln() - x1.ln()) / (z2 - z1);
                let mu = x2.ln() - sigma * z2;
                (sigma.is_finite() && sigma > 0.0).then_some((mu, sigma))
            } else {
                None
            }
        } else {
            None
        };
        Ok(QuantileCdf { pts, tail })
    }

    /// `P(D ≤ x)`.
    pub fn cdf(&self, x: f64) -> f64 {
        let (p0, x0) = self.pts[0];
        if x < x0 {
            return 0.0;
        }
        for w in self.pts.windows(2) {
            let ((pa, xa), (pb, xb)) = (w[0], w[1]);
            if x < xb {
                if xb <= xa {
                    return pa;
                }
                let frac = if xa > 0.0 { (x / xa).ln() / (xb / xa).ln() } else { x / xb };
                return pa + (pb - pa) * frac.clamp(0.0, 1.0);
            }
        }
        let (pn, xn) = *self.pts.last().unwrap_or(&(p0, x0));
        match self.tail {
            Some((mu, sigma)) if x > 0.0 && x > xn => norm_cdf((x.ln() - mu) / sigma).max(pn),
            _ => pn,
        }
    }

    /// The `p`-quantile by bisection on [`cdf`](Self::cdf).
    pub fn ppf(&self, p: f64) -> f64 {
        let (p0, x0) = self.pts[0];
        if p <= p0 {
            return x0;
        }
        let (pn, xn) = *self.pts.last().unwrap_or(&(p0, x0));
        if p > pn {
            return match self.tail {
                Some((mu, sigma)) => (mu + sigma * norm_ppf(p)).exp().max(xn),
                None => xn,
            };
        }
        let (mut lo, mut hi) = (x0.max(1e-12), xn.max(1e-12));
        for _ in 0..100 {
            let mid = (lo * hi).sqrt();
            if self.cdf(mid) < p { lo = mid } else { hi = mid }
            if hi / lo < 1.0 + 1e-10 {
                break;
            }
        }
        hi
    }
}

/// Cell probabilities of a continuous CDF over `values` (top cell takes the
/// remainder). With `zero_cell`, cell 0 takes `F(0)`.
pub fn cells_from_cdf(values: &[f64], zero_cell: bool, cdf: impl Fn(f64) -> f64) -> Vec<f64> {
    let n = values.len();
    let mut probs = vec![0.0; n];
    let mut prev = 0.0;
    for (j, &v) in values.iter().enumerate() {
        let f = if j + 1 == n { 1.0 } else { cdf(v).clamp(prev, 1.0) };
        let f = if zero_cell && j == 0 { cdf(0.0).clamp(0.0, 1.0) } else { f };
        probs[j] = (f - prev).max(0.0);
        prev = f.max(prev);
    }
    probs
}

/// Log-normal CDF with median `m` and σ.
fn lognormal_cdf(x: f64, m: f64, sigma: f64) -> f64 {
    if x <= 0.0 {
        0.0
    } else if m <= 0.0 {
        1.0
    } else {
        norm_cdf((x / m).ln() / sigma)
    }
}

/// Normalize one axis estimate onto `axis` of `grid`.
pub fn normalize_axis(
    est: &AxisEstimate,
    grid: &Grid,
    axis: GridAxis,
    point_sigma: f64,
    name: &'static str,
) -> Result<CellDist, NormalizeError> {
    let values = grid.values(axis);
    let zero_cell = axis == GridAxis::GpuMemory && grid.has_gpu_none();
    match est {
        AxisEstimate::Cells { grid: gid, probs } => {
            let probs: Vec<f64> = probs.iter().map(|&p| p as f64).collect();
            if gid == &grid.id && probs.len() == values.len() {
                return CellDist::new(values, probs, zero_cell, name);
            }
            let src = match Grid::by_id(gid) {
                Some(g) => g.values(axis),
                // Unknown grid: read it as a prefix of the target axis (never rejected).
                None => values.clone(),
            };
            let moved = rebin(&src, &probs, &values);
            CellDist::new(values, moved, zero_cell, name)
        }
        AxisEstimate::Quantiles { q } => {
            let cdf = QuantileCdf::new(q, name)?;
            let probs = cells_from_cdf(&values, zero_cell, |x| cdf.cdf(x));
            CellDist::new(values, probs, zero_cell, name)
        }
        AxisEstimate::Point { value } => {
            if !value.is_finite() {
                return Err(NormalizeError::NonFinite { axis: name });
            }
            if *value <= 0.0 {
                return Ok(CellDist::degenerate(values, 0, zero_cell));
            }
            let probs = cells_from_cdf(&values, zero_cell, |x| lognormal_cdf(x, *value, point_sigma));
            CellDist::new(values, probs, zero_cell, name)
        }
    }
}

/// The degenerate estimate the `prior` estimator emits: all mass on the cell
/// covering `value` (SPEC §5.5).
pub fn degenerate_at(grid: &Grid, axis: GridAxis, value: f64) -> AxisEstimate {
    let values = grid.values(axis);
    let idx = crate::grid::covering(&values, value).0;
    let mut probs = vec![0.0f32; values.len()];
    probs[idx] = 1.0;
    AxisEstimate::Cells { grid: grid.id.clone(), probs }
}

/// Duration scenarios for the cost model (SPEC §5.5, §7.2).
#[derive(Clone, Debug, PartialEq)]
pub struct DurationModel {
    /// `(weight, seconds at stretch 1)`.
    pub scenarios: Vec<(f64, f64)>,
    /// Whether timeouts are modeled (false when the duration is the 1 h default).
    pub timeout_modeling: bool,
    /// Median duration, seconds.
    pub median_s: f64,
}

impl DurationModel {
    /// Weighted mean duration.
    pub fn mean_s(&self) -> f64 {
        self.scenarios.iter().map(|(w, d)| w * d).sum()
    }
    /// The `p`-quantile among scenarios (step function).
    pub fn quantile(&self, p: f64) -> f64 {
        let mut acc = 0.0;
        for (w, d) in &self.scenarios {
            acc += w;
            if acc >= p - 1e-12 {
                return *d;
            }
        }
        self.scenarios.last().map(|s| s.1).unwrap_or(self.median_s)
    }

    fn from_ppf(ppf: impl Fn(f64) -> f64, median_s: f64) -> Self {
        let w = 1.0 / DURATION_LEVELS.len() as f64;
        let scenarios = DURATION_LEVELS.iter().map(|&p| (w, ppf(p).max(1.0))).collect();
        DurationModel { scenarios, timeout_modeling: true, median_s: median_s.max(1.0) }
    }

    /// The fixed 1-hour default, timeout modeling disabled.
    pub fn default_hour() -> Self {
        DurationModel { scenarios: vec![(1.0, DEFAULT_DURATION_S)], timeout_modeling: false, median_s: DEFAULT_DURATION_S }
    }
}

/// Successful, non-cached history durations.
fn history_durations(history: &[RunRecord]) -> Vec<f64> {
    history
        .iter()
        .filter(|r| r.status == RunStatus::Succeeded)
        .filter_map(|r| r.duration_s)
        .filter(|d| d.is_finite() && *d > 0.0)
        .collect()
}

/// Build the duration model: the estimate's `duration_s` (Quantiles or Point),
/// else a log-normal (σ = `point_sigma`) around the same-key history median,
/// else 1 h with timeout modeling disabled (SPEC §5.5).
pub fn duration_model(est: Option<&AxisEstimate>, history: &[RunRecord], point_sigma: f64) -> DurationModel {
    match est {
        Some(AxisEstimate::Quantiles { q }) => {
            if let Ok(cdf) = QuantileCdf::new(q, "duration_s") {
                let m = cdf.ppf(0.5);
                return DurationModel::from_ppf(|p| cdf.ppf(p), m);
            }
        }
        Some(AxisEstimate::Point { value }) if value.is_finite() && *value > 0.0 => {
            let m = *value;
            return DurationModel::from_ppf(|p| m * (point_sigma * norm_ppf(p)).exp(), m);
        }
        _ => {}
    }
    match median(&history_durations(history)) {
        Some(m) => DurationModel::from_ppf(|p| m * (point_sigma * norm_ppf(p)).exp(), m),
        None => DurationModel::default_hour(),
    }
}

/// `t_peak_frac`: the estimate's, else the history median, else 0.5 (SPEC §5.5).
pub fn t_peak_frac(est: Option<f32>, history: &[RunRecord]) -> f64 {
    if let Some(t) = est.filter(|t| t.is_finite()) {
        return (t as f64).clamp(0.0, 1.0);
    }
    let v: Vec<f64> = history.iter().filter_map(|r| r.t_peak_frac).filter(|t| t.is_finite()).map(|t| t as f64).collect();
    median(&v).map(|m| m.clamp(0.0, 1.0)).unwrap_or(DEFAULT_T_PEAK_FRAC)
}

/// A whole estimate, normalized.
#[derive(Clone, Debug, PartialEq)]
pub struct Normalized {
    /// Demand or action.
    pub kind: EstimateKind,
    /// Memory demand (bytes).
    pub memory: Option<CellDist>,
    /// Mean CPU demand (cores).
    pub cpu_mean: Option<CellDist>,
    /// Total GPU memory demand (bytes; cell 0 = none when the grid has it).
    pub gpu_memory: Option<CellDist>,
    /// Parallelism (default: single, imbalance 1.25).
    pub parallelism: Parallelism,
    /// Duration scenarios.
    pub duration: DurationModel,
    /// Fraction of the run elapsed at the memory peak.
    pub t_peak_frac: f64,
}

/// Normalize an estimate onto `grid` (SPEC §5.5).
pub fn normalize(est: &Estimate, grid: &Grid, history: &[RunRecord], point_sigma: f64) -> Result<Normalized, NormalizeError> {
    let ax = |e: &Option<AxisEstimate>, axis, name| e.as_ref().map(|e| normalize_axis(e, grid, axis, point_sigma, name)).transpose();
    Ok(Normalized {
        kind: est.kind,
        memory: ax(&est.memory, GridAxis::Memory, "memory")?,
        cpu_mean: ax(&est.cpu_mean, GridAxis::Cpu, "cpu_mean")?,
        gpu_memory: ax(&est.gpu_memory, GridAxis::GpuMemory, "gpu_memory")?,
        parallelism: est.parallelism.unwrap_or_default(),
        duration: duration_model(est.duration_s.as_ref(), history, point_sigma),
        t_peak_frac: t_peak_frac(est.t_peak_frac, history),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Bytes;
    use proptest::prelude::*;

    fn g() -> Grid {
        Grid::rs_grid_v1()
    }

    fn sums_to_one(d: &CellDist) -> bool {
        (d.probs.iter().sum::<f64>() - 1.0).abs() < 1e-9
    }

    #[test]
    fn point_is_lognormal_around_value() {
        let d = normalize_axis(&AxisEstimate::Point { value: 3.0 * Bytes::GIB as f64 }, &g(), GridAxis::Memory, POINT_SIGMA, "m").unwrap();
        assert!(sums_to_one(&d));
        // 3Gi is a cell edge: half the mass at or below it.
        assert!((d.cdf(3.0 * Bytes::GIB as f64) - 0.5).abs() < 1e-6);
        assert_eq!(d.argmax(), 7);
        let med = d.median() / Bytes::GIB as f64;
        assert!((med - 3.0).abs() < 0.05, "{med}");
    }

    #[test]
    fn quantiles_sorted_and_tail() {
        let gib = Bytes::GIB as f64;
        // Crossed: level 0.9 has a smaller value than 0.5.
        let q = vec![(0.9, 2.0 * gib), (0.5, 4.0 * gib), (0.1, 1.0 * gib)];
        let d = normalize_axis(&AxisEstimate::Quantiles { q }, &g(), GridAxis::Memory, POINT_SIGMA, "m").unwrap();
        assert!(sums_to_one(&d));
        // Sorted to (0.1,1Gi), (0.5,2Gi), (0.9,4Gi).
        assert!((d.cdf(2.0 * gib) - 0.5).abs() < 1e-9);
        assert!((d.cdf(4.0 * gib) - 0.9).abs() < 1e-6);
        // Below q_min: the 0.1 mass sits in 1Gi's covering cell.
        assert!((d.probs[4] - 0.1).abs() < 1e-6);
        assert_eq!(d.probs[..4].iter().sum::<f64>(), 0.0);
        // Tail: log-normal through the top two points, so P(>8Gi) is small but positive.
        let s8 = d.sf(8.0 * gib);
        assert!(s8 > 0.0 && s8 < 0.1, "{s8}");
    }

    #[test]
    fn quantile_tail_remainder_at_top_cell() {
        let gib = Bytes::GIB as f64;
        let q = vec![(0.1, 1000.0 * gib), (0.5, 1800.0 * gib), (0.9, 1900.0 * gib)];
        let d = normalize_axis(&AxisEstimate::Quantiles { q }, &g(), GridAxis::Memory, POINT_SIGMA, "m").unwrap();
        assert!(sums_to_one(&d));
        assert!(d.probs[26] > 0.5, "everything above 1.5Ti lands on the top cell");
    }

    #[test]
    fn gpu_point_zero_is_none_cell() {
        let d = normalize_axis(&AxisEstimate::Point { value: 0.0 }, &g(), GridAxis::GpuMemory, POINT_SIGMA, "g").unwrap();
        assert_eq!(d.probs[0], 1.0);
        assert_eq!(d.cdf(0.0), 1.0);
        let d = normalize_axis(&AxisEstimate::Point { value: 20.0 * Bytes::GIB as f64 }, &g(), GridAxis::GpuMemory, POINT_SIGMA, "g").unwrap();
        assert_eq!(d.probs[0], 0.0);
        assert!(sums_to_one(&d));
    }

    #[test]
    fn foreign_cells_rebinned() {
        let probs = vec![0.0f32; 16].into_iter().enumerate().map(|(i, _)| if i == 3 { 1.0 } else { 0.0 }).collect();
        let d = normalize_axis(&AxisEstimate::Cells { grid: "tuner-v1".into(), probs }, &g(), GridAxis::Cpu, POINT_SIGMA, "c").unwrap();
        assert_eq!(d.probs[3], 1.0);
        assert_eq!(d.probs.len(), 25);
        let bad = AxisEstimate::Cells { grid: "rs-grid-v1".into(), probs: vec![0.0; 25] };
        assert!(normalize_axis(&bad, &g(), GridAxis::Cpu, POINT_SIGMA, "c").is_err());
    }

    #[test]
    fn duration_defaults() {
        let d = duration_model(None, &[], POINT_SIGMA);
        assert!(!d.timeout_modeling);
        assert_eq!(d.mean_s(), 3600.0);
        let d = duration_model(Some(&AxisEstimate::Point { value: 600.0 }), &[], POINT_SIGMA);
        assert!(d.timeout_modeling);
        assert_eq!(d.scenarios.len(), 5);
        assert!((d.quantile(0.5) - 600.0).abs() < 1e-6);
        assert_eq!(t_peak_frac(None, &[]), 0.5);
        assert_eq!(t_peak_frac(Some(0.2), &[]), 0.2f32 as f64);
    }

    #[test]
    fn cell_dist_quantile_inverts_cdf() {
        let d = normalize_axis(&AxisEstimate::Point { value: 5e9 }, &g(), GridAxis::Memory, POINT_SIGMA, "m").unwrap();
        for p in [0.05, 0.25, 0.5, 0.75, 0.95] {
            let x = d.quantile(p);
            assert!((d.cdf(x) - p).abs() < 1e-6, "p={p}");
        }
    }

    proptest! {
        #[test]
        fn quantiles_always_normalize(
            pts in proptest::collection::vec((0.01f32..0.99, 1e6f64..1e13), 1..8)
        ) {
            let d = normalize_axis(&AxisEstimate::Quantiles { q: pts }, &g(), GridAxis::Memory, POINT_SIGMA, "m").unwrap();
            prop_assert!(sums_to_one(&d));
            prop_assert!(d.probs.iter().all(|p| *p >= 0.0));
        }

        #[test]
        fn cdf_is_monotone(v in 1e7f64..1e12, a in 1e6f64..1e13, b in 1e6f64..1e13) {
            let d = normalize_axis(&AxisEstimate::Point { value: v }, &g(), GridAxis::Memory, POINT_SIGMA, "m").unwrap();
            let (lo, hi) = if a < b { (a, b) } else { (b, a) };
            prop_assert!(d.cdf(lo) <= d.cdf(hi) + 1e-12);
        }

        #[test]
        fn rebin_preserves_mass_and_never_moves_down(probs in proptest::collection::vec(0.0f32..1.0, 7)) {
            prop_assume!(probs.iter().sum::<f32>() > 0.01);
            let d = normalize_axis(&AxisEstimate::Cells { grid: "tuner-mem-v1".into(), probs: probs.clone() }, &g(), GridAxis::Cpu, POINT_SIGMA, "c").unwrap();
            prop_assert!(sums_to_one(&d));
            // P(D ≤ x) on the target never exceeds the source's at the source's edges.
            let src = Grid::tuner_mem_v1().values(GridAxis::Cpu);
            let total: f32 = probs.iter().sum();
            let mut acc = 0.0f64;
            for (i, &x) in src.iter().enumerate() {
                acc += (probs[i] / total) as f64;
                let tgt: f64 = d.values.iter().zip(&d.probs).filter(|(v, _)| **v <= x + 1e-9).map(|(_, p)| p).sum();
                prop_assert!(tgt <= acc + 1e-6);
            }
        }
    }
}
