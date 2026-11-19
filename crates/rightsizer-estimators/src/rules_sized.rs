//! The `rules-sized` estimator (SPEC §6.2; the tuner's `rules-sized` arm,
//! `tuner:bench_all/arms.py` `_sized_one`, with the per-format power law of
//! `tuner:bench/arms.py` `_size_one`).
//!
//! **Memory, warm** (some learnable same-key record has a memory peak):
//!
//! 1. candidates = learnable records with a peak, same code digest when any
//!    has it (SPEC §8.5);
//! 2. anchor = the candidate nearest in total input bytes, by
//!    `|ln(b_hist / b_now)|` (ties → newest); without sizes, the newest;
//! 3. demand = (anchor's container peak − overhead median) × `scale(b_now,
//!    b_hist)` (SPEC §7.4: never below 1) × `margin`;
//! 4. censoring: if the anchor's peak is `AtLeast` (OOM, near-limit), the
//!    center is `bump(peak) × scale` and no mass sits below `peak × scale`.
//!    Every other `AtLeast` candidate whose inputs were no larger than this
//!    invocation's also bounds demand from below (bumped as the center).
//!
//! **Memory, cold** (input bytes, no usable peak): the dominant format (the
//! one holding the most bytes) picks a [`ColdStart`] law,
//! `demand_mib = base_mib + k × input_mib^exponent`. With `base_mib = 0` this
//! is exactly the tuner's `2^(a + b·log2 bytes)` (`k = 2^(a + 20 b)`,
//! `exponent = b`); with `exponent = 1` it is exactly `bench_all`'s
//! `base + k × MiB`.
//!
//! Output: `Cells` on rs-grid-v1 from a log-normal around the center
//! (σ `warm_sigma` / `cold_sigma`), truncated at any lower bound.
//!
//! **CPU**: `cpu_mean` / `cpu_peak` are the censored max of the candidates'
//! measurements × `cpu_margin` (the tuner's `max(cores) × margin_c`); cold,
//! `cpu_mean` is `cold_cpu_cores` (the tuner's 1 core). **GPU memory**: the
//! censored max of GPU peaks (`GpuOom` bumped), when history has any.
//! **Duration**: successful candidates' durations × their `scale`;
//! **`t_peak_frac`**: the candidates' median.
//!
//! Declines (`Insufficient`) when the context has no input bytes and no
//! learnable history with a memory peak.
//!
//! Deviation from the tuner, by the spec: the tuner scales the nearest run
//! *down* for a smaller invocation; SPEC §7.4 `scale` never goes below 1.

use std::collections::HashMap;

use async_trait::async_trait;
use rightsizer_core::grid::Grid;
use rightsizer_core::safety::scale;
use rightsizer_core::{Estimate, EstimateKind, InputFormat, Provenance, RunRecord, WorkloadContext};

use crate::common::{
    Bump, MIB, Obs, Resource, censored_max, cpu_mean_obs, cpu_peak_obs, duration_estimate, durations, gpu_obs, learnable, lognormal_cells,
    memory_demand_obs, prefer_same_digest, staged_now, t_peak_median,
};
use crate::{EstimateAxis, EstimateError, Estimator, EstimatorInfo, Needs};

/// A cold-start law: `demand_mib = base_mib + k × input_mib^exponent`.
#[derive(Copy, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ColdStart {
    /// Fixed demand, MiB (interpreter, libraries).
    pub base_mib: f64,
    /// Multiplier on input MiB^exponent.
    pub k: f64,
    /// Power-law exponent (1 = linear).
    pub exponent: f64,
}

impl ColdStart {
    /// Linear `base + k × MiB` (the tuner's `bench_all` form).
    pub const fn linear(base_mib: f64, k: f64) -> Self {
        ColdStart { base_mib, k, exponent: 1.0 }
    }

    /// The tuner's `bench` power law `2^(a + b·log2 bytes)` MiB.
    pub fn power_law(a: f64, b: f64) -> Self {
        ColdStart { base_mib: 0.0, k: (a + 20.0 * b).exp2(), exponent: b }
    }

    /// Demand in bytes for `input_bytes` total.
    pub fn demand_bytes(&self, input_bytes: u64) -> f64 {
        let mib = (input_bytes.max(1) as f64) / MIB;
        (self.base_mib + self.k * mib.powf(self.exponent)) * MIB
    }
}

/// `rules-sized` parameters.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct RulesSizedParams {
    /// Cold-start law per format.
    pub cold: HashMap<InputFormat, ColdStart>,
    /// Law for formats not in `cold`.
    pub cold_default: ColdStart,
    /// Multiplier on the warm demand (the tuner's tuned `margin`).
    pub margin: f64,
    /// σ of the warm log-normal.
    pub warm_sigma: f64,
    /// σ of the cold log-normal.
    pub cold_sigma: f64,
    /// Multiplier on history CPU (the tuner's `margin_c`).
    pub cpu_margin: f64,
    /// σ of CPU log-normals.
    pub cpu_sigma: f64,
    /// Cold-start mean CPU, cores.
    pub cold_cpu_cores: f64,
    /// Cold-start CPU σ.
    pub cold_cpu_sigma: f64,
    /// Records considered (newest first).
    pub history_k: usize,
    /// The VPA bump for lower bounds.
    pub bump_factor: f64,
    /// Additive bump, MiB.
    pub bump_add_mib: f64,
}

/// Default cold-start laws (MiB of demand for MiB of input). The tuner
/// tuned `base ∈ {64, 128, 256, 512}`, `k ∈ {0.5 … 8}` on its validation
/// split without recording the winners; these defaults sit inside that box,
/// ordered by how much in-memory expansion each format typically has
/// (columnar decode and pandas objects for Parquet, parsing for CSV/JSONL,
/// none for raw tensors).
pub fn default_cold_starts() -> HashMap<InputFormat, ColdStart> {
    use InputFormat::*;
    HashMap::from([
        (Parquet, ColdStart::linear(256.0, 4.0)),
        (Csv, ColdStart::linear(256.0, 3.0)),
        (Jsonl, ColdStart::linear(256.0, 6.0)),
        (Npy, ColdStart::linear(128.0, 1.25)),
        (Safetensors, ColdStart::linear(512.0, 1.25)),
        (ArrowIpc, ColdStart::linear(256.0, 1.5)),
        (Dir, ColdStart::linear(256.0, 2.0)),
        (Blob, ColdStart::linear(256.0, 2.0)),
        (ModelRef, ColdStart::linear(512.0, 1.25)),
    ])
}

impl Default for RulesSizedParams {
    fn default() -> Self {
        RulesSizedParams {
            cold: default_cold_starts(),
            cold_default: ColdStart::linear(256.0, 2.0),
            margin: 1.0,
            warm_sigma: 0.25,
            cold_sigma: 0.5,
            cpu_margin: 1.0,
            cpu_sigma: 0.25,
            cold_cpu_cores: 1.0,
            cold_cpu_sigma: 0.5,
            history_k: rightsizer_core::WorkloadContext::HISTORY_LIMIT,
            bump_factor: 1.2,
            bump_add_mib: 100.0,
        }
    }
}

impl RulesSizedParams {
    /// Reject parameters that would collapse estimates to the floor, make
    /// the cold law decrease with input size, or undo the OOM bump.
    pub fn validate(&self) -> Result<(), String> {
        crate::common::check_params(&[
            ("margin", self.margin, 0.0, false),
            ("warm_sigma", self.warm_sigma, 0.0, false),
            ("cold_sigma", self.cold_sigma, 0.0, false),
            ("cpu_margin", self.cpu_margin, 0.0, false),
            ("cpu_sigma", self.cpu_sigma, 0.0, false),
            ("cold_cpu_cores", self.cold_cpu_cores, 0.0, false),
            ("cold_cpu_sigma", self.cold_cpu_sigma, 0.0, false),
            ("bump_factor", self.bump_factor, 1.0, true),
            ("bump_add_mib", self.bump_add_mib, 0.0, true),
        ])?;
        for (name, c) in self.cold.iter().map(|(f, c)| (format!("cold.{f:?}"), c)).chain(std::iter::once(("cold_default".to_string(), &self.cold_default))) {
            crate::common::check_params(&[
                (&format!("{name}.base_mib"), c.base_mib, 0.0, true),
                (&format!("{name}.k"), c.k, 0.0, true),
                (&format!("{name}.exponent"), c.exponent, 0.0, true),
            ])?;
            if c.base_mib <= 0.0 && c.k <= 0.0 {
                return Err(format!("{name}: base_mib and k are both 0"));
            }
        }
        Ok(())
    }

    fn bump(&self) -> Bump {
        Bump { factor: self.bump_factor, add_bytes: self.bump_add_mib * MIB }
    }
    /// The law for `fmt`.
    pub fn cold_for(&self, fmt: Option<InputFormat>) -> ColdStart {
        fmt.and_then(|f| self.cold.get(&f).copied()).unwrap_or(self.cold_default)
    }
}

/// The format holding the most input bytes (ties → first in canonical order).
pub fn dominant_format(ctx: &WorkloadContext) -> Option<InputFormat> {
    let mut by: Vec<(InputFormat, u64)> = Vec::new();
    for i in &ctx.inputs {
        let (Some(b), f) = (i.bytes, i.format) else { continue };
        if f == InputFormat::Scalar {
            continue;
        }
        match by.iter_mut().find(|x| x.0 == f) {
            Some(x) => x.1 = x.1.saturating_add(b),
            None => by.push((f, b)),
        }
    }
    let mut best: Option<(InputFormat, u64)> = None;
    for x in by {
        if best.is_none_or(|b| x.1 > b.1) {
            best = Some(x);
        }
    }
    best.map(|b| b.0)
}

/// What `rules-sized` concluded about memory, for explanations and tests.
#[derive(Clone, Debug, PartialEq)]
pub enum MemoryBasis {
    /// Scaled from a history record.
    Warm {
        /// Invocation id of the anchor record.
        anchor: String,
        /// `scale` applied.
        scale: f64,
        /// Demand center, bytes.
        center: f64,
        /// Lower bound, bytes (0 = none).
        lower: f64,
    },
    /// From the cold-start law.
    Cold {
        /// Dominant format.
        format: Option<InputFormat>,
        /// Demand center, bytes.
        center: f64,
    },
}

/// The `rules-sized` estimator.
pub struct RulesSized {
    info: EstimatorInfo,
    params: RulesSizedParams,
    grid: Grid,
}

impl Default for RulesSized {
    fn default() -> Self {
        Self::new(RulesSizedParams::default())
    }
}

impl RulesSized {
    /// Id.
    pub const ID: &'static str = "rules-sized";
    /// Version (part of memo and exact-cache keys).
    pub const VERSION: &'static str = "1";

    /// With `params`, on rs-grid-v1.
    pub fn new(params: RulesSizedParams) -> Self {
        let grid = Grid::rs_grid_v1();
        RulesSized {
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
                needs: Needs { code: false, history: true, input_bytes: true },
                features: rightsizer_features::FEATURES_VERSION.into(),
                novelty: None,
                max_batch: None,
            },
            params,
            grid,
        }
    }

    /// Parameters.
    pub fn params(&self) -> &RulesSizedParams {
        &self.params
    }

    /// The memory basis for `ctx`, or `None` when it declines.
    pub fn memory_basis(&self, ctx: &WorkloadContext) -> Option<MemoryBasis> {
        let p = &self.params;
        let o = ctx.platform.overhead.median_bytes();
        let st = staged_now(ctx);
        let b_now = ctx.input_bytes();
        let digest = ctx.code.as_ref().map(|c| c.digest.as_str());
        let cands = prefer_same_digest(&ctx.history, digest, p.history_k, |r| memory_demand_obs(r, o, st).is_some());
        if let Some(anchor) = nearest_by_size(&cands, b_now) {
            let bump = p.bump();
            let obs = memory_demand_obs(anchor, o, st)?;
            let s = scale(b_now, anchor.input_bytes());
            let (mut center, mut lower) = if obs.is_lower_bound() {
                (bump.apply(obs.value, Resource::Memory) * s, obs.value * s)
            } else {
                (obs.value * s * p.margin, 0.0)
            };
            // Other lower bounds from runs no larger than this one.
            for r in &cands {
                let Some(ob) = memory_demand_obs(r, o, st).filter(Obs::is_lower_bound) else { continue };
                let no_larger = match (b_now, r.input_bytes()) {
                    (Some(n), Some(h)) => h <= n,
                    _ => true,
                };
                if no_larger {
                    let rs = scale(b_now, r.input_bytes());
                    lower = lower.max(ob.value * rs);
                    center = center.max(bump.apply(ob.value, Resource::Memory) * rs);
                }
            }
            return Some(MemoryBasis::Warm { anchor: anchor.invocation_id.clone(), scale: s, center, lower });
        }
        let b = b_now?;
        let format = dominant_format(ctx);
        Some(MemoryBasis::Cold { format, center: p.cold_for(format).demand_bytes(b) })
    }
}

impl RulesSized {
    /// The mean-CPU center (cores) `estimate` uses: the censored max of the
    /// candidates' `cpu_mean` × `cpu_margin`, else `cold_cpu_cores`.
    pub fn cpu_basis(&self, ctx: &WorkloadContext) -> f64 {
        let p = &self.params;
        let digest = ctx.code.as_ref().map(|c| c.digest.as_str());
        let cpu_o = ctx.platform.overhead.cpu_millis as f64 / 1000.0;
        let hist = prefer_same_digest(&ctx.history, digest, p.history_k, |_| true);
        match censored_max(hist.iter().filter_map(|r| cpu_mean_obs(r, cpu_o)), Resource::Cpu, &p.bump()) {
            Some(c) => c.center * p.cpu_margin,
            None => p.cold_cpu_cores,
        }
    }
}

/// The record nearest in total input bytes (`|ln(b_hist / b_now)|`; ties →
/// first, i.e. newest). Records without sizes only win when none has one, or
/// when this invocation's size is unknown (then the newest wins).
pub fn nearest_by_size<'a>(cands: &[&'a RunRecord], b_now: Option<u64>) -> Option<&'a RunRecord> {
    let first = cands.first().copied()?;
    let Some(now) = b_now else { return Some(first) };
    let now = now.max(1) as f64;
    let mut best: Option<(&RunRecord, f64)> = None;
    for r in cands {
        let Some(h) = r.input_bytes() else { continue };
        let d = ((h.max(1) as f64) / now).ln().abs();
        if best.is_none_or(|(_, bd)| d < bd) {
            best = Some((r, d));
        }
    }
    Some(best.map(|b| b.0).unwrap_or(first))
}

#[async_trait]
impl Estimator for RulesSized {
    fn info(&self) -> &EstimatorInfo {
        &self.info
    }

    async fn estimate(&self, ctx: &WorkloadContext) -> Result<Estimate, EstimateError> {
        let started = std::time::Instant::now();
        let Some(basis) = self.memory_basis(ctx) else {
            return Err(EstimateError::Insufficient("rules-sized: no input bytes and no history with a memory peak".into()));
        };
        let p = &self.params;
        let g = &self.grid;
        let bump = p.bump();
        let b_now = ctx.input_bytes();
        let digest = ctx.code.as_ref().map(|c| c.digest.as_str());
        let cpu_o = ctx.platform.overhead.cpu_millis as f64 / 1000.0;
        let hist = prefer_same_digest(&ctx.history, digest, p.history_k, |_| true);

        let mut e = Estimate::empty(Provenance {
            estimator: self.info.id.clone(),
            version: self.info.version.clone(),
            grid: g.id.clone(),
            features: self.info.features.clone(),
            latency_ms: 0,
        });
        e.memory = Some(match basis {
            MemoryBasis::Warm { center, lower, .. } => lognormal_cells(g, Resource::Memory, center, p.warm_sigma, lower),
            MemoryBasis::Cold { center, .. } => lognormal_cells(g, Resource::Memory, center, p.cold_sigma, 0.0),
        });
        let cpu = censored_max(hist.iter().filter_map(|r| cpu_mean_obs(r, cpu_o)), Resource::Cpu, &bump);
        e.cpu_mean = Some(match cpu {
            Some(c) => lognormal_cells(g, Resource::Cpu, c.center * p.cpu_margin, p.cpu_sigma, c.lower),
            None => lognormal_cells(g, Resource::Cpu, p.cold_cpu_cores, p.cold_cpu_sigma, 0.0),
        });
        e.cpu_peak = censored_max(hist.iter().filter_map(|r| cpu_peak_obs(r, cpu_o)), Resource::Cpu, &bump)
            .map(|c| lognormal_cells(g, Resource::Cpu, c.center * p.cpu_margin, p.cpu_sigma, c.lower));
        e.gpu_memory = censored_max(hist.iter().filter_map(|r| gpu_obs(r)), Resource::GpuMemory, &bump)
            .map(|c| lognormal_cells(g, Resource::GpuMemory, c.center * p.margin, p.warm_sigma, c.lower));
        e.duration_s = duration_estimate(&durations(&hist, |r| scale(b_now, r.input_bytes())));
        e.t_peak_frac = t_peak_median(&hist);
        debug_assert!(hist.iter().all(|r| learnable(r)));
        e.provenance.latency_ms = started.elapsed().as_millis().min(u32::MAX as u128) as u32;
        Ok(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::*;
    use rightsizer_core::*;

    fn mem_center(e: &Estimate) -> f64 {
        let d = rightsizer_core::normalize::normalize_axis(
            e.memory.as_ref().unwrap(),
            &Grid::rs_grid_v1(),
            rightsizer_core::grid::GridAxis::Memory,
            0.35,
            "m",
        )
        .unwrap();
        d.median()
    }

    #[tokio::test]
    async fn declines_without_bytes_or_history() {
        let ctx = ctx_with(vec![], vec![]);
        let err = RulesSized::default().estimate(&ctx).await.unwrap_err();
        assert!(err.is_insufficient());
        // Cached runs and peak-less runs are not history.
        let mut r = record("a", RunStatus::Cached, Some(Measurement::exact(gib(2.0))), None);
        r.duration_s = Some(10.0);
        let r2 = record("b", RunStatus::Failed, None, None);
        let ctx = ctx_with(vec![], vec![r, r2]);
        assert!(RulesSized::default().estimate(&ctx).await.unwrap_err().is_insufficient());
    }

    #[tokio::test]
    async fn cold_start_per_format() {
        let est = RulesSized::default();
        let mut prev = 0.0;
        for (fmt, expect) in [
            (InputFormat::Npy, 128.0 + 1.25 * 1024.0),
            (InputFormat::Csv, 256.0 + 3.0 * 1024.0),
            (InputFormat::Parquet, 256.0 + 4.0 * 1024.0),
            (InputFormat::Jsonl, 256.0 + 6.0 * 1024.0),
        ] {
            let mut i = InputMeta::blob("x", gib(1.0) as u64);
            i.format = fmt;
            let ctx = ctx_with(vec![i], vec![]);
            let MemoryBasis::Cold { format, center } = est.memory_basis(&ctx).unwrap() else { panic!() };
            assert_eq!(format, Some(fmt));
            assert!((center / MIB - expect).abs() < 1e-6, "{fmt:?}: {}", center / MIB);
            assert!(center > prev);
            prev = center;
            let e = est.estimate(&ctx).await.unwrap();
            assert!((mem_center(&e) / center - 1.0).abs() < 0.05);
            assert_eq!(e.provenance.estimator, "rules-sized");
            // Cold CPU: the tuner's 1 core.
            assert!(e.cpu_mean.is_some());
        }
        // The dominant format is the one with the most bytes.
        let mut a = InputMeta::blob("a", 10);
        a.format = InputFormat::Csv;
        let mut b = InputMeta::blob("b", 1000);
        b.format = InputFormat::Parquet;
        assert_eq!(dominant_format(&ctx_with(vec![a, b], vec![])), Some(InputFormat::Parquet));
    }

    #[test]
    fn power_law_matches_tuner_form() {
        let c = ColdStart::power_law(3.0, 0.5);
        let bytes = 1u64 << 30;
        let tuner = (3.0 + 0.5 * (bytes as f64).log2()).exp2() * MIB;
        assert!((c.demand_bytes(bytes) / tuner - 1.0).abs() < 1e-12);
    }

    #[tokio::test]
    async fn warm_anchors_on_nearest_size_and_scales_up_only() {
        let est = RulesSized::default();
        let mut small = record("small", RunStatus::Succeeded, Some(Measurement::exact(gib(1.0))), Some(gib(1.0) as u64));
        small.started_at -= chrono::Duration::hours(2);
        let big = record("big", RunStatus::Succeeded, Some(Measurement::exact(gib(8.0))), Some(gib(10.0) as u64));
        // Newest first: big then small.
        let ctx = ctx_with(vec![InputMeta::blob("x", gib(2.0) as u64)], vec![big.clone(), small.clone()]);
        let MemoryBasis::Warm { anchor, scale, center, lower } = est.memory_basis(&ctx).unwrap() else { panic!() };
        assert_eq!(anchor, "small");
        assert_eq!(scale, 2.0);
        assert_eq!(center, gib(2.0));
        assert_eq!(lower, 0.0);
        // Smaller than the nearest run: scale stays 1 (SPEC §7.4).
        let ctx = ctx_with(vec![InputMeta::blob("x", gib(8.0) as u64)], vec![big, small]);
        let MemoryBasis::Warm { anchor, scale, center, .. } = est.memory_basis(&ctx).unwrap() else { panic!() };
        assert_eq!((anchor.as_str(), scale, center), ("big", 1.0, gib(8.0)));
    }

    #[tokio::test]
    async fn warm_subtracts_overhead_median() {
        let r = record("a", RunStatus::Succeeded, Some(Measurement::exact(gib(1.0) + 70.0 * MIB)), Some(100));
        let mut ctx = ctx_with(vec![InputMeta::blob("x", 100)], vec![r]);
        ctx.platform.overhead = OverheadModel { memory_mib: vec![(0.5, 70.0), (0.9, 242.0)], cpu_millis: 0 };
        let MemoryBasis::Warm { center, .. } = RulesSized::default().memory_basis(&ctx).unwrap() else { panic!() };
        assert!((center - gib(1.0)).abs() < 1.0);
    }

    #[tokio::test]
    async fn oom_anchor_shifts_mass_up() {
        let mut oom = record("o", RunStatus::OomKilled, Some(Measurement::exact(gib(2.0))), Some(100));
        oom.requested =
            Some(ResourceSpec { requests: Requests { memory: Some(Bytes::gib(2)), ..Default::default() }, limits: Default::default() });
        let ctx = ctx_with(vec![InputMeta::blob("x", 100)], vec![oom]);
        let est = RulesSized::default();
        let MemoryBasis::Warm { center, lower, .. } = est.memory_basis(&ctx).unwrap() else { panic!() };
        assert_eq!(lower, gib(2.0));
        assert!((center - gib(2.4)).abs() < 1.0);
        let e = est.estimate(&ctx).await.unwrap();
        let AxisEstimate::Cells { probs, .. } = e.memory.unwrap() else { panic!() };
        // Nothing at or below the 2Gi cell (index 6).
        assert!(probs[..=6].iter().all(|p| *p == 0.0), "{probs:?}");
    }

    #[tokio::test]
    async fn smaller_oom_bounds_an_exact_anchor() {
        // Nearest run is an exact 1Gi at the same size, but a run with
        // smaller inputs OOMed at 3Gi: demand ≥ 3Gi.
        let exact = record("e", RunStatus::Succeeded, Some(Measurement::exact(gib(1.0))), Some(1000));
        let oom = record(
            "o",
            RunStatus::OomKilled,
            Some(Measurement { value: gib(3.0), censor: Censor::AtLeast, resolution_s: None }),
            Some(500),
        );
        let ctx = ctx_with(vec![InputMeta::blob("x", 1000)], vec![exact, oom]);
        let MemoryBasis::Warm { anchor, center, lower, .. } = RulesSized::default().memory_basis(&ctx).unwrap() else { panic!() };
        assert_eq!(anchor, "e");
        assert_eq!(lower, gib(3.0) * 2.0);
        assert!(center >= lower);
    }

    #[tokio::test]
    async fn same_digest_preferred() {
        let mut other = record("other", RunStatus::Succeeded, Some(Measurement::exact(gib(16.0))), Some(1000));
        other.code_digest = Some("sha256:old".into());
        let mut same = record("same", RunStatus::Succeeded, Some(Measurement::exact(gib(1.0))), Some(10));
        same.code_digest = Some("sha256:new".into());
        let mut ctx = ctx_with(vec![InputMeta::blob("x", 1000)], vec![other, same]);
        ctx.code = Some(CodeInfo { digest: "sha256:new".into(), tokens: vec![], source: None });
        let MemoryBasis::Warm { anchor, .. } = RulesSized::default().memory_basis(&ctx).unwrap() else { panic!() };
        assert_eq!(anchor, "same");
    }

    #[tokio::test]
    async fn unverified_records_are_used() {
        let mut r = record("u", RunStatus::Succeeded, Some(Measurement::exact(gib(3.0))), None);
        r.trust = Trust::Unverified;
        let ctx = ctx_with(vec![], vec![r]);
        assert!(matches!(RulesSized::default().memory_basis(&ctx), Some(MemoryBasis::Warm { .. })));
    }

    #[tokio::test]
    async fn cpu_duration_and_t_peak_from_history() {
        let mut a = record("a", RunStatus::Succeeded, Some(Measurement::exact(gib(1.0))), Some(100));
        a.cpu_mean = Some(Measurement::exact(1.5));
        a.cpu_peak_window = Some(Measurement::exact(3.0));
        a.duration_s = Some(100.0);
        a.t_peak_frac = Some(0.8);
        let mut b = a.clone();
        b.invocation_id = "b".into();
        b.cpu_mean = Some(Measurement { value: 2.0, censor: Censor::AtLeast, resolution_s: None });
        b.duration_s = Some(200.0);
        b.t_peak_frac = Some(0.6);
        let ctx = ctx_with(vec![InputMeta::blob("x", 200)], vec![a, b]);
        let e = RulesSized::default().estimate(&ctx).await.unwrap();
        let AxisEstimate::Cells { probs, .. } = e.cpu_mean.unwrap() else { panic!() };
        // Throttled 2 cores is a lower bound: no mass at or below 2 cores (cell 7).
        assert!(probs[..=6].iter().all(|p| *p == 0.0));
        assert!(e.cpu_peak.is_some());
        // Two durations × scale 2: a point at the median, 300 s.
        assert_eq!(e.duration_s, Some(AxisEstimate::Point { value: 300.0 }));
        assert!((e.t_peak_frac.unwrap() - 0.7).abs() < 1e-6);
    }
}
