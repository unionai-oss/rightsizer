//! Estimates (SPEC §5.5).

use serde::{Deserialize, Serialize};

/// Per-axis distributions over demand, or a policy over actions (SPEC §5.5).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Estimate {
    /// Wire version (1).
    #[serde(default = "crate::wire_v")]
    pub v: u16,
    /// Demand distribution, or action policy.
    #[serde(default)]
    pub kind: EstimateKind,
    /// Workload memory demand, bytes (excludes runtime overhead, SPEC §5.2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<AxisEstimate>,
    /// Mean cores over the run: drives stretch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_mean: Option<AxisEstimate>,
    /// Busiest-window cores: reported and used for diagnostics, not cost.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_peak: Option<AxisEstimate>,
    /// Total GPU memory demand, bytes; `Cells` index 0 = no GPU.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_memory: Option<AxisEstimate>,
    /// How GPU memory spreads over devices.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parallelism: Option<Parallelism>,
    /// Run duration at stretch 1, seconds (`Quantiles` or `Point` only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_s: Option<AxisEstimate>,
    /// Fraction of the run elapsed at the memory peak.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub t_peak_frac: Option<f32>,
    /// Novelty ECDF value in `[0, 1]` (SPEC §6.5).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub novelty: Option<f32>,
    /// Who produced it.
    pub provenance: Provenance,
}

impl Estimate {
    /// An empty demand estimate with the given provenance.
    pub fn empty(provenance: Provenance) -> Self {
        Estimate {
            v: crate::WIRE_VERSION,
            kind: EstimateKind::Demand,
            memory: None,
            cpu_mean: None,
            cpu_peak: None,
            gpu_memory: None,
            parallelism: None,
            duration_s: None,
            t_peak_frac: None,
            novelty: None,
            provenance,
        }
    }
}

/// Demand distribution vs. action policy (SPEC §5.5).
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum EstimateKind {
    /// Distributions over workload demand; decoded by expected cost.
    #[default]
    Demand,
    /// A policy over cells; decoded by argmax over the feasible set.
    Action,
}

/// One axis's estimate (SPEC §5.5). JSON: `{"kind": "cells", ...}`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AxisEstimate {
    /// Probabilities over a named grid's cells (sum to 1).
    Cells {
        /// Grid id, e.g. `"rs-grid-v1"`.
        grid: String,
        /// Per-cell probabilities.
        probs: Vec<f32>,
    },
    /// Quantile points `(level, value)`, ≥ 3 points.
    Quantiles {
        /// `(level, value)` pairs.
        q: Vec<(f32, f64)>,
    },
    /// A point value.
    Point {
        /// The value (bytes, cores or seconds).
        value: f64,
    },
}

/// How a run's GPU memory spreads over devices (SPEC §5.4).
#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Parallelism {
    /// Placement mode.
    pub mode: ParallelismMode,
    /// Imbalance factor for sharded runs (default 1.25).
    #[serde(default = "default_imbalance")]
    pub imbalance: f32,
}

fn default_imbalance() -> f32 {
    1.25
}

impl Default for Parallelism {
    fn default() -> Self {
        Parallelism { mode: ParallelismMode::Single, imbalance: default_imbalance() }
    }
}

/// GPU placement mode (SPEC §5.4).
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ParallelismMode {
    /// One device holds everything.
    #[default]
    Single,
    /// Every device holds a full copy.
    Replicated,
    /// Spread across all devices.
    Sharded,
}

/// Who produced an estimate (SPEC §5.5).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Provenance {
    /// Estimator id.
    pub estimator: String,
    /// Estimator version.
    pub version: String,
    /// Grid id the estimate uses.
    #[serde(default)]
    pub grid: String,
    /// Feature schema version (e.g. `"features-v1"`).
    #[serde(default)]
    pub features: String,
    /// Estimation latency.
    #[serde(default)]
    pub latency_ms: u32,
}
