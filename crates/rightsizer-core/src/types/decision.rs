//! Decisions, holds, sources and advisories (SPEC §5.6, §7.7).

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::{AcceleratorId, Axis, Estimate, GroupId, Mode, Prior, ResourceSpec, UnitId, WorkloadKey};

/// The outcome of the decision rule for one unit (SPEC §5.6).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Decision {
    /// Wire version (1).
    #[serde(default = "crate::wire_v")]
    pub v: u16,
    /// Decision id.
    #[cfg_attr(feature = "schema", schemars(with = "String"))]
    pub id: Ulid,
    /// Workload key.
    pub key: WorkloadKey,
    /// Unit.
    pub unit: UnitId,
    /// Group, when units are sized together.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<GroupId>,
    /// Effective mode.
    pub mode: Mode,
    /// The best spec within hard constraints only (pins, ceiling, S2).
    pub recommended: ResourceSpec,
    /// What is (or, below enforce, would be) applied: recommended with held
    /// axes reset to the prior, within floor and trust region.
    pub applied: ResourceSpec,
    /// The catalog / snap shape `applied` lands on, for adapters that render a
    /// machine type (SPEC §5.8). Extension to SPEC §5.6.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shape: Option<String>,
    /// The origin prior.
    pub prior: Prior,
    /// Held axes and why.
    #[serde(default)]
    pub held: Vec<Hold>,
    /// Where the decision came from.
    pub source: DecisionSource,
    /// The estimate, logged for audit and off-policy learning.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimate: Option<Estimate>,
    /// P(applied | context) under the decision policy.
    pub propensity: f32,
    /// Expected cost of `applied` (dollars, penalties included).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_cost: Option<f64>,
    /// Probability that `applied`'s first attempt fails.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p_fail: Option<f32>,
    /// Recommend-only findings (SPEC §7.7).
    #[serde(default)]
    pub advisories: Vec<Advisory>,
    /// Human-readable explanation lines.
    #[serde(default)]
    pub explanation: Vec<String>,
    /// Digest of the context the decision was made from.
    #[serde(default)]
    pub context_digest: String,
}

impl Decision {
    /// Whether `axis` is held.
    pub fn is_held(&self, axis: Axis) -> bool {
        self.held.iter().any(|h| h.axis == axis)
    }
    /// Whether the decision flagged an infeasible floor (SPEC §7.4).
    pub fn infeasible_floor(&self) -> bool {
        self.advisories.iter().any(|a| matches!(a, Advisory::InfeasibleFloor { .. }))
    }
}

/// One held axis (SPEC §5.6 `held: Vec<(Axis, HoldReason)>`; an object on
/// the wire so it can carry detail).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Hold {
    /// The axis.
    pub axis: Axis,
    /// Why.
    pub reason: HoldReason,
    /// Optional detail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl Hold {
    /// A hold without detail.
    pub fn new(axis: Axis, reason: HoldReason) -> Self {
        Hold { axis, reason, detail: None }
    }
    /// A hold with detail.
    pub fn with(axis: Axis, reason: HoldReason, detail: impl Into<String>) -> Self {
        Hold { axis, reason, detail: Some(detail.into()) }
    }
}

/// Why an axis was held at the prior (SPEC §5.6, §7.4).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum HoldReason {
    /// The user pinned it.
    Pinned,
    /// S2 forbids changing it (limits, QoS).
    S2Limit,
    /// An incompressible reduction S3 does not permit.
    S3NotMet,
    /// Memory is not enforced and S3(b) evidence is missing.
    NotEnforced,
    /// A breaker is open (or unreadable) for reductions.
    Breaker,
    /// The axis has no grid in v1 (ephemeral storage, shm).
    NoGrid,
    /// GPU count is held for every unit in v1.
    GpuCountV1,
    /// The unit is distributed: GPU count permanently held.
    Distributed,
    /// GPU type changes are not allowed.
    GpuType,
    /// The prior's GPU type is unknown and never changed.
    UnknownGpu,
    /// QoS class is held.
    Qos,
    /// No prior on this axis and `enforce_without_prior` is off.
    NoPrior,
    /// The estimate has nothing for this axis.
    NoEstimate,
    /// The surface cannot get failures classified (S5): reductions recommend-only.
    Unclassifiable,
    /// The store is unreachable past its TTL.
    StoreUnreachable,
    /// The request model's snap function is not known exactly (SPEC §5.8).
    InexactSnap,
    /// An adapter- or platform-specific hold (see detail).
    Platform,
}

impl HoldReason {
    /// Hard holds bind `recommended` too (pins, S2, GPU count/type, no grid,
    /// no estimate, platform holds). Soft holds (S3, breakers, missing prior,
    /// unclassifiable surfaces, store health, inexact snap) only reset
    /// `applied`: the reduction is still recommended (SPEC §5.6, §7.4).
    pub fn is_hard(self) -> bool {
        matches!(
            self,
            HoldReason::Pinned
                | HoldReason::S2Limit
                | HoldReason::NoGrid
                | HoldReason::GpuCountV1
                | HoldReason::Distributed
                | HoldReason::GpuType
                | HoldReason::UnknownGpu
                | HoldReason::Qos
                | HoldReason::NoEstimate
                | HoldReason::Platform
        )
    }
}

/// Where a decision came from (SPEC §5.6). JSON: `{"kind": "estimator", ...}`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DecisionSource {
    /// A fresh estimate.
    Estimator {
        /// Estimator id.
        id: String,
        /// Estimator version.
        version: String,
    },
    /// A memoized normalized estimate (SPEC §6.3).
    Memo {
        /// Estimator id.
        id: String,
        /// Estimator version.
        version: String,
    },
    /// The prior, unchanged.
    Prior {
        /// Why.
        reason: PriorReason,
        /// Optional detail.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
    },
}

/// Why a decision fell back to the prior (S1, SPEC §7.5).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum PriorReason {
    /// The decision budget expired.
    Budget,
    /// Every estimator declined (`Insufficient`).
    Insufficient,
    /// An estimator failed.
    EstimatorError,
    /// A panic was caught.
    Panic,
    /// A configuration error.
    ConfigError,
    /// The adapter's `validate` rejected the patch.
    Rejected,
    /// The estimate could not be normalized.
    InvalidEstimate,
    /// Mode is `off` or `observe`.
    Mode,
    /// The workload is skipped (already sized, opted out, …).
    Skipped,
    /// Any other internal error.
    Internal,
}

/// A recommend-only finding (SPEC §7.7). JSON: `{"kind": "gpu_lower_count", ...}`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Advisory {
    /// A lower GPU count would fit (busiest-device need).
    GpuLowerCount {
        /// Current count.
        count: u32,
        /// Smallest count that fits.
        fits_count: u32,
    },
    /// A cheaper GPU shape would fit.
    GpuCheaperShape {
        /// Suggested accelerator.
        accel: AcceleratorId,
        /// Suggested count.
        count: u32,
        /// $/h saved.
        saving_per_hour: f64,
    },
    /// Mean GPU utilization was low over recent runs.
    GpuLowUtil {
        /// Mean utilization (fraction 0..1).
        mean_util: f32,
        /// Runs considered.
        runs: u32,
    },
    /// P(need > device memory) at the prior exceeds 0.2.
    GpuOomPredicted {
        /// Probability.
        p: f32,
    },
    /// A timeout is predicted (P > 0.1).
    WalltimeTimeout {
        /// Probability.
        p: f32,
        /// Suggested walltime, seconds.
        suggested_s: u64,
    },
    /// The walltime is over 10× the p95 duration.
    WalltimeExcessive {
        /// Walltime, seconds.
        walltime_s: u64,
        /// p95 duration, seconds.
        p95_s: u64,
    },
    /// The prior is more than 4× the size-scaled p95 demand.
    PriorOversized {
        /// Axis.
        axis: Axis,
        /// prior / p95.
        ratio: f64,
    },
    /// The floor exceeded the ceiling; the ceiling was used (SPEC §7.4).
    InfeasibleFloor {
        /// Axis.
        axis: Axis,
    },
    /// An increase was clamped to the ceiling (S4).
    Clamped {
        /// Axis.
        axis: Axis,
        /// What clamped it (`"shape"`, `"quota"`, `"user"`).
        reason: String,
    },
}
