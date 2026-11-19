//! Platform semantics: retries and ladders (SPEC §7.2), constraints (SPEC
//! §7.4), live platform state, request and price models (SPEC §5.8, §7.2).

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::{AcceleratorId, AxisSet, Bytes, GpuShape, Hold, MilliCpu, Requests, RunStatus, WorkloadKey};

/// Retry semantics of the platform (SPEC §7.2).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct PlatformRetry {
    /// Retries the platform performs at the same size (K8s `backoffLimit`,
    /// Flyte retries, Ray `max_retries`).
    #[serde(default)]
    pub same_size: u32,
    /// Resubmission at a larger size, if the integration drives one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ladder: Option<Ladder>,
}

/// An escalation ladder (SPEC §7.2).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Ladder {
    /// Memory multiplier per step (2.0).
    #[serde(default = "two_f32")]
    pub memory_factor: f32,
    /// Number of steps (2).
    #[serde(default = "two_u32")]
    pub max_steps: u32,
    /// The last step is raised to at least this: max(origin prior, largest
    /// observed success). The core also computes it from the context and
    /// uses the larger.
    #[serde(default)]
    pub final_floor: Requests,
    /// Who drives the ladder.
    pub driver: LadderDriver,
}

fn two_f32() -> f32 {
    2.0
}
fn two_u32() -> u32 {
    2
}

impl Ladder {
    /// The default caller-driven ladder (×2, two steps).
    pub fn caller() -> Self {
        Ladder { memory_factor: 2.0, max_steps: 2, final_floor: Requests::default(), driver: LadderDriver::Caller }
    }
}

/// Who drives a ladder (SPEC §7.2).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum LadderDriver {
    /// A rightsizer wrapper owns the wait and returns only the final attempt.
    Caller,
    /// The orchestrator retries through a rightsizer hook that sees the attempt.
    Orchestrator,
}

/// The current attempt, as an orchestrator publishes it (SPEC §7.2).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct AttemptContext {
    /// Wire version (1).
    #[serde(default = "crate::wire_v")]
    pub v: u16,
    /// Stable across attempts of one task instance.
    pub invocation_id: String,
    /// 1-based.
    pub attempt: u32,
    /// `None`: unknown → no ladder.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<u32>,
}

/// An attempt record and ladder lease, written before every attempt
/// rightsizer changes (SPEC §7.2):
/// `{prefix}/v1/attempts/{tenant}/{key-hash}/{invocation-hash}.json`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct AttemptRecord {
    /// Wire version (1).
    #[serde(default = "crate::wire_v")]
    pub v: u16,
    /// Workload key.
    pub key: WorkloadKey,
    /// Invocation id.
    pub invocation_id: String,
    /// The origin prior's requests.
    pub origin_prior: Requests,
    /// Attempt 1's applied requests.
    pub applied: Requests,
    /// Attempt 1's decision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "schema", schemars(with = "Option<String>"))]
    pub decision_id: Option<Ulid>,
    /// Whether attempt 1 was below the origin prior on an incompressible axis.
    pub reduced: bool,
    /// When written.
    pub written_at: DateTime<Utc>,
    /// The ladder lease, for Caller ladders.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease: Option<Lease>,
}

/// A ladder lease (SPEC §7.2): open before each reduced attempt; closed by
/// the component that classifies the failure.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Lease {
    /// Opened at.
    pub opened_at: DateTime<Utc>,
    /// Closed at, if closed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closed_at: Option<DateTime<Utc>>,
    /// Final status, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<RunStatus>,
}

/// Constraints defining the feasible set (SPEC §7.4).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Constraints {
    /// User pins and explicit call-site resources: stay at the prior's value.
    #[serde(default)]
    pub pinned: AxisSet,
    /// Platform and adapter holds (the core adds S2/S3 holds itself).
    #[serde(default)]
    pub held: Vec<Hold>,
    /// Platform minimums (the core adds the size-scaled floor).
    #[serde(default)]
    pub floor: Requests,
    /// User ceiling (the core intersects it with the largest shape and quota).
    #[serde(default)]
    pub ceiling: Requests,
    /// Allowed GPU shapes when GPU type changes are opted in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_shapes: Option<Vec<GpuShape>>,
    /// Why `ceiling` is set, reported as the clamp reason (advisory and
    /// `rightsizer_clamped_total{reason}`) instead of `"user"` when the user
    /// ceiling binds: e.g. `"no_platform_state"` when an adapter without
    /// node shapes set it to the definition ceiling or the prior (S4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ceiling_reason: Option<String>,
}

/// Live platform state, given to the decision rule only (plugins.md §2).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct PlatformState {
    /// Schedulable node shapes.
    #[serde(default)]
    pub shapes: Vec<NodeShape>,
    /// What a request is (SPEC §5.8).
    #[serde(default)]
    pub request_model: RequestModel,
    /// How a request is priced (SPEC §7.2).
    #[serde(default)]
    pub price: PriceModel,
    /// Remaining quota, minus the adapter's reservation ledger.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota: Option<Quota>,
    /// Platform minimums.
    #[serde(default)]
    pub floors: Requests,
}

/// A schedulable node shape (SPEC §5.4).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct NodeShape {
    /// Name (instance type, node pool).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Allocatable CPU.
    pub cpu: MilliCpu,
    /// Allocatable memory.
    pub memory: Bytes,
    /// Accelerators.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu: Option<GpuShape>,
    /// Price of the whole node, $/h.
    pub price_per_hour: f64,
    /// Host CPU and memory bundled per GPU (default: node / GPU count).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_per_gpu: Option<HostShare>,
}

/// Host resources bundled with one GPU (SPEC §5.4).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct HostShare {
    /// CPU per GPU.
    pub cpu: MilliCpu,
    /// Memory per GPU.
    pub memory: Bytes,
}

/// What a request is on this platform (SPEC §5.8). JSON: `{"kind": "free"}`,
/// `{"kind": "catalog", "shapes": [...]}`, …
///
/// `Snap` is data, not a function: the platform snaps a lower bound to the
/// cheapest listed shape that covers it. An in-process caller with an exact
/// custom snap function passes a [`SnapFn`] to the decision rule instead;
/// when `exact` is false the unit is recommend-only (SPEC §5.8).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RequestModel {
    /// Any grid cell within floor and ceiling.
    #[default]
    Free,
    /// Discrete machine types.
    Catalog {
        /// The catalog.
        shapes: Vec<Shape>,
    },
    /// Legal CPU/memory pairs only (Fargate).
    Combos {
        /// Legal combinations.
        combos: Vec<Combo>,
    },
    /// The platform turns a lower bound into a shape itself.
    Snap {
        /// Shapes the platform may choose from.
        shapes: Vec<Shape>,
        /// Whether the snap rule (cheapest covering shape) is known exactly.
        #[serde(default)]
        exact: bool,
        /// Whether memory is bounded by the instance (failure uses the snapped
        /// shape's usable memory) rather than by the ask.
        #[serde(default)]
        instance_bounded: bool,
    },
}

/// One legal CPU with its legal memory values (SPEC §5.8 `Combos`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Combo {
    /// CPU.
    pub cpu: MilliCpu,
    /// Legal memory values for that CPU.
    pub memory: Vec<Bytes>,
}

/// A catalog machine type (SPEC §5.8).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Shape {
    /// Name, e.g. `"ml.g5.2xlarge"`.
    pub name: String,
    /// vCPUs.
    pub cpu: MilliCpu,
    /// Installed memory.
    pub memory: Bytes,
    /// Taken by the platform's agents and OS: usable = memory − reserved.
    #[serde(default)]
    pub reserved: Bytes,
    /// Accelerators.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu: Option<GpuShape>,
    /// $/h by usage type (`"training"`, `"spot"`, …).
    #[serde(default)]
    pub price: BTreeMap<String, f64>,
    /// Instance family.
    #[serde(default)]
    pub family: String,
}

impl Shape {
    /// Usable memory.
    pub fn usable(&self) -> Bytes {
        self.memory - self.reserved
    }
    /// Price for `usage`, else `"default"`, else the cheapest listed, else 0.
    pub fn price_for(&self, usage: Option<&str>) -> f64 {
        if let Some(u) = usage
            && let Some(p) = self.price.get(u)
        {
            return *p;
        }
        if let Some(p) = self.price.get("default") {
            return *p;
        }
        self.price.values().copied().fold(None, |a: Option<f64>, b| Some(a.map_or(b, |a| a.min(b)))).unwrap_or(0.0)
    }
}

/// An exact, in-process snap function (SPEC §5.8). Not serialized.
pub trait SnapFn: Send + Sync {
    /// The shape the platform would choose for `ask`; `None` = infeasible.
    fn snap(&self, ask: &Requests) -> Option<Shape>;
}

/// How an attempt is priced (SPEC §7.2). JSON: `{"kind": "packed"}`, …
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PriceModel {
    /// The dominant share of the cheapest node shape that holds the request.
    #[default]
    Packed,
    /// Per-resource rates.
    Linear {
        /// Rates.
        rates: Rates,
    },
    /// The shape's price × instance count (Catalog and Snap platforms).
    Instance {
        /// Usage type to price by (`"training"`, …).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        usage: Option<String>,
    },
    /// Billed on max(request, use) per resource-second (Modal).
    UsageFloor {
        /// Rates.
        rates: Rates,
    },
}

/// Per-resource hourly rates.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Rates {
    /// $ per core-hour.
    pub cpu_core_hr: f64,
    /// $ per GiB-hour.
    pub mem_gib_hr: f64,
    /// $ per device-hour, by accelerator.
    #[serde(default)]
    pub gpu_hr: BTreeMap<AcceleratorId, f64>,
}

/// Remaining quota (SPEC §7.4, S4).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Quota {
    /// CPU remaining.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<MilliCpu>,
    /// Memory remaining.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<Bytes>,
    /// GPUs remaining.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_count: Option<u32>,
    /// Ephemeral storage remaining.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ephemeral_storage: Option<Bytes>,
}
