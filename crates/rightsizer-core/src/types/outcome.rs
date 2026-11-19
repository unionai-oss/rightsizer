//! Outcomes: run records, statuses, censored measurements (SPEC §5.7, §8.2).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::{InputMeta, Requests, ResourceSpec, WorkloadKey};

/// One attempt's outcome (SPEC §5.7). Dedup key: `(key, invocation_id,
/// attempt, source)`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct RunRecord {
    /// Wire version (1).
    #[serde(default = "crate::wire_v")]
    pub v: u16,
    /// Workload key.
    pub key: WorkloadKey,
    /// Cluster the run ran on (history is per cluster, SPEC §8.5).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster: Option<String>,
    /// Invocation id, stable across attempts.
    pub invocation_id: String,
    /// 1-based attempt.
    pub attempt: u32,
    /// Decision that sized this attempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "schema", schemars(with = "Option<String>"))]
    pub decision_id: Option<Ulid>,
    /// Start time.
    pub started_at: DateTime<Utc>,
    /// Inputs of the invocation.
    #[serde(default)]
    pub inputs: Vec<InputMeta>,
    /// Code digest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code_digest: Option<String>,
    /// What was requested (`None` for local runs).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested: Option<ResourceSpec>,
    /// The origin prior's requests, when the writer knows it (webhook
    /// annotation, ladder lease). Used by attribution (S5). Extension to SPEC
    /// §5.7; absent = look it up from the decision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_prior: Option<Requests>,
    /// Status.
    pub status: RunStatus,
    /// Whether the platform said so, or it was inferred.
    #[serde(default)]
    pub status_confidence: Confidence,
    /// Container memory peak as measured (includes runtime overhead).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_peak: Option<Measurement>,
    /// Mean cores.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_mean: Option<Measurement>,
    /// Busiest-window cores.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_peak_window: Option<Measurement>,
    /// Fraction of time throttled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_throttled_frac: Option<f32>,
    /// Busiest device's GPU memory peak, bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_memory_peak: Option<Measurement>,
    /// Mean GPU utilization as a fraction in `[0, 1]`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_util_mean: Option<f32>,
    /// Fraction of the run elapsed at the memory peak.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub t_peak_frac: Option<f32>,
    /// Duration, seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_s: Option<f64>,
    /// Where the record came from.
    pub source: OutcomeSource,
    /// Trust level (SPEC §8.2).
    pub trust: Trust,
}

impl RunRecord {
    /// Total known input bytes of the run.
    pub fn input_bytes(&self) -> Option<u64> {
        super::total_input_bytes(&self.inputs)
    }
    /// Requested memory, if any.
    pub fn requested_memory(&self) -> Option<u64> {
        self.requested.as_ref().and_then(|r| r.requests.memory).map(|b| b.0)
    }
}

/// Run status (SPEC §5.7).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    /// Succeeded.
    Succeeded,
    /// Killed for memory (host).
    OomKilled,
    /// CUDA / device out of memory.
    GpuOom,
    /// Evicted (e.g. node memory pressure).
    Evicted,
    /// Hit its walltime.
    Timeout,
    /// Preempted by the platform.
    Preempted,
    /// Failed for another (application) reason.
    Failed,
    /// Served from cache: no measurements, excluded from learning.
    Cached,
    /// Unknown.
    Unknown,
}

impl RunStatus {
    /// OOM-class failures (host or device).
    pub fn is_oom_class(self) -> bool {
        matches!(self, RunStatus::OomKilled | RunStatus::GpuOom)
    }
    /// Whether the attempt failed (anything but success or cache).
    pub fn is_failure(self) -> bool {
        !matches!(self, RunStatus::Succeeded | RunStatus::Cached)
    }
}

/// How the status is known (SPEC §5.7).
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum Confidence {
    /// The platform said so.
    #[default]
    Observed,
    /// Inferred from metrics; never proof of an OOM.
    Inferred,
}

/// A censored measurement (SPEC §5.7).
#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Measurement {
    /// Value (bytes or cores).
    pub value: f64,
    /// Censoring.
    #[serde(default)]
    pub censor: Censor,
    /// Sampling interval, seconds (`None` = exact high-water mark).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution_s: Option<f32>,
}

impl Measurement {
    /// An exact measurement.
    pub fn exact(value: f64) -> Self {
        Measurement { value, censor: Censor::Exact, resolution_s: None }
    }
}

/// Censoring of a measurement (SPEC §5.7).
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum Censor {
    /// The value.
    #[default]
    Exact,
    /// A lower bound.
    AtLeast,
    /// An upper bound.
    AtMost,
}

/// Where a run record came from (SPEC §5.7). JSON: `{"kind": "cgroup"}`,
/// `{"kind": "importer", "platform": "aws"}`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OutcomeSource {
    /// The workload reported itself.
    SelfReport,
    /// Child-side cgroup measurement.
    Cgroup,
    /// Prometheus.
    Prometheus,
    /// Slurm `sacct`.
    Sacct,
    /// `flyte.remote` backfill.
    FlyteRemote,
    /// Ray state API.
    RayState,
    /// The K8s watcher.
    Watcher,
    /// Another platform importer.
    Importer {
        /// Platform name.
        platform: String,
    },
}

/// Trust level of a record (SPEC §8.2). Ordered: `Unverified < Authenticated < Verified`.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum Trust {
    /// Anything else.
    Unverified,
    /// Written with tenant-scoped credentials.
    Authenticated,
    /// Written by a trusted component with an authenticated tenant.
    Verified,
}
