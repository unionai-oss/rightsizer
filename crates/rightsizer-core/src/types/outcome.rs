//! Outcomes: run records, statuses, censored measurements (SPEC §5.7, §8.2).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::{Bytes, InputMeta, Requests, ResourceSpec, WorkloadKey};

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
    /// Memory-backed (tmpfs) staging bytes the unit had at this run (its
    /// `manifest.memory_backed_bytes`), counted in `memory_peak`. Estimators
    /// learn demand = peak − overhead median − this (SPEC §5.2), so the
    /// decision's `+ memory_backed_bytes` does not count staging twice.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged_bytes: Option<Bytes>,
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
    /// The trust readers grant this record: its claimed `trust`, capped by
    /// what its `source` can carry ([`OutcomeSource::max_trust`]). Floors,
    /// trust-region anchors, S3(b) and breaker recovery read this, never the
    /// raw claim (SPEC §8.2).
    pub fn effective_trust(&self) -> Trust {
        self.trust.min(self.source.max_trust())
    }
    /// The lower bound on per-device GPU memory demand of a `GpuOom` record:
    /// its GPU peak when one was sampled (whatever its censoring: an OOM
    /// always proves demand at least that high), else the usable memory of
    /// one device of the requested shape when the accelerator is known
    /// (SPEC §5.7: an OOM-class failure records `AtLeast(limit)`; a device's
    /// memory is its limit). `None` for other statuses or an unknown shape.
    pub fn gpu_oom_bound(&self) -> Option<f64> {
        if self.status != RunStatus::GpuOom {
            return None;
        }
        if let Some(m) = self.gpu_memory_peak.filter(|m| m.value.is_finite() && m.value > 0.0) {
            return Some(m.value);
        }
        let g = self.requested.as_ref()?.requests.gpu?;
        g.accel.usable_memory().map(|b| b.0 as f64)
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

impl OutcomeSource {
    /// The highest trust a record from this source can carry (SPEC §8.2).
    ///
    /// | source | ceiling | why |
    /// |---|---|---|
    /// | `self_report`, `cgroup`, `ray_state` | `authenticated` | written by the workload (or its driver) with the workload's own credentials: at best tenant-scoped |
    /// | `prometheus`, `sacct`, `flyte_remote`, `watcher`, `importer` | `verified` | written by a trusted component reading platform accounting |
    ///
    /// A record claiming more than its source can carry (a `self_report`
    /// claiming `verified`) is read at the ceiling; the claim is ignored.
    pub fn max_trust(&self) -> Trust {
        match self {
            OutcomeSource::SelfReport | OutcomeSource::Cgroup | OutcomeSource::RayState => Trust::Authenticated,
            OutcomeSource::Prometheus | OutcomeSource::Sacct | OutcomeSource::FlyteRemote | OutcomeSource::Watcher | OutcomeSource::Importer { .. } => {
                Trust::Verified
            }
        }
    }
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
