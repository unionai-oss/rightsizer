//! `rightsizer-history`: the [`HistoryStore`] trait (SPEC §8.1) and its
//! backends: object store (local FS, S3, GCS, Azure; default for Flyte, Ray,
//! Slurm), sqlite (single-user CLI), postgres (feature `postgres`; the K8s
//! webhook and watcher).
//!
//! Status: the trait, its types, the object-store [`layout`] and the
//! in-memory [`MemoryStore`] (used in tests across the workspace) are
//! complete; the object-store, sqlite and postgres backends are stubs.

#![forbid(unsafe_code)]

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rightsizer_core::breaker::{BreakerObjects, BreakerScope, Control, Reset, Trip};
use rightsizer_core::{AttemptRecord, Decision, Mode, RunRecord, RunStatus, Tenant, WorkloadKey};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

pub mod layout;
pub mod memory;
pub mod object;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "sqlite")]
pub mod sqlite;

pub use memory::MemoryStore;

/// Store errors. Callers treat every error as "unreachable" for S1 / §8.4.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The backend could not be reached.
    #[error("store unreachable: {0}")]
    Unreachable(String),
    /// The credentials do not allow this (e.g. a client writing a reset).
    #[error("permission denied: {0}")]
    PermissionDenied(String),
    /// A conditional write lost (ETag / `If-None-Match` precondition failed).
    #[error("precondition failed: {0}")]
    Conflict(String),
    /// A stored object failed to decode.
    #[error("corrupt object {path}: {message}")]
    Corrupt {
        /// Object path.
        path: String,
        /// Message.
        message: String,
    },
    /// Configuration (URI, backend) is invalid, e.g. sqlite on a network filesystem.
    #[error("invalid store configuration: {0}")]
    Config(String),
    /// The backend does not implement this operation yet.
    #[error("not implemented: {0}")]
    Unimplemented(&'static str),
    /// Anything else.
    #[error("store error: {0}")]
    Other(String),
}

/// A store backend (SPEC §8.1).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Backend {
    /// Object store (local FS, S3, GCS, Azure).
    ObjectStore,
    /// sqlite.
    Sqlite,
    /// postgres.
    Postgres,
    /// In-memory (tests).
    Memory,
}

/// Facts about an open store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreInfo {
    /// Backend.
    pub backend: Backend,
    /// The URI it was opened with (credentials redacted).
    pub uri: String,
    /// Durable and shared across hosts: enforce needs it (SPEC §8.1).
    pub durable_shared: bool,
}

/// Result of [`HistoryStore::put_run`].
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum PutOutcome {
    /// Written.
    Inserted,
    /// A record with the same `(key, invocation_id, attempt, source)` exists.
    Duplicate,
}

/// Filter for [`HistoryStore::decisions`].
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DecisionFilter {
    /// One key.
    pub key: Option<WorkloadKey>,
    /// One tenant (with the key's platform).
    pub tenant: Option<(String, Tenant)>,
    /// Decided at or after.
    pub since: Option<DateTime<Utc>>,
    /// Decided before.
    pub until: Option<DateTime<Utc>>,
    /// Decided by this estimator `(id, version)`.
    pub estimator: Option<(String, String)>,
    /// With this effective mode.
    pub mode: Option<Mode>,
    /// At most this many, newest first.
    pub limit: Option<usize>,
}

impl DecisionFilter {
    /// Whether `d` matches (time bounds use the ULID timestamp).
    pub fn matches(&self, d: &Decision) -> bool {
        let at = ulid_time(d.id);
        self.key.as_ref().is_none_or(|k| &d.key == k)
            && self.tenant.as_ref().is_none_or(|(p, t)| &d.key.platform == p && &d.key.tenant == t)
            && self.since.is_none_or(|s| at >= s)
            && self.until.is_none_or(|u| at < u)
            && self.mode.is_none_or(|m| d.mode == m)
            && self.estimator.as_ref().is_none_or(|(id, v)| {
                matches!(&d.source, rightsizer_core::DecisionSource::Estimator { id: i, version: x }
                    | rightsizer_core::DecisionSource::Memo { id: i, version: x } if i == id && x == v)
            })
    }
}

/// The timestamp encoded in a ULID.
pub fn ulid_time(id: Ulid) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(id.timestamp_ms() as i64).unwrap_or_default()
}

/// Compaction settings (SPEC §8.1).
#[derive(Clone, Debug, PartialEq)]
pub struct CompactOptions {
    /// Records per key kept in `latest` pointers (64).
    pub keep_last: usize,
    /// Shard and snapshot retention (90 d).
    pub max_age: Duration,
    /// Decision shard retention (30 d).
    pub decision_max_age: Duration,
    /// Shards newer than this are never deleted (24 h).
    pub grace: Duration,
    /// Compute but do not write.
    pub dry_run: bool,
    /// Now.
    pub now: DateTime<Utc>,
}

impl CompactOptions {
    /// Defaults at `now`.
    pub fn at(now: DateTime<Utc>) -> Self {
        CompactOptions {
            keep_last: 64,
            max_age: Duration::from_secs(90 * 86_400),
            decision_max_age: Duration::from_secs(30 * 86_400),
            grace: Duration::from_secs(86_400),
            dry_run: false,
            now,
        }
    }
}

/// What compaction did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CompactReport {
    /// Pointers rewritten.
    pub pointers: usize,
    /// Snapshot files written.
    pub snapshots: usize,
    /// Shards deleted.
    pub deleted: usize,
}

/// The run-record dedup key `(key, invocation_id, attempt, source)` (SPEC §8.3).
pub fn dedup_key(r: &RunRecord) -> (String, String, u32, String) {
    (r.key.to_string(), r.invocation_id.clone(), r.attempt, serde_json::to_string(&r.source).unwrap_or_default())
}

/// A history store (SPEC §8.1). Every method may fail; callers apply S1 and
/// the availability rules of SPEC §8.4 (cache, buffered writes, demotion).
#[async_trait]
pub trait HistoryStore: Send + Sync {
    /// Facts about the store.
    fn info(&self) -> StoreInfo;

    /// Append a run record (immutable shard), deduplicated.
    async fn put_run(&self, rec: &RunRecord) -> Result<PutOutcome, StoreError>;

    /// Append a decision (decision log).
    async fn put_decision(&self, d: &Decision) -> Result<(), StoreError>;

    /// The newest `limit` records of `key`, newest first.
    async fn runs(&self, key: &WorkloadKey, limit: usize) -> Result<Vec<RunRecord>, StoreError>;

    /// Decisions matching `filter`, newest first.
    async fn decisions(&self, filter: &DecisionFilter) -> Result<Vec<Decision>, StoreError>;

    /// One decision by id (`rsz explain --decision ID`).
    async fn decision(&self, id: Ulid) -> Result<Option<Decision>, StoreError>;

    /// All trips and resets of one breaker scope.
    async fn breaker_get(&self, scope: &BreakerScope) -> Result<BreakerObjects, StoreError>;

    /// Every scope with at least one trip (`rsz breaker status`).
    async fn breaker_list(&self) -> Result<Vec<(BreakerScope, BreakerObjects)>, StoreError>;

    /// Create a trip object (clients may only create trips).
    async fn breaker_trip(&self, trip: &Trip) -> Result<(), StoreError>;

    /// Write a reset (operator credentials).
    async fn breaker_reset(&self, reset: &Reset) -> Result<(), StoreError>;

    /// The fleet control object (`mode_ceiling`), if written.
    async fn control_get(&self) -> Result<Option<Control>, StoreError>;

    /// Write the control object (operator credentials).
    async fn control_set(&self, control: &Control) -> Result<(), StoreError>;

    /// The attempt record / ladder lease of one invocation (SPEC §7.2).
    async fn attempt_get(&self, key: &WorkloadKey, invocation_id: &str) -> Result<Option<AttemptRecord>, StoreError>;

    /// Create the attempt record if absent (conditional PUT); `false` if one exists.
    async fn attempt_put_if_absent(&self, rec: &AttemptRecord) -> Result<bool, StoreError>;

    /// Close the ladder lease of an invocation with its final status.
    async fn attempt_close_lease(
        &self,
        key: &WorkloadKey,
        invocation_id: &str,
        at: DateTime<Utc>,
        outcome: RunStatus,
    ) -> Result<(), StoreError>;

    /// Compaction (a singleton job, SPEC §8.1).
    async fn compact(&self, opts: &CompactOptions) -> Result<CompactReport, StoreError>;
}
