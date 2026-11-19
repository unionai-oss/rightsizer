//! `rightsizer-history`: the [`HistoryStore`] trait (SPEC §8.1) and its
//! backends: object store (local FS, S3 and S3-compatible, GCS, Azure; the
//! default for Flyte, Ray, Slurm), sqlite (single-user CLI), postgres
//! (feature `postgres`; the K8s webhook and watcher), and the in-memory
//! [`MemoryStore`] used in tests across the workspace.
//!
//! Around the backends: [`CachedStore`] (per-key LRU with TTL, 60 s breaker
//! and control cache, unreachable semantics of SPEC §8.4), [`BufferedWriter`]
//! (bounded write buffer with retry), [`open`] (store URI → backend) and
//! [`export`] (`rsz history export --parquet`, SPEC §14.1).

#![forbid(unsafe_code)]

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rightsizer_core::breaker::{BreakerObjects, BreakerScope, Control, Reset, Trip};
use rightsizer_core::{AttemptRecord, Decision, Mode, RunRecord, RunStatus, Tenant, WorkloadKey};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

pub mod buffer;
pub mod cache;
pub mod export;
pub mod factory;
pub mod fsdetect;
pub mod layout;
pub mod memory;
pub mod object;
#[cfg(feature = "postgres")]
pub mod postgres;
pub mod snapshot;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod sql;
#[cfg(feature = "sqlite")]
pub mod sqlite;

pub use buffer::{BufferOptions, BufferStats, BufferedWriter, WriteOutcome};
pub use cache::{BreakerRead, CacheOptions, CachedStore, ClusterScope, ControlRead, HistoryRead};
pub use factory::{StoreConfig, StoreUri, open, parse_uri};
pub use memory::MemoryStore;
pub use object::{ObjectHistoryStore, ObjectStoreOptions};

/// The credentials a store was opened with (SPEC §8.1 "control objects are
/// operator-written"). A [`Role::Client`] store creates trips, runs,
/// decisions and attempt records but refuses breaker resets and control
/// writes with [`StoreError::PermissionDenied`], so a tenant can trip a
/// breaker (more conservative) but never reset one or raise `mode_ceiling`.
/// Real enforcement is the backend's own (IAM prefix policy, postgres
/// role); this is the client-side guard that matches it.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// Tenant / workload credentials.
    #[default]
    Client,
    /// Operator credentials (`rsz breaker reset`, `rsz control`, compaction).
    Operator,
}

impl Role {
    /// `Ok` for operators, `PermissionDenied` naming `what` otherwise.
    pub fn require_operator(self, what: &str) -> Result<(), StoreError> {
        match self {
            Role::Operator => Ok(()),
            Role::Client => Err(StoreError::PermissionDenied(format!("{what} needs operator credentials"))),
        }
    }
}

/// Readers accept the current and the previous wire version (SPEC §13.2 N/N-1).
pub const MIN_READ_VERSION: u16 = rightsizer_core::WIRE_VERSION.saturating_sub(1);
/// The newest wire version this build reads (and the default `write_version`).
pub const MAX_READ_VERSION: u16 = rightsizer_core::WIRE_VERSION;

/// Whether a stored artifact with wire version `v` is readable here (N/N-1).
pub fn readable_version(v: u16) -> bool {
    (MIN_READ_VERSION..=MAX_READ_VERSION).contains(&v)
}

/// Validate a configured `history.write_version` (SPEC §13.2): writers may
/// only write a version every reader of this build accepts.
pub fn check_write_version(v: u16) -> Result<u16, StoreError> {
    if readable_version(v) && v >= 1 {
        Ok(v)
    } else {
        Err(StoreError::Config(format!(
            "history.write_version = {v} is not supported (this build writes {lo}..={MAX_READ_VERSION})",
            lo = std::cmp::max(MIN_READ_VERSION, 1)
        )))
    }
}

/// Decode one stored artifact with the N/N-1 rule: unknown fields are
/// ignored, missing optional fields take their serde defaults, a missing
/// `"v"` reads as 1, and a version outside [`readable_version`] yields
/// `Ok(None)` (skipped and counted by the caller, never a read failure).
pub fn decode_versioned<T: serde::de::DeserializeOwned>(bytes: &[u8], path: &str) -> Result<Option<T>, StoreError> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|e| StoreError::Corrupt { path: path.to_string(), message: e.to_string() })?;
    decode_value(value, path)
}

/// [`decode_versioned`] over an already-parsed JSON value.
pub fn decode_value<T: serde::de::DeserializeOwned>(value: serde_json::Value, path: &str) -> Result<Option<T>, StoreError> {
    let v = value.get("v").and_then(|v| v.as_u64()).unwrap_or(1);
    if v > u16::MAX as u64 || !readable_version(v as u16) {
        tracing::warn!(path, v, "skipping artifact with unsupported wire version");
        return Ok(None);
    }
    serde_json::from_value(value).map(Some).map_err(|e| StoreError::Corrupt { path: path.to_string(), message: e.to_string() })
}

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

/// Operational facts for metrics (SPEC §8.1, §13.1:
/// `rightsizer_store_shards`, `rightsizer_store_compaction_lag_seconds`).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct StoreStats {
    /// Run shards / rows known at the last count (object store: after the
    /// last compaction; SQL: live row count).
    pub shards: Option<u64>,
    /// Seconds since the last successful compaction finished.
    pub compaction_lag_seconds: Option<f64>,
    /// The last compaction's details.
    pub last_compaction: Option<CompactionStatus>,
    /// Artifacts skipped on read in this process (corrupt or unsupported version).
    pub skipped_on_read: u64,
}

/// What the last compaction did (persisted by the backend, read by
/// [`HistoryStore::stats`]).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CompactionStatus {
    /// Wire version.
    #[serde(default = "one")]
    pub v: u16,
    /// Who ran it.
    #[serde(default)]
    pub holder: String,
    /// Started.
    pub started_at: Option<DateTime<Utc>>,
    /// Finished.
    pub finished_at: Option<DateTime<Utc>>,
    /// Keys visited.
    #[serde(default)]
    pub keys: u64,
    /// Pointers rewritten.
    #[serde(default)]
    pub pointers: u64,
    /// Snapshot files written.
    #[serde(default)]
    pub snapshots: u64,
    /// Run shards deleted.
    #[serde(default)]
    pub run_shards_deleted: u64,
    /// Decision shards deleted.
    #[serde(default)]
    pub decisions_deleted: u64,
    /// Expired attempt records deleted.
    #[serde(default)]
    pub attempts_deleted: u64,
    /// Snapshots past retention deleted.
    #[serde(default)]
    pub snapshots_deleted: u64,
    /// Run shards remaining after compaction.
    #[serde(default)]
    pub shards_remaining: u64,
    /// Newest decision id included in a snapshot (object store).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_watermark: Option<Ulid>,
}

fn one() -> u16 {
    1
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

/// Order run records newest first: `started_at` descending, then attempt
/// descending, then invocation id (deterministic across backends).
pub fn sort_newest_first(v: &mut [RunRecord]) {
    v.sort_by(|a, b| {
        b.started_at.cmp(&a.started_at).then(b.attempt.cmp(&a.attempt)).then_with(|| b.invocation_id.cmp(&a.invocation_id))
    });
}

/// Merge records from several sources, deduplicated on [`dedup_key`] (the
/// first occurrence wins, so pass sources oldest-written first), newest
/// first, at most `limit`.
pub fn merge_runs(records: impl IntoIterator<Item = RunRecord>, limit: usize) -> Vec<RunRecord> {
    let mut seen = std::collections::HashSet::new();
    let mut v: Vec<RunRecord> = records.into_iter().filter(|r| seen.insert(dedup_key(r))).collect();
    sort_newest_first(&mut v);
    v.truncate(limit);
    v
}

/// Attempt records expire after 7 days (SPEC §8.1).
pub const ATTEMPT_TTL: Duration = Duration::from_secs(7 * 86_400);

/// Whether an attempt record written at `written_at` has expired at `now`.
pub fn attempt_expired(written_at: DateTime<Utc>, now: DateTime<Utc>, ttl: Duration) -> bool {
    chrono::Duration::from_std(ttl).is_ok_and(|ttl| written_at + ttl < now)
}

/// A compaction lease (singleton job, SPEC §8.1).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionLease {
    /// Wire version.
    #[serde(default = "one")]
    pub v: u16,
    /// Holder id.
    pub holder: String,
    /// Acquired.
    pub acquired_at: DateTime<Utc>,
    /// Expires; a lease past this may be taken over.
    pub expires_at: DateTime<Utc>,
}

/// A URI with its password (if any) replaced by `***` (for [`StoreInfo::uri`] and logs).
pub fn redact_uri(uri: &str) -> String {
    match url::Url::parse(uri) {
        Ok(mut u) if u.password().is_some() => {
            let _ = u.set_password(Some("***"));
            u.to_string()
        }
        _ => uri.to_string(),
    }
}

/// A default compaction holder id: `{host}-{pid}-{ulid}`.
pub fn default_holder() -> String {
    let host = std::env::var("HOSTNAME").ok().filter(|h| !h.is_empty()).unwrap_or_else(|| "host".into());
    format!("{host}-{}-{}", std::process::id(), Ulid::generate())
}

/// A history store (SPEC §8.1). Every method may fail; callers apply S1 and
/// the availability rules of SPEC §8.4 (cache, buffered writes, demotion).
#[async_trait]
pub trait HistoryStore: Send + Sync {
    /// Facts about the store.
    fn info(&self) -> StoreInfo;

    /// Durable: survives the process and the host's restart (SPEC §8.1).
    /// Default: [`StoreInfo::durable_shared`].
    fn durable(&self) -> bool {
        self.info().durable_shared
    }

    /// Shared across hosts. Enforce needs `durable() && shared()`; the engine
    /// demotes enforce to recommend otherwise (SPEC §8.1). Default:
    /// [`StoreInfo::durable_shared`].
    fn shared(&self) -> bool {
        self.info().durable_shared
    }

    /// The credentials' role. Default: operator (stores that do not model
    /// roles accept every write).
    fn role(&self) -> Role {
        Role::Operator
    }

    /// Every workload key with stored runs (export, compaction tooling).
    /// Default: unsupported.
    async fn keys(&self) -> Result<Vec<WorkloadKey>, StoreError> {
        Err(StoreError::Unimplemented("keys"))
    }

    /// Operational stats for metrics. Default: empty.
    async fn stats(&self) -> Result<StoreStats, StoreError> {
        Ok(StoreStats::default())
    }

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
