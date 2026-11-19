//! The engine's view of the history store (SPEC §8.1, §8.4, §8.5):
//!
//! - [`open_store`]: URI → backend (`rightsizer_history::open`);
//! - [`StoreView`]: `rightsizer_history::CachedStore` (per-key history with
//!   TTL `history.cache_ttl`; `mode_ceiling` and breakers for
//!   `breaker.cache_ttl`; per-cluster filter) and
//!   `rightsizer_history::BufferedWriter` (bounded `history.buffer`,
//!   overflow dropped and counted), both on the engine's injected clock.
//!
//! Key listing is `HistoryStore::keys()` on every backend (object store:
//! latest pointers plus run prefixes without a pointer; sqlite, postgres,
//! memory: distinct keys). The interim single-file `json://` store is gone:
//! use `sqlite:///path/history.db` or `file:///dir` for a local store.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use rightsizer_core::breaker::{BreakerObjects, BreakerScope};
use rightsizer_core::{Decision, Mode, RunRecord, WorkloadKey};
pub use rightsizer_history::Role;
use rightsizer_history::{
    BreakerRead, BufferOptions, BufferedWriter, CacheOptions, CachedStore, ClusterScope, ControlRead, HistoryRead as CacheRead,
    HistoryStore, MemoryStore, StoreConfig, StoreError, StoreInfo, WriteOutcome,
};

use crate::clock::Clock;
use crate::config::HistoryConfig;
use crate::metrics::Metrics;

/// Lists the workload keys a store holds. Superseded by
/// `HistoryStore::keys()` (every backend implements it); kept so callers
/// that inject a lister keep compiling. [`crate::Engine::keys`] prefers an
/// injected lister, then the store.
pub trait KeyLister: Send + Sync {
    /// Every key with run records.
    fn keys(&self) -> Vec<WorkloadKey>;
}

/// An opened store.
#[derive(Clone)]
pub struct OpenedStore {
    /// The store.
    pub store: Arc<dyn HistoryStore>,
    /// An injected key lister (`None`: `HistoryStore::keys()`).
    pub keys: Option<Arc<dyn KeyLister>>,
}

/// Open the store named by `uri` (or `history.uri`) with `role`'s credentials.
///
/// - none: in-memory (not durable: enforce demotes);
/// - `json://…` (the removed interim store): a configuration error naming the replacements;
/// - anything else (`memory://`, `file://`, `s3://`, `gs://`, `az://`, `sqlite://`, `postgres://`):
///   `rightsizer_history::open`.
pub async fn open_store(uri: Option<&str>, cfg: &HistoryConfig, role: Role) -> Result<OpenedStore, StoreError> {
    let Some(uri) = uri.filter(|u| !u.is_empty()) else {
        return Ok(OpenedStore { store: Arc::new(MemoryStore::new()), keys: None });
    };
    if uri.starts_with("json://") || uri.starts_with("file+json://") {
        return Err(StoreError::Config(format!(
            "{uri}: the interim json:// store was removed; use sqlite:///path/history.db or file:///dir"
        )));
    }
    let sc = StoreConfig { uri: uri.to_string(), role, write_version: cfg.write_version, durable: cfg.durable, shared: cfg.shared, options: cfg.storage_options.clone(), ..StoreConfig::default() };
    Ok(OpenedStore { store: rightsizer_history::open(&sc).await?, keys: None })
}

// ── per-process caches, availability, write buffer ─────────────────────

/// One pending write.
#[derive(Clone, Debug)]
pub enum Pending {
    /// A run record.
    Run(Box<RunRecord>),
    /// A decision.
    Decision(Box<Decision>),
}

impl Pending {
    fn kind(&self) -> &'static str {
        match self {
            Pending::Run(_) => "run",
            Pending::Decision(_) => "decision",
        }
    }
}

/// What [`StoreView::put_run`] did.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum PutResult {
    /// Stored now.
    Inserted,
    /// The store already holds a record with this dedup key.
    Duplicate,
    /// Not written yet: buffered for retry (whether it is a duplicate is unknown).
    Buffered,
    /// Dropped (buffer full or rejected).
    Dropped,
}

/// History availability for one key.
#[derive(Clone, Debug)]
pub struct HistoryRead {
    /// Records, newest first, filtered per cluster (SPEC §8.5).
    pub runs: Vec<RunRecord>,
    /// Fresh, or cached within the TTL.
    pub available: bool,
    /// Served from cache.
    pub cached: bool,
}

/// The engine's store view over `rightsizer_history::CachedStore` (per-key
/// history with TTL `history.cache_ttl`; `mode_ceiling` and breakers for
/// `breaker.cache_ttl`; per-cluster filter) and
/// `rightsizer_history::BufferedWriter` (bounded `history.buffer`; overflow
/// dropped and counted), both on the engine's injected clock.
pub struct StoreView {
    store: Arc<dyn HistoryStore>,
    cached: Arc<CachedStore>,
    writer: Arc<BufferedWriter>,
    metrics: Metrics,
}

impl StoreView {
    /// A view over `store` on the system clock.
    pub fn new(store: Arc<dyn HistoryStore>, cfg: HistoryConfig, breaker_ttl: std::time::Duration, metrics: Metrics) -> Self {
        Self::with_clock(store, cfg, breaker_ttl, metrics, Arc::new(crate::SystemClock))
    }

    /// A view over `store` whose cache TTLs run on `clock`.
    pub fn with_clock(store: Arc<dyn HistoryStore>, cfg: HistoryConfig, breaker_ttl: std::time::Duration, metrics: Metrics, clock: Arc<dyn Clock>) -> Self {
        let opts = CacheOptions {
            history_ttl: cfg.cache_ttl,
            control_ttl: breaker_ttl,
            // Over-fetch under a cluster filter so it still leaves `limit`.
            depth: if cfg.cluster.is_some() { 256 } else { 64 },
            scope: ClusterScope { cluster: cfg.cluster.clone(), share: cfg.share.clone() },
            revalidate: (cfg.revalidate_s > 0).then(|| std::time::Duration::from_secs(cfg.revalidate_s)),
            ..CacheOptions::default()
        };
        let cached = Arc::new(CachedStore::with_clock(store.clone(), opts, clock));
        let writer = BufferedWriter::new(cached.clone(), BufferOptions { capacity: cfg.buffer.max(1), ..BufferOptions::default() });
        StoreView { store, cached, writer, metrics }
    }

    /// The underlying store (uncached).
    pub fn store(&self) -> &Arc<dyn HistoryStore> {
        &self.store
    }

    /// The caching layer.
    pub fn cached(&self) -> &Arc<CachedStore> {
        &self.cached
    }

    /// The write buffer.
    pub fn writer(&self) -> &Arc<BufferedWriter> {
        &self.writer
    }

    /// Facts about the store.
    pub fn info(&self) -> StoreInfo {
        self.store.info()
    }

    /// Same-key history (newest first, ≤ `limit`), per cluster unless shared.
    /// Store errors fall back to the cache within its TTL (SPEC §8.4). `now`
    /// is ignored: the cache runs on the clock it was built with.
    pub async fn history(&self, key: &WorkloadKey, limit: usize, now: DateTime<Utc>) -> HistoryRead {
        self.history_within(key, limit, rightsizer_history::cache::REFRESH_WAIT, now).await
    }

    /// [`history`](Self::history), waiting at most `wait` for a refresh or a
    /// due revalidation (SPEC §8.4; the decision path passes what its budget
    /// allows, so a slow check answers from the cache instead of expiring
    /// the decision).
    pub async fn history_within(&self, key: &WorkloadKey, limit: usize, wait: std::time::Duration, _now: DateTime<Utc>) -> HistoryRead {
        match self.cached.history_within(key, limit, wait).await {
            CacheRead::Fresh(runs) => {
                if self.writer.buffered() > 0 {
                    self.flush().await;
                }
                HistoryRead { runs, available: true, cached: false }
            }
            CacheRead::Cached { records, .. } => HistoryRead { runs: records, available: true, cached: true },
            CacheRead::Unavailable => {
                self.metrics.store_errors.with_label_values(&["runs"]).inc();
                tracing::warn!(key = %key, "history unavailable past its cache TTL");
                HistoryRead { runs: Vec::new(), available: false, cached: false }
            }
        }
    }

    /// Forget the cached history of `key` (after an outcome).
    pub fn invalidate(&self, key: &WorkloadKey) {
        self.cached.invalidate_key(key);
    }

    /// The fleet `mode_ceiling`: `Some(Enforce)` when none was written, `None`
    /// when unreadable (and not cached within the TTL).
    pub async fn mode_ceiling(&self, _now: DateTime<Utc>) -> Option<Mode> {
        match self.cached.control().await {
            ControlRead::Known(c) => Some(c.map(|c| c.mode_ceiling).unwrap_or(Mode::Enforce)),
            ControlRead::Unknown => {
                self.metrics.store_errors.with_label_values(&["control_get"]).inc();
                None
            }
        }
    }

    /// A breaker's objects; `None` when unreadable.
    pub async fn breaker(&self, scope: &BreakerScope, _now: DateTime<Utc>) -> Option<BreakerObjects> {
        match self.cached.breaker(scope).await {
            BreakerRead::Known(o) => Some(o),
            BreakerRead::Unknown => {
                self.metrics.store_errors.with_label_values(&["breaker_get"]).inc();
                None
            }
        }
    }

    /// Drop cached breaker and control state (after a trip or reset by this process).
    pub fn invalidate_breakers(&self) {
        self.cached.invalidate_control();
    }

    /// Queue a write without trying it now (the budget is spent).
    pub fn defer(&self, p: Pending) {
        let kind = p.kind();
        let out = match p {
            Pending::Decision(d) => self.writer.defer_decision_now(*d),
            Pending::Run(r) => {
                let w = self.writer.clone();
                match tokio::runtime::Handle::try_current() {
                    Ok(h) => {
                        h.spawn(async move {
                            w.defer_run(*r).await;
                        });
                        WriteOutcome::Buffered
                    }
                    Err(_) => WriteOutcome::Dropped,
                }
            }
        };
        if out == WriteOutcome::Dropped {
            self.metrics.dropped.with_label_values(&[kind]).inc();
        }
    }

    /// Write a run record now, or buffer it on failure (SPEC §8.4), and say
    /// which: inserted, a duplicate of a stored record, or not written yet.
    pub async fn put_run(&self, rec: RunRecord) -> PutResult {
        if self.writer.buffered() > 0 {
            self.flush().await;
        }
        match self.writer.put_run(rec).await {
            WriteOutcome::Written(rightsizer_history::PutOutcome::Inserted) => PutResult::Inserted,
            WriteOutcome::Written(rightsizer_history::PutOutcome::Duplicate) => PutResult::Duplicate,
            WriteOutcome::Buffered => {
                self.metrics.store_errors.with_label_values(&["put_run"]).inc();
                tracing::warn!(kind = "run", "store write failed: buffered");
                PutResult::Buffered
            }
            WriteOutcome::Dropped => {
                self.metrics.dropped.with_label_values(&["run"]).inc();
                PutResult::Dropped
            }
        }
    }

    /// Write now, or buffer on failure (SPEC §8.4). Returns whether it was written.
    pub async fn put(&self, p: Pending) -> bool {
        if self.writer.buffered() > 0 {
            self.flush().await;
        }
        let kind = p.kind();
        let out = match p {
            Pending::Run(r) => self.writer.put_run(*r).await,
            Pending::Decision(d) => self.writer.put_decision(*d).await,
        };
        match out {
            WriteOutcome::Written(_) => true,
            WriteOutcome::Buffered => {
                let op = if kind == "run" { "put_run" } else { "put_decision" };
                self.metrics.store_errors.with_label_values(&[op]).inc();
                tracing::warn!(kind, "store write failed: buffered");
                false
            }
            WriteOutcome::Dropped => {
                self.metrics.dropped.with_label_values(&[kind]).inc();
                false
            }
        }
    }

    /// Retry buffered writes in order; stops at the first failure.
    pub async fn flush(&self) -> usize {
        self.writer.flush().await
    }

    /// Writes waiting in the buffer.
    pub fn buffered(&self) -> usize {
        self.writer.buffered()
    }
}
