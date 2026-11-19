//! The engine's view of the history store (SPEC §8.1, §8.4, §8.5):
//!
//! - [`open_store`]: URI → backend;
//! - [`StoreView`]: the per-process caches (per-key history with TTL
//!   `history.cache_ttl`; `mode_ceiling` and breakers for
//!   `breaker.cache_ttl`), store-availability tracking, and the bounded
//!   write buffer (10k; overflow dropped and counted);
//! - [`SnapshotStore`]: an interim single-file JSON store for the CLI and
//!   tests (`json:///path/history.json`), not durable or shared.
//!
//! `open_store` delegates every URI but `json://` to the history crate's
//! factory. `StoreView` keeps its own caches rather than
//! `rightsizer_history::CachedStore` because the engine's TTLs run on the
//! injected [`crate::Clock`] (the safety tests move time by hand;
//! `CachedStore` uses `Instant`). TODO(rightsizer-history): swap to
//! `CachedStore` + `BufferedWriter` once they accept a clock.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rightsizer_core::breaker::{BreakerObjects, BreakerScope, Control, Reset, Trip};
use rightsizer_core::{AttemptRecord, Decision, Lease, Mode, RunRecord, RunStatus, WorkloadKey};
pub use rightsizer_history::Role;
use rightsizer_history::{
    Backend, CompactOptions, CompactReport, DecisionFilter, HistoryStore, MemoryStore, PutOutcome, StoreConfig, StoreError,
    StoreInfo, dedup_key,
};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::clock::since;
use crate::config::HistoryConfig;
use crate::metrics::Metrics;

/// Lists the workload keys a store holds (shadow over a tenant).
///
/// TODO(rightsizer-history): the `HistoryStore` trait has no key listing
/// yet; backends that can list keys implement this.
pub trait KeyLister: Send + Sync {
    /// Every key with run records, optionally only one `(platform, tenant)`.
    fn keys(&self) -> Vec<WorkloadKey>;
}

/// An opened store.
#[derive(Clone)]
pub struct OpenedStore {
    /// The store.
    pub store: Arc<dyn HistoryStore>,
    /// Key listing, when the backend supports it.
    pub keys: Option<Arc<dyn KeyLister>>,
}

/// Open the store named by `uri` (or `history.uri`) with `role`'s credentials.
///
/// - none: in-memory (not durable: enforce demotes);
/// - `json://PATH` / `file+json://PATH` / a path ending in `.json`: [`SnapshotStore`] (with key listing);
/// - anything else (`memory://`, `file://`, `s3://`, `gs://`, `az://`, `sqlite://`, `postgres://`):
///   `rightsizer_history::open`.
pub async fn open_store(uri: Option<&str>, cfg: &HistoryConfig, role: Role) -> Result<OpenedStore, StoreError> {
    let Some(uri) = uri.filter(|u| !u.is_empty()) else {
        return Ok(OpenedStore { store: Arc::new(MemoryStore::new()), keys: None });
    };
    let path = uri.strip_prefix("json://").or_else(|| uri.strip_prefix("file+json://")).or_else(|| uri.ends_with(".json").then_some(uri));
    if let Some(p) = path {
        let s = Arc::new(SnapshotStore::open(Path::new(p))?);
        return Ok(OpenedStore { store: s.clone(), keys: Some(s) });
    }
    let sc = StoreConfig { uri: uri.to_string(), role, write_version: cfg.write_version, ..StoreConfig::default() };
    // TODO(rightsizer-history): key listing for the factory's backends (shadow over a tenant).
    Ok(OpenedStore { store: rightsizer_history::open(&sc).await?, keys: None })
}

// ── per-process caches, availability, write buffer ─────────────────────

#[derive(Clone)]
struct Cached<T> {
    value: T,
    at: DateTime<Utc>,
}

/// One pending write.
#[derive(Clone, Debug)]
pub enum Pending {
    /// A run record.
    Run(Box<RunRecord>),
    /// A decision.
    Decision(Box<Decision>),
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

/// The engine's store view: caches, availability and buffered writes.
pub struct StoreView {
    store: Arc<dyn HistoryStore>,
    cfg: HistoryConfig,
    breaker_ttl: std::time::Duration,
    history: Mutex<HashMap<String, Cached<Vec<RunRecord>>>>,
    control: Mutex<Option<Cached<Option<Mode>>>>,
    breakers: Mutex<HashMap<BreakerScope, Cached<BreakerObjects>>>,
    buffer: Mutex<VecDeque<Pending>>,
    metrics: Metrics,
}

impl StoreView {
    /// A view over `store`.
    pub fn new(store: Arc<dyn HistoryStore>, cfg: HistoryConfig, breaker_ttl: std::time::Duration, metrics: Metrics) -> Self {
        StoreView {
            store,
            cfg,
            breaker_ttl,
            history: Mutex::new(HashMap::new()),
            control: Mutex::new(None),
            breakers: Mutex::new(HashMap::new()),
            buffer: Mutex::new(VecDeque::new()),
            metrics,
        }
    }

    /// The underlying store.
    pub fn store(&self) -> &Arc<dyn HistoryStore> {
        &self.store
    }

    /// Facts about the store.
    pub fn info(&self) -> StoreInfo {
        self.store.info()
    }

    fn cluster_ok(&self, r: &RunRecord) -> bool {
        match (&self.cfg.cluster, &r.cluster) {
            (Some(mine), Some(theirs)) => mine == theirs || self.cfg.share.iter().any(|s| s == theirs),
            _ => true,
        }
    }

    /// Same-key history (newest first, ≤ `limit`), per cluster unless shared.
    /// Store errors fall back to the cache within its TTL (SPEC §8.4).
    pub async fn history(&self, key: &WorkloadKey, limit: usize, now: DateTime<Utc>) -> HistoryRead {
        let k = key.to_string();
        let cached = self.history.lock().unwrap_or_else(|e| e.into_inner()).get(&k).cloned();
        if let Some(c) = &cached
            && since(now, c.at) < self.cfg.cache_ttl
        {
            return HistoryRead { runs: c.value.iter().take(limit).cloned().collect(), available: true, cached: true };
        }
        // Over-fetch so the per-cluster filter still leaves `limit`.
        let fetch = if self.cfg.cluster.is_some() { limit.saturating_mul(4).max(limit) } else { limit };
        match self.store.runs(key, fetch).await {
            Ok(v) => {
                let runs: Vec<RunRecord> = v.into_iter().filter(|r| self.cluster_ok(r)).take(limit).collect();
                self.history.lock().unwrap_or_else(|e| e.into_inner()).insert(k, Cached { value: runs.clone(), at: now });
                self.flush().await;
                HistoryRead { runs, available: true, cached: false }
            }
            Err(e) => {
                self.metrics.store_errors.with_label_values(&["runs"]).inc();
                tracing::warn!(error = %e, key = %key, "history unavailable");
                HistoryRead { runs: cached.map(|c| c.value).unwrap_or_default(), available: false, cached: true }
            }
        }
    }

    /// Forget the cached history of `key` (after an outcome).
    pub fn invalidate(&self, key: &WorkloadKey) {
        self.history.lock().unwrap_or_else(|e| e.into_inner()).remove(&key.to_string());
    }

    /// The fleet `mode_ceiling`: `Some(Enforce)` when none was written, `None`
    /// when unreadable (and not cached within the TTL).
    pub async fn mode_ceiling(&self, now: DateTime<Utc>) -> Option<Mode> {
        let cached = self.control.lock().unwrap_or_else(|e| e.into_inner()).clone();
        if let Some(c) = &cached
            && since(now, c.at) < self.breaker_ttl
        {
            return c.value;
        }
        match self.store.control_get().await {
            Ok(c) => {
                let v = Some(c.map(|c| c.mode_ceiling).unwrap_or(Mode::Enforce));
                *self.control.lock().unwrap_or_else(|e| e.into_inner()) = Some(Cached { value: v, at: now });
                v
            }
            Err(e) => {
                self.metrics.store_errors.with_label_values(&["control_get"]).inc();
                tracing::warn!(error = %e, "mode_ceiling unreadable: reductions blocked");
                None
            }
        }
    }

    /// A breaker's objects; `None` when unreadable.
    pub async fn breaker(&self, scope: &BreakerScope, now: DateTime<Utc>) -> Option<BreakerObjects> {
        let cached = self.breakers.lock().unwrap_or_else(|e| e.into_inner()).get(scope).cloned();
        if let Some(c) = &cached
            && since(now, c.at) < self.breaker_ttl
        {
            return Some(c.value.clone());
        }
        match self.store.breaker_get(scope).await {
            Ok(o) => {
                self.breakers.lock().unwrap_or_else(|e| e.into_inner()).insert(scope.clone(), Cached { value: o.clone(), at: now });
                Some(o)
            }
            Err(e) => {
                self.metrics.store_errors.with_label_values(&["breaker_get"]).inc();
                tracing::warn!(error = %e, scope = %scope, "breaker unreadable: treated as open for reductions");
                None
            }
        }
    }

    /// Drop cached breaker and control state (after a trip or reset by this process).
    pub fn invalidate_breakers(&self) {
        self.breakers.lock().unwrap_or_else(|e| e.into_inner()).clear();
        *self.control.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }

    /// Queue a write without trying it now (the budget is spent).
    pub fn defer(&self, p: Pending) {
        self.enqueue(p);
    }

    /// Queue a write; the oldest is dropped (and counted) on overflow.
    fn enqueue(&self, p: Pending) {
        let mut b = self.buffer.lock().unwrap_or_else(|e| e.into_inner());
        if b.len() >= self.cfg.buffer.max(1) {
            let dropped = b.pop_front();
            let kind = match dropped {
                Some(Pending::Run(_)) => "run",
                Some(Pending::Decision(_)) => "decision",
                None => "none",
            };
            self.metrics.dropped.with_label_values(&[kind]).inc();
        }
        b.push_back(p);
    }

    async fn write(&self, p: &Pending) -> Result<(), StoreError> {
        match p {
            Pending::Run(r) => self.store.put_run(r).await.map(|_| ()),
            Pending::Decision(d) => self.store.put_decision(d).await,
        }
    }

    /// Write now, or buffer on failure (SPEC §8.4). Returns whether it was written.
    pub async fn put(&self, p: Pending) -> bool {
        self.flush().await;
        match self.write(&p).await {
            Ok(()) => true,
            Err(e) => {
                let op = if matches!(p, Pending::Run(_)) { "put_run" } else { "put_decision" };
                self.metrics.store_errors.with_label_values(&[op]).inc();
                tracing::warn!(error = %e, "store write failed: buffered");
                self.enqueue(p);
                false
            }
        }
    }

    /// Retry buffered writes in order; stops at the first failure.
    pub async fn flush(&self) -> usize {
        let mut written = 0;
        loop {
            let next = self.buffer.lock().unwrap_or_else(|e| e.into_inner()).front().cloned();
            let Some(p) = next else { break };
            if self.write(&p).await.is_err() {
                break;
            }
            self.buffer.lock().unwrap_or_else(|e| e.into_inner()).pop_front();
            written += 1;
        }
        written
    }

    /// Writes waiting in the buffer.
    pub fn buffered(&self) -> usize {
        self.buffer.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
}

// ── SnapshotStore: one JSON file ────────────────────────────────────────

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Snapshot {
    #[serde(default)]
    v: u16,
    #[serde(default)]
    runs: Vec<RunRecord>,
    #[serde(default)]
    decisions: Vec<Decision>,
    #[serde(default)]
    breakers: Vec<(BreakerScope, BreakerObjects)>,
    #[serde(default)]
    control: Option<Control>,
    #[serde(default)]
    attempts: Vec<AttemptRecord>,
}

/// An interim store in one JSON file, rewritten atomically on every change.
/// For the single-user CLI and tests only: not durable/shared (enforce
/// demotes), O(n) per write.
pub struct SnapshotStore {
    path: PathBuf,
    data: Mutex<Snapshot>,
}

impl SnapshotStore {
    /// Open (or create on first write) the file at `path`.
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        let data = match std::fs::read(path) {
            Ok(b) if !b.is_empty() => serde_json::from_slice(&b)
                .map_err(|e| StoreError::Corrupt { path: path.display().to_string(), message: e.to_string() })?,
            Ok(_) => Snapshot::default(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Snapshot::default(),
            Err(e) => return Err(StoreError::Unreachable(format!("{}: {e}", path.display()))),
        };
        Ok(SnapshotStore { path: path.to_path_buf(), data: Mutex::new(data) })
    }

    fn save(&self, s: &Snapshot) -> Result<(), StoreError> {
        let bytes = serde_json::to_vec(s).map_err(|e| StoreError::Other(e.to_string()))?;
        if let Some(dir) = self.path.parent().filter(|d| !d.as_os_str().is_empty()) {
            std::fs::create_dir_all(dir).map_err(|e| StoreError::Unreachable(e.to_string()))?;
        }
        let tmp = self.path.with_extension("json.tmp");
        std::fs::write(&tmp, bytes).map_err(|e| StoreError::Unreachable(e.to_string()))?;
        std::fs::rename(&tmp, &self.path).map_err(|e| StoreError::Unreachable(e.to_string()))
    }

    fn with<T>(&self, f: impl FnOnce(&mut Snapshot) -> Result<(T, bool), StoreError>) -> Result<T, StoreError> {
        let mut g = self.data.lock().map_err(|_| StoreError::Other("poisoned".into()))?;
        let (out, dirty) = f(&mut g)?;
        if dirty {
            g.v = 1;
            self.save(&g)?;
        }
        Ok(out)
    }

    fn read<T>(&self, f: impl FnOnce(&Snapshot) -> T) -> Result<T, StoreError> {
        let g = self.data.lock().map_err(|_| StoreError::Other("poisoned".into()))?;
        Ok(f(&g))
    }

    /// Every run record (export).
    pub fn all_runs(&self) -> Vec<RunRecord> {
        self.read(|s| s.runs.clone()).unwrap_or_default()
    }
}

impl KeyLister for SnapshotStore {
    fn keys(&self) -> Vec<WorkloadKey> {
        let mut m: BTreeMap<String, WorkloadKey> = BTreeMap::new();
        for r in self.all_runs() {
            m.entry(r.key.to_string()).or_insert(r.key);
        }
        m.into_values().collect()
    }
}

#[async_trait]
impl HistoryStore for SnapshotStore {
    fn info(&self) -> StoreInfo {
        StoreInfo { backend: Backend::Memory, uri: format!("json://{}", self.path.display()), durable_shared: false }
    }

    async fn put_run(&self, rec: &RunRecord) -> Result<PutOutcome, StoreError> {
        let k = dedup_key(rec);
        self.with(|s| {
            if s.runs.iter().any(|r| dedup_key(r) == k) {
                return Ok((PutOutcome::Duplicate, false));
            }
            s.runs.push(rec.clone());
            Ok((PutOutcome::Inserted, true))
        })
    }

    async fn put_decision(&self, d: &Decision) -> Result<(), StoreError> {
        self.with(|s| {
            s.decisions.push(d.clone());
            Ok(((), true))
        })
    }

    async fn runs(&self, key: &WorkloadKey, limit: usize) -> Result<Vec<RunRecord>, StoreError> {
        self.read(|s| {
            let mut v: Vec<RunRecord> = s.runs.iter().filter(|r| &r.key == key).cloned().collect();
            v.sort_by(|a, b| b.started_at.cmp(&a.started_at).then(b.attempt.cmp(&a.attempt)));
            v.truncate(limit);
            v
        })
    }

    async fn decisions(&self, filter: &DecisionFilter) -> Result<Vec<Decision>, StoreError> {
        self.read(|s| {
            let mut v: Vec<Decision> = s.decisions.iter().filter(|d| filter.matches(d)).cloned().collect();
            v.sort_by_key(|d| std::cmp::Reverse(d.id));
            if let Some(n) = filter.limit {
                v.truncate(n);
            }
            v
        })
    }

    async fn decision(&self, id: Ulid) -> Result<Option<Decision>, StoreError> {
        self.read(|s| s.decisions.iter().find(|d| d.id == id).cloned())
    }

    async fn breaker_get(&self, scope: &BreakerScope) -> Result<BreakerObjects, StoreError> {
        self.read(|s| s.breakers.iter().find(|(k, _)| k == scope).map(|(_, v)| v.clone()).unwrap_or_default())
    }

    async fn breaker_list(&self) -> Result<Vec<(BreakerScope, BreakerObjects)>, StoreError> {
        self.read(|s| s.breakers.clone())
    }

    async fn breaker_trip(&self, trip: &Trip) -> Result<(), StoreError> {
        self.with(|s| {
            let i = match s.breakers.iter().position(|(k, _)| k == &trip.scope) {
                Some(i) => i,
                None => {
                    s.breakers.push((trip.scope.clone(), BreakerObjects::default()));
                    s.breakers.len() - 1
                }
            };
            if s.breakers[i].1.trips.iter().any(|t| t.id == trip.id) {
                return Err(StoreError::Conflict(format!("trip {} exists", trip.id)));
            }
            s.breakers[i].1.trips.push(trip.clone());
            Ok(((), true))
        })
    }

    async fn breaker_reset(&self, reset: &Reset) -> Result<(), StoreError> {
        self.with(|s| {
            match s.breakers.iter_mut().find(|(k, _)| k == &reset.scope) {
                Some((_, o)) => o.resets.push(reset.clone()),
                None => s.breakers.push((reset.scope.clone(), BreakerObjects { trips: vec![], resets: vec![reset.clone()] })),
            }
            Ok(((), true))
        })
    }

    async fn control_get(&self) -> Result<Option<Control>, StoreError> {
        self.read(|s| s.control.clone())
    }

    async fn control_set(&self, control: &Control) -> Result<(), StoreError> {
        self.with(|s| {
            s.control = Some(control.clone());
            Ok(((), true))
        })
    }

    async fn attempt_get(&self, key: &WorkloadKey, invocation_id: &str) -> Result<Option<AttemptRecord>, StoreError> {
        self.read(|s| s.attempts.iter().find(|a| &a.key == key && a.invocation_id == invocation_id).cloned())
    }

    async fn attempt_put_if_absent(&self, rec: &AttemptRecord) -> Result<bool, StoreError> {
        self.with(|s| {
            if s.attempts.iter().any(|a| a.key == rec.key && a.invocation_id == rec.invocation_id) {
                return Ok((false, false));
            }
            s.attempts.push(rec.clone());
            Ok((true, true))
        })
    }

    async fn attempt_close_lease(&self, key: &WorkloadKey, invocation_id: &str, at: DateTime<Utc>, outcome: RunStatus) -> Result<(), StoreError> {
        self.with(|s| {
            let a = s
                .attempts
                .iter_mut()
                .find(|a| &a.key == key && a.invocation_id == invocation_id)
                .ok_or_else(|| StoreError::Other(format!("no attempt record for {invocation_id}")))?;
            let lease = a.lease.get_or_insert(Lease { opened_at: at, closed_at: None, outcome: None });
            lease.closed_at = Some(at);
            lease.outcome = Some(outcome);
            Ok(((), true))
        })
    }

    async fn compact(&self, opts: &CompactOptions) -> Result<CompactReport, StoreError> {
        let cutoff = opts.now - chrono::Duration::from_std(opts.max_age).unwrap_or_default();
        let dcut = opts.now - chrono::Duration::from_std(opts.decision_max_age).unwrap_or_default();
        self.with(|s| {
            let before = s.runs.len() + s.decisions.len();
            let mut keys: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
            for r in &s.runs {
                keys.insert(r.key.to_string());
            }
            if !opts.dry_run {
                s.runs.retain(|r| r.started_at >= cutoff);
                s.decisions.retain(|d| rightsizer_history::ulid_time(d.id) >= dcut);
            }
            let deleted = before - (s.runs.len() + s.decisions.len());
            Ok((CompactReport { pointers: keys.len(), snapshots: 0, deleted }, !opts.dry_run && deleted > 0))
        })
    }
}
