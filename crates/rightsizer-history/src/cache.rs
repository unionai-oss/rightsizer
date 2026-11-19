//! Per-process caching (SPEC §8.4, §2.3 S1/S5).
//!
//! - **History:** a per-key LRU with TTL (`history_cache_ttl`, default
//!   10 min). Within the TTL a key is served from the cache without touching
//!   the store; past it the key is refetched. If the store is unreachable,
//!   the cached history is used **up to the TTL**; after that, and for keys
//!   never cached, the read reports [`HistoryRead::Unavailable`] and the
//!   engine demotes enforce to recommend (`store_unreachable`).
//! - **Breakers and control:** cached 60 s. If they cannot be read past
//!   that, [`BreakerRead::Unknown`] / [`ControlRead::Unknown`]: callers treat
//!   the breaker as **open for reductions**. Reads are **single flight** per
//!   scope (and for the control object) and run as their own task, like
//!   history reads. Between one and two TTLs old, an entry is served while
//!   one background refresh runs (stale-while-revalidate); a refresh that
//!   fails drops the entry, so an unreadable store still ends in `Unknown`.
//!   A trip, reset or control write by this process (or an invalidation)
//!   bumps an epoch that keeps an older in-flight read out of the cache.
//!   Why: every decision reads several scopes (workload, input bucket,
//!   fleet, each estimator version) and the control object, and each
//!   breaker read is several store calls; without this, a fan-out in a new
//!   process, or at each expiry, sent every concurrent decision to the store
//!   for each scope and spent its whole budget there (M1 devbox QA: up to 50
//!   of 90 decisions per round fell back to the prior).
//! - **Cluster scope (SPEC §8.5):** history is per cluster unless
//!   `history.share` merges clusters. Records of other clusters are dropped
//!   from reads; records without a cluster are kept.
//!
//! - **Clock:** every TTL runs on an injectable
//!   [`rightsizer_core::clock::Clock`] ([`CachedStore::with_clock`]); the
//!   engine passes its own, so tests that move its clock move these TTLs.
//!   [`CachedStore::new`] uses [`TokioClock`] (wall time anchored to tokio's
//!   clock, so `tokio::time::pause`/`advance` move it too).
//!
//! - **Single flight (history):** concurrent misses on one key share one read,
//!   and the read runs as its own task: a caller that gives up (an expired
//!   decision budget) does not cancel it, so the cache still fills and the
//!   next decision is a hit. An invalidation while a read is in flight keeps
//!   its (possibly stale) result out of the cache, and so does a write
//!   through this cache ([`HistoryStore::put_run`]). A read that takes longer
//!   than [`HISTORY_READ_TIMEOUT`] fails (store unreachable) instead of
//!   holding every reader of the key.
//! - **Refresh after invalidation:** [`CachedStore::invalidate_key`] (the
//!   engine calls it after every outcome, to see records other processes
//!   wrote) marks the entry stale rather than dropping it. The next read
//!   starts a refresh and waits up to [`REFRESH_WAIT`] for it; if the store is
//!   slower, it answers from the stale entry (still within its TTL) and the
//!   refresh fills the cache in the background. A fan-out whose outcomes keep
//!   invalidating a key therefore never waits on a full history read.
//!
//! - **Revalidation (cross-process freshness):** history written by another
//!   process is otherwise invisible until the TTL. With
//!   [`CacheOptions::revalidate`] set (the engine's `history.revalidate_s`,
//!   30 s), a hit whose last check is older than that asks the store whether
//!   the key changed since the entry's run marker
//!   ([`HistoryStore::runs_changed_since`]: one LIST `start-after` on object
//!   stores, one indexed query on databases). The check runs as its own task
//!   (one per key per interval); the reader waits for it, and for the refresh
//!   it triggers, at most its `wait` ([`CachedStore::history_within`]) and
//!   otherwise answers from the cached entry. A detected change marks the
//!   entry stale and calls the [`CachedStore::set_on_change`] hook (the
//!   engine drops the key's memo entries). A hit with no check due costs a
//!   clock read.
//!
//! [`CachedStore`] also implements [`HistoryStore`] (reads go through the
//! cache; an unavailable read is `StoreError::Unreachable`), so it can be
//! used wherever a store is.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rightsizer_core::breaker::{BreakerObjects, BreakerScope, Control, Reset, Trip};
use rightsizer_core::clock::{Clock, elapsed};
use rightsizer_core::{AttemptRecord, Decision, RunRecord, RunStatus, WorkloadKey};
use ulid::Ulid;

/// How long a read of an invalidated key waits for its refresh before
/// answering from the stale entry.
pub const REFRESH_WAIT: Duration = Duration::from_millis(25);

/// Deadline of one shared history read; past it the read is an error
/// (the store counts as unreachable for SPEC §8.4).
pub const HISTORY_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// The generation slot bumped by [`CachedStore::invalidate`] (no key looks like this).
const ALL_KEYS: &str = "\u{0}all";

/// Wall time that follows tokio's clock: `Utc::now()` at construction plus
/// the tokio time elapsed since (so a paused runtime's `advance` moves it).
#[derive(Debug)]
pub struct TokioClock {
    base: DateTime<Utc>,
    start: tokio::time::Instant,
}

impl TokioClock {
    /// Anchored now.
    pub fn new() -> Self {
        TokioClock { base: Utc::now(), start: tokio::time::Instant::now() }
    }
}

impl Default for TokioClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for TokioClock {
    fn now(&self) -> DateTime<Utc> {
        self.base + chrono::Duration::from_std(self.start.elapsed()).unwrap_or_default()
    }
}

type Stamp = DateTime<Utc>;

use crate::{
    CompactOptions, CompactReport, DecisionFilter, HistoryStore, PutOutcome, Role, StoreError, StoreInfo, StoreStats, merge_runs,
};

/// Which clusters' records a reader sees (SPEC §8.5).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClusterScope {
    /// This cluster (`None`: every cluster).
    pub cluster: Option<String>,
    /// Clusters merged with this one (`history.share`).
    pub share: Vec<String>,
}

impl ClusterScope {
    /// Whether a record from `cluster` is visible.
    pub fn admits(&self, cluster: Option<&str>) -> bool {
        match (&self.cluster, cluster) {
            (None, _) | (_, None) => true,
            (Some(me), Some(c)) => me == c || self.share.iter().any(|s| s == c),
        }
    }
}

/// Cache settings.
#[derive(Clone, Debug, PartialEq)]
pub struct CacheOptions {
    /// History TTL (10 min).
    pub history_ttl: Duration,
    /// Breaker and control TTL (60 s).
    pub control_ttl: Duration,
    /// Keys kept (LRU beyond this).
    pub capacity: usize,
    /// Records fetched and cached per key (at least the largest `limit` asked).
    pub depth: usize,
    /// Cluster scope.
    pub scope: ClusterScope,
    /// Check a cached key for runs written elsewhere when its last check is
    /// older than this (`None`: never; only the TTL refreshes).
    pub revalidate: Option<Duration>,
}

impl Default for CacheOptions {
    fn default() -> Self {
        CacheOptions {
            history_ttl: Duration::from_secs(600),
            control_ttl: Duration::from_secs(60),
            capacity: 10_000,
            depth: 64,
            scope: ClusterScope::default(),
            revalidate: None,
        }
    }
}

/// A history read.
#[derive(Clone, Debug, PartialEq)]
pub enum HistoryRead {
    /// Read from the store now.
    Fresh(Vec<RunRecord>),
    /// From the cache, fetched `age` ago (within the TTL).
    Cached {
        /// Records.
        records: Vec<RunRecord>,
        /// Age of the cache entry.
        age: Duration,
    },
    /// Store unreachable past the TTL (or never cached): demote enforce.
    Unavailable,
}

impl HistoryRead {
    /// The records, if available.
    pub fn records(&self) -> Option<&[RunRecord]> {
        match self {
            HistoryRead::Fresh(r) | HistoryRead::Cached { records: r, .. } => Some(r),
            HistoryRead::Unavailable => None,
        }
    }
    /// Whether history is unavailable.
    pub fn is_unavailable(&self) -> bool {
        matches!(self, HistoryRead::Unavailable)
    }
}

/// A breaker read.
#[derive(Clone, Debug, PartialEq)]
pub enum BreakerRead {
    /// Known state.
    Known(BreakerObjects),
    /// Unreadable: treat as open for reductions (S1, S5).
    Unknown,
}

impl BreakerRead {
    /// Open for reductions: open, or unknown.
    pub fn blocks_reductions(&self) -> bool {
        match self {
            BreakerRead::Known(o) => o.is_open(),
            BreakerRead::Unknown => true,
        }
    }
}

/// A control read.
#[derive(Clone, Debug, PartialEq)]
pub enum ControlRead {
    /// Known (`None`: no control object written).
    Known(Option<Control>),
    /// Unreadable: reductions blocked, mode kept (SPEC §8.4).
    Unknown,
}

struct Entry<T> {
    value: T,
    at: Stamp,
    used: u64,
}

struct Lru<K, V> {
    map: HashMap<K, Entry<V>>,
    tick: u64,
    capacity: usize,
}

impl<K: std::hash::Hash + Eq + Clone, V: Clone> Lru<K, V> {
    fn new(capacity: usize) -> Self {
        Lru { map: HashMap::new(), tick: 0, capacity: capacity.max(1) }
    }
    fn get(&mut self, k: &K) -> Option<(V, Stamp)> {
        self.tick += 1;
        let t = self.tick;
        self.map.get_mut(k).map(|e| {
            e.used = t;
            (e.value.clone(), e.at)
        })
    }
    fn put(&mut self, k: K, v: V, at: Stamp) {
        self.tick += 1;
        if !self.map.contains_key(&k) && self.map.len() >= self.capacity {
            // Evict the least recently used (O(n) at capacity; capacity is small).
            if let Some(old) = self.map.iter().min_by_key(|(_, e)| e.used).map(|(k, _)| k.clone()) {
                self.map.remove(&old);
            }
        }
        self.map.insert(k, Entry { value: v, at, used: self.tick });
    }
    fn update(&mut self, k: &K, f: impl FnOnce(&mut V)) {
        if let Some(e) = self.map.get_mut(k) {
            f(&mut e.value);
        }
    }
    fn contains(&self, k: &K) -> bool {
        self.map.contains_key(k)
    }
    fn remove(&mut self, k: &K) {
        self.map.remove(k);
    }
    fn len(&self) -> usize {
        self.map.len()
    }
}

/// One key's cached history.
#[derive(Clone)]
struct CachedRuns {
    /// Records, newest first.
    records: Vec<RunRecord>,
    /// How many were asked for.
    fetched: usize,
    /// The store's run marker taken before the read (`None`: no revalidation).
    marker: Option<String>,
    /// Last revalidation (or the fill).
    checked: Stamp,
}

/// Called with a key whose history changed in the store (revalidation).
pub type ChangeHook = Arc<dyn Fn(&WorkloadKey) + Send + Sync>;

/// A caching wrapper around any store.
type RunsCache = Arc<Mutex<Lru<String, CachedRuns>>>;
type HistoryFlight = futures::future::Shared<futures::future::BoxFuture<'static, Result<Arc<Vec<RunRecord>>, String>>>;
type BreakerCache = Arc<Mutex<HashMap<BreakerScope, (BreakerObjects, Stamp)>>>;
type BreakerFlight = futures::future::Shared<futures::future::BoxFuture<'static, Result<BreakerObjects, String>>>;
type ControlCache = Arc<Mutex<Option<(Option<Control>, Stamp)>>>;
type ControlFlight = futures::future::Shared<futures::future::BoxFuture<'static, Result<Option<Control>, String>>>;

pub struct CachedStore {
    inner: Arc<dyn HistoryStore>,
    opts: CacheOptions,
    clock: Arc<dyn Clock>,
    runs: RunsCache,
    /// In-flight history reads by key, with the key's invalidation generation.
    inflight: Arc<Mutex<HashMap<String, (u64, HistoryFlight)>>>,
    /// Bumped by every invalidation of a key (and of everything: the [`ALL_KEYS`] slot).
    generations: Arc<Mutex<HashMap<String, u64>>>,
    /// Keys invalidated since their entry was cached (served stale while refreshing).
    stale: Arc<Mutex<std::collections::HashSet<String>>>,
    breakers: BreakerCache,
    control: ControlCache,
    /// In-flight breaker reads by scope, and the control read (single flight).
    breaker_flights: Arc<Mutex<HashMap<BreakerScope, (u64, BreakerFlight)>>>,
    control_flight: Arc<Mutex<Option<(u64, ControlFlight)>>>,
    /// Bumped by every trip, reset, control write and invalidation through this
    /// cache: a read started before it does not fill the cache.
    control_epoch: Arc<std::sync::atomic::AtomicU64>,
    /// Revalidation hook.
    on_change: Arc<std::sync::OnceLock<ChangeHook>>,
}

impl CachedStore {
    /// Wrap `inner`; TTLs run on [`TokioClock`].
    pub fn new(inner: Arc<dyn HistoryStore>, opts: CacheOptions) -> Self {
        Self::with_clock(inner, opts, Arc::new(TokioClock::new()))
    }

    /// Wrap `inner`; TTLs run on `clock`.
    pub fn with_clock(inner: Arc<dyn HistoryStore>, opts: CacheOptions, clock: Arc<dyn Clock>) -> Self {
        CachedStore {
            runs: Arc::new(Mutex::new(Lru::new(opts.capacity))),
            inflight: Arc::new(Mutex::new(HashMap::new())),
            generations: Arc::new(Mutex::new(HashMap::new())),
            stale: Arc::new(Mutex::new(std::collections::HashSet::new())),
            inner,
            opts,
            clock,
            breakers: Arc::new(Mutex::new(HashMap::new())),
            control: Arc::new(Mutex::new(None)),
            breaker_flights: Arc::new(Mutex::new(HashMap::new())),
            control_flight: Arc::new(Mutex::new(None)),
            control_epoch: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            on_change: Arc::new(std::sync::OnceLock::new()),
        }
    }

    /// Set the hook called when revalidation finds a key changed in the
    /// store (once; later calls are ignored).
    pub fn set_on_change(&self, hook: ChangeHook) {
        let _ = self.on_change.set(hook);
    }

    /// The wrapped store.
    pub fn inner(&self) -> &Arc<dyn HistoryStore> {
        &self.inner
    }

    /// Keys currently cached.
    pub fn cached_keys(&self) -> usize {
        self.runs.lock().map(|g| g.len()).unwrap_or(0)
    }

    /// Mark one key's cached history stale: the next read refreshes it (an
    /// in-flight read started before this no longer fills the cache).
    pub fn invalidate_key(&self, key: &WorkloadKey) {
        mark_stale(&self.generations, &self.inflight, &self.runs, &self.stale, &key.to_string());
    }

    /// Drop one key's cached history entirely.
    pub fn forget_key(&self, key: &WorkloadKey) {
        let k = key.to_string();
        self.bump_generation(&k);
        if let Ok(mut st) = self.stale.lock() {
            st.remove(&k);
        }
        if let Ok(mut g) = self.runs.lock() {
            g.remove(&k);
        }
    }

    fn bump_generation(&self, k: &str) {
        bump(&self.generations, &self.inflight, k);
    }

    fn generation(gens: &Mutex<HashMap<String, u64>>, k: &str) -> u64 {
        gens.lock().map(|g| g.get(k).copied().unwrap_or(0).wrapping_add(g.get(ALL_KEYS).copied().unwrap_or(0))).unwrap_or(0)
    }

    /// The shared read of `key` (joining one in flight, or starting it as its own task).
    fn history_flight(&self, key: &WorkloadKey, k: &str, depth: usize) -> HistoryFlight {
        use futures::FutureExt as _;

        let Ok(mut g) = self.inflight.lock() else {
            let inner = self.inner.clone();
            let key = key.clone();
            return async move { inner.runs(&key, depth).await.map(Arc::new).map_err(|e| e.to_string()) }.boxed().shared();
        };
        let generation = Self::generation(&self.generations, k);
        if let Some((gen0, f)) = g.get(k)
            && *gen0 == generation
        {
            return f.clone();
        }
        let (inner, runs, clock, inflight, gens, stale) = (
            self.inner.clone(),
            self.runs.clone(),
            self.clock.clone(),
            self.inflight.clone(),
            self.generations.clone(),
            self.stale.clone(),
        );
        let (key2, k2) = (key.clone(), k.to_string());
        let in_runtime = tokio::runtime::Handle::try_current().is_ok();
        let read = async move {
            // A hung store must not pin this flight (and every reader of the
            // key that joins it) forever: past the deadline it is an error, so
            // readers fall back to the cache within its TTL or demote.
            let r = if in_runtime {
                match tokio::time::timeout(HISTORY_READ_TIMEOUT, inner.runs_marked(&key2, depth)).await {
                    Ok(r) => r,
                    Err(_) => Err(StoreError::Unreachable(format!("history read of {key2} timed out after {HISTORY_READ_TIMEOUT:?}"))),
                }
            } else {
                inner.runs_marked(&key2, depth).await
            };
            if let Ok((v, marker)) = &r
                && Self::generation(&gens, &k2) == generation
                && let Ok(mut c) = runs.lock()
            {
                let now = clock.now();
                c.put(k2.clone(), CachedRuns { records: v.clone(), fetched: depth, marker: marker.clone(), checked: now }, now);
                if let Ok(mut st) = stale.lock() {
                    st.remove(&k2);
                }
            }
            if let Ok(mut m) = inflight.lock()
                && m.get(&k2).is_some_and(|(g0, _)| *g0 == generation)
            {
                m.remove(&k2);
            }
            r.map(|(v, _)| Arc::new(v)).map_err(|e| e.to_string())
        };
        let flight: HistoryFlight = match tokio::runtime::Handle::try_current() {
            Ok(h) => {
                let task = h.spawn(read);
                async move { task.await.unwrap_or_else(|e| Err(format!("history read task: {e}"))) }.boxed().shared()
            }
            Err(_) => read.boxed().shared(),
        };
        g.insert(k.to_string(), (generation, flight.clone()));
        flight
    }

    /// The clock TTLs run on.
    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }

    /// The cache settings.
    pub fn options(&self) -> &CacheOptions {
        &self.opts
    }

    /// Drop cached breakers and the control object (after a trip, reset or
    /// control write elsewhere in this process); history stays cached.
    pub fn invalidate_control(&self) {
        self.bump_control_epoch();
        if let Ok(mut g) = self.breakers.lock() {
            g.clear();
        }
        if let Ok(mut g) = self.control.lock() {
            *g = None;
        }
    }

    /// Drop everything cached.
    pub fn invalidate(&self) {
        self.bump_generation(ALL_KEYS);
        if let Ok(mut g) = self.inflight.lock() {
            g.clear();
        }
        if let Ok(mut st) = self.stale.lock() {
            st.clear();
        }
        if let Ok(mut g) = self.runs.lock() {
            *g = Lru::new(self.opts.capacity);
        }
        self.bump_control_epoch();
        if let Ok(mut g) = self.breakers.lock() {
            g.clear();
        }
        if let Ok(mut g) = self.control.lock() {
            *g = None;
        }
    }

    fn scoped(&self, v: &[RunRecord], limit: usize) -> Vec<RunRecord> {
        v.iter().filter(|r| self.opts.scope.admits(r.cluster.as_deref())).take(limit).cloned().collect()
    }

    /// History of `key` under the SPEC §8.4 rules (waits up to
    /// [`REFRESH_WAIT`] for a refresh or revalidation).
    pub async fn history(&self, key: &WorkloadKey, limit: usize) -> HistoryRead {
        self.history_within(key, limit, REFRESH_WAIT).await
    }

    /// [`history`](Self::history), waiting at most `wait` for a refresh of
    /// an invalidated entry or for a due revalidation (callers pass what
    /// their decision budget allows); past it the cached entry answers and
    /// the refresh completes in the background. A miss still waits for the
    /// read (bounded by [`HISTORY_READ_TIMEOUT`] and the caller's own budget).
    pub async fn history_within(&self, key: &WorkloadKey, limit: usize, wait: Duration) -> HistoryRead {
        let k = key.to_string();
        let cached = self.runs.lock().ok().and_then(|mut g| g.get(&k));
        let now = self.clock.now();
        // A cached entry answers `limit` if it holds that many records, or
        // if its fetch returned fewer than it asked for (nothing more exists).
        if let Some((e, at)) = &cached
            && elapsed(now, *at) < self.opts.history_ttl
            && (e.records.len() >= limit || e.records.len() < e.fetched)
        {
            let age = elapsed(now, *at);
            let stale = self.stale.lock().map(|st| st.contains(&k)).unwrap_or(false);
            if !stale {
                if let Some(check) = self.revalidation_due(key, &k, e, now) {
                    // Wait for the check and the refresh it triggers, within `wait`.
                    let depth = self.opts.depth.max(limit);
                    let fresh = tokio::time::timeout(wait, async {
                        match check.await {
                            Ok(Ok(true)) => self.history_flight(key, &k, depth).await.ok(),
                            _ => None,
                        }
                    })
                    .await;
                    if let Ok(Some(v)) = fresh {
                        return HistoryRead::Fresh(self.scoped(&v, limit));
                    }
                }
                return HistoryRead::Cached { records: self.scoped(&e.records, limit), age };
            }
            // Invalidated: refresh, but answer from the stale entry if the store is slow.
            let depth = self.opts.depth.max(limit);
            let flight = self.history_flight(key, &k, depth);
            if let Ok(Ok(fresh)) = tokio::time::timeout(wait, flight).await {
                return HistoryRead::Fresh(self.scoped(&fresh, limit));
            }
            return HistoryRead::Cached { records: self.scoped(&e.records, limit), age };
        }
        let depth = self.opts.depth.max(limit);
        match self.history_flight(key, &k, depth).await {
            Ok(v) => HistoryRead::Fresh(self.scoped(&v, limit)),
            Err(err) => {
                tracing::warn!(error = %err, key = %key, "history read failed");
                match cached {
                    Some((e, at)) if elapsed(now, at) < self.opts.history_ttl => {
                        HistoryRead::Cached { records: self.scoped(&e.records, limit), age: elapsed(now, at) }
                    }
                    _ => HistoryRead::Unavailable,
                }
            }
        }
    }

    /// Start a revalidation of `key` when one is due: claim it (so one check
    /// runs per key per interval) and spawn the store check as its own task.
    /// The task marks the entry stale and calls the change hook when the
    /// store moved past the entry's marker, whether or not anyone waits.
    fn revalidation_due(&self, key: &WorkloadKey, k: &str, e: &CachedRuns, now: Stamp) -> Option<tokio::task::JoinHandle<Result<bool, StoreError>>> {
        let every = self.opts.revalidate?;
        let marker = e.marker.clone()?;
        if elapsed(now, e.checked) < every {
            return None;
        }
        let handle = tokio::runtime::Handle::try_current().ok()?;
        {
            let mut g = self.runs.lock().ok()?;
            let mut claimed = false;
            g.update(&k.to_string(), |c| {
                if elapsed(now, c.checked) >= every {
                    c.checked = now;
                    claimed = true;
                }
            });
            if !claimed {
                return None;
            }
        }
        let (inner, gens, inflight, runs, stale, hook) =
            (self.inner.clone(), self.generations.clone(), self.inflight.clone(), self.runs.clone(), self.stale.clone(), self.on_change.clone());
        let (key, k) = (key.clone(), k.to_string());
        Some(handle.spawn(async move {
            let changed = match tokio::time::timeout(HISTORY_READ_TIMEOUT, inner.runs_changed_since(&key, &marker)).await {
                Ok(r) => r,
                Err(_) => Err(StoreError::Unreachable(format!("revalidation of {key} timed out"))),
            };
            match &changed {
                Ok(true) => {
                    mark_stale(&gens, &inflight, &runs, &stale, &k);
                    if let Some(h) = hook.get() {
                        h(&key);
                    }
                }
                Ok(false) => {}
                Err(err) => tracing::debug!(error = %err, key = %key, "history revalidation failed; the cached entry stands (TTL)"),
            }
            changed
        }))
    }

    /// Breaker state, cached 60 s; `Unknown` (treat as open) when unreadable past that.
    /// Single flight per scope; between one and two TTLs the cached state is
    /// served while one background refresh runs (see the module docs).
    pub async fn breaker(&self, scope: &BreakerScope) -> BreakerRead {
        let now = self.clock.now();
        let cached = self.breakers.lock().ok().and_then(|g| g.get(scope).cloned());
        if let Some((o, at)) = &cached {
            let age = elapsed(now, *at);
            if age < self.opts.control_ttl {
                return BreakerRead::Known(o.clone());
            }
            if age < self.opts.control_ttl * 2 && tokio::runtime::Handle::try_current().is_ok() {
                drop(self.breaker_flight(scope)); // runs as its own task; fills the cache or drops the entry
                return BreakerRead::Known(o.clone());
            }
        }
        match self.breaker_flight(scope).await {
            Ok(o) => BreakerRead::Known(o),
            Err(e) => {
                tracing::warn!(error = %e, %scope, "breaker unreadable: treated as open for reductions");
                BreakerRead::Unknown
            }
        }
    }

    fn bump_control_epoch(&self) {
        self.control_epoch.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    /// The shared read of a breaker scope (joining one in flight, or starting it as its own task).
    fn breaker_flight(&self, scope: &BreakerScope) -> BreakerFlight {
        use futures::FutureExt as _;
        use std::sync::atomic::Ordering;

        let epoch = self.control_epoch.load(Ordering::SeqCst);
        let mut g = match self.breaker_flights.lock() {
            Ok(g) => g,
            Err(_) => {
                let (inner, scope) = (self.inner.clone(), scope.clone());
                return async move { inner.breaker_get(&scope).await.map_err(|e| e.to_string()) }.boxed().shared();
            }
        };
        if let Some((e0, f)) = g.get(scope)
            && *e0 == epoch
        {
            return f.clone();
        }
        let (inner, cache, flights, clock, epochs) =
            (self.inner.clone(), self.breakers.clone(), self.breaker_flights.clone(), self.clock.clone(), self.control_epoch.clone());
        let scope2 = scope.clone();
        let in_runtime = tokio::runtime::Handle::try_current().is_ok();
        let read = async move {
            let r = if in_runtime {
                match tokio::time::timeout(HISTORY_READ_TIMEOUT, inner.breaker_get(&scope2)).await {
                    Ok(r) => r,
                    Err(_) => Err(StoreError::Unreachable(format!("breaker read of {scope2} timed out"))),
                }
            } else {
                inner.breaker_get(&scope2).await
            };
            let current = epochs.load(Ordering::SeqCst) == epoch;
            if let Ok(mut c) = cache.lock() {
                match &r {
                    Ok(o) if current => {
                        c.insert(scope2.clone(), (o.clone(), clock.now()));
                    }
                    Ok(_) => {}
                    // A failed refresh drops a stale entry: past the TTL an unreadable breaker is Unknown.
                    Err(_) => {
                        c.remove(&scope2);
                    }
                }
            }
            if let Ok(mut m) = flights.lock()
                && m.get(&scope2).is_some_and(|(e0, _)| *e0 == epoch)
            {
                m.remove(&scope2);
            }
            r.map_err(|e| e.to_string())
        };
        let f: BreakerFlight = if in_runtime {
            let h = tokio::spawn(read);
            async move { h.await.unwrap_or_else(|e| Err(format!("breaker read task: {e}"))) }.boxed().shared()
        } else {
            read.boxed().shared()
        };
        g.insert(scope.clone(), (epoch, f.clone()));
        f
    }

    /// Control object, cached 60 s; `Unknown` when unreadable past that.
    /// Single flight; stale-while-revalidate as for breakers.
    pub async fn control(&self) -> ControlRead {
        let now = self.clock.now();
        let cached = self.control.lock().ok().and_then(|g| g.clone());
        if let Some((c, at)) = &cached {
            let age = elapsed(now, *at);
            if age < self.opts.control_ttl {
                return ControlRead::Known(c.clone());
            }
            if age < self.opts.control_ttl * 2 && tokio::runtime::Handle::try_current().is_ok() {
                drop(self.control_read());
                return ControlRead::Known(c.clone());
            }
        }
        match self.control_read().await {
            Ok(c) => ControlRead::Known(c),
            Err(e) => {
                tracing::warn!(error = %e, "control object unreadable");
                ControlRead::Unknown
            }
        }
    }

    /// The shared read of the control object.
    fn control_read(&self) -> ControlFlight {
        use futures::FutureExt as _;
        use std::sync::atomic::Ordering;

        let epoch = self.control_epoch.load(Ordering::SeqCst);
        let mut g = match self.control_flight.lock() {
            Ok(g) => g,
            Err(_) => {
                let inner = self.inner.clone();
                return async move { inner.control_get().await.map_err(|e| e.to_string()) }.boxed().shared();
            }
        };
        if let Some((e0, f)) = g.as_ref()
            && *e0 == epoch
        {
            return f.clone();
        }
        let (inner, cache, flight, clock, epochs) =
            (self.inner.clone(), self.control.clone(), self.control_flight.clone(), self.clock.clone(), self.control_epoch.clone());
        let in_runtime = tokio::runtime::Handle::try_current().is_ok();
        let read = async move {
            let r = if in_runtime {
                match tokio::time::timeout(HISTORY_READ_TIMEOUT, inner.control_get()).await {
                    Ok(r) => r,
                    Err(_) => Err(StoreError::Unreachable("control read timed out".into())),
                }
            } else {
                inner.control_get().await
            };
            let current = epochs.load(Ordering::SeqCst) == epoch;
            if let Ok(mut c) = cache.lock() {
                match &r {
                    Ok(v) if current => *c = Some((v.clone(), clock.now())),
                    Ok(_) => {}
                    Err(_) => *c = None,
                }
            }
            if let Ok(mut m) = flight.lock()
                && m.as_ref().is_some_and(|(e0, _)| *e0 == epoch)
            {
                *m = None;
            }
            r.map_err(|e| e.to_string())
        };
        let f: ControlFlight = if in_runtime {
            let h = tokio::spawn(read);
            async move { h.await.unwrap_or_else(|e| Err(format!("control read task: {e}"))) }.boxed().shared()
        } else {
            read.boxed().shared()
        };
        *g = Some((epoch, f.clone()));
        f
    }
}

#[async_trait]
impl HistoryStore for CachedStore {
    fn info(&self) -> StoreInfo {
        self.inner.info()
    }
    fn durable(&self) -> bool {
        self.inner.durable()
    }
    fn shared(&self) -> bool {
        self.inner.shared()
    }
    fn role(&self) -> Role {
        self.inner.role()
    }

    async fn put_run(&self, rec: &RunRecord) -> Result<PutOutcome, StoreError> {
        let r = self.inner.put_run(rec).await?;
        if r == PutOutcome::Inserted {
            // A read in flight may have read the store before this write: keep
            // its result out of the cache (and let the next read start anew),
            // or the cache would hold history without our own record for up
            // to the TTL (e.g. an OOM that must bump the next request).
            self.bump_generation(&rec.key.to_string());
        }
        if r == PutOutcome::Inserted
            && let Ok(mut g) = self.runs.lock()
        {
            // Our own writes are visible to our own reads.
            let depth = self.opts.depth;
            g.update(&rec.key.to_string(), |e| {
                let mut all = std::mem::take(&mut e.records);
                all.push(rec.clone());
                let cap = depth.max(e.fetched);
                e.records = merge_runs(all, cap);
                e.fetched = cap;
            });
        }
        Ok(r)
    }
    async fn put_decision(&self, d: &Decision) -> Result<(), StoreError> {
        self.inner.put_decision(d).await
    }
    async fn runs(&self, key: &WorkloadKey, limit: usize) -> Result<Vec<RunRecord>, StoreError> {
        match self.history(key, limit).await {
            HistoryRead::Fresh(v) | HistoryRead::Cached { records: v, .. } => Ok(v),
            HistoryRead::Unavailable => Err(StoreError::Unreachable(format!("history of {key} unavailable past its cache TTL"))),
        }
    }
    async fn decisions(&self, filter: &DecisionFilter) -> Result<Vec<Decision>, StoreError> {
        self.inner.decisions(filter).await
    }
    async fn decision(&self, id: Ulid) -> Result<Option<Decision>, StoreError> {
        self.inner.decision(id).await
    }
    async fn breaker_get(&self, scope: &BreakerScope) -> Result<BreakerObjects, StoreError> {
        match self.breaker(scope).await {
            BreakerRead::Known(o) => Ok(o),
            BreakerRead::Unknown => Err(StoreError::Unreachable(format!("breaker {scope} unreadable"))),
        }
    }
    async fn breaker_list(&self) -> Result<Vec<(BreakerScope, BreakerObjects)>, StoreError> {
        self.inner.breaker_list().await
    }
    async fn breaker_trip(&self, trip: &Trip) -> Result<(), StoreError> {
        let r = self.inner.breaker_trip(trip).await;
        self.bump_control_epoch();
        if let Ok(mut g) = self.breakers.lock() {
            g.remove(&trip.scope);
        }
        r
    }
    async fn breaker_reset(&self, reset: &Reset) -> Result<(), StoreError> {
        let r = self.inner.breaker_reset(reset).await;
        self.bump_control_epoch();
        if let Ok(mut g) = self.breakers.lock() {
            g.remove(&reset.scope);
        }
        r
    }
    async fn control_get(&self) -> Result<Option<Control>, StoreError> {
        match self.control().await {
            ControlRead::Known(c) => Ok(c),
            ControlRead::Unknown => Err(StoreError::Unreachable("control object unreadable".into())),
        }
    }
    async fn control_set(&self, control: &Control) -> Result<(), StoreError> {
        let r = self.inner.control_set(control).await;
        self.bump_control_epoch();
        if let Ok(mut g) = self.control.lock() {
            *g = None;
        }
        r
    }
    async fn attempt_get(&self, key: &WorkloadKey, invocation_id: &str) -> Result<Option<AttemptRecord>, StoreError> {
        self.inner.attempt_get(key, invocation_id).await
    }
    async fn attempt_put_if_absent(&self, rec: &AttemptRecord) -> Result<bool, StoreError> {
        self.inner.attempt_put_if_absent(rec).await
    }
    async fn attempt_close_lease(&self, key: &WorkloadKey, invocation_id: &str, at: DateTime<Utc>, outcome: RunStatus) -> Result<(), StoreError> {
        self.inner.attempt_close_lease(key, invocation_id, at, outcome).await
    }
    async fn compact(&self, opts: &CompactOptions) -> Result<CompactReport, StoreError> {
        let r = self.inner.compact(opts).await;
        if let Ok(mut g) = self.runs.lock() {
            *g = Lru::new(self.opts.capacity);
        }
        r
    }
    async fn keys(&self) -> Result<Vec<WorkloadKey>, StoreError> {
        self.inner.keys().await
    }
    async fn stats(&self) -> Result<StoreStats, StoreError> {
        self.inner.stats().await
    }
}

fn bump(gens: &Mutex<HashMap<String, u64>>, inflight: &Mutex<HashMap<String, (u64, HistoryFlight)>>, k: &str) {
    if let Ok(mut g) = gens.lock() {
        *g.entry(k.to_string()).or_insert(0) += 1;
    }
    if let Ok(mut g) = inflight.lock() {
        g.remove(k);
    }
}

/// Bump `k`'s generation and mark its cached entry stale (if any).
fn mark_stale(
    gens: &Mutex<HashMap<String, u64>>,
    inflight: &Mutex<HashMap<String, (u64, HistoryFlight)>>,
    runs: &Mutex<Lru<String, CachedRuns>>,
    stale: &Mutex<std::collections::HashSet<String>>,
    k: &str,
) {
    bump(gens, inflight, k);
    let present = runs.lock().map(|g| g.contains(&k.to_string())).unwrap_or(false);
    if present && let Ok(mut st) = stale.lock() {
        st.insert(k.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lru_evicts_least_recent() {
        let mut l: Lru<u32, u32> = Lru::new(2);
        let t = Utc::now();
        l.put(1, 1, t);
        l.put(2, 2, t);
        assert!(l.get(&1).is_some());
        l.put(3, 3, t);
        assert!(l.get(&2).is_none(), "2 was least recently used");
        assert!(l.get(&1).is_some() && l.get(&3).is_some());
        l.remove(&1);
        assert_eq!(l.len(), 1);
    }

    #[test]
    fn cluster_scope() {
        let s = ClusterScope { cluster: Some("a".into()), share: vec!["b".into()] };
        assert!(s.admits(Some("a")) && s.admits(Some("b")) && s.admits(None));
        assert!(!s.admits(Some("c")));
        assert!(ClusterScope::default().admits(Some("c")));
    }
}
