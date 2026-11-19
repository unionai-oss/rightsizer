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
//!   the breaker as **open for reductions**.
//! - **Cluster scope (SPEC §8.5):** history is per cluster unless
//!   `history.share` merges clusters. Records of other clusters are dropped
//!   from reads; records without a cluster are kept.
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
use rightsizer_core::{AttemptRecord, Decision, RunRecord, RunStatus, WorkloadKey};
use tokio::time::Instant;
use ulid::Ulid;

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
}

impl Default for CacheOptions {
    fn default() -> Self {
        CacheOptions { history_ttl: Duration::from_secs(600), control_ttl: Duration::from_secs(60), capacity: 10_000, depth: 64, scope: ClusterScope::default() }
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
    at: Instant,
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
    fn get(&mut self, k: &K) -> Option<(V, Instant)> {
        self.tick += 1;
        let t = self.tick;
        self.map.get_mut(k).map(|e| {
            e.used = t;
            (e.value.clone(), e.at)
        })
    }
    fn put(&mut self, k: K, v: V, at: Instant) {
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
    fn remove(&mut self, k: &K) {
        self.map.remove(k);
    }
    fn len(&self) -> usize {
        self.map.len()
    }
}

/// A caching wrapper around any store.
pub struct CachedStore {
    inner: Arc<dyn HistoryStore>,
    opts: CacheOptions,
    runs: Mutex<Lru<String, (Vec<RunRecord>, usize)>>,
    breakers: Mutex<HashMap<BreakerScope, (BreakerObjects, Instant)>>,
    control: Mutex<Option<(Option<Control>, Instant)>>,
}

impl CachedStore {
    /// Wrap `inner`.
    pub fn new(inner: Arc<dyn HistoryStore>, opts: CacheOptions) -> Self {
        CachedStore {
            runs: Mutex::new(Lru::new(opts.capacity)),
            inner,
            opts,
            breakers: Mutex::new(HashMap::new()),
            control: Mutex::new(None),
        }
    }

    /// The wrapped store.
    pub fn inner(&self) -> &Arc<dyn HistoryStore> {
        &self.inner
    }

    /// Keys currently cached.
    pub fn cached_keys(&self) -> usize {
        self.runs.lock().map(|g| g.len()).unwrap_or(0)
    }

    /// Drop one key's cached history.
    pub fn invalidate_key(&self, key: &WorkloadKey) {
        if let Ok(mut g) = self.runs.lock() {
            g.remove(&key.to_string());
        }
    }

    /// Drop everything cached.
    pub fn invalidate(&self) {
        if let Ok(mut g) = self.runs.lock() {
            *g = Lru::new(self.opts.capacity);
        }
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

    /// History of `key` under the SPEC §8.4 rules.
    pub async fn history(&self, key: &WorkloadKey, limit: usize) -> HistoryRead {
        let k = key.to_string();
        let cached = self.runs.lock().ok().and_then(|mut g| g.get(&k));
        let now = Instant::now();
        // A cached entry answers `limit` if it holds that many records, or
        // if its fetch returned fewer than it asked for (nothing more exists).
        if let Some(((v, fetched), at)) = &cached
            && now.duration_since(*at) < self.opts.history_ttl
            && (v.len() >= limit || v.len() < *fetched)
        {
            return HistoryRead::Cached { records: self.scoped(v, limit), age: now.duration_since(*at) };
        }
        let depth = self.opts.depth.max(limit);
        match self.inner.runs(key, depth).await {
            Ok(v) => {
                if let Ok(mut g) = self.runs.lock() {
                    g.put(k, (v.clone(), depth), Instant::now());
                }
                HistoryRead::Fresh(self.scoped(&v, limit))
            }
            Err(e) => {
                tracing::warn!(error = %e, key = %key, "history read failed");
                match cached {
                    Some(((v, _), at)) if now.duration_since(at) < self.opts.history_ttl => {
                        HistoryRead::Cached { records: self.scoped(&v, limit), age: now.duration_since(at) }
                    }
                    _ => HistoryRead::Unavailable,
                }
            }
        }
    }

    /// Breaker state, cached 60 s; `Unknown` (treat as open) when unreadable past that.
    pub async fn breaker(&self, scope: &BreakerScope) -> BreakerRead {
        let now = Instant::now();
        let cached = self.breakers.lock().ok().and_then(|g| g.get(scope).cloned());
        if let Some((o, at)) = &cached
            && now.duration_since(*at) < self.opts.control_ttl
        {
            return BreakerRead::Known(o.clone());
        }
        match self.inner.breaker_get(scope).await {
            Ok(o) => {
                if let Ok(mut g) = self.breakers.lock() {
                    g.insert(scope.clone(), (o.clone(), Instant::now()));
                }
                BreakerRead::Known(o)
            }
            Err(e) => {
                tracing::warn!(error = %e, %scope, "breaker unreadable: treated as open for reductions");
                BreakerRead::Unknown
            }
        }
    }

    /// Control object, cached 60 s; `Unknown` when unreadable past that.
    pub async fn control(&self) -> ControlRead {
        let now = Instant::now();
        let cached = self.control.lock().ok().and_then(|g| g.clone());
        if let Some((c, at)) = &cached
            && now.duration_since(*at) < self.opts.control_ttl
        {
            return ControlRead::Known(c.clone());
        }
        match self.inner.control_get().await {
            Ok(c) => {
                if let Ok(mut g) = self.control.lock() {
                    *g = Some((c.clone(), Instant::now()));
                }
                ControlRead::Known(c)
            }
            Err(e) => {
                tracing::warn!(error = %e, "control object unreadable");
                ControlRead::Unknown
            }
        }
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
        if r == PutOutcome::Inserted
            && let Ok(mut g) = self.runs.lock()
        {
            // Our own writes are visible to our own reads.
            let depth = self.opts.depth;
            g.update(&rec.key.to_string(), |(v, fetched)| {
                let mut all = std::mem::take(v);
                all.push(rec.clone());
                let cap = depth.max(*fetched);
                *v = merge_runs(all, cap);
                *fetched = cap;
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
        if let Ok(mut g) = self.breakers.lock() {
            g.remove(&trip.scope);
        }
        r
    }
    async fn breaker_reset(&self, reset: &Reset) -> Result<(), StoreError> {
        let r = self.inner.breaker_reset(reset).await;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lru_evicts_least_recent() {
        let mut l: Lru<u32, u32> = Lru::new(2);
        let t = Instant::now();
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
