//! SPEC §8.4: the per-key history cache with TTL, breaker/control caching
//! and unreadable semantics, cluster scoping (§8.5), and the bounded,
//! retrying write buffer.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use common::*;
use rightsizer_core::breaker::{BreakerObjects, BreakerScope, Control, Reset, Trip};
use rightsizer_core::{AttemptRecord, Decision, Mode, RunRecord, RunStatus, WorkloadKey};
use rightsizer_history::{
    BreakerRead, BufferOptions, BufferedWriter, CacheOptions, CachedStore, ClusterScope, CompactOptions, CompactReport, ControlRead,
    DecisionFilter, HistoryRead, HistoryStore, MemoryStore, PutOutcome, StoreError, StoreInfo, WriteOutcome,
};
use ulid::Ulid;

/// A store that fails every call while `down` is set, and counts reads.
struct Flaky {
    inner: MemoryStore,
    down: AtomicBool,
    reject: AtomicBool,
    reads: AtomicUsize,
}

impl Flaky {
    fn new() -> Arc<Self> {
        Arc::new(Flaky { inner: MemoryStore::new(), down: AtomicBool::new(false), reject: AtomicBool::new(false), reads: AtomicUsize::new(0) })
    }
    fn set_down(&self, d: bool) {
        self.down.store(d, Ordering::SeqCst);
    }
    fn check(&self) -> Result<(), StoreError> {
        if self.reject.load(Ordering::SeqCst) {
            return Err(StoreError::PermissionDenied("nope".into()));
        }
        if self.down.load(Ordering::SeqCst) { Err(StoreError::Unreachable("down".into())) } else { Ok(()) }
    }
}

#[async_trait]
impl HistoryStore for Flaky {
    fn info(&self) -> StoreInfo {
        self.inner.info()
    }
    async fn put_run(&self, rec: &RunRecord) -> Result<PutOutcome, StoreError> {
        self.check()?;
        self.inner.put_run(rec).await
    }
    async fn put_decision(&self, d: &Decision) -> Result<(), StoreError> {
        self.check()?;
        self.inner.put_decision(d).await
    }
    async fn runs(&self, key: &WorkloadKey, limit: usize) -> Result<Vec<RunRecord>, StoreError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.check()?;
        self.inner.runs(key, limit).await
    }
    async fn decisions(&self, f: &DecisionFilter) -> Result<Vec<Decision>, StoreError> {
        self.check()?;
        self.inner.decisions(f).await
    }
    async fn decision(&self, id: Ulid) -> Result<Option<Decision>, StoreError> {
        self.check()?;
        self.inner.decision(id).await
    }
    async fn breaker_get(&self, s: &BreakerScope) -> Result<BreakerObjects, StoreError> {
        self.check()?;
        self.inner.breaker_get(s).await
    }
    async fn breaker_list(&self) -> Result<Vec<(BreakerScope, BreakerObjects)>, StoreError> {
        self.check()?;
        self.inner.breaker_list().await
    }
    async fn breaker_trip(&self, t: &Trip) -> Result<(), StoreError> {
        self.check()?;
        self.inner.breaker_trip(t).await
    }
    async fn breaker_reset(&self, r: &Reset) -> Result<(), StoreError> {
        self.check()?;
        self.inner.breaker_reset(r).await
    }
    async fn control_get(&self) -> Result<Option<Control>, StoreError> {
        self.check()?;
        self.inner.control_get().await
    }
    async fn control_set(&self, c: &Control) -> Result<(), StoreError> {
        self.check()?;
        self.inner.control_set(c).await
    }
    async fn attempt_get(&self, k: &WorkloadKey, i: &str) -> Result<Option<AttemptRecord>, StoreError> {
        self.check()?;
        self.inner.attempt_get(k, i).await
    }
    async fn attempt_put_if_absent(&self, r: &AttemptRecord) -> Result<bool, StoreError> {
        self.check()?;
        self.inner.attempt_put_if_absent(r).await
    }
    async fn attempt_close_lease(&self, k: &WorkloadKey, i: &str, at: DateTime<Utc>, o: RunStatus) -> Result<(), StoreError> {
        self.check()?;
        self.inner.attempt_close_lease(k, i, at, o).await
    }
    async fn compact(&self, o: &CompactOptions) -> Result<CompactReport, StoreError> {
        self.check()?;
        self.inner.compact(o).await
    }
}

#[tokio::test(start_paused = true)]
async fn history_cache_ttl_and_unreachable() {
    let inner = Flaky::new();
    let k = k1();
    inner.put_run(&rec(&k, "a", 1, t(0))).await.unwrap();
    let c = CachedStore::new(inner.clone(), CacheOptions::default());
    assert!(matches!(c.history(&k, 10).await, HistoryRead::Fresh(v) if v.len() == 1));
    // Within the TTL: served from cache, store not touched.
    tokio::time::advance(Duration::from_secs(300)).await;
    assert!(matches!(c.history(&k, 10).await, HistoryRead::Cached { records, .. } if records.len() == 1));
    assert_eq!(inner.reads.load(Ordering::SeqCst), 1);
    // Store down, still within the TTL: cached history is used.
    inner.set_down(true);
    assert!(c.history(&k, 10).await.records().is_some());
    // Past the TTL: unavailable (the engine demotes enforce).
    tokio::time::advance(Duration::from_secs(301)).await;
    assert_eq!(c.history(&k, 10).await, HistoryRead::Unavailable);
    // A key never cached is unavailable while the store is down.
    assert!(c.history(&k2(), 10).await.is_unavailable());
    assert!(matches!(c.runs(&k, 10).await, Err(StoreError::Unreachable(_))), "trait view: unreachable");
    // Back up: fresh again.
    inner.set_down(false);
    assert!(matches!(c.history(&k, 10).await, HistoryRead::Fresh(_)));
    // Own writes are visible without waiting for the TTL.
    c.put_run(&rec(&k, "b", 1, t(10))).await.unwrap();
    assert_eq!(invs(c.history(&k, 10).await.records().unwrap()), ["b#1", "a#1"]);
    // Asking for more than was fetched refetches.
    let before = inner.reads.load(Ordering::SeqCst);
    let c2 = CachedStore::new(inner.clone(), CacheOptions { depth: 1, ..Default::default() });
    c2.history(&k, 1).await;
    c2.history(&k, 2).await;
    assert_eq!(inner.reads.load(Ordering::SeqCst), before + 2);
}

#[tokio::test(start_paused = true)]
async fn lru_capacity_bounds_keys() {
    let inner = Flaky::new();
    let c = CachedStore::new(inner.clone(), CacheOptions { capacity: 2, ..Default::default() });
    for k in [k1(), k2(), key("ray://t/c/m.f")] {
        c.history(&k, 5).await;
    }
    assert_eq!(c.cached_keys(), 2);
}

#[tokio::test(start_paused = true)]
async fn breaker_and_control_cache_60s_then_unknown() {
    let inner = Flaky::new();
    let scope = BreakerScope::Workload { key: k1() };
    let c = CachedStore::new(inner.clone(), CacheOptions::default());
    assert_eq!(c.breaker(&scope).await, BreakerRead::Known(BreakerObjects::default()));
    assert!(!c.breaker(&scope).await.blocks_reductions());
    assert_eq!(c.control().await, ControlRead::Known(None));
    inner.inner.control_set(&control(Mode::Recommend)).await.unwrap();
    inner.inner.breaker_trip(&trip(scope.clone(), t(1))).await.unwrap();
    // Cached for 60 s.
    tokio::time::advance(Duration::from_secs(30)).await;
    assert_eq!(c.control().await, ControlRead::Known(None));
    assert!(!c.breaker(&scope).await.blocks_reductions());
    tokio::time::advance(Duration::from_secs(31)).await;
    assert!(matches!(c.control().await, ControlRead::Known(Some(x)) if x.mode_ceiling == Mode::Recommend));
    assert!(c.breaker(&scope).await.blocks_reductions(), "open");
    // Unreadable past the cache: unknown, which blocks reductions.
    inner.set_down(true);
    tokio::time::advance(Duration::from_secs(61)).await;
    assert_eq!(c.breaker(&scope).await, BreakerRead::Unknown);
    assert!(BreakerRead::Unknown.blocks_reductions());
    assert_eq!(c.control().await, ControlRead::Unknown);
    assert!(c.breaker_get(&scope).await.is_err());
    // A trip through the cache invalidates its scope.
    inner.set_down(false);
    let other = BreakerScope::Fleet;
    assert!(!c.breaker(&other).await.blocks_reductions());
    c.breaker_trip(&trip(other.clone(), t(2))).await.unwrap();
    assert!(c.breaker(&other).await.blocks_reductions());
}

#[tokio::test]
async fn cluster_scope_filters_history() {
    let inner = Flaky::new();
    let k = k1();
    for (i, cl) in [Some("a"), Some("b"), Some("c"), None].iter().enumerate() {
        let mut r = rec(&k, &format!("i{i}"), 1, t(i as i64));
        r.cluster = cl.map(str::to_string);
        inner.put_run(&r).await.unwrap();
    }
    let opts = CacheOptions { scope: ClusterScope { cluster: Some("a".into()), share: vec![] }, ..Default::default() };
    let c = CachedStore::new(inner.clone(), opts);
    assert_eq!(invs(c.history(&k, 10).await.records().unwrap()), ["i3#1", "i0#1"], "own cluster + unlabelled");
    let opts = CacheOptions { scope: ClusterScope { cluster: Some("a".into()), share: vec!["b".into()] }, ..Default::default() };
    let c = CachedStore::new(inner.clone(), opts);
    assert_eq!(invs(c.history(&k, 10).await.records().unwrap()), ["i3#1", "i1#1", "i0#1"], "history.share merges b");
}

#[tokio::test]
async fn buffer_overflow_is_dropped_and_counted() {
    let inner = Flaky::new();
    let w = BufferedWriter::new(inner.clone(), BufferOptions { capacity: 3, ..Default::default() });
    let k = k1();
    assert_eq!(w.put_run(rec(&k, "ok", 1, t(0))).await, WriteOutcome::Written(PutOutcome::Inserted));
    inner.set_down(true);
    let mut outcomes = Vec::new();
    for i in 0..5 {
        outcomes.push(w.put_run(rec(&k, &format!("b{i}"), 1, t(i))).await);
    }
    assert_eq!(outcomes.iter().filter(|o| **o == WriteOutcome::Buffered).count(), 3);
    assert_eq!(outcomes.iter().filter(|o| **o == WriteOutcome::Dropped).count(), 2);
    assert_eq!(w.put_decision(decision(&k, Utc::now(), Mode::Enforce, "gbt")).await, WriteOutcome::Dropped);
    let st = w.stats();
    assert_eq!((st.buffered, st.dropped, st.written), (3, 3, 1));
    // Still down: flush stops at the first failure.
    assert_eq!(w.flush().await, 0);
    inner.set_down(false);
    assert_eq!(w.flush().await, 3);
    let st = w.stats();
    assert_eq!((st.buffered, st.retried, st.written), (0, 3, 4));
    assert_eq!(inner.inner.runs(&k, 10).await.unwrap().len(), 4, "order kept, nothing lost but the overflow");
    // Permanent rejections are dropped and counted, never retried.
    inner.reject.store(true, Ordering::SeqCst);
    assert_eq!(w.put_run(rec(&k, "x", 1, t(9))).await, WriteOutcome::Dropped);
    assert_eq!(w.stats().rejected, 1);
}

#[tokio::test(start_paused = true)]
async fn buffer_retries_in_the_background_with_backoff() {
    let inner = Flaky::new();
    let w = BufferedWriter::new(inner.clone(), BufferOptions { capacity: 100, initial_backoff: Duration::from_millis(100), max_backoff: Duration::from_secs(2) });
    let h = w.spawn_retry();
    inner.set_down(true);
    for i in 0..10 {
        assert_eq!(w.put_run(rec(&k1(), &format!("r{i}"), 1, t(i))).await, WriteOutcome::Buffered);
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(w.pending().await, 10);
    assert!(w.stats().errors >= 4 && w.stats().errors <= 12, "retried with exponential backoff: {:?}", w.stats());
    inner.set_down(false);
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(w.pending().await, 0);
    assert_eq!(inner.inner.runs(&k1(), 100).await.unwrap().len(), 10);
    h.abort();
}

#[tokio::test]
async fn cache_ttls_run_on_an_injected_clock() {
    use rightsizer_core::clock::ManualClock;
    let inner = Flaky::new();
    let k = k1();
    inner.put_run(&rec(&k, "a", 1, t(0))).await.unwrap();
    let clock = Arc::new(ManualClock::new(t(0)));
    let c = CachedStore::with_clock(inner.clone(), CacheOptions::default(), clock.clone());
    assert!(matches!(c.history(&k, 10).await, HistoryRead::Fresh(_)));
    assert!(matches!(c.breaker(&BreakerScope::Fleet).await, BreakerRead::Known(_)));
    inner.set_down(true);
    // Real time does not matter; only the injected clock does.
    clock.advance(Duration::from_secs(59));
    assert!(matches!(c.history(&k, 10).await, HistoryRead::Cached { age, .. } if age == Duration::from_secs(59)));
    assert!(matches!(c.breaker(&BreakerScope::Fleet).await, BreakerRead::Known(_)));
    clock.advance(Duration::from_secs(2));
    assert_eq!(c.breaker(&BreakerScope::Fleet).await, BreakerRead::Unknown, "control TTL (60 s) passed");
    assert!(c.history(&k, 10).await.records().is_some(), "history TTL (10 min) not yet");
    clock.advance(Duration::from_secs(600));
    assert!(c.history(&k, 10).await.is_unavailable());
    // invalidate_control keeps history cached.
    inner.set_down(false);
    c.history(&k, 10).await;
    c.invalidate_control();
    assert_eq!(c.cached_keys(), 1);
}

#[tokio::test]
async fn deferred_writes_are_queued_untried() {
    let inner = Flaky::new();
    let w = BufferedWriter::new(inner.clone(), BufferOptions { capacity: 2, ..Default::default() });
    let k = k1();
    assert_eq!(w.defer_run(rec(&k, "a", 1, t(0))).await, WriteOutcome::Buffered);
    assert_eq!(w.defer_decision_now(decision(&k, Utc::now(), Mode::Enforce, "gbt")), WriteOutcome::Buffered);
    assert_eq!(w.buffered(), 2);
    assert_eq!(w.defer_run(rec(&k, "b", 1, t(1))).await, WriteOutcome::Dropped, "full");
    assert_eq!(inner.inner.runs(&k, 10).await.unwrap().len(), 0, "not tried");
    assert_eq!(w.flush().await, 2);
    assert_eq!(w.buffered(), 0);
    assert_eq!(inner.inner.runs(&k, 10).await.unwrap().len(), 1);
}
