//! Caches (SPEC §6.3 "Cache"), reusable by the engine for every estimator:
//!
//! - [`TtlCache`]: a bounded map with a time-to-live (oldest entries are
//!   evicted when full).
//! - [`Memo`]: the **memo** layer — the one memo of the workspace (the
//!   engine and the remote client both use it). It stores the **normalized
//!   estimate only** — every axis as `Cells` on the grid — (or a clean
//!   decline) under [`MemoKey`] = (workload key, unit, code digest, feature
//!   bucket, policy hash), with a TTL on an injectable clock, per-workload
//!   invalidation and single-flight ([`Memo::get_or_compute`]).
//!   Constraints, S3, breakers and the mode are re-evaluated on every hit; the
//!   memo never stores a decision. The feature bucket
//!   ([`rightsizer_features::feature_bucket`]) puts each input's bytes on the
//!   memory ladder and keeps numeric scalars, so a fan-out over similar inputs
//!   costs a handful of estimates.
//! - [`exact_key`]: the **exact** layer's key, used by the remote client:
//!   (estimator id, version, grid, axes, features, canonical(context with the
//!   history replaced by its watermark)).
//!
//! The memo key adds the unit to the spec's tuple: two units of one
//! submission (a pod's containers) share a workload key but not a demand.

use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use rightsizer_core::clock::{Clock, SystemClock, elapsed};
use tokio::sync::OnceCell;

use rightsizer_core::digest::{canonical_json, history_watermark, sha256_hex};
use rightsizer_core::grid::{Grid, GridAxis};
use rightsizer_core::normalize::{NormalizeError, normalize_axis};
use rightsizer_core::{AxisEstimate, Estimate, WorkloadContext};
use serde::{Deserialize, Serialize};

use crate::EstimateAxis;
use crate::common::cells;

/// The generation slot bumped by [`Memo::clear`] (no workload key looks like this).
const ALL_WORKLOADS: &str = "\u{0}all";

/// A bounded TTL map. Thread-safe; values are cloned out.
pub struct TtlCache<K, V> {
    inner: Mutex<HashMap<K, (Instant, V)>>,
    ttl: Duration,
    capacity: usize,
}

impl<K: Eq + Hash + Clone, V: Clone> TtlCache<K, V> {
    /// A cache holding at most `capacity` entries for `ttl` each.
    pub fn new(ttl: Duration, capacity: usize) -> Self {
        TtlCache { inner: Mutex::new(HashMap::new()), ttl, capacity: capacity.max(1) }
    }

    /// The live value for `k`.
    pub fn get(&self, k: &K) -> Option<V> {
        self.get_at(k, Instant::now())
    }

    /// The value for `k` as of `now` (expired entries are dropped).
    pub fn get_at(&self, k: &K, now: Instant) -> Option<V> {
        let mut m = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        match m.get(k) {
            Some((at, v)) if now.saturating_duration_since(*at) < self.ttl => Some(v.clone()),
            Some(_) => {
                m.remove(k);
                None
            }
            None => None,
        }
    }

    /// Insert, stamped now.
    pub fn insert(&self, k: K, v: V) {
        self.insert_at(k, v, Instant::now())
    }

    /// Insert, stamped `now`. When full, expired entries go first, then the
    /// oldest.
    pub fn insert_at(&self, k: K, v: V, now: Instant) {
        let mut m = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        if m.len() >= self.capacity && !m.contains_key(&k) {
            let ttl = self.ttl;
            m.retain(|_, (at, _)| now.saturating_duration_since(*at) < ttl);
            while m.len() >= self.capacity {
                let Some(oldest) = m.iter().min_by_key(|(_, (at, _))| *at).map(|(k, _)| k.clone()) else { break };
                m.remove(&oldest);
            }
        }
        m.insert(k, (now, v));
    }

    /// Number of entries (live or not yet evicted).
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap_or_else(|p| p.into_inner()).len()
    }

    /// Whether empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drop everything.
    pub fn clear(&self) {
        self.inner.lock().unwrap_or_else(|p| p.into_inner()).clear();
    }
}

/// The memo key (SPEC §6.3).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MemoKey {
    /// Workload key (canonical string).
    pub workload_key: String,
    /// Unit within the submission.
    pub unit: String,
    /// Code digest, if known.
    pub code_digest: Option<String>,
    /// Feature bucket of the inputs.
    pub feature_bucket: String,
    /// Hash of the effective policy (estimator chain, risk, …), supplied by the engine.
    pub policy_hash: String,
}

impl MemoKey {
    /// The key for `ctx` on `grid` under `policy_hash`.
    pub fn new(ctx: &WorkloadContext, grid: &Grid, policy_hash: impl Into<String>) -> Self {
        MemoKey {
            workload_key: ctx.key.to_string(),
            unit: ctx.unit.0.clone(),
            code_digest: ctx.code.as_ref().map(|c| c.digest.clone()),
            feature_bucket: rightsizer_features::feature_bucket(&ctx.inputs, grid),
            policy_hash: policy_hash.into(),
        }
    }
}

/// The estimate with `memory`, `cpu_mean`, `cpu_peak` and `gpu_memory` as
/// `Cells` on `grid` (SPEC §5.5 normalization; `Point` estimates get σ =
/// `point_sigma`). Duration, `t_peak_frac`, novelty and provenance are kept.
pub fn normalized_estimate(est: &Estimate, grid: &Grid, point_sigma: f64) -> Result<Estimate, NormalizeError> {
    let ax = |e: &Option<AxisEstimate>, axis: GridAxis, name: &'static str| -> Result<Option<AxisEstimate>, NormalizeError> {
        e.as_ref().map(|e| normalize_axis(e, grid, axis, point_sigma, name).map(|d| cells(grid, d.probs))).transpose()
    };
    let mut out = est.clone();
    out.memory = ax(&est.memory, GridAxis::Memory, "memory")?;
    out.cpu_mean = ax(&est.cpu_mean, GridAxis::Cpu, "cpu_mean")?;
    out.cpu_peak = ax(&est.cpu_peak, GridAxis::Cpu, "cpu_peak")?;
    out.gpu_memory = ax(&est.gpu_memory, GridAxis::GpuMemory, "gpu_memory")?;
    out.provenance.grid = grid.id.clone();
    Ok(out)
}

/// What one chain (or estimator) call answered, as the memo keeps it: the
/// winning estimate (normalized when stored), the members that did not
/// answer, and whether any of them failed (rather than declined). Answers
/// with `any_error` are shared with concurrent waiters but never stored.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MemoAnswer {
    /// The winning estimate; `None` = every member declined (or failed).
    pub estimate: Option<Estimate>,
    /// `(id, reason)` of members that did not answer.
    pub skipped: Vec<(String, String)>,
    /// Whether any member failed (rather than declined).
    pub any_error: bool,
}

struct MemoEntry {
    answer: MemoAnswer,
    at: DateTime<Utc>,
}

/// The memo layer (SPEC §6.3): normalized estimates — and clean declines —
/// by [`MemoKey`], with a TTL on an injectable clock, bounded capacity
/// (expired entries, then the oldest, are evicted), per-workload
/// invalidation (an outcome changes the key's history) and single-flight:
/// concurrent misses on one key share one computation
/// ([`Memo::get_or_compute`]). Shared by the engine and the remote client.
pub struct Memo {
    entries: Mutex<HashMap<MemoKey, MemoEntry>>,
    inflight: Mutex<HashMap<MemoKey, Arc<OnceCell<MemoAnswer>>>>,
    /// Invalidation generations by workload key (and [`ALL_WORKLOADS`]).
    generations: Mutex<HashMap<String, u64>>,
    grid: Grid,
    point_sigma: f64,
    ttl: Duration,
    capacity: usize,
    clock: Arc<dyn Clock>,
}

impl Memo {
    /// Default TTL (24 h, as the exact cache).
    pub const DEFAULT_TTL: Duration = Duration::from_secs(24 * 3600);

    /// A memo on `grid` holding at most `capacity` entries for `ttl`, on the system clock.
    pub fn new(grid: Grid, ttl: Duration, capacity: usize) -> Self {
        Self::with_clock(grid, ttl, capacity, Arc::new(SystemClock))
    }

    /// A memo whose TTL runs on `clock`.
    pub fn with_clock(grid: Grid, ttl: Duration, capacity: usize, clock: Arc<dyn Clock>) -> Self {
        Memo {
            entries: Mutex::new(HashMap::new()),
            inflight: Mutex::new(HashMap::new()),
            generations: Mutex::new(HashMap::new()),
            grid,
            point_sigma: rightsizer_core::normalize::POINT_SIGMA,
            ttl,
            capacity: capacity.max(1),
            clock,
        }
    }

    /// Normalize `Point` estimates with this σ (the engine's `decision.point_sigma`,
    /// so a memo hit decides exactly like the miss that filled it).
    pub fn with_point_sigma(mut self, sigma: f64) -> Self {
        self.point_sigma = sigma;
        self
    }

    /// The grid estimates are normalized onto.
    pub fn grid(&self) -> &Grid {
        &self.grid
    }

    /// The key for `ctx`.
    pub fn key(&self, ctx: &WorkloadContext, policy_hash: &str) -> MemoKey {
        MemoKey::new(ctx, &self.grid, policy_hash)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<MemoKey, MemoEntry>> {
        self.entries.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// A memoized normalized estimate (`None` also for a memoized decline).
    pub fn get(&self, k: &MemoKey) -> Option<Estimate> {
        self.get_answer(k).and_then(|a| a.estimate)
    }

    /// A live memoized answer (estimate or clean decline).
    pub fn get_answer(&self, k: &MemoKey) -> Option<MemoAnswer> {
        let now = self.clock.now();
        let mut m = self.lock();
        match m.get(k) {
            Some(e) if elapsed(now, e.at) < self.ttl => Some(e.answer.clone()),
            Some(_) => {
                m.remove(k);
                None
            }
            None => None,
        }
    }

    /// Normalize and store `est`; returns the stored (normalized) estimate.
    pub fn put(&self, k: MemoKey, est: &Estimate) -> Result<Estimate, NormalizeError> {
        let n = normalized_estimate(est, &self.grid, self.point_sigma)?;
        self.insert(k, MemoAnswer { estimate: Some(n.clone()), skipped: Vec::new(), any_error: false });
        Ok(n)
    }

    /// Store an answer (its estimate normalized). Answers with `any_error`
    /// are not stored (errors are retried next time); neither is an estimate
    /// that does not normalize. Returns the answer as stored.
    pub fn put_answer(&self, k: MemoKey, answer: MemoAnswer) -> MemoAnswer {
        let generation = self.generation(&k.workload_key);
        self.put_answer_at_generation(k, answer, generation)
    }

    /// [`Memo::put_answer`], stored only if `k`'s workload was not
    /// invalidated since `generation`.
    pub fn put_answer_at_generation(&self, k: MemoKey, mut answer: MemoAnswer, generation: u64) -> MemoAnswer {
        if answer.any_error {
            return answer;
        }
        if let Some(e) = &answer.estimate {
            match normalized_estimate(e, &self.grid, self.point_sigma) {
                Ok(n) => answer.estimate = Some(n),
                Err(err) => {
                    tracing::debug!(error = %err, "estimate not memoized");
                    return answer;
                }
            }
        }
        self.insert_at_generation(k, answer.clone(), generation);
        answer
    }

    fn insert(&self, k: MemoKey, answer: MemoAnswer) {
        let generation = self.generation(&k.workload_key);
        self.insert_at_generation(k, answer, generation)
    }

    fn insert_at_generation(&self, k: MemoKey, answer: MemoAnswer, generation: u64) {
        let now = self.clock.now();
        let mut m = self.lock();
        // Checked under the entries lock, which invalidation also takes
        // after bumping: either the bump is seen here, or the entry inserted
        // here is removed by the invalidation's retain.
        if self.generation(&k.workload_key) != generation {
            return;
        }
        if m.len() >= self.capacity && !m.contains_key(&k) {
            let ttl = self.ttl;
            m.retain(|_, e| elapsed(now, e.at) < ttl);
            while m.len() >= self.capacity {
                let Some(oldest) = m.iter().min_by_key(|(_, e)| e.at).map(|(k, _)| k.clone()) else { break };
                m.remove(&oldest);
            }
        }
        m.insert(k, MemoEntry { answer, at: now });
    }

    /// The memoized answer for `k`, or `compute()` — run once for all
    /// concurrent misses on `k` (single-flight) and stored unless it has
    /// `any_error`. Returns the answer and whether it was a memo hit.
    pub async fn get_or_compute<F, Fut>(&self, k: &MemoKey, compute: F) -> (MemoAnswer, bool)
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = MemoAnswer>,
    {
        if let Some(hit) = self.get_answer(k) {
            return (hit, true);
        }
        let cell = {
            let mut g = self.inflight.lock().unwrap_or_else(|p| p.into_inner());
            g.entry(k.clone()).or_insert_with(|| Arc::new(OnceCell::new())).clone()
        };
        let mut ran = false;
        let generation = self.generation(&k.workload_key);
        let answer = cell
            .get_or_init(|| {
                ran = true;
                async move { self.put_answer_at_generation(k.clone(), compute().await, generation) }
            })
            .await
            .clone();
        if ran {
            let mut g = self.inflight.lock().unwrap_or_else(|p| p.into_inner());
            if g.get(k).is_some_and(|c| Arc::ptr_eq(c, &cell)) {
                g.remove(k);
            }
        }
        (answer, false)
    }

    /// The invalidation generation of a workload key: bumped by
    /// [`Memo::invalidate_workload`] and [`Memo::clear`]. A caller that
    /// captures it before computing stores with [`Memo::put_at_generation`],
    /// so an estimate computed from history older than an invalidation is
    /// never stored.
    pub fn generation(&self, workload_key: &str) -> u64 {
        let g = self.generations.lock().unwrap_or_else(|p| p.into_inner());
        g.get(workload_key).copied().unwrap_or(0).wrapping_add(g.get(ALL_WORKLOADS).copied().unwrap_or(0))
    }

    /// [`Memo::put`] unless `k`'s workload was invalidated since `generation`
    /// was read (then normalized and returned, but not stored).
    pub fn put_at_generation(&self, k: MemoKey, est: &Estimate, generation: u64) -> Result<Estimate, NormalizeError> {
        let n = normalized_estimate(est, &self.grid, self.point_sigma)?;
        self.insert_at_generation(k, MemoAnswer { estimate: Some(n.clone()), skipped: Vec::new(), any_error: false }, generation);
        Ok(n)
    }

    fn bump(&self, workload_key: &str) {
        let mut g = self.generations.lock().unwrap_or_else(|p| p.into_inner());
        let e = g.entry(workload_key.to_string()).or_insert(0);
        *e = e.wrapping_add(1);
    }

    /// Drop every entry of a workload key (any unit), e.g. after an outcome.
    /// Computations in flight for it are not joined by later callers and
    /// their answers are not stored.
    pub fn invalidate_workload(&self, workload_key: &str) {
        self.bump(workload_key);
        self.inflight.lock().unwrap_or_else(|p| p.into_inner()).retain(|k, _| k.workload_key != workload_key);
        self.lock().retain(|k, _| k.workload_key != workload_key);
    }

    /// Drop everything.
    pub fn clear(&self) {
        self.bump(ALL_WORKLOADS);
        self.inflight.lock().unwrap_or_else(|p| p.into_inner()).clear();
        self.lock().clear();
    }

    /// Entries held.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The exact-cache key (SPEC §6.3): sha256 of canonical JSON of (id,
/// version, grid, axes, features, context with `history` replaced by the
/// history watermark).
pub fn exact_key(id: &str, version: &str, grid: &str, axes: &[EstimateAxis], features: &str, ctx: &WorkloadContext) -> String {
    let mut c = serde_json::to_value(ctx).unwrap_or(serde_json::Value::Null);
    if let Some(o) = c.as_object_mut() {
        o.insert("history".into(), serde_json::Value::String(history_watermark(&ctx.history)));
    }
    let mut axes = axes.to_vec();
    axes.sort();
    let v = serde_json::json!({"id": id, "version": version, "grid": grid, "axes": axes, "features": features, "context": c});
    sha256_hex(&canonical_json(&v))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::*;
    use rightsizer_core::*;

    #[test]
    fn ttl_cache_expires_and_evicts() {
        let c: TtlCache<u32, u32> = TtlCache::new(Duration::from_secs(10), 2);
        let t0 = Instant::now();
        c.insert_at(1, 10, t0);
        c.insert_at(2, 20, t0 + Duration::from_secs(1));
        assert_eq!(c.get_at(&1, t0 + Duration::from_secs(5)), Some(10));
        assert_eq!(c.get_at(&1, t0 + Duration::from_secs(11)), None, "expired");
        c.insert_at(1, 11, t0 + Duration::from_secs(2));
        c.insert_at(3, 30, t0 + Duration::from_secs(3));
        assert_eq!(c.len(), 2);
        assert_eq!(c.get_at(&2, t0 + Duration::from_secs(3)), None, "oldest evicted");
        assert_eq!(c.get_at(&3, t0 + Duration::from_secs(3)), Some(30));
    }

    #[test]
    fn memo_key_buckets_inputs_and_normalizes() {
        let g = Grid::rs_grid_v1();
        let a = ctx_with(vec![InputMeta::blob("x", 1_000_000_000)], vec![]);
        let b = ctx_with(vec![InputMeta::blob("x", 1_010_000_000)], vec![record("r", RunStatus::Succeeded, None, None)]);
        assert_eq!(MemoKey::new(&a, &g, "p"), MemoKey::new(&b, &g, "p"), "same bucket, history ignored");
        assert_ne!(MemoKey::new(&a, &g, "p"), MemoKey::new(&a, &g, "q"));
        let memo = Memo::new(g.clone(), Memo::DEFAULT_TTL, 100);
        let mut e = Estimate::empty(Provenance { estimator: "x".into(), version: "1".into(), ..Default::default() });
        e.memory = Some(AxisEstimate::Point { value: gib(3.0) });
        e.duration_s = Some(AxisEstimate::Point { value: 60.0 });
        let k = memo.key(&a, "p");
        let stored = memo.put(k.clone(), &e).unwrap();
        let Some(AxisEstimate::Cells { grid, probs }) = &stored.memory else { panic!() };
        assert_eq!(grid, "rs-grid-v1");
        assert!((probs.iter().sum::<f32>() - 1.0).abs() < 1e-6);
        assert_eq!(stored.duration_s, e.duration_s);
        assert_eq!(memo.get(&k), Some(stored));
    }

    /// An outcome (invalidation) that lands while an estimate computed from
    /// the older history is in flight must not let that stale estimate be
    /// stored, or every later decision reuses pre-outcome (e.g. pre-OOM)
    /// sizing until the TTL.
    #[tokio::test]
    async fn invalidation_during_compute_keeps_the_stale_answer_out() {
        let g = Grid::rs_grid_v1();
        let memo = Arc::new(Memo::new(g.clone(), Memo::DEFAULT_TTL, 100));
        let ctx = ctx_with(vec![InputMeta::blob("x", 1_000_000_000)], vec![]);
        let k = memo.key(&ctx, "p");
        let mut e = Estimate::empty(Provenance { estimator: "x".into(), version: "1".into(), ..Default::default() });
        e.memory = Some(AxisEstimate::Point { value: gib(1.0) });
        let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
        let (go_tx, go_rx) = tokio::sync::oneshot::channel::<()>();
        let m2 = memo.clone();
        let (k2, e2) = (k.clone(), e.clone());
        let h = tokio::spawn(async move {
            m2.get_or_compute(&k2, || async move {
                let _ = started_tx.send(());
                let _ = go_rx.await;
                MemoAnswer { estimate: Some(e2), skipped: vec![], any_error: false }
            })
            .await
        });
        started_rx.await.unwrap();
        memo.invalidate_workload(&k.workload_key);
        go_tx.send(()).unwrap();
        let (answer, hit) = h.await.unwrap();
        assert!(!hit && answer.estimate.is_some(), "the caller still gets its answer");
        assert!(memo.get_answer(&k).is_none(), "stale answer stored after invalidation");
        // Remote-style put with a captured generation.
        let gen0 = memo.generation(&k.workload_key);
        memo.invalidate_workload(&k.workload_key);
        memo.put_at_generation(k.clone(), &e, gen0).unwrap();
        assert!(memo.get_answer(&k).is_none());
        memo.put_at_generation(k.clone(), &e, memo.generation(&k.workload_key)).unwrap();
        assert!(memo.get_answer(&k).is_some());
    }

    #[test]
    fn exact_key_uses_history_watermark() {
        let r1 = record("r1", RunStatus::Succeeded, None, None);
        let r2 = record("r2", RunStatus::Succeeded, None, None);
        let a = ctx_with(vec![], vec![r1.clone(), r2.clone()]);
        let b = ctx_with(vec![], vec![r2, r1]);
        let k = |c: &WorkloadContext| exact_key("x", "1", "rs-grid-v1", &[EstimateAxis::Memory], "features-v1", c);
        assert_eq!(k(&a), k(&b), "order-insensitive watermark");
        let c = ctx_with(vec![], vec![]);
        assert_ne!(k(&a), k(&c));
        assert_ne!(k(&c), exact_key("x", "2", "rs-grid-v1", &[EstimateAxis::Memory], "features-v1", &c));
    }
}
