//! Caches (SPEC §6.3 "Cache"), reusable by the engine for every estimator:
//!
//! - [`TtlCache`]: a bounded map with a time-to-live (oldest entries are
//!   evicted when full).
//! - [`Memo`]: the **memo** layer. It stores the **normalized estimate
//!   only** — every axis as `Cells` on the grid — under [`MemoKey`] =
//!   (workload key, unit, code digest, feature bucket, policy hash).
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
use std::hash::Hash;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use rightsizer_core::digest::{canonical_json, history_watermark, sha256_hex};
use rightsizer_core::grid::{Grid, GridAxis};
use rightsizer_core::normalize::{NormalizeError, normalize_axis};
use rightsizer_core::{AxisEstimate, Estimate, WorkloadContext};
use serde::{Deserialize, Serialize};

use crate::EstimateAxis;
use crate::common::cells;

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

/// The memo layer: normalized estimates by [`MemoKey`].
pub struct Memo {
    cache: TtlCache<MemoKey, Estimate>,
    grid: Grid,
    point_sigma: f64,
}

impl Memo {
    /// Default TTL (24 h, as the exact cache).
    pub const DEFAULT_TTL: Duration = Duration::from_secs(24 * 3600);

    /// A memo on `grid` holding at most `capacity` entries for `ttl`.
    pub fn new(grid: Grid, ttl: Duration, capacity: usize) -> Self {
        Memo { cache: TtlCache::new(ttl, capacity), grid, point_sigma: rightsizer_core::normalize::POINT_SIGMA }
    }

    /// The grid estimates are normalized onto.
    pub fn grid(&self) -> &Grid {
        &self.grid
    }

    /// The key for `ctx`.
    pub fn key(&self, ctx: &WorkloadContext, policy_hash: &str) -> MemoKey {
        MemoKey::new(ctx, &self.grid, policy_hash)
    }

    /// A memoized normalized estimate.
    pub fn get(&self, k: &MemoKey) -> Option<Estimate> {
        self.cache.get(k)
    }

    /// Normalize and store `est`; returns the stored (normalized) estimate.
    pub fn put(&self, k: MemoKey, est: &Estimate) -> Result<Estimate, NormalizeError> {
        let n = normalized_estimate(est, &self.grid, self.point_sigma)?;
        self.cache.insert(k, n.clone());
        Ok(n)
    }

    /// Entries held.
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Whether empty.
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
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
