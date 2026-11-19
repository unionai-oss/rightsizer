//! The memo and single-flight (SPEC §6.3).
//!
//! The memo stores the chain's **estimate only**; constraints, S3, breakers
//! and the mode are re-evaluated on every hit. Key = (workload key, code
//! digest, feature bucket, policy hash). An outcome for a key invalidates
//! every entry of that key (its history changed). Concurrent misses on one
//! memo key share one chain call (single-flight).
//!
//! TODO(rightsizer-estimators memo module): when the estimators crate ships
//! its memo, replace [`Memo`] with it; the key and invalidation contract here
//! are what the engine relies on.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use rightsizer_core::Estimate;
use tokio::sync::OnceCell;

use crate::clock::since;

/// A memo key (SPEC §6.3).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MemoKey {
    /// Canonical workload key (+ unit).
    pub key: String,
    /// Code digest (`""` when unknown).
    pub code_digest: String,
    /// Feature bucket ([`rightsizer_features::feature_bucket`]).
    pub bucket: String,
    /// Hash of everything that shapes the estimate: chain members and versions, grid.
    pub policy_hash: String,
}

/// What the chain answered for one context.
#[derive(Clone, Debug)]
pub struct ChainResult {
    /// The winning estimate.
    pub estimate: Option<Estimate>,
    /// `(id, reason)` of members that did not answer.
    pub skipped: Vec<(String, String)>,
    /// Whether any member failed (rather than declined).
    pub any_error: bool,
}

#[derive(Clone)]
struct Entry {
    result: ChainResult,
    at: DateTime<Utc>,
}

/// The memo: bounded, with TTL.
pub struct Memo {
    entries: Mutex<HashMap<MemoKey, Entry>>,
    inflight: Mutex<HashMap<MemoKey, Arc<OnceCell<ChainResult>>>>,
    ttl: std::time::Duration,
    capacity: usize,
}

impl Memo {
    /// A memo holding at most `capacity` entries for `ttl`.
    pub fn new(ttl: std::time::Duration, capacity: usize) -> Self {
        Memo { entries: Mutex::new(HashMap::new()), inflight: Mutex::new(HashMap::new()), ttl, capacity: capacity.max(1) }
    }

    /// A live entry.
    pub fn get(&self, k: &MemoKey, now: DateTime<Utc>) -> Option<ChainResult> {
        let mut g = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        match g.get(k) {
            Some(e) if since(now, e.at) < self.ttl => Some(e.result.clone()),
            Some(_) => {
                g.remove(k);
                None
            }
            None => None,
        }
    }

    /// Store an answer. Only answers worth reusing are stored: an estimate,
    /// or a clean decline (errors are retried next time).
    pub fn put(&self, k: MemoKey, result: ChainResult, now: DateTime<Utc>) {
        if result.any_error {
            return;
        }
        let mut g = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        if g.len() >= self.capacity && !g.contains_key(&k) {
            // Evict expired entries, then the oldest.
            let ttl = self.ttl;
            g.retain(|_, e| since(now, e.at) < ttl);
            if g.len() >= self.capacity
                && let Some(oldest) = g.iter().min_by_key(|(_, e)| e.at).map(|(k, _)| k.clone())
            {
                g.remove(&oldest);
            }
        }
        g.insert(k, Entry { result, at: now });
    }

    /// Drop every entry of a workload key (any unit).
    pub fn invalidate_key(&self, key: &str) {
        let prefix = format!("{key}#");
        let mut g = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        g.retain(|k, _| k.key != key && !k.key.starts_with(&prefix));
    }

    /// Entries currently held.
    pub fn len(&self) -> usize {
        self.entries.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Whether the memo is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The single-flight cell for `k`: every concurrent miss gets the same cell.
    pub(crate) fn flight(&self, k: &MemoKey) -> Arc<OnceCell<ChainResult>> {
        let mut g = self.inflight.lock().unwrap_or_else(|e| e.into_inner());
        g.entry(k.clone()).or_insert_with(|| Arc::new(OnceCell::new())).clone()
    }

    /// Forget a finished flight.
    pub(crate) fn land(&self, k: &MemoKey, cell: &Arc<OnceCell<ChainResult>>) {
        let mut g = self.inflight.lock().unwrap_or_else(|e| e.into_inner());
        if g.get(k).is_some_and(|c| Arc::ptr_eq(c, cell)) {
            g.remove(k);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(key: &str) -> MemoKey {
        MemoKey { key: key.into(), code_digest: String::new(), bucket: String::new(), policy_hash: "p".into() }
    }

    #[test]
    fn ttl_capacity_and_invalidation() {
        let t0 = Utc::now();
        let m = Memo::new(std::time::Duration::from_secs(60), 2);
        let r = ChainResult { estimate: None, skipped: vec![], any_error: false };
        m.put(k("a#u"), r.clone(), t0);
        m.put(k("b#u"), r.clone(), t0 + chrono::Duration::seconds(1));
        m.put(k("c#u"), r.clone(), t0 + chrono::Duration::seconds(2));
        assert_eq!(m.len(), 2);
        assert!(m.get(&k("a#u"), t0).is_none(), "oldest evicted");
        assert!(m.get(&k("b#u"), t0 + chrono::Duration::seconds(30)).is_some());
        assert!(m.get(&k("b#u"), t0 + chrono::Duration::seconds(90)).is_none(), "expired");
        m.invalidate_key("c");
        assert!(m.is_empty());
        m.put(k("d#u"), ChainResult { any_error: true, ..r }, t0);
        assert!(m.is_empty(), "errors are not memoized");
    }
}
