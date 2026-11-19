//! The memo and single-flight (SPEC §6.3): `rightsizer_estimators::memo`,
//! the one memo of the workspace, re-exported.
//!
//! The memo stores the chain's **normalized estimate** (or a clean
//! decline); constraints, S3, breakers and the mode are re-evaluated on
//! every hit. Key = (workload key, unit, code digest, feature bucket, policy
//! hash). The engine builds it on its injected clock with
//! `decision.memo_ttl`, `decision.memo_capacity` and `decision.point_sigma`
//! (so a hit decides exactly like the miss that filled it). An outcome for a
//! key invalidates every entry of that key (`Memo::invalidate_workload`).
//! Concurrent misses on one key share one chain call
//! (`Memo::get_or_compute`); errors are shared with waiters but never stored.

pub use rightsizer_estimators::memo::{Memo, MemoAnswer, MemoAnswer as ChainResult, MemoKey};

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use rightsizer_core::clock::ManualClock;
    use rightsizer_core::{AxisEstimate, Estimate, Grid, Provenance};

    use super::*;

    fn k(key: &str, unit: &str) -> MemoKey {
        MemoKey { workload_key: key.into(), unit: unit.into(), code_digest: None, feature_bucket: String::new(), policy_hash: "p".into() }
    }

    fn est() -> Estimate {
        let mut e = Estimate::empty(Provenance { estimator: "x".into(), version: "1".into(), ..Default::default() });
        e.memory = Some(AxisEstimate::Point { value: 3.0 * (1u64 << 30) as f64 });
        e
    }

    #[tokio::test]
    async fn ttl_on_the_clock_capacity_invalidation_and_single_flight() {
        let t0 = chrono::DateTime::from_timestamp(1_800_000_000, 0).unwrap();
        let clock = Arc::new(ManualClock::new(t0));
        let m = Memo::with_clock(Grid::rs_grid_v1(), std::time::Duration::from_secs(60), 2, clock.clone());
        let r = ChainResult { estimate: Some(est()), skipped: vec![], any_error: false };
        m.put_answer(k("a", "u"), r.clone());
        clock.advance(std::time::Duration::from_secs(1));
        m.put_answer(k("b", "u"), r.clone());
        clock.advance(std::time::Duration::from_secs(1));
        m.put_answer(k("c", "u"), r.clone());
        assert_eq!(m.len(), 2);
        assert!(m.get_answer(&k("a", "u")).is_none(), "oldest evicted");
        clock.advance(std::time::Duration::from_secs(30));
        assert!(m.get_answer(&k("b", "u")).is_some());
        clock.advance(std::time::Duration::from_secs(60));
        assert!(m.get_answer(&k("b", "u")).is_none(), "expired on the injected clock");
        m.put_answer(k("c", "v"), r.clone());
        m.invalidate_workload("c");
        assert!(m.is_empty(), "every unit of the key");
        m.put_answer(k("d", "u"), ChainResult { any_error: true, ..r.clone() });
        assert!(m.is_empty(), "errors are not memoized");
        let declined = m.put_answer(k("e", "u"), ChainResult::default());
        assert_eq!(m.get_answer(&k("e", "u")), Some(declined), "clean declines are");

        // Single-flight: 8 concurrent misses, one computation.
        let m = Arc::new(Memo::with_clock(Grid::rs_grid_v1(), std::time::Duration::from_secs(60), 10, clock.clone()));
        let calls = Arc::new(AtomicUsize::new(0));
        let mut hs = Vec::new();
        for _ in 0..8 {
            let (m, calls, r) = (m.clone(), calls.clone(), r.clone());
            hs.push(tokio::spawn(async move {
                m.get_or_compute(&k("f", "u"), || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
                    r
                })
                .await
            }));
        }
        for h in hs {
            let (a, _) = h.await.unwrap();
            assert!(matches!(a.estimate.and_then(|e| e.memory), Some(AxisEstimate::Cells { .. })), "stored normalized");
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let (_, hit) = m.get_or_compute(&k("f", "u"), || async { unreachable!() }).await;
        assert!(hit);
    }
}
