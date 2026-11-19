//! An in-memory [`HistoryStore`] with the same semantics as the durable
//! backends (dedup, newest-first, trip/reset rules, conditional attempt
//! writes). Not durable: `durable_shared = false`, so enforce demotes.

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rightsizer_core::breaker::{BreakerObjects, BreakerScope, Control, Reset, Trip};
use rightsizer_core::{AttemptRecord, Decision, Lease, RunRecord, RunStatus, WorkloadKey};
use ulid::Ulid;

use crate::{
    Backend, CompactOptions, CompactReport, DecisionFilter, HistoryStore, PutOutcome, Role, StoreError, StoreInfo, dedup_key,
};

#[derive(Default)]
struct Inner {
    runs: BTreeMap<String, Vec<RunRecord>>,
    seen: std::collections::BTreeSet<(String, String, u32, String)>,
    decisions: Vec<Decision>,
    breakers: BTreeMap<BreakerScope, BreakerObjects>,
    control: Option<Control>,
    attempts: BTreeMap<(String, String), AttemptRecord>,
}

/// An in-memory store.
#[derive(Default)]
pub struct MemoryStore {
    inner: Mutex<Inner>,
    /// Refuse resets and control writes, like client credentials do.
    pub client_credentials: bool,
}

impl MemoryStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Inner>, StoreError> {
        self.inner.lock().map_err(|_| StoreError::Other("poisoned".into()))
    }
}

#[async_trait]
impl HistoryStore for MemoryStore {
    fn info(&self) -> StoreInfo {
        StoreInfo { backend: Backend::Memory, uri: "memory://".into(), durable_shared: false }
    }

    fn role(&self) -> Role {
        if self.client_credentials { Role::Client } else { Role::Operator }
    }

    async fn keys(&self) -> Result<Vec<WorkloadKey>, StoreError> {
        Ok(self.lock()?.runs.values().filter_map(|v| v.first().map(|r| r.key.clone())).collect())
    }

    async fn put_run(&self, rec: &RunRecord) -> Result<PutOutcome, StoreError> {
        let mut g = self.lock()?;
        if !g.seen.insert(dedup_key(rec)) {
            return Ok(PutOutcome::Duplicate);
        }
        let v = g.runs.entry(rec.key.to_string()).or_default();
        v.push(rec.clone());
        crate::sort_newest_first(v);
        Ok(PutOutcome::Inserted)
    }

    async fn put_decision(&self, d: &Decision) -> Result<(), StoreError> {
        let mut g = self.lock()?;
        if !g.decisions.iter().any(|x| x.id == d.id) {
            g.decisions.push(d.clone()); // same id = the same decision (a retry)
        }
        Ok(())
    }

    async fn runs(&self, key: &WorkloadKey, limit: usize) -> Result<Vec<RunRecord>, StoreError> {
        Ok(self.lock()?.runs.get(&key.to_string()).map(|v| v.iter().take(limit).cloned().collect()).unwrap_or_default())
    }

    async fn decisions(&self, filter: &DecisionFilter) -> Result<Vec<Decision>, StoreError> {
        let g = self.lock()?;
        let mut v: Vec<Decision> = g.decisions.iter().filter(|d| filter.matches(d)).cloned().collect();
        v.sort_by_key(|d| std::cmp::Reverse(d.id));
        if let Some(n) = filter.limit {
            v.truncate(n);
        }
        Ok(v)
    }

    async fn decision(&self, id: Ulid) -> Result<Option<Decision>, StoreError> {
        Ok(self.lock()?.decisions.iter().find(|d| d.id == id).cloned())
    }

    async fn breaker_get(&self, scope: &BreakerScope) -> Result<BreakerObjects, StoreError> {
        Ok(self.lock()?.breakers.get(scope).cloned().unwrap_or_default())
    }

    async fn breaker_list(&self) -> Result<Vec<(BreakerScope, BreakerObjects)>, StoreError> {
        Ok(self.lock()?.breakers.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
    }

    async fn breaker_trip(&self, trip: &Trip) -> Result<(), StoreError> {
        let mut g = self.lock()?;
        let objs = g.breakers.entry(trip.scope.clone()).or_default();
        if objs.trips.iter().any(|t| t.id == trip.id) {
            return Err(StoreError::Conflict(format!("trip {} exists", trip.id)));
        }
        objs.trips.push(trip.clone());
        Ok(())
    }

    async fn breaker_reset(&self, reset: &Reset) -> Result<(), StoreError> {
        if self.client_credentials {
            return Err(StoreError::PermissionDenied("resets need operator credentials".into()));
        }
        self.lock()?.breakers.entry(reset.scope.clone()).or_default().resets.push(reset.clone());
        Ok(())
    }

    async fn control_get(&self) -> Result<Option<Control>, StoreError> {
        Ok(self.lock()?.control.clone())
    }

    async fn control_set(&self, control: &Control) -> Result<(), StoreError> {
        if self.client_credentials {
            return Err(StoreError::PermissionDenied("control needs operator credentials".into()));
        }
        self.lock()?.control = Some(control.clone());
        Ok(())
    }

    async fn attempt_get(&self, key: &WorkloadKey, invocation_id: &str) -> Result<Option<AttemptRecord>, StoreError> {
        Ok(self.lock()?.attempts.get(&(key.to_string(), invocation_id.to_string())).cloned())
    }

    async fn attempt_put_if_absent(&self, rec: &AttemptRecord) -> Result<bool, StoreError> {
        let mut g = self.lock()?;
        let k = (rec.key.to_string(), rec.invocation_id.clone());
        if g.attempts.contains_key(&k) {
            return Ok(false);
        }
        g.attempts.insert(k, rec.clone());
        Ok(true)
    }

    async fn attempt_close_lease(&self, key: &WorkloadKey, invocation_id: &str, at: DateTime<Utc>, outcome: RunStatus) -> Result<(), StoreError> {
        let mut g = self.lock()?;
        let rec = g
            .attempts
            .get_mut(&(key.to_string(), invocation_id.to_string()))
            .ok_or_else(|| StoreError::Other(format!("no attempt record for {invocation_id}")))?;
        let lease = rec.lease.get_or_insert(Lease { opened_at: at, closed_at: None, outcome: None });
        lease.closed_at = Some(at);
        lease.outcome = Some(outcome);
        Ok(())
    }

    async fn compact(&self, opts: &CompactOptions) -> Result<CompactReport, StoreError> {
        let mut g = self.lock()?;
        let mut report = CompactReport::default();
        let cutoff = opts.now - chrono::Duration::from_std(opts.max_age).unwrap_or_default();
        for v in g.runs.values_mut() {
            let before = v.len();
            if !opts.dry_run {
                v.retain(|r| r.started_at >= cutoff);
            }
            report.deleted += before - v.len();
            report.pointers += 1;
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rightsizer_core::breaker::TripReason;
    use rightsizer_core::{Confidence, OutcomeSource, Trust};

    fn rec(inv: &str, attempt: u32, secs: i64) -> RunRecord {
        RunRecord {
            v: 1,
            key: WorkloadKey::parse("k8s://c/ns/job/x").unwrap(),
            cluster: None,
            invocation_id: inv.into(),
            attempt,
            decision_id: None,
            started_at: DateTime::from_timestamp(1_700_000_000 + secs, 0).unwrap(),
            inputs: vec![],
            code_digest: None,
            requested: None,
            origin_prior: None,
            status: RunStatus::Succeeded,
            status_confidence: Confidence::Observed,
            memory_peak: None,
            cpu_mean: None,
            cpu_peak_window: None,
            cpu_throttled_frac: None,
            gpu_memory_peak: None,
            gpu_util_mean: None,
            t_peak_frac: None,
            duration_s: None,
            staged_bytes: None,
            source: OutcomeSource::Watcher,
            trust: Trust::Verified,
        }
    }

    #[tokio::test]
    async fn dedup_and_order() {
        let s = MemoryStore::new();
        assert_eq!(s.put_run(&rec("a", 1, 0)).await.unwrap(), PutOutcome::Inserted);
        assert_eq!(s.put_run(&rec("a", 1, 0)).await.unwrap(), PutOutcome::Duplicate);
        s.put_run(&rec("b", 1, 100)).await.unwrap();
        let k = WorkloadKey::parse("k8s://c/ns/job/x").unwrap();
        let v = s.runs(&k, 10).await.unwrap();
        assert_eq!(v.iter().map(|r| r.invocation_id.as_str()).collect::<Vec<_>>(), vec!["b", "a"]);
        assert_eq!(s.runs(&k, 1).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn trips_resets_and_credentials() {
        let s = MemoryStore { client_credentials: true, ..Default::default() };
        let scope = BreakerScope::Fleet;
        let trip = Trip { v: 1, id: Ulid::from_parts(1, 1), scope: scope.clone(), at: DateTime::from_timestamp(10, 0).unwrap(), reason: TripReason::Manual, detail: None };
        s.breaker_trip(&trip).await.unwrap();
        assert!(s.breaker_trip(&trip).await.is_err(), "If-None-Match: *");
        assert!(s.breaker_get(&scope).await.unwrap().is_open());
        let reset = Reset { v: 1, scope: scope.clone(), at: DateTime::from_timestamp(20, 0).unwrap(), by: None };
        assert!(matches!(s.breaker_reset(&reset).await, Err(StoreError::PermissionDenied(_))));
    }

    #[tokio::test]
    async fn attempts_are_conditional() {
        let s = MemoryStore::new();
        let k = WorkloadKey::parse("k8s://c/ns/job/x").unwrap();
        let a = AttemptRecord {
            v: 1,
            key: k.clone(),
            invocation_id: "i".into(),
            origin_prior: Default::default(),
            applied: Default::default(),
            decision_id: None,
            reduced: true,
            written_at: DateTime::from_timestamp(0, 0).unwrap(),
            lease: Some(Lease { opened_at: DateTime::from_timestamp(0, 0).unwrap(), closed_at: None, outcome: None }),
        };
        assert!(s.attempt_put_if_absent(&a).await.unwrap());
        assert!(!s.attempt_put_if_absent(&a).await.unwrap());
        s.attempt_close_lease(&k, "i", DateTime::from_timestamp(5, 0).unwrap(), RunStatus::Succeeded).await.unwrap();
        let got = s.attempt_get(&k, "i").await.unwrap().unwrap();
        assert_eq!(got.lease.unwrap().outcome, Some(RunStatus::Succeeded));
    }
}
