//! The reservation ledger shared across webhook replicas through the store
//! (kubernetes.md §3.4), feature `engine`.
//!
//! The history store has no generic key-value API, and an increase is only
//! ever admitted together with a recorded decision. So a replica publishes
//! its reservation as one explanation line of the object's first decision
//! (`reservation:{json}`), and every replica refreshes the decisions of the
//! last 2 minutes from the decision log (every 3 s) to learn the others'.
//! `reserved()` = local reservations + remote ones not already local.
//! Reservations expire after 2 minutes on both sides; the refresh lag means
//! two replicas can still admit concurrently within one refresh period
//! (documented, conservative elsewhere: quota scopes are not evaluated).

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration as StdDuration;

use chrono::{DateTime, Duration, Utc};
use rightsizer_engine::Engine;
use rightsizer_history::DecisionFilter;
use serde_json::{Value, json};

use crate::ledger::{MemoryLedger, RESERVATION_TTL_S, Reservation, ReservationLedger};
use crate::resources::Totals;

const PREFIX: &str = "reservation:";

fn totals_json(t: &Totals) -> Value {
    json!({"cpu": t.cpu, "memory": t.memory, "ephemeral": t.ephemeral, "gpu": t.gpu})
}

fn totals_of(v: &Value) -> Totals {
    Totals {
        cpu: v.get("cpu").and_then(Value::as_u64).unwrap_or(0),
        memory: v.get("memory").and_then(Value::as_u64).unwrap_or(0),
        ephemeral: v.get("ephemeral").and_then(Value::as_u64).unwrap_or(0),
        gpu: v.get("gpu").and_then(|g| serde_json::from_value::<BTreeMap<String, u64>>(g.clone()).ok()).unwrap_or_default(),
    }
}

/// The explanation line carrying a reservation.
pub fn encode(r: &Reservation) -> String {
    format!("{PREFIX}{}", json!({"ns": r.namespace, "id": r.id, "requests": totals_json(&r.requests), "limits": totals_json(&r.limits), "at": r.at.to_rfc3339()}))
}

/// Parse an explanation line.
pub fn decode(line: &str) -> Option<Reservation> {
    let v: Value = serde_json::from_str(line.strip_prefix(PREFIX)?).ok()?;
    Some(Reservation {
        namespace: v.get("ns")?.as_str()?.to_string(),
        id: v.get("id")?.as_str()?.to_string(),
        requests: totals_of(v.get("requests")?),
        limits: totals_of(v.get("limits")?),
        at: v.get("at").and_then(Value::as_str).and_then(|s| DateTime::parse_from_rfc3339(s).ok()).map(|d| d.with_timezone(&Utc))?,
    })
}

/// Local + remote reservations.
#[derive(Debug, Default)]
pub struct SharedLedger {
    local: MemoryLedger,
    remote: Mutex<HashMap<String, Reservation>>,
}

impl SharedLedger {
    /// A ledger.
    pub fn new() -> Arc<Self> {
        Arc::new(SharedLedger::default())
    }

    /// A local reservation (to publish it).
    pub fn local_reservation(&self, ns: &str, id: &str) -> Option<Reservation> {
        self.local.get(ns, id)
    }

    /// Replace the remote set (from the decision log).
    pub fn set_remote(&self, rs: Vec<Reservation>) {
        if let Ok(mut m) = self.remote.lock() {
            m.clear();
            for r in rs {
                m.insert(format!("{}/{}", r.namespace, r.id), r);
            }
        }
    }

    /// Refresh the remote set from `engine`'s decision log once.
    pub async fn refresh(&self, engine: &Engine) -> Result<usize, String> {
        let now = engine.now();
        let f = DecisionFilter { since: Some(now - Duration::seconds(RESERVATION_TTL_S)), mode: Some(rightsizer_core::Mode::Enforce), ..Default::default() };
        let ds = engine.store().decisions(&f).await.map_err(|e| e.to_string())?;
        let rs: Vec<Reservation> = ds.iter().filter(|d| d.key.platform == "k8s").flat_map(|d| d.explanation.iter().filter_map(|l| decode(l))).collect();
        let n = rs.len();
        self.set_remote(rs);
        Ok(n)
    }

    /// Refresh every `every` until the process exits.
    pub fn spawn_refresh(self: &Arc<Self>, engine: Engine, every: StdDuration) {
        let me = self.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = me.refresh(&engine).await {
                    tracing::debug!(error = %e, "shared ledger refresh failed");
                }
                tokio::time::sleep(every).await;
            }
        });
    }
}

impl ReservationLedger for SharedLedger {
    fn reserve(&self, r: Reservation) {
        self.local.reserve(r);
    }
    fn release(&self, namespace: &str, id: &str) {
        self.local.release(namespace, id);
        if let Ok(mut m) = self.remote.lock() {
            m.remove(&format!("{namespace}/{id}"));
        }
    }
    fn reserved(&self, namespace: &str, now: DateTime<Utc>) -> (Totals, Totals) {
        let (mut r, mut l) = self.local.reserved(namespace, now);
        if let Ok(m) = self.remote.lock() {
            let ttl = Duration::seconds(RESERVATION_TTL_S);
            for x in m.values().filter(|x| x.namespace == namespace && now - x.at < ttl && self.local.get(namespace, &x.id).is_none()) {
                r = r.add(&x.requests);
                l = l.add(&x.limits);
            }
        }
        (r, l)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_sums() {
        let now = Utc::now();
        let r = Reservation { namespace: "ml".into(), id: "01J".into(), requests: Totals { cpu: 500, memory: 1 << 30, ..Default::default() }, limits: Totals::default(), at: now };
        let back = decode(&encode(&r)).unwrap();
        assert_eq!(back.namespace, "ml");
        assert_eq!(back.requests, r.requests);
        let l = SharedLedger::default();
        l.set_remote(vec![back]);
        assert_eq!(l.reserved("ml", now).0.memory, 1 << 30);
        l.reserve(r.clone());
        assert_eq!(l.reserved("ml", now).0.memory, 1 << 30, "local and remote copies of one id count once");
        assert_eq!(l.reserved("other", now).0.memory, 0);
    }
}
