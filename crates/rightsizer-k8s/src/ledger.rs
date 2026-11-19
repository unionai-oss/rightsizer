//! The reservation ledger (kubernetes.md §3.4): increases admitted in the
//! last 2 minutes whose pods do not exist yet. ResourceQuota `status.used`
//! lags admission, so `validate` subtracts these from the remaining quota.
//!
//! [`ReservationLedger`] is the interface; [`MemoryLedger`] is the
//! in-process implementation. A store-backed implementation (shared across
//! webhook replicas) comes with the cluster components.

use std::sync::Mutex;

use chrono::{DateTime, Duration, Utc};

use crate::resources::Totals;

/// Default reservation lifetime.
pub const RESERVATION_TTL_S: i64 = 120;

/// One reservation.
#[derive(Clone, Debug, PartialEq)]
pub struct Reservation {
    /// Namespace.
    pub namespace: String,
    /// Reservation id (decision id or admission uid).
    pub id: String,
    /// Reserved increase (requests).
    pub requests: Totals,
    /// Reserved increase (limits).
    pub limits: Totals,
    /// When admitted.
    pub at: DateTime<Utc>,
}

/// Reserved increases by namespace.
pub trait ReservationLedger: Send + Sync {
    /// Record an admitted increase.
    fn reserve(&self, r: Reservation);
    /// Drop a reservation (its pods exist, or the object was rejected).
    fn release(&self, namespace: &str, id: &str);
    /// The sum of live reservations in `namespace` at `now` (requests, limits).
    fn reserved(&self, namespace: &str, now: DateTime<Utc>) -> (Totals, Totals);
}

/// In-process ledger with a fixed TTL.
#[derive(Debug)]
pub struct MemoryLedger {
    ttl: Duration,
    items: Mutex<Vec<Reservation>>,
}

impl Default for MemoryLedger {
    fn default() -> Self {
        MemoryLedger::new(Duration::seconds(RESERVATION_TTL_S))
    }
}

impl MemoryLedger {
    /// A ledger whose reservations expire after `ttl`.
    pub fn new(ttl: Duration) -> Self {
        MemoryLedger { ttl, items: Mutex::new(Vec::new()) }
    }
    /// Number of reservations held (expired ones included until the next call).
    pub fn len(&self) -> usize {
        self.items.lock().map(|i| i.len()).unwrap_or(0)
    }
    /// Whether empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl ReservationLedger for MemoryLedger {
    fn reserve(&self, r: Reservation) {
        if let Ok(mut items) = self.items.lock() {
            items.retain(|x| !(x.namespace == r.namespace && x.id == r.id));
            items.push(r);
        }
    }
    fn release(&self, namespace: &str, id: &str) {
        if let Ok(mut items) = self.items.lock() {
            items.retain(|x| !(x.namespace == namespace && x.id == id));
        }
    }
    fn reserved(&self, namespace: &str, now: DateTime<Utc>) -> (Totals, Totals) {
        let Ok(mut items) = self.items.lock() else { return (Totals::default(), Totals::default()) };
        let ttl = self.ttl;
        items.retain(|x| now.signed_duration_since(x.at) < ttl);
        items.iter().filter(|x| x.namespace == namespace).fold((Totals::default(), Totals::default()), |(r, l), x| (r.add(&x.requests), l.add(&x.limits)))
    }
}

/// A ledger that holds nothing.
#[derive(Debug, Default)]
pub struct NoLedger;

impl ReservationLedger for NoLedger {
    fn reserve(&self, _r: Reservation) {}
    fn release(&self, _namespace: &str, _id: &str) {}
    fn reserved(&self, _namespace: &str, _now: DateTime<Utc>) -> (Totals, Totals) {
        (Totals::default(), Totals::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expiry_after_two_minutes() {
        let l = MemoryLedger::default();
        let t0 = DateTime::parse_from_rfc3339("2026-11-18T12:00:00Z").unwrap().with_timezone(&Utc);
        let r = Totals { cpu: 2000, memory: 4 << 30, ..Default::default() };
        l.reserve(Reservation { namespace: "ml".into(), id: "a".into(), requests: r.clone(), limits: Totals::default(), at: t0 });
        l.reserve(Reservation { namespace: "ml".into(), id: "b".into(), requests: r.clone(), limits: Totals::default(), at: t0 + Duration::seconds(60) });
        l.reserve(Reservation { namespace: "other".into(), id: "c".into(), requests: r.clone(), limits: Totals::default(), at: t0 });
        assert_eq!(l.reserved("ml", t0 + Duration::seconds(30)).0.cpu, 4000);
        assert_eq!(l.reserved("ml", t0 + Duration::seconds(121)).0.cpu, 2000);
        assert_eq!(l.reserved("ml", t0 + Duration::seconds(200)).0.cpu, 0);
        l.reserve(Reservation { namespace: "ml".into(), id: "d".into(), requests: r, limits: Totals::default(), at: t0 + Duration::seconds(200) });
        l.release("ml", "d");
        assert_eq!(l.reserved("ml", t0 + Duration::seconds(201)).0.cpu, 0);
    }
}
