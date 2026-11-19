//! Mode resolution (SPEC §2.3 S1/S5, §8.1, §8.4): the effective mode is
//! `min(requested, max_mode, mode_ceiling)`, then demoted by breakers, store
//! health and durability. Inputs are flags; I/O lives in the engine.

use serde::{Deserialize, Serialize};

use crate::breaker::BreakerState;
use crate::types::{HoldReason, Mode};

/// Store health as the engine sees it (SPEC §8.4).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct StoreHealth {
    /// History for this key is available (fresh, or cached within TTL).
    pub history_available: bool,
    /// The store is durable and shared (SPEC §8.1: enforce needs it).
    pub durable_shared: bool,
}

impl Default for StoreHealth {
    fn default() -> Self {
        StoreHealth { history_available: true, durable_shared: true }
    }
}

/// Breaker states relevant to one decision.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct BreakerView {
    /// Per-workload (or input-bucket) breaker.
    pub workload: BreakerState,
    /// Per-estimator-version breaker of the estimator that would answer.
    pub estimator: BreakerState,
    /// Fleet breaker.
    pub fleet: BreakerState,
    /// Breaker state could not be read: treated as open for reductions (SPEC §8.4).
    pub unreadable: bool,
}

/// Everything mode resolution needs.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct PolicyInputs {
    /// The mode the policy asks for.
    pub requested: Mode,
    /// The integration's highest mode (e.g. a surface that only recommends).
    pub max_mode: Mode,
    /// The fleet `mode_ceiling`; `None` = unreadable.
    pub mode_ceiling: Option<Mode>,
    /// Breakers.
    pub breakers: BreakerView,
    /// Store health.
    pub store: StoreHealth,
    /// Whether the surface can get failures classified (S5).
    pub can_classify: bool,
}

/// Why the mode was lowered.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum Demotion {
    /// Capped by the integration's `max_mode`.
    MaxMode,
    /// Capped by the fleet `mode_ceiling`.
    ModeCeiling,
    /// A per-workload breaker is open.
    WorkloadBreaker,
    /// The fleet breaker is open.
    FleetBreaker,
    /// History is unavailable past the cache TTL.
    StoreUnreachable,
    /// The store is not durable and shared.
    NonDurableStore,
}

/// The resolved policy for one decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Effective {
    /// The effective mode.
    pub mode: Mode,
    /// Every demotion applied, in order.
    pub demotions: Vec<Demotion>,
    /// Incompressible reductions are held for this reason (they are still
    /// recommended).
    pub reductions_blocked: Option<HoldReason>,
    /// The estimator-version breaker is open: the chain skips that estimator.
    pub skip_estimator: bool,
}

/// Resolve the effective mode (SPEC §2.3, §8.1, §8.4).
///
/// - `min(requested, max_mode, mode_ceiling)`; an unreadable `mode_ceiling`
///   keeps the mode but blocks reductions.
/// - An open workload or fleet breaker demotes `enforce` to `recommend`; a
///   half-open workload breaker allows a trial enforce.
/// - Unavailable history or a non-durable store demotes `enforce` to `recommend`.
/// - Unreadable breakers and unclassifiable surfaces block reductions.
pub fn effective_mode(i: &PolicyInputs) -> Effective {
    let mut mode = i.requested;
    let mut demotions = Vec::new();
    let mut cap = |m: &mut Mode, to: Mode, why: Demotion| {
        if *m > to {
            *m = to;
            demotions.push(why);
        }
    };
    cap(&mut mode, i.max_mode, Demotion::MaxMode);
    if let Some(c) = i.mode_ceiling {
        cap(&mut mode, c, Demotion::ModeCeiling);
    }
    if i.breakers.workload == BreakerState::Open {
        cap(&mut mode, Mode::Recommend, Demotion::WorkloadBreaker);
    }
    if i.breakers.fleet == BreakerState::Open {
        cap(&mut mode, Mode::Recommend, Demotion::FleetBreaker);
    }
    if !i.store.history_available {
        cap(&mut mode, Mode::Recommend, Demotion::StoreUnreachable);
    }
    if !i.store.durable_shared {
        cap(&mut mode, Mode::Recommend, Demotion::NonDurableStore);
    }
    let reductions_blocked = if i.breakers.unreadable || i.mode_ceiling.is_none() {
        Some(HoldReason::Breaker)
    } else if !i.store.history_available {
        Some(HoldReason::StoreUnreachable)
    } else if !i.can_classify {
        Some(HoldReason::Unclassifiable)
    } else {
        None
    };
    Effective { mode, demotions, reductions_blocked, skip_estimator: i.breakers.estimator == BreakerState::Open }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> PolicyInputs {
        PolicyInputs {
            requested: Mode::Enforce,
            max_mode: Mode::Enforce,
            mode_ceiling: Some(Mode::Enforce),
            breakers: BreakerView::default(),
            store: StoreHealth::default(),
            can_classify: true,
        }
    }

    #[test]
    fn min_of_requested_max_and_ceiling() {
        assert_eq!(effective_mode(&base()).mode, Mode::Enforce);
        let e = effective_mode(&PolicyInputs { mode_ceiling: Some(Mode::Observe), ..base() });
        assert_eq!(e.mode, Mode::Observe);
        assert_eq!(e.demotions, vec![Demotion::ModeCeiling]);
        let e = effective_mode(&PolicyInputs { max_mode: Mode::Recommend, ..base() });
        assert_eq!(e.mode, Mode::Recommend);
        let e = effective_mode(&PolicyInputs { requested: Mode::Off, ..base() });
        assert_eq!(e.mode, Mode::Off);
        assert!(e.demotions.is_empty());
    }

    #[test]
    fn breakers_demote() {
        let open = BreakerView { workload: BreakerState::Open, ..Default::default() };
        assert_eq!(effective_mode(&PolicyInputs { breakers: open, ..base() }).mode, Mode::Recommend);
        let half = BreakerView { workload: BreakerState::HalfOpen, ..Default::default() };
        assert_eq!(effective_mode(&PolicyInputs { breakers: half, ..base() }).mode, Mode::Enforce);
        let fleet = BreakerView { fleet: BreakerState::Open, ..Default::default() };
        assert_eq!(effective_mode(&PolicyInputs { breakers: fleet, ..base() }).demotions, vec![Demotion::FleetBreaker]);
        let est = BreakerView { estimator: BreakerState::Open, ..Default::default() };
        let e = effective_mode(&PolicyInputs { breakers: est, ..base() });
        assert!(e.skip_estimator);
        assert_eq!(e.mode, Mode::Enforce);
        let unreadable = BreakerView { unreadable: true, ..Default::default() };
        assert_eq!(effective_mode(&PolicyInputs { breakers: unreadable, ..base() }).reductions_blocked, Some(HoldReason::Breaker));
    }

    #[test]
    fn store_and_classification() {
        let e = effective_mode(&PolicyInputs { store: StoreHealth { history_available: false, durable_shared: true }, ..base() });
        assert_eq!(e.mode, Mode::Recommend);
        assert_eq!(e.reductions_blocked, Some(HoldReason::StoreUnreachable));
        let e = effective_mode(&PolicyInputs { store: StoreHealth { history_available: true, durable_shared: false }, ..base() });
        assert_eq!(e.demotions, vec![Demotion::NonDurableStore]);
        let e = effective_mode(&PolicyInputs { can_classify: false, ..base() });
        assert_eq!(e.mode, Mode::Enforce);
        assert_eq!(e.reductions_blocked, Some(HoldReason::Unclassifiable));
        let e = effective_mode(&PolicyInputs { mode_ceiling: None, ..base() });
        assert_eq!(e.reductions_blocked, Some(HoldReason::Breaker));
    }
}
