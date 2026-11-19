//! Clock injection: every time the engine reads (cache TTLs, breaker windows,
//! classification windows, decision ids) comes from a [`Clock`], so tests can
//! move time by hand. The trait lives in `rightsizer_core::clock` so the
//! history crate's caches run on the same clock; it is re-exported here.

use chrono::{DateTime, Utc};
pub use rightsizer_core::clock::{Clock, ManualClock, SystemClock};

/// Elapsed time between two instants as a `std` duration (0 if negative).
pub(crate) fn since(now: DateTime<Utc>, then: DateTime<Utc>) -> std::time::Duration {
    rightsizer_core::clock::elapsed(now, then)
}
