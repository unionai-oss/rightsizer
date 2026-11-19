//! Clock injection: every time the engine reads (cache TTLs, breaker windows,
//! classification windows, decision ids) comes from a [`Clock`], so tests can
//! move time by hand.

use std::sync::Mutex;

use chrono::{DateTime, Duration, Utc};

/// A source of "now".
pub trait Clock: Send + Sync {
    /// The current time.
    fn now(&self) -> DateTime<Utc>;
}

/// The system clock.
#[derive(Copy, Clone, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// A clock that moves only when told to (tests).
#[derive(Debug)]
pub struct ManualClock(Mutex<DateTime<Utc>>);

impl ManualClock {
    /// A clock stopped at `at`.
    pub fn new(at: DateTime<Utc>) -> Self {
        ManualClock(Mutex::new(at))
    }
    /// Set the time.
    pub fn set(&self, at: DateTime<Utc>) {
        *self.0.lock().unwrap_or_else(|e| e.into_inner()) = at;
    }
    /// Move forward.
    pub fn advance(&self, by: std::time::Duration) {
        let mut g = self.0.lock().unwrap_or_else(|e| e.into_inner());
        *g += Duration::from_std(by).unwrap_or(Duration::zero());
    }
}

impl Clock for ManualClock {
    fn now(&self) -> DateTime<Utc> {
        *self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Elapsed time between two instants as a `std` duration (0 if negative).
pub(crate) fn since(now: DateTime<Utc>, then: DateTime<Utc>) -> std::time::Duration {
    (now - then).to_std().unwrap_or_default()
}
