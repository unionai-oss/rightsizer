//! Breakers: pure trip/reset semantics and burn-rate evaluation (SPEC §2.3 S5, §8.1).
//!
//! Clients can only create **trip** objects; resets and `control/` are
//! operator-written. A breaker is **open while any trip is newer than its
//! latest reset**. A per-workload breaker goes **half-open** after
//! `recovery_shadow_fits` (5) shadow decisions whose measured outcomes fit;
//! the others need `rsz breaker reset`.
//!
//! | breaker | trips when |
//! |---|---|
//! | per-workload | one rightsizer-caused failure that escalation did not recover (scope: the key, or the input-size bucket if the run was beyond its history's sizes) |
//! | per-estimator-version | caused rate > 2% over ≥ 50 enforced decisions from that version |
//! | fleet fast-burn | > 5% of enforced invocations fail rightsizer-caused within 1 h, with ≥ 20 such failures |
//! | fleet slow-burn | > 0.5% over 30 days |

use std::collections::BTreeMap;
use std::fmt;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::safety::Attribution;
use crate::types::{Mode, WorkloadKey};

/// What a breaker covers (SPEC §2.3 S5). JSON: `{"kind": "workload", "key": ...}`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BreakerScope {
    /// One workload key.
    Workload {
        /// The key.
        key: WorkloadKey,
    },
    /// One input-size bucket of a workload key.
    InputBucket {
        /// The key.
        key: WorkloadKey,
        /// The bucket (e.g. the memory-ladder cell of the input bytes).
        bucket: String,
    },
    /// Every decision from one estimator version.
    EstimatorVersion {
        /// Estimator id.
        id: String,
        /// Version.
        version: String,
    },
    /// The whole store prefix.
    Fleet,
}

impl BreakerScope {
    /// The store path segment for `{prefix}/v1/breakers/{scope}` (SPEC §8.1).
    /// Workload-scoped breakers sit under `{platform}/{tenant}`, like runs
    /// (`workload/{platform}/{tenant}/{key hash}`,
    /// `bucket/{platform}/{tenant}/{key hash}/{bucket}`), so per-tenant IAM
    /// can tell platforms apart.
    pub fn path(&self) -> String {
        match self {
            BreakerScope::Workload { key } => format!("workload/{}/{}/{}", key.platform, key.tenant, key.hash()),
            BreakerScope::InputBucket { key, bucket } => format!("bucket/{}/{}/{}/{}", key.platform, key.tenant, key.hash(), sanitize(bucket)),
            BreakerScope::EstimatorVersion { id, version } => format!("estimator/{}/{}", sanitize(id), sanitize(version)),
            BreakerScope::Fleet => "fleet".into(),
        }
    }

    /// The previous (N-1) path of a workload-scoped breaker, without the
    /// platform segment (`workload/{tenant}/{key hash}`). Readers look at
    /// both; writers use [`path`](Self::path) only. `None` when the path did
    /// not change.
    pub fn legacy_path(&self) -> Option<String> {
        match self {
            BreakerScope::Workload { key } => Some(format!("workload/{}/{}", key.tenant, key.hash())),
            BreakerScope::InputBucket { key, bucket } => Some(format!("bucket/{}/{}/{}", key.tenant, key.hash(), sanitize(bucket))),
            BreakerScope::EstimatorVersion { .. } | BreakerScope::Fleet => None,
        }
    }

    /// Every path a reader checks: [`path`](Self::path), then the legacy one.
    pub fn read_paths(&self) -> Vec<String> {
        let mut v = vec![self.path()];
        v.extend(self.legacy_path());
        v
    }
}

fn sanitize(s: &str) -> String {
    s.chars().map(|c| if c.is_ascii_alphanumeric() || "._-".contains(c) { c } else { '_' }).collect()
}

impl fmt::Display for BreakerScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BreakerScope::Workload { key } => write!(f, "workload {key}"),
            BreakerScope::InputBucket { key, bucket } => write!(f, "workload {key} bucket {bucket}"),
            BreakerScope::EstimatorVersion { id, version } => write!(f, "estimator {id}@{version}"),
            BreakerScope::Fleet => f.write_str("fleet"),
        }
    }
}

/// Why a breaker tripped.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum TripReason {
    /// A caused failure escalation did not recover.
    UnrecoveredFailure,
    /// Per-estimator-version failure rate.
    EstimatorRate,
    /// Fleet fast-burn.
    FastBurn,
    /// Fleet slow-burn.
    SlowBurn,
    /// Tripped by hand (`rsz breaker trip`).
    Manual,
}

/// A trip object (`breakers/{scope}/trips/{ulid}.json`, `If-None-Match: *`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Trip {
    /// Wire version (1).
    #[serde(default = "crate::wire_v")]
    pub v: u16,
    /// Trip id.
    #[cfg_attr(feature = "schema", schemars(with = "String"))]
    pub id: Ulid,
    /// Scope.
    pub scope: BreakerScope,
    /// When.
    pub at: DateTime<Utc>,
    /// Why.
    pub reason: TripReason,
    /// Detail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// An operator-written reset.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Reset {
    /// Wire version (1).
    #[serde(default = "crate::wire_v")]
    pub v: u16,
    /// Scope.
    pub scope: BreakerScope,
    /// When.
    pub at: DateTime<Utc>,
    /// Who.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub by: Option<String>,
}

/// All objects of one breaker scope.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct BreakerObjects {
    /// Trips.
    #[serde(default)]
    pub trips: Vec<Trip>,
    /// Resets.
    #[serde(default)]
    pub resets: Vec<Reset>,
}

impl BreakerObjects {
    /// The latest reset time.
    pub fn latest_reset(&self) -> Option<DateTime<Utc>> {
        self.resets.iter().map(|r| r.at).max()
    }
    /// Trips newer than the latest reset, oldest first.
    pub fn open_trips(&self) -> Vec<&Trip> {
        let reset = self.latest_reset();
        let mut v: Vec<&Trip> = self.trips.iter().filter(|t| reset.is_none_or(|r| t.at > r)).collect();
        v.sort_by_key(|t| (t.at, t.id));
        v
    }
    /// Open while any trip is newer than the latest reset.
    pub fn is_open(&self) -> bool {
        !self.open_trips().is_empty()
    }
    /// The newest open trip.
    pub fn newest_open_trip(&self) -> Option<&Trip> {
        self.open_trips().last().copied()
    }
}

/// A breaker's state.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum BreakerState {
    /// Not tripped.
    #[default]
    Closed,
    /// Tripped: enforce demotes to recommend.
    Open,
    /// Per-workload only: recovered in shadow; the next enforced decision is a trial.
    HalfOpen,
}

/// The state of a breaker given its objects and the count of fitting shadow
/// decisions since its newest open trip (per-workload scopes only go half-open).
pub fn state(objs: &BreakerObjects, shadow_fits_since_trip: u32, recovery_shadow_fits: u32) -> BreakerState {
    let Some(t) = objs.newest_open_trip() else { return BreakerState::Closed };
    let workload = matches!(t.scope, BreakerScope::Workload { .. } | BreakerScope::InputBucket { .. });
    if workload && t.reason != TripReason::Manual && shadow_fits_since_trip >= recovery_shadow_fits {
        BreakerState::HalfOpen
    } else {
        BreakerState::Open
    }
}

/// The fleet control object (`{prefix}/v1/control/mode.json`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Control {
    /// Wire version (1).
    #[serde(default = "crate::wire_v")]
    pub v: u16,
    /// Caps every policy at every level (the fleet kill switch).
    pub mode_ceiling: Mode,
    /// When written.
    pub updated_at: DateTime<Utc>,
    /// Who wrote it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub by: Option<String>,
}

/// One attempt, attributed (the engine joins records with decisions and runs
/// [`crate::safety::attribute`]).
#[derive(Clone, Debug, PartialEq)]
pub struct AttemptOutcome {
    /// Workload key.
    pub key: WorkloadKey,
    /// Invocation id.
    pub invocation_id: String,
    /// Attempt number.
    pub attempt: u32,
    /// When the attempt started.
    pub at: DateTime<Utc>,
    /// Estimator `(id, version)` that decided it.
    pub estimator: Option<(String, String)>,
    /// Whether the attempt ran at a rightsizer-enforced request.
    pub enforced: bool,
    /// Whether it succeeded.
    pub succeeded: bool,
    /// S5 attribution (with `input_beyond_history = false`).
    pub attribution: Attribution,
    /// Whether the run's input exceeded every size in its history.
    pub beyond_history: bool,
    /// The input-size bucket.
    pub input_bucket: Option<String>,
    /// A Caller ladder died with the lease open past `lease_ttl`.
    pub lease_expired: bool,
}

/// Breaker thresholds (SPEC §2.3 S5).
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct BreakerParams {
    /// Per-estimator-version caused-failure rate (0.02).
    pub estimator_rate: f64,
    /// Minimum enforced decisions per version (50).
    pub estimator_min: usize,
    /// Fleet fast-burn rate (0.05).
    pub fast_rate: f64,
    /// Fast-burn window (1 h).
    pub fast_window: Duration,
    /// Fast-burn minimum caused failures (20).
    pub fast_min_failures: usize,
    /// Fleet slow-burn rate (0.005, the SLO).
    pub slow_rate: f64,
    /// Slow-burn window (30 d).
    pub slow_window: Duration,
    /// Slow-burn minimum enforced invocations, so one failure in a handful
    /// does not trip the fleet (200).
    pub slow_min_invocations: usize,
    /// Shadow fits before a per-workload breaker goes half-open (5).
    pub recovery_shadow_fits: u32,
}

impl Default for BreakerParams {
    fn default() -> Self {
        BreakerParams {
            estimator_rate: 0.02,
            estimator_min: 50,
            fast_rate: 0.05,
            fast_window: Duration::hours(1),
            fast_min_failures: 20,
            slow_rate: 0.005,
            slow_window: Duration::days(30),
            slow_min_invocations: 200,
            recovery_shadow_fits: 5,
        }
    }
}

/// One invocation, summarized over its attempts.
#[derive(Clone, Debug, PartialEq)]
pub struct InvocationSummary {
    /// Key.
    pub key: WorkloadKey,
    /// Invocation id.
    pub invocation_id: String,
    /// First attempt's start.
    pub at: DateTime<Utc>,
    /// Estimator of the first enforced attempt.
    pub estimator: Option<(String, String)>,
    /// Any attempt enforced.
    pub enforced: bool,
    /// Any attempt rightsizer-caused (beyond-history failures excluded).
    pub caused: bool,
    /// A caused failure beyond the history's sizes.
    pub caused_beyond_history: bool,
    /// Recovered: a later attempt succeeded and no lease expired.
    pub recovered: bool,
    /// Input bucket.
    pub input_bucket: Option<String>,
}

/// Group attempts into invocations.
pub fn summarize(outcomes: &[AttemptOutcome]) -> Vec<InvocationSummary> {
    let mut by: BTreeMap<(String, String), Vec<&AttemptOutcome>> = BTreeMap::new();
    for o in outcomes {
        by.entry((o.key.to_string(), o.invocation_id.clone())).or_default().push(o);
    }
    by.into_values()
        .map(|mut v| {
            v.sort_by_key(|o| o.attempt);
            let first = v[0];
            let caused_at: Vec<&&AttemptOutcome> = v.iter().filter(|o| o.attribution == Attribution::Caused).collect();
            let caused = caused_at.iter().any(|o| !o.beyond_history);
            let caused_beyond_history = caused_at.iter().any(|o| o.beyond_history);
            let last_caused = caused_at.iter().map(|o| o.attempt).max();
            let recovered = match last_caused {
                Some(a) => v.iter().any(|o| o.attempt > a && o.succeeded) && !v.iter().any(|o| o.lease_expired),
                None => true,
            };
            InvocationSummary {
                key: first.key.clone(),
                invocation_id: first.invocation_id.clone(),
                at: first.at,
                estimator: v.iter().find(|o| o.enforced).and_then(|o| o.estimator.clone()).or_else(|| first.estimator.clone()),
                enforced: v.iter().any(|o| o.enforced),
                caused,
                caused_beyond_history,
                recovered,
                input_bucket: first.input_bucket.clone(),
            }
        })
        .collect()
}

/// A breaker that should be tripped.
#[derive(Clone, Debug, PartialEq)]
pub struct TripProposal {
    /// Scope.
    pub scope: BreakerScope,
    /// Reason.
    pub reason: TripReason,
    /// Detail.
    pub detail: String,
}

/// Evaluate every breaker over attributed outcomes (SPEC §2.3 S5). The
/// caller trips the proposals that are not already open.
pub fn evaluate(outcomes: &[AttemptOutcome], now: DateTime<Utc>, p: &BreakerParams) -> Vec<TripProposal> {
    let inv = summarize(outcomes);
    let mut out = Vec::new();

    // Per-workload: one caused failure escalation did not recover.
    for s in &inv {
        if s.caused && !s.recovered {
            out.push(TripProposal {
                scope: BreakerScope::Workload { key: s.key.clone() },
                reason: TripReason::UnrecoveredFailure,
                detail: format!("invocation {} failed rightsizer-caused and was not recovered", s.invocation_id),
            });
        } else if s.caused_beyond_history && !s.recovered {
            let bucket = s.input_bucket.clone().unwrap_or_else(|| "beyond-history".into());
            out.push(TripProposal {
                scope: BreakerScope::InputBucket { key: s.key.clone(), bucket },
                reason: TripReason::UnrecoveredFailure,
                detail: format!("invocation {} beyond its history's input sizes failed", s.invocation_id),
            });
        }
    }

    // Per-estimator-version.
    let mut by_est: BTreeMap<(String, String), (usize, usize)> = BTreeMap::new();
    for s in inv.iter().filter(|s| s.enforced) {
        if let Some(e) = &s.estimator {
            let c = by_est.entry(e.clone()).or_default();
            c.0 += 1;
            c.1 += s.caused as usize;
        }
    }
    for ((id, version), (n, f)) in by_est {
        if n >= p.estimator_min && f as f64 / n as f64 > p.estimator_rate {
            out.push(TripProposal {
                scope: BreakerScope::EstimatorVersion { id, version },
                reason: TripReason::EstimatorRate,
                detail: format!("{f}/{n} enforced decisions failed rightsizer-caused"),
            });
        }
    }

    // Fleet.
    let window = |w: Duration| {
        let e: Vec<&InvocationSummary> = inv.iter().filter(|s| s.enforced && s.at > now - w && s.at <= now).collect();
        let f = e.iter().filter(|s| s.caused).count();
        (e.len(), f)
    };
    let (n, f) = window(p.fast_window);
    if f >= p.fast_min_failures && n > 0 && f as f64 / n as f64 > p.fast_rate {
        out.push(TripProposal { scope: BreakerScope::Fleet, reason: TripReason::FastBurn, detail: format!("{f}/{n} in {}", p.fast_window) });
    }
    let (n, f) = window(p.slow_window);
    if n >= p.slow_min_invocations && f as f64 / n as f64 > p.slow_rate {
        out.push(TripProposal { scope: BreakerScope::Fleet, reason: TripReason::SlowBurn, detail: format!("{f}/{n} in {}", p.slow_window) });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000 + secs, 0).unwrap()
    }
    fn key() -> WorkloadKey {
        WorkloadKey::parse("k8s://c/ns/job/x").unwrap()
    }
    fn trip(at: i64) -> Trip {
        Trip { v: 1, id: Ulid::from_parts(at as u64, 0), scope: BreakerScope::Workload { key: key() }, at: t(at), reason: TripReason::UnrecoveredFailure, detail: None }
    }

    #[test]
    fn open_while_any_trip_newer_than_latest_reset() {
        let mut o = BreakerObjects::default();
        assert!(!o.is_open());
        o.trips.push(trip(10));
        assert!(o.is_open());
        o.resets.push(Reset { v: 1, scope: BreakerScope::Workload { key: key() }, at: t(20), by: None });
        assert!(!o.is_open());
        o.trips.push(trip(15));
        assert!(!o.is_open(), "older than the reset");
        o.trips.push(trip(30));
        assert!(o.is_open());
        assert_eq!(state(&o, 0, 5), BreakerState::Open);
        assert_eq!(state(&o, 5, 5), BreakerState::HalfOpen);
        let mut f = BreakerObjects::default();
        f.trips.push(Trip { scope: BreakerScope::Fleet, ..trip(1) });
        assert_eq!(state(&f, 100, 5), BreakerState::Open, "only per-workload breakers half-open");
    }

    fn att(inv: &str, attempt: u32, at: i64, attribution: Attribution, succeeded: bool) -> AttemptOutcome {
        AttemptOutcome {
            key: key(),
            invocation_id: inv.into(),
            attempt,
            at: t(at),
            estimator: Some(("rules-sized".into(), "1".into())),
            enforced: true,
            succeeded,
            attribution,
            beyond_history: false,
            input_bucket: None,
            lease_expired: false,
        }
    }

    #[test]
    fn per_workload_trips_only_when_unrecovered() {
        let p = BreakerParams::default();
        let recovered = vec![att("a", 1, 0, Attribution::Caused, false), att("a", 2, 10, Attribution::NotCaused, true)];
        assert!(evaluate(&recovered, t(100), &p).is_empty());
        let unrecovered = vec![att("a", 1, 0, Attribution::Caused, false)];
        let props = evaluate(&unrecovered, t(100), &p);
        assert_eq!(props.len(), 1);
        assert_eq!(props[0].reason, TripReason::UnrecoveredFailure);
        let mut lease = recovered.clone();
        lease[0].lease_expired = true;
        assert_eq!(evaluate(&lease, t(100), &p).len(), 1, "an expired lease is unrecovered");
        let mut beyond = unrecovered.clone();
        beyond[0].beyond_history = true;
        beyond[0].input_bucket = Some("64Gi".into());
        let props = evaluate(&beyond, t(100), &p);
        assert!(matches!(props[0].scope, BreakerScope::InputBucket { .. }));
    }

    #[test]
    fn estimator_and_fleet_burn() {
        let p = BreakerParams::default();
        // 100 enforced invocations, 3 caused (recovered) → 3% > 2% for the version; fleet fast-burn needs ≥ 20.
        let mut v = Vec::new();
        for i in 0..100 {
            let inv = format!("i{i}");
            if i < 3 {
                v.push(att(&inv, 1, i, Attribution::Caused, false));
                v.push(att(&inv, 2, i + 1, Attribution::NotCaused, true));
            } else {
                v.push(att(&inv, 1, i, Attribution::NotCaused, true));
            }
        }
        let props = evaluate(&v, t(200), &p);
        assert!(props.iter().any(|x| x.reason == TripReason::EstimatorRate));
        assert!(!props.iter().any(|x| x.reason == TripReason::FastBurn));
        // 25 caused in 1 h out of 100 → fast burn.
        let mut w = Vec::new();
        for i in 0..100 {
            let inv = format!("j{i}");
            let a = if i < 25 { Attribution::Caused } else { Attribution::NotCaused };
            w.push(att(&inv, 1, i, a, i >= 25));
            if i < 25 {
                w.push(att(&inv, 2, i + 1, Attribution::NotCaused, true));
            }
        }
        let props = evaluate(&w, t(200), &p);
        assert!(props.iter().any(|x| x.reason == TripReason::FastBurn));
        // Slow burn: 2/300 over 30 d = 0.67% > 0.5%.
        let mut s = Vec::new();
        for i in 0..300 {
            let inv = format!("k{i}");
            let at = i * 3600;
            if i < 2 {
                s.push(att(&inv, 1, at, Attribution::Caused, false));
                s.push(att(&inv, 2, at + 1, Attribution::NotCaused, true));
            } else {
                s.push(att(&inv, 1, at, Attribution::NotCaused, true));
            }
        }
        let props = evaluate(&s, t(300 * 3600), &BreakerParams { estimator_min: 10_000, ..p });
        assert_eq!(props.len(), 1);
        assert_eq!(props[0].reason, TripReason::SlowBurn);
    }

    #[test]
    fn scope_paths() {
        assert_eq!(BreakerScope::Fleet.path(), "fleet");
        let w = BreakerScope::Workload { key: key() };
        assert_eq!(w.path(), format!("workload/k8s/c/ns/{}", key().hash()), "platform segment, like runs");
        assert_eq!(w.legacy_path(), Some(format!("workload/c/ns/{}", key().hash())));
        assert_eq!(w.read_paths().len(), 2);
        assert_eq!(BreakerScope::Fleet.legacy_path(), None);
        assert_eq!(BreakerScope::EstimatorVersion { id: "gbt".into(), version: "1/2".into() }.path(), "estimator/gbt/1_2");
        let j = serde_json::to_value(BreakerScope::Fleet).unwrap();
        assert_eq!(j, serde_json::json!({"kind": "fleet"}));
    }
}
