//! Safety checks: S3 recoverable reductions and S5 attribution (SPEC §2.3).
//!
//! - **S3(a) ladder**: the integration resubmits on failure and the ladder's
//!   final step reaches at least max(origin prior, largest observed successful
//!   request) within the ceiling. Only for enforced memory.
//! - **S3(b) evidence**: at least `min_history` measured successes (`Exact`
//!   or `AtMost`, trust ≥ `Authenticated` unless `allow_unverified`) whose
//!   peak × `scale` × headroom fits under the proposed request. Headroom 1.05,
//!   1.25 for `resolution_s > 1` or another digest, 1.5 for both; coarse peaks
//!   (`resolution_s ≥ 10`) 1.5 / 1.75 and they count half (so S3(b) needs
//!   `2 × min_history` of them).
//! - **S5 attribution** ([`attribute`]): which failures are rightsizer-caused.

use chrono::{DateTime, Duration, Utc};

use crate::types::{
    AttemptContext, Bytes, Censor, Confidence, Ladder, LadderDriver, PlatformRetry, Requests, RunRecord, RunStatus,
    Trust,
};

/// Resolution at or above which a peak is coarse (seconds).
pub const COARSE_RESOLUTION_S: f32 = 10.0;

/// S3(b) / floor headroom for one record (SPEC §2.3, §7.4).
pub fn headroom(resolution_s: Option<f32>, other_digest: bool) -> f64 {
    let r = resolution_s.unwrap_or(0.0);
    if r >= COARSE_RESOLUTION_S {
        if other_digest { 1.75 } else { 1.5 }
    } else if r > 1.0 {
        if other_digest { 1.5 } else { 1.25 }
    } else if other_digest {
        1.25
    } else {
        1.05
    }
}

/// Whether a peak is coarse (`resolution_s ≥ 10`).
pub fn is_coarse(resolution_s: Option<f32>) -> bool {
    resolution_s.is_some_and(|r| r >= COARSE_RESOLUTION_S)
}

/// `scale` (SPEC §7.4): `max(1, b_now / b_hist)` if both known, else 1.
pub fn scale(b_now: Option<u64>, b_hist: Option<u64>) -> f64 {
    match (b_now, b_hist) {
        (Some(n), Some(h)) if h > 0 => (n as f64 / h as f64).max(1.0),
        _ => 1.0,
    }
}

/// Whether a record's trust admits it for floors, anchors and S3(b) (SPEC §8.2).
pub fn trusted(rec: &RunRecord, allow_unverified: bool) -> bool {
    allow_unverified || rec.trust >= Trust::Authenticated
}

/// Whether `rec` has the same code digest as the current context.
pub fn same_digest(rec: &RunRecord, digest: Option<&str>) -> bool {
    rec.code_digest.as_deref() == digest
}

/// Parameters of the S3 checks.
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct S3Params {
    /// Measured successes needed (3).
    pub min_history: u32,
    /// `history.allow_unverified_for_reductions`.
    pub allow_unverified: bool,
}

impl Default for S3Params {
    fn default() -> Self {
        S3Params { min_history: 3, allow_unverified: false }
    }
}

/// The result of the S3(b) evidence check.
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct Evidence {
    /// Whether S3(b) holds.
    pub ok: bool,
    /// Weighted count of fitting successes (coarse count ½).
    pub weight: f64,
    /// Fitting successes.
    pub fitting: usize,
    /// Qualifying successes (fitting or not).
    pub qualifying: usize,
}

/// The peak × scale × headroom requirement a qualifying record puts on a request.
pub fn record_requirement(rec: &RunRecord, digest: Option<&str>, b_now: Option<u64>) -> Option<f64> {
    let peak = rec.memory_peak?;
    if !peak.value.is_finite() || peak.value < 0.0 {
        return None;
    }
    let h = headroom(peak.resolution_s, !same_digest(rec, digest));
    Some(peak.value * scale(b_now, rec.input_bytes()) * h)
}

/// Whether a record is a measured success usable as S3(b) evidence.
pub fn is_measured_success(rec: &RunRecord, allow_unverified: bool) -> bool {
    rec.status == RunStatus::Succeeded
        && rec.status_confidence == Confidence::Observed
        && trusted(rec, allow_unverified)
        && rec.memory_peak.is_some_and(|m| matches!(m.censor, Censor::Exact | Censor::AtMost))
}

/// S3(b): evidence that a request of `proposed` bytes is safe (SPEC §2.3).
pub fn s3b_evidence(history: &[RunRecord], digest: Option<&str>, b_now: Option<u64>, proposed: Bytes, p: &S3Params) -> Evidence {
    let mut weight = 0.0;
    let mut fitting = 0;
    let mut qualifying = 0;
    for rec in history.iter().filter(|r| is_measured_success(r, p.allow_unverified)) {
        let Some(req) = record_requirement(rec, digest, b_now) else { continue };
        qualifying += 1;
        if req <= proposed.0 as f64 {
            fitting += 1;
            weight += if is_coarse(rec.memory_peak.and_then(|m| m.resolution_s)) { 0.5 } else { 1.0 };
        }
    }
    Evidence { ok: weight + 1e-9 >= p.min_history as f64, weight, fitting, qualifying }
}

/// Whether a ladder counts for S3(a) (SPEC §7.2): a `Caller` ladder always;
/// an `Orchestrator` ladder only when `max_attempts` is known and
/// `max_attempts − 1 ≥ max_steps`.
pub fn ladder_counts(ladder: &Ladder, attempt: Option<&AttemptContext>) -> bool {
    if ladder.max_steps == 0 || ladder.memory_factor.is_nan() || ladder.memory_factor <= 1.0 {
        return false;
    }
    match ladder.driver {
        LadderDriver::Caller => true,
        LadderDriver::Orchestrator => {
            attempt.and_then(|a| a.max_attempts).is_some_and(|m| m.saturating_sub(1) >= ladder.max_steps)
        }
    }
}

/// The largest `requested` memory among successful runs.
pub fn largest_observed_success(history: &[RunRecord], allow_unverified: bool) -> Option<Bytes> {
    history
        .iter()
        .filter(|r| r.status == RunStatus::Succeeded && trusted(r, allow_unverified))
        .filter_map(|r| r.requested_memory())
        .max()
        .map(Bytes)
}

/// The ladder's `final_floor` = max(origin prior, largest observed success,
/// the adapter's `final_floor`) (SPEC §7.2).
pub fn final_floor(prior_memory: Option<Bytes>, history: &[RunRecord], ladder: &Ladder, allow_unverified: bool) -> Option<Bytes> {
    [prior_memory, largest_observed_success(history, allow_unverified), ladder.final_floor.memory].into_iter().flatten().max()
}

/// S3(a): the ladder counts, memory is enforced, and the final floor fits
/// within the ceiling (SPEC §2.3).
pub fn s3a_ladder(
    retry: &PlatformRetry,
    attempt: Option<&AttemptContext>,
    memory_enforced: bool,
    final_floor: Option<Bytes>,
    ceiling: Option<Bytes>,
) -> bool {
    let Some(l) = &retry.ladder else { return false };
    if !memory_enforced || !ladder_counts(l, attempt) {
        return false;
    }
    match (final_floor, ceiling) {
        (Some(f), Some(c)) => f <= c,
        _ => true,
    }
}

/// Which S3 clause permits a reduction.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum S3Via {
    /// The ladder.
    Ladder,
    /// History evidence.
    Evidence,
}

/// Attribution of a failed attempt (SPEC §2.3 S5).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Attribution {
    /// Rightsizer-caused.
    Caused,
    /// Not rightsizer-caused.
    NotCaused,
    /// Unclassified, still within `classification_window`.
    Pending,
}

/// What [`attribute`] needs beyond the record.
#[derive(Clone, Debug, PartialEq)]
pub struct AttributionContext {
    /// The origin prior's requests.
    pub origin_prior: Requests,
    /// A retry of the same invocation at or above the prior also failed.
    pub retry_at_or_above_prior_failed: bool,
    /// The run's input exceeded every input size in its history.
    pub input_beyond_history: bool,
    /// Now.
    pub now: DateTime<Utc>,
    /// How long a failure may stay unclassified (1 h).
    pub classification_window: Duration,
}

fn below(applied: Option<u64>, prior: Option<u64>) -> bool {
    matches!((applied, prior), (Some(a), Some(p)) if a < p)
}

/// Whether the enforced request was below the origin prior on an
/// incompressible axis (memory, ephemeral storage, shm, or GPU memory via a
/// smaller device).
pub fn reduced_incompressible(requested: &Requests, prior: &Requests) -> bool {
    let gpu_mem = |r: &Requests| r.gpu.and_then(|g| g.total_memory()).map(|b| b.0);
    below(requested.memory.map(|b| b.0), prior.memory.map(|b| b.0))
        || below(requested.ephemeral_storage.map(|b| b.0), prior.ephemeral_storage.map(|b| b.0))
        || below(requested.shm.map(|b| b.0), prior.shm.map(|b| b.0))
        || below(gpu_mem(requested), gpu_mem(prior))
}

/// Whether `rec`'s status is unclassified: `Unknown`, or any failure whose
/// status was only inferred (an `Inferred` OOM is never proof, SPEC §5.7).
pub fn is_unclassified(rec: &RunRecord) -> bool {
    rec.status == RunStatus::Unknown || (rec.status.is_failure() && rec.status_confidence == Confidence::Inferred)
}

/// S5 attribution of one attempt (SPEC §2.3).
///
/// - OOM-class (host OOM on reduced memory; GPU OOM on a smaller device) and
///   timeouts with any axis below the prior are caused; an eviction of a unit
///   whose memory was reduced is caused.
/// - Re-attributed as not caused when a retry at/above the prior also failed,
///   or the input exceeded every input size in history.
/// - An unclassified failure with an incompressible axis below the prior is
///   `Pending` within `classification_window`, then `Caused`.
pub fn attribute(rec: &RunRecord, ctx: &AttributionContext) -> Attribution {
    if !rec.status.is_failure() {
        return Attribution::NotCaused;
    }
    let Some(req) = rec.requested.as_ref().map(|r| &r.requests) else { return Attribution::NotCaused };
    let prior = rec.origin_prior.as_ref().unwrap_or(&ctx.origin_prior);
    let mem_reduced = below(req.memory.map(|b| b.0), prior.memory.map(|b| b.0));
    let cpu_reduced = below(req.cpu.map(|c| c.0 as u64), prior.cpu.map(|c| c.0 as u64));
    let gpu_reduced = {
        let g = |r: &Requests| r.gpu.and_then(|g| g.total_memory()).map(|b| b.0);
        below(g(req), g(prior))
    };
    let incompressible = reduced_incompressible(req, prior);

    if is_unclassified(rec) {
        if !incompressible {
            return Attribution::NotCaused;
        }
        let ended = rec.started_at + Duration::milliseconds((rec.duration_s.unwrap_or(0.0).max(0.0) * 1000.0) as i64);
        return if ctx.now - ended >= ctx.classification_window { Attribution::Caused } else { Attribution::Pending };
    }
    let caused = match rec.status {
        RunStatus::OomKilled => mem_reduced,
        RunStatus::GpuOom => gpu_reduced,
        RunStatus::Timeout => mem_reduced || cpu_reduced,
        RunStatus::Evicted => mem_reduced,
        _ => false,
    };
    if !caused || ctx.retry_at_or_above_prior_failed || ctx.input_beyond_history {
        Attribution::NotCaused
    } else {
        Attribution::Caused
    }
}

/// Whether the run's input exceeded every input size in `history` (S5
/// re-attribution). False when sizes are unknown or history is empty.
pub fn input_beyond_history(rec: &RunRecord, history: &[RunRecord]) -> bool {
    let Some(now) = rec.input_bytes() else { return false };
    let sizes: Vec<u64> =
        history.iter().filter(|h| h.invocation_id != rec.invocation_id).filter_map(|h| h.input_bytes()).collect();
    !sizes.is_empty() && sizes.iter().all(|&s| now > s)
}

/// Whether another attempt of the same invocation, requested at or above the
/// prior on every incompressible axis, also failed (S5 re-attribution).
pub fn retry_at_or_above_prior_failed(rec: &RunRecord, attempts: &[RunRecord], prior: &Requests) -> bool {
    attempts.iter().any(|a| {
        a.invocation_id == rec.invocation_id
            && a.attempt != rec.attempt
            && a.status.is_failure()
            && a.status != RunStatus::Preempted
            && a.requested.as_ref().is_some_and(|r| !reduced_incompressible(&r.requests, prior))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Measurement, OutcomeSource, ResourceSpec, WorkloadKey};

    pub(crate) fn rec(status: RunStatus, peak_gib: Option<f64>, req_gib: f64) -> RunRecord {
        RunRecord {
            v: 1,
            key: WorkloadKey::parse("k8s://c/ns/job/x").unwrap(),
            cluster: None,
            invocation_id: "inv".into(),
            attempt: 1,
            decision_id: None,
            started_at: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            inputs: vec![],
            code_digest: Some("d1".into()),
            requested: Some(ResourceSpec {
                requests: Requests { memory: Some(Bytes::from_gib_f64(req_gib)), ..Default::default() },
                ..Default::default()
            }),
            origin_prior: None,
            status,
            status_confidence: Confidence::Observed,
            memory_peak: peak_gib.map(|g| Measurement::exact(g * Bytes::GIB as f64)),
            cpu_mean: None,
            cpu_peak_window: None,
            cpu_throttled_frac: None,
            gpu_memory_peak: None,
            gpu_util_mean: None,
            t_peak_frac: None,
            duration_s: Some(60.0),
            staged_bytes: None,
            source: OutcomeSource::Watcher,
            trust: Trust::Verified,
        }
    }

    #[test]
    fn headroom_table() {
        assert_eq!(headroom(None, false), 1.05);
        assert_eq!(headroom(Some(1.0), false), 1.05);
        assert_eq!(headroom(Some(5.0), false), 1.25);
        assert_eq!(headroom(None, true), 1.25);
        assert_eq!(headroom(Some(5.0), true), 1.5);
        assert_eq!(headroom(Some(10.0), false), 1.5);
        assert_eq!(headroom(Some(60.0), true), 1.75);
    }

    #[test]
    fn scale_never_below_one() {
        assert_eq!(scale(Some(10), Some(20)), 1.0);
        assert_eq!(scale(Some(40), Some(20)), 2.0);
        assert_eq!(scale(None, Some(20)), 1.0);
        assert_eq!(scale(Some(40), Some(0)), 1.0);
    }

    #[test]
    fn s3b_needs_min_history_trusted_measured_successes() {
        let h: Vec<RunRecord> = (0..3).map(|_| rec(RunStatus::Succeeded, Some(2.0), 8.0)).collect();
        let p = S3Params::default();
        assert!(s3b_evidence(&h, Some("d1"), None, Bytes::gib(3), &p).ok);
        // 2 GiB × 1.05 = 2.1 GiB does not fit under 2 GiB.
        assert!(!s3b_evidence(&h, Some("d1"), None, Bytes::gib(2), &p).ok);
        // Too few.
        assert!(!s3b_evidence(&h[..2], Some("d1"), None, Bytes::gib(3), &p).ok);
        // Unverified excluded unless allowed.
        let mut u = h.clone();
        for r in &mut u {
            r.trust = Trust::Unverified;
        }
        assert!(!s3b_evidence(&u, Some("d1"), None, Bytes::gib(3), &p).ok);
        assert!(s3b_evidence(&u, Some("d1"), None, Bytes::gib(3), &S3Params { allow_unverified: true, ..p }).ok);
        // AtLeast peaks are not evidence.
        let mut a = h.clone();
        for r in &mut a {
            r.memory_peak.as_mut().unwrap().censor = Censor::AtLeast;
        }
        assert!(!s3b_evidence(&a, Some("d1"), None, Bytes::gib(3), &p).ok);
    }

    #[test]
    fn s3b_coarse_needs_double() {
        let mk = |n| -> Vec<RunRecord> {
            (0..n)
                .map(|_| {
                    let mut r = rec(RunStatus::Succeeded, Some(2.0), 8.0);
                    r.memory_peak.as_mut().unwrap().resolution_s = Some(60.0);
                    r
                })
                .collect()
        };
        let p = S3Params::default();
        // 2 GiB × 1.5 = 3 GiB.
        assert!(!s3b_evidence(&mk(5), Some("d1"), None, Bytes::gib(3), &p).ok);
        assert!(s3b_evidence(&mk(6), Some("d1"), None, Bytes::gib(3), &p).ok);
        assert!(!s3b_evidence(&mk(6), Some("d1"), None, Bytes::from_gib_f64(2.9), &p).ok);
    }

    #[test]
    fn s3b_scales_with_inputs() {
        let mut h: Vec<RunRecord> = (0..3).map(|_| rec(RunStatus::Succeeded, Some(2.0), 8.0)).collect();
        for r in &mut h {
            r.inputs = vec![crate::types::InputMeta::blob("x", 100)];
        }
        let p = S3Params::default();
        assert!(s3b_evidence(&h, Some("d1"), Some(100), Bytes::gib(3), &p).ok);
        assert!(!s3b_evidence(&h, Some("d1"), Some(200), Bytes::gib(3), &p).ok, "2× input needs 4.2 GiB");
    }

    #[test]
    fn ladder_rules() {
        let mut l = Ladder::caller();
        assert!(ladder_counts(&l, None));
        l.driver = LadderDriver::Orchestrator;
        assert!(!ladder_counts(&l, None));
        let a = AttemptContext { v: 1, invocation_id: "i".into(), attempt: 1, max_attempts: Some(3) };
        assert!(ladder_counts(&l, Some(&a)));
        let a2 = AttemptContext { max_attempts: Some(2), ..a };
        assert!(!ladder_counts(&l, Some(&a2)));
        let retry = PlatformRetry { same_size: 0, ladder: Some(Ladder::caller()) };
        assert!(s3a_ladder(&retry, None, true, Some(Bytes::gib(8)), Some(Bytes::gib(16))));
        assert!(!s3a_ladder(&retry, None, true, Some(Bytes::gib(32)), Some(Bytes::gib(16))));
        assert!(!s3a_ladder(&retry, None, false, Some(Bytes::gib(8)), Some(Bytes::gib(16))), "unenforced memory: (b) only");
        assert!(!s3a_ladder(&PlatformRetry::default(), None, true, None, None));
    }

    #[test]
    fn final_floor_takes_largest_success() {
        let h = vec![rec(RunStatus::Succeeded, Some(2.0), 12.0), rec(RunStatus::OomKilled, None, 32.0)];
        assert_eq!(final_floor(Some(Bytes::gib(8)), &h, &Ladder::caller(), false), Some(Bytes::gib(12)));
    }

    fn actx() -> AttributionContext {
        AttributionContext {
            origin_prior: Requests { memory: Some(Bytes::gib(8)), cpu: Some(crate::types::MilliCpu(2000)), ..Default::default() },
            retry_at_or_above_prior_failed: false,
            input_beyond_history: false,
            now: DateTime::from_timestamp(1_700_000_000 + 7200, 0).unwrap(),
            classification_window: Duration::hours(1),
        }
    }

    #[test]
    fn attribution_rules() {
        let c = actx();
        assert_eq!(attribute(&rec(RunStatus::OomKilled, None, 4.0), &c), Attribution::Caused);
        assert_eq!(attribute(&rec(RunStatus::OomKilled, None, 8.0), &c), Attribution::NotCaused, "at the prior");
        assert_eq!(attribute(&rec(RunStatus::Evicted, None, 4.0), &c), Attribution::Caused);
        assert_eq!(attribute(&rec(RunStatus::Failed, None, 4.0), &c), Attribution::NotCaused, "app error");
        assert_eq!(attribute(&rec(RunStatus::Preempted, None, 4.0), &c), Attribution::NotCaused);
        assert_eq!(attribute(&rec(RunStatus::Succeeded, None, 4.0), &c), Attribution::NotCaused);
        let re = AttributionContext { retry_at_or_above_prior_failed: true, ..c.clone() };
        assert_eq!(attribute(&rec(RunStatus::OomKilled, None, 4.0), &re), Attribution::NotCaused);
        let big = AttributionContext { input_beyond_history: true, ..c.clone() };
        assert_eq!(attribute(&rec(RunStatus::OomKilled, None, 4.0), &big), Attribution::NotCaused);
    }

    #[test]
    fn unclassified_failures_count_after_the_window() {
        let mut r = rec(RunStatus::Unknown, None, 4.0);
        let c = actx();
        assert_eq!(attribute(&r, &c), Attribution::Caused);
        let early = AttributionContext { now: r.started_at + Duration::minutes(10), ..c.clone() };
        assert_eq!(attribute(&r, &early), Attribution::Pending);
        // Inferred OOM is unclassified, not proof.
        r.status = RunStatus::OomKilled;
        r.status_confidence = Confidence::Inferred;
        assert_eq!(attribute(&r, &early), Attribution::Pending);
        // Not below the prior: never caused.
        let at_prior = rec(RunStatus::Unknown, None, 8.0);
        assert_eq!(attribute(&at_prior, &c), Attribution::NotCaused);
    }

    #[test]
    fn reattribution_helpers() {
        let mut a = rec(RunStatus::OomKilled, None, 4.0);
        let mut b = rec(RunStatus::OomKilled, None, 8.0);
        b.attempt = 2;
        let prior = Requests { memory: Some(Bytes::gib(8)), ..Default::default() };
        assert!(retry_at_or_above_prior_failed(&a, &[a.clone(), b.clone()], &prior));
        b.status = RunStatus::Succeeded;
        assert!(!retry_at_or_above_prior_failed(&a, &[a.clone(), b], &prior));
        let mut h = rec(RunStatus::Succeeded, Some(1.0), 8.0);
        h.invocation_id = "old".into();
        h.inputs = vec![crate::types::InputMeta::blob("x", 10)];
        a.inputs = vec![crate::types::InputMeta::blob("x", 20)];
        assert!(input_beyond_history(&a, &[h.clone()]));
        a.inputs = vec![crate::types::InputMeta::blob("x", 5)];
        assert!(!input_beyond_history(&a, &[h]));
    }
}
