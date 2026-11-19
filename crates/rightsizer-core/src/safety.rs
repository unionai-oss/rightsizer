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
//!   `2 × min_history` of them). **Vetoes** (refinement round): no
//!   qualifying success among the last K (`history_limit`) runs may need more
//!   than the proposal (peak × scale × headroom), and no OOM-class run's
//!   lower bound × scale may exceed it ([`Veto`]).
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

/// Whether a record's trust admits it for floors, anchors and S3(b) (SPEC
/// §8.2). Reads [`RunRecord::effective_trust`]: a claim its source cannot
/// carry (a `self_report` claiming `verified`) counts at the source's ceiling.
pub fn trusted(rec: &RunRecord, allow_unverified: bool) -> bool {
    allow_unverified || rec.effective_trust() >= Trust::Authenticated
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
    /// K of the vetoes: the newest K runs of the key (`decision.history_limit`, 32).
    pub recent_k: usize,
}

impl Default for S3Params {
    fn default() -> Self {
        S3Params { min_history: 3, allow_unverified: false, recent_k: 32 }
    }
}

/// Why S3(b) refused a reduction although enough successes fit.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum Veto {
    /// A qualifying success among the last K runs needs more than the
    /// proposal (its peak × scale × headroom, bytes).
    Success {
        /// The requirement, bytes.
        requirement: f64,
    },
    /// An OOM-class run among the last K proves demand above the proposal
    /// (its `AtLeast` bound × scale, bytes).
    Oom {
        /// The bound × scale, bytes.
        bound: f64,
    },
}

impl std::fmt::Display for Veto {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let gib = |b: f64| b / Bytes::GIB as f64;
        match self {
            Veto::Success { requirement } => write!(f, "a recent success needs {:.2}Gi", gib(*requirement)),
            Veto::Oom { bound } => write!(f, "a recent OOM proves demand ≥ {:.2}Gi", gib(*bound)),
        }
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
    /// The largest veto, if any (then `ok` is false).
    pub veto: Option<Veto>,
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

/// The memory lower bound an OOM-class record proves (SPEC §5.7): its peak
/// when sampled (an OOM's peak is a lower bound whatever the importer
/// wrote), else the limit (or request) it was killed at. `None` for other
/// statuses.
pub fn oom_memory_bound(rec: &RunRecord) -> Option<f64> {
    if rec.status != RunStatus::OomKilled {
        return None;
    }
    if let Some(m) = rec.memory_peak.filter(|m| m.value.is_finite() && m.value > 0.0) {
        return Some(m.value);
    }
    let spec = rec.requested.as_ref()?;
    spec.limits.memory.or(spec.requests.memory).filter(|b| b.0 > 0).map(|b| b.0 as f64)
}

fn bigger(a: Option<Veto>, b: Veto) -> Option<Veto> {
    let size = |v: &Veto| match v {
        Veto::Success { requirement } => *requirement,
        Veto::Oom { bound } => *bound,
    };
    match a {
        Some(x) if size(&x) >= size(&b) => Some(x),
        _ => Some(b),
    }
}

/// S3(b): evidence that a request of `proposed` bytes is safe (SPEC §2.3).
///
/// Allowed only if (i) at least `min_history` qualifying successes fit
/// (coarse ones count ½), (ii) no qualifying success among the newest
/// `recent_k` runs needs more than `proposed` (peak × scale × headroom), and
/// (iii) no `OomKilled` run among them has a lower bound
/// ([`oom_memory_bound`]) × scale above `proposed`. Vetoes ignore trust: a
/// planted record can only make the rule more conservative.
pub fn s3b_evidence(history: &[RunRecord], digest: Option<&str>, b_now: Option<u64>, proposed: Bytes, p: &S3Params) -> Evidence {
    let mut weight = 0.0;
    let mut fitting = 0;
    let mut qualifying = 0;
    let mut veto = None;
    let cap = proposed.0 as f64;
    for (i, rec) in history.iter().enumerate() {
        let recent = i < p.recent_k;
        if recent && let Some(bound) = oom_memory_bound(rec) {
            let b = bound * scale(b_now, rec.input_bytes());
            if b > cap {
                veto = bigger(veto, Veto::Oom { bound: b });
            }
        }
        if !is_measured_success(rec, p.allow_unverified) {
            continue;
        }
        let Some(req) = record_requirement(rec, digest, b_now) else { continue };
        qualifying += 1;
        if req <= cap {
            fitting += 1;
            weight += if is_coarse(rec.memory_peak.and_then(|m| m.resolution_s)) { 0.5 } else { 1.0 };
        } else if recent {
            veto = bigger(veto, Veto::Success { requirement: req });
        }
    }
    Evidence { ok: veto.is_none() && weight + 1e-9 >= p.min_history as f64, weight, fitting, qualifying, veto }
}

/// S3(b) on the GPU-memory axis: a smaller device (a GPU type change) is an
/// incompressible reduction, so it needs the same evidence as memory, on
/// `gpu_memory_peak`: measured successes whose GPU peak (`Exact` or
/// `AtMost`) × `scale` × headroom (by resolution and digest) fits the
/// device; coarse peaks count ½ (SPEC §2.3). The same vetoes apply: a recent
/// qualifying success that does not fit, or a recent `GpuOom` whose bound
/// ([`RunRecord::gpu_oom_bound`]: the GPU peak, else the requested device's
/// memory) × scale exceeds the device.
pub fn s3b_gpu_evidence(history: &[RunRecord], digest: Option<&str>, b_now: Option<u64>, device: Bytes, p: &S3Params) -> Evidence {
    let mut weight = 0.0;
    let mut fitting = 0;
    let mut qualifying = 0;
    let mut veto = None;
    let cap = device.0 as f64;
    for (i, rec) in history.iter().enumerate() {
        let recent = i < p.recent_k;
        if recent && let Some(bound) = rec.gpu_oom_bound() {
            let b = bound * scale(b_now, rec.input_bytes());
            if b > cap {
                veto = bigger(veto, Veto::Oom { bound: b });
            }
        }
        if !(rec.status == RunStatus::Succeeded && rec.status_confidence == Confidence::Observed && trusted(rec, p.allow_unverified)) {
            continue;
        }
        let Some(peak) = rec.gpu_memory_peak.filter(|m| matches!(m.censor, Censor::Exact | Censor::AtMost)) else { continue };
        if !peak.value.is_finite() || peak.value < 0.0 {
            continue;
        }
        qualifying += 1;
        let req = peak.value * scale(b_now, rec.input_bytes()) * headroom(peak.resolution_s, !same_digest(rec, digest));
        if req <= cap {
            fitting += 1;
            weight += if is_coarse(peak.resolution_s) { 0.5 } else { 1.0 };
        } else if recent {
            veto = bigger(veto, Veto::Success { requirement: req });
        }
    }
    Evidence { ok: veto.is_none() && weight + 1e-9 >= p.min_history as f64, weight, fitting, qualifying, veto }
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

    /// Refinement round (item 2): readers grant a record at most what its
    /// source can carry.
    #[test]
    fn trust_matrix_caps_claims_by_source() {
        use crate::types::OutcomeSource as S;
        let cases = [
            (S::SelfReport, Trust::Verified, Trust::Authenticated),
            (S::SelfReport, Trust::Authenticated, Trust::Authenticated),
            (S::SelfReport, Trust::Unverified, Trust::Unverified),
            (S::Cgroup, Trust::Verified, Trust::Authenticated),
            (S::RayState, Trust::Verified, Trust::Authenticated),
            (S::Watcher, Trust::Verified, Trust::Verified),
            (S::Sacct, Trust::Verified, Trust::Verified),
            (S::FlyteRemote, Trust::Verified, Trust::Verified),
            (S::Prometheus, Trust::Unverified, Trust::Unverified),
            (S::Importer { platform: "aws".into() }, Trust::Verified, Trust::Verified),
        ];
        for (src, claim, want) in cases {
            let mut r = rec(RunStatus::Succeeded, Some(2.0), 8.0);
            r.source = src.clone();
            r.trust = claim;
            assert_eq!(r.effective_trust(), want, "{src:?} claiming {claim:?}");
            assert_eq!(trusted(&r, false), want >= Trust::Authenticated);
        }
        // Readers use the effective trust everywhere they gate on it.
        let mut h: Vec<RunRecord> = (0..3).map(|_| rec(RunStatus::Succeeded, Some(2.0), 8.0)).collect();
        for r in &mut h {
            r.source = S::SelfReport;
            r.trust = Trust::Unverified;
        }
        assert!(!s3b_evidence(&h, Some("d1"), None, Bytes::gib(3), &S3Params::default()).ok);
    }

    /// Refinement round (item 6): three old fitting runs no longer authorize
    /// a reduction past a newer success that peaked above the proposal.
    #[test]
    fn s3b_vetoed_by_a_recent_success_above_the_proposal() {
        let p = S3Params::default();
        let mut h: Vec<RunRecord> = (0..3).map(|_| rec(RunStatus::Succeeded, Some(2.0), 8.0)).collect();
        assert!(s3b_evidence(&h, Some("d1"), None, Bytes::gib(3), &p).ok);
        // Newest first: a same-digest success that peaked at 6 GiB.
        h.insert(0, rec(RunStatus::Succeeded, Some(6.0), 8.0));
        let e = s3b_evidence(&h, Some("d1"), None, Bytes::gib(3), &p);
        assert!(!e.ok && e.fitting == 3, "{e:?}");
        assert!(matches!(e.veto, Some(Veto::Success { requirement }) if (requirement - 6.0 * 1.05 * Bytes::GIB as f64).abs() < 1.0));
        // A proposal that fits the big run too is allowed.
        assert!(s3b_evidence(&h, Some("d1"), None, Bytes::from_gib_f64(6.4), &p).ok);
        // Outside the last K the old run no longer vetoes.
        let old = S3Params { recent_k: 0, ..p };
        assert!(s3b_evidence(&h, Some("d1"), None, Bytes::gib(3), &old).ok);
        // Another digest's success vetoes too (with its larger headroom).
        let mut d = h.clone();
        d[0].code_digest = Some("d0".into());
        d[0].memory_peak = Some(Measurement::exact(2.5 * Bytes::GIB as f64));
        assert!(!s3b_evidence(&d, Some("d1"), None, Bytes::gib(3), &p).ok, "2.5 × 1.25 > 3");
    }

    #[test]
    fn s3b_vetoed_by_a_recent_oom_above_the_proposal() {
        let p = S3Params::default();
        let mut h: Vec<RunRecord> = (0..4).map(|_| rec(RunStatus::Succeeded, Some(2.0), 8.0)).collect();
        // An OOM at a 4 GiB limit (no sampled peak): demand ≥ 4 GiB.
        let mut oom = rec(RunStatus::OomKilled, None, 4.0);
        oom.trust = Trust::Unverified; // vetoes ignore trust
        h.insert(1, oom.clone());
        let e = s3b_evidence(&h, Some("d1"), None, Bytes::gib(3), &p);
        assert!(!e.ok && matches!(e.veto, Some(Veto::Oom { .. })), "{e:?}");
        assert!(s3b_evidence(&h, Some("d1"), None, Bytes::gib(5), &p).ok);
        // A sampled AtLeast peak is the bound; scale applies.
        let mut o2 = rec(RunStatus::OomKilled, Some(2.0), 16.0);
        o2.memory_peak.as_mut().unwrap().censor = Censor::AtLeast;
        o2.inputs = vec![crate::types::InputMeta::blob("x", 100)];
        let mut h2: Vec<RunRecord> = (0..3).map(|_| rec(RunStatus::Succeeded, Some(1.0), 8.0)).collect();
        h2.push(o2);
        assert!(s3b_evidence(&h2, Some("d1"), Some(100), Bytes::gib(3), &p).ok, "2 GiB bound fits 3 GiB");
        assert!(!s3b_evidence(&h2, Some("d1"), Some(200), Bytes::gib(3), &p).ok, "2× input: bound 4 GiB");
    }

    #[test]
    fn s3b_gpu_vetoes() {
        use crate::types::{AcceleratorId, GpuShape};
        let p = S3Params::default();
        let a100 = AcceleratorId::A100_80G;
        let gpu_rec = |status, peak_gib: Option<f64>| {
            let mut r = rec(status, Some(1.0), 8.0);
            r.gpu_memory_peak = peak_gib.map(|g| Measurement::exact(g * Bytes::GIB as f64));
            r.requested.as_mut().unwrap().requests.gpu = Some(GpuShape { accel: a100, count: 1 });
            r
        };
        let mut h: Vec<RunRecord> = (0..3).map(|_| gpu_rec(RunStatus::Succeeded, Some(10.0))).collect();
        assert!(s3b_gpu_evidence(&h, Some("d1"), None, Bytes::gib(22), &p).ok);
        // A GPU OOM with no sampled peak proves demand ≥ the requested device's memory.
        h.insert(0, gpu_rec(RunStatus::GpuOom, None));
        let e = s3b_gpu_evidence(&h, Some("d1"), None, Bytes::gib(22), &p);
        assert!(!e.ok && matches!(e.veto, Some(Veto::Oom { .. })), "{e:?}");
        // A recent success that needs more than the device.
        let mut h2: Vec<RunRecord> = (0..3).map(|_| gpu_rec(RunStatus::Succeeded, Some(10.0))).collect();
        h2.insert(0, gpu_rec(RunStatus::Succeeded, Some(30.0)));
        assert!(matches!(s3b_gpu_evidence(&h2, Some("d1"), None, Bytes::gib(22), &p).veto, Some(Veto::Success { .. })));
    }

    mod props {
        use super::*;
        use proptest::prelude::*;

        fn arb_rec() -> impl Strategy<Value = RunRecord> {
            (
                prop_oneof![Just(RunStatus::Succeeded), Just(RunStatus::Succeeded), Just(RunStatus::OomKilled), Just(RunStatus::GpuOom), Just(RunStatus::Failed)],
                prop::option::of(0.1f64..20.0),
                prop::option::of(0.1f64..40.0),
                1.0f64..32.0,
                0u8..3,
                prop::option::of(prop_oneof![Just(0.5f32), Just(5.0f32), Just(60.0f32)]),
                any::<bool>(),
                prop::option::of(1u64..1000),
                0u8..3,
            )
                .prop_map(|(status, peak, gpu_peak, req, censor, res, other_digest, input, trust)| {
                    let mut r = rec(status, peak, req);
                    if let Some(m) = r.memory_peak.as_mut() {
                        m.censor = [Censor::Exact, Censor::AtLeast, Censor::AtMost][censor as usize];
                        m.resolution_s = res;
                    }
                    r.gpu_memory_peak = gpu_peak.map(|g| Measurement::exact(g * Bytes::GIB as f64));
                    r.requested.as_mut().unwrap().requests.gpu =
                        Some(crate::types::GpuShape { accel: crate::types::AcceleratorId::L4_24G, count: 1 });
                    if other_digest {
                        r.code_digest = Some("d0".into());
                    }
                    r.inputs = input.map(|b| vec![crate::types::InputMeta::blob("x", b)]).unwrap_or_default();
                    r.trust = [Trust::Unverified, Trust::Authenticated, Trust::Verified][trust as usize];
                    r
                })
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(256))]

            /// When S3(b) allows a proposal, nothing among the last K says
            /// demand may exceed it: every qualifying success needs at most
            /// the proposal, and no OOM bound × scale exceeds it.
            #[test]
            fn allowed_means_no_recent_evidence_above(h in prop::collection::vec(arb_rec(), 0..12), gib in 0.5f64..48.0, k in 0usize..14, b_now in prop::option::of(1u64..2000)) {
                let p = S3Params { recent_k: k, ..S3Params::default() };
                let proposed = Bytes::from_gib_f64(gib);
                for gpu in [false, true] {
                    let e = if gpu { s3b_gpu_evidence(&h, Some("d1"), b_now, proposed, &p) } else { s3b_evidence(&h, Some("d1"), b_now, proposed, &p) };
                    if !e.ok {
                        continue;
                    }
                    prop_assert!(e.veto.is_none());
                    for r in h.iter().take(k) {
                        let sc = scale(b_now, r.input_bytes());
                        let oom = if gpu { r.gpu_oom_bound() } else { oom_memory_bound(r) };
                        if let Some(b) = oom {
                            prop_assert!(b * sc <= proposed.0 as f64, "OOM bound above an allowed proposal");
                        }
                        if !gpu && is_measured_success(r, p.allow_unverified) && let Some(req) = record_requirement(r, Some("d1"), b_now) {
                            prop_assert!(req <= proposed.0 as f64, "recent success needs more than an allowed proposal");
                        }
                    }
                }
            }

            /// Monotone in the proposal: allowed at x ⇒ allowed at any y ≥ x.
            #[test]
            fn monotone_in_the_proposal(h in prop::collection::vec(arb_rec(), 0..12), a in 0.5f64..48.0, b in 0.5f64..48.0) {
                let (lo, hi) = (a.min(b), a.max(b));
                let p = S3Params::default();
                if s3b_evidence(&h, Some("d1"), None, Bytes::from_gib_f64(lo), &p).ok {
                    prop_assert!(s3b_evidence(&h, Some("d1"), None, Bytes::from_gib_f64(hi), &p).ok);
                }
                if s3b_gpu_evidence(&h, Some("d1"), None, Bytes::from_gib_f64(lo), &p).ok {
                    prop_assert!(s3b_gpu_evidence(&h, Some("d1"), None, Bytes::from_gib_f64(hi), &p).ok);
                }
            }

            /// Adding a newer run (at the front) never turns a refusal into
            /// an allowance unless it is itself a fitting qualifying success.
            #[test]
            fn a_newer_non_fitting_run_never_unlocks(h in prop::collection::vec(arb_rec(), 0..10), new in arb_rec(), gib in 0.5f64..48.0) {
                let p = S3Params::default();
                let proposed = Bytes::from_gib_f64(gib);
                let before = s3b_evidence(&h, Some("d1"), None, proposed, &p).ok;
                let mut h2 = h.clone();
                h2.insert(0, new.clone());
                let after = s3b_evidence(&h2, Some("d1"), None, proposed, &p).ok;
                let fits = is_measured_success(&new, false) && record_requirement(&new, Some("d1"), None).is_some_and(|r| r <= proposed.0 as f64);
                if !before && after {
                    prop_assert!(fits);
                }
            }
        }
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
