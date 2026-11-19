//! Counterfactual replay: `rsz shadow` (SPEC §12.2) and `rsz audit` (§12.3)
//! share this code path, so on the same history snapshot and configuration
//! they give the same decisions.
//!
//! For each key, each past run is decided **as of its start**, using only
//! runs that **completed** before it started, by the in-process chain. The
//! replay is a recommend-mode steady state: history was produced at the
//! prior, so the trust-region anchor stays at the prior. Per run:
//!
//! - **would-fail** (memory, censoring-aware): an `Exact` peak above the
//!   decision fails; an `AtLeast` peak (e.g. an OOM at the prior) that does
//!   not exceed the decision, an `AtMost` peak above it, or no peak, is
//!   **unknown**, never "fit";
//! - **simulated breakers**: a would-fail run opens the key's breaker; while
//!   open, later runs save nothing, until `recovery_shadow_fits` fitting
//!   runs close it again;
//! - **ladder retries are priced**: a would-fail run behind a ladder pays
//!   its burnt attempts and the step that fits; without one it pays the
//!   failed attempt and the rerun at the prior;
//! - **would-save**: GiB·h, core·h and dollars at the platform's prices
//!   (`usage_floor` platforms bill `max(request, peak)`, so savings are
//!   `E[max(prior, D)] − E[max(decision, D)]`), with a 95% bootstrap
//!   interval over runs (seeded);
//! - holds and advisories are counted; **readiness** comes from the decision
//!   as of now: `ready` (S3(b) holds, or nothing to reduce), `ladder-only`,
//!   or `not-ready` with the number of measured runs still needed.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use rightsizer_core::policy::Effective;
use rightsizer_core::safety::{S3Params, is_coarse, s3b_evidence};
use rightsizer_core::stats::SplitMix64;
use rightsizer_core::{
    Bytes, Censor, CodeInfo, Constraints, Decision, GpuShape, ManifestFeatures, MilliCpu, Mode, PlatformRef,
    PlatformState, PriceModel, Prior, RequestModel, ResourceSpec, RunRecord, RunStatus, UnitId, WorkloadContext, WorkloadKey,
};
use serde::{Deserialize, Serialize};

use crate::engine::{DecideOpts, Engine, Resolved};
use crate::{DecideRequest, Policy};

/// What to replay and how.
#[derive(Clone, Debug)]
pub struct ReplayOptions {
    /// Runs started at or after this are decided; earlier runs are history only.
    pub since: Option<DateTime<Utc>>,
    /// Static platform facts per key; default `PlatformRef::new(key.platform)`.
    pub platform: Option<PlatformRef>,
    /// Shapes and prices.
    pub state: PlatformState,
    /// Constraints.
    pub constraints: Constraints,
    /// Policy (mode is forced to `recommend`).
    pub policy: Policy,
    /// Bootstrap resamples (1000).
    pub bootstrap: usize,
    /// Bootstrap seed.
    pub seed: u64,
    /// Keep per-run rows in the report.
    pub per_run: bool,
}

impl Default for ReplayOptions {
    fn default() -> Self {
        ReplayOptions {
            since: None,
            platform: None,
            state: PlatformState::default(),
            constraints: Constraints::default(),
            policy: Policy::default(),
            bootstrap: 1000,
            seed: 0x5EED,
            per_run: false,
        }
    }
}

/// How a run fares against the decision.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Fit {
    /// The measured peak fits.
    Fit,
    /// The measured peak exceeds the decision.
    Fail,
    /// Censored or unmeasured: cannot tell.
    Unknown,
}

/// Readiness verdict (SPEC §12.2).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "verdict", rename_all = "kebab-case")]
pub enum Readiness {
    /// S3(b) holds now (or there is nothing to reduce).
    Ready,
    /// Enforceable only with escalation (S3(a)).
    LadderOnly,
    /// Needs this many more measured runs.
    NotReady {
        /// Measured successes still needed.
        more_runs: u32,
    },
}

impl std::fmt::Display for Readiness {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Readiness::Ready => f.write_str("ready"),
            Readiness::LadderOnly => f.write_str("ladder-only"),
            Readiness::NotReady { more_runs } => write!(f, "not-ready ({more_runs} more runs)"),
        }
    }
}

/// One replayed run.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RunReplay {
    /// Invocation.
    pub invocation_id: String,
    /// Attempt.
    pub attempt: u32,
    /// Start.
    pub started_at: DateTime<Utc>,
    /// History records the decision saw.
    pub history: usize,
    /// Prior memory.
    pub prior_memory: Option<u64>,
    /// Decided (would-be applied) memory.
    pub decided_memory: Option<u64>,
    /// Recommended memory.
    pub recommended_memory: Option<u64>,
    /// Measured peak.
    pub peak: Option<f64>,
    /// Fit.
    pub fit: Fit,
    /// The simulated breaker was open: no savings counted.
    pub breaker_open: bool,
    /// GiB·h saved.
    pub saved_gib_h: f64,
    /// core·h saved.
    pub saved_core_h: f64,
    /// Dollars saved.
    pub saved_dollars: f64,
}

/// A 95% interval.
#[derive(Copy, Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Interval {
    /// 2.5th percentile.
    pub lo: f64,
    /// 97.5th percentile.
    pub hi: f64,
}

/// Would-save totals.
#[derive(Copy, Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Savings {
    /// GiB·h.
    pub gib_h: f64,
    /// Interval on GiB·h.
    pub gib_h_ci: Interval,
    /// core·h.
    pub core_h: f64,
    /// Dollars.
    pub dollars: f64,
    /// Interval on dollars.
    pub dollars_ci: Interval,
}

/// One key's report.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KeyReport {
    /// Key.
    pub key: WorkloadKey,
    /// Records of the key (history included).
    pub records: usize,
    /// Runs decided.
    pub runs: usize,
    /// The latest prior's memory.
    pub prior_memory: Option<u64>,
    /// Median decided memory.
    pub decided_memory_p50: Option<u64>,
    /// Decisions that changed memory from the prior.
    pub changed: usize,
    /// Would-save.
    pub would_save: Savings,
    /// Runs whose peak exceeds the decision.
    pub would_fail: usize,
    /// Runs whose fit is unknown (censored or unmeasured).
    pub unknown: usize,
    /// Holds by `axis:reason`.
    pub holds: BTreeMap<String, usize>,
    /// Advisories by kind.
    pub advisories: BTreeMap<String, usize>,
    /// Readiness.
    pub readiness: Readiness,
    /// Whether dollars use real prices (node shapes or rates were given).
    pub prices_known: bool,
    /// Decision sources by kind.
    pub sources: BTreeMap<String, usize>,
    /// Per-run rows ([`ReplayOptions::per_run`]).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub per_run: Vec<RunReplay>,
}

/// The whole report.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReplayReport {
    /// Replayed from.
    pub since: Option<DateTime<Utc>>,
    /// Keys, sorted.
    pub keys: Vec<KeyReport>,
    /// Totals.
    pub total: Savings,
    /// Total runs decided.
    pub runs: usize,
    /// Total would-fail.
    pub would_fail: usize,
    /// Total unknown.
    pub unknown: usize,
}

fn gib(b: u64) -> f64 {
    b as f64 / Bytes::GIB as f64
}

/// $/h of a request under the platform's price model (the cost model's
/// pricing without failure terms); `peak` feeds `usage_floor` billing.
pub fn price_per_hour(state: &PlatformState, mem: Option<u64>, cpu: Option<MilliCpu>, gpu: Option<GpuShape>, peak: Option<f64>) -> f64 {
    let mem_b = mem.unwrap_or(0) as f64;
    let cores = cpu.map(|c| c.as_cores()).unwrap_or(0.0);
    let gpu_price = |g: GpuShape, rates: Option<&rightsizer_core::Rates>| {
        g.count as f64 * rates.and_then(|r| r.gpu_hr.get(&g.accel).copied()).unwrap_or_else(|| rightsizer_core::cost::default_gpu_price(g.accel))
    };
    match &state.price {
        PriceModel::Linear { rates } => cores * rates.cpu_core_hr + gib(mem_b as u64) * rates.mem_gib_hr + gpu.map(|g| gpu_price(g, Some(rates))).unwrap_or(0.0),
        PriceModel::UsageFloor { rates } => {
            let billed = mem_b.max(peak.unwrap_or(0.0));
            cores * rates.cpu_core_hr + billed / Bytes::GIB as f64 * rates.mem_gib_hr + gpu.map(|g| gpu_price(g, Some(rates))).unwrap_or(0.0)
        }
        PriceModel::Instance { usage } => {
            let shapes: &[rightsizer_core::Shape] = match &state.request_model {
                RequestModel::Catalog { shapes } | RequestModel::Snap { shapes, .. } => shapes,
                _ => &[],
            };
            shapes
                .iter()
                .filter(|s| s.usable().0 as f64 >= mem_b && cpu.is_none_or(|c| s.cpu >= c) && s.gpu == gpu)
                .map(|s| s.price_for(usage.as_deref()))
                .fold(None, |a: Option<f64>, b| Some(a.map_or(b, |a| a.min(b))))
                .unwrap_or_else(|| packed(state, mem_b, cores, gpu))
        }
        PriceModel::Packed => packed(state, mem_b, cores, gpu),
    }
}

fn packed(state: &PlatformState, mem_b: f64, cores: f64, gpu: Option<GpuShape>) -> f64 {
    let nodes: Vec<&rightsizer_core::NodeShape> = match gpu {
        Some(g) => state.shapes.iter().filter(|n| n.gpu.is_some_and(|ng| ng.accel == g.accel && ng.count >= g.count)).collect(),
        None => state.shapes.iter().filter(|n| n.gpu.is_none()).collect(),
    };
    if nodes.is_empty() {
        let (nc, nm, p) = rightsizer_core::cost::DEFAULT_NODE;
        let base = p * (cores / nc).max(mem_b / Bytes::GIB as f64 / nm);
        return base + gpu.map(|g| g.count as f64 * rightsizer_core::cost::default_gpu_price(g.accel)).unwrap_or(0.0);
    }
    let mut best_fit = f64::INFINITY;
    let mut best_any = f64::INFINITY;
    for n in nodes {
        let mut share = (cores / n.cpu.as_cores().max(1e-9)).max(mem_b / (n.memory.0 as f64).max(1.0));
        if let (Some(g), Some(ng)) = (gpu, n.gpu) {
            share = share.max(g.count as f64 / ng.count.max(1) as f64);
        }
        let c = n.price_per_hour * share;
        if share <= 1.0 + 1e-9 {
            best_fit = best_fit.min(c);
        }
        best_any = best_any.min(c);
    }
    if best_fit.is_finite() { best_fit } else { best_any }
}

/// Whether a run's measured memory fits `decided` (censoring-aware).
pub fn classify(run: &RunRecord, prior: Option<u64>, decided: Option<u64>) -> Fit {
    let Some(a) = decided else { return Fit::Unknown };
    if prior.is_some_and(|p| a >= p) && run.status == RunStatus::Succeeded {
        return Fit::Fit;
    }
    let Some(m) = run.memory_peak else { return Fit::Unknown };
    let fits = m.value <= a as f64;
    match m.censor {
        Censor::Exact => {
            if fits {
                Fit::Fit
            } else {
                Fit::Fail
            }
        }
        Censor::AtMost => {
            if fits {
                Fit::Fit
            } else {
                Fit::Unknown
            }
        }
        Censor::AtLeast => {
            if fits {
                Fit::Unknown
            } else {
                Fit::Fail
            }
        }
    }
}

/// The run's prior: its origin prior, else what it requested.
fn run_prior(r: &RunRecord) -> Prior {
    let requests = r.origin_prior.clone().or_else(|| r.requested.as_ref().map(|s| s.requests.clone())).unwrap_or_default();
    let limits = r.requested.as_ref().map(|s| s.limits.clone()).unwrap_or_default();
    Prior { spec: ResourceSpec { requests, limits }, ..Prior::default() }
}

/// Whether `h` had finished (and its outcome could be known) by `t`. A run
/// of unknown duration is assumed to have run for the default duration (1 h,
/// SPEC §5.5) rather than to have ended as it started: otherwise its outcome
/// leaks into decisions for runs that started while it was still running.
fn completed_before(h: &RunRecord, t: DateTime<Utc>) -> bool {
    let d = h.duration_s.filter(|d| d.is_finite()).unwrap_or(rightsizer_core::normalize::DEFAULT_DURATION_S);
    let end = h.started_at + chrono::Duration::milliseconds((d.max(0.0) * 1000.0) as i64);
    h.started_at < t && end <= t
}

fn percentile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let i = ((sorted.len() - 1) as f64 * q).round() as usize;
    sorted[i.min(sorted.len() - 1)]
}

/// 95% bootstrap interval of the sum over `xs`.
pub fn bootstrap_sum(xs: &[f64], resamples: usize, seed: u64) -> Interval {
    if xs.is_empty() || resamples == 0 {
        return Interval::default();
    }
    let mut rng = SplitMix64::new(seed);
    let n = xs.len();
    let mut sums: Vec<f64> = (0..resamples)
        .map(|_| (0..n).map(|_| xs[(rng.next_u64() % n as u64) as usize]).sum())
        .collect();
    sums.sort_by(f64::total_cmp);
    Interval { lo: percentile(&sums, 0.025), hi: percentile(&sums, 0.975) }
}

fn seed_for(seed: u64, key: &str) -> u64 {
    let mut s = seed;
    for b in key.bytes() {
        s = s.rotate_left(7) ^ b as u64;
    }
    s
}

fn kind_of<T: Serialize>(v: &T) -> String {
    match serde_json::to_value(v) {
        Ok(serde_json::Value::String(s)) => s,
        Ok(serde_json::Value::Object(m)) => m.get("kind").and_then(|k| k.as_str()).unwrap_or("?").to_string(),
        _ => "?".into(),
    }
}

impl Engine {
    /// Replay `records` (any keys, any order) counterfactually.
    pub async fn replay(&self, records: Vec<RunRecord>, opts: &ReplayOptions) -> ReplayReport {
        let mut by_key: BTreeMap<String, Vec<RunRecord>> = BTreeMap::new();
        for r in records.into_iter().filter(|r| r.status != RunStatus::Cached) {
            by_key.entry(r.key.to_string()).or_default().push(r);
        }
        let mut keys = Vec::new();
        for (_, mut runs) in by_key {
            runs.sort_by(|a, b| a.started_at.cmp(&b.started_at).then(a.attempt.cmp(&b.attempt)));
            keys.push(self.replay_key(runs, opts).await);
        }
        let mut dollars = Vec::new();
        let mut gibh = Vec::new();
        for k in &keys {
            for r in &k.per_run {
                dollars.push(r.saved_dollars);
                gibh.push(r.saved_gib_h);
            }
        }
        let total = Savings {
            gib_h: keys.iter().map(|k| k.would_save.gib_h).sum(),
            core_h: keys.iter().map(|k| k.would_save.core_h).sum(),
            dollars: keys.iter().map(|k| k.would_save.dollars).sum(),
            gib_h_ci: bootstrap_sum(&gibh, opts.bootstrap, opts.seed),
            dollars_ci: bootstrap_sum(&dollars, opts.bootstrap, opts.seed ^ 1),
        };
        let mut report = ReplayReport {
            since: opts.since,
            runs: keys.iter().map(|k| k.runs).sum(),
            would_fail: keys.iter().map(|k| k.would_fail).sum(),
            unknown: keys.iter().map(|k| k.unknown).sum(),
            total,
            keys,
        };
        if !opts.per_run {
            for k in &mut report.keys {
                k.per_run.clear();
            }
        }
        report
    }

    fn replay_context(&self, key: &WorkloadKey, run: &RunRecord, history: Vec<RunRecord>, platform: &PlatformRef) -> WorkloadContext {
        let mut ctx = WorkloadContext {
            v: rightsizer_core::WIRE_VERSION,
            key: key.clone(),
            unit: UnitId("main".into()),
            platform: platform.clone(),
            code: run.code_digest.as_ref().map(|d| CodeInfo { digest: d.clone(), tokens: vec![], source: None }),
            inputs: run.inputs.clone(),
            manifest: ManifestFeatures::default(),
            prior: run_prior(run),
            history,
            labels: BTreeMap::new(),
        };
        ctx.canonicalize(self.config().decision.history_limit);
        ctx
    }

    async fn replay_decide(&self, ctx: &WorkloadContext, opts: &ReplayOptions) -> Decision {
        let mut req = DecideRequest::for_context(ctx.clone());
        req.state = opts.state.clone();
        req.constraints = opts.constraints.clone();
        // No exploration: the replay is the policy's argmin, and stays
        // deterministic (exploration is seeded per decision id).
        req.policy = Policy { mode: Some(Mode::Recommend), exploration: Some(0.0), ..opts.policy.clone() };
        let r = Resolved {
            effective: Effective { mode: Mode::Recommend, demotions: vec![], reductions_blocked: None, skip_estimator: false },
            skip: vec![],
        };
        let o = DecideOpts { no_memo: true, ..Default::default() };
        let chain = self.chain().clone();
        let id = ulid::Ulid::from_datetime(ctx.history.first().map(|h| h.started_at).unwrap_or_default().into());
        let fut = std::panic::AssertUnwindSafe(self.decide_context(&req, ctx, &r, &chain, &o, id));
        match futures::FutureExt::catch_unwind(fut).await {
            Ok(d) => d,
            Err(_) => {
                self.metrics().error("replay", "panic");
                let params = rightsizer_core::DecisionParams::default();
                let input = rightsizer_core::DecisionInput::new(ctx, None, self.grid(), &opts.state, &opts.constraints, &params, Mode::Recommend);
                rightsizer_core::decide::prior_decision(&input, rightsizer_core::PriorReason::Panic, None)
            }
        }
    }

    async fn replay_key(&self, runs: Vec<RunRecord>, opts: &ReplayOptions) -> KeyReport {
        let cfg = self.config();
        let key = runs[0].key.clone();
        let platform = opts.platform.clone().unwrap_or_else(|| PlatformRef::new(&key.platform));
        let limit = cfg.decision.history_limit;
        let recovery = cfg.breaker.recovery_shadow_fits;
        let ladder = platform.retry.ladder.clone().filter(|_| platform.memory_enforced);
        let prices_known = !opts.state.shapes.is_empty() || !matches!(opts.state.price, PriceModel::Packed);

        let mut rows = Vec::new();
        let mut holds: BTreeMap<String, usize> = BTreeMap::new();
        let mut advisories: BTreeMap<String, usize> = BTreeMap::new();
        let mut sources: BTreeMap<String, usize> = BTreeMap::new();
        let mut decided = Vec::new();
        let mut changed = 0;
        let (mut breaker_open, mut fits_since) = (false, 0u32);

        for run in runs.iter().filter(|r| opts.since.is_none_or(|s| r.started_at >= s)) {
            let mut history: Vec<RunRecord> =
                runs.iter().filter(|h| h.invocation_id != run.invocation_id && completed_before(h, run.started_at)).cloned().collect();
            history.sort_by_key(|a| std::cmp::Reverse(a.started_at));
            history.truncate(limit);
            let nh = history.len();
            let ctx = self.replay_context(&key, run, history, &platform);
            let d = self.replay_decide(&ctx, opts).await;
            for h in &d.held {
                *holds.entry(format!("{}:{}", h.axis, kind_of(&h.reason))).or_default() += 1;
            }
            for a in &d.advisories {
                *advisories.entry(kind_of(a)).or_default() += 1;
            }
            *sources.entry(kind_of(&d.source)).or_default() += 1;
            let p = &ctx.prior.spec.requests;
            let a = &d.applied.requests;
            let prior_mem = p.memory.map(|b| b.0);
            let dec_mem = a.memory.map(|b| b.0);
            if dec_mem != prior_mem {
                changed += 1;
            }
            if let Some(m) = dec_mem {
                decided.push(m);
            }
            let fit = classify(run, prior_mem, dec_mem);
            let peak = run.memory_peak.map(|m| m.value);
            let h = run.duration_s.unwrap_or(0.0).max(0.0) / 3600.0;
            let was_open = breaker_open;
            let (mut s_mem, mut s_cpu, mut s_usd) = (0.0, 0.0, 0.0);
            match fit {
                Fit::Fail if !breaker_open => {
                    breaker_open = true;
                    fits_since = 0;
                    // Priced retries: burnt attempts, then the ladder step that fits, or the prior.
                    let burn = run.t_peak_frac.map(|t| t as f64).unwrap_or(0.5).max(0.05) * h;
                    let prior_rate = price_per_hour(&opts.state, prior_mem, p.cpu, p.gpu, peak);
                    let mut cost = 0.0;
                    let mut gib_h = 0.0;
                    let mut mem = dec_mem.unwrap_or(0) as f64;
                    let mut done = false;
                    let steps = ladder.as_ref().map(|l| l.max_steps).unwrap_or(0);
                    for _ in 0..=steps {
                        if peak.is_some_and(|pk| pk <= mem) {
                            cost += price_per_hour(&opts.state, Some(mem as u64), a.cpu, a.gpu, peak) * h;
                            gib_h += gib(mem as u64) * h;
                            done = true;
                            break;
                        }
                        cost += price_per_hour(&opts.state, Some(mem as u64), a.cpu, a.gpu, peak) * burn;
                        gib_h += gib(mem as u64) * burn;
                        mem *= ladder.as_ref().map(|l| l.memory_factor as f64).unwrap_or(1.0);
                    }
                    if !done {
                        cost += prior_rate * h;
                        gib_h += gib(prior_mem.unwrap_or(0)) * h;
                    }
                    s_usd = prior_rate * h - cost;
                    s_mem = gib(prior_mem.unwrap_or(0)) * h - gib_h;
                }
                _ if breaker_open => {
                    if fit == Fit::Fit {
                        fits_since += 1;
                        if fits_since >= recovery {
                            breaker_open = false;
                        }
                    }
                }
                _ => {
                    s_mem = (gib(prior_mem.unwrap_or(0)) - gib(dec_mem.unwrap_or(prior_mem.unwrap_or(0)))) * h;
                    s_cpu = (p.cpu.map(|c| c.as_cores()).unwrap_or(0.0) - a.cpu.or(p.cpu).map(|c| c.as_cores()).unwrap_or(0.0)) * h;
                    s_usd = (price_per_hour(&opts.state, prior_mem, p.cpu, p.gpu, peak) - price_per_hour(&opts.state, dec_mem.or(prior_mem), a.cpu.or(p.cpu), a.gpu, peak)) * h;
                }
            }
            rows.push(RunReplay {
                invocation_id: run.invocation_id.clone(),
                attempt: run.attempt,
                started_at: run.started_at,
                history: nh,
                prior_memory: prior_mem,
                decided_memory: dec_mem,
                recommended_memory: d.recommended.requests.memory.map(|b| b.0),
                peak,
                fit,
                breaker_open: was_open,
                saved_gib_h: s_mem,
                saved_core_h: s_cpu,
                saved_dollars: s_usd,
            });
        }

        // Readiness: the decision as of now, from every completed run.
        // The latest run that carries a prior (manual records may not).
        let latest = runs.iter().rev().find(|r| run_prior(r).spec.requests.memory.is_some()).unwrap_or_else(|| runs.last().expect("non-empty"));
        let mut all: Vec<RunRecord> = runs.clone();
        all.sort_by_key(|a| std::cmp::Reverse(a.started_at));
        all.truncate(limit);
        let ctx = self.replay_context(&key, latest, all.clone(), &platform);
        let now_d = self.replay_decide(&ctx, opts).await;
        let prior_mem = ctx.prior.spec.requests.memory;
        let readiness = match (prior_mem, now_d.recommended.requests.memory) {
            (Some(p), Some(r)) if r < p => {
                let s3 = S3Params { min_history: cfg.policy.min_history, allow_unverified: cfg.history.allow_unverified_for_reductions };
                let digest = ctx.code.as_ref().map(|c| c.digest.as_str());
                let ev = s3b_evidence(&all, digest, ctx.input_bytes(), r, &s3);
                if ev.ok {
                    Readiness::Ready
                } else if ladder.is_some() {
                    Readiness::LadderOnly
                } else {
                    let coarse = all.iter().filter_map(|x| x.memory_peak).any(|m| is_coarse(m.resolution_s));
                    let per = if coarse { 0.5 } else { 1.0 };
                    let more = ((cfg.policy.min_history as f64 - ev.weight) / per).ceil().max(1.0) as u32;
                    Readiness::NotReady { more_runs: more }
                }
            }
            _ => Readiness::Ready,
        };

        decided.sort_unstable();
        let seed = seed_for(opts.seed, &key.to_string());
        let usd: Vec<f64> = rows.iter().map(|r| r.saved_dollars).collect();
        let gh: Vec<f64> = rows.iter().map(|r| r.saved_gib_h).collect();
        KeyReport {
            records: runs.len(),
            runs: rows.len(),
            prior_memory: prior_mem.map(|b| b.0),
            decided_memory_p50: (!decided.is_empty()).then(|| decided[(decided.len() - 1) / 2]),
            changed,
            would_save: Savings {
                gib_h: gh.iter().sum(),
                gib_h_ci: bootstrap_sum(&gh, opts.bootstrap, seed),
                core_h: rows.iter().map(|r| r.saved_core_h).sum(),
                dollars: usd.iter().sum(),
                dollars_ci: bootstrap_sum(&usd, opts.bootstrap, seed ^ 1),
            },
            would_fail: rows.iter().filter(|r| r.fit == Fit::Fail).count(),
            unknown: rows.iter().filter(|r| r.fit == Fit::Unknown).count(),
            holds,
            advisories,
            readiness,
            prices_known,
            sources,
            per_run: rows,
            key,
        }
    }
}
