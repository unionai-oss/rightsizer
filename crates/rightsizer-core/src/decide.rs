//! The decision rule (SPEC §7): normalized estimate + grid + `PlatformRef` +
//! `PlatformState` + risk + constraints + prior + history → [`Decision`].
//!
//! - **recommended** is the argmin of expected cost within hard constraints
//!   only (pins, ceiling, platform minimums, S2 incl. GPU count/type holds).
//! - **applied** is the argmin over the full feasible set (SPEC §7.4): held
//!   axes at the prior, floor (platform ∨ size-scaled), trust region, and
//!   incompressible reductions only under S3 (ladder or evidence), never while
//!   reductions are blocked (breakers, store, unclassifiable surfaces).
//!   Below `enforce` it is still computed: what *would* be applied.
//! - `kind = Action` estimates are decoded by argmax over the feasible set.
//! - Exploration (ε, off by default) moves memory one cell up with
//!   probability ε, from a deterministic seed; the propensity is logged.
//!
//! Pure and synchronous; under 5 ms on `rs-grid-v1` (`benches/decide.rs`).

use std::collections::HashMap;

use ulid::Ulid;

use crate::constraints::{
    Bounds, ConstraintParams, apply_limits, bounds, structural_holds, trust_region_min_grid, trust_region_min_ratio,
    whole_cpu_required,
};
use crate::cost::{
    Candidate, CandidateCost, CostInputs, CostModel, CostParams, LadderPlan, MemoryStepper, busiest_device_need,
    gpu_fail_prob,
};
use crate::digest::context_digest;
use crate::grid::{Grid, GridAxis, covering};
use crate::normalize::{Normalized, normalize};
use crate::safety::{
    S3Params, final_floor, headroom, is_measured_success, ladder_counts, s3a_ladder, s3b_evidence, trusted,
};
use crate::stats::SplitMix64;
use crate::types::{
    AcceleratorId, Advisory, AttemptContext, Axis, Bytes, Combo, Constraints, Decision, DecisionSource, Estimate,
    EstimateKind, GpuShape, GroupId, Hold, HoldReason, MilliCpu, Mode, ParallelismMode, PlatformState, Prior,
    PriorReason, Requests, RequestModel, ResourceSpec, RiskProfile, Shape, SnapFn, WorkloadContext,
};
use crate::units::{format_k8s_bytes, format_k8s_cpu};

/// Tunables of the decision rule. Defaults follow the spec.
#[derive(Clone, Debug, PartialEq)]
pub struct DecisionParams {
    /// Risk profile; `None` = [`RiskProfile::default_for`] the platform's retry semantics.
    pub risk: Option<RiskProfile>,
    /// Exploration ε (≤ 0.05; 0 = off, SPEC §7.6).
    pub exploration: f32,
    /// Seed for exploration draws (deterministic).
    pub seed: u64,
    /// Constraint-stage tunables.
    pub constraints: ConstraintParams,
    /// S3(b) `min_history` (3).
    pub min_history: u32,
    /// Headroom on demand + overhead + staged bytes (1.05).
    pub need_headroom: f64,
    /// σ for `Point` estimates (0.35).
    pub point_sigma: f64,
    /// Fixed attempt overhead, fraction of the nominal run (0.02).
    pub attempt_overhead: f64,
    /// Hours a GPU task without a GPU burns (0.05).
    pub no_gpu_fail_hours: f64,
    /// Whether the surface can get failures classified (S5); false holds reductions.
    pub can_classify: bool,
    /// Incompressible reductions are blocked for this reason (from [`crate::policy`]).
    pub reductions_blocked: Option<HoldReason>,
    /// Mean GPU utilization below which an advisory is raised (0.30).
    pub low_util_threshold: f32,
    /// Runs considered for history-based advisories (5).
    pub advisory_k: usize,
}

impl Default for DecisionParams {
    fn default() -> Self {
        DecisionParams {
            risk: None,
            exploration: 0.0,
            seed: 0,
            constraints: ConstraintParams::default(),
            min_history: 3,
            need_headroom: 1.05,
            point_sigma: crate::normalize::POINT_SIGMA,
            attempt_overhead: 0.02,
            no_gpu_fail_hours: 0.05,
            can_classify: true,
            reductions_blocked: None,
            low_util_threshold: 0.30,
            advisory_k: 5,
        }
    }
}

/// Everything one decision needs (SPEC §7.1).
pub struct DecisionInput<'a> {
    /// Decision id (supplied by the caller, so the rule stays deterministic).
    pub id: Ulid,
    /// The context.
    pub context: &'a WorkloadContext,
    /// Group, when sized together.
    pub group: Option<GroupId>,
    /// The estimate (`None` → a prior decision).
    pub estimate: Option<&'a Estimate>,
    /// Source override (e.g. `Memo`); default: the estimate's provenance.
    pub source: Option<DecisionSource>,
    /// Why there is no estimate, when `estimate` is `None`.
    pub fallback_reason: Option<PriorReason>,
    /// The grid.
    pub grid: &'a Grid,
    /// Live platform state.
    pub state: &'a PlatformState,
    /// Adapter constraints.
    pub constraints: &'a Constraints,
    /// Tunables.
    pub params: &'a DecisionParams,
    /// The effective mode.
    pub mode: Mode,
    /// The current attempt (orchestrator ladders).
    pub attempt: Option<&'a AttemptContext>,
    /// An exact in-process snap function (SPEC §5.8).
    pub snap: Option<&'a dyn SnapFn>,
    /// Precomputed context digest.
    pub context_digest: Option<&'a str>,
}

impl<'a> DecisionInput<'a> {
    /// An input with defaults for everything but the essentials.
    pub fn new(
        context: &'a WorkloadContext,
        estimate: Option<&'a Estimate>,
        grid: &'a Grid,
        state: &'a PlatformState,
        constraints: &'a Constraints,
        params: &'a DecisionParams,
        mode: Mode,
    ) -> Self {
        DecisionInput {
            id: Ulid::nil(),
            context,
            group: None,
            estimate,
            source: None,
            fallback_reason: None,
            grid,
            state,
            constraints,
            params,
            mode,
            attempt: None,
            snap: None,
            context_digest: None,
        }
    }
}

/// A decision that keeps the prior (S1 fallbacks, SPEC §7.5).
pub fn prior_decision(input: &DecisionInput, reason: PriorReason, detail: Option<String>) -> Decision {
    let ctx = input.context;
    let digest = input.context_digest.map(str::to_string).unwrap_or_else(|| context_digest(ctx));
    let mut explanation = vec![format!("kept the prior: {}", serde_plain(&reason))];
    if let Some(d) = &detail {
        explanation.push(d.clone());
    }
    Decision {
        v: crate::WIRE_VERSION,
        id: input.id,
        key: ctx.key.clone(),
        unit: ctx.unit.clone(),
        group: input.group.clone(),
        mode: input.mode,
        recommended: ctx.prior.spec.clone(),
        applied: ctx.prior.spec.clone(),
        shape: None,
        prior: ctx.prior.clone(),
        held: Vec::new(),
        source: DecisionSource::Prior { reason, detail },
        estimate: input.estimate.cloned(),
        propensity: 1.0,
        expected_cost: None,
        p_fail: None,
        advisories: Vec::new(),
        explanation,
        context_digest: digest,
    }
}

fn serde_plain<T: serde::Serialize>(v: &T) -> String {
    serde_json::to_value(v).ok().and_then(|v| v.as_str().map(str::to_string)).unwrap_or_default()
}

/// Dedup key of a candidate: (memory, effective memory, cpu, gpu, shape).
type CandKey = (Option<u64>, u64, Option<u32>, Option<GpuShape>, Option<usize>);

/// One axis's candidate values.
#[derive(Clone, Debug)]
enum Choice<T> {
    /// Held at this value.
    Fixed(Option<T>),
    /// Free over these values.
    Free(Vec<T>),
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum SpaceKind {
    Free,
    Catalog,
    Combos,
    Snap,
}

/// The request model as a candidate generator and ladder stepper (SPEC §5.8).
struct Space<'a> {
    kind: SpaceKind,
    grid_mem: Vec<f64>,
    catalog: Vec<Shape>,
    combos: &'a [Combo],
    instance_bounded: bool,
    exact: bool,
    usage: Option<String>,
    custom: Option<&'a dyn SnapFn>,
    ceiling: Bytes,
}

impl<'a> Space<'a> {
    fn new(state: &'a PlatformState, grid: &Grid, custom: Option<&'a dyn SnapFn>, ceiling: Bytes) -> Self {
        let usage = match &state.price {
            crate::types::PriceModel::Instance { usage } => usage.clone(),
            _ => None,
        };
        let mut s = Space {
            kind: SpaceKind::Free,
            grid_mem: grid.values(GridAxis::Memory),
            catalog: Vec::new(),
            combos: &[],
            instance_bounded: false,
            exact: true,
            usage,
            custom,
            ceiling,
        };
        match &state.request_model {
            RequestModel::Free => {}
            RequestModel::Catalog { shapes } => {
                s.kind = SpaceKind::Catalog;
                s.catalog = shapes.clone();
            }
            RequestModel::Combos { combos } => {
                s.kind = SpaceKind::Combos;
                s.combos = combos;
            }
            RequestModel::Snap { shapes, exact, instance_bounded } => {
                s.kind = SpaceKind::Snap;
                s.catalog = shapes.clone();
                s.exact = *exact || custom.is_some();
                s.instance_bounded = *instance_bounded;
            }
        }
        s
    }

    fn gpu_match(have: Option<GpuShape>, want: Option<GpuShape>) -> bool {
        match (have, want) {
            (Some(h), Some(w)) => h.accel == w.accel && h.count == w.count,
            (None, None) => true,
            _ => false,
        }
    }

    /// Cheapest listed shape covering the ask (the data snap rule).
    fn cheapest_covering(&self, mem: Bytes, cpu: Option<MilliCpu>, gpu: Option<GpuShape>) -> Option<usize> {
        let mut best: Option<(usize, f64, Bytes)> = None;
        for (i, s) in self.catalog.iter().enumerate() {
            if !Self::gpu_match(s.gpu, gpu) || s.usable() < mem || cpu.is_some_and(|c| s.cpu < c) {
                continue;
            }
            let p = s.price_for(self.usage.as_deref());
            if best.is_none_or(|(_, bp, bm)| p < bp || (p == bp && s.usable() < bm)) {
                best = Some((i, p, s.usable()));
            }
        }
        best.map(|b| b.0)
    }

    fn largest_compatible(&self, gpu: Option<GpuShape>) -> Option<usize> {
        self.catalog.iter().enumerate().filter(|(_, s)| Self::gpu_match(s.gpu, gpu)).max_by_key(|(_, s)| (s.usable(), s.cpu)).map(|(i, _)| i)
    }

    fn shape_candidate(&self, i: usize) -> Candidate {
        let s = &self.catalog[i];
        Candidate { memory: Some(s.usable()), effective_memory: s.usable(), cpu: Some(s.cpu), gpu: s.gpu, shape: Some(i) }
    }

    /// Snap an ask to a shape (custom function or the data rule).
    fn snap_ask(&mut self, mem: Bytes, cpu: Option<MilliCpu>, gpu: Option<GpuShape>) -> Option<usize> {
        if let Some(f) = self.custom {
            let ask = Requests { memory: Some(mem), cpu, gpu, ..Default::default() };
            let shape = f.snap(&ask)?;
            if let Some(i) = self.catalog.iter().position(|s| s == &shape) {
                return Some(i);
            }
            self.catalog.push(shape);
            return Some(self.catalog.len() - 1);
        }
        self.cheapest_covering(mem, cpu, gpu)
    }

    fn snap_ro(&self, mem: Bytes, cpu: Option<MilliCpu>, gpu: Option<GpuShape>) -> Option<usize> {
        match self.custom {
            Some(f) => {
                let ask = Requests { memory: Some(mem), cpu, gpu, ..Default::default() };
                let shape = f.snap(&ask)?;
                self.catalog.iter().position(|s| s == &shape)
            }
            None => self.cheapest_covering(mem, cpu, gpu),
        }
    }

    fn combo_snap(&self, mem: Bytes, cpu: Option<MilliCpu>) -> Option<(MilliCpu, Bytes)> {
        let c0 = cpu.unwrap_or(MilliCpu(0));
        let mut best: Option<(MilliCpu, Bytes)> = None;
        for c in self.combos.iter().filter(|c| c.cpu >= c0) {
            if let Some(m) = c.memory.iter().copied().filter(|m| *m >= mem).min()
                && best.is_none_or(|(bc, bm)| c.cpu < bc || (c.cpu == bc && m < bm))
            {
                best = Some((c.cpu, m));
            }
        }
        best
    }

    /// Candidates over the axis choices.
    fn candidates(&mut self, mem: &Choice<Bytes>, cpu: &Choice<MilliCpu>, gpus: &[Option<GpuShape>], prior: &Requests) -> Vec<Candidate> {
        let mut out = Vec::new();
        let fixed_any = matches!(mem, Choice::Fixed(_)) || matches!(cpu, Choice::Fixed(_));
        let mems: Vec<Option<Bytes>> = match mem {
            Choice::Fixed(v) => vec![*v],
            Choice::Free(v) => v.iter().map(|x| Some(*x)).collect(),
        };
        let cpus: Vec<Option<MilliCpu>> = match cpu {
            Choice::Fixed(v) => vec![*v],
            Choice::Free(v) => v.iter().map(|x| Some(*x)).collect(),
        };
        match self.kind {
            SpaceKind::Free => {
                for &g in gpus {
                    for &m in &mems {
                        for &c in &cpus {
                            out.push(Candidate { memory: m, effective_memory: m.unwrap_or(self.ceiling), cpu: c, gpu: g, shape: None });
                        }
                    }
                }
            }
            SpaceKind::Catalog => {
                if fixed_any {
                    out.push(self.prior_candidate(prior));
                } else {
                    for i in 0..self.catalog.len() {
                        if gpus.iter().any(|g| Self::gpu_match(self.catalog[i].gpu, *g)) {
                            out.push(self.shape_candidate(i));
                        }
                    }
                }
            }
            SpaceKind::Combos => {
                let mut seen = std::collections::HashSet::new();
                for &g in gpus {
                    for &m in &mems {
                        for &c in &cpus {
                            let Some(mv) = m else {
                                out.push(Candidate { memory: None, effective_memory: self.ceiling, cpu: c, gpu: g, shape: None });
                                continue;
                            };
                            let Some((sc, sm)) = self.combo_snap(mv, c) else { continue };
                            if matches!(mem, Choice::Fixed(_)) && sm != mv {
                                continue;
                            }
                            if matches!(cpu, Choice::Fixed(Some(pc)) if *pc != sc) {
                                continue;
                            }
                            if seen.insert((sc, sm, g)) {
                                out.push(Candidate { memory: Some(sm), effective_memory: sm, cpu: Some(sc), gpu: g, shape: None });
                            }
                        }
                    }
                }
            }
            SpaceKind::Snap => {
                // Candidates that snap to the same shape collapse to the smallest ask.
                let mut by_shape: HashMap<(usize, Option<MilliCpu>), Candidate> = HashMap::new();
                let mut order = Vec::new();
                for &g in gpus {
                    for &m in &mems {
                        for &c in &cpus {
                            let mv = m.unwrap_or(self.ceiling);
                            let Some(i) = self.snap_ask(mv, c, g) else { continue };
                            let eff = if self.instance_bounded { self.catalog[i].usable() } else { mv };
                            let cand = Candidate { memory: m, effective_memory: eff, cpu: c, gpu: g, shape: Some(i) };
                            let k = (i, c);
                            match by_shape.get(&k) {
                                Some(prev) if prev.memory <= cand.memory => {}
                                _ => {
                                    if !by_shape.contains_key(&k) {
                                        order.push(k);
                                    }
                                    by_shape.insert(k, cand);
                                }
                            }
                        }
                    }
                }
                out.extend(order.into_iter().filter_map(|k| by_shape.remove(&k)));
            }
        }
        out
    }

    /// The prior as a candidate (catalog: the cheapest shape covering it, at the prior's values).
    fn prior_candidate(&self, pr: &Requests) -> Candidate {
        let eff = pr.memory.unwrap_or(self.ceiling);
        match self.kind {
            SpaceKind::Catalog | SpaceKind::Snap => {
                let i = self.snap_ro(eff, pr.cpu, pr.gpu).or_else(|| self.largest_compatible(pr.gpu));
                let eff = match (self.kind, i) {
                    (SpaceKind::Catalog, Some(i)) => self.catalog[i].usable().max(eff),
                    (SpaceKind::Snap, Some(i)) if self.instance_bounded => self.catalog[i].usable(),
                    _ => eff,
                };
                Candidate { memory: pr.memory, effective_memory: eff, cpu: pr.cpu, gpu: pr.gpu, shape: i }
            }
            _ => Candidate { memory: pr.memory, effective_memory: eff, cpu: pr.cpu, gpu: pr.gpu, shape: None },
        }
    }
}

impl MemoryStepper for Space<'_> {
    fn step_to(&self, from: &Candidate, at_least: Bytes) -> Candidate {
        let snap_grid = |b: Bytes| {
            let (i, over) = covering(&self.grid_mem, b.0 as f64);
            if over { b } else { Bytes(self.grid_mem[i] as u64).max(b) }
        };
        match self.kind {
            SpaceKind::Free => {
                let m = snap_grid(at_least);
                Candidate { memory: Some(m), effective_memory: m, ..from.clone() }
            }
            SpaceKind::Combos => match self.combo_snap(at_least, from.cpu) {
                Some((c, m)) => Candidate { memory: Some(m), effective_memory: m, cpu: Some(c), ..from.clone() },
                None => from.clone(),
            },
            SpaceKind::Catalog => {
                match self.cheapest_covering(at_least, from.cpu, from.gpu).or_else(|| self.largest_compatible(from.gpu)) {
                    Some(i) => self.shape_candidate(i),
                    None => from.clone(),
                }
            }
            SpaceKind::Snap => {
                let ask = snap_grid(at_least);
                match self.snap_ro(ask, from.cpu, from.gpu) {
                    Some(i) => {
                        let eff = if self.instance_bounded { self.catalog[i].usable() } else { ask };
                        Candidate { memory: Some(ask), effective_memory: eff, shape: Some(i), ..from.clone() }
                    }
                    None => from.clone(),
                }
            }
        }
    }
}

fn push_hold(holds: &mut Vec<Hold>, h: Hold) {
    if !holds.iter().any(|x| x.axis == h.axis) {
        holds.push(h);
    }
}

fn fmt_b(b: Option<Bytes>) -> String {
    b.map(|b| {
        let s = format_k8s_bytes(b.0);
        if s.bytes().all(|c| c.is_ascii_digit()) && b.0 >= Bytes::MIB { human_bytes(b.0 as f64) } else { s }
    })
    .unwrap_or_else(|| "unset".into())
}

/// Approximate human form for explanations (`"9.07Gi"`).
fn human_bytes(v: f64) -> String {
    for (suf, unit) in [("Ti", Bytes::TIB), ("Gi", Bytes::GIB), ("Mi", Bytes::MIB), ("Ki", Bytes::KIB)] {
        if v >= unit as f64 {
            return format!("{:.2}{suf}", v / unit as f64);
        }
    }
    format!("{v:.0}")
}
fn fmt_c(c: Option<MilliCpu>) -> String {
    c.map(format_k8s_cpu).unwrap_or_else(|| "unset".into())
}

/// Whether `a` beats `b`: lower expected cost; exact ties (within 1e-9
/// relative) prefer the prior, then less memory, then less CPU (risk is
/// already priced in; a tie means the extra is free insurance only in the
/// model, while it strands capacity in practice).
fn better(a: (f64, &Candidate), b: (f64, &Candidate), prior: &Requests) -> bool {
    let tol = 1e-9 * a.0.abs().max(b.0.abs()).max(1e-12);
    if a.0 < b.0 - tol {
        return true;
    }
    if a.0 > b.0 + tol {
        return false;
    }
    let is_prior = |c: &Candidate| c.memory == prior.memory && c.cpu == prior.cpu;
    match (is_prior(a.1), is_prior(b.1)) {
        (true, false) => return true,
        (false, true) => return false,
        _ => {}
    }
    (a.1.effective_memory, a.1.cpu) < (b.1.effective_memory, b.1.cpu)
}

/// Run the decision rule (SPEC §7). Never panics on valid inputs; any
/// unusable estimate yields a prior decision.
pub fn decide(input: &DecisionInput) -> Decision {
    let ctx = input.context;
    let Some(est) = input.estimate else {
        return prior_decision(input, input.fallback_reason.unwrap_or(PriorReason::Insufficient), None);
    };
    let p = input.params;
    let grid = input.grid;
    let norm = match normalize(est, grid, &ctx.history, p.point_sigma) {
        Ok(n) => n,
        Err(e) => return prior_decision(input, PriorReason::InvalidEstimate, Some(e.to_string())),
    };
    let source = input.source.clone().unwrap_or_else(|| DecisionSource::Estimator {
        id: est.provenance.estimator.clone(),
        version: est.provenance.version.clone(),
    });
    if est.provenance.estimator == "prior" {
        // The `prior` estimator (the tuner's `static` arm) keeps the prior.
        let mut d = prior_decision(input, PriorReason::Insufficient, None);
        d.source = source;
        d.explanation = vec!["prior estimator: kept the prior".into()];
        return d;
    }
    Decider::new(input, est, norm, source).run()
}

struct Decider<'a, 'b> {
    input: &'b DecisionInput<'a>,
    est: &'b Estimate,
    norm: Normalized,
    source: DecisionSource,
}

/// Per-candidate feasibility result for `applied`.
#[derive(Clone, Debug)]
enum Gate {
    Ok,
    Excluded(HoldReason, String),
}

impl<'a, 'b> Decider<'a, 'b> {
    fn new(input: &'b DecisionInput<'a>, est: &'b Estimate, norm: Normalized, source: DecisionSource) -> Self {
        Decider { input, est, norm, source }
    }

    fn run(self) -> Decision {
        let inp = self.input;
        let ctx = inp.context;
        let p = inp.params;
        let cp = &p.constraints;
        let grid = inp.grid;
        let prior: &Prior = &ctx.prior;
        let pr = &prior.spec.requests;
        let code_digest = ctx.code.as_ref().map(|c| c.digest.as_str());
        let b_now = ctx.input_bytes();
        let history = &ctx.history;
        let mut explanation = Vec::new();

        // ── holds (SPEC §7.4 items 1–2) ───────────────────────────────────
        let mut holds = structural_holds(prior, inp.constraints, ctx.manifest.distributed, cp);
        if self.norm.memory.is_none() {
            push_hold(&mut holds, Hold::new(Axis::Memory, HoldReason::NoEstimate));
        }
        if self.norm.cpu_mean.is_none() {
            push_hold(&mut holds, Hold::new(Axis::Cpu, HoldReason::NoEstimate));
        }
        let b: Bounds = bounds(prior, inp.constraints, inp.state, grid, history, code_digest, b_now, cp);
        let mut space = Space::new(inp.state, grid, inp.snap, b.mem_ceiling);
        if space.kind == SpaceKind::Snap && !space.exact {
            push_hold(&mut holds, Hold::with(Axis::Memory, HoldReason::InexactSnap, "snap function not known exactly"));
            push_hold(&mut holds, Hold::with(Axis::Cpu, HoldReason::InexactSnap, "snap function not known exactly"));
        }
        let held_any = |a: Axis| holds.iter().any(|h| h.axis == a);
        let held_hard = |a: Axis| holds.iter().any(|h| h.axis == a && h.reason.is_hard());

        // ── GPU options (count always held in v1; type only if allowed) ──
        let gpu_opts: Vec<Option<GpuShape>> = match pr.gpu {
            None => vec![None],
            Some(g) if held_any(Axis::GpuType) => vec![Some(g)],
            Some(g) => {
                let mut v = vec![Some(g)];
                for s in inp.constraints.allowed_shapes.iter().flatten() {
                    let accel_ok = cp.allowed_accelerators.as_ref().is_none_or(|a| a.contains(&s.accel));
                    if s.count == g.count && accel_ok && !s.accel.is_unknown() && !v.contains(&Some(*s)) {
                        v.push(Some(*s));
                    }
                }
                v
            }
        };

        // ── value lists ───────────────────────────────────────────────────
        let whole = whole_cpu_required(prior);
        let mem_lo = b.mem_platform_floor.unwrap_or(Bytes(0));
        let mut mem_vals: Vec<Bytes> = grid.memory.iter().copied().filter(|m| *m >= mem_lo && *m <= b.mem_ceiling).collect();
        for extra in [Some(b.mem_ceiling), pr.memory] {
            if let Some(x) = extra.filter(|x| *x >= mem_lo && *x <= b.mem_ceiling)
                && !mem_vals.contains(&x)
            {
                mem_vals.push(x);
            }
        }
        mem_vals.sort();
        let cpu_lo = b.cpu_floor.unwrap_or(MilliCpu(0));
        let mut cpu_vals: Vec<MilliCpu> = grid
            .cpu
            .iter()
            .map(|c| if whole { c.ceil_whole() } else { *c })
            .chain([Some(b.cpu_ceiling), pr.cpu].into_iter().flatten())
            .filter(|c| *c >= cpu_lo && *c <= b.cpu_ceiling)
            .collect();
        cpu_vals.sort();
        cpu_vals.dedup();

        let rec_mem = if held_hard(Axis::Memory) { Choice::Fixed(pr.memory) } else { Choice::Free(mem_vals.clone()) };
        let rec_cpu = if held_hard(Axis::Cpu) { Choice::Fixed(pr.cpu) } else { Choice::Free(cpu_vals.clone()) };
        let app_mem = if held_any(Axis::Memory) {
            Choice::Fixed(pr.memory)
        } else {
            // The floor itself is a candidate for `applied` (it is usually off-grid).
            let mut v = mem_vals;
            if let Some(f) = b.mem_floor.filter(|f| *f >= mem_lo && *f <= b.mem_ceiling && !v.contains(f)) {
                v.push(f);
                v.sort();
            }
            Choice::Free(v)
        };
        let app_cpu = if held_any(Axis::Cpu) { Choice::Fixed(pr.cpu) } else { Choice::Free(cpu_vals) };

        let mut rec_cands = space.candidates(&rec_mem, &rec_cpu, &gpu_opts, pr);
        let mut app_cands = space.candidates(&app_mem, &app_cpu, &gpu_opts, pr);
        let mem_fixed_rec = matches!(rec_mem, Choice::Fixed(_));
        let mem_fixed_app = matches!(app_mem, Choice::Fixed(_));
        let cpu_fixed_app = matches!(app_cpu, Choice::Fixed(_));

        // Hard bounds for both sets.
        let within_hard = |c: &Candidate, mem_fixed: bool, cpu_fixed: bool| {
            (mem_fixed || (c.effective_memory <= b.mem_ceiling && c.effective_memory >= mem_lo))
                && (cpu_fixed || c.cpu.is_none_or(|x| x <= b.cpu_ceiling && x >= cpu_lo))
        };
        rec_cands.retain(|c| within_hard(c, mem_fixed_rec, matches!(rec_cpu, Choice::Fixed(_))));

        // ── S3 inputs ─────────────────────────────────────────────────────
        let platform = &ctx.platform;
        let s3p = S3Params { min_history: p.min_history, allow_unverified: cp.allow_unverified };
        let ladder = platform.retry.ladder.as_ref().filter(|l| ladder_counts(l, inp.attempt));
        let ffloor = ladder.and_then(|l| final_floor(pr.memory, history, l, cp.allow_unverified));
        let s3a = s3a_ladder(&platform.retry, inp.attempt, platform.memory_enforced, ffloor, Some(b.mem_ceiling));
        let trust_min = b.anchor.map(|a| {
            let t = match space.kind {
                SpaceKind::Free | SpaceKind::Combos => trust_region_min_grid(grid, a, cp.max_step_down),
                _ => trust_region_min_ratio(a, cp.max_step_down),
            };
            t.min(b.mem_ceiling)
        });
        let mut evidence_cache: HashMap<u64, (bool, usize, usize)> = HashMap::new();
        let mut gate = |c: &Candidate| -> Gate {
            if mem_fixed_app {
                return Gate::Ok;
            }
            // Incompressible reductions first: S3 and blocks are the binding reasons.
            if let Some(pm) = pr.memory
                && c.effective_memory < pm
            {
                if let Some(r) = p.reductions_blocked {
                    return Gate::Excluded(r, "reductions are blocked".into());
                }
                if !p.can_classify {
                    return Gate::Excluded(HoldReason::Unclassifiable, "failures on this surface cannot be classified".into());
                }
                if !s3a {
                    let (ok, fit, q) = *evidence_cache.entry(c.effective_memory.0).or_insert_with(|| {
                        let e = s3b_evidence(history, code_digest, b_now, c.effective_memory, &s3p);
                        (e.ok, e.fitting, e.qualifying)
                    });
                    if !ok {
                        let reason = if platform.memory_enforced { HoldReason::S3NotMet } else { HoldReason::NotEnforced };
                        let why = if platform.memory_enforced && platform.retry.ladder.is_none() { "no ladder; " } else { "" };
                        return Gate::Excluded(reason, format!("{why}{fit} of {q} measured successes fit (need {})", p.min_history));
                    }
                }
            }
            if let Some(f) = b.mem_floor.filter(|f| c.effective_memory < *f) {
                return Gate::Excluded(HoldReason::S3NotMet, format!("below the floor {}", fmt_b(Some(f))));
            }
            if let Some(t) = trust_min.filter(|t| c.effective_memory < *t) {
                return Gate::Excluded(HoldReason::S3NotMet, format!("below the trust region {}", fmt_b(Some(t))));
            }
            Gate::Ok
        };
        let gpu_gate = |c: &Candidate| -> bool {
            // A smaller device is an incompressible reduction: S3(b) on GPU peaks only.
            let (Some(g), Some(pg)) = (c.gpu, pr.gpu) else { return true };
            let (Some(have), Some(was)) = (g.accel.usable_memory(), pg.accel.usable_memory()) else { return g == pg };
            if have >= was {
                return true;
            }
            let n = history
                .iter()
                .filter(|r| is_measured_success(r, cp.allow_unverified) && trusted(r, cp.allow_unverified))
                .filter_map(|r| r.gpu_memory_peak)
                .filter(|m| m.value * headroom(m.resolution_s, false) <= have.0 as f64)
                .count();
            p.reductions_blocked.is_none() && n as u32 >= p.min_history
        };
        let mut excluded: Vec<(Candidate, HoldReason, String)> = Vec::new();
        app_cands.retain(|c| {
            if !within_hard(c, mem_fixed_app, cpu_fixed_app) || !gpu_gate(c) {
                return false;
            }
            match gate(c) {
                Gate::Ok => true,
                Gate::Excluded(r, why) => {
                    excluded.push((c.clone(), r, why));
                    false
                }
            }
        });
        let prior_cand = space.prior_candidate(pr);
        let app_fallback = app_cands.is_empty();
        if app_fallback {
            app_cands.push(prior_cand.clone());
        }
        let rec_fallback = rec_cands.is_empty();
        if rec_fallback {
            rec_cands.push(prior_cand.clone());
        }

        // ── cost model (SPEC §7.2) ────────────────────────────────────────
        let risk = p.risk.unwrap_or_else(|| RiskProfile::default_for(&platform.retry));
        let gpu_chain: Vec<GpuShape> = if gpu_opts.len() > 1 {
            let mut v: Vec<GpuShape> = gpu_opts.iter().flatten().copied().collect();
            v.sort_by_key(|g| g.total_memory());
            let cur = pr.gpu.and_then(|g| g.total_memory());
            v.into_iter().filter(|g| g.total_memory() > cur).collect()
        } else {
            Vec::new()
        };
        let plan = ladder.map(|l| LadderPlan {
            factor: l.memory_factor as f64,
            steps: l.max_steps,
            final_floor: ffloor,
            ceiling: b.mem_ceiling,
            gpu_chain: gpu_chain.clone(),
        });
        let staged = ctx.manifest.memory_backed_bytes.map(|b| b.0 as f64).unwrap_or(0.0);
        let catalog_for_cost = space.catalog.clone();
        let model = CostModel::new(
            CostInputs {
                demand: &self.norm,
                platform,
                price: &inp.state.price,
                nodes: &inp.state.shapes,
                catalog: &catalog_for_cost,
                grid,
                prior: pr,
                prior_candidate: prior_cand.clone(),
                staged_bytes: staged,
                ladder: plan,
                params: CostParams {
                    penalties: risk.penalties(),
                    attempt_overhead: p.attempt_overhead,
                    no_gpu_fail_hours: p.no_gpu_fail_hours,
                    min_burn_frac: 0.05,
                    need_headroom: p.need_headroom,
                    semantics: crate::cost::CostSemantics::Rightsizer,
                },
            },
            &rec_cands,
        );

        // Evaluate the union once.
        let mut all: Vec<Candidate> = Vec::with_capacity(rec_cands.len() + app_cands.len() + 1);
        let mut index: HashMap<CandKey, usize> = HashMap::new();
        let mut idx_of = |c: &Candidate, all: &mut Vec<Candidate>| -> usize {
            let k = (c.memory.map(|b| b.0), c.effective_memory.0, c.cpu.map(|x| x.0), c.gpu, c.shape);
            *index.entry(k).or_insert_with(|| {
                all.push(c.clone());
                all.len() - 1
            })
        };
        let rec_idx: Vec<usize> = rec_cands.iter().map(|c| idx_of(c, &mut all)).collect();
        let app_idx: Vec<usize> = app_cands.iter().map(|c| idx_of(c, &mut all)).collect();
        let prior_idx = idx_of(&prior_cand, &mut all);
        let costs: Vec<CandidateCost> = all.iter().map(|c| model.evaluate(c, &space)).collect();

        let action = self.norm.kind == EstimateKind::Action;
        let pick = |set: &[usize]| -> usize {
            let mut best = set[0];
            if action {
                let score = |c: &Candidate| {
                    let pm = self.norm.memory.as_ref().map_or(1.0, |d| d.probs[grid.memory_cell(c.effective_memory)]);
                    let pc = match (&self.norm.cpu_mean, c.cpu) {
                        (Some(d), Some(x)) => d.probs[grid.cpu_cell(x)],
                        _ => 1.0,
                    };
                    pm * pc
                };
                for &i in &set[1..] {
                    let (a, bb) = (score(&all[i]), score(&all[best]));
                    if a > bb || (a == bb && better((costs[i].expected, &all[i]), (costs[best].expected, &all[best]), pr)) {
                        best = i;
                    }
                }
            } else {
                for &i in &set[1..] {
                    if better((costs[i].expected, &all[i]), (costs[best].expected, &all[best]), pr) {
                        best = i;
                    }
                }
            }
            best
        };
        let rec_best = pick(&rec_idx);
        let mut app_best = pick(&app_idx);

        // ── S4: an increase the ceiling blocks is clamped to the ceiling ──
        let clamp = |best: usize, set: &[usize], fixed: bool| -> (usize, bool) {
            let c = &all[best];
            if fixed || action || space.kind != SpaceKind::Free || c.memory.is_none() {
                return (best, false);
            }
            let above = grid.memory.iter().copied().filter(|m| *m > b.mem_ceiling);
            let cheaper_above = above
                .map(|m| model.evaluate(&Candidate { memory: Some(m), effective_memory: m, ..c.clone() }, &space).expected)
                .any(|x| x < costs[best].expected);
            if !cheaper_above {
                return (best, false);
            }
            let at_ceiling = set
                .iter()
                .copied()
                .filter(|&i| all[i].effective_memory == b.mem_ceiling && all[i].gpu == c.gpu)
                .min_by(|&i, &j| costs[i].expected.total_cmp(&costs[j].expected));
            (at_ceiling.unwrap_or(best), true)
        };
        let (rec_best, _) = clamp(rec_best, &rec_idx, mem_fixed_rec);
        let (clamped_best, mem_clamped) = clamp(app_best, &app_idx, mem_fixed_app);
        app_best = clamped_best;

        // ── exploration (SPEC §7.6) ───────────────────────────────────────
        let mut propensity = 1.0f32;
        let eps = p.exploration.clamp(0.0, 0.05);
        if eps > 0.0 && !mem_fixed_app {
            let chosen = all[app_best].clone();
            let up = app_idx
                .iter()
                .copied()
                .filter(|&i| all[i].cpu == chosen.cpu && all[i].gpu == chosen.gpu && all[i].effective_memory > chosen.effective_memory)
                .min_by_key(|&i| all[i].effective_memory);
            if let Some(up) = up {
                let mut seed = p.seed;
                for b in ctx.key.to_string().bytes().chain(ctx.unit.0.bytes()) {
                    seed = seed.rotate_left(5) ^ b as u64;
                }
                let mut rng = SplitMix64::new(seed);
                if rng.next_f64() < eps as f64 {
                    app_best = up;
                    propensity = eps;
                    explanation.push(format!("exploring: memory one cell up (ε = {eps})"));
                } else {
                    propensity = 1.0 - eps;
                }
            }
        }

        // ── specs ─────────────────────────────────────────────────────────
        let spec_of = |c: &Candidate| {
            let requests = Requests {
                cpu: c.cpu,
                memory: c.memory,
                gpu: c.gpu,
                ephemeral_storage: pr.ephemeral_storage,
                shm: pr.shm,
            };
            ResourceSpec { limits: apply_limits(prior, &requests), requests }
        };
        let rec = &all[rec_best];
        let app = &all[app_best];
        let recommended = spec_of(rec);
        let applied = spec_of(app);
        let shape = app.shape.and_then(|i| space.catalog.get(i)).map(|s| s.name.clone());

        // Soft holds: a recommended reduction S3 (or a block) kept at the prior.
        if let (Some(pm), Some(rm)) = (pr.memory, rec.memory)
            && rm < pm
            && app.memory == pr.memory
            && !held_any(Axis::Memory)
        {
            let why = excluded.iter().find(|(c, _, _)| c.memory == rec.memory).or_else(|| excluded.first());
            match why {
                Some((_, r, d)) => holds.push(Hold::with(Axis::Memory, *r, d.clone())),
                None => holds.push(Hold::new(Axis::Memory, HoldReason::S3NotMet)),
            }
        }

        // ── advisories (SPEC §7.7) ────────────────────────────────────────
        let mut advisories = Vec::new();
        if b.infeasible_floor {
            advisories.push(Advisory::InfeasibleFloor { axis: Axis::Memory });
            explanation.push(format!(
                "infeasible_floor: the floor {} exceeds the ceiling {}; using the ceiling",
                fmt_b(b.mem_size_floor.max(b.mem_platform_floor)),
                fmt_b(Some(b.mem_ceiling))
            ));
        }
        if mem_clamped {
            let reason = clamp_reason(b.mem_ceiling_source, self.input.constraints);
            explanation.push(format!("memory clamped to the ceiling {} ({reason})", fmt_b(Some(b.mem_ceiling))));
            advisories.push(Advisory::Clamped { axis: Axis::Memory, reason });
        }
        self.clamp_advisories(&model, &space, app, &costs[app_best], &b, &mut advisories);
        self.gpu_advisories(&model, pr, &mut advisories);
        self.walltime_advisories(&costs[app_best], &mut advisories);
        self.prior_advisories(&model, pr, &mut advisories);

        // ── explanation ───────────────────────────────────────────────────
        match &self.source {
            DecisionSource::Estimator { id, version } => explanation.insert(0, format!("estimate from {id}@{version}")),
            DecisionSource::Memo { id, version } => explanation.insert(0, format!("memoized estimate from {id}@{version}")),
            DecisionSource::Prior { .. } => {}
        }
        if let Some(m) = &self.norm.memory {
            explanation.push(format!(
                "memory demand p50 {} p95 {}; need = (demand + overhead {} + staged {}) × {}",
                fmt_b(Some(Bytes::from_f64(m.quantile(0.5)))),
                fmt_b(Some(Bytes::from_f64(m.quantile(0.95)))),
                fmt_b(Some(Bytes::from_f64(platform.overhead.median_bytes()))),
                fmt_b(Some(Bytes::from_f64(staged))),
                p.need_headroom
            ));
        }
        if let Some(c) = &self.norm.cpu_mean {
            explanation.push(format!("cpu mean p50 {:.2} p95 {:.2} cores", c.quantile(0.5), c.quantile(0.95)));
        }
        explanation.push(format!(
            "bounds: memory floor {} (platform {}, size-scaled {}), ceiling {} ({}); cpu ceiling {} ({}); trust region ≥ {} (anchor {})",
            fmt_b(b.mem_floor),
            fmt_b(b.mem_platform_floor),
            fmt_b(b.mem_size_floor),
            fmt_b(Some(b.mem_ceiling)),
            b.mem_ceiling_source.as_str(),
            fmt_c(Some(b.cpu_ceiling)),
            b.cpu_ceiling_source.as_str(),
            fmt_b(trust_min),
            fmt_b(b.anchor)
        ));
        explanation.push(format!(
            "S3: ladder {}; risk oom×{} delay×{} neighbour×{}",
            if s3a { "counts" } else if ladder.is_some() { "does not reach the final floor" } else { "none" },
            risk.penalties().oom_penalty,
            risk.penalties().delay_penalty,
            risk.penalties().neighbour_penalty
        ));
        let rc = &costs[rec_best];
        let ac = &costs[app_best];
        let pc = &costs[prior_idx];
        explanation.push(format!(
            "recommended memory {} cpu {}: expected ${:.4}, p_fail {:.3}",
            fmt_b(rec.memory),
            fmt_c(rec.cpu),
            rc.expected,
            rc.p_fail
        ));
        explanation.push(format!(
            "applied memory {} cpu {}: expected ${:.4}, p_fail {:.3} (prior memory {} cpu {}: expected ${:.4}, p_fail {:.3})",
            fmt_b(app.memory),
            fmt_c(app.cpu),
            ac.expected,
            ac.p_fail,
            fmt_b(pr.memory),
            fmt_c(pr.cpu),
            pc.expected,
            pc.p_fail
        ));
        if let Some(s) = &shape {
            explanation.push(format!("shape {s}"));
        }
        for h in &holds {
            explanation.push(format!(
                "{} held at the prior: {}{}",
                h.axis,
                serde_plain(&h.reason),
                h.detail.as_ref().map(|d| format!(" ({d})")).unwrap_or_default()
            ));
        }
        if app_fallback {
            explanation.push("no feasible candidate: kept the prior".into());
        }
        if inp.mode < Mode::Enforce {
            explanation.push(format!("mode {}: applied is what enforce would apply", inp.mode));
        }
        if action {
            explanation.push("action estimate: decoded by argmax over the feasible set".into());
        }

        Decision {
            v: crate::WIRE_VERSION,
            id: inp.id,
            key: ctx.key.clone(),
            unit: ctx.unit.clone(),
            group: inp.group.clone(),
            mode: inp.mode,
            recommended,
            applied,
            shape,
            prior: prior.clone(),
            held: holds,
            source: self.source.clone(),
            estimate: Some(self.est.clone()),
            propensity,
            expected_cost: (!action).then_some(ac.expected),
            p_fail: (!action).then_some(ac.p_fail as f32),
            advisories,
            explanation,
            context_digest: inp.context_digest.map(str::to_string).unwrap_or_else(|| context_digest(ctx)),
        }
    }

    fn clamp_advisories(&self, model: &CostModel, space: &Space, app: &Candidate, cost: &CandidateCost, b: &Bounds, out: &mut Vec<Advisory>) {
        if space.kind != SpaceKind::Free {
            return;
        }
        let grid = self.input.grid;
        if let Some(c) = app.cpu.filter(|c| *c >= b.cpu_ceiling)
            && let Some(next) = grid.cpu.iter().copied().find(|x| *x > c)
        {
            let up = Candidate { cpu: Some(next), ..app.clone() };
            if model.evaluate(&up, space).expected < cost.expected {
                out.push(Advisory::Clamped { axis: Axis::Cpu, reason: clamp_reason(b.cpu_ceiling_source, self.input.constraints) });
            }
        }
    }

    fn gpu_advisories(&self, model: &CostModel, pr: &Requests, out: &mut Vec<Advisory>) {
        let ctx = self.input.context;
        let Some(g) = pr.gpu else { return };
        let k = self.input.params.advisory_k;
        let utils: Vec<f32> = ctx.history.iter().filter_map(|r| r.gpu_util_mean).filter(|u| u.is_finite()).take(k).collect();
        if !utils.is_empty() {
            let mean = utils.iter().sum::<f32>() / utils.len() as f32;
            if mean < self.input.params.low_util_threshold {
                out.push(Advisory::GpuLowUtil { mean_util: mean, runs: utils.len() as u32 });
            }
        }
        let Some(d) = &self.norm.gpu_memory else { return };
        let Some(dev) = g.accel.usable_memory() else { return };
        let par = self.norm.parallelism;
        let p_prior = model.gpu_sf(Some(g));
        if p_prior > 0.2 {
            out.push(Advisory::GpuOomPredicted { p: p_prior as f32 });
        }
        if g.count > 1 && par.mode != ParallelismMode::Replicated {
            let ok = |n: u32| gpu_fail_prob(d, dev.0 as f64, n, par.mode, par.imbalance as f64) <= (p_prior + 0.01).max(0.05);
            if let Some(n) = (1..g.count).find(|&n| ok(n)) {
                out.push(Advisory::GpuLowerCount { count: g.count, fits_count: n });
            }
        }
        // A cheaper shape that fits (p95 busiest-device need).
        let need95 = d.quantile(0.95);
        let prior_price = self
            .input
            .state
            .shapes
            .iter()
            .filter(|n| n.gpu.is_some_and(|ng| ng.accel == g.accel && ng.count >= g.count))
            .map(|n| n.price_per_hour)
            .fold(f64::INFINITY, f64::min);
        if prior_price.is_finite() {
            let mut best: Option<(AcceleratorId, u32, f64)> = None;
            for n in &self.input.state.shapes {
                let Some(ng) = n.gpu else { continue };
                let Some(ndev) = ng.accel.usable_memory() else { continue };
                if busiest_device_need(need95, ng.count, par.mode, par.imbalance as f64) <= ndev.0 as f64
                    && n.price_per_hour < prior_price * 0.95
                    && best.is_none_or(|(_, _, p)| n.price_per_hour < p)
                {
                    best = Some((ng.accel, ng.count, n.price_per_hour));
                }
            }
            if let Some((accel, count, price)) = best {
                out.push(Advisory::GpuCheaperShape { accel, count, saving_per_hour: prior_price - price });
            }
        }
    }

    fn walltime_advisories(&self, cost: &CandidateCost, out: &mut Vec<Advisory>) {
        let platform = &self.input.context.platform;
        let Some(w) = platform.walltime_s else { return };
        let dur = &self.norm.duration;
        if !dur.timeout_modeling {
            return;
        }
        if cost.p_timeout > 0.1 {
            let suggested = (dur.quantile(0.9) * cost.stretch * 1.2).ceil() as u64;
            out.push(Advisory::WalltimeTimeout { p: cost.p_timeout as f32, suggested_s: suggested.max(w) });
        }
        let p95 = dur.quantile(0.9).max(dur.median_s);
        if w as f64 > 10.0 * p95 {
            out.push(Advisory::WalltimeExcessive { walltime_s: w, p95_s: p95.ceil() as u64 });
        }
    }

    fn prior_advisories(&self, model: &CostModel, pr: &Requests, out: &mut Vec<Advisory>) {
        let _ = model;
        let platform = &self.input.context.platform;
        if let (Some(pm), Some(d)) = (pr.memory, &self.norm.memory) {
            let staged = self.input.context.manifest.memory_backed_bytes.map(|b| b.0 as f64).unwrap_or(0.0);
            let need95 = (d.quantile(0.95) + platform.overhead.median_bytes() + staged) * self.input.params.need_headroom;
            if need95 > 0.0 && pm.0 as f64 > 4.0 * need95 {
                out.push(Advisory::PriorOversized { axis: Axis::Memory, ratio: pm.0 as f64 / need95 });
            }
        }
        if let (Some(pc), Some(d)) = (pr.cpu, &self.norm.cpu_mean) {
            let p95 = d.quantile(0.95);
            if p95 > 0.0 && pc.as_cores() > 4.0 * p95 {
                out.push(Advisory::PriorOversized { axis: Axis::Cpu, ratio: pc.as_cores() / p95 });
            }
        }
    }
}

/// The clamp reason for a ceiling source: `Constraints::ceiling_reason`
/// overrides `"user"` (e.g. `"no_platform_state"`).
fn clamp_reason(src: crate::constraints::CeilingSource, c: &Constraints) -> String {
    match (src, &c.ceiling_reason) {
        (crate::constraints::CeilingSource::User, Some(r)) if !r.is_empty() => r.clone(),
        _ => src.as_str().into(),
    }
}

#[cfg(test)]
mod tests;
