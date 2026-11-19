//! The cost model (SPEC §7.2).
//!
//! Expected cost of a candidate *a* = (memory, CPU, GPU shape) under the
//! normalized demand:
//!
//! - **Per attempt**: price × hours under the [`PriceModel`]: `packed`
//!   (dominant share of the cheapest node shape that holds it; on a GPU
//!   shape host CPU and memory up to the per-GPU share are free, beyond it
//!   they strand GPUs), `linear`, `instance` (the shape's price) or
//!   `usage_floor` (billed on `E[max(request, demand)]`).
//! - **Stretch** `E[max(1, cpu_mean / c)]`; it multiplies every hour,
//!   GPU-hours included.
//! - **Timeout**: `stretch × duration > walltime` is a failure priced like an
//!   OOM; it burns the walltime and is not escalated by a ladder.
//! - **A failing attempt** burns `max(t_peak_frac, 0.05) × stretch × duration`
//!   plus `attempt_overhead` (0.02) of the nominal run.
//! - **Penalties**: `oom_penalty × oracle_cost` per failed attempt;
//!   `delay_penalty × oracle_rate × (hours − oracle_hours)`; when memory is
//!   not enforced, `neighbour_penalty × P(need > request) × oracle_cost` and
//!   no OOM of the unit itself.
//! - **Missing GPU**: P(GPU needed) × (`no_gpu_fail_hours` at the rate +
//!   one OOM penalty). A needless GPU costs its price (it is in the rate).
//!
//! **Computation.** Demand is fixed per run, so the retry process is a chain
//! over ladder states (memory step × GPU step) whose path is determined by the
//! first step at which each axis fits. With `P(k_m = i) = S(r_{i-1}) − S(r_i)`
//! (and the same for GPU), expected cost is a closed-form sum over at most
//! `(max_steps + 2)²` paths per candidate. A failure at a state is repeated by
//! the platform's `same_size` retries (the geometric term with repeat
//! probability 1, since demand is deterministic given the run), and a failure
//! at the last state ends with the author resubmitting at the prior. Per-cell
//! failure probabilities and per-CPU-cell stretch are precomputed once per
//! decision.
//!
//! `oracle_cost` is the cost of the cheapest candidate that succeeds on its
//! first attempt with stretch 1 for the demand's median scenario.

use crate::grid::{Grid, GridAxis};
use crate::normalize::{CellDist, Normalized};
use crate::types::{
    AcceleratorId, Bytes, GpuShape, MilliCpu, NodeShape, ParallelismMode, Penalties, PlatformRef, PriceModel, Rates,
    Requests, Shape,
};

/// The tuner's default CPU node: m6i.xlarge, 4 vCPU, 16 GiB, $0.192/h
/// (`tuner:bench/grid.py`), used when the platform lists no shapes.
pub const DEFAULT_NODE: (f64, f64, f64) = (4.0, 16.0, 0.192);
/// Host CPU (cores) and memory (GiB) bundled per GPU on a synthetic GPU node,
/// used when the platform lists no GPU shapes.
pub const DEFAULT_HOST_PER_GPU: (f64, f64) = (8.0, 32.0);
/// CUDA context + allocator reserve per used GPU (`tuner:bench_gpu/grid.py`).
pub const GPU_CONTEXT_BYTES: f64 = 0.5 * Bytes::GIB as f64;

/// Placeholder on-demand $/device-hour when the platform gives no price.
pub fn default_gpu_price(a: AcceleratorId) -> f64 {
    use AcceleratorId::*;
    match a {
        T4_16G => 0.526,
        L4_24G => 0.978,
        A10G_24G => 1.212,
        A10_24G => 1.1,
        L40S_48G => 2.242,
        A100_40G => 3.0,
        A100_80G => 4.0,
        H100_80G => 6.0,
        H200_141G => 8.0,
        B200_180G => 10.0,
        MI300X_192G => 6.0,
        TPU_V5E => 1.2,
        TPU_V5P => 4.2,
        TPU_V6E => 2.7,
        TRN1 => 1.34,
        TRN2 => 4.0,
        INF2 => 0.76,
        UnknownGpu => 2.0,
    }
}

/// Tunables of the cost model.
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct CostParams {
    /// Risk penalties (SPEC §7.3).
    pub penalties: Penalties,
    /// Fixed cost of an attempt as a fraction of the nominal run (0.02).
    pub attempt_overhead: f64,
    /// Hours a GPU task without a GPU burns before failing (0.05).
    pub no_gpu_fail_hours: f64,
    /// Minimum burn fraction of a failing attempt (0.05).
    pub min_burn_frac: f64,
    /// Headroom applied to demand + overhead + staged bytes (1.05, SPEC §5.2).
    pub need_headroom: f64,
}

impl Default for CostParams {
    fn default() -> Self {
        CostParams {
            penalties: Penalties { oom_penalty: 4.0, delay_penalty: 1.0, neighbour_penalty: 2.0 },
            attempt_overhead: 0.02,
            no_gpu_fail_hours: 0.05,
            min_burn_frac: 0.05,
            need_headroom: 1.05,
        }
    }
}

/// A candidate request (SPEC §7.2 *a*).
#[derive(Clone, Debug, PartialEq)]
pub struct Candidate {
    /// Memory ask (`None` = unset).
    pub memory: Option<Bytes>,
    /// Memory that bounds failure: the ask, or the shape's usable memory for
    /// catalog / instance-bounded snap platforms (SPEC §5.8).
    pub effective_memory: Bytes,
    /// CPU ask (`None` = unset).
    pub cpu: Option<MilliCpu>,
    /// Accelerators.
    pub gpu: Option<GpuShape>,
    /// Index into the request model's shape list, for catalog / snap.
    pub shape: Option<usize>,
}

/// Expected cost and first-attempt failure probability of a candidate.
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct CandidateCost {
    /// Expected dollars, penalties included.
    pub expected: f64,
    /// P(first attempt fails).
    pub p_fail: f64,
    /// P(memory need > effective memory).
    pub p_mem: f64,
    /// P(busiest device's need > its memory).
    pub p_gpu: f64,
    /// P(timeout) at this CPU.
    pub p_timeout: f64,
    /// Expected stretch.
    pub stretch: f64,
    /// $/h of the first attempt.
    pub rate: f64,
}

/// Moves a candidate up the ladder: the request model's "smallest legal
/// request with at least this much memory".
pub trait MemoryStepper {
    /// The candidate with at least `at_least` effective memory (or the
    /// largest legal one), other axes unchanged.
    fn step_to(&self, from: &Candidate, at_least: Bytes) -> Candidate;
}

/// A ladder resolved for one decision.
#[derive(Clone, Debug, PartialEq)]
pub struct LadderPlan {
    /// Memory multiplier per step.
    pub factor: f64,
    /// Steps.
    pub steps: u32,
    /// The last step is raised to at least this.
    pub final_floor: Option<Bytes>,
    /// Nothing escalates above this.
    pub ceiling: Bytes,
    /// GPU shapes to step through on GPU OOM (empty = GPU held, not escalated).
    pub gpu_chain: Vec<GpuShape>,
}

/// Everything the cost model needs for one decision.
pub struct CostInputs<'a> {
    /// The normalized estimate.
    pub demand: &'a Normalized,
    /// Static platform facts.
    pub platform: &'a PlatformRef,
    /// Price model.
    pub price: &'a PriceModel,
    /// Node shapes (packed pricing).
    pub nodes: &'a [NodeShape],
    /// Catalog / snap shapes (instance pricing).
    pub catalog: &'a [Shape],
    /// The grid.
    pub grid: &'a Grid,
    /// The origin prior's requests (the author's resubmission after a terminal failure).
    pub prior: &'a Requests,
    /// The prior as a candidate (for pricing the terminal resubmission).
    pub prior_candidate: Candidate,
    /// Bytes staged in memory-backed volumes.
    pub staged_bytes: f64,
    /// The ladder, if it counts.
    pub ladder: Option<LadderPlan>,
    /// Model parameters.
    pub params: CostParams,
}

/// The precomputed cost model for one decision.
pub struct CostModel<'a> {
    inp: CostInputs<'a>,
    overhead: Vec<(f64, f64)>,
    cpu_overhead: f64,
    grid_mem: Vec<f64>,
    grid_mem_sf: Vec<f64>,
    grid_cpu: Vec<f64>,
    grid_cpu_stretch: Vec<f64>,
    walltime_h: Option<f64>,
    durations_h: Vec<(f64, f64)>,
    burn_frac: f64,
    oracle_rate: f64,
    prior_rate: f64,
    prior_stretch: f64,
    p_need_gpu: f64,
}

impl<'a> CostModel<'a> {
    /// Precompute per-cell failure probabilities, per-CPU-cell stretch and the oracle.
    pub fn new(inp: CostInputs<'a>, oracle_pool: &[Candidate]) -> Self {
        let overhead = inp.platform.overhead.memory_points();
        let cpu_overhead = inp.platform.overhead.cpu_millis as f64 / 1000.0;
        let grid_mem = inp.grid.values(GridAxis::Memory);
        let grid_cpu = inp.grid.values(GridAxis::Cpu);
        let d = inp.demand;
        let walltime_h = if d.duration.timeout_modeling { inp.platform.walltime_s.map(|w| w as f64 / 3600.0) } else { None };
        let durations_h = if walltime_h.is_some() {
            d.duration.scenarios.iter().map(|(w, s)| (*w, s / 3600.0)).collect()
        } else {
            vec![(1.0, d.duration.mean_s() / 3600.0)]
        };
        let p_need_gpu = match &d.gpu_memory {
            Some(g) if g.zero_cell => 1.0 - g.probs[0],
            Some(_) => 1.0,
            None => 0.0,
        };
        let mut m = CostModel {
            overhead,
            cpu_overhead,
            grid_mem_sf: Vec::new(),
            grid_cpu_stretch: Vec::new(),
            grid_mem,
            grid_cpu,
            walltime_h,
            durations_h,
            burn_frac: d.t_peak_frac.max(inp.params.min_burn_frac),
            oracle_rate: 0.0,
            prior_rate: 0.0,
            prior_stretch: 1.0,
            p_need_gpu,
            inp,
        };
        m.grid_mem_sf = m.grid_mem.iter().map(|&v| m.mem_sf_raw(v)).collect();
        m.grid_cpu_stretch = m.grid_cpu.iter().map(|&c| m.stretch_raw(c)).collect();
        let pc = m.inp.prior_candidate.clone();
        m.prior_rate = m.rate(&pc);
        m.prior_stretch = m.stretch(pc.cpu);
        m.oracle_rate = m.find_oracle_rate(oracle_pool);
        m
    }

    /// P(memory need > `effective` bytes): need = (demand + overhead + staged) × headroom.
    fn mem_sf_raw(&self, effective: f64) -> f64 {
        let Some(d) = &self.inp.demand.memory else { return 0.0 };
        let h = self.inp.params.need_headroom;
        self.overhead.iter().map(|(w, o)| w * d.sf(effective / h - o - self.inp.staged_bytes)).sum::<f64>().clamp(0.0, 1.0)
    }

    /// P(memory need > `effective`), using the per-cell table when on the grid.
    pub fn mem_sf(&self, effective: Bytes) -> f64 {
        let v = effective.0 as f64;
        let i = self.grid_mem.partition_point(|&x| x < v);
        if i < self.grid_mem.len() && self.grid_mem[i] == v { self.grid_mem_sf[i] } else { self.mem_sf_raw(v) }
    }

    fn stretch_raw(&self, cores: f64) -> f64 {
        let Some(d) = &self.inp.demand.cpu_mean else { return 1.0 };
        if cores <= 0.0 {
            return 1.0;
        }
        let o = self.cpu_overhead;
        d.expect(|x| ((x + o) / cores).max(1.0))
    }

    /// Expected stretch `E[max(1, cpu_mean / c)]` (1 when unset or no estimate).
    pub fn stretch(&self, cpu: Option<MilliCpu>) -> f64 {
        let Some(c) = cpu else { return 1.0 };
        let v = c.as_cores();
        let i = self.grid_cpu.partition_point(|&x| x < v - 1e-9);
        if i < self.grid_cpu.len() && (self.grid_cpu[i] - v).abs() < 1e-9 {
            self.grid_cpu_stretch[i]
        } else {
            self.stretch_raw(v)
        }
    }

    /// P(busiest device's need > its usable memory) on `gpu`.
    pub fn gpu_sf(&self, gpu: Option<GpuShape>) -> f64 {
        let (Some(g), Some(d)) = (gpu, &self.inp.demand.gpu_memory) else { return 0.0 };
        let Some(dev) = g.accel.usable_memory() else { return 0.0 };
        gpu_fail_prob(d, dev.0 as f64, g.count, self.inp.demand.parallelism.mode, self.inp.demand.parallelism.imbalance as f64)
    }

    /// $/h of an attempt at this candidate.
    pub fn rate(&self, c: &Candidate) -> f64 {
        let mem_b = c.memory.unwrap_or(Bytes(0)).0 as f64;
        let cores = c.cpu.map(|x| x.as_cores()).unwrap_or(0.0);
        let shape = c.shape.and_then(|i| self.inp.catalog.get(i));
        match self.inp.price {
            PriceModel::Packed => self.packed_rate(mem_b, cores, c.gpu),
            PriceModel::Linear { rates } => linear_rate(rates, mem_b, cores, c.gpu),
            PriceModel::Instance { usage } => match shape {
                Some(s) => s.price_for(usage.as_deref()),
                None => self.packed_rate(mem_b, cores, c.gpu),
            },
            PriceModel::UsageFloor { rates } => {
                let billed_mem = match &self.inp.demand.memory {
                    Some(d) => {
                        let o = self.inp.platform.overhead.median_bytes() + self.inp.staged_bytes;
                        d.expect(|x| (x + o).max(mem_b))
                    }
                    None => mem_b,
                };
                let billed_cpu = match &self.inp.demand.cpu_mean {
                    Some(d) => d.expect(|x| x.max(cores)),
                    None => cores,
                };
                linear_rate(rates, billed_mem, billed_cpu, c.gpu)
            }
        }
    }

    fn packed_rate(&self, mem_b: f64, cores: f64, gpu: Option<GpuShape>) -> f64 {
        let gib = mem_b / Bytes::GIB as f64;
        let nodes = self.inp.nodes;
        let compatible: Vec<&NodeShape> = match gpu {
            Some(g) => nodes.iter().filter(|n| n.gpu.is_some_and(|ng| ng.accel == g.accel && ng.count >= g.count)).collect(),
            None => {
                let cpu_only: Vec<&NodeShape> = nodes.iter().filter(|n| n.gpu.is_none()).collect();
                if cpu_only.is_empty() { nodes.iter().collect() } else { cpu_only }
            }
        };
        if compatible.is_empty() {
            return match gpu {
                Some(g) => {
                    let (hc, hm) = DEFAULT_HOST_PER_GPU;
                    let n = g.count.max(1) as f64;
                    default_gpu_price(g.accel) * n.max(cores / hc).max(gib / hm)
                }
                None => {
                    let (nc, nm, p) = DEFAULT_NODE;
                    p * (cores / nc).max(gib / nm)
                }
            };
        }
        let mut best_fit = f64::INFINITY;
        let mut best_any = f64::INFINITY;
        for n in compatible {
            let mut share = (cores / n.cpu.as_cores().max(1e-9)).max(mem_b / (n.memory.0 as f64).max(1.0));
            if let (Some(g), Some(ng)) = (gpu, n.gpu) {
                share = share.max(g.count as f64 / ng.count.max(1) as f64);
            }
            let cost = n.price_per_hour * share;
            if share <= 1.0 + 1e-9 {
                best_fit = best_fit.min(cost);
            }
            best_any = best_any.min(cost);
        }
        if best_fit.is_finite() { best_fit } else { best_any }
    }

    fn find_oracle_rate(&self, pool: &[Candidate]) -> f64 {
        let d = self.inp.demand;
        let h = self.inp.params.need_headroom;
        let need_mem = d.memory.as_ref().map(|m| (m.median() + self.inp.platform.overhead.median_bytes() + self.inp.staged_bytes) * h);
        let need_cpu = d.cpu_mean.as_ref().map(|c| c.median() + self.cpu_overhead);
        let gpu_ok = |c: &Candidate| {
            let (Some(g), Some(dist)) = (c.gpu, &d.gpu_memory) else { return true };
            let Some(dev) = g.accel.usable_memory() else { return true };
            let med = dist.median();
            busiest_device_need(med, g.count, d.parallelism.mode, d.parallelism.imbalance as f64) <= dev.0 as f64 + 1.0
        };
        let ok = |c: &Candidate| {
            need_mem.is_none_or(|n| c.effective_memory.0 as f64 >= n)
                && need_cpu.is_none_or(|n| c.cpu.is_none_or(|x| x.as_cores() + 1e-9 >= n))
                && gpu_ok(c)
        };
        let fitting = pool.iter().filter(|c| ok(c)).map(|c| self.rate(c)).fold(f64::INFINITY, f64::min);
        if fitting.is_finite() {
            return fitting.max(1e-9);
        }
        // Nothing fits the median: the largest candidate stands in.
        pool.iter()
            .max_by_key(|c| (c.effective_memory, c.cpu))
            .map(|c| self.rate(c))
            .unwrap_or(self.prior_rate)
            .max(1e-9)
    }

    /// The oracle's $/h.
    pub fn oracle_rate(&self) -> f64 {
        self.oracle_rate
    }

    /// P(timeout) at a stretch.
    fn p_timeout(&self, stretch: f64) -> f64 {
        match self.walltime_h {
            Some(w) => self.durations_h.iter().filter(|(_, d)| stretch * d > w).map(|(p, _)| p).sum(),
            None => 0.0,
        }
    }

    /// Ladder states for a candidate: memory steps `r_0..r_M` and GPU steps `g_0..g_G`.
    fn ladder_states(&self, c: &Candidate, stepper: &dyn MemoryStepper) -> (Vec<Candidate>, Vec<Option<GpuShape>>) {
        let mut mem = vec![c.clone()];
        let mut gpus = vec![c.gpu];
        let enforced = self.inp.platform.memory_enforced;
        if let Some(l) = &self.inp.ladder {
            if enforced && self.inp.demand.memory.is_some() {
                for s in 0..l.steps {
                    let prev = mem.last().expect("non-empty");
                    let mut target = Bytes::from_f64(prev.effective_memory.0 as f64 * l.factor);
                    if s + 1 == l.steps
                        && let Some(f) = l.final_floor
                    {
                        target = target.max(f);
                    }
                    target = target.min(l.ceiling);
                    let next = stepper.step_to(prev, target);
                    if next.effective_memory <= prev.effective_memory {
                        break;
                    }
                    mem.push(next);
                }
            }
            if c.gpu.is_some() {
                for g in l.gpu_chain.iter().take(l.steps as usize) {
                    gpus.push(Some(*g));
                }
            }
        }
        (mem, gpus)
    }

    /// Expected cost of a candidate (closed form over ladder paths).
    pub fn evaluate(&self, c: &Candidate, stepper: &dyn MemoryStepper) -> CandidateCost {
        let p = &self.inp.params;
        let pen = p.penalties;
        let enforced = self.inp.platform.memory_enforced;
        let n_same = self.inp.platform.retry.same_size as f64;
        let (mem_states, gpu_states) = self.ladder_states(c, stepper);
        let stretch = self.stretch(c.cpu);
        let ao = p.attempt_overhead;

        // P(memory first fits at step i), i = 0..=M, plus M+1 = never.
        let sf_m: Vec<f64> = mem_states.iter().map(|s| if enforced { self.mem_sf(s.effective_memory) } else { 0.0 }).collect();
        let pm = first_fit_probs(&sf_m);
        let sf_g: Vec<f64> = gpu_states.iter().map(|g| self.gpu_sf(*g)).collect();
        let pg = first_fit_probs(&sf_g);

        // Rates per (memory step, GPU step).
        let rates: Vec<Vec<f64>> = mem_states
            .iter()
            .map(|ms| gpu_states.iter().map(|g| self.rate(&Candidate { gpu: *g, ..ms.clone() })).collect())
            .collect();
        let rate0 = rates[0][0];
        let p_timeout = self.p_timeout(stretch);
        let (big_m, big_g) = (mem_states.len() - 1, gpu_states.len() - 1);

        let mut expected = 0.0;
        for &(wd, d) in &self.durations_h {
            let timeout = self.walltime_h.is_some_and(|w| stretch * d > w);
            let oracle_cost = self.oracle_rate * d * (1.0 + ao);
            let oracle_hours = d * (1.0 + ao);
            let mut e_d = 0.0;
            for (km, &pkm) in pm.iter().enumerate() {
                if pkm <= 0.0 {
                    continue;
                }
                for (kg, &pkg) in pg.iter().enumerate() {
                    let prob = pkm * pkg;
                    if prob <= 0.0 {
                        continue;
                    }
                    let (mut i, mut g) = (0usize, 0usize);
                    let (mut cost, mut hours, mut fails) = (0.0, 0.0, 0.0);
                    let terminal = loop {
                        let rate = rates[i][g];
                        let mem_fail = i < km;
                        let gpu_fail = g < kg;
                        if !mem_fail && !gpu_fail {
                            if timeout {
                                let burn = self.walltime_h.unwrap_or(d) + ao * d;
                                cost += (1.0 + n_same) * rate * burn;
                                hours += (1.0 + n_same) * burn;
                                fails += 1.0 + n_same;
                                break true;
                            }
                            let run = d * (stretch + ao);
                            cost += rate * run;
                            hours += run;
                            break false;
                        }
                        let burn = d * (self.burn_frac * stretch + ao);
                        cost += (1.0 + n_same) * rate * burn;
                        hours += (1.0 + n_same) * burn;
                        fails += 1.0 + n_same;
                        if (mem_fail && i == big_m) || (gpu_fail && g == big_g) {
                            break true;
                        }
                        if mem_fail {
                            i += 1;
                        }
                        if gpu_fail {
                            g += 1;
                        }
                    };
                    if terminal {
                        // The author resubmits at the prior.
                        let run = d * (self.prior_stretch + ao);
                        cost += self.prior_rate * run;
                        hours += run;
                    }
                    let total = cost
                        + pen.oom_penalty * oracle_cost * fails
                        + pen.delay_penalty * self.oracle_rate * (hours - oracle_hours).max(0.0);
                    e_d += prob * total;
                }
            }
            if !enforced {
                let p_under = self.mem_sf_raw(c.effective_memory.0 as f64);
                e_d += pen.neighbour_penalty * p_under * oracle_cost;
            }
            if c.gpu.is_none() && self.p_need_gpu > 0.0 {
                e_d += self.p_need_gpu * (rate0 * p.no_gpu_fail_hours + pen.oom_penalty * oracle_cost);
            }
            expected += wd * e_d;
        }
        let p_mem = if enforced { sf_m[0] } else { 0.0 };
        let p_gpu = sf_g[0];
        let p_fail = 1.0 - (1.0 - p_mem) * (1.0 - p_gpu) * (1.0 - p_timeout);
        CandidateCost { expected, p_fail, p_mem, p_gpu, p_timeout, stretch, rate: rate0 }
    }
}

/// `P(first fits at step i)` for survival values `S(r_0) ≥ S(r_1) ≥ …`; the
/// last entry is P(never fits).
pub fn first_fit_probs(sf: &[f64]) -> Vec<f64> {
    let mut out = Vec::with_capacity(sf.len() + 1);
    let mut prev = 1.0;
    let mut run_min = 1.0f64;
    for &s in sf {
        let s = s.clamp(0.0, 1.0).min(run_min);
        run_min = s;
        out.push((prev - s).max(0.0));
        prev = s;
    }
    out.push(prev.max(0.0));
    out
}

/// The busiest device's need for a total GPU memory demand (SPEC §5.4).
pub fn busiest_device_need(total: f64, count: u32, mode: ParallelismMode, imbalance: f64) -> f64 {
    match mode {
        ParallelismMode::Sharded if count > 1 => {
            (total - GPU_CONTEXT_BYTES).max(0.0) * imbalance.max(1.0) / count as f64 + GPU_CONTEXT_BYTES
        }
        _ => total,
    }
}

/// P(busiest device's need > `device_bytes`) on `count` devices.
pub fn gpu_fail_prob(d: &CellDist, device_bytes: f64, count: u32, mode: ParallelismMode, imbalance: f64) -> f64 {
    let threshold = match mode {
        ParallelismMode::Sharded if count > 1 => {
            (device_bytes - GPU_CONTEXT_BYTES).max(0.0) * count as f64 / imbalance.max(1.0) + GPU_CONTEXT_BYTES
        }
        _ => device_bytes,
    };
    d.sf(threshold)
}

fn linear_rate(r: &Rates, mem_b: f64, cores: f64, gpu: Option<GpuShape>) -> f64 {
    let mut x = cores * r.cpu_core_hr + mem_b / Bytes::GIB as f64 * r.mem_gib_hr;
    if let Some(g) = gpu {
        x += g.count as f64 * r.gpu_hr.get(&g.accel).copied().unwrap_or_else(|| default_gpu_price(g.accel));
    }
    x
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::normalize::{DurationModel, normalize_axis};
    use crate::types::{AxisEstimate, EstimateKind, Ladder, Parallelism, PlatformRetry};

    struct GridStepper(Vec<f64>);
    impl MemoryStepper for GridStepper {
        fn step_to(&self, from: &Candidate, at_least: Bytes) -> Candidate {
            let (i, over) = crate::grid::covering(&self.0, at_least.0 as f64);
            let m = if over { at_least } else { Bytes(self.0[i] as u64) };
            Candidate { memory: Some(m), effective_memory: m, ..from.clone() }
        }
    }

    fn demand(mem_gib: f64, cpu: f64) -> Normalized {
        let g = Grid::rs_grid_v1();
        Normalized {
            kind: EstimateKind::Demand,
            memory: Some(normalize_axis(&AxisEstimate::Point { value: mem_gib * Bytes::GIB as f64 }, &g, GridAxis::Memory, 0.35, "m").unwrap()),
            cpu_mean: Some(normalize_axis(&AxisEstimate::Point { value: cpu }, &g, GridAxis::Cpu, 0.35, "c").unwrap()),
            gpu_memory: None,
            parallelism: Parallelism::default(),
            duration: DurationModel::default_hour(),
            t_peak_frac: 0.5,
        }
    }

    fn cand(mem_gib: f64, cpu: f64) -> Candidate {
        let m = Bytes::from_gib_f64(mem_gib);
        Candidate { memory: Some(m), effective_memory: m, cpu: Some(MilliCpu::from_cores_f64(cpu)), gpu: None, shape: None }
    }

    fn model<'a>(d: &'a Normalized, p: &'a PlatformRef, g: &'a Grid, prior: &'a Requests, ladder: Option<LadderPlan>) -> CostModel<'a> {
        let pool: Vec<Candidate> = g
            .memory
            .iter()
            .flat_map(|m| g.cpu.iter().map(move |c| Candidate { memory: Some(*m), effective_memory: *m, cpu: Some(*c), gpu: None, shape: None }))
            .collect();
        CostModel::new(
            CostInputs {
                demand: d,
                platform: p,
                price: &PriceModel::Packed,
                nodes: &[],
                catalog: &[],
                grid: g,
                prior,
                prior_candidate: cand(16.0, 4.0),
                staged_bytes: 0.0,
                ladder,
                params: CostParams::default(),
            },
            &pool,
        )
    }

    #[test]
    fn first_fit_probs_sum_to_one() {
        let p = first_fit_probs(&[0.5, 0.2, 0.05]);
        assert_eq!(p.len(), 4);
        assert!((p.iter().sum::<f64>() - 1.0).abs() < 1e-12);
        assert!((p[0] - 0.5).abs() < 1e-12 && (p[3] - 0.05).abs() < 1e-12);
        // Non-monotone survival is clamped.
        let q = first_fit_probs(&[0.2, 0.4]);
        assert!(q.iter().all(|x| *x >= 0.0));
    }

    #[test]
    fn under_ask_costs_more_than_a_fit() {
        let g = Grid::rs_grid_v1();
        let d = demand(3.0, 1.0);
        let p = PlatformRef::new("k8s");
        let prior = Requests::default();
        let m = model(&d, &p, &g, &prior, None);
        let st = GridStepper(g.values(GridAxis::Memory));
        let small = m.evaluate(&cand(1.0, 1.0), &st);
        let fit = m.evaluate(&cand(6.0, 1.0), &st);
        let huge = m.evaluate(&cand(64.0, 1.0), &st);
        assert!(small.p_mem > 0.9 && fit.p_mem < 0.05);
        assert!(fit.expected < small.expected, "{fit:?} vs {small:?}");
        assert!(fit.expected < huge.expected);
    }

    #[test]
    fn stretch_penalizes_cpu_under_ask() {
        let g = Grid::rs_grid_v1();
        let d = demand(1.0, 4.0);
        let p = PlatformRef::new("k8s");
        let prior = Requests::default();
        let m = model(&d, &p, &g, &prior, None);
        assert!(m.stretch(Some(MilliCpu::cores(1))) > 3.0);
        assert!((m.stretch(Some(MilliCpu::cores(16))) - 1.0).abs() < 1e-3);
        assert_eq!(m.stretch(None), 1.0);
    }

    #[test]
    fn ladder_makes_reductions_cheaper() {
        let g = Grid::rs_grid_v1();
        let d = demand(3.0, 1.0);
        let mut p = PlatformRef::new("k8s");
        p.retry = PlatformRetry { same_size: 0, ladder: Some(Ladder::caller()) };
        let prior = Requests::default();
        let st = GridStepper(g.values(GridAxis::Memory));
        let with = model(&d, &p, &g, &prior, Some(LadderPlan { factor: 2.0, steps: 2, final_floor: None, ceiling: Bytes::gib(64), gpu_chain: vec![] }));
        let p0 = PlatformRef::new("k8s");
        let without = model(&d, &p0, &g, &prior, None);
        let a = with.evaluate(&cand(2.0, 1.0), &st);
        let b = without.evaluate(&cand(2.0, 1.0), &st);
        assert!(a.expected <= b.expected + 1e-12, "{a:?} {b:?}");
    }

    #[test]
    fn same_size_retries_burn_more() {
        let g = Grid::rs_grid_v1();
        let d = demand(3.0, 1.0);
        let st = GridStepper(g.values(GridAxis::Memory));
        let prior = Requests::default();
        let p0 = PlatformRef::new("k8s");
        let mut p3 = PlatformRef::new("k8s");
        p3.retry.same_size = 3;
        let a = model(&d, &p0, &g, &prior, None).evaluate(&cand(1.0, 1.0), &st);
        let b = model(&d, &p3, &g, &prior, None).evaluate(&cand(1.0, 1.0), &st);
        assert!(b.expected > a.expected);
    }

    #[test]
    fn unenforced_memory_has_no_oom_but_a_neighbour_premium() {
        let g = Grid::rs_grid_v1();
        let d = demand(3.0, 1.0);
        let st = GridStepper(g.values(GridAxis::Memory));
        let prior = Requests::default();
        let mut p = PlatformRef::new("ray");
        p.memory_enforced = false;
        let m = model(&d, &p, &g, &prior, None);
        let small = m.evaluate(&cand(1.0, 1.0), &st);
        assert_eq!(small.p_mem, 0.0);
        let fit = m.evaluate(&cand(6.0, 1.0), &st);
        assert!(small.expected > fit.expected * 0.5, "premium applies");
    }

    #[test]
    fn sharded_gpu_need() {
        let gib = Bytes::GIB as f64;
        let n = busiest_device_need(40.0 * gib, 4, ParallelismMode::Sharded, 1.25);
        assert!((n / gib - (39.5 * 1.25 / 4.0 + 0.5)).abs() < 1e-9);
        assert_eq!(busiest_device_need(40.0 * gib, 4, ParallelismMode::Replicated, 1.25), 40.0 * gib);
    }

    #[test]
    fn packed_default_node_matches_tuner() {
        let g = Grid::rs_grid_v1();
        let d = demand(3.0, 1.0);
        let p = PlatformRef::new("k8s");
        let prior = Requests::default();
        let m = model(&d, &p, &g, &prior, None);
        // 16Gi with 1 CPU on a 4-core/16Gi node costs the whole node.
        assert!((m.rate(&cand(16.0, 1.0)) - 0.192).abs() < 1e-12);
        assert!((m.rate(&cand(1.0, 2.0)) - 0.096).abs() < 1e-12);
    }
}
