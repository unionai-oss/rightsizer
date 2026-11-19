//! Constraints: the feasible set before the argmin (SPEC §7.4), in order of
//! authority: pins, holds, ceiling (S4), floor (platform ∨ size-scaled), trust
//! region; plus the limits policy (S2).

use crate::grid::Grid;
use crate::safety::{headroom, same_digest, scale, trusted};
use crate::types::{
    AcceleratorId, Axis, Bytes, Constraints, GpuShape, Hold, HoldReason, Limits, MilliCpu, PlatformState, Prior,
    Requests, RequestModel, RunRecord, RunStatus, Shape,
};

/// Tunables of the constraint stage.
#[derive(Clone, Debug, PartialEq)]
pub struct ConstraintParams {
    /// `enforce_without_prior` (SPEC §7.4).
    pub enforce_without_prior: bool,
    /// `allow_gpu_type_change` (SPEC §5.4).
    pub allow_gpu_type_change: bool,
    /// `allowed_accelerators` when type changes are allowed.
    pub allowed_accelerators: Option<Vec<AcceleratorId>>,
    /// `history.allow_unverified_for_reductions` (SPEC §8.2).
    pub allow_unverified: bool,
    /// Trust region width in cells (2).
    pub max_step_down: u32,
    /// Records considered for the anchor (5).
    pub anchor_k: usize,
    /// Records considered for the size-scaled floor (5).
    pub floor_k: usize,
}

impl Default for ConstraintParams {
    fn default() -> Self {
        ConstraintParams {
            enforce_without_prior: false,
            allow_gpu_type_change: false,
            allowed_accelerators: None,
            allow_unverified: false,
            max_step_down: 2,
            anchor_k: 5,
            floor_k: 5,
        }
    }
}

fn push_hold(holds: &mut Vec<Hold>, h: Hold) {
    if !holds.iter().any(|x| x.axis == h.axis) {
        holds.push(h);
    }
}

/// Structural holds, highest authority first; at most one per axis (SPEC §7.4
/// items 1–2, §5.4): pins, adapter holds, GPU count (always in v1; permanently
/// for distributed units), GPU type unless allowed (never for `unknown-gpu`),
/// ephemeral storage and shm (no grid), and missing priors unless
/// `enforce_without_prior`.
pub fn structural_holds(prior: &Prior, c: &Constraints, distributed: bool, p: &ConstraintParams) -> Vec<Hold> {
    let mut holds = Vec::new();
    let r = &prior.spec.requests;
    for a in c.pinned.iter() {
        push_hold(&mut holds, Hold::new(a, HoldReason::Pinned));
    }
    for h in &c.held {
        push_hold(&mut holds, h.clone());
    }
    if let Some(g) = r.gpu {
        push_hold(
            &mut holds,
            Hold::new(Axis::GpuCount, if distributed { HoldReason::Distributed } else { HoldReason::GpuCountV1 }),
        );
        if g.accel.is_unknown() {
            push_hold(&mut holds, Hold::new(Axis::GpuType, HoldReason::UnknownGpu));
        } else if !p.allow_gpu_type_change {
            push_hold(&mut holds, Hold::new(Axis::GpuType, HoldReason::GpuType));
        }
    }
    if r.ephemeral_storage.is_some() || prior.spec.limits.ephemeral_storage.is_some() {
        push_hold(&mut holds, Hold::new(Axis::EphemeralStorage, HoldReason::NoGrid));
    }
    if r.shm.is_some() {
        push_hold(&mut holds, Hold::new(Axis::Shm, HoldReason::NoGrid));
    }
    if !p.enforce_without_prior {
        if r.memory.is_none() {
            push_hold(&mut holds, Hold::new(Axis::Memory, HoldReason::NoPrior));
        }
        if r.cpu.is_none() {
            push_hold(&mut holds, Hold::new(Axis::Cpu, HoldReason::NoPrior));
        }
    }
    holds
}

/// What bounds an axis's ceiling.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum CeilingSource {
    /// The largest node or catalog shape.
    Shape,
    /// Remaining quota.
    Quota,
    /// A user ceiling.
    User,
    /// Nothing: the grid's top cell.
    Grid,
}

impl CeilingSource {
    /// Name used in advisories.
    pub fn as_str(self) -> &'static str {
        match self {
            CeilingSource::Shape => "shape",
            CeilingSource::Quota => "quota",
            CeilingSource::User => "user",
            CeilingSource::Grid => "grid",
        }
    }
}

/// Per-unit bounds (SPEC §7.4 items 3–5).
#[derive(Clone, Debug, PartialEq)]
pub struct Bounds {
    /// Platform minimum memory.
    pub mem_platform_floor: Option<Bytes>,
    /// Size-scaled floor from history.
    pub mem_size_floor: Option<Bytes>,
    /// Effective memory floor (max of the two, clamped to the ceiling).
    pub mem_floor: Option<Bytes>,
    /// Memory ceiling.
    pub mem_ceiling: Bytes,
    /// What bounds it.
    pub mem_ceiling_source: CeilingSource,
    /// CPU floor.
    pub cpu_floor: Option<MilliCpu>,
    /// CPU ceiling.
    pub cpu_ceiling: MilliCpu,
    /// What bounds it.
    pub cpu_ceiling_source: CeilingSource,
    /// The trust-region anchor.
    pub anchor: Option<Bytes>,
    /// The floor exceeded the ceiling (memory): the ceiling wins.
    pub infeasible_floor: bool,
}

fn gpu_compatible(have: Option<GpuShape>, want: Option<GpuShape>) -> bool {
    match (want, have) {
        (Some(w), Some(h)) => h.accel == w.accel && h.count >= w.count,
        (Some(_), None) => false,
        (None, _) => true,
    }
}

/// The ceiling (S4): the largest compatible node or catalog shape, ∧ the
/// remaining quota, ∧ the user ceiling; the grid's top cell when nothing
/// bounds it.
pub fn ceiling(state: &PlatformState, c: &Constraints, gpu: Option<GpuShape>, grid: &Grid) -> (Bytes, CeilingSource, MilliCpu, CeilingSource) {
    let mut mem = (grid.memory.last().copied().unwrap_or(Bytes(u64::MAX)), CeilingSource::Grid);
    let mut cpu = (grid.cpu.last().copied().unwrap_or(MilliCpu(u32::MAX)), CeilingSource::Grid);
    let mut shape_mem: Option<Bytes> = None;
    let mut shape_cpu: Option<MilliCpu> = None;
    let catalog: &[Shape] = match &state.request_model {
        RequestModel::Catalog { shapes } | RequestModel::Snap { shapes, .. } => shapes,
        _ => &[],
    };
    let cat: Vec<&Shape> = catalog.iter().filter(|s| gpu_compatible(s.gpu, gpu)).collect();
    if !cat.is_empty() {
        shape_mem = cat.iter().map(|s| s.usable()).max();
        shape_cpu = cat.iter().map(|s| s.cpu).max();
    } else {
        let nodes: Vec<_> = match gpu {
            Some(_) => state.shapes.iter().filter(|n| gpu_compatible(n.gpu, gpu)).collect(),
            None => {
                let cpu_only: Vec<_> = state.shapes.iter().filter(|n| n.gpu.is_none()).collect();
                if cpu_only.is_empty() { state.shapes.iter().collect() } else { cpu_only }
            }
        };
        if !nodes.is_empty() {
            shape_mem = nodes.iter().map(|n| n.memory).max();
            shape_cpu = nodes.iter().map(|n| n.cpu).max();
        }
    }
    if let Some(m) = shape_mem {
        mem = (m, CeilingSource::Shape);
    }
    if let Some(x) = shape_cpu {
        cpu = (x, CeilingSource::Shape);
    }
    if let Some(q) = &state.quota {
        if let Some(m) = q.memory.filter(|m| *m < mem.0) {
            mem = (m, CeilingSource::Quota);
        }
        if let Some(x) = q.cpu.filter(|x| *x < cpu.0) {
            cpu = (x, CeilingSource::Quota);
        }
    }
    if let Some(m) = c.ceiling.memory.filter(|m| *m < mem.0) {
        mem = (m, CeilingSource::User);
    }
    if let Some(x) = c.ceiling.cpu.filter(|x| *x < cpu.0) {
        cpu = (x, CeilingSource::User);
    }
    (mem.0, mem.1, cpu.0, cpu.1)
}

/// Same-digest, trusted, successful records, newest first, at most `k`.
fn recent_successes<'a>(history: &'a [RunRecord], digest: Option<&'a str>, k: usize, allow_unverified: bool) -> impl Iterator<Item = &'a RunRecord> {
    history
        .iter()
        .filter(move |r| r.status == RunStatus::Succeeded && same_digest(r, digest) && trusted(r, allow_unverified))
        .take(k)
}

/// The size-scaled floor (SPEC §7.4 item 4): the largest measured peak among
/// the last K successful same-digest runs × `scale` × that record's headroom.
pub fn size_scaled_floor(history: &[RunRecord], digest: Option<&str>, b_now: Option<u64>, p: &ConstraintParams) -> Option<Bytes> {
    recent_successes(history, digest, p.floor_k, p.allow_unverified)
        .filter_map(|r| {
            let m = r.memory_peak?;
            (m.value.is_finite() && m.value > 0.0)
                .then(|| m.value * scale(b_now, r.input_bytes()) * headroom(m.resolution_s, false))
        })
        .fold(None, |a: Option<f64>, b| Some(a.map_or(b, |a| a.max(b))))
        // Rounded up to a whole MiB so rendered requests stay readable.
        .map(|f| Bytes(Bytes::from_f64(f).0.div_ceil(Bytes::MIB) * Bytes::MIB))
}

/// The trust-region anchor (SPEC §7.4 item 5): the smallest `requested` memory
/// among the last K same-digest `Succeeded` records with trust ≥
/// Authenticated, else the prior.
pub fn anchor(history: &[RunRecord], digest: Option<&str>, prior_memory: Option<Bytes>, p: &ConstraintParams) -> Option<Bytes> {
    recent_successes(history, digest, p.anchor_k, p.allow_unverified)
        .filter_map(|r| r.requested_memory())
        .min()
        .map(Bytes)
        .or(prior_memory)
}

/// The smallest memory the trust region admits on a grid: `max_step_down`
/// cells below the anchor's covering cell.
pub fn trust_region_min_grid(grid: &Grid, anchor: Bytes, max_step_down: u32) -> Bytes {
    let idx = grid.memory_cell(anchor);
    grid.memory[idx.saturating_sub(max_step_down as usize)].min(anchor)
}

/// The smallest effective memory the trust region admits on catalog / snap
/// platforms: `anchor × 2^(−max_step_down / 2)` (2 cells = half, SPEC §5.8).
pub fn trust_region_min_ratio(anchor: Bytes, max_step_down: u32) -> Bytes {
    Bytes::from_f64(anchor.0 as f64 * 0.5f64.powf(max_step_down as f64 / 2.0))
}

/// Compute bounds for one unit.
#[allow(clippy::too_many_arguments)]
pub fn bounds(
    prior: &Prior,
    c: &Constraints,
    state: &PlatformState,
    grid: &Grid,
    history: &[RunRecord],
    digest: Option<&str>,
    b_now: Option<u64>,
    p: &ConstraintParams,
) -> Bounds {
    let gpu = prior.spec.requests.gpu;
    let (mem_ceiling, mem_src, cpu_ceiling, cpu_src) = ceiling(state, c, gpu, grid);
    let platform_floor = [state.floors.memory, c.floor.memory].into_iter().flatten().max();
    let size_floor = size_scaled_floor(history, digest, b_now, p);
    let raw_floor = [platform_floor, size_floor].into_iter().flatten().max();
    let infeasible_floor = raw_floor.is_some_and(|f| f > mem_ceiling);
    let cpu_floor = [state.floors.cpu, c.floor.cpu].into_iter().flatten().max().map(|f| f.min(cpu_ceiling));
    Bounds {
        mem_platform_floor: platform_floor.map(|f| f.min(mem_ceiling)),
        mem_size_floor: size_floor,
        mem_floor: raw_floor.map(|f| f.min(mem_ceiling)),
        mem_ceiling,
        mem_ceiling_source: mem_src,
        cpu_floor,
        cpu_ceiling,
        cpu_ceiling_source: cpu_src,
        anchor: anchor(history, digest, prior.spec.requests.memory, p),
        infeasible_floor,
    }
}

/// Whether CPU must stay whole cores: a Guaranteed prior with whole CPUs
/// (static CPU manager, SPEC §7.4).
pub fn whole_cpu_required(prior: &Prior) -> bool {
    prior.is_guaranteed() && prior.spec.requests.cpu.is_some_and(|c| c.is_whole())
}

fn ratio_limit(new_req: u64, prior_req: Option<u64>, prior_lim: u64) -> u64 {
    match prior_req {
        Some(r) if r > 0 => {
            let ratio = prior_lim as f64 / r as f64;
            (((new_req as f64) * ratio).ceil() as u64).max(new_req)
        }
        // A limit with no request (K8s defaults request = limit): limit = request.
        _ => new_req,
    }
}

/// The limits policy (S2, SPEC §7.4): a limit exists only where the prior had
/// one; it preserves the prior's limit/request ratio and is ≥ the new
/// request; a Guaranteed prior keeps limits = requests. Ephemeral-storage
/// limits are held.
pub fn apply_limits(prior: &Prior, req: &Requests) -> Limits {
    let pl = &prior.spec.limits;
    let pr = &prior.spec.requests;
    let guaranteed = prior.is_guaranteed();
    let memory = match (pl.memory, req.memory) {
        (Some(l), Some(n)) => Some(Bytes(if guaranteed { n.0 } else { ratio_limit(n.0, pr.memory.map(|b| b.0), l.0) })),
        (Some(l), None) => Some(l),
        (None, _) => None,
    };
    let cpu = match (pl.cpu, req.cpu) {
        (Some(l), Some(n)) => {
            let v = if guaranteed { n.0 as u64 } else { ratio_limit(n.0 as u64, pr.cpu.map(|c| c.0 as u64), l.0 as u64) };
            Some(MilliCpu(v.min(u32::MAX as u64) as u32))
        }
        (Some(l), None) => Some(l),
        (None, _) => None,
    };
    Limits { cpu, memory, ephemeral_storage: pl.ephemeral_storage }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{AxisSet, NodeShape, QosClass, Quota, ResourceSpec};

    fn prior(mem_gib: u64, cpu_m: u32) -> Prior {
        Prior::from_requests(Requests { memory: Some(Bytes::gib(mem_gib)), cpu: Some(MilliCpu(cpu_m)), ..Default::default() })
    }

    #[test]
    fn holds_in_order_of_authority() {
        let mut p = prior(8, 2000);
        p.spec.requests.gpu = Some("A100-80G:2".parse().unwrap());
        p.spec.requests.shm = Some(Bytes::gib(1));
        let c = Constraints { pinned: AxisSet::from(Axis::Memory), ..Default::default() };
        let h = structural_holds(&p, &c, true, &ConstraintParams::default());
        let get = |a| h.iter().find(|x| x.axis == a).map(|x| x.reason);
        assert_eq!(get(Axis::Memory), Some(HoldReason::Pinned));
        assert_eq!(get(Axis::GpuCount), Some(HoldReason::Distributed));
        assert_eq!(get(Axis::GpuType), Some(HoldReason::GpuType));
        assert_eq!(get(Axis::Shm), Some(HoldReason::NoGrid));
        assert_eq!(get(Axis::Cpu), None);
        let mut u = prior(8, 2000);
        u.spec.requests.gpu = Some("unknown-gpu:1".parse().unwrap());
        let h = structural_holds(&u, &Constraints::default(), false, &ConstraintParams { allow_gpu_type_change: true, ..Default::default() });
        assert!(h.iter().any(|x| x.axis == Axis::GpuType && x.reason == HoldReason::UnknownGpu));
        assert!(h.iter().any(|x| x.axis == Axis::GpuCount && x.reason == HoldReason::GpuCountV1));
        let none = Prior::default();
        let h = structural_holds(&none, &Constraints::default(), false, &ConstraintParams::default());
        assert_eq!(h.len(), 2);
        assert!(h.iter().all(|x| x.reason == HoldReason::NoPrior));
        let h = structural_holds(&none, &Constraints::default(), false, &ConstraintParams { enforce_without_prior: true, ..Default::default() });
        assert!(h.is_empty());
    }

    #[test]
    fn ceiling_is_min_of_shape_quota_user() {
        let g = Grid::rs_grid_v1();
        let mut s = PlatformState {
            shapes: vec![
                NodeShape { name: None, cpu: MilliCpu::cores(16), memory: Bytes::gib(64), gpu: None, price_per_hour: 0.8, host_per_gpu: None },
                NodeShape { name: None, cpu: MilliCpu::cores(8), memory: Bytes::gib(32), gpu: None, price_per_hour: 0.4, host_per_gpu: None },
            ],
            ..Default::default()
        };
        let (m, ms, c, _) = ceiling(&s, &Constraints::default(), None, &g);
        assert_eq!((m, ms, c), (Bytes::gib(64), CeilingSource::Shape, MilliCpu::cores(16)));
        s.quota = Some(Quota { memory: Some(Bytes::gib(48)), ..Default::default() });
        assert_eq!(ceiling(&s, &Constraints::default(), None, &g).1, CeilingSource::Quota);
        let user = Constraints { ceiling: Requests { memory: Some(Bytes::gib(20)), ..Default::default() }, ..Default::default() };
        let (m, ms, ..) = ceiling(&s, &user, None, &g);
        assert_eq!((m, ms), (Bytes::gib(20), CeilingSource::User));
        let (m, ms, ..) = ceiling(&PlatformState::default(), &Constraints::default(), None, &g);
        assert_eq!((m, ms), (Bytes::gib(2048), CeilingSource::Grid));
    }

    #[test]
    fn limits_policy() {
        let mut p = prior(8, 2000);
        // No limit: none added.
        let r = Requests { memory: Some(Bytes::gib(4)), cpu: Some(MilliCpu(1000)), ..Default::default() };
        assert!(apply_limits(&p, &r).is_empty());
        // 2× ratio preserved.
        p.spec.limits.memory = Some(Bytes::gib(16));
        p.spec.limits.cpu = Some(MilliCpu(3000));
        let l = apply_limits(&p, &r);
        assert_eq!(l.memory, Some(Bytes::gib(8)));
        assert_eq!(l.cpu, Some(MilliCpu(1500)));
        // Guaranteed: limits = requests.
        p.spec.limits = crate::types::Limits { memory: Some(Bytes::gib(8)), cpu: Some(MilliCpu(2000)), ephemeral_storage: None };
        p.qos = Some(QosClass::Guaranteed);
        let l = apply_limits(&p, &r);
        assert_eq!((l.memory, l.cpu), (r.memory, r.cpu));
        assert!(whole_cpu_required(&p));
    }

    #[test]
    fn trust_region() {
        let g = Grid::rs_grid_v1();
        assert_eq!(trust_region_min_grid(&g, Bytes::gib(8), 2), Bytes::gib(4));
        assert_eq!(trust_region_min_grid(&g, Bytes::mib(1000), 2), Bytes::mib(512));
        assert_eq!(trust_region_min_grid(&g, Bytes::mib(256), 2), Bytes::mib(256));
        assert_eq!(trust_region_min_ratio(Bytes::gib(64), 2), Bytes::gib(32));
    }

    #[test]
    fn floor_and_anchor_from_history() {
        use crate::types::{Confidence, Measurement, OutcomeSource, Trust, WorkloadKey};
        let mk = |peak: f64, req: u64, digest: &str| RunRecord {
            v: 1,
            key: WorkloadKey::parse("k8s://c/ns/job/x").unwrap(),
            cluster: None,
            invocation_id: "i".into(),
            attempt: 1,
            decision_id: None,
            started_at: chrono::DateTime::from_timestamp(0, 0).unwrap(),
            inputs: vec![],
            code_digest: Some(digest.into()),
            requested: Some(ResourceSpec { requests: Requests { memory: Some(Bytes::gib(req)), ..Default::default() }, ..Default::default() }),
            origin_prior: None,
            status: RunStatus::Succeeded,
            status_confidence: Confidence::Observed,
            memory_peak: Some(Measurement::exact(peak * Bytes::GIB as f64)),
            cpu_mean: None,
            cpu_peak_window: None,
            cpu_throttled_frac: None,
            gpu_memory_peak: None,
            gpu_util_mean: None,
            t_peak_frac: None,
            duration_s: None,
            staged_bytes: None,
            source: OutcomeSource::Watcher,
            trust: Trust::Verified,
        };
        let h = vec![mk(2.0, 8, "a"), mk(3.0, 6, "a"), mk(10.0, 16, "b")];
        let p = ConstraintParams::default();
        let f = size_scaled_floor(&h, Some("a"), None, &p).unwrap();
        assert_eq!(f, Bytes(Bytes::from_f64(3.0 * 1.05 * Bytes::GIB as f64).0.div_ceil(Bytes::MIB) * Bytes::MIB));
        assert_eq!(anchor(&h, Some("a"), Some(Bytes::gib(32)), &p), Some(Bytes::gib(6)));
        assert_eq!(anchor(&h, Some("zzz"), Some(Bytes::gib(32)), &p), Some(Bytes::gib(32)));
        let b = bounds(&prior(32, 1000), &Constraints::default(), &PlatformState::default(), &Grid::rs_grid_v1(), &h, Some("a"), None, &p);
        assert!(!b.infeasible_floor);
        let tight = Constraints { ceiling: Requests { memory: Some(Bytes::gib(2)), ..Default::default() }, ..Default::default() };
        let b = bounds(&prior(32, 1000), &tight, &PlatformState::default(), &Grid::rs_grid_v1(), &h, Some("a"), None, &p);
        assert!(b.infeasible_floor);
        assert_eq!(b.mem_floor, Some(Bytes::gib(2)));
    }
}
