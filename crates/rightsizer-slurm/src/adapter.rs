//! The Slurm [`Adapter`]: native submissions ⇄ units and patches.
//!
//! # Native forms
//!
//! - `Native::Document { media_type: "text/x-shellscript", bytes }`: an
//!   sbatch script alone. Render → `Patch::TextEdits`.
//! - `Native::Object { value: {"kind": "sbatch", "argv": [...], "script": "...", "env": {...}} }`:
//!   what `rsz slurm sbatch` sees (argv after `sbatch`, the script text,
//!   the submitter's `SBATCH_*` variables). Render → `Patch::TextEdits`
//!   when every change is in the script, `Patch::Options` (long option
//!   name → value; `het{k}/{name}` for component *k* ≥ 1) when every change
//!   is in argv, and `Patch::Options` with the script edits under
//!   [`SCRIPT_EDITS_KEY`] when both.
//! - `Native::Object { value: {"kind": "cli_filter", "type": "sbatch", "offset": 0, "options": {...}, "script": "..."} }`:
//!   cli_filter's merged option table (`slurm.json_cli_options`). Render →
//!   `Patch::Options` with plain long names, the form cli_filter applies.
//!
//! Units are per job component (`job`, or `het[k]`); quantities are per
//! node (`--mem` is per node). Memory renders in the user's form (§3).

use std::collections::BTreeMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use rightsizer_core::units::format_slurm_mem;
use rightsizer_core::{
    AcceleratorId, Axis, AxisSet, Bytes, CodeInfo, CodeRef, Constraints, ContextSeed, GpuShape, GroupId, Hold, HoldReason, ManifestFeatures,
    MilliCpu, Prior, Requests, ResourceSpec, ScalarValue, UnitId,
};
use rightsizer_plugin::{Adapter, AdapterEnv, Extracted, Native, Patch, PluginError, Rejection, Result, SkipReason, TextEdit, Unit, apply_text_edits};

use crate::inventory::{Inventory, PartitionInfo};
use crate::keys::{key_suffix, key_suffix_het, script_digest, tenant};
use crate::options::{ArgForm, lookup_long};
use crate::sbatch::{CompResources, GpuFlag, MemForm, Origin, SbatchJob, from_cli_options, parse, parse_gres};
use crate::settings::SlurmSettings;
use crate::token::TOKEN_ENV;

/// Key under which mixed patches carry the script's text edits.
pub const SCRIPT_EDITS_KEY: &str = "_script_edits";

/// The object forms of [`Native::Object`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SlurmNative {
    /// `rsz slurm sbatch ARGS…`.
    Sbatch {
        /// argv after `sbatch`.
        argv: Vec<String>,
        /// Script text (absent for `--wrap`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        script: Option<String>,
        /// Submitter environment (`SBATCH_*`, `RIGHTSIZER_*`).
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        env: BTreeMap<String, String>,
    },
    /// cli_filter's option table.
    CliFilter {
        /// `sbatch`, `salloc`, `srun`.
        #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
        cmd: Option<String>,
        /// Het component.
        #[serde(default)]
        offset: usize,
        /// `slurm.json_cli_options(options)`.
        options: serde_json::Map<String, serde_json::Value>,
        /// Script text, read by the decide client.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        script: Option<String>,
        /// Script path (informational).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        script_path: Option<String>,
    },
}

/// How the submission arrived.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum NativeKind {
    /// Script only.
    Document,
    /// argv + script.
    Sbatch,
    /// cli_filter option table.
    CliFilter,
}

/// A parsed native submission.
#[derive(Clone, Debug, PartialEq)]
pub struct Parsed {
    /// The job.
    pub job: SbatchJob,
    /// Form.
    pub kind: NativeKind,
    /// Environment (sbatch form).
    pub env: BTreeMap<String, String>,
    /// cli_filter command type.
    pub cmd: Option<String>,
    /// cli_filter het offset.
    pub offset: usize,
}

/// Parse a native submission.
pub fn parse_native(native: &Native) -> Result<Parsed> {
    match native {
        Native::Document { bytes, .. } => {
            let s = std::str::from_utf8(bytes).map_err(|_| PluginError::Parse("script is not UTF-8".into()))?;
            let job = parse(&[], Some(s), &BTreeMap::new()).map_err(|e| PluginError::Parse(e.to_string()))?;
            Ok(Parsed { job, kind: NativeKind::Document, env: BTreeMap::new(), cmd: None, offset: 0 })
        }
        Native::Object { value } => {
            let n: SlurmNative = serde_json::from_value(value.clone()).map_err(|e| PluginError::InvalidNative(e.to_string()))?;
            match n {
                SlurmNative::Sbatch { argv, script, env } => {
                    let job = parse(&argv, script.as_deref(), &env).map_err(|e| PluginError::Parse(e.to_string()))?;
                    Ok(Parsed { job, kind: NativeKind::Sbatch, env, cmd: Some("sbatch".into()), offset: 0 })
                }
                SlurmNative::CliFilter { cmd, offset, options, script, .. } => {
                    let job = from_cli_options(&options, offset, script.as_deref());
                    Ok(Parsed { job, kind: NativeKind::CliFilter, env: BTreeMap::new(), cmd, offset })
                }
            }
        }
        Native::ApiCall { .. } => Err(PluginError::Unsupported("slurm: API calls are not sbatch submissions".into())),
    }
}

impl Parsed {
    /// Unit id of component `k`.
    pub fn unit_id(&self, k: usize) -> UnitId {
        if self.is_het() { UnitId(format!("het[{}]", self.component_index(k))) } else { UnitId("job".into()) }
    }

    fn component_index(&self, k: usize) -> usize {
        if self.kind == NativeKind::CliFilter { self.offset } else { k }
    }

    /// Whether the job is heterogeneous.
    pub fn is_het(&self) -> bool {
        match self.kind {
            NativeKind::CliFilter => {
                self.offset > 0 || self.job.script.as_deref().is_some_and(|s| crate::sbatch::lines_with_offsets(s).iter().any(|(_, l)| {
                    crate::sbatch::directive_magic(l).is_some_and(|m| {
                        let r = l[m..].trim_start().to_ascii_lowercase();
                        r.starts_with("hetjob") || r.starts_with("packjob")
                    })
                }))
            }
            _ => self.job.components.len() > 1,
        }
    }

    /// Component index for a unit id.
    pub fn component_of(&self, unit: &UnitId) -> Option<usize> {
        (0..self.job.components.len()).find(|k| &self.unit_id(*k) == unit)
    }

    fn suffix(&self, k: usize) -> String {
        match self.kind {
            NativeKind::CliFilter => key_suffix_het(&self.job, self.offset, self.is_het()).0,
            _ => key_suffix(&self.job, k).0,
        }
    }
}

/// The Slurm adapter.
#[derive(Clone, Debug, Default)]
pub struct SlurmAdapter {
    /// Settings used when the env carries none.
    pub defaults: SlurmSettings,
}

impl SlurmAdapter {
    /// An adapter with default settings.
    pub fn new(defaults: SlurmSettings) -> Self {
        SlurmAdapter { defaults }
    }

    fn settings(&self, env: &AdapterEnv) -> SlurmSettings {
        if env.settings.is_empty() { self.defaults.clone() } else { SlurmSettings::from_env(env) }
    }
}

/// The account a submission is charged to: the authenticated one
/// (`AdapterEnv.namespace`, set by the daemon from `SO_PEERCRED` and the
/// user's associations), else `--account`, else the configured default.
pub fn account_of(job: &SbatchJob, env: &AdapterEnv, s: &SlurmSettings) -> String {
    env.namespace
        .clone()
        .or_else(|| job.components.first().and_then(|c| c.value("account")).map(str::to_string))
        .or_else(|| s.default_account.clone())
        .unwrap_or_default()
        .to_ascii_lowercase()
}

/// Accelerator of a gres type: the site table, else the partition's only
/// GPU type (untyped requests), else unknown.
fn accel_of(ty: Option<&str>, s: &SlurmSettings, part: Option<&PartitionInfo>) -> AcceleratorId {
    let map = s.accelerators();
    let ty = ty.map(str::to_string).or_else(|| {
        let mut types: Vec<String> =
            s.inventory.nodes_of(part).iter().flat_map(|n| n.gres.iter()).filter(|g| g.name == "gpu").filter_map(|g| g.gpu_type.clone()).collect();
        types.sort();
        types.dedup();
        (types.len() == 1).then(|| types.remove(0))
    });
    match ty {
        Some(t) => {
            let t = t.to_ascii_lowercase();
            let c = map.canonical(&t);
            if c.is_unknown() { t.parse::<AcceleratorId>().unwrap_or(AcceleratorId::UnknownGpu) } else { c }
        }
        None => AcceleratorId::UnknownGpu,
    }
}

const DIST_MARKERS: &[&str] = &["srun", "mpirun", "mpiexec", "torchrun", "deepspeed", "accelerate launch", "horovodrun", "WORLD_SIZE"];

fn markers(script: Option<&str>) -> Vec<String> {
    let Some(s) = script else { return vec![] };
    DIST_MARKERS.iter().filter(|m| s.contains(*m)).map(|m| m.to_string()).collect()
}

fn mib(b: Bytes) -> u64 {
    b.0.div_ceil(1 << 20)
}

/// The default memory per node when the job gives no memory option.
fn default_memory(r: &CompResources, part: Option<&PartitionInfo>, inv: &Inventory) -> Option<Bytes> {
    let per_cpu = part.and_then(|p| p.def_mem_per_cpu).or(if part.and_then(|p| p.def_mem_per_node).is_some() { None } else { inv.config.def_mem_per_cpu });
    let per_node = part.and_then(|p| p.def_mem_per_node).or(inv.config.def_mem_per_node);
    if let Some(c) = per_cpu {
        return Some(Bytes::mib(c * r.cpus_per_node() as u64));
    }
    per_node.map(Bytes::mib)
}

fn whole_node_memory(part: Option<&PartitionInfo>, inv: &Inventory) -> Option<Bytes> {
    inv.nodes_of(part).iter().filter(|n| n.usable()).map(|n| n.usable_memory_mib()).filter(|m| *m > 0).min().map(Bytes::mib)
}

/// Build the unit for component `k`.
fn build_unit(p: &Parsed, k: usize, s: &SlurmSettings, account: &str, env: &AdapterEnv) -> std::result::Result<Unit, String> {
    let c = &p.job.components[k];
    let r = CompResources::of(c);
    if !r.errors.is_empty() {
        return Err(format!("unparseable options: {}", r.errors.join(", ")));
    }
    let inv = &s.inventory;
    let part = inv.partition(r.partition.as_deref());
    let mut requests = Requests::default();
    let mut defaulted = AxisSet::empty();
    let mut held: Vec<Hold> = Vec::new();
    // CPU.
    requests.cpu = Some(MilliCpu::cores(r.cpus_per_node()));
    if r.cpus_per_task.is_none() {
        defaulted.insert(Axis::Cpu);
    }
    if c.has("cpus-per-gpu") || c.value("tres-per-task").is_some_and(|t| t.contains("cpu")) {
        held.push(Hold::with(Axis::Cpu, HoldReason::Platform, "CPUs derived from --cpus-per-gpu/--tres-per-task"));
    }
    // Memory.
    match (&r.mem, r.mem_per_node()) {
        (Some(_), Some(Bytes(0))) => {
            requests.memory = whole_node_memory(part, inv);
            held.push(Hold::with(Axis::Memory, HoldReason::Platform, "--mem=0 (the whole node) is never changed"));
        }
        (Some(_), Some(b)) => requests.memory = Some(b),
        (Some(_), None) => held.push(Hold::with(Axis::Memory, HoldReason::Platform, "--mem-per-gpu without a per-node GPU count")),
        (None, _) => {
            requests.memory = default_memory(&r, part, inv);
            if requests.memory.is_some() {
                defaulted.insert(Axis::Memory);
            }
        }
    }
    // GPU.
    if let Some(g) = &r.gpu {
        let accel = accel_of(g.gpu_type.as_deref(), s, part);
        let count = r.gpus_per_node().unwrap_or(g.count).max(1);
        requests.gpu = Some(GpuShape { accel, count });
        if r.gpu_ambiguous || g.flag == GpuFlag::GpusPerSocket {
            held.push(Hold::with(Axis::GpuCount, HoldReason::Platform, "several GPU options"));
            held.push(Hold::with(Axis::GpuType, HoldReason::Platform, "several GPU options"));
        }
        if g.gpu_type.is_none() {
            held.push(Hold::with(Axis::GpuType, HoldReason::GpuType, "untyped GPU request: typed and untyped forms are never mixed"));
        }
    }
    // Ephemeral storage and environment-set options.
    if let Some(t) = r.tmp {
        requests.ephemeral_storage = Some(t);
        held.push(Hold::with(Axis::EphemeralStorage, HoldReason::NoGrid, "--tmp is held in v1"));
    }
    for o in &c.opts {
        if let Origin::Env { var } = &o.origin {
            let axis = match o.name {
                "mem" | "mem-per-cpu" | "mem-per-gpu" => Some(Axis::Memory),
                "cpus-per-task" => Some(Axis::Cpu),
                "gres" | "gpus" | "gpus-per-node" | "gpus-per-task" => Some(Axis::GpuCount),
                _ => None,
            };
            if let Some(a) = axis
                && c.get(o.name).is_some_and(|e| matches!(e.origin, Origin::Env { .. }))
            {
                held.push(Hold::with(a, HoldReason::Platform, format!("set by {var}")));
            }
        }
    }
    // Ceilings: partition limits, association/QOS limits, node shapes.
    let nodes = r.nodes() as u64;
    let user = env.principal.as_deref();
    let (job_lim, node_lim, grp) = inv.limits(account, user, r.partition.as_deref(), r.qos.as_deref());
    let mut mem_ceiling: Vec<u64> = Vec::new();
    if let Some(p) = part {
        mem_ceiling.extend(p.max_mem_per_node);
        if let Some(pc) = p.max_mem_per_cpu {
            mem_ceiling.push(pc * r.cpus_per_node() as u64);
        }
    }
    mem_ceiling.extend(job_lim.mem_mib.map(|m| m / nodes));
    mem_ceiling.extend(grp.mem_mib.map(|m| m / nodes));
    mem_ceiling.extend(node_lim.mem_mib);
    let gtype = r.gpu.as_ref().and_then(|g| g.gpu_type.clone());
    let fitting: Vec<_> = inv.nodes_of(part).into_iter().filter(|n| n.usable() && r.gpu.as_ref().is_none_or(|_| n.gpus(gtype.as_deref()) > 0)).collect();
    if let Some(m) = fitting.iter().map(|n| n.usable_memory_mib()).max() {
        mem_ceiling.push(m);
    }
    let mut cpu_ceiling: Vec<u64> = Vec::new();
    cpu_ceiling.extend(fitting.iter().map(|n| n.cpus as u64).max());
    cpu_ceiling.extend(part.and_then(|p| p.max_cpus_per_node).map(u64::from));
    cpu_ceiling.extend(job_lim.cpu.map(|c| c / nodes));
    cpu_ceiling.extend(node_lim.cpu);
    let tpn = r.tasks_per_node() as u64;
    let ceiling = Requests {
        memory: mem_ceiling.into_iter().min().map(Bytes::mib),
        // Whole CPUs per task: the ceiling is a multiple of tasks per node.
        cpu: cpu_ceiling.into_iter().min().map(|c| MilliCpu::cores(((c / tpn) * tpn).max(tpn) as u32)),
        ..Default::default()
    };
    let walltime = match r.time_s {
        Some(t) => t,
        None => part.and_then(|p| p.default_time_s.or(p.max_time_s)),
    };
    let mut extra = BTreeMap::new();
    extra.insert("nodes".into(), ScalarValue::Int(nodes as i64));
    extra.insert("tasks_per_node".into(), ScalarValue::Int(tpn as i64));
    if let Some(w) = walltime {
        extra.insert("walltime_s".into(), ScalarValue::Int(w as i64));
    }
    let digest = format!("sha256:{}", script_digest(&p.job));
    let mut labels = BTreeMap::new();
    if let Some(pn) = part.map(|p| p.name.clone()).or(r.partition.clone()) {
        labels.insert("partition".into(), pn);
    }
    if let Some(q) = &r.qos {
        labels.insert("qos".into(), q.clone());
    }
    let distributed = r.distributed();
    Ok(Unit {
        id: p.unit_id(k),
        group: r.array.as_ref().map(|a| GroupId(format!("array:{a}"))),
        key_suffix: p.suffix(k),
        prior: Prior { spec: ResourceSpec { requests, limits: Default::default() }, defaulted, qos: None },
        seed: ContextSeed {
            inputs: vec![],
            code: Some(CodeRef::Info { info: CodeInfo { digest, tokens: vec![], source: None } }),
            manifest: ManifestFeatures {
                distributed,
                distributed_markers: markers(p.job.script.as_deref()),
                launcher: None,
                extra,
                ..Default::default()
            },
            labels,
        },
        constraints: Constraints { pinned: p.job.rightsizer.pins, held, ceiling, ..Default::default() },
        distributed,
        key: None,
    })
}

fn already_sized(p: &Parsed) -> Option<SkipReason> {
    if p.env.contains_key("RIGHTSIZER_DECIDED") || p.env.keys().any(|k| k.starts_with(TOKEN_ENV)) {
        return Some(SkipReason::AlreadySized { decision_id: p.env.get("RIGHTSIZER_DECIDED").cloned() });
    }
    let exported = p.job.components.iter().any(|c| c.value("export").is_some_and(|e| e.contains(TOKEN_ENV)));
    exported.then_some(SkipReason::AlreadySized { decision_id: None })
}

/// Extract (shared by the trait impl and the wrapper).
pub fn extract_parsed(p: &Parsed, env: &AdapterEnv, s: &SlurmSettings) -> Result<Extracted> {
    let cluster = s.cluster_name(env);
    let account = account_of(&p.job, env, s);
    let tenant = tenant(&cluster, &account).map_err(|e| PluginError::InvalidNative(e.to_string()))?;
    let skip = |reason| Ok(Extracted { tenant: tenant.clone(), units: vec![], skip: Some(reason) });
    if !p.job.problems.is_empty() {
        return skip(SkipReason::Unsupported { detail: p.job.problems.join("; ") });
    }
    if p.job.rightsizer.off {
        return skip(SkipReason::OptedOut);
    }
    if env.principal.as_ref().is_some_and(|u| s.presized_submitters.contains(u)) {
        return skip(SkipReason::AlreadySized { decision_id: None });
    }
    if let Some(r) = already_sized(p) {
        return skip(r);
    }
    let mut units = Vec::new();
    for k in 0..p.job.components.len() {
        match build_unit(p, k, s, &account, env) {
            Ok(u) => units.push(u),
            Err(e) => return skip(SkipReason::Unsupported { detail: e }),
        }
    }
    if units.is_empty() {
        return skip(SkipReason::NoUnits);
    }
    Ok(Extracted { tenant, units, skip: None })
}

/// One option change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Change {
    /// Component.
    pub comp: usize,
    /// Long option name.
    pub option: &'static str,
    /// New value.
    pub value: String,
    /// Where the current value is (`None`: the option is added).
    pub origin: Option<Origin>,
}

fn round_mib(b: u64) -> u64 {
    b.div_ceil(1 << 20) << 20
}

fn fmt_mem(bytes: u64, suffixed: bool) -> String {
    let b = round_mib(bytes);
    if suffixed { format_slurm_mem(Bytes(b)) } else { (b >> 20).to_string() }
}

fn rewrite_gpu_value(flag: GpuFlag, current: &str, ty: Option<&str>, count: u32) -> String {
    match flag {
        GpuFlag::Gres => {
            let mut out = Vec::new();
            let mut done = false;
            for e in current.split(',') {
                let parsed = parse_gres(e).ok().and_then(|v| v.into_iter().next());
                match parsed {
                    Some(g) if g.name == "gpu" && !done => {
                        done = true;
                        let prefix = if e.trim().starts_with("gres/") { "gres/" } else { "" };
                        out.push(match ty {
                            Some(t) => format!("{prefix}gpu:{t}:{count}"),
                            None => format!("{prefix}gpu:{count}"),
                        });
                    }
                    _ => out.push(e.trim().to_string()),
                }
            }
            out.join(",")
        }
        _ => match ty {
            Some(t) => format!("{t}:{count}"),
            None => count.to_string(),
        },
    }
}

/// The option changes that render `spec` onto component `k` (§3 rules).
pub fn component_changes(p: &Parsed, k: usize, spec: &ResourceSpec, s: &SlurmSettings) -> Vec<Change> {
    let c = &p.job.components[k];
    let r = CompResources::of(c);
    let mut out = Vec::new();
    if !r.errors.is_empty() {
        return out;
    }
    let editable = |name: &str| c.get(name).is_none_or(|o| o.origin.editable());
    let want = &spec.requests;
    let tpn = r.tasks_per_node();
    // CPU: --cpus-per-task; ntasks topology unchanged.
    let mut new_cpt = r.cpus_per_task.unwrap_or(1);
    if let Some(cpu) = want.cpu
        && editable("cpus-per-task")
        && !c.has("cpus-per-gpu")
    {
        let n = (cpu.0 as u64).div_ceil(1000 * tpn as u64).max(1) as u32;
        let cur = r.cpus_per_task;
        if cur != Some(n) && !(cur.is_none() && n == 1) {
            new_cpt = n;
            out.push(Change { comp: k, option: "cpus-per-task", value: n.to_string(), origin: c.get("cpus-per-task").map(|o| o.origin.clone()) });
        }
    }
    // GPU: keep the user's flag; count only for single-task single-node; type
    // only when typed, mapped through the site table.
    let mut new_gpn = r.gpus_per_node();
    if let (Some(g), Some(w)) = (&r.gpu, want.gpu)
        && !r.gpu_ambiguous
        && g.flag != GpuFlag::GpusPerSocket
        && editable(g.flag.option())
    {
        let cur_accel = accel_of(g.gpu_type.as_deref(), s, s.inventory.partition(r.partition.as_deref()));
        let new_type = match &g.gpu_type {
            Some(t) if w.accel != cur_accel && !w.accel.is_unknown() => s.accelerators().native(w.accel).map(str::to_string).or(Some(t.clone())),
            other => other.clone(),
        };
        let cur_pn = r.gpus_per_node().unwrap_or(g.count);
        let new_count = if r.distributed() || w.count == cur_pn {
            g.count
        } else {
            match g.flag {
                GpuFlag::Gpus => w.count * r.nodes(),
                GpuFlag::GpusPerTask => w.count.div_ceil(tpn).max(1),
                _ => w.count,
            }
        };
        if new_type != g.gpu_type || new_count != g.count {
            let cur = c.value(g.flag.option()).unwrap_or("");
            new_gpn = match g.flag {
                GpuFlag::Gpus => Some(new_count.div_ceil(r.nodes())),
                GpuFlag::GpusPerTask => Some(new_count * tpn),
                _ => Some(new_count),
            };
            out.push(Change {
                comp: k,
                option: g.flag.option(),
                value: rewrite_gpu_value(g.flag, cur, new_type.as_deref(), new_count),
                origin: c.get(g.flag.option()).map(|o| o.origin.clone()),
            });
        }
    }
    // Memory: keep the user's form; never add a second form; --mem=0 never changes.
    if let Some(mem) = want.memory {
        match &r.mem {
            Some(m) if m.form == MemForm::PerNode && m.value.0 == 0 => {}
            Some(m) if editable(m.form.option()) => {
                let divisor = match m.form {
                    MemForm::PerNode => Some(1u64),
                    MemForm::PerCpu => Some(r.cpus_per_node_with(new_cpt) as u64),
                    MemForm::PerGpu => new_gpn.map(u64::from).filter(|g| *g > 0),
                };
                if let Some(d) = divisor {
                    let per = mem.0.div_ceil(d);
                    if round_mib(per) != round_mib(m.value.0) {
                        out.push(Change {
                            comp: k,
                            option: m.form.option(),
                            value: fmt_mem(per, m.suffixed),
                            origin: c.get(m.form.option()).map(|o| o.origin.clone()),
                        });
                    }
                }
            }
            Some(_) => {}
            None => {
                // Defaulted: add --mem (per node) only when the value differs from
                // the default it replaces.
                let part = s.inventory.partition(r.partition.as_deref());
                let mut r2 = r.clone();
                r2.cpus_per_task = Some(new_cpt);
                let default = default_memory(&r2, part, &s.inventory);
                if default.is_none_or(|d| round_mib(d.0) != round_mib(mem.0)) {
                    out.push(Change { comp: k, option: "mem", value: fmt_mem(mem.0, true), origin: None });
                }
            }
        }
    }
    out
}

/// Text edits and argv/table options for a set of changes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RenderedEdits {
    /// Script edits.
    pub script: Vec<TextEdit>,
    /// Option values (argv or cli_filter table), keyed by [`option_key`].
    pub options: BTreeMap<String, String>,
}

/// The `Patch::Options` key of option `name` in component `k`.
pub fn option_key(k: usize, name: &str) -> String {
    if k == 0 { name.to_string() } else { format!("het{k}/{name}") }
}

/// Split an options key into `(component, name)`.
pub fn split_option_key(key: &str) -> Option<(usize, &str)> {
    match key.strip_prefix("het").and_then(|r| r.split_once('/')) {
        Some((k, n)) => Some((k.parse().ok()?, n)),
        None => Some((0, key)),
    }
}

fn insert_text(script: &str, at: usize, line: &str) -> String {
    if at == script.len() && !script.is_empty() && !script.ends_with('\n') { format!("\n{line}\n") } else { format!("{line}\n") }
}

/// Turn changes into edits.
pub fn render_changes(p: &Parsed, changes: &[Change]) -> RenderedEdits {
    let mut out = RenderedEdits::default();
    let script = p.job.script.as_deref().unwrap_or("");
    let mut inserts: BTreeMap<usize, String> = BTreeMap::new();
    for ch in changes {
        let comp = &p.job.components[ch.comp];
        match (&ch.origin, p.kind) {
            (_, NativeKind::CliFilter) => {
                out.options.insert(ch.option.to_string(), ch.value.clone());
            }
            (Some(Origin::Script { value: Some((a, b)), .. }), _) => out.script.push(TextEdit { start: *a, end: *b, replacement: ch.value.clone() }),
            (Some(Origin::Script { token, form, .. }), _) => {
                let short = lookup_long(ch.option).and_then(|s| s.short);
                let rep = match (form, short) {
                    (ArgForm::ShortAttached, Some(c)) => format!("-{c}{}", ch.value),
                    _ => format!("--{}={}", ch.option, ch.value),
                };
                out.script.push(TextEdit { start: token.0, end: token.1, replacement: rep });
            }
            (Some(Origin::Argv { .. }), _) | (Some(Origin::Table), _) => {
                out.options.insert(option_key(ch.comp, ch.option), ch.value.clone());
            }
            (Some(Origin::Env { .. }), _) => {}
            (None, _) => {
                let wrap = p.job.components.first().is_some_and(|c| c.has("wrap"));
                let in_script = !wrap && p.job.script.is_some() && (p.kind == NativeKind::Document || comp.has_directives);
                match comp.script_insert_at.filter(|_| in_script) {
                    Some(at) => inserts.entry(at).or_default().push_str(&insert_text(script, at, &format!("#SBATCH --{}={}", ch.option, ch.value))),
                    None => {
                        out.options.insert(option_key(ch.comp, ch.option), ch.value.clone());
                    }
                }
            }
        }
    }
    for (at, text) in inserts {
        out.script.push(TextEdit { start: at, end: at, replacement: text });
    }
    out.script.sort_by_key(|e| (e.start, e.end));
    out
}

impl RenderedEdits {
    /// As a [`Patch`] (module docs).
    pub fn into_patch(self) -> Patch {
        match (self.script.is_empty(), self.options.is_empty()) {
            (true, true) => Patch::None,
            (false, true) => Patch::TextEdits { edits: self.script },
            (_, false) => {
                let mut m: serde_json::Map<String, serde_json::Value> =
                    self.options.into_iter().map(|(k, v)| (k, serde_json::Value::String(v))).collect();
                if !self.script.is_empty() {
                    m.insert(SCRIPT_EDITS_KEY.into(), serde_json::to_value(&self.script).unwrap_or_default());
                }
                Patch::Options { options: m }
            }
        }
    }

    /// From a [`Patch`].
    pub fn from_patch(patch: &Patch) -> Result<Self> {
        let mut out = RenderedEdits::default();
        match patch {
            Patch::None => {}
            Patch::TextEdits { edits } => out.script = edits.clone(),
            Patch::Options { options } => {
                for (k, v) in options {
                    if k == SCRIPT_EDITS_KEY {
                        out.script = serde_json::from_value(v.clone()).map_err(|e| PluginError::InvalidPatch(e.to_string()))?;
                    } else {
                        let v = match v {
                            serde_json::Value::String(s) => s.clone(),
                            other => other.to_string(),
                        };
                        out.options.insert(k.clone(), v);
                    }
                }
            }
            Patch::JsonPatch { .. } => return Err(PluginError::InvalidPatch("slurm: JSON patches are not supported".into())),
        }
        Ok(out)
    }
}

/// Apply argv options in place, keeping each option's form; options that
/// are absent are inserted at the start of their component's segment.
pub fn apply_argv_options(job: &SbatchJob, options: &BTreeMap<String, String>) -> Result<Vec<String>> {
    let mut argv = job.argv.clone();
    let mut inserts: BTreeMap<usize, Vec<String>> = BTreeMap::new();
    for (key, value) in options {
        let (k, name) = split_option_key(key).ok_or_else(|| PluginError::InvalidPatch(format!("bad option key {key}")))?;
        let spec = lookup_long(name).ok_or_else(|| PluginError::InvalidPatch(format!("unknown option {name}")))?;
        let comp = job.components.get(k).ok_or_else(|| PluginError::InvalidPatch(format!("no component {k}")))?;
        let argv_opt = comp.opts.iter().rev().find(|o| o.name == spec.long && matches!(o.origin, Origin::Argv { .. }));
        match argv_opt.map(|o| &o.origin) {
            Some(Origin::Argv { token, offset, form }) => {
                let t = &argv[*token];
                argv[*token] = match form {
                    ArgForm::LongEq | ArgForm::ShortAttached => format!("{}{value}", &t[..*offset]),
                    ArgForm::LongSep | ArgForm::ShortSep => value.clone(),
                    ArgForm::Flag => format!("--{}={value}", spec.long),
                };
            }
            _ => {
                let at = comp.argv_segment.map(|s| s.0).unwrap_or(0);
                inserts.entry(at).or_default().push(format!("--{}={value}", spec.long));
            }
        }
    }
    for (at, toks) in inserts.into_iter().rev() {
        for (i, t) in toks.into_iter().enumerate() {
            argv.insert(at + i, t);
        }
    }
    Ok(argv)
}

/// Apply a rendered patch to a parsed submission: the new native.
pub fn apply_patch(native: &Native, patch: &Patch) -> Result<Native> {
    let p = parse_native(native)?;
    let edits = RenderedEdits::from_patch(patch)?;
    let script = match (&p.job.script, edits.script.is_empty()) {
        (Some(s), false) => Some(apply_text_edits(s, &edits.script)?),
        (s, _) => s.clone(),
    };
    Ok(match (native, p.kind) {
        (Native::Document { media_type, .. }, _) => Native::Document { media_type: media_type.clone(), bytes: script.unwrap_or_default().into_bytes() },
        (Native::Object { value }, NativeKind::Sbatch) => {
            let argv = apply_argv_options(&p.job, &edits.options)?;
            let wrap = p.job.components.first().is_some_and(|c| c.has("wrap"));
            let n = SlurmNative::Sbatch { argv, script: if wrap { None } else { script }, env: p.env.clone() };
            let _ = value;
            Native::Object { value: serde_json::to_value(n).map_err(|e| PluginError::Internal(e.to_string()))? }
        }
        (Native::Object { value }, _) => {
            let mut n: SlurmNative = serde_json::from_value(value.clone()).map_err(|e| PluginError::InvalidNative(e.to_string()))?;
            if let SlurmNative::CliFilter { options, .. } = &mut n {
                for (k, v) in &edits.options {
                    options.insert(k.clone(), serde_json::Value::String(v.clone()));
                }
            }
            Native::Object { value: serde_json::to_value(n).map_err(|e| PluginError::Internal(e.to_string()))? }
        }
        (Native::ApiCall { .. }, _) => return Err(PluginError::Unsupported("api call".into())),
    })
}

/// Render decisions (shared by the trait impl and the wrapper).
pub fn render_parsed(p: &Parsed, decisions: &[(UnitId, ResourceSpec)], s: &SlurmSettings) -> RenderedEdits {
    let mut changes = Vec::new();
    for (unit, spec) in decisions {
        if let Some(k) = p.component_of(unit) {
            changes.extend(component_changes(p, k, spec, s));
        }
    }
    render_changes(p, &changes)
}

fn reject(reason: &str, detail: String, field: Option<&str>) -> Rejection {
    Rejection { reason: reason.into(), detail: Some(detail), field: field.map(str::to_string) }
}

/// Validate a patched submission (partition limits, node shapes,
/// association/QOS limits, and the §3 invariants).
pub fn validate_parsed(before: &Parsed, after: &Parsed, env: &AdapterEnv, s: &SlurmSettings) -> std::result::Result<(), Rejection> {
    if !after.job.problems.is_empty() {
        return Err(reject("mixed_memory_forms", after.job.problems.join("; "), Some("mem")));
    }
    let account = account_of(&after.job, env, s);
    for (k, c) in after.job.components.iter().enumerate() {
        let r = CompResources::of(c);
        if !r.errors.is_empty() {
            return Err(reject("invalid_value", r.errors.join(", "), None));
        }
        if let Some(b) = before.job.components.get(k) {
            let rb = CompResources::of(b);
            if rb.mem.as_ref().is_some_and(|m| m.form == MemForm::PerNode && m.value.0 == 0) && r.mem != rb.mem {
                return Err(reject("mem_zero_changed", "--mem=0 must never change".into(), Some("mem")));
            }
            let forms_before = b.memory_forms();
            let forms_after = c.memory_forms();
            if !forms_before.is_empty() && forms_after != forms_before {
                return Err(reject("memory_form_changed", format!("{forms_before:?} -> {forms_after:?}"), Some("mem")));
            }
        }
        let inv = &s.inventory;
        let part = inv.partition(r.partition.as_deref());
        let cpn = r.cpus_per_node() as u64;
        let mem = r.mem_per_node().filter(|m| m.0 > 0).map(mib);
        if let (Some(p), Some(m)) = (part, mem) {
            if let Some(max) = p.max_mem_per_node
                && m > max
            {
                return Err(reject("exceeds_max_mem_per_node", format!("{m} MiB > MaxMemPerNode {max} MiB in {}", p.name), Some("mem")));
            }
            if let Some(max) = p.max_mem_per_cpu
                && m.div_ceil(cpn.max(1)) > max
            {
                return Err(reject("exceeds_max_mem_per_cpu", format!("{} MiB/CPU > MaxMemPerCPU {max} MiB in {}", m.div_ceil(cpn), p.name), Some("mem")));
            }
        }
        let gtype = r.gpu.as_ref().and_then(|g| g.gpu_type.clone());
        let gpn = r.gpus_per_node().unwrap_or(0);
        let candidates = inv.nodes_of(part);
        if !candidates.is_empty() {
            let fits = candidates
                .iter()
                .filter(|n| n.usable() && n.cpus as u64 >= cpn && mem.is_none_or(|m| n.usable_memory_mib() >= m) && n.gpus(gtype.as_deref()) >= gpn)
                .count();
            if fits < r.nodes() as usize {
                return Err(reject(
                    "exceeds_node_shape",
                    format!("{} node(s) with {cpn} CPUs, {} MiB, {gpn} GPU(s): {fits} fit", r.nodes(), mem.unwrap_or(0)),
                    None,
                ));
            }
        }
        let nodes = r.nodes() as u64;
        let (job_lim, node_lim, grp) = inv.limits(&account, env.principal.as_deref(), r.partition.as_deref(), r.qos.as_deref());
        let check = |lim: &crate::inventory::TresLimits, scale: u64, what: &str| -> std::result::Result<(), Rejection> {
            if let Some(l) = lim.cpu
                && cpn * scale > l
            {
                return Err(reject("quota", format!("cpu {} > {what} {l}", cpn * scale), Some("cpus-per-task")));
            }
            if let (Some(l), Some(m)) = (lim.mem_mib, mem)
                && m * scale > l
            {
                return Err(reject("quota", format!("mem {} MiB > {what} {l} MiB", m * scale), Some("mem")));
            }
            if let Some(l) = lim.gpus
                && gpn as u64 * scale > l
            {
                return Err(reject("quota", format!("gres/gpu {} > {what} {l}", gpn as u64 * scale), Some("gres")));
            }
            Ok(())
        };
        check(&job_lim, nodes, "MaxTRESPerJob")?;
        check(&grp, nodes, "GrpTRES")?;
        check(&node_lim, 1, "MaxTRESPerNode")?;
    }
    Ok(())
}

#[async_trait]
impl Adapter for SlurmAdapter {
    fn kind(&self) -> &str {
        "slurm"
    }

    async fn extract(&self, native: &Native, env: &AdapterEnv) -> Result<Extracted> {
        let p = parse_native(native)?;
        extract_parsed(&p, env, &self.settings(env))
    }

    async fn render(&self, native: &Native, decisions: &[(UnitId, ResourceSpec)]) -> Result<Patch> {
        let p = parse_native(native)?;
        Ok(render_parsed(&p, decisions, &self.defaults).into_patch())
    }

    async fn validate(&self, native: &Native, patch: &Patch, env: &AdapterEnv) -> std::result::Result<(), Rejection> {
        let before = parse_native(native).map_err(|e| reject("unparseable", e.to_string(), None))?;
        let patched = apply_patch(native, patch).map_err(|e| reject("invalid_patch", e.to_string(), None))?;
        let after = parse_native(&patched).map_err(|e| reject("unparseable", e.to_string(), None))?;
        validate_parsed(&before, &after, env, &self.settings(env))
    }
}
