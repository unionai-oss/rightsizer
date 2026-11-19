//! The `sacct` importer (slurm.md §5): `sacct --json` (Slurm ≥ 21.08), with
//! `--parsable2` text as a fallback, → [`RunRecord`]s.
//!
//! - **Per-node memory demand**: for each step, `TRESUsageInTot(mem) /
//!   NNodes` (`Exact`, sampled: `resolution_s` = `JobAcctGatherFrequency`),
//!   else `MaxRSS × tasks-per-node` (`AtLeast`). `.batch`, `.extern` and
//!   numbered steps are aggregated: the job's demand is the step with the
//!   largest value.
//! - **Status**: `COMPLETED` → `Succeeded`; `OUT_OF_MEMORY` (job or any step)
//!   → `OomKilled` with `memory_peak = AtLeast(max(limit, peak))`;
//!   `TIMEOUT`/`DEADLINE` → `Timeout`; `PREEMPTED` → `Preempted`;
//!   `NODE_FAIL`, `BOOT_FAIL`, `FAILED`, `CANCELLED` → `Failed` (never
//!   attributed to rightsizer). Jobs that never started or have not ended
//!   are skipped.
//! - **Censoring** (SPEC §5.7): a success within 5% of its limit is
//!   `AtLeast`; CPU at its allocation is `AtLeast`.
//! - **Key**: `AdminComment` `rightsizer:<decision_id>:verified|user` gives
//!   the decision id (resolved to a key by the decision log when a resolver
//!   is configured); otherwise the key is derived exactly as at submit time
//!   from the batch script (a second `sacct --json -B` call — `-B` drops the
//!   steps — needing `AccountingStoreFlags=job_script`) and the submit line; without the
//!   script, from the job name.
//! - **Trust**: `Verified` — the tenant comes from sacct's account, which
//!   Slurm authenticated (SPEC §8.2); `Unverified` when the job has no account.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use futures::stream::{self, BoxStream, StreamExt};
use serde_json::Value;
use ulid::Ulid;

use rightsizer_core::units::parse_slurm_mem;
use rightsizer_core::{
    AcceleratorId, Bytes, Censor, Confidence, GpuShape, Measurement, MilliCpu, OutcomeSource as SourceKind, Requests, ResourceSpec, RunRecord,
    RunStatus, Trust, WorkloadKey,
};
use rightsizer_plugin::{OutcomeSource, PluginError, Result};

use crate::keys::{key_suffix_het, sanitize_suffix, script_digest, tenant};
use crate::runner::{CommandRunner, TokioRunner};
use crate::sbatch::{SbatchJob, parse, parse_time};

/// One job step.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SacctStep {
    /// `27.batch`, `29.0`.
    pub id: String,
    /// State (first word).
    pub state: String,
    /// Nodes.
    pub nnodes: u32,
    /// Tasks.
    pub ntasks: u32,
    /// `TRESUsageInMax` mem (bytes): the largest task.
    pub max_mem: Option<u64>,
    /// `TRESUsageInTot` mem (bytes): sum over tasks.
    pub tot_mem: Option<u64>,
    /// `TRESUsageInMax` gres/gpumem (bytes).
    pub max_gpumem: Option<u64>,
    /// `TRESUsageInAve` gres/gpuutil (percent).
    pub ave_gpuutil: Option<f64>,
}

/// One job (an array task or a het component is its own job).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SacctJob {
    /// Raw job id (unique per array task / het component).
    pub job_id: u64,
    /// Het offset.
    pub het_offset: Option<u32>,
    /// Array task.
    pub array_task: Option<u64>,
    /// Cluster.
    pub cluster: String,
    /// Account.
    pub account: String,
    /// User.
    pub user: String,
    /// Job name.
    pub name: String,
    /// Partition.
    pub partition: String,
    /// State (first word: `CANCELLED by 0` → `CANCELLED`).
    pub state: String,
    /// Exit code.
    pub exit_code: Option<i64>,
    /// Start, unix seconds (`None` if it never started).
    pub start: Option<i64>,
    /// Whether the job has ended.
    pub ended: bool,
    /// Elapsed, seconds.
    pub elapsed_s: u64,
    /// Time limit, seconds.
    pub timelimit_s: Option<u64>,
    /// Allocated nodes.
    pub nnodes: u32,
    /// Allocated (else requested) CPUs, total.
    pub cpus: u32,
    /// Requested memory per node, MiB.
    pub mem_per_node_mib: Option<u64>,
    /// Requested memory per CPU, MiB.
    pub mem_per_cpu_mib: Option<u64>,
    /// Allocated GPUs (total) and type.
    pub gpus: Option<(Option<String>, u32)>,
    /// Total CPU time (user + system), seconds.
    pub cpu_time_s: Option<f64>,
    /// `AdminComment`.
    pub admin_comment: String,
    /// `SubmitLine`.
    pub submit_line: String,
    /// Batch script (`sacct -B`).
    pub script: Option<String>,
    /// Requeues.
    pub restart_cnt: u32,
    /// Steps.
    pub steps: Vec<SacctStep>,
}

fn first_word(s: &str) -> String {
    s.split_whitespace().next().unwrap_or("").to_ascii_uppercase()
}

fn jnum(v: Option<&Value>) -> Option<u64> {
    match v? {
        Value::Number(n) => n.as_u64().or_else(|| n.as_f64().map(|f| f.max(0.0) as u64)),
        Value::Object(o) => {
            if o.get("infinite").and_then(Value::as_bool) == Some(true) || o.get("set").and_then(Value::as_bool) == Some(false) {
                None
            } else {
                jnum(o.get("number"))
            }
        }
        _ => None,
    }
}

fn jstate(v: Option<&Value>) -> String {
    match v {
        Some(Value::Array(a)) => a.first().and_then(Value::as_str).map(first_word).unwrap_or_default(),
        Some(Value::String(s)) => first_word(s),
        Some(Value::Object(o)) => jstate(o.get("current")),
        _ => String::new(),
    }
}

fn jstr(v: Option<&Value>) -> String {
    v.and_then(Value::as_str).unwrap_or("").to_string()
}

fn tres_count(list: Option<&Value>, ty: &str, name: &str) -> Option<u64> {
    list?.as_array()?.iter().find(|t| t.get("type").and_then(Value::as_str) == Some(ty) && t.get("name").and_then(Value::as_str).unwrap_or("") == name).and_then(|t| jnum(t.get("count")))
}

fn gres_gpus(list: Option<&Value>) -> Option<(Option<String>, u32)> {
    let a = list?.as_array()?;
    let mut total = None;
    let mut typed = None;
    for t in a {
        if t.get("type").and_then(Value::as_str) != Some("gres") {
            continue;
        }
        let name = t.get("name").and_then(Value::as_str).unwrap_or("");
        let count = jnum(t.get("count")).unwrap_or(0) as u32;
        if name == "gpu" {
            total = Some(count);
        } else if let Some(ty) = name.strip_prefix("gpu:") {
            typed = Some((ty.to_string(), count));
        }
    }
    match (total, typed) {
        (_, Some((t, c))) => Some((Some(t), c)),
        (Some(c), None) if c > 0 => Some((None, c)),
        _ => None,
    }
}

fn secs(v: Option<&Value>) -> Option<f64> {
    let o = v?.as_object()?;
    Some(jnum(o.get("seconds")).unwrap_or(0) as f64 + jnum(o.get("microseconds")).unwrap_or(0) as f64 / 1e6)
}

/// Parse `sacct --json` output.
pub fn parse_sacct_json(json: &str) -> Result<Vec<SacctJob>> {
    let v: Value = serde_json::from_str(json).map_err(|e| PluginError::Parse(format!("sacct --json: {e}")))?;
    let jobs = v.get("jobs").and_then(Value::as_array).ok_or_else(|| PluginError::Parse("sacct --json: no jobs array".into()))?;
    let mut out = Vec::new();
    for j in jobs {
        let Some(job_id) = jnum(j.get("job_id")) else { continue };
        let time = j.get("time");
        let start = time.and_then(|t| jnum(t.get("start"))).filter(|s| *s > 0 && *s < 4_294_967_294).map(|s| s as i64);
        let end = time.and_then(|t| jnum(t.get("end"))).unwrap_or(0);
        let nnodes = jnum(j.get("allocation_nodes")).unwrap_or(0).max(1) as u32;
        let alloc = j.pointer("/tres/allocated");
        let req = j.pointer("/tres/requested");
        let cpus = tres_count(alloc, "cpu", "").or_else(|| tres_count(req, "cpu", "")).or_else(|| jnum(j.pointer("/required/CPUs"))).unwrap_or(0) as u32;
        let mut steps = Vec::new();
        for s in j.get("steps").and_then(Value::as_array).into_iter().flatten() {
            let usage = s.pointer("/tres/requested");
            steps.push(SacctStep {
                id: jstr(s.pointer("/step/id")),
                state: jstate(s.get("state")),
                nnodes: jnum(s.pointer("/nodes/count")).unwrap_or(1) as u32,
                ntasks: jnum(s.pointer("/tasks/count")).unwrap_or(1) as u32,
                max_mem: tres_count(usage.and_then(|u| u.get("max")), "mem", ""),
                tot_mem: tres_count(usage.and_then(|u| u.get("total")), "mem", ""),
                max_gpumem: tres_count(usage.and_then(|u| u.get("max")), "gres", "gpumem"),
                ave_gpuutil: tres_count(usage.and_then(|u| u.get("average")), "gres", "gpuutil").map(|x| x as f64),
            });
        }
        let het_set = j.pointer("/het/job_offset/set").and_then(Value::as_bool) == Some(true) || jnum(j.pointer("/het/job_id")).is_some_and(|h| h > 0);
        let arr_set = j.pointer("/array/task_id/set").and_then(Value::as_bool) == Some(true);
        out.push(SacctJob {
            job_id,
            het_offset: if het_set { jnum(j.pointer("/het/job_offset")).map(|o| o as u32) } else { None },
            array_task: if arr_set { jnum(j.pointer("/array/task_id")) } else { None },
            cluster: jstr(j.get("cluster")),
            account: jstr(j.get("account")),
            user: jstr(j.get("user")),
            name: jstr(j.get("name")),
            partition: jstr(j.get("partition")),
            state: jstate(j.get("state")),
            exit_code: jnum(j.pointer("/exit_code/return_code")).map(|c| c as i64),
            start,
            ended: end > 0,
            elapsed_s: time.and_then(|t| jnum(t.get("elapsed"))).unwrap_or(0),
            timelimit_s: time.and_then(|t| jnum(t.get("limit"))).map(|m| m * 60),
            nnodes,
            cpus,
            mem_per_node_mib: jnum(j.pointer("/required/memory_per_node")),
            mem_per_cpu_mib: jnum(j.pointer("/required/memory_per_cpu")),
            gpus: gres_gpus(alloc).or_else(|| gres_gpus(req)),
            cpu_time_s: secs(time.and_then(|t| t.get("total"))),
            admin_comment: j.pointer("/comment/administrator").and_then(Value::as_str).or_else(|| j.get("admin_comment").and_then(Value::as_str)).unwrap_or("").to_string(),
            submit_line: jstr(j.get("submit_line")),
            script: j.get("script").and_then(Value::as_str).filter(|s| !s.is_empty()).map(str::to_string),
            restart_cnt: jnum(j.get("restart_cnt")).unwrap_or(0) as u32,
            steps,
        });
    }
    Ok(out)
}

/// Copy batch scripts from a `sacct --json -B` result into jobs from a plain
/// `sacct --json` call. On 26.05 `-B` returns the scripts but **no steps**,
/// so the importer needs both calls.
pub fn merge_scripts(jobs: &mut [SacctJob], with_scripts: &[SacctJob]) {
    for j in jobs.iter_mut() {
        if j.script.is_none()
            && let Some(w) = with_scripts.iter().find(|w| w.job_id == j.job_id)
        {
            j.script = w.script.clone();
        }
    }
}

/// The `--format` the text fallback asks for.
pub const PARSABLE_FORMAT: &str = "JobID,JobIDRaw,JobName,Account,User,Cluster,Partition,State,ExitCode,Start,End,Elapsed,Timelimit,NNodes,NTasks,AllocCPUS,ReqTRES,AllocTRES,ReqMem,MaxRSS,TRESUsageInMax,TRESUsageInTot,TRESUsageInAve,TotalCPU,AdminComment,Restarts,SubmitLine";

fn tres_map(s: &str) -> BTreeMap<String, String> {
    s.split(',').filter_map(|e| e.split_once('=')).map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

fn text_bytes(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    parse_slurm_mem(s).ok().map(|b| b.0).or_else(|| s.parse::<u64>().ok())
}

/// sacct's `[DD-]HH:MM:SS[.mmm]` or `MM:SS.mmm` durations.
fn duration_s(s: &str) -> Option<f64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let (whole, frac) = match s.rsplit_once('.') {
        Some((w, f)) if f.chars().all(|c| c.is_ascii_digit()) => (w, format!("0.{f}").parse::<f64>().unwrap_or(0.0)),
        _ => (s, 0.0),
    };
    let parts: Vec<&str> = whole.split([':', '-']).collect();
    let n: Vec<f64> = parts.iter().map(|p| p.parse::<f64>()).collect::<std::result::Result<_, _>>().ok()?;
    let v = match (whole.contains('-'), n.as_slice()) {
        (true, [d, h, m, s]) => d * 86400.0 + h * 3600.0 + m * 60.0 + s,
        (true, [d, h, m]) => d * 86400.0 + h * 3600.0 + m * 60.0,
        (false, [h, m, s]) => h * 3600.0 + m * 60.0 + s,
        (false, [m, s]) => m * 60.0 + s,
        _ => return None,
    };
    Some(v + frac)
}

fn text_time(s: &str) -> Option<i64> {
    chrono::NaiveDateTime::parse_from_str(s.trim(), "%Y-%m-%dT%H:%M:%S").ok().map(|t| t.and_utc().timestamp())
}

/// Parse `sacct --parsable2` output (header line required).
pub fn parse_sacct_parsable2(text: &str) -> Result<Vec<SacctJob>> {
    let mut lines = text.lines().filter(|l| !l.trim().is_empty());
    let header = lines.next().ok_or_else(|| PluginError::Parse("sacct --parsable2: empty".into()))?;
    let cols: Vec<&str> = header.split('|').collect();
    let col = |name: &str| cols.iter().position(|c| *c == name);
    let need = |name: &str| col(name).ok_or_else(|| PluginError::Parse(format!("sacct --parsable2: missing {name}")));
    let (i_id, i_state) = (need("JobID")?, need("State")?);
    let mut jobs: Vec<SacctJob> = Vec::new();
    let mut by_id: BTreeMap<String, usize> = BTreeMap::new();
    for line in lines {
        let mut f: Vec<&str> = line.split('|').collect();
        if f.len() > cols.len() && cols.len() > 1 {
            // The last column (SubmitLine) may itself contain `|`.
            let start = line.match_indices('|').nth(cols.len() - 2).map(|(i, _)| i + 1).unwrap_or(line.len());
            f.truncate(cols.len() - 1);
            f.push(&line[start..]);
        }
        let get = |name: &str| col(name).and_then(|i| f.get(i)).map(|s| s.trim()).unwrap_or("");
        let id = f.get(i_id).copied().unwrap_or("");
        if let Some((parent, _)) = id.split_once('.') {
            let Some(&ji) = by_id.get(parent) else { continue };
            let tot = tres_map(get("TRESUsageInTot"));
            let max = tres_map(get("TRESUsageInMax"));
            let ave = tres_map(get("TRESUsageInAve"));
            jobs[ji].steps.push(SacctStep {
                id: id.to_string(),
                state: first_word(f.get(i_state).copied().unwrap_or("")),
                nnodes: get("NNodes").parse().unwrap_or(1),
                ntasks: get("NTasks").parse().unwrap_or(1),
                max_mem: max.get("mem").and_then(|v| text_bytes(v)).or_else(|| text_bytes(get("MaxRSS"))),
                tot_mem: tot.get("mem").and_then(|v| text_bytes(v)),
                max_gpumem: max.get("gres/gpumem").and_then(|v| text_bytes(v)),
                ave_gpuutil: ave.get("gres/gpuutil").and_then(|v| v.parse().ok()),
            });
            continue;
        }
        let (het_offset, array_task) = match (id.split_once('+'), id.split_once('_')) {
            (Some((_, o)), _) => (o.parse().ok(), None),
            (_, Some((_, t))) => (None, t.parse().ok()),
            _ => (None, None),
        };
        let raw = get("JobIDRaw");
        let job_id = raw.parse::<u64>().or_else(|_| id.split(['+', '_']).next().unwrap_or("").parse::<u64>()).unwrap_or(0);
        let alloc = tres_map(get("AllocTRES"));
        let req = tres_map(get("ReqTRES"));
        let nnodes = get("NNodes").parse::<u32>().unwrap_or(1).max(1);
        let cpus = get("AllocCPUS").parse::<u32>().ok().filter(|c| *c > 0).or_else(|| alloc.get("cpu").and_then(|c| c.parse().ok())).or_else(|| req.get("cpu").and_then(|c| c.parse().ok())).unwrap_or(0);
        let reqmem = get("ReqMem");
        let (mut mpn, mut mpc) = (None, None);
        if let Some(v) = reqmem.strip_suffix('n') {
            mpn = parse_slurm_mem(v).ok().map(|b| b.0.div_ceil(1 << 20));
        } else if let Some(v) = reqmem.strip_suffix('c') {
            mpc = parse_slurm_mem(v).ok().map(|b| b.0.div_ceil(1 << 20));
        } else if let Ok(b) = parse_slurm_mem(reqmem) {
            mpn = Some(b.0.div_ceil(1 << 20).div_ceil(nnodes as u64));
        }
        let gpus = alloc.iter().chain(req.iter()).find_map(|(k, v)| {
            let c = v.parse::<u32>().ok()?;
            if let Some(t) = k.strip_prefix("gres/gpu:") {
                Some((Some(t.to_string()), c))
            } else {
                (k == "gres/gpu").then_some((None, c))
            }
        });
        let start = text_time(get("Start"));
        let end = text_time(get("End"));
        let tl = get("Timelimit");
        by_id.insert(id.to_string(), jobs.len());
        jobs.push(SacctJob {
            job_id,
            het_offset,
            array_task,
            cluster: get("Cluster").to_string(),
            account: get("Account").to_string(),
            user: get("User").to_string(),
            name: get("JobName").to_string(),
            partition: get("Partition").to_string(),
            state: first_word(f.get(i_state).copied().unwrap_or("")),
            exit_code: get("ExitCode").split(':').next().and_then(|c| c.parse().ok()),
            start,
            ended: end.is_some() || col("End").is_none(),
            elapsed_s: duration_s(get("Elapsed")).unwrap_or(0.0) as u64,
            timelimit_s: parse_time(tl).ok().flatten(),
            nnodes,
            cpus,
            mem_per_node_mib: mpn,
            mem_per_cpu_mib: mpc,
            gpus,
            cpu_time_s: duration_s(get("TotalCPU")),
            admin_comment: get("AdminComment").to_string(),
            submit_line: get("SubmitLine").to_string(),
            script: None,
            restart_cnt: get("Restarts").parse().unwrap_or(0),
            steps: Vec::new(),
        });
    }
    Ok(jobs)
}

/// The decision id in an `AdminComment` written by job_submit.lua, and
/// whether the resources were verified.
pub fn parse_admin_comment(c: &str) -> Option<(Option<Ulid>, bool)> {
    for part in c.split([';', ' ']) {
        if let Some(rest) = part.strip_prefix("rightsizer:") {
            let (id, verdict) = rest.rsplit_once(':')?;
            return Some((Ulid::from_string(id).ok(), verdict == "verified"));
        }
    }
    None
}

const WRAP_HEADER: &str = "#!/bin/sh\n# This script was created by sbatch --wrap.\n\n";

/// Rebuild the submission sacct saw (script + submit line) to derive the
/// key exactly as at submit time.
pub fn reconstruct(job: &SacctJob) -> Option<SbatchJob> {
    let script = job.script.as_deref()?;
    let mut argv = shlex::split(&job.submit_line).unwrap_or_else(|| job.submit_line.split_whitespace().map(str::to_string).collect());
    if argv.first().is_some_and(|a| a.ends_with("sbatch")) {
        argv.remove(0);
    }
    if let Some(cmd) = script.strip_prefix(WRAP_HEADER) {
        // The submit line loses --wrap's quoting: rebuild --wrap from the script.
        let cmd = cmd.strip_suffix('\n').unwrap_or(cmd).to_string();
        let p = crate::options::parse_tokens(&argv, 0, argv.len());
        let mut a: Vec<String> = Vec::new();
        for o in p.opts.iter().filter(|o| o.name != "wrap") {
            match &o.value {
                Some(v) => a.push(format!("--{}={v}", o.name)),
                None => a.push(format!("--{}", o.name)),
            }
        }
        a.push(format!("--wrap={cmd}"));
        return parse(&a, None, &BTreeMap::new()).ok();
    }
    // Keep only the options before the script (the script path itself is
    // not needed: the text is known).
    let p = crate::options::parse_tokens(&argv, 0, argv.len());
    let end = p.first_positional.unwrap_or(argv.len());
    let mut a = argv[..end].to_vec();
    a.push("script".into());
    parse(&a, Some(script), &BTreeMap::new()).ok().or_else(|| parse(&["script".into()], Some(script), &BTreeMap::new()).ok())
}

/// Resolves a decision id to its workload key (the decision log).
pub type DecisionResolver = Arc<dyn Fn(&Ulid) -> Option<WorkloadKey> + Send + Sync>;

/// Import configuration.
#[derive(Clone)]
pub struct ImportConfig {
    /// Cluster name when sacct's is empty.
    pub cluster: String,
    /// `JobAcctGatherFrequency`, seconds (sampling resolution).
    pub resolution_s: f32,
    /// Decision id → key.
    pub resolver: Option<DecisionResolver>,
    /// Canonical ⇄ gres type names.
    pub accelerators: rightsizer_plugin::AcceleratorMap,
}

impl Default for ImportConfig {
    fn default() -> Self {
        ImportConfig { cluster: "default".into(), resolution_s: 30.0, resolver: None, accelerators: Default::default() }
    }
}

impl std::fmt::Debug for ImportConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImportConfig").field("cluster", &self.cluster).field("resolution_s", &self.resolution_s).finish_non_exhaustive()
    }
}

fn status_of(j: &SacctJob) -> Option<RunStatus> {
    if j.state == "OUT_OF_MEMORY" || j.steps.iter().any(|s| s.state == "OUT_OF_MEMORY") {
        return Some(RunStatus::OomKilled);
    }
    Some(match j.state.as_str() {
        "COMPLETED" => RunStatus::Succeeded,
        "TIMEOUT" | "DEADLINE" => RunStatus::Timeout,
        "PREEMPTED" => RunStatus::Preempted,
        "NODE_FAIL" | "BOOT_FAIL" | "FAILED" | "CANCELLED" => RunStatus::Failed,
        _ => return None, // PENDING, RUNNING, REQUEUED, SUSPENDED, RESIZING, REVOKED: not an outcome
    })
}

/// Per-node memory demand: `(bytes, censor)`.
pub fn memory_demand(j: &SacctJob) -> Option<(f64, Censor)> {
    let mut best: Option<(f64, Censor)> = None;
    for s in &j.steps {
        let v = match (s.tot_mem, s.max_mem) {
            (Some(t), _) if t > 0 => Some((t as f64 / s.nnodes.max(1) as f64, Censor::Exact)),
            (_, Some(m)) if m > 0 => Some((m as f64 * s.ntasks.div_ceil(s.nnodes.max(1)).max(1) as f64, Censor::AtLeast)),
            _ => None,
        };
        if let Some(v) = v
            && best.is_none_or(|b| v.0 > b.0)
        {
            best = Some(v);
        }
    }
    best
}

/// Convert one job to a run record (`None` for jobs that are not outcomes).
pub fn to_run_record(j: &SacctJob, cfg: &ImportConfig) -> Option<RunRecord> {
    let status = status_of(j)?;
    let start = j.start?;
    if !j.ended && j.elapsed_s == 0 {
        return None;
    }
    let cluster = if j.cluster.is_empty() { cfg.cluster.clone() } else { j.cluster.clone() };
    let tenant = tenant(&cluster, &j.account).ok()?;
    let ac = parse_admin_comment(&j.admin_comment);
    let decision_id = ac.and_then(|a| a.0);
    let reconstructed = reconstruct(j);
    let resolved = decision_id.and_then(|id| cfg.resolver.as_ref().and_then(|r| r(&id)));
    let key = match resolved {
        Some(k) => k,
        None => {
            let suffix = match &reconstructed {
                Some(sj) => key_suffix_het(sj, j.het_offset.unwrap_or(0) as usize, j.het_offset.is_some()).0,
                None => {
                    let base = sanitize_suffix(&j.name);
                    match j.het_offset {
                        Some(o) => format!("{base}/het-{o}"),
                        None => base,
                    }
                }
            };
            WorkloadKey::new("slurm", tenant, suffix).ok()?
        }
    };
    let nodes = j.nnodes.max(1);
    let mem_limit = j.mem_per_node_mib.map(|m| m << 20).or_else(|| j.mem_per_cpu_mib.map(|m| (m << 20) * (j.cpus.div_ceil(nodes)) as u64)).filter(|m| *m > 0);
    let cpus_per_node = j.cpus.div_ceil(nodes);
    let gpu = j.gpus.as_ref().map(|(t, c)| GpuShape {
        accel: t.as_deref().map(|t| cfg.accelerators.canonical(&t.to_ascii_lowercase())).unwrap_or(AcceleratorId::UnknownGpu),
        count: c.div_ceil(nodes),
    });
    let requested = ResourceSpec {
        requests: Requests {
            cpu: (cpus_per_node > 0).then(|| MilliCpu::cores(cpus_per_node)),
            memory: mem_limit.map(Bytes),
            gpu,
            ..Default::default()
        },
        limits: Default::default(),
    };
    let res = Some(cfg.resolution_s);
    let mut memory_peak = memory_demand(j).map(|(v, c)| Measurement { value: v, censor: c, resolution_s: res });
    match status {
        RunStatus::OomKilled => {
            let floor = mem_limit.map(|m| m as f64).unwrap_or(0.0).max(memory_peak.map(|m| m.value).unwrap_or(0.0));
            memory_peak = (floor > 0.0).then_some(Measurement { value: floor, censor: Censor::AtLeast, resolution_s: res });
        }
        RunStatus::Succeeded => {
            if let (Some(m), Some(l)) = (memory_peak.as_mut(), mem_limit)
                && m.value >= 0.95 * l as f64
            {
                m.censor = Censor::AtLeast;
            }
        }
        _ => {}
    }
    let elapsed = j.elapsed_s as f64;
    let cpu_mean = j.cpu_time_s.filter(|_| elapsed > 0.0).map(|t| {
        let cores = t / elapsed / nodes as f64;
        let at_limit = cpus_per_node > 0 && cores >= 0.95 * cpus_per_node as f64;
        Measurement { value: cores, censor: if at_limit { Censor::AtLeast } else { Censor::Exact }, resolution_s: None }
    });
    let gpu_memory_peak = j.steps.iter().filter_map(|s| s.max_gpumem).max().map(|v| Measurement { value: v as f64, censor: Censor::Exact, resolution_s: res });
    let gpu_util_mean = j.steps.iter().filter_map(|s| s.ave_gpuutil).fold(None, |a: Option<f64>, b| Some(a.map_or(b, |a| a.max(b)))).map(|u| (u / 100.0).clamp(0.0, 1.0) as f32);
    let started_at: DateTime<Utc> = Utc.timestamp_opt(start, 0).single()?;
    Some(RunRecord {
        v: 1,
        key,
        cluster: Some(cluster.clone()),
        invocation_id: format!("{cluster}:{}", j.job_id),
        attempt: j.restart_cnt + 1,
        decision_id,
        started_at,
        inputs: vec![],
        code_digest: reconstructed.as_ref().map(|sj| format!("sha256:{}", script_digest(sj))),
        requested: Some(requested),
        origin_prior: None,
        status,
        status_confidence: Confidence::Observed,
        memory_peak,
        cpu_mean,
        cpu_peak_window: None,
        cpu_throttled_frac: None,
        gpu_memory_peak,
        gpu_util_mean,
        t_peak_frac: None,
        duration_s: Some(elapsed),
        source: SourceKind::Sacct,
        trust: if j.account.is_empty() { Trust::Unverified } else { Trust::Verified },
    })
}

/// Convert many jobs.
pub fn to_run_records(jobs: &[SacctJob], cfg: &ImportConfig) -> Vec<RunRecord> {
    jobs.iter().filter_map(|j| to_run_record(j, cfg)).collect()
}

/// `sacct` as an [`OutcomeSource`].
#[derive(Clone)]
pub struct SacctSource {
    /// Import settings.
    pub config: ImportConfig,
    /// `sacct` binary.
    pub sacct: PathBuf,
    /// Pass `-B` (batch scripts; needs `AccountingStoreFlags=job_script`).
    pub batch_script: bool,
    /// Pass `-M <cluster>`.
    pub cluster_flag: Option<String>,
    /// Runs commands.
    pub runner: Arc<dyn CommandRunner>,
}

impl SacctSource {
    /// A source using `sacct` on `PATH`.
    pub fn new(config: ImportConfig) -> Self {
        SacctSource { config, sacct: PathBuf::from("sacct"), batch_script: true, cluster_flag: None, runner: Arc::new(TokioRunner::default()) }
    }

    /// Fetch and parse jobs since `since` (JSON first, text on failure).
    pub async fn jobs(&self, since: DateTime<Utc>) -> Result<Vec<SacctJob>> {
        let s = since.format("%Y-%m-%dT%H:%M:%S").to_string();
        let mut common = vec!["--allusers".to_string(), "-S".into(), s, "-E".into(), "now".into()];
        if let Some(c) = &self.cluster_flag {
            common.extend(["-M".into(), c.clone()]);
        }
        let mut args = vec!["--json".to_string()];
        args.extend(common.iter().cloned());
        match self.runner.run(&self.sacct, &args).await {
            Ok(o) if o.success => match parse_sacct_json(&o.stdout) {
                Ok(mut j) => {
                    if self.batch_script {
                        let mut b = vec!["--json".to_string(), "-B".into()];
                        b.extend(common.iter().cloned());
                        match self.runner.run(&self.sacct, &b).await {
                            Ok(o) if o.success => match parse_sacct_json(&o.stdout) {
                                Ok(w) => merge_scripts(&mut j, &w),
                                Err(e) => tracing::warn!("sacct -B unusable: {e}"),
                            },
                            _ => tracing::warn!("sacct -B failed; keys fall back to job names"),
                        }
                    }
                    return Ok(j);
                }
                Err(e) => tracing::warn!("sacct --json unusable, falling back to --parsable2: {e}"),
            },
            Ok(o) => tracing::warn!("sacct --json failed ({}), falling back to --parsable2", o.stderr.trim()),
            Err(e) => return Err(PluginError::PlatformUnavailable(e.to_string())),
        }
        let mut args = vec!["--parsable2".to_string(), format!("--format={PARSABLE_FORMAT}")];
        args.extend(common);
        let o = self.runner.run(&self.sacct, &args).await.map_err(|e| PluginError::PlatformUnavailable(e.to_string()))?;
        if !o.success {
            return Err(PluginError::PlatformUnavailable(format!("sacct: {}", o.stderr.trim())));
        }
        parse_sacct_parsable2(&o.stdout)
    }
}

#[async_trait]
impl OutcomeSource for SacctSource {
    async fn fetch(&self, since: DateTime<Utc>, keys: Option<&[WorkloadKey]>) -> Result<BoxStream<'_, Result<RunRecord>>> {
        let jobs = self.jobs(since).await?;
        let mut recs = to_run_records(&jobs, &self.config);
        if let Some(k) = keys {
            recs.retain(|r| k.contains(&r.key));
        }
        Ok(stream::iter(recs.into_iter().map(Ok)).boxed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admin_comments() {
        assert_eq!(parse_admin_comment("rightsizer:01JDZ7Q4W8M7C9B5V3N2X1Y0ZA:verified").unwrap(), (Some(Ulid::from_string("01JDZ7Q4W8M7C9B5V3N2X1Y0ZA").unwrap()), true));
        assert_eq!(parse_admin_comment("rightsizer::user").unwrap(), (None, false));
        assert!(parse_admin_comment("something else").is_none());
    }

    #[test]
    fn durations() {
        assert_eq!(duration_s("00:00:13"), Some(13.0));
        assert_eq!(duration_s("1-02:03:04"), Some(93784.0));
        assert_eq!(duration_s("00:01.500"), Some(1.5));
        assert_eq!(duration_s(""), None);
    }
}
