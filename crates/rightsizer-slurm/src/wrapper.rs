//! `rsz slurm sbatch [--watch] ARGS…` as a library function (slurm.md §1).
//!
//! Parse → `decide` (the engine or the daemon, behind [`Decider`]) →
//! validate → edit a temporary copy of the script (same file name, so the
//! default job name is unchanged) and argv → run `sbatch`. In `recommend`
//! mode it prints the diff and the explanation and submits unchanged. Any
//! rightsizer error submits the user's job unchanged (S1).
//!
//! `--watch` makes the wrapper the job's consumer and so a `Caller` ladder
//! (SPEC §7.2): it polls `squeue`/`sacct` until the job ends and, on
//! `OUT_OF_MEMORY` of an attempt rightsizer changed, resubmits one step up
//! (memory × `memory_factor`; the last step at least the origin prior), up
//! to `max_steps`. Job arrays resubmit only the tasks that ran out of
//! memory. Before every reduced attempt it opens a ladder lease
//! ([`Decider::open_lease`]); if that fails the attempt runs at the prior.
//!
//! Exit codes: `sbatch`'s on success (0), [`EXIT_SBATCH_FAILED`] (4) when
//! `sbatch` itself fails (its own code is reported in the outcome and on
//! stderr), and with `--watch` 0 for a final `COMPLETED`, 1 otherwise.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use ulid::Ulid;

use rightsizer_core::{AttemptRecord, Bytes, Decision, Lease, Mode, Requests, ResourceSpec, RunStatus, WorkloadKey};
use rightsizer_plugin::protocol::{DecideParams, DecideResult};
use rightsizer_plugin::{AdapterEnv, Native, Patch, PluginError, Result};

use crate::adapter::{Parsed, SlurmNative, apply_argv_options, apply_patch, component_changes, extract_parsed, option_key, parse_native, render_changes, validate_parsed};
use crate::attest::split_tokens;
use crate::runner::CommandRunner;
use crate::sbatch::parse;
use crate::settings::SlurmSettings;
use crate::token::TOKEN_ENV;

/// Exit code when `sbatch` fails.
pub const EXIT_SBATCH_FAILED: i32 = 4;

/// The engine or the daemon, as the wrapper sees it.
#[async_trait]
pub trait Decider: Send + Sync {
    /// The `decide` composite (plugins.md §3.1).
    async fn decide(&self, params: DecideParams) -> Result<DecideResult>;
    /// Write the attempt record / ladder lease before a reduced attempt.
    async fn open_lease(&self, _record: &AttemptRecord) -> Result<()> {
        Ok(())
    }
    /// Close the lease of an attempt once its outcome is handled.
    async fn close_lease(&self, _key: &WorkloadKey, _invocation_id: &str, _status: RunStatus) -> Result<()> {
        Ok(())
    }
    /// A token for a ladder resubmission (the patched native), if the
    /// decider can sign.
    async fn sign(&self, _native: &Native, _decision_ids: &[Ulid]) -> Result<Option<String>> {
        Ok(None)
    }
}

/// Wrapper configuration.
#[derive(Clone, Debug)]
pub struct WrapperConfig {
    /// `sbatch`.
    pub sbatch: PathBuf,
    /// `sacct`.
    pub sacct: PathBuf,
    /// `squeue`.
    pub squeue: PathBuf,
    /// `--watch`.
    pub watch: bool,
    /// Poll interval.
    pub poll_interval: Duration,
    /// How long to wait for accounting after the job left the queue.
    pub accounting_wait: Duration,
    /// Give up watching after this long (`None`: never).
    pub max_wait: Option<Duration>,
    /// Ladder memory factor.
    pub memory_factor: f32,
    /// Ladder steps.
    pub max_steps: u32,
    /// Adapter settings.
    pub settings: SlurmSettings,
    /// Caller identity (cluster, user, account).
    pub env: AdapterEnv,
    /// The submitter's environment (`SBATCH_*`, `RIGHTSIZER_*`).
    pub process_env: BTreeMap<String, String>,
    /// Working directory (relative script paths).
    pub cwd: PathBuf,
    /// Where temporary script copies go (default: the system temp dir).
    pub tmp_dir: Option<PathBuf>,
    /// Requested mode.
    pub mode: Option<Mode>,
    /// Decide deadline, ms.
    pub deadline_ms: u32,
}

impl Default for WrapperConfig {
    fn default() -> Self {
        WrapperConfig {
            sbatch: "sbatch".into(),
            sacct: "sacct".into(),
            squeue: "squeue".into(),
            watch: false,
            poll_interval: Duration::from_secs(10),
            accounting_wait: Duration::from_secs(120),
            max_wait: None,
            memory_factor: 2.0,
            max_steps: 2,
            settings: SlurmSettings::default(),
            env: AdapterEnv::default(),
            process_env: BTreeMap::new(),
            cwd: PathBuf::from("."),
            tmp_dir: None,
            mode: None,
            deadline_ms: 1000,
        }
    }
}

/// What happened.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct WrapperOutcome {
    /// Process exit code.
    pub exit_code: i32,
    /// `sbatch`'s exit code when it failed.
    pub sbatch_exit: Option<i32>,
    /// Submitted job ids, one per attempt.
    pub job_ids: Vec<String>,
    /// Final job state (`--watch`).
    pub final_state: Option<String>,
    /// Attempts submitted.
    pub attempts: u32,
    /// Effective mode.
    pub mode: Option<Mode>,
    /// The diff of the first attempt (shown in recommend and enforce).
    pub diff: String,
    /// sbatch's stdout (pass through).
    pub stdout: String,
    /// Messages for stderr.
    pub messages: Vec<String>,
}

/// A line diff (`-`/`+` lines only, with 1-based line numbers).
pub fn line_diff(old: &str, new: &str) -> String {
    let a: Vec<&str> = old.lines().collect();
    let b: Vec<&str> = new.lines().collect();
    let (n, m) = (a.len(), b.len());
    let mut l = vec![vec![0u32; m + 1]; n + 1];
    for i in (0..n).rev() {
        for j in (0..m).rev() {
            l[i][j] = if a[i] == b[j] { l[i + 1][j + 1] + 1 } else { l[i + 1][j].max(l[i][j + 1]) };
        }
    }
    let (mut i, mut j) = (0, 0);
    let mut out = String::new();
    while i < n || j < m {
        if i < n && j < m && a[i] == b[j] {
            i += 1;
            j += 1;
        } else if j < m && (i == n || l[i][j + 1] > l[i + 1][j]) {
            let _ = writeln!(out, "+{:>4} {}", j + 1, b[j]);
            j += 1;
        } else {
            let _ = writeln!(out, "-{:>4} {}", i + 1, a[i]);
            i += 1;
        }
    }
    out
}

fn argv_diff(old: &[String], new: &[String]) -> String {
    if old == new {
        return String::new();
    }
    format!("- sbatch {}\n+ sbatch {}\n", shlex::try_join(old.iter().map(String::as_str)).unwrap_or_default(), shlex::try_join(new.iter().map(String::as_str)).unwrap_or_default())
}

/// Read the script an argv names (relative to `cwd`).
pub fn read_script(argv: &[String], cwd: &Path) -> std::result::Result<Option<String>, String> {
    let j = parse(argv, None, &BTreeMap::new()).map_err(|e| e.to_string())?;
    match &j.script_path {
        Some(p) => {
            let path = if Path::new(p).is_absolute() { PathBuf::from(p) } else { cwd.join(p) };
            std::fs::read_to_string(&path).map(Some).map_err(|e| format!("{}: {e}", path.display()))
        }
        None => Ok(None),
    }
}

struct Submission {
    argv: Vec<String>,
    script: Option<String>,
    script_index: Option<usize>,
}

/// Write `script` under a fresh temp directory with the original's file name.
fn temp_script(cfg: &WrapperConfig, original: Option<&str>, script: &str) -> std::io::Result<(PathBuf, PathBuf)> {
    use std::io::Write as _;
    use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
    let base = cfg.tmp_dir.clone().unwrap_or_else(std::env::temp_dir);
    std::fs::create_dir_all(&base)?;
    // A fresh directory of our own (0700; `create`, not `create_dir_all`,
    // so a pre-existing path is an error) and a 0600 file: the script can
    // hold credentials and the default temp dir is shared.
    let dir = base.join(format!("rsz-sbatch-{}", Ulid::generate()));
    std::fs::DirBuilder::new().mode(0o700).create(&dir)?;
    let name = original.and_then(|p| Path::new(p).file_name()).and_then(|n| n.to_str()).unwrap_or("sbatch").to_string();
    let path = dir.join(name);
    let written = std::fs::OpenOptions::new().write(true).create_new(true).mode(0o600).open(&path).and_then(|mut f| f.write_all(script.as_bytes()));
    if let Err(e) = written {
        let _ = std::fs::remove_dir_all(&dir);
        return Err(e);
    }
    Ok((dir, path))
}

fn parse_job_id(stdout: &str) -> Option<String> {
    for line in stdout.lines() {
        let l = line.trim();
        if let Some(id) = l.strip_prefix("Submitted batch job ") {
            return id.split_whitespace().next().map(str::to_string);
        }
        let first = l.split(';').next().unwrap_or("");
        if !first.is_empty() && first.chars().all(|c| c.is_ascii_digit()) {
            return Some(first.to_string());
        }
    }
    None
}

async fn submit(sub: &Submission, cfg: &WrapperConfig, runner: &dyn CommandRunner, env: &[(String, String)], out: &mut WrapperOutcome) -> Option<String> {
    let mut argv = sub.argv.clone();
    let mut tmp_dir = None;
    if let (Some(i), Some(s)) = (sub.script_index, &sub.script) {
        match temp_script(cfg, Some(&sub.argv[i]), s) {
            Ok((d, p)) => {
                argv[i] = p.to_string_lossy().into_owned();
                tmp_dir = Some(d);
            }
            Err(e) => {
                out.messages.push(format!("rightsizer: cannot write the edited script ({e}); submitting the original"));
            }
        }
    } else if sub.script_index.is_none() && sub.script.is_some() && !argv.iter().any(|a| a.starts_with("--wrap")) {
        // Script from stdin: hand sbatch a file named like its stdin default.
        if let Ok((d, p)) = temp_script(cfg, None, sub.script.as_deref().unwrap_or("")) {
            argv.push(p.to_string_lossy().into_owned());
            tmp_dir = Some(d);
        }
    }
    let r = runner.run_env(&cfg.sbatch, &argv, env).await;
    if let Some(d) = tmp_dir {
        let _ = std::fs::remove_dir_all(d);
    }
    out.attempts += 1;
    match r {
        Ok(o) if o.success => {
            out.stdout.push_str(&o.stdout);
            let id = parse_job_id(&o.stdout);
            if let Some(id) = &id {
                out.job_ids.push(id.clone());
            }
            id
        }
        Ok(o) => {
            out.sbatch_exit = o.code;
            out.exit_code = EXIT_SBATCH_FAILED;
            out.stdout.push_str(&o.stdout);
            out.messages.push(format!("{}rightsizer: sbatch failed with exit code {}", o.stderr, o.code.map_or("signal".to_string(), |c| c.to_string())));
            None
        }
        Err(e) => {
            out.exit_code = EXIT_SBATCH_FAILED;
            out.messages.push(format!("rightsizer: cannot run {}: {e}", cfg.sbatch.display()));
            None
        }
    }
}

/// Final state of a job: `(worst state, failed array task ids)`.
async fn wait_for(job: &str, cfg: &WrapperConfig, runner: &dyn CommandRunner) -> (String, Vec<String>) {
    let started = std::time::Instant::now();
    let mut gone_since: Option<std::time::Instant> = None;
    loop {
        if cfg.max_wait.is_some_and(|m| started.elapsed() > m) {
            return ("UNKNOWN".into(), vec![]);
        }
        let q = runner.run(&cfg.squeue, &["-h".into(), "-j".into(), job.into(), "-o".into(), "%T".into()]).await;
        let queued = matches!(&q, Ok(o) if o.success && !o.stdout.trim().is_empty());
        if !queued {
            let s = runner.run(&cfg.sacct, &["-n".into(), "-X".into(), "-P".into(), "-j".into(), job.into(), "-o".into(), "JobID,State".into()]).await;
            if let Ok(o) = s
                && o.success
            {
                let rows: Vec<(String, String)> = o
                    .stdout
                    .lines()
                    .filter_map(|l| l.split_once('|'))
                    .map(|(id, st)| (id.trim().to_string(), st.split_whitespace().next().unwrap_or("").to_ascii_uppercase()))
                    .collect();
                let terminal = |s: &str| !matches!(s, "" | "PENDING" | "RUNNING" | "REQUEUED" | "SUSPENDED" | "CONFIGURING" | "COMPLETING" | "RESIZING");
                if !rows.is_empty() && rows.iter().all(|(_, s)| terminal(s)) {
                    let rank = |s: &str| match s {
                        "OUT_OF_MEMORY" => 0,
                        "TIMEOUT" => 1,
                        "FAILED" | "NODE_FAIL" | "BOOT_FAIL" => 2,
                        "CANCELLED" | "PREEMPTED" | "DEADLINE" => 3,
                        "COMPLETED" => 9,
                        _ => 4,
                    };
                    let worst = rows.iter().min_by_key(|(_, s)| rank(s)).map(|(_, s)| s.clone()).unwrap_or_default();
                    let oom_tasks = rows.iter().filter(|(_, s)| s == "OUT_OF_MEMORY").filter_map(|(id, _)| id.split_once('_').map(|(_, t)| t.to_string())).collect();
                    return (worst, oom_tasks);
                }
            }
            let since = *gone_since.get_or_insert_with(std::time::Instant::now);
            if since.elapsed() > cfg.accounting_wait {
                return ("UNKNOWN".into(), vec![]);
            }
        }
        tokio::time::sleep(cfg.poll_interval).await;
    }
}

fn status_of(state: &str) -> RunStatus {
    match state {
        "COMPLETED" => RunStatus::Succeeded,
        "OUT_OF_MEMORY" => RunStatus::OomKilled,
        "TIMEOUT" | "DEADLINE" => RunStatus::Timeout,
        "PREEMPTED" => RunStatus::Preempted,
        "UNKNOWN" => RunStatus::Unknown,
        _ => RunStatus::Failed,
    }
}

fn reduced(d: &Decision) -> bool {
    matches!((d.applied.requests.memory, d.prior.spec.requests.memory), (Some(a), Some(p)) if a < p)
}

fn token_exports(p: &Parsed, tokens: &[String]) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    for (k, c) in p.job.components.iter().enumerate() {
        let Some(t) = tokens.get(k) else { break };
        let base = c.value("export").filter(|e| !e.trim().is_empty()).unwrap_or("ALL");
        m.insert(option_key(k, "export"), format!("{base},{TOKEN_ENV}={t}"));
    }
    m
}

fn submission_of(native: &Native) -> Result<Submission> {
    let p = parse_native(native)?;
    let Native::Object { value } = native else { return Err(PluginError::InvalidNative("expected an sbatch object".into())) };
    let n: SlurmNative = serde_json::from_value(value.clone()).map_err(|e| PluginError::InvalidNative(e.to_string()))?;
    let SlurmNative::Sbatch { argv, script, .. } = n else { return Err(PluginError::InvalidNative("expected an sbatch object".into())) };
    Ok(Submission { argv, script, script_index: p.job.script_index })
}

fn with_exports(native: &Native, tokens: &[String]) -> Result<Native> {
    if tokens.is_empty() {
        return Ok(native.clone());
    }
    let p = parse_native(native)?;
    let argv = apply_argv_options(&p.job, &token_exports(&p, tokens))?;
    let Native::Object { value } = native else { return Ok(native.clone()) };
    let mut n: SlurmNative = serde_json::from_value(value.clone()).map_err(|e| PluginError::InvalidNative(e.to_string()))?;
    if let SlurmNative::Sbatch { argv: a, .. } = &mut n {
        *a = argv;
    }
    Ok(Native::Object { value: serde_json::to_value(n).map_err(|e| PluginError::Internal(e.to_string()))? })
}

/// Run the wrapper. `stdin_script` is the script when argv names none and
/// has no `--wrap` (sbatch reads stdin then).
pub async fn run(args: &[String], stdin_script: Option<String>, decider: &dyn Decider, cfg: &WrapperConfig, runner: &dyn CommandRunner) -> WrapperOutcome {
    let mut out = WrapperOutcome::default();
    let script = match read_script(args, &cfg.cwd) {
        Ok(Some(s)) => Some(s),
        Ok(None) => stdin_script,
        Err(e) => {
            // sbatch will report the missing script itself.
            out.messages.push(format!("rightsizer: {e}; submitting unchanged"));
            None
        }
    };
    let env: BTreeMap<String, String> = cfg.process_env.iter().filter(|(k, _)| k.starts_with("SBATCH_") || k.starts_with("RIGHTSIZER_")).map(|(k, v)| (k.clone(), v.clone())).collect();
    let native = SlurmNative::Sbatch { argv: args.to_vec(), script: script.clone(), env };
    let native = Native::Object { value: serde_json::to_value(&native).unwrap_or_default() };
    let original = Submission { argv: args.to_vec(), script: script.clone(), script_index: parse(args, script.as_deref(), &BTreeMap::new()).ok().and_then(|j| j.script_index) };
    let unchanged = |out: &mut WrapperOutcome, extra_env: Vec<(String, String)>| {
        let sub = Submission { script: if original.script_index.is_some() { None } else { original.script.clone() }, argv: original.argv.clone(), script_index: None };
        (sub, extra_env, out.exit_code)
    };

    // Parse and extract; skip → unchanged.
    let parsed = match parse_native(&native) {
        Ok(p) => p,
        Err(e) => {
            out.messages.push(format!("rightsizer: {e}; submitting unchanged"));
            let (sub, env, _) = unchanged(&mut out, vec![]);
            submit(&sub, cfg, runner, &env, &mut out).await;
            return out;
        }
    };
    let extracted = extract_parsed(&parsed, &cfg.env, &cfg.settings);
    if !matches!(&extracted, Ok(x) if x.skip.is_none()) {
        if let Ok(x) = &extracted
            && let Some(s) = &x.skip
        {
            out.messages.push(format!("rightsizer: not sizing ({s:?})"));
        }
        let (sub, env, _) = unchanged(&mut out, vec![]);
        submit(&sub, cfg, runner, &env, &mut out).await;
        return out;
    }

    let params = DecideParams {
        kind: "slurm".into(),
        native: native.clone(),
        mode: cfg.mode,
        env: cfg.env.clone(),
        deadline_ms: Some(cfg.deadline_ms),
        attempt: None,
        watch: cfg.watch,
        dry_run: false,
    };
    let result = match decider.decide(params).await {
        Ok(r) => r,
        Err(e) => {
            out.messages.push(format!("rightsizer: decide failed ({e}); submitting unchanged"));
            let (sub, env, _) = unchanged(&mut out, vec![]);
            submit(&sub, cfg, runner, &env, &mut out).await;
            return out;
        }
    };
    out.mode = Some(result.mode);
    let decided_env: Vec<(String, String)> = result.decisions.first().map(|d| vec![("RIGHTSIZER_DECIDED".to_string(), d.id.to_string())]).unwrap_or_default();

    // Patch, validate, apply.
    let patched = if result.patch.is_none() {
        None
    } else {
        match apply_patch(&native, &result.patch).and_then(|n| parse_native(&n).map(|p| (n, p))) {
            Ok((n, after)) => match validate_parsed(&parsed, &after, &cfg.env, &cfg.settings) {
                Ok(()) => Some(n),
                Err(rej) => {
                    out.messages.push(format!("rightsizer: patch rejected ({}: {}); submitting unchanged", rej.reason, rej.detail.unwrap_or_default()));
                    None
                }
            },
            Err(e) => {
                out.messages.push(format!("rightsizer: cannot apply the patch ({e}); submitting unchanged"));
                None
            }
        }
    };
    if let Some(n) = &patched
        && let Ok(sub) = submission_of(n)
    {
        out.diff = format!("{}{}", argv_diff(&original.argv, &sub.argv), line_diff(original.script.as_deref().unwrap_or(""), sub.script.as_deref().unwrap_or("")));
    }
    let mut explanation = vec![result.summary.clone()];
    for d in &result.decisions {
        explanation.extend(d.explanation.iter().map(|l| format!("  {}: {l}", d.unit)));
    }
    let enforce = result.mode == Mode::Enforce && patched.is_some();
    if result.mode == Mode::Recommend || (result.mode == Mode::Enforce && patched.is_some()) {
        let verb = if enforce { "applied" } else { "recommended (not applied)" };
        let mut m = format!("rightsizer [{}] {verb}:\n", result.mode.as_str());
        if !out.diff.is_empty() {
            m.push_str(&out.diff);
        }
        m.push_str(&explanation.join("\n"));
        out.messages.push(m);
    }
    if !enforce {
        let (sub, _, _) = unchanged(&mut out, vec![]);
        let id = submit(&sub, cfg, runner, &decided_env, &mut out).await;
        if cfg.watch && let Some(id) = id {
            let (state, _) = wait_for(&id, cfg, runner).await;
            out.exit_code = if state == "COMPLETED" { 0 } else { 1 };
            out.final_state = Some(state);
        }
        return out;
    }
    let mut current = patched.unwrap_or_else(|| native.clone());
    let tokens = result.token.as_deref().map(split_tokens).unwrap_or_default();

    // Ladder state.
    let ids: Vec<Ulid> = result.decisions.iter().map(|d| d.id).collect();
    // The invocation is named after the first decision: the sacct importer
    // links every job attributed to that decision (the ladder's attempts)
    // into this invocation.
    let invocation = ids.first().map(crate::sacct::ladder_invocation).unwrap_or_else(|| format!("rsz-sbatch:{}", Ulid::generate()));
    let units: Vec<(WorkloadKey, &Decision)> = result.decisions.iter().map(|d| (d.key.clone(), d)).collect();
    let mut applied: Vec<ResourceSpec> = result.decisions.iter().map(|d| d.applied.clone()).collect();
    let mut step = 0u32;
    let mut submit_native = with_exports(&current, &tokens).unwrap_or_else(|_| current.clone());
    loop {
        // Lease before a reduced attempt; no lease, no reduction.
        let is_reduced = units.iter().zip(&applied).any(|((_, d), a)| matches!((a.requests.memory, d.prior.spec.requests.memory), (Some(x), Some(p)) if x < p));
        if cfg.watch && is_reduced {
            let mut ok = true;
            for ((key, d), a) in units.iter().zip(&applied) {
                let rec = AttemptRecord {
                    v: 1,
                    key: key.clone(),
                    invocation_id: invocation.clone(),
                    origin_prior: d.prior.spec.requests.clone(),
                    applied: a.requests.clone(),
                    decision_id: Some(d.id),
                    reduced: reduced(d),
                    written_at: Utc::now(),
                    lease: Some(Lease { opened_at: Utc::now(), closed_at: None, outcome: None }),
                };
                if let Err(e) = decider.open_lease(&rec).await {
                    out.messages.push(format!("rightsizer: cannot open the ladder lease ({e}); submitting at the prior"));
                    ok = false;
                    break;
                }
            }
            if !ok {
                let (sub, _, _) = unchanged(&mut out, vec![]);
                let id = submit(&sub, cfg, runner, &decided_env, &mut out).await;
                if let Some(id) = id {
                    let (state, _) = wait_for(&id, cfg, runner).await;
                    out.exit_code = if state == "COMPLETED" { 0 } else { 1 };
                    out.final_state = Some(state);
                }
                return out;
            }
        }
        let sub = match submission_of(&submit_native) {
            Ok(s) => s,
            Err(e) => {
                out.messages.push(format!("rightsizer: {e}; submitting unchanged"));
                let (sub, _, _) = unchanged(&mut out, vec![]);
                submit(&sub, cfg, runner, &decided_env, &mut out).await;
                return out;
            }
        };
        let Some(id) = submit(&sub, cfg, runner, &decided_env, &mut out).await else { return out };
        if !cfg.watch {
            return out;
        }
        let (state, oom_tasks) = wait_for(&id, cfg, runner).await;
        let status = status_of(&state);
        out.final_state = Some(state.clone());
        if status != RunStatus::OomKilled || step >= cfg.max_steps {
            for (key, _) in &units {
                let _ = decider.close_lease(key, &invocation, status).await;
            }
            out.exit_code = if state == "COMPLETED" { 0 } else { 1 };
            return out;
        }
        // One ladder step up.
        step += 1;
        let p = match parse_native(&current) {
            Ok(p) => p,
            Err(_) => {
                for (key, _) in &units {
                    let _ = decider.close_lease(key, &invocation, status).await;
                }
                out.exit_code = 1;
                return out;
            }
        };
        let mut changes = Vec::new();
        for (k, ((_, d), a)) in units.iter().zip(applied.iter_mut()).enumerate() {
            let Some(mem) = a.requests.memory else { continue };
            let mut next = Bytes(((mem.0 as f64) * cfg.memory_factor as f64).ceil() as u64);
            if step == cfg.max_steps
                && let Some(prior) = d.prior.spec.requests.memory
            {
                next = next.max(prior);
            }
            a.requests.memory = Some(next);
            let spec = ResourceSpec { requests: Requests { memory: Some(next), ..Default::default() }, limits: Default::default() };
            let comp = p.component_of(&d.unit).unwrap_or(k);
            changes.extend(component_changes(&p, comp, &spec, &cfg.settings));
        }
        let mut edits = render_changes(&p, &changes);
        if !oom_tasks.is_empty() && p.job.components.first().is_some_and(|c| c.has("array")) {
            edits.options.insert("array".into(), oom_tasks.join(","));
        }
        current = match apply_patch(&current, &edits.into_patch()) {
            Ok(n) => n,
            Err(e) => {
                out.messages.push(format!("rightsizer: cannot escalate ({e})"));
                for (key, _) in &units {
                    let _ = decider.close_lease(key, &invocation, status).await;
                }
                out.exit_code = 1;
                return out;
            }
        };
        let tok = decider.sign(&current, &ids).await.ok().flatten().map(|t| split_tokens(&t)).unwrap_or_default();
        submit_native = with_exports(&current, &tok).unwrap_or_else(|_| current.clone());
        // The lease stays open across the ladder: the OOM was not the
        // caller's final answer. It is closed with the final status.
        out.messages.push(format!(
            "rightsizer: job {id} ran out of memory; resubmitting at ladder step {step}/{} ({})",
            cfg.max_steps,
            applied.iter().filter_map(|a| a.requests.memory).map(|m| rightsizer_core::units::format_slurm_mem(Bytes(m.0.div_ceil(1 << 20) << 20))).collect::<Vec<_>>().join(", ")
        ));
    }
}

/// Patch helper for tests and callers: the patch a decision set renders to.
pub fn render_for(native: &Native, decisions: &[Decision], settings: &SlurmSettings) -> Result<Patch> {
    let p = parse_native(native)?;
    let d: Vec<_> = decisions.iter().map(|d| (d.unit.clone(), d.applied.clone())).collect();
    Ok(crate::adapter::render_parsed(&p, &d, settings).into_patch())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diffs_and_job_ids() {
        let d = line_diff("#!/bin/sh\n#SBATCH --mem=64G\necho\n", "#!/bin/sh\n#SBATCH --mem=24G\n#SBATCH --cpus-per-task=2\necho\n");
        assert_eq!(d, "-   2 #SBATCH --mem=64G\n+   2 #SBATCH --mem=24G\n+   3 #SBATCH --cpus-per-task=2\n");
        assert_eq!(parse_job_id("Submitted batch job 42\n"), Some("42".into()));
        assert_eq!(parse_job_id("43;rsz-it\n"), Some("43".into()));
        assert_eq!(parse_job_id("nothing"), None);
    }
}
