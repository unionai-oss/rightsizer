//! `sbatch` submissions: script directives, argv, `SBATCH_*` environment,
//! heterogeneous components, and the resources they ask for.
//!
//! Precedence follows `sbatch` (src/sbatch/opt.c): for component *k*, the
//! script's `#SBATCH` lines of component *k*, then `SBATCH_*` variables,
//! then argv component *k* (later wins). Directives are read only from the
//! leading block: the first line whose first non-blank character is not `#`
//! ends it. Magic words are `#SBATCH` and `#SLURM` at column 0; `hetjob`
//! (or the deprecated `packjob`) as the first word starts a new component.
//! In argv, `:` separates components; the first other non-option token is
//! the script, and the tokens after it are the script's arguments.

use std::collections::BTreeMap;

use rightsizer_core::units::parse_slurm_mem;
use rightsizer_core::{AxisSet, Bytes};

use crate::options::{self, ArgForm, MEMORY_OPTIONS, RESOURCE_ENV, parse_tokens, tokenize_directive};

/// Where an option value came from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Origin {
    /// An `#SBATCH` line. Spans are absolute byte offsets in the script.
    Script {
        /// 0-based line number.
        line: usize,
        /// Raw span of the token holding the option (`--mem=4G`, or `-c` for `-c 4`).
        token: (usize, usize),
        /// Raw span of the value, when it can be replaced in place.
        value: Option<(usize, usize)>,
        /// Form.
        form: ArgForm,
    },
    /// argv.
    Argv {
        /// Index of the token holding the value.
        token: usize,
        /// Byte offset of the value inside it.
        offset: usize,
        /// Form.
        form: ArgForm,
    },
    /// An `SBATCH_*` environment variable.
    Env {
        /// Variable name.
        var: String,
    },
    /// cli_filter's merged option table (no finer origin).
    Table,
}

impl Origin {
    /// Whether the value can be rewritten where it was written.
    pub fn editable(&self) -> bool {
        !matches!(self, Origin::Env { .. })
    }
}

/// One option occurrence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OptValue {
    /// Long name.
    pub name: &'static str,
    /// Value (`None` for flags).
    pub value: Option<String>,
    /// Origin.
    pub origin: Origin,
}

/// One job component (a heterogeneous job has several).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Component {
    /// 0-based het offset.
    pub index: usize,
    /// Occurrences in precedence order (last wins).
    pub opts: Vec<OptValue>,
    /// Where a new `#SBATCH` line for this component can be inserted.
    pub script_insert_at: Option<usize>,
    /// Whether the component has `#SBATCH` lines of its own.
    pub has_directives: bool,
    /// argv token range of this component's options.
    pub argv_segment: Option<(usize, usize)>,
}

impl Component {
    /// The effective occurrence of `name`.
    pub fn get(&self, name: &str) -> Option<&OptValue> {
        self.opts.iter().rev().find(|o| o.name == name)
    }
    /// The effective value of `name`.
    pub fn value(&self, name: &str) -> Option<&str> {
        self.get(name).and_then(|o| o.value.as_deref())
    }
    /// Whether `name` was given at all.
    pub fn has(&self, name: &str) -> bool {
        self.get(name).is_some()
    }
    /// The memory options present (distinct names).
    pub fn memory_forms(&self) -> Vec<&'static str> {
        let mut v: Vec<&'static str> = Vec::new();
        for o in &self.opts {
            if MEMORY_OPTIONS.contains(&o.name) && !v.contains(&o.name) {
                v.push(o.name);
            }
        }
        v
    }
    /// The effective (merged) options as long name → value, what Slurm
    /// would put into `job_desc` from (see [`crate::token::JobDescResources::from_options`]).
    pub fn effective(&self) -> BTreeMap<&'static str, String> {
        let mut m = BTreeMap::new();
        for o in &self.opts {
            m.insert(o.name, o.value.clone().unwrap_or_default());
        }
        m
    }
}

/// `#RIGHTSIZER` directives (`key=<name>`, `pin=<axes>`, `off`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RightsizerDirectives {
    /// `key=`: the workload key suffix.
    pub key: Option<String>,
    /// `pin=memory,cpu`: axes kept at the prior.
    pub pins: AxisSet,
    /// `off` / `opt-out`: never change this job.
    pub off: bool,
}

/// A parsed submission.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SbatchJob {
    /// argv after `sbatch` (options, script, script arguments).
    pub argv: Vec<String>,
    /// Script text (the file, stdin, or `None` for `--wrap`).
    pub script: Option<String>,
    /// Script path from argv.
    pub script_path: Option<String>,
    /// Index of the script path in argv.
    pub script_index: Option<usize>,
    /// Script arguments.
    pub script_args: Vec<String>,
    /// Components.
    pub components: Vec<Component>,
    /// `#RIGHTSIZER` directives.
    pub rightsizer: RightsizerDirectives,
    /// Problems that make the submission unsafe to change (sbatch itself
    /// will reject it, or it cannot be read).
    pub problems: Vec<String>,
}

/// Parse errors that abort parsing.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ParseError {
    /// A directive line sbatch would reject.
    #[error("line {line}: {msg}")]
    Directive {
        /// 1-based line.
        line: usize,
        /// Message.
        msg: String,
    },
}

/// Lines of a script with their byte offsets: `(start, text)` where `text`
/// excludes the `\n`.
pub fn lines_with_offsets(s: &str) -> Vec<(usize, &str)> {
    let mut out = Vec::new();
    let mut start = 0;
    for (i, b) in s.bytes().enumerate() {
        if b == b'\n' {
            out.push((start, &s[start..i]));
            start = i + 1;
        }
    }
    if start < s.len() {
        out.push((start, &s[start..]));
    }
    out
}

/// The magic word an `#SBATCH`-style line starts with, and its length.
pub fn directive_magic(line: &str) -> Option<usize> {
    if line.starts_with("#SBATCH") {
        Some(7)
    } else if line.starts_with("#SLURM") {
        Some(6)
    } else {
        None
    }
}

/// Whether a line ends the leading directive block (sbatch: first
/// non-blank character is not `#`).
pub fn is_command_line(line: &str) -> bool {
    line.trim_start().chars().next().is_some_and(|c| c != '#')
}

fn het_word(rest: &str) -> Option<usize> {
    let t = rest.trim_start();
    let skipped = rest.len() - t.len();
    for w in ["hetjob", "packjob"] {
        if t.len() >= w.len() && t[..w.len()].eq_ignore_ascii_case(w) && t[w.len()..].chars().next().is_none_or(|c| c.is_ascii_whitespace()) {
            return Some(skipped + w.len());
        }
    }
    None
}

/// Script directives per component, plus `#RIGHTSIZER` directives.
fn parse_script(script: &str, comps: &mut Vec<Component>, rs: &mut RightsizerDirectives) -> Result<(), ParseError> {
    let lines = lines_with_offsets(script);
    let mut comp = 0usize;
    let mut in_block = true;
    let first_insert = match lines.first() {
        Some((_, l)) if l.starts_with("#!") => lines.get(1).map_or(script.len(), |(s, _)| *s),
        _ => 0,
    };
    ensure(comps, 0);
    comps[0].script_insert_at = Some(first_insert);
    for (ln, (start, line)) in lines.iter().enumerate() {
        let end_of_line = (start + line.len() + 1).min(script.len());
        if let Some(rest) = line.strip_prefix("#RIGHTSIZER") {
            parse_rightsizer(rest, rs);
            continue;
        }
        if !in_block {
            continue;
        }
        let Some(m) = directive_magic(line) else {
            if is_command_line(line) {
                in_block = false;
            }
            continue;
        };
        let mut from = m;
        if let Some(n) = het_word(&line[m..]) {
            comp += 1;
            from = m + n;
        }
        ensure(comps, comp);
        comps[comp].has_directives = true;
        comps[comp].script_insert_at = Some(end_of_line);
        let toks = tokenize_directive(line, from).map_err(|msg| ParseError::Directive { line: ln + 1, msg })?;
        let texts: Vec<String> = toks.iter().map(|t| t.text.clone()).collect();
        let p = parse_tokens(&texts, 0, texts.len());
        if let Some(i) = p.first_positional {
            return Err(ParseError::Directive { line: ln + 1, msg: format!("invalid directive argument {:?}", texts[i]) });
        }
        for o in p.opts {
            let t = &toks[o.value_token];
            let token_span = (start + t.start, start + t.end);
            let value = match o.form {
                ArgForm::Flag => None,
                ArgForm::LongSep | ArgForm::ShortSep => Some(token_span),
                ArgForm::LongEq | ArgForm::ShortAttached if !t.raw => Some((token_span.0 + o.value_offset, token_span.1)),
                _ => None,
            };
            // For separate-token forms the option token is the previous one.
            let opt_token = if matches!(o.form, ArgForm::LongSep | ArgForm::ShortSep) && o.value_token > 0 {
                let ot = &toks[o.value_token - 1];
                (start + ot.start, start + ot.end)
            } else {
                token_span
            };
            comps[comp].opts.push(OptValue { name: o.name, value: o.value, origin: Origin::Script { line: ln, token: opt_token, value, form: o.form } });
        }
    }
    Ok(())
}

fn parse_rightsizer(rest: &str, rs: &mut RightsizerDirectives) {
    for w in rest.split_whitespace() {
        if let Some(k) = w.strip_prefix("key=") {
            if !k.is_empty() {
                rs.key = Some(k.to_string());
            }
        } else if let Some(p) = w.strip_prefix("pin=") {
            if let Ok(a) = AxisSet::parse_list(p) {
                rs.pins = rs.pins.union(a);
            }
        } else if w == "off" || w == "opt-out" {
            rs.off = true;
        }
    }
}

fn ensure(comps: &mut Vec<Component>, k: usize) {
    while comps.len() <= k {
        let index = comps.len();
        comps.push(Component { index, ..Default::default() });
    }
}

/// Parse a submission: argv (after `sbatch`), the script text (when not
/// `--wrap`), and the submitter's environment (only `SBATCH_*` resource
/// variables are read).
pub fn parse(argv: &[String], script: Option<&str>, env: &BTreeMap<String, String>) -> Result<SbatchJob, ParseError> {
    let mut job = SbatchJob { argv: argv.to_vec(), script: script.map(str::to_string), ..Default::default() };
    // argv components.
    let mut argv_comps: Vec<Vec<OptValue>> = Vec::new();
    let mut segments: Vec<(usize, usize)> = Vec::new();
    let mut seg_start = 0;
    loop {
        let p = parse_tokens(argv, seg_start, argv.len());
        let mut opts = Vec::new();
        for o in p.opts {
            opts.push(OptValue { name: o.name, value: o.value, origin: Origin::Argv { token: o.value_token, offset: o.value_offset, form: o.form } });
        }
        argv_comps.push(opts);
        match p.first_positional {
            Some(i) if argv[i] == ":" => {
                segments.push((seg_start, i));
                seg_start = i + 1;
            }
            Some(i) => {
                segments.push((seg_start, i));
                job.script_index = Some(i);
                job.script_path = Some(argv[i].clone());
                job.script_args = argv[i + 1..].to_vec();
                break;
            }
            None => {
                segments.push((seg_start, argv.len()));
                break;
            }
        }
    }
    let mut comps: Vec<Component> = Vec::new();
    if let Some(s) = script {
        parse_script(s, &mut comps, &mut job.rightsizer)?;
    }
    ensure(&mut comps, argv_comps.len().saturating_sub(1));
    let env_opts: Vec<OptValue> = RESOURCE_ENV
        .iter()
        .filter_map(|(var, name)| {
            env.get(*var).map(|v| OptValue {
                name: options::lookup_long(name).map(|s| s.long).unwrap_or("unknown"),
                value: Some(v.clone()),
                origin: Origin::Env { var: var.to_string() },
            })
        })
        .collect();
    for c in comps.iter_mut() {
        c.opts.extend(env_opts.iter().cloned());
    }
    for (k, opts) in argv_comps.into_iter().enumerate() {
        comps[k].opts.extend(opts);
        comps[k].argv_segment = segments.get(k).copied();
    }
    for c in &comps {
        let forms = c.memory_forms();
        if forms.len() > 1 {
            job.problems.push(format!("component {}: --{} are mutually exclusive (sbatch rejects this)", c.index, forms.join(", --")));
        }
    }
    let wrap = comps.first().and_then(|c| c.value("wrap")).map(str::to_string);
    if job.script.is_none() && wrap.is_none() && job.script_path.is_some() {
        job.problems.push("script text not available".into());
    }
    if let Some(w) = wrap
        && job.script.is_none()
    {
        // The script sbatch generates; directives are not read from it.
        job.script = Some(crate::token::wrap_script(&w));
    }
    job.components = comps;
    Ok(job)
}

/// Build a single-component job from cli_filter's option table
/// (`slurm.json_cli_options(options)`: long name → string, flags `"set"`).
pub fn from_cli_options(options: &serde_json::Map<String, serde_json::Value>, offset: usize, script: Option<&str>) -> SbatchJob {
    let mut c = Component { index: offset, ..Default::default() };
    for (k, v) in options {
        let Some(spec) = options::lookup_long(k) else { continue };
        let value = match v {
            serde_json::Value::String(s) if s == "set" && spec.arg == options::HasArg::No => None,
            serde_json::Value::String(s) => Some(s.clone()),
            serde_json::Value::Number(n) => Some(n.to_string()),
            _ => continue,
        };
        c.opts.push(OptValue { name: spec.long, value, origin: Origin::Table });
    }
    let mut job = SbatchJob { script: script.map(str::to_string), ..Default::default() };
    if let Some(s) = script {
        let mut tmp = Vec::new();
        let _ = parse_script(s, &mut tmp, &mut job.rightsizer);
    }
    // cli_filter reports `cpus-per-task=0` when unset.
    c.opts.retain(|o| !(o.name == "cpus-per-task" && o.value.as_deref() == Some("0")));
    if job.script.is_none()
        && let Some(w) = c.value("wrap")
    {
        job.script = Some(crate::token::wrap_script(w));
    }
    job.components = vec![c];
    job
}

/// Parse a Slurm time limit (`--time`): `minutes`, `minutes:seconds`,
/// `hours:minutes:seconds`, `days-hours`, `days-hours:minutes`,
/// `days-hours:minutes:seconds`. `Ok(None)` is unlimited
/// (`INFINITE`/`UNLIMITED`/`-1`).
pub fn parse_time(s: &str) -> Result<Option<u64>, String> {
    let t = s.trim();
    if t.eq_ignore_ascii_case("infinite") || t.eq_ignore_ascii_case("unlimited") || t == "-1" {
        return Ok(None);
    }
    let bad = || format!("invalid time {s:?}");
    let num = |x: &str| x.parse::<u64>().map_err(|_| bad());
    let (days, rest) = match t.split_once('-') {
        Some((d, r)) => (Some(num(d)?), r),
        None => (None, t),
    };
    let parts: Vec<&str> = rest.split(':').collect();
    if parts.iter().any(|p| p.is_empty()) || parts.len() > 3 {
        return Err(bad());
    }
    let p: Vec<u64> = parts.iter().map(|x| num(x)).collect::<Result<_, _>>()?;
    let secs = match (days, p.as_slice()) {
        (None, [m]) => m * 60,
        (None, [m, s]) => m * 60 + s,
        (None, [h, m, s]) => h * 3600 + m * 60 + s,
        (Some(d), [h]) => d * 86400 + h * 3600,
        (Some(d), [h, m]) => d * 86400 + h * 3600 + m * 60,
        (Some(d), [h, m, s]) => d * 86400 + h * 3600 + m * 60 + s,
        _ => return Err(bad()),
    };
    Ok(Some(secs))
}

/// Format seconds the way `--time` accepts (`D-HH:MM:SS`, or `HH:MM:SS`).
pub fn format_time(secs: u64) -> String {
    let (d, r) = (secs / 86400, secs % 86400);
    let (h, m, s) = (r / 3600, (r % 3600) / 60, r % 60);
    if d > 0 { format!("{d}-{h:02}:{m:02}:{s:02}") } else { format!("{h:02}:{m:02}:{s:02}") }
}

/// Which memory option.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum MemForm {
    /// `--mem` (per node).
    PerNode,
    /// `--mem-per-cpu`.
    PerCpu,
    /// `--mem-per-gpu`.
    PerGpu,
}

impl MemForm {
    /// Long option name.
    pub fn option(self) -> &'static str {
        match self {
            MemForm::PerNode => "mem",
            MemForm::PerCpu => "mem-per-cpu",
            MemForm::PerGpu => "mem-per-gpu",
        }
    }
}

/// A memory request as written.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemReq {
    /// Form.
    pub form: MemForm,
    /// Value (per node / per CPU / per GPU).
    pub value: Bytes,
    /// Whether the user wrote a unit suffix (kept on rewrite).
    pub suffixed: bool,
}

/// Which GPU flag.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum GpuFlag {
    /// `--gres=gpu[:type]:n` (per node).
    Gres,
    /// `--gpus=[type:]n` (per job).
    Gpus,
    /// `--gpus-per-node`.
    GpusPerNode,
    /// `--gpus-per-task`.
    GpusPerTask,
    /// `--gpus-per-socket`.
    GpusPerSocket,
}

impl GpuFlag {
    /// Long option name.
    pub fn option(self) -> &'static str {
        match self {
            GpuFlag::Gres => "gres",
            GpuFlag::Gpus => "gpus",
            GpuFlag::GpusPerNode => "gpus-per-node",
            GpuFlag::GpusPerTask => "gpus-per-task",
            GpuFlag::GpusPerSocket => "gpus-per-socket",
        }
    }
}

/// A GPU request as written.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GpuReq {
    /// Flag.
    pub flag: GpuFlag,
    /// gres type (`a100`), if typed.
    pub gpu_type: Option<String>,
    /// Count as written (per node, job, task or socket).
    pub count: u32,
}

/// One `--gres` entry: `name[:type][:count]` (optionally `gres/`-prefixed).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GresEntry {
    /// Name (`gpu`).
    pub name: String,
    /// Type.
    pub gpu_type: Option<String>,
    /// Count.
    pub count: u32,
}

/// Parse a `--gres` list.
pub fn parse_gres(s: &str) -> Result<Vec<GresEntry>, String> {
    let mut out = Vec::new();
    for e in s.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        if e.eq_ignore_ascii_case("none") {
            continue;
        }
        let e = e.strip_prefix("gres/").or_else(|| e.strip_prefix("gres:")).unwrap_or(e);
        let e = e.split('(').next().unwrap_or(e);
        let parts: Vec<&str> = e.split(':').collect();
        let (name, ty, count) = match parts.as_slice() {
            [n] => (*n, None, 1),
            [n, x] => match x.parse::<u32>() {
                Ok(c) => (*n, None, c),
                Err(_) => (*n, Some(*x), 1),
            },
            [n, t, c] => (*n, Some(*t), c.parse::<u32>().map_err(|_| format!("invalid gres count in {e:?}"))?),
            _ => return Err(format!("invalid gres {e:?}")),
        };
        out.push(GresEntry { name: name.to_string(), gpu_type: ty.map(str::to_string), count });
    }
    Ok(out)
}

/// Parse a `--gpus*` value: `[type:]count`.
pub fn parse_gpus(s: &str) -> Result<(Option<String>, u32), String> {
    let s = s.trim();
    let (ty, n) = match s.rsplit_once(':') {
        Some((t, n)) => (Some(t.to_string()), n),
        None => (None, s),
    };
    let n = n.parse::<u32>().map_err(|_| format!("invalid GPU count {s:?}"))?;
    Ok((ty, n))
}

/// What one component asks for (before translation to [`rightsizer_core::Requests`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CompResources {
    /// `--nodes` minimum.
    pub nodes_min: Option<u32>,
    /// `--ntasks`.
    pub ntasks: Option<u32>,
    /// `--ntasks-per-node`.
    pub ntasks_per_node: Option<u32>,
    /// `--cpus-per-task`.
    pub cpus_per_task: Option<u32>,
    /// Memory request.
    pub mem: Option<MemReq>,
    /// GPU request (the GPU entry of `--gres`, or one `--gpus*` flag).
    pub gpu: Option<GpuReq>,
    /// More than one GPU flag, or a GPU flag we cannot convert.
    pub gpu_ambiguous: bool,
    /// `--tmp`.
    pub tmp: Option<Bytes>,
    /// `--time`: `Some(None)` = unlimited.
    pub time_s: Option<Option<u64>>,
    /// `--partition`.
    pub partition: Option<String>,
    /// `--account`.
    pub account: Option<String>,
    /// `--qos`.
    pub qos: Option<String>,
    /// `--job-name` (explicit).
    pub job_name: Option<String>,
    /// `--array`.
    pub array: Option<String>,
    /// `--exclusive`.
    pub exclusive: bool,
    /// `--constraint`.
    pub constraint: Option<String>,
    /// Values that did not parse.
    pub errors: Vec<String>,
}

fn suffixed(v: &str) -> bool {
    v.trim().chars().last().is_some_and(|c| c.is_ascii_alphabetic())
}

impl CompResources {
    /// Read a component.
    pub fn of(c: &Component) -> Self {
        let mut r = CompResources::default();
        let num = |r: &mut CompResources, name: &str| -> Option<u32> {
            let v = c.value(name)?;
            let first = v.split(['-', ':']).next().unwrap_or(v);
            match first.trim().parse::<u32>() {
                Ok(n) => Some(n),
                Err(_) => {
                    r.errors.push(format!("--{name}={v}"));
                    None
                }
            }
        };
        r.nodes_min = num(&mut r, "nodes");
        r.ntasks = num(&mut r, "ntasks");
        r.ntasks_per_node = num(&mut r, "ntasks-per-node").or_else(|| num(&mut r, "tasks-per-node"));
        r.cpus_per_task = num(&mut r, "cpus-per-task").filter(|n| *n > 0);
        // Memory: the effective occurrence (sbatch rejects mixes; see problems).
        let mem_opt = c.opts.iter().rev().find(|o| MEMORY_OPTIONS.contains(&o.name));
        if let Some(o) = mem_opt {
            let form = match o.name {
                "mem" => MemForm::PerNode,
                "mem-per-cpu" => MemForm::PerCpu,
                _ => MemForm::PerGpu,
            };
            let v = o.value.as_deref().unwrap_or("");
            match parse_slurm_mem(v) {
                Ok(b) => r.mem = Some(MemReq { form, value: b, suffixed: suffixed(v) }),
                Err(_) => r.errors.push(format!("--{}={v}", o.name)),
            }
        }
        let mut gpu_flags = 0;
        if let Some(v) = c.value("gres") {
            match parse_gres(v) {
                Ok(es) => {
                    let gpus: Vec<&GresEntry> = es.iter().filter(|e| e.name == "gpu").collect();
                    if gpus.len() > 1 {
                        r.gpu_ambiguous = true;
                    }
                    if let Some(g) = gpus.first() {
                        gpu_flags += 1;
                        r.gpu = Some(GpuReq { flag: GpuFlag::Gres, gpu_type: g.gpu_type.clone(), count: g.count });
                    }
                }
                Err(e) => r.errors.push(e),
            }
        }
        for flag in [GpuFlag::Gpus, GpuFlag::GpusPerNode, GpuFlag::GpusPerTask, GpuFlag::GpusPerSocket] {
            if let Some(v) = c.value(flag.option()) {
                match parse_gpus(v) {
                    Ok((t, n)) => {
                        gpu_flags += 1;
                        r.gpu = Some(GpuReq { flag, gpu_type: t, count: n });
                    }
                    Err(e) => r.errors.push(e),
                }
            }
        }
        if gpu_flags > 1 {
            r.gpu_ambiguous = true;
        }
        if let Some(v) = c.value("tmp") {
            match parse_slurm_mem(v) {
                Ok(b) => r.tmp = Some(b),
                Err(_) => r.errors.push(format!("--tmp={v}")),
            }
        }
        if let Some(v) = c.value("time") {
            match parse_time(v) {
                Ok(t) => r.time_s = Some(t),
                Err(e) => r.errors.push(e),
            }
        }
        r.partition = c.value("partition").map(|p| p.split(',').next().unwrap_or(p).to_string());
        r.account = c.value("account").map(str::to_string);
        r.qos = c.value("qos").map(str::to_string);
        r.job_name = c.value("job-name").map(str::to_string);
        r.array = c.value("array").map(str::to_string);
        r.exclusive = c.has("exclusive");
        r.constraint = c.value("constraint").map(str::to_string);
        r
    }

    /// Nodes (minimum; at least what ntasks / ntasks-per-node implies).
    pub fn nodes(&self) -> u32 {
        let n = self.nodes_min.unwrap_or(1).max(1);
        match (self.ntasks, self.ntasks_per_node) {
            (Some(t), Some(p)) if p > 0 => n.max(t.div_ceil(p)),
            _ => n,
        }
    }

    /// Tasks per node (ntasks spread over the nodes; one task by default).
    pub fn tasks_per_node(&self) -> u32 {
        if let Some(p) = self.ntasks_per_node.filter(|p| *p > 0) {
            return p;
        }
        match self.ntasks {
            Some(t) if t > 0 => t.div_ceil(self.nodes()),
            _ => 1,
        }
    }

    /// Total tasks.
    pub fn tasks(&self) -> u32 {
        self.ntasks.unwrap_or_else(|| self.tasks_per_node() * self.nodes())
    }

    /// CPUs per node for a given cpus-per-task.
    pub fn cpus_per_node_with(&self, cpus_per_task: u32) -> u32 {
        cpus_per_task.max(1) * self.tasks_per_node()
    }

    /// CPUs per node.
    pub fn cpus_per_node(&self) -> u32 {
        self.cpus_per_node_with(self.cpus_per_task.unwrap_or(1))
    }

    /// GPUs per node, when the GPU request determines it.
    pub fn gpus_per_node(&self) -> Option<u32> {
        let g = self.gpu.as_ref()?;
        match g.flag {
            GpuFlag::Gres | GpuFlag::GpusPerNode => Some(g.count),
            GpuFlag::Gpus => Some(g.count.div_ceil(self.nodes())),
            GpuFlag::GpusPerTask => Some(g.count * self.tasks_per_node()),
            GpuFlag::GpusPerSocket => None,
        }
    }

    /// Memory per node in bytes (`Some(0)` = `--mem=0`, the whole node).
    pub fn mem_per_node(&self) -> Option<Bytes> {
        let m = self.mem.as_ref()?;
        match m.form {
            MemForm::PerNode => Some(m.value),
            MemForm::PerCpu => Some(Bytes(m.value.0.saturating_mul(self.cpus_per_node() as u64))),
            MemForm::PerGpu => self.gpus_per_node().map(|g| Bytes(m.value.0.saturating_mul(g as u64))),
        }
    }

    /// Multi-node or multi-task: GPU count is held (SPEC §5.4).
    pub fn distributed(&self) -> bool {
        self.nodes() > 1 || self.tasks() > 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(s: &str) -> Vec<String> {
        shlex::split(s).unwrap()
    }

    #[test]
    fn leading_block_only() {
        let s = "#!/bin/bash\n#SBATCH --mem=4G\n\n# a comment\n  # indented comment\n#SBATCH -c 2\nmodule load x\n#SBATCH --mem=99G\n";
        let j = parse(&argv("job.sh"), Some(s), &BTreeMap::new()).unwrap();
        let c = &j.components[0];
        assert_eq!(c.value("mem"), Some("4G"));
        assert_eq!(c.value("cpus-per-task"), Some("2"));
        assert_eq!(j.script_path.as_deref(), Some("job.sh"));
        // Insertion point is after the last directive line of the block.
        assert_eq!(&s[c.script_insert_at.unwrap()..c.script_insert_at.unwrap() + 6], "module");
    }

    #[test]
    fn argv_overrides_script_and_env_sits_between() {
        let s = "#!/bin/bash\n#SBATCH --time=10 --cpus-per-task=2\n";
        let env: BTreeMap<String, String> = [("SBATCH_TIMELIMIT".to_string(), "20".to_string())].into();
        let j = parse(&argv("-c 4 job.sh a b"), Some(s), &env).unwrap();
        let c = &j.components[0];
        assert_eq!(c.value("cpus-per-task"), Some("4"));
        assert!(matches!(c.get("cpus-per-task").unwrap().origin, Origin::Argv { token: 1, offset: 0, form: ArgForm::ShortSep }));
        assert_eq!(c.value("time"), Some("20"));
        assert!(matches!(c.get("time").unwrap().origin, Origin::Env { .. }));
        assert_eq!(j.script_args, vec!["a", "b"]);
    }

    #[test]
    fn het_components_script_and_argv() {
        let s = "#!/bin/bash\n#SBATCH --mem=1G\n#SBATCH hetjob --mem=2G\n#SBATCH -c 3\n#SBATCH packjob\n#SBATCH --mem=3G\nsrun x\n";
        let j = parse(&argv("-J a : -J b job.sh"), Some(s), &BTreeMap::new()).unwrap();
        assert_eq!(j.components.len(), 3);
        assert_eq!(j.components[0].value("mem"), Some("1G"));
        assert_eq!(j.components[1].value("mem"), Some("2G"));
        assert_eq!(j.components[1].value("cpus-per-task"), Some("3"));
        assert_eq!(j.components[2].value("mem"), Some("3G"));
        assert_eq!(j.components[1].value("job-name"), Some("b"));
        assert_eq!(j.components[0].argv_segment, Some((0, 2)));
        assert_eq!(j.components[1].argv_segment, Some((3, 5)));
    }

    #[test]
    fn mixed_memory_forms_are_a_problem() {
        let j = parse(&argv("--mem-per-cpu=100 job.sh"), Some("#!/bin/sh\n#SBATCH --mem=1G\n"), &BTreeMap::new()).unwrap();
        assert_eq!(j.problems.len(), 1);
    }

    #[test]
    fn wrap_builds_the_script() {
        let j = parse(&argv("--mem=1G --wrap 'echo hi'"), None, &BTreeMap::new()).unwrap();
        assert_eq!(j.script.as_deref(), Some("#!/bin/sh\n# This script was created by sbatch --wrap.\n\necho hi\n"));
        assert!(j.problems.is_empty());
        assert!(j.script_path.is_none());
    }

    #[test]
    fn script_spans() {
        let s = "#!/bin/bash\n#SBATCH --mem=4G -c 8\n#SBATCH --gres=\"gpu:2\"\n";
        let j = parse(&argv("x.sh"), Some(s), &BTreeMap::new()).unwrap();
        let c = &j.components[0];
        let Origin::Script { value: Some((a, b)), .. } = c.get("mem").unwrap().origin else { panic!() };
        assert_eq!(&s[a..b], "4G");
        let Origin::Script { value: Some((a, b)), .. } = c.get("cpus-per-task").unwrap().origin else { panic!() };
        assert_eq!(&s[a..b], "8");
        // Quoted `--gres="gpu:2"`: no in-place value span; the token is replaced.
        let Origin::Script { value, token, .. } = c.get("gres").unwrap().origin else { panic!() };
        assert!(value.is_none());
        assert_eq!(&s[token.0..token.1], "--gres=\"gpu:2\"");
    }

    #[test]
    fn rightsizer_directives() {
        let s = "#!/bin/bash\n#RIGHTSIZER key=train-small pin=cpu\n#SBATCH --mem=1G\necho\n#RIGHTSIZER off\n";
        let j = parse(&argv("x.sh"), Some(s), &BTreeMap::new()).unwrap();
        assert_eq!(j.rightsizer.key.as_deref(), Some("train-small"));
        assert!(j.rightsizer.pins.contains(rightsizer_core::Axis::Cpu));
        assert!(j.rightsizer.off);
    }

    #[test]
    fn times() {
        assert_eq!(parse_time("10").unwrap(), Some(600));
        assert_eq!(parse_time("10:30").unwrap(), Some(630));
        assert_eq!(parse_time("1:02:03").unwrap(), Some(3723));
        assert_eq!(parse_time("1-02").unwrap(), Some(93600));
        assert_eq!(parse_time("1-02:03").unwrap(), Some(93780));
        assert_eq!(parse_time("1-02:03:04").unwrap(), Some(93784));
        assert_eq!(parse_time("UNLIMITED").unwrap(), None);
        assert!(parse_time("1:2:3:4").is_err());
        assert!(parse_time("abc").is_err());
        assert_eq!(format_time(93784), "1-02:03:04");
        assert_eq!(format_time(3723), "01:02:03");
    }

    #[test]
    fn resources_per_node() {
        let j = parse(&argv("-N 2 --ntasks-per-node=4 -c 2 --mem-per-cpu=2G --gpus=8 x.sh"), Some("#!/bin/sh\n"), &BTreeMap::new()).unwrap();
        let r = CompResources::of(&j.components[0]);
        assert_eq!(r.nodes(), 2);
        assert_eq!(r.tasks_per_node(), 4);
        assert_eq!(r.cpus_per_node(), 8);
        assert_eq!(r.mem_per_node(), Some(Bytes::gib(16)));
        assert_eq!(r.gpus_per_node(), Some(4));
        assert!(r.distributed());
        let g = parse_gres("gpu:a100:2,gres/shard:4,license:1").unwrap();
        assert_eq!(g[0], GresEntry { name: "gpu".into(), gpu_type: Some("a100".into()), count: 2 });
        assert_eq!(parse_gpus("h100:4").unwrap(), (Some("h100".into()), 4));
    }

    #[test]
    fn cli_filter_table() {
        let o: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"cpus-per-task":"0","mem":"150","gres":"gres/gpu:a100:1","parsable":"set","wrap":"true","argv":[]}"#).unwrap();
        let j = from_cli_options(&o, 0, None);
        let c = &j.components[0];
        assert!(!c.has("cpus-per-task"));
        assert_eq!(c.value("mem"), Some("150"));
        assert!(c.has("parsable") && c.value("parsable").is_none());
        let r = CompResources::of(c);
        assert_eq!(r.gpu.unwrap().gpu_type.as_deref(), Some("a100"));
        assert!(j.script.unwrap().ends_with("\ntrue\n"));
    }
}
