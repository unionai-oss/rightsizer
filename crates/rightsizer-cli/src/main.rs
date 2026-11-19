//! `rsz`: the rightsizer CLI (SPEC §12).
//!
//! M0 commands: `recommend`, `explain`, `shadow`, `history import jsonl |
//! show | export | compact`, `outcome`, `breaker`, `control`, `config`.
//! Other commands of SPEC §12.1 are declared and exit 1 with "not
//! implemented yet". Output is `text` (default), `json` or `yaml`.
//!
//! Exit codes (SPEC §12.1): 0 success (including "kept the prior"), 2
//! configuration error, 3 input parse error, 4 a wrapped submitter failed,
//! 1 anything else.

mod adapters;
mod daemon;
mod flyte;
#[cfg(feature = "k8s")]
mod k8s;
mod output;
mod plugin_cmd;
mod ray;
#[cfg(feature = "slurm")]
mod slurm_cmd;

use std::io::Read as _;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};
use rightsizer_core::breaker::BreakerScope;
use rightsizer_core::{
    Bytes, Confidence, Measurement, Mode, OutcomeSource, RunRecord, RunStatus, Trust, WorkloadContext, WorkloadKey,
};
use rightsizer_engine::store::Role;
use rightsizer_engine::{Config, ConfigEnv, DecideRequest, Engine, ReplayOptions};
use rightsizer_history::CompactOptions;

#[derive(Parser, Debug)]
#[command(name = "rsz", version, about = "Right-size AI workloads before they run")]
struct Cli {
    /// Output format.
    #[arg(long, short = 'o', global = true, value_enum, default_value_t = Format::Text)]
    output: Format,
    /// Configuration file (instead of the search order of SPEC §12.1).
    #[arg(long, global = true, env = "RIGHTSIZER_CONFIG")]
    config: Option<PathBuf>,
    /// History store URI (overrides `history.uri`).
    #[arg(long, global = true)]
    store: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub(crate) enum Format {
    Text,
    Json,
    Yaml,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum ReportFormat {
    Text,
    Json,
    Html,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Decisions for a submission (mode = recommend).
    Recommend {
        /// Platform adapter (`k8s`, `slurm`); default: the file is a JSON
        /// `DecideRequest` (or a list of them) or a bare `WorkloadContext`.
        #[arg(short, long)]
        platform: Option<String>,
        /// Decide but do not record the decision.
        #[arg(long)]
        dry_run: bool,
        /// File or `-`.
        file: String,
    },
    /// Rewrite a submission (mode = enforce under SPEC §2.3).
    Apply {
        /// File or `-`.
        file: String,
        /// Output file.
        #[arg(long = "out", short = 'f')]
        out: Option<PathBuf>,
        /// Show a diff.
        #[arg(long)]
        diff: bool,
        /// Resubmit on OOM (Ladder).
        #[arg(long)]
        watch: bool,
    },
    /// Distributions, cost table, constraints, holds, advisories.
    Explain {
        /// Platform adapter.
        #[arg(short, long)]
        platform: Option<String>,
        /// File or `-`.
        file: Option<String>,
        /// A logged decision.
        #[arg(long)]
        decision: Option<String>,
    },
    /// Savings / would-fail / readiness report (SPEC §12.2).
    Shadow {
        /// Tenant `{platform}://{tenant}`.
        #[arg(long, short = 'n')]
        tenant: Option<String>,
        /// One key.
        #[arg(long)]
        key: Option<String>,
        /// Window replayed (runs before it are history only).
        #[arg(long, default_value = "14d")]
        since: String,
        /// Report format (overrides `-o`).
        #[arg(long, value_enum)]
        format: Option<ReportFormat>,
        /// Include per-run rows (JSON).
        #[arg(long)]
        per_run: bool,
    },
    /// Import, show, export or compact history.
    History {
        #[command(subcommand)]
        action: HistoryCmd,
    },
    /// Read-only waste and risk report (SPEC §12.3).
    Audit {
        platform: String,
        #[arg(long, default_value = "30d")]
        since: String,
    },
    /// Resource edits to code and config (SPEC §12.4).
    Suggest {
        paths: Vec<PathBuf>,
        #[arg(long)]
        write: bool,
        #[arg(long)]
        pr: bool,
    },
    /// MCP server for coding agents (SPEC §12.5).
    Mcp,
    /// Manual outcome record.
    Outcome {
        /// Workload key.
        key: String,
        /// Status (`succeeded`, `oom_killed`, `gpu_oom`, `evicted`, `timeout`, `preempted`, `failed`, `unknown`).
        #[arg(long)]
        status: String,
        /// Measured memory peak (`3Gi`, `1500Mi`).
        #[arg(long)]
        memory_peak: Option<String>,
        /// The decision that sized the run.
        #[arg(long)]
        decision: Option<String>,
        /// Invocation id (default: a new ULID).
        #[arg(long)]
        invocation: Option<String>,
        /// Attempt.
        #[arg(long, default_value_t = 1)]
        attempt: u32,
        /// Duration (`10m`, `2h`).
        #[arg(long)]
        duration: Option<String>,
    },
    /// Breakers.
    Breaker {
        #[command(subcommand)]
        action: BreakerCmd,
    },
    /// Fleet control.
    Control {
        #[command(subcommand)]
        action: ControlCmd,
    },
    /// Daemon (plugins.md §3).
    Serve {
        #[arg(long)]
        socket: Option<PathBuf>,
        #[arg(long)]
        listen: Option<String>,
    },
    /// Configuration.
    Config {
        #[command(subcommand)]
        action: ConfigCmd,
    },
    /// Plugins: list | check NAME.
    Plugin {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Platform subcommands (`rsz k8s …`, `rsz slurm …`, `rsz flyte …`, …).
    #[command(external_subcommand)]
    Platform(Vec<String>),
}

#[derive(Subcommand, Debug)]
enum HistoryCmd {
    /// Import run records: `jsonl FILE` (one RunRecord per line), or a platform importer.
    Import {
        /// Source: `jsonl`, or a platform (`k8s`, `slurm`, `flyte`, …).
        source: String,
        /// Source arguments (for `jsonl`: the file, or `-`; importers take flags, e.g. `--since 30d`).
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Show a key's history, newest first.
    Show {
        key: String,
        #[arg(long, default_value_t = 32)]
        limit: usize,
    },
    /// Export run records (and decisions, for Parquet).
    Export {
        /// Parquet (the tuner's corpus columns).
        #[arg(long, conflicts_with = "jsonl")]
        parquet: bool,
        /// One RunRecord per line.
        #[arg(long)]
        jsonl: bool,
        /// Output file.
        out: PathBuf,
    },
    /// Compaction (a singleton job).
    Compact {
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Subcommand, Debug)]
enum BreakerCmd {
    /// Every breaker with a trip.
    Status,
    /// Trip a breaker by hand.
    Trip {
        /// The fleet breaker.
        #[arg(long, conflicts_with = "key")]
        fleet: bool,
        /// A workload key.
        key: Option<String>,
        /// Why.
        #[arg(long)]
        reason: Option<String>,
    },
    /// Reset a breaker (operator credentials); without KEY, the fleet breaker.
    Reset { key: Option<String> },
}

#[derive(Subcommand, Debug)]
enum ControlCmd {
    /// Show, or set, the fleet `mode_ceiling`.
    ModeCeiling { mode: Option<Mode> },
}

#[derive(Subcommand, Debug)]
enum ConfigCmd {
    /// Print the effective configuration.
    Show,
    /// Validate the configuration (exit 2 on error).
    Validate,
}

/// A CLI failure with its exit code.
pub(crate) struct Fail {
    code: u8,
    msg: String,
}

impl Fail {
    pub(crate) fn config(m: impl std::fmt::Display) -> Self {
        Fail { code: 2, msg: format!("config error: {m}") }
    }
    pub(crate) fn parse(m: impl std::fmt::Display) -> Self {
        Fail { code: 3, msg: format!("input error: {m}") }
    }
    pub(crate) fn other(m: impl std::fmt::Display) -> Self {
        Fail { code: 1, msg: m.to_string() }
    }
}

type Res = Result<(), Fail>;

fn main() -> ExitCode {
    // `rsz slurm decide-client` is on every sbatch's path (cli_filter.lua):
    // no config, no runtime.
    #[cfg(feature = "slurm")]
    {
        let a: Vec<String> = std::env::args().skip(1).collect();
        if a.len() >= 2 && a[0] == "slurm" && a[1] == "decide-client" {
            return ExitCode::from(slurm_cmd::decide_client_main(&a[2..]) as u8);
        }
    }
    let cli = Cli::parse_from(hoist_global_flags(std::env::args().collect()));
    tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::from_env("RSZ_LOG")).with_writer(std::io::stderr).init();
    let rt = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("rsz: {e}");
            return ExitCode::from(1);
        }
    };
    match rt.block_on(run(cli)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(f) => {
            eprintln!("rsz: {}", f.msg);
            ExitCode::from(f.code)
        }
    }
}

/// `history import <source> ARGS…` passes hyphenated ARGS (`--since 30d`)
/// through to the importer, so clap would swallow global flags given after
/// them (`… -o json`). Move `-o/--output`, `--config` and `--store` that
/// follow `history import` in front of the subcommand.
fn hoist_global_flags(args: Vec<String>) -> Vec<String> {
    let Some(h) = args.iter().position(|a| a == "history") else { return args };
    if args.get(h + 1).map(String::as_str) != Some("import") {
        return args;
    }
    let (mut head, mut tail): (Vec<String>, Vec<String>) = (args[..h].to_vec(), Vec::new());
    let rest = &args[h..];
    let mut i = 0;
    while i < rest.len() {
        let a = &rest[i];
        let takes_value = matches!(a.as_str(), "-o" | "--output" | "--config" | "--store");
        let inline = ["--output=", "--config=", "--store="].iter().any(|p| a.starts_with(p)) || (a.starts_with("-o") && a.len() > 2 && !a.starts_with("--"));
        if i >= 3 && takes_value && i + 1 < rest.len() {
            head.push(a.clone());
            head.push(rest[i + 1].clone());
            i += 2;
            continue;
        }
        if i >= 3 && inline {
            head.push(a.clone());
        } else {
            tail.push(a.clone());
        }
        i += 1;
    }
    head.extend(tail);
    head
}

fn load_config(cli: &Cli) -> Result<rightsizer_engine::Loaded, Fail> {
    let mut env = ConfigEnv::from_process();
    if let Some(p) = &cli.config {
        env.vars.insert(rightsizer_engine::config::CONFIG_ENV.into(), p.display().to_string());
    }
    let mut loaded = Config::load_with(&env).map_err(Fail::config)?;
    if let Some(s) = &cli.store {
        loaded.config.history.uri = Some(s.clone());
    }
    Ok(loaded)
}

async fn engine(config: &Config, role: Role) -> Result<Engine, Fail> {
    let e = Engine::builder(config.clone()).role(role).build().await.map_err(Fail::config)?;
    if let Some(err) = e.config_error() {
        eprintln!("rsz: warning: {err}; decisions keep the prior (S1; set strict = true to fail)");
    }
    Ok(e)
}

async fn run(cli: Cli) -> Res {
    let out = cli.output;
    if let Command::Config { action } = &cli.command {
        return config_cmd(&cli, action, out);
    }
    let loaded = load_config(&cli)?;
    let cfg = loaded.config;
    match cli.command {
        Command::Recommend { platform, dry_run, file } => recommend(&cfg, platform, &file, dry_run, out, false).await,
        Command::Explain { platform, file, decision } => match (file, decision) {
            (_, Some(id)) => explain_logged(&cfg, &id, out).await,
            (Some(f), None) => recommend(&cfg, platform, &f, true, out, true).await,
            (None, None) => Err(Fail::parse("explain needs FILE or --decision ID")),
        },
        Command::Shadow { tenant, key, since, format, per_run } => shadow(&cfg, tenant, key, &since, format, per_run, out).await,
        Command::History { action } => history(&cfg, action, out).await,
        Command::Outcome { key, status, memory_peak, decision, invocation, attempt, duration } => {
            outcome(&cfg, &key, &status, memory_peak, decision, invocation, attempt, duration, out).await
        }
        Command::Breaker { action } => breaker(&cfg, action, out).await,
        Command::Control { action } => control(&cfg, action, out).await,
        Command::Config { .. } => unreachable!(),
        #[cfg(feature = "k8s")]
        Command::Platform(args) if k8s::handles(&args) => k8s::platform(&cfg, args, out).await,
        #[cfg(feature = "slurm")]
        Command::Platform(args) if slurm_cmd::handles(&args) => slurm_cmd::platform(&cfg, args, out).await,
        Command::Platform(args) if ray::handles(&args) => ray::platform(&cfg, cli.config.as_deref(), cli.store.as_deref(), args),
        Command::Platform(args) if flyte::handles(&args) => flyte::platform(&cfg, cli.config.as_deref(), cli.store.as_deref(), args),
        Command::Serve { socket, listen } => daemon::serve(&cfg, socket, listen).await,
        Command::Plugin { args } => plugin_cmd::run(&cfg, args, out).await,
        #[cfg(feature = "k8s")]
        Command::Apply { file, out: out_path, diff, watch } => k8s::apply(&cfg, &file, out_path, diff, watch, out).await,
        other => Err(Fail::other(format!("{} is not implemented yet", name(&other)))),
    }
}

fn name(c: &Command) -> String {
    match c {
        Command::Platform(args) => args.first().cloned().unwrap_or_default(),
        other => format!("{other:?}").split([' ', '{', '(']).next().unwrap_or_default().to_lowercase(),
    }
}

fn config_cmd(cli: &Cli, action: &ConfigCmd, out: Format) -> Res {
    let loaded = load_config(cli)?;
    match action {
        ConfigCmd::Validate => {
            loaded.config.validate().map_err(Fail::config)?;
            match &loaded.source {
                Some(p) => println!("ok: {}", p.display()),
                None => println!("ok: no config file (defaults)"),
            }
            Ok(())
        }
        ConfigCmd::Show => {
            let text = match out {
                Format::Text => loaded.config.to_toml(),
                Format::Json => serde_json::to_string_pretty(&loaded.config).unwrap_or_default(),
                Format::Yaml => serde_norway::to_string(&loaded.config).unwrap_or_default(),
            };
            if let Some(p) = &loaded.source {
                eprintln!("# from {}", p.display());
            }
            for o in &loaded.overrides {
                eprintln!("# override {o}");
            }
            print!("{text}");
            Ok(())
        }
    }
}

fn read_input(file: &str) -> Result<Vec<u8>, Fail> {
    if file == "-" {
        let mut b = Vec::new();
        std::io::stdin().read_to_end(&mut b).map_err(|e| Fail::parse(format!("stdin: {e}")))?;
        Ok(b)
    } else {
        std::fs::read(file).map_err(|e| Fail::parse(format!("{file}: {e}")))
    }
}

/// `DecideRequest`, a list of them, or a bare `WorkloadContext` (JSON or YAML).
fn parse_requests(bytes: &[u8]) -> Result<Vec<DecideRequest>, Fail> {
    let v: serde_json::Value = match serde_json::from_slice(bytes) {
        Ok(v) => v,
        Err(je) => serde_norway::from_slice(bytes).map_err(|ye| Fail::parse(format!("not JSON ({je}) nor YAML ({ye})")))?,
    };
    let one = |v: serde_json::Value| -> Result<DecideRequest, Fail> {
        if v.get("context").is_some() {
            serde_json::from_value(v).map_err(|e| Fail::parse(format!("DecideRequest: {e}")))
        } else {
            let ctx: WorkloadContext = serde_json::from_value(v).map_err(|e| Fail::parse(format!("WorkloadContext: {e}")))?;
            Ok(DecideRequest::for_context(ctx))
        }
    };
    match v {
        serde_json::Value::Array(xs) => xs.into_iter().map(one).collect(),
        v => Ok(vec![one(v)?]),
    }
}

async fn recommend(cfg: &Config, platform: Option<String>, file: &str, dry_run: bool, out: Format, explain: bool) -> Res {
    let bytes = read_input(file)?;
    let groups: Vec<Vec<DecideRequest>> = match platform.as_deref() {
        None | Some("json") => parse_requests(&bytes)?.into_iter().map(|r| vec![r]).collect(),
        Some(p) => adapters::extract(p, file, bytes, cfg).await?,
    };
    let e = engine(cfg, Role::Client).await?;
    let mut decisions = Vec::new();
    for mut g in groups {
        for r in &mut g {
            r.policy.mode = Some(Mode::Recommend);
            r.policy.max_mode = Some(Mode::Recommend);
            r.dry_run |= dry_run;
        }
        if g.len() == 1 {
            decisions.push(e.decide(g.remove(0)).await);
        } else {
            decisions.extend(e.decide_group(g).await);
        }
    }
    output::decisions(&decisions, out, explain);
    Ok(())
}

async fn explain_logged(cfg: &Config, id: &str, out: Format) -> Res {
    let id: ulid::Ulid = id.parse().map_err(|e| Fail::parse(format!("decision id {id:?}: {e}")))?;
    let e = engine(cfg, Role::Client).await?;
    match e.store().decision(id).await.map_err(Fail::other)? {
        Some(d) => {
            output::decisions(&[d], out, true);
            Ok(())
        }
        None => Err(Fail::other(format!("decision {id} not found"))),
    }
}

fn parse_since(s: &str) -> Result<std::time::Duration, Fail> {
    humantime::parse_duration(s).map_err(|e| Fail::parse(format!("--since {s:?}: {e}")))
}

fn parse_key(k: &str) -> Result<WorkloadKey, Fail> {
    WorkloadKey::parse(k).map_err(Fail::parse)
}

#[allow(clippy::too_many_arguments)]
async fn shadow(cfg: &Config, tenant: Option<String>, key: Option<String>, since: &str, format: Option<ReportFormat>, per_run: bool, out: Format) -> Res {
    let window = parse_since(since)?;
    let e = engine(cfg, Role::Client).await?;
    let keys: Vec<WorkloadKey> = match (&key, &tenant) {
        (Some(k), _) => vec![parse_key(k)?],
        (None, t) => {
            let tenant_of = |k: &WorkloadKey| format!("{}://{}", k.platform, k.tenant);
            let mut ks: Vec<WorkloadKey> = e.keys().await.map_err(Fail::other)?;
            if let Some(t) = t {
                let t = t.trim_end_matches('/');
                ks.retain(|k| tenant_of(k) == t || tenant_of(k).starts_with(&format!("{t}/")));
            }
            ks
        }
    };
    let mut records = Vec::new();
    for k in &keys {
        records.extend(e.store().runs(k, 100_000).await.map_err(Fail::other)?);
    }
    let now = e.now();
    let opts = ReplayOptions { since: Some(now - chrono::Duration::from_std(window).unwrap_or_default()), per_run, ..Default::default() };
    let report = e.replay(records, &opts).await;
    let fmt = format.unwrap_or(match out {
        Format::Json => ReportFormat::Json,
        _ => ReportFormat::Text,
    });
    match (fmt, out) {
        (ReportFormat::Html, _) => print!("{}", rightsizer_engine::report::html(&report, "rsz shadow")),
        (ReportFormat::Json, _) => println!("{}", serde_json::to_string_pretty(&report).unwrap_or_default()),
        (ReportFormat::Text, Format::Yaml) => print!("{}", serde_norway::to_string(&report).unwrap_or_default()),
        (ReportFormat::Text, _) => print!("{}", rightsizer_engine::report::text(&report)),
    }
    Ok(())
}

async fn history(cfg: &Config, action: HistoryCmd, out: Format) -> Res {
    match action {
        #[cfg(feature = "k8s")]
        HistoryCmd::Import { source, args } if source == "k8s" || source == "kubernetes" => k8s::import(cfg, args, out).await,
        #[cfg(feature = "slurm")]
        HistoryCmd::Import { source, args } if source == "slurm" => slurm_cmd::import(cfg, args, out).await,
        HistoryCmd::Import { source, args } if source == "flyte" => flyte::import(cfg, args),
        HistoryCmd::Import { source, args } => {
            if source != "jsonl" {
                // TODO(platform importers): `rsz history import k8s|slurm|…` dispatch to each adapter's OutcomeSource.
                return Err(Fail::other(format!("history import {source}: importer not available in this build yet (use `jsonl`)")));
            }
            let file = args.first().ok_or_else(|| Fail::parse("history import jsonl FILE"))?;
            let text = String::from_utf8(read_input(file)?).map_err(|e| Fail::parse(e.to_string()))?;
            let mut recs = Vec::new();
            for (i, line) in text.lines().enumerate().filter(|(_, l)| !l.trim().is_empty()) {
                let r: RunRecord = serde_json::from_str(line).map_err(|e| Fail::parse(format!("{file}:{}: {e}", i + 1)))?;
                recs.push(r);
            }
            let e = engine(cfg, Role::Client).await?;
            let (mut inserted, mut dup) = (0, 0);
            let mut imp = rightsizer_history::Importer::new();
            // A file can claim any trust: cap it like `outcome` does (SPEC §8.2).
            for r in recs.iter_mut() {
                e.cap_trust(r);
            }
            for r in &recs {
                match imp.put_run(e.store().as_ref(), r).await.map_err(Fail::other)? {
                    rightsizer_history::PutOutcome::Inserted => inserted += 1,
                    rightsizer_history::PutOutcome::Duplicate => dup += 1,
                }
            }
            output::value(&serde_json::json!({"read": recs.len(), "inserted": inserted, "duplicates": dup}), out, || {
                format!("imported {inserted} run records ({dup} duplicates) from {file}\n")
            });
            Ok(())
        }
        HistoryCmd::Show { key, limit } => {
            let k = parse_key(&key)?;
            let e = engine(cfg, Role::Client).await?;
            let runs = e.store().runs(&k, limit).await.map_err(Fail::other)?;
            output::runs(&runs, out);
            Ok(())
        }
        HistoryCmd::Export { parquet, jsonl, out: path } => {
            let e = engine(cfg, Role::Client).await?;
            if parquet {
                let s = rightsizer_history::export::export_parquet(e.store().as_ref(), &rightsizer_history::export::ExportFilter::all(), &path)
                    .await
                    .map_err(Fail::other)?;
                eprintln!("wrote {} runs and {} decisions to {} ({} bytes)", s.runs, s.decisions, path.display(), s.bytes);
                return Ok(());
            }
            if !jsonl {
                return Err(Fail::parse("history export needs --parquet or --jsonl"));
            }
            let keys = e.keys().await.map_err(Fail::other)?;
            let mut buf = String::new();
            let mut n = 0;
            for k in keys {
                for r in e.store().runs(&k, usize::MAX).await.map_err(Fail::other)? {
                    buf.push_str(&serde_json::to_string(&r).map_err(Fail::other)?);
                    buf.push('\n');
                    n += 1;
                }
            }
            std::fs::write(&path, buf).map_err(Fail::other)?;
            eprintln!("wrote {n} run records to {}", path.display());
            Ok(())
        }
        HistoryCmd::Compact { dry_run } => {
            let e = engine(cfg, Role::Operator).await?;
            let mut o = CompactOptions::at(e.now());
            o.keep_last = cfg.history.keep_last;
            o.max_age = cfg.history.max_age;
            o.dry_run = dry_run;
            let r = e.store().compact(&o).await.map_err(Fail::other)?;
            // The unclassified-failure sweep: ladder leases that expired open are unrecovered failures.
            let sw = e.sweep(dry_run).await.map_err(Fail::other)?;
            let tripped: Vec<String> = sw.tripped.iter().map(|s| s.to_string()).collect();
            output::value(
                &serde_json::json!({"pointers": r.pointers, "snapshots": r.snapshots, "deleted": r.deleted, "dry_run": dry_run,
                    "sweep": {"keys": sw.keys, "expired_leases": sw.expired_leases, "not_caused": sw.not_caused, "tripped": tripped, "leases_closed": sw.leases_closed}}),
                out,
                || {
                    let dr = if dry_run { " (dry run)" } else { "" };
                    let mut t = format!("compaction: {} pointers, {} snapshots, {} deleted{dr}\n", r.pointers, r.snapshots, r.deleted);
                    t.push_str(&format!("sweep: {} keys, {} leases expired open, {} breakers tripped{dr}\n", sw.keys, sw.expired_leases, sw.tripped.len()));
                    for s in &tripped {
                        t.push_str(&format!("  tripped: {s}\n"));
                    }
                    t
                },
            );
            Ok(())
        }
    }
}

fn parse_status(s: &str) -> Result<RunStatus, Fail> {
    serde_json::from_value(serde_json::Value::String(s.to_lowercase().replace('-', "_"))).map_err(|_| Fail::parse(format!("unknown status {s:?}")))
}

#[allow(clippy::too_many_arguments)]
async fn outcome(cfg: &Config, key: &str, status: &str, memory_peak: Option<String>, decision: Option<String>, invocation: Option<String>, attempt: u32, duration: Option<String>, out: Format) -> Res {
    let k = parse_key(key)?;
    let status = parse_status(status)?;
    let peak = memory_peak
        .map(|q| rightsizer_core::units::parse_k8s_bytes(&q).map_err(|e| Fail::parse(format!("--memory-peak {q:?}: {e}"))))
        .transpose()?;
    let decision_id = decision.map(|d| d.parse::<ulid::Ulid>().map_err(|e| Fail::parse(format!("--decision: {e}")))).transpose()?;
    let duration_s = duration.map(|d| parse_since(&d).map(|x| x.as_secs_f64())).transpose()?;
    let e = engine(cfg, Role::Client).await?;
    let requested = match decision_id {
        Some(id) => e.store().decision(id).await.ok().flatten().map(|d| d.applied),
        None => None,
    };
    let rec = RunRecord {
        v: rightsizer_core::WIRE_VERSION,
        key: k,
        cluster: cfg.history.cluster.clone(),
        invocation_id: invocation.unwrap_or_else(|| ulid::Ulid::generate().to_string()),
        attempt,
        decision_id,
        started_at: e.now() - chrono::Duration::milliseconds((duration_s.unwrap_or(0.0) * 1000.0) as i64),
        inputs: vec![],
        code_digest: None,
        requested,
        origin_prior: None,
        status,
        status_confidence: Confidence::Observed,
        memory_peak: peak.map(|b: Bytes| Measurement::exact(b.0 as f64)),
        cpu_mean: None,
        cpu_peak_window: None,
        cpu_throttled_frac: None,
        gpu_memory_peak: None,
        gpu_util_mean: None,
        t_peak_frac: None,
        duration_s,
        staged_bytes: None,
        source: OutcomeSource::SelfReport,
        trust: Trust::Unverified,
    };
    let rep = e.outcome(rec.clone()).await.map_err(Fail::other)?;
    let v = serde_json::json!({
        "record": rec,
        "written": rep.written,
        "joined": rep.joined,
        "attribution": rep.attribution,
        "tripped": rep.tripped,
        "lease_closed": rep.lease_closed,
    });
    output::value(&v, out, || {
        let mut s = format!("recorded {} attempt {} of {}: {}\n", rec.invocation_id, rec.attempt, rec.key, serde_json::to_value(rec.status).ok().and_then(|v| v.as_str().map(String::from)).unwrap_or_default());
        if let Some(a) = &rep.attribution {
            s.push_str(&format!("attribution: {a}\n"));
        }
        for t in &rep.tripped {
            s.push_str(&format!("tripped breaker: {t}\n"));
        }
        if !rep.written {
            s.push_str("store unreachable: buffered (lost when this process exits)\n");
        }
        s
    });
    Ok(())
}

async fn breaker(cfg: &Config, action: BreakerCmd, out: Format) -> Res {
    match action {
        BreakerCmd::Status => {
            let e = engine(cfg, Role::Client).await?;
            let st = e.breaker_status().await.map_err(Fail::other)?;
            output::breakers(&st, out);
        }
        BreakerCmd::Trip { fleet, key, reason } => {
            let scope = match (fleet, key) {
                (true, _) => BreakerScope::Fleet,
                (false, Some(k)) => BreakerScope::Workload { key: parse_key(&k)? },
                (false, None) => return Err(Fail::parse("breaker trip needs --fleet or KEY")),
            };
            let e = engine(cfg, Role::Client).await?;
            let t = e.breaker_trip(scope, reason).await.map_err(Fail::other)?;
            output::value(&serde_json::to_value(&t).unwrap_or_default(), out, || format!("tripped {}\n", t.scope));
        }
        BreakerCmd::Reset { key } => {
            let scope = match key {
                Some(k) => BreakerScope::Workload { key: parse_key(&k)? },
                None => BreakerScope::Fleet,
            };
            let e = engine(cfg, Role::Operator).await?;
            let r = e.breaker_reset(scope, std::env::var("USER").ok()).await.map_err(Fail::other)?;
            output::value(&serde_json::to_value(&r).unwrap_or_default(), out, || format!("reset {}\n", r.scope));
        }
    }
    Ok(())
}

async fn control(cfg: &Config, action: ControlCmd, out: Format) -> Res {
    let ControlCmd::ModeCeiling { mode } = action;
    match mode {
        None => {
            let e = engine(cfg, Role::Client).await?;
            let c = e.control_get().await.map_err(Fail::other)?;
            let m = c.as_ref().map(|c| c.mode_ceiling).unwrap_or(Mode::Enforce);
            output::value(&serde_json::json!({"mode_ceiling": m, "control": c}), out, || {
                format!("mode_ceiling: {m}{}\n", if c.is_none() { " (not set)" } else { "" })
            });
        }
        Some(m) => {
            let e = engine(cfg, Role::Operator).await?;
            let c = e.control_set(m, std::env::var("USER").ok()).await.map_err(Fail::other)?;
            output::value(&serde_json::to_value(&c).unwrap_or_default(), out, || format!("mode_ceiling set to {m} (clients see it within 60 s)\n"));
        }
    }
    Ok(())
}
