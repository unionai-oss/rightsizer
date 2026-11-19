//! `rsz slurm …` (slurm.md) and `rsz history import slurm`.
//!
//! ```text
//! rsz slurm sbatch [--watch] [--socket S | --local] [--mode M] [--poll 10s] [--] SBATCH-ARGS...
//! rsz slurm decide-client --socket S [--timeout-ms 1000]      (the helper cli_filter.lua runs)
//! rsz slurm install --cluster C --cli-filter PATH --job-submit PATH [--rsz PATH] [--socket S] [--key-file F] [--disable]
//! rsz slurm keygen --out FILE [--force]                       (0600)
//! rsz slurm rotate-key stage|promote|retire|status --key FILE [--copy-to FILE]...
//! rsz history import slurm [--since 30d] [--sacct PATH] [--cluster C] [--dry-run]
//! ```
//!
//! `rsz slurm sbatch` decides in-process (engine + the daemon's key ring)
//! when the configured `[slurm] token_key_file` is readable by the caller,
//! i.e. when run as the daemon user (`--local` forces it), and otherwise
//! asks the daemon over its socket (`--socket`, `[serve] socket`, else
//! `/run/rightsizer.sock`): users never hold the key.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rightsizer_core::{AttemptRecord, RunStatus, WorkloadKey};
use rightsizer_engine::store::Role;
use rightsizer_engine::{Config, Engine};
use rightsizer_plugin::protocol::{DecideParams, DecideResult, LeaseAction, LeaseParams, SignParams, SignResult, method};
use rightsizer_plugin::{AdapterEnv, Native, PluginError};
use rightsizer_slurm::runner::TokioRunner;
use rightsizer_slurm::sacct::{ImportConfig, SacctSource, ladder_invocation, link_ladders, to_run_records, verified_groups};
use rightsizer_slurm::service::{Identity, SlurmService};
use rightsizer_slurm::token::{KeyRing, RotateStep};
use rightsizer_slurm::wrapper::{self, Decider, WrapperConfig};
use ulid::Ulid;

use crate::{Fail, Format, output};

/// Default daemon socket.
pub const DEFAULT_SOCKET: &str = "/run/rightsizer.sock";

/// Whether `rsz <args>` is ours.
pub fn handles(args: &[String]) -> bool {
    args.first().is_some_and(|a| a == "slurm")
}

fn usage() -> Fail {
    Fail::parse(
        "usage: rsz slurm sbatch [--watch] [--socket S|--local] [--mode M] ARGS... | decide-client --socket S [--timeout-ms N] | \
         install --cluster C --cli-filter PATH --job-submit PATH [--rsz PATH] [--socket S] [--key-file F] [--disable] | \
         keygen --out FILE [--force] | rotate-key stage|promote|retire|status --key FILE [--copy-to FILE]",
    )
}

/// `rsz slurm decide-client`: runs before the tokio runtime and config
/// loading (it is on every `sbatch`'s path). Returns the exit code.
pub fn decide_client_main(args: &[String]) -> i32 {
    let mut socket = PathBuf::from(DEFAULT_SOCKET);
    let mut timeout = Duration::from_millis(1000);
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--socket" => {
                if let Some(s) = args.get(i + 1) {
                    socket = PathBuf::from(s);
                }
                i += 1;
            }
            "--timeout-ms" => {
                if let Some(t) = args.get(i + 1).and_then(|t| t.parse::<u64>().ok()) {
                    timeout = Duration::from_millis(t);
                }
                i += 1;
            }
            _ => {}
        }
        i += 1;
    }
    match rightsizer_slurm::client::run(&socket, timeout, std::io::stdin().lock()) {
        Ok(resp) => {
            use std::io::Write;
            let mut o = std::io::stdout().lock();
            let _ = o.write_all(&resp);
            let _ = o.write_all(b"\n");
            0
        }
        Err(e) => {
            if std::env::var_os("RSZ_LOG").is_some() {
                eprintln!("rsz slurm decide-client: {e}");
            }
            1
        }
    }
}

/// Dispatch `rsz slurm …`.
pub async fn platform(cfg: &Config, args: Vec<String>, out: Format) -> Result<(), Fail> {
    let sub = args.get(1).cloned().ok_or_else(usage)?;
    let rest: Vec<String> = args[2..].to_vec();
    match sub.as_str() {
        "sbatch" => sbatch(cfg, rest).await,
        "decide-client" => match decide_client_main(&rest) {
            0 => Ok(()),
            c => Err(Fail { code: c as u8, msg: "decide-client failed".into() }),
        },
        "install" => install(rest, out),
        "keygen" => keygen(rest, out),
        "rotate-key" => rotate_key(rest, out),
        _ => Err(usage()),
    }
}

fn flag(rest: &[String], name: &str) -> bool {
    rest.iter().any(|a| a == name)
}

fn opt(rest: &[String], name: &str) -> Option<String> {
    rest.iter().position(|a| a == name).and_then(|i| rest.get(i + 1).cloned()).or_else(|| {
        let p = format!("{name}=");
        rest.iter().find_map(|a| a.strip_prefix(&p).map(str::to_string))
    })
}

fn opts(rest: &[String], name: &str) -> Vec<String> {
    let mut v = Vec::new();
    for (i, a) in rest.iter().enumerate() {
        if a == name
            && let Some(x) = rest.get(i + 1)
        {
            v.push(x.clone());
        }
    }
    v
}

// ── install / keys ──────────────────────────────────────────────────────

fn install(rest: Vec<String>, out: Format) -> Result<(), Fail> {
    use rightsizer_slurm::install::{InstallOptions, install};
    let o = InstallOptions {
        cluster: opt(&rest, "--cluster"),
        cli_filter: opt(&rest, "--cli-filter").map(PathBuf::from),
        job_submit: opt(&rest, "--job-submit").map(PathBuf::from),
        rsz: opt(&rest, "--rsz").or_else(|| std::env::current_exe().ok().map(|p| p.display().to_string())),
        socket: opt(&rest, "--socket"),
        key_file: opt(&rest, "--key-file"),
        disable: flag(&rest, "--disable"),
    };
    if o.cli_filter.is_none() && o.job_submit.is_none() {
        return Err(Fail::parse("rsz slurm install needs --cli-filter PATH and/or --job-submit PATH"));
    }
    let r = install(&o).map_err(Fail::other)?;
    let v = serde_json::json!({"written": r.written, "removed": r.removed, "slurm_conf": r.slurm_conf, "header": r.header, "disabled": o.disable});
    output::value(&v, out, || {
        let mut s = String::new();
        for p in &r.written {
            s.push_str(&format!("wrote {}\n", p.display()));
        }
        for p in &r.removed {
            s.push_str(&format!("removed {}\n", p.display()));
        }
        if o.disable {
            s.push_str("rightsizer is DISABLED: both scripts short-circuit (submissions unchanged, no admin_comment).\n");
        }
        s.push_str("add to slurm.conf (then `scontrol reconfigure`):\n");
        for l in &r.slurm_conf {
            s.push_str(&format!("  {l}\n"));
        }
        s
    });
    Ok(())
}

fn keygen(rest: Vec<String>, out: Format) -> Result<(), Fail> {
    let path = PathBuf::from(opt(&rest, "--out").ok_or_else(|| Fail::parse("rsz slurm keygen --out FILE"))?);
    if path.exists() && !flag(&rest, "--force") {
        return Err(Fail::other(format!("{} exists (use `rsz slurm rotate-key` to rotate, or --force)", path.display())));
    }
    if path.exists() {
        std::fs::remove_file(&path).map_err(Fail::other)?;
    }
    let ring = KeyRing::generate().map_err(Fail::other)?;
    ring.save(&path).map_err(Fail::other)?;
    let kid = ring.active().map(|k| k.kid.clone()).unwrap_or_default();
    output::value(&serde_json::json!({"path": path, "kid": kid, "mode": "0600"}), out, || {
        format!("wrote {} (mode 0600, active key {kid}); give job_submit.lua a SlurmUser-only copy\n", path.display())
    });
    Ok(())
}

fn ring_summary(r: &KeyRing) -> Vec<serde_json::Value> {
    r.keys.iter().map(|k| serde_json::json!({"kid": k.kid, "state": k.state, "created": k.created})).collect()
}

fn rotate_key(rest: Vec<String>, out: Format) -> Result<(), Fail> {
    let step = rest.first().cloned().ok_or_else(|| Fail::parse("rsz slurm rotate-key stage|promote|retire|status --key FILE"))?;
    let path = PathBuf::from(opt(&rest, "--key").ok_or_else(|| Fail::parse("rotate-key needs --key FILE"))?);
    let mut ring = KeyRing::load(&path).map_err(Fail::other)?;
    let step = match step.as_str() {
        "stage" => Some(RotateStep::Stage),
        "promote" => Some(RotateStep::Promote),
        "retire" => Some(RotateStep::Retire),
        "status" => None,
        _ => return Err(Fail::parse("rotate-key step must be stage, promote, retire or status")),
    };
    // Replace a key file atomically, keeping its owner and group (the
    // daemon's copy belongs to the service user, job_submit.lua's to SlurmUser).
    let replace = |p: &Path, ring: &KeyRing| -> Result<(), Fail> {
        use std::os::unix::fs::MetadataExt;
        let owner = std::fs::metadata(p).ok().map(|m| (m.uid(), m.gid()));
        if p.exists() {
            std::fs::remove_file(p).map_err(Fail::other)?;
        }
        ring.save(p).map_err(Fail::other)?;
        if let Some((u, g)) = owner {
            let _ = std::os::unix::fs::chown(p, Some(u), Some(g));
        }
        Ok(())
    };
    if let Some(s) = step {
        ring.rotate(s).map_err(Fail::other)?;
        replace(&path, &ring)?;
    }
    let mut copies = Vec::new();
    if step.is_some() {
        for c in opts(&rest, "--copy-to") {
            let p = PathBuf::from(&c);
            replace(&p, &ring)?;
            copies.push(p);
        }
    }
    let v = serde_json::json!({"key_file": path, "keys": ring_summary(&ring), "copies": copies});
    output::value(&v, out, || {
        let mut s = String::new();
        for k in &ring.keys {
            s.push_str(&format!("{} {:?} (created {})\n", k.kid, k.state, k.created));
        }
        for c in &copies {
            s.push_str(&format!("copied to {}\n", c.display()));
        }
        if step.is_some() {
            s.push_str("the daemon re-reads the key file on change; touch job_submit.lua so slurmctld reloads its copy\n");
        }
        s
    });
    Ok(())
}

// ── deciders ────────────────────────────────────────────────────────────

/// In-process: the engine and the daemon's key ring (the daemon user).
pub struct EngineDecider {
    engine: Engine,
    svc: Arc<SlurmService>,
    who: Identity,
    tenants: Vec<String>,
}

impl EngineDecider {
    fn allows(&self) -> impl Fn(&str) -> bool + Sync + '_ {
        move |f: &str| rightsizer_slurm::service::tenant_matches_any(&self.tenants, f)
    }
}

#[async_trait]
impl Decider for EngineDecider {
    async fn decide(&self, params: DecideParams) -> rightsizer_plugin::Result<DecideResult> {
        let a = self.allows();
        self.svc.decide(&self.engine, params, &self.who, &a).await.map_err(|e| PluginError::Internal(e.to_string()))
    }
    async fn open_lease(&self, record: &AttemptRecord) -> rightsizer_plugin::Result<()> {
        self.engine.store().attempt_put_if_absent(record).await.map(|_| ()).map_err(|e| PluginError::PlatformUnavailable(e.to_string()))
    }
    async fn close_lease(&self, key: &WorkloadKey, invocation_id: &str, status: RunStatus) -> rightsizer_plugin::Result<()> {
        self.engine.close_lease(key, invocation_id, status).await.map_err(|e| PluginError::PlatformUnavailable(e.to_string()))
    }
    async fn sign(&self, native: &Native, decision_ids: &[Ulid]) -> rightsizer_plugin::Result<Option<String>> {
        let a = self.allows();
        let p = SignParams { kind: "slurm".into(), native: native.clone(), decision_ids: decision_ids.to_vec() };
        self.svc.sign(&self.engine, p, &self.who, &a).await.map(|r| r.token).map_err(|e| PluginError::Internal(e.to_string()))
    }
}

/// Over the daemon's socket (a user).
pub struct SocketDecider {
    socket: PathBuf,
    timeout: Duration,
}

impl SocketDecider {
    async fn call<P: serde::Serialize, R: serde::de::DeserializeOwned>(&self, m: &str, params: &P, timeout: Duration) -> rightsizer_plugin::Result<R> {
        let req = serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": m, "params": params});
        let body = serde_json::to_vec(&req).map_err(|e| PluginError::Internal(e.to_string()))?;
        let sock = self.socket.clone();
        let resp = tokio::task::spawn_blocking(move || rightsizer_slurm::client::exchange(&sock, timeout, &body))
            .await
            .map_err(|e| PluginError::Internal(e.to_string()))?
            .map_err(|e| PluginError::PlatformUnavailable(format!("daemon at {}: {e}", self.socket.display())))?;
        let v: serde_json::Value = serde_json::from_slice(&resp).map_err(|e| PluginError::Internal(e.to_string()))?;
        if let Some(e) = v.get("error").filter(|e| !e.is_null()) {
            return Err(PluginError::PlatformUnavailable(e.get("message").and_then(|m| m.as_str()).unwrap_or("error").to_string()));
        }
        serde_json::from_value(v.get("result").cloned().unwrap_or_default()).map_err(|e| PluginError::Internal(e.to_string()))
    }
}

#[async_trait]
impl Decider for SocketDecider {
    async fn decide(&self, params: DecideParams) -> rightsizer_plugin::Result<DecideResult> {
        self.call(method::DECIDE, &params, self.timeout).await
    }
    async fn open_lease(&self, record: &AttemptRecord) -> rightsizer_plugin::Result<()> {
        let p = LeaseParams { action: LeaseAction::Open, record: Some(record.clone()), key: None, invocation_id: None, status: None };
        self.call::<_, serde_json::Value>(method::LEASE, &p, self.timeout).await.map(|_| ())
    }
    async fn close_lease(&self, key: &WorkloadKey, invocation_id: &str, status: RunStatus) -> rightsizer_plugin::Result<()> {
        let p = LeaseParams { action: LeaseAction::Close, record: None, key: Some(key.clone()), invocation_id: Some(invocation_id.into()), status: Some(status) };
        self.call::<_, serde_json::Value>(method::LEASE, &p, self.timeout).await.map(|_| ())
    }
    async fn sign(&self, native: &Native, decision_ids: &[Ulid]) -> rightsizer_plugin::Result<Option<String>> {
        let p = SignParams { kind: "slurm".into(), native: native.clone(), decision_ids: decision_ids.to_vec() };
        self.call::<_, SignResult>(method::SIGN, &p, self.timeout).await.map(|r| r.token)
    }
}

fn readable(p: &Path) -> bool {
    std::fs::File::open(p).is_ok()
}

async fn sbatch(cfg: &Config, rest: Vec<String>) -> Result<(), Fail> {
    // Wrapper options come first; everything from the first other argument on is sbatch's.
    let mut watch = false;
    let mut socket: Option<PathBuf> = None;
    let mut local = false;
    let mut mode = None;
    let mut poll = Duration::from_secs(10);
    let mut i = 0;
    while i < rest.len() {
        match rest[i].as_str() {
            "--watch" => watch = true,
            "--local" => local = true,
            "--socket" => {
                socket = rest.get(i + 1).map(PathBuf::from);
                i += 1;
            }
            "--mode" => {
                mode = rest.get(i + 1).and_then(|m| serde_json::from_value(serde_json::Value::String(m.clone())).ok());
                i += 1;
            }
            "--poll" => {
                poll = rest.get(i + 1).and_then(|p| humantime::parse_duration(p).ok()).unwrap_or(poll);
                i += 1;
            }
            "--" => {
                i += 1;
                break;
            }
            _ => break,
        }
        i += 1;
    }
    let args: Vec<String> = rest[i.min(rest.len())..].to_vec();
    let s = &cfg.slurm;
    let mut settings: rightsizer_slurm::SlurmSettings = serde_json::to_value(s).ok().and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default();
    settings.watch = watch;
    let user = std::env::var("USER").ok();
    let wcfg = WrapperConfig {
        sbatch: s.sbatch.clone().unwrap_or_else(|| "sbatch".into()),
        sacct: s.sacct.clone().unwrap_or_else(|| "sacct".into()),
        squeue: s.squeue.clone().unwrap_or_else(|| "squeue".into()),
        watch,
        poll_interval: poll,
        memory_factor: s.ladder_memory_factor.unwrap_or(2.0),
        max_steps: s.ladder_max_steps.unwrap_or(2),
        settings,
        env: AdapterEnv { cluster: s.cluster.clone(), principal: user.clone(), ..Default::default() },
        process_env: std::env::vars().collect(),
        cwd: std::env::current_dir().unwrap_or_else(|_| ".".into()),
        mode,
        deadline_ms: 2000,
        ..Default::default()
    };
    // In-process when the key is ours to read, else the daemon.
    let key_readable = s.token_key_file.as_deref().is_some_and(readable);
    let use_local = local || (socket.is_none() && key_readable);
    let decider: Box<dyn Decider> = if use_local {
        let engine = Engine::builder(cfg.clone()).role(Role::Client).build().await.map_err(Fail::config)?;
        let svc = crate::daemon::slurm_service(cfg).ok_or_else(|| Fail::config("--local needs [slurm] cluster and token_key_file"))?;
        let uid = rightsizer_plugin::server::current_uid().unwrap_or(u32::MAX);
        let who = svc.identity(uid);
        let tenants: Vec<String> = cfg.serve.tenants_by_uid.get(&uid.to_string()).into_iter().chain(who.user.as_ref().filter(|u| u.as_str() != "http" && !u.bytes().all(|b| b.is_ascii_digit())).and_then(|u| cfg.serve.tenants_by_uid.get(u))).flatten().cloned().collect();
        Box::new(EngineDecider { engine, svc, who, tenants })
    } else {
        let sock = socket.or_else(|| cfg.serve.socket.clone()).unwrap_or_else(|| PathBuf::from(DEFAULT_SOCKET));
        Box::new(SocketDecider { socket: sock, timeout: Duration::from_millis(wcfg.deadline_ms as u64 + 500) })
    };
    let stdin_script = {
        let parsed = rightsizer_slurm::sbatch::parse(&args, None, &BTreeMap::new()).ok();
        let needs_stdin = parsed.is_some_and(|p| p.script_path.is_none() && !p.components.iter().any(|c| c.has("wrap")));
        if needs_stdin {
            let mut s = String::new();
            use std::io::Read;
            let _ = std::io::stdin().read_to_string(&mut s);
            Some(s)
        } else {
            None
        }
    };
    let o = wrapper::run(&args, stdin_script, decider.as_ref(), &wcfg, &TokioRunner::default()).await;
    print!("{}", o.stdout);
    for m in &o.messages {
        eprintln!("{m}");
    }
    if let Some(st) = &o.final_state {
        eprintln!("rightsizer: final state {st} after {} attempt(s): jobs {}", o.attempts, o.job_ids.join(", "));
    }
    match o.exit_code {
        0 => Ok(()),
        c => Err(Fail { code: c.clamp(1, 255) as u8, msg: match o.sbatch_exit {
            Some(e) => format!("sbatch exited with {e}"),
            None => format!("job ended {}", o.final_state.unwrap_or_else(|| "unsuccessfully".into())),
        } }),
    }
}

// ── history import slurm ────────────────────────────────────────────────

/// `rsz history import slurm [--since 30d] [--sacct PATH] [--cluster C] [--dry-run]`.
pub async fn import(cfg: &Config, args: Vec<String>, out: Format) -> Result<(), Fail> {
    let since = opt(&args, "--since").unwrap_or_else(|| "30d".into());
    let window = humantime::parse_duration(&since).map_err(|e| Fail::parse(format!("--since {since:?}: {e}")))?;
    let dry = flag(&args, "--dry-run");
    // `sacct` is platform accounting: a trusted importer (SPEC §8.2).
    let e = Engine::builder(cfg.clone()).role(Role::Client).max_ingest_trust(rightsizer_core::Trust::Verified).build().await.map_err(Fail::config)?;
    let cluster = opt(&args, "--cluster").or_else(|| cfg.slurm.cluster.clone()).or_else(|| cfg.history.cluster.clone()).unwrap_or_else(|| "default".into());
    let settings: rightsizer_slurm::SlurmSettings = serde_json::to_value(&cfg.slurm).ok().and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default();
    let resolution = match cfg.slurm.job_acct_gather_frequency {
        Some(f) => f,
        None => {
            let scontrol = cfg.slurm.scontrol.clone().unwrap_or_else(|| "scontrol".into());
            match tokio::process::Command::new(scontrol).args(["show", "config"]).output().await {
                Ok(o) if o.status.success() => rightsizer_slurm::inventory::parse_show_config(&String::from_utf8_lossy(&o.stdout)).job_acct_gather_frequency.unwrap_or(30),
                _ => 30,
            }
        }
    };
    let mut src = SacctSource::new(ImportConfig { cluster: cluster.clone(), resolution_s: resolution as f32, resolver: None, accelerators: settings.accelerators() });
    if let Some(p) = opt(&args, "--sacct").map(PathBuf::from).or_else(|| cfg.slurm.sacct.clone()) {
        src.sacct = p;
    }
    let start = e.now() - chrono::Duration::from_std(window).unwrap_or_default();
    let jobs = src.jobs(start).await.map_err(|err| Fail::other(format!("sacct: {err}")))?;
    // Decision ids → keys (the decision log), for AdminComment-attributed jobs.
    let ids: BTreeSet<Ulid> = jobs.iter().filter_map(|j| rightsizer_slurm::sacct::parse_admin_comment(&j.admin_comment).and_then(|a| a.0)).collect();
    let mut keys: BTreeMap<Ulid, WorkloadKey> = BTreeMap::new();
    for id in &ids {
        if let Ok(Some(d)) = e.store().decision(*id).await {
            keys.insert(*id, d.key);
        }
    }
    let km = Arc::new(keys.clone());
    src.config.resolver = Some(Arc::new(move |id: &Ulid| km.get(id).cloned()));
    let mut recs = to_run_records(&jobs, &src.config);
    // Ladders: verified groups whose attempt record the wrapper wrote.
    let mut ladders: BTreeSet<Ulid> = BTreeSet::new();
    for (id, _, _) in verified_groups(&jobs).keys() {
        if let Some(k) = keys.get(id)
            && let Ok(Some(_)) = e.store().attempt_get(k, &ladder_invocation(id)).await
        {
            ladders.insert(*id);
        }
    }
    let linked = link_ladders(&jobs, &mut recs, &src.config, |id| ladders.contains(id));
    // A verified job whose resources no longer match its decision was changed
    // after submission (`scontrol update`): not rightsizer's decision (S5).
    let mut detached = Vec::new();
    for r in recs.iter_mut() {
        let Some(id) = r.decision_id else { continue };
        let Ok(Some(d)) = e.store().decision(id).await else { continue };
        let got = r.requested.as_ref().map(|s| (s.requests.memory, s.requests.cpu));
        // Slurm renders whole CPUs: the decision's CPU rounded up.
        let want = (d.applied.requests.memory, d.applied.requests.cpu.map(|c| rightsizer_core::MilliCpu(c.0.div_ceil(1000).max(1) * 1000)));
        let ladder = r.invocation_id == ladder_invocation(&id) || r.invocation_id.starts_with(&format!("{}_", ladder_invocation(&id)));
        let ok = match got {
            Some((m, c)) if ladder => c.is_none() || want.1.is_none() || c == want.1 || m.is_none() || m >= want.0,
            Some((m, c)) => (m.is_none() || want.0.is_none() || m.map(|x| x.0.div_ceil(1 << 20)) == want.0.map(|x| x.0.div_ceil(1 << 20))) && (c.is_none() || want.1.is_none() || c == want.1),
            None => true,
        };
        if !ok && d.mode == rightsizer_core::Mode::Enforce {
            detached.push(format!("{} ({})", r.invocation_id, id));
            r.decision_id = None;
        }
    }
    let mut by_status: BTreeMap<String, usize> = BTreeMap::new();
    for r in &recs {
        *by_status.entry(serde_json::to_value(r.status).ok().and_then(|v| v.as_str().map(String::from)).unwrap_or_default()).or_default() += 1;
    }
    let (mut inserted, mut dup) = (0usize, 0usize);
    let mut fresh = Vec::new();
    if !dry {
        let mut imp = rightsizer_history::Importer::new();
        for r in recs.iter_mut() {
            e.cap_trust(r);
        }
        for r in &recs {
            match imp.put_run(e.store().as_ref(), r).await.map_err(Fail::other)? {
                rightsizer_history::PutOutcome::Inserted => {
                    inserted += 1;
                    fresh.push(r.clone());
                }
                rightsizer_history::PutOutcome::Duplicate => dup += 1,
            }
        }
    }
    // Attribution, breakers and lease closes for new records (every attempt
    // of a ladder is in the store first, so a recovered OOM is seen as such).
    let mut tripped = Vec::new();
    let mut attributions: BTreeMap<String, usize> = BTreeMap::new();
    let mut leases_closed = 0;
    // `prewritten`: these were just inserted above; duplicates (records an
    // earlier import already processed) never reach `outcome` again.
    for r in fresh {
        if let Ok(rep) = e.outcome_with(r, rightsizer_engine::OutcomeOptions { prewritten: true }).await {
            if let Some(a) = rep.attribution {
                *attributions.entry(a).or_default() += 1;
            }
            tripped.extend(rep.tripped.into_iter().map(|s| s.to_string()));
            leases_closed += rep.lease_closed as usize;
        }
    }
    e.flush().await;
    let v = serde_json::json!({
        "cluster": cluster, "since": since, "jobs": jobs.len(), "records": recs.len(), "inserted": inserted, "duplicates": dup,
        "ladder_attempts_linked": linked, "detached_after_modification": detached, "by_status": by_status, "attributions": attributions, "tripped": tripped,
        "leases_closed": leases_closed, "resolution_s": resolution, "dry_run": dry,
        "records_detail": if dry { serde_json::to_value(&recs).unwrap_or_default() } else { serde_json::Value::Null },
    });
    output::value(&v, out, || {
        let mut s = format!("sacct: {} jobs since {since} on {cluster} -> {} run records", jobs.len(), recs.len());
        if dry {
            s.push_str(" (dry run, nothing written)\n");
        } else {
            s.push_str(&format!("; {inserted} imported, {dup} duplicates\n"));
        }
        s.push_str(&format!("statuses: {}\n", by_status.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join(" ")));
        if linked > 0 {
            s.push_str(&format!("ladder attempts linked: {linked}\n"));
        }
        for d in &detached {
            s.push_str(&format!("not attributed (resources changed after verification): {d}\n"));
        }
        if !attributions.is_empty() {
            s.push_str(&format!("failure attribution: {}\n", attributions.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join(" ")));
        }
        for t in &tripped {
            s.push_str(&format!("tripped breaker: {t}\n"));
        }
        s
    });
    Ok(())
}
