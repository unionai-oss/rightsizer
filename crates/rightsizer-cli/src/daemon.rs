//! `rsz serve [--socket PATH | --listen ADDR]`: the daemon (plugins.md §3,
//! slurm.md §1). It serves `rightsizer-plugin/1` shim → core methods:
//!
//! | method | what |
//! |---|---|
//! | `initialize` | version negotiation (N/N-1) |
//! | `decide` | `{kind, native, mode}` → `{decisions, patch, mode, summary, token?}`: the named adapter (built-in `slurm`/`k8s` by cargo feature, or a configured plugin), the engine, render + validate, and for Slurm batch submissions the signed token |
//! | `outcome` | a `RunRecord` → the engine (trust capped at `authenticated`) |
//! | `status` | version, `mode_ceiling`, fleet breaker, store, adapters, `decide` latency percentiles, the caller as seen |
//! | `lease` (ext.) | open / close a ladder lease (`rsz slurm sbatch --watch` run as a user) |
//! | `sign` (ext.) | sign a Slurm ladder resubmission of an enforced decision |
//!
//! **Callers.** On the Unix socket the caller is its peer uid
//! (`SO_PEERCRED` / `getpeereid`). It may decide and report only for the
//! tenants `[serve] tenants_by_uid` maps its uid (or user name) to (exact
//! `{platform}://{tenant}` or a `prefix*` pattern), plus, for Slurm, the
//! accounts it has associations with. HTTP callers have no credentials:
//! they get the tenants mapped to `"http"`. Anything else is an error,
//! never a decision.
//!
//! **Plugins** are loaded only from `[plugins.<kind>]` entries with an
//! absolute path, sha256-pinned when configured (checked before every
//! spawn); a refused plugin's submissions pass through unchanged.
//!
//! **Lifecycle.** `READY=1` / `STATUS=` / `STOPPING=1` and `WATCHDOG=1`
//! (every `WATCHDOG_USEC / 2`) go to `$NOTIFY_SOCKET` (systemd notify
//! protocol, no libsystemd). SIGTERM / SIGINT stop accepting, let in-flight
//! requests finish within `[serve] shutdown_grace` (10 s), flush the
//! engine's write buffer and stop plugins.

use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use rightsizer_core::breaker::{BreakerScope, BreakerState};
use rightsizer_core::{Tenant, Trust};
use rightsizer_engine::store::Role;
use rightsizer_engine::{Config, Engine};
use rightsizer_plugin::client::{PluginAdapter, PluginClient};
use rightsizer_plugin::compose::{BuiltIn, ComposeError, ComposeOptions, Target, decide_native};
use rightsizer_plugin::protocol::{
    DecideParams, ErrorCode, InitializeResult, LatencySummary, LeaseAction, LeaseParams, OutcomeParams, OutcomeResult, PROTOCOL, RpcError, SignParams, StatusResult, method,
};
use rightsizer_plugin::rpc::{self, codes};
use rightsizer_plugin::server::{self, Caller, Ctx, Handler, ServerOptions, Transport, invalid_params, method_not_found};
use rightsizer_plugin::{AdapterEnv, discovery};
use serde_json::Value;
use tokio::sync::watch;

use crate::Fail;

/// uid / user → tenant patterns.
#[derive(Clone, Debug, Default)]
pub struct TenantMap {
    by: BTreeMap<String, Vec<String>>,
}

impl TenantMap {
    fn new(m: &BTreeMap<String, Vec<String>>) -> Self {
        TenantMap { by: m.clone() }
    }

    /// The patterns that apply to a caller.
    pub fn patterns(&self, caller: &Caller, user: Option<&str>) -> Vec<String> {
        let mut v = Vec::new();
        let keys: Vec<String> = match caller.transport {
            Transport::Http => vec!["http".into()],
            _ => caller.uid.map(|u| u.to_string()).into_iter().chain(user.map(str::to_string)).collect(),
        };
        for k in keys {
            if let Some(p) = self.by.get(&k) {
                v.extend(p.iter().cloned());
            }
        }
        v
    }

    fn allows(patterns: &[String], full: &str) -> bool {
        patterns.iter().any(|p| match p.strip_suffix('*') {
            Some(pre) => full.starts_with(pre),
            None => p.trim_end_matches('/') == full,
        })
    }
}

struct Latency {
    samples: Mutex<VecDeque<f64>>,
}

impl Latency {
    fn push(&self, ms: f64) {
        let mut s = self.samples.lock().unwrap_or_else(|e| e.into_inner());
        if s.len() >= 20_000 {
            s.pop_front();
        }
        s.push_back(ms);
    }
    fn summary(&self) -> Option<LatencySummary> {
        let s = self.samples.lock().unwrap_or_else(|e| e.into_inner());
        if s.is_empty() {
            return None;
        }
        let mut v: Vec<f64> = s.iter().copied().collect();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let q = |p: f64| v[((v.len() as f64 - 1.0) * p).round() as usize];
        Some(LatencySummary { count: v.len() as u64, p50: q(0.5), p99: q(0.99), max: *v.last().unwrap_or(&0.0) })
    }
}

/// The daemon's handler.
pub struct Daemon {
    engine: Engine,
    builtins: BTreeMap<String, BuiltIn>,
    plugins: BTreeMap<String, Arc<PluginAdapter>>,
    refused: BTreeMap<String, String>,
    #[cfg(feature = "slurm")]
    slurm: Option<Arc<rightsizer_slurm::service::SlurmService>>,
    tenants: TenantMap,
    users: Mutex<BTreeMap<u32, Option<String>>>,
    latency: Latency,
    requests: AtomicU64,
    decide_budget_ms: u32,
}

fn forbidden(m: impl Into<String>) -> RpcError {
    rpc::err(codes::FORBIDDEN, ErrorCode::Unsupported, format!("forbidden: {}", m.into()), false)
}

fn app(code: ErrorCode, m: impl Into<String>, retryable: bool) -> RpcError {
    RpcError::app(code, m, retryable)
}

impl Daemon {
    fn user(&self, uid: Option<u32>) -> Option<String> {
        let uid = uid?;
        let mut m = self.users.lock().unwrap_or_else(|e| e.into_inner());
        m.entry(uid).or_insert_with(|| server::user_name(uid)).clone()
    }

    fn patterns(&self, caller: &Caller) -> Vec<String> {
        let user = self.user(caller.uid);
        self.tenants.patterns(caller, user.as_deref())
    }

    fn target(&self, kind: &str) -> Option<Box<dyn Target + '_>> {
        if let Some(b) = self.builtins.get(kind) {
            return Some(Box::new(b.clone()));
        }
        self.plugins.get(kind).map(|p| Box::new(p.as_ref().clone()) as Box<dyn Target>)
    }

    fn kinds(&self) -> Vec<String> {
        let mut k: Vec<String> = self.builtins.keys().cloned().collect();
        #[cfg(feature = "slurm")]
        if self.slurm.is_some() {
            k.push("slurm".into());
        }
        k.extend(self.plugins.keys().cloned());
        k.sort();
        k.dedup();
        k
    }

    async fn decide(&self, params: Value, ctx: &Ctx) -> Result<Value, RpcError> {
        let p: DecideParams = serde_json::from_value(params).map_err(invalid_params)?;
        let patterns = self.patterns(&ctx.caller);
        let allows = |full: &str| TenantMap::allows(&patterns, full);
        let budget = p.deadline_ms.unwrap_or(self.decide_budget_ms).min(ctx.remaining_ms().unwrap_or(u32::MAX).saturating_sub(20).max(1));
        #[cfg(feature = "slurm")]
        if p.kind == "slurm" {
            let Some(svc) = &self.slurm else { return Err(app(ErrorCode::Unsupported, "slurm is not configured on this daemon ([slurm] cluster)", false)) };
            let Some(uid) = ctx.caller.uid else { return Err(forbidden("slurm decisions need a Unix-socket caller (peer uid)")) };
            let who = svc.identity(uid);
            let mut p = p;
            p.deadline_ms = Some(budget);
            return match svc.decide(&self.engine, p, &who, &allows).await {
                Ok(r) => Ok(serde_json::to_value(r).unwrap_or_default()),
                Err(rightsizer_slurm::service::ServiceError::Forbidden(m)) => Err(forbidden(m)),
                Err(rightsizer_slurm::service::ServiceError::Invalid(m)) => Err(app(ErrorCode::InvalidNative, m, false)),
                Err(rightsizer_slurm::service::ServiceError::Internal(m)) => Err(app(ErrorCode::Internal, m, true)),
            };
        }
        if let Some(reason) = self.refused.get(&p.kind) {
            return Err(app(ErrorCode::PlatformUnavailable, format!("plugin {} refused: {reason}", p.kind), false));
        }
        let Some(target) = self.target(&p.kind) else {
            return Err(app(ErrorCode::Unsupported, format!("no adapter for kind {:?} (served: {})", p.kind, self.kinds().join(", ")), false));
        };
        let user = self.user(ctx.caller.uid);
        // The shim's env is not trusted for identity: principal is ours.
        let env = AdapterEnv { principal: user.clone().or_else(|| ctx.caller.uid.map(|u| u.to_string())), ..p.env.clone() };
        let kind = p.kind.clone();
        let authorize = |t: &Tenant| {
            let full = format!("{kind}://{}", t.as_str());
            if allows(&full) { Ok(()) } else { Err(format!("uid {} may not act for {full}", ctx.caller.uid.map(|u| u.to_string()).unwrap_or_else(|| "http".into()))) }
        };
        let opts = ComposeOptions { mode: p.mode, budget_ms: Some(budget), dry_run: p.dry_run, attempt: p.attempt.clone(), ..Default::default() };
        match decide_native(&self.engine, target.as_ref(), &p.native, &env, &opts, &authorize).await {
            Ok(c) => Ok(serde_json::to_value(c.into_result(None)).unwrap_or_default()),
            Err(ComposeError::Forbidden(m)) => Err(forbidden(m)),
        }
    }

    fn may_report(&self, key: &rightsizer_core::WorkloadKey, caller: &Caller) -> bool {
        let patterns = self.patterns(caller);
        let full = format!("{}://{}", key.platform, key.tenant.as_str());
        if TenantMap::allows(&patterns, &full) {
            return true;
        }
        #[cfg(feature = "slurm")]
        if let (Some(svc), Some(uid)) = (&self.slurm, caller.uid) {
            return svc.may_report(key, &svc.identity(uid), &|f: &str| TenantMap::allows(&patterns, f));
        }
        false
    }

    async fn outcome(&self, params: Value, ctx: &Ctx) -> Result<Value, RpcError> {
        let p: OutcomeParams = serde_json::from_value(params).map_err(invalid_params)?;
        if !self.may_report(&p.record.key, &ctx.caller) {
            return Err(forbidden(format!("may not report outcomes for {}", p.record.key)));
        }
        let mut rec = p.record;
        // A shim's report is at most authenticated (SPEC §8.2).
        if rec.trust > Trust::Authenticated {
            rec.trust = Trust::Authenticated;
        }
        let r = self.engine.outcome(rec).await.map_err(|e| app(ErrorCode::Internal, e.to_string(), true))?;
        Ok(serde_json::to_value(OutcomeResult { accepted: true, duplicate: false }).unwrap_or_default()).map(|mut v| {
            v["written"] = Value::Bool(r.written);
            v["attribution"] = serde_json::to_value(&r.attribution).unwrap_or_default();
            v["lease_closed"] = Value::Bool(r.lease_closed);
            v
        })
    }

    async fn lease(&self, params: Value, ctx: &Ctx) -> Result<Value, RpcError> {
        let p: LeaseParams = serde_json::from_value(params).map_err(invalid_params)?;
        match p.action {
            LeaseAction::Open => {
                let rec = p.record.ok_or_else(|| invalid_params("open needs record"))?;
                if !self.may_report(&rec.key, &ctx.caller) {
                    return Err(forbidden(format!("may not open leases for {}", rec.key)));
                }
                let created = self.engine.store().attempt_put_if_absent(&rec).await.map_err(|e| app(ErrorCode::PlatformUnavailable, e.to_string(), true))?;
                Ok(serde_json::json!({"ok": true, "created": created}))
            }
            LeaseAction::Close => {
                let (Some(key), Some(inv)) = (p.key, p.invocation_id) else { return Err(invalid_params("close needs key and invocation_id")) };
                if !self.may_report(&key, &ctx.caller) {
                    return Err(forbidden(format!("may not close leases for {key}")));
                }
                self.engine
                    .close_lease(&key, &inv, p.status.unwrap_or(rightsizer_core::RunStatus::Unknown))
                    .await
                    .map_err(|e| app(ErrorCode::PlatformUnavailable, e.to_string(), true))?;
                Ok(serde_json::json!({"ok": true}))
            }
        }
    }

    #[allow(unused_variables)]
    async fn sign(&self, params: Value, ctx: &Ctx) -> Result<Value, RpcError> {
        let p: SignParams = serde_json::from_value(params).map_err(invalid_params)?;
        #[cfg(feature = "slurm")]
        if let Some(svc) = &self.slurm {
            let Some(uid) = ctx.caller.uid else { return Err(forbidden("sign needs a Unix-socket caller")) };
            let patterns = self.patterns(&ctx.caller);
            return match svc.sign(&self.engine, p, &svc.identity(uid), &|f: &str| TenantMap::allows(&patterns, f)).await {
                Ok(r) => Ok(serde_json::to_value(r).unwrap_or_default()),
                Err(rightsizer_slurm::service::ServiceError::Forbidden(m)) => Err(forbidden(m)),
                Err(rightsizer_slurm::service::ServiceError::Invalid(m)) => Err(app(ErrorCode::InvalidNative, m, false)),
                Err(rightsizer_slurm::service::ServiceError::Internal(m)) => Err(app(ErrorCode::Internal, m, true)),
            };
        }
        Err(app(ErrorCode::Unsupported, "sign: slurm is not configured", false))
    }

    async fn status(&self, ctx: &Ctx) -> Value {
        let ceiling = self.engine.control_get().await.ok().map(|c| c.map(|c| c.mode_ceiling).unwrap_or(rightsizer_core::Mode::Enforce));
        let (fleet, reachable) = match self.engine.breaker_status().await {
            Ok(v) => (v.into_iter().find(|b| b.scope == BreakerScope::Fleet).map(|b| b.state).unwrap_or(BreakerState::Closed), true),
            Err(_) => (BreakerState::Closed, false),
        };
        let user = self.user(ctx.caller.uid);
        let r = StatusResult {
            protocol: ctx.protocol.clone(),
            version: env!("CARGO_PKG_VERSION").into(),
            mode_ceiling: ceiling,
            fleet_breaker: fleet,
            store_reachable: reachable && ceiling.is_some(),
            plugins: self.plugins.keys().cloned().collect(),
            kinds: self.kinds(),
            requests: self.requests.load(Ordering::Relaxed),
            decide_latency_ms: self.latency.summary(),
            caller: Some(serde_json::json!({
                "uid": ctx.caller.uid, "gid": ctx.caller.gid, "pid": ctx.caller.pid, "user": user,
                "tenants": self.patterns(&ctx.caller),
                "transport": format!("{:?}", ctx.caller.transport).to_lowercase(),
            })),
        };
        let mut v = serde_json::to_value(r).unwrap_or_default();
        if !self.refused.is_empty() {
            v["refused"] = serde_json::to_value(&self.refused).unwrap_or_default();
        }
        v
    }
}

#[async_trait]
impl Handler for Daemon {
    fn info(&self) -> InitializeResult {
        InitializeResult {
            protocol: PROTOCOL.into(),
            kind: "rightsizer".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            methods: vec![method::DECIDE.into(), method::OUTCOME.into(), method::STATUS.into(), method::LEASE.into(), method::SIGN.into()],
            native_media_types: vec![],
            concurrency: Some(64),
        }
    }

    async fn call(&self, m: &str, params: Value, ctx: &Ctx) -> Result<Value, RpcError> {
        self.requests.fetch_add(1, Ordering::Relaxed);
        match m {
            method::DECIDE => {
                let t = Instant::now();
                let r = self.decide(params, ctx).await;
                self.latency.push(t.elapsed().as_secs_f64() * 1000.0);
                r
            }
            method::OUTCOME => self.outcome(params, ctx).await,
            method::STATUS => Ok(self.status(ctx).await),
            method::LEASE => self.lease(params, ctx).await,
            method::SIGN => self.sign(params, ctx).await,
            other => Err(method_not_found(other)),
        }
    }
}

// ── systemd notify ──────────────────────────────────────────────────────

/// Send a systemd notification (`READY=1`, `WATCHDOG=1`, …) to
/// `$NOTIFY_SOCKET`; a no-op without it. Abstract sockets (`@name`) on Linux.
pub fn sd_notify(msg: &str) -> bool {
    let Some(path) = std::env::var_os("NOTIFY_SOCKET") else { return false };
    let Ok(sock) = std::os::unix::net::UnixDatagram::unbound() else { return false };
    let p = path.to_string_lossy().into_owned();
    if let Some(name) = p.strip_prefix('@') {
        #[cfg(target_os = "linux")]
        {
            use std::os::linux::net::SocketAddrExt;
            return std::os::unix::net::SocketAddr::from_abstract_name(name.as_bytes()).is_ok_and(|a| sock.send_to_addr(msg.as_bytes(), &a).is_ok());
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = name;
            return false;
        }
    }
    sock.send_to(msg.as_bytes(), &p).is_ok()
}

/// The watchdog interval: half of `WATCHDOG_USEC`, when it is for us.
pub fn watchdog_interval() -> Option<Duration> {
    let usec: u64 = std::env::var("WATCHDOG_USEC").ok()?.parse().ok()?;
    if let Ok(pid) = std::env::var("WATCHDOG_PID")
        && pid.parse::<u32>().ok() != Some(std::process::id())
    {
        return None;
    }
    (usec > 0).then(|| Duration::from_micros(usec / 2))
}

// ── startup ─────────────────────────────────────────────────────────────

fn parse_mode(s: Option<&str>) -> u32 {
    s.and_then(|m| u32::from_str_radix(m.trim_start_matches("0o"), 8).ok()).unwrap_or(0o666)
}

/// Build the daemon's handler from config.
pub async fn build(cfg: &Config) -> Result<Arc<Daemon>, Fail> {
    let engine = Engine::builder(cfg.clone()).role(Role::Client).build().await.map_err(Fail::config)?;
    if let Some(err) = engine.config_error() {
        tracing::warn!("{err}; decisions keep the prior (S1)");
    }
    #[allow(unused_mut)]
    let mut builtins: BTreeMap<String, BuiltIn> = BTreeMap::new();
    #[cfg(feature = "k8s")]
    {
        let a = Arc::new(rightsizer_k8s::K8sAdapter::new(Default::default()));
        builtins.insert("k8s".into(), BuiltIn { adapter: a.clone(), model: Some(a) });
    }
    // Plugins: configured absolute paths only (server mode).
    let (found, refused_list) = discovery::discover(&cfg.plugins, discovery::Mode::Server, None);
    let mut refused: BTreeMap<String, String> = refused_list.into_iter().map(|r| (r.kind, r.reason)).collect();
    for (k, r) in &refused {
        tracing::error!(plugin = %k, "plugin refused: {r}; its submissions pass through unchanged");
    }
    let mut plugins = BTreeMap::new();
    for f in found {
        let client = Arc::new(PluginClient::new(f.spec()));
        match client.start().await {
            Ok(info) => tracing::info!(plugin = %f.kind, version = %info.version, protocol = %info.protocol, "plugin started"),
            Err(e @ rightsizer_plugin::client::CallError::PinMismatch { .. }) => {
                tracing::error!(plugin = %f.kind, "{e}");
                refused.insert(f.kind.clone(), e.to_string());
                continue;
            }
            Err(e) => tracing::warn!(plugin = %f.kind, "plugin did not start ({e}); retried on first use"),
        }
        plugins.insert(f.kind.clone(), Arc::new(PluginAdapter::new(client)));
    }
    #[cfg(feature = "slurm")]
    let slurm = slurm_service(cfg);
    Ok(Arc::new(Daemon {
        engine,
        builtins,
        plugins,
        refused,
        #[cfg(feature = "slurm")]
        slurm,
        tenants: TenantMap::new(&cfg.serve.tenants_by_uid),
        users: Mutex::new(BTreeMap::new()),
        latency: Latency { samples: Mutex::new(VecDeque::new()) },
        requests: AtomicU64::new(0),
        decide_budget_ms: cfg.policy.budget_ms.max(1),
    }))
}

/// The Slurm service from `[slurm]` (when a cluster or key file is configured).
#[cfg(feature = "slurm")]
pub fn slurm_service(cfg: &Config) -> Option<Arc<rightsizer_slurm::service::SlurmService>> {
    use rightsizer_slurm::service::{Bins, ServiceConfig, SlurmService};
    let s = &cfg.slurm;
    if s.cluster.is_none() && s.token_key_file.is_none() {
        return None;
    }
    let settings: rightsizer_slurm::SlurmSettings = serde_json::to_value(s).ok().and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default();
    let sc = ServiceConfig {
        settings,
        key_file: s.token_key_file.clone(),
        token_ttl_s: s.token_ttl_s.unwrap_or(rightsizer_slurm::token::DEFAULT_TTL_S),
        use_associations: cfg.serve.slurm_associations.unwrap_or(true),
        refresh: s.inventory_refresh.unwrap_or(Duration::from_secs(300)),
        bins: Bins { scontrol: s.scontrol.clone().unwrap_or_else(|| "scontrol".into()), sacctmgr: s.sacctmgr.clone().unwrap_or_else(|| "sacctmgr".into()) },
        ladder_factor: s.ladder_memory_factor.unwrap_or(2.0),
        ladder_steps: s.ladder_max_steps.unwrap_or(2),
    };
    Some(Arc::new(SlurmService::new(sc)))
}

async fn wait_signal() -> &'static str {
    use tokio::signal::unix::{SignalKind, signal};
    let (Ok(mut term), Ok(mut int)) = (signal(SignalKind::terminate()), signal(SignalKind::interrupt())) else {
        std::future::pending::<()>().await;
        return "";
    };
    tokio::select! {
        _ = term.recv() => "SIGTERM",
        _ = int.recv() => "SIGINT",
    }
}

/// `rsz serve`.
pub async fn serve(cfg: &Config, socket: Option<PathBuf>, listen: Option<String>) -> Result<(), Fail> {
    let socket = socket.or_else(|| cfg.serve.socket.clone());
    let listen = listen.or_else(|| cfg.serve.listen.clone());
    if socket.is_none() && listen.is_none() {
        return Err(Fail::parse("serve needs --socket PATH or --listen ADDR (or [serve] socket / listen)"));
    }
    let daemon = build(cfg).await?;
    let handler: Arc<dyn Handler> = daemon.clone();
    let opts = ServerOptions { max_concurrent: cfg.serve.max_concurrent_requests.unwrap_or(64), ..Default::default() };
    let grace = cfg.serve.shutdown_grace.unwrap_or(Duration::from_secs(10));
    let (tx, rx) = watch::channel(false);
    let mut tasks = tokio::task::JoinSet::new();

    if let Some(path) = &socket {
        let l = server::bind_unix(path, parse_mode(cfg.serve.socket_mode.as_deref())).map_err(|e| Fail::other(format!("{}: {e}", path.display())))?;
        eprintln!("rsz serve: listening on {} (rightsizer-plugin/1)", path.display());
        tasks.spawn(server::serve_unix(l, path.clone(), handler.clone(), opts.clone(), rx.clone(), grace));
    }
    if let Some(addr) = &listen {
        let l = tokio::net::TcpListener::bind(addr).await.map_err(|e| Fail::other(format!("{addr}: {e}")))?;
        eprintln!("rsz serve: listening on http://{} (POST / with a JSON-RPC body)", l.local_addr().map(|a| a.to_string()).unwrap_or_default());
        let (h, d, o, mut r) = (handler.clone(), daemon.clone(), opts.clone(), rx.clone());
        tasks.spawn(async move {
            let app = http_app(h, d, o);
            let _ = axum::serve(l, app).with_graceful_shutdown(async move { server::shutdown_signal(&mut r).await }).await;
        });
    }
    // Inventory refresh (Slurm).
    #[cfg(feature = "slurm")]
    if let Some(svc) = daemon.slurm.clone() {
        let mut r = rx.clone();
        let every = cfg.slurm.inventory_refresh.unwrap_or(Duration::from_secs(300));
        tasks.spawn(async move {
            loop {
                svc.refresh().await;
                tokio::select! {
                    _ = tokio::time::sleep(every) => {}
                    _ = server::shutdown_signal(&mut r) => break,
                }
            }
        });
    }
    // systemd.
    sd_notify(&format!("READY=1\nSTATUS=serving {} adapters: {}\nMAINPID={}", PROTOCOL, daemon.kinds().join(","), std::process::id()));
    if let Some(iv) = watchdog_interval() {
        let mut r = rx.clone();
        tasks.spawn(async move {
            loop {
                sd_notify("WATCHDOG=1");
                tokio::select! {
                    _ = tokio::time::sleep(iv) => {}
                    _ = server::shutdown_signal(&mut r) => break,
                }
            }
        });
    }
    let sig = wait_signal().await;
    eprintln!("rsz serve: {sig}: shutting down (grace {grace:?})");
    sd_notify("STOPPING=1\nSTATUS=shutting down");
    let _ = tx.send(true);
    let _ = tokio::time::timeout(grace + Duration::from_secs(1), async { while tasks.join_next().await.is_some() {} }).await;
    let flushed = daemon.engine.flush().await;
    for p in daemon.plugins.values() {
        p.client().shutdown().await;
    }
    eprintln!("rsz serve: stopped ({flushed} buffered writes flushed)");
    Ok(())
}

fn http_app(handler: Arc<dyn Handler>, daemon: Arc<Daemon>, opts: ServerOptions) -> axum::Router {
    use axum::body::Bytes;
    use axum::http::{StatusCode, header};
    use axum::response::IntoResponse;
    use axum::routing::{get, post};
    let rpc = move |body: Bytes| {
        let (h, o) = (handler.clone(), opts.clone());
        async move {
            match server::serve_body(&body, h, Caller::anonymous(Transport::Http), o).await {
                Some(v) => ([(header::CONTENT_TYPE, "application/json")], serde_json::to_vec(&v).unwrap_or_default()).into_response(),
                None => StatusCode::NO_CONTENT.into_response(),
            }
        }
    };
    let metrics = move || {
        let d = daemon.clone();
        async move { ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], d.engine.metrics().render()) }
    };
    axum::Router::new().route("/", post(rpc.clone())).route("/rpc", post(rpc)).route("/healthz", get(|| async { "ok\n" })).route("/metrics", get(metrics))
}
