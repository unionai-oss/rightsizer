//! The daemon side of Slurm (slurm.md §1, §2, §4.1): what `rsz serve` (and
//! `rsz slurm sbatch` run as the daemon user) does for a `decide {kind:
//! "slurm"}`:
//!
//! 1. **Identity.** The caller's uid comes from the socket's peer
//!    credentials; its user name from `/etc/passwd` (else `id -nu`).
//! 2. **Account and tenant.** The account is the submission's `--account`,
//!    else the user's default account (`sacctmgr show user`), else
//!    `[slurm] default_account`. The tenant is `slurm://{cluster}/{account}`.
//!    The caller may act for it when `[serve] tenants_by_uid` maps its uid to
//!    it, or (`slurm_associations`, default on) when the user has an
//!    association with the account. Otherwise: an error, never a decision.
//! 3. **Decide.** The adapter, the engine and render/validate run through
//!    [`rightsizer_plugin::compose`]; `salloc`/`srun` are capped at
//!    `recommend` (their jobs carry no environment, so they could not be
//!    attributed: S5).
//! 4. **Sign.** An enforced, patched `sbatch` submission gets a token over
//!    the *patched* job_desc fields, signed with the active key. A
//!    submission that cannot be signed is not changed (a layer that cannot
//!    write its marker does not change the submission).
//!
//! The inventory (`scontrol`, `sacctmgr`) is refreshed every
//! `inventory_refresh` (5 min) in the background; a failed refresh keeps
//! the previous one. The key file is re-read when its mtime changes (key
//! rotation needs no restart).

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime};

use rightsizer_core::{Decision, Mode, Tenant, WorkloadKey};
use rightsizer_engine::{Engine, Policy};
use rightsizer_plugin::compose::{BuiltIn, ComposeError, ComposeOptions, decide_native};
use rightsizer_plugin::protocol::{DecideParams, DecideResult, SignParams, SignResult};
use rightsizer_plugin::{AdapterEnv, Native, PlatformModel};
use ulid::Ulid;

use crate::adapter::{NativeKind, SlurmAdapter, parse_native};
use crate::attest::{sign_parsed, sign_patched};
use crate::inventory::{Inventory, parse_sacctmgr_assoc, parse_sacctmgr_qos, parse_show_config, parse_show_node, parse_show_partition};
use crate::keys::{tenant, unescape_segment};
use crate::platform::SlurmPlatform;
use crate::runner::{CommandRunner, TokioRunner};
use crate::sbatch::CompResources;
use crate::settings::SlurmSettings;
use crate::token::{DEFAULT_TTL_S, KeyRing};

/// Binaries used for the inventory.
#[derive(Clone, Debug)]
pub struct Bins {
    /// `scontrol`.
    pub scontrol: PathBuf,
    /// `sacctmgr`.
    pub sacctmgr: PathBuf,
}

impl Default for Bins {
    fn default() -> Self {
        Bins { scontrol: "scontrol".into(), sacctmgr: "sacctmgr".into() }
    }
}

/// Service configuration.
#[derive(Clone, Debug)]
pub struct ServiceConfig {
    /// `[slurm]` settings (the inventory is filled in by refreshes).
    pub settings: SlurmSettings,
    /// Signing key file (the daemon's copy, 0600).
    pub key_file: Option<PathBuf>,
    /// Token lifetime, s.
    pub token_ttl_s: u64,
    /// Allow tenants by Slurm associations.
    pub use_associations: bool,
    /// Inventory refresh interval.
    pub refresh: Duration,
    /// Binaries.
    pub bins: Bins,
    /// Ladder memory factor / steps (for `sign` checks).
    pub ladder_factor: f32,
    /// Ladder steps.
    pub ladder_steps: u32,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        ServiceConfig {
            settings: SlurmSettings::default(),
            key_file: None,
            token_ttl_s: DEFAULT_TTL_S,
            use_associations: true,
            refresh: Duration::from_secs(300),
            bins: Bins::default(),
            ladder_factor: 2.0,
            ladder_steps: 2,
        }
    }
}

/// The caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity {
    /// uid (peer credentials).
    pub uid: u32,
    /// User name.
    pub user: Option<String>,
}

/// Service errors.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum ServiceError {
    /// Not allowed for this tenant.
    #[error("forbidden: {0}")]
    Forbidden(String),
    /// Bad request.
    #[error("invalid: {0}")]
    Invalid(String),
    /// Anything else.
    #[error("internal: {0}")]
    Internal(String),
}

struct Live {
    settings: SlurmSettings,
    default_accounts: BTreeMap<String, String>,
    refreshed: Option<Instant>,
}

struct Ring {
    ring: Option<KeyRing>,
    mtime: Option<SystemTime>,
    checked: Option<Instant>,
    error: Option<String>,
}

/// The Slurm service.
pub struct SlurmService {
    cfg: ServiceConfig,
    live: RwLock<Live>,
    ring: Mutex<Ring>,
    users: Mutex<HashMap<u32, Option<String>>>,
    runner: Arc<dyn CommandRunner>,
    refreshing: tokio::sync::Mutex<()>,
}

impl std::fmt::Debug for SlurmService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlurmService").field("key_file", &self.cfg.key_file).finish_non_exhaustive()
    }
}

pub use rightsizer_plugin::server::{current_uid, user_name};

/// `sacctmgr -P show user format=User,DefaultAccount`.
pub fn parse_default_accounts(text: &str) -> BTreeMap<String, String> {
    let mut lines = text.lines().filter(|l| !l.trim().is_empty());
    let Some(h) = lines.next() else { return BTreeMap::new() };
    let cols: Vec<&str> = h.split('|').collect();
    let iu = cols.iter().position(|c| c.eq_ignore_ascii_case("User"));
    let ia = cols.iter().position(|c| c.eq_ignore_ascii_case("Def Acct") || c.eq_ignore_ascii_case("DefaultAccount"));
    let (Some(iu), Some(ia)) = (iu, ia) else { return BTreeMap::new() };
    lines
        .filter_map(|l| {
            let f: Vec<&str> = l.split('|').collect();
            let u = f.get(iu)?.trim();
            let a = f.get(ia)?.trim();
            (!u.is_empty() && !a.is_empty()).then(|| (u.to_string(), a.to_ascii_lowercase()))
        })
        .collect()
}

/// Whether any pattern matches.
pub fn tenant_matches_any(patterns: &[String], tenant: &str) -> bool {
    patterns.iter().any(|p| tenant_matches(p, tenant))
}

/// A tenant pattern: exact, or a prefix ending in `*`.
pub fn tenant_matches(pattern: &str, tenant: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(p) => tenant.starts_with(p),
        None => pattern.trim_end_matches('/') == tenant,
    }
}

impl SlurmService {
    /// A service; call [`SlurmService::refresh`] (or let `decide` do it lazily).
    pub fn new(cfg: ServiceConfig) -> Self {
        Self::with_runner(cfg, Arc::new(TokioRunner::default()))
    }

    /// With a command runner (tests).
    pub fn with_runner(cfg: ServiceConfig, runner: Arc<dyn CommandRunner>) -> Self {
        let settings = cfg.settings.clone();
        SlurmService {
            cfg,
            live: RwLock::new(Live { settings, default_accounts: BTreeMap::new(), refreshed: None }),
            ring: Mutex::new(Ring { ring: None, mtime: None, checked: None, error: None }),
            users: Mutex::new(HashMap::new()),
            runner,
            refreshing: tokio::sync::Mutex::new(()),
        }
    }

    /// Current settings (inventory included).
    pub fn settings(&self) -> SlurmSettings {
        self.live.read().unwrap_or_else(|e| e.into_inner()).settings.clone()
    }

    /// The cluster name.
    pub fn cluster(&self) -> String {
        self.settings().cluster_name(&AdapterEnv::default())
    }

    fn user(&self, uid: u32) -> Option<String> {
        let mut m = self.users.lock().unwrap_or_else(|e| e.into_inner());
        m.entry(uid).or_insert_with(|| user_name(uid)).clone()
    }

    /// Resolve a caller.
    pub fn identity(&self, uid: u32) -> Identity {
        Identity { uid, user: self.user(uid) }
    }

    /// Whether the inventory is older than the refresh interval.
    pub fn stale(&self) -> bool {
        self.live.read().unwrap_or_else(|e| e.into_inner()).refreshed.is_none_or(|t| t.elapsed() > self.cfg.refresh)
    }

    /// Refresh the inventory now (failures keep the previous one).
    pub async fn refresh(&self) {
        let _g = self.refreshing.lock().await;
        let run = |bin: &Path, args: &[&str]| {
            let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
            let r = self.runner.clone();
            let bin = bin.to_path_buf();
            async move { r.run(&bin, &args).await.ok().filter(|o| o.success).map(|o| o.stdout) }
        };
        let b = &self.cfg.bins;
        let (nodes, parts, conf, assoc, qos, users) = tokio::join!(
            run(&b.scontrol, &["show", "node"]),
            run(&b.scontrol, &["show", "partition"]),
            run(&b.scontrol, &["show", "config"]),
            run(&b.sacctmgr, &["-P", "show", "assoc", "format=Cluster,Account,User,Partition,GrpTRES,MaxTRES,MaxTRESPerNode,QOS,DefaultQOS"]),
            run(&b.sacctmgr, &["-P", "show", "qos", "format=Name,GrpTRES,MaxTRESPerJob,MaxTRESPerNode"]),
            run(&b.sacctmgr, &["-P", "show", "user", "format=User,DefaultAccount"]),
        );
        let mut live = self.live.write().unwrap_or_else(|e| e.into_inner());
        let inv: &mut Inventory = &mut live.settings.inventory;
        let mut got = Vec::new();
        if let Some(t) = nodes {
            inv.nodes = parse_show_node(&t);
            got.push("nodes");
        }
        if let Some(t) = parts {
            inv.partitions = parse_show_partition(&t);
            got.push("partitions");
        }
        if let Some(t) = conf {
            inv.config = parse_show_config(&t);
            got.push("config");
        }
        if let Some(t) = assoc {
            inv.assocs = parse_sacctmgr_assoc(&t);
            got.push("assocs");
        }
        if let Some(t) = qos {
            inv.qos = parse_sacctmgr_qos(&t);
            got.push("qos");
        }
        if let Some(t) = users {
            live.default_accounts = parse_default_accounts(&t);
            got.push("users");
        }
        live.refreshed = Some(Instant::now());
        tracing::info!(refreshed = ?got, "slurm inventory refreshed");
    }

    /// The signing keys (re-read when the file's mtime changes; checked at
    /// most once a second).
    pub fn keys(&self) -> Result<KeyRing, ServiceError> {
        let Some(path) = &self.cfg.key_file else { return Err(ServiceError::Internal("no [slurm] token_key_file configured".into())) };
        let mut r = self.ring.lock().unwrap_or_else(|e| e.into_inner());
        let due = r.checked.is_none_or(|t| t.elapsed() > Duration::from_secs(1));
        if due {
            r.checked = Some(Instant::now());
            let mtime = std::fs::metadata(path).and_then(|m| m.modified()).ok();
            if mtime != r.mtime || r.ring.is_none() {
                match KeyRing::load(path) {
                    Ok(k) => {
                        r.ring = Some(k);
                        r.error = None;
                    }
                    Err(e) => {
                        r.ring = None;
                        r.error = Some(e.to_string());
                    }
                }
                r.mtime = mtime;
            }
        }
        r.ring.clone().ok_or_else(|| ServiceError::Internal(format!("signing keys unavailable: {}", r.error.clone().unwrap_or_default())))
    }

    /// The account a submission is charged to, for a user.
    pub fn account_for(&self, requested: Option<&str>, user: Option<&str>) -> String {
        let live = self.live.read().unwrap_or_else(|e| e.into_inner());
        requested
            .map(str::to_string)
            .filter(|a| !a.is_empty())
            .or_else(|| user.and_then(|u| live.default_accounts.get(u).cloned()))
            .or_else(|| live.settings.default_account.clone())
            .unwrap_or_default()
            .to_ascii_lowercase()
    }

    /// Whether `who` may act for `tenant` (`config_allows`: the `[serve]
    /// tenants_by_uid` check).
    pub fn allowed(&self, t: &Tenant, who: &Identity, config_allows: &(dyn Fn(&str) -> bool + Sync)) -> bool {
        let full = format!("slurm://{}", t.as_str());
        if config_allows(&full) {
            return true;
        }
        if !self.cfg.use_associations {
            return false;
        }
        let Some(user) = &who.user else { return false };
        let account = t.as_str().split('/').nth(1).map(unescape_segment).unwrap_or_default();
        let cluster = t.as_str().split('/').next().map(unescape_segment).unwrap_or_default();
        let live = self.live.read().unwrap_or_else(|e| e.into_inner());
        live.settings.inventory.assocs.iter().any(|a| a.account == account && a.user.as_deref() == Some(user.as_str()) && (a.cluster.is_empty() || a.cluster == cluster))
    }

    /// `decide {kind: "slurm"}`.
    pub async fn decide(&self, engine: &Engine, p: DecideParams, who: &Identity, config_allows: &(dyn Fn(&str) -> bool + Sync)) -> Result<DecideResult, ServiceError> {
        if self.stale() {
            // First call (or overdue): refresh inline; later ones run in the background.
            if self.live.read().unwrap_or_else(|e| e.into_inner()).refreshed.is_none() {
                self.refresh().await;
            }
        }
        let parsed = parse_native(&p.native).map_err(|e| ServiceError::Invalid(e.to_string()))?;
        let mut settings = self.settings();
        settings.watch = p.watch;
        let requested = parsed.job.components.first().and_then(|c| c.value("account")).map(str::to_string);
        let account = self.account_for(requested.as_deref(), who.user.as_deref());
        let cluster = settings.cluster_name(&p.env);
        let env = AdapterEnv {
            cluster: Some(cluster.clone()),
            principal: who.user.clone(),
            namespace: Some(account.clone()),
            deadline_ms: p.deadline_ms,
            settings: settings.to_env_settings(),
        };
        let batch = matches!(parsed.kind, NativeKind::Sbatch | NativeKind::Document) || parsed.cmd.as_deref().is_none_or(|c| c == "sbatch");
        let target = BuiltIn { adapter: Arc::new(SlurmAdapter::new(settings.clone())), model: Some(Arc::new(SlurmPlatform::new(settings.clone())) as Arc<dyn PlatformModel>) };
        let mut policy = Policy::default();
        if !batch {
            policy.max_mode = Some(Mode::Recommend);
        }
        let opts = ComposeOptions { mode: p.mode, budget_ms: Some(p.deadline_ms.unwrap_or(800)), dry_run: p.dry_run, attempt: p.attempt.clone(), policy };
        let authorize = |t: &Tenant| if self.allowed(t, who, config_allows) { Ok(()) } else { Err(format!("uid {} ({}) may not act for slurm://{}", who.uid, who.user.as_deref().unwrap_or("?"), t.as_str())) };
        let mut c = match decide_native(engine, &target, &p.native, &env, &opts, &authorize).await {
            Ok(c) => c,
            Err(ComposeError::Forbidden(m)) => return Err(ServiceError::Forbidden(m)),
        };
        let mut token = None;
        if c.mode == Mode::Enforce && !c.patch.is_none() {
            if !batch {
                c.patch = Default::default();
            } else {
                let ids: Vec<Ulid> = c.decisions.iter().map(|d| d.id).collect();
                let expiry = chrono::Utc::now().timestamp().max(0) as u64 + self.cfg.token_ttl_s;
                match self.keys().and_then(|ring| sign_patched(&p.native, &c.patch, who.uid, &account, &ids, expiry, &ring).map_err(|e| ServiceError::Internal(e.to_string()))) {
                    Ok(t) => token = Some(t),
                    Err(e) => {
                        tracing::warn!(error = %e, "cannot sign: submitting unchanged");
                        c.summary.push_str(&format!("; not applied: cannot sign ({e})"));
                        c.patch = Default::default();
                    }
                }
            }
        }
        Ok(c.into_result(token))
    }

    /// `sign` (ladder resubmissions): the native must be the original
    /// submission escalated along the ladder of enforced decisions the
    /// caller may act for: memory in `[applied, max(prior, applied ×
    /// factor^steps)]`, CPU and GPU unchanged from the decision.
    pub async fn sign(&self, engine: &Engine, p: SignParams, who: &Identity, config_allows: &(dyn Fn(&str) -> bool + Sync)) -> Result<SignResult, ServiceError> {
        if p.kind != "slurm" {
            return Err(ServiceError::Invalid(format!("sign: kind {:?} is not slurm", p.kind)));
        }
        let mut parsed = parse_native(&p.native).map_err(|e| ServiceError::Invalid(e.to_string()))?;
        parsed.env.retain(|k, _| !k.starts_with("RIGHTSIZER_"));
        if parsed.job.components.len() != p.decision_ids.len() && p.decision_ids.len() != 1 {
            return Err(ServiceError::Invalid("one decision id per component".into()));
        }
        let mut decisions: Vec<Decision> = Vec::new();
        for id in &p.decision_ids {
            let d = engine.store().decision(*id).await.map_err(|e| ServiceError::Internal(e.to_string()))?.ok_or_else(|| ServiceError::Invalid(format!("unknown decision {id}")))?;
            if !self.allowed(&d.key.tenant, who, config_allows) {
                return Err(ServiceError::Forbidden(format!("uid {} may not act for {}", who.uid, d.key)));
            }
            if d.mode != Mode::Enforce {
                return Err(ServiceError::Invalid(format!("decision {id} was not enforced")));
            }
            decisions.push(d);
        }
        for (k, c) in parsed.job.components.iter().enumerate() {
            let d = decisions.get(k).unwrap_or(&decisions[0]);
            let r = CompResources::of(c);
            let mem = r.mem_per_node().map(|b| b.0);
            let (applied, prior) = (d.applied.requests.memory.map(|b| b.0), d.prior.spec.requests.memory.map(|b| b.0));
            if let (Some(m), Some(a)) = (mem, applied) {
                let top = (a as f64 * (self.cfg.ladder_factor.max(1.0) as f64).powi(self.cfg.ladder_steps as i32)).ceil() as u64;
                let top = top.max(prior.unwrap_or(0)) + (1 << 20);
                if m + (1 << 20) < a || m > top {
                    return Err(ServiceError::Invalid(format!("component {k}: memory {m} is not a ladder step of decision {} ({a}..{top})", d.id)));
                }
            } else if mem != applied {
                return Err(ServiceError::Invalid(format!("component {k}: memory form differs from decision {}", d.id)));
            }
            // Slurm renders whole CPUs: compare with the decision rounded up.
            let cpu = r.cpus_per_node() as u64;
            if let Some(dc) = d.applied.requests.cpu
                && (dc.0 as u64).div_ceil(1000).max(1) != cpu
            {
                return Err(ServiceError::Invalid(format!("component {k}: {cpu} CPUs differ from decision {} ({}m)", d.id, dc.0)));
            }
        }
        let account = decisions[0].key.tenant.as_str().split('/').nth(1).map(unescape_segment).unwrap_or_default();
        let ring = self.keys()?;
        let expiry = chrono::Utc::now().timestamp().max(0) as u64 + self.cfg.token_ttl_s;
        let toks = sign_parsed(&parsed, who.uid, &account, &p.decision_ids, expiry, &ring).map_err(|e| ServiceError::Internal(e.to_string()))?;
        Ok(SignResult { token: Some(toks.join(".")) })
    }

    /// Whether `who` may write records for `key` (outcomes, leases).
    pub fn may_report(&self, key: &WorkloadKey, who: &Identity, config_allows: &(dyn Fn(&str) -> bool + Sync)) -> bool {
        key.platform == "slurm" && self.allowed(&key.tenant, who, config_allows)
    }
}

/// The tenant of a cluster/account pair (re-export for callers).
pub fn slurm_tenant(cluster: &str, account: &str) -> Option<Tenant> {
    tenant(cluster, account).ok()
}

/// Build a `Native::Object` for an sbatch submission.
pub fn sbatch_native(argv: &[String], script: Option<String>, env: BTreeMap<String, String>) -> Native {
    let n = crate::adapter::SlurmNative::Sbatch { argv: argv.to_vec(), script, env };
    Native::Object { value: serde_json::to_value(&n).unwrap_or_default() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_accounts_and_patterns() {
        let m = parse_default_accounts("User|Def Acct|Admin\nalice|mlteam|None\nroot|root|Administrator\nbob||None\n");
        assert_eq!(m.get("alice").map(String::as_str), Some("mlteam"));
        assert!(!m.contains_key("bob"));
        assert!(tenant_matches("slurm://rsz-it/*", "slurm://rsz-it/mlteam"));
        assert!(tenant_matches("slurm://rsz-it/mlteam", "slurm://rsz-it/mlteam"));
        assert!(!tenant_matches("slurm://rsz-it/ml", "slurm://rsz-it/mlteam"));
    }

    #[test]
    fn account_resolution_and_associations() {
        let mut s = ServiceConfig::default();
        s.settings.cluster = Some("rsz-it".into());
        s.settings.default_account = Some("fallback".into());
        let svc = SlurmService::new(s);
        {
            let mut l = svc.live.write().unwrap();
            l.default_accounts.insert("alice".into(), "mlteam".into());
            l.settings.inventory.assocs = parse_sacctmgr_assoc("Cluster|Account|User|Partition|GrpTRES|MaxTRES|MaxTRESPerNode|QOS|Def QOS\nrsz-it|mlteam|alice||||||\n");
        }
        assert_eq!(svc.account_for(None, Some("alice")), "mlteam");
        assert_eq!(svc.account_for(Some("Other"), Some("alice")), "other");
        assert_eq!(svc.account_for(None, Some("bob")), "fallback");
        let alice = Identity { uid: 1000, user: Some("alice".into()) };
        let bob = Identity { uid: 1001, user: Some("bob".into()) };
        let t = tenant("rsz-it", "mlteam").unwrap();
        let none = |_: &str| false;
        assert!(svc.allowed(&t, &alice, &none));
        assert!(!svc.allowed(&t, &bob, &none));
        assert!(svc.allowed(&t, &bob, &|x: &str| tenant_matches("slurm://rsz-it/*", x)));
    }
}
