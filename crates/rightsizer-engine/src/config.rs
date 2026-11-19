//! `rightsizer.toml` (ARCHITECTURE.md §5; SPEC §12.1): one [`Config`] shared
//! by every surface (CLI, daemon, webhook, Python).
//!
//! **Search order** (first found wins): `$RIGHTSIZER_CONFIG` (an error if it
//! names a missing file), `./rightsizer.toml`,
//! `$XDG_CONFIG_HOME/rightsizer/config.toml` (XDG default `~/.config`), then
//! `/etc/rightsizer/config.toml`. No file is fine: defaults apply.
//!
//! **Overrides**: any key can be set with `RIGHTSIZER__SECTION__KEY=value`
//! (nested: `RIGHTSIZER__ESTIMATOR__MEMBERS__GBT__PATH`). Segments are
//! lower-cased; values are parsed as TOML literals (`5`, `true`,
//! `["a","b"]`, `"x"`), a bare comma-separated list becomes an array of
//! strings, anything else a string. Keys containing `-` cannot be addressed
//! from the environment.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use rightsizer_core::{AcceleratorId, Mode, Penalties, RiskName, RiskProfile};
use serde::{Deserialize, Serialize};

/// Environment variable naming the config file.
pub const CONFIG_ENV: &str = "RIGHTSIZER_CONFIG";
/// Prefix of override variables.
pub const OVERRIDE_PREFIX: &str = "RIGHTSIZER__";

/// Configuration errors (exit code 2, SPEC §12.1).
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// `$RIGHTSIZER_CONFIG` names a file that does not exist.
    #[error("{CONFIG_ENV}={0} does not exist")]
    Missing(PathBuf),
    /// The file could not be read.
    #[error("reading {path}: {source}")]
    Io {
        /// Path.
        path: PathBuf,
        /// Cause.
        source: std::io::Error,
    },
    /// TOML syntax or schema error.
    #[error("{origin}: {message}")]
    Parse {
        /// File path or `"environment"`.
        origin: String,
        /// Message.
        message: String,
    },
    /// A semantically invalid value.
    #[error("invalid configuration: {0}")]
    Invalid(String),
}

/// The whole configuration.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Fail on configuration errors instead of running inert (CI; SPEC §2.3 S1).
    pub strict: bool,
    /// Mode and safety policy.
    pub policy: PolicyConfig,
    /// Risk profile (SPEC §7.3).
    pub risk: RiskConfig,
    /// Decision-rule tunables (SPEC §5.5, §7).
    pub decision: DecisionConfig,
    /// History store (SPEC §8).
    pub history: HistoryConfig,
    /// Estimator chain (SPEC §6).
    pub estimator: EstimatorConfig,
    /// What remote estimators may see (SPEC §11).
    pub privacy: PrivacyConfig,
    /// Breaker thresholds (SPEC §2.3 S5).
    pub breaker: BreakerConfig,
    /// Input metadata reading (SPEC §9).
    pub features: FeaturesConfig,
    /// Kubernetes adapter (kubernetes.md).
    pub k8s: K8sConfig,
    /// Slurm adapter (slurm.md).
    pub slurm: SlurmConfig,
    /// Flyte integration (flyte.md).
    pub flyte: FlyteConfig,
    /// Ray integration (ray.md).
    pub ray: RayConfig,
    /// Daemon (`rsz serve`, plugins.md §3).
    pub serve: ServeConfig,
    /// Out-of-process plugins by kind (plugins.md §3.4).
    pub plugins: BTreeMap<String, PluginConfig>,
}

/// `[policy]`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PolicyConfig {
    /// Requested mode.
    pub mode: Mode,
    /// Highest mode any surface may use.
    pub max_mode: Mode,
    /// Enforce on units with no prior (SPEC §7.4).
    pub enforce_without_prior: bool,
    /// Allow GPU type changes (SPEC §5.4).
    pub allow_gpu_type_change: bool,
    /// Allowed accelerators when type changes are allowed.
    pub allowed_accelerators: Option<Vec<AcceleratorId>>,
    /// Exploration ε (≤ 0.05; SPEC §7.6).
    pub exploration: f32,
    /// S3(b) measured successes needed.
    pub min_history: u32,
    /// Trust region width, cells.
    pub max_step_down: u32,
    /// Unclassified failures count as caused after this (SPEC §2.3 S5).
    #[serde(with = "humantime_serde")]
    pub classification_window: Duration,
    /// Ladder lease TTL past the failure (SPEC §7.2).
    #[serde(with = "humantime_serde")]
    pub lease_ttl: Duration,
    /// Decision budget, ms (expiry → prior).
    pub budget_ms: u32,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        PolicyConfig {
            mode: Mode::Recommend,
            max_mode: Mode::Enforce,
            enforce_without_prior: false,
            allow_gpu_type_change: false,
            allowed_accelerators: None,
            exploration: 0.0,
            min_history: 3,
            max_step_down: 2,
            classification_window: Duration::from_secs(3600),
            lease_ttl: Duration::from_secs(15 * 60),
            budget_ms: 2000,
        }
    }
}

/// `[risk]`: a named profile, optionally with overridden penalties.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RiskConfig {
    /// Named profile; `None` = per platform (`averse` without ladder or retries).
    pub profile: Option<RiskName>,
    /// Override `oom_penalty`.
    pub oom_penalty: Option<f64>,
    /// Override `delay_penalty`.
    pub delay_penalty: Option<f64>,
    /// Override `neighbour_penalty`.
    pub neighbour_penalty: Option<f64>,
}

impl RiskConfig {
    /// The resolved profile; `None` = choose per platform.
    pub fn profile(&self) -> Option<RiskProfile> {
        let base = self.profile.map(RiskProfile::Named);
        if self.oom_penalty.is_none() && self.delay_penalty.is_none() && self.neighbour_penalty.is_none() {
            return base;
        }
        let p = base.unwrap_or_default().penalties();
        Some(RiskProfile::Custom(Penalties {
            oom_penalty: self.oom_penalty.unwrap_or(p.oom_penalty),
            delay_penalty: self.delay_penalty.unwrap_or(p.delay_penalty),
            neighbour_penalty: self.neighbour_penalty.unwrap_or(p.neighbour_penalty),
        }))
    }
}

/// `[decision]`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DecisionConfig {
    /// Grid id.
    pub grid: String,
    /// Headroom on demand + overhead (1.05).
    pub need_headroom: f64,
    /// σ for point estimates (0.35).
    pub point_sigma: f64,
    /// Same-key history records in a context (32).
    pub history_limit: usize,
}

impl Default for DecisionConfig {
    fn default() -> Self {
        DecisionConfig { grid: "rs-grid-v1".into(), need_headroom: 1.05, point_sigma: 0.35, history_limit: 32 }
    }
}

/// History backends.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum HistoryBackend {
    /// Object store (default for Flyte, Ray, Slurm and multi-host use).
    ObjectStore,
    /// sqlite (single-user CLI).
    Sqlite,
    /// postgres (webhook / watcher).
    Postgres,
}

/// `[history]`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HistoryConfig {
    /// Store URI (`s3://bucket/prefix`, `file:///var/lib/rightsizer`, `sqlite:///…`, `postgres://…`).
    pub uri: Option<String>,
    /// Backend; `None` = inferred from the URI scheme.
    pub backend: Option<HistoryBackend>,
    /// Wire version written (SPEC §13.2).
    pub write_version: u16,
    /// Admit `Unverified` records for floors, anchors and S3(b) (SPEC §8.2).
    pub allow_unverified_for_reductions: bool,
    /// Per-key history cache TTL (SPEC §8.4).
    #[serde(with = "humantime_serde")]
    pub cache_ttl: Duration,
    /// Clusters whose histories are merged (SPEC §8.5).
    pub share: Vec<String>,
    /// This cluster's name (records always carry it).
    pub cluster: Option<String>,
    /// Records kept per key in pointers (64).
    pub keep_last: usize,
    /// Shard retention.
    #[serde(with = "humantime_serde")]
    pub max_age: Duration,
    /// Write buffer bound when the store is unreachable (10k).
    pub buffer: usize,
}

impl Default for HistoryConfig {
    fn default() -> Self {
        HistoryConfig {
            uri: None,
            backend: None,
            write_version: 1,
            allow_unverified_for_reductions: false,
            cache_ttl: Duration::from_secs(600),
            share: Vec::new(),
            cluster: None,
            keep_last: 64,
            max_age: Duration::from_secs(90 * 86_400),
            buffer: 10_000,
        }
    }
}

impl HistoryConfig {
    /// The backend, explicit or inferred from the URI scheme.
    pub fn resolved_backend(&self) -> Option<HistoryBackend> {
        if let Some(b) = self.backend {
            return Some(b);
        }
        let uri = self.uri.as_deref()?;
        Some(if uri.starts_with("sqlite:") {
            HistoryBackend::Sqlite
        } else if uri.starts_with("postgres:") || uri.starts_with("postgresql:") {
            HistoryBackend::Postgres
        } else {
            HistoryBackend::ObjectStore
        })
    }
}

/// `[estimator]`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EstimatorConfig {
    /// Top-down chain; the first non-declining answer wins (SPEC §6.5).
    pub chain: Vec<String>,
    /// Members by name: built-ins configured, remotes, pools, gates.
    pub members: BTreeMap<String, MemberConfig>,
}

impl Default for EstimatorConfig {
    fn default() -> Self {
        EstimatorConfig { chain: vec!["rules-sized".into(), "history-peak".into(), "prior".into()], members: BTreeMap::new() }
    }
}

/// One `[estimator.members.NAME]` table. Which keys apply depends on the member.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MemberConfig {
    /// Built-in kind when the name is not one (`"gbt"`, `"remote"`, `"pool"`, `"gate"`).
    pub kind: Option<String>,
    /// Estimator Protocol v1 base URL.
    pub url: Option<String>,
    /// Env var holding a bearer token.
    pub token_env: Option<String>,
    /// File holding a bearer token.
    pub token_file: Option<PathBuf>,
    /// Per-call timeout, ms.
    pub timeout_ms: Option<u64>,
    /// `rightsizer-trees/1` model file (gbt).
    pub path: Option<PathBuf>,
    /// `rightsizer-novelty/1` companion (gbt).
    pub novelty: Option<PathBuf>,
    /// Pool operator (`"log-linear"`).
    pub op: Option<String>,
    /// Pool / gate members.
    pub members: Option<Vec<String>>,
    /// Pool weights.
    pub weights: Option<Vec<f64>>,
    /// Gate signal source.
    pub signal_from: Option<String>,
    /// Gate threshold (an ECDF value).
    pub threshold: Option<f64>,
    /// Gate: familiar branch.
    pub familiar: Option<String>,
    /// Gate: novel branch.
    pub novel: Option<String>,
    /// Gate: when the signal is missing (`"novel"`).
    pub on_missing_signal: Option<String>,
}

/// `[privacy]` (SPEC §11).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PrivacyConfig {
    /// Send full source to remote estimators.
    pub send_source: bool,
    /// Extra fields to send (`"columns"`, `"labels:team"`).
    pub send: Vec<String>,
    /// Fields to redact.
    pub redact: Vec<String>,
    /// History records sent.
    pub history_limit: usize,
    /// Env var holding the URI-hash salt.
    pub salt_env: Option<String>,
    /// Labels allowed into contexts.
    pub labels: Vec<String>,
}

impl Default for PrivacyConfig {
    fn default() -> Self {
        PrivacyConfig { send_source: false, send: Vec::new(), redact: Vec::new(), history_limit: 32, salt_env: None, labels: Vec::new() }
    }
}

/// `[breaker]`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BreakerConfig {
    /// Shadow fits before a per-workload breaker goes half-open (5).
    pub recovery_shadow_fits: u32,
    /// Breaker / control cache (60 s).
    #[serde(with = "humantime_serde")]
    pub cache_ttl: Duration,
    /// Per-estimator-version rate (0.02).
    pub estimator_rate: f64,
    /// Per-estimator-version minimum decisions (50).
    pub estimator_min: usize,
    /// Fleet fast-burn rate (0.05).
    pub fast_rate: f64,
    /// Fleet fast-burn minimum failures (20).
    pub fast_min_failures: usize,
    /// Fleet slow-burn rate (0.005).
    pub slow_rate: f64,
    /// Fleet slow-burn minimum invocations (200).
    pub slow_min_invocations: usize,
}

impl Default for BreakerConfig {
    fn default() -> Self {
        BreakerConfig {
            recovery_shadow_fits: 5,
            cache_ttl: Duration::from_secs(60),
            estimator_rate: 0.02,
            estimator_min: 50,
            fast_rate: 0.05,
            fast_min_failures: 20,
            slow_rate: 0.005,
            slow_min_invocations: 200,
        }
    }
}

/// `[features]` (SPEC §9).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FeaturesConfig {
    /// Per-context read budget, ms (1000).
    pub budget_ms: u32,
    /// Bytes read per input (1 MiB).
    pub max_input_bytes: u64,
    /// Hosts HTTP(S) readers may reach despite private addresses.
    pub allow_hosts: Vec<String>,
}

impl Default for FeaturesConfig {
    fn default() -> Self {
        FeaturesConfig { budget_ms: 1000, max_input_bytes: 1 << 20, allow_hosts: Vec::new() }
    }
}

/// `[k8s]` (kubernetes.md).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct K8sConfig {
    /// Cluster name (tenant prefix).
    pub cluster: Option<String>,
    /// Node label carrying the GPU product.
    pub gpu_product_label: String,
    /// Namespaces sized (empty = all not excluded).
    pub namespaces: Vec<String>,
    /// Namespaces never sized.
    pub exclude_namespaces: Vec<String>,
    /// Webhook listen address.
    pub webhook_listen: String,
    /// TLS certificate.
    pub tls_cert: Option<PathBuf>,
    /// TLS key.
    pub tls_key: Option<PathBuf>,
    /// Webhook internal deadline, ms (shorter than the API server's).
    pub webhook_timeout_ms: u32,
    /// HMAC key file for the signed prior annotation.
    pub signing_key_file: Option<PathBuf>,
    /// Prometheus URL for the importer.
    pub prometheus_url: Option<String>,
}

impl Default for K8sConfig {
    fn default() -> Self {
        K8sConfig {
            cluster: None,
            gpu_product_label: "nvidia.com/gpu.product".into(),
            namespaces: Vec::new(),
            exclude_namespaces: vec!["kube-system".into()],
            webhook_listen: "0.0.0.0:8443".into(),
            tls_cert: None,
            tls_key: None,
            webhook_timeout_ms: 2000,
            signing_key_file: None,
            prometheus_url: None,
        }
    }
}

/// `[slurm]` (slurm.md).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SlurmConfig {
    /// Cluster name (tenant prefix).
    pub cluster: Option<String>,
    /// HMAC token key file.
    pub token_key_file: Option<PathBuf>,
    /// `sacct` binary.
    pub sacct: Option<PathBuf>,
    /// `sbatch` binary.
    pub sbatch: Option<PathBuf>,
    /// Canonical accelerator → gres name (`"A100-80G" = "gpu:a100_80g"`).
    pub gres: BTreeMap<String, String>,
    /// Whether `ConstrainRAMSpace` is on (memory enforced).
    pub constrain_ram_space: Option<bool>,
}

/// `[flyte]` (flyte.md).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FlyteConfig {
    /// Flyte endpoint for `flyte.remote` backfill.
    pub endpoint: Option<String>,
    /// Python interpreter for `rsz flyte …` delegation.
    pub python: Option<PathBuf>,
}

/// `[ray]` (ray.md).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RayConfig {
    /// Deployment-configured tenant (`"team-a/cluster-1"`).
    pub tenant: Option<String>,
    /// Ray dashboard / state API address.
    pub address: Option<String>,
    /// Python interpreter for `rsz ray …` delegation.
    pub python: Option<PathBuf>,
}

/// `[serve]` (plugins.md §3).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServeConfig {
    /// Unix socket path.
    pub socket: Option<PathBuf>,
    /// HTTP listen address.
    pub listen: Option<String>,
    /// uid → tenants a caller may act for (`SO_PEERCRED`).
    pub tenants_by_uid: BTreeMap<String, Vec<String>>,
    /// Prometheus metrics listen address.
    pub metrics_listen: Option<String>,
}

/// `[plugins.KIND]` (plugins.md §3.4).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PluginConfig {
    /// Absolute path (required in server modes).
    pub path: PathBuf,
    /// Pinned SHA-256 of the binary.
    pub sha256: Option<String>,
    /// Extra arguments.
    pub args: Vec<String>,
    /// Extra environment.
    pub env: BTreeMap<String, String>,
    /// Per-request deadline, ms.
    pub deadline_ms: Option<u32>,
}

/// The process environment, injectable for tests.
#[derive(Clone, Debug, Default)]
pub struct ConfigEnv {
    /// Environment variables.
    pub vars: BTreeMap<String, String>,
    /// Working directory.
    pub cwd: PathBuf,
    /// System config root (`/etc`).
    pub etc: PathBuf,
}

impl ConfigEnv {
    /// The real process environment.
    pub fn from_process() -> Self {
        ConfigEnv {
            vars: std::env::vars().collect(),
            cwd: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            etc: PathBuf::from("/etc"),
        }
    }

    /// Candidate config paths, in search order.
    pub fn candidates(&self) -> Vec<PathBuf> {
        let mut v = vec![self.cwd.join("rightsizer.toml")];
        let xdg = self
            .vars
            .get("XDG_CONFIG_HOME")
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .or_else(|| self.vars.get("HOME").filter(|s| !s.is_empty()).map(|h| Path::new(h).join(".config")));
        if let Some(x) = xdg {
            v.push(x.join("rightsizer").join("config.toml"));
        }
        v.push(self.etc.join("rightsizer").join("config.toml"));
        v
    }
}

/// A loaded configuration and where it came from.
#[derive(Clone, Debug, PartialEq)]
pub struct Loaded {
    /// The configuration.
    pub config: Config,
    /// The file read, if any.
    pub source: Option<PathBuf>,
    /// Override variables applied, sorted.
    pub overrides: Vec<String>,
}

/// The config file to read (SPEC §12.1 search order).
pub fn find_config_file(env: &ConfigEnv) -> Result<Option<PathBuf>, ConfigError> {
    if let Some(p) = env.vars.get(CONFIG_ENV).filter(|s| !s.is_empty()) {
        let p = if Path::new(p).is_absolute() { PathBuf::from(p) } else { env.cwd.join(p) };
        return if p.is_file() { Ok(Some(p)) } else { Err(ConfigError::Missing(p)) };
    }
    Ok(env.candidates().into_iter().find(|p| p.is_file()))
}

/// Parse an override value: a TOML literal, else a bare comma list, else a string.
fn parse_override_value(raw: &str) -> toml::Value {
    let doc = format!("v = {raw}");
    if let Ok(t) = doc.parse::<toml::Table>()
        && let Some(v) = t.get("v")
    {
        return v.clone();
    }
    if raw.contains(',') {
        return toml::Value::Array(raw.split(',').map(|s| toml::Value::String(s.trim().to_string())).filter(|v| v.as_str() != Some("")).collect());
    }
    toml::Value::String(raw.to_string())
}

/// Apply `RIGHTSIZER__A__B=v` overrides to a TOML table; returns the names applied.
pub fn apply_env_overrides(table: &mut toml::Table, vars: &BTreeMap<String, String>) -> Result<Vec<String>, ConfigError> {
    let mut applied = Vec::new();
    for (name, raw) in vars {
        let Some(rest) = name.strip_prefix(OVERRIDE_PREFIX) else { continue };
        let path: Vec<String> = rest.split("__").map(|s| s.to_ascii_lowercase()).collect();
        if path.iter().any(|s| s.is_empty()) {
            return Err(ConfigError::Parse { origin: "environment".into(), message: format!("{name}: empty key segment") });
        }
        let mut cur = &mut *table;
        for seg in &path[..path.len() - 1] {
            let entry = cur.entry(seg.clone()).or_insert_with(|| toml::Value::Table(toml::Table::new()));
            cur = match entry {
                toml::Value::Table(t) => t,
                _ => {
                    return Err(ConfigError::Parse { origin: "environment".into(), message: format!("{name}: {seg} is not a table") });
                }
            };
        }
        cur.insert(path[path.len() - 1].clone(), parse_override_value(raw));
        applied.push(name.clone());
    }
    Ok(applied)
}

impl Config {
    /// Parse a TOML document (no overrides).
    pub fn from_toml_str(s: &str) -> Result<Config, ConfigError> {
        let table: toml::Table = s.parse().map_err(|e: toml::de::Error| ConfigError::Parse { origin: "<string>".into(), message: e.to_string() })?;
        Self::from_table(table, "<string>")
    }

    fn from_table(table: toml::Table, origin: &str) -> Result<Config, ConfigError> {
        let c: Config = toml::Value::Table(table)
            .try_into()
            .map_err(|e: toml::de::Error| ConfigError::Parse { origin: origin.into(), message: e.to_string() })?;
        c.validate()?;
        Ok(c)
    }

    /// Load from the process environment (search order + overrides).
    pub fn load() -> Result<Loaded, ConfigError> {
        Self::load_with(&ConfigEnv::from_process())
    }

    /// Load from an explicit environment.
    pub fn load_with(env: &ConfigEnv) -> Result<Loaded, ConfigError> {
        let source = find_config_file(env)?;
        let mut table = match &source {
            Some(p) => {
                let text = std::fs::read_to_string(p).map_err(|e| ConfigError::Io { path: p.clone(), source: e })?;
                text.parse::<toml::Table>().map_err(|e| ConfigError::Parse { origin: p.display().to_string(), message: e.to_string() })?
            }
            None => toml::Table::new(),
        };
        let overrides = apply_env_overrides(&mut table, &env.vars)?;
        let origin = match (&source, overrides.is_empty()) {
            (Some(p), true) => p.display().to_string(),
            (Some(p), false) => format!("{} + environment", p.display()),
            (None, _) => "environment".into(),
        };
        let config = Self::from_table(table, &origin)?;
        Ok(Loaded { config, source, overrides })
    }

    /// Semantic validation (`rsz config validate`).
    pub fn validate(&self) -> Result<(), ConfigError> {
        let bad = |m: String| Err(ConfigError::Invalid(m));
        if !(0.0..=0.05).contains(&self.policy.exploration) {
            return bad(format!("policy.exploration must be in [0, 0.05], got {}", self.policy.exploration));
        }
        if self.policy.min_history == 0 {
            return bad("policy.min_history must be ≥ 1".into());
        }
        if self.decision.need_headroom.is_nan() || self.decision.need_headroom < 1.0 {
            return bad(format!("decision.need_headroom must be ≥ 1, got {}", self.decision.need_headroom));
        }
        if self.decision.point_sigma.is_nan() || self.decision.point_sigma <= 0.0 {
            return bad("decision.point_sigma must be > 0".into());
        }
        if rightsizer_core::Grid::by_id(&self.decision.grid).is_none() {
            return bad(format!("decision.grid: unknown grid {:?}", self.decision.grid));
        }
        for p in [self.risk.oom_penalty, self.risk.delay_penalty, self.risk.neighbour_penalty].into_iter().flatten() {
            if p.is_nan() || p < 0.0 {
                return bad(format!("risk penalties must be ≥ 0, got {p}"));
            }
        }
        if self.estimator.chain.is_empty() {
            return bad("estimator.chain must not be empty".into());
        }
        for (name, m) in &self.estimator.members {
            if let (Some(ms), Some(ws)) = (&m.members, &m.weights)
                && ms.len() != ws.len()
            {
                return bad(format!("estimator.members.{name}: {} members but {} weights", ms.len(), ws.len()));
            }
        }
        if self.history.resolved_backend() == Some(HistoryBackend::Postgres) && self.history.uri.is_none() {
            return bad("history.backend = postgres needs history.uri".into());
        }
        for (kind, p) in &self.plugins {
            if p.path.as_os_str().is_empty() {
                return bad(format!("plugins.{kind}.path is required"));
            }
        }
        Ok(())
    }

    /// Plugins usable in server modes: only absolute paths (plugins.md §3.4).
    pub fn server_plugins(&self) -> impl Iterator<Item = (&String, &PluginConfig)> {
        self.plugins.iter().filter(|(_, p)| p.path.is_absolute())
    }

    /// Render as TOML (`rsz config show`).
    pub fn to_toml(&self) -> String {
        toml::to_string_pretty(self).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(dir: &Path, vars: &[(&str, &str)]) -> ConfigEnv {
        ConfigEnv {
            vars: vars.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            cwd: dir.join("cwd"),
            etc: dir.join("etc"),
        }
    }

    fn write(p: &Path, s: &str) {
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, s).unwrap();
    }

    #[test]
    fn defaults_without_any_file() {
        let d = tempfile::tempdir().unwrap();
        let l = Config::load_with(&env(d.path(), &[])).unwrap();
        assert_eq!(l.source, None);
        assert_eq!(l.config, Config::default());
        assert_eq!(l.config.policy.mode, Mode::Recommend);
        assert_eq!(l.config.estimator.chain, vec!["rules-sized", "history-peak", "prior"]);
        assert_eq!(l.config.history.cache_ttl, Duration::from_secs(600));
    }

    #[test]
    fn search_order() {
        let d = tempfile::tempdir().unwrap();
        let etc = d.path().join("etc/rightsizer/config.toml");
        let xdg = d.path().join("xdg/rightsizer/config.toml");
        let home = d.path().join("home/.config/rightsizer/config.toml");
        let cwd = d.path().join("cwd/rightsizer.toml");
        let explicit = d.path().join("explicit.toml");
        write(&etc, "[policy]\nmin_history = 11\n");
        let e = env(d.path(), &[("HOME", d.path().join("home").to_str().unwrap())]);
        assert_eq!(Config::load_with(&e).unwrap().config.policy.min_history, 11);
        write(&home, "[policy]\nmin_history = 12\n");
        assert_eq!(Config::load_with(&e).unwrap().config.policy.min_history, 12, "XDG default ~/.config");
        write(&xdg, "[policy]\nmin_history = 13\n");
        let e = env(d.path(), &[("HOME", d.path().join("home").to_str().unwrap()), ("XDG_CONFIG_HOME", d.path().join("xdg").to_str().unwrap())]);
        assert_eq!(Config::load_with(&e).unwrap().config.policy.min_history, 13);
        write(&cwd, "[policy]\nmin_history = 14\n");
        assert_eq!(Config::load_with(&e).unwrap().config.policy.min_history, 14);
        write(&explicit, "[policy]\nmin_history = 15\n");
        let mut e2 = e.clone();
        e2.vars.insert(CONFIG_ENV.into(), explicit.to_str().unwrap().into());
        let l = Config::load_with(&e2).unwrap();
        assert_eq!(l.config.policy.min_history, 15);
        assert_eq!(l.source.as_deref(), Some(explicit.as_path()));
        e2.vars.insert(CONFIG_ENV.into(), d.path().join("nope.toml").to_str().unwrap().into());
        assert!(matches!(Config::load_with(&e2), Err(ConfigError::Missing(_))));
    }

    #[test]
    fn env_overrides() {
        let d = tempfile::tempdir().unwrap();
        write(&d.path().join("cwd/rightsizer.toml"), "[policy]\nmode = \"observe\"\n[history]\nuri = \"s3://b/p\"\n");
        let e = env(
            d.path(),
            &[
                ("RIGHTSIZER__POLICY__MODE", "enforce"),
                ("RIGHTSIZER__POLICY__MIN_HISTORY", "5"),
                ("RIGHTSIZER__HISTORY__CACHE_TTL", "5m"),
                ("RIGHTSIZER__HISTORY__SHARE", "a,b"),
                ("RIGHTSIZER__HISTORY__ALLOW_UNVERIFIED_FOR_REDUCTIONS", "true"),
                ("RIGHTSIZER__ESTIMATOR__CHAIN", "[\"gbt\", \"prior\"]"),
                ("RIGHTSIZER__ESTIMATOR__MEMBERS__GBT__PATH", "/models/trees.json"),
                ("RIGHTSIZER__RISK__PROFILE", "averse"),
                ("RIGHTSIZER__STRICT", "true"),
                ("RIGHTSIZER_CONFIG_UNRELATED", "x"),
            ],
        );
        let l = Config::load_with(&e).unwrap();
        let c = &l.config;
        assert_eq!(c.policy.mode, Mode::Enforce);
        assert_eq!(c.policy.min_history, 5);
        assert_eq!(c.history.cache_ttl, Duration::from_secs(300));
        assert_eq!(c.history.share, vec!["a", "b"]);
        assert!(c.history.allow_unverified_for_reductions);
        assert_eq!(c.history.uri.as_deref(), Some("s3://b/p"), "file values survive");
        assert_eq!(c.estimator.chain, vec!["gbt", "prior"]);
        assert_eq!(c.estimator.members["gbt"].path.as_deref(), Some(Path::new("/models/trees.json")));
        assert_eq!(c.risk.profile(), Some(RiskProfile::AVERSE));
        assert!(c.strict);
        assert_eq!(l.overrides.len(), 9);
    }

    #[test]
    fn errors() {
        assert!(matches!(Config::from_toml_str("[policy]\nmood = \"x\"\n"), Err(ConfigError::Parse { .. })), "unknown keys are errors");
        assert!(matches!(Config::from_toml_str("[policy]\nmode = \"turbo\"\n"), Err(ConfigError::Parse { .. })));
        assert!(matches!(Config::from_toml_str("[policy]\nexploration = 0.5\n"), Err(ConfigError::Invalid(_))));
        assert!(matches!(Config::from_toml_str("[decision]\ngrid = \"nope\"\n"), Err(ConfigError::Invalid(_))));
        assert!(matches!(Config::from_toml_str("[history]\nbackend = \"postgres\"\n"), Err(ConfigError::Invalid(_))));
        assert!(Config::from_toml_str("[estimator.members.pool]\nmembers = [\"a\", \"b\"]\nweights = [1.0]\n").is_err());
        let d = tempfile::tempdir().unwrap();
        let e = env(d.path(), &[("RIGHTSIZER__POLICY__MIN_HISTORY", "lots")]);
        assert!(Config::load_with(&e).is_err());
        let e = env(d.path(), &[("RIGHTSIZER__POLICY____X", "1")]);
        assert!(Config::load_with(&e).is_err());
    }

    #[test]
    fn full_document_round_trips() {
        let doc = r#"
strict = false
[policy]
mode = "enforce"
classification_window = "2h"
allowed_accelerators = ["A100-80G", "H100-80G"]
[risk]
profile = "lean"
oom_penalty = 2.5
[history]
uri = "sqlite:///tmp/rs.db"
[estimator]
chain = ["gate", "rules-sized", "history-peak", "prior"]
[estimator.members.decision]
url = "https://rightsizer-model.internal/v1"
[estimator.members.pool]
op = "log-linear"
members = ["gbt", "decision"]
weights = [0.7, 0.3]
[estimator.members.gate]
signal_from = "gbt"
threshold = 0.95
familiar = "pool"
novel = "rules-sized"
on_missing_signal = "novel"
[plugins.nomad]
path = "/opt/rightsizer/plugins/rsz-plugin-nomad"
sha256 = "9f2c"
[slurm.gres]
"A100-80G" = "gpu:a100_80g"
"#;
        let c = Config::from_toml_str(doc).unwrap();
        assert_eq!(c.policy.classification_window, Duration::from_secs(7200));
        assert_eq!(c.history.resolved_backend(), Some(HistoryBackend::Sqlite));
        assert_eq!(c.risk.profile().unwrap().penalties().oom_penalty, 2.5);
        assert_eq!(c.server_plugins().count(), 1);
        assert_eq!(c.estimator.members["gate"].threshold, Some(0.95));
        let again = Config::from_toml_str(&c.to_toml()).unwrap();
        assert_eq!(again, c);
    }
}
