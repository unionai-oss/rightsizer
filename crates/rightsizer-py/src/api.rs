//! The JSON-in / JSON-out operations behind `rightsizer._core`, free of
//! PyO3 so they are unit-tested with plain `cargo test`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rightsizer_core::decide::prior_decision;
use rightsizer_core::units;
use rightsizer_core::{
    Constraints, Decision, DecisionInput, DecisionParams, Grid, Mode, PlatformRef, PlatformState, Prior, PriorReason, UnitId, WorkloadContext,
    WorkloadKey,
};
use rightsizer_engine::config::{CONFIG_ENV, ConfigEnv, apply_env_overrides};
use rightsizer_engine::{Config, ContextInput, DecideRequest, Engine, OutcomeReport, Policy, ReplayOptions};
use rightsizer_estimators::{Estimator, EstimatorChain};
use rightsizer_history::Role;
use serde::{Deserialize, Serialize};

/// The key used for a decision whose request could not be read at all.
pub const INVALID_KEY: &str = "python://unknown/invalid-request";

// ── configuration ────────────────────────────────────────────────────────

/// Load the configuration.
///
/// - `None`: exactly `Config::load()` (`RIGHTSIZER_CONFIG`, then
///   `./rightsizer.toml`, `$XDG_CONFIG_HOME/rightsizer/config.toml`,
///   `/etc/rightsizer/config.toml`, then `RIGHTSIZER__SECTION__KEY`).
/// - a path to an existing file: as if `RIGHTSIZER_CONFIG` named it.
/// - a JSON object or a TOML document: parsed, then the overrides.
///
/// `env_overrides = false` skips `RIGHTSIZER__*` for explicit documents
/// (tests, reproducible tooling). `store` overrides `history.uri`
/// (`rsz --store`).
pub fn load_config(arg: Option<&str>, store: Option<&str>, env_overrides: bool) -> Result<Config, String> {
    let env = ConfigEnv::from_process();
    let mut config = match arg.map(str::trim).filter(|s| !s.is_empty()) {
        None => Config::load_with(&env).map_err(|e| e.to_string())?.config,
        Some(s) if s.starts_with('{') => {
            let v: serde_json::Value = serde_json::from_str(s).map_err(|e| format!("configuration JSON: {e}"))?;
            let v = strip_nulls(v);
            let table: toml::Table = serde_json::from_value(v).map_err(|e| format!("configuration JSON: {e}"))?;
            from_table(table, &env, env_overrides)?
        }
        Some(s) if !s.contains('\n') && !s.contains('=') => {
            let p = PathBuf::from(s);
            let p = if p.is_absolute() { p } else { env.cwd.join(p) };
            if !p.is_file() {
                return Err(format!("configuration file not found: {}", p.display()));
            }
            if env_overrides {
                let mut e = env.clone();
                e.vars.insert(CONFIG_ENV.to_string(), p.display().to_string());
                Config::load_with(&e).map_err(|e| e.to_string())?.config
            } else {
                let text = std::fs::read_to_string(&p).map_err(|e| format!("{}: {e}", p.display()))?;
                Config::from_toml_str(&text).map_err(|e| e.to_string())?
            }
        }
        Some(s) => {
            let table: toml::Table = s.parse().map_err(|e: toml::de::Error| format!("configuration TOML: {e}"))?;
            from_table(table, &env, env_overrides)?
        }
    };
    if let Some(uri) = store {
        config.history.uri = Some(uri.to_string());
    }
    config.validate().map_err(|e| e.to_string())?;
    Ok(config)
}

fn strip_nulls(v: serde_json::Value) -> serde_json::Value {
    match v {
        serde_json::Value::Object(m) => serde_json::Value::Object(m.into_iter().filter(|(_, v)| !v.is_null()).map(|(k, v)| (k, strip_nulls(v))).collect()),
        serde_json::Value::Array(a) => serde_json::Value::Array(a.into_iter().map(strip_nulls).collect()),
        other => other,
    }
}

fn from_table(mut table: toml::Table, env: &ConfigEnv, env_overrides: bool) -> Result<Config, String> {
    if env_overrides {
        apply_env_overrides(&mut table, &env.vars).map_err(|e| e.to_string())?;
    }
    let text = toml::to_string(&table).map_err(|e| format!("configuration: {e}"))?;
    Config::from_toml_str(&text).map_err(|e| e.to_string())
}

/// Parse a role name.
pub fn parse_role(s: Option<&str>) -> Result<Role, String> {
    match s.unwrap_or("client") {
        "client" => Ok(Role::Client),
        "operator" => Ok(Role::Operator),
        other => Err(format!("unknown role {other:?} (client | operator)")),
    }
}

/// Build an engine. `front` estimators (Python estimators) go in front of
/// the configured chain. Configuration errors fail only under `strict`; the
/// engine is otherwise inert (S1).
pub async fn build_engine(config: Config, role: Role, front: Vec<Arc<dyn Estimator>>) -> Result<Engine, String> {
    let mut b = Engine::builder(config.clone()).role(role);
    if !front.is_empty() {
        // On a chain error the builder rebuilds the chain itself and records
        // the error (inert engine, or an error under `strict`).
        if let Ok(chain) = rightsizer_engine::chain::build_chain(&config, None).await {
            let mut members = front;
            members.extend(chain.members().iter().cloned());
            b = b.chain(EstimatorChain::new(members));
        }
    }
    b.build().await.map_err(|e| e.to_string())
}

// ── requests ─────────────────────────────────────────────────────────────

/// Parse a decide request; an unreadable one becomes a prior decision (S1:
/// decide never raises) built from whatever of the request is readable.
pub fn parse_request(json: &str) -> Result<DecideRequest, Box<Decision>> {
    match serde_json::from_str::<DecideRequest>(json) {
        Ok(r) => Ok(r),
        Err(e) => Err(Box::new(invalid_request_decision(json, &e.to_string()))),
    }
}

fn get<'a>(v: &'a serde_json::Value, path: &[&str]) -> Option<&'a serde_json::Value> {
    path.iter().try_fold(v, |v, k| v.get(k))
}

/// A prior decision for a request that does not parse.
pub fn invalid_request_decision(json: &str, error: &str) -> Decision {
    let v: serde_json::Value = serde_json::from_str(json).unwrap_or(serde_json::Value::Null);
    let pick = |a: &[&str], b: &[&str]| get(&v, a).or_else(|| get(&v, b)).cloned();
    let key = pick(&["context", "key"], &["context", "context", "key"])
        .and_then(|k| serde_json::from_value::<WorkloadKey>(k).ok())
        .unwrap_or_else(|| WorkloadKey::parse(INVALID_KEY).expect("valid fallback key"));
    let unit = pick(&["context", "unit"], &["context", "context", "unit"]).and_then(|u| serde_json::from_value::<UnitId>(u).ok()).unwrap_or_else(|| UnitId("main".into()));
    let prior = pick(&["context", "prior"], &["context", "context", "prior"]).and_then(|p| serde_json::from_value::<Prior>(p).ok()).unwrap_or_default();
    let mode = get(&v, &["policy", "mode"]).and_then(|m| serde_json::from_value::<Mode>(m.clone()).ok()).unwrap_or(Mode::Recommend).min(Mode::Recommend);
    let ctx = WorkloadContext {
        v: rightsizer_core::WIRE_VERSION,
        key,
        unit,
        platform: PlatformRef::new("python"),
        code: None,
        inputs: vec![],
        manifest: Default::default(),
        prior,
        history: vec![],
        labels: BTreeMap::new(),
    };
    let grid = Grid::rs_grid_v1();
    let (state, constraints, params) = (PlatformState::default(), Constraints::default(), DecisionParams::default());
    let mut input = DecisionInput::new(&ctx, None, &grid, &state, &constraints, &params, mode);
    input.id = ulid::Ulid::generate();
    let mut d = prior_decision(&input, PriorReason::Internal, Some(format!("invalid request: {error}")));
    d.mode = mode;
    d
}

/// A prior decision for a request that parsed but could not be decided
/// (no engine: configuration or runtime failure).
pub fn fallback_decision(req: &DecideRequest, reason: PriorReason, detail: String) -> Decision {
    let ctx = match &req.context {
        ContextInput::Full { context } => (**context).clone(),
        ContextInput::Seed { key, unit, prior, platform, .. } => WorkloadContext {
            v: rightsizer_core::WIRE_VERSION,
            key: key.clone(),
            unit: unit.clone(),
            platform: platform.clone(),
            code: None,
            inputs: vec![],
            manifest: Default::default(),
            prior: prior.clone(),
            history: vec![],
            labels: BTreeMap::new(),
        },
    };
    let mode = req.policy.mode.unwrap_or(Mode::Recommend).min(Mode::Recommend);
    let grid = Grid::rs_grid_v1();
    let params = DecisionParams::default();
    let mut input = DecisionInput::new(&ctx, None, &grid, &req.state, &req.constraints, &params, mode);
    input.id = ulid::Ulid::generate();
    let mut d = prior_decision(&input, reason, Some(detail));
    d.mode = mode;
    d
}

/// JSON of a decision (never fails for these types).
pub fn to_json<T: Serialize>(v: &T) -> String {
    serde_json::to_string(v).unwrap_or_else(|e| format!("{{\"error\":{:?}}}", e.to_string()))
}

/// `OutcomeReport` as JSON.
pub fn outcome_report(r: &OutcomeReport) -> serde_json::Value {
    serde_json::json!({
        "written": r.written,
        "joined": r.joined,
        "attribution": r.attribution,
        "tripped": r.tripped,
        "lease_closed": r.lease_closed,
    })
}

/// [`ReplayOptions`] on the wire (every field optional).
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReplayOpts {
    /// Runs started at or after this are decided.
    pub since: Option<chrono::DateTime<chrono::Utc>>,
    /// Static platform facts.
    pub platform: Option<PlatformRef>,
    /// Shapes and prices.
    pub state: Option<PlatformState>,
    /// Constraints.
    pub constraints: Option<Constraints>,
    /// Policy.
    pub policy: Option<Policy>,
    /// Bootstrap resamples.
    pub bootstrap: Option<usize>,
    /// Bootstrap seed.
    pub seed: Option<u64>,
    /// Keep per-run rows.
    pub per_run: Option<bool>,
}

impl ReplayOpts {
    /// Into the engine's options.
    pub fn into_options(self) -> ReplayOptions {
        let d = ReplayOptions::default();
        ReplayOptions {
            since: self.since,
            platform: self.platform,
            state: self.state.unwrap_or(d.state),
            constraints: self.constraints.unwrap_or(d.constraints),
            policy: self.policy.unwrap_or(d.policy),
            bootstrap: self.bootstrap.unwrap_or(d.bootstrap),
            seed: self.seed.unwrap_or(d.seed),
            per_run: self.per_run.unwrap_or(d.per_run),
        }
    }
}

// ── pure helpers ─────────────────────────────────────────────────────────

/// A quantity as Python passes it.
#[derive(Clone, Debug, PartialEq)]
pub enum QValue {
    /// A string (`"64Gi"`, `"500m"`, `"64G"`).
    Str(String),
    /// A number (bytes, cores; Slurm: MiB for memory).
    Num(f64),
}

fn num_text(v: f64) -> String {
    if v.fract() == 0.0 && v.abs() < 1e18 { format!("{}", v as i64) } else { format!("{v}") }
}

fn is_memory_axis(axis: &str) -> Result<bool, String> {
    match axis {
        "memory" | "mem" | "ephemeral_storage" | "shm" | "gpu_memory" | "disk" => Ok(true),
        "cpu" | "cpus" => Ok(false),
        other => Err(format!("unknown axis {other:?} (memory, cpu, ephemeral_storage, shm, gpu_memory)")),
    }
}

/// Normalize a platform quantity to wire units: bytes for memory-like axes,
/// millicores for CPU (rounded up, SPEC §5.1).
///
/// | platform | memory | cpu |
/// |---|---|---|
/// | `k8s`, `flyte` | K8s quantity (`"64Gi"`, `"129M"`, `"1e9"`); a number is bytes | K8s quantity (`"500m"`, `"2"`); a number is cores |
/// | `slurm` | `--mem` syntax (`"64G"`; no suffix = MiB); a number is MiB | CPUs (integer or decimal) |
/// | `ray` | bytes (number or numeric string) | `num_cpus` (fractional cores) |
pub fn normalize_quantity(value: &QValue, axis: &str, platform: &str) -> Result<u64, String> {
    let mem = is_memory_axis(axis)?;
    let text = match value {
        QValue::Str(s) => s.trim().to_string(),
        QValue::Num(n) => num_text(*n),
    };
    let r = match (platform, mem) {
        ("k8s" | "kubernetes" | "flyte" | "python", true) => units::parse_k8s_bytes(&text).map(|b| b.0),
        ("k8s" | "kubernetes" | "flyte" | "python", false) => units::parse_k8s_cpu(&text).map(|c| c.0 as u64),
        ("slurm", true) => units::parse_slurm_mem(&text).map(|b| b.0),
        ("slurm", false) => units::parse_k8s_cpu(&text).map(|c| c.0 as u64),
        ("ray", true) => {
            let v: f64 = text.parse().map_err(|_| format!("ray memory must be a number of bytes, got {text:?}"))?;
            units::parse_ray_memory(v).map(|b| b.0)
        }
        ("ray", false) => {
            let v: f64 = text.parse().map_err(|_| format!("ray num_cpus must be a number, got {text:?}"))?;
            units::parse_ray_cpus(v).map(|c| c.0 as u64)
        }
        (p, _) => return Err(format!("unknown platform {p:?} (k8s, flyte, slurm, ray)")),
    };
    r.map_err(|e| e.to_string())
}

/// Format wire units for a platform (the inverse of [`normalize_quantity`]).
pub fn format_quantity(value: u64, axis: &str, platform: &str) -> Result<serde_json::Value, String> {
    let mem = is_memory_axis(axis)?;
    Ok(match (platform, mem) {
        ("k8s" | "kubernetes" | "flyte" | "python", true) => units::format_k8s_bytes(value).into(),
        ("k8s" | "kubernetes" | "flyte" | "python", false) => {
            units::format_k8s_cpu(rightsizer_core::MilliCpu(u32::try_from(value).map_err(|_| "cpu out of range")?)).into()
        }
        ("slurm", true) => units::format_slurm_mem(rightsizer_core::Bytes(value)).into(),
        ("slurm", false) => serde_json::json!((value as f64 / 1000.0).ceil() as u64),
        ("ray", true) => serde_json::json!(value),
        ("ray", false) => serde_json::json!(value as f64 / 1000.0),
        (p, _) => return Err(format!("unknown platform {p:?} (k8s, flyte, slurm, ray)")),
    })
}

/// The memo feature bucket of some inputs (SPEC §6.3).
pub fn feature_bucket(inputs_json: &str, grid: Option<&str>) -> Result<String, String> {
    let inputs: Vec<rightsizer_core::InputMeta> = serde_json::from_str(inputs_json).map_err(|e| format!("inputs: {e}"))?;
    let g = match grid {
        None => Grid::rs_grid_v1(),
        Some(id) => Grid::by_id(id).ok_or_else(|| format!("unknown grid {id:?}"))?,
    };
    Ok(rightsizer_features::feature_bucket(&inputs, &g))
}

/// `CodeInfo` JSON for a source text.
pub fn code_info(source: &str, language: Option<&str>, keep_source: bool) -> String {
    let lang = language.and_then(rightsizer_features::Language::from_hint);
    to_json(&rightsizer_features::code_info(source, lang, keep_source))
}

/// The context digest (SPEC §5.2) of a `WorkloadContext`.
pub fn context_digest(ctx_json: &str) -> Result<String, String> {
    let ctx: WorkloadContext = serde_json::from_str(ctx_json).map_err(|e| format!("context: {e}"))?;
    Ok(rightsizer_core::digest::context_digest(&ctx))
}

/// Read input metadata for `InputRef`s within `budget_ms` (SSRF-guarded;
/// `allow_hosts` as in `[features]`).
pub async fn read_inputs(refs: Vec<rightsizer_core::InputRef>, budget_ms: u64, allow_hosts: Vec<String>) -> Vec<rightsizer_core::InputMeta> {
    let guard = rightsizer_features::SsrfGuard::new(&allow_hosts);
    let resolver = rightsizer_features::DefaultResolver::new(guard.clone());
    let budget = rightsizer_features::ReadBudget { per_context: std::time::Duration::from_millis(budget_ms.max(1)), ..Default::default() };
    let opts = rightsizer_features::ReadOptions::new(budget).with_guard(guard);
    rightsizer_features::read_inputs_with(&refs, &resolver, &opts).await
}

/// Whether a path names an existing file (config argument sniffing; tests).
pub fn is_file(p: &str) -> bool {
    Path::new(p).is_file()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_requests_become_prior_decisions() {
        let d = invalid_request_decision("not json", "expected value");
        assert_eq!(d.key.to_string(), INVALID_KEY);
        assert!(matches!(d.source, rightsizer_core::DecisionSource::Prior { reason: PriorReason::Internal, .. }));
        let j = r#"{"context":{"kind":"seed","key":"k8s://c/ns/job/x","unit":"main","prior":{"spec":{"requests":{"memory":1024}}}},"policy":{"mode":"enforce"},"budget_ms":"x"}"#;
        assert!(parse_request(j).is_err());
        let d = invalid_request_decision(j, "bad budget");
        assert_eq!(d.key.to_string(), "k8s://c/ns/job/x");
        assert_eq!(d.applied.requests.memory, Some(rightsizer_core::Bytes(1024)));
        assert_eq!(d.mode, Mode::Recommend, "never above recommend");
    }

    #[test]
    fn quantities() {
        let s = |x: &str| QValue::Str(x.into());
        assert_eq!(normalize_quantity(&s("64Gi"), "memory", "k8s").unwrap(), 64 << 30);
        assert_eq!(normalize_quantity(&s("500m"), "cpu", "flyte").unwrap(), 500);
        assert_eq!(normalize_quantity(&QValue::Num(8.0), "cpu", "k8s").unwrap(), 8000);
        assert_eq!(normalize_quantity(&QValue::Num(0.25), "cpu", "k8s").unwrap(), 250);
        assert_eq!(normalize_quantity(&s("64G"), "memory", "slurm").unwrap(), 64 << 30);
        assert_eq!(normalize_quantity(&QValue::Num(1536.0), "memory", "slurm").unwrap(), 1536 << 20);
        assert_eq!(normalize_quantity(&QValue::Num(1.5), "cpu", "ray").unwrap(), 1500);
        assert_eq!(normalize_quantity(&QValue::Num(1e9), "memory", "ray").unwrap(), 1_000_000_000);
        assert!(normalize_quantity(&s("lots"), "memory", "k8s").is_err());
        assert!(normalize_quantity(&s("1Gi"), "memory", "nomad").is_err());
        assert_eq!(format_quantity(64 << 30, "memory", "k8s").unwrap(), "64Gi");
        assert_eq!(format_quantity(1536 << 20, "memory", "slurm").unwrap(), "1536M");
        assert_eq!(format_quantity(2500, "cpu", "ray").unwrap(), 2.5);
    }

    #[test]
    fn config_sources() {
        let c = load_config(Some(r#"{"policy": {"mode": "enforce"}, "history": {"uri": null}}"#), Some("memory://"), false).unwrap();
        assert_eq!(c.policy.mode, Mode::Enforce);
        assert_eq!(c.history.uri.as_deref(), Some("memory://"));
        let c = load_config(Some("[policy]\nmode = \"observe\"\n"), None, false).unwrap();
        assert_eq!(c.policy.mode, Mode::Observe);
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("rs.toml");
        std::fs::write(&p, "strict = true\n").unwrap();
        assert!(load_config(Some(p.to_str().unwrap()), None, false).unwrap().strict);
        assert!(load_config(Some("/nonexistent/rightsizer.toml"), None, false).is_err());
        assert!(load_config(Some(r#"{"nope": 1}"#), None, false).is_err(), "unknown keys are errors");
    }

    #[test]
    fn replay_options_defaults() {
        let o: ReplayOpts = serde_json::from_str("{}").unwrap();
        let o = o.into_options();
        assert_eq!(o.bootstrap, 1000);
        assert!(serde_json::from_str::<ReplayOpts>(r#"{"bogus": 1}"#).is_err());
    }

    #[test]
    fn engine_decides_json() {
        let c = load_config(Some("{}"), Some("memory://"), false).unwrap();
        let e = crate::runtime::block_on(build_engine(c, Role::Client, vec![])).unwrap().unwrap();
        let req = r#"{"context":{"kind":"seed","key":"python://alice/train","unit":"main","seed":{"inputs":[{"name":"x","bytes":1073741824,"format":"parquet"}]},"prior":{"spec":{"requests":{"cpu":2000,"memory":8589934592}}},"platform":{"kind":"python"}}}"#;
        let r = parse_request(req).unwrap();
        let d = crate::runtime::block_on(async move { e.decide(r).await }).unwrap();
        assert_eq!(d.key.to_string(), "python://alice/train");
        assert_eq!(d.mode, Mode::Recommend);
        assert!(d.recommended.requests.memory.is_some());
    }
}
