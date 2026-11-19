//! Chains from configuration (SPEC §6.2 default chain, §6.5 `[estimator]`).
//!
//! [`ChainSpec`] has the shape of `[estimator]` in `rightsizer.toml` (the
//! engine's `EstimatorConfig`): `chain = [...]` and `[estimator.members.NAME]`
//! tables. It ignores keys it does not use (pool and gate keys), so the
//! engine can convert its config with
//! `serde_json::from_value(serde_json::to_value(&config.estimator)?)`.
//!
//! Resolution of each chain entry `NAME` (kind = `members.NAME.kind`, else
//! `NAME`):
//!
//! | kind | member |
//! |---|---|
//! | `rules-sized`, `history-peak`, `prior` | the built-in (`params` overrides its parameters) |
//! | `gbt` | trees from `path` (alias `model_path`), novelty from `novelty` (alias `novelty_path`); without `path` it declines every context |
//! | `remote`, or any member with a `url` | Estimator Protocol v1 client (feature `remote`) |
//! | `pool`, `gate` | M4: skipped with a warning |
//!
//! **Default chain.** `rules-sized → history-peak → prior`; when the chain is
//! the default and `members.gbt.path` is set, `gbt` goes in front. A member
//! that cannot be built for a reason other than a broken file (unknown
//! kind, M4 kinds, `remote` without the feature) is skipped with a warning;
//! an unreadable or invalid model file is an error. An empty result gets
//! `prior` (S1).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::privacy::RemotePrivacy;
use crate::trees::ModelError;
use crate::{Estimator, EstimatorChain, Gbt, HistoryPeak, HistoryPeakParams, PriorEstimator, RulesSized, RulesSizedParams};

/// The default chain (SPEC §6.2).
pub fn default_chain() -> Vec<String> {
    vec!["rules-sized".into(), "history-peak".into(), "prior".into()]
}

/// `[estimator]`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ChainSpec {
    /// Top-down chain.
    pub chain: Vec<String>,
    /// Members by name.
    pub members: BTreeMap<String, MemberSpec>,
}

impl Default for ChainSpec {
    fn default() -> Self {
        ChainSpec { chain: default_chain(), members: BTreeMap::new() }
    }
}

/// `[estimator.members.NAME]`: the keys this crate reads.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct MemberSpec {
    /// Kind when the name is not one.
    pub kind: Option<String>,
    /// Estimator Protocol v1 base URL.
    pub url: Option<String>,
    /// Env var holding a bearer token.
    pub token_env: Option<String>,
    /// File holding a bearer token.
    pub token_file: Option<PathBuf>,
    /// Per-call deadline, ms (10 000).
    pub timeout_ms: Option<u64>,
    /// mTLS client certificate (PEM).
    pub client_cert: Option<PathBuf>,
    /// mTLS client key (PEM).
    pub client_key: Option<PathBuf>,
    /// Extra CA certificate (PEM).
    pub ca_cert: Option<PathBuf>,
    /// Cap on the server's `max_batch`.
    pub max_batch: Option<u32>,
    /// Coalescing window, ms (20).
    pub coalesce_ms: Option<u64>,
    /// Largest response body accepted from a remote estimator, bytes (8 MiB).
    pub max_body_bytes: Option<u64>,
    /// `rightsizer-trees/1` file (gbt).
    #[serde(alias = "model_path")]
    pub path: Option<PathBuf>,
    /// `rightsizer-novelty/1` file (gbt).
    #[serde(alias = "novelty_path")]
    pub novelty: Option<PathBuf>,
    /// Built-in parameter overrides (`RulesSizedParams`, `HistoryPeakParams`).
    pub params: Option<serde_json::Value>,
}

/// A built chain.
#[derive(Clone)]
pub struct BuiltChain {
    /// The chain.
    pub chain: EstimatorChain,
    /// Member names, top-down (as configured).
    pub names: Vec<String>,
    /// Members skipped and why.
    pub warnings: Vec<String>,
}

/// Errors building a chain.
#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    /// A model or novelty file is unreadable or invalid.
    #[error("estimator.members.{name}: {source}")]
    Model {
        /// Member name.
        name: String,
        /// Cause.
        source: ModelError,
    },
    /// Bad parameters or remote settings.
    #[error("estimator.members.{name}: {message}")]
    Config {
        /// Member name.
        name: String,
        /// Cause.
        message: String,
    },
}

fn params<T: serde::de::DeserializeOwned + Default>(name: &str, m: Option<&MemberSpec>) -> Result<T, BuildError> {
    match m.and_then(|m| m.params.clone()) {
        None => Ok(T::default()),
        Some(v) => serde_json::from_value(v).map_err(|e| BuildError::Config { name: name.into(), message: format!("params: {e}") }),
    }
}

/// Build the chain described by `spec`. Remote members get `privacy`.
pub async fn build_chain(spec: &ChainSpec, privacy: &RemotePrivacy) -> Result<BuiltChain, BuildError> {
    let mut names = spec.chain.clone();
    let gbt_configured = spec.members.get("gbt").is_some_and(|m| m.path.is_some());
    if names == default_chain() && gbt_configured && !names.iter().any(|n| n == "gbt") {
        names.insert(0, "gbt".into());
    }
    let mut members: Vec<Arc<dyn Estimator>> = Vec::new();
    let mut built_names = Vec::new();
    let mut warnings = Vec::new();
    for name in &names {
        let m = spec.members.get(name);
        let kind = m
            .and_then(|m| m.kind.clone())
            .unwrap_or_else(|| if m.is_some_and(|m| m.url.is_some()) { "remote".into() } else { name.clone() });
        let est: Option<Arc<dyn Estimator>> = match kind.as_str() {
            "rules-sized" => {
                let p = params::<RulesSizedParams>(name, m)?;
                p.validate().map_err(|message| BuildError::Config { name: name.clone(), message })?;
                Some(Arc::new(RulesSized::new(p)))
            }
            "history-peak" => {
                let p = params::<HistoryPeakParams>(name, m)?;
                p.validate().map_err(|message| BuildError::Config { name: name.clone(), message })?;
                Some(Arc::new(HistoryPeak::new(p)))
            }
            "prior" => Some(Arc::new(PriorEstimator::default())),
            "gbt" => {
                let g = match m.and_then(|m| m.path.as_deref()) {
                    Some(p) => Gbt::load(p, m.and_then(|m| m.novelty.as_deref()))
                        .map_err(|e| BuildError::Model { name: name.clone(), source: e })?,
                    None => {
                        warnings.push(format!("estimator.members.{name}: no model path; gbt declines every context"));
                        Gbt::default()
                    }
                };
                Some(Arc::new(if name != "gbt" { g.with_id(name) } else { g }))
            }
            "remote" => remote_member(name, m, privacy, &mut warnings).await?,
            "pool" | "gate" => {
                warnings.push(format!("estimator.members.{name}: {kind} arrives in M4; skipped"));
                None
            }
            other => {
                warnings.push(format!("estimator chain entry {name:?}: unknown estimator kind {other:?}; skipped"));
                None
            }
        };
        if let Some(e) = est {
            members.push(e);
            built_names.push(name.clone());
        }
    }
    if members.is_empty() {
        warnings.push("estimator chain is empty; using prior".into());
        members.push(Arc::new(PriorEstimator::default()));
        built_names.push("prior".into());
    }
    Ok(BuiltChain { chain: EstimatorChain::new(members), names: built_names, warnings })
}

#[cfg(feature = "remote")]
async fn remote_member(
    name: &str,
    m: Option<&MemberSpec>,
    privacy: &RemotePrivacy,
    _warnings: &mut Vec<String>,
) -> Result<Option<Arc<dyn Estimator>>, BuildError> {
    let cfg = crate::remote::RemoteConfig::from_member(name, m, privacy.clone())
        .map_err(|message| BuildError::Config { name: name.into(), message })?;
    let r = crate::remote::RemoteEstimator::connect(cfg).await.map_err(|message| BuildError::Config { name: name.into(), message })?;
    Ok(Some(Arc::new(r)))
}

#[cfg(not(feature = "remote"))]
async fn remote_member(
    name: &str,
    _m: Option<&MemberSpec>,
    _privacy: &RemotePrivacy,
    warnings: &mut Vec<String>,
) -> Result<Option<Arc<dyn Estimator>>, BuildError> {
    warnings.push(format!("estimator.members.{name}: remote estimators need the `remote` feature; skipped"));
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn build(toml_like: serde_json::Value) -> Result<BuiltChain, BuildError> {
        let spec: ChainSpec = serde_json::from_value(toml_like).unwrap();
        build_chain(&spec, &RemotePrivacy::default()).await
    }

    #[tokio::test]
    async fn default_chain_and_gbt_in_front() {
        let b = build(serde_json::json!({})).await.unwrap();
        assert_eq!(b.chain.ids(), vec!["rules-sized", "history-peak", "prior"]);
        let dir = std::env::temp_dir().join(format!("rsz-est-build-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("trees.json");
        std::fs::write(
            &p,
            r#"{"format":"rightsizer-trees/1","version":"v7","feature_names":["a"],"axes":{"memory":{"quantiles":[{"q":0.5,"base":1e9,"trees":[]}]}}}"#,
        )
        .unwrap();
        // The engine's field names (`path`, `novelty`) and the aliases both work; pool keys are ignored.
        let b = build(serde_json::json!({"members": {"gbt": {"model_path": p, "op": "ignored"}}})).await.unwrap();
        assert_eq!(b.chain.ids(), vec!["gbt", "rules-sized", "history-peak", "prior"]);
        assert_eq!(b.chain.members()[0].info().version, "v7");
        // An explicit chain is kept as written.
        let b = build(serde_json::json!({"chain": ["prior"], "members": {"gbt": {"path": p}}})).await.unwrap();
        assert_eq!(b.chain.ids(), vec!["prior"]);
        // A renamed gbt member.
        let b = build(serde_json::json!({"chain": ["trees2", "prior"], "members": {"trees2": {"kind": "gbt", "path": p}}})).await.unwrap();
        assert_eq!(b.chain.ids(), vec!["trees2", "prior"]);
        // A broken model file is an error.
        std::fs::write(&p, "{").unwrap();
        assert!(matches!(build(serde_json::json!({"members": {"gbt": {"path": p}}})).await, Err(BuildError::Model { .. })));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn warnings_and_params() {
        let b = build(serde_json::json!({"chain": ["gate", "mystery", "gbt"], "members": {"gate": {"kind": "gate"}}})).await.unwrap();
        assert_eq!(b.chain.ids(), vec!["gbt"]);
        assert_eq!(b.warnings.len(), 3, "{:?}", b.warnings);
        let b = build(serde_json::json!({"chain": ["nothing"]})).await.unwrap();
        assert_eq!(b.chain.ids(), vec!["prior"]);
        let b =
            build(serde_json::json!({"chain": ["history-peak"], "members": {"history-peak": {"params": {"k": 3, "margin": 1.0}}}})).await;
        assert!(b.is_ok());
        let bad = build(serde_json::json!({"chain": ["history-peak"], "members": {"history-peak": {"params": {"k": "x"}}}})).await;
        assert!(matches!(bad, Err(BuildError::Config { .. })));
    }

    /// Parameters that silently collapse every estimate to the 16 MiB floor
    /// (a zero or negative margin, a negative cold-start slope) or that
    /// undo the OOM bump are configuration errors, not estimates.
    #[tokio::test]
    async fn rejects_degenerate_params() {
        for (member, params) in [
            ("history-peak", serde_json::json!({"margin": 0.0})),
            ("history-peak", serde_json::json!({"bump_factor": 0.5})),
            ("history-peak", serde_json::json!({"sigma": -1.0})),
            ("rules-sized", serde_json::json!({"margin": -1.0})),
            ("rules-sized", serde_json::json!({"cold_default": {"base_mib": 256.0, "k": -2.0, "exponent": 1.0}})),
            ("rules-sized", serde_json::json!({"bump_add_mib": -100.0})),
        ] {
            let b = build(serde_json::json!({"chain": [member], "members": {member: {"params": params.clone()}}})).await;
            assert!(matches!(b, Err(BuildError::Config { .. })), "{member} {params}: accepted");
        }
    }
}
