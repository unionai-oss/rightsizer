//! The estimator chain from `[estimator]` (SPEC §6.2, §6.5) and the privacy
//! settings remote members get (SPEC §11), via
//! `rightsizer_estimators::build_chain` and `rightsizer_features::Privacy`.

use rightsizer_estimators::{ChainSpec, EstimatorChain};

use crate::config::{Config, ConfigError, PrivacyConfig};

/// `[privacy]` as the features crate takes it, with the salt from [`resolve_salt`].
pub fn privacy(p: &PrivacyConfig) -> rightsizer_features::Privacy {
    rightsizer_features::Privacy {
        send_source: p.send_source,
        send: p.send.clone(),
        redact: p.redact.clone(),
        history_limit: p.history_limit,
        labels: p.labels.clone(),
        salt: resolve_salt(p).0,
    }
}

/// The URI-hash salt (SPEC §11), the engine's single source for every
/// reader it configures (`ReadOptions`, contexts, the K8s prefetcher via
/// `Engine::read_options`): the env var named by `privacy.salt_env`, else
/// `RIGHTSIZER_URI_SALT`, else none (hashes use the empty salt). A variable
/// that is set but **empty** is refused — it would silently make hashes
/// unsalted and guessable — and the next source is used; the second value
/// is the warning to log (a configuration error under `strict`).
pub fn resolve_salt(p: &PrivacyConfig) -> (Option<String>, Option<String>) {
    let mut warnings = Vec::new();
    let mut vars: Vec<&str> = Vec::new();
    if let Some(v) = p.salt_env.as_deref() {
        vars.push(v);
    }
    vars.push(rightsizer_features::privacy::DEFAULT_SALT_ENV);
    let mut salt = None;
    for v in vars {
        match std::env::var(v) {
            Ok(s) if !s.is_empty() => {
                salt = Some(s);
                break;
            }
            Ok(_) => warnings.push(format!("privacy: the URI salt variable {v} is set but empty (refused)")),
            Err(_) => {}
        }
    }
    (salt, (!warnings.is_empty()).then(|| warnings.join("; ")))
}

/// Build the chain named by `names` (or `[estimator] chain`). A broken
/// model file or bad member table is a configuration error. Members the
/// builder skips (unknown or M4 kinds; an empty chain falls back to
/// `prior`) are logged, and are configuration errors under `strict`.
pub async fn build_chain(config: &Config, names: Option<&[String]>) -> Result<EstimatorChain, ConfigError> {
    let mut spec: ChainSpec = serde_json::to_value(&config.estimator)
        .and_then(serde_json::from_value)
        .map_err(|e| ConfigError::Invalid(format!("estimator: {e}")))?;
    if let Some(n) = names {
        spec.chain = n.to_vec();
    }
    let built = rightsizer_estimators::build_chain(&spec, &privacy(&config.privacy)).await.map_err(|e| ConfigError::Invalid(e.to_string()))?;
    for w in &built.warnings {
        tracing::warn!(warning = %w, "estimator chain");
    }
    if config.strict && !built.warnings.is_empty() {
        return Err(ConfigError::Invalid(built.warnings.join("; ")));
    }
    Ok(built.chain)
}
