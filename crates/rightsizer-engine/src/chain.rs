//! The estimator chain from `[estimator]` (SPEC §6.2, §6.5) and the privacy
//! settings remote members get (SPEC §11), via
//! `rightsizer_estimators::build_chain` and `rightsizer_features::Privacy`.

use rightsizer_estimators::{ChainSpec, EstimatorChain};

use crate::config::{Config, ConfigError, PrivacyConfig};

/// `[privacy]` as the features crate takes it; the salt is read from `salt_env`.
pub fn privacy(p: &PrivacyConfig) -> rightsizer_features::Privacy {
    rightsizer_features::Privacy {
        send_source: p.send_source,
        send: p.send.clone(),
        redact: p.redact.clone(),
        history_limit: p.history_limit,
        labels: p.labels.clone(),
        salt: p.salt_env.as_deref().and_then(|v| std::env::var(v).ok()).or_else(|| std::env::var("RIGHTSIZER_URI_SALT").ok()),
    }
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
