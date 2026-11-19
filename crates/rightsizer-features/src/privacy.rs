//! Privacy (SPEC §11): salted URI hashes, label allowlists and the view of a
//! context that remote estimators receive.
//!
//! In-process estimators see the whole context. Remote estimators get
//! [`redact_for_remote`]'s output:
//!
//! | field | remote default | override |
//! |---|---|---|
//! | code digest, tokens | sent | — |
//! | full source | dropped | `send_source = true` |
//! | input URIs | never present (salted hash only) | — |
//! | column names, string scalars (inputs and `manifest.extra`), labels | dropped (counts and kinds stay) | `send = ["columns", "strings", "labels", "labels:<key>"]` |
//! | numeric scalars, sizes, rows, shapes, dtypes, parameter counts | sent | `redact = ["scalars", "sizes", "rows", "shapes", "dtypes", "params", "columns"]` |
//! | history | sent, ≤ `history_limit` | `history_limit` |
//!
//! `redact` and `send` apply to the inputs recorded in history records too.

use std::collections::BTreeMap;

use rightsizer_core::{InputMeta, ScalarValue, WorkloadContext};
use sha2::{Digest, Sha256};

/// Env var read by [`Privacy::from_env`] for the URI salt when the config
/// names none.
pub const DEFAULT_SALT_ENV: &str = "RIGHTSIZER_URI_SALT";

/// Privacy settings, mirroring `[privacy]` (SPEC §11,
/// `rightsizer-engine::config::PrivacyConfig`) with the salt resolved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Privacy {
    /// Send full source to remote estimators.
    pub send_source: bool,
    /// Extra fields remote estimators receive: `"columns"` (column names),
    /// `"strings"` (string scalars), `"labels"` (all allowlisted labels),
    /// `"labels:<key>"` (one label).
    pub send: Vec<String>,
    /// Fields withheld from remote estimators: `"scalars"` (numeric
    /// scalars), `"sizes"` (bytes), `"rows"`, `"shapes"`, `"dtypes"`,
    /// `"params"`, `"columns"` (column counts too).
    pub redact: Vec<String>,
    /// History records kept in a context, and sent (32).
    pub history_limit: usize,
    /// Labels allowed into contexts at all (`"*"` = every label). Default:
    /// none.
    pub labels: Vec<String>,
    /// Salt for [`uri_hash`]. `None` hashes with an empty salt, which is
    /// deterministic but guessable: deployments should set one.
    pub salt: Option<String>,
}

impl Default for Privacy {
    fn default() -> Self {
        Privacy {
            send_source: false,
            send: Vec::new(),
            redact: Vec::new(),
            history_limit: WorkloadContext::HISTORY_LIMIT,
            labels: Vec::new(),
            salt: None,
        }
    }
}

impl Privacy {
    /// Defaults, with the salt read from `salt_env` (or
    /// [`DEFAULT_SALT_ENV`] when `None`).
    pub fn from_env(salt_env: Option<&str>) -> Self {
        Privacy { salt: std::env::var(salt_env.unwrap_or(DEFAULT_SALT_ENV)).ok().filter(|s| !s.is_empty()), ..Privacy::default() }
    }

    fn sends(&self, what: &str) -> bool {
        self.send.iter().any(|s| s == what)
    }
    fn redacts(&self, what: &str) -> bool {
        self.redact.iter().any(|s| s == what)
    }

    /// Whether label `key` may enter a context.
    pub fn label_allowed(&self, key: &str) -> bool {
        self.labels.iter().any(|l| l == "*" || l == key)
    }

    /// Whether label `key` is sent to remote estimators.
    pub fn label_sent(&self, key: &str) -> bool {
        self.sends("labels") || self.send.iter().any(|s| s.strip_prefix("labels:") == Some(key))
    }

    /// The allowlisted subset of `labels`.
    pub fn allow_labels(&self, labels: &BTreeMap<String, String>) -> BTreeMap<String, String> {
        labels.iter().filter(|(k, _)| self.label_allowed(k)).map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    /// `uri_hash` with this config's salt.
    pub fn hash_uri(&self, uri: &str) -> String {
        uri_hash(uri, self.salt.as_deref().unwrap_or(""))
    }

    /// One input as remote estimators may see it.
    pub fn redact_input(&self, m: &InputMeta) -> InputMeta {
        let mut m = m.clone();
        if let Some(c) = m.columns.as_mut()
            && !self.sends("columns")
        {
            c.names = None;
        }
        if self.redacts("columns") {
            m.columns = None;
        }
        match &m.scalar {
            Some(ScalarValue::Str(_)) if !self.sends("strings") => m.scalar = None,
            Some(ScalarValue::Int(_) | ScalarValue::Float(_) | ScalarValue::Bool(_)) if self.redacts("scalars") => m.scalar = None,
            _ => {}
        }
        if self.redacts("sizes") {
            m.bytes = None;
        }
        if self.redacts("rows") {
            m.rows = None;
        }
        if self.redacts("shapes") {
            m.shape = None;
        }
        if self.redacts("dtypes") {
            m.dtype = None;
        }
        if self.redacts("params") {
            m.params = None;
        }
        m
    }
}

/// Salted URI hash: `sha256:` + hex(SHA-256(`"rightsizer-uri/1\0"` + salt +
/// `"\0"` + uri)). Deterministic per salt; the URI itself never leaves the
/// process.
pub fn uri_hash(uri: &str, salt: &str) -> String {
    let mut h = Sha256::new();
    h.update(b"rightsizer-uri/1\0");
    h.update(salt.as_bytes());
    h.update(b"\0");
    h.update(uri.as_bytes());
    format!("sha256:{}", hex::encode(h.finalize()))
}

/// The context a remote estimator receives (SPEC §11): source dropped unless
/// `send_source`; column names, string scalars and labels dropped unless in
/// `send`; `redact` fields removed from inputs (and history inputs); history
/// truncated to `history_limit`.
pub fn redact_for_remote(ctx: &WorkloadContext, privacy: &Privacy) -> WorkloadContext {
    let mut out = ctx.clone();
    if let Some(code) = out.code.as_mut()
        && !privacy.send_source
    {
        code.source = None;
    }
    out.inputs = ctx.inputs.iter().map(|i| privacy.redact_input(i)).collect();
    // Adapter extras are scalars too (e.g. the K8s container image, which
    // names private registries): strings only with `send = ["strings"]`.
    if !privacy.sends("strings") {
        out.manifest.extra.retain(|_, v| !matches!(v, ScalarValue::Str(_)));
    }
    out.labels = ctx.labels.iter().filter(|(k, _)| privacy.label_sent(k)).map(|(k, v)| (k.clone(), v.clone())).collect();
    out.history.truncate(privacy.history_limit);
    for r in &mut out.history {
        r.inputs = r.inputs.iter().map(|i| privacy.redact_input(i)).collect();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uri_hash_is_salted_and_stable() {
        let a = uri_hash("s3://b/k", "salt");
        assert_eq!(a, uri_hash("s3://b/k", "salt"));
        assert_ne!(a, uri_hash("s3://b/k", "pepper"));
        assert_ne!(a, uri_hash("s3://b/k2", "salt"));
        assert!(a.starts_with("sha256:") && a.len() == 7 + 64);
        assert!(!a.contains("s3://"));
    }

    #[test]
    fn labels() {
        let p = Privacy { labels: vec!["team".into()], send: vec!["labels:team".into()], ..Privacy::default() };
        let mut l = BTreeMap::new();
        l.insert("team".to_string(), "ml".to_string());
        l.insert("owner".to_string(), "ann".to_string());
        assert_eq!(p.allow_labels(&l).len(), 1);
        assert!(p.label_sent("team") && !p.label_sent("owner"));
        assert!(Privacy { labels: vec!["*".into()], ..Privacy::default() }.label_allowed("x"));
        assert!(!Privacy::default().label_allowed("x"));
    }
}
