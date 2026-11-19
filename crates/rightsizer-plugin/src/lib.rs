//! `rightsizer-plugin`: the platform plugin interface (plugins.md §2) and
//! the `rightsizer-plugin/1` protocol (plugins.md §3, [`protocol`]).
//!
//! A plugin implements an [`Adapter`] (native submission ⇄ units and
//! patches), a [`PlatformModel`] (static [`PlatformRef`] and live
//! [`PlatformState`]) and optionally an [`OutcomeSource`]; none of them touch
//! estimators or the decision rule.
//!
//! Modules:
//! - [`protocol`]: message types; [`rpc`]: NDJSON framing and N/N-1 version
//!   negotiation;
//! - [`server`]: the serving loop (concurrency, `$/cancel`, `deadline_ms`)
//!   and the Unix-socket listener with peer credentials (shim → core);
//! - [`client`]: long-lived stdio plugin processes and [`client::PluginAdapter`]
//!   (core → plugin);
//! - [`compose`]: the `decide` composite (extract → engine → render → validate);
//! - [`discovery`]: `PATH` / config discovery and sha256 pins;
//! - [`conformance`]: `rsz plugin check`.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};

pub use rightsizer_core::{
    AcceleratorId, Constraints, ContextSeed, GroupId, InputRef, CodeRef, PlatformRef, PlatformState, Prior,
    ResourceSpec, RunRecord, Tenant, UnitId, WorkloadKey,
};

pub mod client;
pub mod compose;
pub mod conformance;
pub mod discovery;
pub mod protocol;
pub mod rpc;
pub mod server;

/// A native submission, as the plugin received it (plugins.md §2).
/// JSON: `{"kind": "document", "media_type": ..., "bytes": "<base64>"}`, …
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Native {
    /// YAML/JSON manifests, sbatch scripts, pipeline definitions.
    Document {
        /// Media type (`application/yaml`, `text/x-shellscript`).
        media_type: String,
        /// The bytes (base64 on the wire).
        #[serde(with = "b64")]
        #[cfg_attr(feature = "schema", schemars(with = "String"))]
        bytes: Vec<u8>,
    },
    /// Admission request objects, Slurm option tables, Ray option dicts.
    Object {
        /// The object.
        value: serde_json::Value,
    },
    /// A cloud API request before signing (`"sagemaker"` / `"CreateTrainingJob"`).
    ApiCall {
        /// Service.
        service: String,
        /// Operation.
        operation: String,
        /// Parameters.
        params: serde_json::Value,
    },
}

mod b64 {
    use base64::Engine as _;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(b: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&base64::engine::general_purpose::STANDARD.encode(b))
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(d)?;
        base64::engine::general_purpose::STANDARD.decode(s).map_err(serde::de::Error::custom)
    }
}

/// One resource-bearing thing in a submission (plugins.md §2).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Unit {
    /// Stable within the submission, e.g. `"spec.template.spec.containers[trainer]"`.
    pub id: UnitId,
    /// Units sized identically (replicas of one replica type, array tasks).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<GroupId>,
    /// The user-overridable part of the workload key.
    pub key_suffix: String,
    /// The origin prior.
    pub prior: Prior,
    /// Input refs, code ref, manifest features, labels.
    #[serde(default)]
    pub seed: ContextSeed,
    /// Pins, platform holds, floor, ceiling, allowed shapes (SPEC §7.4).
    #[serde(default)]
    pub constraints: Constraints,
    /// SPEC §5.4: GPU count permanently held.
    #[serde(default)]
    pub distributed: bool,
    /// The full workload key, when it is not `{adapter kind}://{tenant}/{key_suffix}`
    /// (e.g. a Flyte task pod sized by the K8s webhook is keyed
    /// `flyte://{org}/{project}/{domain}/{task}`, kubernetes.md §2). The
    /// adapter derives it from trusted data only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "schema", schemars(with = "Option<String>"))]
    pub key: Option<WorkloadKey>,
}

impl Unit {
    /// The unit's workload key: [`Unit::key`] when set, else
    /// `{kind}://{tenant}/{key_suffix}`.
    pub fn workload_key(&self, kind: &str, tenant: &Tenant) -> Result<WorkloadKey, rightsizer_core::TypeError> {
        match &self.key {
            Some(k) => Ok(k.clone()),
            None => WorkloadKey::new(kind, tenant.clone(), self.key_suffix.clone()),
        }
    }
}

/// Why a submission is skipped (plugins.md §2). JSON: `{"kind": "already_sized", ...}`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SkipReason {
    /// Another layer already sized it (decision marker present).
    AlreadySized {
        /// The marker's decision id.
        decision_id: Option<String>,
    },
    /// Queue-managed (Kueue, Volcano): never changed by this layer.
    QueueManaged,
    /// The user opted out.
    OptedOut,
    /// No resource-bearing units.
    NoUnits,
    /// Not handled by this plugin.
    Unsupported {
        /// Detail.
        detail: String,
    },
}

/// What `extract` returns (plugins.md §2).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Extracted {
    /// Derived from the trusted request, never from user fields.
    pub tenant: Tenant,
    /// Units.
    #[serde(default)]
    pub units: Vec<Unit>,
    /// Why nothing should be changed, if so.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skip: Option<SkipReason>,
}

/// An RFC 6902 JSON Patch operation (K8s admission responses).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum JsonPatchOp {
    /// `add`.
    Add {
        /// JSON pointer.
        path: String,
        /// Value.
        value: serde_json::Value,
    },
    /// `remove`.
    Remove {
        /// JSON pointer.
        path: String,
    },
    /// `replace`.
    Replace {
        /// JSON pointer.
        path: String,
        /// Value.
        value: serde_json::Value,
    },
    /// `move`.
    Move {
        /// Source pointer.
        from: String,
        /// Target pointer.
        path: String,
    },
    /// `copy`.
    Copy {
        /// Source pointer.
        from: String,
        /// Target pointer.
        path: String,
    },
    /// `test`.
    Test {
        /// JSON pointer.
        path: String,
        /// Expected value.
        value: serde_json::Value,
    },
}

/// A byte-range edit; comments and formatting elsewhere are preserved.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct TextEdit {
    /// Start byte offset (inclusive).
    pub start: usize,
    /// End byte offset (exclusive).
    pub end: usize,
    /// Replacement text.
    pub replacement: String,
}

/// Apply non-overlapping text edits to `text`. Edits are applied from the
/// end so offsets refer to the original text; overlapping or out-of-range
/// edits are an error.
pub fn apply_text_edits(text: &str, edits: &[TextEdit]) -> Result<String, PluginError> {
    let mut sorted: Vec<&TextEdit> = edits.iter().collect();
    sorted.sort_by_key(|e| (e.start, e.end));
    for w in sorted.windows(2) {
        if w[0].end > w[1].start {
            return Err(PluginError::InvalidPatch("overlapping text edits".into()));
        }
    }
    let mut out = text.to_string();
    for e in sorted.iter().rev() {
        if e.start > e.end || e.end > out.len() || !out.is_char_boundary(e.start) || !out.is_char_boundary(e.end) {
            return Err(PluginError::InvalidPatch(format!("edit {}..{} out of range", e.start, e.end)));
        }
        out.replace_range(e.start..e.end, &e.replacement);
    }
    Ok(out)
}

/// A rendered change (plugins.md §2). JSON: `{"kind": "json_patch", "ops": [...]}`, …
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Patch {
    /// Nothing to change.
    #[default]
    None,
    /// K8s admission responses.
    JsonPatch {
        /// Operations.
        ops: Vec<JsonPatchOp>,
    },
    /// Byte-range edits (YAML, sbatch).
    TextEdits {
        /// Edits.
        edits: Vec<TextEdit>,
    },
    /// Option tables (Slurm cli_filter/job_submit, Ray `.options()`, Flyte Resources).
    Options {
        /// Options.
        options: serde_json::Map<String, serde_json::Value>,
    },
}

impl Patch {
    /// Whether nothing changes.
    pub fn is_none(&self) -> bool {
        match self {
            Patch::None => true,
            Patch::JsonPatch { ops } => ops.is_empty(),
            Patch::TextEdits { edits } => edits.is_empty(),
            Patch::Options { options } => options.is_empty(),
        }
    }
}

/// Why `validate` rejected a patch: the core drops it and records
/// `Prior{reason = rejected}` (plugins.md §2).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[error("rejected: {reason}")]
pub struct Rejection {
    /// Machine-readable reason (`"exceeds_node_shape"`, `"limitrange"`, `"quota"`).
    pub reason: String,
    /// Detail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// The offending field, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
}

/// What the hosting surface tells an adapter (cluster, caller identity, deadline).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct AdapterEnv {
    /// Cluster name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster: Option<String>,
    /// Authenticated caller (admission `userInfo.username`, Slurm `submit_uid`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal: Option<String>,
    /// Authenticated namespace / account / project.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Remaining budget, ms.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_ms: Option<u32>,
    /// Adapter-specific settings (the `[k8s]`, `[slurm]`, … config section).
    #[serde(default)]
    pub settings: serde_json::Map<String, serde_json::Value>,
}

/// Canonical accelerator ids ⇄ a platform's native names (SPEC §5.1.1).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct AcceleratorMap {
    /// Canonical → native.
    pub to_native: BTreeMap<AcceleratorId, String>,
    /// Native → canonical (several native names may map to one id).
    pub from_native: BTreeMap<String, AcceleratorId>,
}

impl AcceleratorMap {
    /// Add a pair (both directions).
    pub fn insert(&mut self, id: AcceleratorId, native: impl Into<String>) {
        let n = native.into();
        self.from_native.insert(n.clone(), id);
        self.to_native.entry(id).or_insert(n);
    }
    /// Native name for a canonical id.
    pub fn native(&self, id: AcceleratorId) -> Option<&str> {
        self.to_native.get(&id).map(String::as_str)
    }
    /// Canonical id for a native name; unknown names are `unknown-gpu`.
    pub fn canonical(&self, native: &str) -> AcceleratorId {
        self.from_native.get(native).copied().unwrap_or(AcceleratorId::UnknownGpu)
    }
}

/// Plugin errors; codes mirror the protocol's (plugins.md §3.3).
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum PluginError {
    /// The native submission could not be parsed.
    #[error("parse error: {0}")]
    Parse(String),
    /// Not handled by this plugin.
    #[error("unsupported: {0}")]
    Unsupported(String),
    /// The native submission is invalid.
    #[error("invalid native submission: {0}")]
    InvalidNative(String),
    /// A patch is malformed.
    #[error("invalid patch: {0}")]
    InvalidPatch(String),
    /// The platform could not be reached (live state, outcomes).
    #[error("platform unavailable: {0}")]
    PlatformUnavailable(String),
    /// Anything else.
    #[error("internal: {0}")]
    Internal(String),
}

impl PluginError {
    /// The protocol error code.
    pub fn code(&self) -> protocol::ErrorCode {
        use protocol::ErrorCode::*;
        match self {
            PluginError::Parse(_) => ParseError,
            PluginError::Unsupported(_) => Unsupported,
            PluginError::InvalidNative(_) | PluginError::InvalidPatch(_) => InvalidNative,
            PluginError::PlatformUnavailable(_) => PlatformUnavailable,
            PluginError::Internal(_) => Internal,
        }
    }
}

/// Plugin result.
pub type Result<T, E = PluginError> = std::result::Result<T, E>;

/// Native submission ⇄ units and patches (plugins.md §2).
#[async_trait]
pub trait Adapter: Send + Sync {
    /// Platform kind (`"k8s"`, `"slurm"`, `"ray"`, …).
    fn kind(&self) -> &str;
    /// Find the resource-bearing units; derive the tenant from trusted data.
    async fn extract(&self, native: &Native, env: &AdapterEnv) -> Result<Extracted>;
    /// Render decisions as a minimal patch.
    async fn render(&self, native: &Native, decisions: &[(UnitId, ResourceSpec)]) -> Result<Patch>;
    /// Check a patch against everything the platform will check after it.
    async fn validate(&self, native: &Native, patch: &Patch, env: &AdapterEnv) -> Result<(), Rejection>;
}

/// Static platform facts and live state (plugins.md §2).
#[async_trait]
pub trait PlatformModel: Send + Sync {
    /// SPEC §5.2: static, part of the context digest.
    fn reference(&self, unit: &Unit) -> PlatformRef;
    /// Live shapes, prices, quota; given to the decision rule only.
    async fn state(&self, tenant: &Tenant, env: &AdapterEnv) -> Result<PlatformState>;
    /// Canonical ⇄ native accelerator names.
    fn accelerators(&self) -> &AcceleratorMap;
}

/// Where run records come from (plugins.md §2). Classification and censoring
/// happen inside the source (SPEC §5.7).
#[async_trait]
pub trait OutcomeSource: Send + Sync {
    /// Records since `since`, optionally only for `keys`.
    async fn fetch(&self, since: DateTime<Utc>, keys: Option<&[WorkloadKey]>) -> Result<BoxStream<'_, Result<RunRecord>>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_document_is_base64() {
        let n = Native::Document { media_type: "text/x-shellscript".into(), bytes: b"#!/bin/bash\n".to_vec() };
        let j = serde_json::to_value(&n).unwrap();
        assert_eq!(j["kind"], "document");
        assert_eq!(j["bytes"], "IyEvYmluL2Jhc2gK");
        assert_eq!(serde_json::from_value::<Native>(j).unwrap(), n);
    }

    #[test]
    fn patch_forms() {
        let p = Patch::JsonPatch { ops: vec![JsonPatchOp::Replace { path: "/spec/x".into(), value: serde_json::json!("1Gi") }] };
        let j = serde_json::to_value(&p).unwrap();
        assert_eq!(j, serde_json::json!({"kind": "json_patch", "ops": [{"op": "replace", "path": "/spec/x", "value": "1Gi"}]}));
        assert!(Patch::None.is_none());
        assert!(Patch::TextEdits { edits: vec![] }.is_none());
    }

    #[test]
    fn text_edits_apply_from_the_end() {
        let t = "#SBATCH --mem=64G\n#SBATCH -c 8\n";
        let edits = vec![
            TextEdit { start: 14, end: 17, replacement: "24G".into() },
            TextEdit { start: 29, end: 30, replacement: "4".into() },
        ];
        assert_eq!(apply_text_edits(t, &edits).unwrap(), "#SBATCH --mem=24G\n#SBATCH -c 4\n");
        assert!(apply_text_edits(t, &[TextEdit { start: 0, end: 5, replacement: String::new() }, TextEdit { start: 3, end: 6, replacement: String::new() }]).is_err());
        assert!(apply_text_edits(t, &[TextEdit { start: 0, end: 500, replacement: String::new() }]).is_err());
    }

    #[test]
    fn accelerator_map() {
        let mut m = AcceleratorMap::default();
        m.insert(AcceleratorId::A100_80G, "NVIDIA-A100-SXM4-80GB");
        m.insert(AcceleratorId::A100_80G, "NVIDIA-A100-80GB-PCIe");
        assert_eq!(m.canonical("NVIDIA-A100-80GB-PCIe"), AcceleratorId::A100_80G);
        assert_eq!(m.native(AcceleratorId::A100_80G), Some("NVIDIA-A100-SXM4-80GB"));
        assert_eq!(m.canonical("Tesla-K80"), AcceleratorId::UnknownGpu);
    }
}
