//! `WorkloadContext`, the information boundary (SPEC §5.2), and the context
//! seed an adapter hands the engine (plugins.md §2).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::{Bytes, PlatformRetry, Prior, RunRecord, UnitId, WorkloadKey};

/// Everything that may be known before the run (SPEC §5.2). Nothing measured
/// on the run being decided is in here.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct WorkloadContext {
    /// Wire version (1).
    #[serde(default = "crate::wire_v")]
    pub v: u16,
    /// Workload key.
    pub key: WorkloadKey,
    /// Unit within the submission.
    pub unit: UnitId,
    /// Static platform facts (part of the context digest).
    pub platform: PlatformRef,
    /// Code identity and tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<CodeInfo>,
    /// Input metadata, ≤ 64, largest first then by name (deterministic truncation).
    #[serde(default)]
    pub inputs: Vec<InputMeta>,
    /// Adapter-derived manifest features.
    #[serde(default)]
    pub manifest: ManifestFeatures,
    /// The author's prior.
    pub prior: Prior,
    /// Same-key history, newest first, ≤ `history_limit` (32).
    #[serde(default)]
    pub history: Vec<RunRecord>,
    /// Allowlisted labels (SPEC §11).
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

impl WorkloadContext {
    /// Maximum number of inputs kept (SPEC §5.2).
    pub const MAX_INPUTS: usize = 64;
    /// Default `history_limit` (SPEC §5.2).
    pub const HISTORY_LIMIT: usize = 32;

    /// Total input bytes of this invocation, if any input has a size.
    pub fn input_bytes(&self) -> Option<u64> {
        total_input_bytes(&self.inputs)
    }

    /// Apply the deterministic truncation rules of SPEC §5.2: inputs sorted
    /// largest first (unknown sizes last), then by name, truncated to 64;
    /// history truncated to `history_limit`.
    pub fn canonicalize(&mut self, history_limit: usize) {
        self.inputs.sort_by(|a, b| b.bytes.unwrap_or(0).cmp(&a.bytes.unwrap_or(0)).then_with(|| a.name.cmp(&b.name)));
        self.inputs.truncate(Self::MAX_INPUTS);
        self.history.truncate(history_limit);
    }
}

/// Sum of known input sizes; `None` when no input has a size.
pub fn total_input_bytes(inputs: &[InputMeta]) -> Option<u64> {
    let mut any = false;
    let mut sum = 0u64;
    for i in inputs {
        if let Some(b) = i.bytes {
            any = true;
            sum = sum.saturating_add(b);
        }
    }
    any.then_some(sum)
}

/// Static facts about the platform (SPEC §5.2): part of the context digest,
/// so nothing here may change minute to minute.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct PlatformRef {
    /// Platform kind (`"k8s"`, `"slurm"`, …).
    pub kind: String,
    /// Platform version string.
    #[serde(default)]
    pub version: String,
    /// Runtime overhead distribution.
    #[serde(default)]
    pub overhead: OverheadModel,
    /// Retry semantics (SPEC §7.2).
    #[serde(default)]
    pub retry: PlatformRetry,
    /// Walltime limit, seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub walltime_s: Option<u64>,
    /// CPU measurement window, seconds.
    #[serde(default = "default_cpu_window")]
    pub cpu_window_s: u32,
    /// Whether the platform enforces memory such that an under-ask fails the
    /// unit itself (SPEC §2.3 S3).
    #[serde(default = "default_true")]
    pub memory_enforced: bool,
}

fn default_cpu_window() -> u32 {
    15
}
fn default_true() -> bool {
    true
}

impl PlatformRef {
    /// A minimal reference for `kind` with defaults.
    pub fn new(kind: impl Into<String>) -> Self {
        PlatformRef {
            kind: kind.into(),
            version: String::new(),
            overhead: OverheadModel::default(),
            retry: PlatformRetry::default(),
            walltime_s: None,
            cpu_window_s: default_cpu_window(),
            memory_enforced: true,
        }
    }
}

/// Runtime overhead: memory the platform runtime holds before user code
/// allocates (SPEC §5.2), as quantiles in MiB, e.g. Flyte
/// `[[0.5, 70], [0.9, 242]]`. Empty = no overhead.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct OverheadModel {
    /// `(quantile level, MiB)` pairs.
    #[serde(default)]
    pub memory_mib: Vec<(f32, f64)>,
    /// Fixed CPU overhead, millicores.
    #[serde(default)]
    pub cpu_millis: u32,
}

impl OverheadModel {
    /// The overhead as a small discrete distribution `(weight, bytes)`.
    ///
    /// Each given quantile stands for the probability mass between the
    /// midpoints of its neighbours' levels (the first from 0, the last to 1),
    /// which puts the upper tail on the top quantile: conservative.
    pub fn memory_points(&self) -> Vec<(f64, f64)> {
        let mut q: Vec<(f64, f64)> =
            self.memory_mib.iter().filter(|(p, v)| p.is_finite() && v.is_finite()).map(|&(p, v)| (p as f64, v.max(0.0))).collect();
        if q.is_empty() {
            return vec![(1.0, 0.0)];
        }
        q.sort_by(|a, b| a.0.total_cmp(&b.0));
        let mut vals: Vec<f64> = q.iter().map(|x| x.1).collect();
        vals.sort_by(|a, b| a.total_cmp(b));
        let n = q.len();
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let lo = if i == 0 { 0.0 } else { 0.5 * (q[i - 1].0 + q[i].0) };
            let hi = if i + 1 == n { 1.0 } else { 0.5 * (q[i].0 + q[i + 1].0) };
            out.push(((hi - lo).max(0.0), vals[i] * Bytes::MIB as f64));
        }
        let total: f64 = out.iter().map(|x| x.0).sum();
        if total > 0.0 {
            for x in &mut out {
                x.0 /= total;
            }
        }
        out
    }

    /// Median overhead in bytes (0 when empty).
    pub fn median_bytes(&self) -> f64 {
        let pts = self.memory_points();
        let mut acc = 0.0;
        for (w, v) in &pts {
            acc += w;
            if acc >= 0.5 - 1e-12 {
                return *v;
            }
        }
        pts.last().map(|p| p.1).unwrap_or(0.0)
    }
}

/// Code identity (SPEC §5.2).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct CodeInfo {
    /// Normalized digest (comments, whitespace and resource directives stripped).
    pub digest: String,
    /// `features-v1` code tokens.
    #[serde(default)]
    pub tokens: Vec<String>,
    /// Source text, only if privacy allows; truncated at 64 KiB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

/// Input formats readers understand (SPEC §5.2, §9).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum InputFormat {
    /// Apache Parquet.
    Parquet,
    /// CSV.
    Csv,
    /// JSON lines.
    Jsonl,
    /// NumPy `.npy`.
    Npy,
    /// safetensors.
    Safetensors,
    /// Arrow IPC (M3).
    ArrowIpc,
    /// A directory listing.
    Dir,
    /// Opaque bytes.
    Blob,
    /// A scalar parameter.
    Scalar,
    /// A model reference (e.g. `hf://`).
    ModelRef,
}

/// Column counts by kind; names only if privacy allows (SPEC §11).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ColumnsInfo {
    /// Total columns.
    pub total: u32,
    /// Numeric columns.
    #[serde(default)]
    pub numeric: u32,
    /// String / binary columns.
    #[serde(default)]
    pub string: u32,
    /// Nested / list / struct columns.
    #[serde(default)]
    pub nested: u32,
    /// Other columns (temporal, boolean, …).
    #[serde(default)]
    pub other: u32,
    /// Column names, only if allowed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub names: Option<Vec<String>>,
}

/// A scalar input value. JSON: `{"kind": "int", "value": 3}`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ScalarValue {
    /// Integer.
    Int(i64),
    /// Float.
    Float(f64),
    /// Boolean.
    Bool(bool),
    /// String (only if privacy allows).
    Str(String),
}

impl ScalarValue {
    /// Numeric value, if numeric or boolean.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            ScalarValue::Int(i) => Some(*i as f64),
            ScalarValue::Float(f) => Some(*f),
            ScalarValue::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
            ScalarValue::Str(_) => None,
        }
    }
}

/// Metadata of one input (SPEC §5.2). Readers read bounded byte ranges only.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct InputMeta {
    /// Input name (parameter name).
    pub name: String,
    /// Salted hash of the URI; URIs never leave the process.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uri_hash: Option<String>,
    /// Detected format.
    pub format: InputFormat,
    /// Size in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
    /// Row count (parquet footer only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<u64>,
    /// Column counts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub columns: Option<ColumnsInfo>,
    /// Array shape (npy, safetensors).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shape: Option<Vec<u64>>,
    /// Element dtype.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dtype: Option<String>,
    /// Model parameter count.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<u64>,
    /// Scalar value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scalar: Option<ScalarValue>,
}

impl InputMeta {
    /// A blob input of `bytes` bytes.
    pub fn blob(name: impl Into<String>, bytes: u64) -> Self {
        InputMeta {
            name: name.into(),
            uri_hash: None,
            format: InputFormat::Blob,
            bytes: Some(bytes),
            rows: None,
            columns: None,
            shape: None,
            dtype: None,
            params: None,
            scalar: None,
        }
    }
}

/// Adapter-derived manifest features (SPEC §5.2), e.g. memory-backed volume
/// bytes and sidecars.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ManifestFeatures {
    /// Bytes staged into memory-backed volumes (charged to the memory cgroup);
    /// added to demand to get platform need (SPEC §5.2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_backed_bytes: Option<Bytes>,
    /// Number of sidecar containers.
    #[serde(default)]
    pub sidecars: u32,
    /// Number of init containers.
    #[serde(default)]
    pub init_containers: u32,
    /// Whether the unit is distributed (SPEC §5.4: GPU count permanently held).
    #[serde(default)]
    pub distributed: bool,
    /// Distribution markers found (`torchrun`, `WORLD_SIZE`, `deepspeed`, …).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub distributed_markers: Vec<String>,
    /// Launcher, if detected (e.g. `"accelerate"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub launcher: Option<String>,
    /// Further adapter-specific features.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: BTreeMap<String, ScalarValue>,
}

/// Where an input lives and what is already known about it (plugins.md §2).
/// The engine reads metadata within its budget to build an [`InputMeta`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct InputRef {
    /// Input name.
    pub name: String,
    /// URI (`s3://…`, `gs://…`, `file://…`, a path). Never leaves the process.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
    /// Size, when the platform already knows it (e.g. a Ray ObjectRef).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
    /// Format hint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<InputFormat>,
    /// Scalar value for scalar inputs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scalar: Option<ScalarValue>,
}

/// Code reference (plugins.md §2): source text, or a reference the engine may
/// resolve. JSON: `{"kind": "source", ...}`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CodeRef {
    /// Source text.
    Source {
        /// The text.
        text: String,
        /// Language hint (`"python"`, `"bash"`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        language: Option<String>,
    },
    /// A local file path.
    Path {
        /// Path.
        path: String,
    },
    /// Already-extracted code info.
    Info {
        /// The info.
        info: CodeInfo,
    },
}

/// What an adapter knows about a unit before the core reads anything
/// (plugins.md §2).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ContextSeed {
    /// Inputs to read metadata for.
    #[serde(default)]
    pub inputs: Vec<InputRef>,
    /// Code reference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<CodeRef>,
    /// Manifest features.
    #[serde(default)]
    pub manifest: ManifestFeatures,
    /// Labels (allowlisting happens in the engine, SPEC §11).
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overhead_points() {
        let o = OverheadModel { memory_mib: vec![(0.5, 70.0), (0.9, 242.0)], cpu_millis: 0 };
        let pts = o.memory_points();
        assert_eq!(pts.len(), 2);
        assert!((pts[0].0 - 0.7).abs() < 1e-6 && (pts[1].0 - 0.3).abs() < 1e-6);
        assert_eq!(o.median_bytes(), 70.0 * 1048576.0);
        assert_eq!(OverheadModel::default().memory_points(), vec![(1.0, 0.0)]);
    }

    #[test]
    fn canonical_truncation() {
        let mut inputs: Vec<InputMeta> = (0..70).map(|i| InputMeta::blob(format!("i{i:02}"), (i % 7) as u64)).collect();
        inputs.reverse();
        let mut ctx = WorkloadContext {
            v: 1,
            key: WorkloadKey::parse("k8s://c/ns/job/x").unwrap(),
            unit: UnitId("main".into()),
            platform: PlatformRef::new("k8s"),
            code: None,
            inputs,
            manifest: ManifestFeatures::default(),
            prior: Prior::default(),
            history: vec![],
            labels: BTreeMap::new(),
        };
        ctx.canonicalize(32);
        assert_eq!(ctx.inputs.len(), 64);
        assert_eq!(ctx.inputs[0].bytes, Some(6));
        assert_eq!(ctx.inputs[0].name, "i06");
        let tokens = serde_json::to_value(ScalarValue::Int(3)).unwrap();
        assert_eq!(tokens, serde_json::json!({"kind": "int", "value": 3}));
    }
}
