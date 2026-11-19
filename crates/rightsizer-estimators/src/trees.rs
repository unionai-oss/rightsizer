//! `rightsizer-trees/1`: a minimal JSON tree-ensemble format and its
//! evaluator (SPEC §6.2). Exporters for sklearn
//! `HistGradientBoostingRegressor(loss="quantile")` and LightGBM live in the
//! Python package (`rightsizer.export`); parity is ≤ 1e-6 relative
//! (`tests/trees_parity.rs`).
//!
//! # Format
//!
//! ```json
//! {
//!   "format": "rightsizer-trees/1",
//!   "id": "gbt",                         // optional: estimator id (default "gbt")
//!   "version": "2026-11-18.1",           // model version, goes into provenance
//!   "features": "features-v1",           // feature schema version (SPEC §9)
//!   "feature_names": ["inputs.log2_bytes_total", "tok.read_parquet", ...],
//!   "source": "sklearn-hgb",             // optional, informational
//!   "axes": {
//!     "memory": {
//!       "target": {"transform": "log2", "scale": 1048576.0},
//!       "quantiles": [
//!         {"q": 0.5, "base": 11.2, "trees": [{"nodes": [ ... ]}, ...]},
//!         ...
//!       ]
//!     },
//!     "cpu_mean": { ... }
//!   }
//! }
//! ```
//!
//! - **Axes** are estimate fields: `memory`, `cpu_mean`, `cpu_peak`,
//!   `gpu_memory`, `duration_s`. Every axis predicts **workload demand**
//!   (container peak − runtime overhead median, SPEC §5.2) in natural units
//!   (bytes, cores, seconds) after the target transform.
//! - **Target.** The raw score is `base + Σ trees`; the axis value is
//!   `inverse(raw) × scale`, with `transform` one of `identity`, `log2` (`2^raw`),
//!   `ln` (`e^raw`), `log10`. `scale` defaults to 1. The tuner trains memory on
//!   `log2(MiB)`, i.e. `{"transform": "log2", "scale": 1048576}`.
//! - **Quantiles.** One ensemble per level `q ∈ (0, 1)`. The estimator emits
//!   `AxisEstimate::Quantiles`; crossed quantiles are sorted by the core's
//!   normalization (SPEC §5.5).
//! - **Nodes.** A tree is a flat array; node 0 is the root; children always
//!   have larger indices than their parent (this is validated, so evaluation
//!   terminates). A node is either a leaf `{"leaf": 0.12}` or a split:
//!
//!   ```json
//!   {"feature": 3, "threshold": 0.357, "left": 1, "right": 4,
//!    "default_left": true, "missing": "nan"}
//!   ```
//!
//!   A value `x` goes left when `x <= threshold`. Missing values follow
//!   `missing`:
//!   - `"nan"` (default): NaN is missing and goes to `default_left`
//!     (sklearn's `missing_go_to_left`; LightGBM `missing_type = NaN`);
//!   - `"zero"`: NaN is read as 0, and 0 (|x| ≤ 1e-35) is missing (LightGBM
//!     `missing_type = Zero`);
//!   - `"none"`: NaN is read as 0 and compared (LightGBM `missing_type = None`).
//!
//!   Categorical splits are not part of v1; exporters refuse them.
//! - **Features** are mapped **by name**: the evaluator looks each name up in
//!   the `features-v1` map built from the context; a name it does not know is
//!   missing (NaN).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::EstimateAxis;

/// Format tag.
pub const TREES_FORMAT: &str = "rightsizer-trees/1";

/// Errors reading a trees file.
#[derive(Debug, thiserror::Error)]
pub enum ModelError {
    /// I/O.
    #[error("reading {path}: {source}")]
    Io {
        /// Path.
        path: String,
        /// Error.
        source: std::io::Error,
    },
    /// JSON.
    #[error("parsing model JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// Structurally invalid.
    #[error("invalid model: {0}")]
    Invalid(String),
}

/// A target transform.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Transform {
    /// `raw`.
    #[default]
    Identity,
    /// `2^raw`.
    Log2,
    /// `e^raw`.
    Ln,
    /// `10^raw`.
    Log10,
}

/// An axis target.
#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Target {
    /// Transform the trees were trained on.
    #[serde(default)]
    pub transform: Transform,
    /// Multiplier after the inverse transform.
    #[serde(default = "one")]
    pub scale: f64,
}

fn one() -> f64 {
    1.0
}

impl Default for Target {
    fn default() -> Self {
        Target { transform: Transform::Identity, scale: 1.0 }
    }
}

impl Target {
    /// Natural-unit value of a raw score.
    pub fn value(&self, raw: f64) -> f64 {
        let v = match self.transform {
            Transform::Identity => raw,
            Transform::Log2 => raw.exp2(),
            Transform::Ln => raw.exp(),
            Transform::Log10 => 10f64.powf(raw),
        };
        v * self.scale
    }
}

/// How a split treats missing values.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Missing {
    /// NaN is missing.
    #[default]
    Nan,
    /// NaN reads as 0; 0 is missing.
    Zero,
    /// NaN reads as 0; nothing is missing.
    None,
}

/// A node as written in the file.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeFile {
    /// Leaf value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leaf: Option<f64>,
    /// Split feature index into `feature_names`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feature: Option<u32>,
    /// Split threshold (`x <= threshold` goes left).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub threshold: Option<f64>,
    /// Left child index.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub left: Option<u32>,
    /// Right child index.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub right: Option<u32>,
    /// Missing values go left.
    #[serde(default)]
    pub default_left: bool,
    /// Missing-value semantics.
    #[serde(default)]
    pub missing: Missing,
}

/// A tree as written in the file.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TreeFile {
    /// Flat node array, root first.
    pub nodes: Vec<NodeFile>,
}

/// One quantile's ensemble as written.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct QuantileFile {
    /// Level in (0, 1).
    pub q: f64,
    /// Baseline raw score.
    #[serde(default)]
    pub base: f64,
    /// Trees, summed.
    pub trees: Vec<TreeFile>,
}

/// One axis as written.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AxisFile {
    /// Target transform.
    #[serde(default)]
    pub target: Target,
    /// Ensembles by quantile.
    pub quantiles: Vec<QuantileFile>,
}

/// A `rightsizer-trees/1` file.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TreesFile {
    /// `"rightsizer-trees/1"`.
    pub format: String,
    /// Estimator id override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Model version.
    #[serde(default)]
    pub version: String,
    /// Feature schema version.
    #[serde(default)]
    pub features: String,
    /// Feature names, by index.
    pub feature_names: Vec<String>,
    /// Producer, informational.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// Axes.
    pub axes: BTreeMap<String, AxisFile>,
}

#[derive(Copy, Clone, Debug, PartialEq)]
enum Node {
    Leaf(f64),
    Split { feature: u32, threshold: f64, left: u32, right: u32, default_left: bool, missing: Missing },
}

/// A validated tree.
#[derive(Clone, Debug, PartialEq)]
pub struct Tree {
    nodes: Vec<Node>,
}

/// Threshold below which LightGBM treats a value as zero.
const ZERO_THRESHOLD: f64 = 1e-35;

impl Tree {
    fn from_file(t: &TreeFile, n_features: usize) -> Result<Self, String> {
        if t.nodes.is_empty() {
            return Err("empty tree".into());
        }
        let n = t.nodes.len();
        let mut nodes = Vec::with_capacity(n);
        for (i, nf) in t.nodes.iter().enumerate() {
            let node = match (nf.leaf, nf.feature, nf.threshold, nf.left, nf.right) {
                (Some(v), None, _, None, None) => {
                    if !v.is_finite() {
                        return Err(format!("node {i}: non-finite leaf"));
                    }
                    Node::Leaf(v)
                }
                (None, Some(f), Some(th), Some(l), Some(r)) => {
                    if f as usize >= n_features {
                        return Err(format!("node {i}: feature {f} out of range"));
                    }
                    if (l as usize) <= i || (r as usize) <= i || l as usize >= n || r as usize >= n {
                        return Err(format!("node {i}: children must come after the parent and exist"));
                    }
                    if th.is_nan() {
                        return Err(format!("node {i}: NaN threshold"));
                    }
                    Node::Split { feature: f, threshold: th, left: l, right: r, default_left: nf.default_left, missing: nf.missing }
                }
                _ => return Err(format!("node {i}: neither a leaf nor a complete split")),
            };
            nodes.push(node);
        }
        Ok(Tree { nodes })
    }

    /// The leaf value for `x` (indexed like `feature_names`; NaN = missing).
    pub fn eval(&self, x: &[f64]) -> f64 {
        let mut i = 0usize;
        loop {
            match self.nodes[i] {
                Node::Leaf(v) => return v,
                Node::Split { feature, threshold, left, right, default_left, missing } => {
                    let mut v = x.get(feature as usize).copied().unwrap_or(f64::NAN);
                    if missing != Missing::Nan && v.is_nan() {
                        v = 0.0;
                    }
                    let is_missing = match missing {
                        Missing::Nan => v.is_nan(),
                        Missing::Zero => v.abs() <= ZERO_THRESHOLD,
                        Missing::None => false,
                    };
                    let go_left = if is_missing { default_left } else { v <= threshold };
                    i = if go_left { left } else { right } as usize;
                }
            }
        }
    }

    /// Number of nodes.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the tree has no nodes (never, once validated).
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

/// One quantile's ensemble.
#[derive(Clone, Debug, PartialEq)]
pub struct QuantileEnsemble {
    /// Level.
    pub q: f64,
    /// Baseline.
    pub base: f64,
    /// Trees.
    pub trees: Vec<Tree>,
}

impl QuantileEnsemble {
    /// Raw score: `base + Σ trees`, summed in file order.
    pub fn raw(&self, x: &[f64]) -> f64 {
        let mut s = self.base;
        for t in &self.trees {
            s += t.eval(x);
        }
        s
    }
}

/// One axis model.
#[derive(Clone, Debug, PartialEq)]
pub struct AxisModel {
    /// The estimate field.
    pub axis: EstimateAxis,
    /// Target transform.
    pub target: Target,
    /// Ensembles, by ascending level.
    pub quantiles: Vec<QuantileEnsemble>,
}

impl AxisModel {
    /// `(level, natural value)` per quantile.
    pub fn predict(&self, x: &[f64]) -> Vec<(f64, f64)> {
        self.quantiles.iter().map(|q| (q.q, self.target.value(q.raw(x)))).collect()
    }
}

/// A validated `rightsizer-trees/1` model.
#[derive(Clone, Debug, PartialEq)]
pub struct TreeModel {
    /// Estimator id (default `"gbt"`).
    pub id: String,
    /// Model version.
    pub version: String,
    /// Feature schema version.
    pub features: String,
    /// Feature names by index.
    pub feature_names: Vec<String>,
    /// Axis models.
    pub axes: Vec<AxisModel>,
}

/// The estimate field named `name` in a trees file.
pub fn axis_by_name(name: &str) -> Option<EstimateAxis> {
    Some(match name {
        "memory" => EstimateAxis::Memory,
        "cpu_mean" => EstimateAxis::CpuMean,
        "cpu_peak" => EstimateAxis::CpuPeak,
        "gpu_memory" => EstimateAxis::GpuMemory,
        "duration_s" | "duration" => EstimateAxis::Duration,
        _ => return None,
    })
}

impl TreeModel {
    /// Parse and validate JSON.
    pub fn from_json(s: &str) -> Result<Self, ModelError> {
        Self::from_file(serde_json::from_str(s)?)
    }

    /// Read a file.
    pub fn load(path: &std::path::Path) -> Result<Self, ModelError> {
        let s = std::fs::read_to_string(path).map_err(|e| ModelError::Io { path: path.display().to_string(), source: e })?;
        Self::from_json(&s)
    }

    /// Validate a parsed file.
    pub fn from_file(f: TreesFile) -> Result<Self, ModelError> {
        let bad = |m: String| Err(ModelError::Invalid(m));
        if f.format != TREES_FORMAT {
            return bad(format!("format {:?}, expected {TREES_FORMAT:?}", f.format));
        }
        let nf = f.feature_names.len();
        let mut axes = Vec::new();
        for (name, a) in &f.axes {
            let Some(axis) = axis_by_name(name) else { return bad(format!("unknown axis {name:?}")) };
            if !a.target.scale.is_finite() || a.target.scale <= 0.0 {
                return bad(format!("{name}: target scale must be positive"));
            }
            if a.quantiles.is_empty() {
                return bad(format!("{name}: no quantiles"));
            }
            let mut qs = Vec::new();
            for q in &a.quantiles {
                if !(q.q > 0.0 && q.q < 1.0) {
                    return bad(format!("{name}: quantile level {} outside (0, 1)", q.q));
                }
                if !q.base.is_finite() {
                    return bad(format!("{name}: non-finite base"));
                }
                let trees = q
                    .trees
                    .iter()
                    .enumerate()
                    .map(|(i, t)| Tree::from_file(t, nf).map_err(|e| ModelError::Invalid(format!("{name} q={} tree {i}: {e}", q.q))))
                    .collect::<Result<Vec<_>, _>>()?;
                qs.push(QuantileEnsemble { q: q.q, base: q.base, trees });
            }
            qs.sort_by(|a, b| a.q.total_cmp(&b.q));
            axes.push(AxisModel { axis, target: a.target, quantiles: qs });
        }
        if axes.is_empty() {
            return bad("no axes".into());
        }
        axes.sort_by_key(|a| a.axis);
        Ok(TreeModel {
            id: f.id.unwrap_or_else(|| "gbt".into()),
            version: if f.version.is_empty() { "0".into() } else { f.version },
            features: f.features,
            feature_names: f.feature_names,
            axes,
        })
    }

    /// The feature vector for this model from a name → value map (unknown
    /// names are NaN).
    pub fn vector(&self, features: &BTreeMap<String, f64>) -> Vec<f64> {
        self.feature_names.iter().map(|n| features.get(n).copied().unwrap_or(f64::NAN)).collect()
    }

    /// The axis model for `axis`.
    pub fn axis(&self, axis: EstimateAxis) -> Option<&AxisModel> {
        self.axes.iter().find(|a| a.axis == axis)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn model() -> &'static str {
        r#"{
          "format": "rightsizer-trees/1", "version": "t1", "features": "features-v1",
          "feature_names": ["a", "b"],
          "axes": {"memory": {"target": {"transform": "log2", "scale": 1048576},
            "quantiles": [
              {"q": 0.9, "base": 10.0, "trees": [{"nodes": [
                 {"feature": 0, "threshold": 1.0, "left": 1, "right": 2, "default_left": false},
                 {"leaf": 1.0}, {"leaf": 2.0}]}]},
              {"q": 0.5, "base": 10.0, "trees": [{"nodes": [
                 {"feature": 1, "threshold": 0.0, "left": 1, "right": 2, "default_left": true, "missing": "zero"},
                 {"leaf": -1.0}, {"leaf": 0.5}]}]}
            ]}}
        }"#
    }

    #[test]
    fn evaluates_with_missing_semantics() {
        let m = TreeModel::from_json(model()).unwrap();
        let a = m.axis(EstimateAxis::Memory).unwrap();
        assert_eq!(a.quantiles[0].q, 0.5, "sorted by level");
        // a ≤ 1 → left (1.0); b = 0 is missing under "zero" → default left (−1).
        let p = a.predict(&[0.5, 0.0]);
        assert_eq!(p, vec![(0.5, 2f64.powf(9.0) * 1048576.0), (0.9, 2f64.powf(11.0) * 1048576.0)]);
        // NaN a under "nan" → default_left false → right (2.0); NaN b → 0 → missing → left.
        let p = a.predict(&[f64::NAN, f64::NAN]);
        assert_eq!(p[1].1, 2f64.powf(12.0) * 1048576.0);
        assert_eq!(p[0].1, 2f64.powf(9.0) * 1048576.0);
        // b = 3 > 0 → right.
        assert_eq!(a.predict(&[0.0, 3.0])[0].1, 2f64.powf(10.5) * 1048576.0);
        let mut f = BTreeMap::new();
        f.insert("b".to_string(), 1.0);
        let v = m.vector(&f);
        assert!(v[0].is_nan() && v[1] == 1.0);
    }

    #[test]
    fn none_missing_compares_as_zero() {
        let t = TreeFile {
            nodes: vec![
                NodeFile {
                    feature: Some(0),
                    threshold: Some(-0.5),
                    left: Some(1),
                    right: Some(2),
                    default_left: true,
                    missing: Missing::None,
                    ..Default::default()
                },
                NodeFile { leaf: Some(1.0), ..Default::default() },
                NodeFile { leaf: Some(2.0), ..Default::default() },
            ],
        };
        let t = Tree::from_file(&t, 1).unwrap();
        assert_eq!(t.eval(&[f64::NAN]), 2.0, "NaN → 0 > −0.5 → right, default ignored");
    }

    #[test]
    fn rejects_bad_models() {
        let bad = |s: &str| TreeModel::from_json(s).is_err();
        assert!(bad(r#"{"format":"x","feature_names":[],"axes":{}}"#));
        assert!(bad(r#"{"format":"rightsizer-trees/1","feature_names":[],"axes":{}}"#));
        assert!(bad(r#"{"format":"rightsizer-trees/1","feature_names":["a"],"axes":{"nope":{"quantiles":[{"q":0.5,"trees":[]}]}}}"#));
        // A cycle: child index not after its parent.
        assert!(bad(
            r#"{"format":"rightsizer-trees/1","feature_names":["a"],"axes":{"memory":{"quantiles":[{"q":0.5,"trees":[{"nodes":[{"feature":0,"threshold":1,"left":0,"right":1},{"leaf":1}]}]}]}}}"#
        ));
        assert!(bad(r#"{"format":"rightsizer-trees/1","feature_names":["a"],"axes":{"memory":{"quantiles":[{"q":1.5,"trees":[]}]}}}"#));
        assert!(bad(
            r#"{"format":"rightsizer-trees/1","feature_names":["a"],"axes":{"memory":{"quantiles":[{"q":0.5,"trees":[{"nodes":[{"feature":3,"threshold":1,"left":1,"right":2},{"leaf":1},{"leaf":2}]}]}]}}}"#
        ));
    }
}
