//! `rightsizer-novelty/1`: the novelty companion of a trees file (SPEC §6.2,
//! §6.5; the tuner's `knn` signal, `tuner:bench_all/gating.py`
//! `novelty_knn`).
//!
//! # Format
//!
//! ```json
//! {
//!   "format": "rightsizer-novelty/1",
//!   "features": "features-v1",
//!   "feature_names": ["inputs.log2_bytes_total", "tok.read_parquet", ...],
//!   "mean":  [ ... ],                 // per feature, on the training set
//!   "scale": [ ... ],                 // per feature sd; a constant feature has scale 1
//!   "fill": 0.0,                      // raw value used for a missing feature (default 0)
//!   "reference": [[ ... ], ...],      // standardized training rows
//!   "k": 10,
//!   "ecdf": [0.41, 0.52, ...],        // sorted held-out in-distribution mean kNN distances
//!   "needs_code": true                // without code tokens the signal is missing
//! }
//! ```
//!
//! **Evaluation.** Build the raw vector by name (missing → `fill`, as the
//! tuner's `feats.get(n, 0.0)`), standardize `z = (x − mean) / scale`, take
//! the mean Euclidean distance to the `k` nearest reference rows, and return
//! its ECDF value over `ecdf`: the fraction of knots ≤ the distance, in
//! `[0, 1]`. A gate compares it to a threshold such as 0.95 (SPEC §6.5).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::trees::ModelError;

/// Format tag.
pub const NOVELTY_FORMAT: &str = "rightsizer-novelty/1";

/// A `rightsizer-novelty/1` file.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NoveltyFile {
    /// `"rightsizer-novelty/1"`.
    pub format: String,
    /// Feature schema version.
    #[serde(default)]
    pub features: String,
    /// Feature names by column.
    pub feature_names: Vec<String>,
    /// Per-feature mean.
    pub mean: Vec<f64>,
    /// Per-feature scale.
    pub scale: Vec<f64>,
    /// Raw fill for missing features.
    #[serde(default)]
    pub fill: f64,
    /// Standardized reference rows.
    pub reference: Vec<Vec<f64>>,
    /// Neighbours.
    pub k: usize,
    /// Sorted held-out distances.
    pub ecdf: Vec<f64>,
    /// Whether the signal needs code tokens.
    #[serde(default)]
    pub needs_code: bool,
}

/// A validated novelty model.
#[derive(Clone, Debug, PartialEq)]
pub struct NoveltyModel {
    /// Feature schema version (`""` = unlabeled, read as the trees file's).
    pub features: String,
    /// Feature names.
    pub feature_names: Vec<String>,
    mean: Vec<f64>,
    scale: Vec<f64>,
    fill: f64,
    reference: Vec<Vec<f64>>,
    k: usize,
    ecdf: Vec<f64>,
    /// Whether the signal needs code tokens.
    pub needs_code: bool,
}

impl NoveltyModel {
    /// Parse and validate JSON.
    pub fn from_json(s: &str) -> Result<Self, ModelError> {
        Self::from_file(serde_json::from_str(s)?)
    }

    /// Read a file.
    pub fn load(path: &std::path::Path) -> Result<Self, ModelError> {
        let s = std::fs::read_to_string(path).map_err(|e| ModelError::Io { path: path.display().to_string(), source: e })?;
        Self::from_json(&s)
    }

    /// Validate.
    pub fn from_file(f: NoveltyFile) -> Result<Self, ModelError> {
        let bad = |m: &str| Err(ModelError::Invalid(format!("novelty: {m}")));
        if f.format != NOVELTY_FORMAT {
            return bad("wrong format tag");
        }
        let n = f.feature_names.len();
        if f.mean.len() != n || f.scale.len() != n {
            return bad("mean/scale length differs from feature_names");
        }
        if f.reference.is_empty() || f.reference.iter().any(|r| r.len() != n) {
            return bad("reference rows must be non-empty and as wide as feature_names");
        }
        if f.k == 0 || f.ecdf.is_empty() {
            return bad("k and ecdf must be non-empty");
        }
        let all_finite = f.mean.iter().chain(&f.scale).chain(f.reference.iter().flatten()).chain(&f.ecdf).all(|v| v.is_finite());
        if !all_finite || f.scale.iter().any(|s| *s <= 0.0) {
            return bad("non-finite values or non-positive scale");
        }
        let mut ecdf = f.ecdf;
        ecdf.sort_by(|a, b| a.total_cmp(b));
        Ok(NoveltyModel {
            features: f.features,
            feature_names: f.feature_names,
            mean: f.mean,
            scale: f.scale,
            fill: f.fill,
            k: f.k.min(f.reference.len()),
            reference: f.reference,
            ecdf,
            needs_code: f.needs_code,
        })
    }

    /// Check this companion against its trees file (SPEC §9): the same
    /// feature schema version (an unlabeled file is read as `features-v1`,
    /// like trees), so the novelty signal is computed on the features the
    /// model was trained on. A mismatch is a configuration error: the gate
    /// would otherwise compare distances in another feature space.
    pub fn check_against(&self, trees_features: &str) -> Result<(), ModelError> {
        let norm = |s: &str| if s.is_empty() { rightsizer_features::FEATURES_VERSION.to_string() } else { s.to_string() };
        let (n, t) = (norm(&self.features), norm(trees_features));
        if n != t {
            return Err(ModelError::Invalid(format!("novelty: feature schema {n:?} does not match the trees file's {t:?}")));
        }
        if self.feature_names.is_empty() {
            return Err(ModelError::Invalid("novelty: no feature names".into()));
        }
        Ok(())
    }

    /// Mean distance to the `k` nearest reference rows for a raw vector
    /// (NaN entries take `fill`).
    pub fn mean_knn_distance(&self, raw: &[f64]) -> f64 {
        let z: Vec<f64> = (0..self.mean.len())
            .map(|j| {
                let x = raw.get(j).copied().filter(|v| !v.is_nan()).unwrap_or(self.fill);
                (x - self.mean[j]) / self.scale[j]
            })
            .collect();
        let mut d: Vec<f64> = self.reference.iter().map(|r| r.iter().zip(&z).map(|(a, b)| (a - b) * (a - b)).sum::<f64>().sqrt()).collect();
        let k = self.k;
        d.select_nth_unstable_by(k - 1, |a, b| a.total_cmp(b));
        d[..k].iter().sum::<f64>() / k as f64
    }

    /// ECDF value of a distance: the fraction of knots ≤ `d`.
    pub fn ecdf(&self, d: f64) -> f32 {
        let n = self.ecdf.partition_point(|&k| k <= d);
        (n as f64 / self.ecdf.len() as f64) as f32
    }

    /// Novelty of a feature map (names as in `feature_names`).
    pub fn novelty(&self, features: &BTreeMap<String, f64>) -> f32 {
        let raw: Vec<f64> = self.feature_names.iter().map(|n| features.get(n).copied().unwrap_or(f64::NAN)).collect();
        self.ecdf(self.mean_knn_distance(&raw))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn knn_and_ecdf() {
        let m = NoveltyModel::from_json(
            r#"{"format":"rightsizer-novelty/1","feature_names":["a","b"],"mean":[1,0],"scale":[2,1],
                "reference":[[0,0],[1,0],[0,1]],"k":2,"ecdf":[0.5,1.0,2.0,4.0],"needs_code":false}"#,
        )
        .unwrap();
        // raw (1, 0) → z (0, 0): distances 0, 1, 1 → mean of 2 nearest = 0.5.
        assert_eq!(m.mean_knn_distance(&[1.0, 0.0]), 0.5);
        assert_eq!(m.ecdf(0.5), 0.25);
        assert_eq!(m.ecdf(0.1), 0.0);
        assert_eq!(m.ecdf(100.0), 1.0);
        // Missing a → fill 0 → z_a = −0.5.
        let mut f = BTreeMap::new();
        f.insert("b".to_string(), 0.0);
        let d = m.mean_knn_distance(&[f64::NAN, 0.0]);
        // Distances 0.5, 1.5, √1.25: the two nearest are 0.5 and √1.25.
        assert!((d - (0.5 + 1.25f64.sqrt()) / 2.0).abs() < 1e-12, "{d}");
        assert_eq!(m.novelty(&f), m.ecdf(d));
        assert!(
            NoveltyModel::from_json(r#"{"format":"x","feature_names":[],"mean":[],"scale":[],"reference":[],"k":1,"ecdf":[]}"#).is_err()
        );
    }
}
