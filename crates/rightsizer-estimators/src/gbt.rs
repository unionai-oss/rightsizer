//! The `gbt` estimator (SPEC §6.2; the tuner's `gbt-dist` arm,
//! `tuner:bench/arms.py:168`): quantile GBTs over `features-v1`
//! ([`crate::features_v1`]) read from a `rightsizer-trees/1` file
//! ([`crate::trees`]), with an optional `rightsizer-novelty/1` companion
//! ([`crate::novelty`]).
//!
//! - Each axis in the model emits `AxisEstimate::Quantiles` (one point per
//!   trained level, values in natural units; crossed quantiles are sorted by
//!   the core's normalization, SPEC §5.5). Values are workload demand: a
//!   trainer must label with container peak − the overhead median (SPEC §5.2).
//! - `novelty` is the ECDF value of the context's mean kNN distance when a
//!   novelty file is configured; it is `None` (missing signal) when the file
//!   says `needs_code` and the context has no code (SPEC §6.5).
//! - Provenance: id from the file (`"gbt"`), version = the model version,
//!   features = the model's feature schema.
//!
//! Declines (`Insufficient`) when no model is configured.

use std::path::Path;

use async_trait::async_trait;
use rightsizer_core::grid::RS_GRID_V1;
use rightsizer_core::{AxisEstimate, Estimate, EstimateKind, Provenance, WorkloadContext};

use crate::features_v1::{TOKEN_PREFIX, features_v1};
use crate::novelty::NoveltyModel;
use crate::trees::{ModelError, TreeModel};
use crate::{EstimateAxis, EstimateError, Estimator, EstimatorInfo, Needs, NoveltyInfo};

/// The `gbt` estimator.
pub struct Gbt {
    info: EstimatorInfo,
    model: Option<TreeModel>,
    novelty: Option<NoveltyModel>,
}

impl Default for Gbt {
    /// No model configured: declines every context.
    fn default() -> Self {
        Gbt {
            info: EstimatorInfo {
                id: Self::ID.into(),
                version: "0".into(),
                grids: vec![RS_GRID_V1.into()],
                axes: vec![EstimateAxis::Memory, EstimateAxis::CpuMean],
                kind: EstimateKind::Demand,
                needs: Needs { code: false, history: true, input_bytes: true },
                features: rightsizer_features::FEATURES_VERSION.into(),
                novelty: None,
                max_batch: None,
            },
            model: None,
            novelty: None,
        }
    }
}

impl Gbt {
    /// Default id.
    pub const ID: &'static str = "gbt";

    /// From a loaded model and optional novelty companion.
    pub fn new(model: TreeModel, novelty: Option<NoveltyModel>) -> Self {
        let needs_code = model.feature_names.iter().any(|n| n.starts_with(TOKEN_PREFIX) || n.starts_with("code."));
        let info = EstimatorInfo {
            id: model.id.clone(),
            version: model.version.clone(),
            grids: vec![RS_GRID_V1.into()],
            axes: model.axes.iter().map(|a| a.axis).collect(),
            kind: EstimateKind::Demand,
            needs: Needs {
                code: needs_code,
                history: model.feature_names.iter().any(|n| n.starts_with("hist.")),
                input_bytes: model.feature_names.iter().any(|n| n.starts_with("inputs.") || n.starts_with("fmt.")),
            },
            features: model.features.clone(),
            novelty: novelty.as_ref().map(|n| NoveltyInfo { needs_code: n.needs_code }),
            max_batch: None,
        };
        Gbt { info, model: Some(model), novelty }
    }

    /// Load `trees` and optionally `novelty` files.
    pub fn load(trees: &Path, novelty: Option<&Path>) -> Result<Self, ModelError> {
        let m = TreeModel::load(trees)?;
        let n = novelty.map(NoveltyModel::load).transpose()?;
        Ok(Self::new(m, n))
    }

    /// The same estimator under another id (a renamed chain member).
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.info.id = id.into();
        self
    }

    /// The model, if configured.
    pub fn model(&self) -> Option<&TreeModel> {
        self.model.as_ref()
    }

    /// Novelty of `ctx` (ECDF value), when a novelty model is configured and
    /// the signal is not missing.
    pub fn novelty(&self, ctx: &WorkloadContext) -> Option<f32> {
        let n = self.novelty.as_ref()?;
        if n.needs_code && ctx.code.as_ref().is_none_or(|c| c.tokens.is_empty()) {
            return None;
        }
        let f = features_v1(ctx, n.feature_names.iter().map(String::as_str));
        Some(n.novelty(&f))
    }
}

#[async_trait]
impl Estimator for Gbt {
    fn info(&self) -> &EstimatorInfo {
        &self.info
    }

    async fn estimate(&self, ctx: &WorkloadContext) -> Result<Estimate, EstimateError> {
        let started = std::time::Instant::now();
        let Some(model) = &self.model else {
            return Err(EstimateError::Insufficient("gbt: no model configured".into()));
        };
        // The evaluator computes `features-v1` only; a model of another schema
        // would be fed vectors whose names may mean something else (SPEC §9).
        // An unlabeled model (`features` empty) is read as `features-v1`.
        if !model.features.is_empty() && model.features != rightsizer_features::FEATURES_VERSION {
            return Err(EstimateError::Unsupported(format!(
                "gbt: model features {:?}, this build computes {:?}",
                model.features,
                rightsizer_features::FEATURES_VERSION
            )));
        }
        let feats = features_v1(ctx, model.feature_names.iter().map(String::as_str));
        let x = model.vector(&feats);
        let mut e = Estimate::empty(Provenance {
            estimator: self.info.id.clone(),
            version: self.info.version.clone(),
            grid: RS_GRID_V1.into(),
            features: model.features.clone(),
            latency_ms: 0,
        });
        for a in &model.axes {
            let pred = a.predict(&x);
            if pred.iter().any(|(_, v)| !v.is_finite()) {
                return Err(EstimateError::Failed { message: "gbt: non-finite prediction".into(), retryable: false });
            }
            let q: Vec<(f32, f64)> = pred.into_iter().map(|(l, v)| (l as f32, v.max(0.0))).collect();
            let est = Some(AxisEstimate::Quantiles { q });
            match a.axis {
                EstimateAxis::Memory => e.memory = est,
                EstimateAxis::CpuMean => e.cpu_mean = est,
                EstimateAxis::CpuPeak => e.cpu_peak = est,
                EstimateAxis::GpuMemory => e.gpu_memory = est,
                EstimateAxis::Duration => e.duration_s = est,
            }
        }
        e.novelty = self.novelty(ctx);
        e.provenance.latency_ms = started.elapsed().as_millis().min(u32::MAX as u128) as u32;
        Ok(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::*;
    use rightsizer_core::*;

    fn model_json() -> String {
        // memory: base 10 (1Gi); +1 when bytes > 2^20; +1 when groupby is present (missing → right).
        let tree = |f: u32, t: f64, dl: bool| {
            serde_json::json!({"nodes": [
                {"feature": f, "threshold": t, "left": 1, "right": 2, "default_left": dl},
                {"leaf": 0.0}, {"leaf": 1.0}]})
        };
        let q = |l: f64, b: f64| serde_json::json!({"q": l, "base": b, "trees": [tree(0, 20.0, true), tree(1, 0.5, false), tree(2, 0.0, true)]});
        serde_json::json!({
            "format": "rightsizer-trees/1", "version": "m1", "features": "features-v1",
            "feature_names": ["inputs.log2_bytes_total", "tok.groupby", "no.such.feature"],
            "axes": {"memory": {"target": {"transform": "log2", "scale": 1048576.0},
                                "quantiles": [q(0.1, 9.0), q(0.5, 10.0), q(0.9, 11.0)]}}
        })
        .to_string()
    }

    #[tokio::test]
    async fn declines_without_model() {
        let ctx = ctx_with(vec![InputMeta::blob("x", 10)], vec![]);
        assert!(Gbt::default().estimate(&ctx).await.unwrap_err().is_insufficient());
    }

    #[tokio::test]
    async fn predicts_quantiles_by_feature_name() {
        let gbt = Gbt::new(TreeModel::from_json(&model_json()).unwrap(), None);
        assert_eq!(gbt.info().version, "m1");
        assert!(gbt.info().needs.code);
        let mut ctx = ctx_with(vec![InputMeta::blob("x", 1 << 30)], vec![]);
        ctx.code = Some(CodeInfo { digest: "d".into(), tokens: vec!["groupby".into()], source: None });
        let e = gbt.estimate(&ctx).await.unwrap();
        let Some(AxisEstimate::Quantiles { q }) = &e.memory else { panic!() };
        // bytes 2^30 > 2^20 → +1; groupby 1 > 0.5 → +1; unknown feature NaN → default left (0).
        let mib = 1048576.0;
        assert_eq!(q, &vec![(0.1, 2048.0 * mib), (0.5, 4096.0 * mib), (0.9, 8192.0 * mib)]);
        assert_eq!(e.provenance.estimator, "gbt");
        assert_eq!(e.provenance.features, "features-v1");
        // No code: tok.groupby is missing → default_left false → right (+1) anyway.
        ctx.code = None;
        let e = gbt.estimate(&ctx).await.unwrap();
        let Some(AxisEstimate::Quantiles { q }) = &e.memory else { panic!() };
        assert_eq!(q[1].1, 4096.0 * mib);
        assert!(e.novelty.is_none());
    }

    /// A model trained on another feature schema must not be evaluated on
    /// `features-v1` vectors (names may match with other meanings; SPEC §9:
    /// "models trained on another version get it, or a 409").
    #[tokio::test]
    async fn refuses_a_model_of_another_feature_schema() {
        let json = model_json().replace("\"features\":\"features-v1\"", "\"features\":\"features-v2\"");
        assert!(json.contains("features-v2"));
        let gbt = Gbt::new(TreeModel::from_json(&json).unwrap(), None);
        let ctx = ctx_with(vec![InputMeta::blob("x", 1 << 30)], vec![]);
        let e = gbt.estimate(&ctx).await;
        assert!(matches!(e, Err(EstimateError::Unsupported(_))), "{e:?}");
    }

    #[tokio::test]
    async fn novelty_needs_code() {
        let nov = NoveltyModel::from_json(
            r#"{"format":"rightsizer-novelty/1","feature_names":["tok.groupby"],"mean":[0],"scale":[1],
                "reference":[[0],[0]],"k":1,"ecdf":[0.1,0.5,0.9,1.5],"needs_code":true}"#,
        )
        .unwrap();
        let gbt = Gbt::new(TreeModel::from_json(&model_json()).unwrap(), Some(nov));
        assert_eq!(gbt.info().novelty, Some(NoveltyInfo { needs_code: true }));
        let mut ctx = ctx_with(vec![], vec![]);
        assert_eq!(gbt.novelty(&ctx), None);
        ctx.code = Some(CodeInfo { digest: "d".into(), tokens: vec!["groupby".into()], source: None });
        // distance 1 → 3 of 4 knots ≤ 1.
        assert_eq!(gbt.novelty(&ctx), Some(0.75));
        ctx.code = Some(CodeInfo { digest: "d".into(), tokens: vec!["other".into()], source: None });
        assert_eq!(gbt.novelty(&ctx), Some(0.0));
    }
}
