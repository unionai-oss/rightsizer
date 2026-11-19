//! The `rules-sized` estimator (SPEC §6.2).
//!
//! Per-format power law in input bytes, anchored on the nearest-size same-key run and scaled to this invocation (SPEC §7.4 `scale`); cold-start constants per format. Declines when there are no input bytes and no history.
//!
//! **Stub:** declines every context (`Insufficient`) until implemented, so a
//! chain falls through to the next member.

use async_trait::async_trait;
use rightsizer_core::{Estimate, EstimateKind, WorkloadContext};

use crate::{EstimateAxis, EstimateError, Estimator, EstimatorInfo, Needs};

/// The `rules-sized` estimator.
pub struct RulesSized {
    info: EstimatorInfo,
}

impl Default for RulesSized {
    fn default() -> Self {
        RulesSized {
            info: EstimatorInfo {
                id: "rules-sized".into(),
                version: "0".into(),
                grids: vec![rightsizer_core::grid::RS_GRID_V1.into()],
                axes: vec![EstimateAxis::Memory, EstimateAxis::CpuMean],
                kind: EstimateKind::Demand,
                needs: Needs { code: false, history: true, input_bytes: true },
                features: "features-v1".into(),
                novelty: None,
                max_batch: None,
            },
        }
    }
}

#[async_trait]
impl Estimator for RulesSized {
    fn info(&self) -> &EstimatorInfo {
        &self.info
    }

    async fn estimate(&self, _ctx: &WorkloadContext) -> Result<Estimate, EstimateError> {
        Err(EstimateError::Insufficient("rules-sized: not implemented yet".into()))
    }
}
