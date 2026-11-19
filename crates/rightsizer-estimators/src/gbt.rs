//! The `gbt` estimator (SPEC §6.2, rightsizer-trees/1).
//!
//! Quantile GBTs over `features-v1` read from a `rightsizer-trees/1` JSON file, with an optional `rightsizer-novelty/1` companion. Declines when no model is configured.
//!
//! **Stub:** declines every context (`Insufficient`) until implemented, so a
//! chain falls through to the next member.

use async_trait::async_trait;
use rightsizer_core::{Estimate, EstimateKind, WorkloadContext};

use crate::{EstimateAxis, EstimateError, Estimator, EstimatorInfo, Needs};

/// The `gbt` estimator.
pub struct Gbt {
    info: EstimatorInfo,
}

impl Default for Gbt {
    fn default() -> Self {
        Gbt {
            info: EstimatorInfo {
                id: "gbt".into(),
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
impl Estimator for Gbt {
    fn info(&self) -> &EstimatorInfo {
        &self.info
    }

    async fn estimate(&self, _ctx: &WorkloadContext) -> Result<Estimate, EstimateError> {
        Err(EstimateError::Insufficient("gbt: not implemented yet".into()))
    }
}
