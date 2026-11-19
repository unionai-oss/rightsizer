//! The `history-peak` estimator (SPEC §6.2).
//!
//! Censored max over the last K same-key runs (same digest preferred) × margin; OOM bump max(×1.2, +100Mi) (VPA's rule). Declines without history.
//!
//! **Stub:** declines every context (`Insufficient`) until implemented, so a
//! chain falls through to the next member.

use async_trait::async_trait;
use rightsizer_core::{Estimate, EstimateKind, WorkloadContext};

use crate::{EstimateAxis, EstimateError, Estimator, EstimatorInfo, Needs};

/// The `history-peak` estimator.
pub struct HistoryPeak {
    info: EstimatorInfo,
}

impl Default for HistoryPeak {
    fn default() -> Self {
        HistoryPeak {
            info: EstimatorInfo {
                id: "history-peak".into(),
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
impl Estimator for HistoryPeak {
    fn info(&self) -> &EstimatorInfo {
        &self.info
    }

    async fn estimate(&self, _ctx: &WorkloadContext) -> Result<Estimate, EstimateError> {
        Err(EstimateError::Insufficient("history-peak: not implemented yet".into()))
    }
}
