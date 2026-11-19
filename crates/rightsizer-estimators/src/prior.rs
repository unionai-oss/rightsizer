//! The `prior` estimator (SPEC §6.2; the tuner's `static` arm): a degenerate
//! `Cells` estimate at the prior's cell on every axis the prior sets. Never
//! declines. The decision rule recognises it and keeps the prior.

use async_trait::async_trait;
use rightsizer_core::grid::{Grid, GridAxis};
use rightsizer_core::normalize::degenerate_at;
use rightsizer_core::{Estimate, EstimateKind, Provenance, WorkloadContext};

use crate::{EstimateAxis, EstimateError, Estimator, EstimatorInfo, Needs};

/// The `prior` estimator.
pub struct PriorEstimator {
    info: EstimatorInfo,
    grid: Grid,
}

impl Default for PriorEstimator {
    fn default() -> Self {
        Self::new(Grid::rs_grid_v1())
    }
}

impl PriorEstimator {
    /// Id.
    pub const ID: &'static str = "prior";

    /// On `grid`.
    pub fn new(grid: Grid) -> Self {
        PriorEstimator {
            info: EstimatorInfo {
                id: Self::ID.into(),
                version: "1".into(),
                grids: vec![grid.id.clone()],
                axes: vec![EstimateAxis::Memory, EstimateAxis::CpuMean, EstimateAxis::GpuMemory],
                kind: EstimateKind::Demand,
                needs: Needs::default(),
                features: String::new(),
                novelty: None,
                max_batch: None,
            },
            grid,
        }
    }
}

#[async_trait]
impl Estimator for PriorEstimator {
    fn info(&self) -> &EstimatorInfo {
        &self.info
    }

    async fn estimate(&self, ctx: &WorkloadContext) -> Result<Estimate, EstimateError> {
        let r = &ctx.prior.spec.requests;
        let mut e = Estimate::empty(Provenance {
            estimator: self.info.id.clone(),
            version: self.info.version.clone(),
            grid: self.grid.id.clone(),
            features: String::new(),
            latency_ms: 0,
        });
        e.memory = r.memory.map(|m| degenerate_at(&self.grid, GridAxis::Memory, m.0 as f64));
        e.cpu_mean = r.cpu.map(|c| degenerate_at(&self.grid, GridAxis::Cpu, c.as_cores()));
        e.gpu_memory = r.gpu.and_then(|g| g.total_memory()).map(|b| degenerate_at(&self.grid, GridAxis::GpuMemory, b.0 as f64));
        Ok(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rightsizer_core::*;
    use std::collections::BTreeMap;

    #[tokio::test]
    async fn degenerate_at_the_prior() {
        let ctx = WorkloadContext {
            v: 1,
            key: WorkloadKey::parse("k8s://c/ns/job/x").unwrap(),
            unit: UnitId("main".into()),
            platform: PlatformRef::new("k8s"),
            code: None,
            inputs: vec![],
            manifest: ManifestFeatures::default(),
            prior: Prior::from_requests(Requests { memory: Some(Bytes::gib(3)), ..Default::default() }),
            history: vec![],
            labels: BTreeMap::new(),
        };
        let e = PriorEstimator::default().estimate(&ctx).await.unwrap();
        match e.memory.unwrap() {
            AxisEstimate::Cells { probs, .. } => assert_eq!(probs[7], 1.0),
            other => panic!("{other:?}"),
        }
        assert!(e.cpu_mean.is_none());
    }
}
