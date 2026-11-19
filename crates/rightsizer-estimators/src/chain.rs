//! Estimator chains (SPEC §6.2 default chain, §6.5): top-down, the first
//! non-declining answer wins. `Insufficient` passes to the next member; any
//! other error is recorded and also passes on (S1). Pools and gates (M4)
//! will be members of their own.

use std::sync::Arc;

use rightsizer_core::{Estimate, WorkloadContext};

use crate::{EstimateError, Estimator};

/// An ordered chain of estimators.
#[derive(Clone, Default)]
pub struct EstimatorChain {
    members: Vec<Arc<dyn Estimator>>,
}

/// The chain's answer for one context.
#[derive(Clone, Debug)]
pub struct ChainAnswer {
    /// The winning estimate, if any member answered.
    pub estimate: Option<Estimate>,
    /// `(estimator id, error)` of every member that did not answer, in order.
    pub skipped: Vec<(String, EstimateError)>,
}

impl EstimatorChain {
    /// A chain over `members`, top-down.
    pub fn new(members: Vec<Arc<dyn Estimator>>) -> Self {
        EstimatorChain { members }
    }

    /// Members, top-down.
    pub fn members(&self) -> &[Arc<dyn Estimator>] {
        &self.members
    }

    /// The first non-declining answer, skipping estimators whose id is in `skip`
    /// (e.g. an open per-estimator-version breaker).
    pub async fn estimate(&self, ctx: &WorkloadContext, skip: &[String]) -> ChainAnswer {
        let mut skipped = Vec::new();
        for m in &self.members {
            let id = m.info().id.clone();
            if skip.contains(&id) {
                skipped.push((id, EstimateError::Unavailable("skipped by breaker".into())));
                continue;
            }
            match m.estimate(ctx).await {
                Ok(e) => return ChainAnswer { estimate: Some(e), skipped },
                Err(e) => skipped.push((id, e)),
            }
        }
        ChainAnswer { estimate: None, skipped }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EstimatorInfo, PriorEstimator};
    use async_trait::async_trait;
    use rightsizer_core::*;
    use std::collections::BTreeMap;

    struct Declines(EstimatorInfo, EstimateError);
    #[async_trait]
    impl Estimator for Declines {
        fn info(&self) -> &EstimatorInfo {
            &self.0
        }
        async fn estimate(&self, _: &WorkloadContext) -> Result<Estimate, EstimateError> {
            Err(self.1.clone())
        }
    }

    fn info(id: &str) -> EstimatorInfo {
        EstimatorInfo { id: id.into(), version: "1".into(), grids: vec![], axes: vec![], kind: EstimateKind::Demand, needs: Default::default(), features: String::new(), novelty: None, max_batch: None }
    }

    #[tokio::test]
    async fn first_answer_wins_and_errors_pass_on() {
        let chain = EstimatorChain::new(vec![
            Arc::new(Declines(info("a"), EstimateError::Insufficient("no inputs".into()))),
            Arc::new(Declines(info("b"), EstimateError::Failed { message: "boom".into(), retryable: false })),
            Arc::new(PriorEstimator::default()),
        ]);
        let ctx = WorkloadContext {
            v: 1,
            key: WorkloadKey::parse("k8s://c/ns/job/x").unwrap(),
            unit: UnitId("main".into()),
            platform: PlatformRef::new("k8s"),
            code: None,
            inputs: vec![],
            manifest: ManifestFeatures::default(),
            prior: Prior::default(),
            history: vec![],
            labels: BTreeMap::new(),
        };
        let a = chain.estimate(&ctx, &[]).await;
        assert_eq!(a.estimate.unwrap().provenance.estimator, "prior");
        assert_eq!(a.skipped.len(), 2);
        let none = EstimatorChain::new(vec![Arc::new(Declines(info("a"), EstimateError::Insufficient("x".into())))]);
        assert!(none.estimate(&ctx, &[]).await.estimate.is_none());
        let a = chain.estimate(&ctx, &["prior".to_string()]).await;
        assert!(a.estimate.is_none());
    }
}
