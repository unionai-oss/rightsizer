//! Estimator chains (SPEC §6.2 default chain, §6.5): top-down, the first
//! non-declining answer wins. `Insufficient` passes to the next member; any
//! other error is recorded, counted, and also passes on (S1). A
//! [`SkipPolicy`] (the engine's per-estimator-version breaker) can skip
//! members before they are called. Pools and gates (M4) will be members of
//! their own.

use std::sync::Arc;
use std::time::{Duration, Instant};

use rightsizer_core::{Estimate, WorkloadContext};

use crate::{EstimateError, Estimator, EstimatorInfo};

/// Decides whether to skip a member before calling it (e.g. an open
/// per-estimator-version breaker). Implemented for closures
/// `Fn(&EstimatorInfo) -> bool` and for id lists (`[String]`: an entry
/// matches `id` or `id@version`).
pub trait SkipPolicy: Send + Sync {
    /// Whether to skip the estimator described by `info`.
    fn skip(&self, info: &EstimatorInfo) -> bool;
}

impl<F: Fn(&EstimatorInfo) -> bool + Send + Sync> SkipPolicy for F {
    fn skip(&self, info: &EstimatorInfo) -> bool {
        self(info)
    }
}

impl SkipPolicy for [String] {
    fn skip(&self, info: &EstimatorInfo) -> bool {
        self.iter()
            .any(|s| *s == info.id || s.strip_prefix(info.id.as_str()).and_then(|r| r.strip_prefix('@')) == Some(info.version.as_str()))
    }
}

/// Skips nothing.
pub struct NoSkip;

impl SkipPolicy for NoSkip {
    fn skip(&self, _: &EstimatorInfo) -> bool {
        false
    }
}

/// An ordered chain of estimators.
#[derive(Clone, Default)]
pub struct EstimatorChain {
    members: Vec<Arc<dyn Estimator>>,
}

/// The chain's answer for one context.
#[derive(Clone, Debug, Default)]
pub struct ChainAnswer {
    /// The winning estimate, if any member answered.
    pub estimate: Option<Estimate>,
    /// `(estimator id, error)` of every member that did not answer, in order
    /// (declines, failures and skips).
    pub skipped: Vec<(String, EstimateError)>,
    /// Which member answered: `(index in the chain, id, version)`.
    pub answered_by: Option<(usize, String, String)>,
    /// Members that failed (any error other than `Insufficient`; skips are
    /// not failures). The engine feeds these to its breakers and metrics.
    pub failures: Vec<(String, String, EstimateError)>,
    /// Members skipped by the [`SkipPolicy`] (`id`, `version`).
    pub skipped_by_policy: Vec<(String, String)>,
}

impl ChainAnswer {
    /// Number of members that failed.
    pub fn error_count(&self) -> usize {
        self.failures.len()
    }
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

    /// Ids of the members, top-down.
    pub fn ids(&self) -> Vec<String> {
        self.members.iter().map(|m| m.info().id.clone()).collect()
    }

    /// The first non-declining answer, skipping estimators whose id (or
    /// `id@version`) is in `skip` (e.g. an open per-estimator-version breaker).
    pub async fn estimate(&self, ctx: &WorkloadContext, skip: &[String]) -> ChainAnswer {
        self.estimate_with(ctx, skip, None).await
    }

    /// The first non-declining answer under a [`SkipPolicy`] and an optional
    /// overall budget: each member gets what remains of it
    /// ([`Estimator::estimate_within`]); once it is spent, the remaining
    /// members are recorded as `Timeout` without being called.
    pub async fn estimate_with(&self, ctx: &WorkloadContext, skip: &(impl SkipPolicy + ?Sized), budget: Option<Duration>) -> ChainAnswer {
        let started = Instant::now();
        let mut a = ChainAnswer::default();
        for (i, m) in self.members.iter().enumerate() {
            let info = m.info();
            let (id, version) = (info.id.clone(), info.version.clone());
            if skip.skip(info) {
                a.skipped.push((id.clone(), EstimateError::Unavailable("skipped by breaker".into())));
                a.skipped_by_policy.push((id, version));
                continue;
            }
            let remaining = match budget {
                Some(b) => match b.checked_sub(started.elapsed()) {
                    Some(r) if !r.is_zero() => Some(r),
                    _ => {
                        let e = EstimateError::Timeout(b.as_millis() as u64);
                        a.skipped.push((id.clone(), e.clone()));
                        a.failures.push((id, version, e));
                        continue;
                    }
                },
                None => None,
            };
            match m.estimate_within(ctx, remaining).await {
                Ok(e) => {
                    a.estimate = Some(e);
                    a.answered_by = Some((i, id, version));
                    return a;
                }
                Err(e) => {
                    if !e.is_insufficient() {
                        tracing::debug!(estimator = %id, error = %e, "estimator failed; trying the next");
                        a.failures.push((id.clone(), version, e.clone()));
                    }
                    a.skipped.push((id, e));
                }
            }
        }
        a
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::ctx_with;
    use crate::{EstimatorInfo, PriorEstimator};
    use async_trait::async_trait;
    use rightsizer_core::*;

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

    struct Slow(EstimatorInfo);
    #[async_trait]
    impl Estimator for Slow {
        fn info(&self) -> &EstimatorInfo {
            &self.0
        }
        async fn estimate(&self, ctx: &WorkloadContext) -> Result<Estimate, EstimateError> {
            tokio::time::sleep(Duration::from_secs(5)).await;
            PriorEstimator::default().estimate(ctx).await
        }
    }

    fn info(id: &str) -> EstimatorInfo {
        EstimatorInfo {
            id: id.into(),
            version: "1".into(),
            grids: vec![],
            axes: vec![],
            kind: EstimateKind::Demand,
            needs: Default::default(),
            features: String::new(),
            novelty: None,
            max_batch: None,
        }
    }

    #[tokio::test]
    async fn first_answer_wins_and_errors_pass_on() {
        let chain = EstimatorChain::new(vec![
            Arc::new(Declines(info("a"), EstimateError::Insufficient("no inputs".into()))),
            Arc::new(Declines(info("b"), EstimateError::Failed { message: "boom".into(), retryable: false })),
            Arc::new(PriorEstimator::default()),
        ]);
        let ctx = ctx_with(vec![], vec![]);
        let a = chain.estimate(&ctx, &[]).await;
        assert_eq!(a.estimate.as_ref().unwrap().provenance.estimator, "prior");
        assert_eq!(a.skipped.len(), 2);
        assert_eq!(a.answered_by, Some((2, "prior".into(), "1".into())));
        assert_eq!(a.error_count(), 1, "Insufficient is not a failure");
        assert_eq!(a.failures[0].0, "b");
        let none = EstimatorChain::new(vec![Arc::new(Declines(info("a"), EstimateError::Insufficient("x".into())))]);
        let n = none.estimate(&ctx, &[]).await;
        assert!(n.estimate.is_none() && n.answered_by.is_none());
        let a = chain.estimate(&ctx, &["prior".to_string()]).await;
        assert!(a.estimate.is_none());
        assert_eq!(a.skipped_by_policy, vec![("prior".to_string(), "1".to_string())]);
        // id@version matches too; another version does not.
        assert!(chain.estimate(&ctx, &["prior@1".to_string()]).await.estimate.is_none());
        assert!(chain.estimate(&ctx, &["prior@2".to_string()]).await.estimate.is_some());
    }

    #[tokio::test]
    async fn skip_hook_closure() {
        let chain = EstimatorChain::new(vec![Arc::new(PriorEstimator::default())]);
        let ctx = ctx_with(vec![], vec![]);
        let hook = |i: &EstimatorInfo| i.id == "prior" && i.version == "1";
        let a = chain.estimate_with(&ctx, &hook, None).await;
        assert!(a.estimate.is_none());
        let a = chain.estimate_with(&ctx, &NoSkip, None).await;
        assert!(a.estimate.is_some());
    }

    #[tokio::test]
    async fn budget_moves_on_from_a_slow_member() {
        let chain = EstimatorChain::new(vec![Arc::new(Slow(info("slow"))), Arc::new(PriorEstimator::default())]);
        let ctx = ctx_with(vec![], vec![]);
        let t = Instant::now();
        let a = chain.estimate_with(&ctx, &NoSkip, Some(Duration::from_millis(50))).await;
        assert!(t.elapsed() < Duration::from_secs(2));
        // The slow member timed out; the budget is then spent, so prior is not called either.
        assert!(matches!(a.failures[0].2, EstimateError::Timeout(_)));
        assert!(a.estimate.is_none() || a.answered_by.as_ref().unwrap().1 == "prior");
    }
}
