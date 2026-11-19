//! `rightsizer-estimators`: the [`Estimator`] trait (SPEC §6.1) and the
//! built-in estimators (SPEC §6.2): `rules-sized`, `history-peak`, `prior`,
//! `gbt` (trees/1), the Estimator Protocol v1 client (feature `remote`, SPEC
//! §6.3) and chains (SPEC §6.5).
//!
//! | module | what |
//! |---|---|
//! | [`rules_sized`] | `rules-sized`: per-format cold-start law, nearest-size anchoring × `scale` |
//! | [`history_peak`] | `history-peak`: censored max × margin, VPA OOM bump |
//! | [`prior`] | `prior`: degenerate at the prior |
//! | [`gbt`] | `gbt`: quantile trees ([`trees`], `rightsizer-trees/1`) over [`features_v1`], novelty ([`novelty`], `rightsizer-novelty/1`) |
//! | [`chain`] | top-down chains with a skip hook and budgets |
//! | [`build`] | chains from the `[estimator]` config section |
//! | [`memo`] | the memo and exact-cache layers (reusable by the engine) |
//! | [`privacy`] | the remote view of a context (SPEC §11) |
//! | `remote` | Estimator Protocol v1 client (feature `remote`) |
//!
//! Every built-in estimator emits workload **demand** (container peaks minus
//! the overhead model's median, SPEC §5.2) as `Cells` on rs-grid-v1, except
//! `gbt`, which emits `Quantiles`, and durations, which are `Quantiles` or a
//! `Point` (SPEC §5.5).

#![forbid(unsafe_code)]

use std::time::Duration;

use async_trait::async_trait;
use rightsizer_core::{Estimate, EstimateKind, WorkloadContext};
use serde::{Deserialize, Serialize};

pub mod build;
pub mod chain;
pub(crate) mod common;
pub mod features_v1;
pub mod gbt;
pub mod history_peak;
pub mod memo;
pub mod novelty;
pub mod prior;
pub mod privacy;
#[cfg(feature = "remote")]
pub mod remote;
pub mod rules_sized;
pub mod trees;

#[cfg(test)]
pub(crate) mod test_util;

pub use build::{BuildError, BuiltChain, ChainSpec, MemberSpec, build_chain};
pub use chain::{ChainAnswer, EstimatorChain, NoSkip, SkipPolicy};
pub use gbt::Gbt;
pub use history_peak::{HistoryPeak, HistoryPeakParams};
pub use memo::{Memo, MemoKey, TtlCache, exact_key, normalized_estimate};
pub use novelty::NoveltyModel;
pub use prior::PriorEstimator;
pub use rules_sized::{ColdStart, RulesSized, RulesSizedParams};
pub use trees::TreeModel;

/// Which estimate fields an estimator fills.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EstimateAxis {
    /// `memory`.
    Memory,
    /// `cpu_mean`.
    CpuMean,
    /// `cpu_peak`.
    CpuPeak,
    /// `gpu_memory`.
    GpuMemory,
    /// `duration_s`.
    Duration,
}

/// What an estimator needs from the context (SPEC §6.1, §6.3 `needs`).
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Needs {
    /// Code tokens / digest.
    #[serde(default)]
    pub code: bool,
    /// Same-key history.
    #[serde(default)]
    pub history: bool,
    /// Input sizes.
    #[serde(default)]
    pub input_bytes: bool,
}

/// Novelty support (SPEC §6.5).
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoveltyInfo {
    /// Without code tokens the novelty signal is missing.
    pub needs_code: bool,
}

/// Static facts about an estimator (SPEC §6.1; the `/v1/info` body of §6.3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EstimatorInfo {
    /// Id, e.g. `"rules-sized"`.
    pub id: String,
    /// Version (part of memo and exact-cache keys).
    pub version: String,
    /// Grids it emits `Cells` on.
    #[serde(default)]
    pub grids: Vec<String>,
    /// Axes it estimates.
    #[serde(default)]
    pub axes: Vec<EstimateAxis>,
    /// Demand or action.
    #[serde(default)]
    pub kind: EstimateKind,
    /// Context it needs.
    #[serde(default)]
    pub needs: Needs,
    /// Feature schema version (`"features-v1"`).
    #[serde(default)]
    pub features: String,
    /// Novelty support.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub novelty: Option<NoveltyInfo>,
    /// Largest batch it accepts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch: Option<u32>,
}

/// Why an estimator did not answer (SPEC §6.1, §6.3 error codes).
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum EstimateError {
    /// "I can't answer this context": passes to the next estimator; not a failure.
    #[error("insufficient context: {0}")]
    Insufficient(String),
    /// Unsupported grid or features version (protocol `409`).
    #[error("unsupported: {0}")]
    Unsupported(String),
    /// The estimator's deadline expired.
    #[error("estimator timed out after {0} ms")]
    Timeout(u64),
    /// Back off (protocol `429` / `503`).
    #[error("estimator unavailable: {0}")]
    Unavailable(String),
    /// Malformed request or response (protocol `400`, bad JSON).
    #[error("protocol error: {0}")]
    Protocol(String),
    /// Any other failure; moves to the next estimator (S1).
    #[error("estimator failed: {message}")]
    Failed {
        /// Message.
        message: String,
        /// Whether a retry may succeed.
        retryable: bool,
    },
}

impl EstimateError {
    /// Whether this is a decline rather than a failure.
    pub fn is_insufficient(&self) -> bool {
        matches!(self, EstimateError::Insufficient(_))
    }
    /// Whether a retry may succeed.
    pub fn is_retryable(&self) -> bool {
        match self {
            EstimateError::Unavailable(_) | EstimateError::Timeout(_) => true,
            EstimateError::Failed { retryable, .. } => *retryable,
            _ => false,
        }
    }
}

/// An estimator (SPEC §6.1). Implementations must be cheap to call
/// concurrently; the engine adds caching, single-flight and budgets.
#[async_trait]
pub trait Estimator: Send + Sync {
    /// Static facts.
    fn info(&self) -> &EstimatorInfo;

    /// Estimate one context.
    async fn estimate(&self, ctx: &WorkloadContext) -> Result<Estimate, EstimateError>;

    /// Estimate within a caller budget. The default runs
    /// [`estimate`](Self::estimate) under a timeout (expiry →
    /// [`EstimateError::Timeout`]); network estimators override it to carry
    /// the deadline into their requests (the caller's budget wins over their
    /// default).
    async fn estimate_within(&self, ctx: &WorkloadContext, budget: Option<Duration>) -> Result<Estimate, EstimateError> {
        match budget {
            None => self.estimate(ctx).await,
            Some(b) => match tokio::time::timeout(b, self.estimate(ctx)).await {
                Ok(r) => r,
                Err(_) => Err(EstimateError::Timeout(b.as_millis().min(u64::MAX as u128) as u64)),
            },
        }
    }

    /// Estimate many contexts. The default calls [`estimate`](Self::estimate)
    /// for each, in order.
    async fn estimate_batch(&self, ctxs: &[WorkloadContext]) -> Vec<Result<Estimate, EstimateError>> {
        let mut out = Vec::with_capacity(ctxs.len());
        for c in ctxs {
            out.push(self.estimate(c).await);
        }
        out
    }
}
