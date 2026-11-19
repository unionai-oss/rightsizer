//! `rightsizer-engine`: orchestration shared by the CLI, the daemon, the
//! webhook and the Python module (ARCHITECTURE.md §2 deviation, §4.1):
//! configuration, context building from a [`ContextSeed`], the estimator
//! chain, memo + single-flight + limiter + budget, mode/breaker resolution,
//! decision logging, outcome ingestion, shadow/audit replay.
//!
//! Status: [`config`] is complete; [`Engine`] methods are **stubs** that
//! keep the prior (S1-inert) until the next round implements them.

#![forbid(unsafe_code)]

use std::sync::Arc;

use rightsizer_core::{
    Constraints, ContextSeed, Decision, DecisionSource, GroupId, Mode, PlatformRef, PlatformState, Prior, PriorReason,
    RiskProfile, RunRecord, UnitId, WorkloadContext, WorkloadKey,
};
use rightsizer_estimators::EstimatorChain;
use rightsizer_history::HistoryStore;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

pub mod config;

pub use config::{Config, ConfigEnv, ConfigError, Loaded};

/// What the engine decides from (ARCHITECTURE.md §4.1): a full context, or a
/// seed the engine turns into one (reading input metadata within budget and
/// fetching history). JSON: `{"kind": "full", ...}` / `{"kind": "seed", ...}`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub enum ContextInput {
    /// A complete context (history included).
    Full {
        /// The context.
        context: Box<WorkloadContext>,
    },
    /// A seed from an adapter (plugins.md §2).
    Seed {
        /// Workload key.
        key: WorkloadKey,
        /// Unit.
        unit: UnitId,
        /// Group.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        group: Option<GroupId>,
        /// Inputs, code, manifest features, labels.
        seed: ContextSeed,
        /// The origin prior.
        prior: Prior,
        /// Static platform facts.
        platform: PlatformRef,
    },
}

impl ContextInput {
    /// The workload key.
    pub fn key(&self) -> &WorkloadKey {
        match self {
            ContextInput::Full { context } => &context.key,
            ContextInput::Seed { key, .. } => key,
        }
    }
}

/// The policy a caller asks for (ARCHITECTURE.md §4.1). `None` fields come
/// from [`Config`].
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Policy {
    /// Requested mode.
    pub mode: Option<Mode>,
    /// The surface's highest mode (e.g. `recommend` for surfaces without `render`).
    pub max_mode: Option<Mode>,
    /// Risk profile.
    pub risk: Option<RiskProfile>,
    /// Estimator chain name or members.
    pub chain: Option<Vec<String>>,
    /// Enforce on units with no prior.
    pub enforce_without_prior: Option<bool>,
    /// Exploration ε.
    pub exploration: Option<f32>,
    /// Allow GPU type changes.
    pub allow_gpu_type_change: Option<bool>,
    /// Whether the surface can get failures classified (S5).
    pub can_classify: Option<bool>,
}

/// One decision request (ARCHITECTURE.md §4.1).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DecideRequest {
    /// Full context or seed.
    pub context: ContextInput,
    /// SPEC §7.4 pins, holds, floor, ceiling, allowed shapes.
    #[serde(default)]
    pub constraints: Constraints,
    /// Live shapes, request model, prices, quota, floors.
    #[serde(default)]
    pub state: PlatformState,
    /// Requested policy.
    #[serde(default)]
    pub policy: Policy,
    /// Hard deadline; expiry → `Prior{reason = budget}`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget_ms: Option<u32>,
    /// Decide but do not record.
    #[serde(default)]
    pub dry_run: bool,
}

/// Engine errors (only from non-decision operations; `decide` never fails, S1).
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// Configuration.
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// History store.
    #[error(transparent)]
    Store(#[from] rightsizer_history::StoreError),
    /// Not implemented yet.
    #[error("not implemented: {0}")]
    Unimplemented(&'static str),
}

/// The engine: owns the store, the estimator chain, caches and budgets.
pub struct Engine {
    config: Config,
    store: Arc<dyn HistoryStore>,
    chain: EstimatorChain,
}

impl Engine {
    /// Build an engine.
    pub fn new(config: Config, store: Arc<dyn HistoryStore>, chain: EstimatorChain) -> Self {
        Engine { config, store, chain }
    }

    /// The configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// The store.
    pub fn store(&self) -> &Arc<dyn HistoryStore> {
        &self.store
    }

    /// The estimator chain.
    pub fn chain(&self) -> &EstimatorChain {
        &self.chain
    }

    /// Decide one unit. Never fails (S1).
    ///
    /// **Stub:** keeps the prior with `Prior{reason = internal}`.
    pub async fn decide(&self, req: DecideRequest) -> Decision {
        stub_prior_decision(&req)
    }

    /// Decide a group: one decision from the union of the members' history,
    /// applied to every member (plugins.md §2).
    ///
    /// **Stub:** keeps every prior.
    pub async fn decide_group(&self, reqs: Vec<DecideRequest>) -> Vec<Decision> {
        reqs.iter().map(stub_prior_decision).collect()
    }

    /// Record an outcome (buffered and retried when the store is unreachable, SPEC §8.4).
    ///
    /// **Stub:** writes straight through.
    pub async fn outcome(&self, rec: RunRecord) -> Result<(), EngineError> {
        self.store.put_run(&rec).await?;
        Ok(())
    }
}

fn stub_prior_decision(req: &DecideRequest) -> Decision {
    let (key, unit, group, prior, digest) = match &req.context {
        ContextInput::Full { context } => (
            context.key.clone(),
            context.unit.clone(),
            None,
            context.prior.clone(),
            rightsizer_core::digest::context_digest(context),
        ),
        ContextInput::Seed { key, unit, group, prior, .. } => (key.clone(), unit.clone(), group.clone(), prior.clone(), String::new()),
    };
    Decision {
        v: rightsizer_core::WIRE_VERSION,
        id: Ulid::generate(),
        key,
        unit,
        group,
        mode: Mode::Recommend,
        recommended: prior.spec.clone(),
        applied: prior.spec.clone(),
        shape: None,
        prior,
        held: Vec::new(),
        source: DecisionSource::Prior { reason: PriorReason::Internal, detail: Some("engine not implemented yet".into()) },
        estimate: None,
        propensity: 1.0,
        expected_cost: None,
        p_fail: None,
        advisories: Vec::new(),
        explanation: vec!["kept the prior: engine not implemented yet".into()],
        context_digest: digest,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rightsizer_history::MemoryStore;

    #[tokio::test]
    async fn stub_engine_is_inert() {
        let e = Engine::new(Config::default(), Arc::new(MemoryStore::new()), EstimatorChain::default());
        let req = DecideRequest {
            context: ContextInput::Seed {
                key: WorkloadKey::parse("k8s://c/ns/job/x").unwrap(),
                unit: UnitId("main".into()),
                group: None,
                seed: ContextSeed::default(),
                prior: Prior::default(),
                platform: PlatformRef::new("k8s"),
            },
            constraints: Constraints::default(),
            state: PlatformState::default(),
            policy: Policy::default(),
            budget_ms: Some(50),
            dry_run: true,
        };
        let j = serde_json::to_string(&req).unwrap();
        let back: DecideRequest = serde_json::from_str(&j).unwrap();
        assert_eq!(back, req);
        let d = e.decide(req).await;
        assert_eq!(d.applied, d.prior.spec);
    }
}
