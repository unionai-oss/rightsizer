//! `rightsizer-engine`: orchestration shared by the CLI, the daemon, the
//! webhook and the Python module (ARCHITECTURE.md §2 deviation, §4.1):
//! configuration, context building from a [`ContextSeed`], the estimator
//! chain, memo + single-flight + limiter + budget, mode/breaker resolution,
//! decision logging, outcome ingestion, shadow/audit replay.
//!
//! Modules: [`config`] (`rightsizer.toml`), [`engine`] (decide, groups,
//! attempts, outcomes, breakers), [`store`] (caches, availability, buffered
//! writes), [`memo`] (memo + single-flight), [`context`] (seed → context),
//! [`chain`] (estimator chain and privacy from config), [`replay`] (shadow / audit),
//! [`report`] (text / JSON / HTML rendering of replays), [`metrics`],
//! [`clock`].

#![forbid(unsafe_code)]

use rightsizer_core::{Constraints, ContextSeed, GroupId, Mode, PlatformRef, PlatformState, Prior, RiskProfile, UnitId, WorkloadContext, WorkloadKey};
use serde::{Deserialize, Serialize};

pub mod chain;
pub mod clock;
pub mod config;
pub mod context;
pub mod engine;
pub mod memo;
pub mod metrics;
pub mod replay;
pub mod report;
pub mod store;

pub use clock::{Clock, ManualClock, SystemClock};
pub use config::{Config, ConfigEnv, ConfigError, Loaded};
pub use engine::{BreakerStatus, Engine, EngineBuilder, OutcomeReport, input_bucket};
pub use metrics::Metrics;
pub use replay::{KeyReport, ReplayOptions, ReplayReport, Readiness};
pub use store::{KeyLister, OpenedStore, SnapshotStore, StoreView, open_store};

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
    /// Bad input (CLI exit 3).
    #[error("invalid input: {0}")]
    Input(String),
}

impl ContextInput {
    /// A full context.
    pub fn full(context: WorkloadContext) -> Self {
        ContextInput::Full { context: Box::new(context) }
    }
}

impl DecideRequest {
    /// A request for a full context with defaults elsewhere.
    pub fn for_context(context: WorkloadContext) -> Self {
        DecideRequest {
            context: ContextInput::full(context),
            constraints: Constraints::default(),
            state: PlatformState::default(),
            policy: Policy::default(),
            budget_ms: None,
            dry_run: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_round_trips() {
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
            policy: Policy { mode: Some(Mode::Recommend), ..Default::default() },
            budget_ms: Some(50),
            dry_run: true,
        };
        let j = serde_json::to_string(&req).unwrap();
        let back: DecideRequest = serde_json::from_str(&j).unwrap();
        assert_eq!(back, req);
    }
}
