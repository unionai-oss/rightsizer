//! The [`Engine`] (ARCHITECTURE.md §4.1): decide, decide_group, attempts,
//! outcomes, breakers and control.
//!
//! `decide` never fails (S1). Every error, panic and expired budget yields a
//! decision that keeps the prior, with the reason recorded and counted.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use futures::FutureExt;
use rightsizer_core::breaker::{self, AttemptOutcome, BreakerObjects, BreakerParams, BreakerScope, BreakerState, Control, Reset, Trip, TripReason};
use rightsizer_core::constraints::{ConstraintParams, apply_limits};
use rightsizer_core::decide::prior_decision;
use rightsizer_core::policy::{BreakerView, Demotion, Effective, PolicyInputs, StoreHealth, effective_mode};
use rightsizer_core::safety::{self, Attribution, AttributionContext};
use rightsizer_core::{
    Advisory, AttemptContext, AttemptRecord, Axis, Bytes, Censor, Confidence, Decision, DecisionInput, DecisionParams, DecisionSource,
    Grid, Hold, HoldReason, Ladder, LadderDriver, Lease, Mode, PlatformRef, Prior, PriorReason, Requests, ResourceSpec,
    RunRecord, RunStatus, UnitId, WorkloadContext, WorkloadKey,
};
use rightsizer_estimators::EstimatorChain;
use rightsizer_features::{DefaultResolver, ReadBudget, ReadOptions, SsrfGuard};
use rightsizer_history::{DecisionFilter, HistoryStore};
use tokio::sync::Semaphore;
use ulid::Ulid;

use crate::chain::build_chain;
use crate::clock::{Clock, SystemClock, since};
use crate::config::{Config, ConfigError};
use crate::memo::{ChainResult, Memo, MemoKey};
use crate::metrics::Metrics;
use crate::store::{KeyLister, OpenedStore, Pending, StoreView, open_store};
use crate::{ContextInput, DecideRequest, EngineError};

/// Builds an [`Engine`].
pub struct EngineBuilder {
    config: Config,
    store: Option<OpenedStore>,
    chain: Option<EstimatorChain>,
    clock: Arc<dyn Clock>,
    metrics: Option<Metrics>,
    role: crate::store::Role,
}

impl EngineBuilder {
    /// Credentials role for opening `history.uri` (operators reset breakers and write control).
    pub fn role(mut self, role: crate::store::Role) -> Self {
        self.role = role;
        self
    }
    /// Use this store instead of opening `history.uri`.
    pub fn store(mut self, store: Arc<dyn HistoryStore>) -> Self {
        self.store = Some(OpenedStore { store, keys: None });
        self
    }
    /// Use this opened store (with key listing).
    pub fn opened_store(mut self, store: OpenedStore) -> Self {
        self.store = Some(store);
        self
    }
    /// Use this chain instead of building `[estimator] chain`.
    pub fn chain(mut self, chain: EstimatorChain) -> Self {
        self.chain = Some(chain);
        self
    }
    /// Inject a clock (tests).
    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }
    /// Use these metrics (to share a registry).
    pub fn metrics(mut self, metrics: Metrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Build. Configuration errors fail only with `strict = true`; otherwise
    /// the engine is inert: every decision keeps the prior with
    /// `Prior{reason = config_error}` (S1).
    pub async fn build(self) -> Result<Engine, EngineError> {
        let config = self.config;
        let metrics = self.metrics.unwrap_or_default();
        let mut errors: Vec<String> = Vec::new();
        if let Err(e) = config.validate() {
            errors.push(e.to_string());
        }
        let grid = Grid::by_id(&config.decision.grid).unwrap_or_else(Grid::rs_grid_v1);
        let opened = match self.store {
            Some(s) => s,
            None => match open_store(config.history.uri.as_deref(), &config.history, self.role).await {
                Ok(s) => s,
                Err(e) => {
                    errors.push(format!("history store: {e}"));
                    OpenedStore { store: Arc::new(rightsizer_history::MemoryStore::new()), keys: None }
                }
            },
        };
        let chain = match self.chain {
            Some(c) => c,
            None => build_chain(&config, None).await.unwrap_or_else(|e| {
                errors.push(e.to_string());
                EstimatorChain::default()
            }),
        };
        if !errors.is_empty() {
            if config.strict {
                return Err(EngineError::Config(ConfigError::Invalid(errors.join("; "))));
            }
            for e in &errors {
                tracing::error!(error = %e, "configuration error: rightsizer is inert (S1)");
                metrics.error("config", "invalid");
            }
        }
        let view = StoreView::with_clock(opened.store, config.history.clone(), config.breaker.cache_ttl, metrics.clone(), self.clock.clone());
        let memo = Memo::with_clock(grid.clone(), config.decision.memo_ttl, config.decision.memo_capacity, self.clock.clone()).with_point_sigma(config.decision.point_sigma);
        let limiter = Semaphore::new(config.decision.max_concurrent_decisions.max(1));
        let resolver = Arc::new(DefaultResolver::new(SsrfGuard::new(&config.features.allow_hosts)));
        let privacy = crate::chain::privacy(&config.privacy);
        let read_opts = ReadOptions::new(ReadBudget {
            per_input_bytes: config.features.max_input_bytes,
            per_context: Duration::from_millis(config.features.budget_ms.max(1) as u64),
            ..ReadBudget::default()
        })
        .with_guard(SsrfGuard::new(&config.features.allow_hosts))
        .with_salt(privacy.salt.clone());
        Ok(Engine {
            inner: Arc::new(Inner {
                resolver,
                read_opts,
                privacy,
                chains: Mutex::new(HashMap::new()),
                config_error: (!errors.is_empty()).then(|| errors.join("; ")),
                config,
                view,
                keys: opened.keys,
                chain,
                grid,
                memo,
                limiter,
                clock: self.clock,
                metrics,
                window: Mutex::new(Vec::new()),
                recent: Mutex::new(RecentDecisions::default()),
            }),
        })
    }
}

struct Inner {
    /// Input readers' resolver (SSRF-guarded).
    resolver: Arc<DefaultResolver>,
    /// Read options template (budgets, guard, salt); per decision the time budget and `hash_only` are set.
    read_opts: ReadOptions,
    privacy: rightsizer_features::Privacy,
    /// Chains for per-request `policy.chain` overrides, by member names.
    chains: Mutex<HashMap<Vec<String>, EstimatorChain>>,
    config: Config,
    config_error: Option<String>,
    view: StoreView,
    keys: Option<Arc<dyn KeyLister>>,
    chain: EstimatorChain,
    grid: Grid,
    memo: Memo,
    limiter: Semaphore,
    clock: Arc<dyn Clock>,
    metrics: Metrics,
    /// Recent attributed attempts (fleet and estimator breakers).
    window: Mutex<Vec<AttemptOutcome>>,
    /// Decisions this engine recorded recently, by id: `outcome` joins to them
    /// without `HistoryStore::decision` (which lists the decision log).
    recent: Mutex<RecentDecisions>,
}

/// A bounded id → decision map (oldest evicted first).
#[derive(Default)]
struct RecentDecisions {
    by_id: HashMap<ulid::Ulid, Decision>,
    order: std::collections::VecDeque<ulid::Ulid>,
}

/// Recorded decisions kept for outcome joins.
const RECENT_DECISIONS: usize = 20_000;

impl RecentDecisions {
    fn insert(&mut self, d: &Decision) {
        if self.by_id.insert(d.id, d.clone()).is_none() {
            self.order.push_back(d.id);
            while self.order.len() > RECENT_DECISIONS {
                if let Some(old) = self.order.pop_front() {
                    self.by_id.remove(&old);
                }
            }
        }
    }
}

/// The engine: owns the store view, the estimator chain, the memo and budgets.
/// Cheap to clone.
#[derive(Clone)]
pub struct Engine {
    inner: Arc<Inner>,
}

/// Internal knobs of one decision.
#[derive(Clone, Debug, Default)]
pub(crate) struct DecideOpts {
    pub attempt: Option<AttemptContext>,
    pub block_reductions: Option<HoldReason>,
    pub history: Option<Vec<RunRecord>>,
    pub record: bool,
    pub no_memo: bool,
}

/// Everything resolved for one decision before the rule runs.
pub(crate) struct Resolved {
    pub effective: Effective,
    pub skip: Vec<String>,
}

/// What [`Engine::outcome`] did.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OutcomeReport {
    /// Written now (false: buffered for retry).
    pub written: bool,
    /// Joined to its decision.
    pub joined: bool,
    /// `"caused"`, `"not_caused"`, `"pending"`, or `None` for successes.
    pub attribution: Option<String>,
    /// Breakers tripped by this outcome.
    pub tripped: Vec<BreakerScope>,
    /// A ladder lease was closed.
    pub lease_closed: bool,
}

/// What [`Engine::sweep`] found and did.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize)]
pub struct SweepReport {
    /// Keys looked at.
    pub keys: usize,
    /// Failed invocations whose ladder lease expired open.
    pub expired_leases: usize,
    /// Of those, failures attributed as not caused by rightsizer (no trip).
    pub not_caused: usize,
    /// Breakers tripped (dry run: that would be tripped).
    pub tripped: Vec<BreakerScope>,
    /// Leases closed.
    pub leases_closed: usize,
}

/// One breaker as `rsz breaker status` shows it.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct BreakerStatus {
    /// Scope.
    pub scope: BreakerScope,
    /// State now.
    pub state: BreakerState,
    /// Trips newer than the latest reset.
    pub open_trips: usize,
    /// The newest open trip's reason and time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_trip: Option<Trip>,
    /// Latest reset.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_reset: Option<DateTime<Utc>>,
}

/// The input-size bucket of a breaker scope: the memory-ladder cell of the input bytes.
pub fn input_bucket(bytes: Option<u64>, grid: &Grid) -> Option<String> {
    let b = bytes?;
    let (i, _) = rightsizer_core::grid::covering(&grid.values(rightsizer_core::grid::GridAxis::Memory), b as f64);
    Some(format!("input-cell-{i}"))
}

fn state_rank(s: BreakerState) -> u8 {
    match s {
        BreakerState::Closed => 0,
        BreakerState::HalfOpen => 1,
        BreakerState::Open => 2,
    }
}

fn platform_kind(req: &DecideRequest) -> String {
    match &req.context {
        ContextInput::Full { context } => context.platform.kind.clone(),
        ContextInput::Seed { platform, .. } => platform.kind.clone(),
    }
}

/// A context with no history or input metadata (fallback decisions).
pub(crate) fn bare_context(req: &DecideRequest) -> WorkloadContext {
    match &req.context {
        ContextInput::Full { context } => (**context).clone(),
        ContextInput::Seed { key, unit, prior, platform, .. } => WorkloadContext {
            v: rightsizer_core::WIRE_VERSION,
            key: key.clone(),
            unit: unit.clone(),
            platform: platform.clone(),
            code: None,
            inputs: vec![],
            manifest: Default::default(),
            prior: prior.clone(),
            history: vec![],
            labels: Default::default(),
        },
    }
}

fn group_of(req: &DecideRequest) -> Option<rightsizer_core::GroupId> {
    match &req.context {
        ContextInput::Seed { group, .. } => group.clone(),
        ContextInput::Full { .. } => None,
    }
}

fn source_label(s: &DecisionSource) -> &'static str {
    match s {
        DecisionSource::Estimator { .. } => "estimator",
        DecisionSource::Memo { .. } => "memo",
        DecisionSource::Prior { .. } => "prior",
    }
}

fn demotion_text(d: Demotion) -> &'static str {
    match d {
        Demotion::MaxMode => "capped by the surface's max_mode",
        Demotion::ModeCeiling => "capped by the fleet mode_ceiling",
        Demotion::WorkloadBreaker => "a per-workload breaker is open",
        Demotion::FleetBreaker => "the fleet breaker is open",
        Demotion::StoreUnreachable => "history unavailable past its cache TTL",
        Demotion::NonDurableStore => "the history store is not durable and shared",
    }
}

fn ulid_at(now: DateTime<Utc>) -> Ulid {
    Ulid::from_datetime(now.into())
}

impl Engine {
    /// A builder over `config`.
    pub fn builder(config: Config) -> EngineBuilder {
        EngineBuilder { config, store: None, chain: None, clock: Arc::new(SystemClock), metrics: None, role: crate::store::Role::Client }
    }

    /// Build from `config` alone: store from `history.uri`, chain from `[estimator]`.
    pub async fn new(config: Config) -> Result<Engine, EngineError> {
        Self::builder(config).build().await
    }

    /// The configuration.
    pub fn config(&self) -> &Config {
        &self.inner.config
    }

    /// The configuration error that made this engine inert, if any.
    pub fn config_error(&self) -> Option<&str> {
        self.inner.config_error.as_deref()
    }

    /// The store.
    pub fn store(&self) -> &Arc<dyn HistoryStore> {
        self.inner.view.store()
    }

    /// The store view (caches, buffer).
    pub fn view(&self) -> &StoreView {
        &self.inner.view
    }

    /// An injected key lister (compat; prefer [`Engine::keys`]).
    pub fn key_lister(&self) -> Option<&Arc<dyn KeyLister>> {
        self.inner.keys.as_ref()
    }

    /// Every workload key with stored runs: an injected lister, else
    /// `HistoryStore::keys()` (every backend), sorted and de-duplicated.
    pub async fn keys(&self) -> Result<Vec<WorkloadKey>, EngineError> {
        let mut ks = match &self.inner.keys {
            Some(l) => l.keys(),
            None => self.store().keys().await?,
        };
        ks.sort_by_key(|k| k.to_string());
        ks.dedup();
        Ok(ks)
    }

    /// The estimator chain.
    pub fn chain(&self) -> &EstimatorChain {
        &self.inner.chain
    }

    /// The grid.
    pub fn grid(&self) -> &Grid {
        &self.inner.grid
    }

    /// The memo.
    pub fn memo(&self) -> &Memo {
        &self.inner.memo
    }

    /// Metrics.
    pub fn metrics(&self) -> &Metrics {
        &self.inner.metrics
    }

    /// Now, from the injected clock.
    pub fn now(&self) -> DateTime<Utc> {
        self.inner.clock.now()
    }

    // ── decide ──────────────────────────────────────────────────────────

    /// Decide one unit. Never fails (S1).
    pub async fn decide(&self, req: DecideRequest) -> Decision {
        let record = !req.dry_run;
        self.decide_guarded(&req, DecideOpts { record, ..Default::default() }).await
    }

    /// The S1 wrapper: hard budget, panics caught, metrics.
    pub(crate) async fn decide_guarded(&self, req: &DecideRequest, opts: DecideOpts) -> Decision {
        let t0 = Instant::now();
        let platform = platform_kind(req);
        let budget = Duration::from_millis(req.budget_ms.unwrap_or(self.inner.config.policy.budget_ms).max(1) as u64);
        let deadline = t0 + budget;
        let fut = AssertUnwindSafe(self.decide_inner(req, &opts, deadline)).catch_unwind();
        let d = match tokio::time::timeout(budget, fut).await {
            Ok(Ok(d)) => d,
            Ok(Err(panic)) => {
                let msg = panic
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "panic".into());
                tracing::error!(panic = %msg, "decision panicked: kept the prior (S1)");
                self.inner.metrics.error("decide", "panic");
                self.fallback(req, PriorReason::Panic, Some(msg), opts.record)
            }
            Err(_) => {
                tracing::warn!(budget_ms = budget.as_millis() as u64, "decision budget expired: kept the prior (S1)");
                self.inner.metrics.budget_expired.with_label_values(&[&platform]).inc();
                self.fallback(req, PriorReason::Budget, Some(format!("budget of {} ms expired", budget.as_millis())), opts.record)
            }
        };
        let m = &self.inner.metrics;
        m.latency.with_label_values(&[&platform]).observe(t0.elapsed().as_secs_f64());
        m.decisions.with_label_values(&[d.mode.as_str(), source_label(&d.source), &platform]).inc();
        for h in &d.held {
            m.held.with_label_values(&[h.axis.as_str(), &plain(&h.reason)]).inc();
        }
        for a in &d.advisories {
            if let Advisory::Clamped { reason, .. } = a {
                m.clamped.with_label_values(&[reason]).inc();
            }
        }
        d
    }

    /// A prior decision for a request that failed before or during the rule.
    fn fallback(&self, req: &DecideRequest, reason: PriorReason, detail: Option<String>, record: bool) -> Decision {
        let ctx = bare_context(req);
        let grid = &self.inner.grid;
        let params = DecisionParams::default();
        let mut input = DecisionInput::new(&ctx, None, grid, &req.state, &req.constraints, &params, Mode::Recommend);
        input.id = ulid_at(self.now());
        input.group = group_of(req);
        let mut d = prior_decision(&input, reason, detail);
        d.mode = req.policy.mode.unwrap_or(self.inner.config.policy.mode).min(Mode::Recommend);
        if record && reason != PriorReason::Mode {
            // Never wait here: the budget is already spent.
            if let Ok(mut r) = self.inner.recent.lock() {
                r.insert(&d);
            }
            self.inner.view.defer(Pending::Decision(Box::new(d.clone())));
        }
        d
    }

    /// Build the context: full as given, or seed + history + input metadata
    /// (read within `features.budget_ms`, and never past half of what is
    /// left of the decision budget).
    pub(crate) async fn context_of(&self, req: &DecideRequest, opts: &DecideOpts, now: DateTime<Utc>, deadline: Option<Instant>) -> (WorkloadContext, bool) {
        let cfg = &self.inner.config;
        match &req.context {
            ContextInput::Full { context } => {
                let mut c = (**context).clone();
                if let Some(h) = &opts.history {
                    c.history = h.clone();
                }
                (c, true)
            }
            ContextInput::Seed { key, unit, seed, prior, platform, .. } => {
                let (history, available) = match &opts.history {
                    Some(h) => (h.clone(), true),
                    None => {
                        let r = self.inner.view.history(key, cfg.decision.history_limit, now).await;
                        (r.runs, r.available)
                    }
                };
                let ctx = self.seed_context(key, unit, seed, prior, platform, history, deadline).await;
                (ctx, available)
            }
        }
    }

    /// Seed → context via `rightsizer_features::build_context_with`: code
    /// info, distribution markers, input metadata read within
    /// min(`features.budget_ms`, half of what is left of the decision
    /// budget), salted URI hashes, allowlisted labels. With
    /// `manifest.extra.inputs_hash_only` (the K8s admission path) no input
    /// is read: URIs are only hashed. `history` is used as given (groups
    /// pass the union of their members' history), truncated to
    /// `decision.history_limit`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn seed_context(
        &self,
        key: &WorkloadKey,
        unit: &UnitId,
        seed: &rightsizer_core::ContextSeed,
        prior: &Prior,
        platform: &PlatformRef,
        history: Vec<RunRecord>,
        deadline: Option<Instant>,
    ) -> WorkloadContext {
        let cfg = &self.inner.config;
        let mut opts = self.inner.read_opts.clone();
        let mut per = Duration::from_millis(cfg.features.budget_ms.max(1) as u64);
        if let Some(d) = deadline {
            per = per.min(d.saturating_duration_since(Instant::now()) / 2);
        }
        opts.budget.per_context = per.max(Duration::from_millis(1));
        opts.hash_only = crate::context::inputs_hash_only(seed);
        let labels = std::collections::BTreeMap::new();
        let mut ctx = rightsizer_features::build_context_with(
            seed,
            key.clone(),
            unit.clone(),
            prior.clone(),
            platform.clone(),
            Vec::new(),
            &labels,
            &self.inner.privacy,
            &opts,
            self.inner.resolver.as_ref(),
        )
        .await;
        ctx.history = history;
        ctx.history.truncate(cfg.decision.history_limit);
        ctx
    }

    /// Resolve the effective mode and the estimator skip list.
    pub(crate) async fn resolve(&self, req: &DecideRequest, ctx: &WorkloadContext, history_available: bool, chain: &EstimatorChain, now: DateTime<Utc>) -> Resolved {
        let cfg = &self.inner.config;
        let view = &self.inner.view;
        let requested = req.policy.mode.unwrap_or(cfg.policy.mode);
        let max_mode = req.policy.max_mode.unwrap_or(Mode::Enforce).min(cfg.policy.max_mode);
        let mode_ceiling = view.mode_ceiling(now).await;
        let mut unreadable = false;
        let recovery = cfg.breaker.recovery_shadow_fits;

        // Per-workload and input-bucket breakers.
        let mut workload = BreakerState::Closed;
        let mut scopes = vec![BreakerScope::Workload { key: ctx.key.clone() }];
        if let Some(b) = input_bucket(ctx.input_bytes(), &self.inner.grid) {
            scopes.push(BreakerScope::InputBucket { key: ctx.key.clone(), bucket: b });
        }
        for s in scopes {
            match view.breaker(&s, now).await {
                Some(objs) => {
                    let fits = match objs.newest_open_trip() {
                        Some(t) if t.reason != TripReason::Manual => self.shadow_fits_since(&ctx.history, t.at, recovery).await,
                        _ => 0,
                    };
                    let st = breaker::state(&objs, fits, recovery);
                    if state_rank(st) > state_rank(workload) {
                        workload = st;
                    }
                }
                None => unreadable = true,
            }
        }
        let fleet = match view.breaker(&BreakerScope::Fleet, now).await {
            Some(o) => breaker::state(&o, 0, recovery),
            None => {
                unreadable = true;
                BreakerState::Closed
            }
        };
        // Per-estimator-version breakers: the chain skips open ones.
        let mut skip = Vec::new();
        for m in chain.members() {
            let i = m.info();
            let s = BreakerScope::EstimatorVersion { id: i.id.clone(), version: i.version.clone() };
            match view.breaker(&s, now).await {
                Some(o) if o.is_open() => skip.push(i.id.clone()),
                Some(_) => {}
                None => unreadable = true,
            }
        }
        let inputs = PolicyInputs {
            requested,
            max_mode,
            mode_ceiling,
            breakers: BreakerView { workload, estimator: BreakerState::Closed, fleet, unreadable },
            store: StoreHealth { history_available, durable_shared: view.info().durable_shared },
            can_classify: req.policy.can_classify.unwrap_or(true),
        };
        Resolved { effective: effective_mode(&inputs), skip }
    }

    /// Shadow decisions since `since` whose measured outcomes fit (breaker recovery).
    async fn shadow_fits_since(&self, history: &[RunRecord], since_t: DateTime<Utc>, need: u32) -> u32 {
        let mut fits = 0;
        let mut looked = 0;
        for r in history.iter().filter(|r| r.started_at > since_t && r.status == RunStatus::Succeeded) {
            if fits >= need || looked >= need * 4 {
                break;
            }
            let (Some(id), Some(peak)) = (r.decision_id, r.memory_peak) else { continue };
            if !matches!(peak.censor, Censor::Exact | Censor::AtMost) || r.status_confidence != Confidence::Observed {
                continue;
            }
            looked += 1;
            if let Ok(Some(d)) = self.store().decision(id).await
                && d.mode < Mode::Enforce
                && d.applied.requests.memory.is_none_or(|m| peak.value <= m.0 as f64)
            {
                fits += 1;
            }
        }
        fits
    }

    async fn chain_for(&self, req: &DecideRequest) -> Result<EstimatorChain, ConfigError> {
        match &req.policy.chain {
            Some(names) if names != &self.inner.config.estimator.chain => {
                if let Some(c) = self.inner.chains.lock().unwrap_or_else(|e| e.into_inner()).get(names) {
                    return Ok(c.clone());
                }
                let c = build_chain(&self.inner.config, Some(names)).await?;
                self.inner.chains.lock().unwrap_or_else(|e| e.into_inner()).insert(names.clone(), c.clone());
                Ok(c)
            }
            _ => Ok(self.inner.chain.clone()),
        }
    }

    fn memo_key(&self, ctx: &WorkloadContext, chain: &EstimatorChain, skip: &[String]) -> MemoKey {
        let mut ph = String::new();
        ph.push_str(&self.inner.grid.id);
        for m in chain.members() {
            let i = m.info();
            ph.push_str(&format!("|{}@{}", i.id, i.version));
            if skip.contains(&i.id) {
                ph.push_str("(skip)");
            }
        }
        MemoKey::new(ctx, &self.inner.grid, &rightsizer_core::digest::sha256_hex(ph.as_bytes())[..16])
    }

    /// The chain's answer: memo hit, or one (single-flight) chain call.
    pub(crate) async fn estimate(&self, ctx: &WorkloadContext, chain: &EstimatorChain, skip: &[String], use_memo: bool) -> (ChainResult, bool) {
        let run = || async {
            let _permit = self.inner.limiter.acquire().await.ok();
            let a = chain.estimate(ctx, skip).await;
            let mut any_error = false;
            let skipped = a
                .skipped
                .iter()
                .map(|(id, e)| {
                    if !e.is_insufficient() && !skip.contains(id) {
                        any_error = true;
                        self.inner.metrics.error("estimator", id);
                        tracing::warn!(estimator = %id, error = %e, "estimator failed (S1: next in chain)");
                    }
                    (id.clone(), e.to_string())
                })
                .collect();
            ChainResult { estimate: a.estimate, skipped, any_error }
        };
        if !use_memo {
            return (run().await, false);
        }
        let k = self.memo_key(ctx, chain, skip);
        self.inner.memo.get_or_compute(&k, run).await
    }

    pub(crate) fn params(&self, req: &DecideRequest, eff: &Effective, block: Option<HoldReason>) -> DecisionParams {
        let cfg = &self.inner.config;
        let pol = &req.policy;
        DecisionParams {
            risk: pol.risk.or_else(|| cfg.risk.profile()),
            exploration: pol.exploration.unwrap_or(cfg.policy.exploration),
            seed: 0,
            constraints: ConstraintParams {
                enforce_without_prior: pol.enforce_without_prior.unwrap_or(cfg.policy.enforce_without_prior),
                allow_gpu_type_change: pol.allow_gpu_type_change.unwrap_or(cfg.policy.allow_gpu_type_change),
                allowed_accelerators: cfg.policy.allowed_accelerators.clone(),
                allow_unverified: cfg.history.allow_unverified_for_reductions,
                max_step_down: cfg.policy.max_step_down,
                ..ConstraintParams::default()
            },
            min_history: cfg.policy.min_history,
            need_headroom: cfg.decision.need_headroom,
            point_sigma: cfg.decision.point_sigma,
            can_classify: pol.can_classify.unwrap_or(true),
            reductions_blocked: eff.reductions_blocked.or(block),
            ..DecisionParams::default()
        }
    }

    async fn decide_inner(&self, req: &DecideRequest, opts: &DecideOpts, deadline: Instant) -> Decision {
        let cfg = &self.inner.config;
        let now = self.now();
        let id = ulid_at(now);
        if let Some(e) = &self.inner.config_error {
            return self.fallback(req, PriorReason::ConfigError, Some(e.clone()), false);
        }
        // `off`: nothing is read, decided or recorded.
        let requested = req.policy.mode.unwrap_or(cfg.policy.mode);
        if requested == Mode::Off {
            let mut d = self.fallback(req, PriorReason::Mode, Some("mode off".into()), false);
            d.mode = Mode::Off;
            return d;
        }
        let chain = match self.chain_for(req).await {
            Ok(c) => c,
            Err(e) => return self.fallback(req, PriorReason::ConfigError, Some(e.to_string()), opts.record),
        };
        let (ctx, available) = self.context_of(req, opts, now, Some(deadline)).await;
        let r = self.resolve(req, &ctx, available, &chain, now).await;
        let mut d = self.decide_context(req, &ctx, &r, &chain, opts, id).await;
        if opts.record {
            self.record(&d, deadline).await;
        }
        if r.effective.mode < requested {
            for dm in &r.effective.demotions {
                d.explanation.push(format!("mode {} → {}: {}", requested, r.effective.mode, demotion_text(*dm)));
            }
        }
        d
    }

    /// The rule for a resolved context (shared by decide, attempts, groups and replay).
    pub(crate) async fn decide_context(&self, req: &DecideRequest, ctx: &WorkloadContext, r: &Resolved, chain: &EstimatorChain, opts: &DecideOpts, id: Ulid) -> Decision {
        let grid = &self.inner.grid;
        let eff = &r.effective;
        let params = self.params(req, eff, opts.block_reductions);
        let mut input = DecisionInput::new(ctx, None, grid, &req.state, &req.constraints, &params, eff.mode);
        input.id = id;
        input.group = group_of(req);
        input.attempt = opts.attempt.as_ref();
        if eff.mode <= Mode::Observe {
            let mut d = prior_decision(&input, PriorReason::Mode, Some(format!("mode {}", eff.mode)));
            d.mode = eff.mode;
            return d;
        }
        let (res, hit) = self.estimate(ctx, chain, &r.skip, !opts.no_memo).await;
        let Some(est) = res.estimate.as_ref() else {
            let reason = if res.any_error { PriorReason::EstimatorError } else { PriorReason::Insufficient };
            let detail = res.skipped.iter().map(|(id, e)| format!("{id}: {e}")).collect::<Vec<_>>().join("; ");
            return prior_decision(&input, reason, (!detail.is_empty()).then_some(detail));
        };
        let memo_src = hit.then(|| DecisionSource::Memo { id: est.provenance.estimator.clone(), version: est.provenance.version.clone() });
        input.estimate = Some(est);
        input.source = memo_src;
        let mut d = rightsizer_core::decide(&input);
        if !r.skip.is_empty() {
            d.explanation.push(format!("skipped by an open estimator breaker: {}", r.skip.join(", ")));
        }
        d
    }

    /// Write a decision within what is left of the budget; buffer otherwise.
    async fn record(&self, d: &Decision, deadline: Instant) {
        let left = deadline.saturating_duration_since(Instant::now());
        let view = &self.inner.view;
        let p = Pending::Decision(Box::new(d.clone()));
        if let Ok(mut r) = self.inner.recent.lock() {
            r.insert(d);
        }
        if tokio::time::timeout(left.mul_f32(0.8), view.put(p.clone())).await.is_err() {
            view.defer(p);
        }
        self.inner.metrics.recorded.with_label_values(&[&d_platform(d)]).inc();
        self.inner.metrics.update_join_ratio(&d_platform(d));
    }

    // ── groups ──────────────────────────────────────────────────────────

    /// Decide a group: one decision from the union of the members' history
    /// (demand = max across members), applied to every member (plugins.md §2).
    pub async fn decide_group(&self, reqs: Vec<DecideRequest>) -> Vec<Decision> {
        if reqs.is_empty() {
            return vec![];
        }
        let t0 = Instant::now();
        let budget = Duration::from_millis(reqs.iter().filter_map(|r| r.budget_ms).min().unwrap_or(self.inner.config.policy.budget_ms).max(1) as u64);
        let fut = AssertUnwindSafe(self.group_inner(&reqs, t0 + budget)).catch_unwind();
        match tokio::time::timeout(budget, fut).await {
            Ok(Ok(v)) => v,
            Ok(Err(_)) => {
                self.inner.metrics.error("decide_group", "panic");
                reqs.iter().map(|r| self.fallback(r, PriorReason::Panic, None, !r.dry_run)).collect()
            }
            Err(_) => {
                for r in &reqs {
                    self.inner.metrics.budget_expired.with_label_values(&[&platform_kind(r)]).inc();
                }
                reqs.iter().map(|r| self.fallback(r, PriorReason::Budget, None, !r.dry_run)).collect()
            }
        }
    }

    async fn group_inner(&self, reqs: &[DecideRequest], deadline: Instant) -> Vec<Decision> {
        let now = self.now();
        let cfg = &self.inner.config;
        // The union of the members' history, deduplicated, newest first.
        let mut union: Vec<RunRecord> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut all_available = true;
        for r in reqs {
            let (ctx, available) = self.context_of(r, &DecideOpts::default(), now, Some(deadline)).await;
            all_available &= available;
            for h in ctx.history {
                if seen.insert(rightsizer_history::dedup_key(&h)) {
                    union.push(h);
                }
            }
        }
        union.sort_by_key(|a| std::cmp::Reverse(a.started_at));
        union.truncate(cfg.decision.history_limit.saturating_mul(reqs.len().max(1)));
        let opts = DecideOpts { history: Some(union), ..Default::default() };
        let mut best: Option<Decision> = None;
        let mut min_mode = Mode::Enforce;
        for r in reqs {
            let chain = match self.chain_for(r).await {
                Ok(c) => c,
                Err(e) => return reqs.iter().map(|q| self.fallback(q, PriorReason::ConfigError, Some(e.to_string()), !q.dry_run)).collect(),
            };
            let (ctx, _) = self.context_of(r, &opts, now, Some(deadline)).await;
            let res = self.resolve(r, &ctx, all_available, &chain, now).await;
            let d = self.decide_context(r, &ctx, &res, &chain, &opts, ulid_at(now)).await;
            min_mode = min_mode.min(d.mode);
            let size = |d: &Decision| (d.applied.requests.memory, d.applied.requests.cpu);
            if best.as_ref().is_none_or(|b| size(&d) > size(b)) {
                best = Some(d);
            }
        }
        let Some(rep) = best else { return vec![] };
        let gid = reqs.iter().find_map(group_of);
        let out: Vec<Decision> = reqs
            .iter()
            .map(|r| {
                let ctx = bare_context(r);
                let mut d = rep.clone();
                d.key = ctx.key;
                d.unit = ctx.unit;
                d.group = gid.clone();
                d.mode = min_mode;
                d
            })
            .collect();
        let mut g = rep.clone();
        g.group = gid;
        g.mode = min_mode;
        g.explanation.push(format!("group of {}: one decision from the union of the members' history", reqs.len()));
        if reqs.iter().any(|r| !r.dry_run) {
            self.record(&g, deadline).await;
        }
        out.into_iter()
            .map(|mut d| {
                d.explanation = g.explanation.clone();
                d
            })
            .collect()
    }

    // ── attempts and ladders (SPEC §7.2) ────────────────────────────────

    /// Decide one attempt of an invocation (escalate-on-retry, attempt
    /// records). Attempt 1 is a normal decision; when it changes the request
    /// under `enforce`, the attempt record (and, for a reduced attempt behind
    /// a Caller ladder, the ladder lease) is written first: if that write
    /// fails, the attempt is not reduced. Attempt k ≥ 2 reads the record:
    ///
    /// - missing or unreadable: max(origin prior, a fresh decision with reductions disabled);
    /// - attempt 1 reduced: ladder step `min(k − 1, max_steps)` of attempt 1, or the fresh decision if larger;
    /// - otherwise: the fresh decision, never below attempt 1's request.
    pub async fn attempt_decide(&self, req: DecideRequest, attempt: AttemptContext) -> Decision {
        let record = !req.dry_run;
        let t0 = Instant::now();
        let budget = Duration::from_millis(req.budget_ms.unwrap_or(self.inner.config.policy.budget_ms).max(1) as u64);
        let fut = AssertUnwindSafe(self.attempt_inner(&req, attempt, record, t0 + budget)).catch_unwind();
        match tokio::time::timeout(budget, fut).await {
            Ok(Ok(d)) => d,
            Ok(Err(_)) => {
                self.inner.metrics.error("attempt", "panic");
                self.fallback(&req, PriorReason::Panic, None, record)
            }
            Err(_) => {
                self.inner.metrics.budget_expired.with_label_values(&[&platform_kind(&req)]).inc();
                self.fallback(&req, PriorReason::Budget, None, record)
            }
        }
    }

    async fn attempt_inner(&self, req: &DecideRequest, at: AttemptContext, record: bool, deadline: Instant) -> Decision {
        let ctx0 = bare_context(req);
        let key = ctx0.key.clone();
        let origin = ctx0.prior.spec.requests.clone();
        let ladder = ctx0.platform.retry.ladder.clone();
        let base = DecideOpts { attempt: Some(at.clone()), record: false, ..Default::default() };
        if at.attempt <= 1 {
            let mut d = self.decide_guarded(req, base.clone()).await;
            let changed = d.applied.requests != origin;
            if d.mode == Mode::Enforce && changed && !req.dry_run {
                let reduced = safety::reduced_incompressible(&d.applied.requests, &origin);
                let now = self.now();
                let caller = ladder.as_ref().is_some_and(|l| l.driver == LadderDriver::Caller);
                let rec = AttemptRecord {
                    v: rightsizer_core::WIRE_VERSION,
                    key: key.clone(),
                    invocation_id: at.invocation_id.clone(),
                    origin_prior: origin.clone(),
                    applied: d.applied.requests.clone(),
                    decision_id: Some(d.id),
                    reduced,
                    written_at: now,
                    lease: (reduced && caller).then_some(Lease { opened_at: now, closed_at: None, outcome: None }),
                };
                let left = deadline.saturating_duration_since(Instant::now());
                let wrote = matches!(tokio::time::timeout(left.mul_f32(0.5), self.store().attempt_put_if_absent(&rec)).await, Ok(Ok(_)));
                if !wrote {
                    self.inner.metrics.store_errors.with_label_values(&["attempt_put"]).inc();
                    if reduced {
                        tracing::warn!(key = %key, "attempt record write failed: the attempt is not reduced");
                        d = self.decide_guarded(req, DecideOpts { block_reductions: Some(HoldReason::StoreUnreachable), ..base }).await;
                        d.explanation.push("attempt record could not be written: reductions disabled for this attempt".into());
                    }
                }
            }
            if record {
                self.record(&d, deadline).await;
            }
            return d;
        }
        // Attempt k ≥ 2.
        let rec = self.store().attempt_get(&key, &at.invocation_id).await;
        let blocked = DecideOpts { block_reductions: Some(HoldReason::S3NotMet), ..base.clone() };
        let mut d;
        let explanation;
        match rec {
            Ok(Some(r)) if r.reduced => {
                d = self.decide_guarded(req, base.clone()).await;
                let l = ladder.clone().unwrap_or_else(Ladder::caller);
                let step = (at.attempt - 1).min(l.max_steps.max(1));
                let mut mem = r.applied.memory.map(|m| Bytes::from_f64(m.0 as f64 * (l.memory_factor.max(1.0) as f64).powi(step as i32)));
                if step >= l.max_steps.max(1) {
                    let history = match &req.context {
                        ContextInput::Full { context } => context.history.clone(),
                        ContextInput::Seed { .. } => self.inner.view.history(&key, self.inner.config.decision.history_limit, self.now()).await.runs,
                    };
                    let floor = safety::final_floor(r.origin_prior.memory, &history, &l, self.inner.config.history.allow_unverified_for_reductions);
                    mem = max_opt(mem, floor);
                }
                let target = Requests { memory: max_opt(mem, d.applied.requests.memory), cpu: max_opt(r.applied.cpu, d.applied.requests.cpu), ..d.applied.requests.clone() };
                explanation = format!("attempt {}: ladder step {step} of attempt 1 (reduced), or the fresh decision if larger", at.attempt);
                set_applied(&mut d, target);
            }
            Ok(Some(r)) => {
                d = self.decide_guarded(req, base.clone()).await;
                let target = Requests {
                    memory: max_opt(r.applied.memory, d.applied.requests.memory),
                    cpu: max_opt(r.applied.cpu, d.applied.requests.cpu),
                    ..d.applied.requests.clone()
                };
                explanation = format!("attempt {}: the fresh decision, never below attempt 1's request", at.attempt);
                set_applied(&mut d, target);
            }
            Ok(None) | Err(_) => {
                d = self.decide_guarded(req, blocked).await;
                let target = Requests { memory: max_opt(origin.memory, d.applied.requests.memory), cpu: max_opt(origin.cpu, d.applied.requests.cpu), ..d.applied.requests.clone() };
                explanation = format!("attempt {}: no attempt record; max(origin prior, a fresh decision with reductions disabled)", at.attempt);
                set_applied(&mut d, target);
            }
        }
        d.explanation.push(explanation);
        if record {
            self.record(&d, deadline).await;
        }
        d
    }

    /// Close an invocation's ladder lease (the classifying component calls this).
    pub async fn close_lease(&self, key: &WorkloadKey, invocation_id: &str, status: RunStatus) -> Result<(), EngineError> {
        self.store().attempt_close_lease(key, invocation_id, self.now(), status).await?;
        Ok(())
    }

    // ── outcomes (SPEC §2.3 S5, §8.4) ───────────────────────────────────

    /// Record an outcome: buffered write; join to its decision; attribution;
    /// breaker trips for rightsizer-caused failures that escalation did not
    /// recover; lease close on classification; memo invalidation.
    pub async fn outcome(&self, rec: RunRecord) -> Result<OutcomeReport, EngineError> {
        let cfg = &self.inner.config;
        let now = self.now();
        let mut report = OutcomeReport { written: self.inner.view.put(Pending::Run(Box::new(rec.clone()))).await, ..Default::default() };
        let key_s = rec.key.to_string();
        self.inner.memo.invalidate_workload(&key_s);
        self.inner.view.invalidate(&rec.key);
        let platform = rec.key.platform.clone();

        let decision = match rec.decision_id {
            Some(id) => match self.inner.recent.lock().ok().and_then(|r| r.by_id.get(&id).cloned()) {
                Some(d) => Some(d),
                None => self.store().decision(id).await.ok().flatten(),
            },
            None => None,
        };
        if decision.is_some() {
            report.joined = true;
            self.inner.metrics.joined.with_label_values(&[&platform]).inc();
            self.inner.metrics.update_join_ratio(&platform);
        }
        let attempt_rec = self.store().attempt_get(&rec.key, &rec.invocation_id).await.ok().flatten();
        let origin = rec
            .origin_prior
            .clone()
            .or_else(|| decision.as_ref().map(|d| d.prior.spec.requests.clone()))
            .or_else(|| attempt_rec.as_ref().map(|a| a.origin_prior.clone()))
            .unwrap_or_default();

        // Saved resource-hours (enforced successes).
        if rec.status == RunStatus::Succeeded
            && decision.as_ref().is_some_and(|d| d.mode == Mode::Enforce)
            && let (Some(req), Some(dur)) = (rec.requested.as_ref(), rec.duration_s)
        {
            let h = dur.max(0.0) / 3600.0;
            if let (Some(p), Some(a)) = (origin.memory, req.requests.memory)
                && p > a
            {
                self.inner.metrics.saved_hours.with_label_values(&["memory"]).inc_by((p.0 - a.0) as f64 / Bytes::GIB as f64 * h);
            }
            if let (Some(p), Some(a)) = (origin.cpu, req.requests.cpu)
                && p > a
            {
                self.inner.metrics.saved_hours.with_label_values(&["cpu"]).inc_by((p.0 - a.0) as f64 / 1000.0 * h);
            }
        }

        let mut attribution = Attribution::NotCaused;
        let mut beyond = false;
        if rec.status.is_failure() {
            let history = self.store().runs(&rec.key, cfg.decision.history_limit.max(64)).await.unwrap_or_default();
            let attempts: Vec<RunRecord> = history.iter().filter(|h| h.invocation_id == rec.invocation_id).cloned().collect();
            (attribution, beyond) = self.attribute_failure(&rec, &history, &origin, now);
            report.attribution = Some(match attribution {
                Attribution::Caused => "caused",
                Attribution::NotCaused => "not_caused",
                Attribution::Pending => "pending",
            }
            .into());
            if attribution == Attribution::Caused {
                self.inner.metrics.caused_failures.with_label_values(&[&platform]).inc();
                let recovered = attempts.iter().any(|a| a.attempt > rec.attempt && a.status == RunStatus::Succeeded);
                let failure_end = rec.started_at + chrono::Duration::milliseconds((rec.duration_s.unwrap_or(0.0).max(0.0) * 1000.0) as i64);
                let lease_live = attempt_rec.as_ref().and_then(|a| a.lease.as_ref()).is_some_and(|l| {
                    l.closed_at.is_none() && since(now, failure_end) < cfg.policy.lease_ttl
                });
                if !recovered && !lease_live {
                    let scope = self.failure_scope(&rec, beyond);
                    let detail = format!("invocation {} attempt {} failed rightsizer-caused ({:?}) and was not recovered", rec.invocation_id, rec.attempt, rec.status);
                    if self.trip_if_closed(scope.clone(), TripReason::UnrecoveredFailure, detail).await {
                        report.tripped.push(scope);
                    }
                }
            }
        }

        // The classifier closes the lease.
        let classified = rec.status != RunStatus::Unknown && !(rec.status.is_failure() && rec.status_confidence == Confidence::Inferred);
        if classified && attempt_rec.as_ref().and_then(|a| a.lease.as_ref()).is_some_and(|l| l.closed_at.is_none()) {
            report.lease_closed = self.store().attempt_close_lease(&rec.key, &rec.invocation_id, now, rec.status).await.is_ok();
        }

        // Fleet and per-estimator-version breakers over the recent window.
        let enforced = decision.as_ref().is_some_and(|d| d.mode == Mode::Enforce);
        let estimator = decision.as_ref().and_then(|d| match &d.source {
            DecisionSource::Estimator { id, version } | DecisionSource::Memo { id, version } => Some((id.clone(), version.clone())),
            DecisionSource::Prior { .. } => None,
        });
        let proposals = {
            let mut w = self.inner.window.lock().unwrap_or_else(|e| e.into_inner());
            w.push(AttemptOutcome {
                key: rec.key.clone(),
                invocation_id: rec.invocation_id.clone(),
                attempt: rec.attempt,
                at: rec.started_at,
                estimator,
                enforced,
                succeeded: rec.status == RunStatus::Succeeded,
                attribution,
                beyond_history: beyond,
                input_bucket: input_bucket(rec.input_bytes(), &self.inner.grid),
                lease_expired: false,
            });
            let horizon = now - chrono::Duration::days(31);
            w.retain(|o| o.at > horizon);
            if w.len() > 200_000 {
                let n = w.len() - 200_000;
                w.drain(..n);
            }
            breaker::evaluate(&w, now, &self.breaker_params())
        };
        for p in proposals {
            if matches!(p.scope, BreakerScope::Fleet | BreakerScope::EstimatorVersion { .. }) && self.trip_if_closed(p.scope.clone(), p.reason, p.detail).await {
                report.tripped.push(p.scope);
            }
        }
        Ok(report)
    }

    /// S5 attribution of a failed record against its key's `history` (the
    /// invocation's other attempts and the other invocations), as
    /// `(attribution, input beyond history)`. Shared by [`Engine::outcome`]
    /// and [`Engine::sweep`].
    fn attribute_failure(&self, rec: &RunRecord, history: &[RunRecord], origin: &Requests, now: DateTime<Utc>) -> (Attribution, bool) {
        let cfg = &self.inner.config;
        let attempts: Vec<RunRecord> = history.iter().filter(|h| h.invocation_id == rec.invocation_id).cloned().collect();
        let others: Vec<RunRecord> = history.iter().filter(|h| h.invocation_id != rec.invocation_id).cloned().collect();
        let beyond = safety::input_beyond_history(rec, &others);
        let actx = AttributionContext {
            origin_prior: origin.clone(),
            retry_at_or_above_prior_failed: safety::retry_at_or_above_prior_failed(rec, &attempts, origin),
            input_beyond_history: false,
            now,
            classification_window: chrono::Duration::from_std(cfg.policy.classification_window).unwrap_or_default(),
        };
        (safety::attribute(rec, &actx), beyond)
    }

    /// The breaker a caused, unrecovered failure trips: the input bucket when
    /// its input was beyond the history's sizes, else the workload.
    fn failure_scope(&self, rec: &RunRecord, beyond: bool) -> BreakerScope {
        match (beyond, input_bucket(rec.input_bytes(), &self.inner.grid)) {
            (true, Some(b)) => BreakerScope::InputBucket { key: rec.key.clone(), bucket: b },
            _ => BreakerScope::Workload { key: rec.key.clone() },
        }
    }

    /// The unclassified-failure sweep (SPEC §2.3 S5, §7.2): a ladder lease
    /// still open `policy.lease_ttl` past the end of its invocation's last
    /// failure means the escalation never reported, so that failure counts
    /// as **unrecovered**. For each such failure that is caused (or still
    /// unclassified: S5 counts it once the classification window passes)
    /// the per-workload breaker (input-bucket when the input was beyond the
    /// history) is tripped if closed, and the lease is closed with the
    /// failure's status so it is swept once. Run by `rsz history compact`;
    /// the K8s watcher can call it periodically. `dry_run` reports without
    /// writing.
    pub async fn sweep(&self, dry_run: bool) -> Result<SweepReport, EngineError> {
        let cfg = &self.inner.config;
        let now = self.now();
        let mut report = SweepReport::default();
        for key in self.keys().await? {
            report.keys += 1;
            let history = self.store().runs(&key, cfg.decision.history_limit.max(64)).await?;
            let mut invocations: Vec<&str> = history.iter().filter(|r| r.status.is_failure()).map(|r| r.invocation_id.as_str()).collect();
            invocations.sort_unstable();
            invocations.dedup();
            for inv in invocations {
                let Some(att) = self.store().attempt_get(&key, inv).await? else { continue };
                if !att.lease.as_ref().is_some_and(|l| l.closed_at.is_none()) {
                    continue;
                }
                let tries: Vec<&RunRecord> = history.iter().filter(|r| r.invocation_id == inv).collect();
                let Some(last) = tries.iter().filter(|r| r.status.is_failure()).max_by_key(|r| (r.attempt, r.started_at)).copied() else { continue };
                if tries.iter().any(|r| r.attempt > last.attempt && r.status == RunStatus::Succeeded) {
                    continue; // recovered; the classifier will close the lease
                }
                let end = last.started_at + chrono::Duration::milliseconds((last.duration_s.unwrap_or(0.0).max(0.0) * 1000.0) as i64);
                if since(now, end) < cfg.policy.lease_ttl {
                    continue; // the ladder may still report
                }
                report.expired_leases += 1;
                let origin = last.origin_prior.clone().unwrap_or_else(|| att.origin_prior.clone());
                let (attribution, beyond) = self.attribute_failure(last, &history, &origin, now);
                if attribution == Attribution::NotCaused {
                    report.not_caused += 1;
                } else {
                    let scope = self.failure_scope(last, beyond);
                    if dry_run {
                        report.tripped.push(scope);
                    } else {
                        let detail = format!(
                            "invocation {inv} attempt {} failed ({:?}) and its ladder lease expired open {} s past the failure: unrecovered",
                            last.attempt,
                            last.status,
                            cfg.policy.lease_ttl.as_secs()
                        );
                        if self.trip_if_closed(scope.clone(), TripReason::UnrecoveredFailure, detail).await {
                            report.tripped.push(scope);
                        }
                    }
                }
                if !dry_run {
                    match self.store().attempt_close_lease(&key, inv, now, last.status).await {
                        Ok(()) => report.leases_closed += 1,
                        Err(e) => tracing::warn!(error = %e, key = %key, invocation = inv, "sweep: lease close failed"),
                    }
                }
            }
        }
        Ok(report)
    }

    fn breaker_params(&self) -> BreakerParams {
        let b = &self.inner.config.breaker;
        BreakerParams {
            estimator_rate: b.estimator_rate,
            estimator_min: b.estimator_min,
            fast_rate: b.fast_rate,
            fast_min_failures: b.fast_min_failures,
            slow_rate: b.slow_rate,
            slow_min_invocations: b.slow_min_invocations,
            recovery_shadow_fits: b.recovery_shadow_fits,
            ..BreakerParams::default()
        }
    }

    async fn trip_if_closed(&self, scope: BreakerScope, reason: TripReason, detail: String) -> bool {
        if self.store().breaker_get(&scope).await.is_ok_and(|o| o.is_open()) {
            return false;
        }
        let now = self.now();
        let trip = Trip { v: rightsizer_core::WIRE_VERSION, id: ulid_at(now), scope: scope.clone(), at: now, reason, detail: Some(detail) };
        match self.store().breaker_trip(&trip).await {
            Ok(()) => {
                tracing::warn!(scope = %scope, ?reason, "breaker tripped");
                self.inner.metrics.breaker_open.with_label_values(&[&scope_label(&scope)]).set(1.0);
                self.inner.view.invalidate_breakers();
                true
            }
            Err(e) => {
                self.inner.metrics.store_errors.with_label_values(&["breaker_trip"]).inc();
                tracing::error!(error = %e, scope = %scope, "breaker trip failed");
                false
            }
        }
    }

    // ── breakers and control (operators) ────────────────────────────────

    /// Every breaker with at least one trip.
    pub async fn breaker_status(&self) -> Result<Vec<BreakerStatus>, EngineError> {
        let list = self.store().breaker_list().await?;
        let recovery = self.inner.config.breaker.recovery_shadow_fits;
        let mut out = Vec::new();
        for (scope, objs) in list {
            let fits = match (&scope, objs.newest_open_trip()) {
                (BreakerScope::Workload { key }, Some(t)) if t.reason != TripReason::Manual => {
                    let h = self.store().runs(key, self.inner.config.decision.history_limit).await.unwrap_or_default();
                    self.shadow_fits_since(&h, t.at, recovery).await
                }
                _ => 0,
            };
            out.push(status_of(scope, &objs, fits, recovery));
        }
        Ok(out)
    }

    /// Trip a breaker by hand (`rsz breaker trip`).
    pub async fn breaker_trip(&self, scope: BreakerScope, detail: Option<String>) -> Result<Trip, EngineError> {
        let now = self.now();
        let trip = Trip { v: rightsizer_core::WIRE_VERSION, id: ulid_at(now), scope, at: now, reason: TripReason::Manual, detail };
        self.store().breaker_trip(&trip).await?;
        self.inner.view.invalidate_breakers();
        Ok(trip)
    }

    /// Reset a breaker (operator credentials).
    pub async fn breaker_reset(&self, scope: BreakerScope, by: Option<String>) -> Result<Reset, EngineError> {
        let reset = Reset { v: rightsizer_core::WIRE_VERSION, scope, at: self.now(), by };
        self.store().breaker_reset(&reset).await?;
        self.inner.view.invalidate_breakers();
        Ok(reset)
    }

    /// The fleet control object.
    pub async fn control_get(&self) -> Result<Option<Control>, EngineError> {
        Ok(self.store().control_get().await?)
    }

    /// Set `mode_ceiling` (operator credentials).
    pub async fn control_set(&self, mode: Mode, by: Option<String>) -> Result<Control, EngineError> {
        let c = Control { v: rightsizer_core::WIRE_VERSION, mode_ceiling: mode, updated_at: self.now(), by };
        self.store().control_set(&c).await?;
        self.inner.view.invalidate_breakers();
        Ok(c)
    }

    /// Decisions from the log.
    pub async fn decisions(&self, filter: &DecisionFilter) -> Result<Vec<Decision>, EngineError> {
        Ok(self.store().decisions(filter).await?)
    }

    /// Flush buffered writes.
    pub async fn flush(&self) -> usize {
        self.inner.view.flush().await
    }

    /// A prior-like context for a key (CLI `outcome`): the latest decision's prior, if any.
    pub async fn latest_prior(&self, key: &WorkloadKey) -> Option<(Prior, UnitId, PlatformRef)> {
        let f = DecisionFilter { key: Some(key.clone()), limit: Some(1), ..Default::default() };
        let d = self.store().decisions(&f).await.ok()?.into_iter().next()?;
        Some((d.prior, d.unit, PlatformRef::new(&key.platform)))
    }
}

fn d_platform(d: &Decision) -> String {
    d.key.platform.clone()
}

fn plain<T: serde::Serialize>(v: &T) -> String {
    serde_json::to_value(v).ok().and_then(|v| v.as_str().map(str::to_string)).unwrap_or_default()
}

fn scope_label(s: &BreakerScope) -> String {
    match s {
        BreakerScope::Workload { .. } => "workload".into(),
        BreakerScope::InputBucket { .. } => "input_bucket".into(),
        BreakerScope::EstimatorVersion { id, version } => format!("estimator:{id}@{version}"),
        BreakerScope::Fleet => "fleet".into(),
    }
}

fn status_of(scope: BreakerScope, objs: &BreakerObjects, fits: u32, recovery: u32) -> BreakerStatus {
    BreakerStatus {
        state: breaker::state(objs, fits, recovery),
        open_trips: objs.open_trips().len(),
        last_trip: objs.newest_open_trip().cloned(),
        last_reset: objs.latest_reset(),
        scope,
    }
}

fn max_opt<T: Ord + Copy>(a: Option<T>, b: Option<T>) -> Option<T> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.max(y)),
        (x, None) => x,
        (None, y) => y,
    }
}

fn set_applied(d: &mut Decision, target: Requests) {
    if target != d.applied.requests {
        let limits = apply_limits(&d.prior, &target);
        d.applied = ResourceSpec { requests: target, limits };
        // Memory was raised past the rule's choice: any memory hold no longer describes `applied`.
        d.held.retain(|h: &Hold| !(h.axis == Axis::Memory && !h.reason.is_hard()));
    }
}
