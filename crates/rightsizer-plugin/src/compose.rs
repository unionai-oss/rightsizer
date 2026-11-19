//! The `decide` composite (plugins.md §3.1): native submission → the named
//! adapter's `extract` → the engine (one decision per unit, one per group)
//! → `render` → `validate` → `{decisions, patch, mode, summary}`.
//!
//! The same function serves the daemon (`rsz serve`), the conformance suite
//! and in-process callers, for built-in adapters and plugins alike
//! ([`Target`]). Platform-specific post-processing (Slurm tokens) is the
//! caller's.
//!
//! S1: every adapter or plugin failure (error, crash, timeout, rejection)
//! yields `Patch::None` with the reason in `summary`; only an authorization
//! failure ([`ComposeError::Forbidden`]) is an error, so a caller that may
//! not act for a tenant gets an error and never a decision.
//! The patch is `none` below `enforce`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use rightsizer_core::{AttemptContext, Decision, Mode, PlatformRef, PlatformState, ResourceSpec, Tenant};
use rightsizer_engine::{ContextInput, DecideRequest, Engine, Policy};

use crate::client::PluginAdapter;
use crate::protocol::DecideResult;
use crate::{Adapter, AdapterEnv, Extracted, Native, Patch, PlatformModel, Rejection, SkipReason, Unit, UnitId};

/// What `decide` runs: an adapter plus (optionally) a platform model.
#[async_trait]
pub trait Target: Send + Sync {
    /// Kind.
    fn kind(&self) -> &str;
    /// The adapter.
    fn adapter(&self) -> &dyn Adapter;
    /// Static platform facts for a unit.
    async fn reference(&self, unit: &Unit, env: &AdapterEnv) -> PlatformRef;
    /// Live state.
    async fn state(&self, tenant: &Tenant, env: &AdapterEnv) -> PlatformState;
}

/// A built-in (in-process) adapter and model.
#[derive(Clone)]
pub struct BuiltIn {
    /// Adapter.
    pub adapter: Arc<dyn Adapter>,
    /// Model.
    pub model: Option<Arc<dyn PlatformModel>>,
}

#[async_trait]
impl Target for BuiltIn {
    fn kind(&self) -> &str {
        self.adapter.kind()
    }
    fn adapter(&self) -> &dyn Adapter {
        self.adapter.as_ref()
    }
    async fn reference(&self, unit: &Unit, _env: &AdapterEnv) -> PlatformRef {
        match &self.model {
            Some(m) => m.reference(unit),
            None => PlatformRef::new(self.adapter.kind()),
        }
    }
    async fn state(&self, tenant: &Tenant, env: &AdapterEnv) -> PlatformState {
        match &self.model {
            Some(m) => m.state(tenant, env).await.unwrap_or_else(|e| {
                tracing::warn!(kind = self.adapter.kind(), error = %e, "platform state unavailable; using defaults");
                PlatformState::default()
            }),
            None => PlatformState::default(),
        }
    }
}

#[async_trait]
impl Target for PluginAdapter {
    fn kind(&self) -> &str {
        Adapter::kind(self)
    }
    fn adapter(&self) -> &dyn Adapter {
        self
    }
    async fn reference(&self, unit: &Unit, env: &AdapterEnv) -> PlatformRef {
        self.reference_rpc(unit, Duration::from_millis(env.deadline_ms.unwrap_or(500) as u64)).await
    }
    async fn state(&self, tenant: &Tenant, env: &AdapterEnv) -> PlatformState {
        self.state_rpc(tenant, env, Duration::from_millis(env.deadline_ms.unwrap_or(500) as u64)).await
    }
}

/// Options.
#[derive(Clone, Debug, Default)]
pub struct ComposeOptions {
    /// Requested mode (capped by policy, breakers, `mode_ceiling`).
    pub mode: Option<Mode>,
    /// The whole call's deadline (adapter calls + decisions), ms.
    pub budget_ms: Option<u32>,
    /// Decide without recording.
    pub dry_run: bool,
    /// The attempt (escalate-on-retry / Caller ladders).
    pub attempt: Option<AttemptContext>,
    /// Policy overrides (mode is taken from `mode`).
    pub policy: Policy,
}

/// The composite's outcome.
#[derive(Clone, Debug, Default)]
pub struct Composed {
    /// Tenant (when extract succeeded).
    pub tenant: Option<Tenant>,
    /// Units extracted.
    pub units: Vec<Unit>,
    /// One per unit.
    pub decisions: Vec<Decision>,
    /// The patch to apply (`none` below enforce, and on any failure).
    pub patch: Patch,
    /// The patch rendered from the decisions (also below enforce), when it validated.
    pub rendered: Patch,
    /// Effective mode (the lowest over the units; `off` when nothing was decided).
    pub mode: Mode,
    /// One line for humans.
    pub summary: String,
    /// Why the submission was skipped.
    pub skipped: Option<SkipReason>,
    /// `validate` rejected the rendered patch.
    pub rejection: Option<Rejection>,
    /// An adapter / plugin failure (S1: unchanged).
    pub error: Option<String>,
}

impl Composed {
    /// The protocol result.
    pub fn into_result(self, token: Option<String>) -> DecideResult {
        DecideResult { decisions: self.decisions, patch: self.patch, mode: self.mode, summary: self.summary, token }
    }

    fn unchanged(summary: String) -> Self {
        Composed { mode: Mode::Off, summary, ..Default::default() }
    }
}

/// Errors that are not decisions.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum ComposeError {
    /// The caller may not act for the tenant the adapter derived.
    #[error("forbidden: {0}")]
    Forbidden(String),
}

fn left(deadline: Option<Instant>) -> Option<u32> {
    deadline.map(|d| d.saturating_duration_since(Instant::now()).as_millis().min(u32::MAX as u128) as u32)
}

async fn within<T>(deadline: Option<Instant>, fut: impl std::future::Future<Output = T>) -> Option<T> {
    match deadline {
        Some(d) => tokio::time::timeout(d.saturating_duration_since(Instant::now()), fut).await.ok(),
        None => Some(fut.await),
    }
}

/// Human-readable bytes (`1.5 GiB`, `512 MiB`).
pub fn fmt_bytes(b: u64) -> String {
    const GIB: f64 = (1u64 << 30) as f64;
    const MIB: f64 = (1u64 << 20) as f64;
    let f = b as f64;
    if f >= GIB {
        let g = f / GIB;
        if (g - g.round()).abs() < 0.005 { format!("{} GiB", g.round() as u64) } else { format!("{g:.2} GiB") }
    } else {
        format!("{} MiB", (f / MIB).ceil() as u64)
    }
}

fn fmt_cpu(m: u32) -> String {
    if m.is_multiple_of(1000) { format!("{}", m / 1000) } else { format!("{m}m") }
}

/// The one-line summary of a set of decisions.
pub fn summarize(decisions: &[Decision], mode: Mode, applied: bool) -> String {
    let mut parts = Vec::new();
    for d in decisions {
        let (p, a) = (&d.prior.spec.requests, &d.applied.requests);
        let mut axes = Vec::new();
        match (p.memory, a.memory) {
            (Some(x), Some(y)) if x != y => axes.push(format!("memory {} -> {}", fmt_bytes(x.0), fmt_bytes(y.0))),
            (None, Some(y)) => axes.push(format!("memory -> {}", fmt_bytes(y.0))),
            _ => {}
        }
        match (p.cpu, a.cpu) {
            (Some(x), Some(y)) if x != y => axes.push(format!("cpu {} -> {}", fmt_cpu(x.0), fmt_cpu(y.0))),
            _ => {}
        }
        let what = if axes.is_empty() {
            let held: Vec<String> = d.held.iter().map(|h| format!("{:?}", h.reason).to_lowercase()).collect::<std::collections::BTreeSet<_>>().into_iter().collect();
            if held.is_empty() { "keeps the request".to_string() } else { format!("keeps the request (held: {})", held.join(", ")) }
        } else {
            axes.join(", ")
        };
        parts.push(format!("{}: {what}", d.unit));
    }
    let verb = match (mode, applied) {
        (Mode::Enforce, true) => "applied",
        (Mode::Enforce, false) => "nothing to apply",
        (Mode::Recommend, _) => "recommendation, not applied",
        (Mode::Observe, _) => "observe only",
        (Mode::Off, _) => "off",
    };
    let ids: Vec<String> = decisions.iter().map(|d| d.id.to_string()).collect();
    let key = decisions.first().map(|d| d.key.to_string()).unwrap_or_default();
    format!("[{mode}] {} ({verb}; {key}; decision {})", parts.join("; "), ids.join(","))
}

/// Run the composite. `authorize` checks the tenant the adapter derived.
pub async fn decide_native(
    engine: &Engine,
    target: &dyn Target,
    native: &Native,
    env: &AdapterEnv,
    opts: &ComposeOptions,
    authorize: &(dyn Fn(&Tenant) -> Result<(), String> + Sync),
) -> Result<Composed, ComposeError> {
    let deadline = opts.budget_ms.map(|b| Instant::now() + Duration::from_millis(b as u64));
    let kind = target.kind().to_string();
    let mut env = env.clone();
    // Adapter calls get at most 40 % of the budget each.
    env.deadline_ms = opts.budget_ms.map(|b| (b * 2 / 5).max(1));

    // 1. extract.
    let ex: Extracted = match within(deadline, target.adapter().extract(native, &env)).await {
        Some(Ok(x)) => x,
        Some(Err(e)) => return Ok(Composed { error: Some(e.to_string()), ..Composed::unchanged(format!("{kind}: not sized: extract failed ({e})")) }),
        None => return Ok(Composed { error: Some("deadline".into()), ..Composed::unchanged(format!("{kind}: not sized: extract exceeded the deadline")) }),
    };
    authorize(&ex.tenant).map_err(ComposeError::Forbidden)?;
    if let Some(s) = &ex.skip {
        let r = serde_json::to_value(s).ok().and_then(|v| v.get("kind").and_then(|k| k.as_str()).map(String::from)).unwrap_or_default();
        return Ok(Composed { tenant: Some(ex.tenant.clone()), skipped: Some(s.clone()), ..Composed::unchanged(format!("{kind}: not sized ({r})")) });
    }
    if ex.units.is_empty() {
        return Ok(Composed { tenant: Some(ex.tenant.clone()), skipped: Some(SkipReason::NoUnits), ..Composed::unchanged(format!("{kind}: no resource-bearing units")) });
    }

    // 2. requests, grouped.
    let state = within(deadline, target.state(&ex.tenant, &env)).await.unwrap_or_default();
    let mut groups: BTreeMap<String, Vec<(usize, DecideRequest)>> = BTreeMap::new();
    for (i, u) in ex.units.iter().enumerate() {
        let key = match u.workload_key(&kind, &ex.tenant) {
            Ok(k) => k,
            Err(e) => return Ok(Composed { tenant: Some(ex.tenant.clone()), error: Some(e.to_string()), ..Composed::unchanged(format!("{kind}: not sized: bad key ({e})")) }),
        };
        let platform = target.reference(u, &env).await;
        let g = u.group.as_ref().map(|g| format!("g:{}", g.0)).unwrap_or_else(|| format!("u:{i}"));
        let mut policy = opts.policy.clone();
        policy.mode = opts.mode.or(policy.mode);
        groups.entry(g).or_default().push((
            i,
            DecideRequest {
                context: ContextInput::Seed { key, unit: u.id.clone(), group: u.group.clone(), seed: u.seed.clone(), prior: u.prior.clone(), platform },
                constraints: u.constraints.clone(),
                state: state.clone(),
                policy,
                budget_ms: left(deadline).map(|l| l.max(1)),
                dry_run: opts.dry_run,
            },
        ));
    }

    // 3. decide (one decision per group; S1 inside the engine).
    let mut decided: Vec<(usize, Decision)> = Vec::new();
    for (_, mut g) in groups {
        if g.len() == 1 {
            let (i, r) = g.remove(0);
            let d = match &opts.attempt {
                Some(a) => engine.attempt_decide(r, a.clone()).await,
                None => engine.decide(r).await,
            };
            decided.push((i, d));
        } else {
            let idx: Vec<usize> = g.iter().map(|(i, _)| *i).collect();
            let ds = engine.decide_group(g.into_iter().map(|(_, r)| r).collect()).await;
            decided.extend(idx.into_iter().zip(ds));
        }
    }
    decided.sort_by_key(|(i, _)| *i);
    let decisions: Vec<Decision> = decided.into_iter().map(|(_, d)| d).collect();
    let mode = decisions.iter().map(|d| d.mode).min().unwrap_or(Mode::Off);

    // 4. render + validate (changed units only).
    let changes: Vec<(UnitId, ResourceSpec)> = decisions.iter().filter(|d| d.applied != d.prior.spec).map(|d| (d.unit.clone(), d.applied.clone())).collect();
    let mut out = Composed { tenant: Some(ex.tenant.clone()), units: ex.units.clone(), mode, ..Default::default() };
    if !changes.is_empty() {
        match within(deadline, target.adapter().render(native, &changes)).await {
            Some(Ok(p)) if !p.is_none() => match within(deadline, target.adapter().validate(native, &p, &env)).await {
                Some(Ok(())) => out.rendered = p,
                Some(Err(r)) => out.rejection = Some(r),
                None => out.error = Some("validate exceeded the deadline".into()),
            },
            Some(Ok(_)) => {}
            Some(Err(e)) => out.error = Some(format!("render failed: {e}")),
            None => out.error = Some("render exceeded the deadline".into()),
        }
    }
    if mode == Mode::Enforce {
        out.patch = out.rendered.clone();
    }
    let applied = !out.patch.is_none();
    out.summary = format!("{kind} {}", summarize(&decisions, mode, applied));
    if let Some(r) = &out.rejection {
        out.summary.push_str(&format!("; patch rejected by validate ({}{}), kept the prior", r.reason, r.detail.as_deref().map(|d| format!(": {d}")).unwrap_or_default()));
    }
    if let Some(e) = &out.error {
        out.summary.push_str(&format!("; {e}, kept the prior"));
    }
    out.decisions = decisions;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes() {
        assert_eq!(fmt_bytes(1 << 30), "1 GiB");
        assert_eq!(fmt_bytes(3 << 29), "1.50 GiB");
        assert_eq!(fmt_bytes(100 << 20), "100 MiB");
    }
}
