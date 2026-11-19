//! Glue between the pure adapter and the async engine (feature `engine`):
//! adapter settings from `[k8s]`, signing keys, decision requests per group,
//! decisions → render units. Shared by the webhook, the watcher and the CLI
//! (`rsz recommend|apply -p k8s`).

use std::collections::BTreeMap;
use std::path::Path;

use rightsizer_core::{Decision, Mode, PlatformState, UnitId};
use rightsizer_engine::config::K8sConfig;
use rightsizer_engine::{ContextInput, DecideRequest, Policy};
use rightsizer_plugin::{Adapter, AdapterEnv, Extracted, PlatformModel, Unit};
use serde_json::{Map, Value};

use crate::K8sAdapter;
use crate::prior::SigningKeys;
use crate::render::UnitRender;
use crate::settings::K8sSettings;

/// `[k8s]` as adapter settings: the whole table (unknown keys are ignored by
/// [`K8sSettings`]) with `[k8s.adapter]` merged on top.
pub fn settings_map(cfg: &K8sConfig) -> Map<String, Value> {
    let mut m = serde_json::to_value(cfg).ok().and_then(|v| v.as_object().cloned()).unwrap_or_default();
    if let Some(Value::Object(extra)) = m.remove("adapter") {
        for (k, v) in extra {
            m.insert(k, v);
        }
    }
    // `[k8s] inputs` is the prefetcher's, not the adapter's.
    m.remove("inputs");
    m
}

/// Static adapter settings from `[k8s]`.
pub fn settings(cfg: &K8sConfig) -> Result<K8sSettings, String> {
    let env = AdapterEnv { cluster: cfg.cluster.clone(), settings: settings_map(cfg), ..Default::default() };
    K8sSettings::from_env(&env).map_err(|e| e.to_string())
}

/// Read a signing key file (`kid:base64` lines, first active).
pub fn load_keys(path: &Path) -> Result<SigningKeys, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    SigningKeys::parse(&text).map_err(|e| format!("{}: {e}", path.display()))
}

/// An adapter from `[k8s]` (with keys when `signing_key_file` is set and readable).
pub fn adapter(cfg: &K8sConfig) -> Result<K8sAdapter, String> {
    let a = K8sAdapter::new(settings(cfg)?);
    Ok(match &cfg.signing_key_file {
        Some(p) => a.with_keys(load_keys(p)?),
        None => a,
    })
}

/// One decision request per unit, grouped (a unit without a group is its own group).
pub fn requests(adapter: &K8sAdapter, ex: &Extracted, state: &PlatformState, policy: &Policy, budget_ms: Option<u32>, dry_run: bool) -> Result<Vec<Vec<(Unit, DecideRequest)>>, String> {
    let mut groups: BTreeMap<String, Vec<(Unit, DecideRequest)>> = BTreeMap::new();
    for (i, u) in ex.units.iter().enumerate() {
        let key = u.workload_key(adapter.kind(), &ex.tenant).map_err(|e| e.to_string())?;
        let pref = adapter.reference(u);
        let g = u.group.as_ref().map(|g| format!("g:{}", g.0)).unwrap_or_else(|| format!("u:{i:05}"));
        let req = DecideRequest {
            context: ContextInput::Seed { key, unit: u.id.clone(), group: u.group.clone(), seed: u.seed.clone(), prior: u.prior.clone(), platform: pref },
            constraints: u.constraints.clone(),
            state: state.clone(),
            policy: policy.clone(),
            budget_ms,
            dry_run,
        };
        groups.entry(g).or_default().push((u.clone(), req));
    }
    Ok(groups.into_values().collect())
}

/// Decide every group (one `decide` or `decide_group` call each).
pub async fn decide_all(engine: &rightsizer_engine::Engine, groups: Vec<Vec<(Unit, DecideRequest)>>) -> Vec<(Unit, Decision)> {
    let mut out = Vec::new();
    for g in groups {
        let (units, reqs): (Vec<Unit>, Vec<DecideRequest>) = g.into_iter().unzip();
        let ds = if reqs.len() == 1 { vec![engine.decide(reqs.into_iter().next().expect("one")).await] } else { engine.decide_group(reqs).await };
        out.extend(units.into_iter().zip(ds));
    }
    out
}

/// The object's render mode: `enforce` when any unit's effective mode is
/// enforce, else the highest unit mode (annotation-only).
pub fn render_mode(decisions: &[(Unit, Decision)]) -> Mode {
    decisions.iter().map(|(_, d)| d.mode).max().unwrap_or(Mode::Off)
}

/// Render units: enforce units get `applied`; units decided below enforce
/// are left out of an enforced object (untouched), and show `recommended`
/// when the object is annotation-only.
pub fn render_units(decisions: &[(Unit, Decision)], mode: Mode) -> Vec<UnitRender> {
    decisions
        .iter()
        .filter_map(|(u, d)| {
            let spec = match (mode, d.mode) {
                // Nothing to change: no requests to write (render minimal). The
                // effective prior may hold LimitRange defaults the object does
                // not declare; writing them would be an unasked-for change.
                (Mode::Enforce, Mode::Enforce) if !changes(u, d) => rightsizer_core::ResourceSpec::default(),
                (Mode::Enforce, Mode::Enforce) => d.applied.clone(),
                (Mode::Enforce, _) => return None,
                _ => d.recommended.clone(),
            };
            Some(UnitRender { unit: u.id.clone(), spec, defaulted: u.prior.defaulted, decision_id: Some(d.id.to_string()) })
        })
        .collect()
}

/// Whether the decision changes the unit's CPU or memory.
pub fn changes(u: &Unit, d: &Decision) -> bool {
    let (a, p) = (&d.applied, &u.prior.spec);
    a.requests.cpu != p.requests.cpu || a.requests.memory != p.requests.memory || a.limits.cpu != p.limits.cpu || a.limits.memory != p.limits.memory
}

/// Units by id.
pub fn by_id(decisions: &[(Unit, Decision)]) -> BTreeMap<UnitId, Decision> {
    decisions.iter().map(|(u, d)| (u.id.clone(), d.clone())).collect()
}

/// The decision ids the object carries: per-unit ids from the
/// `rightsizer.io/recommendation` annotation, falling back to
/// `rightsizer.io/decision-id` for every unit.
pub fn decision_ids(object: &Value) -> (Option<String>, BTreeMap<String, String>) {
    let ann = crate::objects::string_map(object, "/metadata/annotations");
    let obj = ann.get(crate::extract::DECISION_ID).cloned();
    let mut per = BTreeMap::new();
    if let Some(r) = ann.get(crate::extract::RECOMMENDATION).and_then(|s| serde_json::from_str::<Value>(s).ok()) {
        let id = r.get("id").and_then(Value::as_str).map(str::to_string);
        if let Some(units) = r.get("units").and_then(Value::as_object) {
            for (u, v) in units {
                if let Some(d) = v.get("decision").and_then(Value::as_str).map(str::to_string).or_else(|| id.clone()) {
                    per.insert(u.clone(), d);
                }
            }
        }
    }
    (obj, per)
}

/// A decision id read from an object's annotations (user-settable) for an
/// outcome of `key`: kept unless the log holds that decision for another
/// workload. Another tenant's (or workload's) decision id copied onto an
/// object must not join this outcome to that decision: the engine looks
/// decisions up by id alone for attribution, saved-hours and breaker
/// recovery (SPEC §2.3 S5, §8.2). An id the log does not know (not flushed
/// yet, or made up) joins nothing and is kept.
pub async fn verified_decision_id(engine: &rightsizer_engine::Engine, id: Option<ulid::Ulid>, key: &rightsizer_core::WorkloadKey) -> Option<ulid::Ulid> {
    let id = id?;
    match engine.store().decision(id).await {
        Ok(Some(d)) if d.key != *key => {
            tracing::warn!(decision = %id, decision_key = %d.key, outcome_key = %key, "decision id of another workload ignored for the join");
            None
        }
        _ => Some(id),
    }
}
