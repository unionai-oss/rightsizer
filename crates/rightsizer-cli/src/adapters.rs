//! Adapter dispatch for `rsz recommend -p PLATFORM` / `rsz explain -p`,
//! through `rightsizer_plugin::Adapter` (+ `PlatformModel` where the adapter
//! has one). Adapters are compiled in by cargo features (`k8s`, `slurm`).
//!
//! Integration status: the K8s adapter's `extract` / `PlatformModel` are
//! used as they stand; the Slurm adapter has no `PlatformModel` yet (its
//! units get `PlatformRef::new("slurm")` and default state) and its
//! `extract` may still return `Unsupported`, which is reported as exit 1.
//! TODO(rightsizer-slurm): pass its `PlatformModel` here when it lands.

use std::collections::BTreeMap;

use rightsizer_core::{PlatformRef, PlatformState, WorkloadKey};
use rightsizer_engine::{Config, ContextInput, DecideRequest, Policy};
#[allow(unused_imports)]
use rightsizer_plugin::{Adapter, AdapterEnv, Extracted, Native, PlatformModel};

use crate::Fail;

fn media_type(file: &str, platform: &str) -> String {
    match platform {
        "slurm" => "text/x-shellscript".into(),
        _ if file.ends_with(".json") => "application/json".into(),
        _ => "application/yaml".into(),
    }
}

/// Requests per group (a unit without a group is its own group).
#[allow(dead_code)]
async fn units(adapter: &dyn Adapter, model: Option<&dyn PlatformModel>, platform: &str, native: Native, env: AdapterEnv) -> Result<Vec<Vec<DecideRequest>>, Fail> {
    let ex: Extracted = adapter.extract(&native, &env).await.map_err(|e| {
        let msg = format!("{platform} adapter: {e}");
        if matches!(e, rightsizer_plugin::PluginError::InvalidNative(_)) { Fail::parse(msg) } else { Fail::other(msg) }
    })?;
    if let Some(skip) = &ex.skip {
        eprintln!("rsz: {platform}: submission skipped: {}", serde_json::to_string(skip).unwrap_or_default());
        return Ok(vec![]);
    }
    let state = match model {
        Some(m) => m.state(&ex.tenant, &env).await.unwrap_or_else(|e| {
            eprintln!("rsz: {platform}: platform state unavailable ({e}); using defaults");
            PlatformState::default()
        }),
        None => PlatformState::default(),
    };
    let mut groups: BTreeMap<String, Vec<DecideRequest>> = BTreeMap::new();
    for (i, u) in ex.units.into_iter().enumerate() {
        let key = WorkloadKey::new(platform, ex.tenant.clone(), u.key_suffix.clone()).map_err(Fail::parse)?;
        let pref = model.map(|m| m.reference(&u)).unwrap_or_else(|| PlatformRef::new(platform));
        let g = u.group.as_ref().map(|g| format!("g:{}", g.0)).unwrap_or_else(|| format!("u:{i}"));
        groups.entry(g).or_default().push(DecideRequest {
            context: ContextInput::Seed { key, unit: u.id, group: u.group, seed: u.seed, prior: u.prior, platform: pref },
            constraints: u.constraints,
            state: state.clone(),
            policy: Policy::default(),
            budget_ms: None,
            dry_run: false,
        });
    }
    Ok(groups.into_values().collect())
}

/// Extract decision requests from a native submission.
#[allow(unused_variables)]
pub async fn extract(platform: &str, file: &str, bytes: Vec<u8>, cfg: &Config) -> Result<Vec<Vec<DecideRequest>>, Fail> {
    let native = Native::Document { media_type: media_type(file, platform), bytes };
    match platform {
        #[cfg(feature = "k8s")]
        "k8s" | "kubernetes" => {
            let env = AdapterEnv {
                cluster: cfg.k8s.cluster.clone(),
                settings: serde_json::to_value(&cfg.k8s).ok().and_then(|v| v.as_object().cloned()).unwrap_or_default(),
                ..Default::default()
            };
            let a = rightsizer_k8s::K8sAdapter::new(Default::default());
            units(&a, Some(&a), "k8s", native, env).await
        }
        #[cfg(feature = "slurm")]
        "slurm" => {
            let env = AdapterEnv {
                cluster: cfg.slurm.cluster.clone(),
                settings: serde_json::to_value(&cfg.slurm).ok().and_then(|v| v.as_object().cloned()).unwrap_or_default(),
                ..Default::default()
            };
            // `default()`: the adapter is a unit struct today and will grow settings.
            #[allow(clippy::default_constructed_unit_structs)]
            let a = rightsizer_slurm::SlurmAdapter::default();
            units(&a, None, "slurm", native, env).await
        }
        other => Err(Fail::parse(format!(
            "no adapter for platform {other:?} in this build (compiled in: {})",
            compiled().join(", ")
        ))),
    }
}

fn compiled() -> Vec<&'static str> {
    #[allow(unused_mut)]
    let mut v = vec!["json"];
    #[cfg(feature = "k8s")]
    v.push("k8s");
    #[cfg(feature = "slurm")]
    v.push("slurm");
    v
}
