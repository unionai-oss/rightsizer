//! Adapter settings, read from [`AdapterEnv::settings`]: the `[slurm]`
//! config section (`rightsizer-engine::config::SlurmConfig` field names) plus
//! the live inventory the daemon refreshes (slurm.md §7). Unknown keys are
//! ignored so the config section can grow independently.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use rightsizer_core::AcceleratorId;
use rightsizer_plugin::{AcceleratorMap, AdapterEnv};

use crate::inventory::Inventory;

/// `[slurm]` plus inventory.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SlurmSettings {
    /// Cluster name (tenant prefix).
    pub cluster: Option<String>,
    /// HMAC token key file.
    pub token_key_file: Option<PathBuf>,
    /// `sacct` binary.
    pub sacct: Option<PathBuf>,
    /// `sbatch` binary.
    pub sbatch: Option<PathBuf>,
    /// `squeue` binary.
    pub squeue: Option<PathBuf>,
    /// Canonical accelerator → gres type (`"A100-80G" = "gpu:a100_80g"` or `"a100_80g"`).
    pub gres: BTreeMap<String, String>,
    /// `ConstrainRAMSpace` (memory enforced); falls back to the inventory, then `true`.
    pub constrain_ram_space: Option<bool>,
    /// `JobAcctGatherFrequency` task interval, s; falls back to the inventory, then 30.
    pub job_acct_gather_frequency: Option<u32>,
    /// Default account for the caller (`sacctmgr show user withassoc`).
    pub default_account: Option<String>,
    /// Slurm version.
    pub version: Option<String>,
    /// Whether a `--watch` Caller ladder drives this submission.
    pub watch: bool,
    /// Ladder memory factor (2.0).
    pub ladder_memory_factor: Option<f32>,
    /// Ladder steps (2).
    pub ladder_max_steps: Option<u32>,
    /// Price per node-hour by partition (`"default"` as fallback).
    pub prices: BTreeMap<String, f64>,
    /// Submitters (user names) whose jobs are already sized (launchers.md §1.4).
    pub presized_submitters: Vec<String>,
    /// Live inventory.
    pub inventory: Inventory,
}

impl SlurmSettings {
    /// Read from an adapter env (invalid settings → defaults: S1).
    pub fn from_env(env: &AdapterEnv) -> Self {
        serde_json::from_value(serde_json::Value::Object(env.settings.clone())).unwrap_or_default()
    }

    /// As adapter-env settings.
    pub fn to_env_settings(&self) -> serde_json::Map<String, serde_json::Value> {
        match serde_json::to_value(self) {
            Ok(serde_json::Value::Object(m)) => m,
            _ => serde_json::Map::new(),
        }
    }

    /// Memory enforced.
    pub fn memory_enforced(&self) -> bool {
        self.constrain_ram_space.or(self.inventory.config.constrain_ram_space).unwrap_or(true)
    }

    /// Sampling interval of `sacct` usage, seconds (default 30).
    pub fn acct_frequency_s(&self) -> u32 {
        self.job_acct_gather_frequency.or(self.inventory.config.job_acct_gather_frequency).filter(|f| *f > 0).unwrap_or(30)
    }

    /// Cluster name.
    pub fn cluster_name(&self, env: &AdapterEnv) -> String {
        env.cluster.clone().or_else(|| self.cluster.clone()).or_else(|| self.inventory.config.cluster_name.clone()).unwrap_or_else(|| "default".into())
    }

    /// The site's gres-type table as an accelerator map (gres type names,
    /// lower-cased, without the `gpu:` prefix).
    pub fn accelerators(&self) -> AcceleratorMap {
        let mut m = AcceleratorMap::default();
        for (canon, native) in &self.gres {
            let Ok(id) = canon.parse::<AcceleratorId>() else { continue };
            let n = native.trim().strip_prefix("gres/").unwrap_or(native.trim());
            let n = n.strip_prefix("gpu:").unwrap_or(n);
            m.insert(id, n.to_ascii_lowercase());
        }
        m
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_config_fields_are_accepted() {
        let env = AdapterEnv {
            settings: serde_json::json!({"cluster": "gpu-a", "gres": {"A100-80G": "gpu:a100_80g", "H100-80G": "h100"}, "constrain_ram_space": false, "something_new": 1})
                .as_object()
                .unwrap()
                .clone(),
            ..Default::default()
        };
        let s = SlurmSettings::from_env(&env);
        assert_eq!(s.cluster.as_deref(), Some("gpu-a"));
        assert!(!s.memory_enforced());
        assert_eq!(s.acct_frequency_s(), 30);
        let m = s.accelerators();
        assert_eq!(m.canonical("a100_80g"), AcceleratorId::A100_80G);
        assert_eq!(m.native(AcceleratorId::H100_80G), Some("h100"));
        assert_eq!(m.canonical("v100"), AcceleratorId::UnknownGpu);
        assert_eq!(SlurmSettings::from_env(&AdapterEnv { settings: s.to_env_settings(), ..Default::default() }), s);
    }
}
