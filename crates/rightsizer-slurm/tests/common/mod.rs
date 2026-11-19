//! Shared test helpers: an inventory built from the fixtures captured on the
//! Docker Slurm cluster (26.05.5) plus a synthetic GPU node file.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::PathBuf;

use rightsizer_plugin::{AdapterEnv, Native};
use rightsizer_slurm::SlurmSettings;
use rightsizer_slurm::adapter::SlurmNative;
use rightsizer_slurm::inventory::{self, Inventory};

pub fn fixture(rel: &str) -> String {
    std::fs::read_to_string(fixture_path(rel)).unwrap_or_else(|e| panic!("{rel}: {e}"))
}

pub fn fixture_path(rel: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(rel)
}

/// The captured cluster: partitions `normal` (c1, c2: 2 CPUs, 2048 MiB) and
/// `gpu` (MaxMemPerNode 32000; synthetic nodes g1: 64 CPUs/a100:4, g2: 96 CPUs/h100:8),
/// DefMemPerCPU=256, associations of alice/mlteam/vision, QOS gpuq.
pub fn inventory() -> Inventory {
    let mut nodes = inventory::parse_show_node(&fixture("scontrol/show-node.txt"));
    nodes.extend(inventory::parse_show_node(&fixture("scontrol/show-node-gpu.txt")));
    Inventory {
        config: inventory::parse_show_config(&fixture("scontrol/show-config.txt")),
        partitions: inventory::parse_show_partition(&fixture("scontrol/show-partition.txt")),
        nodes,
        assocs: inventory::parse_sacctmgr_assoc(&fixture("sacctmgr/assoc.txt")),
        qos: inventory::parse_sacctmgr_qos(&fixture("sacctmgr/qos.txt")),
    }
}

pub fn settings() -> SlurmSettings {
    let mut s = SlurmSettings { cluster: Some("rsz-it".into()), inventory: inventory(), ..Default::default() };
    s.gres.insert("A100-80G".into(), "gpu:a100".into());
    s.gres.insert("H100-80G".into(), "gpu:h100".into());
    s.gres.insert("A10-24G".into(), "gpu:v100".into());
    s
}

pub fn env() -> AdapterEnv {
    AdapterEnv { cluster: Some("rsz-it".into()), principal: Some("alice".into()), settings: settings().to_env_settings(), ..Default::default() }
}

pub fn sbatch(argv: &str, script: Option<&str>) -> Native {
    let n = SlurmNative::Sbatch { argv: shlex::split(argv).unwrap(), script: script.map(str::to_string), env: BTreeMap::new() };
    Native::Object { value: serde_json::to_value(n).unwrap() }
}

pub fn document(script: &str) -> Native {
    Native::Document { media_type: "text/x-shellscript".into(), bytes: script.as_bytes().to_vec() }
}
