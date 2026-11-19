//! What the adapter reads from [`AdapterEnv::settings`]: the `[k8s]` config
//! section plus the cluster inputs a surface supplies (LimitRanges,
//! ResourceQuotas, Nodes, Kueue objects, namespace labels). The adapter does
//! no cluster I/O itself; the webhook and CLI (feature `cluster`) fetch these.
//!
//! Unknown keys are ignored, so the engine can pass its whole `[k8s]` table.

use std::collections::BTreeMap;

use rightsizer_core::{NodeShape, OverheadModel};
use rightsizer_plugin::{AdapterEnv, PluginError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::autopilot::AutopilotSettings;

/// `[k8s.flyte]`: keying Flyte task pods (kubernetes.md §2, flyte.md §12).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct FlyteSettings {
    /// Whether Flyte pods are keyed `flyte://…` at all.
    pub enabled: bool,
    /// The namespace Flyte runs `{project}-{domain}` in; `{org}` is also available.
    pub namespace_template: String,
    /// Pod label carrying the org.
    pub org_label: String,
    /// Pod label carrying the project.
    pub project_label: String,
    /// Pod label carrying the domain.
    pub domain_label: String,
    /// Pod label carrying the task name.
    pub task_label: String,
    /// Org used when the pod has no org label (`None`: such pods are not Flyte-keyed).
    pub default_org: Option<String>,
}

impl Default for FlyteSettings {
    fn default() -> Self {
        FlyteSettings {
            enabled: true,
            namespace_template: "{project}-{domain}".into(),
            org_label: "organization".into(),
            project_label: "project".into(),
            domain_label: "domain".into(),
            task_label: "task-name".into(),
            default_org: None,
        }
    }
}

/// The adapter's settings and cluster inputs.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct K8sSettings {
    /// Cluster name (tenant prefix; required).
    pub cluster: Option<String>,
    /// Flyte keying.
    pub flyte: FlyteSettings,
    /// Extra native accelerator names → canonical ids (`"NVIDIA-H100-NVL" = "H100-80G"`).
    pub accelerators: BTreeMap<String, String>,
    /// Node label used when rendering a GPU type change.
    pub gpu_product_label: String,
    /// GPU type changes (opt-in, off; never for queue-managed jobs).
    pub allow_gpu_type_change: bool,
    /// `rsz apply --watch`: a caller ladder exists.
    pub apply_watch: bool,
    /// CPU measurement window of the configured outcome source, seconds.
    pub cpu_window_s: u32,
    /// Per-image runtime overhead (longest image prefix wins).
    pub overhead: BTreeMap<String, OverheadModel>,
    /// Configured node shapes.
    pub shapes: Vec<NodeShape>,
    /// Node prices by instance type, $/h.
    pub prices: BTreeMap<String, f64>,
    /// GKE Autopilot, when detected.
    pub autopilot: Option<AutopilotSettings>,
    /// Owner kinds whose pods are never sized as bare pods.
    pub pod_skip_owner_kinds: Vec<String>,
    /// Labels of the request's namespace (mode, enablement).
    pub namespace_labels: BTreeMap<String, String>,
    /// LimitRange objects of the namespace.
    pub limit_ranges: Vec<Value>,
    /// ResourceQuota objects of the namespace (with `status.used`).
    pub resource_quotas: Vec<Value>,
    /// Node objects (allocatable, labels).
    pub nodes: Vec<Value>,
    /// Kueue ClusterQueues.
    pub cluster_queues: Vec<Value>,
    /// Kueue LocalQueues of the namespace.
    pub local_queues: Vec<Value>,
    /// Kueue ResourceFlavors.
    pub resource_flavors: Vec<Value>,
    /// RuntimeClass objects (for `overhead.podFixed`).
    pub runtime_classes: Vec<Value>,
    /// Admission dry run.
    pub dry_run: bool,
}

/// Owner kinds whose pods are never bare pods (kubernetes.md §3.1, plus the
/// long-running controllers; their pods are recreated from templates).
pub const DEFAULT_SKIP_OWNERS: [&str; 13] = [
    "Job", "CronJob", "JobSet", "PyTorchJob", "TrainJob", "RayCluster", "RayJob", "Workload", "PodGroup",
    "ReplicaSet", "StatefulSet", "DaemonSet", "ReplicationController",
];

impl Default for K8sSettings {
    fn default() -> Self {
        K8sSettings {
            cluster: None,
            flyte: FlyteSettings::default(),
            accelerators: BTreeMap::new(),
            gpu_product_label: "nvidia.com/gpu.product".into(),
            allow_gpu_type_change: false,
            apply_watch: false,
            cpu_window_s: 15,
            overhead: BTreeMap::new(),
            shapes: Vec::new(),
            prices: BTreeMap::new(),
            autopilot: None,
            pod_skip_owner_kinds: DEFAULT_SKIP_OWNERS.iter().map(|s| s.to_string()).collect(),
            namespace_labels: BTreeMap::new(),
            limit_ranges: Vec::new(),
            resource_quotas: Vec::new(),
            nodes: Vec::new(),
            cluster_queues: Vec::new(),
            local_queues: Vec::new(),
            resource_flavors: Vec::new(),
            runtime_classes: Vec::new(),
            dry_run: false,
        }
    }
}

impl K8sSettings {
    /// Read from an [`AdapterEnv`]: `settings` keys, with `env.cluster`
    /// taking precedence over `settings.cluster`.
    pub fn from_env(env: &AdapterEnv) -> Result<K8sSettings> {
        let mut s: K8sSettings =
            serde_json::from_value(Value::Object(env.settings.clone())).map_err(|e| PluginError::InvalidNative(format!("[k8s] settings: {e}")))?;
        if let Some(c) = &env.cluster {
            s.cluster = Some(c.clone());
        }
        Ok(s)
    }

    /// As an [`AdapterEnv`] (for tests and surfaces).
    pub fn to_env(&self, namespace: Option<&str>) -> AdapterEnv {
        let Value::Object(settings) = serde_json::to_value(self).expect("settings serialize") else { unreachable!() };
        AdapterEnv { cluster: self.cluster.clone(), principal: None, namespace: namespace.map(str::to_string), deadline_ms: None, settings }
    }

    /// The overhead model for an image (longest matching prefix).
    pub fn overhead_for(&self, image: &str) -> OverheadModel {
        self.overhead.iter().filter(|(p, _)| image.starts_with(p.as_str())).max_by_key(|(p, _)| p.len()).map(|(_, m)| m.clone()).unwrap_or_default()
    }
}
