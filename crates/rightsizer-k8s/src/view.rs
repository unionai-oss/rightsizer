//! The cluster inputs the webhook and the watcher read (kubernetes.md §3.4,
//! §5): namespace labels, LimitRanges, ResourceQuotas, Nodes, Kueue objects,
//! RuntimeClasses. [`ClusterView`] is the interface; [`StaticView`] serves
//! fixed objects (tests, offline `rsz recommend -p k8s`); the kube-backed
//! cached view lives in [`crate::cluster`] (feature `cluster`).

use std::collections::BTreeMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Namespace label that opts a namespace in.
pub const ENABLED_LABEL: &str = "rightsizer.io/enabled";

/// Everything the adapter needs about one namespace and the cluster.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ClusterInputs {
    /// Whether the namespace exists (labels known).
    pub namespace_found: bool,
    /// Labels of the namespace.
    pub namespace_labels: BTreeMap<String, String>,
    /// LimitRange objects of the namespace.
    pub limit_ranges: Vec<Value>,
    /// ResourceQuota objects of the namespace (with `status.used`).
    pub resource_quotas: Vec<Value>,
    /// Kueue LocalQueues of the namespace.
    pub local_queues: Vec<Value>,
    /// Node objects.
    pub nodes: Vec<Value>,
    /// Kueue ClusterQueues.
    pub cluster_queues: Vec<Value>,
    /// Kueue ResourceFlavors.
    pub resource_flavors: Vec<Value>,
    /// RuntimeClasses.
    pub runtime_classes: Vec<Value>,
}

impl ClusterInputs {
    /// Whether the namespace carries `rightsizer.io/enabled=true`.
    pub fn enabled(&self) -> bool {
        self.namespace_labels.get(ENABLED_LABEL).is_some_and(|v| v == "true")
    }

    /// Merge into adapter settings (`AdapterEnv.settings` keys of [`crate::K8sSettings`]).
    pub fn apply_to(&self, settings: &mut Map<String, Value>) {
        let put = |s: &mut Map<String, Value>, k: &str, v: &[Value]| {
            s.insert(k.into(), Value::Array(v.to_vec()));
        };
        settings.insert("namespace_labels".into(), serde_json::to_value(&self.namespace_labels).unwrap_or_default());
        put(settings, "limit_ranges", &self.limit_ranges);
        put(settings, "resource_quotas", &self.resource_quotas);
        put(settings, "local_queues", &self.local_queues);
        put(settings, "nodes", &self.nodes);
        put(settings, "cluster_queues", &self.cluster_queues);
        put(settings, "resource_flavors", &self.resource_flavors);
        put(settings, "runtime_classes", &self.runtime_classes);
    }
}

/// Read access to the cluster inputs, cached by the implementation. Must be
/// cheap on the admission path (cached; a miss is bounded by the caller's
/// deadline).
#[async_trait]
pub trait ClusterView: Send + Sync {
    /// Inputs for `namespace`. An error means "unknown": the webhook then
    /// admits without a patch (S1).
    async fn inputs(&self, namespace: &str) -> Result<ClusterInputs, String>;
}

/// Fixed inputs, the same for every namespace except the labels.
#[derive(Clone, Debug, Default)]
pub struct StaticView {
    /// Cluster-wide objects and defaults.
    pub base: ClusterInputs,
    /// Labels per namespace (missing = not found).
    pub namespaces: BTreeMap<String, BTreeMap<String, String>>,
}

#[async_trait]
impl ClusterView for StaticView {
    async fn inputs(&self, namespace: &str) -> Result<ClusterInputs, String> {
        let mut i = self.base.clone();
        match self.namespaces.get(namespace) {
            Some(l) => {
                i.namespace_found = true;
                i.namespace_labels = l.clone();
            }
            None => i.namespace_found = !self.base.namespace_labels.is_empty(),
        }
        Ok(i)
    }
}
