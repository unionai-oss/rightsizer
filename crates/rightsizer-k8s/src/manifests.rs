//! `rsz webhook manifests` (kubernetes.md §3.2): the
//! `MutatingWebhookConfiguration` with `jobs.rightsizer.io` and
//! `pods.rightsizer.io`. Pure: returns JSON values; the CLI prints YAML.

use serde_json::{Value, json};

use crate::view::ENABLED_LABEL;

/// Webhook names.
pub const JOBS_WEBHOOK: &str = "jobs.rightsizer.io";
/// Webhook names.
pub const PODS_WEBHOOK: &str = "pods.rightsizer.io";
/// Paths served for each webhook.
pub const JOBS_PATH: &str = "/mutate/jobs";
/// Paths served for each webhook.
pub const PODS_PATH: &str = "/mutate/pods";

/// Where the apiserver reaches the webhook.
#[derive(Clone, Debug, PartialEq)]
pub enum Endpoint {
    /// `clientConfig.service` (in-cluster).
    Service {
        /// Namespace.
        namespace: String,
        /// Service name.
        name: String,
        /// Service port.
        port: u16,
    },
    /// `clientConfig.url` (a base URL; the path is appended).
    Url(String),
}

/// Options for [`mutating_webhook_configuration`].
#[derive(Clone, Debug)]
pub struct ManifestOptions {
    /// Object name.
    pub name: String,
    /// Endpoint.
    pub endpoint: Endpoint,
    /// PEM CA bundle (base64-encoded into `caBundle`); `None` leaves it for
    /// cert-manager's CA injector (`cert-manager.io/inject-ca-from`).
    pub ca_bundle_pem: Option<String>,
    /// cert-manager Certificate (`namespace/name`) to inject the CA from.
    pub inject_ca_from: Option<String>,
    /// Include the M3 kinds (TrainJob, RayCluster/RayJob, Volcano Job).
    pub m3_kinds: bool,
    /// `timeoutSeconds` (kubernetes.md §3.2: 2).
    pub timeout_seconds: u32,
}

impl Default for ManifestOptions {
    fn default() -> Self {
        ManifestOptions {
            name: "rightsizer".into(),
            endpoint: Endpoint::Service { namespace: "rightsizer".into(), name: "rightsizer-webhook".into(), port: 443 },
            ca_bundle_pem: None,
            inject_ca_from: None,
            m3_kinds: true,
            timeout_seconds: 2,
        }
    }
}

fn client_config(o: &ManifestOptions, path: &str) -> Value {
    let mut c = match &o.endpoint {
        Endpoint::Service { namespace, name, port } => json!({"service": {"namespace": namespace, "name": name, "port": port, "path": path}}),
        Endpoint::Url(u) => json!({"url": format!("{}{}", u.trim_end_matches('/'), path)}),
    };
    if let Some(pem) = &o.ca_bundle_pem {
        use base64::Engine as _;
        c["caBundle"] = Value::String(base64::engine::general_purpose::STANDARD.encode(pem.as_bytes()));
    }
    c
}

fn rule(group: &str, resources: &[&str]) -> Value {
    json!({
        "apiGroups": [group],
        "apiVersions": ["*"],
        "operations": ["CREATE"],
        "resources": resources,
        "scope": "Namespaced",
    })
}

fn webhook(o: &ManifestOptions, name: &str, path: &str, rules: Vec<Value>) -> Value {
    json!({
        "name": name,
        "clientConfig": client_config(o, path),
        "rules": rules,
        "namespaceSelector": {"matchLabels": {ENABLED_LABEL: "true"}},
        "objectSelector": {"matchExpressions": [{"key": crate::mode::MODE, "operator": "NotIn", "values": ["off"]}]},
        "failurePolicy": "Ignore",
        "matchPolicy": "Equivalent",
        "timeoutSeconds": o.timeout_seconds,
        "reinvocationPolicy": "IfNeeded",
        "sideEffects": "NoneOnDryRun",
        "admissionReviewVersions": ["v1"],
    })
}

/// The rules of `jobs.rightsizer.io`.
pub fn job_rules(m3: bool) -> Vec<Value> {
    let mut r = vec![
        rule("batch", &["jobs", "cronjobs"]),
        rule("jobset.x-k8s.io", &["jobsets"]),
        rule("kubeflow.org", &["pytorchjobs"]),
    ];
    if m3 {
        r.push(rule("trainer.kubeflow.org", &["trainjobs"]));
        r.push(rule("ray.io", &["rayclusters", "rayjobs"]));
        r.push(rule("batch.volcano.sh", &["jobs"]));
    }
    r
}

/// The `MutatingWebhookConfiguration`.
pub fn mutating_webhook_configuration(o: &ManifestOptions) -> Value {
    let mut meta = json!({"name": o.name, "labels": {"app.kubernetes.io/name": "rightsizer"}});
    if let Some(c) = &o.inject_ca_from {
        meta["annotations"] = json!({"cert-manager.io/inject-ca-from": c});
    }
    json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "MutatingWebhookConfiguration",
        "metadata": meta,
        "webhooks": [
            webhook(o, JOBS_WEBHOOK, JOBS_PATH, job_rules(o.m3_kinds)),
            webhook(o, PODS_WEBHOOK, PODS_PATH, vec![rule("", &["pods"])]),
        ],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_the_spec() {
        let v = mutating_webhook_configuration(&ManifestOptions::default());
        let hooks = v["webhooks"].as_array().unwrap();
        assert_eq!(hooks.len(), 2);
        for h in hooks {
            assert_eq!(h["failurePolicy"], "Ignore");
            assert_eq!(h["timeoutSeconds"], 2);
            assert_eq!(h["reinvocationPolicy"], "IfNeeded");
            assert_eq!(h["sideEffects"], "NoneOnDryRun");
            assert_eq!(h["admissionReviewVersions"], json!(["v1"]));
            assert_eq!(h["namespaceSelector"]["matchLabels"]["rightsizer.io/enabled"], "true");
            assert_eq!(h["objectSelector"]["matchExpressions"][0]["values"], json!(["off"]));
        }
        assert_eq!(hooks[0]["name"], JOBS_WEBHOOK);
        assert_eq!(hooks[1]["name"], PODS_WEBHOOK);
        assert_eq!(hooks[1]["rules"][0]["resources"], json!(["pods"]));
        let u = mutating_webhook_configuration(&ManifestOptions { endpoint: Endpoint::Url("https://host.k3d.internal:18443".into()), ca_bundle_pem: Some("PEM".into()), ..Default::default() });
        assert_eq!(u["webhooks"][0]["clientConfig"]["url"], "https://host.k3d.internal:18443/mutate/jobs");
        assert_eq!(u["webhooks"][0]["clientConfig"]["caBundle"], "UEVN");
    }
}
