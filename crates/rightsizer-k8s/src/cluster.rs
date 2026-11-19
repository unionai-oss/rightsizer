//! Cluster components (kubernetes.md §5–§6), feature `cluster`: the kube-rs
//! watcher (outcomes, OOM classification), `rsz k8s restore`, the Prometheus
//! importer with owner joins.
//!
//! Skeleton: client type aliases only.

/// The Kubernetes client.
pub type Client = kube::Client;

/// Pods.
pub type Pod = k8s_openapi::api::core::v1::Pod;
