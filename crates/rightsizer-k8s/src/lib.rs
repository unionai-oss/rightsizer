//! `rightsizer-k8s`: the Kubernetes adapter (kubernetes.md).
//!
//! - [`adapter`] (always built, pure): manifest extract / render / validate.
//! - [`webhook`] (feature `webhook`): the mutating admission webhook (axum + rustls).
//! - [`cluster`] (feature `cluster`): kube-rs watcher, restore, Prometheus importer.
//!
//! Built with `--no-default-features` this crate has no `kube` dependency
//! (`cargo xtask deps-check` enforces it).
//!
//! Status: skeleton; the adapter returns `Unsupported` until implemented.

#![forbid(unsafe_code)]

pub mod adapter;
#[cfg(feature = "cluster")]
pub mod cluster;
#[cfg(feature = "webhook")]
pub mod webhook;

pub use adapter::K8sAdapter;
