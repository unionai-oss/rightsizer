//! `rightsizer-k8s`: the Kubernetes adapter (kubernetes.md).
//!
//! Pure modules (always built; no `kube`, `k8s-openapi` or `axum`):
//!
//! - [`adapter`]: [`K8sAdapter`], the `Adapter` + `PlatformModel` for kind `"k8s"`.
//! - [`objects`]: native submissions (YAML/JSON documents, admission requests) and
//!   the workload model (pod templates per kind).
//! - [`extract`]: units, groups, keys, skip rules, priors, context seeds.
//! - [`render`]: S2 targets, JSON patches and minimal YAML text edits.
//! - [`validate`]: LimitRange, ResourceQuota (with the [`ledger`]), Kueue,
//!   pod-spec invariants, QoS, GKE [`autopilot`], node shapes.
//! - [`prior`]: the HMAC-signed `rightsizer.io/prior` annotation.
//! - [`restore`], [`ladder`] (`rsz apply --watch` step function), [`mode`].
//! - [`yaml`], [`edit`]: the YAML span locator and the edit renderers.
//!
//! Features: [`webhook`] (axum + rustls), [`cluster`] (kube-rs watcher,
//! restore I/O, Prometheus importer).

#![forbid(unsafe_code)]

pub mod edit;
pub mod yaml;

pub mod adapter;
pub mod autopilot;
pub mod extract;
pub mod gpu;
pub mod keys;
pub mod ladder;
pub mod ledger;
pub mod limitrange;
pub mod mode;
pub mod objects;
pub mod prior;
pub mod render;
pub mod resources;
pub mod restore;
pub mod settings;
pub mod validate;

#[cfg(feature = "cluster")]
pub mod cluster;
#[cfg(feature = "webhook")]
pub mod webhook;

pub use adapter::{Finalized, K8sAdapter};
pub use ledger::{MemoryLedger, Reservation, ReservationLedger};
pub use prior::SigningKeys;
pub use render::{Output, RenderRequest, Rendered, UnitRender};
pub use settings::{FlyteSettings, K8sSettings};
