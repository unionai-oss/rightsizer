//! The pure K8s adapter (kubernetes.md §2–§4): units per container / replica
//! type, priors and defaults (LimitRange), QoS, GPU node selectors, JSON
//! patches for admission and minimal text edits for `rsz apply`.

use async_trait::async_trait;
use rightsizer_core::{ResourceSpec, UnitId};
use rightsizer_plugin::{Adapter, AdapterEnv, Extracted, Native, Patch, PluginError, Rejection, Result};

/// The Kubernetes adapter.
#[derive(Clone, Debug, Default)]
pub struct K8sAdapter;

#[async_trait]
impl Adapter for K8sAdapter {
    fn kind(&self) -> &str {
        "k8s"
    }
    async fn extract(&self, _native: &Native, _env: &AdapterEnv) -> Result<Extracted> {
        Err(PluginError::Unsupported("k8s extract: not implemented yet".into()))
    }
    async fn render(&self, _native: &Native, _decisions: &[(UnitId, ResourceSpec)]) -> Result<Patch> {
        Err(PluginError::Unsupported("k8s render: not implemented yet".into()))
    }
    async fn validate(&self, _native: &Native, _patch: &Patch, _env: &AdapterEnv) -> Result<(), Rejection> {
        Err(Rejection { reason: "unimplemented".into(), detail: None, field: None })
    }
}
