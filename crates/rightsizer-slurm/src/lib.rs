//! `rightsizer-slurm`: the Slurm adapter (slurm.md): `sbatch` script parsing,
//! `#SBATCH` minimal text edits, option tables for `cli_filter` /
//! `job_submit`, HMAC attribution tokens, the `sacct` importer and the
//! `sbatch --watch` wrapper (a Caller ladder).
//!
//! Status: skeleton; the adapter returns `Unsupported` until implemented.
//! Memory quantities use [`rightsizer_core::units::parse_slurm_mem`].

#![forbid(unsafe_code)]

use async_trait::async_trait;
use rightsizer_core::{ResourceSpec, UnitId};
use rightsizer_plugin::{Adapter, AdapterEnv, Extracted, Native, Patch, PluginError, Rejection, Result};

pub mod token;

/// The Slurm adapter.
#[derive(Clone, Debug, Default)]
pub struct SlurmAdapter;

#[async_trait]
impl Adapter for SlurmAdapter {
    fn kind(&self) -> &str {
        "slurm"
    }
    async fn extract(&self, _native: &Native, _env: &AdapterEnv) -> Result<Extracted> {
        Err(PluginError::Unsupported("slurm extract: not implemented yet".into()))
    }
    async fn render(&self, _native: &Native, _decisions: &[(UnitId, ResourceSpec)]) -> Result<Patch> {
        Err(PluginError::Unsupported("slurm render: not implemented yet".into()))
    }
    async fn validate(&self, _native: &Native, _patch: &Patch, _env: &AdapterEnv) -> Result<(), Rejection> {
        Err(Rejection { reason: "unimplemented".into(), detail: None, field: None })
    }
}
