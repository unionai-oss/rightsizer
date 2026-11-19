//! `rightsizer-slurm`: the Slurm adapter (slurm.md).
//!
//! - [`sbatch`]: `sbatch` submissions — `#SBATCH` directives (leading block
//!   only), argv (short/long forms; argv overrides the script),
//!   `SBATCH_*` variables, `#RIGHTSIZER` directives, het components,
//!   arrays, `--wrap`; the resources they ask for ([`sbatch::CompResources`]).
//! - [`keys`]: tenants `slurm://{cluster}/{account}` and key suffixes
//!   (`#RIGHTSIZER key=`, `--job-name`, normalized script digest).
//! - [`adapter`]: [`SlurmAdapter`] — extract / render (`#SBATCH` text edits,
//!   argv / cli_filter option tables) / validate.
//! - [`platform`]: [`SlurmPlatform`] — `PlatformRef` and live state.
//! - [`inventory`]: `scontrol` / `sinfo` / `sacctmgr` parsers.
//! - [`token`]: HMAC attribution tokens and signing keys; [`attest`]: what
//!   `job_desc` will carry after a patch, and its tokens.
//! - [`sacct`]: the `sacct` importer ([`sacct::SacctSource`], an `OutcomeSource`).
//! - [`wrapper`]: `rsz slurm sbatch [--watch]`; [`client`]: `rsz slurm
//!   decide-client`, the helper cli_filter.lua runs.
//! - [`service`]: the daemon side (`rsz serve`): identity → account →
//!   tenant authorization, decide, signing, inventory refresh.
//!
//! Lua plugins live in `contrib/slurm/` (cli_filter.lua, job_submit.lua).

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod adapter;
pub mod attest;
pub mod client;
pub mod install;
pub mod inventory;
pub mod keys;
pub mod options;
pub mod platform;
pub mod runner;
pub mod sacct;
pub mod sbatch;
pub mod service;
pub mod settings;
pub mod token;
pub mod wrapper;

pub use adapter::{SlurmAdapter, SlurmNative};
pub use platform::SlurmPlatform;
pub use sacct::SacctSource;
pub use settings::SlurmSettings;
