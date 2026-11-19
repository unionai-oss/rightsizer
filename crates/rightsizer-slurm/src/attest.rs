//! Daemon-side attestation: what `job_desc` will carry after a patch, and
//! the tokens that attest it (slurm.md §4.1).
//!
//! For a heterogeneous submission the `DecideResult.token` string holds one
//! token per component, in component order, joined with `.` (base64url
//! never contains `.`); [`split_tokens`] reverses it.

use ulid::Ulid;

use rightsizer_plugin::{Native, Patch, Result};

use crate::adapter::{Parsed, apply_patch, parse_native};
use crate::sbatch::{Component, SbatchJob};
use crate::token::{JobDescResources, KeyRing, TokenClaims, TokenError, script_sha256};

/// The `job_desc` resource fields Slurm derives from a component's
/// effective options.
pub fn job_desc_resources(c: &Component) -> JobDescResources {
    let eff = c.effective();
    JobDescResources::from_options(|k| eff.get(k).map(String::as_str))
}

/// Sign every component of an already-patched submission.
pub fn sign_parsed(p: &Parsed, uid: u32, account: &str, decision_ids: &[Ulid], expiry: u64, ring: &KeyRing) -> std::result::Result<Vec<String>, TokenError> {
    sign_job(&p.job, uid, account, decision_ids, expiry, ring)
}

/// Sign every component of a job (one token per component).
pub fn sign_job(job: &SbatchJob, uid: u32, account: &str, decision_ids: &[Ulid], expiry: u64, ring: &KeyRing) -> std::result::Result<Vec<String>, TokenError> {
    let script = job.script.as_deref().unwrap_or("");
    let digest = script_sha256(script.as_bytes());
    job.components
        .iter()
        .enumerate()
        .map(|(k, c)| {
            let id = decision_ids.get(k).or(decision_ids.first()).copied().ok_or_else(|| TokenError::NoKey("no decision id".into()))?;
            ring.sign(&TokenClaims {
                uid,
                account: account.to_string(),
                script_sha256: digest.clone(),
                resources: job_desc_resources(c),
                decision_id: id,
                expiry,
            })
        })
        .collect()
}

/// Apply `patch` to `native` and sign the result: the `token` field of a
/// `decide` result.
pub fn sign_patched(native: &Native, patch: &Patch, uid: u32, account: &str, decision_ids: &[Ulid], expiry: u64, ring: &KeyRing) -> Result<String> {
    let after = parse_native(&apply_patch(native, patch)?)?;
    let toks = sign_parsed(&after, uid, account, decision_ids, expiry, ring).map_err(|e| rightsizer_plugin::PluginError::Internal(e.to_string()))?;
    Ok(toks.join("."))
}

/// Split a `decide` token string into per-component tokens.
pub fn split_tokens(s: &str) -> Vec<String> {
    s.split('.').filter(|t| !t.is_empty()).map(str::to_string).collect()
}
