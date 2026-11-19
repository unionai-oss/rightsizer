//! `rightsizer-features`: what may be known about a run before it runs
//! (SPEC §5.2, §9): input metadata readers with bounded range reads, code
//! tokens and digests, and the memo feature bucket (SPEC §6.3).
//!
//! I/O goes through `object_store` (features `s3`, `gcs`, `azure`, `http`).
//!
//! Status: [`code_digest`] and [`feature_bucket`] are implemented; the
//! readers ([`readers`]) and [`tokens`] are contracts with stub bodies.

#![forbid(unsafe_code)]

use std::time::Duration;

use rightsizer_core::grid::{Grid, GridAxis, covering};
use rightsizer_core::{InputMeta, InputRef};
use sha2::{Digest, Sha256};

pub mod readers;
pub mod tokens;

/// Feature schema version recorded in provenance (SPEC §9).
pub const FEATURES_VERSION: &str = "features-v1";

/// Errors reading input metadata. Readers never fail a decision: an error
/// leaves the input with what is already known (S1).
#[derive(Debug, thiserror::Error)]
pub enum FeatureError {
    /// The per-context time budget expired.
    #[error("feature budget expired")]
    Budget,
    /// The URI is not allowed (private address, unknown scheme, …).
    #[error("input not allowed: {0}")]
    NotAllowed(String),
    /// A footer or header larger than the per-input byte budget (SPEC §5.2).
    #[error("header or footer exceeds the {0}-byte budget")]
    TooLarge(u64),
    /// The object store failed.
    #[error("object store: {0}")]
    Store(#[from] object_store::Error),
    /// Malformed metadata.
    #[error("malformed {format}: {message}")]
    Malformed {
        /// Format.
        format: &'static str,
        /// Message.
        message: String,
    },
}

/// Read budgets (SPEC §5.2 "no peeking", §9 I/O).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ReadBudget {
    /// Bytes per input (1 MiB).
    pub per_input_bytes: u64,
    /// Time per context (1 s by default; integrations pass tighter ones).
    pub per_context: Duration,
    /// Directory entries listed (10,000).
    pub max_dir_entries: usize,
}

impl Default for ReadBudget {
    fn default() -> Self {
        ReadBudget { per_input_bytes: 1 << 20, per_context: Duration::from_secs(1), max_dir_entries: 10_000 }
    }
}

/// Normalized code digest (SPEC §5.2): comments (`#`, `//`), blank lines,
/// surrounding whitespace and resource directive lines (`#SBATCH`,
/// `#RIGHTSIZER`, `@resources`-style decorators are the adapter's job) are
/// stripped before hashing. `sha256:` + hex.
pub fn code_digest(source: &str) -> String {
    let mut h = Sha256::new();
    for line in source.lines() {
        let t = line.trim();
        if t.is_empty() || t.starts_with('#') || t.starts_with("//") {
            continue;
        }
        let t = match t.find(" #") {
            Some(i) => t[..i].trim_end(),
            None => t,
        };
        h.update(t.as_bytes());
        h.update(b"\n");
    }
    format!("sha256:{}", hex::encode(h.finalize()))
}

/// The memo feature bucket (SPEC §6.3): each input's bytes on the memory
/// ladder and numeric scalars as given, so a fan-out over similar inputs
/// shares one estimate.
pub fn feature_bucket(inputs: &[InputMeta], grid: &Grid) -> String {
    let ladder = grid.values(GridAxis::Memory);
    let mut parts: Vec<String> = inputs
        .iter()
        .map(|i| {
            let size = i.bytes.map(|b| covering(&ladder, b as f64).0.to_string()).unwrap_or_else(|| "-".into());
            let scalar = i.scalar.as_ref().and_then(|s| s.as_f64()).map(|v| format!("={v}")).unwrap_or_default();
            format!("{}:{size}{scalar}", i.name)
        })
        .collect();
    parts.sort();
    parts.join(",")
}

/// Build `InputMeta` for refs whose size is already known, without I/O.
/// Readers ([`readers::read_inputs`]) fill the rest.
pub fn known_inputs(refs: &[InputRef]) -> Vec<InputMeta> {
    refs.iter()
        .map(|r| InputMeta {
            name: r.name.clone(),
            uri_hash: None,
            format: r.format.unwrap_or(if r.scalar.is_some() {
                rightsizer_core::InputFormat::Scalar
            } else {
                rightsizer_core::InputFormat::Blob
            }),
            bytes: r.bytes,
            rows: None,
            columns: None,
            shape: None,
            dtype: None,
            params: None,
            scalar: r.scalar.clone(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_ignores_comments_and_whitespace() {
        let a = "import pandas as pd\n# comment\n\ndf = pd.read_parquet(x)  # trailing\n";
        let b = "  import pandas as pd\ndf = pd.read_parquet(x)\n";
        assert_eq!(code_digest(a), code_digest(b));
        assert_ne!(code_digest(a), code_digest("import polars\n"));
    }

    #[test]
    fn bucket_groups_similar_sizes() {
        let g = Grid::rs_grid_v1();
        let a = vec![InputMeta::blob("x", 1_000_000_000)];
        let b = vec![InputMeta::blob("x", 1_050_000_000)];
        let c = vec![InputMeta::blob("x", 9_000_000_000)];
        assert_eq!(feature_bucket(&a, &g), feature_bucket(&b, &g));
        assert_ne!(feature_bucket(&a, &g), feature_bucket(&c, &g));
    }
}
