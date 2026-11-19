//! `rightsizer-features`: what may be known about a run before it runs
//! (SPEC §5.2, §9): input metadata readers with bounded range reads, code
//! tokens and digests, the memo feature bucket (SPEC §6.3), privacy
//! (SPEC §11) and context assembly.
//!
//! I/O goes through `object_store`: local files always; features `s3`,
//! `gcs`, `azure`, `http` (HTTP(S) inputs and `hf://` model refs).
//!
//! # Context assembly
//!
//! [`build_context`]`(seed, key, unit, prior, platform, history, labels,
//! privacy, budget, resolver)` is what the engine calls to turn an adapter's
//! [`ContextSeed`](rightsizer_core::ContextSeed) into a
//! [`WorkloadContext`](rightsizer_core::WorkloadContext). It never fails:
//!
//! 1. **Code.** `CodeRef::Source` → [`code_info`] (digest, `features-v1`
//!    tokens); `CodeRef::Path` → the file (≤ 4 MiB, within the time budget)
//!    with the language from its extension; `CodeRef::Info` → as given,
//!    tokens sorted. `CodeInfo.source` is kept (truncated to 64 KiB) only
//!    when `privacy.send_source`.
//! 2. **Manifest.** The seed's, plus any distribution markers
//!    ([`distributed_markers`]) found in the source (`distributed = true`).
//! 3. **Inputs.** Read within `budget` ([`readers`]). With more than 64
//!    refs, sizes are read first and only the 64 largest (then by name) get
//!    a full read, so truncation is deterministic. URIs become salted
//!    hashes ([`uri_hash`], salt `privacy.salt`).
//! 4. **History.** Records of the same key only, newest first (`started_at`,
//!    then `invocation_id`, then `attempt`, all descending), at most
//!    `privacy.history_limit`.
//! 5. **Labels.** The seed's labels overlaid by `labels`, filtered by the
//!    `privacy.labels` allowlist (`"*"` = all; empty = none).
//! 6. **Canonical order.** [`canonical_inputs`]: inputs largest first
//!    (unknown sizes as 0), then by name, then by content (a total order, so
//!    the result never depends on ref order), ≤ 64; then
//!    `WorkloadContext::canonicalize` (which leaves that order unchanged).
//!
//! In-process estimators get this context; remote ones get
//! [`redact_for_remote`] of it.
//!
//! # Determinism
//!
//! The same seed and store contents always yield the same context and
//! digest, except when the per-context time budget expires mid-read (then
//! the unread fields are `None`; the context is still valid).

#![forbid(unsafe_code)]

use std::time::Duration;

use rightsizer_core::grid::{Grid, GridAxis, covering};
use rightsizer_core::{InputMeta, InputRef};

pub mod code;
mod context;
pub mod privacy;
pub mod readers;
pub mod ssrf;
pub mod store;
pub mod tokens;

pub use code::{Language, code_digest, code_digest_with, code_info};
pub use context::{MAX_CODE_FILE_BYTES, build_context, build_context_with, canonical_inputs};
pub use privacy::{Privacy, redact_for_remote, uri_hash};
pub use readers::{ReadOptions, read_inputs, read_inputs_with};
pub use ssrf::SsrfGuard;
pub use store::{DefaultResolver, StoreResolver};
pub use tokens::{code_tokens, distributed_markers};

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

/// The memo feature bucket (SPEC §6.3): each input's bytes on the memory
/// ladder and numeric scalars as given, so a fan-out over similar inputs
/// shares one estimate.
///
/// Format: `name:cell[=scalar]` per input, sorted, joined by `,`. `cell` is
/// the index of the smallest memory cell of `grid` covering the input's
/// bytes (`-` when unknown; `N+` above the top cell). Booleans are `0`/`1`;
/// string scalars are left out.
pub fn feature_bucket(inputs: &[InputMeta], grid: &Grid) -> String {
    let ladder = grid.values(GridAxis::Memory);
    let mut parts: Vec<String> = inputs
        .iter()
        .map(|i| {
            let size = i
                .bytes
                .map(|b| {
                    let (cell, over) = covering(&ladder, b as f64);
                    if over { format!("{cell}+") } else { cell.to_string() }
                })
                .unwrap_or_else(|| "-".into());
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
        let huge = vec![InputMeta::blob("x", 1 << 50)];
        assert!(feature_bucket(&huge, &g).ends_with('+'));
        let mut s = InputMeta::blob("n", 0);
        s.bytes = None;
        s.scalar = Some(rightsizer_core::ScalarValue::Int(3));
        assert_eq!(feature_bucket(&[s], &g), "n:-=3");
    }
}
