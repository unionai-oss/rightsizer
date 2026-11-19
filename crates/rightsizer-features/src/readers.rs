//! Input metadata readers (SPEC §9): Parquet footer (range read), NPY header,
//! safetensors header (shapes → parameter count), CSV first line, JSONL first
//! record, directory listings (count + total bytes, ≤ 10,000 entries); M3:
//! Arrow IPC schema, Hugging Face model refs.
//!
//! Readers read bounded byte ranges only (≤ 1 MiB per input); a larger
//! footer is skipped and `columns = None` is recorded (SPEC §5.2,
//! `test_context_cannot_see_the_answer`).
//!
//! **Stub:** [`read_inputs`] returns what the refs already know.

use std::sync::Arc;

use object_store::ObjectStore;
use rightsizer_core::{InputMeta, InputRef};

use crate::ReadBudget;

/// Where inputs are read from: one object store per URI scheme/bucket,
/// resolved by the engine.
pub trait StoreResolver: Send + Sync {
    /// The store and in-store path for a URI, or `None` if not allowed.
    fn resolve(&self, uri: &str) -> Option<(Arc<dyn ObjectStore>, object_store::path::Path)>;
}

/// Read metadata for every input within `budget`. Deterministic: the same
/// refs always yield the same metadata (SPEC §5.2). Errors leave an input
/// with what its ref already knew.
pub async fn read_inputs(refs: &[InputRef], resolver: &dyn StoreResolver, budget: ReadBudget) -> Vec<InputMeta> {
    let _ = (resolver, budget);
    crate::known_inputs(refs)
}
