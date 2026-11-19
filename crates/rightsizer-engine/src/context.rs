//! Context building from a seed (plugins.md §2, SPEC §5.2, §9, §11).
//!
//! Interim: input metadata is what the refs already know (no reads), code
//! info comes from `rightsizer_features::code_info` (digest, `features-v1`
//! tokens, source only if privacy allows; `Path` refs are read locally),
//! labels are allowlisted, history is the store's.
//!
//! TODO(rightsizer-features): call `rightsizer_features::build_context`
//! (bounded reads within `features.budget_ms`, salted URI hashes). It is
//! wired in `Engine::context_of` but not used yet: its future is not
//! provably `Send` for callers that `tokio::spawn` a decision (rustc's
//! higher-ranked inference on `readers::read_pass`'s
//! `which.iter().filter_map(|&i| Some(async move { .. }))` fed to
//! `buffer_unordered`). Collecting the futures into a `Vec` first fixes it.

use rightsizer_core::{CodeInfo, CodeRef, ContextSeed, PlatformRef, Prior, RunRecord, UnitId, WorkloadContext, WorkloadKey};
use rightsizer_features::Privacy;

/// Code info from a code ref; an unreadable path yields `None` (S1).
pub fn code_info(code: &CodeRef, privacy: &Privacy) -> Option<CodeInfo> {
    match code {
        CodeRef::Info { info } => Some(info.clone()),
        CodeRef::Source { text, language } => {
            Some(rightsizer_features::code_info(text, language.as_deref().and_then(rightsizer_features::Language::from_hint), privacy.send_source))
        }
        CodeRef::Path { path } => {
            let text = std::fs::read_to_string(path).ok()?;
            Some(rightsizer_features::code_info(&text, rightsizer_features::Language::from_path(path), privacy.send_source))
        }
    }
}

/// Build a context from a seed and fetched history.
#[allow(clippy::too_many_arguments)]
pub fn complete_seed(
    key: &WorkloadKey,
    unit: &UnitId,
    seed: &ContextSeed,
    prior: &Prior,
    platform: &PlatformRef,
    history: Vec<RunRecord>,
    privacy: &Privacy,
    history_limit: usize,
) -> WorkloadContext {
    let mut manifest = seed.manifest.clone();
    let code = seed.code.as_ref().and_then(|c| code_info(c, privacy));
    if let Some(CodeRef::Source { text, .. }) = &seed.code {
        for m in rightsizer_features::distributed_markers(text) {
            if !manifest.distributed_markers.contains(&m) {
                manifest.distributed_markers.push(m);
            }
        }
    }
    manifest.distributed |= !manifest.distributed_markers.is_empty();
    let mut inputs = rightsizer_features::known_inputs(&seed.inputs);
    for (m, r) in inputs.iter_mut().zip(&seed.inputs) {
        m.uri_hash = r.uri.as_deref().map(|u| privacy.hash_uri(u));
    }
    let mut ctx = WorkloadContext {
        v: rightsizer_core::WIRE_VERSION,
        key: key.clone(),
        unit: unit.clone(),
        platform: platform.clone(),
        code,
        inputs,
        manifest,
        prior: prior.clone(),
        history,
        labels: privacy.allow_labels(&seed.labels),
    };
    ctx.canonicalize(history_limit);
    ctx
}
