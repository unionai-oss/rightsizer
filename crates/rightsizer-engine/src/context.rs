//! Context building from a seed (plugins.md §2, SPEC §5.2, §9, §11).
//!
//! The engine calls `rightsizer_features::build_context_with`
//! (`Engine::seed_context`): bounded input reads within
//! min(`features.budget_ms`, half the decision budget left), salted URI
//! hashes, code info, distribution markers, allowlisted labels. On the
//! admission path (`manifest.extra.inputs_hash_only = true`) inputs are
//! never read inline, only hashed.
//!
//! [`complete_seed`] is the synchronous, I/O-free variant (known sizes only),
//! kept for callers without a runtime.

use rightsizer_core::{CodeInfo, CodeRef, ContextSeed, ScalarValue, PlatformRef, Prior, RunRecord, UnitId, WorkloadContext, WorkloadKey};
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

/// Whether the seed asks for hash-only inputs (`manifest.extra.inputs_hash_only`:
/// `true`, `"true"` or `1`): no input URI is read inline.
pub fn inputs_hash_only(seed: &ContextSeed) -> bool {
    match seed.manifest.extra.get("inputs_hash_only") {
        Some(ScalarValue::Bool(b)) => *b,
        Some(ScalarValue::Str(s)) => s.eq_ignore_ascii_case("true"),
        Some(ScalarValue::Int(i)) => *i != 0,
        _ => false,
    }
}
