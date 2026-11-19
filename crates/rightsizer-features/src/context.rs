//! Context assembly (SPEC §5.2): a [`ContextSeed`] plus what the engine
//! knows becomes a [`WorkloadContext`].

use std::collections::BTreeMap;
use std::time::Duration;

use rightsizer_core::{CodeInfo, CodeRef, ContextSeed, InputMeta, PlatformRef, Prior, RunRecord, UnitId, WorkloadContext, WorkloadKey};

use crate::code::{Language, MAX_SOURCE_BYTES, code_info, truncate_utf8};
use crate::privacy::Privacy;
use crate::readers::{ReadOptions, read_inputs_truncated};
use crate::store::StoreResolver;
use crate::{ReadBudget, tokens};

/// Largest code file [`CodeRef::Path`] reads (4 MiB).
pub const MAX_CODE_FILE_BYTES: u64 = 4 << 20;

/// Build the context for one unit. See the crate docs for the rules;
/// [`build_context_with`] takes explicit [`ReadOptions`] (SSRF allowlist,
/// concurrency, cache) instead of `budget`.
#[allow(clippy::too_many_arguments)]
pub async fn build_context(
    seed: &ContextSeed,
    key: WorkloadKey,
    unit: UnitId,
    prior: Prior,
    platform: PlatformRef,
    history: Vec<RunRecord>,
    labels: &BTreeMap<String, String>,
    privacy: &Privacy,
    budget: ReadBudget,
    resolver: &dyn StoreResolver,
) -> WorkloadContext {
    let opts = ReadOptions::new(budget).with_salt(privacy.salt.clone());
    build_context_with(seed, key, unit, prior, platform, history, labels, privacy, &opts, resolver).await
}

/// [`build_context`] with explicit read options (`opts.salt` is replaced
/// by `privacy.salt` when that is set).
#[allow(clippy::too_many_arguments)]
pub async fn build_context_with(
    seed: &ContextSeed,
    key: WorkloadKey,
    unit: UnitId,
    prior: Prior,
    platform: PlatformRef,
    mut history: Vec<RunRecord>,
    labels: &BTreeMap<String, String>,
    privacy: &Privacy,
    opts: &ReadOptions,
    resolver: &dyn StoreResolver,
) -> WorkloadContext {
    let mut opts = opts.clone();
    if privacy.salt.is_some() {
        opts.salt = privacy.salt.clone();
    }
    let started = tokio::time::Instant::now();

    let (code, text) = match &seed.code {
        None => (None, None),
        Some(c) => match resolve_code(c, privacy.send_source, opts.budget.per_context).await {
            Some((info, text)) => (Some(info), text),
            None => (None, None),
        },
    };

    let mut manifest = seed.manifest.clone();
    if let Some(src) = text {
        let found = tokens::distributed_markers(&src);
        if !found.is_empty() {
            manifest.distributed = true;
            manifest.distributed_markers.extend(found);
            manifest.distributed_markers.sort();
            manifest.distributed_markers.dedup();
        }
    }

    // The code read shares the context's time budget.
    opts.budget.per_context = opts.budget.per_context.saturating_sub(started.elapsed()).max(Duration::from_millis(1));
    let inputs = read_inputs_truncated(&seed.inputs, resolver, &opts, WorkloadContext::MAX_INPUTS).await;

    history.retain(|r| r.key == key);
    history.sort_by(|a, b| {
        b.started_at
            .cmp(&a.started_at)
            .then_with(|| b.invocation_id.cmp(&a.invocation_id))
            .then_with(|| b.attempt.cmp(&a.attempt))
    });

    let mut all_labels = seed.labels.clone();
    all_labels.extend(labels.iter().map(|(k, v)| (k.clone(), v.clone())));

    let mut ctx = WorkloadContext {
        v: 1,
        key,
        unit,
        platform,
        code,
        inputs,
        manifest,
        prior,
        history,
        labels: privacy.allow_labels(&all_labels),
    };
    canonical_inputs(&mut ctx.inputs);
    ctx.canonicalize(privacy.history_limit);
    ctx
}

/// SPEC §5.2 truncation with a total order
/// ([`rightsizer_core::canonical_input_order`]: largest first, unknown
/// sizes as 0, then by name, then known before unknown sizes, then by the
/// canonical JSON of the whole entry); at most 64 kept. The same order
/// `WorkloadContext::canonicalize` applies.
pub fn canonical_inputs(inputs: &mut Vec<InputMeta>) {
    rightsizer_core::canonical_input_order(inputs);
    inputs.truncate(WorkloadContext::MAX_INPUTS);
}

/// The code info and the full source text (for marker scanning).
async fn resolve_code(c: &CodeRef, keep_source: bool, budget: Duration) -> Option<(CodeInfo, Option<String>)> {
    match c {
        CodeRef::Source { text, language } => {
            let lang = language.as_deref().and_then(Language::from_hint);
            Some((code_info(text, lang, keep_source), Some(text.clone())))
        }
        CodeRef::Path { path } => {
            let read = async {
                let meta = tokio::fs::metadata(path).await.ok()?;
                if meta.len() > MAX_CODE_FILE_BYTES {
                    return None;
                }
                tokio::fs::read_to_string(path).await.ok()
            };
            let text = tokio::time::timeout(budget, read).await.ok().flatten()?;
            Some((code_info(&text, Language::from_path(path), keep_source), Some(text)))
        }
        CodeRef::Info { info } => {
            let mut info = info.clone();
            let text = info.source.clone();
            info.tokens.sort();
            info.tokens.dedup();
            info.source = if keep_source { info.source.map(|s| truncate_utf8(&s, MAX_SOURCE_BYTES).to_string()) } else { None };
            Some((info, text))
        }
    }
}
