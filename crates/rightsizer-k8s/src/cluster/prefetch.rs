//! The input prefetcher (kubernetes.md §4.3): never on the request path.
//! After admission it reads the metadata of annotated inputs whose URIs are
//! under the admin-configured prefixes of the pod's namespace
//! (`[k8s.inputs.allow]`, key = namespace or `"*"`), stores it in the
//! [`InputCache`] the webhook fills seeds from, and re-decides the
//! object's units (dry run) with the metadata so the memo is warm for the
//! next admission with the same feature bucket. Anything else stays
//! hash-only. HTTP(S) inputs pointing at private, loopback or link-local
//! ranges are refused by the features crate's SSRF guard.

use std::collections::BTreeMap;
use std::sync::Arc;

use rightsizer_engine::Engine;
use rightsizer_features::{DefaultResolver, ReadBudget, ReadOptions, SsrfGuard};

use crate::admit::{InputCache, PrefetchJob};

/// Whether `uri` may be read for `namespace`.
pub fn allowed(allow: &BTreeMap<String, Vec<String>>, namespace: &str, uri: &str) -> bool {
    [namespace, "*"].iter().filter_map(|k| allow.get(*k)).flatten().any(|p| !p.is_empty() && uri.starts_with(p.as_str()))
}

/// Run the prefetcher until the channel closes.
pub async fn run(mut rx: tokio::sync::mpsc::Receiver<PrefetchJob>, cache: Arc<InputCache>, engine: Engine, allow: BTreeMap<String, Vec<String>>, allow_hosts: Vec<String>) {
    let resolver = DefaultResolver::new(SsrfGuard::new(&allow_hosts));
    while let Some(job) = rx.recv().await {
        let refs: Vec<_> = job.refs.iter().filter(|r| r.uri.as_deref().is_some_and(|u| allowed(&allow, &job.namespace, u) && cache.get(u).is_none())).cloned().collect();
        if refs.is_empty() {
            continue;
        }
        let mut opts = ReadOptions::new(ReadBudget::default());
        opts.guard = Arc::new(SsrfGuard::new(&allow_hosts));
        let metas = rightsizer_features::read_inputs_with(&refs, &resolver, &opts).await;
        let mut filled = 0;
        for (r, m) in refs.iter().zip(metas) {
            if let Some(u) = &r.uri
                && m.bytes.is_some()
            {
                cache.put(u, m);
                filled += 1;
            }
        }
        tracing::debug!(namespace = %job.namespace, read = refs.len(), filled, "prefetched input metadata");
        if filled == 0 {
            continue;
        }
        // Warm the memo: the next admission fills the same sizes from the cache.
        for mut g in job.requests {
            for r in g.iter_mut() {
                if let rightsizer_engine::ContextInput::Seed { seed, .. } = &mut r.context {
                    cache.fill(&mut seed.inputs);
                }
                r.dry_run = true;
                r.budget_ms = Some(2000);
            }
            if g.len() == 1 {
                let _ = engine.decide(g.remove(0)).await;
            } else {
                let _ = engine.decide_group(g).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefixes() {
        let mut a = BTreeMap::new();
        a.insert("ml".to_string(), vec!["s3://ml-data/".to_string()]);
        a.insert("*".to_string(), vec!["s3://public/".to_string()]);
        assert!(allowed(&a, "ml", "s3://ml-data/train.parquet"));
        assert!(allowed(&a, "other", "s3://public/x"));
        assert!(!allowed(&a, "other", "s3://ml-data/train.parquet"));
        assert!(!allowed(&a, "ml", "s3://ml-database/x"));
    }
}
