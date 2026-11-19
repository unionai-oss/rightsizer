//! Workload keys (kubernetes.md §2, SPEC §8.5): tenant `k8s://{cluster}/{namespace}`
//! from trusted data; suffixes from object names with generated suffixes
//! stripped; `rightsizer.io/workload-key` overrides only the suffix base;
//! Flyte task pods keyed `flyte://{org}/{project}/{domain}/{task}` only when
//! the namespace matches the configured template.

use std::collections::BTreeMap;

use rightsizer_core::{Tenant, WorkloadKey};

use crate::settings::FlyteSettings;

/// Annotation that sets the key suffix base.
pub const WORKLOAD_KEY: &str = "rightsizer.io/workload-key";

/// The K8s tenant `{cluster}/{namespace}`.
pub fn tenant(cluster: &str, namespace: &str) -> Option<Tenant> {
    Tenant::new(format!("{cluster}/{namespace}")).ok()
}

/// Characters of K8s generated name suffixes (`k8s.io/apimachinery/pkg/util/rand`).
const RAND_ALPHABET: &[u8] = b"bcdfghjklmnpqrstvwxz2456789";

fn is_rand_suffix(s: &str) -> bool {
    s.len() == 5 && s.bytes().all(|b| RAND_ALPHABET.contains(&b)) && s.bytes().any(|b| b.is_ascii_digit())
}

/// A CronJob-created Job name ends in `-<scheduled time in unix minutes>`.
fn is_cron_suffix(s: &str) -> bool {
    (8..=10).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_digit())
}

/// Strip generated suffixes from an object name:
/// - `generateName` → the prefix (trailing `-` removed);
/// - a 5-character random suffix (`-x7k2p`, K8s alphabet, at least one digit);
/// - then a CronJob schedule suffix (`-29012345`, unix minutes).
pub fn base_name(name: &str, generate_name: Option<&str>) -> String {
    if let Some(g) = generate_name.filter(|g| !g.is_empty())
        && (name.is_empty() || name.starts_with(g))
    {
        let base = g.trim_end_matches('-');
        if !base.is_empty() {
            return strip_cron(base).to_string();
        }
    }
    let mut n = name;
    if let Some((head, tail)) = n.rsplit_once('-')
        && !head.is_empty()
        && is_rand_suffix(tail)
    {
        n = head;
    }
    strip_cron(n).to_string()
}

fn strip_cron(n: &str) -> &str {
    match n.rsplit_once('-') {
        Some((head, tail)) if !head.is_empty() && is_cron_suffix(tail) => head,
        _ => n,
    }
}

/// Whether a suffix is acceptable (WorkloadKey rules).
pub fn valid_suffix(s: &str) -> bool {
    !s.is_empty() && s.len() <= 512 && !s.starts_with('/') && !s.ends_with('/') && !s.contains("//") && s.chars().all(|c| !c.is_control() && !c.is_whitespace())
}

/// The key suffix base `{tag}/{name}`, or the annotation's value when valid.
pub fn suffix_base(tag: &str, name: &str, annotations: &BTreeMap<String, String>) -> String {
    if let Some(a) = annotations.get(WORKLOAD_KEY).map(|s| s.trim()).filter(|s| valid_suffix(s)) {
        return a.to_string();
    }
    format!("{tag}/{name}")
}

/// Render `[k8s.flyte] namespace_template`.
pub fn render_namespace(template: &str, org: &str, project: &str, domain: &str) -> String {
    template.replace("{org}", org).replace("{project}", project).replace("{domain}", domain)
}

/// The Flyte key of a task pod: from Flyte's pod labels, **only if** the
/// pod's (trusted) namespace equals the rendered namespace template.
pub fn flyte_key(labels: &BTreeMap<String, String>, namespace: &str, s: &FlyteSettings) -> Option<WorkloadKey> {
    if !s.enabled {
        return None;
    }
    let project = labels.get(&s.project_label)?;
    let domain = labels.get(&s.domain_label)?;
    let task = labels.get(&s.task_label)?;
    let org = labels.get(&s.org_label).cloned().or_else(|| s.default_org.clone())?;
    if render_namespace(&s.namespace_template, &org, project, domain) != namespace {
        return None;
    }
    let tenant = Tenant::new(format!("{org}/{project}/{domain}")).ok()?;
    WorkloadKey::new("flyte", tenant, task.clone()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_suffixes() {
        assert_eq!(base_name("train-x7k2p", Some("train-")), "train");
        assert_eq!(base_name("", Some("train-")), "train");
        assert_eq!(base_name("train-x7k2p", None), "train");
        // A real word is not a random suffix (vowels, no digit).
        assert_eq!(base_name("model-train", None), "model-train");
        assert_eq!(base_name("model-bcdfg", None), "model-bcdfg");
        // CronJob job and its pods.
        assert_eq!(base_name("nightly-29012345", None), "nightly");
        assert_eq!(base_name("nightly-29012345-q8z4v", None), "nightly");
        assert_eq!(base_name("nightly-29012345-q8z4v", Some("nightly-29012345-")), "nightly");
        // Short numeric suffixes are kept (versions, shards).
        assert_eq!(base_name("shard-42", None), "shard-42");
    }

    #[test]
    fn annotation_sets_only_the_suffix() {
        let mut a = BTreeMap::new();
        assert_eq!(suffix_base("job", "x", &a), "job/x");
        a.insert(WORKLOAD_KEY.into(), "team/llm-finetune".into());
        assert_eq!(suffix_base("job", "x", &a), "team/llm-finetune");
        a.insert(WORKLOAD_KEY.into(), "bad key//".into());
        assert_eq!(suffix_base("job", "x", &a), "job/x");
    }

    #[test]
    fn flyte_namespace_check() {
        let s = FlyteSettings { default_org: Some("acme".into()), ..Default::default() };
        let mut l = BTreeMap::new();
        l.insert("project".to_string(), "ml".to_string());
        l.insert("domain".to_string(), "development".to_string());
        l.insert("task-name".to_string(), "env.train".to_string());
        let k = flyte_key(&l, "ml-development", &s).unwrap();
        assert_eq!(k.to_string(), "flyte://acme/ml/development/env.train");
        // Forged labels in another namespace: not Flyte-keyed.
        assert!(flyte_key(&l, "team-b", &s).is_none());
        l.insert("organization".to_string(), "other".to_string());
        assert_eq!(flyte_key(&l, "ml-development", &s).unwrap().tenant.as_str(), "other/ml/development");
    }
}
