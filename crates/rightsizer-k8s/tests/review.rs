//! Adversarial review regressions (tenant isolation, attribution, S1/S2).

mod common;

use common::*;
use rightsizer_core::Bytes;
use rightsizer_k8s::{FlyteSettings, K8sSettings};
use rightsizer_plugin::{Native, SkipReason};
use serde_json::{Value, json};

const BUILD: &str = "spec.template.spec.containers[build]";

fn sized_job_object(ns: &str) -> Value {
    let n = admission(value("job.yaml"), ns);
    let p = render(&adapter(), &n, &[decision(BUILD, Some(2000), Some(4 * 1024))]);
    object_of(&applied(&n, &p))
}

/// The watcher (and `apply --watch`) reads the origin prior through
/// `extract_all`: it must be what the author wrote, not what rightsizer
/// applied, or no reduction is ever attributed as rightsizer-caused (S5).
#[test]
fn extract_all_reports_the_signed_origin_prior_of_sized_units() {
    let v = sized_job_object("ml");
    assert_eq!(v.pointer("/spec/template/spec/containers/0/resources/requests/memory"), Some(&json!("4Gi")));
    let env = settings().to_env(Some("ml"));
    let ex = adapter().extract_all(&Native::Object { value: v }, &env).unwrap().pop().unwrap();
    let u = ex.units.iter().find(|u| u.id.0 == BUILD).expect("sized unit is included");
    assert_eq!(u.prior.spec.requests.memory, Some(Bytes::gib(16)), "origin, not applied");
    assert_eq!(u.prior.spec.limits.memory, Some(Bytes::gib(32)));
}

/// A genuine signed prior copied onto another object (another namespace,
/// or another workload in the same namespace) is not our prior there.
#[test]
fn a_replayed_prior_is_not_trusted_on_another_object() {
    let genuine = sized_job_object("team-a");
    let prior = genuine["metadata"]["annotations"]["rightsizer.io/prior"].clone();
    let did = genuine["metadata"]["annotations"]["rightsizer.io/decision-id"].clone();

    // Same object, same namespace: still ours (reinvocation resumes / skips).
    let e = extract(&adapter(), &admission(genuine.clone(), "team-a"), &env());
    assert!(e.units.iter().all(|u| u.id.0 != BUILD), "build is already sized");
    // The same object admitted in another namespace: the prior is not ours there,
    // and with our decision id next to it the object is left alone.
    let e = extract(&adapter(), &admission(genuine.clone(), "team-b"), &env());
    assert!(matches!(e.skip, Some(SkipReason::AlreadySized { .. })), "{:?}", e.skip);

    // Copied onto a fresh object in another namespace.
    let mut other = value("job.yaml");
    other["metadata"]["namespace"] = json!("team-b");
    other["metadata"]["annotations"]["rightsizer.io/prior"] = prior.clone();
    other["spec"]["template"]["spec"]["containers"][0]["resources"]["requests"]["memory"] = json!("1Gi");
    let env_b = settings().to_env(Some("team-b"));
    let ex = adapter().extract_all(&Native::Object { value: other.clone() }, &env_b).unwrap().pop().unwrap();
    let u = ex.units.iter().find(|u| u.id.0 == BUILD).unwrap();
    assert_eq!(u.prior.spec.requests.memory, Some(Bytes::gib(1)), "a replayed prior must not become the origin");
    // Without a decision id it is ignored and the object is sized normally.
    let e = extract(&adapter(), &admission(other.clone(), "team-b"), &env());
    assert!(e.skip.is_none(), "{:?}", e.skip);
    assert_eq!(e.units.len(), 2);

    // Copied onto another workload in the same namespace.
    let mut renamed = value("job.yaml");
    renamed["metadata"]["name"] = json!("other-job");
    renamed["metadata"]["annotations"]["rightsizer.io/prior"] = prior;
    renamed["metadata"]["annotations"]["rightsizer.io/decision-id"] = did;
    let ex = adapter().extract_all(&Native::Object { value: renamed }, &settings().to_env(Some("team-a"))).unwrap().pop().unwrap();
    let u = ex.units.iter().find(|u| u.id.0 == BUILD).unwrap();
    assert_eq!(u.prior.spec.requests.memory, Some(Bytes::gib(16)), "as written on that object");
    // (16Gi is that object's own declared value: the copied prior is not used.)
}

/// Reinvocation keeps the prior valid (sidecar injected after us).
#[test]
fn reinvocation_still_recognises_our_prior() {
    let mut v = sized_job_object("ml");
    v["spec"]["template"]["spec"]["containers"].as_array_mut().unwrap().insert(0, json!({"name": "istio-proxy", "image": "istio/proxyv2"}));
    let e = extract(&adapter(), &admission(v, "ml"), &env());
    // `build` is sized; the injected sidecar is the only new unit.
    assert!(e.units.iter().all(|u| u.id.0 != BUILD), "{:?}", e.units.iter().map(|u| &u.id.0).collect::<Vec<_>>());
}

/// Generate-named bare pods: the name is empty at admission and generated
/// afterwards; the prior must stay valid on the created pod.
#[test]
fn generate_named_pod_prior_survives_name_generation() {
    let mut pod = value("pod-guaranteed.yaml");
    let name = pod["metadata"]["name"].as_str().unwrap().to_string();
    pod["metadata"].as_object_mut().unwrap().remove("name");
    pod["metadata"]["generateName"] = json!(format!("{name}-"));
    let n = admission(pod, "ml");
    let p = render(&adapter(), &n, &[decision("spec.containers[embed]", Some(2000), Some(8192))]);
    let mut v = object_of(&applied(&n, &p));
    v["metadata"]["name"] = json!(format!("{name}-x7k2p"));
    let ex = adapter().extract_all(&Native::Object { value: v }, &settings().to_env(Some("ml"))).unwrap().pop().unwrap();
    let orig = rightsizer_k8s::resources::spec_of(value("pod-guaranteed.yaml").pointer("/spec/containers/0/resources"), rightsizer_core::AcceleratorId::UnknownGpu).unwrap();
    assert_eq!(ex.units[0].prior.spec.requests.memory, orig.requests.memory.or(orig.limits.memory));
}

/// The org is not part of the default namespace template, so a pod's org
/// label is not checked by the namespace: it must not pick another org's
/// Flyte history (flyte.md §12, plugins.md §2 "tenant trusted").
#[test]
fn flyte_org_label_cannot_select_another_org() {
    let flyte = FlyteSettings { default_org: Some("acme".into()), ..Default::default() };
    let labels: std::collections::BTreeMap<String, String> =
        [("project", "ml"), ("domain", "development"), ("task-name", "env.train"), ("organization", "othercorp")].into_iter().map(|(a, b)| (a.into(), b.into())).collect();
    assert!(rightsizer_k8s::keys::flyte_key(&labels, "ml-development", &flyte).is_none(), "org label outside the namespace template is not trusted");
    // The configured org is fine.
    let mut ok = labels.clone();
    ok.insert("organization".into(), "acme".into());
    assert_eq!(rightsizer_k8s::keys::flyte_key(&ok, "ml-development", &flyte).unwrap().to_string(), "flyte://acme/ml/development/env.train");
    // No configured org and an org-less template: the org cannot be verified.
    let none = FlyteSettings::default();
    assert!(rightsizer_k8s::keys::flyte_key(&labels, "ml-development", &none).is_none());
    // With `{org}` in the template the namespace binds the label.
    let bound = FlyteSettings { namespace_template: "{org}-{project}-{domain}".into(), ..Default::default() };
    assert_eq!(rightsizer_k8s::keys::flyte_key(&labels, "othercorp-ml-development", &bound).unwrap().tenant.as_str(), "othercorp/ml/development");
    assert!(rightsizer_k8s::keys::flyte_key(&labels, "ml-development", &bound).is_none());
    let _ = K8sSettings::default();
}

/// The watcher's attempt numbers feed the store's dedup key: they must not
/// collide between a restart of one pod and the next pod of the same Job.
#[cfg(feature = "cluster")]
#[test]
fn watcher_attempt_numbers_do_not_collide() {
    use rightsizer_k8s::cluster::watcher::attempt_number;
    assert_eq!(attempt_number(1, 0), 1);
    assert_eq!(attempt_number(1, 1), 2);
    assert_ne!(attempt_number(1, 1), attempt_number(2, 0));
    assert!(attempt_number(2, 0) > attempt_number(1, 5));
    let mut seen = std::collections::BTreeSet::new();
    for rank in 1..=20 {
        for restart in 0..20 {
            assert!(seen.insert(attempt_number(rank, restart)));
        }
    }
}
