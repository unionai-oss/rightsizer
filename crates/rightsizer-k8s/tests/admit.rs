//! The admission request path without a cluster (feature `engine`):
//! a `StaticView` and an in-memory engine.
#![cfg(feature = "engine")]

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use base64::Engine as _;
use rightsizer_engine::{Config, Engine};
use rightsizer_k8s::admit::{AdmitOptions, Admitter};
use rightsizer_k8s::view::{ClusterInputs, StaticView};
use serde_json::{Value, json};

fn review(object: Value, ns: &str, dry_run: bool) -> Value {
    json!({"apiVersion": "admission.k8s.io/v1", "kind": "AdmissionReview",
           "request": {"uid": "u-1", "operation": "CREATE", "namespace": ns, "dryRun": dry_run,
                       "userInfo": {"username": "alice"}, "object": object}})
}

async fn admitter(mode: &str) -> Admitter {
    let engine = Engine::builder(Config::default()).build().await.unwrap();
    let mut namespaces = BTreeMap::new();
    let labels: BTreeMap<String, String> = [("rightsizer.io/enabled", "true"), ("rightsizer.io/mode", mode)].into_iter().map(|(a, b)| (a.to_string(), b.to_string())).collect();
    namespaces.insert("ml".to_string(), labels);
    namespaces.insert("off-ns".to_string(), BTreeMap::new());
    let view = StaticView { base: ClusterInputs::default(), namespaces };
    let s = common::settings();
    let base = serde_json::to_value(&s).unwrap().as_object().cloned().unwrap();
    Admitter::new(engine, common::adapter(), base, Arc::new(view), AdmitOptions::default())
}

fn patch_of(resp: &Value) -> Vec<Value> {
    match resp.pointer("/response/patch").and_then(Value::as_str) {
        Some(p) => serde_json::from_slice(&base64::engine::general_purpose::STANDARD.decode(p).unwrap()).unwrap(),
        None => vec![],
    }
}

#[tokio::test]
async fn recommend_is_annotation_only_and_always_allowed() {
    let a = admitter("recommend").await;
    let resp = a.review("jobs.rightsizer.io", &review(common::value("job.yaml"), "ml", false)).await;
    assert_eq!(resp["response"]["allowed"], true);
    assert_eq!(resp["response"]["uid"], "u-1");
    let ops = patch_of(&resp);
    assert!(!ops.is_empty());
    for o in &ops {
        let p = o["path"].as_str().unwrap();
        assert!(p.contains("/annotations") || p.contains("/labels"), "{p}");
    }
}

#[tokio::test]
async fn not_enabled_excluded_and_garbage_admit_unchanged() {
    let a = admitter("enforce").await;
    for (obj, ns) in [(common::value("job.yaml"), "off-ns"), (common::value("job.yaml"), "kube-system"), (json!({"kind": "Nope"}), "ml")] {
        let resp = a.review("jobs.rightsizer.io", &review(obj, ns, false)).await;
        assert_eq!(resp["response"]["allowed"], true);
        assert!(resp["response"].get("patch").is_none(), "{ns}: {resp}");
    }
    // Not even an AdmissionReview: allowed, counted as fail-open.
    let resp = a.review("jobs.rightsizer.io", &json!({"hello": 1})).await;
    assert_eq!(resp["response"]["allowed"], true);
    assert!(a.metrics().fail_open.with_label_values(&["jobs.rightsizer.io", "error"]).get() >= 1);
}

#[tokio::test]
async fn enforce_on_a_non_durable_store_is_demoted_to_annotations() {
    // The default (in-memory) store is not durable: enforce demotes to recommend (SPEC §8.1).
    let a = admitter("enforce").await;
    let out = a.admit(&review(common::value("job.yaml"), "ml", true), Instant::now()).await.unwrap();
    assert!(out.decisions.iter().all(|d| d.mode < rightsizer_core::Mode::Enforce));
    assert!(out.patch.iter().all(|o| serde_json::to_value(o).unwrap()["path"].as_str().unwrap().contains("/annotations") || serde_json::to_value(o).unwrap()["path"].as_str().unwrap().contains("/labels")));
}

/// The watcher joins outcomes by decision ids read from annotations, which
/// any pod author can set: another workload's decision id is not joined.
#[tokio::test]
async fn copied_decision_ids_of_other_workloads_are_not_joined() {
    let a = admitter("recommend").await;
    let out = a.admit(&review(common::value("job.yaml"), "ml", false), Instant::now()).await.unwrap();
    let d = out.decisions.first().unwrap().clone();
    // Recorded in a spawned task; wait for it, then flush.
    for _ in 0..50 {
        a.engine().flush().await;
        if a.engine().store().decision(d.id).await.unwrap().is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(a.engine().store().decision(d.id).await.unwrap().is_some());
    let own = rightsizer_k8s::runtime::verified_decision_id(a.engine(), Some(d.id), &d.key).await;
    assert_eq!(own, Some(d.id));
    let other = rightsizer_core::WorkloadKey::new("k8s", rightsizer_core::Tenant::new("prod/team-b").unwrap(), "job/victim/main").unwrap();
    assert_eq!(rightsizer_k8s::runtime::verified_decision_id(a.engine(), Some(d.id), &other).await, None, "another tenant's decision");
    let unknown = ulid::Ulid::from_datetime(std::time::SystemTime::now());
    assert_eq!(rightsizer_k8s::runtime::verified_decision_id(a.engine(), Some(unknown), &other).await, Some(unknown));
}

/// An admitter over a durable, shared (file) store so enforce is not demoted.
async fn enforcing_admitter(base_inputs: rightsizer_k8s::view::ClusterInputs) -> Admitter {
    let dir = std::env::temp_dir().join(format!("rsz-k8s-review-{}", ulid::Ulid::from_datetime(std::time::SystemTime::now())));
    std::fs::create_dir_all(&dir).unwrap();
    let store = rightsizer_history::open(&rightsizer_history::StoreConfig {
        durable: Some(true),
        shared: Some(true),
        ..rightsizer_history::StoreConfig::new(format!("file://{}", dir.display()))
    })
    .await
    .unwrap();
    let engine = Engine::builder(Config::default()).store(store).build().await.unwrap();
    let mut namespaces = BTreeMap::new();
    let labels: BTreeMap<String, String> = [("rightsizer.io/enabled", "true"), ("rightsizer.io/mode", "enforce")].into_iter().map(|(a, b)| (a.to_string(), b.to_string())).collect();
    namespaces.insert("ml".to_string(), labels);
    let view = StaticView { base: base_inputs, namespaces };
    let base = serde_json::to_value(common::settings()).unwrap().as_object().cloned().unwrap();
    Admitter::new(engine, common::adapter(), base, Arc::new(view), AdmitOptions::default())
}

/// Render minimal (plugins.md §2): an enforced unit whose decision changes
/// nothing (no history: the prior, here LimitRange defaults) must not get
/// resources written into its template; only the annotations.
#[tokio::test]
async fn unchanged_enforced_units_get_no_resource_writes() {
    let lr = json!({"apiVersion": "v1", "kind": "LimitRange", "metadata": {"name": "defaults", "namespace": "ml"},
                    "spec": {"limits": [{"type": "Container", "default": {"memory": "4Gi", "cpu": "2"}, "defaultRequest": {"memory": "1Gi", "cpu": "500m"}}]}});
    let a = enforcing_admitter(rightsizer_k8s::view::ClusterInputs { limit_ranges: vec![lr], ..Default::default() }).await;
    let out = a.admit(&review(common::value("job-no-resources.yaml"), "ml", true), Instant::now()).await.unwrap();
    assert!(out.decisions.iter().any(|d| d.mode == rightsizer_core::Mode::Enforce), "{:?}", out.decisions.iter().map(|d| (d.mode, &d.source)).collect::<Vec<_>>());
    for o in &out.patch {
        let p = serde_json::to_value(o).unwrap()["path"].as_str().unwrap().to_string();
        assert!(!p.contains("/resources"), "resource write for an unchanged unit: {p} ({})", out.result);
    }
}

/// S1: the apiserver rejects an object whose annotations exceed 256 KiB in
/// total, and a patch failure fails the user's request regardless of
/// `failurePolicy`. An object close to the limit is admitted unpatched.
#[tokio::test]
async fn no_patch_that_would_push_annotations_over_the_apiserver_limit() {
    let a = admitter("recommend").await;
    let mut j = common::value("job.yaml");
    j["metadata"]["annotations"]["example.com/blob"] = json!("x".repeat(256 * 1024 - 200));
    let resp = a.review("jobs.rightsizer.io", &review(j, "ml", false)).await;
    assert_eq!(resp["response"]["allowed"], true);
    assert!(resp["response"].get("patch").is_none(), "patched over the annotation limit");
    // Far from the limit: patched as usual.
    let resp = a.review("jobs.rightsizer.io", &review(common::value("job.yaml"), "ml", false)).await;
    assert!(resp["response"].get("patch").is_some());
}
