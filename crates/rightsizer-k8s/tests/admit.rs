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
