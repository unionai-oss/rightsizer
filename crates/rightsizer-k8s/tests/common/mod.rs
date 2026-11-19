//! Shared helpers for the integration tests.
#![allow(dead_code)]

use std::path::PathBuf;

use rightsizer_core::{Bytes, MilliCpu, Requests, ResourceSpec, UnitId};
use rightsizer_k8s::{K8sAdapter, K8sSettings, SigningKeys};
use rightsizer_plugin::{Adapter, AdapterEnv, Extracted, Native, Patch};
use serde_json::{Value, json};

pub fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(name)
}

pub fn fixture(name: &str) -> String {
    std::fs::read_to_string(fixture_path(name)).unwrap_or_else(|e| panic!("{name}: {e}"))
}

pub fn doc_text(text: &str) -> Native {
    Native::Document { media_type: "application/yaml".into(), bytes: text.as_bytes().to_vec() }
}

pub fn doc(name: &str) -> Native {
    doc_text(&fixture(name))
}

/// The first YAML document of a fixture as a JSON value.
pub fn value(name: &str) -> Value {
    rightsizer_k8s::yaml::parse(&fixture(name)).unwrap().remove(0).value
}

/// An AdmissionReview for `object` in `namespace`.
pub fn admission(object: Value, namespace: &str) -> Native {
    Native::Object {
        value: json!({
            "apiVersion": "admission.k8s.io/v1", "kind": "AdmissionReview",
            "request": {"uid": "8f1d", "operation": "CREATE", "namespace": namespace,
                        "userInfo": {"username": "alice@acme.dev"}, "dryRun": false, "object": object}
        }),
    }
}

pub fn keys() -> SigningKeys {
    SigningKeys::single("k1", b"test-key-0123456789abcdef".to_vec())
}

pub fn settings() -> K8sSettings {
    K8sSettings { cluster: Some("prod".into()), ..Default::default() }
}

pub fn adapter() -> K8sAdapter {
    K8sAdapter::new(settings()).with_keys(keys())
}

pub fn env() -> AdapterEnv {
    settings().to_env(None)
}

pub fn env_with(s: &K8sSettings) -> AdapterEnv {
    s.to_env(None)
}

pub fn block<F: std::future::Future>(f: F) -> F::Output {
    futures::executor::block_on(f)
}

pub fn extract(a: &K8sAdapter, n: &Native, e: &AdapterEnv) -> Extracted {
    block(a.extract(n, e)).unwrap()
}

pub fn req(cpu_m: Option<u32>, mem_mib: Option<u64>) -> ResourceSpec {
    ResourceSpec { requests: Requests { cpu: cpu_m.map(MilliCpu), memory: mem_mib.map(Bytes::mib), ..Default::default() }, ..Default::default() }
}

pub fn decision(unit: &str, cpu_m: Option<u32>, mem_mib: Option<u64>) -> (UnitId, ResourceSpec) {
    (UnitId(unit.into()), req(cpu_m, mem_mib))
}

pub fn render(a: &K8sAdapter, n: &Native, d: &[(UnitId, ResourceSpec)]) -> Patch {
    block(a.render(n, d)).unwrap()
}

/// Apply a patch to a native submission and return the new native.
pub fn applied(n: &Native, p: &Patch) -> Native {
    match (n, p) {
        (_, Patch::None) => n.clone(),
        (Native::Document { media_type, bytes }, Patch::TextEdits { edits }) => {
            let t = rightsizer_plugin::apply_text_edits(std::str::from_utf8(bytes).unwrap(), edits).unwrap();
            Native::Document { media_type: media_type.clone(), bytes: t.into_bytes() }
        }
        (Native::Object { value }, Patch::JsonPatch { ops }) => {
            let mut v = value.clone();
            let target = if v.get("request").is_some() { v.pointer_mut("/request/object").unwrap() } else { &mut v };
            let jp: json_patch::Patch = serde_json::from_value(serde_json::to_value(ops).unwrap()).unwrap();
            json_patch::patch(target, &jp).unwrap();
            Native::Object { value: v }
        }
        _ => panic!("patch kind does not fit the native"),
    }
}

pub fn text_of(n: &Native) -> String {
    match n {
        Native::Document { bytes, .. } => String::from_utf8(bytes.clone()).unwrap(),
        _ => panic!("not a document"),
    }
}

pub fn object_of(n: &Native) -> Value {
    match n {
        Native::Object { value } => value.get("request").map(|r| r["object"].clone()).unwrap_or_else(|| value.clone()),
        Native::Document { .. } => rightsizer_k8s::yaml::parse(&text_of(n)).unwrap().remove(0).value,
        _ => panic!(),
    }
}

/// Compare against a golden file; `UPDATE_GOLDEN=1` rewrites it.
pub fn golden(name: &str, actual: &str) {
    let p = fixture_path(&format!("golden/{name}"));
    if std::env::var_os("UPDATE_GOLDEN").is_some() || !p.exists() {
        std::fs::write(&p, actual).unwrap();
    }
    let want = std::fs::read_to_string(&p).unwrap();
    pretty_assertions::assert_eq!(actual, want, "golden {name}");
}

/// Numbers as strings: `cpu: 8` and `cpu: "8"` are the same quantity.
pub fn stringly(v: &Value) -> Value {
    match v {
        Value::Number(n) => Value::String(n.to_string()),
        Value::Array(a) => Value::Array(a.iter().map(stringly).collect()),
        Value::Object(m) => Value::Object(m.iter().map(|(k, v)| (k.clone(), stringly(v))).collect()),
        o => o.clone(),
    }
}
