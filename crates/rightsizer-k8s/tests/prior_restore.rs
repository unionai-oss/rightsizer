//! The signed prior through the adapter (tampering, rotation) and restore.

mod common;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use common::*;
use rightsizer_core::Bytes;
use rightsizer_k8s::prior::{VerifyError, verify};
use rightsizer_k8s::{K8sAdapter, SigningKeys};
use rightsizer_plugin::{Native, Patch, SkipReason};
use serde_json::{Value, json};

const BUILD: &str = "spec.template.spec.containers[build]";

fn sized_job() -> (Native, Value) {
    let n = admission(value("job.yaml"), "ml");
    let p = render(&adapter(), &n, &[decision(BUILD, Some(2000), Some(12 * 1024))]);
    let s = applied(&n, &p);
    let v = object_of(&s);
    (s, v)
}

fn with_prior(v: &Value, prior: &str) -> Native {
    let mut v = v.clone();
    v["metadata"]["annotations"]["rightsizer.io/prior"] = json!(prior);
    admission(v, "ml")
}

#[test]
fn tampered_prior_does_not_verify() {
    let (_, v) = sized_job();
    let a = v["metadata"]["annotations"]["rightsizer.io/prior"].as_str().unwrap();
    let mut j: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(a).unwrap()).unwrap();
    j["prior"][BUILD]["requests"]["memory"] = json!(Bytes::gib(512).0);
    let forged = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&j).unwrap());
    assert_eq!(verify(&forged, &keys()), Err(VerifyError::BadSignature));
    // Restore refuses a tampered prior.
    assert!(adapter().restore(&with_prior(&v, &forged)).is_err());
    // A tampered prior next to a decision id: treated as another layer's marker.
    let e = extract(&adapter(), &with_prior(&v, &forged), &env());
    assert!(matches!(e.skip, Some(SkipReason::AlreadySized { .. })));
    // An unsigned "prior" without a decision id is ignored: the object is sized.
    let mut fresh = value("job.yaml");
    fresh["metadata"]["annotations"]["rightsizer.io/prior"] = json!(URL_SAFE_NO_PAD.encode(br#"{"v":1,"kid":"k1","prior":{}}"#));
    assert_eq!(extract(&adapter(), &admission(fresh, "ml"), &env()).units.len(), 2);
    // Swapping the key id does not help.
    j["kid"] = json!("k2");
    let swapped = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&j).unwrap());
    assert!(matches!(verify(&swapped, &keys()), Err(VerifyError::UnknownKey(_))));
}

#[test]
fn key_rotation_keeps_old_priors_valid() {
    let (s, _) = sized_job();
    let mut rotated = SigningKeys::single("k2", b"rotated-key-abcdefghijkl".to_vec());
    rotated.keys.insert("k1".into(), keys().keys["k1"].clone());
    let a = K8sAdapter::new(settings()).with_keys(rotated);
    assert!(a.restore(&s).is_ok());
    // New priors are signed with the active key.
    let p = render(&a, &admission(value("pod-guaranteed.yaml"), "ml"), &[decision("spec.containers[embed]", Some(2000), Some(8192))]);
    let v = object_of(&applied(&admission(value("pod-guaranteed.yaml"), "ml"), &p));
    let prior = v["metadata"]["annotations"]["rightsizer.io/prior"].as_str().map(str::to_string);
    let raw: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(prior.unwrap()).unwrap()).unwrap();
    assert_eq!(raw["kid"], json!("k2"));
}

#[test]
fn restore_round_trips_every_kind() {
    type Case<'a> = (&'a str, &'a [(&'a str, Option<u32>, Option<u64>)]);
    let cases: &[Case] = &[
        ("job.yaml", &[(BUILD, Some(2000), Some(12 * 1024)), ("spec.template.spec.initContainers[stage]", Some(250), Some(256))]),
        ("job-no-resources.yaml", &[("spec.template.spec.containers[eval]", Some(1000), Some(2048))]),
        ("cronjob.yaml", &[("spec.jobTemplate.spec.template.spec.containers[retrain]", Some(6000), Some(48 * 1024))]),
        ("pod-guaranteed.yaml", &[("spec.containers[embed]", Some(2000), Some(8192))]),
        ("pod-level.yaml", &[("spec.resources", Some(1000), Some(2048))]),
        ("trainjob.yaml", &[("spec.trainer.resourcesPerNode", Some(16_000), Some(128 * 1024))]),
        ("rayjob.yaml", &[("spec.rayClusterSpec.workerGroupSpecs[gpu].template.spec.containers[ray-worker]", Some(6000), Some(24 * 1024))]),
        ("volcano-job.yaml", &[("spec.tasks[worker].template.spec.containers[mpi]", Some(12_000), Some(48 * 1024))]),
    ];
    let a = adapter();
    for (fx, ds) in cases {
        let n = admission(value(fx), "ml");
        let d: Vec<_> = ds.iter().map(|(u, c, m)| decision(u, *c, *m)).collect();
        let p = render(&a, &n, &d);
        assert!(!p.is_none(), "{fx}");
        let sized = applied(&n, &p);
        assert_ne!(object_of(&sized), value(fx));
        let r = a.restore(&sized).unwrap();
        assert_eq!(r.units.len(), ds.len(), "{fx}");
        assert!(r.missing.is_empty());
        let back = applied(&sized, &Patch::JsonPatch { ops: r.ops });
        pretty_assertions::assert_eq!(stringly(&object_of(&back)), stringly(&value(fx)), "{fx}");
    }
}

#[test]
fn restore_needs_a_valid_prior() {
    let a = adapter();
    assert!(a.restore(&admission(value("job.yaml"), "ml")).is_err());
    let (s, _) = sized_job();
    assert!(K8sAdapter::new(settings()).restore(&s).is_err(), "no keys");
    assert!(K8sAdapter::new(settings()).with_keys(SigningKeys::single("k1", b"some-other-key-0000000".to_vec())).restore(&s).is_err());
}
