//! validate: a rejection per check; capacity checks only reject what the
//! patch makes worse; the reservation ledger; Autopilot normalization.

mod common;

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use common::*;
use rightsizer_core::{Axis, Bytes, HoldReason, MilliCpu, NodeShape};
use rightsizer_k8s::autopilot::AutopilotSettings;
use rightsizer_k8s::validate::{check, prior_reason};
use rightsizer_k8s::{K8sAdapter, K8sSettings, MemoryLedger, RenderRequest};
use rightsizer_plugin::{AdapterEnv, Native, Patch, Rejection};
use serde_json::{Value, json};

const BUILD: &str = "spec.template.spec.containers[build]";

fn t0() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-11-18T12:00:00Z").unwrap().with_timezone(&Utc)
}

fn check_with(s: &K8sSettings, n: &Native, d: &[(rightsizer_core::UnitId, rightsizer_core::ResourceSpec)]) -> Result<(), Rejection> {
    let a = K8sAdapter::new(s.clone()).with_keys(keys());
    let p = a.render_with(n, &RenderRequest::enforce(d)).unwrap().patch;
    assert!(!p.is_none());
    a.validate_at(n, &p, &s.to_env(None), t0())
}

fn job_admission() -> Native {
    admission(value("job.yaml"), "ml")
}

#[test]
fn a_clean_patch_validates() {
    let s = settings();
    assert_eq!(check_with(&s, &job_admission(), &[decision(BUILD, Some(2000), Some(12 * 1024))]), Ok(()));
    assert_eq!(check_with(&s, &doc("job.yaml"), &[decision(BUILD, Some(2000), Some(12 * 1024))]), Ok(()));
}

#[test]
fn rejects_limitrange_max_and_ratio() {
    let lr = json!({"kind": "LimitRange", "metadata": {"name": "caps", "namespace": "ml"},
        "spec": {"limits": [{"type": "Container", "max": {"memory": "48Gi"}}]}});
    let s = K8sSettings { limit_ranges: vec![lr], ..settings() };
    // 40Gi request → 80Gi limit (ratio kept) > max 48Gi.
    let r = check_with(&s, &job_admission(), &[decision(BUILD, Some(4000), Some(40 * 1024))]).unwrap_err();
    assert_eq!(r.reason, check::LIMITRANGE);
    assert_eq!(prior_reason(&r), "validation:limitrange");
    assert!(r.detail.unwrap().contains("maximum"));
    assert_eq!(check_with(&s, &job_admission(), &[decision(BUILD, Some(4000), Some(20 * 1024))]), Ok(()));
    // maxLimitRequestRatio: the patch keeps the ratio, so only a violating
    // original fails (and then no patch is emitted for it either).
    let ratio = json!({"kind": "LimitRange", "metadata": {"name": "ratio", "namespace": "ml"},
        "spec": {"limits": [{"type": "Container", "maxLimitRequestRatio": {"memory": "2"}}]}});
    let s = K8sSettings { limit_ranges: vec![ratio], ..settings() };
    assert_eq!(check_with(&s, &job_admission(), &[decision(BUILD, Some(4000), Some(10 * 1024))]), Ok(()));
    let mut j = value("job.yaml");
    j["spec"]["template"]["spec"]["containers"][0]["resources"]["limits"]["memory"] = json!("64Gi");
    let r = check_with(&s, &admission(j, "ml"), &[decision(BUILD, Some(2000), None)]).unwrap_err();
    assert!(r.detail.unwrap().contains("ratio"));
}

#[test]
fn rejects_template_request_above_defaulted_limit() {
    // Templates are checked as the pods will be: the LimitRange default limit
    // (4Gi) lands on pods; a 6Gi request would make every pod invalid.
    let lr = json!({"kind": "LimitRange", "metadata": {"name": "defaults", "namespace": "ml"},
        "spec": {"limits": [{"type": "Container", "default": {"memory": "4Gi"}, "defaultRequest": {"memory": "1Gi"}}]}});
    let s = K8sSettings { limit_ranges: vec![lr], ..settings() };
    let n = admission(value("job-no-resources.yaml"), "ml");
    let r = check_with(&s, &n, &[decision("spec.template.spec.containers[eval]", None, Some(6 * 1024))]).unwrap_err();
    assert_eq!(r.reason, check::LIMITRANGE);
    assert_eq!(check_with(&s, &n, &[decision("spec.template.spec.containers[eval]", None, Some(3 * 1024))]), Ok(()));
}

#[test]
fn rejects_pod_level_limitrange() {
    let lr = json!({"kind": "LimitRange", "metadata": {"name": "pod", "namespace": "ml"}, "spec": {"limits": [{"type": "Pod", "max": {"cpu": "6"}}]}});
    let s = K8sSettings { limit_ranges: vec![lr], ..settings() };
    // init stage 500m; build 8 → pod request 8 > 6.
    let r = check_with(&s, &job_admission(), &[decision(BUILD, Some(8000), None)]).unwrap_err();
    assert_eq!(r.reason, check::LIMITRANGE);
}

#[test]
fn rejects_qos_change() {
    // defaults make every container Guaranteed (default = defaultRequest).
    let lr = json!({"kind": "LimitRange", "metadata": {"name": "g", "namespace": "ml"},
        "spec": {"limits": [{"type": "Container", "default": {"memory": "2Gi", "cpu": "1"}, "defaultRequest": {"memory": "2Gi", "cpu": "1"}}]}});
    let s = K8sSettings { limit_ranges: vec![lr], ..settings() };
    let mut j = value("job-no-resources.yaml");
    j["spec"]["template"]["spec"]["containers"].as_array_mut().unwrap().pop();
    let n = admission(j, "ml");
    let r = check_with(&s, &n, &[decision("spec.template.spec.containers[eval]", None, Some(1024))]).unwrap_err();
    assert_eq!(r.reason, check::QOS);
}

#[test]
fn rejects_pod_spec_invariants() {
    let mut j = value("job.yaml");
    j["spec"]["template"]["spec"]["volumes"].as_array_mut().unwrap().push(json!({"name": "scratch", "emptyDir": {}}));
    let r = check_with(&settings(), &admission(j, "ml"), &[decision(BUILD, Some(2000), None)]).unwrap_err();
    assert_eq!(r.reason, check::POD_SPEC);
    assert!(r.detail.unwrap().contains("duplicate volume"));
    let mut j = value("job.yaml");
    let mounts = j["spec"]["template"]["spec"]["containers"][0]["volumeMounts"].as_array_mut().unwrap();
    mounts.push(json!({"name": "a", "mountPath": "/dev/shm"}));
    mounts.push(json!({"name": "b", "mountPath": "/dev/shm/"}));
    let r = check_with(&settings(), &admission(j, "ml"), &[decision(BUILD, Some(2000), None)]).unwrap_err();
    assert_eq!(r.reason, check::POD_SPEC);
    // Pod-level requests below the containers' sum.
    let mut p = value("pod-level.yaml");
    p["spec"]["containers"][0]["resources"] = json!({"requests": {"cpu": "1500m"}});
    let r = check_with(&settings(), &admission(p, "ml"), &[decision("spec.resources", Some(1000), None)]).unwrap_err();
    assert_eq!(r.reason, check::POD_SPEC);
}

fn quota(hard_mem: &str, used_mem: &str) -> Value {
    json!({"kind": "ResourceQuota", "metadata": {"name": "team", "namespace": "ml"},
           "spec": {"hard": {"requests.memory": hard_mem, "requests.cpu": "100"}},
           "status": {"hard": {"requests.memory": hard_mem, "requests.cpu": "100"}, "used": {"requests.memory": used_mem, "requests.cpu": "10"}}})
}

#[test]
fn rejects_quota_overflow_only_for_increases() {
    let s = K8sSettings { resource_quotas: vec![quota("100Gi", "60Gi")], ..settings() };
    let mut j = value("job.yaml");
    j["spec"]["parallelism"] = json!(2);
    let n = admission(j, "ml");
    // 2 × 24Gi = 48Gi > 40Gi remaining, and more than the original 32Gi.
    let r = check_with(&s, &n, &[decision(BUILD, None, Some(24 * 1024))]).unwrap_err();
    assert_eq!(r.reason, check::RESOURCE_QUOTA);
    assert_eq!(check_with(&s, &n, &[decision(BUILD, None, Some(18 * 1024))]), Ok(()));
    // Already over quota: a reduction is still fine.
    let tight = K8sSettings { resource_quotas: vec![quota("100Gi", "90Gi")], ..settings() };
    assert_eq!(check_with(&tight, &n, &[decision(BUILD, None, Some(8 * 1024))]), Ok(()));
}

#[test]
fn ledger_reservations_count_for_two_minutes() {
    let s = K8sSettings { resource_quotas: vec![quota("100Gi", "60Gi")], ..settings() };
    let ledger = Arc::new(MemoryLedger::default());
    let a = K8sAdapter::new(s.clone()).with_keys(keys()).with_ledger(ledger.clone());
    let env: AdapterEnv = s.to_env(None);
    // An earlier admission reserved +20Gi.
    let first = job_admission();
    let p1 = a.render_with(&first, &RenderRequest::enforce(&[decision(BUILD, None, Some(36 * 1024))])).unwrap().patch;
    assert_eq!(a.validate_at(&first, &p1, &env, t0()), Ok(()));
    a.reserve(&first, &p1, &env, "uid-1", t0()).unwrap();
    assert_eq!(ledger.len(), 1);
    // Now +6Gi (22Gi total) fits 40Gi but not 40 − 20.
    let second = job_admission();
    let p2 = a.render_with(&second, &RenderRequest::enforce(&[decision(BUILD, None, Some(22 * 1024))])).unwrap().patch;
    assert_eq!(a.validate_at(&second, &p2, &env, t0() + Duration::seconds(30)).unwrap_err().reason, check::RESOURCE_QUOTA);
    // After two minutes the reservation expired (the pods exist; status.used has them).
    assert_eq!(a.validate_at(&second, &p2, &env, t0() + Duration::seconds(121)), Ok(()));
}

fn kueue_settings(borrow: Option<&str>) -> K8sSettings {
    let mut cpu = json!({"name": "cpu", "nominalQuota": "1000"});
    if let Some(b) = borrow {
        cpu["borrowingLimit"] = json!(b);
    }
    K8sSettings {
        local_queues: vec![json!({"kind": "LocalQueue", "metadata": {"name": "training", "namespace": "ml"}, "spec": {"clusterQueue": "cq-a"}})],
        cluster_queues: vec![json!({"kind": "ClusterQueue", "metadata": {"name": "cq-a"}, "spec": {"cohort": "research", "resourceGroups": [
            {"coveredResources": ["cpu", "memory", "nvidia.com/gpu"], "flavors": [{"name": "h100", "resources": [
                cpu, {"name": "memory", "nominalQuota": "20000Gi", "borrowingLimit": "0"}, {"name": "nvidia.com/gpu", "nominalQuota": 64, "borrowingLimit": 0}]}]}]}})],
        resource_flavors: vec![json!({"kind": "ResourceFlavor", "metadata": {"name": "h100"}, "spec": {"nodeLabels": {"cloud.google.com/gke-accelerator": "nvidia-h100-80gb"}}})],
        nodes: vec![json!({"kind": "Node", "metadata": {"name": "gpu-1", "labels": {"cloud.google.com/gke-accelerator": "nvidia-h100-80gb", "node.kubernetes.io/instance-type": "a3-highgpu-8g"}},
                          "status": {"allocatable": {"cpu": "206", "memory": "1800Gi", "nvidia.com/gpu": "8"}}}),
                    json!({"kind": "Node", "metadata": {"name": "cpu-1", "labels": {}}, "status": {"allocatable": {"cpu": "16", "memory": "64Gi"}}}),
                    json!({"kind": "Node", "metadata": {"name": "b200-1", "labels": {"cloud.google.com/gke-accelerator": "nvidia-b200"}},
                          "status": {"allocatable": {"cpu": "220", "memory": "3900Gi", "nvidia.com/gpu": "8"}}})],
        ..settings()
    }
}

#[test]
fn rejects_kueue_nominal_and_flavor_shape() {
    let n = admission(value("jobset.yaml"), "ml");
    let trainer = "spec.replicatedJobs[workers].template.spec.template.spec.containers[trainer]";
    let s = kueue_settings(Some("0"));
    // 8 pods × 120 cpu + 2 = 962 ≤ 1000.
    assert_eq!(check_with(&s, &n, &[decision(trainer, Some(120_000), None)]), Ok(()));
    // 8 × 128 + 2 = 1026 > 1000 nominal + 0 borrowing.
    assert_eq!(check_with(&s, &n, &[decision(trainer, Some(128_000), None)]).unwrap_err().reason, check::KUEUE);
    // With a borrowing limit it is admissible (it may wait; it is not never-admissible).
    assert_eq!(check_with(&kueue_settings(Some("100")), &n, &[decision(trainer, Some(128_000), None)]), Ok(()));
    // In a cohort without a borrowing limit, borrowing is unbounded.
    assert_eq!(check_with(&kueue_settings(None), &n, &[decision(trainer, Some(128_000), None)]), Ok(()));
    // Larger than any node of the queue's flavors (a B200 node would fit, but is not a flavor).
    let r = check_with(&s, &n, &[decision(trainer, None, Some(1900 * 1024))]).unwrap_err();
    assert_eq!(r.reason, check::KUEUE);
}

#[test]
fn rejects_unschedulable_increase_only() {
    let s = K8sSettings {
        shapes: vec![NodeShape { name: Some("m6i.4xlarge".into()), cpu: MilliCpu(15_800), memory: Bytes::gib(60), gpu: None, price_per_hour: 0.768, host_per_gpu: None }],
        runtime_classes: vec![json!({"kind": "RuntimeClass", "metadata": {"name": "kata"}, "handler": "kata", "overhead": {"podFixed": {"cpu": "250m", "memory": "160Mi"}}})],
        ..settings()
    };
    let n = job_admission();
    let r = check_with(&s, &n, &[decision(BUILD, Some(16_000), None)]).unwrap_err();
    assert_eq!(r.reason, check::NODE_SHAPE);
    assert_eq!(check_with(&s, &n, &[decision(BUILD, Some(15_000), None)]), Ok(()));
    // RuntimeClass overhead counts.
    let mut j = value("job.yaml");
    j["spec"]["template"]["spec"]["runtimeClassName"] = json!("kata");
    assert_eq!(check_with(&s, &admission(j, "ml"), &[decision(BUILD, Some(15_600), None)]).unwrap_err().reason, check::NODE_SHAPE);
    // A pod that never fit is not made worse by a reduction.
    let mut big = value("job.yaml");
    big["spec"]["template"]["spec"]["containers"][0]["resources"]["requests"]["cpu"] = json!("32");
    assert_eq!(check_with(&s, &admission(big, "ml"), &[decision(BUILD, Some(24_000), None)]), Ok(()));
}

fn autopilot(bursting: bool) -> K8sSettings {
    K8sSettings { autopilot: Some(AutopilotSettings { bursting, ..Default::default() }), ..settings() }
}

#[test]
fn rejects_what_autopilot_would_change() {
    let s = autopilot(true);
    let n = job_admission();
    // 4 CPU with 1Gi: below the 1 GiB/vCPU ratio, Autopilot would raise memory.
    let r = check_with(&s, &n, &[decision(BUILD, None, Some(1024))]).unwrap_err();
    assert_eq!(r.reason, check::AUTOPILOT);
    // Above the general-purpose maximum.
    assert_eq!(check_with(&s, &n, &[decision(BUILD, Some(31_000), Some(100 * 1024))]).unwrap_err().reason, check::AUTOPILOT);
    assert_eq!(check_with(&s, &n, &[decision(BUILD, Some(3000), Some(12 * 1024))]), Ok(()));
    // Non-bursting: limits = requests applies to the original too, so it is no reason to reject.
    let nb = autopilot(false);
    assert_eq!(check_with(&nb, &n, &[decision(BUILD, Some(3000), Some(12 * 1024))]), Ok(()));
    assert_eq!(check_with(&nb, &n, &[decision(BUILD, Some(3100), Some(12 * 1024))]).unwrap_err().reason, check::AUTOPILOT);
}

#[test]
fn finalize_normalizes_before_the_decision_is_final() {
    let nb = autopilot(false);
    let a = K8sAdapter::new(nb.clone()).with_keys(keys());
    let env = nb.to_env(None);
    // A reduction Autopilot would undo is held at the prior.
    let small = admission(json!({"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "tiny"}, "spec": {"containers": [
        {"name": "c", "image": "x", "resources": {"requests": {"cpu": "250m", "memory": "1Gi"}}}]}}), "ml");
    let f = a.finalize(&small, &[decision("spec.containers[c]", Some(100), Some(1024))], &env).unwrap();
    assert_eq!(f[0].spec.requests.cpu, Some(MilliCpu(250)));
    assert!(f[0].holds.iter().any(|h| h.axis == Axis::Cpu && h.reason == HoldReason::Platform));
    // A change Autopilot adjusts (0.25 vCPU rounding) is described as what will run.
    let n = job_admission();
    let f = a.finalize(&n, &[decision(BUILD, Some(2100), Some(12 * 1024))], &env).unwrap();
    assert_eq!(f[0].spec.requests.cpu, Some(MilliCpu(2250)));
    assert!(f[0].holds.is_empty());
    assert_eq!(f[0].spec.limits.memory, Some(Bytes::gib(12)));
    // The finalized decision then validates.
    let d: Vec<_> = f.iter().map(|x| (x.unit.clone(), x.spec.clone())).collect();
    let p = a.render_with(&n, &RenderRequest::enforce(&d)).unwrap().patch;
    assert_eq!(a.validate_at(&n, &p, &env, t0()), Ok(()));
    // An increase above a GPU pod's maximum is held.
    let gpu = admission(json!({"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "g"}, "spec": {
        "nodeSelector": {"cloud.google.com/gke-accelerator": "nvidia-tesla-a100"},
        "containers": [{"name": "c", "image": "x", "resources": {"requests": {"cpu": "8", "memory": "40Gi"}, "limits": {"cpu": "8", "memory": "40Gi", "nvidia.com/gpu": 1}}}]}}), "ml");
    let f = a.finalize(&gpu, &[decision("spec.containers[c]", Some(12_000), Some(40 * 1024))], &env).unwrap();
    assert_eq!(f[0].spec.requests.cpu, Some(MilliCpu(8000)));
    assert!(f[0].holds.iter().any(|h| h.axis == Axis::Cpu));
    // Without Autopilot finalize is just the S2 target.
    let plain = adapter().finalize(&n, &[decision(BUILD, Some(2100), Some(12 * 1024))], &common::env()).unwrap();
    assert_eq!(plain[0].spec.requests.cpu, Some(MilliCpu(2100)));
    assert_eq!(plain[0].spec.limits.memory, Some(Bytes::gib(24)));
}

#[test]
fn malformed_patches_are_rejected() {
    let a = adapter();
    let n = job_admission();
    let bad = Patch::JsonPatch { ops: vec![rightsizer_plugin::JsonPatchOp::Remove { path: "/spec/does/not/exist".into() }] };
    assert!(a.validate_at(&n, &bad, &env(), t0()).is_err());
    assert_eq!(a.validate_at(&n, &Patch::None, &env(), t0()), Ok(()));
}

/// A ledger shared by `peers` replicas whose reservations reach the others
/// only after a refresh: the ones not yet seen elsewhere are `unsynced`.
struct PeerLedger {
    peers: u64,
    unsynced: rightsizer_k8s::resources::Totals,
}

impl rightsizer_k8s::ReservationLedger for PeerLedger {
    fn reserve(&self, _r: rightsizer_k8s::Reservation) {}
    fn release(&self, _namespace: &str, _id: &str) {}
    fn reserved(&self, _namespace: &str, _now: DateTime<Utc>) -> (rightsizer_k8s::resources::Totals, rightsizer_k8s::resources::Totals) {
        (self.unsynced.clone(), Default::default())
    }
    fn unsynced(&self, _namespace: &str, _now: DateTime<Utc>) -> (rightsizer_k8s::resources::Totals, rightsizer_k8s::resources::Totals) {
        (self.unsynced.clone(), Default::default())
    }
    fn peers(&self) -> u64 {
        self.peers
    }
}

/// Two webhook replicas can each admit an increase before either sees the
/// other's reservation (the shared ledger syncs through the decision log).
/// Each replica may then use only its share of the remaining headroom for
/// increases that are not yet visible to the others.
#[test]
fn concurrent_replicas_split_the_quota_headroom() {
    let s = K8sSettings { resource_quotas: vec![quota("100Gi", "60Gi")], ..settings() };
    let env: AdapterEnv = s.to_env(None);
    // 40Gi remaining; the Job's pod asks 16Gi → 30Gi (+14Gi).
    let n = job_admission();
    let d = [decision(BUILD, None, Some(30 * 1024))];
    let single = K8sAdapter::new(s.clone()).with_keys(keys()).with_ledger(Arc::new(PeerLedger { peers: 1, unsynced: Default::default() }));
    let p = single.render_with(&n, &RenderRequest::enforce(&d)).unwrap().patch;
    assert_eq!(single.validate_at(&n, &p, &env, t0()), Ok(()));
    // Two replicas, nothing pending: +14Gi > (40 − 16) / 2 = 12Gi: rejected.
    let two = K8sAdapter::new(s.clone()).with_keys(keys()).with_ledger(Arc::new(PeerLedger { peers: 2, unsynced: Default::default() }));
    assert_eq!(two.validate_at(&n, &p, &env, t0()).unwrap_err().reason, check::RESOURCE_QUOTA);
    // A smaller increase within the share is fine: +8Gi ≤ 12Gi.
    let small = [decision(BUILD, None, Some(24 * 1024))];
    let ps = two.render_with(&n, &RenderRequest::enforce(&small)).unwrap().patch;
    assert_eq!(two.validate_at(&n, &ps, &env, t0()), Ok(()));
    // ... unless this replica already has +5Gi not yet visible to the other one
    // (5 + 8 > 12; the old check passed: 24 ≤ 40 − 5).
    let busy = K8sAdapter::new(s.clone())
        .with_keys(keys())
        .with_ledger(Arc::new(PeerLedger { peers: 2, unsynced: rightsizer_k8s::resources::Totals { memory: Bytes::gib(5).0, ..Default::default() } }));
    assert_eq!(busy.validate_at(&n, &ps, &env, t0()).unwrap_err().reason, check::RESOURCE_QUOTA);
}
