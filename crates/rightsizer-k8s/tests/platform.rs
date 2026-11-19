//! PlatformModel: PlatformRef per unit, PlatformState from config and
//! cluster inputs, quota minus the ledger; and the webhook's mode function.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::Utc;
use common::*;
use rightsizer_core::{AcceleratorId, Bytes, MilliCpu, Mode, NodeShape, OverheadModel, Tenant};
use rightsizer_k8s::ledger::Reservation;
use rightsizer_k8s::resources::Totals;
use rightsizer_k8s::{K8sAdapter, K8sSettings, MemoryLedger, ReservationLedger};
use rightsizer_plugin::PlatformModel;
use serde_json::json;

#[test]
fn platform_ref_per_unit() {
    let mut s = settings();
    s.cpu_window_s = 30;
    s.overhead.insert("ghcr.io/acme/".into(), OverheadModel { memory_mib: vec![(0.5, 300.0), (0.9, 600.0)], cpu_millis: 0 });
    let a = K8sAdapter::new(s.clone()).with_keys(keys());
    let e = extract(&a, &doc("job.yaml"), &env_with(&s));
    let build = e.units.iter().find(|u| u.id.0.ends_with("containers[build]")).unwrap();
    let r = a.reference(build);
    assert_eq!(r.kind, "k8s");
    assert_eq!(r.retry.same_size, 2);
    assert!(r.retry.ladder.is_none());
    assert_eq!(r.walltime_s, Some(7200));
    assert!(r.memory_enforced);
    assert_eq!(r.cpu_window_s, 30);
    assert_eq!(r.overhead.memory_mib.len(), 2);
    let stage = e.units.iter().find(|u| u.id.0.ends_with("initContainers[stage]")).unwrap();
    let r = a.reference(stage);
    assert!(!r.memory_enforced, "no memory limit → not enforced");
    assert!(r.overhead.memory_mib.is_empty(), "aws-cli image has no configured overhead");
    // Pods: no same-size retries; a Job without backoffLimit: 6.
    let e = extract(&a, &doc("pod-guaranteed.yaml"), &env_with(&s));
    assert_eq!(a.reference(&e.units[0]).retry.same_size, 0);
    let e = extract(&a, &doc("job-no-resources.yaml"), &env_with(&s));
    assert_eq!(a.reference(&e.units[0]).retry.same_size, 6);
}

#[test]
fn ladder_only_with_apply_watch() {
    let s = K8sSettings { apply_watch: true, ..settings() };
    let a = K8sAdapter::new(s.clone()).with_keys(keys());
    let e = extract(&a, &doc("job.yaml"), &env_with(&s));
    let r = a.reference(&e.units[1]);
    let l = r.retry.ladder.expect("ladder");
    assert_eq!(l.final_floor.memory, Some(Bytes::gib(16)));
    // Never on the admission path.
    let e = extract(&a, &admission(value("job.yaml"), "ml"), &env_with(&s));
    assert!(a.reference(&e.units[1]).retry.ladder.is_none());
}

#[test]
fn state_from_shapes_nodes_and_quota() {
    let mut prices = BTreeMap::new();
    prices.insert("a2-highgpu-1g".to_string(), 3.67);
    let s = K8sSettings {
        shapes: vec![NodeShape { name: Some("m6i.2xlarge".into()), cpu: MilliCpu(7910), memory: Bytes::gib(30), gpu: None, price_per_hour: 0.384, host_per_gpu: None }],
        prices,
        nodes: vec![
            json!({"metadata": {"name": "g1", "labels": {"cloud.google.com/gke-accelerator": "nvidia-tesla-a100", "node.kubernetes.io/instance-type": "a2-highgpu-1g"}},
                   "status": {"allocatable": {"cpu": "11", "memory": "80Gi", "nvidia.com/gpu": "1"}}}),
            json!({"metadata": {"name": "g2", "labels": {"cloud.google.com/gke-accelerator": "nvidia-tesla-a100", "node.kubernetes.io/instance-type": "a2-highgpu-1g"}},
                   "status": {"allocatable": {"cpu": "11", "memory": "80Gi", "nvidia.com/gpu": "1"}}}),
            json!({"metadata": {"name": "c1", "labels": {}}, "status": {"allocatable": {"cpu": "3920m", "memory": "15Gi"}}}),
        ],
        resource_quotas: vec![json!({"metadata": {"name": "q", "namespace": "ml"}, "status": {
            "hard": {"requests.cpu": "64", "requests.memory": "256Gi", "requests.nvidia.com/gpu": "4"},
            "used": {"requests.cpu": "16", "requests.memory": "64Gi", "requests.nvidia.com/gpu": "1"}}})],
        ..settings()
    };
    let ledger = Arc::new(MemoryLedger::default());
    ledger.reserve(Reservation { namespace: "ml".into(), id: "x".into(), requests: Totals { cpu: 8000, memory: 32 << 30, ..Default::default() }, limits: Totals::default(), at: Utc::now() });
    let a = K8sAdapter::new(s.clone()).with_ledger(ledger);
    let st = block(a.state(&Tenant::new("prod/ml").unwrap(), &env_with(&s))).unwrap();
    assert_eq!(st.shapes.len(), 3, "duplicate nodes collapse: {:?}", st.shapes);
    let g = st.shapes.iter().find(|x| x.gpu.is_some()).unwrap();
    assert_eq!(g.gpu.unwrap().accel, AcceleratorId::A100_40G);
    assert_eq!(g.price_per_hour, 3.67);
    let c = st.shapes.iter().find(|x| x.cpu == MilliCpu(3920)).unwrap();
    assert!(c.price_per_hour > 0.1 && c.price_per_hour < 0.3, "fallback price {}", c.price_per_hour);
    let q = st.quota.unwrap();
    assert_eq!(q.cpu, Some(MilliCpu(40_000)));
    assert_eq!(q.memory, Some(Bytes::gib(160)));
    assert_eq!(q.gpu_count, Some(3));
    assert_eq!(a.accelerators().canonical("nvidia-l4"), AcceleratorId::L4_24G);
}

#[test]
fn mode_from_namespace_and_object() {
    let mut s = settings();
    s.namespace_labels.insert("rightsizer.io/mode".into(), "enforce".into());
    let a = K8sAdapter::new(s.clone());
    assert_eq!(a.mode(&admission(value("job.yaml"), "ml"), &env_with(&s)).unwrap(), Mode::Enforce);
    let mut j = value("job.yaml");
    j["metadata"]["labels"]["rightsizer.io/mode"] = json!("observe");
    assert_eq!(a.mode(&admission(j, "ml"), &env_with(&s)).unwrap(), Mode::Observe);
    assert_eq!(a.mode(&admission(value("job.yaml"), "ml"), &env()).unwrap(), Mode::Recommend);
    // Settings in the env override static settings.
    let mut e = env();
    e.settings.insert("namespace_labels".into(), json!({"rightsizer.io/mode": "observe"}));
    assert_eq!(a.mode(&admission(value("job.yaml"), "ml"), &e).unwrap(), Mode::Observe);
    let _: &dyn ReservationLedger = &MemoryLedger::default();
}
