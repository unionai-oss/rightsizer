//! extract: every kind, keys, skip rules, distributed detection, priors.

mod common;

use common::*;
use rightsizer_core::{AcceleratorId, Axis, Bytes, HoldReason, MilliCpu, QosClass, ScalarValue};
use rightsizer_k8s::K8sSettings;
use rightsizer_plugin::{Extracted, SkipReason, Unit};
use serde_json::json;

fn unit<'a>(e: &'a Extracted, id: &str) -> &'a Unit {
    e.units.iter().find(|u| u.id.0 == id).unwrap_or_else(|| panic!("no unit {id}; have {:?}", e.units.iter().map(|u| &u.id.0).collect::<Vec<_>>()))
}

fn suffixes(e: &Extracted) -> Vec<String> {
    e.units.iter().map(|u| u.key_suffix.clone()).collect()
}

fn extra<'a>(u: &'a Unit, k: &str) -> Option<&'a ScalarValue> {
    u.seed.manifest.extra.get(k)
}

#[test]
fn job_units_keys_prior_and_seed() {
    let e = extract(&adapter(), &doc("job.yaml"), &env());
    assert_eq!(e.tenant.as_str(), "prod/ml");
    assert!(e.skip.is_none());
    assert_eq!(suffixes(&e), vec!["job/feature-build/stage", "job/feature-build/build"]);
    let b = unit(&e, "spec.template.spec.containers[build]");
    assert_eq!(b.prior.spec.requests.cpu, Some(MilliCpu(4000)));
    assert_eq!(b.prior.spec.requests.memory, Some(Bytes::gib(16)));
    assert_eq!(b.prior.spec.limits.memory, Some(Bytes::gib(32)));
    assert_eq!(b.prior.qos, Some(QosClass::Burstable));
    assert!(b.prior.defaulted.is_empty());
    assert_eq!(b.group.as_ref().unwrap().0, "spec.template.spec.containers[build]");
    assert!(!b.distributed);
    assert_eq!(extra(b, "backoff_limit"), Some(&ScalarValue::Int(2)));
    assert_eq!(extra(b, "active_deadline_seconds"), Some(&ScalarValue::Int(7200)));
    assert_eq!(extra(b, "staging"), Some(&ScalarValue::Str("init-tmpfs".into())));
    assert_eq!(extra(b, "memory_limit"), Some(&ScalarValue::Bool(true)));
    assert_eq!(b.seed.manifest.memory_backed_bytes, Some(Bytes::gib(8)));
    assert_eq!(b.seed.manifest.init_containers, 1);
    assert_eq!(b.seed.inputs.len(), 1);
    assert_eq!(b.seed.inputs[0].uri.as_deref(), Some("s3://ml-data/features/2026-11-17.parquet"));
    assert!(matches!(&b.seed.code, Some(rightsizer_core::CodeRef::Info { info }) if info.digest.len() == 64));
    assert_eq!(b.seed.labels.get("team").map(String::as_str), Some("ranking"));
    let s = unit(&e, "spec.template.spec.initContainers[stage]");
    assert_eq!(extra(s, "role"), Some(&ScalarValue::Str("init".into())));
    assert!(s.prior.spec.limits.memory.is_none());
    // Same image + args → same digest; a different arg → a different digest.
    let e2 = extract(&adapter(), &doc_text(&fixture("job.yaml").replace("--workers=8", "--workers=16")), &env());
    assert_ne!(unit(&e2, "spec.template.spec.containers[build]").seed.code, b.seed.code);
}

#[test]
fn bare_pod_with_init_and_sidecar() {
    let e = extract(&adapter(), &doc("pod.yaml"), &env());
    assert_eq!(suffixes(&e), vec!["pod/notebook-export/fetch", "pod/notebook-export/proxy", "pod/notebook-export/export"]);
    let p = unit(&e, "spec.initContainers[proxy]");
    assert_eq!(extra(p, "role"), Some(&ScalarValue::Str("sidecar".into())));
    assert_eq!(p.seed.manifest.sidecars, 1);
    assert!(unit(&e, "spec.containers[export]").group.is_none());
    assert_eq!(unit(&e, "spec.containers[export]").prior.qos, Some(QosClass::Burstable));
    assert_eq!(extra(unit(&e, "spec.containers[export]"), "backoff_limit"), Some(&ScalarValue::Int(0)));
}

#[test]
fn generated_names_are_stripped() {
    let e = extract(&adapter(), &doc("pod-guaranteed.yaml"), &env());
    assert_eq!(suffixes(&e), vec!["pod/embed-batch/embed"]);
    assert_eq!(e.units[0].prior.qos, Some(QosClass::Guaranteed));
    let gen_job = fixture("job.yaml").replace("name: feature-build\n", "generateName: feature-build-\n");
    let e = extract(&adapter(), &doc_text(&gen_job), &env());
    assert_eq!(suffixes(&e)[1], "job/feature-build/build");
}

#[test]
fn cronjob_and_its_jobs_share_the_cronjob_key() {
    let e = extract(&adapter(), &doc("cronjob.yaml"), &env());
    assert_eq!(suffixes(&e), vec!["cronjob/nightly-retrain/retrain"]);
    assert_eq!(e.units[0].id.0, "spec.jobTemplate.spec.template.spec.containers[retrain]");
    assert_eq!(extra(&e.units[0], "backoff_limit"), Some(&ScalarValue::Int(1)));
    let child = extract(&adapter(), &doc("cron-child-job.yaml"), &env());
    assert_eq!(suffixes(&child), vec!["cronjob/nightly-retrain/retrain"]);
    // A CronJob job without the owner reference still strips the schedule suffix.
    let manual = fixture("cron-child-job.yaml");
    let no_owner: String = manual.split("  ownerReferences:").next().unwrap().to_string() + &manual[manual.find("spec:\n  backoffLimit").unwrap()..];
    let e = extract(&adapter(), &doc_text(&no_owner), &env());
    assert_eq!(suffixes(&e), vec!["job/nightly-retrain/retrain"]);
}

#[test]
fn jobset_groups_gpu_and_queue() {
    let e = extract(&adapter(), &doc("jobset.yaml"), &env());
    assert_eq!(suffixes(&e), vec!["jobset/llm-pretrain/driver/driver", "jobset/llm-pretrain/workers/trainer"]);
    let t = unit(&e, "spec.replicatedJobs[workers].template.spec.template.spec.containers[trainer]");
    assert!(t.distributed);
    assert!(t.seed.manifest.distributed_markers.contains(&"torchrun".to_string()));
    assert!(t.seed.manifest.distributed_markers.contains(&"nproc_per_node".to_string()));
    assert_eq!(t.seed.manifest.launcher.as_deref(), Some("torchrun"));
    assert_eq!(extra(t, "replicas"), Some(&ScalarValue::Int(8)));
    let g = t.prior.spec.requests.gpu.unwrap();
    assert_eq!((g.accel, g.count), (AcceleratorId::H100_80G, 8));
    assert!(t.constraints.held.iter().any(|h| h.axis == Axis::GpuType && h.reason == HoldReason::Platform));
    assert_eq!(extra(t, "queue_managed"), Some(&ScalarValue::Bool(true)));
    assert_eq!(extra(t, "backoff_limit"), Some(&ScalarValue::Int(3)));
}

#[test]
fn pytorchjob_replica_types() {
    let e = extract(&adapter(), &doc("pytorchjob.yaml"), &env());
    assert_eq!(suffixes(&e), vec!["pytorchjob/resnet-ddp/Master/pytorch", "pytorchjob/resnet-ddp/Worker/pytorch"]);
    assert!(e.units.iter().all(|u| u.distributed));
    let w = unit(&e, "spec.pytorchReplicaSpecs.Worker.template.spec.containers[pytorch]");
    assert_eq!(w.prior.spec.requests.gpu.unwrap().accel, AcceleratorId::A100_80G);
    let m = unit(&e, "spec.pytorchReplicaSpecs.Master.template.spec.containers[pytorch]");
    assert_eq!(m.prior.spec.requests.gpu.unwrap().accel, AcceleratorId::UnknownGpu);
    assert_eq!(extra(m, "backoff_limit"), Some(&ScalarValue::Int(2)));
}

#[test]
fn trainjob_trainer_unit() {
    let e = extract(&adapter(), &doc("trainjob.yaml"), &env());
    assert_eq!(suffixes(&e), vec!["trainjob/llama-sft/trainer"]);
    let u = &e.units[0];
    assert_eq!(u.id.0, "spec.trainer.resourcesPerNode");
    assert!(u.distributed);
    assert_eq!(u.prior.spec.requests.gpu.unwrap().count, 4);
    assert_eq!(u.prior.spec.requests.memory, Some(Bytes::gib(256)));
}

#[test]
fn ray_groups_and_params() {
    let e = extract(&adapter(), &doc("raycluster.yaml"), &env());
    assert_eq!(suffixes(&e), vec!["ray/batch-infer/head", "ray/batch-infer/cpu-workers"]);
    let w = unit(&e, "spec.workerGroupSpecs[cpu-workers].template.spec.containers[ray-worker]");
    assert!(w.distributed);
    assert_eq!(extra(w, "ray_num_cpus"), Some(&ScalarValue::Str("8".into())));
    let h = unit(&e, "spec.headGroupSpec.template.spec.containers[ray-head]");
    assert!(!h.distributed);
    let j = extract(&adapter(), &doc("rayjob.yaml"), &env());
    assert_eq!(suffixes(&j), vec!["ray/tune-sweep/head", "ray/tune-sweep/gpu"]);
    // Requests default to limits.
    let g = unit(&j, "spec.rayClusterSpec.workerGroupSpecs[gpu].template.spec.containers[ray-worker]");
    assert_eq!(g.prior.spec.requests.memory, Some(Bytes::gib(30)));
    assert_eq!(extra(g, "backoff_limit"), Some(&ScalarValue::Int(1)));
}

#[test]
fn volcano_tasks() {
    let e = extract(&adapter(), &doc("volcano-job.yaml"), &env());
    assert_eq!(suffixes(&e), vec!["volcano/mpi-bench/launcher", "volcano/mpi-bench/worker", "volcano/mpi-bench/worker/metrics"]);
    assert_eq!(extra(&e.units[1], "backoff_limit"), Some(&ScalarValue::Int(2)));
    assert_eq!(extra(&e.units[1], "queue_managed"), Some(&ScalarValue::Bool(true)));
}

#[test]
fn multi_document_ids() {
    let e = extract(&adapter(), &doc("multi.yaml"), &env());
    let ids: Vec<&str> = e.units.iter().map(|u| u.id.0.as_str()).collect();
    assert_eq!(ids, vec!["1:spec.template.spec.containers[train]", "2:spec.template.spec.containers[train]"]);
    assert_eq!(suffixes(&e), vec!["job/train-small/train", "job/train-large/train"]);
    // Mixed namespaces need extract_documents.
    let mixed = fixture("multi.yaml").replacen("name: train-large\n  namespace: ml", "name: train-large\n  namespace: other", 1);
    assert!(block(rightsizer_plugin::Adapter::extract(&adapter(), &doc_text(&mixed), &env())).is_err());
    let per = adapter().extract_documents(&doc_text(&mixed), &env()).unwrap();
    assert_eq!(per[2].tenant.as_str(), "prod/other");
}

#[test]
fn pod_level_resources_are_the_unit() {
    let e = extract(&adapter(), &doc("pod-level.yaml"), &env());
    assert_eq!(e.units.len(), 1);
    assert_eq!(e.units[0].id.0, "spec.resources");
    assert_eq!(e.units[0].key_suffix, "pod/pod-level/@pod");
    assert_eq!(e.units[0].prior.spec.requests.cpu, Some(MilliCpu(2000)));
}

#[test]
fn workload_key_annotation_sets_only_the_suffix() {
    let t = fixture("job.yaml").replace("  annotations:\n", "  annotations:\n    rightsizer.io/workload-key: ranking/features\n");
    let e = extract(&adapter(), &doc_text(&t), &env());
    assert_eq!(suffixes(&e)[1], "ranking/features/build");
    assert_eq!(e.tenant.as_str(), "prod/ml");
}

#[test]
fn tenant_comes_from_the_admission_request() {
    let mut o = value("pod-guaranteed.yaml");
    o["metadata"]["namespace"] = json!("someone-elses-ns");
    let e = extract(&adapter(), &admission(o, "ml"), &env());
    assert_eq!(e.tenant.as_str(), "prod/ml");
    let mut s = settings();
    s.cluster = None;
    assert!(block(rightsizer_plugin::Adapter::extract(&rightsizer_k8s::K8sAdapter::new(s.clone()), &doc("job.yaml"), &s.to_env(None))).is_err());
}

#[test]
fn flyte_pods_keyed_only_in_their_namespace() {
    // The org-less default template does not bind the org label: the org is configured.
    let mut fs = settings();
    fs.flyte.default_org = Some("acme".into());
    let n = rightsizer_plugin::Native::Object { value: serde_json::from_str(&fixture("flyte-pod.json")).unwrap() };
    assert!(extract(&adapter(), &n, &env()).units[0].key.is_none(), "no configured org: not Flyte-keyed");
    let e = extract(&adapter(), &n, &env_with(&fs));
    let k = e.units[0].key.as_ref().expect("flyte key");
    assert_eq!(k.to_string(), "flyte://acme/flytesnacks/development/workflows.example.train");
    assert_eq!(e.units[0].workload_key("k8s", &e.tenant).unwrap(), *k);
    // Forged labels in another namespace: a bare pod keyed k8s://.
    let mut v: serde_json::Value = serde_json::from_str(&fixture("flyte-pod.json")).unwrap();
    v["request"]["namespace"] = json!("team-b");
    let e = extract(&adapter(), &rightsizer_plugin::Native::Object { value: v }, &env());
    assert!(e.units[0].key.is_none());
    assert_eq!(e.units[0].workload_key("k8s", &e.tenant).unwrap().to_string(), "k8s://prod/team-b/pod/a7f8k2l9m3n4-n0-0/a7f8k2l9m3n4-n0-0");
    // A custom namespace template.
    let mut s = fs.clone();
    s.flyte.namespace_template = "flyte-{project}".into();
    let mut v: serde_json::Value = serde_json::from_str(&fixture("flyte-pod.json")).unwrap();
    v["request"]["namespace"] = json!("flyte-flytesnacks");
    let e = extract(&adapter(), &rightsizer_plugin::Native::Object { value: v }, &env_with(&s));
    assert!(e.units[0].key.is_some());
}

fn skip_of(n: &rightsizer_plugin::Native) -> SkipReason {
    let e = extract(&adapter(), n, &env());
    assert!(e.units.is_empty());
    e.skip.expect("skip")
}

#[test]
fn skip_rules() {
    let pod = value("pod-guaranteed.yaml");
    let with = |f: &dyn Fn(&mut serde_json::Value)| {
        let mut p = pod.clone();
        f(&mut p);
        admission(p, "ml")
    };
    // Owned by a Job: sized at the template.
    let owned = with(&|p| p["metadata"]["ownerReferences"] = json!([{"kind": "Job", "name": "j", "uid": "1", "controller": true}]));
    assert!(matches!(skip_of(&owned), SkipReason::AlreadySized { .. }));
    // Owned by a ReplicaSet: not a bare pod.
    let rs = with(&|p| p["metadata"]["ownerReferences"] = json!([{"kind": "ReplicaSet", "name": "r", "uid": "1"}]));
    assert!(matches!(skip_of(&rs), SkipReason::Unsupported { .. }));
    // Kueue.
    let kq = with(&|p| p["metadata"]["labels"] = json!({"kueue.x-k8s.io/queue-name": "q"}));
    assert_eq!(skip_of(&kq), SkipReason::QueueManaged);
    // Another layer's decision id.
    let sized = with(&|p| p["metadata"]["annotations"] = json!({"rightsizer.io/decision-id": "01JD000000000000000000000"}));
    assert_eq!(skip_of(&sized), SkipReason::AlreadySized { decision_id: Some("01JD000000000000000000000".into()) });
    // The Flyte integration's container marker.
    let flyte = with(&|p| p["spec"]["containers"][0]["env"] = json!([{"name": "RIGHTSIZER_DECISION", "value": "{\"v\":1,\"decision_id\":\"01JDFLYTE\"}"}]));
    assert_eq!(skip_of(&flyte), SkipReason::AlreadySized { decision_id: Some("01JDFLYTE".into()) });
    // Opted out.
    let off = with(&|p| p["metadata"]["annotations"] = json!({"rightsizer.io/mode": "off"}));
    assert_eq!(skip_of(&off), SkipReason::OptedOut);
    // Not ours.
    let dep = admission(json!({"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "d"}}), "ml");
    assert!(matches!(skip_of(&dep), SkipReason::Unsupported { .. }));
    // Not CREATE.
    let mut upd = match admission(pod.clone(), "ml") {
        rightsizer_plugin::Native::Object { value } => value,
        _ => unreachable!(),
    };
    upd["request"]["operation"] = json!("UPDATE");
    assert!(matches!(skip_of(&rightsizer_plugin::Native::Object { value: upd }), SkipReason::Unsupported { .. }));
    // A Job created by a JobSet.
    let mut j = value("job.yaml");
    j["metadata"]["ownerReferences"] = json!([{"kind": "JobSet", "name": "js", "uid": "1", "controller": true}]);
    assert!(matches!(skip_of(&admission(j, "ml")), SkipReason::AlreadySized { .. }));
    // No resource-bearing units.
    let empty = admission(json!({"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "e"}, "spec": {"containers": []}}), "ml");
    assert_eq!(skip_of(&empty), SkipReason::NoUnits);
}

#[test]
fn reinvocation_resumes_only_unsized_units() {
    let a = adapter();
    let n = admission(value("pod-guaranteed.yaml"), "ml");
    let p = render(&a, &n, &[decision("spec.containers[embed]", Some(2000), Some(8192))]);
    let mut sized = object_of(&applied(&n, &p));
    // A mesh injects a sidecar after us.
    sized["spec"]["containers"].as_array_mut().unwrap().push(json!({"name": "istio-proxy", "image": "istio/proxyv2", "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}}}));
    let e = extract(&a, &admission(sized.clone(), "ml"), &env());
    assert_eq!(e.units.len(), 1);
    assert_eq!(e.units[0].id.0, "spec.containers[istio-proxy]");
    // Without the injected container everything is sized.
    sized["spec"]["containers"].as_array_mut().unwrap().pop();
    assert!(matches!(skip_of(&admission(sized.clone(), "ml")), SkipReason::AlreadySized { decision_id: Some(_) }));
    // extract_all sees the sized units with their origin prior.
    let all = a.extract_all(&admission(sized, "ml"), &env()).unwrap();
    // (The origin is the author's 4 CPUs, not the applied 2: attribution needs it.)
    assert_eq!(all[0].units[0].prior.spec.requests.cpu, Some(MilliCpu(4000)));
}

#[test]
fn distributed_markers() {
    let base = value("job.yaml");
    let cases = [
        json!([{"name": "WORLD_SIZE", "value": "4"}]),
        json!([{"name": "LAUNCH", "value": "deepspeed --num_gpus 8 train.py"}]),
        json!([{"name": "CMD", "value": "accelerate launch train.py"}]),
    ];
    for env_list in cases {
        let mut j = base.clone();
        j["spec"]["template"]["spec"]["containers"][0]["env"] = env_list.clone();
        let e = extract(&adapter(), &admission(j, "ml"), &env());
        assert!(unit(&e, "spec.template.spec.containers[build]").distributed, "{env_list}");
    }
    let mut j = base.clone();
    j["spec"]["template"]["spec"]["containers"][0]["args"] = json!(["--nproc_per_node=4"]);
    assert!(extract(&adapter(), &admission(j, "ml"), &env()).units[1].distributed);
    let mut j = base.clone();
    j["spec"]["parallelism"] = json!(4);
    assert!(extract(&adapter(), &admission(j, "ml"), &env()).units[1].distributed);
    let mut j = base;
    j["spec"]["parallelism"] = json!(4);
    j["spec"]["completions"] = json!(1);
    assert!(!extract(&adapter(), &admission(j, "ml"), &env()).units[1].distributed);
}

fn limit_range() -> serde_json::Value {
    json!({"apiVersion": "v1", "kind": "LimitRange", "metadata": {"name": "defaults", "namespace": "ml"},
           "spec": {"limits": [{"type": "Container", "default": {"memory": "4Gi", "cpu": "2"}, "defaultRequest": {"memory": "1Gi", "cpu": "500m"}}]}})
}

#[test]
fn limitrange_simulation_for_templates() {
    let s = K8sSettings { limit_ranges: vec![limit_range()], ..settings() };
    let e = extract(&adapter(), &doc("job-no-resources.yaml"), &env_with(&s));
    let u = unit(&e, "spec.template.spec.containers[eval]");
    assert_eq!(u.prior.spec.requests.memory, Some(Bytes::gib(1)));
    assert_eq!(u.prior.spec.requests.cpu, Some(MilliCpu(500)));
    assert_eq!(u.prior.spec.limits.memory, Some(Bytes::gib(4)));
    assert!(u.prior.defaulted.contains(Axis::Memory) && u.prior.defaulted.contains(Axis::Cpu));
    assert_eq!(extra(u, "memory_limit"), Some(&ScalarValue::Bool(true)));
    // Without a LimitRange the unit has no prior.
    let e = extract(&adapter(), &doc("job-no-resources.yaml"), &env());
    let u = unit(&e, "spec.template.spec.containers[eval]");
    assert!(u.prior.spec.requests.memory.is_none() && u.prior.defaulted.is_empty());
    // A user value is not defaulted.
    let e = extract(&adapter(), &doc("job.yaml"), &env_with(&s));
    let b = unit(&e, "spec.template.spec.containers[build]");
    assert!(b.prior.defaulted.is_empty());
    // The CPU limit came from the LimitRange default.
    assert_eq!(b.prior.spec.limits.cpu, Some(MilliCpu(2000)));
}

#[test]
fn limitrange_defaults_inferred_for_pods() {
    let s = K8sSettings { limit_ranges: vec![limit_range()], ..settings() };
    let pod = json!({"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "p"}, "spec": {"containers": [
        {"name": "defaulted", "image": "x", "resources": {"requests": {"cpu": "500m", "memory": "1Gi"}, "limits": {"cpu": "2", "memory": "4Gi"}}},
        {"name": "explicit", "image": "x", "resources": {"requests": {"cpu": "1", "memory": "3Gi"}, "limits": {"cpu": "2", "memory": "4Gi"}}}]}});
    let e = extract(&adapter(), &admission(pod, "ml"), &env_with(&s));
    let d = unit(&e, "spec.containers[defaulted]");
    assert!(d.prior.defaulted.contains(Axis::Cpu) && d.prior.defaulted.contains(Axis::Memory));
    let x = unit(&e, "spec.containers[explicit]");
    assert!(x.prior.defaulted.is_empty());
}

#[test]
fn shm_and_extra_accelerator_names() {
    let mut j = value("job.yaml");
    j["spec"]["template"]["spec"]["volumes"].as_array_mut().unwrap().push(json!({"name": "dshm", "emptyDir": {"medium": "Memory", "sizeLimit": "2Gi"}}));
    j["spec"]["template"]["spec"]["containers"][0]["volumeMounts"].as_array_mut().unwrap().push(json!({"name": "dshm", "mountPath": "/dev/shm"}));
    j["spec"]["template"]["spec"]["nodeSelector"] = json!({"nvidia.com/gpu.product": "NVIDIA-H100-NVL"});
    j["spec"]["template"]["spec"]["containers"][0]["resources"]["limits"]["nvidia.com/gpu"] = json!(1);
    let e = extract(&adapter(), &admission(j.clone(), "ml"), &env());
    let b = unit(&e, "spec.template.spec.containers[build]");
    assert_eq!(b.prior.spec.requests.shm, Some(Bytes::gib(2)));
    assert_eq!(b.prior.spec.requests.gpu.unwrap().accel, AcceleratorId::UnknownGpu);
    let mut s = settings();
    s.accelerators.insert("NVIDIA-H100-NVL".into(), "H100-80G".into());
    let a = rightsizer_k8s::K8sAdapter::new(s.clone()).with_keys(keys());
    let e = extract(&a, &admission(j, "ml"), &env_with(&s));
    assert_eq!(unit(&e, "spec.template.spec.containers[build]").prior.spec.requests.gpu.unwrap().accel, AcceleratorId::H100_80G);
}
