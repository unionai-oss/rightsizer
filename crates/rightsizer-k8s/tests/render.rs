//! render: minimal text edits (golden), JSON patch validity, idempotence,
//! S2, KubeRay consistency, annotation-only modes, GPU type changes.

mod common;

use common::*;
use rightsizer_core::{AcceleratorId, Axis, Bytes, GpuShape, MilliCpu, Mode, QosClass, UnitId};
use rightsizer_k8s::render::Output;
use rightsizer_k8s::{K8sAdapter, K8sSettings, RenderRequest, UnitRender};
use rightsizer_plugin::{JsonPatchOp, Native, Patch, SkipReason};
use serde_json::json;

const BUILD: &str = "spec.template.spec.containers[build]";
const FIXED_ID: &str = "01JDRSZ0000000000000000000";

fn req_fixed(units: &[(UnitId, rightsizer_core::ResourceSpec)]) -> RenderRequest {
    RenderRequest { decision_id: Some(FIXED_ID.into()), ..RenderRequest::enforce(units) }
}

fn changed_lines(before: &str, after: &str) -> Vec<String> {
    // Lines of `after` that are not in `before` (order-preserving diff by line).
    let b: Vec<&str> = before.lines().collect();
    after.lines().filter(|l| !b.contains(l)).map(str::to_string).collect()
}

#[test]
fn golden_job_only_resource_lines_change() {
    let a = adapter();
    let n = doc("job.yaml");
    let r = a.render_with(&n, &RenderRequest { annotate: false, ..req_fixed(&[decision(BUILD, Some(2000), Some(12 * 1024))]) }).unwrap();
    let out = text_of(&applied(&n, &r.patch));
    golden("job.minimal.yaml", &out);
    // Comments, quoting style and order are preserved; only these lines differ.
    assert_eq!(changed_lines(&fixture("job.yaml"), &out), vec![
        "              cpu: \"2\"        # tuned by hand in 2024",
        "              memory: '12Gi'",
        "              memory: 24Gi",
    ]);
    // With annotations: only resource lines and three annotation lines.
    let r = a.render_with(&n, &req_fixed(&[decision(BUILD, Some(2000), Some(12 * 1024))])).unwrap();
    let out = text_of(&applied(&n, &r.patch));
    golden("job.enforce.yaml", &out);
    let added = changed_lines(&fixture("job.yaml"), &out);
    assert_eq!(added.len(), 6, "{added:#?}");
    assert!(added[0].starts_with("    rightsizer.io/inputs") || added.iter().filter(|l| l.contains("rightsizer.io/")).count() == 3);
}

#[test]
fn golden_missing_resources_blocks_are_indented() {
    let a = adapter();
    let n = doc("job-no-resources.yaml");
    let r = a
        .render_with(&n, &RenderRequest {
            annotate: false,
            ..req_fixed(&[decision("spec.template.spec.containers[eval]", Some(1500), Some(3 * 1024)), decision("spec.template.spec.containers[logs]", Some(100), Some(64))])
        })
        .unwrap();
    let out = text_of(&applied(&n, &r.patch));
    golden("job-no-resources.yaml", &out);
    assert!(out.contains("                -   name: eval\n                    image: ghcr.io/acme/eval:1.4.2\n                    command: [python, -m, eval.run]\n                    resources:\n                        requests:\n                            cpu: 1500m\n                            memory: 3Gi\n"));
    assert!(out.contains("resources: {requests: {cpu: 100m, memory: 64Mi}}"));
    let v = object_of(&applied(&n, &r.patch));
    assert_eq!(v["spec"]["template"]["spec"]["containers"][0]["resources"], json!({"requests": {"cpu": "1500m", "memory": "3Gi"}}));
}

#[test]
fn golden_multi_document() {
    let a = adapter();
    let n = doc("multi.yaml");
    let r = a.render_with(&n, &RenderRequest { annotate: false, ..req_fixed(&[decision("2:spec.template.spec.containers[train]", Some(12000), Some(48 * 1024))]) }).unwrap();
    let out = text_of(&applied(&n, &r.patch));
    golden("multi.yaml", &out);
    assert_eq!(changed_lines(&fixture("multi.yaml"), &out), vec!["              cpu: 12", "              memory: 48Gi"]);
}

#[test]
fn idempotent_render_documents_and_objects() {
    let a = adapter();
    for n in [doc("job.yaml"), admission(value("job.yaml"), "ml")] {
        let d = [decision(BUILD, Some(2000), Some(12 * 1024))];
        let p1 = render(&a, &n, &d);
        assert!(!p1.is_none());
        let n1 = applied(&n, &p1);
        assert_eq!(render(&a, &n1, &d), Patch::None, "second render must be a no-op");
        // extract resumes only the unit that was not decided (`stage`).
        let e = extract(&a, &n1, &env());
        assert_eq!(e.units.iter().map(|u| u.id.0.as_str()).collect::<Vec<_>>(), vec!["spec.template.spec.initContainers[stage]"]);
        let p2 = render(&a, &n1, &[decision("spec.template.spec.initContainers[stage]", Some(250), None)]);
        let n2 = applied(&n1, &p2);
        assert!(matches!(extract(&a, &n2, &env()).skip, Some(SkipReason::AlreadySized { decision_id: Some(_) })));
        // The merged prior has both origin specs.
        let sp = rightsizer_k8s::prior::verify(object_of(&n2)["metadata"]["annotations"]["rightsizer.io/prior"].as_str().unwrap(), &keys()).unwrap();
        assert_eq!(sp.prior.len(), 2);
        assert_eq!(sp.prior[BUILD].requests.memory, Some(Bytes::gib(16)));
    }
    // Equal quantities in another spelling are not rewritten.
    let n = doc("job.yaml");
    let r = a.render_with(&n, &RenderRequest { annotate: false, ..req_fixed(&[decision(BUILD, Some(4000), Some(16 * 1024))]) }).unwrap();
    assert_eq!(r.patch, Patch::None);
}

#[test]
fn json_patch_is_valid_rfc6902() {
    let a = adapter();
    let n = admission(value("job.yaml"), "ml");
    let p = render(&a, &n, &[decision(BUILD, Some(2000), Some(12 * 1024))]);
    let Patch::JsonPatch { ops } = &p else { panic!("{p:?}") };
    assert!(ops.iter().any(|o| matches!(o, JsonPatchOp::Add { path, .. } if path == "/metadata/annotations/rightsizer.io~1prior")));
    assert!(ops.iter().any(|o| matches!(o, JsonPatchOp::Replace { path, .. } if path == "/spec/template/spec/containers/0/resources/requests/memory")));
    // Applied with an independent RFC 6902 implementation.
    let v = object_of(&applied(&n, &p));
    assert_eq!(v["spec"]["template"]["spec"]["containers"][0]["resources"], json!({"requests": {"cpu": "2", "memory": "12Gi"}, "limits": {"memory": "24Gi"}}));
    assert!(v["metadata"]["annotations"]["rightsizer.io/decision-id"].is_string());
    // Pods of the Job carry the marker.
    assert_eq!(v["spec"]["template"]["metadata"]["annotations"]["rightsizer.io/decision-id"], v["metadata"]["annotations"]["rightsizer.io/decision-id"]);
    // A pod template without metadata gets one added in a single op.
    let n = admission(value("job-no-resources.yaml"), "ml");
    let p = render(&a, &n, &[decision("spec.template.spec.containers[eval]", Some(1000), Some(2048))]);
    let v = object_of(&applied(&n, &p));
    assert!(v["spec"]["template"]["metadata"]["annotations"]["rightsizer.io/decision-id"].is_string());
    assert_eq!(v["spec"]["template"]["spec"]["containers"][0]["resources"]["requests"], json!({"cpu": "1", "memory": "2Gi"}));
}

#[test]
fn cronjob_children_carry_the_marker() {
    let a = adapter();
    let n = admission(value("cronjob.yaml"), "ml");
    let p = render(&a, &n, &[decision("spec.jobTemplate.spec.template.spec.containers[retrain]", Some(6000), Some(48 * 1024))]);
    let v = object_of(&applied(&n, &p));
    let id = v["metadata"]["annotations"]["rightsizer.io/decision-id"].clone();
    assert_eq!(v["spec"]["jobTemplate"]["metadata"]["annotations"]["rightsizer.io/decision-id"], id);
    // A Job the CronJob creates from this template is skipped.
    let mut job = value("cron-child-job.yaml");
    job["metadata"]["annotations"] = json!({"rightsizer.io/decision-id": id});
    let e = extract(&a, &admission(job, "ml"), &env());
    assert!(matches!(e.skip, Some(SkipReason::AlreadySized { .. })));
}

#[test]
fn s2_never_adds_limit() {
    let a = adapter();
    let n = admission(value("job.yaml"), "ml");
    // `stage` has no limits; `build` has a memory limit only.
    let p = render(&a, &n, &[decision("spec.template.spec.initContainers[stage]", Some(1000), Some(2048)), decision(BUILD, Some(8000), Some(40 * 1024))]);
    let v = object_of(&applied(&n, &p));
    let stage = &v["spec"]["template"]["spec"]["initContainers"][0]["resources"];
    assert!(stage.get("limits").is_none(), "{stage}");
    let build = &v["spec"]["template"]["spec"]["containers"][0]["resources"];
    assert!(build["limits"].get("cpu").is_none());
    // The memory limit keeps its 2× ratio (and is ≥ the request).
    assert_eq!(build["limits"]["memory"], json!("80Gi"));
}

#[test]
fn s2_guaranteed_stays_guaranteed() {
    let a = adapter();
    let n = admission(value("pod-guaranteed.yaml"), "ml");
    let p = render(&a, &n, &[decision("spec.containers[embed]", Some(2000), Some(10 * 1024))]);
    let v = object_of(&applied(&n, &p));
    let r = &v["spec"]["containers"][0]["resources"];
    assert_eq!(r["requests"], r["limits"]);
    assert_eq!(rightsizer_k8s::resources::pod_qos(&v["spec"]), QosClass::Guaranteed);
}

#[test]
fn s2_whole_cpus_kept() {
    let a = adapter();
    let n = admission(value("pod-guaranteed.yaml"), "ml");
    let p = render(&a, &n, &[decision("spec.containers[embed]", Some(2500), Some(10 * 1024))]);
    let v = object_of(&applied(&n, &p));
    assert_eq!(v["spec"]["containers"][0]["resources"]["requests"]["cpu"], json!("3"));
    assert_eq!(v["spec"]["containers"][0]["resources"]["limits"]["cpu"], json!("3"));
    // A Burstable fractional prior is not rounded.
    let n = admission(value("job.yaml"), "ml");
    let p = render(&a, &n, &[decision(BUILD, Some(2500), None)]);
    assert_eq!(object_of(&applied(&n, &p))["spec"]["template"]["spec"]["containers"][0]["resources"]["requests"]["cpu"], json!("2500m"));
}

#[test]
fn s2_gpu_count_never_changes() {
    let a = adapter();
    let n = admission(value("jobset.yaml"), "ml");
    let unit = "spec.replicatedJobs[workers].template.spec.template.spec.containers[trainer]";
    let mut d = req(Some(64000), Some(800 * 1024));
    d.requests.gpu = Some(GpuShape { accel: AcceleratorId::H100_80G, count: 4 });
    let r = a.render_with(&n, &RenderRequest::enforce(&[(UnitId(unit.into()), d)])).unwrap();
    let v = object_of(&applied(&n, &r.patch));
    let res = &v["spec"]["replicatedJobs"][1]["template"]["spec"]["template"]["spec"]["containers"][0]["resources"];
    assert_eq!(res["limits"]["nvidia.com/gpu"], json!(8));
    assert_eq!(res["requests"]["memory"], json!("800Gi"));
    assert!(r.held.iter().any(|(u, ax, _)| u.0 == unit && *ax == Axis::GpuCount));
    assert_eq!(r.applied[&UnitId(unit.into())].requests.gpu.unwrap().count, 8);
}

#[test]
fn recommend_and_observe_are_annotation_only() {
    let a = adapter();
    for mode in [Mode::Recommend, Mode::Observe] {
        let n = admission(value("job.yaml"), "ml");
        let r = a.render_with(&n, &RenderRequest { mode, ..req_fixed(&[decision(BUILD, Some(2000), Some(12 * 1024))]) }).unwrap();
        let Patch::JsonPatch { ops } = &r.patch else { panic!() };
        for o in ops {
            let p = match o {
                JsonPatchOp::Add { path, .. } | JsonPatchOp::Replace { path, .. } => path,
                other => panic!("{other:?}"),
            };
            assert!(p.contains("/annotations"), "{p}");
        }
        let v = object_of(&applied(&n, &r.patch));
        assert_eq!(v["spec"]["template"]["spec"]["containers"][0]["resources"], value("job.yaml")["spec"]["template"]["spec"]["containers"][0]["resources"]);
        assert!(v["metadata"]["annotations"]["rightsizer.io/recommendation"].as_str().unwrap().contains("12Gi"));
        assert!(v["metadata"]["annotations"].get("rightsizer.io/prior").is_none());
        // Once annotated, nothing more.
        assert_eq!(a.render_with(&applied(&n, &r.patch), &RenderRequest { mode, ..req_fixed(&[decision(BUILD, Some(2000), Some(12 * 1024))]) }).unwrap().patch, Patch::None);
    }
    let off = a.render_with(&doc("job.yaml"), &RenderRequest { mode: Mode::Off, ..req_fixed(&[decision(BUILD, Some(1), Some(1))]) }).unwrap();
    assert_eq!(off.patch, Patch::None);
}

#[test]
fn kuberay_params_follow_resources() {
    let a = adapter();
    let n = doc("raycluster.yaml");
    let r = a
        .render_with(&n, &RenderRequest {
            annotate: false,
            ..req_fixed(&[
                decision("spec.headGroupSpec.template.spec.containers[ray-head]", Some(2000), Some(8 * 1024)),
                decision("spec.workerGroupSpecs[cpu-workers].template.spec.containers[ray-worker]", Some(6000), Some(24 * 1024)),
            ])
        })
        .unwrap();
    let out = text_of(&applied(&n, &r.patch));
    golden("raycluster.yaml", &out);
    let v = object_of(&applied(&n, &r.patch));
    let w = &v["spec"]["workerGroupSpecs"][0];
    assert_eq!(w["rayStartParams"]["num-cpus"], json!("6"));
    assert_eq!(w["rayStartParams"]["memory"], json!((24u64 << 30).to_string()));
    assert_eq!(w["template"]["spec"]["containers"][0]["resources"]["limits"], json!({"cpu": "6", "memory": "24Gi"}));
    // The head's deliberate num-cpus: "0" is kept, and its comment too.
    assert_eq!(v["spec"]["headGroupSpec"]["rayStartParams"]["num-cpus"], json!("0"));
    assert!(out.contains("num-cpus: \"0\"       # keep tasks off the head"));
    assert!(out.contains("num-cpus: \"6\"\n"));
    // Restore puts the parameters back.
    let n = admission(value("raycluster.yaml"), "ml");
    let p = render(&a, &n, &[decision("spec.workerGroupSpecs[cpu-workers].template.spec.containers[ray-worker]", Some(6000), Some(24 * 1024))]);
    let sized = applied(&n, &p);
    let back = a.restore(&sized).unwrap();
    let restored = applied(&sized, &Patch::JsonPatch { ops: back.ops });
    assert_eq!(object_of(&restored), value("raycluster.yaml"));
}

#[test]
fn trainjob_and_pod_level_render() {
    let a = adapter();
    let n = admission(value("trainjob.yaml"), "ml");
    let p = render(&a, &n, &[decision("spec.trainer.resourcesPerNode", Some(24000), Some(192 * 1024))]);
    let v = object_of(&applied(&n, &p));
    assert_eq!(v["spec"]["trainer"]["resourcesPerNode"], json!({"requests": {"cpu": "24", "memory": "192Gi", "nvidia.com/gpu": 4}, "limits": {"nvidia.com/gpu": 4}}));
    let n = admission(value("pod-level.yaml"), "ml");
    let p = render(&a, &n, &[decision("spec.resources", Some(1000), Some(2048))]);
    let v = object_of(&applied(&n, &p));
    assert_eq!(v["spec"]["resources"], json!({"requests": {"cpu": "1", "memory": "2Gi"}, "limits": {"memory": "4Gi"}}));
    assert!(v["spec"]["containers"][0].get("resources").is_none());
}

#[test]
fn enforce_needs_a_signing_key() {
    let a = K8sAdapter::new(settings());
    assert!(block(rightsizer_plugin::Adapter::render(&a, &doc("job.yaml"), &[decision(BUILD, Some(2000), None)])).is_err());
    // Without annotations, or in recommend mode, no key is needed.
    assert!(a.render_with(&doc("job.yaml"), &RenderRequest { annotate: false, ..RenderRequest::enforce(&[decision(BUILD, Some(2000), None)]) }).is_ok());
    assert!(a.render_with(&doc("job.yaml"), &RenderRequest { mode: Mode::Recommend, ..RenderRequest::enforce(&[decision(BUILD, Some(2000), None)]) }).is_ok());
}

#[test]
fn forged_prior_is_ignored_and_overwritten() {
    let a = adapter();
    let mut j = value("job.yaml");
    // An attacker-supplied "prior" claiming a huge origin (to skew attribution).
    let forged = rightsizer_k8s::prior::sign(&Default::default(), &rightsizer_k8s::SigningKeys::single("k1", b"attacker-key-000000000".to_vec()));
    j["metadata"]["annotations"]["rightsizer.io/prior"] = json!(forged);
    let n = admission(j, "ml");
    let e = extract(&a, &n, &env());
    assert_eq!(e.units.len(), 2, "an invalid prior is ignored");
    let p = render(&a, &n, &[decision(BUILD, Some(2000), Some(12 * 1024))]);
    let v = object_of(&applied(&n, &p));
    let sp = rightsizer_k8s::prior::verify(v["metadata"]["annotations"]["rightsizer.io/prior"].as_str().unwrap(), &keys()).unwrap();
    assert_eq!(sp.prior[BUILD].requests.memory, Some(Bytes::gib(16)));
    assert_eq!(sp.prior[BUILD].requests.cpu, Some(MilliCpu(4000)));
}

#[test]
fn gpu_type_change_opt_in() {
    let unit = "spec.pytorchReplicaSpecs.Worker.template.spec.containers[pytorch]";
    let mut d = req(None, None);
    d.requests.gpu = Some(GpuShape { accel: AcceleratorId::H100_80G, count: 1 });
    let n = admission(value("pytorchjob.yaml"), "ml");
    // Off by default: held.
    let r = adapter().render_with(&n, &RenderRequest::enforce(&[(UnitId(unit.into()), d.clone())])).unwrap();
    assert!(r.held.iter().any(|(_, ax, why)| *ax == Axis::GpuType && why.contains("off")));
    // Opted in: the node selector changes.
    let s = K8sSettings { allow_gpu_type_change: true, ..settings() };
    let a = K8sAdapter::new(s).with_keys(keys());
    let r = a.render_with(&n, &RenderRequest::enforce(&[(UnitId(unit.into()), d.clone())])).unwrap();
    let v = object_of(&applied(&n, &r.patch));
    assert_eq!(v["spec"]["pytorchReplicaSpecs"]["Worker"]["template"]["spec"]["nodeSelector"]["nvidia.com/gpu.product"], json!("NVIDIA-H100-80GB-HBM3"));
    // Queue-managed jobs never change type.
    let mut q = value("pytorchjob.yaml");
    q["metadata"]["labels"] = json!({"kueue.x-k8s.io/queue-name": "gpu"});
    let r = a.render_with(&admission(q, "ml"), &RenderRequest::enforce(&[(UnitId(unit.into()), d.clone())])).unwrap();
    assert!(r.held.iter().any(|(_, ax, why)| *ax == Axis::GpuType && why.contains("queue")));
    // An excluding required node affinity holds it.
    let mut x = value("pytorchjob.yaml");
    x["spec"]["pytorchReplicaSpecs"]["Worker"]["template"]["spec"]["nodeSelector"] = json!({});
    x["spec"]["pytorchReplicaSpecs"]["Worker"]["template"]["spec"]["affinity"] = json!({"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [
        {"matchExpressions": [{"key": "nvidia.com/gpu.product", "operator": "In", "values": ["NVIDIA-A100-SXM4-80GB"]}]}]}}});
    let r = a.render_with(&admission(x, "ml"), &RenderRequest::enforce(&[(UnitId(unit.into()), d)])).unwrap();
    assert!(r.held.iter().any(|(_, ax, why)| *ax == Axis::GpuType && why.contains("EmptyIntersection")));
}

#[test]
fn text_and_json_outputs_agree() {
    let a = adapter();
    let d = [decision(BUILD, Some(3000), Some(20 * 1024))];
    let t = applied(&doc("job.yaml"), &a.render_with(&doc("job.yaml"), &RenderRequest { annotate: false, ..req_fixed(&d) }).unwrap().patch);
    let n = Native::Object { value: value("job.yaml") };
    let j = applied(&n, &a.render_with(&n, &RenderRequest { annotate: false, output: Output::JsonPatch, ..req_fixed(&d) }).unwrap().patch);
    // Same meaning (YAML `cpu: "3"` vs JSON "3").
    assert_eq!(object_of(&t), object_of(&j));
    let _ = UnitRender::default();
}
