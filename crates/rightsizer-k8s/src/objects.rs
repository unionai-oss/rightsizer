//! Native submissions → K8s objects, and the workload model: which pod
//! templates (sites) an object carries and which containers they hold.

use std::collections::BTreeMap;

use rightsizer_plugin::{Native, PluginError, Result};
use serde_json::Value;

use crate::edit::{Path, Seg, join};
use crate::path;
use crate::segs;
use crate::yaml::{self, YDoc};

/// The admission request around an object (`AdmissionReview.request`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Admission {
    /// Request uid.
    pub uid: Option<String>,
    /// Namespace (trusted).
    pub namespace: Option<String>,
    /// `CREATE`, `UPDATE`, …
    pub operation: Option<String>,
    /// `userInfo.username`.
    pub username: Option<String>,
    /// `dryRun`.
    pub dry_run: bool,
}

/// One K8s object of a submission.
#[derive(Clone, Debug, PartialEq)]
pub struct Object {
    /// The object.
    pub value: Value,
    /// Index of the document (documents only).
    pub doc_index: Option<usize>,
}

/// A parsed submission.
#[derive(Clone, Debug, PartialEq)]
pub struct Submission {
    /// Objects in order.
    pub objects: Vec<Object>,
    /// Admission metadata (`Native::Object` admission requests only).
    pub admission: Option<Admission>,
    /// Located YAML documents (`Native::Document` only), parallel to `objects`.
    pub docs: Option<(String, Vec<YDoc>)>,
}

impl Submission {
    /// Whether unit ids need a document prefix.
    pub fn multi(&self) -> bool {
        self.objects.len() > 1
    }
}

/// Parse a native submission.
///
/// - `Native::Document`: YAML or JSON, possibly multi-document (`---`), or a
///   `List` (`kind: List` / `items`).
/// - `Native::Object`: an `AdmissionReview` (`{"request": {...}}`), an
///   `AdmissionRequest` (`{"object": ..., "namespace": ..., "operation": ...}`),
///   or a bare object.
pub fn parse_native(native: &Native) -> Result<Submission> {
    match native {
        Native::Document { bytes, .. } => {
            let text = std::str::from_utf8(bytes).map_err(|e| PluginError::Parse(format!("not UTF-8: {e}")))?.to_string();
            let docs = yaml::parse(&text).map_err(|e| PluginError::Parse(e.to_string()))?;
            if docs.len() == 1 && docs[0].value.get("kind").and_then(Value::as_str) == Some("List") {
                // A List document cannot be edited per item in place reliably by index
                // (items are located), so expose items as objects with the doc located.
                return Err(PluginError::Unsupported("kind: List documents (split them with ---)".into()));
            }
            let objects = docs.iter().enumerate().map(|(i, d)| Object { value: d.value.clone(), doc_index: Some(i) }).collect();
            Ok(Submission { objects, admission: None, docs: Some((text, docs)) })
        }
        Native::Object { value } => {
            let req = value.get("request").filter(|r| r.get("object").is_some()).or_else(|| value.get("object").map(|_| value));
            match req {
                Some(r) => {
                    let adm = Admission {
                        uid: r.get("uid").and_then(Value::as_str).map(str::to_string),
                        namespace: r.get("namespace").and_then(Value::as_str).map(str::to_string),
                        operation: r.get("operation").and_then(Value::as_str).map(str::to_string),
                        username: r.pointer("/userInfo/username").and_then(Value::as_str).map(str::to_string),
                        dry_run: r.get("dryRun").and_then(Value::as_bool).unwrap_or(false),
                    };
                    let obj = r.get("object").cloned().unwrap_or(Value::Null);
                    if !obj.is_object() {
                        return Err(PluginError::InvalidNative("admission request without an object".into()));
                    }
                    Ok(Submission { objects: vec![Object { value: obj, doc_index: None }], admission: Some(adm), docs: None })
                }
                None if value.get("kind").is_some() => Ok(Submission { objects: vec![Object { value: value.clone(), doc_index: None }], admission: None, docs: None }),
                None => Err(PluginError::InvalidNative("expected an AdmissionReview, an AdmissionRequest or a K8s object".into())),
            }
        }
        Native::ApiCall { .. } => Err(PluginError::Unsupported("k8s: ApiCall".into())),
    }
}

/// Workload kinds this adapter understands.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum Kind {
    /// `v1 Pod`.
    Pod,
    /// `batch/v1 Job`.
    Job,
    /// `batch/v1 CronJob`.
    CronJob,
    /// `jobset.x-k8s.io JobSet`.
    JobSet,
    /// `kubeflow.org PyTorchJob`.
    PyTorchJob,
    /// `trainer.kubeflow.org TrainJob`.
    TrainJob,
    /// `ray.io RayCluster`.
    RayCluster,
    /// `ray.io RayJob`.
    RayJob,
    /// `batch.volcano.sh Job`.
    VolcanoJob,
}

impl Kind {
    /// Detect from `apiVersion` and `kind`.
    pub fn of(v: &Value) -> Option<Kind> {
        let api = v.get("apiVersion").and_then(Value::as_str).unwrap_or("");
        let kind = v.get("kind").and_then(Value::as_str)?;
        let group = api.rsplit_once('/').map(|(g, _)| g).unwrap_or("");
        Some(match (group, kind) {
            ("", "Pod") => Kind::Pod,
            ("batch", "Job") => Kind::Job,
            ("batch", "CronJob") => Kind::CronJob,
            ("jobset.x-k8s.io", "JobSet") => Kind::JobSet,
            ("kubeflow.org", "PyTorchJob") => Kind::PyTorchJob,
            ("trainer.kubeflow.org", "TrainJob") => Kind::TrainJob,
            ("ray.io", "RayCluster") => Kind::RayCluster,
            ("ray.io", "RayJob") => Kind::RayJob,
            ("batch.volcano.sh", "Job") => Kind::VolcanoJob,
            _ => return None,
        })
    }

    /// The key-suffix tag (kubernetes.md §2).
    pub fn tag(self) -> &'static str {
        match self {
            Kind::Pod => "pod",
            Kind::Job => "job",
            Kind::CronJob => "cronjob",
            Kind::JobSet => "jobset",
            Kind::PyTorchJob => "pytorchjob",
            Kind::TrainJob => "trainjob",
            Kind::RayCluster | Kind::RayJob => "ray",
            Kind::VolcanoJob => "volcano",
        }
    }

    /// The `kind` string.
    pub fn name(self) -> &'static str {
        match self {
            Kind::Pod => "Pod",
            Kind::Job => "Job",
            Kind::CronJob => "CronJob",
            Kind::JobSet => "JobSet",
            Kind::PyTorchJob => "PyTorchJob",
            Kind::TrainJob => "TrainJob",
            Kind::RayCluster => "RayCluster",
            Kind::RayJob => "RayJob",
            Kind::VolcanoJob => "Job",
        }
    }

    /// Whether the kind is a pod template carrier (not a bare pod).
    pub fn is_template(self) -> bool {
        self != Kind::Pod
    }
}

/// Role of a container in its pod.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Role {
    /// A regular init container.
    Init,
    /// A restartable init container (`restartPolicy: Always`).
    Sidecar,
    /// An app container.
    App,
}

/// A container inside a pod template.
#[derive(Clone, Debug, PartialEq)]
pub struct ContainerRef {
    /// Container name.
    pub name: String,
    /// `initContainers` or `containers`.
    pub list: &'static str,
    /// Index in its list.
    pub index: usize,
    /// Role.
    pub role: Role,
    /// Path to the container object.
    pub path: Path,
}

/// KubeRay group extras.
#[derive(Clone, Debug, PartialEq)]
pub struct RayGroup {
    /// Path to `rayStartParams`.
    pub params_path: Path,
}

/// One pod template (or bare pod spec) of an object.
#[derive(Clone, Debug, PartialEq)]
pub struct Site {
    /// Path to the pod spec.
    pub spec_path: Path,
    /// Path to the pod template's metadata (`None` for bare pods: the object's metadata).
    pub meta_path: Path,
    /// Stable named id of the pod spec, e.g. `spec.replicatedJobs[workers].template.spec.template.spec`.
    pub id: String,
    /// Group name (replicatedJob, replicaType, Ray group, Volcano task).
    pub group: Option<String>,
    /// Concurrent pods of this template.
    pub replicas: u64,
    /// KubeRay group.
    pub ray: Option<RayGroup>,
}

/// The `spec.trainer` of a TrainJob.
#[derive(Clone, Debug, PartialEq)]
pub struct Trainer {
    /// Path to `spec.trainer`.
    pub path: Path,
    /// `numNodes`.
    pub nodes: u64,
}

/// A recognised workload object.
#[derive(Clone, Debug, PartialEq)]
pub struct Workload {
    /// Kind.
    pub kind: Kind,
    /// `metadata.name` (may be empty with `generateName`).
    pub name: String,
    /// `metadata.generateName`.
    pub generate_name: Option<String>,
    /// Pod templates.
    pub sites: Vec<Site>,
    /// TrainJob trainer.
    pub trainer: Option<Trainer>,
}

fn u64_at(v: &Value, p: &str) -> Option<u64> {
    v.pointer(p).and_then(|x| x.as_u64().or_else(|| x.as_str().and_then(|s| s.parse().ok())))
}

/// `min(parallelism, completions)` of a Job spec at `p` (defaults 1).
fn job_pods(v: &Value, p: &str) -> u64 {
    let par = u64_at(v, &format!("{p}/parallelism")).unwrap_or(1);
    match u64_at(v, &format!("{p}/completions")) {
        Some(c) => par.min(c),
        None => par,
    }
    .max(1)
}

/// Recognise a workload object and enumerate its pod templates.
pub fn workload(v: &Value) -> Option<Workload> {
    let kind = Kind::of(v)?;
    let name = v.pointer("/metadata/name").and_then(Value::as_str).unwrap_or("").to_string();
    let generate_name = v.pointer("/metadata/generateName").and_then(Value::as_str).filter(|s| !s.is_empty()).map(str::to_string);
    let mut sites = Vec::new();
    let mut trainer = None;
    match kind {
        Kind::Pod => sites.push(Site { spec_path: path!["spec"], meta_path: path!["metadata"], id: "spec".into(), group: None, replicas: 1, ray: None }),
        Kind::Job => sites.push(Site {
            spec_path: path!["spec", "template", "spec"],
            meta_path: path!["spec", "template", "metadata"],
            id: "spec.template.spec".into(),
            group: None,
            replicas: job_pods(v, "/spec"),
            ray: None,
        }),
        Kind::CronJob => sites.push(Site {
            spec_path: path!["spec", "jobTemplate", "spec", "template", "spec"],
            meta_path: path!["spec", "jobTemplate", "spec", "template", "metadata"],
            id: "spec.jobTemplate.spec.template.spec".into(),
            group: None,
            replicas: job_pods(v, "/spec/jobTemplate/spec"),
            ray: None,
        }),
        Kind::JobSet => {
            for (i, rj) in v.pointer("/spec/replicatedJobs").and_then(Value::as_array).into_iter().flatten().enumerate() {
                let rname = rj.get("name").and_then(Value::as_str).unwrap_or("").to_string();
                let reps = rj.get("replicas").and_then(Value::as_u64).unwrap_or(1).max(1);
                sites.push(Site {
                    spec_path: path!["spec", "replicatedJobs", i, "template", "spec", "template", "spec"],
                    meta_path: path!["spec", "replicatedJobs", i, "template", "spec", "template", "metadata"],
                    id: format!("spec.replicatedJobs[{rname}].template.spec.template.spec"),
                    group: Some(rname),
                    replicas: reps * job_pods(rj, "/template/spec"),
                    ray: None,
                });
            }
        }
        Kind::PyTorchJob => {
            if let Some(m) = v.pointer("/spec/pytorchReplicaSpecs").and_then(Value::as_object) {
                for (rt, spec) in m {
                    let reps = spec.get("replicas").and_then(Value::as_u64).unwrap_or(1).max(1);
                    sites.push(Site {
                        spec_path: path!["spec", "pytorchReplicaSpecs", rt.as_str(), "template", "spec"],
                        meta_path: path!["spec", "pytorchReplicaSpecs", rt.as_str(), "template", "metadata"],
                        id: format!("spec.pytorchReplicaSpecs.{rt}.template.spec"),
                        group: Some(rt.clone()),
                        replicas: reps,
                        ray: None,
                    });
                }
            }
        }
        Kind::TrainJob => {
            if v.pointer("/spec/trainer").is_some() {
                trainer = Some(Trainer { path: path!["spec", "trainer"], nodes: u64_at(v, "/spec/trainer/numNodes").unwrap_or(1).max(1) });
            }
        }
        Kind::RayCluster | Kind::RayJob => {
            let base: Path = if kind == Kind::RayJob { path!["spec", "rayClusterSpec"] } else { path!["spec"] };
            let base_id = if kind == Kind::RayJob { "spec.rayClusterSpec" } else { "spec" };
            let base_ptr = crate::edit::pointer(&base);
            if v.pointer(&format!("{base_ptr}/headGroupSpec")).is_some() {
                let hg = join(&base, &[Seg::from("headGroupSpec")]);
                sites.push(Site {
                    spec_path: join(&hg, &segs!["template", "spec"]),
                    meta_path: join(&hg, &segs!["template", "metadata"]),
                    id: format!("{base_id}.headGroupSpec.template.spec"),
                    group: Some("head".into()),
                    replicas: 1,
                    ray: Some(RayGroup { params_path: join(&hg, &segs!["rayStartParams"]) }),
                });
            }
            for (i, wg) in v.pointer(&format!("{base_ptr}/workerGroupSpecs")).and_then(Value::as_array).into_iter().flatten().enumerate() {
                let gname = wg.get("groupName").and_then(Value::as_str).unwrap_or("workers").to_string();
                let reps = wg.get("replicas").and_then(Value::as_u64).or_else(|| wg.get("minReplicas").and_then(Value::as_u64)).unwrap_or(1).max(1);
                let nhosts = wg.get("numOfHosts").and_then(Value::as_u64).unwrap_or(1).max(1);
                let g = join(&base, &[Seg::from("workerGroupSpecs"), Seg::Idx(i)]);
                sites.push(Site {
                    spec_path: join(&g, &segs!["template", "spec"]),
                    meta_path: join(&g, &segs!["template", "metadata"]),
                    id: format!("{base_id}.workerGroupSpecs[{gname}].template.spec"),
                    group: Some(gname),
                    replicas: reps * nhosts,
                    ray: Some(RayGroup { params_path: join(&g, &segs!["rayStartParams"]) }),
                });
            }
        }
        Kind::VolcanoJob => {
            for (i, t) in v.pointer("/spec/tasks").and_then(Value::as_array).into_iter().flatten().enumerate() {
                let tname = t.get("name").and_then(Value::as_str).unwrap_or("task").to_string();
                let reps = t.get("replicas").and_then(Value::as_u64).unwrap_or(1).max(1);
                sites.push(Site {
                    spec_path: path!["spec", "tasks", i, "template", "spec"],
                    meta_path: path!["spec", "tasks", i, "template", "metadata"],
                    id: format!("spec.tasks[{tname}].template.spec"),
                    group: Some(tname),
                    replicas: reps,
                    ray: None,
                });
            }
        }
    }
    Some(Workload { kind, name, generate_name, sites, trainer })
}

/// Containers of a pod spec, init containers first.
pub fn containers(spec: &Value, spec_path: &Path) -> Vec<ContainerRef> {
    let mut out = Vec::new();
    for list in ["initContainers", "containers"] {
        for (i, c) in spec.get(list).and_then(Value::as_array).into_iter().flatten().enumerate() {
            let name = c.get("name").and_then(Value::as_str).unwrap_or("").to_string();
            let role = if list == "containers" {
                Role::App
            } else if crate::resources::is_sidecar(c) {
                Role::Sidecar
            } else {
                Role::Init
            };
            out.push(ContainerRef { name, list, index: i, role, path: join(spec_path, &[Seg::from(list), Seg::Idx(i)]) });
        }
    }
    out
}

/// String map at a JSON pointer (labels, annotations, nodeSelector).
pub fn string_map(v: &Value, ptr: &str) -> BTreeMap<String, String> {
    v.pointer(ptr)
        .and_then(Value::as_object)
        .map(|m| m.iter().filter_map(|(k, v)| Some((k.clone(), match v {
            Value::String(s) => s.clone(),
            Value::Null => return None,
            other => other.to_string(),
        }))).collect())
        .unwrap_or_default()
}

/// The object's owner references `(kind, name, controller)`.
pub fn owners(v: &Value) -> Vec<(String, String, bool)> {
    v.pointer("/metadata/ownerReferences")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|o| {
            (
                o.get("kind").and_then(Value::as_str).unwrap_or("").to_string(),
                o.get("name").and_then(Value::as_str).unwrap_or("").to_string(),
                o.get("controller").and_then(Value::as_bool).unwrap_or(false),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn admission_forms() {
        let review = json!({"apiVersion": "admission.k8s.io/v1", "kind": "AdmissionReview", "request": {
            "uid": "u1", "namespace": "ml", "operation": "CREATE", "userInfo": {"username": "alice"}, "dryRun": true,
            "object": {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "p", "namespace": "evil"}}}});
        let s = parse_native(&Native::Object { value: review }).unwrap();
        let a = s.admission.unwrap();
        assert_eq!(a.namespace.as_deref(), Some("ml"));
        assert_eq!(a.username.as_deref(), Some("alice"));
        assert!(a.dry_run);
        let bare = parse_native(&Native::Object { value: json!({"apiVersion": "v1", "kind": "Pod"}) }).unwrap();
        assert!(bare.admission.is_none());
        assert!(parse_native(&Native::Object { value: json!({"x": 1}) }).is_err());
    }

    #[test]
    fn kinds() {
        assert_eq!(Kind::of(&json!({"apiVersion": "batch.volcano.sh/v1alpha1", "kind": "Job"})), Some(Kind::VolcanoJob));
        assert_eq!(Kind::of(&json!({"apiVersion": "batch/v1", "kind": "Job"})), Some(Kind::Job));
        assert_eq!(Kind::of(&json!({"apiVersion": "apps/v1", "kind": "Deployment"})), None);
        let js = json!({"apiVersion": "jobset.x-k8s.io/v1alpha2", "kind": "JobSet", "metadata": {"name": "js"},
            "spec": {"replicatedJobs": [{"name": "workers", "replicas": 2, "template": {"spec": {"parallelism": 4, "completions": 4, "template": {"spec": {"containers": []}}}}}]}});
        let w = workload(&js).unwrap();
        assert_eq!(w.sites[0].replicas, 8);
        assert_eq!(w.sites[0].id, "spec.replicatedJobs[workers].template.spec.template.spec");
    }
}
