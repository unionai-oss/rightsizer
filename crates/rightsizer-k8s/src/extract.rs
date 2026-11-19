//! `extract` (kubernetes.md §2–§4): units, groups and keys per workload
//! kind; skip rules (§3.1); priors with LimitRange defaulting (§3.3);
//! context seeds (§4).

use std::collections::BTreeMap;

use rightsizer_core::{
    AcceleratorId, Axis, AxisSet, CodeInfo, CodeRef, Constraints, ContextSeed, GroupId, Hold, HoldReason, InputRef, ManifestFeatures,
    Prior, ResourceSpec, ScalarValue, Tenant, UnitId,
};
use rightsizer_plugin::{AcceleratorMap, Extracted, PluginError, Result, SkipReason, Unit};
use serde_json::Value;

use crate::edit::{Path, join, pointer};
use crate::keys;
use crate::limitrange::{self, LimitRanges};
use crate::mode;
use crate::objects::{self, ContainerRef, Kind, Role, Site, Workload};
use crate::segs;
use crate::prior::{self, SignedPrior, SigningKeys};
use crate::resources;
use crate::settings::K8sSettings;

/// `rightsizer.io/decision-id`.
pub const DECISION_ID: &str = "rightsizer.io/decision-id";
/// `rightsizer.io/recommendation`.
pub const RECOMMENDATION: &str = "rightsizer.io/recommendation";
/// `rightsizer.io/inputs`.
pub const INPUTS: &str = "rightsizer.io/inputs";
/// `rightsizer.io/code-configmap`.
pub const CODE_CONFIGMAP: &str = "rightsizer.io/code-configmap";
/// The Flyte integration's container marker.
pub const ENV_DECISION: &str = "RIGHTSIZER_DECISION";
/// Kueue's queue label.
pub const KUEUE_QUEUE: &str = "kueue.x-k8s.io/queue-name";
/// Pod labels/annotations of queue-managed pods.
pub const QUEUE_POD_MARKERS: [&str; 4] = [KUEUE_QUEUE, "kueue.x-k8s.io/managed", "kueue.x-k8s.io/pod-group-name", "scheduling.k8s.io/group-name"];
/// Owner kinds of template carriers that create the object being admitted
/// (their template was sized; the child never is).
const TEMPLATE_OWNERS: [&str; 6] = ["JobSet", "TrainJob", "RayJob", "RayCluster", "PyTorchJob", "Job"];

/// Where a unit's resources live.
#[derive(Clone, Debug, PartialEq)]
pub enum Target {
    /// A container.
    Container(ContainerRef),
    /// Pod-level `spec.resources`.
    PodLevel,
    /// TrainJob `spec.trainer.resourcesPerNode`.
    Trainer,
}

/// A unit's location in its object.
#[derive(Clone, Debug, PartialEq)]
pub struct UnitLoc {
    /// Id within the object (`spec.template.spec.containers[main]`).
    pub local_id: String,
    /// Index into [`Workload::sites`].
    pub site: Option<usize>,
    /// Target.
    pub target: Target,
    /// Key parts after the suffix base.
    pub key_parts: Vec<String>,
}

impl UnitLoc {
    /// Path of the `resources` object.
    pub fn resources_path(&self, w: &Workload) -> Path {
        match &self.target {
            Target::Container(c) => join(&c.path, &segs!["resources"]),
            Target::PodLevel => join(&w.sites[self.site.expect("site")].spec_path, &segs!["resources"]),
            Target::Trainer => join(&w.trainer.as_ref().expect("trainer").path, &segs!["resourcesPerNode"]),
        }
    }
    /// Path of the object holding command/args/env/image.
    pub fn code_path(&self, w: &Workload) -> Option<Path> {
        match &self.target {
            Target::Container(c) => Some(c.path.clone()),
            Target::PodLevel => None,
            Target::Trainer => w.trainer.as_ref().map(|t| t.path.clone()),
        }
    }
}

fn has_pod_level(spec: &Value) -> bool {
    spec.get("resources").is_some_and(|r| r.get("requests").is_some() || r.get("limits").is_some())
}

/// Enumerate unit locations of a workload (deterministic; shared by
/// extract, render, validate and restore).
pub fn locate(v: &Value, w: &Workload) -> Vec<UnitLoc> {
    let mut out = Vec::new();
    if let Some(_t) = &w.trainer {
        out.push(UnitLoc { local_id: "spec.trainer.resourcesPerNode".into(), site: None, target: Target::Trainer, key_parts: vec!["trainer".into()] });
    }
    for (si, site) in w.sites.iter().enumerate() {
        let Some(spec) = v.pointer(&pointer(&site.spec_path)) else { continue };
        let group: Vec<String> = site.group.iter().cloned().collect();
        if has_pod_level(spec) {
            let mut parts = group.clone();
            parts.push("@pod".into());
            out.push(UnitLoc { local_id: format!("{}.resources", site.id), site: Some(si), target: Target::PodLevel, key_parts: parts });
            continue;
        }
        let cs = objects::containers(spec, &site.spec_path);
        let first_app = cs.iter().position(|c| c.role == Role::App);
        for (ci, c) in cs.into_iter().enumerate() {
            let short = matches!(w.kind, Kind::RayCluster | Kind::RayJob | Kind::VolcanoJob) && Some(ci) == first_app;
            let mut parts = group.clone();
            if !short {
                parts.push(c.name.clone());
            }
            out.push(UnitLoc { local_id: format!("{}.{}[{}]", site.id, c.list, c.name), site: Some(si), target: Target::Container(c), key_parts: parts });
        }
    }
    out
}

/// Unit id in a submission: local id, prefixed with `{doc}:` in multi-document submissions.
pub fn unit_id(doc: Option<usize>, multi: bool, local: &str) -> String {
    match (doc, multi) {
        (Some(i), true) => format!("{i}:{local}"),
        _ => local.to_string(),
    }
}

/// Split a submission unit id into (doc index, local id).
pub fn split_unit_id(id: &str, multi: bool) -> (Option<usize>, &str) {
    if multi
        && let Some((d, rest)) = id.split_once(':')
        && let Ok(i) = d.parse::<usize>()
    {
        return (Some(i), rest);
    }
    (None, id)
}

/// Markers that make a unit distributed (kubernetes.md §2).
const DIST_MARKERS: [(&str, &str); 8] = [
    ("torchrun", "torchrun"),
    ("torch.distributed.run", "torchrun"),
    ("torch.distributed.launch", "torchrun"),
    ("--nproc_per_node", "torchrun"),
    ("--nproc-per-node", "torchrun"),
    ("WORLD_SIZE", ""),
    ("deepspeed", "deepspeed"),
    ("accelerate launch", "accelerate"),
];

/// Distribution markers in command/args/env of a container-like object.
pub fn distributed_markers(c: &Value) -> (Vec<String>, Option<String>) {
    let mut texts: Vec<String> = Vec::new();
    for k in ["command", "args"] {
        if let Some(a) = c.get(k).and_then(Value::as_array) {
            texts.push(a.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(" "));
        }
    }
    for e in c.get("env").and_then(Value::as_array).into_iter().flatten() {
        if let Some(n) = e.get("name").and_then(Value::as_str) {
            texts.push(n.to_string());
        }
        if let Some(v) = e.get("value").and_then(Value::as_str) {
            texts.push(v.to_string());
        }
    }
    let mut markers = Vec::new();
    let mut launcher = None;
    for (m, l) in DIST_MARKERS {
        if texts.iter().any(|t| t.contains(m)) {
            let name = m.trim_start_matches("--").to_string();
            if !markers.contains(&name) {
                markers.push(name);
            }
            if launcher.is_none() && !l.is_empty() {
                launcher = Some(l.to_string());
            }
        }
    }
    (markers, launcher)
}

fn decision_marker(c: &Value) -> Option<Option<String>> {
    for e in c.get("env").and_then(Value::as_array).into_iter().flatten() {
        if e.get("name").and_then(Value::as_str) == Some(ENV_DECISION) {
            let id = e.get("value").and_then(Value::as_str).and_then(|v| serde_json::from_str::<Value>(v).ok()).and_then(|j| j.get("decision_id").and_then(Value::as_str).map(str::to_string));
            return Some(id);
        }
    }
    None
}

/// The signed prior of an object, when present and valid.
pub fn signed_prior(object: &Value, keys: Option<&SigningKeys>) -> Option<SignedPrior> {
    let a = objects::string_map(object, "/metadata/annotations");
    let v = a.get(prior::PRIOR_ANNOTATION)?;
    prior::verify(v, keys?).ok()
}

fn str_val(v: Option<&Value>) -> Option<String> {
    v.and_then(Value::as_str).map(str::to_string)
}

fn u64_val(v: Option<&Value>) -> Option<u64> {
    v.and_then(|x| x.as_u64().or_else(|| x.as_str().and_then(|s| s.parse().ok())))
}

/// `backoffLimit`-like retries of a kind (same-size retries).
pub fn retries(v: &Value, kind: Kind) -> u32 {
    let r = match kind {
        Kind::Pod => Some(0),
        Kind::Job => u64_val(v.pointer("/spec/backoffLimit")).or(Some(6)),
        Kind::CronJob => u64_val(v.pointer("/spec/jobTemplate/spec/backoffLimit")).or(Some(6)),
        Kind::JobSet => u64_val(v.pointer("/spec/failurePolicy/maxRestarts")).or(Some(0)),
        Kind::PyTorchJob => u64_val(v.pointer("/spec/runPolicy/backoffLimit")).or(Some(0)),
        Kind::RayJob => u64_val(v.pointer("/spec/backoffLimit")).or(Some(0)),
        Kind::RayCluster => Some(0),
        Kind::VolcanoJob => u64_val(v.pointer("/spec/maxRetry")).or(Some(3)),
        Kind::TrainJob => Some(0),
    };
    r.unwrap_or(0).min(u32::MAX as u64) as u32
}

/// `activeDeadlineSeconds` of an object (Job-level first, then the pod's).
pub fn active_deadline(v: &Value, kind: Kind, site_spec: Option<&Value>) -> Option<u64> {
    let job = match kind {
        Kind::Job => u64_val(v.pointer("/spec/activeDeadlineSeconds")),
        Kind::CronJob => u64_val(v.pointer("/spec/jobTemplate/spec/activeDeadlineSeconds")),
        Kind::RayJob => u64_val(v.pointer("/spec/activeDeadlineSeconds")),
        Kind::PyTorchJob => u64_val(v.pointer("/spec/runPolicy/activeDeadlineSeconds")),
        _ => None,
    };
    job.or_else(|| site_spec.and_then(|s| u64_val(s.get("activeDeadlineSeconds"))))
}

/// Memory-backed emptyDir volumes of a pod spec: name → sizeLimit bytes (None = unbounded).
pub fn memory_volumes(spec: &Value) -> BTreeMap<String, Option<u64>> {
    let mut out = BTreeMap::new();
    for vol in spec.get("volumes").and_then(Value::as_array).into_iter().flatten() {
        if vol.pointer("/emptyDir/medium").and_then(Value::as_str) == Some("Memory") {
            let name = vol.get("name").and_then(Value::as_str).unwrap_or_default().to_string();
            let lim = vol.pointer("/emptyDir/sizeLimit").and_then(|q| resources::bytes_of(q).ok()).map(|b| b.0);
            out.insert(name, lim);
        }
    }
    out
}

fn mounts(c: &Value) -> Vec<(String, String)> {
    c.get("volumeMounts")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|m| Some((m.get("name")?.as_str()?.to_string(), m.get("mountPath")?.as_str()?.to_string())))
        .collect()
}

/// The code digest of a container: image, command and args.
pub fn code_digest(c: &Value) -> String {
    let j = serde_json::json!({
        "image": c.get("image").cloned().unwrap_or(Value::Null),
        "command": c.get("command").cloned().unwrap_or(Value::Null),
        "args": c.get("args").cloned().unwrap_or(Value::Null),
    });
    rightsizer_core::digest::sha256_hex(&rightsizer_core::digest::canonical_json(&j))
}

/// Parse `rightsizer.io/inputs`: a JSON array of URIs, or an object `{name: uri}`.
pub fn parse_inputs(s: &str) -> Vec<InputRef> {
    let mk = |name: String, uri: &str| InputRef { name, uri: Some(uri.to_string()), bytes: None, format: None, scalar: None };
    match serde_json::from_str::<Value>(s) {
        Ok(Value::Array(a)) => a.iter().filter_map(Value::as_str).enumerate().map(|(i, u)| mk(format!("input{i}"), u)).take(64).collect(),
        Ok(Value::Object(m)) => m.iter().filter_map(|(k, v)| v.as_str().map(|u| mk(k.clone(), u))).take(64).collect(),
        _ => Vec::new(),
    }
}

/// What extract needs besides the object.
pub struct ExtractCtx<'a> {
    /// Settings.
    pub settings: &'a K8sSettings,
    /// Accelerator map.
    pub accel: &'a AcceleratorMap,
    /// Signing keys (to recognise our own prior).
    pub keys: Option<&'a SigningKeys>,
    /// Trusted namespace.
    pub namespace: &'a str,
    /// Whether this is an admission request (hash-only inputs).
    pub admission: bool,
    /// Whether ids carry a document prefix.
    pub multi: bool,
    /// Include units already in a valid signed prior (watcher, restore, explain).
    pub include_sized: bool,
}

fn skip(tenant: Tenant, reason: SkipReason) -> Extracted {
    Extracted { tenant, units: vec![], skip: Some(reason) }
}

/// Extract one object.
pub fn extract_object(v: &Value, doc: Option<usize>, cx: &ExtractCtx<'_>) -> Result<Extracted> {
    let cluster = cx.settings.cluster.as_deref().ok_or_else(|| PluginError::InvalidNative("[k8s] cluster is not configured".into()))?;
    let tenant = keys::tenant(cluster, cx.namespace).ok_or_else(|| PluginError::InvalidNative(format!("invalid tenant {cluster}/{}", cx.namespace)))?;
    let Some(w) = objects::workload(v) else {
        let k = v.get("kind").and_then(Value::as_str).unwrap_or("?");
        return Ok(skip(tenant, SkipReason::Unsupported { detail: format!("kind {k}") }));
    };
    let labels = objects::string_map(v, "/metadata/labels");
    let annotations = objects::string_map(v, "/metadata/annotations");
    if mode::object_mode(v) == Some(rightsizer_core::Mode::Off) {
        return Ok(skip(tenant, SkipReason::OptedOut));
    }
    // Owners.
    let owners = objects::owners(v);
    if w.kind == Kind::Pod {
        if let Some((k, _, _)) = owners.iter().find(|(k, _, _)| cx.settings.pod_skip_owner_kinds.iter().any(|s| s == k)) {
            let reason = if TEMPLATE_OWNERS.contains(&k.as_str()) || k == "CronJob" {
                SkipReason::AlreadySized { decision_id: annotations.get(DECISION_ID).cloned() }
            } else if k == "Workload" || k == "PodGroup" {
                SkipReason::QueueManaged
            } else {
                SkipReason::Unsupported { detail: format!("owned by a {k}") }
            };
            return Ok(skip(tenant, reason));
        }
        if QUEUE_POD_MARKERS.iter().any(|m| labels.contains_key(*m) || annotations.contains_key(*m)) {
            return Ok(skip(tenant, SkipReason::QueueManaged));
        }
    } else if owners.iter().any(|(k, _, _)| TEMPLATE_OWNERS.contains(&k.as_str())) {
        return Ok(skip(tenant, SkipReason::AlreadySized { decision_id: annotations.get(DECISION_ID).cloned() }));
    }
    // Containers already sized by another layer (the Flyte integration).
    for site in &w.sites {
        let Some(spec) = v.pointer(&pointer(&site.spec_path)) else { continue };
        for c in objects::containers(spec, &site.spec_path) {
            if let Some(cv) = v.pointer(&pointer(&c.path))
                && let Some(id) = decision_marker(cv)
            {
                return Ok(skip(tenant, SkipReason::AlreadySized { decision_id: id }));
            }
        }
    }
    // Our own prior (reinvocation) vs another layer's decision id.
    let signed = signed_prior(v, cx.keys);
    if annotations.contains_key(DECISION_ID) && signed.is_none() && !cx.include_sized {
        return Ok(skip(tenant, SkipReason::AlreadySized { decision_id: annotations.get(DECISION_ID).cloned() }));
    }
    let queue_managed = labels.contains_key(KUEUE_QUEUE) || w.kind == Kind::VolcanoJob;

    // Key base.
    let owner_cron = if w.kind == Kind::Job { owners.iter().find(|(k, _, _)| k == "CronJob").map(|(_, n, _)| n.clone()) } else { None };
    let (tag, name) = match &owner_cron {
        Some(cron) => ("cronjob", cron.clone()),
        None => (w.kind.tag(), keys::base_name(&w.name, w.generate_name.as_deref())),
    };
    if name.is_empty() {
        return Err(PluginError::InvalidNative("object has neither name nor generateName".into()));
    }
    let base = keys::suffix_base(tag, &name, &annotations);
    let lr = LimitRanges::parse(&cx.settings.limit_ranges, Some(cx.namespace));
    let total_pods: u64 = w.sites.iter().map(|s| s.replicas).sum::<u64>() + w.trainer.as_ref().map(|t| t.nodes).unwrap_or(0);
    let multi_group_kind = matches!(w.kind, Kind::JobSet | Kind::PyTorchJob | Kind::TrainJob | Kind::VolcanoJob);
    let flyte = if w.kind == Kind::Pod { keys::flyte_key(&labels, cx.namespace, &cx.settings.flyte) } else { None };

    let locs = locate(v, &w);
    let mut units = Vec::new();
    let mut sized_ids = Vec::new();
    for loc in &locs {
        if let Some(sp) = &signed
            && sp.prior.contains_key(&loc.local_id)
            && !cx.include_sized
        {
            sized_ids.push(loc.local_id.clone());
            continue;
        }
        let site: Option<&Site> = loc.site.map(|i| &w.sites[i]);
        let site_spec = site.and_then(|s| v.pointer(&pointer(&s.spec_path)));
        let accel = match (&loc.target, site_spec) {
            (Target::Trainer, _) => AcceleratorId::UnknownGpu,
            (_, Some(spec)) => crate::gpu::accel_of(spec, cx.accel),
            _ => AcceleratorId::UnknownGpu,
        };
        let res_path = loc.resources_path(&w);
        let declared_res = v.pointer(&pointer(&res_path));
        let declared = resources::spec_of(declared_res, accel).map_err(|e| PluginError::InvalidNative(format!("{}: {e}", loc.local_id)))?;

        // Effective prior: LimitRange defaults (simulated for templates), API defaulting.
        let mut effective = declared.clone();
        let mut defaulted = AxisSet::empty();
        let mut qos = None;
        if let (Some(spec), Some(site)) = (site_spec, site) {
            let (defaulted_spec, filled) = if w.kind.is_template() { limitrange::default_pod_spec(spec, &lr) } else { (spec.clone(), vec![]) };
            qos = Some(resources::pod_qos(&defaulted_spec));
            if let Target::Container(c) = &loc.target {
                let eff_res = defaulted_spec.get(c.list).and_then(|l| l.get(c.index)).and_then(|cv| cv.get("resources"));
                effective = resources::spec_of(eff_res, accel).map_err(PluginError::InvalidNative)?;
                if w.kind.is_template() {
                    if let Some((_, a)) = filled.iter().find(|(n, _)| *n == c.name) {
                        defaulted = *a;
                    }
                } else if let Some(d) = signed.as_ref().and_then(|s| s.defaulted.get(&loc.local_id)) {
                    defaulted = *d;
                } else if !lr.is_empty() {
                    defaulted = limitrange::inferred_defaulted(declared_res, &lr);
                }
                // /dev/shm.
                let mv = memory_volumes(spec);
                if let Some(cv) = v.pointer(&pointer(&c.path)) {
                    for (vn, mp) in mounts(cv) {
                        if mp == "/dev/shm"
                            && let Some(Some(b)) = mv.get(&vn)
                        {
                            effective.requests.shm = Some(rightsizer_core::Bytes(*b));
                        }
                    }
                }
            } else if let Target::PodLevel = &loc.target {
                // GPUs stay on containers.
                let mut n = 0u32;
                for c in objects::containers(spec, &site.spec_path) {
                    if let Ok(Some((_, k))) = resources::gpu_of(spec.get(c.list).and_then(|l| l.get(c.index)).and_then(|cv| cv.get("resources"))) {
                        n += k;
                    }
                }
                if n > 0 {
                    effective.requests.gpu = Some(rightsizer_core::GpuShape { accel, count: n });
                }
            }
        }
        resources::default_requests_from_limits(&mut effective);
        let prior = Prior { spec: effective.clone(), defaulted, qos };

        // Seed.
        let mut manifest = ManifestFeatures::default();
        let mut extra: BTreeMap<String, ScalarValue> = BTreeMap::new();
        let code_obj = loc.code_path(&w).and_then(|p| v.pointer(&pointer(&p)));
        let (mut markers, launcher) = code_obj.map(distributed_markers).unwrap_or_default();
        let replicas = match &loc.target {
            Target::Trainer => w.trainer.as_ref().map(|t| t.nodes).unwrap_or(1),
            _ => site.map(|s| s.replicas).unwrap_or(1),
        };
        if replicas > 1 && !markers.iter().any(|m| m == "replicas") {
            markers.push("replicas".into());
        }
        if multi_group_kind && total_pods > 1 && !markers.iter().any(|m| m == "replicas") {
            markers.push("replicas".into());
        }
        let distributed = !markers.is_empty();
        manifest.distributed = distributed;
        manifest.distributed_markers = markers;
        manifest.launcher = launcher;
        extra.insert("kind".into(), ScalarValue::Str(w.kind.name().into()));
        extra.insert("replicas".into(), ScalarValue::Int(replicas as i64));
        extra.insert("backoff_limit".into(), ScalarValue::Int(retries(v, w.kind) as i64));
        if let Some(d) = active_deadline(v, w.kind, site_spec) {
            extra.insert("active_deadline_seconds".into(), ScalarValue::Int(d as i64));
        }
        extra.insert("memory_limit".into(), ScalarValue::Bool(effective.limits.memory.is_some()));
        if let Some(img) = code_obj.and_then(|c| str_val(c.get("image"))) {
            extra.insert("image".into(), ScalarValue::Str(img));
        }
        if queue_managed {
            extra.insert("queue_managed".into(), ScalarValue::Bool(true));
        }
        if cx.settings.apply_watch && !cx.admission {
            extra.insert("apply_watch".into(), ScalarValue::Bool(true));
        }
        if let (Some(spec), Some(site)) = (site_spec, site) {
            let cs = objects::containers(spec, &site.spec_path);
            let sidecars = cs.iter().filter(|c| c.role == Role::Sidecar).count() + cs.iter().filter(|c| c.role == Role::App).count().saturating_sub(1);
            manifest.sidecars = sidecars as u32;
            manifest.init_containers = cs.iter().filter(|c| c.role == Role::Init).count() as u32;
            // Init container requests (max).
            let mut init_cpu = 0u64;
            let mut init_mem = 0u64;
            for c in cs.iter().filter(|c| c.role == Role::Init) {
                if let Ok(t) = resources::container_totals(spec.get(c.list).and_then(|l| l.get(c.index)).and_then(|x| x.get("resources")), resources::Side::Requests) {
                    init_cpu = init_cpu.max(t.cpu);
                    init_mem = init_mem.max(t.memory);
                }
            }
            if init_cpu > 0 {
                extra.insert("init_cpu_millis".into(), ScalarValue::Int(init_cpu as i64));
            }
            if init_mem > 0 {
                extra.insert("init_memory_bytes".into(), ScalarValue::Int(init_mem as i64));
            }
            if let Some(sel) = crate::gpu::selector_of(spec) {
                extra.insert("gpu_selector".into(), ScalarValue::Str(format!("{}={}", sel.label, sel.value)));
            }
            let gpu_tol = spec.get("tolerations").and_then(Value::as_array).into_iter().flatten().any(|t| {
                t.get("key").and_then(Value::as_str).is_some_and(|k| k.contains("gpu") || k.contains("tpu"))
            });
            if gpu_tol {
                extra.insert("gpu_toleration".into(), ScalarValue::Bool(true));
            }
            // Memory-backed volumes and the init-tmpfs staging pattern.
            let mv = memory_volumes(spec);
            if let Target::Container(c) = &loc.target {
                let cv = v.pointer(&pointer(&c.path)).cloned().unwrap_or(Value::Null);
                let mine: Vec<(String, String)> = mounts(&cv).into_iter().filter(|(n, _)| mv.contains_key(n)).collect();
                let tmpfs_limit: u64 = mine.iter().filter(|(_, p)| p != "/dev/shm").filter_map(|(n, _)| mv.get(n).copied().flatten()).sum();
                if tmpfs_limit > 0 {
                    extra.insert("memory_volume_limit_bytes".into(), ScalarValue::Int(tmpfs_limit as i64));
                }
                if !mine.iter().all(|(_, p)| p == "/dev/shm") && !mine.is_empty() {
                    extra.insert("memory_volumes".into(), ScalarValue::Int(mine.iter().filter(|(_, p)| p != "/dev/shm").count() as i64));
                }
                if c.role == Role::App {
                    let staged = cs.iter().filter(|o| o.role == Role::Init).any(|ic| {
                        let icv = spec.get(ic.list).and_then(|l| l.get(ic.index)).cloned().unwrap_or(Value::Null);
                        mounts(&icv).iter().any(|(n, p)| p != "/dev/shm" && mine.iter().any(|(m, mp)| m == n && mp != "/dev/shm"))
                    });
                    if staged {
                        extra.insert("staging".into(), ScalarValue::Str("init-tmpfs".into()));
                        if tmpfs_limit > 0 {
                            manifest.memory_backed_bytes = Some(rightsizer_core::Bytes(tmpfs_limit));
                        }
                    }
                }
                extra.insert("role".into(), ScalarValue::Str(match c.role {
                    Role::Init => "init",
                    Role::Sidecar => "sidecar",
                    Role::App => "app",
                }
                .into()));
            }
            // KubeRay params.
            if let Some(rg) = &site.ray
                && let Some(params) = v.pointer(&pointer(&rg.params_path)).and_then(Value::as_object)
            {
                for p in ["num-cpus", "num-gpus", "memory"] {
                    if let Some(val) = params.get(p) {
                        let s = match val {
                            Value::String(s) => s.clone(),
                            other => other.to_string(),
                        };
                        extra.insert(format!("ray_{}", p.replace('-', "_")), ScalarValue::Str(s));
                    }
                }
            }
        }
        // Code.
        let mut code = code_obj.map(|c| CodeRef::Info { info: CodeInfo { digest: code_digest(c), tokens: vec![], source: None } });
        if code.is_none()
            && let Some(spec) = site_spec
        {
            code = Some(CodeRef::Info { info: CodeInfo { digest: code_digest(&serde_json::json!({"containers": spec.get("containers")})), tokens: vec![], source: None } });
        }
        let template_ann = site.map(|s| objects::string_map(v, &format!("{}/annotations", pointer(&s.meta_path)))).unwrap_or_default();
        if let Some(cm) = annotations.get(CODE_CONFIGMAP).or_else(|| template_ann.get(CODE_CONFIGMAP)) {
            extra.insert("code_configmap".into(), ScalarValue::Str(format!("{}/{}", cx.namespace, cm.trim())));
        }
        let inputs = annotations.get(INPUTS).or_else(|| template_ann.get(INPUTS)).map(|s| parse_inputs(s)).unwrap_or_default();
        if cx.admission && !inputs.is_empty() {
            extra.insert("inputs_hash_only".into(), ScalarValue::Bool(true));
        }
        manifest.extra = extra;
        let mut seed_labels = labels.clone();
        if let Some(s) = site {
            for (k, val) in objects::string_map(v, &format!("{}/labels", pointer(&s.meta_path))) {
                seed_labels.entry(k).or_insert(val);
            }
        }
        let seed = ContextSeed { inputs, code, manifest, labels: seed_labels };

        // Constraints.
        let mut constraints = Constraints::default();
        if queue_managed && effective.requests.gpu.is_some() {
            constraints.held.push(Hold::with(Axis::GpuType, HoldReason::Platform, "queue-managed: flavors are the queue's choice"));
        }
        if let Some(rc) = declared_res.and_then(|r| r.get("claims")).and_then(Value::as_array)
            && !rc.is_empty()
        {
            constraints.held.push(Hold::with(Axis::GpuCount, HoldReason::Platform, "DRA resource claims"));
        }

        let parts = loc.key_parts.join("/");
        let key_suffix = format!("{base}/{parts}");
        let first_app = matches!(&loc.target, Target::Container(c) if c.role == Role::App && c.index == 0);
        let key = if first_app { flyte.clone() } else { None };
        let id = unit_id(doc, cx.multi, &loc.local_id);
        units.push(Unit {
            id: UnitId(id.clone()),
            group: if w.kind.is_template() { Some(GroupId(id)) } else { None },
            key_suffix,
            prior,
            seed,
            constraints,
            distributed,
            key,
        });
    }
    if units.is_empty() {
        let reason = if !sized_ids.is_empty() { SkipReason::AlreadySized { decision_id: annotations.get(DECISION_ID).cloned() } } else { SkipReason::NoUnits };
        return Ok(skip(tenant, reason));
    }
    Ok(Extracted { tenant, units, skip: None })
}

/// Declared (as written) spec of a unit, for render and restore.
pub fn declared_spec(v: &Value, w: &Workload, loc: &UnitLoc, accel: &AcceleratorMap) -> std::result::Result<ResourceSpec, String> {
    let site_spec = loc.site.and_then(|i| v.pointer(&pointer(&w.sites[i].spec_path)));
    let a = match (&loc.target, site_spec) {
        (Target::Trainer, _) | (_, None) => AcceleratorId::UnknownGpu,
        (_, Some(s)) => crate::gpu::accel_of(s, accel),
    };
    resources::spec_of(v.pointer(&pointer(&loc.resources_path(w))), a)
}
