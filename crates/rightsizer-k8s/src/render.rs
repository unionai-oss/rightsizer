//! `render` (kubernetes.md §3.5): decisions → resource edits under the S2
//! limits policy, KubeRay `rayStartParams` kept consistent, the signed prior
//! and decision annotations; then a JSON Patch (admission) or minimal text
//! edits (YAML files).

use std::collections::BTreeMap;

use rightsizer_core::constraints::{apply_limits, whole_cpu_required};
use rightsizer_core::{AcceleratorId, Axis, AxisSet, Bytes, MilliCpu, Mode, Prior, Requests, ResourceSpec, UnitId};
use rightsizer_plugin::{AcceleratorMap, Patch, PluginError, Result, TextEdit};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::edit::{self, Op, Path, Seg, join, pointer};
use crate::extract::{self, DECISION_ID, RECOMMENDATION, Target, UnitLoc};
use crate::objects::{self, Kind, Submission, Workload};
use crate::path;
use crate::segs;
use crate::prior::{self, PRIOR_ANNOTATION, SigningKeys};
use crate::resources::{self, bytes_value, cpu_value};
use crate::settings::K8sSettings;

/// One unit's decision to render.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct UnitRender {
    /// Unit id (as extracted).
    pub unit: UnitId,
    /// Decided spec (requests are used; limits are recomputed under S2).
    pub spec: ResourceSpec,
    /// Axes filled by platform defaults (`Unit.prior.defaulted`), kept in the signed prior.
    #[serde(default)]
    pub defaulted: AxisSet,
    /// The unit's decision id, if it differs from the object's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_id: Option<String>,
}

/// Output form.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Output {
    /// JSON Patch for objects, text edits for documents.
    #[default]
    Auto,
    /// RFC 6902.
    JsonPatch,
    /// Byte-range edits.
    TextEdits,
}

/// A render request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RenderRequest {
    /// The effective mode: `enforce` changes resources; `observe` and
    /// `recommend` write an annotation-only patch; `off` renders nothing.
    pub mode: Mode,
    /// The object's decision id (a fresh ULID when `None`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_id: Option<String>,
    /// Decisions.
    pub units: Vec<UnitRender>,
    /// Write `rightsizer.io/*` annotations (default true).
    #[serde(default = "yes")]
    pub annotate: bool,
    /// Output form.
    #[serde(default)]
    pub output: Output,
}

fn yes() -> bool {
    true
}

impl RenderRequest {
    /// An enforce request from `(unit, spec)` pairs.
    pub fn enforce(decisions: &[(UnitId, ResourceSpec)]) -> Self {
        RenderRequest {
            mode: Mode::Enforce,
            decision_id: None,
            units: decisions.iter().map(|(u, s)| UnitRender { unit: u.clone(), spec: s.clone(), ..Default::default() }).collect(),
            annotate: true,
            output: Output::Auto,
        }
    }
}

/// What render did besides the patch.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Rendered {
    /// The patch.
    pub patch: Patch,
    /// What each unit will run with (after S2), by unit id.
    pub applied: BTreeMap<UnitId, ResourceSpec>,
    /// Axes render held, with why (GPU count, GPU type).
    pub held: Vec<(UnitId, Axis, String)>,
    /// The decision id written.
    pub decision_id: Option<String>,
}

/// Inputs render needs besides the request.
pub struct RenderCtx<'a> {
    /// Accelerator names.
    pub accel: &'a AcceleratorMap,
    /// Signing keys (enforce requires them).
    pub keys: Option<&'a SigningKeys>,
    /// Settings (GPU type change).
    pub settings: &'a K8sSettings,
}

fn ann_path(meta: &Path, key: &str) -> Path {
    join(meta, &[Seg::from("annotations"), Seg::from(key)])
}

/// Ray parameter derived from resources: `num-cpus` = whole cores of the
/// CPU limit (else request), `memory` = bytes of the memory limit (else request).
fn ray_derived(param: &str, s: &ResourceSpec) -> Option<String> {
    match param {
        "num-cpus" => s.limits.cpu.or(s.requests.cpu).map(|c| (c.0.div_ceil(1000)).to_string()),
        "memory" => s.limits.memory.or(s.requests.memory).map(|b| b.0.to_string()),
        _ => None,
    }
}

fn param_text(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.trim().trim_matches('"').to_string()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn param_eq(param: &str, a: &str, b: &str) -> bool {
    match param {
        "num-cpus" => a.parse::<f64>().ok() == b.parse::<f64>().ok(),
        _ => a.parse::<u64>().ok().zip(b.parse::<u64>().ok()).is_some_and(|(x, y)| x == y) || resources::bytes_of(&Value::String(a.into())).ok() == resources::bytes_of(&Value::String(b.into())).ok(),
    }
}

/// The S2 target of one unit: requests from the decision (CPU and memory
/// only; GPU, ephemeral storage and shm stay), whole CPUs kept for a
/// Guaranteed whole-CPU prior, limits by `apply_limits` against the
/// *declared* spec (so no limit is ever added).
pub fn s2_target(declared: &ResourceSpec, qos: rightsizer_core::QosClass, decided: &Requests) -> ResourceSpec {
    let mut dp = declared.clone();
    resources::default_requests_from_limits(&mut dp);
    let prior = Prior { spec: dp.clone(), defaulted: AxisSet::empty(), qos: Some(qos) };
    let mut req = dp.requests.clone();
    if let Some(c) = decided.cpu {
        req.cpu = Some(if whole_cpu_required(&prior) { c.ceil_whole() } else { c });
    }
    if let Some(m) = decided.memory {
        req.memory = Some(m);
    }
    // A Guaranteed prior needs both axes; it stays Guaranteed.
    let limits = apply_limits(&prior, &req);
    ResourceSpec { requests: req, limits }
}

struct ObjectPlan {
    ops: Vec<Op>,
    resource_ops: usize,
}

fn q_eq_cpu(a: Option<MilliCpu>, b: Option<MilliCpu>) -> bool {
    a == b
}
fn q_eq_mem(a: Option<Bytes>, b: Option<Bytes>) -> bool {
    a == b
}

/// Ops for one unit; returns (ops, applied spec).
fn unit_ops(v: &Value, w: &Workload, loc: &UnitLoc, d: &UnitRender, cx: &RenderCtx<'_>, held: &mut Vec<(UnitId, Axis, String)>, ray_orig: &mut BTreeMap<String, Option<String>>) -> Result<(Vec<Op>, ResourceSpec)> {
    let declared = extract::declared_spec(v, w, loc, cx.accel).map_err(PluginError::InvalidNative)?;
    let site_spec = loc.site.and_then(|i| v.pointer(&pointer(&w.sites[i].spec_path)));
    let qos = site_spec.map(resources::pod_qos).unwrap_or(rightsizer_core::QosClass::Burstable);
    let target = s2_target(&declared, qos, &d.spec.requests);
    let res = loc.resources_path(w);
    let mut ops = Vec::new();
    // GPU: count is never changed (S2, SPEC §5.4).
    if let (Some(dg), Some(pg)) = (d.spec.requests.gpu, declared.requests.gpu) {
        if dg.count != pg.count {
            held.push((d.unit.clone(), Axis::GpuCount, "GPU count is held (S2)".into()));
        }
        if dg.accel != pg.accel {
            let queue = objects::string_map(v, "/metadata/labels").contains_key(extract::KUEUE_QUEUE) || w.kind == Kind::VolcanoJob;
            let why = if !cx.settings.allow_gpu_type_change {
                Some("GPU type changes are off".to_string())
            } else if queue {
                Some("queue-managed".to_string())
            } else if pg.accel == AcceleratorId::UnknownGpu {
                Some("unknown-gpu prior".to_string())
            } else {
                match (site_spec, loc.site) {
                    (Some(spec), Some(si)) => match crate::gpu::type_change_ops(spec, &w.sites[si].spec_path, &cx.settings.gpu_product_label, dg.accel, cx.accel) {
                        Ok(more) => {
                            ops.extend(more);
                            None
                        }
                        Err(e) => Some(format!("GPU type held: {e:?}")),
                    },
                    _ => Some("no pod spec".to_string()),
                }
            };
            if let Some(w) = why {
                held.push((d.unit.clone(), Axis::GpuType, w));
            }
        }
    }
    let cur_req_cpu = declared.requests.cpu.or(declared.limits.cpu);
    let cur_req_mem = declared.requests.memory.or(declared.limits.memory);
    if !q_eq_cpu(target.requests.cpu, cur_req_cpu)
        && let Some(c) = target.requests.cpu
    {
        ops.push(Op::Set { path: join(&res, &segs!["requests", "cpu"]), value: cpu_value(c) });
    }
    if !q_eq_mem(target.requests.memory, cur_req_mem)
        && let Some(m) = target.requests.memory
    {
        ops.push(Op::Set { path: join(&res, &segs!["requests", "memory"]), value: bytes_value(m) });
    }
    if declared.limits.cpu.is_some()
        && !q_eq_cpu(target.limits.cpu, declared.limits.cpu)
        && let Some(c) = target.limits.cpu
    {
        ops.push(Op::Set { path: join(&res, &segs!["limits", "cpu"]), value: cpu_value(c) });
    }
    if declared.limits.memory.is_some()
        && !q_eq_mem(target.limits.memory, declared.limits.memory)
        && let Some(m) = target.limits.memory
    {
        ops.push(Op::Set { path: join(&res, &segs!["limits", "memory"]), value: bytes_value(m) });
    }
    // KubeRay: rayStartParams change together with the Ray container.
    if let (Some(si), Target::Container(c)) = (loc.site, &loc.target)
        && let Some(rg) = &w.sites[si].ray
        && c.role == objects::Role::App
        && c.index == 0
        && let Some(params) = v.pointer(&pointer(&rg.params_path)).and_then(Value::as_object)
    {
        let mut dp = declared.clone();
        resources::default_requests_from_limits(&mut dp);
        for p in ["num-cpus", "memory"] {
            let Some(orig) = params.get(p).and_then(param_text) else { continue };
            let (Some(was), Some(now)) = (ray_derived(p, &dp), ray_derived(p, &target)) else { continue };
            // Only keep parameters consistent that were consistent: a head
            // with `num-cpus: "0"` stays unschedulable for tasks.
            if param_eq(p, &orig, &was) && !param_eq(p, &orig, &now) {
                ray_orig.insert(format!("{}#{p}", loc.local_id), Some(orig.clone()));
                ops.push(Op::Set { path: join(&rg.params_path, &[Seg::from(p)]), value: Value::String(now) });
            }
        }
    }
    let mut applied = target;
    applied.requests.gpu = declared.requests.gpu;
    Ok((ops, applied))
}

fn recommendation(mode: Mode, id: &str, applied: &BTreeMap<String, ResourceSpec>) -> String {
    let mut units = serde_json::Map::new();
    for (u, s) in applied {
        let mut r = serde_json::Map::new();
        if let Some(c) = s.requests.cpu {
            r.insert("cpu".into(), cpu_value(c));
        }
        if let Some(m) = s.requests.memory {
            r.insert("memory".into(), bytes_value(m));
        }
        if let Some(g) = s.requests.gpu {
            r.insert("gpu".into(), Value::String(g.to_string()));
        }
        units.insert(u.clone(), Value::Object(r));
    }
    let j = serde_json::json!({"v": 1, "mode": mode.as_str(), "id": id, "units": units});
    let s = j.to_string();
    if s.len() <= 8192 { s } else { serde_json::json!({"v": 1, "mode": mode.as_str(), "id": id, "units_count": applied.len()}).to_string() }
}

/// Render a request against a parsed submission.
pub fn render(sub: &Submission, req: &RenderRequest, cx: &RenderCtx<'_>) -> Result<Rendered> {
    let mut out = Rendered::default();
    if req.mode == Mode::Off {
        return Ok(out);
    }
    let multi = sub.multi();
    let output = match req.output {
        Output::Auto => {
            if sub.docs.is_some() {
                Output::TextEdits
            } else {
                Output::JsonPatch
            }
        }
        o => o,
    };
    if output == Output::TextEdits && sub.docs.is_none() {
        return Err(PluginError::Unsupported("text edits need a document".into()));
    }
    let decision_id = req.decision_id.clone().unwrap_or_else(|| ulid::Ulid::from_datetime(std::time::SystemTime::now()).to_string());
    // Group decisions by object.
    let mut by_obj: BTreeMap<usize, Vec<&UnitRender>> = BTreeMap::new();
    for d in &req.units {
        let (doc, _) = extract::split_unit_id(&d.unit.0, multi);
        let oi = match doc {
            Some(i) => sub.objects.iter().position(|o| o.doc_index == Some(i)).ok_or_else(|| PluginError::InvalidPatch(format!("unknown unit {}", d.unit)))?,
            None => 0,
        };
        by_obj.entry(oi).or_default().push(d);
    }
    let mut json_ops = Vec::new();
    let mut text_edits: Vec<TextEdit> = Vec::new();
    for (oi, decisions) in by_obj {
        let obj = &sub.objects[oi];
        let v = &obj.value;
        let w = objects::workload(v).ok_or_else(|| PluginError::Unsupported("not a workload".into()))?;
        let locs = extract::locate(v, &w);
        let existing = extract::signed_prior(v, cx.keys);
        let annotations = objects::string_map(v, "/metadata/annotations");
        let mut plan = ObjectPlan { ops: Vec::new(), resource_ops: 0 };
        let mut applied: BTreeMap<String, ResourceSpec> = BTreeMap::new();
        let mut new_prior = existing.clone().unwrap_or_default();
        let mut ray_orig: BTreeMap<String, Option<String>> = BTreeMap::new();
        for d in &decisions {
            let (_, local) = extract::split_unit_id(&d.unit.0, multi);
            let loc = locs.iter().find(|l| l.local_id == local).ok_or_else(|| PluginError::InvalidPatch(format!("unknown unit {}", d.unit)))?;
            let declared = extract::declared_spec(v, &w, loc, cx.accel).map_err(PluginError::InvalidNative)?;
            let (ops, spec) = unit_ops(v, &w, loc, d, cx, &mut out.held, &mut ray_orig)?;
            out.applied.insert(d.unit.clone(), if req.mode == Mode::Enforce { spec.clone() } else { declared.clone() });
            applied.insert(loc.local_id.clone(), spec);
            if req.mode == Mode::Enforce {
                plan.resource_ops += ops.len();
                plan.ops.extend(ops);
                if !new_prior.prior.contains_key(&loc.local_id) {
                    new_prior.prior.insert(loc.local_id.clone(), declared);
                    if !d.defaulted.is_empty() {
                        new_prior.defaulted.insert(loc.local_id.clone(), d.defaulted);
                    }
                }
            }
        }
        for (k, val) in ray_orig {
            let (unit, param) = k.split_once('#').expect("ray key");
            new_prior.ray.entry(unit.to_string()).or_default().entry(param.to_string()).or_insert(val);
        }
        let meta = path!["metadata"];
        if req.mode == Mode::Enforce {
            let covered = existing.as_ref().is_some_and(|e| decisions.iter().all(|d| e.prior.contains_key(extract::split_unit_id(&d.unit.0, multi).1)));
            if plan.resource_ops == 0 && covered {
                continue;
            }
            if req.annotate {
                let keys = cx.keys.ok_or_else(|| PluginError::Internal("enforce needs a signing key ([k8s] signing_key_file)".into()))?;
                plan.ops.push(Op::set(ann_path(&meta, PRIOR_ANNOTATION), prior::sign(&new_prior, keys)));
                plan.ops.push(Op::set(ann_path(&meta, DECISION_ID), decision_id.clone()));
                plan.ops.push(Op::set(ann_path(&meta, RECOMMENDATION), recommendation(req.mode, &decision_id, &applied)));
            }
        } else {
            // observe / recommend: annotation-only, once.
            if !req.annotate || annotations.contains_key(DECISION_ID) {
                continue;
            }
            plan.ops.push(Op::set(ann_path(&meta, DECISION_ID), decision_id.clone()));
            plan.ops.push(Op::set(ann_path(&meta, RECOMMENDATION), recommendation(req.mode, &decision_id, &applied)));
        }
        // Children carry the marker so pods and CronJob-created Jobs are not
        // sized again, and the watcher joins outcomes (admission only: files
        // in Git would churn).
        if output == Output::JsonPatch && req.annotate && w.kind.is_template() {
            for site in &w.sites {
                plan.ops.push(Op::set(ann_path(&site.meta_path, DECISION_ID), decision_id.clone()));
            }
            if w.kind == Kind::CronJob {
                plan.ops.push(Op::set(ann_path(&path!["spec", "jobTemplate", "metadata"], DECISION_ID), decision_id.clone()));
            }
        }
        if plan.ops.is_empty() {
            continue;
        }
        match output {
            Output::JsonPatch => json_ops.extend(edit::to_json_patch(v, &plan.ops).map_err(|e| PluginError::InvalidPatch(e.to_string()))?),
            Output::TextEdits => {
                let (src, docs) = sub.docs.as_ref().expect("docs");
                let di = obj.doc_index.expect("doc index");
                text_edits.extend(edit::to_text_edits(src, &docs[di], &plan.ops).map_err(|e| PluginError::InvalidPatch(e.to_string()))?);
            }
            Output::Auto => unreachable!(),
        }
        out.decision_id = Some(decision_id.clone());
    }
    out.patch = match output {
        Output::JsonPatch if !json_ops.is_empty() => Patch::JsonPatch { ops: json_ops },
        Output::TextEdits if !text_edits.is_empty() => Patch::TextEdits { edits: text_edits },
        _ => Patch::None,
    };
    if out.patch.is_none() {
        out.decision_id = None;
    }
    Ok(out)
}

/// Apply a patch to a submission's objects (for validate and tests).
pub fn apply_patch(sub: &Submission, patch: &Patch) -> Result<Vec<Value>> {
    match patch {
        Patch::None => Ok(sub.objects.iter().map(|o| o.value.clone()).collect()),
        Patch::JsonPatch { ops } => {
            if sub.objects.len() != 1 {
                return Err(PluginError::InvalidPatch("a JSON patch applies to one object".into()));
            }
            let mut doc = sub.objects[0].value.clone();
            let p: json_patch::Patch = serde_json::from_value(serde_json::to_value(ops).map_err(|e| PluginError::Internal(e.to_string()))?).map_err(|e| PluginError::InvalidPatch(e.to_string()))?;
            json_patch::patch(&mut doc, &p).map_err(|e| PluginError::InvalidPatch(e.to_string()))?;
            Ok(vec![doc])
        }
        Patch::TextEdits { edits } => {
            let (src, _) = sub.docs.as_ref().ok_or_else(|| PluginError::InvalidPatch("text edits on an object".into()))?;
            let text = rightsizer_plugin::apply_text_edits(src, edits)?;
            let docs = crate::yaml::parse(&text).map_err(|e| PluginError::InvalidPatch(format!("edited YAML does not parse: {e}")))?;
            if docs.len() != sub.objects.len() {
                return Err(PluginError::InvalidPatch("edited YAML has a different number of documents".into()));
            }
            Ok(docs.into_iter().map(|d| d.value).collect())
        }
        Patch::Options { .. } => Err(PluginError::InvalidPatch("k8s does not use option tables".into())),
    }
}
