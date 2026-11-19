//! `validate` (kubernetes.md §3.4): everything that runs after a mutating
//! webhook and could reject or silently undo the patched object.
//!
//! Checks (the `Rejection.reason` names; the caller records
//! `Prior{reason = "validation:<check>"}`):
//! `pod_spec`, `limitrange`, `qos`, `autopilot`, `node_shape`,
//! `resource_quota`, `kueue`.
//!
//! Capacity checks (`node_shape`, `resource_quota`, `kueue`) reject only a
//! patch that makes things worse: patched exceeds capacity *and* asks for
//! more than the original on that resource. A reduction never fails them.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use rightsizer_plugin::{Patch, Rejection};
use serde_json::Value;

use crate::autopilot;
use crate::edit::pointer;
use crate::extract::KUEUE_QUEUE;
use crate::ledger::ReservationLedger;
use crate::limitrange::{self, LimitRanges};
use crate::objects::{self, Submission, Workload};
use crate::resources::{self, Side, Totals, bytes_of, cpu_of, effective_pod, list_totals};
use crate::settings::K8sSettings;

/// Check names.
pub mod check {
    /// Pod-spec invariants.
    pub const POD_SPEC: &str = "pod_spec";
    /// LimitRange.
    pub const LIMITRANGE: &str = "limitrange";
    /// QoS preservation.
    pub const QOS: &str = "qos";
    /// GKE Autopilot.
    pub const AUTOPILOT: &str = "autopilot";
    /// Effective pod request vs the largest node shape.
    pub const NODE_SHAPE: &str = "node_shape";
    /// ResourceQuota.
    pub const RESOURCE_QUOTA: &str = "resource_quota";
    /// Kueue ClusterQueue.
    pub const KUEUE: &str = "kueue";
}

/// The `Prior` reason detail for a rejection: `validation:<check>`.
pub fn prior_reason(r: &Rejection) -> String {
    format!("validation:{}", r.reason)
}

fn reject(check: &str, detail: impl Into<String>, field: Option<String>) -> Rejection {
    Rejection { reason: check.to_string(), detail: Some(detail.into()), field }
}

/// What validate needs besides the submission and the patch.
pub struct ValidateCtx<'a> {
    /// Settings with cluster inputs.
    pub settings: &'a K8sSettings,
    /// Reservation ledger.
    pub ledger: &'a dyn ReservationLedger,
    /// Trusted namespace.
    pub namespace: &'a str,
    /// Now (ledger expiry).
    pub now: DateTime<Utc>,
}

/// Pod overhead: `spec.overhead`, else the RuntimeClass's `overhead.podFixed`.
pub fn pod_overhead(spec: &Value, runtime_classes: &[Value]) -> Totals {
    if let Some(o) = spec.get("overhead")
        && let Ok(t) = list_totals(Some(o))
    {
        return t;
    }
    let Some(rc) = spec.get("runtimeClassName").and_then(Value::as_str) else { return Totals::default() };
    runtime_classes
        .iter()
        .find(|r| r.pointer("/metadata/name").and_then(Value::as_str) == Some(rc))
        .and_then(|r| list_totals(r.pointer("/overhead/podFixed")).ok())
        .unwrap_or_default()
}

/// Pod-spec invariants: unique container names, volume names and per-container
/// mount paths; at most one `/dev/shm` mount; pod-level resources cover the
/// containers' sum.
pub fn pod_spec_invariants(spec: &Value) -> Result<(), String> {
    let mut names = BTreeSet::new();
    for list in ["initContainers", "containers"] {
        for c in spec.get(list).and_then(Value::as_array).into_iter().flatten() {
            let n = c.get("name").and_then(Value::as_str).unwrap_or_default();
            if !names.insert(n.to_string()) {
                return Err(format!("duplicate container name {n:?}"));
            }
            let mut paths = BTreeSet::new();
            let mut shm = 0;
            for m in c.get("volumeMounts").and_then(Value::as_array).into_iter().flatten() {
                let p = m.get("mountPath").and_then(Value::as_str).unwrap_or_default();
                if !paths.insert(p.trim_end_matches('/').to_string()) {
                    return Err(format!("duplicate mountPath {p:?} in {n:?}"));
                }
                if p.trim_end_matches('/') == "/dev/shm" {
                    shm += 1;
                }
            }
            if shm > 1 {
                return Err(format!("/dev/shm mounted twice in {n:?}"));
            }
        }
    }
    let mut vols = BTreeSet::new();
    for v in spec.get("volumes").and_then(Value::as_array).into_iter().flatten() {
        let n = v.get("name").and_then(Value::as_str).unwrap_or_default();
        if !vols.insert(n.to_string()) {
            return Err(format!("duplicate volume name {n:?}"));
        }
    }
    if let Some(pr) = spec.get("resources").filter(|r| r.get("requests").is_some() || r.get("limits").is_some()) {
        let mut sum = Totals::default();
        for c in spec.get("containers").and_then(Value::as_array).into_iter().flatten() {
            if c.get("resources").and_then(|r| r.get("requests")).is_some() {
                let r = c.get("resources").and_then(|r| r.get("requests"));
                sum = sum.add(&list_totals(r)?);
            }
        }
        let pod = list_totals(pr.get("requests").or_else(|| pr.get("limits")))?;
        if (pod.cpu > 0 && sum.cpu > pod.cpu) || (pod.memory > 0 && sum.memory > pod.memory) {
            return Err("container requests exceed pod-level requests".into());
        }
        if let Some(pl) = pr.get("limits") {
            let pl = list_totals(Some(pl))?;
            let pq = list_totals(pr.get("requests"))?;
            if (pl.cpu > 0 && pq.cpu > pl.cpu) || (pl.memory > 0 && pq.memory > pl.memory) {
                return Err("pod-level requests exceed limits".into());
            }
        }
    }
    // Requests never above limits per container.
    for list in ["initContainers", "containers"] {
        for c in spec.get(list).and_then(Value::as_array).into_iter().flatten() {
            let res = c.get("resources");
            for (k, is_cpu) in [("cpu", true), ("memory", false)] {
                let r = res.and_then(|r| r.get("requests")).and_then(|r| r.get(k));
                let l = res.and_then(|r| r.get("limits")).and_then(|l| l.get(k));
                if let (Some(r), Some(l)) = (r, l) {
                    let bad = if is_cpu { cpu_of(r).ok() > cpu_of(l).ok() } else { bytes_of(r).ok() > bytes_of(l).ok() };
                    if bad {
                        return Err(format!("{k} request above limit in {:?}", c.get("name").and_then(Value::as_str).unwrap_or_default()));
                    }
                }
            }
        }
    }
    Ok(())
}

/// A schedulable node shape for fit checks.
#[derive(Clone, Debug, PartialEq)]
pub struct Capacity {
    /// Allocatable CPU, millicores.
    pub cpu: u64,
    /// Allocatable memory.
    pub memory: u64,
    /// Accelerators.
    pub gpu: u64,
    /// Labels (Nodes only).
    pub labels: BTreeMap<String, String>,
}

/// Node capacities from configured shapes and Node objects.
pub fn capacities(settings: &K8sSettings) -> Vec<Capacity> {
    let mut out: Vec<Capacity> = settings
        .shapes
        .iter()
        .map(|s| Capacity { cpu: s.cpu.0 as u64, memory: s.memory.0, gpu: s.gpu.map(|g| g.count as u64).unwrap_or(0), labels: BTreeMap::new() })
        .collect();
    for n in &settings.nodes {
        let a = n.pointer("/status/allocatable");
        let cpu = a.and_then(|a| a.get("cpu")).and_then(|v| cpu_of(v).ok()).map(|c| c.0 as u64).unwrap_or(0);
        let memory = a.and_then(|a| a.get("memory")).and_then(|v| bytes_of(v).ok()).map(|b| b.0).unwrap_or(0);
        let gpu = resources::GPU_RESOURCES.iter().filter_map(|g| a.and_then(|a| a.get(*g)).and_then(|v| resources::count_of(v).ok())).sum::<u32>() as u64;
        out.push(Capacity { cpu, memory, gpu, labels: objects::string_map(n, "/metadata/labels") });
    }
    out
}

fn fits(t: &Totals, c: &Capacity) -> bool {
    t.cpu <= c.cpu && t.memory <= c.memory && t.gpu_count() <= c.gpu
}

/// A pod template as validate sees it: the defaulted pod spec and its replicas.
struct PodSet {
    id: String,
    spec: Value,
    replicas: u64,
}

fn pod_sets(v: &Value, w: &Workload, lr: &LimitRanges) -> Vec<PodSet> {
    let mut out = Vec::new();
    for s in &w.sites {
        let Some(spec) = v.pointer(&pointer(&s.spec_path)) else { continue };
        let spec = if w.kind.is_template() { limitrange::default_pod_spec(spec, lr).0 } else { spec.clone() };
        out.push(PodSet { id: s.id.clone(), spec, replicas: s.replicas });
    }
    if let Some(t) = &w.trainer {
        // A TrainJob node as a one-container pod.
        let res = v.pointer(&format!("{}/resourcesPerNode", pointer(&t.path))).cloned().unwrap_or(Value::Null);
        out.push(PodSet { id: "spec.trainer".into(), spec: serde_json::json!({"containers": [{"name": "node", "resources": res}]}), replicas: t.nodes });
    }
    out
}

fn more(a: &Totals, b: &Totals) -> bool {
    a.cpu > b.cpu || a.memory > b.memory || a.ephemeral > b.ephemeral || a.gpu_count() > b.gpu_count()
}

/// Remaining quota per resource name (`hard − used`, the minimum over quotas) (`requests.cpu`, `limits.memory`, …) over all quotas.
pub fn quota_remaining(quotas: &[Value], namespace: &str) -> BTreeMap<String, i128> {
    let mut out: BTreeMap<String, i128> = BTreeMap::new();
    for q in quotas {
        if let Some(ns) = q.pointer("/metadata/namespace").and_then(Value::as_str)
            && ns != namespace
        {
            continue;
        }
        let Some(hard) = q.pointer("/status/hard").or_else(|| q.pointer("/spec/hard")).and_then(Value::as_object) else { continue };
        let used = q.pointer("/status/used").and_then(Value::as_object);
        for (k, hv) in hard {
            let canon = match k.as_str() {
                "cpu" => "requests.cpu".to_string(),
                "memory" => "requests.memory".to_string(),
                "ephemeral-storage" => "requests.ephemeral-storage".to_string(),
                other => other.to_string(),
            };
            let amount = |v: &Value| -> Option<i128> {
                if canon.ends_with("cpu") {
                    cpu_of(v).ok().map(|c| c.0 as i128)
                } else if canon.ends_with("memory") || canon.ends_with("ephemeral-storage") {
                    bytes_of(v).ok().map(|b| b.0 as i128)
                } else if resources::GPU_RESOURCES.iter().any(|g| canon.ends_with(g)) {
                    resources::count_of(v).ok().map(|c| c as i128)
                } else {
                    None
                }
            };
            let Some(h) = amount(hv) else { continue };
            let u = used.and_then(|u| u.get(k)).and_then(amount).unwrap_or(0);
            let rem = h - u;
            out.entry(canon).and_modify(|x| *x = (*x).min(rem)).or_insert(rem);
        }
    }
    out
}

fn quota_amount(name: &str, req: &Totals, lim: &Totals) -> Option<i128> {
    let gpu = |t: &Totals, g: &str| t.gpu.get(g).copied().unwrap_or(0) as i128;
    Some(match name {
        "requests.cpu" => req.cpu as i128,
        "requests.memory" => req.memory as i128,
        "requests.ephemeral-storage" => req.ephemeral as i128,
        "limits.cpu" => lim.cpu as i128,
        "limits.memory" => lim.memory as i128,
        "limits.ephemeral-storage" => lim.ephemeral as i128,
        n => {
            let g = n.strip_prefix("requests.").or_else(|| n.strip_prefix("limits."))?;
            if resources::GPU_RESOURCES.contains(&g) {
                gpu(req, g)
            } else {
                return None;
            }
        }
    })
}

/// Requests and limits of a whole object: Σ replicas × effective pod.
pub fn object_totals(v: &Value, settings: &K8sSettings, namespace: &str) -> Result<(Totals, Totals), String> {
    let Some(w) = objects::workload(v) else { return Ok((Totals::default(), Totals::default())) };
    let lr = LimitRanges::parse(&settings.limit_ranges, Some(namespace));
    let mut r = Totals::default();
    let mut l = Totals::default();
    for ps in pod_sets(v, &w, &lr) {
        let oh = pod_overhead(&ps.spec, &settings.runtime_classes);
        r = r.add(&effective_pod(&ps.spec, Side::Requests, &oh)?.times(ps.replicas));
        l = l.add(&effective_pod(&ps.spec, Side::Limits, &oh)?.times(ps.replicas));
    }
    Ok((r, l))
}

/// The ClusterQueue capacity per resource for a queue name in `namespace`:
/// `(max over flavors of nominal + borrowing, sum over flavors)`; `None` = unbounded.
/// Per-resource capacity (`None` = unbounded) and the flavor names.
type QueueCapacity = (BTreeMap<String, Option<u64>>, Vec<String>);

fn kueue_capacity(settings: &K8sSettings, queue: &str, namespace: &str) -> Option<QueueCapacity> {
    let lq = settings.local_queues.iter().find(|q| {
        q.pointer("/metadata/name").and_then(Value::as_str) == Some(queue) && q.pointer("/metadata/namespace").and_then(Value::as_str).is_none_or(|n| n == namespace)
    })?;
    let cq_name = lq.pointer("/spec/clusterQueue").and_then(Value::as_str)?;
    let cq = settings.cluster_queues.iter().find(|c| c.pointer("/metadata/name").and_then(Value::as_str) == Some(cq_name))?;
    let cohort = cq.pointer("/spec/cohort").and_then(Value::as_str).is_some();
    let mut cap: BTreeMap<String, Option<u64>> = BTreeMap::new();
    let mut flavors = Vec::new();
    for rg in cq.pointer("/spec/resourceGroups").and_then(Value::as_array).into_iter().flatten() {
        for f in rg.get("flavors").and_then(Value::as_array).into_iter().flatten() {
            if let Some(n) = f.get("name").and_then(Value::as_str) {
                flavors.push(n.to_string());
            }
            for r in f.get("resources").and_then(Value::as_array).into_iter().flatten() {
                let Some(name) = r.get("name").and_then(Value::as_str) else { continue };
                let amt = |v: Option<&Value>| -> Option<u64> {
                    let v = v?;
                    if name == "cpu" {
                        cpu_of(v).ok().map(|c| c.0 as u64)
                    } else if name == "memory" {
                        bytes_of(v).ok().map(|b| b.0)
                    } else {
                        resources::count_of(v).ok().map(|c| c as u64)
                    }
                };
                let nominal = amt(r.get("nominalQuota")).unwrap_or(0);
                let total = match (amt(r.get("borrowingLimit")), cohort) {
                    (Some(b), _) => Some(nominal + b),
                    (None, true) => None,
                    (None, false) => Some(nominal),
                };
                let e = cap.entry(name.to_string()).or_insert(Some(0));
                *e = match (*e, total) {
                    (Some(a), Some(b)) => Some(a.max(b)),
                    _ => None,
                };
            }
        }
    }
    Some((cap, flavors))
}

fn flavor_capacities(settings: &K8sSettings, flavors: &[String]) -> Vec<Capacity> {
    let caps = capacities(settings);
    let mut out = Vec::new();
    for f in flavors {
        let Some(rf) = settings.resource_flavors.iter().find(|r| r.pointer("/metadata/name").and_then(Value::as_str) == Some(f)) else { continue };
        let labels = objects::string_map(rf, "/spec/nodeLabels");
        for c in &caps {
            if !c.labels.is_empty() && labels.iter().all(|(k, v)| c.labels.get(k) == Some(v)) {
                out.push(c.clone());
            }
        }
    }
    out
}

/// Validate a patch against a submission.
pub fn validate(sub: &Submission, patch: &Patch, cx: &ValidateCtx<'_>) -> Result<(), Rejection> {
    let patched = crate::render::apply_patch(sub, patch).map_err(|e| reject(check::POD_SPEC, e.to_string(), None))?;
    let lr = LimitRanges::parse(&cx.settings.limit_ranges, Some(cx.namespace));
    let caps = capacities(cx.settings);
    for (orig_obj, new) in sub.objects.iter().zip(&patched) {
        let orig = &orig_obj.value;
        let Some(w) = objects::workload(new) else { continue };
        let Some(w0) = objects::workload(orig) else { continue };
        let before = pod_sets(orig, &w0, &lr);
        let after = pod_sets(new, &w, &lr);
        for ps in &after {
            let ps0 = before.iter().find(|b| b.id == ps.id);
            let raw = w.sites.iter().find(|s| s.id == ps.id).and_then(|s| new.pointer(&pointer(&s.spec_path))).unwrap_or(&ps.spec);
            pod_spec_invariants(raw).map_err(|e| reject(check::POD_SPEC, e, Some(ps.id.clone())))?;
            limitrange::validate_pod_spec(&ps.spec, &lr).map_err(|v| {
                reject(check::LIMITRANGE, format!("{}: {}{}", v.limit_range, v.container.map(|c| format!("{c}: ")).unwrap_or_default(), v.detail), Some(ps.id.clone()))
            })?;
            if let Some(ps0) = ps0 {
                let (q0, q1) = (resources::pod_qos(&ps0.spec), resources::pod_qos(&ps.spec));
                if q0 != q1 {
                    return Err(reject(check::QOS, format!("QoS would change from {q0:?} to {q1:?}"), Some(ps.id.clone())));
                }
            }
            if let Some(ap) = &cx.settings.autopilot {
                match autopilot::normalize_pod_spec(&ps.spec, ap) {
                    Err(e) => return Err(reject(check::AUTOPILOT, e.detail, Some(ps.id.clone()))),
                    Ok(n) if n.requests_changed => {
                        return Err(reject(check::AUTOPILOT, format!("Autopilot would change the pod: {}", n.notes.join("; ")), Some(ps.id.clone())));
                    }
                    Ok(_) => {}
                }
            }
            if !caps.is_empty() {
                let oh = pod_overhead(&ps.spec, &cx.settings.runtime_classes);
                let t = effective_pod(&ps.spec, Side::Requests, &oh).map_err(|e| reject(check::POD_SPEC, e, Some(ps.id.clone())))?;
                if !caps.iter().any(|c| fits(&t, c)) {
                    let worse = match ps0 {
                        Some(p0) => {
                            let oh0 = pod_overhead(&p0.spec, &cx.settings.runtime_classes);
                            let t0 = effective_pod(&p0.spec, Side::Requests, &oh0).unwrap_or_default();
                            more(&t, &t0)
                        }
                        None => true,
                    };
                    if worse {
                        return Err(reject(check::NODE_SHAPE, format!("effective pod request {}m CPU, {} bytes fits no node shape", t.cpu, t.memory), Some(ps.id.clone())));
                    }
                }
            }
        }
        // ResourceQuota.
        if !cx.settings.resource_quotas.is_empty() {
            let (r1, l1) = object_totals(new, cx.settings, cx.namespace).map_err(|e| reject(check::POD_SPEC, e, None))?;
            let (r0, l0) = object_totals(orig, cx.settings, cx.namespace).map_err(|e| reject(check::POD_SPEC, e, None))?;
            let (rr, rl) = cx.ledger.reserved(cx.namespace, cx.now);
            for (name, rem) in quota_remaining(&cx.settings.resource_quotas, cx.namespace) {
                let (Some(a1), Some(a0), Some(res)) = (quota_amount(&name, &r1, &l1), quota_amount(&name, &r0, &l0), quota_amount(&name, &rr, &rl)) else { continue };
                if a1 > rem - res && a1 > a0 {
                    return Err(reject(check::RESOURCE_QUOTA, format!("{name}: needs {a1}, {} remaining after reservations", rem - res), None));
                }
            }
        }
        // Kueue.
        if let Some(queue) = objects::string_map(new, "/metadata/labels").get(KUEUE_QUEUE).cloned()
            && let Some((cap, flavors)) = kueue_capacity(cx.settings, &queue, cx.namespace)
        {
            let (r1, _) = object_totals(new, cx.settings, cx.namespace).map_err(|e| reject(check::POD_SPEC, e, None))?;
            let (r0, _) = object_totals(orig, cx.settings, cx.namespace).map_err(|e| reject(check::POD_SPEC, e, None))?;
            for (name, c) in &cap {
                let Some(c) = c else { continue };
                let (a1, a0) = match name.as_str() {
                    "cpu" => (r1.cpu, r0.cpu),
                    "memory" => (r1.memory, r0.memory),
                    g => (r1.gpu.get(g).copied().unwrap_or(0), r0.gpu.get(g).copied().unwrap_or(0)),
                };
                if a1 > *c && a1 > a0 {
                    return Err(reject(check::KUEUE, format!("{name}: workload needs {a1}, ClusterQueue admits at most {c}"), None));
                }
            }
            let fcaps = flavor_capacities(cx.settings, &flavors);
            if !fcaps.is_empty() {
                for ps in &after {
                    let t = effective_pod(&ps.spec, Side::Requests, &pod_overhead(&ps.spec, &cx.settings.runtime_classes)).unwrap_or_default();
                    if !fcaps.iter().any(|c| fits(&t, c)) {
                        let t0 = before.iter().find(|b| b.id == ps.id).and_then(|b| effective_pod(&b.spec, Side::Requests, &Totals::default()).ok()).unwrap_or_default();
                        if more(&t, &t0) {
                            return Err(reject(check::KUEUE, "pod fits no node of the queue's flavors", Some(ps.id.clone())));
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

/// The increase a patch admits in its namespace (for the reservation ledger).
pub fn increase(sub: &Submission, patch: &Patch, settings: &K8sSettings, namespace: &str) -> Option<(Totals, Totals)> {
    let patched = crate::render::apply_patch(sub, patch).ok()?;
    let mut inc_r = Totals::default();
    let mut inc_l = Totals::default();
    for (o, n) in sub.objects.iter().zip(&patched) {
        let (r0, l0) = object_totals(&o.value, settings, namespace).ok()?;
        let (r1, l1) = object_totals(n, settings, namespace).ok()?;
        let d = |a: &Totals, b: &Totals| Totals {
            cpu: a.cpu.saturating_sub(b.cpu),
            memory: a.memory.saturating_sub(b.memory),
            ephemeral: a.ephemeral.saturating_sub(b.ephemeral),
            gpu: Default::default(),
        };
        inc_r = inc_r.add(&d(&r1, &r0));
        inc_l = inc_l.add(&d(&l1, &l0));
    }
    Some((inc_r, inc_l))
}

