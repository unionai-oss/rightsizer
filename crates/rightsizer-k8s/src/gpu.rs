//! GPUs: node-label names ⇄ canonical accelerator ids (SPEC §5.1.1), the GPU
//! type a pod spec selects, and the opt-in GPU type change (kubernetes.md
//! §3.5): a node-selector term intersected with the existing required node
//! affinity; an empty intersection holds the type.

use rightsizer_core::AcceleratorId;
use rightsizer_plugin::AcceleratorMap;
use serde_json::Value;

use crate::edit::{Op, Path, join};
use crate::segs;

/// Node labels that name a GPU type, in lookup order.
pub const GPU_LABELS: [&str; 4] =
    ["nvidia.com/gpu.product", "cloud.google.com/gke-accelerator", "karpenter.k8s.aws/instance-gpu-name", "cloud.google.com/gke-tpu-accelerator"];

const NV: &str = "nvidia.com/gpu.product";
const GKE: &str = "cloud.google.com/gke-accelerator";
const KARP: &str = "karpenter.k8s.aws/instance-gpu-name";
const TPU: &str = "cloud.google.com/gke-tpu-accelerator";

/// `(label, canonical, native)` for the built-in names; the first native per
/// (label, canonical) is the one written on a type change.
pub fn builtin_names() -> &'static [(&'static str, AcceleratorId, &'static str)] {
    use AcceleratorId::*;
    &[
        (NV, A100_80G, "NVIDIA-A100-SXM4-80GB"),
        (NV, A100_80G, "NVIDIA-A100-80GB-PCIe"),
        (NV, A100_40G, "NVIDIA-A100-SXM4-40GB"),
        (NV, A100_40G, "NVIDIA-A100-PCIE-40GB"),
        (NV, H100_80G, "NVIDIA-H100-80GB-HBM3"),
        (NV, H100_80G, "NVIDIA-H100-PCIe"),
        (NV, H200_141G, "NVIDIA-H200"),
        (NV, B200_180G, "NVIDIA-B200"),
        (NV, L4_24G, "NVIDIA-L4"),
        (NV, L40S_48G, "NVIDIA-L40S"),
        (NV, A10G_24G, "NVIDIA-A10G"),
        (NV, A10_24G, "NVIDIA-A10"),
        (NV, T4_16G, "Tesla-T4"),
        (NV, MI300X_192G, "AMD-Instinct-MI300X"),
        (GKE, T4_16G, "nvidia-tesla-t4"),
        (GKE, L4_24G, "nvidia-l4"),
        (GKE, A100_40G, "nvidia-tesla-a100"),
        (GKE, A100_80G, "nvidia-a100-80gb"),
        (GKE, H100_80G, "nvidia-h100-80gb"),
        (GKE, H100_80G, "nvidia-h100-mega-80gb"),
        (GKE, H200_141G, "nvidia-h200-141gb"),
        (GKE, B200_180G, "nvidia-b200"),
        (KARP, T4_16G, "t4"),
        (KARP, L4_24G, "l4"),
        (KARP, A10G_24G, "a10g"),
        (KARP, L40S_48G, "l40s"),
        (KARP, A100_40G, "a100"),
        (KARP, H100_80G, "h100"),
        (KARP, H200_141G, "h200"),
        (KARP, B200_180G, "b200"),
        (TPU, TPU_V5E, "tpu-v5-lite-podslice"),
        (TPU, TPU_V5P, "tpu-v5p-slice"),
        (TPU, TPU_V6E, "tpu-v6e-slice"),
    ]
}

/// The built-in map of native names (all of [`GPU_LABELS`]) to canonical ids.
pub fn default_accelerators() -> AcceleratorMap {
    let mut m = AcceleratorMap::default();
    for (_, id, n) in builtin_names() {
        m.insert(*id, *n);
    }
    m
}

/// The native value of `label` for `id`: a built-in name for that label,
/// else the map's native name.
pub fn native_for(label: &str, id: AcceleratorId, map: &AcceleratorMap) -> Option<String> {
    builtin_names().iter().find(|(l, i, _)| *l == label && *i == id).map(|(_, _, n)| n.to_string()).or_else(|| map.native(id).map(str::to_string))
}

/// The built-in map extended by configured `native → canonical` pairs.
pub fn accelerator_map(extra: &std::collections::BTreeMap<String, String>) -> AcceleratorMap {
    let mut m = default_accelerators();
    for (native, canon) in extra {
        if let Ok(id) = canon.parse::<AcceleratorId>() {
            m.from_native.insert(native.clone(), id);
            m.to_native.entry(id).or_insert_with(|| native.clone());
        }
    }
    m
}

/// A GPU type requirement found in a pod spec.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GpuSelector {
    /// The label.
    pub label: String,
    /// The native value.
    pub value: String,
    /// Where: `nodeSelector` or `affinity`.
    pub source: &'static str,
}

/// The GPU type label a pod spec requires: a `nodeSelector` entry, or a
/// required node-affinity `In` with a single value present in every term.
pub fn selector_of(spec: &Value) -> Option<GpuSelector> {
    for l in GPU_LABELS {
        if let Some(v) = spec.get("nodeSelector").and_then(|n| n.get(l)).and_then(Value::as_str) {
            return Some(GpuSelector { label: l.into(), value: v.into(), source: "nodeSelector" });
        }
    }
    let terms = spec.pointer("/affinity/nodeAffinity/requiredDuringSchedulingIgnoredDuringExecution/nodeSelectorTerms").and_then(Value::as_array)?;
    for l in GPU_LABELS {
        let mut found: Option<String> = None;
        let mut all = !terms.is_empty();
        for t in terms {
            let vals = t.get("matchExpressions").and_then(Value::as_array).into_iter().flatten().find(|e| {
                e.get("key").and_then(Value::as_str) == Some(l) && e.get("operator").and_then(Value::as_str) == Some("In")
            });
            match vals.and_then(|e| e.get("values")).and_then(Value::as_array) {
                Some(vs) if vs.len() == 1 => {
                    let v = vs[0].as_str().unwrap_or_default().to_string();
                    if found.as_ref().is_some_and(|f| *f != v) {
                        all = false;
                    }
                    found = Some(v);
                }
                _ => all = false,
            }
        }
        if all && let Some(v) = found {
            return Some(GpuSelector { label: l.into(), value: v, source: "affinity" });
        }
    }
    None
}

/// The canonical accelerator a pod spec requests: from its GPU selector, or
/// `unknown-gpu`.
pub fn accel_of(spec: &Value, map: &AcceleratorMap) -> AcceleratorId {
    selector_of(spec).map(|s| map.canonical(&s.value)).unwrap_or(AcceleratorId::UnknownGpu)
}

/// Whether a node-selector requirement `key ∈ values` term is compatible
/// with `key = value`.
fn term_admits(term: &Value, key: &str, value: &str) -> bool {
    for e in term.get("matchExpressions").and_then(Value::as_array).into_iter().flatten() {
        if e.get("key").and_then(Value::as_str) != Some(key) {
            continue;
        }
        let vals: Vec<&str> = e.get("values").and_then(Value::as_array).into_iter().flatten().filter_map(Value::as_str).collect();
        let ok = match e.get("operator").and_then(Value::as_str).unwrap_or("") {
            "In" => vals.contains(&value),
            "NotIn" => !vals.contains(&value),
            "Exists" => true,
            "DoesNotExist" => false,
            "Gt" | "Lt" => false,
            _ => false,
        };
        if !ok {
            return false;
        }
    }
    true
}

/// Why a GPU type change is held.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TypeChangeHeld {
    /// GPU type changes are off.
    Disabled,
    /// Queue-managed (Kueue chooses flavors).
    QueueManaged,
    /// No native name for the target.
    NoNativeName,
    /// The requirement does not intersect the pod's node constraints.
    EmptyIntersection,
}

/// Ops that select `target` on the node, intersected with the pod spec's
/// existing constraints: the `nodeSelector` entry is set (it ANDs with
/// node affinity); required node-affinity terms that exclude the target
/// are dropped; if none remains, the type is held.
pub fn type_change_ops(spec: &Value, spec_path: &Path, label: &str, target: AcceleratorId, map: &AcceleratorMap) -> Result<Vec<Op>, TypeChangeHeld> {
    let native = native_for(label, target, map).ok_or(TypeChangeHeld::NoNativeName)?;
    let mut ops = Vec::new();
    // Any other GPU label pinned to a different type in nodeSelector would conflict.
    if let Some(ns) = spec.get("nodeSelector").and_then(Value::as_object) {
        for l in GPU_LABELS {
            if l == label {
                continue;
            }
            if let Some(v) = ns.get(l).and_then(Value::as_str)
                && map.canonical(v) != target
            {
                return Err(TypeChangeHeld::EmptyIntersection);
            }
        }
    }
    if let Some(terms) = spec.pointer("/affinity/nodeAffinity/requiredDuringSchedulingIgnoredDuringExecution/nodeSelectorTerms").and_then(Value::as_array) {
        let keep: Vec<Value> = terms.iter().filter(|t| term_admits(t, label, &native)).cloned().collect();
        if keep.is_empty() {
            return Err(TypeChangeHeld::EmptyIntersection);
        }
        if keep.len() != terms.len() {
            ops.push(Op::Set {
                path: join(spec_path, &segs!["affinity", "nodeAffinity", "requiredDuringSchedulingIgnoredDuringExecution", "nodeSelectorTerms"]),
                value: Value::Array(keep),
            });
        }
    }
    ops.push(Op::set(join(spec_path, &segs!["nodeSelector", label]), native));
    Ok(ops)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::path;
    use serde_json::json;

    #[test]
    fn selector_detection() {
        let m = default_accelerators();
        let s = json!({"nodeSelector": {"cloud.google.com/gke-accelerator": "nvidia-l4"}});
        assert_eq!(accel_of(&s, &m), AcceleratorId::L4_24G);
        let a = json!({"affinity": {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [
            {"matchExpressions": [{"key": "nvidia.com/gpu.product", "operator": "In", "values": ["NVIDIA-A100-SXM4-80GB"]}]}]}}}});
        assert_eq!(accel_of(&a, &m), AcceleratorId::A100_80G);
        assert_eq!(accel_of(&json!({}), &m), AcceleratorId::UnknownGpu);
        assert_eq!(accel_of(&json!({"nodeSelector": {"nvidia.com/gpu.product": "Weird-GPU"}}), &m), AcceleratorId::UnknownGpu);
    }

    #[test]
    fn type_change_intersection() {
        let m = default_accelerators();
        let sp = path!["spec"];
        let ok = type_change_ops(&json!({}), &sp, "nvidia.com/gpu.product", AcceleratorId::L4_24G, &m).unwrap();
        assert_eq!(ok, vec![Op::set(path!["spec", "nodeSelector", "nvidia.com/gpu.product"], "NVIDIA-L4")]);
        let excl = json!({"affinity": {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [
            {"matchExpressions": [{"key": "nvidia.com/gpu.product", "operator": "In", "values": ["NVIDIA-A100-SXM4-80GB"]}]}]}}}});
        assert_eq!(type_change_ops(&excl, &sp, "nvidia.com/gpu.product", AcceleratorId::L4_24G, &m), Err(TypeChangeHeld::EmptyIntersection));
        let partial = json!({"affinity": {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [
            {"matchExpressions": [{"key": "nvidia.com/gpu.product", "operator": "In", "values": ["NVIDIA-A100-SXM4-80GB"]}]},
            {"matchExpressions": [{"key": "nvidia.com/gpu.product", "operator": "NotIn", "values": ["Tesla-T4"]}]}]}}}});
        let ops = type_change_ops(&partial, &sp, "nvidia.com/gpu.product", AcceleratorId::L4_24G, &m).unwrap();
        assert_eq!(ops.len(), 2);
        let conflict = json!({"nodeSelector": {"cloud.google.com/gke-accelerator": "nvidia-tesla-a100"}});
        assert!(type_change_ops(&conflict, &sp, "nvidia.com/gpu.product", AcceleratorId::L4_24G, &m).is_err());
    }
}
