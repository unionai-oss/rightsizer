//! GKE Autopilot normalization (managed-ml.md §6, kubernetes.md §3.4).
//!
//! Autopilot raises requests below a compute class's minimum, rejects
//! requests above its maximum, enforces a CPU:memory ratio (by raising the
//! smaller resource), rounds CPU up to 0.25 vCPU on non-bursting clusters,
//! sets limits to requests on non-bursting clusters, and caps GPU pods per
//! GPU count. `normalize_pod_spec` applies these rules to a pod spec so the
//! decision describes what will actually run.
//!
//! The tables are from the GKE documentation (2025) and can be overridden in
//! `[k8s.autopilot]`. Adjustments to pod totals are applied to the first app
//! container, a deterministic stand-in for Autopilot's own distribution.

use rightsizer_core::{AcceleratorId, Bytes};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::gpu;
use crate::resources::{Side, Totals, bytes_value, cpu_of, cpu_value, effective_pod};

/// A compute class's bounds.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ComputeClass {
    /// Name (`general-purpose`, `balanced`, `scale-out`).
    pub name: String,
    /// Minimum GiB of memory per vCPU.
    pub min_ratio: f64,
    /// Maximum GiB of memory per vCPU.
    pub max_ratio: f64,
    /// Pod minimum CPU (millicores), non-bursting.
    pub min_cpu_m: u64,
    /// Pod minimum memory (bytes), non-bursting.
    pub min_memory: u64,
    /// Pod minimum CPU (millicores), bursting clusters.
    pub min_cpu_m_bursting: u64,
    /// Pod minimum memory (bytes), bursting clusters.
    pub min_memory_bursting: u64,
    /// Pod maximum CPU (millicores).
    pub max_cpu_m: u64,
    /// Pod maximum memory (bytes).
    pub max_memory: u64,
}

/// Per-GPU-count maximums of GPU pods.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GpuLimit {
    /// Accelerator.
    pub accel: AcceleratorId,
    /// GPUs per pod.
    pub count: u32,
    /// Max CPU, millicores.
    pub max_cpu_m: u64,
    /// Max memory, bytes.
    pub max_memory: u64,
}

/// `[k8s.autopilot]`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct AutopilotSettings {
    /// Bursting cluster (GKE ≥ 1.29.2-gke.1060000 with bursting enabled).
    pub bursting: bool,
    /// Default compute class (pods may select one with the
    /// `cloud.google.com/compute-class` node selector).
    pub compute_class: String,
    /// Compute classes (built-ins when empty).
    pub classes: Vec<ComputeClass>,
    /// GPU maximums (built-ins when empty).
    pub gpu_limits: Vec<GpuLimit>,
}

impl Default for AutopilotSettings {
    fn default() -> Self {
        AutopilotSettings { bursting: true, compute_class: "general-purpose".into(), classes: Vec::new(), gpu_limits: Vec::new() }
    }
}

const GIB: u64 = 1 << 30;
const MIB: u64 = 1 << 20;

/// Built-in compute classes.
pub fn builtin_classes() -> Vec<ComputeClass> {
    vec![
        ComputeClass {
            name: "general-purpose".into(),
            min_ratio: 1.0,
            max_ratio: 6.5,
            min_cpu_m: 250,
            min_memory: 512 * MIB,
            min_cpu_m_bursting: 50,
            min_memory_bursting: 52 * MIB,
            max_cpu_m: 30_000,
            max_memory: 110 * GIB,
        },
        ComputeClass {
            name: "balanced".into(),
            min_ratio: 1.0,
            max_ratio: 8.0,
            min_cpu_m: 250,
            min_memory: 512 * MIB,
            min_cpu_m_bursting: 50,
            min_memory_bursting: 52 * MIB,
            max_cpu_m: 222_000,
            max_memory: 851 * GIB,
        },
        ComputeClass {
            name: "scale-out".into(),
            min_ratio: 4.0,
            max_ratio: 4.0,
            min_cpu_m: 250,
            min_memory: GIB,
            min_cpu_m_bursting: 50,
            min_memory_bursting: 52 * MIB,
            max_cpu_m: 54_000,
            max_memory: 216 * GIB,
        },
    ]
}

/// Built-in GPU maximums (per pod, by GPU count).
pub fn builtin_gpu_limits() -> Vec<GpuLimit> {
    use AcceleratorId::*;
    let t: &[(AcceleratorId, u32, u64, u64)] = &[
        (T4_16G, 1, 46, 202),
        (T4_16G, 2, 46, 202),
        (T4_16G, 4, 94, 422),
        (L4_24G, 1, 31, 115),
        (L4_24G, 2, 23, 83),
        (L4_24G, 4, 47, 177),
        (L4_24G, 8, 95, 363),
        (A100_40G, 1, 11, 74),
        (A100_40G, 2, 22, 148),
        (A100_40G, 4, 46, 310),
        (A100_40G, 8, 94, 635),
        (A100_40G, 16, 94, 1300),
        (A100_80G, 1, 11, 148),
        (A100_80G, 2, 22, 310),
        (A100_80G, 4, 46, 635),
        (A100_80G, 8, 94, 1300),
        (H100_80G, 8, 206, 1795),
    ];
    t.iter().map(|&(accel, count, c, m)| GpuLimit { accel, count, max_cpu_m: c * 1000, max_memory: m * GIB }).collect()
}

/// Why Autopilot would reject a pod.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AutopilotReject {
    /// Detail.
    pub detail: String,
}

/// What normalization changed.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Normalized {
    /// The normalized pod spec.
    pub spec: Value,
    /// Human-readable notes, one per rule that changed something.
    pub notes: Vec<String>,
    /// Whether any request changed (limits = requests alone does not count:
    /// Autopilot applies it to the original object as well).
    pub requests_changed: bool,
}

fn first_app_resources(spec: &mut Value) -> Option<&mut serde_json::Map<String, Value>> {
    let c = spec.get_mut("containers")?.as_array_mut()?.get_mut(0)?;
    let m = c.as_object_mut()?;
    let r = m.entry("resources").or_insert_with(|| Value::Object(Default::default()));
    if !r.is_object() {
        *r = Value::Object(Default::default());
    }
    r.as_object_mut()
}

fn bump(spec: &mut Value, resource: &str, delta: u64) {
    if delta == 0 {
        return;
    }
    let Some(r) = first_app_resources(spec) else { return };
    let req = r.entry("requests").or_insert_with(|| Value::Object(Default::default()));
    if !req.is_object() {
        *req = Value::Object(Default::default());
    }
    let req = req.as_object_mut().expect("object");
    let cur = req.get(resource).map(|v| if resource == "cpu" { cpu_of(v).map(|c| c.0 as u64).unwrap_or(0) } else { crate::resources::bytes_of(v).map(|b| b.0).unwrap_or(0) });
    let new = cur.unwrap_or(0) + delta;
    let val = if resource == "cpu" { cpu_value(rightsizer_core::MilliCpu(new.min(u32::MAX as u64) as u32)) } else { bytes_value(Bytes(new)) };
    req.insert(resource.into(), val);
}

/// Normalize a pod spec under Autopilot's rules.
pub fn normalize_pod_spec(spec: &Value, s: &AutopilotSettings) -> Result<Normalized, AutopilotReject> {
    let classes = if s.classes.is_empty() { builtin_classes() } else { s.classes.clone() };
    let class_name = spec.pointer("/nodeSelector/cloud.google.com~1compute-class").and_then(Value::as_str).unwrap_or(&s.compute_class).to_string();
    let mut out = spec.clone();
    let mut notes = Vec::new();
    let zero = Totals::default();
    let req = effective_pod(&out, Side::Requests, &zero).map_err(|e| AutopilotReject { detail: e })?;
    let gpus = req.gpu_count();
    if gpus > 0 {
        // Accelerator class: per-GPU-count maximums; no ratio rule.
        let accel = gpu::accel_of(spec, &gpu::default_accelerators());
        let limits = if s.gpu_limits.is_empty() { builtin_gpu_limits() } else { s.gpu_limits.clone() };
        if let Some(l) = limits.iter().find(|l| l.accel == accel && l.count as u64 == gpus)
            && (req.cpu > l.max_cpu_m || req.memory > l.max_memory)
        {
            return Err(AutopilotReject { detail: format!("{accel}×{gpus} pod above Autopilot maximum ({}m CPU, {} bytes)", l.max_cpu_m, l.max_memory) });
        }
    } else {
        let Some(class) = classes.iter().find(|c| c.name == class_name) else {
            return Ok(Normalized { spec: out, notes, requests_changed: false });
        };
        let (min_cpu, min_mem) = if s.bursting { (class.min_cpu_m_bursting, class.min_memory_bursting) } else { (class.min_cpu_m, class.min_memory) };
        // Minimums.
        if req.cpu < min_cpu {
            bump(&mut out, "cpu", min_cpu - req.cpu);
            notes.push(format!("cpu raised to the {class_name} minimum {min_cpu}m"));
        }
        let req = effective_pod(&out, Side::Requests, &zero).map_err(|e| AutopilotReject { detail: e })?;
        if req.memory < min_mem {
            bump(&mut out, "memory", min_mem - req.memory);
            notes.push(format!("memory raised to the {class_name} minimum"));
        }
        // Ratio: raise the smaller resource.
        let req = effective_pod(&out, Side::Requests, &zero).map_err(|e| AutopilotReject { detail: e })?;
        let gib = req.memory as f64 / GIB as f64;
        let cores = req.cpu as f64 / 1000.0;
        if cores > 0.0 && gib / cores > class.max_ratio + 1e-9 {
            let need = ((gib / class.max_ratio) * 1000.0).ceil() as u64;
            bump(&mut out, "cpu", need.saturating_sub(req.cpu));
            notes.push(format!("cpu raised to keep memory:cpu ≤ {}", class.max_ratio));
        } else if gib / cores.max(1e-9) < class.min_ratio - 1e-9 {
            let need = (cores * class.min_ratio * GIB as f64).ceil() as u64;
            // Round up to a MiB so the quantity stays readable.
            let need = need.div_ceil(MIB) * MIB;
            bump(&mut out, "memory", need.saturating_sub(req.memory));
            notes.push(format!("memory raised to keep memory:cpu ≥ {}", class.min_ratio));
        }
        // CPU rounds up to 0.25 vCPU without bursting.
        if !s.bursting {
            let req = effective_pod(&out, Side::Requests, &zero).map_err(|e| AutopilotReject { detail: e })?;
            let rounded = req.cpu.div_ceil(250) * 250;
            if rounded != req.cpu {
                bump(&mut out, "cpu", rounded - req.cpu);
                notes.push("cpu rounded up to 0.25 vCPU".into());
            }
        }
        let req = effective_pod(&out, Side::Requests, &zero).map_err(|e| AutopilotReject { detail: e })?;
        if req.cpu > class.max_cpu_m || req.memory > class.max_memory {
            return Err(AutopilotReject { detail: format!("pod above the {class_name} maximum ({}m CPU, {} bytes)", class.max_cpu_m, class.max_memory) });
        }
    }
    // Limits = requests on non-bursting clusters.
    if !s.bursting {
        let mut changed = false;
        for list in ["initContainers", "containers"] {
            if let Some(cs) = out.get_mut(list).and_then(Value::as_array_mut) {
                for c in cs {
                    let Some(res) = c.get_mut("resources").and_then(Value::as_object_mut) else { continue };
                    let reqs = res.get("requests").and_then(Value::as_object).cloned().unwrap_or_default();
                    let lim = res.entry("limits").or_insert_with(|| Value::Object(Default::default()));
                    if !lim.is_object() {
                        *lim = Value::Object(Default::default());
                    }
                    let lim = lim.as_object_mut().expect("object");
                    for k in ["cpu", "memory", "ephemeral-storage"] {
                        if let Some(v) = reqs.get(k) {
                            let same = match lim.get(k) {
                                Some(l) if k == "cpu" => cpu_of(l).ok() == cpu_of(v).ok(),
                                Some(l) => crate::resources::bytes_of(l).ok() == crate::resources::bytes_of(v).ok(),
                                None => false,
                            };
                            if !same {
                                lim.insert(k.into(), v.clone());
                                changed = true;
                            }
                        }
                    }
                    if lim.is_empty() {
                        res.remove("limits");
                    }
                }
            }
        }
        if changed {
            notes.push("limits set to requests (non-bursting)".into());
        }
    }
    let requests_changed = !notes.is_empty() && notes.iter().any(|n| !n.starts_with("limits set"));
    Ok(Normalized { spec: out, notes, requests_changed })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn pod(cpu: &str, mem: &str) -> Value {
        json!({"containers": [{"name": "a", "resources": {"requests": {"cpu": cpu, "memory": mem}}}]})
    }
    fn reqs(v: &Value) -> (String, String) {
        let r = &v["containers"][0]["resources"]["requests"];
        (r["cpu"].as_str().unwrap().to_string(), r["memory"].as_str().unwrap().to_string())
    }

    #[test]
    fn table() {
        let nb = AutopilotSettings { bursting: false, ..Default::default() };
        let b = AutopilotSettings::default();
        // (cpu, memory, settings) → normalized (cpu, memory)
        let cases: &[(&str, &str, &AutopilotSettings, &str, &str)] = &[
            // already normal
            ("1", "4Gi", &nb, "1", "4Gi"),
            // below minimum (non-bursting): 250m, then ratio ≥1 GiB/vCPU holds at 512Mi? 0.5 GiB / 0.25 = 2 → ok
            ("100m", "256Mi", &nb, "250m", "512Mi"),
            // bursting minimum is 50m / 52Mi
            ("10m", "16Mi", &b, "50m", "52Mi"),
            // ratio above 6.5 → cpu raised: 13Gi / 6.5 = 2 cores
            ("1", "13Gi", &nb, "2", "13Gi"),
            // ratio below 1 → memory raised to 4Gi for 4 cores
            ("4", "1Gi", &nb, "4", "4Gi"),
            // 0.25 vCPU rounding without bursting
            ("1100m", "4Gi", &nb, "1250m", "4Gi"),
            // with bursting, no rounding
            ("1100m", "4Gi", &b, "1100m", "4Gi"),
        ];
        for (cpu, mem, s, ecpu, emem) in cases {
            let n = normalize_pod_spec(&pod(cpu, mem), s).unwrap();
            assert_eq!(reqs(&n.spec), (ecpu.to_string(), emem.to_string()), "{cpu} {mem} bursting={}", s.bursting);
        }
    }

    #[test]
    fn limits_equal_requests_without_bursting() {
        let nb = AutopilotSettings { bursting: false, ..Default::default() };
        let p = json!({"containers": [{"name": "a", "resources": {"requests": {"cpu": "1", "memory": "4Gi"}, "limits": {"memory": "8Gi"}}}]});
        let n = normalize_pod_spec(&p, &nb).unwrap();
        assert_eq!(n.spec["containers"][0]["resources"]["limits"], json!({"cpu": "1", "memory": "4Gi"}));
        let n = normalize_pod_spec(&p, &AutopilotSettings::default()).unwrap();
        assert_eq!(n.spec, p);
    }

    #[test]
    fn maximums_reject() {
        assert!(normalize_pod_spec(&pod("31", "100Gi"), &AutopilotSettings::default()).is_err());
        let g = json!({"nodeSelector": {"cloud.google.com/gke-accelerator": "nvidia-tesla-a100"},
            "containers": [{"name": "a", "resources": {"requests": {"cpu": "12", "memory": "60Gi"}, "limits": {"nvidia.com/gpu": 1}}}]});
        assert!(normalize_pod_spec(&g, &AutopilotSettings::default()).unwrap_err().detail.contains("A100-40G"));
        let ok = json!({"nodeSelector": {"cloud.google.com/gke-accelerator": "nvidia-tesla-a100"},
            "containers": [{"name": "a", "resources": {"requests": {"cpu": "12", "memory": "60Gi"}, "limits": {"nvidia.com/gpu": 2}}}]});
        assert!(normalize_pod_spec(&ok, &AutopilotSettings::default()).is_ok());
    }
}
