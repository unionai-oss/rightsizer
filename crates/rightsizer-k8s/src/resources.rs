//! Container `resources` ⇄ [`ResourceSpec`], QoS classes, and the effective
//! pod request (init containers, restartable sidecars, pod overhead).

use std::collections::BTreeMap;

use rightsizer_core::units::{format_k8s_bytes, format_k8s_cpu, parse_k8s_bytes, parse_k8s_cpu};
use rightsizer_core::{AcceleratorId, Bytes, GpuShape, Limits, MilliCpu, QosClass, Requests, ResourceSpec};
use serde_json::Value;

/// Extended resources that count as accelerators.
pub const GPU_RESOURCES: [&str; 3] = ["nvidia.com/gpu", "amd.com/gpu", "google.com/tpu"];

/// A K8s quantity as text, from a JSON string or number.
pub fn quantity_text(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.trim().to_string()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// Parse a CPU quantity value.
pub fn cpu_of(v: &Value) -> Result<MilliCpu, String> {
    let t = quantity_text(v).ok_or_else(|| format!("cpu quantity {v}"))?;
    parse_k8s_cpu(&t).map_err(|e| format!("cpu {t:?}: {e}"))
}

/// Parse a byte quantity value.
pub fn bytes_of(v: &Value) -> Result<Bytes, String> {
    let t = quantity_text(v).ok_or_else(|| format!("quantity {v}"))?;
    parse_k8s_bytes(&t).map_err(|e| format!("quantity {t:?}: {e}"))
}

/// Parse a count quantity (extended resources).
pub fn count_of(v: &Value) -> Result<u32, String> {
    let m = cpu_of(v)?;
    if m.0 % 1000 != 0 {
        return Err(format!("fractional count {v}"));
    }
    Ok(m.0 / 1000)
}

/// Formatted CPU for writing.
pub fn cpu_value(c: MilliCpu) -> Value {
    Value::String(format_k8s_cpu(c))
}

/// Formatted memory for writing.
pub fn bytes_value(b: Bytes) -> Value {
    Value::String(format_k8s_bytes(b.0))
}

/// The GPU resource name and count of a `resources` object (limits first,
/// then requests: extended resources default request = limit).
pub fn gpu_of(res: Option<&Value>) -> Result<Option<(String, u32)>, String> {
    let Some(res) = res else { return Ok(None) };
    for section in ["limits", "requests"] {
        if let Some(m) = res.get(section).and_then(Value::as_object) {
            for g in GPU_RESOURCES {
                if let Some(v) = m.get(g) {
                    let n = count_of(v)?;
                    if n > 0 {
                        return Ok(Some((g.to_string(), n)));
                    }
                }
            }
        }
    }
    Ok(None)
}

/// Parse a `resources` object into a spec as written (no defaulting).
pub fn spec_of(res: Option<&Value>, accel: AcceleratorId) -> Result<ResourceSpec, String> {
    let mut s = ResourceSpec::default();
    let Some(res) = res else { return Ok(s) };
    if let Some(r) = res.get("requests").and_then(Value::as_object) {
        if let Some(v) = r.get("cpu") {
            s.requests.cpu = Some(cpu_of(v)?);
        }
        if let Some(v) = r.get("memory") {
            s.requests.memory = Some(bytes_of(v)?);
        }
        if let Some(v) = r.get("ephemeral-storage") {
            s.requests.ephemeral_storage = Some(bytes_of(v)?);
        }
    }
    if let Some(l) = res.get("limits").and_then(Value::as_object) {
        if let Some(v) = l.get("cpu") {
            s.limits.cpu = Some(cpu_of(v)?);
        }
        if let Some(v) = l.get("memory") {
            s.limits.memory = Some(bytes_of(v)?);
        }
        if let Some(v) = l.get("ephemeral-storage") {
            s.limits.ephemeral_storage = Some(bytes_of(v)?);
        }
    }
    if let Some((_, n)) = gpu_of(Some(res))? {
        s.requests.gpu = Some(GpuShape { accel, count: n });
    }
    Ok(s)
}

/// Apply K8s API defaulting: an unset request defaults to the limit.
pub fn default_requests_from_limits(s: &mut ResourceSpec) {
    if s.requests.cpu.is_none() {
        s.requests.cpu = s.limits.cpu;
    }
    if s.requests.memory.is_none() {
        s.requests.memory = s.limits.memory;
    }
    if s.requests.ephemeral_storage.is_none() {
        s.requests.ephemeral_storage = s.limits.ephemeral_storage;
    }
}

/// Resource totals for quota and node-shape arithmetic.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Totals {
    /// Millicores.
    pub cpu: u64,
    /// Bytes.
    pub memory: u64,
    /// Ephemeral storage bytes.
    pub ephemeral: u64,
    /// Accelerators by resource name.
    pub gpu: BTreeMap<String, u64>,
}

impl Totals {
    /// Per-resource maximum.
    pub fn max(&self, o: &Totals) -> Totals {
        let mut gpu = self.gpu.clone();
        for (k, v) in &o.gpu {
            let e = gpu.entry(k.clone()).or_insert(0);
            *e = (*e).max(*v);
        }
        Totals { cpu: self.cpu.max(o.cpu), memory: self.memory.max(o.memory), ephemeral: self.ephemeral.max(o.ephemeral), gpu }
    }
    /// Per-resource sum.
    pub fn add(&self, o: &Totals) -> Totals {
        let mut gpu = self.gpu.clone();
        for (k, v) in &o.gpu {
            *gpu.entry(k.clone()).or_insert(0) += *v;
        }
        Totals { cpu: self.cpu + o.cpu, memory: self.memory + o.memory, ephemeral: self.ephemeral + o.ephemeral, gpu }
    }
    /// Scaled by a replica count.
    pub fn times(&self, n: u64) -> Totals {
        Totals {
            cpu: self.cpu.saturating_mul(n),
            memory: self.memory.saturating_mul(n),
            ephemeral: self.ephemeral.saturating_mul(n),
            gpu: self.gpu.iter().map(|(k, v)| (k.clone(), v.saturating_mul(n))).collect(),
        }
    }
    /// Total accelerators.
    pub fn gpu_count(&self) -> u64 {
        self.gpu.values().sum()
    }
}

/// Which side of `resources` to total.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Side {
    /// Requests (defaulting to limits).
    Requests,
    /// Limits.
    Limits,
}

/// Totals of one container's `resources`.
pub fn container_totals(res: Option<&Value>, side: Side) -> Result<Totals, String> {
    let mut t = Totals::default();
    let Some(res) = res else { return Ok(t) };
    let get = |name: &str| -> Option<&Value> {
        let primary = match side {
            Side::Requests => res.get("requests").and_then(|r| r.get(name)),
            Side::Limits => res.get("limits").and_then(|r| r.get(name)),
        };
        primary.or_else(|| if side == Side::Requests { res.get("limits").and_then(|r| r.get(name)) } else { None })
    };
    if let Some(v) = get("cpu") {
        t.cpu = cpu_of(v)?.0 as u64;
    }
    if let Some(v) = get("memory") {
        t.memory = bytes_of(v)?.0;
    }
    if let Some(v) = get("ephemeral-storage") {
        t.ephemeral = bytes_of(v)?.0;
    }
    for g in GPU_RESOURCES {
        // Extended resources: request = limit.
        let v = res.get("limits").and_then(|r| r.get(g)).or_else(|| res.get("requests").and_then(|r| r.get(g)));
        if let Some(v) = v {
            let n = count_of(v)? as u64;
            if n > 0 {
                t.gpu.insert(g.to_string(), n);
            }
        }
    }
    Ok(t)
}

/// Totals of a `{cpu, memory}` resource list (pod overhead, RuntimeClass overhead).
pub fn list_totals(list: Option<&Value>) -> Result<Totals, String> {
    let mut t = Totals::default();
    let Some(m) = list.and_then(Value::as_object) else { return Ok(t) };
    if let Some(v) = m.get("cpu") {
        t.cpu = cpu_of(v)?.0 as u64;
    }
    if let Some(v) = m.get("memory") {
        t.memory = bytes_of(v)?.0;
    }
    Ok(t)
}

/// Whether an init container is a restartable sidecar (`restartPolicy: Always`).
pub fn is_sidecar(c: &Value) -> bool {
    c.get("restartPolicy").and_then(Value::as_str) == Some("Always")
}

fn arr<'a>(spec: &'a Value, key: &str) -> &'a [Value] {
    spec.get(key).and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[])
}

/// The effective pod request (or limit) of a pod spec, kubernetes.md §3.4:
/// `max(max over init containers (counting restartable sidecars started
/// before each), sum(app) + sum(sidecars)) + overhead`. With pod-level
/// `spec.resources`, those are the pod's totals.
pub fn effective_pod(spec: &Value, side: Side, overhead: &Totals) -> Result<Totals, String> {
    if let Some(pr) = spec.get("resources").filter(|r| r.get("requests").is_some() || r.get("limits").is_some()) {
        let mut t = container_totals(Some(pr), side)?;
        // Extended resources stay per container.
        let mut gpus = Totals::default();
        for c in arr(spec, "containers") {
            gpus = gpus.add(&container_totals(c.get("resources"), side)?);
        }
        t.gpu = gpus.gpu;
        return Ok(t.add(overhead));
    }
    let mut sidecars = Totals::default();
    let mut init_peak = Totals::default();
    for c in arr(spec, "initContainers") {
        let ct = container_totals(c.get("resources"), side)?;
        if is_sidecar(c) {
            sidecars = sidecars.add(&ct);
            init_peak = init_peak.max(&sidecars);
        } else {
            init_peak = init_peak.max(&sidecars.add(&ct));
        }
    }
    let mut app = Totals::default();
    for c in arr(spec, "containers") {
        app = app.add(&container_totals(c.get("resources"), side)?);
    }
    Ok(init_peak.max(&app.add(&sidecars)).add(overhead))
}

/// The pod's QoS class (K8s rules: Guaranteed when every container has CPU
/// and memory limits equal to its requests; BestEffort when no container
/// sets any CPU or memory request or limit; otherwise Burstable). Pod-level
/// resources decide alone when present.
pub fn pod_qos(spec: &Value) -> QosClass {
    fn cpu_mem(res: Option<&Value>) -> (Option<Value>, Option<Value>, Option<Value>, Option<Value>) {
        let g = |sec: &str, k: &str| res.and_then(|r| r.get(sec)).and_then(|s| s.get(k)).cloned();
        (g("requests", "cpu"), g("requests", "memory"), g("limits", "cpu"), g("limits", "memory"))
    }
    fn eq(a: &Option<Value>, b: &Option<Value>, cpu: bool) -> bool {
        match (a, b) {
            (Some(a), Some(b)) => {
                if cpu {
                    cpu_of(a).ok() == cpu_of(b).ok()
                } else {
                    bytes_of(a).ok() == bytes_of(b).ok()
                }
            }
            _ => false,
        }
    }
    let classify = |list: Vec<Option<&Value>>| -> QosClass {
        let mut any = false;
        let mut all_guaranteed = true;
        for res in list {
            let (rc, rm, lc, lm) = cpu_mem(res);
            if rc.is_some() || rm.is_some() || lc.is_some() || lm.is_some() {
                any = true;
            }
            let rc = rc.or_else(|| lc.clone());
            let rm = rm.or_else(|| lm.clone());
            if !(eq(&rc, &lc, true) && eq(&rm, &lm, false)) {
                all_guaranteed = false;
            }
        }
        if !any {
            QosClass::BestEffort
        } else if all_guaranteed {
            QosClass::Guaranteed
        } else {
            QosClass::Burstable
        }
    };
    if let Some(pr) = spec.get("resources").filter(|r| r.get("requests").is_some() || r.get("limits").is_some()) {
        return classify(vec![Some(pr)]);
    }
    let list: Vec<Option<&Value>> = arr(spec, "initContainers").iter().chain(arr(spec, "containers")).map(|c| c.get("resources")).collect();
    classify(list)
}

/// Requests of a spec as totals (for comparisons).
pub fn spec_totals(s: &ResourceSpec) -> Totals {
    Totals {
        cpu: s.requests.cpu.map(|c| c.0 as u64).unwrap_or(0),
        memory: s.requests.memory.map(|b| b.0).unwrap_or(0),
        ephemeral: s.requests.ephemeral_storage.map(|b| b.0).unwrap_or(0),
        gpu: BTreeMap::new(),
    }
}

/// Requests and limits equal on CPU and memory (Guaranteed shape of one container).
pub fn is_guaranteed_spec(s: &ResourceSpec) -> bool {
    let r = &s.requests;
    let l = &s.limits;
    l.cpu.is_some() && l.memory.is_some() && r.cpu.or(l.cpu) == l.cpu && r.memory.or(l.memory) == l.memory
}

/// Requests with only CPU and memory.
pub fn cpu_mem_requests(r: &Requests) -> Requests {
    Requests { cpu: r.cpu, memory: r.memory, ..Default::default() }
}

/// Limits with only CPU and memory.
pub fn cpu_mem_limits(l: &Limits) -> Limits {
    Limits { cpu: l.cpu, memory: l.memory, ephemeral_storage: None }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn spec_parsing() {
        let r = json!({"requests": {"cpu": "500m", "memory": "1Gi"}, "limits": {"memory": 2147483648u64, "nvidia.com/gpu": 2}});
        let s = spec_of(Some(&r), AcceleratorId::A100_80G).unwrap();
        assert_eq!(s.requests.cpu, Some(MilliCpu(500)));
        assert_eq!(s.limits.memory, Some(Bytes::gib(2)));
        assert_eq!(s.requests.gpu, Some(GpuShape { accel: AcceleratorId::A100_80G, count: 2 }));
        assert!(spec_of(Some(&json!({"requests": {"cpu": "lots"}})), AcceleratorId::UnknownGpu).is_err());
    }

    #[test]
    fn effective_pod_with_sidecars() {
        let spec = json!({
            "initContainers": [
                {"name": "mesh", "restartPolicy": "Always", "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}}},
                {"name": "stage", "resources": {"requests": {"cpu": "2", "memory": "4Gi"}}},
            ],
            "containers": [
                {"name": "a", "resources": {"requests": {"cpu": "1", "memory": "1Gi"}}},
                {"name": "b", "resources": {"limits": {"cpu": "1", "memory": "1Gi"}}},
            ],
        });
        let t = effective_pod(&spec, Side::Requests, &Totals::default()).unwrap();
        // init peak: stage (2 cpu, 4Gi) + mesh = 2100m, 4Gi+128Mi; app: 2 cpu + mesh = 2100m, 2Gi+128Mi.
        assert_eq!(t.cpu, 2100);
        assert_eq!(t.memory, (4 << 30) + (128 << 20));
        let oh = Totals { cpu: 250, memory: 120 << 20, ..Default::default() };
        assert_eq!(effective_pod(&spec, Side::Requests, &oh).unwrap().cpu, 2350);
    }

    #[test]
    fn qos_classes() {
        let g = json!({"containers": [{"resources": {"limits": {"cpu": "1", "memory": "1Gi"}}}]});
        assert_eq!(pod_qos(&g), QosClass::Guaranteed);
        let g2 = json!({"containers": [{"resources": {"requests": {"cpu": "1000m", "memory": "1024Mi"}, "limits": {"cpu": "1", "memory": "1Gi"}}}]});
        assert_eq!(pod_qos(&g2), QosClass::Guaranteed);
        let b = json!({"containers": [{"resources": {"requests": {"cpu": "1"}}}, {"name": "x"}]});
        assert_eq!(pod_qos(&b), QosClass::Burstable);
        assert_eq!(pod_qos(&json!({"containers": [{"name": "x"}]})), QosClass::BestEffort);
    }
}
