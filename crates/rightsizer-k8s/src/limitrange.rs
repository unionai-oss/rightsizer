//! LimitRange defaulting (simulated for templates, inferred for pods) and
//! validation (`min`, `max`, `maxLimitRequestRatio`, per container and per
//! pod), as the in-tree LimitRanger admission plugin does.

use rightsizer_core::{Axis, AxisSet};
use serde_json::{Map, Value};

use crate::resources::{Side, Totals, bytes_of, cpu_of, effective_pod};

/// One `spec.limits[]` item of a LimitRange.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LimitItem {
    /// `Container`, `Pod` or `PersistentVolumeClaim`.
    pub kind: String,
    /// Default limits.
    pub default: Map<String, Value>,
    /// Default requests.
    pub default_request: Map<String, Value>,
    /// Minimums.
    pub min: Map<String, Value>,
    /// Maximums.
    pub max: Map<String, Value>,
    /// Max limit / request ratios.
    pub max_ratio: Map<String, Value>,
}

/// The LimitRange items that apply in a namespace.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LimitRanges {
    /// Items with the name of their LimitRange.
    pub items: Vec<(String, LimitItem)>,
}

fn obj(v: Option<&Value>) -> Map<String, Value> {
    v.and_then(Value::as_object).cloned().unwrap_or_default()
}

impl LimitRanges {
    /// Parse LimitRange objects, keeping those in `namespace` (or without one).
    pub fn parse(objects: &[Value], namespace: Option<&str>) -> LimitRanges {
        let mut items = Vec::new();
        for o in objects {
            if let (Some(ns), Some(ons)) = (namespace, o.pointer("/metadata/namespace").and_then(Value::as_str))
                && ns != ons
            {
                continue;
            }
            let name = o.pointer("/metadata/name").and_then(Value::as_str).unwrap_or("limitrange").to_string();
            for it in o.pointer("/spec/limits").and_then(Value::as_array).into_iter().flatten() {
                let mut item = LimitItem {
                    kind: it.get("type").and_then(Value::as_str).unwrap_or("Container").to_string(),
                    default: obj(it.get("default")),
                    default_request: obj(it.get("defaultRequest")),
                    min: obj(it.get("min")),
                    max: obj(it.get("max")),
                    max_ratio: obj(it.get("maxLimitRequestRatio")),
                };
                // API defaulting of LimitRange objects: defaultRequest defaults
                // to default, and default/defaultRequest to max (for Containers).
                if item.kind == "Container" {
                    for (k, v) in item.max.clone() {
                        item.default.entry(k).or_insert(v);
                    }
                    for (k, v) in item.default.clone() {
                        item.default_request.entry(k).or_insert(v);
                    }
                }
                items.push((name.clone(), item));
            }
        }
        LimitRanges { items }
    }

    /// Whether there is nothing to apply.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    fn containers(&self) -> impl Iterator<Item = &(String, LimitItem)> {
        self.items.iter().filter(|(_, i)| i.kind == "Container")
    }
}

fn axis_of(resource: &str) -> Option<Axis> {
    match resource {
        "cpu" => Some(Axis::Cpu),
        "memory" => Some(Axis::Memory),
        "ephemeral-storage" => Some(Axis::EphemeralStorage),
        _ => None,
    }
}

/// Defaults LimitRanger would apply to one container's `resources`, and the
/// axes they fill. Returns the defaulted `resources` object.
pub fn default_container(res: Option<&Value>, lr: &LimitRanges) -> (Value, AxisSet) {
    let mut out = res.cloned().filter(Value::is_object).unwrap_or_else(|| Value::Object(Map::new()));
    let mut axes = AxisSet::empty();
    for (_, item) in lr.containers() {
        for (k, v) in &item.default {
            let limits = out.as_object_mut().unwrap().entry("limits").or_insert_with(|| Value::Object(Map::new()));
            if limits.is_null() {
                *limits = Value::Object(Map::new());
            }
            if let Some(l) = limits.as_object_mut()
                && !l.contains_key(k)
            {
                // A defaulted limit alone does not make the axis defaulted:
                // the request (what is sized) is still the author's.
                l.insert(k.clone(), v.clone());
            }
        }
        for (k, v) in &item.default_request {
            let has_limit = out.get("limits").and_then(|l| l.get(k)).is_some();
            let requests = out.as_object_mut().unwrap().entry("requests").or_insert_with(|| Value::Object(Map::new()));
            if requests.is_null() {
                *requests = Value::Object(Map::new());
            }
            if let Some(r) = requests.as_object_mut()
                && !r.contains_key(k)
            {
                // LimitRanger sets defaultRequest; API defaulting would set
                // request = limit when only a user limit exists, but
                // LimitRanger's defaultRequest runs first and wins.
                let _ = has_limit;
                r.insert(k.clone(), v.clone());
                if let Some(a) = axis_of(k) {
                    axes.insert(a);
                }
            }
        }
    }
    // Drop empty sections we created.
    if let Some(m) = out.as_object_mut() {
        for s in ["limits", "requests"] {
            if m.get(s).and_then(Value::as_object).is_some_and(Map::is_empty) {
                m.remove(s);
            }
        }
    }
    (out, axes)
}

/// Simulate LimitRanger on a pod spec: returns the defaulted pod spec and,
/// per container name, the axes defaults filled.
pub fn default_pod_spec(spec: &Value, lr: &LimitRanges) -> (Value, Vec<(String, AxisSet)>) {
    let mut out = spec.clone();
    let mut filled = Vec::new();
    if lr.is_empty() {
        return (out, filled);
    }
    for list in ["initContainers", "containers"] {
        if let Some(cs) = out.get_mut(list).and_then(Value::as_array_mut) {
            for c in cs.iter_mut() {
                let name = c.get("name").and_then(Value::as_str).unwrap_or_default().to_string();
                let (res, axes) = default_container(c.get("resources"), lr);
                if c.get("resources") != Some(&res) && res.as_object().is_some_and(|m| !m.is_empty()) {
                    c.as_object_mut().map(|m| m.insert("resources".into(), res));
                }
                filled.push((name, axes));
            }
        }
    }
    (out, filled)
}

/// Infer, for a pod LimitRanger already processed, the axes whose values
/// equal the LimitRange defaults (the user probably did not set them).
pub fn inferred_defaulted(res: Option<&Value>, lr: &LimitRanges) -> AxisSet {
    let mut axes = AxisSet::empty();
    let Some(res) = res else { return axes };
    for (_, item) in lr.containers() {
        for (k, dv) in &item.default_request {
            let Some(a) = axis_of(k) else { continue };
            let Some(v) = res.get("requests").and_then(|r| r.get(k)) else { continue };
            if quantity_eq(k, v, dv) {
                let lim_ok = match (item.default.get(k), res.get("limits").and_then(|l| l.get(k))) {
                    (Some(dl), Some(l)) => quantity_eq(k, l, dl),
                    (None, None) => true,
                    // A default limit exists but the pod has none: not defaulted by us.
                    (Some(_), None) => false,
                    (None, Some(_)) => true,
                };
                if lim_ok {
                    axes.insert(a);
                }
            }
        }
    }
    axes
}

fn quantity_eq(resource: &str, a: &Value, b: &Value) -> bool {
    if resource == "cpu" { cpu_of(a).ok() == cpu_of(b).ok() } else { bytes_of(a).ok() == bytes_of(b).ok() }
}

fn amount(resource: &str, v: &Value) -> Option<u64> {
    if resource == "cpu" { cpu_of(v).ok().map(|c| c.0 as u64) } else { bytes_of(v).ok().map(|b| b.0) }
}

fn ratio_of(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => cpu_of(&Value::String(s.clone())).ok().map(|m| m.0 as f64 / 1000.0),
        _ => None,
    }
}

/// A LimitRange violation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Violation {
    /// LimitRange name.
    pub limit_range: String,
    /// Container name (`None` for pod-level checks).
    pub container: Option<String>,
    /// Human-readable detail.
    pub detail: String,
}

/// Validate a *defaulted* pod spec against every LimitRange item.
pub fn validate_pod_spec(spec: &Value, lr: &LimitRanges) -> Result<(), Violation> {
    let containers: Vec<&Value> = ["initContainers", "containers"]
        .iter()
        .flat_map(|l| spec.get(*l).and_then(Value::as_array).map(|a| a.iter().collect::<Vec<_>>()).unwrap_or_default())
        .collect();
    for (name, item) in &lr.items {
        match item.kind.as_str() {
            "Container" => {
                for c in &containers {
                    let cname = c.get("name").and_then(Value::as_str).unwrap_or_default().to_string();
                    let res = c.get("resources");
                    let req = |k: &str| res.and_then(|r| r.get("requests")).and_then(|r| r.get(k)).or_else(|| res.and_then(|r| r.get("limits")).and_then(|l| l.get(k)));
                    let lim = |k: &str| res.and_then(|r| r.get("limits")).and_then(|l| l.get(k));
                    let v = |detail: String| Violation { limit_range: name.clone(), container: Some(cname.clone()), detail };
                    if let (Some(rq), Some(lm)) = (req("cpu"), lim("cpu"))
                        && amount("cpu", rq) > amount("cpu", lm)
                    {
                        return Err(v("cpu request exceeds limit".into()));
                    }
                    if let (Some(rq), Some(lm)) = (req("memory"), lim("memory"))
                        && amount("memory", rq) > amount("memory", lm)
                    {
                        return Err(v("memory request exceeds limit".into()));
                    }
                    for (k, mv) in &item.min {
                        let m = amount(k, mv);
                        match req(k) {
                            None => return Err(v(format!("minimum {k} usage requires a request"))),
                            Some(r) if amount(k, r) < m => return Err(v(format!("{k} request below minimum {mv}"))),
                            _ => {}
                        }
                        if let Some(l) = lim(k)
                            && amount(k, l) < m
                        {
                            return Err(v(format!("{k} limit below minimum {mv}")));
                        }
                    }
                    for (k, mv) in &item.max {
                        let m = amount(k, mv);
                        match lim(k) {
                            None => return Err(v(format!("maximum {k} usage requires a limit"))),
                            Some(l) if amount(k, l) > m => return Err(v(format!("{k} limit above maximum {mv}"))),
                            _ => {}
                        }
                        if let Some(r) = req(k)
                            && amount(k, r) > m
                        {
                            return Err(v(format!("{k} request above maximum {mv}")));
                        }
                    }
                    for (k, rv) in &item.max_ratio {
                        let Some(maxr) = ratio_of(rv) else { continue };
                        let (Some(r), Some(l)) = (req(k).and_then(|x| amount(k, x)), lim(k).and_then(|x| amount(k, x))) else {
                            if lim(k).is_some() {
                                return Err(v(format!("{k} maxLimitRequestRatio requires a request")));
                            }
                            continue;
                        };
                        if r == 0 || (l as f64 / r as f64) > maxr + 1e-9 {
                            return Err(v(format!("{k} limit/request ratio above {maxr}")));
                        }
                    }
                }
            }
            "Pod" => {
                let req = effective_pod(spec, Side::Requests, &Totals::default()).map_err(|e| Violation { limit_range: name.clone(), container: None, detail: e })?;
                let lim = effective_pod(spec, Side::Limits, &Totals::default()).map_err(|e| Violation { limit_range: name.clone(), container: None, detail: e })?;
                let pick = |t: &Totals, k: &str| match k {
                    "cpu" => Some(t.cpu),
                    "memory" => Some(t.memory),
                    "ephemeral-storage" => Some(t.ephemeral),
                    _ => None,
                };
                let v = |detail: String| Violation { limit_range: name.clone(), container: None, detail };
                for (k, mv) in &item.min {
                    if let (Some(r), Some(m)) = (pick(&req, k), amount(k, mv))
                        && r < m
                    {
                        return Err(v(format!("pod {k} request below minimum {mv}")));
                    }
                }
                for (k, mv) in &item.max {
                    if let (Some(l), Some(m)) = (pick(&lim, k), amount(k, mv))
                        && l > m
                    {
                        return Err(v(format!("pod {k} limit above maximum {mv}")));
                    }
                    if let (Some(r), Some(m)) = (pick(&req, k), amount(k, mv))
                        && r > m
                    {
                        return Err(v(format!("pod {k} request above maximum {mv}")));
                    }
                }
                for (k, rv) in &item.max_ratio {
                    let (Some(maxr), Some(r), Some(l)) = (ratio_of(rv), pick(&req, k), pick(&lim, k)) else { continue };
                    if r > 0 && l > 0 && (l as f64 / r as f64) > maxr + 1e-9 {
                        return Err(v(format!("pod {k} limit/request ratio above {maxr}")));
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn lr() -> LimitRanges {
        LimitRanges::parse(
            &[json!({
                "kind": "LimitRange", "metadata": {"name": "defaults", "namespace": "ml"},
                "spec": {"limits": [{"type": "Container", "default": {"memory": "2Gi"}, "defaultRequest": {"cpu": "500m", "memory": "1Gi"},
                                     "max": {"memory": "64Gi"}, "maxLimitRequestRatio": {"memory": "4"}}]}
            })],
            Some("ml"),
        )
    }

    #[test]
    fn defaulting_fills_and_marks() {
        let spec = json!({"containers": [{"name": "a", "resources": {"requests": {"memory": "4Gi"}}}, {"name": "b"}]});
        let (d, filled) = default_pod_spec(&spec, &lr());
        assert_eq!(d["containers"][0]["resources"], json!({"requests": {"memory": "4Gi", "cpu": "500m"}, "limits": {"memory": "2Gi"}}));
        // cpu request filled → defaulted; memory request is the author's (only its limit was filled).
        assert!(filled[0].1.contains(Axis::Cpu) && !filled[0].1.contains(Axis::Memory));
        assert_eq!(d["containers"][1]["resources"]["requests"], json!({"cpu": "500m", "memory": "1Gi"}));
        // a: request 4Gi > defaulted limit 2Gi → invalid pod.
        assert!(validate_pod_spec(&d, &lr()).is_err());
    }

    #[test]
    fn ratio_and_max() {
        let ok = json!({"containers": [{"name": "a", "resources": {"requests": {"memory": "4Gi"}, "limits": {"memory": "16Gi"}}}]});
        assert!(validate_pod_spec(&ok, &lr()).is_ok());
        let ratio = json!({"containers": [{"name": "a", "resources": {"requests": {"memory": "1Gi"}, "limits": {"memory": "8Gi"}}}]});
        assert!(validate_pod_spec(&ratio, &lr()).unwrap_err().detail.contains("ratio"));
        let max = json!({"containers": [{"name": "a", "resources": {"requests": {"memory": "65Gi"}, "limits": {"memory": "65Gi"}}}]});
        assert!(validate_pod_spec(&max, &lr()).unwrap_err().detail.contains("maximum"));
    }

    #[test]
    fn inferred_for_pods() {
        let res = json!({"requests": {"cpu": "500m", "memory": "1Gi"}, "limits": {"memory": "2Gi"}});
        let a = inferred_defaulted(Some(&res), &lr());
        assert!(a.contains(Axis::Cpu) && a.contains(Axis::Memory));
        let res = json!({"requests": {"cpu": "500m", "memory": "3Gi"}, "limits": {"memory": "6Gi"}});
        let a = inferred_defaulted(Some(&res), &lr());
        assert!(a.contains(Axis::Cpu) && !a.contains(Axis::Memory));
    }

    #[test]
    fn pod_level_items() {
        let lr = LimitRanges::parse(&[json!({"metadata": {"name": "p"}, "spec": {"limits": [{"type": "Pod", "max": {"cpu": "4"}}]}})], None);
        let s = json!({"containers": [{"name": "a", "resources": {"limits": {"cpu": "3"}}}, {"name": "b", "resources": {"limits": {"cpu": "2"}}}]});
        assert!(validate_pod_spec(&s, &lr).is_err());
    }
}
