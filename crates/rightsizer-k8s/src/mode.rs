//! Mode resolution for the webhook (kubernetes.md §3.2): the namespace label
//! `rightsizer.io/mode` (default `recommend`), lowered — never raised — by
//! the object's label or annotation of the same name. `mode_ceiling` and
//! breakers are applied by the engine (`rightsizer_core::policy`).

use std::collections::BTreeMap;

use rightsizer_core::Mode;
use serde_json::Value;

use crate::objects::string_map;

/// Label / annotation name.
pub const MODE: &str = "rightsizer.io/mode";

/// The namespace's mode: its label, default `recommend`. An unreadable value
/// is treated as the default, never as `enforce`.
pub fn namespace_mode(labels: &BTreeMap<String, String>) -> Mode {
    labels.get(MODE).and_then(|m| m.trim().parse::<Mode>().ok()).unwrap_or(Mode::Recommend)
}

/// The object's own mode request (label or annotation, the lower of the two).
/// Unreadable values are ignored.
pub fn object_mode(object: &Value) -> Option<Mode> {
    let l = string_map(object, "/metadata/labels").get(MODE).and_then(|m| m.trim().parse::<Mode>().ok());
    let a = string_map(object, "/metadata/annotations").get(MODE).and_then(|m| m.trim().parse::<Mode>().ok());
    match (l, a) {
        (Some(l), Some(a)) => Some(l.min(a)),
        (x, None) | (None, x) => x,
    }
}

/// The effective requested mode: the namespace's, lowered by the object's.
pub fn effective_mode(namespace_labels: &BTreeMap<String, String>, object: &Value) -> Mode {
    let ns = namespace_mode(namespace_labels);
    match object_mode(object) {
        Some(o) => ns.min(o),
        None => ns,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn objects_only_lower() {
        let mut ns = BTreeMap::new();
        assert_eq!(effective_mode(&ns, &json!({})), Mode::Recommend);
        ns.insert(MODE.to_string(), "enforce".to_string());
        assert_eq!(effective_mode(&ns, &json!({})), Mode::Enforce);
        let obs = json!({"metadata": {"annotations": {MODE: "observe"}}});
        assert_eq!(effective_mode(&ns, &obs), Mode::Observe);
        ns.insert(MODE.to_string(), "recommend".to_string());
        let up = json!({"metadata": {"labels": {MODE: "enforce"}}});
        assert_eq!(effective_mode(&ns, &up), Mode::Recommend);
        let both = json!({"metadata": {"labels": {MODE: "recommend"}, "annotations": {MODE: "off"}}});
        assert_eq!(effective_mode(&ns, &both), Mode::Off);
        ns.insert(MODE.to_string(), "enforec".to_string());
        assert_eq!(effective_mode(&ns, &json!({})), Mode::Recommend);
    }
}
