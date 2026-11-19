//! Restore (pure part of `rsz k8s restore`, kubernetes.md §3.2): from an
//! object with a valid signed prior, the patch that puts back the original
//! resources (and KubeRay parameters) and removes rightsizer's annotations.
//! Fetching and patching the live object is the cluster component's job.

use rightsizer_core::ResourceSpec;
use rightsizer_plugin::{JsonPatchOp, PluginError, Result};
use serde_json::Value;

use crate::edit::{self, Op, Path, Seg, join, pointer};
use crate::extract::{self, DECISION_ID, RECOMMENDATION};
use crate::objects::{self, Kind};
use crate::path;
use crate::segs;
use crate::prior::{PRIOR_ANNOTATION, SigningKeys};
use crate::resources::{bytes_value, cpu_value};

/// What restore found.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Restore {
    /// RFC 6902 operations against the object.
    pub ops: Vec<JsonPatchOp>,
    /// Units restored.
    pub units: Vec<String>,
    /// Units in the prior that no longer exist in the object.
    pub missing: Vec<String>,
}

fn field_ops(res: &Path, cur: &Value, orig: &ResourceSpec) -> Vec<Op> {
    let mut ops = Vec::new();
    let fields: [(&str, &str, Option<Value>); 4] = [
        ("requests", "cpu", orig.requests.cpu.map(cpu_value)),
        ("requests", "memory", orig.requests.memory.map(bytes_value)),
        ("limits", "cpu", orig.limits.cpu.map(cpu_value)),
        ("limits", "memory", orig.limits.memory.map(bytes_value)),
    ];
    for (sec, k, want) in fields {
        let p = join(res, &[Seg::from(sec), Seg::from(k)]);
        let have = edit::get(cur, &p);
        match (want, have) {
            (Some(w), Some(h)) => {
                let same = if k == "cpu" { crate::resources::cpu_of(h).ok() == crate::resources::cpu_of(&w).ok() } else { crate::resources::bytes_of(h).ok() == crate::resources::bytes_of(&w).ok() };
                if !same {
                    ops.push(Op::Set { path: p, value: w });
                }
            }
            (Some(w), None) => ops.push(Op::Set { path: p, value: w }),
            (None, Some(_)) => ops.push(Op::Remove { path: p }),
            (None, None) => {}
        }
    }
    ops
}

/// The restore patch of an object. Errors when there is no valid signed prior.
pub fn restore(object: &Value, keys: &SigningKeys) -> Result<Restore> {
    let ann = objects::string_map(object, "/metadata/annotations");
    let raw = ann.get(PRIOR_ANNOTATION).ok_or_else(|| PluginError::InvalidNative("no rightsizer.io/prior annotation".into()))?;
    let sp = crate::prior::verify(raw, keys).map_err(|e| PluginError::InvalidNative(format!("rightsizer.io/prior: {e}")))?;
    let w = objects::workload(object).ok_or_else(|| PluginError::Unsupported("not a workload".into()))?;
    let locs = extract::locate(object, &w);
    let mut ops = Vec::new();
    let mut out = Restore::default();
    for (unit, orig) in &sp.prior {
        let Some(loc) = locs.iter().find(|l| &l.local_id == unit) else {
            out.missing.push(unit.clone());
            continue;
        };
        let res = loc.resources_path(&w);
        let mut fops = field_ops(&res, object, orig);
        // Drop `requests` / `limits` / `resources` maps that restoring empties.
        let after = edit::apply(object, &fops).map_err(|e| PluginError::InvalidPatch(e.to_string()))?;
        let empty = |p: &Path| edit::get(&after, p).and_then(Value::as_object).is_some_and(|m| m.is_empty());
        let nonempty_before = |p: &Path| edit::get(object, p).and_then(Value::as_object).is_some_and(|m| !m.is_empty());
        let mut secs_left = edit::get(&after, &res).and_then(Value::as_object).map(|m| m.len()).unwrap_or(0);
        for sec in ["requests", "limits"] {
            let p = join(&res, &[Seg::from(sec)]);
            if empty(&p) && nonempty_before(&p) {
                fops.push(Op::Remove { path: p });
                secs_left = secs_left.saturating_sub(1);
            }
        }
        if secs_left == 0 && nonempty_before(&res) && edit::get(&after, &res).is_some() {
            fops.push(Op::Remove { path: res.clone() });
        }
        ops.extend(fops);
        if let (Some(si), Some(params)) = (loc.site, sp.ray.get(unit))
            && let Some(rg) = &w.sites[si].ray
        {
            for (p, v) in params {
                let path = join(&rg.params_path, &[Seg::from(p.as_str())]);
                match v {
                    Some(s) => ops.push(Op::Set { path, value: Value::String(s.clone()) }),
                    None => ops.push(Op::Remove { path }),
                }
            }
        }
        out.units.push(unit.clone());
    }
    for a in [PRIOR_ANNOTATION, DECISION_ID, RECOMMENDATION] {
        ops.push(Op::Remove { path: path!["metadata", "annotations", a] });
    }
    for s in &w.sites {
        ops.push(Op::Remove { path: join(&s.meta_path, &segs!["annotations", DECISION_ID]) });
    }
    if w.kind == Kind::CronJob {
        ops.push(Op::Remove { path: path!["spec", "jobTemplate", "metadata", "annotations", DECISION_ID] });
    }
    // Drop annotation maps (and template metadata) that only rightsizer filled.
    let after = edit::apply(object, &ops).map_err(|e| PluginError::InvalidPatch(e.to_string()))?;
    let mut metas: Vec<Path> = vec![path!["metadata"]];
    metas.extend(w.sites.iter().map(|s| s.meta_path.clone()));
    if w.kind == Kind::CronJob {
        metas.push(path!["spec", "jobTemplate", "metadata"]);
    }
    for m in metas {
        let ann = join(&m, &segs!["annotations"]);
        if edit::get(&after, &ann).and_then(Value::as_object).is_some_and(|o| o.is_empty()) {
            let had = edit::get(object, &ann).and_then(Value::as_object).is_some_and(|o| o.keys().all(|k| k.starts_with("rightsizer.io/")));
            if had {
                ops.push(Op::Remove { path: ann.clone() });
                let rest = edit::get(&after, &m).and_then(Value::as_object).map(|o| o.len()).unwrap_or(0);
                if rest == 1 && m != path!["metadata"] {
                    ops.push(Op::Remove { path: m.clone() });
                }
            }
        }
    }
    let _ = pointer(&[]);
    out.ops = edit::to_json_patch(object, &ops).map_err(|e| PluginError::InvalidPatch(e.to_string()))?;
    Ok(out)
}
