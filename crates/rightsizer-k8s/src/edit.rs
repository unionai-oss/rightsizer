//! Abstract value edits and their two renderings: RFC 6902 JSON Patch (for
//! admission responses) and minimal byte-range text edits on YAML source
//! (for `rsz apply`), which preserve comments, ordering, quoting style and
//! indentation.

use rightsizer_plugin::{JsonPatchOp, TextEdit};
use serde_json::Value;

use crate::yaml::{self, Style, YDoc, YKind, YNode};

/// One path segment.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Seg {
    /// A mapping key.
    Key(String),
    /// A sequence index.
    Idx(usize),
}

impl From<&str> for Seg {
    fn from(s: &str) -> Self {
        Seg::Key(s.to_string())
    }
}
impl From<String> for Seg {
    fn from(s: String) -> Self {
        Seg::Key(s)
    }
}
impl From<usize> for Seg {
    fn from(i: usize) -> Self {
        Seg::Idx(i)
    }
}

/// A path into a JSON/YAML value.
pub type Path = Vec<Seg>;

/// Build a path from segments: `path!["spec", "containers", 0]`.
#[macro_export]
macro_rules! path {
    ($($s:expr),* $(,)?) => { vec![$($crate::edit::Seg::from($s)),*] };
}

/// Path segments as an array (for [`join`]): `segs!["resources", "requests"]`.
#[macro_export]
macro_rules! segs {
    ($($s:expr),* $(,)?) => { [$($crate::edit::Seg::from($s)),*] };
}

/// Extend a path.
pub fn join(base: &Path, more: &[Seg]) -> Path {
    let mut p = base.clone();
    p.extend_from_slice(more);
    p
}

/// RFC 6901 JSON pointer for a path (`~` → `~0`, `/` → `~1`).
pub fn pointer(p: &[Seg]) -> String {
    let mut s = String::new();
    for seg in p {
        s.push('/');
        match seg {
            Seg::Key(k) => s.push_str(&k.replace('~', "~0").replace('/', "~1")),
            Seg::Idx(i) => s.push_str(&i.to_string()),
        }
    }
    s
}

/// Look up a path in a value.
pub fn get<'a>(v: &'a Value, p: &[Seg]) -> Option<&'a Value> {
    let mut cur = v;
    for seg in p {
        cur = match (seg, cur) {
            (Seg::Key(k), Value::Object(m)) => m.get(k)?,
            (Seg::Idx(i), Value::Array(a)) => a.get(*i)?,
            _ => return None,
        };
    }
    Some(cur)
}

/// An edit of one value.
#[derive(Clone, Debug, PartialEq)]
pub enum Op {
    /// Set a value, creating missing parent mappings.
    Set {
        /// Path.
        path: Path,
        /// Value.
        value: Value,
    },
    /// Remove a value (no-op when absent).
    Remove {
        /// Path.
        path: Path,
    },
}

impl Op {
    /// Set constructor.
    pub fn set(path: Path, value: impl Into<Value>) -> Op {
        Op::Set { path, value: value.into() }
    }
    /// The op's path.
    pub fn path(&self) -> &Path {
        match self {
            Op::Set { path, .. } | Op::Remove { path } => path,
        }
    }
}

/// Edit errors (all of them leave the submission unchanged).
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum EditError {
    /// A path goes through a non-container value.
    #[error("cannot edit {0}: parent is not a mapping")]
    NotAMapping(String),
    /// A path goes through a YAML alias or anchored node.
    #[error("cannot edit {0}: it is part of a YAML anchor/alias")]
    Aliased(String),
    /// A block scalar or a collection would have to be replaced in text.
    #[error("cannot edit {0} in place")]
    Unsupported(String),
}

/// Apply ops to a clone of `v`.
pub fn apply(v: &Value, ops: &[Op]) -> Result<Value, EditError> {
    let mut out = v.clone();
    for op in ops {
        match op {
            Op::Set { path, value } => set_in(&mut out, path, value.clone())?,
            Op::Remove { path } => {
                remove_in(&mut out, path);
            }
        }
    }
    Ok(out)
}

fn set_in(v: &mut Value, p: &[Seg], value: Value) -> Result<(), EditError> {
    let Some((first, rest)) = p.split_first() else {
        *v = value;
        return Ok(());
    };
    if v.is_null() && matches!(first, Seg::Key(_)) {
        *v = Value::Object(Default::default());
    }
    match (first, v) {
        (Seg::Key(k), Value::Object(m)) => {
            let child = m.entry(k.clone()).or_insert(Value::Null);
            set_in(child, rest, value)
        }
        (Seg::Idx(i), Value::Array(a)) if *i < a.len() => set_in(&mut a[*i], rest, value),
        _ => Err(EditError::NotAMapping(pointer(p))),
    }
}

fn remove_in(v: &mut Value, p: &[Seg]) -> bool {
    let Some((last, parent)) = p.split_last() else { return false };
    let mut cur = v;
    for seg in parent {
        cur = match (seg, cur) {
            (Seg::Key(k), Value::Object(m)) => match m.get_mut(k) {
                Some(c) => c,
                None => return false,
            },
            (Seg::Idx(i), Value::Array(a)) => match a.get_mut(*i) {
                Some(c) => c,
                None => return false,
            },
            _ => return false,
        };
    }
    match (last, cur) {
        (Seg::Key(k), Value::Object(m)) => m.remove(k).is_some(),
        (Seg::Idx(i), Value::Array(a)) if *i < a.len() => {
            a.remove(*i);
            true
        }
        _ => false,
    }
}

/// The deepest existing prefix of `p` in `v`: returns `k` such that
/// `p[..k]` exists and is a non-null value. `p[..k]` exists; `p[k]` does
/// not (or is null).
fn existing_prefix(v: &Value, p: &[Seg]) -> usize {
    let mut cur = v;
    for (i, seg) in p.iter().enumerate() {
        let next = match (seg, cur) {
            (Seg::Key(k), Value::Object(m)) => m.get(k),
            (Seg::Idx(j), Value::Array(a)) => a.get(*j),
            _ => None,
        };
        match next {
            Some(n) if !n.is_null() => cur = n,
            _ => return i,
        }
    }
    p.len()
}

/// Render ops as RFC 6902 operations against `original`.
///
/// Values that exist are `replace`d; a missing subtree is created with one
/// `add` of the whole subtree at its first missing key (JSON Patch cannot add
/// into a missing parent). A null intermediate is `replace`d.
pub fn to_json_patch(original: &Value, ops: &[Op]) -> Result<Vec<JsonPatchOp>, EditError> {
    let patched = apply(original, ops)?;
    let mut out: Vec<JsonPatchOp> = Vec::new();
    let mut roots: Vec<Path> = Vec::new();
    let mut removed: Vec<Path> = Vec::new();
    for op in ops {
        match op {
            Op::Set { path, .. } => {
                let k = existing_prefix(original, path);
                let root: Path = if k == path.len() { path.clone() } else { path[..=k].to_vec() };
                if roots.iter().any(|r| root.starts_with(r)) {
                    continue;
                }
                roots.retain(|r| !r.starts_with(&root));
                out.retain(|o| !json_op_path(o).starts_with(&pointer(&root)));
                let value = get(&patched, &root).cloned().unwrap_or(Value::Null);
                let ptr = pointer(&root);
                let exists = get(original, &root).is_some();
                if exists {
                    if get(original, &root) == Some(&value) {
                        continue;
                    }
                    out.push(JsonPatchOp::Replace { path: ptr, value });
                } else {
                    out.push(JsonPatchOp::Add { path: ptr, value });
                }
                roots.push(root);
            }
            Op::Remove { path } => {
                if get(original, path).is_some() && !roots.iter().any(|r| path.starts_with(r)) && !removed.iter().any(|r| path.starts_with(r)) {
                    out.push(JsonPatchOp::Remove { path: pointer(path) });
                    removed.push(path.clone());
                }
            }
        }
    }
    Ok(out)
}

fn json_op_path(o: &JsonPatchOp) -> &str {
    match o {
        JsonPatchOp::Add { path, .. }
        | JsonPatchOp::Remove { path }
        | JsonPatchOp::Replace { path, .. }
        | JsonPatchOp::Move { path, .. }
        | JsonPatchOp::Copy { path, .. }
        | JsonPatchOp::Test { path, .. } => path,
    }
}

/// Located context of a node during a walk.
struct Located<'a> {
    node: &'a YNode,
    /// Column of the mapping that holds this node as a value (block keys).
    key_col: Option<usize>,
}

fn walk_yaml<'a>(root: &'a YNode, p: &[Seg]) -> (usize, Vec<Located<'a>>) {
    // Returns (number of segments found, chain of located nodes incl. root).
    let mut chain = vec![Located { node: root, key_col: None }];
    let mut cur = root;
    for (i, seg) in p.iter().enumerate() {
        let next = match (seg, &cur.kind) {
            (Seg::Key(k), YKind::Map { .. }) => cur.get(k).map(|n| Located { node: n, key_col: Some(cur.col) }),
            (Seg::Idx(j), YKind::Seq { .. }) => cur.item(*j).map(|n| Located { node: n, key_col: None }),
            _ => None,
        };
        match next {
            Some(l) => {
                cur = l.node;
                chain.push(l);
            }
            None => return (i, chain),
        }
    }
    (p.len(), chain)
}

/// Text for a replaced scalar, preserving the original quoting style where
/// the new value can be written in it.
fn replacement_scalar(orig_text: &str, style: Style, new: &Value) -> Option<String> {
    let s = match new {
        Value::String(s) => s.clone(),
        Value::Null => return Some("null".into()),
        other => other.to_string(),
    };
    Some(match style {
        Style::Double => yaml::double_quote(&s),
        Style::Single if !s.contains('\n') => yaml::single_quote(&s),
        Style::Single => yaml::double_quote(&s),
        Style::Plain => {
            let orig_is_string = matches!(yaml::resolve_scalar(orig_text, Style::Plain, None), Value::String(_));
            let new_plain = yaml::resolve_scalar(&s, Style::Plain, None);
            let new_is_string = matches!(new_plain, Value::String(_));
            if new.is_string() && !new_is_string && orig_is_string {
                // A string field (annotation, rayStartParams) must stay a string.
                yaml::double_quote(&s)
            } else if new.is_string() && !new_is_string {
                // The original was a number (`cpu: 2`); a number is fine.
                s
            } else if yaml::plain_safe(&s) || !new.is_string() {
                s
            } else {
                yaml::double_quote(&s)
            }
        }
        Style::Block => return None,
    })
}

/// Render ops as minimal text edits on one located YAML document. Only
/// the bytes of changed scalars and inserted entries change.
pub fn to_text_edits(src: &str, doc: &YDoc, ops: &[Op]) -> Result<Vec<TextEdit>, EditError> {
    let patched = apply(&doc.value, ops)?;
    let step = yaml::indent_step(&doc.root);
    let mut edits: Vec<TextEdit> = Vec::new();
    // Insertion groups: parent node start → (parent path, missing keys in order).
    let mut inserts: Vec<(Path, usize, Vec<String>)> = Vec::new();
    // Paths already handled by replacing an entire subtree.
    let mut whole: Vec<Path> = Vec::new();
    for op in ops {
        let Op::Set { path, value } = op else {
            return Err(EditError::Unsupported(format!("remove {} in text mode", pointer(op.path()))));
        };
        if whole.iter().any(|w| path.starts_with(w)) {
            continue;
        }
        let (found, chain) = walk_yaml(&doc.root, path);
        if chain.iter().any(|l| l.node.aliased) {
            return Err(EditError::Aliased(pointer(path)));
        }
        let last = chain.last().expect("root");
        if found == path.len() {
            let n = last.node;
            match n.scalar() {
                Some((text, style)) if !n.is_null() => {
                    if get(&doc.value, path) == Some(value) {
                        continue;
                    }
                    let t = replacement_scalar(text, style, value).ok_or_else(|| EditError::Unsupported(pointer(path)))?;
                    edits.push(TextEdit { start: n.start, end: n.end, replacement: t });
                }
                Some(_) => {
                    // A null value: write the new scalar in its place.
                    let (s, e) = null_span(src, n);
                    edits.push(TextEdit { start: s, end: e, replacement: format!(" {}", yaml::scalar_text(value, false)) });
                }
                None => return Err(EditError::Unsupported(pointer(path))),
            }
            continue;
        }
        // `path[..found]` exists as `last.node`; `path[found]` is missing (or null).
        let parent = last.node;
        let parent_path: Path = path[..found].to_vec();
        match &parent.kind {
            YKind::Map { flow: true, entries } => {
                // Insert `, key: value` before the closing brace.
                let Seg::Key(k) = &path[found] else { return Err(EditError::NotAMapping(pointer(path))) };
                let sub_path = join(&parent_path, &[Seg::Key(k.clone())]);
                if whole.iter().any(|w| sub_path.starts_with(w)) {
                    continue;
                }
                let v = get(&patched, &sub_path).cloned().unwrap_or(Value::Null);
                let close = parent.end.saturating_sub(1);
                let sep = if entries.is_empty() { "" } else { ", " };
                edits.push(TextEdit { start: close, end: close, replacement: format!("{sep}{}: {}", yaml::scalar_text(&Value::String(k.clone()), true), yaml::flow_text(&v)) });
                whole.push(sub_path);
            }
            YKind::Map { flow: false, .. } => {
                let Seg::Key(k) = &path[found] else { return Err(EditError::NotAMapping(pointer(path))) };
                match inserts.iter_mut().find(|(pp, _, _)| *pp == parent_path) {
                    Some((_, _, keys)) => {
                        if !keys.contains(k) {
                            keys.push(k.clone());
                        }
                    }
                    None => inserts.push((parent_path.clone(), parent.col, vec![k.clone()])),
                }
                whole.push(join(&parent_path, &[Seg::Key(k.clone())]));
            }
            YKind::Scalar { .. } if parent.is_null() => {
                // `resources:` with no value: write a block mapping in its place.
                let v = get(&patched, &parent_path).cloned().unwrap_or(Value::Null);
                let Value::Object(m) = &v else { return Err(EditError::NotAMapping(pointer(path))) };
                let indent = last.key_col.map(|c| c + step).ok_or_else(|| EditError::Unsupported(pointer(path)))?;
                let mut text = String::new();
                for (ck, cv) in m {
                    yaml::block_entry(ck, cv, indent, step, &mut text);
                }
                let text = text.trim_end_matches('\n').to_string();
                let (s, e) = null_span(src, parent);
                edits.push(TextEdit { start: s, end: e, replacement: format!("\n{text}") });
                whole.push(parent_path);
            }
            _ => return Err(EditError::NotAMapping(pointer(path))),
        }
    }
    for (parent_path, col, keys) in inserts {
        let (_, chain) = walk_yaml(&doc.root, &parent_path);
        let parent = chain.last().expect("root").node;
        let at = yaml::line_after(src, parent.end);
        let mut text = String::new();
        if at == src.len() && !src.ends_with('\n') {
            text.push('\n');
        }
        for k in keys {
            let v = get(&patched, &join(&parent_path, &[Seg::Key(k.clone())])).cloned().unwrap_or(Value::Null);
            yaml::block_entry(&k, &v, col, step, &mut text);
        }
        edits.push(TextEdit { start: at, end: at, replacement: text });
    }
    // Merge edits at the same insertion point, keeping emission order.
    edits.sort_by_key(|e| (e.start, e.end));
    let mut merged: Vec<TextEdit> = Vec::new();
    for e in edits {
        if let Some(last) = merged.last_mut()
            && last.start == e.start
            && last.end == e.start
            && e.start == e.end
        {
            last.replacement.push_str(&e.replacement);
            continue;
        }
        merged.push(e);
    }
    Ok(merged)
}

/// The span to replace for a null scalar: the scalar itself, extended back
/// over the spaces after the `:`.
fn null_span(src: &str, n: &YNode) -> (usize, usize) {
    let b = src.as_bytes();
    if n.start == n.end {
        // An empty value: the parser marks it at the `:`; insert after it.
        let p = if b.get(n.start) == Some(&b':') { n.start + 1 } else { n.start };
        return (p, p);
    }
    let mut s = n.start;
    while s > 0 && b[s - 1] == b' ' {
        s -= 1;
    }
    (s, n.end)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rightsizer_plugin::apply_text_edits;
    use serde_json::json;

    /// Numbers as strings: `cpu: 2` and `cpu: "2"` mean the same quantity.
    fn stringly(v: &Value) -> Value {
        match v {
            Value::Number(n) => Value::String(n.to_string()),
            Value::Array(a) => Value::Array(a.iter().map(stringly).collect()),
            Value::Object(m) => Value::Object(m.iter().map(|(k, v)| (k.clone(), stringly(v))).collect()),
            o => o.clone(),
        }
    }

    fn text_apply(src: &str, ops: &[Op]) -> String {
        let docs = yaml::parse(src).unwrap();
        let edits = to_text_edits(src, &docs[0], ops).unwrap();
        let out = apply_text_edits(src, &edits).unwrap();
        // The result means the same as applying the ops to the value.
        let after = yaml::parse(&out).unwrap();
        assert_eq!(stringly(&after[0].value), stringly(&apply(&docs[0].value, ops).unwrap()), "text:\n{out}");
        out
    }

    #[test]
    fn pointer_escaping() {
        assert_eq!(pointer(&segs!["metadata", "annotations", "rightsizer.io/prior"]), "/metadata/annotations/rightsizer.io~1prior");
        assert_eq!(pointer(&segs!["a~b", 3]), "/a~0b/3");
    }

    #[test]
    fn replace_preserves_style_and_comments() {
        let src = "spec:\n  containers:\n  - name: a # main\n    resources:\n      requests:\n        memory: '8Gi' # big\n        cpu: 4\n";
        let out = text_apply(
            src,
            &[Op::set(path!["spec", "containers", 0, "resources", "requests", "memory"], "4Gi"), Op::set(path!["spec", "containers", 0, "resources", "requests", "cpu"], "2")],
        );
        assert_eq!(out, "spec:\n  containers:\n  - name: a # main\n    resources:\n      requests:\n        memory: '4Gi' # big\n        cpu: 2\n");
    }

    #[test]
    fn insert_missing_blocks_indented() {
        let src = "spec:\n    containers:\n      - name: a\n        image: x\n      - name: b\n        image: y\n";
        let out = text_apply(src, &[Op::set(path!["spec", "containers", 0, "resources", "requests", "memory"], "4Gi")]);
        assert_eq!(out, "spec:\n    containers:\n      - name: a\n        image: x\n        resources:\n            requests:\n                memory: 4Gi\n      - name: b\n        image: y\n");
    }

    #[test]
    fn insert_into_flow_and_null() {
        let src = "r:\n  requests: {cpu: 1}\n  limits:\nx: 1";
        let out = text_apply(src, &[Op::set(path!["r", "requests", "memory"], "1Gi"), Op::set(path!["r", "limits", "memory"], "2Gi")]);
        assert_eq!(out, "r:\n  requests: {cpu: 1, memory: 1Gi}\n  limits:\n    memory: 2Gi\nx: 1");
        let out = text_apply("a: 1", &[Op::set(path!["b", "c"], "2")]);
        assert_eq!(out, "a: 1\nb:\n  c: \"2\"\n");
    }

    #[test]
    fn json_patch_adds_subtrees_once() {
        let v = json!({"spec": {"containers": [{"name": "a", "resources": null}]}});
        let ops = vec![
            Op::set(path!["spec", "containers", 0, "resources", "requests", "cpu"], "1"),
            Op::set(path!["spec", "containers", 0, "resources", "requests", "memory"], "1Gi"),
            Op::set(path!["metadata", "annotations", "a/b"], "x"),
        ];
        let p = to_json_patch(&v, &ops).unwrap();
        assert_eq!(p.len(), 2, "{p:?}");
        let mut doc = v.clone();
        let jp: json_patch::Patch = serde_json::from_value(serde_json::to_value(&p).unwrap()).unwrap();
        json_patch::patch(&mut doc, &jp).unwrap();
        assert_eq!(doc, apply(&v, &ops).unwrap());
    }

    #[test]
    fn aliased_paths_are_refused() {
        let src = "base: &b {cpu: 1}\nr:\n  requests: *b\n";
        let docs = yaml::parse(src).unwrap();
        assert!(to_text_edits(src, &docs[0], &[Op::set(path!["r", "requests", "cpu"], "2")]).is_err());
    }
}
