//! A small YAML span locator over `saphyr-parser` events.
//!
//! `rsz apply` edits manifests in place, so it needs to know *where* every
//! node is in the source text, not only what it means. This module builds a
//! tree of nodes with byte spans (start of the node, end of its content),
//! the column of block mappings and the scalar style, and converts it to
//! `serde_json::Value` with a YAML 1.2 core-schema resolver (plain `true`,
//! `null`, integers and floats are typed; everything quoted is a string).
//!
//! Anchors are resolved into the JSON value; aliased nodes are marked so the
//! editor refuses to edit through them (an edit would change every alias).

use std::collections::HashMap;

use saphyr_parser::{Event, Parser, ScalarStyle, Span, SpannedEventReceiver};
use serde_json::Value;

/// Scalar presentation style.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Style {
    /// Plain (unquoted).
    Plain,
    /// `'single'`.
    Single,
    /// `"double"`.
    Double,
    /// `|` or `>` block scalar.
    Block,
}

/// One node of the located tree.
#[derive(Clone, Debug, PartialEq)]
pub struct YNode {
    /// What the node is.
    pub kind: YKind,
    /// Byte offset of the node's first character (`{` / `[` for flow collections,
    /// the first key for block mappings, the opening quote for quoted scalars).
    pub start: usize,
    /// Byte offset just past the node's last content character.
    pub end: usize,
    /// Column (in characters) of `start`.
    pub col: usize,
    /// Whether this node (or an ancestor) carries an anchor or is an alias.
    pub aliased: bool,
}

/// Node kinds.
#[derive(Clone, Debug, PartialEq)]
pub enum YKind {
    /// A scalar with its resolved text and style.
    Scalar {
        /// Scalar text (unescaped).
        value: String,
        /// Style.
        style: Style,
    },
    /// A mapping.
    Map {
        /// Flow (`{...}`) or block.
        flow: bool,
        /// Entries in source order.
        entries: Vec<(YNode, YNode)>,
    },
    /// A sequence.
    Seq {
        /// Flow (`[...]`) or block.
        flow: bool,
        /// Items.
        items: Vec<YNode>,
    },
    /// An alias (`*x`).
    Alias,
}

impl YNode {
    /// The scalar text, if a scalar.
    pub fn scalar(&self) -> Option<(&str, Style)> {
        match &self.kind {
            YKind::Scalar { value, style } => Some((value.as_str(), *style)),
            _ => None,
        }
    }
    /// Whether this is a plain scalar that resolves to null.
    pub fn is_null(&self) -> bool {
        matches!(&self.kind, YKind::Scalar { value, style: Style::Plain } if is_null_plain(value))
    }
    /// Look up a mapping entry by key text.
    pub fn get(&self, key: &str) -> Option<&YNode> {
        match &self.kind {
            YKind::Map { entries, .. } => entries.iter().find(|(k, _)| k.scalar().map(|s| s.0) == Some(key)).map(|(_, v)| v),
            _ => None,
        }
    }
    /// The key node of a mapping entry.
    pub fn get_key(&self, key: &str) -> Option<&YNode> {
        match &self.kind {
            YKind::Map { entries, .. } => entries.iter().find(|(k, _)| k.scalar().map(|s| s.0) == Some(key)).map(|(k, _)| k),
            _ => None,
        }
    }
    /// Sequence item.
    pub fn item(&self, i: usize) -> Option<&YNode> {
        match &self.kind {
            YKind::Seq { items, .. } => items.get(i),
            _ => None,
        }
    }
}

/// One YAML document of a stream.
#[derive(Clone, Debug, PartialEq)]
pub struct YDoc {
    /// Root node.
    pub root: YNode,
    /// The resolved value.
    pub value: Value,
}

/// A parse error with a byte offset.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
#[error("yaml: {msg} at byte {at}")]
pub struct YamlError {
    /// Message.
    pub msg: String,
    /// Byte offset.
    pub at: usize,
}

enum Frame {
    Map { start: usize, col: usize, flow: bool, aliased: bool, anchor: usize, entries: Vec<(YNode, YNode)>, pending_key: Option<YNode>, vals: Vec<(String, Value)> },
    Seq { start: usize, col: usize, flow: bool, aliased: bool, anchor: usize, items: Vec<YNode>, vals: Vec<Value> },
}

struct Builder<'s> {
    src: &'s str,
    /// char index → byte offset (None when the source is ASCII).
    char_to_byte: Option<Vec<usize>>,
    stack: Vec<Frame>,
    docs: Vec<YDoc>,
    anchors: HashMap<usize, Value>,
    err: Option<YamlError>,
}

impl<'s> Builder<'s> {
    fn byte(&self, char_idx: usize) -> usize {
        match &self.char_to_byte {
            None => char_idx.min(self.src.len()),
            Some(t) => t.get(char_idx).copied().unwrap_or(self.src.len()),
        }
    }

    fn push_node(&mut self, node: YNode, value: Value) {
        match self.stack.last_mut() {
            None => {
                self.docs.push(YDoc { root: node, value });
            }
            Some(Frame::Seq { items, vals, .. }) => {
                items.push(node);
                vals.push(value);
            }
            Some(Frame::Map { entries, pending_key, vals, .. }) => match pending_key.take() {
                None => {
                    // `node` is a key.
                    *pending_key = Some(node);
                    vals.push((key_text(&value), Value::Null));
                }
                Some(k) => {
                    entries.push((k, node));
                    if let Some(last) = vals.last_mut() {
                        last.1 = value;
                    }
                }
            },
        }
    }
}

fn key_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => "null".into(),
        other => other.to_string(),
    }
}

impl<'i, 's> SpannedEventReceiver<'i> for Builder<'s> {
    fn on_event(&mut self, ev: Event<'i>, span: Span) {
        if self.err.is_some() {
            return;
        }
        let start = self.byte(span.start.index());
        let end = self.byte(span.end.index());
        let col = span.start.col();
        match ev {
            Event::Scalar(text, style, anchor, tag) => {
                let st = match style {
                    ScalarStyle::Plain => Style::Plain,
                    ScalarStyle::SingleQuoted => Style::Single,
                    ScalarStyle::DoubleQuoted => Style::Double,
                    _ => Style::Block,
                };
                let tag_str = tag.as_ref().map(|t| format!("{}{}", t.handle, t.suffix));
                let value = resolve_scalar(&text, st, tag_str.as_deref());
                if anchor > 0 {
                    self.anchors.insert(anchor, value.clone());
                }
                // Block scalars: the parser's span runs into the next line's indentation.
                let end = scalar_end(self.src, start, end, st);
                let node = YNode { kind: YKind::Scalar { value: text.into_owned(), style: st }, start, end, col, aliased: anchor > 0 };
                self.push_node(node, value);
            }
            Event::Alias(id) => {
                let value = self.anchors.get(&id).cloned().unwrap_or(Value::Null);
                let node = YNode { kind: YKind::Alias, start, end, col, aliased: true };
                self.push_node(node, value);
            }
            Event::MappingStart(anchor, _) => {
                let flow = end > start && self.src.as_bytes().get(start) == Some(&b'{');
                self.stack.push(Frame::Map { start, col, flow, aliased: anchor > 0, anchor, entries: vec![], pending_key: None, vals: vec![] });
            }
            Event::SequenceStart(anchor, _) => {
                let flow = end > start && self.src.as_bytes().get(start) == Some(&b'[');
                self.stack.push(Frame::Seq { start, col, flow, aliased: anchor > 0, anchor, items: vec![], vals: vec![] });
            }
            Event::MappingEnd => {
                let Some(Frame::Map { start: s, col: c, flow, aliased, anchor, entries, pending_key, vals }) = self.stack.pop() else {
                    self.err = Some(YamlError { msg: "unbalanced mapping".into(), at: start });
                    return;
                };
                let mut entries = entries;
                let mut vals = vals;
                if let Some(k) = pending_key {
                    // A key with no value event cannot happen with saphyr (it emits an
                    // empty scalar), but be defensive.
                    let e = k.end;
                    entries.push((k, YNode { kind: YKind::Scalar { value: String::new(), style: Style::Plain }, start: e, end: e, col: 0, aliased: false }));
                    if let Some(last) = vals.last_mut() {
                        last.1 = Value::Null;
                    }
                }
                let content_end = if flow {
                    end
                } else {
                    entries.last().map(|(k, v)| k.end.max(v.end)).unwrap_or(s)
                };
                let mut map = serde_json::Map::new();
                for (k, v) in vals {
                    if k == "<<" {
                        // Merge keys: insert missing keys from the merged mapping(s).
                        let srcs: Vec<Value> = match v {
                            Value::Array(a) => a,
                            other => vec![other],
                        };
                        for m in srcs {
                            if let Value::Object(o) = m {
                                for (mk, mv) in o {
                                    map.entry(mk).or_insert(mv);
                                }
                            }
                        }
                        continue;
                    }
                    map.insert(k, v);
                }
                let value = Value::Object(map);
                if anchor > 0 {
                    self.anchors.insert(anchor, value.clone());
                }
                let node = YNode { kind: YKind::Map { flow, entries }, start: s, end: content_end, col: c, aliased };
                self.push_node(node, value);
            }
            Event::SequenceEnd => {
                let Some(Frame::Seq { start: s, col: c, flow, aliased, anchor, items, vals }) = self.stack.pop() else {
                    self.err = Some(YamlError { msg: "unbalanced sequence".into(), at: start });
                    return;
                };
                let content_end = if flow { end } else { items.last().map(|i| i.end).unwrap_or(s) };
                let value = Value::Array(vals);
                if anchor > 0 {
                    self.anchors.insert(anchor, value.clone());
                }
                let node = YNode { kind: YKind::Seq { flow, items }, start: s, end: content_end, col: c, aliased };
                self.push_node(node, value);
            }
            Event::StreamStart | Event::StreamEnd | Event::DocumentStart(_) | Event::DocumentEnd | Event::Nothing => {}
        }
    }
}

/// The true end of a scalar's text: the closing quote for quoted scalars,
/// the last non-space character before a comment for plain ones (the
/// parser's span may run over trailing spaces and comments).
fn scalar_end(src: &str, start: usize, end: usize, style: Style) -> usize {
    let b = src.as_bytes();
    match style {
        Style::Single => {
            let mut i = start + 1;
            while i < b.len() {
                if b[i] == b'\'' {
                    if b.get(i + 1) == Some(&b'\'') {
                        i += 2;
                        continue;
                    }
                    return i + 1;
                }
                i += 1;
            }
            end
        }
        Style::Double => {
            let mut i = start + 1;
            while i < b.len() {
                match b[i] {
                    b'\\' => i += 2,
                    b'"' => return i + 1,
                    _ => i += 1,
                }
            }
            end
        }
        Style::Plain => {
            let mut e = end.min(b.len());
            // Cut at a comment (` #`) inside the span.
            if let Some(p) = src[start..e].find(" #") {
                e = start + p;
            }
            trim_end_ws(src, start, e)
        }
        Style::Block => trim_end_ws(src, start, end),
    }
}

fn trim_end_ws(src: &str, start: usize, end: usize) -> usize {
    let b = src.as_bytes();
    let mut e = end.min(b.len());
    while e > start && (b[e - 1] == b' ' || b[e - 1] == b'\n' || b[e - 1] == b'\r' || b[e - 1] == b'\t') {
        e -= 1;
    }
    e
}

fn mark_aliased(n: &mut YNode, inherited: bool) {
    let a = n.aliased || inherited;
    n.aliased = a;
    match &mut n.kind {
        YKind::Map { entries, .. } => {
            for (k, v) in entries {
                mark_aliased(k, a);
                mark_aliased(v, a);
            }
        }
        YKind::Seq { items, .. } => {
            for i in items {
                mark_aliased(i, a);
            }
        }
        _ => {}
    }
}

/// Parse a YAML stream into located documents. Empty documents (only
/// comments, or a null root) are dropped.
pub fn parse(src: &str) -> Result<Vec<YDoc>, YamlError> {
    let char_to_byte = if src.is_ascii() {
        None
    } else {
        let mut t: Vec<usize> = src.char_indices().map(|(b, _)| b).collect();
        t.push(src.len());
        Some(t)
    };
    let mut b = Builder { src, char_to_byte, stack: vec![], docs: vec![], anchors: HashMap::new(), err: None };
    let mut p = Parser::new_from_str(src);
    p.load(&mut b, true).map_err(|e| {
        let at = e.marker().index();
        YamlError { msg: e.info().to_string(), at }
    })?;
    if let Some(e) = b.err {
        return Err(e);
    }
    let mut docs = b.docs;
    for d in &mut docs {
        mark_aliased(&mut d.root, false);
    }
    docs.retain(|d| !d.value.is_null());
    Ok(docs)
}

/// Whether plain `s` resolves to null (YAML 1.2 core schema).
pub fn is_null_plain(s: &str) -> bool {
    matches!(s, "" | "~" | "null" | "Null" | "NULL")
}

/// Resolve a scalar to JSON (YAML 1.2 core schema; tags `!!str`, `!!int`, … honoured).
pub fn resolve_scalar(text: &str, style: Style, tag: Option<&str>) -> Value {
    if let Some(t) = tag {
        let t = t.trim_start_matches("tag:yaml.org,2002:").trim_start_matches("!!");
        match t {
            "str" => return Value::String(text.into()),
            "int" | "float" => {
                if let Some(v) = resolve_number(text) {
                    return v;
                }
                return Value::String(text.into());
            }
            "bool" => {
                if let Some(b) = resolve_bool(text) {
                    return Value::Bool(b);
                }
            }
            "null" => return Value::Null,
            _ => {}
        }
    }
    if style != Style::Plain {
        return Value::String(text.into());
    }
    if is_null_plain(text) {
        return Value::Null;
    }
    if let Some(b) = resolve_bool(text) {
        return Value::Bool(b);
    }
    if let Some(n) = resolve_number(text) {
        return n;
    }
    Value::String(text.into())
}

fn resolve_bool(s: &str) -> Option<bool> {
    match s {
        "true" | "True" | "TRUE" => Some(true),
        "false" | "False" | "FALSE" => Some(false),
        _ => None,
    }
}

fn resolve_number(s: &str) -> Option<Value> {
    let b = s.as_bytes();
    if b.is_empty() {
        return None;
    }
    // Integers: [-+]?[0-9]+, 0o[0-7]+, 0x[0-9a-fA-F]+
    let (neg, digits) = match b[0] {
        b'-' => (true, &s[1..]),
        b'+' => (false, &s[1..]),
        _ => (false, s),
    };
    if !digits.is_empty() && digits.bytes().all(|c| c.is_ascii_digit()) {
        if let Ok(v) = s.parse::<i64>() {
            return Some(Value::from(v));
        }
        if !neg && let Ok(v) = digits.parse::<u64>() {
            return Some(Value::from(v));
        }
        return s.parse::<f64>().ok().and_then(serde_json::Number::from_f64).map(Value::Number);
    }
    if let Some(h) = s.strip_prefix("0x")
        && !h.is_empty()
        && h.bytes().all(|c| c.is_ascii_hexdigit())
    {
        return i64::from_str_radix(h, 16).ok().map(Value::from);
    }
    if let Some(o) = s.strip_prefix("0o")
        && !o.is_empty()
        && o.bytes().all(|c| (b'0'..=b'7').contains(&c))
    {
        return i64::from_str_radix(o, 8).ok().map(Value::from);
    }
    // Floats: [-+]?(\.[0-9]+|[0-9]+(\.[0-9]*)?)([eE][-+]?[0-9]+)?
    if is_yaml_float(digits) {
        return s.parse::<f64>().ok().and_then(serde_json::Number::from_f64).map(Value::Number);
    }
    None
}

fn is_yaml_float(s: &str) -> bool {
    let b = s.as_bytes();
    let mut i = 0;
    let mut int_digits = 0;
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
        int_digits += 1;
    }
    let mut frac_digits = 0;
    if i < b.len() && b[i] == b'.' {
        i += 1;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
            frac_digits += 1;
        }
    }
    if int_digits == 0 && frac_digits == 0 {
        return false;
    }
    if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
        i += 1;
        if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
            i += 1;
        }
        let mut e = 0;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
            e += 1;
        }
        if e == 0 {
            return false;
        }
    }
    i == b.len()
}

/// Whether `s` can be written as a plain scalar and read back as the same string.
pub fn plain_safe(s: &str) -> bool {
    if s.is_empty() || s.trim() != s {
        return false;
    }
    if !matches!(resolve_scalar(s, Style::Plain, None), Value::String(_)) {
        return false;
    }
    let first = s.as_bytes()[0];
    if b"-?:,[]{}#&*!|>'\"%@`".contains(&first) {
        // `-` alone or `- ` is an indicator; a leading `-` followed by a
        // non-space is fine in YAML 1.2, but keep it simple and quote.
        return false;
    }
    if s.contains(": ") || s.contains(" #") || s.ends_with(':') || s.contains('\n') || s.contains('\t') {
        return false;
    }
    // Avoid YAML 1.1 surprises in other readers (yes/no/on/off).
    !matches!(s.to_ascii_lowercase().as_str(), "yes" | "no" | "on" | "off" | "y" | "n")
}

/// Whether `s` is safe as a plain scalar inside a flow collection.
pub fn flow_plain_safe(s: &str) -> bool {
    plain_safe(s) && !s.contains([',', '[', ']', '{', '}'])
}

/// Quote `s` as a double-quoted YAML scalar.
pub fn double_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Quote `s` as a single-quoted YAML scalar.
pub fn single_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Text of a scalar value for writing, preferring plain style.
pub fn scalar_text(v: &Value, flow: bool) -> String {
    match v {
        Value::String(s) => {
            if (flow && flow_plain_safe(s)) || (!flow && plain_safe(s)) {
                s.clone()
            } else {
                double_quote(s)
            }
        }
        Value::Null => "null".into(),
        other => other.to_string(),
    }
}

/// Render a JSON value in flow style (`{a: b, c: [1, 2]}`).
pub fn flow_text(v: &Value) -> String {
    match v {
        Value::Object(m) => {
            let parts: Vec<String> = m.iter().map(|(k, v)| format!("{}: {}", scalar_text(&Value::String(k.clone()), true), flow_text(v))).collect();
            format!("{{{}}}", parts.join(", "))
        }
        Value::Array(a) => format!("[{}]", a.iter().map(flow_text).collect::<Vec<_>>().join(", ")),
        other => scalar_text(other, true),
    }
}

/// Render `key: value` lines in block style at `indent` spaces; nested
/// mappings are indented by `step`. Sequences are written in flow style.
/// Every line ends with `\n`.
pub fn block_entry(key: &str, v: &Value, indent: usize, step: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    let k = scalar_text(&Value::String(key.to_string()), false);
    match v {
        Value::Object(m) if !m.is_empty() => {
            out.push_str(&format!("{pad}{k}:\n"));
            for (ck, cv) in m {
                block_entry(ck, cv, indent + step, step, out);
            }
        }
        Value::Object(_) => out.push_str(&format!("{pad}{k}: {{}}\n")),
        Value::Array(_) => out.push_str(&format!("{pad}{k}: {}\n", flow_text(v))),
        other => out.push_str(&format!("{pad}{k}: {}\n", scalar_text(other, false))),
    }
}

/// Infer the document's indentation step: the smallest positive difference
/// between a nested block mapping's column and its parent mapping's column.
pub fn indent_step(root: &YNode) -> usize {
    fn walk(n: &YNode, parent_col: Option<usize>, best: &mut Option<usize>) {
        match &n.kind {
            YKind::Map { flow: false, entries } => {
                if let Some(p) = parent_col
                    && n.col > p
                {
                    let d = n.col - p;
                    *best = Some(best.map_or(d, |b: usize| b.min(d)));
                }
                for (_, v) in entries {
                    walk(v, Some(n.col), best);
                }
            }
            YKind::Seq { flow: false, items } => {
                for i in items {
                    // Items of a block sequence under a key: compare with the
                    // key's mapping column only when the item is a mapping
                    // that is indented relative to it (`- ` adds 2).
                    walk(i, None, best);
                }
            }
            _ => {}
        }
    }
    let mut best = None;
    walk(root, None, &mut best);
    best.filter(|b| *b > 0 && *b <= 8).unwrap_or(2)
}

/// Byte offset of the start of the line after the last non-whitespace
/// character at or before `end` (where a new block entry goes).
pub fn line_after(src: &str, end: usize) -> usize {
    let b = src.as_bytes();
    let mut i = end.min(b.len());
    while i < b.len() && b[i] != b'\n' {
        i += 1;
    }
    if i < b.len() { i + 1 } else { b.len() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn values_and_spans() {
        let y = "# c\napiVersion: v1\nkind: Pod\nmetadata:\n  name: x # n\nspec:\n  containers:\n  - name: a\n    resources:\n      requests: {cpu: 1, memory: \"2Gi\"}\n    empty:\n---\nfoo: 1\n";
        let d = parse(y).unwrap();
        assert_eq!(d.len(), 2);
        assert_eq!(d[0].value["spec"]["containers"][0]["resources"]["requests"], json!({"cpu": 1, "memory": "2Gi"}));
        assert_eq!(d[0].value["spec"]["containers"][0]["empty"], Value::Null);
        let c = d[0].root.get("spec").unwrap().get("containers").unwrap().item(0).unwrap();
        let mem = c.get("resources").unwrap().get("requests").unwrap().get("memory").unwrap();
        assert_eq!(&y[mem.start..mem.end], "\"2Gi\"");
        assert_eq!(mem.scalar().unwrap().1, Style::Double);
        let res = c.get("resources").unwrap();
        assert_eq!(res.col, 6);
        assert_eq!(indent_step(&d[0].root), 2);
        assert_eq!(d[1].value, json!({"foo": 1}));
    }

    #[test]
    fn unicode_offsets_are_bytes() {
        let y = "a: \"é\"\nb: x\n";
        let d = parse(y).unwrap();
        let b = d[0].root.get("b").unwrap();
        assert_eq!(&y[b.start..b.end], "x");
    }

    #[test]
    fn anchors_resolve() {
        let y = "base: &b {cpu: 1}\nx:\n  <<: *b\n  memory: 2Gi\ny: *b\n";
        let d = parse(y).unwrap();
        assert_eq!(d[0].value["x"], json!({"cpu": 1, "memory": "2Gi"}));
        assert_eq!(d[0].value["y"], json!({"cpu": 1}));
        assert!(d[0].root.get("base").unwrap().aliased);
    }

    #[test]
    fn scalar_resolution() {
        assert_eq!(resolve_scalar("1e3", Style::Plain, None), json!(1000.0));
        assert_eq!(resolve_scalar("500m", Style::Plain, None), json!("500m"));
        assert_eq!(resolve_scalar("2", Style::Double, None), json!("2"));
        assert_eq!(resolve_scalar("0x10", Style::Plain, None), json!(16));
        assert_eq!(resolve_scalar("~", Style::Plain, None), Value::Null);
        assert!(plain_safe("4Gi"));
        assert!(!plain_safe("2"));
        assert!(!plain_safe("on"));
        assert_eq!(scalar_text(&json!("2"), false), "\"2\"");
    }

    #[test]
    fn block_render() {
        let mut s = String::new();
        block_entry("resources", &json!({"requests": {"cpu": "2", "memory": "4Gi"}}), 4, 2, &mut s);
        assert_eq!(s, "    resources:\n      requests:\n        cpu: \"2\"\n        memory: 4Gi\n");
    }
}
