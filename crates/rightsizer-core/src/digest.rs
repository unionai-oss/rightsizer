//! Content digests: SHA-256 hex, canonical JSON, the context digest (SPEC
//! §5.2) and the history watermark (SPEC §6.3).

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::types::{RunRecord, WorkloadContext};

/// Lowercase hex SHA-256 of `data`.
pub fn sha256_hex(data: &[u8]) -> String {
    let out = Sha256::digest(data);
    let mut s = String::with_capacity(64);
    for b in out.iter() {
        s.push(char::from_digit((b >> 4) as u32, 16).unwrap_or('0'));
        s.push(char::from_digit((b & 0xf) as u32, 16).unwrap_or('0'));
    }
    s
}

/// Canonical JSON bytes of a value: struct fields in declaration order, maps
/// sorted (every map in the data model is a `BTreeMap`), no whitespace.
pub fn canonical_json<T: Serialize>(value: &T) -> Vec<u8> {
    // Round-trip through `Value` so any map ordering is normalized.
    match serde_json::to_value(value) {
        Ok(v) => serde_json::to_vec(&sort_value(v)).unwrap_or_default(),
        Err(_) => Vec::new(),
    }
}

fn sort_value(v: serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    match v {
        Value::Object(m) => {
            let mut entries: Vec<(String, Value)> = m.into_iter().collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            Value::Object(entries.into_iter().map(|(k, v)| (k, sort_value(v))).collect())
        }
        Value::Array(a) => Value::Array(a.into_iter().map(sort_value).collect()),
        other => other,
    }
}

/// `sha256:` digest of the context's canonical JSON (SPEC §5.2).
pub fn context_digest(ctx: &WorkloadContext) -> String {
    format!("sha256:{}", sha256_hex(&canonical_json(ctx)))
}

/// History watermark: SHA-256 of the sorted ids of the included records
/// (SPEC §6.3). A record's id is `(invocation_id, attempt, source)`.
pub fn history_watermark(history: &[RunRecord]) -> String {
    let mut ids: Vec<String> = history
        .iter()
        .map(|r| format!("{}#{}#{}", r.invocation_id, r.attempt, serde_json::to_string(&r.source).unwrap_or_default()))
        .collect();
    ids.sort();
    sha256_hex(ids.join("\n").as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_sha256() {
        assert_eq!(sha256_hex(b"abc"), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    }

    #[test]
    fn canonical_sorts_keys() {
        let a: serde_json::Value = serde_json::from_str(r#"{"b":1,"a":{"d":2,"c":3}}"#).unwrap();
        assert_eq!(String::from_utf8(canonical_json(&a)).unwrap(), r#"{"a":{"c":3,"d":2},"b":1}"#);
    }
}
