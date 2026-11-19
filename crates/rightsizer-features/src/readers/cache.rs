//! Process-wide metadata cache: (URI digest, validator) → extracted
//! metadata, LRU-bounded. Keys hold a SHA-256 of the URI, not the URI.
//! Entries without a content validator (`hf://` refs, keyed by endpoint and
//! credential identity) are read with a maximum age ([`HF_METADATA_TTL`]).

use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, OnceLock};

use sha2::{Digest, Sha256};

use super::Extracted;

/// Entries kept before the least recently used is evicted.
pub const METADATA_CACHE_CAPACITY: usize = 4096;

/// Maximum age of an `hf://` entry (1 h): a revision such as `main` moves,
/// and access to a gated repo can be revoked.
pub const HF_METADATA_TTL: std::time::Duration = std::time::Duration::from_secs(3600);

pub(crate) type Key = (String, String);

#[derive(Default)]
struct Lru {
    tick: u64,
    map: HashMap<Key, (u64, Extracted, std::time::Instant)>,
    order: BTreeMap<u64, Key>,
}

fn cache() -> &'static Mutex<Lru> {
    static C: OnceLock<Mutex<Lru>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(Lru::default()))
}

fn lock() -> std::sync::MutexGuard<'static, Lru> {
    cache().lock().unwrap_or_else(|e| e.into_inner())
}

/// Cache key for `uri` at `validator` (ETag, or `size:mtime`). HTTP(S)
/// URIs are keyed without their query ([`crate::privacy::identity_uri`]):
/// a re-signed presigned URL is the same object.
pub(crate) fn key(uri: &str, validator: &str) -> Key {
    (hex::encode(Sha256::digest(crate::privacy::identity_uri(uri).as_bytes())), validator.to_string())
}

pub(crate) fn get(k: &Key) -> Option<Extracted> {
    get_fresh(k, None)
}

/// An entry no older than `max_age` (expired entries are dropped).
pub(crate) fn get_fresh(k: &Key, max_age: Option<std::time::Duration>) -> Option<Extracted> {
    let mut c = lock();
    if let Some(age) = max_age
        && c.map.get(k).is_some_and(|e| e.2.elapsed() > age)
    {
        if let Some((old, ..)) = c.map.remove(k) {
            c.order.remove(&old);
        }
        return None;
    }
    c.tick += 1;
    let t = c.tick;
    let (old, value) = {
        let e = c.map.get_mut(k)?;
        let old = e.0;
        e.0 = t;
        (old, e.1.clone())
    };
    c.order.remove(&old);
    c.order.insert(t, k.clone());
    Some(value)
}

pub(crate) fn put(k: Key, v: Extracted) {
    let mut c = lock();
    c.tick += 1;
    let t = c.tick;
    if let Some((old, ..)) = c.map.insert(k.clone(), (t, v, std::time::Instant::now())) {
        c.order.remove(&old);
    }
    c.order.insert(t, k);
    while c.map.len() > METADATA_CACHE_CAPACITY {
        let Some((_, oldest)) = c.order.pop_first() else { break };
        c.map.remove(&oldest);
    }
}

/// Drop every cached entry (tests, or after credentials change).
pub fn clear_metadata_cache() {
    let mut c = lock();
    c.map.clear();
    c.order.clear();
}

/// Number of cached entries.
pub fn metadata_cache_len() -> usize {
    lock().map.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lru_evicts_oldest() {
        // Unique URIs so parallel tests sharing the cache do not interfere.
        let k0 = key("lru-test://0", "v");
        put(k0.clone(), Extracted::default());
        for i in 1..=METADATA_CACHE_CAPACITY {
            if i == 10 {
                assert!(get(&k0).is_some()); // touch: k0 becomes recent
            }
            put(key(&format!("lru-test://{i}"), "v"), Extracted::default());
        }
        assert!(metadata_cache_len() <= METADATA_CACHE_CAPACITY);
        assert!(get(&k0).is_some());
        assert!(get(&key("lru-test://1", "v")).is_none());
        assert!(get(&key("lru-test://0", "other")).is_none());
    }

    #[test]
    fn max_age_expires_entries() {
        let k = key("ttl-test://0", "v");
        put(k.clone(), Extracted::default());
        assert!(get_fresh(&k, Some(std::time::Duration::from_secs(60))).is_some());
        std::thread::sleep(std::time::Duration::from_millis(20));
        assert!(get_fresh(&k, Some(std::time::Duration::from_millis(5))).is_none(), "expired");
        assert!(get(&k).is_none(), "and dropped");
    }

    #[test]
    fn http_query_is_not_part_of_the_key() {
        assert_eq!(key("https://h/x.parquet?sig=1", "e"), key("https://h/x.parquet?sig=2", "e"));
        assert_ne!(key("s3://b/x?1", "e"), key("s3://b/x?2", "e"));
    }
}
