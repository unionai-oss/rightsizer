//! Process-wide metadata cache: (URI digest, validator) → extracted
//! metadata, LRU-bounded. Keys hold a SHA-256 of the URI, not the URI.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, OnceLock};

use sha2::{Digest, Sha256};

use super::Extracted;

/// Entries kept before the least recently used is evicted.
pub const METADATA_CACHE_CAPACITY: usize = 4096;

pub(crate) type Key = (String, String);

#[derive(Default)]
struct Lru {
    tick: u64,
    map: HashMap<Key, (u64, Extracted)>,
    order: BTreeMap<u64, Key>,
}

fn cache() -> &'static Mutex<Lru> {
    static C: OnceLock<Mutex<Lru>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(Lru::default()))
}

fn lock() -> std::sync::MutexGuard<'static, Lru> {
    cache().lock().unwrap_or_else(|e| e.into_inner())
}

/// Cache key for `uri` at `validator` (ETag, or `size:mtime`).
pub(crate) fn key(uri: &str, validator: &str) -> Key {
    (hex::encode(Sha256::digest(uri.as_bytes())), validator.to_string())
}

pub(crate) fn get(k: &Key) -> Option<Extracted> {
    let mut c = lock();
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
    if let Some((old, _)) = c.map.insert(k.clone(), (t, v)) {
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
}
