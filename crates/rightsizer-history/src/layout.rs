//! Object-store layout (SPEC §8.1). Pure path builders.
//!
//! ```text
//! {prefix}/v1/runs/{tenant}/{key-hash}/{ulid}.json        immutable shards (one per record)
//! {prefix}/v1/decisions/{tenant}/{date}/{ulid}.json
//! {prefix}/v1/attempts/{tenant}/{key-hash}/{invocation-hash}.json
//! {prefix}/v1/latest/{tenant}/{key-hash}.json             pointer: last K records inline + watermark ULID
//! {prefix}/v1/breakers/{scope}.json                       resets: operator-written
//! {prefix}/v1/breakers-trips/{scope}/{ulid}.json           trips: client-creatable (N-1: breakers/{scope}/trips/{ulid}.json, still read)
//! {prefix}/v1/control/mode.json
//! {prefix}/v1/snapshots/{tenant}/{date}.parquet
//! {prefix}/v1/compaction/lease.json                        singleton compaction lease (addition)
//! {prefix}/v1/compaction/status.json                       last compaction, for metrics (addition)
//! ```

use chrono::{DateTime, Utc};
use rightsizer_core::WorkloadKey;
use rightsizer_core::breaker::BreakerScope;
use rightsizer_core::digest::sha256_hex;
use ulid::Ulid;

fn join(prefix: &str, rest: &str) -> String {
    let p = prefix.trim_end_matches('/');
    if p.is_empty() { format!("v1/{rest}") } else { format!("{p}/v1/{rest}") }
}

/// Tenant path segment: the key's platform plus its tenant.
pub fn tenant_dir(key: &WorkloadKey) -> String {
    format!("{}/{}", key.platform, key.tenant)
}

/// A run shard.
pub fn run_shard(prefix: &str, key: &WorkloadKey, id: Ulid) -> String {
    join(prefix, &format!("runs/{}/{}/{id}.json", tenant_dir(key), key.hash()))
}

/// The directory of a key's run shards (LIST with `start-after`).
pub fn run_dir(prefix: &str, key: &WorkloadKey) -> String {
    join(prefix, &format!("runs/{}/{}/", tenant_dir(key), key.hash()))
}

/// A decision shard.
pub fn decision_shard(prefix: &str, key: &WorkloadKey, at: DateTime<Utc>, id: Ulid) -> String {
    join(prefix, &format!("decisions/{}/{}/{id}.json", tenant_dir(key), at.format("%Y-%m-%d")))
}

/// An attempt record / ladder lease.
pub fn attempt(prefix: &str, key: &WorkloadKey, invocation_id: &str) -> String {
    join(prefix, &format!("attempts/{}/{}/{}.json", tenant_dir(key), key.hash(), &sha256_hex(invocation_id.as_bytes())[..32]))
}

/// A key's `latest` pointer.
pub fn latest(prefix: &str, key: &WorkloadKey) -> String {
    join(prefix, &format!("latest/{}/{}.json", tenant_dir(key), key.hash()))
}

/// A breaker's reset object.
pub fn breaker(prefix: &str, scope: &BreakerScope) -> String {
    join(prefix, &scope.reset_object())
}

/// A breaker trip object (`If-None-Match: *`).
pub fn breaker_trip(prefix: &str, scope: &BreakerScope, id: Ulid) -> String {
    join(prefix, &scope.trip_object(id))
}

/// The control object.
pub fn control(prefix: &str) -> String {
    join(prefix, "control/mode.json")
}

/// A daily snapshot.
pub fn snapshot(prefix: &str, key: &WorkloadKey, day: DateTime<Utc>) -> String {
    join(prefix, &format!("snapshots/{}/{}.parquet", tenant_dir(key), day.format("%Y-%m-%d")))
}

/// The compaction lease (`If-None-Match: *`, takeover on expiry by ETag).
pub fn compaction_lease(prefix: &str) -> String {
    join(prefix, "compaction/lease.json")
}

/// The last compaction's status.
pub fn compaction_status(prefix: &str) -> String {
    join(prefix, "compaction/status.json")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths() {
        let k = WorkloadKey::parse("k8s://prod/ml/job/x").unwrap();
        let id = Ulid::from_parts(1_700_000_000_000, 7);
        let p = run_shard("s3://b/rs/", &k, id);
        assert!(p.starts_with("s3://b/rs/v1/runs/k8s/prod/ml/"));
        assert!(p.ends_with(&format!("/{id}.json")));
        assert_eq!(control(""), "v1/control/mode.json");
        assert!(attempt("p", &k, "inv-1").starts_with("p/v1/attempts/k8s/prod/ml/"));
        assert_eq!(breaker("p", &BreakerScope::Fleet), "p/v1/breakers/fleet.json");
        assert_eq!(breaker_trip("p", &BreakerScope::Fleet, id), format!("p/v1/breakers-trips/fleet/{id}.json"));
    }
}
