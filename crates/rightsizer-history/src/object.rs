//! The object-store backend (SPEC §8.1): immutable shards, `latest`
//! pointers with watermarks, conditional PUTs for trips, attempt records,
//! pointers and the compaction lease; local FS, S3 (and S3-compatible
//! stores such as MinIO), GCS and Azure through `object_store`.
//!
//! Layout ([`crate::layout`]):
//!
//! ```text
//! {prefix}/v1/runs/{platform}/{tenant}/{key-hash}/{ulid}.json      immutable shards
//! {prefix}/v1/latest/{platform}/{tenant}/{key-hash}.json           pointer (keep_last records + watermark)
//! {prefix}/v1/decisions/{platform}/{tenant}/{date}/{ulid}.json
//! {prefix}/v1/attempts/{platform}/{tenant}/{key-hash}/{inv-hash}.json
//! {prefix}/v1/breakers/{scope}.json                                operator-written reset
//! {prefix}/v1/breakers/{scope}/trips/{ulid}.json                   client trips (create-only)
//! {prefix}/v1/control/mode.json                                    operator-written
//! {prefix}/v1/snapshots/{platform}/{tenant}/{date}.parquet         daily snapshots
//! {prefix}/v1/compaction/lease.json                                singleton lease (addition)
//! {prefix}/v1/compaction/status.json                               last compaction (addition)
//! ```
//!
//! **Reads.** GET the `latest` pointer, LIST the key's shards after the
//! pointer's watermark (`start-after`), GET those, merge with the pointer's
//! records, deduplicate on `(key, invocation_id, attempt, source)` (first
//! written wins) and return newest first. Only `*.json` objects are read,
//! so the Python writer's `.rs-*.tmp` staging files are ignored.
//!
//! **Conditional writes.** Trips, shards, decisions and attempt records use
//! `PutMode::Create` (`If-None-Match: *`). Pointers, attempt-lease closes and
//! the compaction lease takeover use `PutMode::Update` on the ETag. Local
//! filesystems implement `Create` (hard link) but not `Update`; there the
//! update is **emulated** with a create-only lock object `{path}.lock`
//! (stale after [`ObjectStoreOptions::lock_stale_after`]) plus an ETag
//! re-check under the lock. Every writer of that path goes through the
//! same lock, so the emulation is exact among rightsizer processes sharing
//! the directory.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, NaiveDate, Utc};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use rightsizer_core::breaker::{BreakerObjects, BreakerScope, Control, Reset, Trip};
use rightsizer_core::{AttemptRecord, Decision, Lease, RunRecord, RunStatus, WorkloadKey};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::snapshot::{SnapshotRow, read_snapshot, write_snapshot};
use crate::{
    ATTEMPT_TTL, Backend, CompactOptions, CompactReport, CompactionLease, CompactionStatus, DecisionFilter, HistoryStore,
    PutOutcome, Role, StoreError, StoreInfo, StoreStats, attempt_expired, decode_value, decode_versioned, dedup_key,
    layout, merge_runs, ulid_time,
};

/// Records the write-time duplicate check looks at (`check_duplicates_on_write`).
pub const DUPLICATE_CHECK_DEPTH: usize = 8;

/// Concurrent GETs of one history read (it is on a decision's critical path,
/// at most `limit + limit/4` objects).
pub const HISTORY_READ_CONCURRENCY: usize = 64;

/// Tuning of an [`ObjectHistoryStore`].
#[derive(Clone, Debug, PartialEq)]
pub struct ObjectStoreOptions {
    /// Credentials role (client stores refuse resets and control writes).
    pub role: Role,
    /// Wire version written (SPEC §13.2 `history.write_version`).
    pub write_version: u16,
    /// Survives the host (false for temp dirs and `memory://`).
    pub durable: bool,
    /// Shared across hosts (false for a host-local directory).
    pub shared: bool,
    /// Read the key's history before `put_run` to report duplicates (one
    /// GET + LIST per write). Reads deduplicate regardless.
    pub check_duplicates_on_write: bool,
    /// Compaction folds a shard into the pointer watermark only once it is
    /// this old, so a PUT whose ULID was minted before compaction's LIST but
    /// that lands after it is never skipped by readers (5 min).
    pub settle: Duration,
    /// Compaction lease lifetime (1 h); a stale lease is taken over.
    pub lease_ttl: Duration,
    /// Attempt record lifetime (7 d).
    pub attempt_ttl: Duration,
    /// Emulate `PutMode::Update` with a lock object (auto for local FS).
    pub emulate_update: bool,
    /// A lock object older than this is stale and broken (30 s).
    pub lock_stale_after: Duration,
    /// Concurrent GETs per read (16).
    pub concurrency: usize,
    /// Compaction holder id.
    pub holder: String,
}

impl Default for ObjectStoreOptions {
    fn default() -> Self {
        ObjectStoreOptions {
            role: Role::Client,
            write_version: rightsizer_core::WIRE_VERSION,
            durable: true,
            shared: true,
            check_duplicates_on_write: true,
            settle: Duration::from_secs(300),
            lease_ttl: Duration::from_secs(3600),
            attempt_ttl: ATTEMPT_TTL,
            emulate_update: false,
            lock_stale_after: Duration::from_secs(30),
            concurrency: 16,
            holder: crate::default_holder(),
        }
    }
}

/// The `latest` pointer: the newest `keep_last` records inline plus the
/// watermark (the newest shard ULID compaction folded in).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LatestPointer {
    /// Wire version.
    #[serde(default = "v1")]
    pub v: u16,
    /// Key.
    pub key: WorkloadKey,
    /// Readers LIST shards after this ULID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watermark: Option<Ulid>,
    /// Records, newest first (decoded with the N/N-1 rule one by one).
    #[serde(default)]
    pub records: Vec<serde_json::Value>,
    /// When compaction wrote it.
    pub updated_at: DateTime<Utc>,
}

fn v1() -> u16 {
    1
}

/// An object store over any `object_store` backend.
pub struct ObjectHistoryStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    uri: String,
    opts: ObjectStoreOptions,
    ulids: std::sync::Mutex<ulid::Generator>,
    skipped: AtomicU64,
}

/// Lease bookkeeping of one compaction run.
struct LeaseGuard {
    started: std::time::Instant,
    last: std::time::Instant,
}

/// Write mode of [`ObjectHistoryStore`]'s blob layer.
enum Mode {
    Overwrite,
    Create,
    Update(UpdateVersion),
}

impl ObjectHistoryStore {
    /// Wrap an object store at `prefix` (a path inside the store, e.g. `"rs"`
    /// or `""`). `durable_shared` is false for local paths.
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>, uri: impl Into<String>, durable_shared: bool) -> Self {
        let opts = ObjectStoreOptions { durable: durable_shared, shared: durable_shared, ..Default::default() };
        Self::with_options(store, prefix, uri, opts)
    }

    /// Wrap with explicit options.
    pub fn with_options(store: Arc<dyn ObjectStore>, prefix: impl Into<String>, uri: impl Into<String>, opts: ObjectStoreOptions) -> Self {
        ObjectHistoryStore {
            store,
            prefix: prefix.into().trim_matches('/').to_string(),
            uri: uri.into(),
            opts,
            ulids: std::sync::Mutex::new(ulid::Generator::new()),
            skipped: AtomicU64::new(0),
        }
    }

    /// A store on a local directory (created if missing). Updates are
    /// emulated with lock objects; `shared` is true when the directory is on
    /// a network filesystem ([`crate::fsdetect`]), `durable` false under the
    /// system temp dir.
    pub fn local(dir: &std::path::Path, mut opts: ObjectStoreOptions) -> Result<Self, StoreError> {
        std::fs::create_dir_all(dir).map_err(|e| StoreError::Config(format!("{}: {e}", dir.display())))?;
        let dir = dir.canonicalize().map_err(|e| StoreError::Config(format!("{}: {e}", dir.display())))?;
        let fs = object_store::local::LocalFileSystem::new_with_prefix(&dir).map_err(|e| StoreError::Config(e.to_string()))?;
        opts.emulate_update = true;
        Ok(Self::with_options(Arc::new(fs), "", format!("file://{}", dir.display()), opts))
    }

    /// An in-memory object store (tests; behaves like S3: native
    /// `Create`/`Update`). Not durable.
    pub fn in_memory(mut opts: ObjectStoreOptions) -> Self {
        opts.durable = false;
        opts.shared = false;
        Self::with_options(Arc::new(object_store::memory::InMemory::new()), "", "memory://", opts)
    }

    /// The underlying store.
    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    /// The prefix.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The options.
    pub fn options(&self) -> &ObjectStoreOptions {
        &self.opts
    }

    fn next_ulid(&self) -> Ulid {
        self.ulids.lock().ok().and_then(|mut g| g.generate().ok()).unwrap_or_else(Ulid::generate)
    }

    fn skip(&self) {
        self.skipped.fetch_add(1, Ordering::Relaxed);
    }

    fn rel(&self, rest: &str) -> String {
        layout_join(&self.prefix, rest)
    }

    // ── blob layer ──────────────────────────────────────────────────────

    async fn get(&self, path: &str) -> Result<Option<(Bytes, UpdateVersion)>, StoreError> {
        match self.store.get(&Path::from(path)).await {
            Ok(r) => {
                let version = UpdateVersion { e_tag: r.meta.e_tag.clone(), version: r.meta.version.clone() };
                let bytes = r.bytes().await.map_err(|e| map_err(path, e))?;
                Ok(Some((bytes, version)))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(map_err(path, e)),
        }
    }

    async fn get_json<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<Option<(T, UpdateVersion)>, StoreError> {
        match self.get(path).await? {
            None => Ok(None),
            Some((b, ver)) => match decode_versioned::<T>(&b, path) {
                Ok(Some(t)) => Ok(Some((t, ver))),
                Ok(None) => {
                    self.skip();
                    Ok(None)
                }
                Err(e) => Err(e),
            },
        }
    }

    async fn put(&self, path: &str, data: Vec<u8>, mode: Mode) -> Result<UpdateVersion, StoreError> {
        let location = Path::from(path);
        let pm = match mode {
            Mode::Overwrite => PutMode::Overwrite,
            Mode::Create => PutMode::Create,
            Mode::Update(v) => {
                if self.opts.emulate_update {
                    return self.put_update_emulated(path, data, v).await;
                }
                PutMode::Update(v)
            }
        };
        let fallback = if let PutMode::Update(v) = &pm { Some(v.clone()) } else { None };
        let payload = PutPayload::from(data.clone());
        match self.store.put_opts(&location, payload, PutOptions { mode: pm, ..Default::default() }).await {
            Ok(r) => Ok(UpdateVersion { e_tag: r.e_tag, version: r.version }),
            Err(object_store::Error::AlreadyExists { .. }) => Err(StoreError::Conflict(format!("{path} exists"))),
            Err(object_store::Error::Precondition { .. }) => Err(StoreError::Conflict(format!("{path} changed"))),
            Err(object_store::Error::NotImplemented { .. }) if fallback.is_some() => {
                // A backend without conditional updates: emulate.
                self.put_update_emulated(path, data, fallback.unwrap_or(UpdateVersion { e_tag: None, version: None })).await
            }
            Err(e) => Err(map_err(path, e)),
        }
    }

    /// `PutMode::Update` emulation: hold `{path}.lock` (create-only), re-check
    /// the ETag, overwrite, release.
    ///
    /// The lock carries a unique token. A stale lock is broken only under a
    /// second create-only lock and after re-checking that it is still the
    /// stale one observed ([`Self::break_stale_lock`]); release deletes the
    /// lock only if it still carries our token. Deleting unconditionally (the previous
    /// scheme) let two waiters that both saw the same stale lock both end up
    /// "holding" it, and a holder that outlived `lock_stale_after` delete its
    /// successor's lock.
    async fn put_update_emulated(&self, path: &str, data: Vec<u8>, expected: UpdateVersion) -> Result<UpdateVersion, StoreError> {
        let lock = format!("{path}.lock");
        let lock_path = Path::from(lock.as_str());
        let token = self.next_ulid().to_string();
        let holder = serde_json::to_vec(&serde_json::json!({"holder": self.opts.holder, "token": token, "at": Utc::now()})).unwrap_or_default();
        let mut delay = Duration::from_millis(2);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let r = self
                .store
                .put_opts(&lock_path, PutPayload::from(holder.clone()), PutOptions { mode: PutMode::Create, ..Default::default() })
                .await;
            match r {
                Ok(_) => break,
                Err(object_store::Error::AlreadyExists { .. }) => {
                    // Break a stale lock (its holder died).
                    if let Ok(g) = self.store.get(&lock_path).await {
                        let age = Utc::now() - g.meta.last_modified;
                        let stale = age.to_std().unwrap_or_default() > self.opts.lock_stale_after;
                        if stale
                            && let Ok(seen) = g.bytes().await
                            && self.break_stale_lock(&lock_path, &seen).await
                        {
                            continue;
                        }
                    }
                    if tokio::time::Instant::now() > deadline {
                        return Err(StoreError::Conflict(format!("{path} is locked")));
                    }
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_millis(50));
                }
                Err(e) => return Err(map_err(&lock, e)),
            }
        }
        let result = async {
            let current = match self.store.head(&Path::from(path)).await {
                Ok(m) => Some(m.e_tag),
                Err(object_store::Error::NotFound { .. }) => None,
                Err(e) => return Err(map_err(path, e)),
            };
            match current {
                Some(tag) if tag == expected.e_tag => {}
                _ => return Err(StoreError::Conflict(format!("{path} changed"))),
            }
            let r = self
                .store
                .put_opts(&Path::from(path), PutPayload::from(data), PutOptions { mode: PutMode::Overwrite, ..Default::default() })
                .await
                .map_err(|e| map_err(path, e))?;
            Ok(UpdateVersion { e_tag: r.e_tag, version: r.version })
        }
        .await;
        // Release only our own lock.
        if let Ok(g) = self.store.get(&lock_path).await
            && let Ok(b) = g.bytes().await
            && b.as_ref() == holder.as_slice()
        {
            let _ = self.store.delete(&lock_path).await;
        }
        result
    }

    /// Delete the stale lock whose content is `seen`, under a create-only
    /// "breaker" lock `{lock}.break`: only its holder deletes, and only after
    /// re-reading the lock and finding the same stale content. A waiter that
    /// saw the stale lock earlier but breaks later finds the new holder's
    /// fresh lock and leaves it. `true` if this call deleted the lock.
    async fn break_stale_lock(&self, lock_path: &Path, seen: &[u8]) -> bool {
        let brk = Path::from(format!("{lock_path}.break"));
        let mine = serde_json::to_vec(&serde_json::json!({"holder": self.opts.holder, "token": self.next_ulid().to_string()})).unwrap_or_default();
        match self.store.put_opts(&brk, PutPayload::from(mine), PutOptions { mode: PutMode::Create, ..Default::default() }).await {
            Ok(_) => {}
            Err(object_store::Error::AlreadyExists { .. }) => {
                // Breaking takes milliseconds: a breaker lock this old is a crash.
                if let Ok(meta) = self.store.head(&brk).await
                    && (Utc::now() - meta.last_modified).to_std().unwrap_or_default() > self.opts.lock_stale_after * 2
                {
                    let _ = self.store.delete(&brk).await;
                }
                return false;
            }
            Err(_) => return false,
        }
        let mut broke = false;
        if let Ok(g) = self.store.get(lock_path).await {
            let age = (Utc::now() - g.meta.last_modified).to_std().unwrap_or_default();
            if age > self.opts.lock_stale_after
                && let Ok(b) = g.bytes().await
                && b.as_ref() == seen
            {
                broke = self.store.delete(lock_path).await.is_ok();
            }
        }
        let _ = self.store.delete(&brk).await;
        broke
    }

    async fn list(&self, prefix: &str, offset: Option<&str>) -> Result<Vec<ObjectMeta>, StoreError> {
        let p = Path::from(prefix.trim_end_matches('/'));
        let stream = match offset {
            Some(o) => self.store.list_with_offset(Some(&p), &Path::from(o)),
            None => self.store.list(Some(&p)),
        };
        match stream.try_collect::<Vec<_>>().await {
            Ok(v) => Ok(v),
            Err(object_store::Error::NotFound { .. }) => Ok(Vec::new()),
            Err(e) => Err(map_err(prefix, e)),
        }
    }

    async fn delete(&self, path: &str) -> Result<(), StoreError> {
        match self.store.delete(&Path::from(path)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(map_err(path, e)),
        }
    }

    async fn get_many<T: serde::de::DeserializeOwned + Send>(&self, paths: Vec<String>) -> Result<Vec<(String, T)>, StoreError> {
        self.get_many_with(paths, self.opts.concurrency).await
    }

    async fn get_many_with<T: serde::de::DeserializeOwned + Send>(&self, paths: Vec<String>, concurrency: usize) -> Result<Vec<(String, T)>, StoreError> {
        Ok(self.get_many_detailed(paths, concurrency).await?.0)
    }

    /// [`Self::get_many_with`], also returning the paths skipped because their
    /// wire version is not readable by this build (as opposed to corrupt or
    /// deleted ones): compaction must neither delete those nor fold its
    /// watermark past them (SPEC §13.2: a newer build, or this one after a
    /// revert, still reads them).
    async fn get_many_detailed<T: serde::de::DeserializeOwned + Send>(
        &self,
        paths: Vec<String>,
        concurrency: usize,
    ) -> Result<(Vec<(String, T)>, std::collections::HashSet<String>), StoreError> {
        enum Got<T> {
            Ok(String, T),
            Unsupported(String),
            Skipped,
        }
        let results: Vec<Result<Got<T>, StoreError>> = futures::stream::iter(paths)
            .map(|p| async move {
                match self.get(&p).await? {
                    None => Ok(Got::Skipped), // deleted between LIST and GET
                    Some((b, _)) => match decode_versioned::<T>(&b, &p) {
                        Ok(Some(t)) => Ok(Got::Ok(p, t)),
                        Ok(None) => {
                            self.skip();
                            Ok(Got::Unsupported(p))
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "skipping unreadable object");
                            self.skip();
                            Ok(Got::Skipped)
                        }
                    },
                }
            })
            .buffered(concurrency.max(1))
            .collect()
            .await;
        let mut out = Vec::with_capacity(results.len());
        let mut unsupported = std::collections::HashSet::new();
        for r in results {
            match r? {
                Got::Ok(p, t) => out.push((p, t)),
                Got::Unsupported(p) => {
                    unsupported.insert(p);
                }
                Got::Skipped => {}
            }
        }
        Ok((out, unsupported))
    }

    // ── runs ────────────────────────────────────────────────────────────

    async fn read_pointer(&self, key: &WorkloadKey) -> Result<Option<(LatestPointer, UpdateVersion)>, StoreError> {
        let path = self.rel(&latest_rel(key));
        // A pointer naming another key is not this key's (planted or a bug):
        // ignore it, records and watermark alike.
        Ok(self.get_json::<LatestPointer>(&path).await?.filter(|(p, _)| &p.key == key))
    }

    fn pointer_records(&self, p: &LatestPointer, path: &str) -> Vec<RunRecord> {
        let mut out = Vec::with_capacity(p.records.len());
        for v in &p.records {
            match decode_value::<RunRecord>(v.clone(), path) {
                // A pointer only ever holds its own key's records.
                Ok(Some(r)) if r.key == p.key => out.push(r),
                _ => self.skip(),
            }
        }
        out
    }

    /// Shards of `key` after `watermark`, ULID ascending, as `(ulid, path)`.
    async fn shard_list(&self, key: &WorkloadKey, watermark: Option<Ulid>) -> Result<Vec<(Ulid, String)>, StoreError> {
        let dir = self.rel(&run_dir_rel(key));
        let offset = watermark.map(|w| format!("{dir}{w}.json"));
        let metas = self.list(&dir, offset.as_deref()).await?;
        let mut v: Vec<(Ulid, String)> = metas
            .into_iter()
            .filter_map(|m| {
                let loc = m.location.to_string();
                let id = shard_ulid(&loc)?;
                // Only direct children of the key's directory.
                (loc.len() == dir.len() + 31 && watermark.is_none_or(|w| id > w)).then_some((id, loc))
            })
            .collect();
        v.sort();
        Ok(v)
    }

    /// (Also the write-time duplicate check, over [`DUPLICATE_CHECK_DEPTH`] records.)
    /// The newest `limit` records of `key`: the pointer's records plus only the
    /// newest `limit + limit/4` shards above its watermark (shard ULIDs follow
    /// write time; the slack covers records written out of start order), so an
    /// uncompacted key costs at most that many GETs per read instead of one per
    /// shard. A large `limit` reads every shard above the watermark.
    async fn newest_runs(&self, key: &WorkloadKey, limit: usize) -> Result<Vec<RunRecord>, StoreError> {
        Ok(self.newest_runs_marked(key, limit).await?.0)
    }

    /// [`Self::newest_runs`] plus the run marker from the same pointer GET
    /// and LIST: the newest shard's ULID, else the pointer watermark, else
    /// `""` (no runs).
    async fn newest_runs_marked(&self, key: &WorkloadKey, limit: usize) -> Result<(Vec<RunRecord>, Option<String>), StoreError> {
        let ptr = self.read_pointer(key).await?;
        let (mut records, watermark) = match &ptr {
            Some((p, _)) => (self.pointer_records(p, &self.rel(&latest_rel(key))), p.watermark),
            None => (Vec::new(), None),
        };
        let shards = self.shard_list(key, watermark).await?;
        let marker = shards.last().map(|(id, _)| *id).or(watermark).map(|u| u.to_string()).unwrap_or_default();
        let window = limit.saturating_add(limit / 4);
        let skip = shards.len().saturating_sub(window);
        let paths: Vec<String> = shards.into_iter().skip(skip).map(|(_, p)| p).collect();
        let got: Vec<(String, RunRecord)> = if paths.is_empty() {
            Vec::new()
        } else {
            self.get_many_with(paths, HISTORY_READ_CONCURRENCY.max(self.opts.concurrency)).await?
        };
        records.extend(got.into_iter().map(|(_, r)| r).filter(|r| &r.key == key));
        Ok((merge_runs(records, limit), Some(marker)))
    }

    // ── compaction lease ────────────────────────────────────────────────

    fn lease_path(&self) -> String {
        self.rel(&strip_v1(layout::compaction_lease("")))
    }

    fn status_path(&self) -> String {
        self.rel(&strip_v1(layout::compaction_status("")))
    }

    /// Acquire the compaction lease for `holder` at `now` for `ttl`: create
    /// it (`If-None-Match: *`), or take over an expired one with a
    /// conditional PUT on its ETag. `Conflict` while another holder's lease
    /// is live (or when another taker won the takeover race).
    pub async fn acquire_compaction_lease(&self, holder: &str, now: DateTime<Utc>, ttl: Duration) -> Result<CompactionLease, StoreError> {
        let lease = CompactionLease {
            v: self.opts.write_version,
            holder: holder.to_string(),
            acquired_at: now,
            expires_at: now + chrono::Duration::from_std(ttl).unwrap_or_else(|_| chrono::Duration::hours(1)),
        };
        let data = serde_json::to_vec(&lease).map_err(|e| StoreError::Other(e.to_string()))?;
        let path = self.lease_path();
        match self.put(&path, data.clone(), Mode::Create).await {
            Ok(_) => return Ok(lease),
            Err(StoreError::Conflict(_)) => {}
            Err(e) => return Err(e),
        }
        let Some((cur, ver)) = self.get_json::<CompactionLease>(&path).await? else {
            // Released between our create and our read: try once more.
            return self.put(&path, data, Mode::Create).await.map(|_| lease);
        };
        if cur.holder == holder || cur.expires_at <= now {
            self.put(&path, data, Mode::Update(ver)).await?;
            return Ok(lease);
        }
        Err(StoreError::Conflict(format!("compaction lease held by {} until {}", cur.holder, cur.expires_at)))
    }

    /// Release the lease if `holder` still holds it.
    pub async fn release_compaction_lease(&self, holder: &str) -> Result<(), StoreError> {
        let path = self.lease_path();
        if let Some((cur, _)) = self.get_json::<CompactionLease>(&path).await?
            && cur.holder == holder
        {
            self.delete(&path).await?;
        }
        Ok(())
    }

    /// Check that this process still holds the compaction lease and extend
    /// it (conditional PUT on its ETag). Called before every destructive
    /// phase of compaction and periodically during it, so a run that outlives
    /// its lease, or whose lease was taken over, stops instead of racing the
    /// new holder's snapshot rewrites and deletions. A no-op for dry runs.
    async fn keep_lease(&self, opts: &CompactOptions, guard: &mut LeaseGuard, force: bool) -> Result<(), StoreError> {
        if opts.dry_run {
            return Ok(());
        }
        let every = self.opts.lease_ttl / 4;
        if !force && guard.last.elapsed() < every {
            return Ok(());
        }
        let now = opts.now + chrono::Duration::from_std(guard.started.elapsed()).unwrap_or_default();
        let path = self.lease_path();
        let holder = &self.opts.holder;
        let Some((cur, ver)) = self.get_json::<CompactionLease>(&path).await? else {
            return Err(StoreError::Conflict("compaction lease lost (released by another holder)".into()));
        };
        if &cur.holder != holder {
            return Err(StoreError::Conflict(format!("compaction lease lost to {}", cur.holder)));
        }
        if cur.expires_at <= now {
            return Err(StoreError::Conflict(format!("compaction lease expired at {}", cur.expires_at)));
        }
        let lease = CompactionLease {
            expires_at: now + chrono::Duration::from_std(self.opts.lease_ttl).unwrap_or_else(|_| chrono::Duration::hours(1)),
            ..cur
        };
        let data = serde_json::to_vec(&lease).map_err(|e| StoreError::Other(e.to_string()))?;
        self.put(&path, data, Mode::Update(ver)).await.map_err(|e| match e {
            StoreError::Conflict(m) => StoreError::Conflict(format!("compaction lease lost: {m}")),
            e => e,
        })?;
        guard.last = std::time::Instant::now();
        Ok(())
    }

    async fn compact_inner(&self, opts: &CompactOptions, status: &mut CompactionStatus) -> Result<(), StoreError> {
        let mut guard = LeaseGuard { started: std::time::Instant::now(), last: std::time::Instant::now() };
        let now = opts.now;
        let settle_cut = now - chrono::Duration::from_std(self.opts.settle).unwrap_or_default();
        let grace_cut = now - chrono::Duration::from_std(opts.grace).unwrap_or_default();
        let age_cut = now - chrono::Duration::from_std(opts.max_age).unwrap_or_default();
        let decision_cut = now - chrono::Duration::from_std(opts.decision_max_age).unwrap_or_default();
        let prev_status: Option<CompactionStatus> = self.get_json::<CompactionStatus>(&self.status_path()).await?.map(|x| x.0);

        // Snapshot rows to merge, per (tenant dir, day).
        let mut snap: BTreeMap<(String, NaiveDate), Vec<SnapshotRow>> = BTreeMap::new();

        // ── runs: group shards by key directory.
        let runs_root = self.rel("runs/");
        let mut by_dir: BTreeMap<String, Vec<(Ulid, String)>> = BTreeMap::new();
        for m in self.list(&runs_root, None).await? {
            let loc = m.location.to_string();
            if let Some(id) = shard_ulid(&loc) {
                by_dir.entry(loc[..loc.len() - 31].to_string()).or_default().push((id, loc));
            }
        }
        // Pointers whose shards are all gone still get retention applied.
        let latest_root = self.rel("latest/");
        let mut pointer_paths: Vec<String> =
            self.list(&latest_root, None).await?.into_iter().map(|m| m.location.to_string()).filter(|p| p.ends_with(".json")).collect();
        pointer_paths.sort();
        let mut delete_runs: Vec<String> = Vec::new();
        let mut remaining = 0u64;

        // Visit each shard directory; then pointers without shards.
        let mut visited_pointers = std::collections::HashSet::new();
        for (dir, mut shards) in by_dir {
            self.keep_lease(opts, &mut guard, false).await?;
            shards.sort();
            status.keys += 1;
            let (got, unsupported): (Vec<(String, RunRecord)>, _) =
                self.get_many_detailed(shards.iter().map(|(_, p)| p.clone()).collect(), self.opts.concurrency).await?;
            // The directory's key is the one whose layout path *is* this
            // directory: a record naming another key (planted with create-only
            // credentials under a chosen ULID) must not decide which pointer
            // this directory folds into, or the real records would be dropped
            // and their shards deleted.
            let Some(key) = got.iter().map(|(_, r)| &r.key).find(|k| self.rel(&run_dir_rel(k)) == dir).cloned() else {
                // Every shard unreadable (or foreign): leave them for an operator.
                remaining += shards.len() as u64;
                continue;
            };
            // Shards of a wire version this build cannot read: the watermark
            // stays below the oldest of them and they are never deleted here.
            let first_unsupported: Option<Ulid> = shards.iter().filter(|(_, p)| unsupported.contains(p)).map(|(id, _)| *id).min();
            let ptr_path = self.rel(&latest_rel(&key));
            visited_pointers.insert(ptr_path.clone());
            let shard_records: BTreeMap<String, RunRecord> = got.into_iter().filter(|(_, r)| r.key == key).collect();
            let mut attempt = 0;
            let watermark = loop {
                attempt += 1;
                let ptr = self.get_json::<LatestPointer>(&ptr_path).await?;
                let (old_records, old_wm, ver) = match &ptr {
                    Some((p, v)) => (self.pointer_records(p, &ptr_path), p.watermark, Some(v.clone())),
                    None => (Vec::new(), None, None),
                };
                let mut all = old_records.clone();
                all.extend(shards.iter().filter_map(|(_, p)| shard_records.get(p).cloned()));
                let merged = merge_runs(all, usize::MAX);
                let kept: Vec<RunRecord> = merged.iter().filter(|r| r.started_at >= age_cut).take(opts.keep_last).cloned().collect();
                let new_wm = shards
                    .iter()
                    .filter(|(id, _)| ulid_time(*id) <= settle_cut && first_unsupported.is_none_or(|u| *id < u))
                    .map(|(id, _)| *id)
                    .max()
                    .max(old_wm);
                let changed = new_wm != old_wm || kept != old_records;
                if !changed || opts.dry_run {
                    if changed {
                        status.pointers += 1;
                    }
                    break new_wm;
                }
                let p = LatestPointer {
                    v: self.opts.write_version,
                    key: key.clone(),
                    watermark: new_wm,
                    records: kept.iter().filter_map(|r| serde_json::to_value(r).ok()).collect(),
                    updated_at: now,
                };
                let data = serde_json::to_vec(&p).map_err(|e| StoreError::Other(e.to_string()))?;
                let mode = match ver {
                    Some(v) => Mode::Update(v),
                    None => Mode::Create,
                };
                match self.put(&ptr_path, data, mode).await {
                    Ok(_) => {
                        status.pointers += 1;
                        break new_wm;
                    }
                    Err(StoreError::Conflict(_)) if attempt < 5 => continue,
                    Err(e) => return Err(e),
                }
            };
            // Shards older than both the watermark and the grace period go,
            // after their records land in the day's snapshot.
            for (id, path) in &shards {
                let old = ulid_time(*id) < grace_cut && watermark.is_some_and(|w| *id <= w) && !unsupported.contains(path);
                if old {
                    if let Some(r) = shard_records.get(path) {
                        let day = r.started_at.date_naive();
                        snap.entry((layout::tenant_dir(&key), day)).or_default().push(SnapshotRow::Run(Box::new(r.clone())));
                    }
                    delete_runs.push(path.clone());
                } else {
                    remaining += 1;
                }
            }
        }
        for ptr_path in pointer_paths.into_iter().filter(|p| !visited_pointers.contains(p)) {
            for attempt in 1..=5 {
                let Some((p, ver)) = self.get_json::<LatestPointer>(&ptr_path).await? else { break };
                let old = self.pointer_records(&p, &ptr_path);
                let kept: Vec<RunRecord> = old.iter().filter(|r| r.started_at >= age_cut).take(opts.keep_last).cloned().collect();
                if kept == old {
                    break;
                }
                status.pointers += 1;
                if opts.dry_run {
                    break;
                }
                let r = if kept.is_empty() {
                    self.delete(&ptr_path).await.map(|_| UpdateVersion { e_tag: None, version: None })
                } else {
                    let np = LatestPointer { records: kept.iter().filter_map(|r| serde_json::to_value(r).ok()).collect(), updated_at: now, ..p };
                    let data = serde_json::to_vec(&np).map_err(|e| StoreError::Other(e.to_string()))?;
                    self.put(&ptr_path, data, Mode::Update(ver)).await
                };
                match r {
                    Ok(_) => break,
                    Err(StoreError::Conflict(_)) if attempt < 5 => continue,
                    Err(e) => return Err(e),
                }
            }
        }

        // ── decisions: snapshot those past the grace period (once, by
        // watermark) and those about to be deleted; delete past retention.
        let dec_root = self.rel("decisions/");
        let mut decision_paths: Vec<(Ulid, String)> = self
            .list(&dec_root, None)
            .await?
            .into_iter()
            .filter_map(|m| {
                let loc = m.location.to_string();
                shard_ulid(&loc).map(|id| (id, loc))
            })
            .collect();
        decision_paths.sort();
        let prev_dwm = prev_status.as_ref().and_then(|s| s.decision_watermark);
        let to_snapshot: Vec<String> = decision_paths
            .iter()
            .filter(|(id, _)| {
                let t = ulid_time(*id);
                (t < grace_cut && prev_dwm.is_none_or(|w| *id > w)) || t < decision_cut
            })
            .map(|(_, p)| p.clone())
            .collect();
        let (decoded, unsupported_decisions) = self.get_many_detailed::<Decision>(to_snapshot, self.opts.concurrency).await?;
        // Never delete what this build cannot read (and so cannot snapshot).
        let delete_decisions: Vec<String> = decision_paths
            .iter()
            .filter(|(id, p)| ulid_time(*id) < decision_cut && !unsupported_decisions.contains(p))
            .map(|(_, p)| p.clone())
            .collect();
        let mut dwm = prev_dwm;
        for (_, d) in decoded {
            if ulid_time(d.id) < grace_cut {
                dwm = dwm.max(Some(d.id));
            }
            snap.entry((layout::tenant_dir(&d.key), ulid_time(d.id).date_naive())).or_default().push(SnapshotRow::Decision(Box::new(d)));
        }
        status.decision_watermark = dwm;

        // ── snapshots: merge into each day's file, then delete what they cover.
        let age_day = age_cut.date_naive();
        self.keep_lease(opts, &mut guard, true).await?;
        for ((tenant, day), rows) in snap {
            if day < age_day {
                continue; // past retention: not worth a snapshot
            }
            self.keep_lease(opts, &mut guard, false).await?;
            let path = self.rel(&format!("snapshots/{tenant}/{}.parquet", day.format("%Y-%m-%d")));
            status.snapshots += 1;
            if opts.dry_run {
                continue;
            }
            let mut all = match self.get(&path).await? {
                Some((b, _)) => read_snapshot(&b).map_err(|e| StoreError::Corrupt { path: path.clone(), message: e })?,
                None => Vec::new(),
            };
            all.extend(rows);
            let bytes = write_snapshot(&all).map_err(StoreError::Other)?;
            self.put(&path, bytes, Mode::Overwrite).await?;
        }
        status.run_shards_deleted = delete_runs.len() as u64;
        status.decisions_deleted = delete_decisions.len() as u64;
        if !opts.dry_run {
            self.keep_lease(opts, &mut guard, true).await?;
            self.delete_all(delete_runs).await?;
            self.delete_all(delete_decisions).await?;
        }
        status.shards_remaining = remaining;

        // ── snapshots past max_age.
        let snaps = self.list(&self.rel("snapshots/"), None).await?;
        let old_snaps: Vec<String> = snaps
            .into_iter()
            .map(|m| m.location.to_string())
            .filter(|p| p.rsplit('/').next().and_then(|f| f.strip_suffix(".parquet")).and_then(|d| NaiveDate::parse_from_str(d, "%Y-%m-%d").ok()).is_some_and(|d| d < age_day))
            .collect();
        status.snapshots_deleted = old_snaps.len() as u64;
        if !opts.dry_run {
            self.keep_lease(opts, &mut guard, true).await?;
            self.delete_all(old_snaps).await?;
        }

        // ── attempt records past their TTL (by last write).
        let attempt_cut = now - chrono::Duration::from_std(self.opts.attempt_ttl).unwrap_or_default();
        let old_attempts: Vec<String> = self
            .list(&self.rel("attempts/"), None)
            .await?
            .into_iter()
            .filter(|m| m.last_modified < attempt_cut && m.location.as_ref().ends_with(".json"))
            .map(|m| m.location.to_string())
            .collect();
        status.attempts_deleted = old_attempts.len() as u64;
        if !opts.dry_run {
            self.keep_lease(opts, &mut guard, true).await?;
            self.delete_all(old_attempts).await?;
        }
        Ok(())
    }

    async fn delete_all(&self, paths: Vec<String>) -> Result<(), StoreError> {
        let results: Vec<Result<(), StoreError>> =
            futures::stream::iter(paths).map(|p| async move { self.delete(&p).await }).buffer_unordered(self.opts.concurrency.max(1)).collect().await;
        results.into_iter().collect()
    }

    // ── decisions ───────────────────────────────────────────────────────

    async fn decision_paths(&self, filter: &DecisionFilter) -> Result<Vec<(Ulid, String)>, StoreError> {
        let tenant = match (&filter.key, &filter.tenant) {
            (Some(k), _) => Some(layout::tenant_dir(k)),
            (None, Some((p, t))) => Some(format!("{p}/{t}")),
            _ => None,
        };
        let root = match &tenant {
            Some(t) => self.rel(&format!("decisions/{t}/")),
            None => self.rel("decisions/"),
        };
        let mut metas = Vec::new();
        // With a tenant and a bounded window, LIST only the date partitions.
        match (&tenant, filter.since, filter.until) {
            (Some(_), Some(since), Some(until)) if until > since && (until - since).num_days() <= 400 => {
                let mut d = since.date_naive();
                let last = until.date_naive();
                while d <= last {
                    metas.extend(self.list(&format!("{root}{}/", d.format("%Y-%m-%d")), None).await?);
                    match d.succ_opt() {
                        Some(n) => d = n,
                        None => break,
                    }
                }
            }
            _ => metas = self.list(&root, None).await?,
        }
        let since_day = filter.since.map(|s| s.date_naive());
        let until_day = filter.until.map(|u| u.date_naive());
        let mut v: Vec<(Ulid, String)> = metas
            .into_iter()
            .filter_map(|m| {
                let loc = m.location.to_string();
                let id = shard_ulid(&loc)?;
                let day = loc.rsplit('/').nth(1).and_then(|d| NaiveDate::parse_from_str(d, "%Y-%m-%d").ok())?;
                let ok = since_day.is_none_or(|s| day >= s) && until_day.is_none_or(|u| day <= u);
                ok.then_some((id, loc))
            })
            .filter(|(id, _)| {
                let at = ulid_time(*id);
                filter.since.is_none_or(|s| at >= s) && filter.until.is_none_or(|u| at < u)
            })
            .collect();
        v.sort_by_key(|x| std::cmp::Reverse(x.0));
        Ok(v)
    }

    // ── breakers ────────────────────────────────────────────────────────

    async fn read_reset(&self, path: &str) -> Result<Vec<Reset>, StoreError> {
        let Some((b, _)) = self.get(path).await? else { return Ok(Vec::new()) };
        let value: serde_json::Value =
            serde_json::from_slice(&b).map_err(|e| StoreError::Corrupt { path: path.to_string(), message: e.to_string() })?;
        // One reset object, or (hand-written) an array of them.
        let items = match value {
            serde_json::Value::Array(a) => a,
            v => vec![v],
        };
        let mut out = Vec::new();
        for v in items {
            match decode_value::<Reset>(v, path)? {
                Some(r) => out.push(r),
                None => self.skip(),
            }
        }
        Ok(out)
    }
}

#[async_trait]
impl HistoryStore for ObjectHistoryStore {
    fn info(&self) -> StoreInfo {
        StoreInfo { backend: Backend::ObjectStore, uri: self.uri.clone(), durable_shared: self.opts.durable && self.opts.shared }
    }

    fn durable(&self) -> bool {
        self.opts.durable
    }

    fn shared(&self) -> bool {
        self.opts.shared
    }

    fn role(&self) -> Role {
        self.opts.role
    }

    async fn put_run(&self, rec: &RunRecord) -> Result<PutOutcome, StoreError> {
        if self.opts.check_duplicates_on_write {
            let dk = dedup_key(rec);
            // The newest records only (the pointer's plus the newest shards, see
            // `newest_runs`): a duplicate is a retried write, close in time to the
            // first. Older duplicates are still removed on read and in compaction.
            if self.newest_runs(&rec.key, DUPLICATE_CHECK_DEPTH).await?.iter().any(|r| dedup_key(r) == dk) {
                return Ok(PutOutcome::Duplicate);
            }
        }
        let mut r = rec.clone();
        r.v = self.opts.write_version;
        let data = serde_json::to_vec(&r).map_err(|e| StoreError::Other(e.to_string()))?;
        let id = self.next_ulid();
        let path = self.rel(&run_shard_rel(&rec.key, id));
        self.put(&path, data, Mode::Create).await?;
        Ok(PutOutcome::Inserted)
    }

    async fn put_decision(&self, d: &Decision) -> Result<(), StoreError> {
        let mut d2 = d.clone();
        d2.v = self.opts.write_version;
        let data = serde_json::to_vec(&d2).map_err(|e| StoreError::Other(e.to_string()))?;
        let path = self.rel(&decision_rel(&d.key, d.id));
        match self.put(&path, data, Mode::Create).await {
            Ok(_) | Err(StoreError::Conflict(_)) => Ok(()), // same id = same decision (retry)
            Err(e) => Err(e),
        }
    }

    async fn runs(&self, key: &WorkloadKey, limit: usize) -> Result<Vec<RunRecord>, StoreError> {
        self.newest_runs(key, limit).await
    }

    async fn runs_marked(&self, key: &WorkloadKey, limit: usize) -> Result<(Vec<RunRecord>, Option<String>), StoreError> {
        self.newest_runs_marked(key, limit).await
    }

    async fn run_marker(&self, key: &WorkloadKey) -> Result<Option<String>, StoreError> {
        Ok(self.newest_runs_marked(key, 0).await?.1)
    }

    /// One LIST `start-after` the marker shard (shard ULIDs follow write
    /// time): any direct child shard newer than it is a change. A writer
    /// whose clock runs behind the marker's can be missed until the cache
    /// TTL, as with the pointer watermark (SPEC §8.1).
    async fn runs_changed_since(&self, key: &WorkloadKey, marker: &str) -> Result<bool, StoreError> {
        let dir = self.rel(&run_dir_rel(key));
        let after = if marker.is_empty() { None } else { Some(Ulid::from_string(marker).map_err(|e| StoreError::Other(format!("run marker {marker:?}: {e}")))?) };
        let offset = after.map(|w| format!("{dir}{w}.json"));
        let p = Path::from(dir.trim_end_matches('/'));
        let mut stream = match &offset {
            Some(o) => self.store.list_with_offset(Some(&p), &Path::from(o.as_str())),
            None => self.store.list(Some(&p)),
        };
        while let Some(m) = stream.next().await {
            let m = match m {
                Ok(m) => m,
                Err(object_store::Error::NotFound { .. }) => return Ok(false),
                Err(e) => return Err(map_err(&dir, e)),
            };
            let loc = m.location.to_string();
            if loc.len() == dir.len() + 31 && shard_ulid(&loc).is_some_and(|id| after.is_none_or(|w| id > w)) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn decisions(&self, filter: &DecisionFilter) -> Result<Vec<Decision>, StoreError> {
        let paths = self.decision_paths(filter).await?;
        let limit = filter.limit.unwrap_or(usize::MAX);
        let chunk = self.opts.concurrency.max(1) * 4;
        let mut out = Vec::new();
        for c in paths.chunks(chunk) {
            for (_, d) in self.get_many::<Decision>(c.iter().map(|(_, p)| p.clone()).collect()).await? {
                if filter.matches(&d) {
                    out.push(d);
                    if out.len() >= limit {
                        return Ok(out);
                    }
                }
            }
        }
        Ok(out)
    }

    async fn decision(&self, id: Ulid) -> Result<Option<Decision>, StoreError> {
        let day = ulid_time(id).format("%Y-%m-%d").to_string();
        let name = format!("/{day}/{id}.json");
        // Stream the LIST and stop at the first match: collecting the whole
        // decision tree (30 d of decisions, millions of objects) into memory
        // to find one id is unbounded.
        let root = Path::from(self.rel("decisions"));
        let mut stream = self.store.list(Some(&root));
        while let Some(m) = stream.next().await {
            let m = match m {
                Ok(m) => m,
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(e) => return Err(map_err(root.as_ref(), e)),
            };
            let loc = m.location.to_string();
            if loc.ends_with(&name) {
                return Ok(self.get_json::<Decision>(&loc).await?.map(|x| x.0).filter(|d| d.id == id));
            }
        }
        Ok(None)
    }

    async fn breaker_get(&self, scope: &BreakerScope) -> Result<BreakerObjects, StoreError> {
        // The current path and the N-1 one (workload scopes without the
        // platform segment); trips are deduplicated by id.
        // Every object and directory is read concurrently: a scope is ~5 store
        // calls (current + N-1 layouts), and each decision reads several scopes.
        let mut trips: Vec<Trip> = Vec::new();
        let mut resets: Vec<Reset> = Vec::new();
        let reset_paths: Vec<String> = scope.reset_objects().iter().map(|o| self.rel(o)).collect();
        for r in futures::future::try_join_all(reset_paths.iter().map(|p| self.read_reset(p))).await? {
            resets.extend(r);
        }
        // Trips: `breakers-trips/{scope}/` (current), then the N-1
        // `breakers/{scope}/trips/` directories.
        let trip_dirs: Vec<String> = scope.trip_dirs().iter().map(|d| self.rel(d)).collect();
        let listed = futures::future::try_join_all(trip_dirs.iter().map(|d| self.list(d, None))).await?;
        let mut paths: Vec<String> = Vec::new();
        for (trip_dir, metas) in trip_dirs.iter().zip(listed) {
            paths.extend(
                metas
                    .into_iter()
                    .map(|m| m.location.to_string())
                    .filter(|p| p.ends_with(".json") && !p[trip_dir.len()..].contains('/')),
            );
        }
        for (_, t) in self.get_many::<Trip>(paths).await? {
            if &t.scope == scope && !trips.iter().any(|x| x.id == t.id) {
                trips.push(t);
            }
        }
        trips.sort_by_key(|t| t.id);
        resets.retain(|r| &r.scope == scope);
        resets.sort_by_key(|r| r.at);
        resets.dedup();
        Ok(BreakerObjects { trips, resets })
    }

    async fn breaker_list(&self) -> Result<Vec<(BreakerScope, BreakerObjects)>, StoreError> {
        let root = self.rel(&format!("{}/", rightsizer_core::breaker::RESETS_ROOT));
        let metas = self.list(&root, None).await?;
        // N-1 trips sit under `breakers/{scope}/trips/`; current ones under `breakers-trips/`.
        // A path is an N-1 trip only if it is `…/trips/{ulid}.json`; one that
        // looks so but does not decode as a trip (a tenant segment named
        // `trips`) is read as a reset.
        let is_trip = |p: &str| shard_ulid(p).is_some() && p.rsplit('/').nth(1) == Some("trips");
        let (legacy_trips, mut reset_paths): (Vec<String>, Vec<String>) =
            metas.into_iter().map(|m| m.location.to_string()).filter(|p| p.ends_with(".json")).partition(|p| is_trip(p));
        let troot = self.rel(&format!("{}/", rightsizer_core::breaker::TRIPS_ROOT));
        let mut trip_paths: Vec<String> = self.list(&troot, None).await?.into_iter().map(|m| m.location.to_string()).filter(|p| p.ends_with(".json")).collect();
        trip_paths.extend(legacy_trips.iter().cloned());
        let got = self.get_many::<Trip>(trip_paths).await?;
        let decoded: std::collections::HashSet<&str> = got.iter().map(|(p, _)| p.as_str()).collect();
        reset_paths.extend(legacy_trips.iter().filter(|p| !decoded.contains(p.as_str())).cloned());
        let mut by: BTreeMap<BreakerScope, BreakerObjects> = BTreeMap::new();
        for (_, t) in got {
            let o = by.entry(t.scope.clone()).or_default();
            if !o.trips.iter().any(|x| x.id == t.id) {
                o.trips.push(t);
            }
        }
        for o in by.values_mut() {
            o.trips.sort_by_key(|t| t.id);
        }
        for p in reset_paths {
            for r in self.read_reset(&p).await? {
                if let Some(o) = by.get_mut(&r.scope) {
                    o.resets.push(r);
                }
            }
        }
        Ok(by.into_iter().collect())
    }

    async fn breaker_trip(&self, trip: &Trip) -> Result<(), StoreError> {
        let mut t = trip.clone();
        t.v = self.opts.write_version;
        let data = serde_json::to_vec(&t).map_err(|e| StoreError::Other(e.to_string()))?;
        let path = self.rel(&trip.scope.trip_object(trip.id));
        self.put(&path, data, Mode::Create).await.map(|_| ())
    }

    async fn breaker_reset(&self, reset: &Reset) -> Result<(), StoreError> {
        self.opts.role.require_operator("a breaker reset")?;
        let path = self.rel(&reset.scope.reset_object());
        // Keep the newest reset if an operator replays an older one.
        let existing = self.read_reset(&path).await?;
        if existing.iter().any(|r| r.at > reset.at) {
            return Ok(());
        }
        let mut r = reset.clone();
        r.v = self.opts.write_version;
        let data = serde_json::to_vec(&r).map_err(|e| StoreError::Other(e.to_string()))?;
        self.put(&path, data, Mode::Overwrite).await.map(|_| ())
    }

    async fn control_get(&self) -> Result<Option<Control>, StoreError> {
        Ok(self.get_json::<Control>(&self.rel("control/mode.json")).await?.map(|x| x.0))
    }

    async fn control_set(&self, control: &Control) -> Result<(), StoreError> {
        self.opts.role.require_operator("the control object")?;
        let mut c = control.clone();
        c.v = self.opts.write_version;
        let data = serde_json::to_vec(&c).map_err(|e| StoreError::Other(e.to_string()))?;
        self.put(&self.rel("control/mode.json"), data, Mode::Overwrite).await.map(|_| ())
    }

    async fn attempt_get(&self, key: &WorkloadKey, invocation_id: &str) -> Result<Option<AttemptRecord>, StoreError> {
        let path = self.rel(&attempt_rel(key, invocation_id));
        let rec = self.get_json::<AttemptRecord>(&path).await?.map(|x| x.0);
        Ok(rec.filter(|r| r.invocation_id == invocation_id && !attempt_expired(r.written_at, Utc::now(), self.opts.attempt_ttl)))
    }

    async fn attempt_put_if_absent(&self, rec: &AttemptRecord) -> Result<bool, StoreError> {
        let path = self.rel(&attempt_rel(&rec.key, &rec.invocation_id));
        let mut r = rec.clone();
        r.v = self.opts.write_version;
        let data = serde_json::to_vec(&r).map_err(|e| StoreError::Other(e.to_string()))?;
        match self.put(&path, data.clone(), Mode::Create).await {
            Ok(_) => return Ok(true),
            Err(StoreError::Conflict(_)) => {}
            Err(e) => return Err(e),
        }
        // An expired record (a rerun after 7 d) is replaced conditionally.
        if let Some((old, ver)) = self.get_json::<AttemptRecord>(&path).await?
            && attempt_expired(old.written_at, Utc::now(), self.opts.attempt_ttl)
        {
            return match self.put(&path, data, Mode::Update(ver)).await {
                Ok(_) => Ok(true),
                Err(StoreError::Conflict(_)) => Ok(false),
                Err(e) => Err(e),
            };
        }
        Ok(false)
    }

    async fn attempt_close_lease(&self, key: &WorkloadKey, invocation_id: &str, at: DateTime<Utc>, outcome: RunStatus) -> Result<(), StoreError> {
        let path = self.rel(&attempt_rel(key, invocation_id));
        for _ in 0..8 {
            let Some((mut rec, ver)) = self.get_json::<AttemptRecord>(&path).await? else {
                return Err(StoreError::Other(format!("no attempt record for {invocation_id}")));
            };
            let lease = rec.lease.get_or_insert(Lease { opened_at: at, closed_at: None, outcome: None });
            lease.closed_at = Some(at);
            lease.outcome = Some(outcome);
            let data = serde_json::to_vec(&rec).map_err(|e| StoreError::Other(e.to_string()))?;
            match self.put(&path, data, Mode::Update(ver)).await {
                Ok(_) => return Ok(()),
                Err(StoreError::Conflict(_)) => continue,
                Err(e) => return Err(e),
            }
        }
        Err(StoreError::Conflict(format!("attempt record {path} kept changing")))
    }

    async fn compact(&self, opts: &CompactOptions) -> Result<CompactReport, StoreError> {
        let holder = self.opts.holder.clone();
        if !opts.dry_run {
            self.acquire_compaction_lease(&holder, opts.now, self.opts.lease_ttl).await?;
        }
        let mut status = CompactionStatus { v: self.opts.write_version, holder: holder.clone(), started_at: Some(opts.now), ..Default::default() };
        let r = self.compact_inner(opts, &mut status).await;
        if !opts.dry_run {
            if r.is_ok() {
                status.finished_at = Some(Utc::now().max(opts.now));
                if let Ok(data) = serde_json::to_vec(&status) {
                    let _ = self.put(&self.status_path(), data, Mode::Overwrite).await;
                }
            }
            let _ = self.release_compaction_lease(&holder).await;
        }
        r?;
        Ok(CompactReport { pointers: status.pointers as usize, snapshots: status.snapshots as usize, deleted: status.run_shards_deleted as usize })
    }

    async fn keys(&self) -> Result<Vec<WorkloadKey>, StoreError> {
        // Keys from pointers, plus keys with shards but no pointer yet.
        let mut keys: BTreeMap<String, WorkloadKey> = BTreeMap::new();
        let ptrs: Vec<String> =
            self.list(&self.rel("latest/"), None).await?.into_iter().map(|m| m.location.to_string()).filter(|p| p.ends_with(".json")).collect();
        let mut have_ptr = std::collections::HashSet::new();
        for (path, p) in self.get_many::<LatestPointer>(ptrs).await? {
            have_ptr.insert(path.rsplit('/').next().unwrap_or_default().trim_end_matches(".json").to_string());
            keys.insert(p.key.to_string(), p.key);
        }
        let mut first_shard: HashMap<String, String> = HashMap::new();
        for m in self.list(&self.rel("runs/"), None).await? {
            let loc = m.location.to_string();
            if shard_ulid(&loc).is_some() {
                let dir = loc[..loc.len() - 31].to_string();
                let hash = dir.trim_end_matches('/').rsplit('/').next().unwrap_or_default().to_string();
                if !have_ptr.contains(&hash) {
                    first_shard.entry(dir).or_insert(loc);
                }
            }
        }
        for (_, r) in self.get_many::<RunRecord>(first_shard.into_values().collect()).await? {
            keys.insert(r.key.to_string(), r.key);
        }
        Ok(keys.into_values().collect())
    }

    async fn stats(&self) -> Result<StoreStats, StoreError> {
        let status = self.get_json::<CompactionStatus>(&self.status_path()).await?.map(|x| x.0);
        Ok(StoreStats {
            shards: status.as_ref().map(|s| s.shards_remaining),
            compaction_lag_seconds: status.as_ref().and_then(|s| s.finished_at).map(|t| (Utc::now() - t).num_milliseconds().max(0) as f64 / 1000.0),
            last_compaction: status,
            skipped_on_read: self.skipped.load(Ordering::Relaxed),
        })
    }
}

// ── helpers ─────────────────────────────────────────────────────────────

fn layout_join(prefix: &str, rest: &str) -> String {
    if prefix.is_empty() { format!("v1/{rest}") } else { format!("{prefix}/v1/{rest}") }
}

// Layout paths relative to `{prefix}/v1/` (the `layout` builders with an
// empty prefix, minus their `v1/`).
fn strip_v1(s: String) -> String {
    s.strip_prefix("v1/").map(str::to_string).unwrap_or(s)
}
fn run_shard_rel(key: &WorkloadKey, id: Ulid) -> String {
    strip_v1(layout::run_shard("", key, id))
}
fn run_dir_rel(key: &WorkloadKey) -> String {
    strip_v1(layout::run_dir("", key))
}
fn latest_rel(key: &WorkloadKey) -> String {
    strip_v1(layout::latest("", key))
}
fn decision_rel(key: &WorkloadKey, id: Ulid) -> String {
    strip_v1(layout::decision_shard("", key, ulid_time(id), id))
}
fn attempt_rel(key: &WorkloadKey, invocation_id: &str) -> String {
    strip_v1(layout::attempt("", key, invocation_id))
}

/// The ULID of a `…/{ulid}.json` object, if the file name is one.
fn shard_ulid(path: &str) -> Option<Ulid> {
    let name = path.rsplit('/').next()?;
    let stem = name.strip_suffix(".json")?;
    if stem.len() != 26 {
        return None;
    }
    Ulid::from_string(stem).ok()
}

fn map_err(path: &str, e: object_store::Error) -> StoreError {
    match e {
        object_store::Error::PermissionDenied { .. } | object_store::Error::Unauthenticated { .. } => {
            StoreError::PermissionDenied(format!("{path}: {e}"))
        }
        object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. } => StoreError::Conflict(format!("{path}: {e}")),
        object_store::Error::InvalidPath { .. } => StoreError::Config(format!("{path}: {e}")),
        e => StoreError::Unreachable(format!("{path}: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_names() {
        let id = Ulid::from_parts(1_700_000_000_000, 7);
        assert_eq!(shard_ulid(&format!("v1/runs/k8s/c/ns/abc/{id}.json")), Some(id));
        assert_eq!(shard_ulid("v1/runs/k8s/c/ns/abc/.rs-x.tmp"), None);
        assert_eq!(shard_ulid("v1/runs/k8s/c/ns/abc/x.json.lock"), None);
        let k = WorkloadKey::parse("flyte://acme/proj/dev/env.train").unwrap();
        assert_eq!(run_dir_rel(&k), "runs/flyte/acme/proj/dev/eef92d58436c80afb2670dc464a4b4f2/");
        assert_eq!(layout_join("", "x"), "v1/x");
        assert_eq!(layout_join("rs", "x"), "rs/v1/x");
    }
}
