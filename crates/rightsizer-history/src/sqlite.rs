//! The sqlite backend (feature `sqlite`, SPEC §8.1): single-user CLI on one
//! machine. WAL mode, a `schema_version` table with ordered migrations,
//! and refused on network filesystems (NFS/SMB/FUSE, detected with
//! `statfs`, [`crate::fsdetect`]). Durable but not shared, so the engine
//! demotes enforce (SPEC §8.1).
//!
//! Same semantics as the object store: dedup via a unique index on
//! `(key, invocation_id, attempt, source)`, create-only trips and attempt
//! records (`INSERT OR IGNORE`), operator-only resets and control (client
//! role refuses them), attempt expiry after 7 d, and a compaction lease row.
//! Compaction applies retention (`max_age` for runs, `decision_max_age` for
//! decisions, the attempt TTL), never deleting rows younger than `grace`;
//! there are no pointers or snapshot files (`rsz history export` writes
//! Parquet from the tables).

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rightsizer_core::breaker::{BreakerObjects, BreakerScope, Control, Reset, Trip};
use rightsizer_core::{AttemptRecord, Decision, Lease, RunRecord, RunStatus, WorkloadKey};
use rusqlite::{Connection, OptionalExtension, params, params_from_iter};
use ulid::Ulid;

use crate::fsdetect::{FsKind, detect};
use crate::sql::{attempt_body, decision_cols, decode, reset_body, run_cols, scope_path, to_json, trip_body};
use crate::{
    ATTEMPT_TTL, Backend, CompactOptions, CompactReport, CompactionStatus, DecisionFilter, HistoryStore, PutOutcome, Role,
    StoreError, StoreInfo, StoreStats, attempt_expired,
};

/// Ordered schema migrations; `schema_version` holds how many have run.
pub const MIGRATIONS: &[&str] = &[
    // 1: initial schema.
    r#"
    CREATE TABLE runs (
        rowid_       INTEGER PRIMARY KEY AUTOINCREMENT,
        key          TEXT NOT NULL,
        tenant       TEXT NOT NULL,
        invocation_id TEXT NOT NULL,
        attempt      INTEGER NOT NULL,
        source       TEXT NOT NULL,
        cluster      TEXT,
        started_at_ms INTEGER NOT NULL,
        written_at_ms INTEGER NOT NULL,
        v            INTEGER NOT NULL,
        body         TEXT NOT NULL,
        UNIQUE (key, invocation_id, attempt, source)
    );
    CREATE INDEX runs_key_started ON runs (key, started_at_ms DESC, attempt DESC);
    CREATE TABLE decisions (
        id           TEXT PRIMARY KEY,
        key          TEXT NOT NULL,
        tenant       TEXT NOT NULL,
        at_ms        INTEGER NOT NULL,
        mode         TEXT NOT NULL,
        estimator_id TEXT,
        estimator_version TEXT,
        body         TEXT NOT NULL
    );
    CREATE INDEX decisions_at ON decisions (at_ms DESC);
    CREATE INDEX decisions_key_at ON decisions (key, at_ms DESC);
    CREATE INDEX decisions_tenant_at ON decisions (tenant, at_ms DESC);
    CREATE TABLE breaker_trips (
        id           TEXT PRIMARY KEY,
        scope_path   TEXT NOT NULL,
        at_ms        INTEGER NOT NULL,
        body         TEXT NOT NULL
    );
    CREATE INDEX breaker_trips_scope ON breaker_trips (scope_path);
    CREATE TABLE breaker_resets (
        rowid_       INTEGER PRIMARY KEY AUTOINCREMENT,
        scope_path   TEXT NOT NULL,
        at_ms        INTEGER NOT NULL,
        body         TEXT NOT NULL
    );
    CREATE INDEX breaker_resets_scope ON breaker_resets (scope_path);
    CREATE TABLE control (
        id           INTEGER PRIMARY KEY CHECK (id = 1),
        body         TEXT NOT NULL
    );
    CREATE TABLE attempts (
        key          TEXT NOT NULL,
        invocation_id TEXT NOT NULL,
        written_at_ms INTEGER NOT NULL,
        body         TEXT NOT NULL,
        PRIMARY KEY (key, invocation_id)
    );
    CREATE TABLE leases (
        name         TEXT PRIMARY KEY,
        holder       TEXT NOT NULL,
        expires_at_ms INTEGER NOT NULL
    );
    CREATE TABLE compaction_status (
        id           INTEGER PRIMARY KEY CHECK (id = 1),
        body         TEXT NOT NULL
    );
    "#,
];

/// Options of a [`SqliteStore`].
#[derive(Clone, Debug, PartialEq)]
pub struct SqliteOptions {
    /// Credentials role (the client role refuses resets and control writes).
    pub role: Role,
    /// Wire version written.
    pub write_version: u16,
    /// Open even on a network filesystem (unsafe: sqlite locking is unreliable there).
    pub allow_network_fs: bool,
    /// Attempt record lifetime (7 d).
    pub attempt_ttl: Duration,
    /// Compaction lease lifetime (1 h).
    pub lease_ttl: Duration,
    /// Compaction holder id.
    pub holder: String,
}

impl Default for SqliteOptions {
    fn default() -> Self {
        SqliteOptions {
            role: Role::Client,
            write_version: rightsizer_core::WIRE_VERSION,
            allow_network_fs: false,
            attempt_ttl: ATTEMPT_TTL,
            lease_ttl: Duration::from_secs(3600),
            holder: crate::default_holder(),
        }
    }
}

/// A sqlite-backed store.
pub struct SqliteStore {
    conn: Arc<Mutex<Connection>>,
    uri: String,
    opts: SqliteOptions,
}

fn db_err(e: rusqlite::Error) -> StoreError {
    match &e {
        rusqlite::Error::SqliteFailure(f, _) if matches!(f.code, rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked) => {
            StoreError::Unreachable(e.to_string())
        }
        rusqlite::Error::SqliteFailure(f, _) if f.code == rusqlite::ErrorCode::ConstraintViolation => StoreError::Conflict(e.to_string()),
        rusqlite::Error::SqliteFailure(f, _) if f.code == rusqlite::ErrorCode::ReadOnly => StoreError::PermissionDenied(e.to_string()),
        _ => StoreError::Other(e.to_string()),
    }
}

/// Refuse `kind` (a network filesystem) unless allowed. Split out so the
/// refusal can be tested with injected filesystem kinds.
pub fn check_filesystem(path: &Path, kind: &FsKind, allow_network_fs: bool) -> Result<(), StoreError> {
    match kind {
        FsKind::Network(t) if !allow_network_fs => Err(StoreError::Config(format!(
            "refusing sqlite at {} on a network filesystem ({t}); use an object store or postgres (SPEC §8.1)",
            path.display()
        ))),
        _ => Ok(()),
    }
}

/// Run pending migrations; refuse a database newer than this build.
pub fn migrate(conn: &mut Connection) -> Result<u32, StoreError> {
    conn.execute_batch("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)").map_err(db_err)?;
    let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate).map_err(db_err)?;
    let cur: u32 = tx.query_row("SELECT COALESCE(MAX(version), 0) FROM schema_version", [], |r| r.get(0)).map_err(db_err)?;
    let known = MIGRATIONS.len() as u32;
    if cur > known {
        return Err(StoreError::Config(format!("sqlite schema version {cur} is newer than this build supports ({known}); upgrade rsz")));
    }
    for (i, m) in MIGRATIONS.iter().enumerate().skip(cur as usize) {
        tx.execute_batch(m).map_err(db_err)?;
        tx.execute("INSERT INTO schema_version (version) VALUES (?1)", params![(i + 1) as u32]).map_err(db_err)?;
    }
    tx.commit().map_err(db_err)?;
    Ok(known)
}

impl SqliteStore {
    /// Open (or create) the database at `path` with default options.
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        Self::open_with(path, SqliteOptions::default())
    }

    /// Open (or create) the database at `path`.
    pub fn open_with(path: &Path, opts: SqliteOptions) -> Result<Self, StoreError> {
        check_filesystem(path, &detect(path), opts.allow_network_fs)?;
        if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
            std::fs::create_dir_all(dir).map_err(|e| StoreError::Config(format!("{}: {e}", dir.display())))?;
        }
        let conn = Connection::open(path).map_err(|e| StoreError::Config(e.to_string()))?;
        Self::init(conn, format!("sqlite://{}", path.display()), opts)
    }

    /// An in-memory database (tests), default options with the operator role.
    pub fn in_memory() -> Result<Self, StoreError> {
        Self::in_memory_with(SqliteOptions { role: Role::Operator, ..Default::default() })
    }

    /// An in-memory database with options.
    pub fn in_memory_with(opts: SqliteOptions) -> Result<Self, StoreError> {
        let conn = Connection::open_in_memory().map_err(|e| StoreError::Config(e.to_string()))?;
        Self::init(conn, "sqlite::memory:".into(), opts)
    }

    fn init(mut conn: Connection, uri: String, opts: SqliteOptions) -> Result<Self, StoreError> {
        crate::check_write_version(opts.write_version)?;
        conn.busy_timeout(Duration::from_secs(5)).map_err(db_err)?;
        // WAL: readers never block the writer (in-memory databases report "memory").
        let _mode: String = conn.query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0)).map_err(db_err)?;
        conn.execute_batch("PRAGMA synchronous=NORMAL; PRAGMA foreign_keys=ON;").map_err(db_err)?;
        migrate(&mut conn)?;
        Ok(SqliteStore { conn: Arc::new(Mutex::new(conn)), uri, opts })
    }

    /// The connection.
    pub fn connection(&self) -> &Mutex<Connection> {
        &self.conn
    }

    /// The journal mode in effect (`wal` for file databases).
    pub fn journal_mode(&self) -> Result<String, StoreError> {
        let c = self.conn.lock().map_err(|_| StoreError::Other("poisoned".into()))?;
        c.query_row("PRAGMA journal_mode", [], |r| r.get(0)).map_err(db_err)
    }

    async fn with<R: Send + 'static>(&self, f: impl FnOnce(&mut Connection) -> Result<R, StoreError> + Send + 'static) -> Result<R, StoreError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let mut c = conn.lock().map_err(|_| StoreError::Other("sqlite connection poisoned".into()))?;
            f(&mut c)
        })
        .await
        .map_err(|e| StoreError::Other(format!("sqlite task: {e}")))?
    }

    /// Acquire the compaction lease (`leases` row; an expired lease is taken over).
    pub async fn acquire_compaction_lease(&self, holder: &str, now: DateTime<Utc>, ttl: Duration) -> Result<(), StoreError> {
        let holder = holder.to_string();
        let exp = (now + chrono::Duration::from_std(ttl).unwrap_or_default()).timestamp_millis();
        let now_ms = now.timestamp_millis();
        self.with(move |c| {
            let n = c
                .execute(
                    "INSERT INTO leases (name, holder, expires_at_ms) VALUES ('compaction', ?1, ?2)
                     ON CONFLICT (name) DO UPDATE SET holder = excluded.holder, expires_at_ms = excluded.expires_at_ms
                     WHERE leases.expires_at_ms <= ?3 OR leases.holder = excluded.holder",
                    params![holder, exp, now_ms],
                )
                .map_err(db_err)?;
            if n == 1 {
                Ok(())
            } else {
                let (h, e): (String, i64) =
                    c.query_row("SELECT holder, expires_at_ms FROM leases WHERE name = 'compaction'", [], |r| Ok((r.get(0)?, r.get(1)?))).map_err(db_err)?;
                Err(StoreError::Conflict(format!("compaction lease held by {h} until {}", DateTime::from_timestamp_millis(e).unwrap_or_default())))
            }
        })
        .await
    }

    /// Release the compaction lease if `holder` holds it.
    pub async fn release_compaction_lease(&self, holder: &str) -> Result<(), StoreError> {
        let holder = holder.to_string();
        self.with(move |c| c.execute("DELETE FROM leases WHERE name = 'compaction' AND holder = ?1", params![holder]).map(|_| ()).map_err(db_err)).await
    }
}

#[async_trait]
impl HistoryStore for SqliteStore {
    fn info(&self) -> StoreInfo {
        StoreInfo { backend: Backend::Sqlite, uri: self.uri.clone(), durable_shared: false }
    }

    fn durable(&self) -> bool {
        !self.uri.ends_with(":memory:")
    }

    fn shared(&self) -> bool {
        false
    }

    fn role(&self) -> Role {
        self.opts.role
    }

    async fn put_run(&self, rec: &RunRecord) -> Result<PutOutcome, StoreError> {
        let c = run_cols(rec, self.opts.write_version)?;
        let now = Utc::now().timestamp_millis();
        self.with(move |conn| {
            let n = conn
                .execute(
                    "INSERT OR IGNORE INTO runs (key, tenant, invocation_id, attempt, source, cluster, started_at_ms, written_at_ms, v, body)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                    params![c.key, c.tenant, c.invocation_id, c.attempt, c.source, c.cluster, c.started_at_ms, now, c.v, c.body],
                )
                .map_err(db_err)?;
            Ok(if n == 1 { PutOutcome::Inserted } else { PutOutcome::Duplicate })
        })
        .await
    }

    async fn put_decision(&self, d: &Decision) -> Result<(), StoreError> {
        let c = decision_cols(d, self.opts.write_version)?;
        self.with(move |conn| {
            conn.execute(
                "INSERT OR IGNORE INTO decisions (id, key, tenant, at_ms, mode, estimator_id, estimator_version, body)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![c.id, c.key, c.tenant, c.at_ms, c.mode, c.estimator_id, c.estimator_version, c.body],
            )
            .map(|_| ())
            .map_err(db_err)
        })
        .await
    }

    async fn run_marker(&self, key: &WorkloadKey) -> Result<Option<String>, StoreError> {
        // `rowid_` is AUTOINCREMENT: never reused, so it only moves forward.
        let k = key.to_string();
        self.with(move |c| {
            let m: Option<i64> = c.query_row("SELECT MAX(rowid_) FROM runs WHERE key = ?1", params![k], |r| r.get(0)).map_err(db_err)?;
            Ok(Some(m.unwrap_or(0).to_string()))
        })
        .await
    }

    async fn runs_changed_since(&self, key: &WorkloadKey, marker: &str) -> Result<bool, StoreError> {
        let Ok(m) = marker.parse::<i64>() else { return Ok(true) };
        let k = key.to_string();
        self.with(move |c| {
            c.query_row("SELECT EXISTS (SELECT 1 FROM runs WHERE key = ?1 AND rowid_ > ?2)", params![k, m], |r| r.get::<_, bool>(0)).map_err(db_err)
        })
        .await
    }

    async fn runs(&self, key: &WorkloadKey, limit: usize) -> Result<Vec<RunRecord>, StoreError> {
        let k = key.to_string();
        let lim = limit.min(i64::MAX as usize) as i64;
        self.with(move |c| {
            let mut st = c
                .prepare_cached("SELECT body FROM runs WHERE key = ?1 ORDER BY started_at_ms DESC, attempt DESC, invocation_id DESC LIMIT ?2")
                .map_err(db_err)?;
            let rows = st.query_map(params![k, lim], |r| r.get::<_, String>(0)).map_err(db_err)?;
            let mut out = Vec::new();
            for b in rows {
                if let Some(r) = decode::<RunRecord>(&b.map_err(db_err)?, "runs") {
                    out.push(r);
                }
            }
            crate::sort_newest_first(&mut out);
            Ok(out)
        })
        .await
    }

    async fn decisions(&self, filter: &DecisionFilter) -> Result<Vec<Decision>, StoreError> {
        let f = filter.clone();
        self.with(move |c| {
            let mut sql = String::from("SELECT body FROM decisions WHERE 1=1");
            let mut args: Vec<rusqlite::types::Value> = Vec::new();
            let mut push = |cond: &str, v: rusqlite::types::Value, sql: &mut String| {
                args.push(v);
                sql.push_str(&format!(" AND {cond} ?{}", args.len()));
            };
            if let Some(k) = &f.key {
                push("key =", k.to_string().into(), &mut sql);
            }
            if let Some((p, t)) = &f.tenant {
                push("tenant =", format!("{p}/{t}").into(), &mut sql);
            }
            if let Some(s) = f.since {
                push("at_ms >=", s.timestamp_millis().into(), &mut sql);
            }
            if let Some(u) = f.until {
                push("at_ms <", u.timestamp_millis().into(), &mut sql);
            }
            if let Some(m) = f.mode {
                push("mode =", m.as_str().to_string().into(), &mut sql);
            }
            if let Some((id, v)) = &f.estimator {
                push("estimator_id =", id.clone().into(), &mut sql);
                push("estimator_version =", v.clone().into(), &mut sql);
            }
            sql.push_str(" ORDER BY id DESC");
            if let Some(n) = f.limit {
                sql.push_str(&format!(" LIMIT {}", n.min(i64::MAX as usize)));
            }
            let mut st = c.prepare(&sql).map_err(db_err)?;
            let rows = st.query_map(params_from_iter(args.iter()), |r| r.get::<_, String>(0)).map_err(db_err)?;
            let mut out = Vec::new();
            for b in rows {
                if let Some(d) = decode::<Decision>(&b.map_err(db_err)?, "decisions").filter(|d| f.matches(d)) {
                    out.push(d);
                }
            }
            Ok(out)
        })
        .await
    }

    async fn decision(&self, id: Ulid) -> Result<Option<Decision>, StoreError> {
        let id = id.to_string();
        self.with(move |c| {
            let b: Option<String> = c.query_row("SELECT body FROM decisions WHERE id = ?1", params![id], |r| r.get(0)).optional().map_err(db_err)?;
            Ok(b.and_then(|b| decode::<Decision>(&b, "decisions")))
        })
        .await
    }

    async fn breaker_get(&self, scope: &BreakerScope) -> Result<BreakerObjects, StoreError> {
        // Current and N-1 (no platform segment) paths.
        let sp = scope_path(scope);
        let legacy = scope.legacy_path().unwrap_or_else(|| sp.clone());
        let scope = scope.clone();
        self.with(move |c| {
            let mut objs = BreakerObjects::default();
            let mut st = c.prepare_cached("SELECT body FROM breaker_trips WHERE scope_path IN (?1, ?2) ORDER BY id").map_err(db_err)?;
            for b in st.query_map(params![sp, legacy], |r| r.get::<_, String>(0)).map_err(db_err)? {
                if let Some(t) = decode::<Trip>(&b.map_err(db_err)?, "breaker_trips").filter(|t| t.scope == scope) {
                    objs.trips.push(t);
                }
            }
            let mut st = c.prepare_cached("SELECT body FROM breaker_resets WHERE scope_path IN (?1, ?2) ORDER BY at_ms").map_err(db_err)?;
            for b in st.query_map(params![sp, legacy], |r| r.get::<_, String>(0)).map_err(db_err)? {
                if let Some(r) = decode::<Reset>(&b.map_err(db_err)?, "breaker_resets").filter(|r| r.scope == scope) {
                    objs.resets.push(r);
                }
            }
            Ok(objs)
        })
        .await
    }

    async fn breaker_list(&self) -> Result<Vec<(BreakerScope, BreakerObjects)>, StoreError> {
        let scopes: Vec<BreakerScope> = self
            .with(|c| {
                let mut st = c.prepare("SELECT body FROM breaker_trips ORDER BY id").map_err(db_err)?;
                let mut v: Vec<BreakerScope> = Vec::new();
                for b in st.query_map([], |r| r.get::<_, String>(0)).map_err(db_err)? {
                    if let Some(t) = decode::<Trip>(&b.map_err(db_err)?, "breaker_trips")
                        && !v.contains(&t.scope)
                    {
                        v.push(t.scope);
                    }
                }
                Ok(v)
            })
            .await?;
        let mut out = Vec::new();
        for s in scopes {
            let o = self.breaker_get(&s).await?;
            out.push((s, o));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    async fn breaker_trip(&self, trip: &Trip) -> Result<(), StoreError> {
        let body = trip_body(trip, self.opts.write_version)?;
        let (id, sp, at) = (trip.id.to_string(), scope_path(&trip.scope), trip.at.timestamp_millis());
        self.with(move |c| {
            let n = c
                .execute("INSERT OR IGNORE INTO breaker_trips (id, scope_path, at_ms, body) VALUES (?1, ?2, ?3, ?4)", params![id, sp, at, body])
                .map_err(db_err)?;
            if n == 1 { Ok(()) } else { Err(StoreError::Conflict(format!("trip {id} exists"))) }
        })
        .await
    }

    async fn breaker_reset(&self, reset: &Reset) -> Result<(), StoreError> {
        self.opts.role.require_operator("a breaker reset")?;
        let body = reset_body(reset, self.opts.write_version)?;
        let (sp, at) = (scope_path(&reset.scope), reset.at.timestamp_millis());
        self.with(move |c| {
            c.execute("INSERT INTO breaker_resets (scope_path, at_ms, body) VALUES (?1, ?2, ?3)", params![sp, at, body]).map(|_| ()).map_err(db_err)
        })
        .await
    }

    async fn control_get(&self) -> Result<Option<Control>, StoreError> {
        self.with(|c| {
            let b: Option<String> = c.query_row("SELECT body FROM control WHERE id = 1", [], |r| r.get(0)).optional().map_err(db_err)?;
            Ok(b.and_then(|b| decode::<Control>(&b, "control")))
        })
        .await
    }

    async fn control_set(&self, control: &Control) -> Result<(), StoreError> {
        self.opts.role.require_operator("the control object")?;
        let mut ctl = control.clone();
        ctl.v = self.opts.write_version;
        let body = to_json(&ctl)?;
        self.with(move |c| {
            c.execute("INSERT INTO control (id, body) VALUES (1, ?1) ON CONFLICT (id) DO UPDATE SET body = excluded.body", params![body])
                .map(|_| ())
                .map_err(db_err)
        })
        .await
    }

    async fn attempt_get(&self, key: &WorkloadKey, invocation_id: &str) -> Result<Option<AttemptRecord>, StoreError> {
        let (k, inv, ttl) = (key.to_string(), invocation_id.to_string(), self.opts.attempt_ttl);
        self.with(move |c| {
            let b: Option<String> =
                c.query_row("SELECT body FROM attempts WHERE key = ?1 AND invocation_id = ?2", params![k, inv], |r| r.get(0)).optional().map_err(db_err)?;
            Ok(b.and_then(|b| decode::<AttemptRecord>(&b, "attempts")).filter(|a| !attempt_expired(a.written_at, Utc::now(), ttl)))
        })
        .await
    }

    async fn attempt_put_if_absent(&self, rec: &AttemptRecord) -> Result<bool, StoreError> {
        let body = attempt_body(rec, self.opts.write_version)?;
        let (k, inv, at) = (rec.key.to_string(), rec.invocation_id.clone(), rec.written_at.timestamp_millis());
        let cut = (Utc::now() - chrono::Duration::from_std(self.opts.attempt_ttl).unwrap_or_default()).timestamp_millis();
        self.with(move |c| {
            let n = c
                .execute(
                    "INSERT INTO attempts (key, invocation_id, written_at_ms, body) VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT (key, invocation_id) DO UPDATE SET written_at_ms = excluded.written_at_ms, body = excluded.body
                     WHERE attempts.written_at_ms < ?5",
                    params![k, inv, at, body, cut],
                )
                .map_err(db_err)?;
            Ok(n == 1)
        })
        .await
    }

    async fn attempt_close_lease(&self, key: &WorkloadKey, invocation_id: &str, at: DateTime<Utc>, outcome: RunStatus) -> Result<(), StoreError> {
        let (k, inv) = (key.to_string(), invocation_id.to_string());
        self.with(move |c| {
            let tx = c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate).map_err(db_err)?;
            let b: Option<String> =
                tx.query_row("SELECT body FROM attempts WHERE key = ?1 AND invocation_id = ?2", params![k, inv], |r| r.get(0)).optional().map_err(db_err)?;
            let mut rec = b.and_then(|b| decode::<AttemptRecord>(&b, "attempts")).ok_or_else(|| StoreError::Other(format!("no attempt record for {inv}")))?;
            let lease = rec.lease.get_or_insert(Lease { opened_at: at, closed_at: None, outcome: None });
            lease.closed_at = Some(at);
            lease.outcome = Some(outcome);
            tx.execute("UPDATE attempts SET body = ?3 WHERE key = ?1 AND invocation_id = ?2", params![k, inv, to_json(&rec)?]).map_err(db_err)?;
            tx.commit().map_err(db_err)
        })
        .await
    }

    async fn compact(&self, opts: &CompactOptions) -> Result<CompactReport, StoreError> {
        let holder = self.opts.holder.clone();
        if !opts.dry_run {
            self.acquire_compaction_lease(&holder, opts.now, self.opts.lease_ttl).await?;
        }
        let ms = |d: Duration| opts.now - chrono::Duration::from_std(d).unwrap_or_default();
        let (age, grace, dec, att) =
            (ms(opts.max_age).timestamp_millis(), ms(opts.grace).timestamp_millis(), ms(opts.decision_max_age).timestamp_millis(), ms(self.opts.attempt_ttl).timestamp_millis());
        let dry = opts.dry_run;
        let mut status = CompactionStatus { v: self.opts.write_version, holder: holder.clone(), started_at: Some(opts.now), ..Default::default() };
        let r = self
            .with(move |c| {
                let tx = c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate).map_err(db_err)?;
                let count = |sql: &str, p: &[&dyn rusqlite::ToSql]| -> Result<u64, StoreError> { tx.query_row(sql, p, |r| r.get::<_, i64>(0)).map(|n| n as u64).map_err(db_err) };
                let runs = count("SELECT COUNT(*) FROM runs WHERE started_at_ms < ?1 AND written_at_ms < ?2", &[&age, &grace])?;
                let decs = count("SELECT COUNT(*) FROM decisions WHERE at_ms < ?1 AND at_ms < ?2", &[&dec, &grace])?;
                let atts = count("SELECT COUNT(*) FROM attempts WHERE written_at_ms < ?1", &[&att])?;
                if !dry {
                    tx.execute("DELETE FROM runs WHERE started_at_ms < ?1 AND written_at_ms < ?2", params![age, grace]).map_err(db_err)?;
                    tx.execute("DELETE FROM decisions WHERE at_ms < ?1 AND at_ms < ?2", params![dec, grace]).map_err(db_err)?;
                    tx.execute("DELETE FROM attempts WHERE written_at_ms < ?1", params![att]).map_err(db_err)?;
                }
                let keys = count("SELECT COUNT(DISTINCT key) FROM runs", &[])?;
                let remaining = count("SELECT COUNT(*) FROM runs", &[])?;
                tx.commit().map_err(db_err)?;
                Ok((runs, decs, atts, keys, remaining))
            })
            .await;
        if let Ok((runs, decs, atts, keys, remaining)) = &r {
            status.run_shards_deleted = *runs;
            status.decisions_deleted = *decs;
            status.attempts_deleted = *atts;
            status.keys = *keys;
            status.shards_remaining = *remaining;
            status.finished_at = Some(Utc::now().max(opts.now));
            if !dry && let Ok(body) = to_json(&status) {
                let _ = self
                    .with(move |c| {
                        c.execute("INSERT INTO compaction_status (id, body) VALUES (1, ?1) ON CONFLICT (id) DO UPDATE SET body = excluded.body", params![body])
                            .map(|_| ())
                            .map_err(db_err)
                    })
                    .await;
            }
        }
        if !dry {
            let _ = self.release_compaction_lease(&holder).await;
        }
        let (runs, ..) = r?;
        Ok(CompactReport { pointers: 0, snapshots: 0, deleted: runs as usize })
    }

    async fn keys(&self) -> Result<Vec<WorkloadKey>, StoreError> {
        self.with(|c| {
            let mut st = c.prepare("SELECT DISTINCT key FROM runs ORDER BY key").map_err(db_err)?;
            let rows = st.query_map([], |r| r.get::<_, String>(0)).map_err(db_err)?;
            let mut out = Vec::new();
            for k in rows {
                if let Ok(k) = WorkloadKey::parse(&k.map_err(db_err)?) {
                    out.push(k);
                }
            }
            Ok(out)
        })
        .await
    }

    async fn stats(&self) -> Result<StoreStats, StoreError> {
        self.with(|c| {
            let shards: i64 = c.query_row("SELECT COUNT(*) FROM runs", [], |r| r.get(0)).map_err(db_err)?;
            let b: Option<String> = c.query_row("SELECT body FROM compaction_status WHERE id = 1", [], |r| r.get(0)).optional().map_err(db_err)?;
            let status = b.and_then(|b| decode::<CompactionStatus>(&b, "compaction_status"));
            Ok(StoreStats {
                shards: Some(shards as u64),
                compaction_lag_seconds: status.as_ref().and_then(|s| s.finished_at).map(|t| (Utc::now() - t).num_milliseconds().max(0) as f64 / 1000.0),
                last_compaction: status,
                skipped_on_read: 0,
            })
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refuses_network_filesystems() {
        let p = Path::new("/mnt/shared/history.db");
        assert!(matches!(check_filesystem(p, &FsKind::Network("nfs".into()), false), Err(StoreError::Config(_))));
        assert!(check_filesystem(p, &FsKind::Network("smbfs".into()), true).is_ok(), "explicit override");
        assert!(check_filesystem(p, &FsKind::Local, false).is_ok());
        assert!(check_filesystem(p, &FsKind::Unknown, false).is_ok());
        assert!(matches!(
            check_filesystem(p, &crate::fsdetect::classify(Some("macfuse"), None), false),
            Err(StoreError::Config(m)) if m.contains("network")
        ));
    }

    #[test]
    fn wal_and_migrations() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("h.db");
        let s = SqliteStore::open(&p).unwrap();
        assert_eq!(s.journal_mode().unwrap(), "wal");
        drop(s);
        // Re-open: migrations are idempotent.
        let s = SqliteStore::open(&p).unwrap();
        let v: u32 = s.connection().lock().unwrap().query_row("SELECT MAX(version) FROM schema_version", [], |r| r.get(0)).unwrap();
        assert_eq!(v as usize, MIGRATIONS.len());
        // A database from a newer build is refused.
        s.connection().lock().unwrap().execute("INSERT INTO schema_version (version) VALUES (99)", []).unwrap();
        drop(s);
        assert!(matches!(SqliteStore::open(&p), Err(StoreError::Config(m)) if m.contains("newer")));
    }
}
