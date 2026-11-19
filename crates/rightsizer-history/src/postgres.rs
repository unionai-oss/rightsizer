//! The postgres backend (feature `postgres`, SPEC §8.1): the K8s webhook and
//! watcher deployment; per-tenant roles give `Authenticated` trust (SPEC §8.2).
//!
//! Same semantics as the other backends, with conditional writes through
//! unique constraints and `INSERT … ON CONFLICT DO NOTHING`: runs are
//! deduplicated by a unique index on `(key, invocation_id, attempt,
//! source)`, trips and attempt records are create-only, the compaction
//! lease is a row taken over only when expired, attempt-lease closes are
//! compare-and-swap updates. Operator-only writes (resets, control) are
//! refused client-side for the client role and server-side by GRANTs
//! (documented in [`MIGRATIONS`]).
//!
//! URL: `postgres://user:pass@host:port/db?sslmode=require&sslrootcert=/ca.pem&schema=rightsizer`
//! (`schema` and `sslrootcert` are consumed here; the rest goes to
//! `tokio-postgres`). TLS uses rustls with the CA bundle from `sslrootcert`
//! (or `$SSL_CERT_FILE`).

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rightsizer_core::breaker::{BreakerObjects, BreakerScope, Control, Reset, Trip};
use rightsizer_core::{AttemptRecord, Decision, Lease, RunRecord, RunStatus, WorkloadKey};
use serde_json::Value;
use tokio::sync::RwLock;
use tokio_postgres::error::SqlState;
use tokio_postgres::types::ToSql;
use ulid::Ulid;

use crate::sql::{attempt_body, decision_cols, reset_body, run_cols, scope_path, to_json, trip_body};
use crate::{
    ATTEMPT_TTL, Backend, CompactOptions, CompactReport, CompactionStatus, DecisionFilter, HistoryStore, PutOutcome, Role,
    StoreError, StoreInfo, StoreStats, attempt_expired, decode_value,
};

/// Keeps the client types in this feature's dependency set.
pub type Client = tokio_postgres::Client;
/// TLS connector type.
pub type Tls = tokio_postgres_rustls::MakeRustlsConnect;

/// Ordered migrations. `{s}` is replaced by the schema name. Each runs in
/// one transaction with its `schema_version` row.
///
/// Roles and GRANTs (run by a DBA; rightsizer does not create roles):
///
/// ```sql
/// -- Clients (webhook, watcher, workloads): append history, create trips
/// -- and attempt records, close leases; read everything.
/// CREATE ROLE rightsizer_client NOLOGIN;
/// GRANT USAGE ON SCHEMA rightsizer TO rightsizer_client;
/// GRANT SELECT ON ALL TABLES IN SCHEMA rightsizer TO rightsizer_client;
/// GRANT INSERT ON rightsizer.runs, rightsizer.decisions, rightsizer.breaker_trips, rightsizer.attempts TO rightsizer_client;
/// GRANT UPDATE (body, written_at_ms) ON rightsizer.attempts TO rightsizer_client;
/// GRANT USAGE ON ALL SEQUENCES IN SCHEMA rightsizer TO rightsizer_client;
/// -- Operators (`rsz breaker reset`, `rsz control`, `rsz history compact`, migrations).
/// CREATE ROLE rightsizer_operator NOLOGIN IN ROLE rightsizer_client;
/// GRANT INSERT ON rightsizer.breaker_resets TO rightsizer_operator;
/// GRANT INSERT, UPDATE ON rightsizer.control TO rightsizer_operator;
/// GRANT DELETE ON rightsizer.runs, rightsizer.decisions, rightsizer.attempts TO rightsizer_operator;
/// GRANT INSERT, UPDATE, DELETE ON rightsizer.leases, rightsizer.compaction_status TO rightsizer_operator;
/// -- Per-tenant credentials (Authenticated trust, SPEC §8.2): one login role
/// -- per tenant in rightsizer_client, restricted by row-level security:
/// ALTER TABLE rightsizer.runs ENABLE ROW LEVEL SECURITY;
/// CREATE POLICY tenant_rows ON rightsizer.runs TO rightsizer_client
///   USING (true) WITH CHECK (tenant = current_setting('rightsizer.tenant', true));
/// -- … with `ALTER ROLE tenant_x SET rightsizer.tenant = 'k8s/prod/ml'`.
/// ```
pub const MIGRATIONS: &[&str] = &[
    // 1: initial schema.
    r#"
    CREATE TABLE {s}.runs (
        id            BIGSERIAL PRIMARY KEY,
        key           TEXT NOT NULL,
        tenant        TEXT NOT NULL,
        invocation_id TEXT NOT NULL,
        attempt       BIGINT NOT NULL,
        source        TEXT NOT NULL,
        cluster       TEXT,
        started_at_ms BIGINT NOT NULL,
        written_at_ms BIGINT NOT NULL,
        v             INTEGER NOT NULL,
        body          JSONB NOT NULL,
        CONSTRAINT runs_dedup UNIQUE (key, invocation_id, attempt, source)
    );
    CREATE INDEX runs_key_started ON {s}.runs (key, started_at_ms DESC, attempt DESC);
    CREATE TABLE {s}.decisions (
        id                TEXT PRIMARY KEY,
        key               TEXT NOT NULL,
        tenant            TEXT NOT NULL,
        at_ms             BIGINT NOT NULL,
        mode              TEXT NOT NULL,
        estimator_id      TEXT,
        estimator_version TEXT,
        body              JSONB NOT NULL
    );
    CREATE INDEX decisions_at ON {s}.decisions (at_ms DESC);
    CREATE INDEX decisions_key_at ON {s}.decisions (key, at_ms DESC);
    CREATE INDEX decisions_tenant_at ON {s}.decisions (tenant, at_ms DESC);
    CREATE TABLE {s}.breaker_trips (
        id         TEXT PRIMARY KEY,
        scope_path TEXT NOT NULL,
        at_ms      BIGINT NOT NULL,
        body       JSONB NOT NULL
    );
    CREATE INDEX breaker_trips_scope ON {s}.breaker_trips (scope_path);
    CREATE TABLE {s}.breaker_resets (
        id         BIGSERIAL PRIMARY KEY,
        scope_path TEXT NOT NULL,
        at_ms      BIGINT NOT NULL,
        body       JSONB NOT NULL
    );
    CREATE INDEX breaker_resets_scope ON {s}.breaker_resets (scope_path);
    CREATE TABLE {s}.control (
        id   INTEGER PRIMARY KEY CHECK (id = 1),
        body JSONB NOT NULL
    );
    CREATE TABLE {s}.attempts (
        key           TEXT NOT NULL,
        invocation_id TEXT NOT NULL,
        written_at_ms BIGINT NOT NULL,
        body          JSONB NOT NULL,
        PRIMARY KEY (key, invocation_id)
    );
    CREATE TABLE {s}.leases (
        name          TEXT PRIMARY KEY,
        holder        TEXT NOT NULL,
        expires_at_ms BIGINT NOT NULL
    );
    CREATE TABLE {s}.compaction_status (
        id   INTEGER PRIMARY KEY CHECK (id = 1),
        body JSONB NOT NULL
    );
    "#,
];

/// Connection settings.
#[derive(Clone, Debug)]
pub struct PostgresConfig {
    /// `postgres://…` URL (without the parameters consumed here).
    pub url: String,
    /// Require TLS (rustls).
    pub tls: bool,
    /// CA bundle for TLS.
    pub ca_file: Option<PathBuf>,
    /// Schema holding the tables (`rightsizer`).
    pub schema: String,
    /// Credentials role.
    pub role: Role,
    /// Run migrations on connect (default: operators only; clients check the version).
    pub migrate: Option<bool>,
    /// Wire version written.
    pub write_version: u16,
    /// Attempt record lifetime (7 d).
    pub attempt_ttl: Duration,
    /// Compaction lease lifetime (1 h).
    pub lease_ttl: Duration,
    /// Compaction holder id.
    pub holder: String,
}

impl PostgresConfig {
    /// Parse a URL, consuming `schema`, `sslrootcert` and deciding TLS from `sslmode`.
    pub fn from_url(url: &str) -> Result<Self, StoreError> {
        let mut u = url::Url::parse(url).map_err(|e| StoreError::Config(format!("postgres URL: {e}")))?;
        let mut schema = "rightsizer".to_string();
        let mut ca_file = None;
        let mut tls = false;
        let mut keep: Vec<(String, String)> = Vec::new();
        for (k, v) in u.query_pairs() {
            match k.as_ref() {
                "schema" => schema = v.to_string(),
                "sslrootcert" => ca_file = Some(PathBuf::from(v.as_ref())),
                "sslmode" => {
                    tls = matches!(v.as_ref(), "require" | "verify-ca" | "verify-full");
                    // tokio-postgres knows disable / prefer / require.
                    let m = if tls { "require" } else { "disable" };
                    keep.push((k.to_string(), m.to_string()));
                }
                _ => keep.push((k.to_string(), v.to_string())),
            }
        }
        if keep.is_empty() {
            u.set_query(None);
        } else {
            u.query_pairs_mut().clear().extend_pairs(keep);
        }
        if !schema.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_') || schema.is_empty() || schema.starts_with(|c: char| c.is_ascii_digit()) {
            return Err(StoreError::Config(format!("postgres schema {schema:?} must match [a-z_][a-z0-9_]*")));
        }
        Ok(PostgresConfig {
            url: u.to_string(),
            tls,
            ca_file,
            schema,
            role: Role::Client,
            migrate: None,
            write_version: rightsizer_core::WIRE_VERSION,
            attempt_ttl: ATTEMPT_TTL,
            lease_ttl: Duration::from_secs(3600),
            holder: crate::default_holder(),
        })
    }
}

/// A postgres-backed store.
pub struct PostgresStore {
    cfg: PostgresConfig,
    client: RwLock<Arc<Client>>,
    uri: String,
}

fn pg_err(e: tokio_postgres::Error) -> StoreError {
    if let Some(code) = e.code() {
        if *code == SqlState::UNIQUE_VIOLATION {
            return StoreError::Conflict(e.to_string());
        }
        if *code == SqlState::INSUFFICIENT_PRIVILEGE {
            return StoreError::PermissionDenied(e.to_string());
        }
        if *code == SqlState::UNDEFINED_TABLE {
            return StoreError::Config(format!("{e} (schema not migrated?)"));
        }
        return StoreError::Other(e.to_string());
    }
    StoreError::Unreachable(e.to_string())
}


async fn connect_client(cfg: &PostgresConfig) -> Result<Client, StoreError> {
    let pc = tokio_postgres::Config::from_str(&cfg.url).map_err(|e| StoreError::Config(format!("postgres URL: {e}")))?;
    if cfg.tls {
        let ca = cfg.ca_file.clone().or_else(|| std::env::var_os("SSL_CERT_FILE").map(PathBuf::from)).ok_or_else(|| {
            StoreError::Config("postgres TLS needs a CA bundle: sslrootcert=/path/ca.pem or $SSL_CERT_FILE".into())
        })?;
        use rustls::pki_types::CertificateDer;
        use rustls::pki_types::pem::PemObject;
        let mut roots = rustls::RootCertStore::empty();
        for c in CertificateDer::pem_file_iter(&ca).map_err(|e| StoreError::Config(format!("{}: {e}", ca.display())))? {
            let c = c.map_err(|e| StoreError::Config(format!("{}: {e}", ca.display())))?;
            roots.add(c).map_err(|e| StoreError::Config(format!("{}: {e}", ca.display())))?;
        }
        let tls = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .map_err(|e| StoreError::Config(e.to_string()))?
            .with_root_certificates(roots)
            .with_no_client_auth();
        let (client, conn) = pc.connect(tokio_postgres_rustls::MakeRustlsConnect::new(tls)).await.map_err(pg_err)?;
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::warn!(error = %e, "postgres connection closed");
            }
        });
        Ok(client)
    } else {
        let (client, conn) = pc.connect(tokio_postgres::NoTls).await.map_err(pg_err)?;
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::warn!(error = %e, "postgres connection closed");
            }
        });
        Ok(client)
    }
}

/// Run (or check) migrations under an advisory lock.
async fn migrate(client: &Client, schema: &str, run: bool) -> Result<u32, StoreError> {
    let known = MIGRATIONS.len() as u32;
    let s = schema;
    if run {
        client.execute("SELECT pg_advisory_lock(hashtext($1))", &[&format!("rightsizer-migrate-{s}")]).await.map_err(pg_err)?;
        let r = async {
            client
                .batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {s}; CREATE TABLE IF NOT EXISTS {s}.schema_version (version INTEGER NOT NULL);"))
                .await
                .map_err(pg_err)?;
            let cur: i32 = client.query_one(&format!("SELECT COALESCE(MAX(version), 0) FROM {s}.schema_version"), &[]).await.map_err(pg_err)?.get(0);
            if cur as u32 > known {
                return Err(StoreError::Config(format!("postgres schema version {cur} is newer than this build supports ({known})")));
            }
            for (i, m) in MIGRATIONS.iter().enumerate().skip(cur as usize) {
                let sql = format!("BEGIN; {} INSERT INTO {s}.schema_version (version) VALUES ({}); COMMIT;", m.replace("{s}", s), i + 1);
                if let Err(e) = client.batch_execute(&sql).await {
                    let _ = client.batch_execute("ROLLBACK").await;
                    return Err(pg_err(e));
                }
            }
            Ok(known)
        }
        .await;
        let _ = client.execute("SELECT pg_advisory_unlock(hashtext($1))", &[&format!("rightsizer-migrate-{s}")]).await;
        r
    } else {
        let row = client.query_one(&format!("SELECT COALESCE(MAX(version), 0) FROM {s}.schema_version"), &[]).await.map_err(pg_err)?;
        let cur: i32 = row.get(0);
        if cur as u32 > known {
            return Err(StoreError::Config(format!("postgres schema version {cur} is newer than this build supports ({known})")));
        }
        if (cur as u32) < known {
            return Err(StoreError::Config(format!("postgres schema version {cur} is older than {known}; run migrations with operator credentials")));
        }
        Ok(known)
    }
}

type Params<'a> = Vec<Box<dyn ToSql + Sync + Send + 'a>>;

fn refs<'a>(p: &'a Params<'_>) -> Vec<&'a (dyn ToSql + Sync)> {
    p.iter().map(|b| b.as_ref() as &(dyn ToSql + Sync)).collect()
}

fn decode<T: serde::de::DeserializeOwned>(v: Value, what: &str) -> Option<T> {
    match decode_value::<T>(v, what) {
        Ok(x) => x,
        Err(e) => {
            tracing::warn!(error = %e, "skipping unreadable row");
            None
        }
    }
}

fn json_value(s: &str) -> Result<Value, StoreError> {
    serde_json::from_str(s).map_err(|e| StoreError::Other(e.to_string()))
}

impl PostgresStore {
    /// Connect, and migrate (operators) or check the schema version (clients).
    pub async fn connect(cfg: PostgresConfig) -> Result<Self, StoreError> {
        crate::check_write_version(cfg.write_version)?;
        let client = connect_client(&cfg).await?;
        migrate(&client, &cfg.schema, cfg.migrate.unwrap_or(cfg.role == Role::Operator)).await?;
        let uri = crate::redact_uri(&cfg.url);
        Ok(PostgresStore { cfg, client: RwLock::new(Arc::new(client)), uri })
    }

    /// The configuration.
    pub fn config(&self) -> &PostgresConfig {
        &self.cfg
    }

    /// The current client, reconnecting if the connection closed.
    pub async fn client(&self) -> Result<Arc<Client>, StoreError> {
        {
            let c = self.client.read().await;
            if !c.is_closed() {
                return Ok(c.clone());
            }
        }
        let mut w = self.client.write().await;
        if w.is_closed() {
            *w = Arc::new(connect_client(&self.cfg).await?);
        }
        Ok(w.clone())
    }

    fn t(&self, table: &str) -> String {
        format!("{}.{table}", self.cfg.schema)
    }

    /// Acquire the compaction lease (an expired lease is taken over).
    pub async fn acquire_compaction_lease(&self, holder: &str, now: DateTime<Utc>, ttl: Duration) -> Result<(), StoreError> {
        let c = self.client().await?;
        let exp = (now + chrono::Duration::from_std(ttl).unwrap_or_default()).timestamp_millis();
        let n = c
            .execute(
                &format!(
                    "INSERT INTO {t} (name, holder, expires_at_ms) VALUES ('compaction', $1, $2)
                     ON CONFLICT (name) DO UPDATE SET holder = excluded.holder, expires_at_ms = excluded.expires_at_ms
                     WHERE {t}.expires_at_ms <= $3 OR {t}.holder = excluded.holder",
                    t = self.t("leases")
                ),
                &[&holder, &exp, &now.timestamp_millis()],
            )
            .await
            .map_err(pg_err)?;
        if n == 1 {
            return Ok(());
        }
        let row = c.query_one(&format!("SELECT holder, expires_at_ms FROM {} WHERE name = 'compaction'", self.t("leases")), &[]).await.map_err(pg_err)?;
        let (h, e): (String, i64) = (row.get(0), row.get(1));
        Err(StoreError::Conflict(format!("compaction lease held by {h} until {}", DateTime::from_timestamp_millis(e).unwrap_or_default())))
    }

    /// Release the compaction lease if `holder` holds it.
    pub async fn release_compaction_lease(&self, holder: &str) -> Result<(), StoreError> {
        let c = self.client().await?;
        c.execute(&format!("DELETE FROM {} WHERE name = 'compaction' AND holder = $1", self.t("leases")), &[&holder]).await.map_err(pg_err)?;
        Ok(())
    }

    async fn bodies(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Value>, StoreError> {
        let c = self.client().await?;
        Ok(c.query(sql, params).await.map_err(pg_err)?.into_iter().map(|r| r.get::<_, Value>(0)).collect())
    }
}

#[async_trait]
impl HistoryStore for PostgresStore {
    fn info(&self) -> StoreInfo {
        StoreInfo { backend: Backend::Postgres, uri: self.uri.clone(), durable_shared: true }
    }

    fn role(&self) -> Role {
        self.cfg.role
    }

    async fn put_run(&self, rec: &RunRecord) -> Result<PutOutcome, StoreError> {
        let r = run_cols(rec, self.cfg.write_version)?;
        let body = json_value(&r.body)?;
        let c = self.client().await?;
        let n = c
            .execute(
                &format!(
                    "INSERT INTO {} (key, tenant, invocation_id, attempt, source, cluster, started_at_ms, written_at_ms, v, body)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT ON CONSTRAINT runs_dedup DO NOTHING",
                    self.t("runs")
                ),
                &[&r.key, &r.tenant, &r.invocation_id, &r.attempt, &r.source, &r.cluster, &r.started_at_ms, &Utc::now().timestamp_millis(), &(r.v as i32), &body],
            )
            .await
            .map_err(pg_err)?;
        Ok(if n == 1 { PutOutcome::Inserted } else { PutOutcome::Duplicate })
    }

    async fn put_decision(&self, d: &Decision) -> Result<(), StoreError> {
        let x = decision_cols(d, self.cfg.write_version)?;
        let body = json_value(&x.body)?;
        let c = self.client().await?;
        c.execute(
            &format!(
                "INSERT INTO {} (id, key, tenant, at_ms, mode, estimator_id, estimator_version, body)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (id) DO NOTHING",
                self.t("decisions")
            ),
            &[&x.id, &x.key, &x.tenant, &x.at_ms, &x.mode, &x.estimator_id, &x.estimator_version, &body],
        )
        .await
        .map_err(pg_err)?;
        Ok(())
    }

    async fn runs(&self, key: &WorkloadKey, limit: usize) -> Result<Vec<RunRecord>, StoreError> {
        let lim = limit.min(i64::MAX as usize) as i64;
        let rows = self
            .bodies(
                &format!("SELECT body FROM {} WHERE key = $1 ORDER BY started_at_ms DESC, attempt DESC, invocation_id DESC LIMIT $2", self.t("runs")),
                &[&key.to_string(), &lim],
            )
            .await?;
        let mut out: Vec<RunRecord> = rows.into_iter().filter_map(|v| decode(v, "runs")).collect();
        crate::sort_newest_first(&mut out);
        Ok(out)
    }

    async fn decisions(&self, f: &DecisionFilter) -> Result<Vec<Decision>, StoreError> {
        let mut sql = format!("SELECT body FROM {} WHERE TRUE", self.t("decisions"));
        let mut p: Params<'_> = Vec::new();
        let mut add = |cond: &str, v: Box<dyn ToSql + Sync + Send>, sql: &mut String| {
            p.push(v);
            sql.push_str(&format!(" AND {cond} ${}", p.len()));
        };
        if let Some(k) = &f.key {
            add("key =", Box::new(k.to_string()), &mut sql);
        }
        if let Some((pl, t)) = &f.tenant {
            add("tenant =", Box::new(format!("{pl}/{t}")), &mut sql);
        }
        if let Some(s) = f.since {
            add("at_ms >=", Box::new(s.timestamp_millis()), &mut sql);
        }
        if let Some(u) = f.until {
            add("at_ms <", Box::new(u.timestamp_millis()), &mut sql);
        }
        if let Some(m) = f.mode {
            add("mode =", Box::new(m.as_str().to_string()), &mut sql);
        }
        if let Some((id, v)) = &f.estimator {
            add("estimator_id =", Box::new(id.clone()), &mut sql);
            add("estimator_version =", Box::new(v.clone()), &mut sql);
        }
        sql.push_str(" ORDER BY id DESC");
        if let Some(n) = f.limit {
            sql.push_str(&format!(" LIMIT {}", n.min(i64::MAX as usize)));
        }
        let rows = self.bodies(&sql, &refs(&p)).await?;
        Ok(rows.into_iter().filter_map(|v| decode::<Decision>(v, "decisions")).filter(|d| f.matches(d)).collect())
    }

    async fn decision(&self, id: Ulid) -> Result<Option<Decision>, StoreError> {
        let rows = self.bodies(&format!("SELECT body FROM {} WHERE id = $1", self.t("decisions")), &[&id.to_string()]).await?;
        Ok(rows.into_iter().next().and_then(|v| decode(v, "decisions")))
    }

    async fn breaker_get(&self, scope: &BreakerScope) -> Result<BreakerObjects, StoreError> {
        // Current and N-1 (no platform segment) paths.
        let sp = scope_path(scope);
        let legacy = scope.legacy_path().unwrap_or_else(|| sp.clone());
        let trips =
            self.bodies(&format!("SELECT body FROM {} WHERE scope_path IN ($1, $2) ORDER BY id", self.t("breaker_trips")), &[&sp, &legacy]).await?;
        let resets =
            self.bodies(&format!("SELECT body FROM {} WHERE scope_path IN ($1, $2) ORDER BY at_ms", self.t("breaker_resets")), &[&sp, &legacy]).await?;
        Ok(BreakerObjects {
            trips: trips.into_iter().filter_map(|v| decode::<Trip>(v, "breaker_trips")).filter(|t| &t.scope == scope).collect(),
            resets: resets.into_iter().filter_map(|v| decode::<Reset>(v, "breaker_resets")).filter(|r| &r.scope == scope).collect(),
        })
    }

    async fn breaker_list(&self) -> Result<Vec<(BreakerScope, BreakerObjects)>, StoreError> {
        let rows = self.bodies(&format!("SELECT body FROM {} ORDER BY id", self.t("breaker_trips")), &[]).await?;
        let mut scopes: Vec<BreakerScope> = Vec::new();
        for t in rows.into_iter().filter_map(|v| decode::<Trip>(v, "breaker_trips")) {
            if !scopes.contains(&t.scope) {
                scopes.push(t.scope);
            }
        }
        scopes.sort();
        let mut out = Vec::new();
        for s in scopes {
            let o = self.breaker_get(&s).await?;
            out.push((s, o));
        }
        Ok(out)
    }

    async fn breaker_trip(&self, trip: &Trip) -> Result<(), StoreError> {
        let body = json_value(&trip_body(trip, self.cfg.write_version)?)?;
        let c = self.client().await?;
        let n = c
            .execute(
                &format!("INSERT INTO {} (id, scope_path, at_ms, body) VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING", self.t("breaker_trips")),
                &[&trip.id.to_string(), &scope_path(&trip.scope), &trip.at.timestamp_millis(), &body],
            )
            .await
            .map_err(pg_err)?;
        if n == 1 { Ok(()) } else { Err(StoreError::Conflict(format!("trip {} exists", trip.id))) }
    }

    async fn breaker_reset(&self, reset: &Reset) -> Result<(), StoreError> {
        self.cfg.role.require_operator("a breaker reset")?;
        let body = json_value(&reset_body(reset, self.cfg.write_version)?)?;
        let c = self.client().await?;
        c.execute(
            &format!("INSERT INTO {} (scope_path, at_ms, body) VALUES ($1, $2, $3)", self.t("breaker_resets")),
            &[&scope_path(&reset.scope), &reset.at.timestamp_millis(), &body],
        )
        .await
        .map_err(pg_err)?;
        Ok(())
    }

    async fn control_get(&self) -> Result<Option<Control>, StoreError> {
        let rows = self.bodies(&format!("SELECT body FROM {} WHERE id = 1", self.t("control")), &[]).await?;
        Ok(rows.into_iter().next().and_then(|v| decode(v, "control")))
    }

    async fn control_set(&self, control: &Control) -> Result<(), StoreError> {
        self.cfg.role.require_operator("the control object")?;
        let mut ctl = control.clone();
        ctl.v = self.cfg.write_version;
        let body = json_value(&to_json(&ctl)?)?;
        let c = self.client().await?;
        c.execute(&format!("INSERT INTO {} (id, body) VALUES (1, $1) ON CONFLICT (id) DO UPDATE SET body = excluded.body", self.t("control")), &[&body])
            .await
            .map_err(pg_err)?;
        Ok(())
    }

    async fn attempt_get(&self, key: &WorkloadKey, invocation_id: &str) -> Result<Option<AttemptRecord>, StoreError> {
        let rows = self
            .bodies(&format!("SELECT body FROM {} WHERE key = $1 AND invocation_id = $2", self.t("attempts")), &[&key.to_string(), &invocation_id])
            .await?;
        Ok(rows
            .into_iter()
            .next()
            .and_then(|v| decode::<AttemptRecord>(v, "attempts"))
            .filter(|a| !attempt_expired(a.written_at, Utc::now(), self.cfg.attempt_ttl)))
    }

    async fn attempt_put_if_absent(&self, rec: &AttemptRecord) -> Result<bool, StoreError> {
        let body = json_value(&attempt_body(rec, self.cfg.write_version)?)?;
        let cut = (Utc::now() - chrono::Duration::from_std(self.cfg.attempt_ttl).unwrap_or_default()).timestamp_millis();
        let c = self.client().await?;
        let n = c
            .execute(
                &format!(
                    "INSERT INTO {t} (key, invocation_id, written_at_ms, body) VALUES ($1, $2, $3, $4)
                     ON CONFLICT (key, invocation_id) DO UPDATE SET written_at_ms = excluded.written_at_ms, body = excluded.body
                     WHERE {t}.written_at_ms < $5",
                    t = self.t("attempts")
                ),
                &[&rec.key.to_string(), &rec.invocation_id, &rec.written_at.timestamp_millis(), &body, &cut],
            )
            .await
            .map_err(pg_err)?;
        Ok(n == 1)
    }

    async fn attempt_close_lease(&self, key: &WorkloadKey, invocation_id: &str, at: DateTime<Utc>, outcome: RunStatus) -> Result<(), StoreError> {
        let k = key.to_string();
        let c = self.client().await?;
        for _ in 0..8 {
            let row = c
                .query_opt(&format!("SELECT body FROM {} WHERE key = $1 AND invocation_id = $2", self.t("attempts")), &[&k, &invocation_id])
                .await
                .map_err(pg_err)?;
            let Some(row) = row else { return Err(StoreError::Other(format!("no attempt record for {invocation_id}"))) };
            let old: Value = row.get(0);
            let mut rec = decode::<AttemptRecord>(old.clone(), "attempts").ok_or_else(|| StoreError::Other(format!("attempt record {invocation_id} unreadable")))?;
            let lease = rec.lease.get_or_insert(Lease { opened_at: at, closed_at: None, outcome: None });
            lease.closed_at = Some(at);
            lease.outcome = Some(outcome);
            let new = json_value(&to_json(&rec)?)?;
            // Compare-and-swap on the old body.
            let n = c
                .execute(&format!("UPDATE {} SET body = $3 WHERE key = $1 AND invocation_id = $2 AND body = $4", self.t("attempts")), &[&k, &invocation_id, &new, &old])
                .await
                .map_err(pg_err)?;
            if n == 1 {
                return Ok(());
            }
        }
        Err(StoreError::Conflict(format!("attempt record {invocation_id} kept changing")))
    }

    async fn compact(&self, opts: &CompactOptions) -> Result<CompactReport, StoreError> {
        let holder = self.cfg.holder.clone();
        if !opts.dry_run {
            self.acquire_compaction_lease(&holder, opts.now, self.cfg.lease_ttl).await?;
        }
        let ms = |d: Duration| (opts.now - chrono::Duration::from_std(d).unwrap_or_default()).timestamp_millis();
        let (age, grace, dec, att) = (ms(opts.max_age), ms(opts.grace), ms(opts.decision_max_age), ms(self.cfg.attempt_ttl));
        let r: Result<CompactionStatus, StoreError> = async {
            let c = self.client().await?;
            let mut st = CompactionStatus { v: self.cfg.write_version, holder: holder.clone(), started_at: Some(opts.now), ..Default::default() };
            let (runs, decs, atts) = if opts.dry_run {
                let one = |sql: String, p: Vec<i64>| {
                    let c = c.clone();
                    async move {
                        let refs: Vec<&(dyn ToSql + Sync)> = p.iter().map(|x| x as &(dyn ToSql + Sync)).collect();
                        c.query_one(&sql, &refs).await.map(|r| r.get::<_, i64>(0) as u64).map_err(pg_err)
                    }
                };
                (
                    one(format!("SELECT COUNT(*) FROM {} WHERE started_at_ms < $1 AND written_at_ms < $2", self.t("runs")), vec![age, grace]).await?,
                    one(format!("SELECT COUNT(*) FROM {} WHERE at_ms < $1 AND at_ms < $2", self.t("decisions")), vec![dec, grace]).await?,
                    one(format!("SELECT COUNT(*) FROM {} WHERE written_at_ms < $1", self.t("attempts")), vec![att]).await?,
                )
            } else {
                (
                    c.execute(&format!("DELETE FROM {} WHERE started_at_ms < $1 AND written_at_ms < $2", self.t("runs")), &[&age, &grace]).await.map_err(pg_err)?,
                    c.execute(&format!("DELETE FROM {} WHERE at_ms < $1 AND at_ms < $2", self.t("decisions")), &[&dec, &grace]).await.map_err(pg_err)?,
                    c.execute(&format!("DELETE FROM {} WHERE written_at_ms < $1", self.t("attempts")), &[&att]).await.map_err(pg_err)?,
                )
            };
            st.run_shards_deleted = runs;
            st.decisions_deleted = decs;
            st.attempts_deleted = atts;
            let row = c.query_one(&format!("SELECT COUNT(*), COUNT(DISTINCT key) FROM {}", self.t("runs")), &[]).await.map_err(pg_err)?;
            st.shards_remaining = row.get::<_, i64>(0) as u64;
            st.keys = row.get::<_, i64>(1) as u64;
            st.finished_at = Some(Utc::now().max(opts.now));
            if !opts.dry_run {
                let body = json_value(&to_json(&st)?)?;
                c.execute(
                    &format!("INSERT INTO {} (id, body) VALUES (1, $1) ON CONFLICT (id) DO UPDATE SET body = excluded.body", self.t("compaction_status")),
                    &[&body],
                )
                .await
                .map_err(pg_err)?;
            }
            Ok(st)
        }
        .await;
        if !opts.dry_run {
            let _ = self.release_compaction_lease(&holder).await;
        }
        let st = r?;
        Ok(CompactReport { pointers: 0, snapshots: 0, deleted: st.run_shards_deleted as usize })
    }

    async fn keys(&self) -> Result<Vec<WorkloadKey>, StoreError> {
        let c = self.client().await?;
        let rows = c.query(&format!("SELECT DISTINCT key FROM {} ORDER BY key", self.t("runs")), &[]).await.map_err(pg_err)?;
        Ok(rows.into_iter().filter_map(|r| WorkloadKey::parse(&r.get::<_, String>(0)).ok()).collect())
    }

    async fn stats(&self) -> Result<StoreStats, StoreError> {
        let c = self.client().await?;
        let shards: i64 = c.query_one(&format!("SELECT COUNT(*) FROM {}", self.t("runs")), &[]).await.map_err(pg_err)?.get(0);
        let status = c
            .query_opt(&format!("SELECT body FROM {} WHERE id = 1", self.t("compaction_status")), &[])
            .await
            .map_err(pg_err)?
            .and_then(|r| decode::<CompactionStatus>(r.get(0), "compaction_status"));
        Ok(StoreStats {
            shards: Some(shards as u64),
            compaction_lag_seconds: status.as_ref().and_then(|s| s.finished_at).map(|t| (Utc::now() - t).num_milliseconds().max(0) as f64 / 1000.0),
            last_compaction: status,
            skipped_on_read: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_parsing() {
        let c = PostgresConfig::from_url("postgres://u:p@h:5/db?sslmode=verify-full&sslrootcert=/ca.pem&schema=rs_test&application_name=x").unwrap();
        assert!(c.tls);
        assert_eq!(c.schema, "rs_test");
        assert_eq!(c.ca_file.as_deref(), Some(std::path::Path::new("/ca.pem")));
        assert!(!c.url.contains("schema=") && !c.url.contains("sslrootcert") && c.url.contains("sslmode=require") && c.url.contains("application_name=x"));
        let c = PostgresConfig::from_url("postgres://h/db").unwrap();
        assert!(!c.tls);
        assert_eq!(c.schema, "rightsizer");
        assert!(PostgresConfig::from_url("postgres://h/db?schema=Bad;drop").is_err());
        assert_eq!(crate::redact_uri("postgres://u:secret@h/db"), "postgres://u:***@h/db");
    }
}
