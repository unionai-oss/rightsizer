//! Store URI → backend (SPEC §8.1, §12.1 `history.uri`).
//!
//! | URI | backend |
//! |---|---|
//! | `memory://` | [`crate::MemoryStore`] (tests; not durable) |
//! | `/path`, `./path`, `file:///path` | object store on a local directory |
//! | `s3://bucket/prefix?endpoint=…&region=…&allow_http=true` | S3 or S3-compatible (MinIO, R2), feature `s3`; credentials from the environment |
//! | `gs://bucket/prefix` | GCS, feature `gcs` |
//! | `az://container/prefix?account=…` (also `azure://`, `abfs://`) | Azure Blob, feature `azure` |
//! | `sqlite:///abs/path.db`, `sqlite:rel.db`, `sqlite::memory:` | sqlite, feature `sqlite` |
//! | `postgres://…`, `postgresql://…` | postgres, feature `postgres` |
//!
//! The engine maps its `[history]` config onto [`StoreConfig`] (this crate
//! does not depend on the engine).

use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{HistoryStore, MemoryStore, ObjectHistoryStore, ObjectStoreOptions, Role, StoreError};

/// Everything needed to open a store.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct StoreConfig {
    /// Store URI.
    pub uri: String,
    /// Credentials role. Default: client (cannot reset breakers or write control).
    pub role: Role,
    /// Wire version written (`history.write_version`, SPEC §13.2).
    pub write_version: u16,
    /// Override the backend's own durability judgement.
    pub durable: Option<bool>,
    /// Override the backend's own sharedness judgement (e.g. a local path on
    /// a cluster filesystem the detector does not know).
    pub shared: Option<bool>,
    /// Open sqlite on a network filesystem anyway (unsafe).
    pub allow_network_fs: bool,
    /// Object store: read before `put_run` to report duplicates.
    pub check_duplicates_on_write: bool,
    /// Compaction holder id (default `{host}-{pid}-{ulid}`).
    pub holder: Option<String>,
}

impl Default for StoreConfig {
    fn default() -> Self {
        StoreConfig {
            uri: "memory://".into(),
            role: Role::Client,
            write_version: rightsizer_core::WIRE_VERSION,
            durable: None,
            shared: None,
            allow_network_fs: false,
            check_duplicates_on_write: true,
            holder: None,
        }
    }
}

impl StoreConfig {
    /// A config for `uri` with defaults.
    pub fn new(uri: impl Into<String>) -> Self {
        StoreConfig { uri: uri.into(), ..Default::default() }
    }
    /// With a role.
    pub fn role(mut self, role: Role) -> Self {
        self.role = role;
        self
    }
}

/// A parsed store URI.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreUri {
    /// In memory.
    Memory,
    /// Local directory (object-store layout).
    Local {
        /// Directory.
        path: PathBuf,
    },
    /// S3 / S3-compatible.
    S3 {
        /// Bucket.
        bucket: String,
        /// Prefix inside the bucket.
        prefix: String,
        /// Custom endpoint (MinIO, R2, …).
        endpoint: Option<String>,
        /// Region.
        region: Option<String>,
        /// Allow plain HTTP (default: when the endpoint is `http://`).
        allow_http: bool,
    },
    /// Google Cloud Storage.
    Gcs {
        /// Bucket.
        bucket: String,
        /// Prefix.
        prefix: String,
    },
    /// Azure Blob Storage.
    Azure {
        /// Container.
        container: String,
        /// Prefix.
        prefix: String,
        /// Storage account (else from the environment).
        account: Option<String>,
    },
    /// sqlite database file (`None`: in memory).
    Sqlite {
        /// Path.
        path: Option<PathBuf>,
    },
    /// postgres connection URL.
    Postgres {
        /// URL.
        url: String,
    },
}

/// Parse a store URI.
pub fn parse_uri(uri: &str) -> Result<StoreUri, StoreError> {
    let bad = |m: &str| StoreError::Config(format!("store URI {uri:?}: {m}"));
    let u = uri.trim();
    if u.is_empty() {
        return Err(bad("empty"));
    }
    if u == "memory://" || u == "memory:" || u == "mem://" {
        return Ok(StoreUri::Memory);
    }
    if let Some(rest) = u.strip_prefix("sqlite:") {
        if rest == ":memory:" || rest == "//:memory:" {
            return Ok(StoreUri::Sqlite { path: None });
        }
        let p = rest.strip_prefix("//").unwrap_or(rest);
        let p = p.split('?').next().unwrap_or(p);
        if p.is_empty() {
            return Err(bad("missing database path"));
        }
        return Ok(StoreUri::Sqlite { path: Some(PathBuf::from(p)) });
    }
    if u.starts_with("postgres://") || u.starts_with("postgresql://") {
        return Ok(StoreUri::Postgres { url: u.to_string() });
    }
    if let Some(rest) = u.strip_prefix("file://") {
        let p = rest.strip_prefix("localhost").unwrap_or(rest);
        if !p.starts_with('/') {
            return Err(bad("file URIs must be absolute (file:///path)"));
        }
        let decoded = url::Url::parse(&format!("file://{p}"))
            .ok()
            .and_then(|x| x.to_file_path().ok())
            .unwrap_or_else(|| PathBuf::from(p));
        return Ok(StoreUri::Local { path: decoded });
    }
    if !u.contains("://") {
        return Ok(StoreUri::Local { path: PathBuf::from(u) });
    }
    let url = url::Url::parse(u).map_err(|e| bad(&e.to_string()))?;
    let host = url.host_str().map(str::to_string).filter(|h| !h.is_empty()).ok_or_else(|| bad("missing bucket"))?;
    let prefix = url.path().trim_matches('/').to_string();
    let q = |name: &str| url.query_pairs().find(|(k, _)| k == name).map(|(_, v)| v.to_string());
    match url.scheme() {
        "s3" | "s3a" => {
            let endpoint = q("endpoint");
            let allow_http = q("allow_http").map(|v| v == "true" || v == "1").unwrap_or_else(|| endpoint.as_deref().is_some_and(|e| e.starts_with("http://")));
            Ok(StoreUri::S3 { bucket: host, prefix, endpoint, region: q("region"), allow_http })
        }
        "gs" | "gcs" => Ok(StoreUri::Gcs { bucket: host, prefix }),
        "az" | "azure" | "abfs" | "abfss" => Ok(StoreUri::Azure { container: host, prefix, account: q("account") }),
        s => Err(bad(&format!("unknown scheme {s:?}"))),
    }
}


fn object_opts(cfg: &StoreConfig, durable: bool, shared: bool) -> ObjectStoreOptions {
    let mut o = ObjectStoreOptions {
        role: cfg.role,
        write_version: cfg.write_version,
        durable: cfg.durable.unwrap_or(durable),
        shared: cfg.shared.unwrap_or(shared),
        check_duplicates_on_write: cfg.check_duplicates_on_write,
        ..Default::default()
    };
    if let Some(h) = &cfg.holder {
        o.holder = h.clone();
    }
    o
}

#[allow(unused_variables)]
fn remote(uri: &StoreUri, cfg: &StoreConfig) -> Result<ObjectHistoryStore, StoreError> {
    let err = |e: object_store::Error| StoreError::Config(e.to_string());
    match uri {
        #[cfg(feature = "s3")]
        StoreUri::S3 { bucket, prefix, endpoint, region, allow_http } => {
            use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
            let mut b = AmazonS3Builder::from_env().with_bucket_name(bucket).with_conditional_put(S3ConditionalPut::ETagMatch);
            if let Some(e) = endpoint {
                // S3-compatible stores generally want path-style requests.
                b = b.with_endpoint(e).with_virtual_hosted_style_request(false);
            }
            if let Some(r) = region {
                b = b.with_region(r);
            }
            if *allow_http {
                b = b.with_allow_http(true);
            }
            let s = b.build().map_err(err)?;
            Ok(ObjectHistoryStore::with_options(Arc::new(s), prefix.clone(), crate::redact_uri(&cfg.uri), object_opts(cfg, true, true)))
        }
        #[cfg(feature = "gcs")]
        StoreUri::Gcs { bucket, prefix } => {
            let s = object_store::gcp::GoogleCloudStorageBuilder::from_env().with_bucket_name(bucket).build().map_err(err)?;
            Ok(ObjectHistoryStore::with_options(Arc::new(s), prefix.clone(), crate::redact_uri(&cfg.uri), object_opts(cfg, true, true)))
        }
        #[cfg(feature = "azure")]
        StoreUri::Azure { container, prefix, account } => {
            let mut b = object_store::azure::MicrosoftAzureBuilder::from_env().with_container_name(container);
            if let Some(a) = account {
                b = b.with_account(a);
            }
            let s = b.build().map_err(err)?;
            Ok(ObjectHistoryStore::with_options(Arc::new(s), prefix.clone(), crate::redact_uri(&cfg.uri), object_opts(cfg, true, true)))
        }
        other => Err(StoreError::Config(format!("{other:?}: support for this store is not compiled in (cargo features s3 / gcs / azure)"))),
    }
}

/// Open the store `cfg.uri` names.
pub async fn open(cfg: &StoreConfig) -> Result<Arc<dyn HistoryStore>, StoreError> {
    crate::check_write_version(cfg.write_version)?;
    let uri = parse_uri(&cfg.uri)?;
    Ok(match uri {
        StoreUri::Memory => {
            {
            let mut m = MemoryStore::new();
            m.client_credentials = cfg.role == Role::Client;
            Arc::new(m)
        }
        }
        StoreUri::Local { path } => {
            std::fs::create_dir_all(&path).map_err(|e| StoreError::Config(format!("{}: {e}", path.display())))?;
            let shared = crate::fsdetect::detect(&path).is_network();
            let durable = !crate::fsdetect::is_temp(&path);
            Arc::new(ObjectHistoryStore::local(&path, object_opts(cfg, durable, shared))?)
        }
        StoreUri::Sqlite { path } => {
            #[cfg(feature = "sqlite")]
            {
                use crate::sqlite::{SqliteOptions, SqliteStore};
                let mut o = SqliteOptions {
                    role: cfg.role,
                    write_version: cfg.write_version,
                    allow_network_fs: cfg.allow_network_fs,
                    ..Default::default()
                };
                if let Some(h) = &cfg.holder {
                    o.holder = h.clone();
                }
                Arc::new(match path {
                    Some(p) => SqliteStore::open_with(&p, o)?,
                    None => SqliteStore::in_memory_with(o)?,
                })
            }
            #[cfg(not(feature = "sqlite"))]
            {
                let _ = path;
                return Err(StoreError::Config("sqlite support is not compiled in (cargo feature sqlite)".into()));
            }
        }
        StoreUri::Postgres { url } => {
            #[cfg(feature = "postgres")]
            {
                use crate::postgres::{PostgresConfig, PostgresStore};
                let mut pc = PostgresConfig::from_url(&url)?;
                pc.role = cfg.role;
                pc.write_version = cfg.write_version;
                if let Some(h) = &cfg.holder {
                    pc.holder = h.clone();
                }
                Arc::new(PostgresStore::connect(pc).await?)
            }
            #[cfg(not(feature = "postgres"))]
            {
                let _ = url;
                return Err(StoreError::Config("postgres support is not compiled in (cargo feature postgres)".into()));
            }
        }
        other => Arc::new(remote(&other, cfg)?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_uris() {
        assert_eq!(parse_uri("memory://").unwrap(), StoreUri::Memory);
        assert_eq!(parse_uri("/var/lib/rs").unwrap(), StoreUri::Local { path: "/var/lib/rs".into() });
        assert_eq!(parse_uri("./rs").unwrap(), StoreUri::Local { path: "./rs".into() });
        assert_eq!(parse_uri("file:///var/lib/r%20s").unwrap(), StoreUri::Local { path: "/var/lib/r s".into() });
        assert!(parse_uri("file://relative").is_err());
        assert_eq!(parse_uri("sqlite:///tmp/h.db").unwrap(), StoreUri::Sqlite { path: Some("/tmp/h.db".into()) });
        assert_eq!(parse_uri("sqlite:h.db").unwrap(), StoreUri::Sqlite { path: Some("h.db".into()) });
        assert_eq!(parse_uri("sqlite::memory:").unwrap(), StoreUri::Sqlite { path: None });
        assert_eq!(parse_uri("postgres://u:p@h/db").unwrap(), StoreUri::Postgres { url: "postgres://u:p@h/db".into() });
        assert_eq!(
            parse_uri("s3://bucket/a/b?endpoint=http://localhost:9000&region=us-east-1").unwrap(),
            StoreUri::S3 {
                bucket: "bucket".into(),
                prefix: "a/b".into(),
                endpoint: Some("http://localhost:9000".into()),
                region: Some("us-east-1".into()),
                allow_http: true
            }
        );
        assert_eq!(
            parse_uri("s3://bucket").unwrap(),
            StoreUri::S3 { bucket: "bucket".into(), prefix: "".into(), endpoint: None, region: None, allow_http: false }
        );
        assert_eq!(parse_uri("gs://b/p").unwrap(), StoreUri::Gcs { bucket: "b".into(), prefix: "p".into() });
        assert_eq!(parse_uri("az://c/p?account=acct").unwrap(), StoreUri::Azure { container: "c".into(), prefix: "p".into(), account: Some("acct".into()) });
        assert!(parse_uri("ftp://x/y").is_err());
        assert!(parse_uri("").is_err());
        assert_eq!(crate::redact_uri("postgres://u:secret@h/db"), "postgres://u:***@h/db");
    }

    #[tokio::test]
    async fn opens_backends() {
        let d = tempfile::tempdir().unwrap();
        let s = open(&StoreConfig::new(d.path().join("os").display().to_string())).await.unwrap();
        assert_eq!(s.info().backend, crate::Backend::ObjectStore);
        assert!(!s.durable(), "temp dirs are not durable");
        assert!(!s.shared(), "local disk is not shared");
        let s = open(&StoreConfig { shared: Some(true), durable: Some(true), ..StoreConfig::new(format!("file://{}", d.path().display())) }).await.unwrap();
        assert!(s.durable() && s.shared() && s.info().durable_shared);
        let s = open(&StoreConfig::new("memory://")).await.unwrap();
        assert_eq!(s.info().backend, crate::Backend::Memory);
        #[cfg(feature = "sqlite")]
        {
            let s = open(&StoreConfig::new(format!("sqlite://{}", d.path().join("h.db").display()))).await.unwrap();
            assert_eq!(s.info().backend, crate::Backend::Sqlite);
            assert!(!s.shared());
        }
        assert!(open(&StoreConfig { write_version: 9, ..StoreConfig::new("memory://") }).await.is_err());
    }
}
