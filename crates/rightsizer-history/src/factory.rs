//! Store URI → backend (SPEC §8.1, §12.1 `history.uri`).
//!
//! | URI | backend |
//! |---|---|
//! | `memory://` | [`crate::MemoryStore`] (tests; not durable) |
//! | `/path`, `./path`, `file:///path` | object store on a local directory |
//! | `s3://bucket/prefix?endpoint=…&region=…&allow_http=true` | S3 or S3-compatible (MinIO, R2), feature `s3`; credentials from [`StoreOptions`], else the environment |
//! | `gs://bucket/prefix` | GCS, feature `gcs` |
//! | `az://container/prefix?account=…` (also `azure://`, `abfs://`) | Azure Blob, feature `azure` |
//! | `sqlite:///abs/path.db`, `sqlite:rel.db`, `sqlite::memory:` | sqlite, feature `sqlite` |
//! | `postgres://…`, `postgresql://…` | postgres, feature `postgres` |
//!
//! The engine maps its `[history]` config onto [`StoreConfig`] (this crate
//! does not depend on the engine).
//!
//! **Store options** (SPEC §10.3): integrations pass the storage options they
//! resolved for their own writes (Flyte's object-store settings, fsspec
//! `storage_options`) as [`StoreOptions`], mapped onto the `object_store`
//! builder of the URI's scheme. Nothing is read from or written to the
//! process environment on their behalf (the builders still start from the
//! ambient credential chain, `*Builder::from_env`, which explicit options
//! override). Values are never logged: `Debug` and `Serialize` redact every
//! value except a short allowlist of non-secret settings (endpoint, region,
//! `allow_http`, …).

use std::collections::BTreeMap;
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
    /// Object-store options (credentials, endpoint, region, …) for `s3://`,
    /// `gs://` and `az://` URIs. Redacted in `Debug` / `Serialize`.
    pub options: StoreOptions,
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
            options: StoreOptions::default(),
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
    /// With object-store options.
    pub fn options(mut self, options: StoreOptions) -> Self {
        self.options = options;
        self
    }
}

// ── store options ──────────────────────────────────────────────────────

/// Object-store options: `object_store` configuration keys (any spelling its
/// `FromStr` accepts: `aws_access_key_id` or `access_key_id`, `endpoint`,
/// `region`, `session_token`, `allow_http`, `virtual_hosted_style_request`,
/// `skip_signature`, `google_service_account`/`service_account_key`,
/// `azure_storage_account_name`/`account_name`, `account_key`, `sas_token`,
/// …) plus the common fsspec spellings (`key`, `secret`, `token`,
/// `endpoint_url`, `region_name`, `anon`). Keys are case-insensitive.
///
/// Values are secrets unless the key is on a short non-secret allowlist:
/// `Debug` and `Serialize` print `***` for them (so `rsz config show` and
/// logs never carry credentials). Read the values with [`StoreOptions::get`]
/// / [`StoreOptions::iter`].
#[derive(Clone, Default, PartialEq, Eq)]
pub struct StoreOptions(BTreeMap<String, String>);

/// Keys whose values are not secret (shown by `Debug` / `Serialize`).
const NON_SECRET: &[&str] = &[
    "endpoint",
    "endpoint_url",
    "aws_endpoint",
    "aws_endpoint_url",
    "region",
    "region_name",
    "aws_region",
    "aws_default_region",
    "default_region",
    "allow_http",
    "aws_allow_http",
    "azure_allow_http",
    "virtual_hosted_style_request",
    "aws_virtual_hosted_style_request",
    "skip_signature",
    "aws_skip_signature",
    "anon",
    "account_name",
    "azure_storage_account_name",
    "use_emulator",
    "azure_storage_use_emulator",
    "project",
];

impl StoreOptions {
    /// Empty.
    pub fn new() -> Self {
        Self::default()
    }
    /// Set `key` (lower-cased) to `value`.
    pub fn insert(&mut self, key: impl AsRef<str>, value: impl Into<String>) {
        self.0.insert(key.as_ref().trim().to_ascii_lowercase(), value.into());
    }
    /// With `key` = `value`.
    pub fn with(mut self, key: impl AsRef<str>, value: impl Into<String>) -> Self {
        self.insert(key, value);
        self
    }
    /// The value of `key`.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(&key.to_ascii_lowercase()).map(String::as_str)
    }
    /// Whether there are none.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    /// How many.
    pub fn len(&self) -> usize {
        self.0.len()
    }
    /// `(key, value)` pairs (values in clear: do not log).
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }
    /// Whether `key`'s value is shown by `Debug` / `Serialize`.
    pub fn is_secret(key: &str) -> bool {
        !NON_SECRET.contains(&key.to_ascii_lowercase().as_str())
    }
    /// Keys and values with every secret value replaced by `***`.
    pub fn redacted(&self) -> BTreeMap<String, String> {
        self.0.iter().map(|(k, v)| (k.clone(), if Self::is_secret(k) { "***".to_string() } else { v.clone() })).collect()
    }
}

impl<K: AsRef<str>, V: Into<String>> FromIterator<(K, V)> for StoreOptions {
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        let mut o = StoreOptions::new();
        for (k, v) in iter {
            o.insert(k, v);
        }
        o
    }
}

impl std::fmt::Debug for StoreOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map().entries(self.redacted()).finish()
    }
}

impl Serialize for StoreOptions {
    /// Redacted (see the type docs): a serialized config never carries secrets.
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.redacted().serialize(s)
    }
}

impl<'de> Deserialize<'de> for StoreOptions {
    /// A map of strings; booleans and numbers (TOML `allow_http = true`) are
    /// accepted and stored as their text.
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum V {
            S(String),
            B(bool),
            I(i64),
            F(f64),
        }
        let m = BTreeMap::<String, V>::deserialize(d)?;
        Ok(m.into_iter()
            .map(|(k, v)| {
                let v = match v {
                    V::S(s) => s,
                    V::B(b) => b.to_string(),
                    V::I(i) => i.to_string(),
                    V::F(f) => f.to_string(),
                };
                (k, v)
            })
            .collect())
    }
}

/// fsspec spellings → `object_store` keys, per scheme (`object_store` parses
/// its own spellings itself).
#[cfg_attr(not(any(feature = "s3", feature = "gcs", feature = "azure")), allow(dead_code))]
fn canonical_key<'a>(scheme: &str, key: &'a str) -> &'a str {
    match (scheme, key) {
        ("s3", "key") => "access_key_id",
        ("s3", "secret") => "secret_access_key",
        ("s3", "region_name") => "region",
        ("s3", "anon") => "skip_signature",
        ("gcs", "anon") => "skip_signature",
        ("azure", "anon") => "skip_signature",
        (_, k) => k,
    }
}

/// The S3 builder for `s3://bucket` with URI parameters and `opts` applied
/// (options override URI parameters, which override the environment).
/// A custom endpoint defaults to path-style requests, and an `http://`
/// endpoint to `allow_http`, unless the options say otherwise.
#[cfg(feature = "s3")]
pub fn s3_builder(
    bucket: &str,
    endpoint: Option<&str>,
    region: Option<&str>,
    allow_http: bool,
    opts: &StoreOptions,
) -> Result<object_store::aws::AmazonS3Builder, StoreError> {
    use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey, S3ConditionalPut};
    use object_store::ClientConfigKey;
    let mut b = AmazonS3Builder::from_env().with_bucket_name(bucket).with_conditional_put(S3ConditionalPut::ETagMatch);
    if let Some(e) = endpoint {
        b = b.with_endpoint(e);
    }
    if let Some(r) = region {
        b = b.with_region(r);
    }
    if allow_http {
        b = b.with_allow_http(true);
    }
    let mut set_vhost = false;
    let mut set_http = false;
    for (k, v) in opts.iter() {
        let ck = canonical_key("s3", k);
        let key: AmazonS3ConfigKey = ck.parse().map_err(|_| StoreError::Config(format!("unknown S3 store option {k:?}")))?;
        set_vhost |= matches!(key, AmazonS3ConfigKey::VirtualHostedStyleRequest);
        set_http |= matches!(key, AmazonS3ConfigKey::Client(ClientConfigKey::AllowHttp));
        b = b.with_config(key, v);
    }
    let ep = b.get_config_value(&AmazonS3ConfigKey::Endpoint);
    if let Some(ep) = ep.as_deref().filter(|e| !e.is_empty()) {
        // S3-compatible stores (MinIO, R2, …) generally want path-style requests.
        if !set_vhost {
            b = b.with_virtual_hosted_style_request(false);
        }
        if !set_http && ep.starts_with("http://") {
            b = b.with_allow_http(true);
        }
    }
    Ok(b)
}

/// The GCS builder for `gs://bucket` with `opts` applied.
#[cfg(feature = "gcs")]
pub fn gcs_builder(bucket: &str, opts: &StoreOptions) -> Result<object_store::gcp::GoogleCloudStorageBuilder, StoreError> {
    use object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
    let mut b = GoogleCloudStorageBuilder::from_env().with_bucket_name(bucket);
    for (k, v) in opts.iter() {
        let key: GoogleConfigKey = canonical_key("gcs", k).parse().map_err(|_| StoreError::Config(format!("unknown GCS store option {k:?}")))?;
        b = b.with_config(key, v);
    }
    Ok(b)
}

/// The Azure builder for `az://container` with `opts` applied.
#[cfg(feature = "azure")]
pub fn azure_builder(container: &str, account: Option<&str>, opts: &StoreOptions) -> Result<object_store::azure::MicrosoftAzureBuilder, StoreError> {
    use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
    let mut b = MicrosoftAzureBuilder::from_env().with_container_name(container);
    if let Some(a) = account {
        b = b.with_account(a);
    }
    for (k, v) in opts.iter() {
        let key: AzureConfigKey = canonical_key("azure", k).parse().map_err(|_| StoreError::Config(format!("unknown Azure store option {k:?}")))?;
        b = b.with_config(key, v);
    }
    Ok(b)
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
            let s = s3_builder(bucket, endpoint.as_deref(), region.as_deref(), *allow_http, &cfg.options)?.build().map_err(err)?;
            Ok(ObjectHistoryStore::with_options(Arc::new(s), prefix.clone(), crate::redact_uri(&cfg.uri), object_opts(cfg, true, true)))
        }
        #[cfg(feature = "gcs")]
        StoreUri::Gcs { bucket, prefix } => {
            let s = gcs_builder(bucket, &cfg.options)?.build().map_err(err)?;
            Ok(ObjectHistoryStore::with_options(Arc::new(s), prefix.clone(), crate::redact_uri(&cfg.uri), object_opts(cfg, true, true)))
        }
        #[cfg(feature = "azure")]
        StoreUri::Azure { container, prefix, account } => {
            let s = azure_builder(container, account.as_deref(), &cfg.options)?.build().map_err(err)?;
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

    #[test]
    fn store_options_are_redacted() {
        let o: StoreOptions = [("AWS_ACCESS_KEY_ID", "AKIAEXAMPLE"), ("secret_access_key", "s3cr3t"), ("endpoint", "http://minio:9000"), ("region", "eu-west-1")]
            .into_iter()
            .collect();
        assert_eq!(o.get("aws_access_key_id"), Some("AKIAEXAMPLE"), "keys are case-insensitive");
        let dbg = format!("{:?}", StoreConfig::new("s3://b/p").options(o.clone()));
        assert!(!dbg.contains("AKIAEXAMPLE") && !dbg.contains("s3cr3t"), "{dbg}");
        assert!(dbg.contains("http://minio:9000") && dbg.contains("eu-west-1"), "{dbg}");
        let json = serde_json::to_string(&o).unwrap();
        assert!(!json.contains("s3cr3t") && json.contains("\"***\""), "{json}");
        // TOML-ish values: booleans and numbers become text.
        let back: StoreOptions = serde_json::from_str(r#"{"allow_http": true, "port": 9000, "key": "k"}"#).unwrap();
        assert_eq!(back.get("allow_http"), Some("true"));
        assert_eq!(back.get("port"), Some("9000"));
        assert_eq!(back.get("key"), Some("k"));
    }

    #[tokio::test]
    async fn local_store_ignores_object_store_options() {
        let d = tempfile::tempdir().unwrap();
        let cfg = StoreConfig::new(format!("file://{}", d.path().display())).options(StoreOptions::new().with("secret_access_key", "x"));
        let s = open(&cfg).await.unwrap();
        assert_eq!(s.info().backend, crate::Backend::ObjectStore);
    }

    #[cfg(feature = "s3")]
    #[test]
    fn s3_options_map_onto_the_builder() {
        use object_store::ClientConfigKey;
        use object_store::aws::AmazonS3ConfigKey as K;
        let o: StoreOptions = [
            ("key", "AKIAEXAMPLE"),
            ("secret", "s3cr3t"),
            ("token", "sess"),
            ("endpoint_url", "http://minio:9000"),
            ("region_name", "eu-west-1"),
        ]
        .into_iter()
        .collect();
        let b = s3_builder("bucket", None, None, false, &o).unwrap();
        assert_eq!(b.get_config_value(&K::AccessKeyId).as_deref(), Some("AKIAEXAMPLE"));
        assert_eq!(b.get_config_value(&K::SecretAccessKey).as_deref(), Some("s3cr3t"));
        assert_eq!(b.get_config_value(&K::Token).as_deref(), Some("sess"));
        assert_eq!(b.get_config_value(&K::Endpoint).as_deref(), Some("http://minio:9000"));
        assert_eq!(b.get_config_value(&K::Region).as_deref(), Some("eu-west-1"));
        assert_eq!(b.get_config_value(&K::Bucket).as_deref(), Some("bucket"));
        // A custom http endpoint: path-style and plain HTTP unless said otherwise.
        assert_eq!(b.get_config_value(&K::VirtualHostedStyleRequest).as_deref(), Some("false"));
        assert_eq!(b.get_config_value(&K::Client(ClientConfigKey::AllowHttp)).as_deref(), Some("true"));
        // Explicit settings win; object_store spellings work too.
        let o = StoreOptions::new()
            .with("aws_endpoint", "https://r2.example")
            .with("virtual_hosted_style_request", "true")
            .with("aws_access_key_id", "A2")
            .with("allow_http", "false");
        let b = s3_builder("bucket", Some("http://uri-endpoint:9000"), Some("us-west-2"), true, &o).unwrap();
        assert_eq!(b.get_config_value(&K::Endpoint).as_deref(), Some("https://r2.example"), "options override URI parameters");
        assert_eq!(b.get_config_value(&K::Region).as_deref(), Some("us-west-2"));
        assert_eq!(b.get_config_value(&K::VirtualHostedStyleRequest).as_deref(), Some("true"));
        assert_eq!(b.get_config_value(&K::Client(ClientConfigKey::AllowHttp)).as_deref(), Some("false"));
        assert_eq!(b.get_config_value(&K::AccessKeyId).as_deref(), Some("A2"));
        // Unknown keys are configuration errors naming the key, never the value.
        let e = s3_builder("bucket", None, None, false, &StoreOptions::new().with("nope", "v4lue")).unwrap_err().to_string();
        assert!(e.contains("nope") && !e.contains("v4lue"), "{e}");
    }

    #[cfg(feature = "gcs")]
    #[test]
    fn gcs_options_map_onto_the_builder() {
        use object_store::gcp::GoogleConfigKey as K;
        let b = gcs_builder("bkt", &StoreOptions::new().with("service_account", "/var/run/sa.json")).unwrap();
        assert_eq!(b.get_config_value(&K::ServiceAccount).as_deref(), Some("/var/run/sa.json"));
    }

    #[cfg(feature = "azure")]
    #[test]
    fn azure_options_map_onto_the_builder() {
        use object_store::azure::AzureConfigKey as K;
        let b = azure_builder("ctr", None, &StoreOptions::new().with("account_name", "acct").with("account_key", "a2V5")).unwrap();
        assert_eq!(b.get_config_value(&K::AccountName).as_deref(), Some("acct"));
        assert_eq!(b.get_config_value(&K::AccessKey).as_deref(), Some("a2V5"));
    }
}
