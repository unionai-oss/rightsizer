//! Where inputs are read from: [`StoreResolver`] and the default resolver.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::path::Path;

use crate::ssrf::SsrfGuard;

/// Where inputs are read from: one object store per URI scheme/bucket,
/// resolved by the engine.
pub trait StoreResolver: Send + Sync {
    /// The store and in-store path for a URI, or `None` if not allowed.
    fn resolve(&self, uri: &str) -> Option<(Arc<dyn ObjectStore>, Path)>;
}

/// Which local paths (bare paths, `file://` URIs) the resolver opens.
///
/// Local inputs are read with the reading process's own credentials: a
/// server (webhook, daemon) that opened whatever path a submitter names
/// would read its own secrets' metadata on their behalf (a confused deputy,
/// SPEC §14 item 2). Servers therefore allow none unless configured.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum LocalPaths {
    /// Any existing path (in-process use: CLI, Python).
    #[default]
    Any,
    /// Only paths under these directories (after resolving symlinks and
    /// `..`); empty = none.
    Under(Vec<std::path::PathBuf>),
}

impl LocalPaths {
    /// Under `prefixes` (canonicalized when they exist).
    pub fn under(prefixes: &[std::path::PathBuf]) -> Self {
        LocalPaths::Under(prefixes.iter().map(|p| std::fs::canonicalize(p).unwrap_or_else(|_| p.clone())).collect())
    }

    /// No local path.
    pub fn none() -> Self {
        LocalPaths::Under(Vec::new())
    }

    /// Whether `path` may be opened: it must exist, and its canonical form
    /// (symlinks and `..` resolved) must lie under an allowed directory.
    pub fn allows(&self, path: &std::path::Path) -> bool {
        match self {
            LocalPaths::Any => true,
            LocalPaths::Under(v) => {
                let Ok(real) = std::fs::canonicalize(path) else { return false };
                v.iter().any(|p| real.starts_with(p))
            }
        }
    }
}

/// Default Hugging Face endpoint; `HF_ENDPOINT` overrides it.
pub const HF_ENDPOINT: &str = "https://huggingface.co";

/// A parsed `hf://org/model[@rev]` reference.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HfRef {
    /// `org/model` (or `model` for legacy un-namespaced repos).
    pub repo: String,
    /// Revision: branch, tag or commit (`main` by default).
    pub revision: String,
}

impl HfRef {
    /// Parse `hf://org/model[@rev]`; also accepts `hf://models/org/model`.
    pub fn parse(uri: &str) -> Option<HfRef> {
        let rest = uri.strip_prefix("hf://")?;
        let rest = rest.strip_prefix("models/").unwrap_or(rest);
        let (repo, revision) = match rest.split_once('@') {
            Some((r, v)) => (r, v),
            None => (rest, "main"),
        };
        let repo = repo.trim_matches('/');
        let ok = |s: &str| !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || "-_.".contains(c)) && s != "." && s != "..";
        let parts: Vec<&str> = repo.split('/').collect();
        if parts.is_empty() || parts.len() > 2 || !parts.iter().all(|p| ok(p)) {
            return None;
        }
        if !revision.chars().all(|c| c.is_ascii_alphanumeric() || "-_.".contains(c)) || revision.is_empty() || revision == "." || revision == ".." {
            return None;
        }
        Some(HfRef { repo: repo.to_string(), revision: revision.to_string() })
    }

    /// The in-store directory holding this revision's files on the hub's
    /// HTTP store: `{repo}/resolve/{revision}`.
    pub fn base_path(&self) -> Path {
        Path::from(format!("{}/resolve/{}", self.repo, self.revision).as_str())
    }
}

/// The default [`StoreResolver`]: stores built from environment
/// credentials, cached per scheme and bucket.
///
/// | URI | store |
/// |---|---|
/// | bare path, `file://` | local filesystem (the path must exist and pass [`LocalPaths`]) |
/// | `s3://bucket/key` (feature `s3`) | `AmazonS3Builder::from_env()` (`AWS_*`) |
/// | `gs://bucket/key` (feature `gcs`) | `GoogleCloudStorageBuilder::from_env()` (`GOOGLE_*`) |
/// | `az://container/key`, `abfs[s]://container@account.dfs.core.windows.net/key` (feature `azure`) | `MicrosoftAzureBuilder::from_env()` (`AZURE_*`) |
/// | `http(s)://host/path` (feature `http`) | HTTP store whose client enforces the [`SsrfGuard`]; a query string (presigned URLs) is kept for the requests |
/// | `hf://org/model[@rev]` (feature `http`) | HTTP store on `HF_ENDPOINT` (default `https://huggingface.co`), `HF_TOKEN` as a bearer token; path `{org}/{model}/resolve/{rev}` |
///
/// Anything else, or a scheme whose feature is off, resolves to `None`
/// (the input keeps what its ref knew).
#[derive(Debug, Default)]
pub struct DefaultResolver {
    guard: Arc<SsrfGuard>,
    stores: Mutex<HashMap<String, Arc<dyn ObjectStore>>>,
    local: LocalPaths,
}

impl DefaultResolver {
    /// A resolver whose HTTP clients use `guard`.
    pub fn new(guard: SsrfGuard) -> Self {
        DefaultResolver { guard: Arc::new(guard), stores: Mutex::new(HashMap::new()), local: LocalPaths::Any }
    }

    /// With a local-path policy (default [`LocalPaths::Any`]).
    pub fn with_local_paths(mut self, local: LocalPaths) -> Self {
        self.local = local;
        self
    }

    /// The local-path policy.
    pub fn local_paths(&self) -> &LocalPaths {
        &self.local
    }

    /// The guard HTTP(S) stores enforce.
    pub fn guard(&self) -> &SsrfGuard {
        &self.guard
    }

    fn cached(&self, key: &str, build: impl FnOnce() -> Option<Arc<dyn ObjectStore>>) -> Option<Arc<dyn ObjectStore>> {
        let mut m = self.stores.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(s) = m.get(key) {
            return Some(Arc::clone(s));
        }
        let s = build()?;
        m.insert(key.to_string(), Arc::clone(&s));
        Some(s)
    }

    fn local(&self, fs_path: &std::path::Path) -> Option<(Arc<dyn ObjectStore>, Path)> {
        if !self.local.allows(fs_path) {
            tracing::debug!(path = %fs_path.display(), "local input path not allowed ([features] local_paths)");
            return None;
        }
        let path = Path::from_filesystem_path(fs_path).ok()?;
        let store = self.cached("file", || Some(Arc::new(LocalFileSystem::new()) as Arc<dyn ObjectStore>))?;
        Some((store, path))
    }

    #[cfg(feature = "http")]
    fn http_store(&self, origin: &str, bearer: Option<String>) -> Option<Arc<dyn ObjectStore>> {
        // Keyed by the bearer too (a different token is another identity).
        let who = bearer.as_deref().map(|t| rightsizer_core::digest::sha256_hex(t.as_bytes())[..16].to_string()).unwrap_or_default();
        let guard = Arc::clone(&self.guard);
        self.cached(&format!("http:{origin}#{who}"), move || build_http(origin, bearer, guard))
    }
}

impl StoreResolver for DefaultResolver {
    fn resolve(&self, uri: &str) -> Option<(Arc<dyn ObjectStore>, Path)> {
        let Some((scheme, _)) = uri.split_once("://") else {
            return self.local(std::path::Path::new(uri));
        };
        match scheme {
            "file" => {
                let url = url::Url::parse(uri).ok()?;
                self.local(&url.to_file_path().ok()?)
            }
            #[cfg(feature = "s3")]
            "s3" | "s3a" => {
                let url = url::Url::parse(uri).ok()?;
                let bucket = url.host_str()?.to_string();
                let path = Path::from_url_path(url.path()).ok()?;
                let store = self.cached(&format!("s3:{bucket}"), || {
                    let s = object_store::aws::AmazonS3Builder::from_env().with_bucket_name(&bucket).build().ok()?;
                    Some(Arc::new(s) as Arc<dyn ObjectStore>)
                })?;
                Some((store, path))
            }
            #[cfg(feature = "gcs")]
            "gs" => {
                let url = url::Url::parse(uri).ok()?;
                let bucket = url.host_str()?.to_string();
                let path = Path::from_url_path(url.path()).ok()?;
                let store = self.cached(&format!("gs:{bucket}"), || {
                    let s = object_store::gcp::GoogleCloudStorageBuilder::from_env().with_bucket_name(&bucket).build().ok()?;
                    Some(Arc::new(s) as Arc<dyn ObjectStore>)
                })?;
                Some((store, path))
            }
            #[cfg(feature = "azure")]
            "az" | "azure" | "abfs" | "abfss" | "adl" => {
                let url = url::Url::parse(uri).ok()?;
                let origin = format!("{}://{}", url.scheme(), &url[url::Position::BeforeUsername..url::Position::AfterPort]);
                let path = Path::from_url_path(url.path()).ok()?;
                let store = self.cached(&format!("az:{origin}"), || {
                    let s = object_store::azure::MicrosoftAzureBuilder::from_env().with_url(origin.clone()).build().ok()?;
                    Some(Arc::new(s) as Arc<dyn ObjectStore>)
                })?;
                Some((store, path))
            }
            #[cfg(feature = "http")]
            "http" | "https" => {
                let mut url = url::Url::parse(uri).ok()?;
                url.set_fragment(None);
                if url.query().is_some() {
                    // A presigned URL: the signature lives in the query, which
                    // the HTTP store keeps only on its base URL. One store per
                    // URL (not cached: each signature is single-use in spirit).
                    let store = build_http(url.as_str(), None, Arc::clone(&self.guard))?;
                    return Some((store, Path::default()));
                }
                let origin = url[..url::Position::BeforePath].to_string();
                let path = Path::from_url_path(url.path()).ok()?;
                Some((self.http_store(&origin, None)?, path))
            }
            #[cfg(feature = "http")]
            "hf" => {
                let r = HfRef::parse(uri)?;
                let endpoint = hf_endpoint();
                let token = std::env::var("HF_TOKEN").ok().filter(|t| !t.is_empty());
                Some((self.http_store(&endpoint, token)?, r.base_path()))
            }
            _ => None,
        }
    }
}

#[cfg(feature = "http")]
fn build_http(base: &str, bearer: Option<String>, guard: Arc<SsrfGuard>) -> Option<Arc<dyn ObjectStore>> {
    let mut headers = reqwest::header::HeaderMap::new();
    if let Some(tok) = bearer
        && let Ok(v) = reqwest::header::HeaderValue::from_str(&format!("Bearer {tok}"))
    {
        headers.insert(reqwest::header::AUTHORIZATION, v);
    }
    let store = object_store::http::HttpBuilder::new()
        .with_url(base)
        .with_http_connector(crate::ssrf::client::GuardedConnector { guard, headers })
        .build()
        .ok()?;
    Some(Arc::new(store) as Arc<dyn ObjectStore>)
}

/// A short hash of the credential identity `hf://` reads use (`HF_TOKEN`),
/// `""` when anonymous: part of the metadata cache key, so one identity's
/// view of a gated repo is never served to another.
pub fn hf_identity() -> String {
    std::env::var("HF_TOKEN").ok().filter(|t| !t.is_empty()).map(|t| rightsizer_core::digest::sha256_hex(t.as_bytes())[..16].to_string()).unwrap_or_default()
}

/// The Hugging Face endpoint (`HF_ENDPOINT` or [`HF_ENDPOINT`]).
pub fn hf_endpoint() -> String {
    std::env::var("HF_ENDPOINT").ok().filter(|e| !e.is_empty()).unwrap_or_else(|| HF_ENDPOINT.to_string()).trim_end_matches('/').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hf_refs() {
        assert_eq!(HfRef::parse("hf://meta-llama/Llama-3.1-8B").unwrap().revision, "main");
        let r = HfRef::parse("hf://org/model@v1.0").unwrap();
        assert_eq!((r.repo.as_str(), r.revision.as_str()), ("org/model", "v1.0"));
        assert_eq!(r.base_path().as_ref(), "org/model/resolve/v1.0");
        assert!(HfRef::parse("hf://org/../x").is_none());
        assert!(HfRef::parse("hf://a/b/c").is_none());
        assert!(HfRef::parse("s3://a/b").is_none());
    }

    /// Refinement round (item 8): servers open no local path unless
    /// allowlisted; allowlists are directory prefixes after resolving `..`
    /// and symlinks.
    #[test]
    fn local_path_policy() {
        let dir = tempfile::tempdir().unwrap();
        let allowed = dir.path().join("data");
        let secret = dir.path().join("secret");
        std::fs::create_dir_all(&allowed).unwrap();
        std::fs::create_dir_all(&secret).unwrap();
        let ok = allowed.join("x.parquet");
        let no = secret.join("key.pem");
        std::fs::write(&ok, b"PAR1").unwrap();
        std::fs::write(&no, b"-----").unwrap();
        let any = DefaultResolver::default();
        assert!(any.resolve(no.to_str().unwrap()).is_some(), "in-process default: any path");
        let none = DefaultResolver::default().with_local_paths(LocalPaths::none());
        assert!(none.resolve(ok.to_str().unwrap()).is_none());
        assert!(none.resolve(url::Url::from_file_path(&ok).unwrap().as_str()).is_none(), "file:// too");
        let under = DefaultResolver::default().with_local_paths(LocalPaths::under(std::slice::from_ref(&allowed)));
        assert!(under.resolve(ok.to_str().unwrap()).is_some());
        assert!(under.resolve(url::Url::from_file_path(&ok).unwrap().as_str()).is_some());
        assert!(under.resolve(no.to_str().unwrap()).is_none());
        let escape = allowed.join("../secret/key.pem");
        assert!(under.resolve(escape.to_str().unwrap()).is_none(), "`..` escapes are resolved first");
        #[cfg(unix)]
        {
            let link = allowed.join("link.pem");
            std::os::unix::fs::symlink(&no, &link).unwrap();
            assert!(under.resolve(link.to_str().unwrap()).is_none(), "a symlink out of the allowlist is refused");
        }
        // A sibling directory sharing a string prefix is not under it.
        let sib = dir.path().join("data-other");
        std::fs::create_dir_all(&sib).unwrap();
        std::fs::write(sib.join("y"), b"x").unwrap();
        assert!(under.resolve(sib.join("y").to_str().unwrap()).is_none());
    }

    #[test]
    fn resolves_local_paths() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("x.bin");
        std::fs::write(&f, b"abc").unwrap();
        let r = DefaultResolver::default();
        assert!(r.resolve(f.to_str().unwrap()).is_some());
        let url = url::Url::from_file_path(&f).unwrap();
        assert!(r.resolve(url.as_str()).is_some());
        assert!(r.resolve("/definitely/not/here").is_none());
        assert!(r.resolve("ftp://x/y").is_none());
    }
}
