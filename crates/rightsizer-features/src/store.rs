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
/// | bare path, `file://` | local filesystem (the path must exist) |
/// | `s3://bucket/key` (feature `s3`) | `AmazonS3Builder::from_env()` (`AWS_*`) |
/// | `gs://bucket/key` (feature `gcs`) | `GoogleCloudStorageBuilder::from_env()` (`GOOGLE_*`) |
/// | `az://container/key`, `abfs[s]://container@account.dfs.core.windows.net/key` (feature `azure`) | `MicrosoftAzureBuilder::from_env()` (`AZURE_*`) |
/// | `http(s)://host/path` (feature `http`) | HTTP store whose client enforces the [`SsrfGuard`] |
/// | `hf://org/model[@rev]` (feature `http`) | HTTP store on `HF_ENDPOINT` (default `https://huggingface.co`), `HF_TOKEN` as a bearer token; path `{org}/{model}/resolve/{rev}` |
///
/// Anything else, or a scheme whose feature is off, resolves to `None`
/// (the input keeps what its ref knew).
#[derive(Debug, Default)]
pub struct DefaultResolver {
    guard: Arc<SsrfGuard>,
    stores: Mutex<HashMap<String, Arc<dyn ObjectStore>>>,
}

impl DefaultResolver {
    /// A resolver whose HTTP clients use `guard`.
    pub fn new(guard: SsrfGuard) -> Self {
        DefaultResolver { guard: Arc::new(guard), stores: Mutex::new(HashMap::new()) }
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
        let path = Path::from_filesystem_path(fs_path).ok()?;
        let store = self.cached("file", || Some(Arc::new(LocalFileSystem::new()) as Arc<dyn ObjectStore>))?;
        Some((store, path))
    }

    #[cfg(feature = "http")]
    fn http_store(&self, origin: &str, bearer: Option<String>) -> Option<Arc<dyn ObjectStore>> {
        let guard = Arc::clone(&self.guard);
        self.cached(&format!("http:{origin}"), move || {
            let mut headers = reqwest::header::HeaderMap::new();
            if let Some(tok) = bearer
                && let Ok(v) = reqwest::header::HeaderValue::from_str(&format!("Bearer {tok}"))
            {
                headers.insert(reqwest::header::AUTHORIZATION, v);
            }
            let store = object_store::http::HttpBuilder::new()
                .with_url(origin)
                .with_http_connector(crate::ssrf::client::GuardedConnector { guard, headers })
                .build()
                .ok()?;
            Some(Arc::new(store) as Arc<dyn ObjectStore>)
        })
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
                let url = url::Url::parse(uri).ok()?;
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
