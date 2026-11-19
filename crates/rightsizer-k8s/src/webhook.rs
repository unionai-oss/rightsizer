//! The mutating admission webhook server (kubernetes.md §3), feature
//! `webhook`: axum over rustls with certificate hot reload (cert-manager
//! rotates the Secret; the kubelet updates the mounted files; we poll their
//! mtimes), signing-key hot reload (`rsz webhook rotate-key`), and the
//! routes:
//!
//! | route | |
//! |---|---|
//! | `POST /mutate/jobs`, `POST /mutate/pods`, `POST /mutate` | AdmissionReview v1 ([`crate::admit::Admitter`]) |
//! | `GET /healthz` | process alive |
//! | `GET /readyz` | certificate loaded and the engine configured |
//! | `GET /metrics` | Prometheus (SPEC §13.1 + `rightsizer_webhook_*`) |
//!
//! Extra routes (the watcher's) are merged by the caller.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use serde_json::Value;

use crate::admit::Admitter;
use crate::manifests::{JOBS_PATH, JOBS_WEBHOOK, PODS_PATH, PODS_WEBHOOK};

/// TLS server configuration type.
pub type TlsConfig = rustls::ServerConfig;

/// Certificate and key files (PEM).
#[derive(Clone, Debug)]
pub struct TlsFiles {
    /// Certificate chain.
    pub cert: PathBuf,
    /// Private key.
    pub key: PathBuf,
}

fn load_certified(files: &TlsFiles) -> Result<CertifiedKey, String> {
    let certs: Vec<_> = rustls_pemfile::certs(&mut std::io::BufReader::new(std::fs::File::open(&files.cert).map_err(|e| format!("{}: {e}", files.cert.display()))?))
        .collect::<Result<_, _>>()
        .map_err(|e| format!("{}: {e}", files.cert.display()))?;
    if certs.is_empty() {
        return Err(format!("{}: no certificate", files.cert.display()));
    }
    let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(std::fs::File::open(&files.key).map_err(|e| format!("{}: {e}", files.key.display()))?))
        .map_err(|e| format!("{}: {e}", files.key.display()))?
        .ok_or_else(|| format!("{}: no private key", files.key.display()))?;
    let signer = rustls::crypto::ring::sign::any_supported_type(&key).map_err(|e| format!("{}: {e}", files.key.display()))?;
    Ok(CertifiedKey::new(certs, signer))
}

/// A certificate resolver whose certificate is swapped on file change.
#[derive(Debug)]
pub struct ReloadingCert {
    files: TlsFiles,
    current: RwLock<Option<Arc<CertifiedKey>>>,
    stamp: RwLock<Option<(SystemTime, SystemTime)>>,
}

fn mtime(p: &Path) -> Option<SystemTime> {
    std::fs::metadata(p).and_then(|m| m.modified()).ok()
}

impl ReloadingCert {
    /// Load `files` (an error if they cannot be read).
    pub fn new(files: TlsFiles) -> Result<Arc<Self>, String> {
        let r = Arc::new(ReloadingCert { files, current: RwLock::new(None), stamp: RwLock::new(None) });
        r.reload()?;
        Ok(r)
    }

    /// Reload when either file changed; `Ok(true)` when reloaded. A broken
    /// new pair keeps the old certificate.
    pub fn reload(&self) -> Result<bool, String> {
        let stamp = (mtime(&self.files.cert).unwrap_or(SystemTime::UNIX_EPOCH), mtime(&self.files.key).unwrap_or(SystemTime::UNIX_EPOCH));
        if self.stamp.read().map(|s| *s == Some(stamp)).unwrap_or(false) {
            return Ok(false);
        }
        let ck = load_certified(&self.files)?;
        if let Ok(mut c) = self.current.write() {
            *c = Some(Arc::new(ck));
        }
        if let Ok(mut s) = self.stamp.write() {
            *s = Some(stamp);
        }
        Ok(true)
    }

    /// Whether a certificate is loaded.
    pub fn loaded(&self) -> bool {
        self.current.read().map(|c| c.is_some()).unwrap_or(false)
    }

    /// Poll the files every `every`.
    pub fn spawn_watch(self: &Arc<Self>, every: Duration) {
        let me = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(every).await;
                match me.reload() {
                    Ok(true) => tracing::info!(cert = %me.files.cert.display(), "TLS certificate reloaded"),
                    Ok(false) => {}
                    Err(e) => tracing::warn!(error = %e, "TLS certificate reload failed; keeping the previous one"),
                }
            }
        });
    }
}

impl ResolvesServerCert for ReloadingCert {
    fn resolve(&self, _hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        self.current.read().ok().and_then(|c| c.clone())
    }
}

/// The rustls config over a resolver (TLS 1.2/1.3, h2 + http/1.1).
pub fn tls_config(resolver: Arc<ReloadingCert>) -> Result<Arc<TlsConfig>, String> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut c = rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?
        .with_no_client_auth()
        .with_cert_resolver(resolver);
    c.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(Arc::new(c))
}

/// Shared server state.
#[derive(Clone)]
pub struct AppState {
    /// The handler.
    pub admitter: Arc<Admitter>,
    /// Ready flag (cert loaded etc.).
    pub ready: Arc<AtomicBool>,
}

async fn mutate(State(s): State<AppState>, name: &'static str, body: axum::body::Bytes) -> impl IntoResponse {
    let review: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            // Still admit: an AdmissionReview we cannot read gets an empty allowed response.
            tracing::warn!(error = %e, "unreadable AdmissionReview");
            s.admitter.metrics().fail_open.with_label_values(&[name, "parse"]).inc();
            serde_json::json!({})
        }
    };
    axum::Json(s.admitter.review(name, &review).await)
}

/// The webhook router (without TLS).
pub fn router(state: AppState) -> Router {
    Router::new()
        .route(JOBS_PATH, post(|s: State<AppState>, b: axum::body::Bytes| mutate(s, JOBS_WEBHOOK, b)))
        .route(PODS_PATH, post(|s: State<AppState>, b: axum::body::Bytes| mutate(s, PODS_WEBHOOK, b)))
        .route("/mutate", post(|s: State<AppState>, b: axum::body::Bytes| mutate(s, "mutate", b)))
        .route("/healthz", get(|| async { "ok" }))
        .route(
            "/readyz",
            get(|State(s): State<AppState>| async move {
                let ok = s.ready.load(Ordering::Relaxed) && s.admitter.engine().config_error().is_none();
                if ok { (StatusCode::OK, "ready") } else { (StatusCode::SERVICE_UNAVAILABLE, "not ready") }
            }),
        )
        .route("/metrics", get(|State(s): State<AppState>| async move { ([("content-type", "text/plain; version=0.0.4")], s.admitter.engine().metrics().render()) }))
        .with_state(state)
}

/// Serve `app` over TLS on `addr` until the process exits. Each connection
/// is handled on its own task (a slow handshake never blocks others).
pub async fn serve_tls(addr: SocketAddr, app: Router, tls: Arc<TlsConfig>) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "webhook listening (https)");
    let acceptor = tokio_rustls::TlsAcceptor::from(tls);
    loop {
        let (tcp, peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!(error = %e, "accept failed");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let _ = tcp.set_nodelay(true);
        let acceptor = acceptor.clone();
        let app = app.clone();
        tokio::spawn(async move {
            let stream = match tokio::time::timeout(Duration::from_secs(10), acceptor.accept(tcp)).await {
                Ok(Ok(s)) => s,
                Ok(Err(e)) => {
                    tracing::debug!(%peer, error = %e, "TLS handshake failed");
                    return;
                }
                Err(_) => return,
            };
            let svc = hyper_util::service::TowerToHyperService::new(app);
            let _ = hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new())
                .serve_connection(hyper_util::rt::TokioIo::new(stream), svc)
                .await;
        });
    }
}

/// Serve `app` over plain HTTP (metrics/health for probes, node agent, tests).
pub async fn serve_http(addr: SocketAddr, app: Router) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "listening (http)");
    axum::serve(listener, app).await
}

/// Reload signing keys from `path` on change, swapping the admitter's adapter.
pub fn spawn_key_reload(admitter: Arc<Admitter>, path: PathBuf, every: Duration) {
    tokio::spawn(async move {
        let mut last = mtime(&path);
        loop {
            tokio::time::sleep(every).await;
            let now = mtime(&path);
            if now == last {
                continue;
            }
            match crate::runtime::load_keys(&path) {
                Ok(k) => {
                    let kids: Vec<_> = k.keys.keys().cloned().collect();
                    let a = admitter.adapter().as_ref().clone().with_keys(k);
                    admitter.set_adapter(a);
                    tracing::info!(?kids, "signing keys reloaded");
                    last = now;
                }
                Err(e) => tracing::warn!(error = %e, "signing key reload failed; keeping the previous keys"),
            }
        }
    });
}

/// A self-signed CA and a serving certificate for `dns` names (the
/// bootstrap path without cert-manager, and the host-served dev loop).
/// Returns PEM `(ca_cert, server_cert, server_key)`; valid 10 years.
pub fn self_signed(dns: &[String]) -> Result<(String, String, String), String> {
    use rcgen::{BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose};
    let e = |e: rcgen::Error| e.to_string();
    let now = rcgen::date_time_ymd(2026, 1, 1);
    let mut ca = CertificateParams::new(Vec::<String>::new()).map_err(e)?;
    ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign, KeyUsagePurpose::DigitalSignature];
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, "rightsizer-webhook-ca");
    ca.distinguished_name = dn;
    ca.not_before = now;
    ca.not_after = rcgen::date_time_ymd(2036, 1, 1);
    let ca_key = KeyPair::generate().map_err(e)?;
    let ca_cert = ca.self_signed(&ca_key).map_err(e)?;
    let issuer = Issuer::new(ca, ca_key);
    let mut leaf = CertificateParams::new(dns.to_vec()).map_err(e)?;
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, dns.first().cloned().unwrap_or_else(|| "rightsizer-webhook".into()));
    leaf.distinguished_name = dn;
    leaf.key_usages = vec![KeyUsagePurpose::DigitalSignature, KeyUsagePurpose::KeyEncipherment];
    leaf.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    // Strict verifiers (Python ≥ 3.13, Go) want the AKI.
    leaf.use_authority_key_identifier_extension = true;
    leaf.not_before = now;
    leaf.not_after = rcgen::date_time_ymd(2036, 1, 1);
    let key = KeyPair::generate().map_err(e)?;
    let cert = leaf.signed_by(&key, &issuer).map_err(e)?;
    Ok((ca_cert.pem(), cert.pem(), key.serialize_pem()))
}
