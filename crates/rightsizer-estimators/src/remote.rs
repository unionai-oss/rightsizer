//! Estimator Protocol v1 client (SPEC §6.3), feature `remote`.
//!
//! ```text
//! GET  /v1/info             -> { protocol: "estimator/1", id, version, grids, axes, kind,
//!                                needs: {code, history, input_bytes}, features, novelty: {needs_code} | null, max_batch }
//! POST /v1/estimate         { context, axes }          -> { estimate }
//! POST /v1/estimate:batch   { contexts, axes }         -> { results: [ {estimate} | {error} ] }
//! GET  /v1/health           -> 200 | 503
//! ```
//!
//! Every call goes through, in order:
//!
//! 1. **Privacy** ([`crate::privacy::redact_for_remote`]): the context is
//!    redacted before anything else, so cache keys and requests never carry
//!    what may not leave the process (SPEC §11). Contexts go only to the
//!    configured URL.
//! 2. **Exact cache** (24 h): key = [`crate::memo::exact_key`] of the
//!    redacted context (history replaced by its watermark).
//! 3. **Memo** (optional, shared with the engine): the normalized estimate
//!    under (workload key, unit, code digest, feature bucket, policy hash).
//! 4. **Single-flight**: concurrent identical requests (same exact key) share
//!    one in-flight call; each caller still waits only for its own deadline.
//! 5. **Retry**: one jittered retry (50–250 ms) on a `retryable` error when
//!    the deadline leaves room.
//! 6. **Coalescing**: distinct requests arriving within 20 ms go out as one
//!    `POST /v1/estimate:batch` of at most `max_batch` (from `/v1/info`,
//!    capped by config); a lone request uses `POST /v1/estimate`. Without a
//!    `max_batch > 1` there is no coalescing.
//! 7. **Circuit breaker**: opens after 5 consecutive failed HTTP calls,
//!    half-opens after 30 s (one trial call); while open, calls fail fast with
//!    `Unavailable`. Counted failures: transport errors, timeouts, `429`,
//!    `5xx` and unexpected statuses — not `400`, `409` or `422`, which are
//!    deliberate answers.
//!
//! **Deadline.** 10 s by default; a caller budget
//! ([`Estimator::estimate_within`]) replaces it.
//!
//! **Errors.** Bodies are `{error: {code, message, retryable}}`:
//!
//! | status | [`EstimateError`] | retried |
//! |---|---|---|
//! | 400 | `Protocol` | no |
//! | 409 | `Unsupported` | no |
//! | 422 | `Insufficient` (the chain passes on, not a failure) | no |
//! | 429, 503 | `Unavailable` | unless the body says `retryable: false` |
//! | other | `Failed { retryable }` | if the body says so |
//! | transport error | `Failed { retryable: true }` | yes |
//! | deadline | `Timeout` | no |
//!
//! Per-item batch errors map by `code`: `"400"`/`"malformed"`,
//! `"409"`/`"unsupported"`, `"422"`/`"insufficient"`,
//! `"429"`/`"503"`/`"rate_limited"`/`"unavailable"`, else `Failed`.
//!
//! **Auth.** A bearer token from an env var or a file (re-read per call, so
//! rotation works), and/or mTLS with a client certificate and key (PEM).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use async_trait::async_trait;
use futures::FutureExt;
use futures::future::{BoxFuture, Shared};
use rightsizer_core::grid::RS_GRID_V1;
use rightsizer_core::stats::SplitMix64;
use rightsizer_core::{Estimate, EstimateKind, WorkloadContext};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::build::MemberSpec;
use crate::memo::{Memo, MemoKey, TtlCache, exact_key};
use crate::privacy::{RemotePrivacy, redact_for_remote};
use crate::{EstimateAxis, EstimateError, Estimator, EstimatorInfo, Needs};

/// Protocol tag in `/v1/info`.
pub const PROTOCOL: &str = "estimator/1";

/// `POST /v1/estimate` body.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EstimateRequest {
    /// The (privacy-filtered) context.
    pub context: WorkloadContext,
    /// Axes requested.
    pub axes: Vec<EstimateAxis>,
}

/// `POST /v1/estimate` response.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EstimateResponse {
    /// The estimate.
    pub estimate: Estimate,
}

/// `POST /v1/estimate:batch` body.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BatchRequest {
    /// Contexts.
    pub contexts: Vec<WorkloadContext>,
    /// Axes requested.
    pub axes: Vec<EstimateAxis>,
}

/// One batch result.
#[allow(clippy::large_enum_variant)] // a transient wire type
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BatchItem {
    /// An estimate.
    Estimate {
        /// The estimate.
        estimate: Estimate,
    },
    /// An error.
    Error {
        /// The error.
        error: ErrorDetail,
    },
}

/// `POST /v1/estimate:batch` response.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BatchResponse {
    /// Results, in request order.
    pub results: Vec<BatchItem>,
}

/// `GET /v1/info` response.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InfoResponse {
    /// `"estimator/1"`.
    pub protocol: String,
    /// The estimator's facts.
    #[serde(flatten)]
    pub info: EstimatorInfo,
}

/// Every protocol error body: `{error: {code, message, retryable}}`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ErrorBody {
    /// The error.
    pub error: ErrorDetail,
}

/// Error detail.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ErrorDetail {
    /// Machine-readable code.
    pub code: String,
    /// Message.
    pub message: String,
    /// Whether a retry may succeed.
    #[serde(default)]
    pub retryable: bool,
}

/// Where the bearer token comes from.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum TokenSource {
    /// No token.
    #[default]
    None,
    /// An environment variable.
    Env(String),
    /// A file (trimmed).
    File(PathBuf),
    /// A literal (tests, embedding).
    Literal(String),
}

impl TokenSource {
    fn resolve(&self) -> Result<Option<String>, String> {
        match self {
            TokenSource::None => Ok(None),
            TokenSource::Literal(t) => Ok(Some(t.clone())),
            TokenSource::Env(v) => std::env::var(v).map(Some).map_err(|_| format!("bearer token env var {v} is not set")),
            TokenSource::File(p) => std::fs::read_to_string(p)
                .map(|s| Some(s.trim().to_string()))
                .map_err(|e| format!("reading bearer token {}: {e}", p.display())),
        }
    }
}

/// Client settings.
#[derive(Clone)]
pub struct RemoteConfig {
    /// Member name (placeholder id when `/v1/info` is unreachable).
    pub name: String,
    /// Base URL (`https://host` or `https://host/v1`).
    pub url: String,
    /// Default per-call deadline (10 s); a caller budget replaces it.
    pub timeout: Duration,
    /// Bearer token.
    pub token: TokenSource,
    /// mTLS client certificate (PEM).
    pub client_cert: Option<PathBuf>,
    /// mTLS client key (PEM).
    pub client_key: Option<PathBuf>,
    /// Extra CA certificate (PEM).
    pub ca_cert: Option<PathBuf>,
    /// Cap on the server's `max_batch`.
    pub max_batch: Option<u32>,
    /// Coalescing window (20 ms).
    pub coalesce_window: Duration,
    /// Consecutive failures that open the breaker (5).
    pub breaker_threshold: u32,
    /// Open → half-open delay (30 s).
    pub breaker_cooldown: Duration,
    /// Exact-cache TTL (24 h).
    pub cache_ttl: Duration,
    /// Exact-cache capacity.
    pub cache_capacity: usize,
    /// Retry jitter range.
    pub retry_jitter: (Duration, Duration),
    /// What the server may see.
    pub privacy: RemotePrivacy,
    /// Axes requested (default: the server's).
    pub axes: Option<Vec<EstimateAxis>>,
    /// Shared memo layer.
    pub memo: Option<Arc<Memo>>,
    /// Policy hash for memo keys.
    pub policy_hash: String,
    /// A preconfigured HTTP client (overrides the TLS settings).
    pub client: Option<reqwest::Client>,
}

impl RemoteConfig {
    /// Defaults for `url`.
    pub fn new(url: impl Into<String>) -> Self {
        RemoteConfig {
            name: "remote".into(),
            url: url.into(),
            timeout: Duration::from_secs(10),
            token: TokenSource::None,
            client_cert: None,
            client_key: None,
            ca_cert: None,
            max_batch: None,
            coalesce_window: Duration::from_millis(20),
            breaker_threshold: 5,
            breaker_cooldown: Duration::from_secs(30),
            cache_ttl: Duration::from_secs(24 * 3600),
            cache_capacity: 10_000,
            retry_jitter: (Duration::from_millis(50), Duration::from_millis(250)),
            privacy: RemotePrivacy::default(),
            axes: None,
            memo: None,
            policy_hash: String::new(),
            client: None,
        }
    }

    /// From an `[estimator.members.NAME]` table.
    pub fn from_member(name: &str, m: Option<&MemberSpec>, privacy: RemotePrivacy) -> Result<Self, String> {
        let m = m.ok_or("remote member needs a table with `url`")?;
        let url = m.url.clone().ok_or("remote member needs `url`")?;
        let mut c = RemoteConfig::new(url);
        c.name = name.into();
        c.privacy = privacy;
        if let Some(ms) = m.timeout_ms {
            c.timeout = Duration::from_millis(ms);
        }
        c.token = match (&m.token_env, &m.token_file) {
            (Some(v), _) => TokenSource::Env(v.clone()),
            (None, Some(f)) => TokenSource::File(f.clone()),
            (None, None) => TokenSource::None,
        };
        c.client_cert = m.client_cert.clone();
        c.client_key = m.client_key.clone();
        c.ca_cert = m.ca_cert.clone();
        c.max_batch = m.max_batch;
        if let Some(ms) = m.coalesce_ms {
            c.coalesce_window = Duration::from_millis(ms);
        }
        if c.client_cert.is_some() != c.client_key.is_some() {
            return Err("mTLS needs both `client_cert` and `client_key`".into());
        }
        Ok(c)
    }

    fn http_client(&self) -> Result<reqwest::Client, String> {
        if let Some(c) = &self.client {
            return Ok(c.clone());
        }
        let mut b = reqwest::Client::builder();
        if let Some(ca) = &self.ca_cert {
            let pem = std::fs::read(ca).map_err(|e| format!("reading {}: {e}", ca.display()))?;
            b = b.add_root_certificate(reqwest::Certificate::from_pem(&pem).map_err(|e| format!("CA certificate: {e}"))?);
        }
        if let (Some(cert), Some(key)) = (&self.client_cert, &self.client_key) {
            let mut pem = std::fs::read(cert).map_err(|e| format!("reading {}: {e}", cert.display()))?;
            pem.push(b'\n');
            pem.extend(std::fs::read(key).map_err(|e| format!("reading {}: {e}", key.display()))?);
            b = b.identity(reqwest::Identity::from_pem(&pem).map_err(|e| format!("client identity: {e}"))?);
        }
        b.build().map_err(|e| format!("building HTTP client: {e}"))
    }
}

/// Breaker state.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum BreakerState {
    /// Calls flow; `failures` consecutive failures so far.
    Closed {
        /// Consecutive failures.
        failures: u32,
    },
    /// Calls fail fast until `until`.
    Open {
        /// When the breaker half-opens.
        until: Instant,
    },
    /// One trial call is allowed; `trial` while it is in flight.
    HalfOpen {
        /// A trial is in flight.
        trial: bool,
    },
}

/// A consecutive-failure circuit breaker (SPEC §6.3).
#[derive(Debug)]
pub struct CircuitBreaker {
    state: Mutex<BreakerState>,
    threshold: u32,
    cooldown: Duration,
}

impl CircuitBreaker {
    /// Opens after `threshold` consecutive failures for `cooldown`.
    pub fn new(threshold: u32, cooldown: Duration) -> Self {
        CircuitBreaker { state: Mutex::new(BreakerState::Closed { failures: 0 }), threshold: threshold.max(1), cooldown }
    }

    /// Current state.
    pub fn state(&self) -> BreakerState {
        *self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Whether a call may go out at `now` (claims the half-open trial).
    pub fn admit(&self, now: Instant) -> bool {
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        match *s {
            BreakerState::Closed { .. } => true,
            BreakerState::Open { until } if now >= until => {
                *s = BreakerState::HalfOpen { trial: true };
                true
            }
            BreakerState::Open { .. } => false,
            BreakerState::HalfOpen { trial: false } => {
                *s = BreakerState::HalfOpen { trial: true };
                true
            }
            BreakerState::HalfOpen { trial: true } => false,
        }
    }

    /// Record a call's outcome. `None` = neither (a deliberate error answer):
    /// releases a half-open trial without changing the count.
    pub fn record(&self, ok: Option<bool>, now: Instant) {
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        *s = match (*s, ok) {
            (_, Some(true)) => BreakerState::Closed { failures: 0 },
            (BreakerState::Closed { failures }, Some(false)) if failures + 1 < self.threshold => {
                BreakerState::Closed { failures: failures + 1 }
            }
            (_, Some(false)) => BreakerState::Open { until: now + self.cooldown },
            (BreakerState::HalfOpen { .. }, None) => BreakerState::HalfOpen { trial: false },
            (st, None) => st,
        };
    }
}

/// An error with its retry and breaker classification.
#[derive(Clone, Debug)]
struct CallError {
    err: EstimateError,
    retryable: bool,
    /// Counts as a breaker failure.
    counted: bool,
}

impl CallError {
    fn new(err: EstimateError, retryable: bool, counted: bool) -> Self {
        CallError { err, retryable, counted }
    }
}

/// Map an HTTP status and optional error body to an error (SPEC §6.3).
fn map_status(status: u16, body: Option<ErrorDetail>) -> CallError {
    let msg = body.as_ref().map(|b| format!("{}: {}", b.code, b.message)).unwrap_or_else(|| format!("HTTP {status}"));
    let flag = body.as_ref().map(|b| b.retryable);
    match status {
        400 => CallError::new(EstimateError::Protocol(msg), false, false),
        409 => CallError::new(EstimateError::Unsupported(msg), false, false),
        422 => CallError::new(EstimateError::Insufficient(msg), false, false),
        429 | 503 => CallError::new(EstimateError::Unavailable(msg), flag.unwrap_or(true), true),
        _ => {
            let r = flag.unwrap_or(false);
            CallError::new(EstimateError::Failed { message: msg, retryable: r }, r, true)
        }
    }
}

/// Map a per-item batch error by its `code`.
fn map_item(e: ErrorDetail) -> CallError {
    let status = match e.code.as_str() {
        "400" | "malformed" | "bad_request" => 400,
        "409" | "unsupported" => 409,
        "422" | "insufficient" => 422,
        "429" | "rate_limited" => 429,
        "503" | "unavailable" => 503,
        _ => 500,
    };
    let mut c = map_status(status, Some(e));
    c.counted = false;
    c
}

struct Job {
    ctx: WorkloadContext,
    deadline: Instant,
    tx: oneshot::Sender<Result<Estimate, CallError>>,
}

type SharedCall = Shared<BoxFuture<'static, Result<Estimate, EstimateError>>>;

struct Inner {
    cfg: RemoteConfig,
    info: EstimatorInfo,
    http: reqwest::Client,
    breaker: CircuitBreaker,
    exact: TtlCache<String, Estimate>,
    inflight: Mutex<HashMap<String, SharedCall>>,
    coalescer: OnceLock<mpsc::UnboundedSender<Job>>,
    max_batch: usize,
    axes: Vec<EstimateAxis>,
    http_calls: AtomicU64,
    seed: AtomicU64,
}

/// An Estimator Protocol v1 client.
#[derive(Clone)]
pub struct RemoteEstimator {
    inner: Arc<Inner>,
}

fn placeholder_info(name: &str) -> EstimatorInfo {
    EstimatorInfo {
        id: name.into(),
        version: "unknown".into(),
        grids: vec![RS_GRID_V1.into()],
        axes: vec![EstimateAxis::Memory, EstimateAxis::CpuMean],
        kind: EstimateKind::Demand,
        needs: Needs { code: true, history: true, input_bytes: true },
        features: rightsizer_features::FEATURES_VERSION.into(),
        novelty: None,
        max_batch: None,
    }
}

fn endpoint(base: &str, path: &str) -> String {
    let b = base.trim_end_matches('/');
    if b.ends_with("/v1") { format!("{b}/{path}") } else { format!("{b}/v1/{path}") }
}

impl RemoteEstimator {
    /// Connect: fetch `/v1/info` (within the default deadline). When the
    /// server is unreachable the client is still built, with a placeholder
    /// info (id = member name, version `"unknown"`), so a chain never fails
    /// to build because a server is down (S1). Errors only for unusable
    /// settings (bad certificates).
    pub async fn connect(cfg: RemoteConfig) -> Result<Self, String> {
        let http = cfg.http_client()?;
        let url = endpoint(&cfg.url, "info");
        let mut req = http.get(&url).timeout(cfg.timeout);
        if let Ok(Some(t)) = cfg.token.resolve() {
            req = req.bearer_auth(t);
        }
        let info = match req.send().await {
            Ok(r) if r.status().is_success() => match r.json::<InfoResponse>().await {
                Ok(i) => {
                    if i.protocol != PROTOCOL {
                        tracing::warn!(url = %url, protocol = %i.protocol, "remote estimator speaks another protocol");
                    }
                    i.info
                }
                Err(e) => {
                    tracing::warn!(url = %url, error = %e, "bad /v1/info body; using a placeholder");
                    placeholder_info(&cfg.name)
                }
            },
            Ok(r) => {
                tracing::warn!(url = %url, status = %r.status(), "/v1/info failed; using a placeholder");
                placeholder_info(&cfg.name)
            }
            Err(e) => {
                tracing::warn!(url = %url, error = %e, "/v1/info unreachable; using a placeholder");
                placeholder_info(&cfg.name)
            }
        };
        Ok(Self::with_http(cfg, info, http))
    }

    /// Build with known info (no network).
    pub fn with_info(cfg: RemoteConfig, info: EstimatorInfo) -> Result<Self, String> {
        let http = cfg.http_client()?;
        Ok(Self::with_http(cfg, info, http))
    }

    fn with_http(cfg: RemoteConfig, info: EstimatorInfo, http: reqwest::Client) -> Self {
        let server = info.max_batch.unwrap_or(1).max(1);
        let max_batch = cfg.max_batch.map(|c| c.max(1).min(server)).unwrap_or(server) as usize;
        let axes = cfg.axes.clone().unwrap_or_else(|| info.axes.clone());
        let seed = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(7);
        RemoteEstimator {
            inner: Arc::new(Inner {
                breaker: CircuitBreaker::new(cfg.breaker_threshold, cfg.breaker_cooldown),
                exact: TtlCache::new(cfg.cache_ttl, cfg.cache_capacity),
                inflight: Mutex::new(HashMap::new()),
                coalescer: OnceLock::new(),
                max_batch,
                axes,
                http_calls: AtomicU64::new(0),
                seed: AtomicU64::new(seed),
                cfg,
                info,
                http,
            }),
        }
    }

    /// `GET /v1/health`: true on 200.
    pub async fn health(&self) -> bool {
        let i = &self.inner;
        let mut req = i.http.get(endpoint(&i.cfg.url, "health")).timeout(i.cfg.timeout);
        if let Ok(Some(t)) = i.cfg.token.resolve() {
            req = req.bearer_auth(t);
        }
        matches!(req.send().await, Ok(r) if r.status().as_u16() == 200)
    }

    /// The breaker's state.
    pub fn breaker_state(&self) -> BreakerState {
        self.inner.breaker.state()
    }

    /// HTTP estimate calls made so far (single and batch).
    pub fn http_calls(&self) -> u64 {
        self.inner.http_calls.load(Ordering::Relaxed)
    }

    /// The effective batch size.
    pub fn max_batch(&self) -> usize {
        self.inner.max_batch
    }

    async fn run(&self, ctx: &WorkloadContext, budget: Option<Duration>) -> Result<Estimate, EstimateError> {
        let i = &self.inner;
        let budget = budget.unwrap_or(i.cfg.timeout);
        let deadline = Instant::now() + budget;
        let timeout_err = || EstimateError::Timeout(budget.as_millis().min(u64::MAX as u128) as u64);
        let rctx = redact_for_remote(ctx, &i.cfg.privacy);
        let grid = i.info.grids.first().map(String::as_str).unwrap_or(RS_GRID_V1);
        let key = exact_key(&i.info.id, &i.info.version, grid, &i.axes, &i.info.features, &rctx);
        if let Some(e) = i.exact.get(&key) {
            return Ok(e);
        }
        let memo_key = i.cfg.memo.as_ref().map(|m| (m.clone(), MemoKey::new(ctx, m.grid(), i.cfg.policy_hash.clone())));
        if let Some((m, k)) = &memo_key
            && let Some(e) = m.get(k)
        {
            return Ok(e);
        }
        let shared = {
            let mut map = i.inflight.lock().unwrap_or_else(|p| p.into_inner());
            match map.get(&key) {
                Some(s) => s.clone(),
                None => {
                    let this = self.clone();
                    let k = key.clone();
                    let fut: BoxFuture<'static, Result<Estimate, EstimateError>> = async move {
                        let r = this.call_with_retry(rctx, deadline).await;
                        if let Ok(e) = &r {
                            this.inner.exact.insert(k.clone(), e.clone());
                            if let Some((m, mk)) = memo_key
                                && let Err(err) = m.put(mk, e)
                            {
                                tracing::debug!(error = %err, "remote estimate not memoized");
                            }
                        }
                        this.inner.inflight.lock().unwrap_or_else(|p| p.into_inner()).remove(&k);
                        r
                    }
                    .boxed();
                    let s = fut.shared();
                    map.insert(key.clone(), s.clone());
                    s
                }
            }
        };
        match tokio::time::timeout_at(deadline, shared).await {
            Ok(r) => r,
            Err(_) => Err(timeout_err()),
        }
    }

    fn jitter(&self) -> Duration {
        let (lo, hi) = self.inner.cfg.retry_jitter;
        let s = self.inner.seed.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed);
        let u = SplitMix64::new(s).next_f64();
        lo + (hi.saturating_sub(lo)).mul_f64(u)
    }

    async fn call_with_retry(&self, ctx: WorkloadContext, deadline: Instant) -> Result<Estimate, EstimateError> {
        match self.submit(ctx.clone(), deadline).await {
            Ok(e) => Ok(e),
            Err(c) if c.retryable => {
                let j = self.jitter();
                if Instant::now() + j >= deadline {
                    return Err(c.err);
                }
                tokio::time::sleep(j).await;
                self.submit(ctx, deadline).await.map_err(|c| c.err)
            }
            Err(c) => Err(c.err),
        }
    }

    /// One attempt: through the coalescer when batching is possible.
    async fn submit(&self, ctx: WorkloadContext, deadline: Instant) -> Result<Estimate, CallError> {
        let i = &self.inner;
        if i.max_batch <= 1 || i.cfg.coalesce_window.is_zero() {
            return Inner::send(&self.inner, vec![ctx], deadline).await.pop().unwrap_or_else(|| Err(lost()));
        }
        let tx = i.coalescer.get_or_init(|| spawn_coalescer(Arc::downgrade(&self.inner)));
        let (otx, orx) = oneshot::channel();
        if tx.send(Job { ctx, deadline, tx: otx }).is_err() {
            return Err(lost());
        }
        orx.await.unwrap_or_else(|_| Err(lost()))
    }
}

fn lost() -> CallError {
    CallError::new(EstimateError::Failed { message: "remote call dropped".into(), retryable: false }, false, false)
}

fn spawn_coalescer(inner: Weak<Inner>) -> mpsc::UnboundedSender<Job> {
    let (tx, mut rx) = mpsc::unbounded_channel::<Job>();
    tokio::spawn(async move {
        while let Some(first) = rx.recv().await {
            let Some(i) = inner.upgrade() else { break };
            let (window, max) = (i.cfg.coalesce_window, i.max_batch);
            drop(i);
            let end = Instant::now() + window;
            let mut batch = vec![first];
            while batch.len() < max {
                match tokio::time::timeout_at(end, rx.recv()).await {
                    Ok(Some(j)) => batch.push(j),
                    _ => break,
                }
            }
            let Some(i) = inner.upgrade() else { break };
            tokio::spawn(async move {
                let deadline = batch.iter().map(|j| j.deadline).min().unwrap_or_else(Instant::now);
                let (ctxs, txs): (Vec<_>, Vec<_>) = batch.into_iter().map(|j| (j.ctx, j.tx)).unzip();
                let results = Inner::send(&i, ctxs, deadline).await;
                for (tx, r) in txs.into_iter().zip(results) {
                    let _ = tx.send(r);
                }
            });
        }
    });
    tx
}

impl Inner {
    /// One HTTP call for `ctxs` (single endpoint for one, batch for more).
    /// Always returns one result per context.
    async fn send(this: &Arc<Inner>, mut ctxs: Vec<WorkloadContext>, deadline: Instant) -> Vec<Result<Estimate, CallError>> {
        let n = ctxs.len();
        if n == 0 {
            return Vec::new();
        }
        let all = |e: CallError| (0..n).map(|_| Err(e.clone())).collect::<Vec<_>>();
        let now = Instant::now();
        let Some(remaining) = deadline.checked_duration_since(now).filter(|d| !d.is_zero()) else {
            return all(CallError::new(EstimateError::Timeout(0), false, false));
        };
        if !this.breaker.admit(now) {
            return all(CallError::new(EstimateError::Unavailable("circuit open".into()), false, false));
        }
        let token = match this.cfg.token.resolve() {
            Ok(t) => t,
            Err(m) => {
                this.breaker.record(None, Instant::now());
                return all(CallError::new(EstimateError::Failed { message: m, retryable: false }, false, false));
            }
        };
        this.http_calls.fetch_add(1, Ordering::Relaxed);
        let (url, body) = if n == 1 {
            let req = EstimateRequest { context: ctxs.swap_remove(0), axes: this.axes.clone() };
            (endpoint(&this.cfg.url, "estimate"), serde_json::to_value(req))
        } else {
            (endpoint(&this.cfg.url, "estimate:batch"), serde_json::to_value(BatchRequest { contexts: ctxs, axes: this.axes.clone() }))
        };
        let body = match body {
            Ok(b) => b,
            Err(e) => return all(CallError::new(EstimateError::Protocol(format!("encoding request: {e}")), false, false)),
        };
        let mut req = this.http.post(&url).timeout(remaining).json(&body);
        if let Some(t) = token {
            req = req.bearer_auth(t);
        }
        let out = tokio::time::timeout_at(deadline, async {
            let resp = req.send().await.map_err(|e| {
                if e.is_timeout() {
                    CallError::new(EstimateError::Timeout(remaining.as_millis() as u64), false, true)
                } else {
                    CallError::new(EstimateError::Failed { message: format!("transport: {e}"), retryable: true }, true, true)
                }
            })?;
            let status = resp.status().as_u16();
            let bytes = resp.bytes().await.map_err(|e| {
                CallError::new(EstimateError::Failed { message: format!("reading body: {e}"), retryable: true }, true, true)
            })?;
            if !(200..300).contains(&status) {
                let detail = serde_json::from_slice::<ErrorBody>(&bytes).ok().map(|b| b.error);
                return Err(map_status(status, detail));
            }
            Ok(bytes)
        })
        .await;
        let bytes = match out {
            Err(_) => {
                this.breaker.record(Some(false), Instant::now());
                return all(CallError::new(EstimateError::Timeout(remaining.as_millis() as u64), false, true));
            }
            Ok(Err(e)) => {
                this.breaker.record(if e.counted { Some(false) } else { None }, Instant::now());
                return all(e);
            }
            Ok(Ok(b)) => b,
        };
        let bad = |e: serde_json::Error| CallError::new(EstimateError::Protocol(format!("decoding response: {e}")), false, true);
        let results: Result<Vec<Result<Estimate, CallError>>, CallError> = if n == 1 {
            serde_json::from_slice::<EstimateResponse>(&bytes).map(|r| vec![Ok(r.estimate)]).map_err(bad)
        } else {
            serde_json::from_slice::<BatchResponse>(&bytes).map_err(bad).and_then(|r| {
                if r.results.len() != n {
                    return Err(CallError::new(
                        EstimateError::Protocol(format!("batch returned {} results for {n} contexts", r.results.len())),
                        false,
                        true,
                    ));
                }
                Ok(r.results
                    .into_iter()
                    .map(|it| match it {
                        BatchItem::Estimate { estimate } => Ok(estimate),
                        BatchItem::Error { error } => Err(map_item(error)),
                    })
                    .collect())
            })
        };
        match results {
            Ok(r) => {
                this.breaker.record(Some(true), Instant::now());
                r
            }
            Err(e) => {
                this.breaker.record(Some(false), Instant::now());
                all(e)
            }
        }
    }
}

#[async_trait]
impl Estimator for RemoteEstimator {
    fn info(&self) -> &EstimatorInfo {
        &self.inner.info
    }

    async fn estimate(&self, ctx: &WorkloadContext) -> Result<Estimate, EstimateError> {
        self.run(ctx, None).await
    }

    async fn estimate_within(&self, ctx: &WorkloadContext, budget: Option<Duration>) -> Result<Estimate, EstimateError> {
        self.run(ctx, budget).await
    }

    /// Concurrent single estimates: the coalescer turns them into batches.
    async fn estimate_batch(&self, ctxs: &[WorkloadContext]) -> Vec<Result<Estimate, EstimateError>> {
        futures::future::join_all(ctxs.iter().map(|c| self.run(c, None))).await
    }
}

#[cfg(test)]
mod tests;
