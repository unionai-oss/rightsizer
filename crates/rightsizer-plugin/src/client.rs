//! The core → plugin side of `rightsizer-plugin/1` (plugins.md §3.3): a
//! long-lived `rsz-plugin-<kind>` process spoken to over stdio, and
//! [`PluginAdapter`], an [`Adapter`] backed by it so the engine treats a
//! plugin like a built-in.
//!
//! - **Spawn and handshake.** The binary's sha256 is checked against the
//!   configured pin before *every* spawn (restarts included); a mismatch
//!   refuses the plugin. `initialize` offers the versions this core speaks
//!   (N/N-1); a plugin answering with a version outside them, or not
//!   answering within the handshake deadline, is killed and not used.
//! - **One request at a time per pipe**, unless the plugin advertises
//!   `"concurrency": n`.
//! - **Deadlines.** Every request carries `deadline_ms`. On expiry the
//!   client sends `$/cancel` and returns [`CallError::Timeout`]; callers
//!   fall back to the prior (S1). Two consecutive timeouts kill the process
//!   (a plugin that ignores cancellation would otherwise hold the pipe).
//! - **Crashes.** EOF on the plugin's stdout fails every pending request
//!   with [`CallError::Crashed`]. The next call respawns it after a backoff
//!   (100 ms doubling to 30 s, reset after 60 s of healthy uptime); calls
//!   inside the backoff window fail fast.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{Mutex, Semaphore, oneshot};

use crate::protocol::{
    ExtractParams, InitializeParams, InitializeResult, Request, Response, RenderParams, StateParams, UnitSpec, ValidateParams, ValidateResult,
    method,
};
use crate::rpc::{self, Incoming, Line};
use crate::{Adapter, AdapterEnv, Extracted, Native, Patch, PlatformRef, PlatformState, PluginError, Rejection, ResourceSpec, Tenant, Unit, UnitId};

/// How to run a plugin.
#[derive(Clone, Debug, PartialEq)]
pub struct PluginSpec {
    /// Kind (`nomad-demo`): what `decide {kind}` names.
    pub kind: String,
    /// Executable.
    pub path: PathBuf,
    /// Arguments.
    pub args: Vec<String>,
    /// Extra environment.
    pub env: Vec<(String, String)>,
    /// Pinned sha256 (hex) of the executable.
    pub sha256: Option<String>,
    /// Default per-request deadline.
    pub deadline: Duration,
    /// Handshake deadline.
    pub handshake_deadline: Duration,
}

impl PluginSpec {
    /// A spec with default deadlines (2 s per request, 5 s handshake).
    pub fn new(kind: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        PluginSpec { kind: kind.into(), path: path.into(), args: vec![], env: vec![], sha256: None, deadline: Duration::from_secs(2), handshake_deadline: Duration::from_secs(5) }
    }
}

/// Why a call failed.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum CallError {
    /// The pinned sha256 does not match the executable.
    #[error("plugin {kind} refused: sha256 mismatch for {path} (pinned {expected}, found {found})")]
    PinMismatch {
        /// Kind.
        kind: String,
        /// Path.
        path: String,
        /// Pinned.
        expected: String,
        /// Actual.
        found: String,
    },
    /// Could not start (exec failed, handshake failed or timed out, version mismatch).
    #[error("plugin {0} did not start: {1}")]
    Spawn(String, String),
    /// Inside the restart backoff window.
    #[error("plugin {0} is restarting (backoff {1:?})")]
    Backoff(String, Duration),
    /// The plugin exited while the request was pending.
    #[error("plugin {0} crashed: {1}")]
    Crashed(String, String),
    /// `deadline_ms` expired.
    #[error("plugin {0}: {1} timed out after {2:?}")]
    Timeout(String, String, Duration),
    /// The plugin answered with an error object.
    #[error("plugin {kind}: {message}")]
    Rpc {
        /// Kind.
        kind: String,
        /// The error.
        message: String,
        /// Protocol code.
        code: Option<crate::protocol::ErrorCode>,
        /// Retryable.
        retryable: bool,
    },
    /// The result did not deserialize.
    #[error("plugin {0}: bad result for {1}: {2}")]
    BadResult(String, String, String),
}

impl From<CallError> for PluginError {
    fn from(e: CallError) -> Self {
        use crate::protocol::ErrorCode as C;
        match &e {
            CallError::Rpc { code: Some(C::ParseError), message, .. } => PluginError::Parse(message.clone()),
            CallError::Rpc { code: Some(C::Unsupported), message, .. } => PluginError::Unsupported(message.clone()),
            CallError::Rpc { code: Some(C::InvalidNative), message, .. } => PluginError::InvalidNative(message.clone()),
            CallError::Rpc { code: Some(C::Internal), message, .. } => PluginError::Internal(message.clone()),
            _ => PluginError::PlatformUnavailable(e.to_string()),
        }
    }
}

/// sha256 (lowercase hex) of a file.
pub fn file_sha256(path: &std::path::Path) -> std::io::Result<String> {
    use sha2::Digest;
    let bytes = std::fs::read(path)?;
    Ok(hex::encode(sha2::Sha256::digest(&bytes)))
}

/// Check a pin (`None` passes).
pub fn verify_pin(spec: &PluginSpec) -> Result<(), CallError> {
    let Some(want) = &spec.sha256 else { return Ok(()) };
    let found = file_sha256(&spec.path).map_err(|e| CallError::Spawn(spec.kind.clone(), format!("{}: {e}", spec.path.display())))?;
    if !found.eq_ignore_ascii_case(want.trim()) {
        return Err(CallError::PinMismatch { kind: spec.kind.clone(), path: spec.path.display().to_string(), expected: want.clone(), found });
    }
    Ok(())
}

/// Longest plugin stderr line kept for the log (the rest is dropped).
pub const MAX_STDERR_LINE: usize = 8 << 10;

/// One line of a plugin's stderr, at most `max` bytes (lossy UTF-8): a
/// plugin that writes megabytes without a newline must not make the daemon
/// buffer them. `None` at EOF.
pub async fn read_log_line<R: tokio::io::AsyncBufRead + Unpin>(r: &mut R, max: usize) -> std::io::Result<Option<String>> {
    let mut buf: Vec<u8> = Vec::new();
    let mut seen = false;
    loop {
        let avail = r.fill_buf().await?;
        if avail.is_empty() {
            return Ok(seen.then(|| String::from_utf8_lossy(&buf).into_owned()));
        }
        seen = true;
        let (chunk, done) = match avail.iter().position(|b| *b == b'\n') {
            Some(i) => (&avail[..i], Some(i + 1)),
            None => (avail, None),
        };
        let room = max.saturating_sub(buf.len());
        buf.extend_from_slice(&chunk[..chunk.len().min(room)]);
        let n = done.unwrap_or(avail.len());
        r.consume(n);
        if done.is_some() {
            return Ok(Some(String::from_utf8_lossy(&buf).into_owned()));
        }
    }
}

type Pending = Arc<std::sync::Mutex<HashMap<i64, oneshot::Sender<Response>>>>;

struct Conn {
    child: std::sync::Mutex<Child>,
    stdin: Mutex<ChildStdin>,
    pending: Pending,
    alive: Arc<AtomicBool>,
    exit_note: Arc<std::sync::Mutex<String>>,
    sem: Arc<Semaphore>,
    info: InitializeResult,
    started: Instant,
    timeouts: AtomicU32,
}

impl Conn {
    fn pid(&self) -> Option<u32> {
        self.child.lock().unwrap_or_else(|e| e.into_inner()).id()
    }

    fn kill(&self) {
        self.alive.store(false, Ordering::SeqCst);
        let _ = self.child.lock().unwrap_or_else(|e| e.into_inner()).start_kill();
    }
}

#[derive(Default)]
struct Restart {
    failures: u32,
    last_failure: Option<Instant>,
}

/// A long-lived plugin process.
pub struct PluginClient {
    spec: PluginSpec,
    conn: Mutex<Option<Arc<Conn>>>,
    restart: std::sync::Mutex<Restart>,
    next_id: AtomicI64,
    spawns: AtomicU32,
}

impl std::fmt::Debug for PluginClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginClient").field("kind", &self.spec.kind).field("path", &self.spec.path).finish()
    }
}

const BACKOFF_MIN: Duration = Duration::from_millis(100);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
const HEALTHY_UPTIME: Duration = Duration::from_secs(60);

impl PluginClient {
    /// A client; the process starts on first use (or [`PluginClient::start`]).
    pub fn new(spec: PluginSpec) -> Self {
        PluginClient { spec, conn: Mutex::new(None), restart: Default::default(), next_id: AtomicI64::new(1), spawns: AtomicU32::new(0) }
    }

    /// Spawn and handshake now.
    pub async fn start(&self) -> Result<InitializeResult, CallError> {
        let c = self.connection().await?;
        Ok(c.info.clone())
    }

    /// The spec.
    pub fn spec(&self) -> &PluginSpec {
        &self.spec
    }

    /// The handshake result of the running process.
    pub async fn info(&self) -> Option<InitializeResult> {
        self.conn.lock().await.as_ref().filter(|c| c.alive.load(Ordering::SeqCst)).map(|c| c.info.clone())
    }

    /// The running process's pid.
    pub async fn pid(&self) -> Option<u32> {
        self.conn.lock().await.as_ref().filter(|c| c.alive.load(Ordering::SeqCst)).and_then(|c| c.pid())
    }

    /// How many times the process was spawned.
    pub fn spawn_count(&self) -> u32 {
        self.spawns.load(Ordering::SeqCst)
    }

    /// Forget the restart backoff (tests, `rsz plugin check`).
    pub fn reset_backoff(&self) {
        *self.restart.lock().unwrap_or_else(|e| e.into_inner()) = Restart::default();
    }

    fn backoff(&self) -> Option<Duration> {
        let r = self.restart.lock().unwrap_or_else(|e| e.into_inner());
        let last = r.last_failure?;
        if r.failures == 0 {
            return None;
        }
        let wait = BACKOFF_MIN.saturating_mul(1u32 << (r.failures - 1).min(16)).min(BACKOFF_MAX);
        let since = last.elapsed();
        (since < wait).then(|| wait - since)
    }

    fn note_failure(&self) {
        let mut r = self.restart.lock().unwrap_or_else(|e| e.into_inner());
        r.failures = r.failures.saturating_add(1);
        r.last_failure = Some(Instant::now());
    }

    async fn connection(&self) -> Result<Arc<Conn>, CallError> {
        let mut g = self.conn.lock().await;
        if let Some(c) = g.as_ref() {
            if c.alive.load(Ordering::SeqCst) {
                if c.started.elapsed() > HEALTHY_UPTIME {
                    self.restart.lock().unwrap_or_else(|e| e.into_inner()).failures = 0;
                }
                return Ok(c.clone());
            }
            // Dead: reap it and count the failure (unless healthy for long).
            if c.started.elapsed() > HEALTHY_UPTIME {
                self.reset_backoff();
            } else if self.restart.lock().unwrap_or_else(|e| e.into_inner()).last_failure.is_none_or(|t| t < c.started) {
                self.note_failure();
            }
            *g = None;
        }
        if let Some(w) = self.backoff() {
            return Err(CallError::Backoff(self.spec.kind.clone(), w));
        }
        match self.spawn().await {
            Ok(c) => {
                let c = Arc::new(c);
                *g = Some(c.clone());
                Ok(c)
            }
            Err(e) => {
                self.note_failure();
                Err(e)
            }
        }
    }

    async fn spawn(&self) -> Result<Conn, CallError> {
        verify_pin(&self.spec)?;
        let kind = self.spec.kind.clone();
        let mut cmd = Command::new(&self.spec.path);
        cmd.args(&self.spec.args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        for (k, v) in &self.spec.env {
            cmd.env(k, v);
        }
        let mut child = cmd.spawn().map_err(|e| CallError::Spawn(kind.clone(), format!("{}: {e}", self.spec.path.display())))?;
        self.spawns.fetch_add(1, Ordering::SeqCst);
        let stdin = child.stdin.take().ok_or_else(|| CallError::Spawn(kind.clone(), "no stdin".into()))?;
        let stdout = child.stdout.take().ok_or_else(|| CallError::Spawn(kind.clone(), "no stdout".into()))?;
        if let Some(err) = child.stderr.take() {
            let k = kind.clone();
            tokio::spawn(async move {
                let mut r = BufReader::new(err);
                while let Ok(Some(l)) = read_log_line(&mut r, MAX_STDERR_LINE).await {
                    tracing::debug!(plugin = %k, "{l}");
                }
            });
        }
        let pending: Pending = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let alive = Arc::new(AtomicBool::new(true));
        let exit_note = Arc::new(std::sync::Mutex::new(String::new()));
        {
            let (pending, alive, note, k) = (pending.clone(), alive.clone(), exit_note.clone(), kind.clone());
            tokio::spawn(async move {
                let mut r = BufReader::new(stdout);
                loop {
                    match rpc::read_line(&mut r, rpc::MAX_LINE).await {
                        Ok(Line::Msg(v)) => match rpc::classify(v) {
                            Incoming::Response(resp) => {
                                let id = match &resp.id {
                                    Some(crate::protocol::Id::Num(n)) => Some(*n),
                                    Some(crate::protocol::Id::Str(s)) => s.parse().ok(),
                                    None => None,
                                };
                                if let Some(id) = id
                                    && let Some(tx) = pending.lock().unwrap_or_else(|e| e.into_inner()).remove(&id)
                                {
                                    let _ = tx.send(resp);
                                }
                            }
                            other => tracing::debug!(plugin = %k, "ignoring message from plugin: {other:?}"),
                        },
                        Ok(Line::Bad(e)) => tracing::warn!(plugin = %k, "unparseable line from plugin: {e}"),
                        Ok(Line::Eof) | Err(_) => break,
                    }
                }
                alive.store(false, Ordering::SeqCst);
                *note.lock().unwrap_or_else(|e| e.into_inner()) = "stdout closed".into();
                // Dropping the senders fails every pending request.
                pending.lock().unwrap_or_else(|e| e.into_inner()).clear();
            });
        }
        let mut conn = Conn {
            child: std::sync::Mutex::new(child),
            stdin: Mutex::new(stdin),
            pending,
            alive,
            exit_note,
            sem: Arc::new(Semaphore::new(1)),
            info: InitializeResult { protocol: String::new(), kind: String::new(), version: String::new(), methods: vec![], native_media_types: vec![], concurrency: None },
            started: Instant::now(),
            timeouts: AtomicU32::new(0),
        };
        let offered = rpc::offered_protocols();
        let params = InitializeParams { protocol: offered[0].clone(), protocols: offered[1..].to_vec(), ..Default::default() };
        let v = match self.raw_call(&conn, method::INITIALIZE, serde_json::to_value(&params).unwrap_or_default(), self.spec.handshake_deadline, false).await {
            Ok(v) => v,
            Err(e) => {
                conn.kill();
                return Err(CallError::Spawn(kind, format!("handshake: {e}")));
            }
        };
        let info: InitializeResult = match serde_json::from_value(v) {
            Ok(i) => i,
            Err(e) => {
                conn.kill();
                return Err(CallError::Spawn(kind, format!("handshake: bad initialize result: {e}")));
            }
        };
        if rpc::negotiate([info.protocol.as_str()]).is_none() {
            conn.kill();
            return Err(CallError::Spawn(kind, format!("handshake: no common protocol version (plugin speaks {:?}, core {:?})", info.protocol, offered)));
        }
        if !info.kind.is_empty() && info.kind != self.spec.kind {
            tracing::warn!(plugin = %self.spec.kind, reported = %info.kind, "plugin reports a different kind");
        }
        conn.sem = Arc::new(Semaphore::new(info.concurrency.unwrap_or(1).clamp(1, 1024) as usize));
        conn.info = info;
        Ok(conn)
    }

    async fn raw_call(&self, conn: &Conn, m: &str, params: Value, deadline: Duration, with_deadline: bool) -> Result<Value, CallError> {
        let kind = &self.spec.kind;
        let start = Instant::now();
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let permit = match tokio::time::timeout(deadline, conn.sem.clone().acquire_owned()).await {
            Ok(Ok(p)) => p,
            Ok(Err(_)) => return Err(CallError::Crashed(kind.clone(), "closed".into())),
            Err(_) => return Err(CallError::Timeout(kind.clone(), m.into(), deadline)),
        };
        let left = deadline.saturating_sub(start.elapsed());
        let params = if with_deadline { rpc::with_deadline(params, left.as_millis().min(u32::MAX as u128) as u32) } else { params };
        let (tx, rx) = oneshot::channel();
        conn.pending.lock().unwrap_or_else(|e| e.into_inner()).insert(id, tx);
        if !conn.alive.load(Ordering::SeqCst) {
            conn.pending.lock().unwrap_or_else(|e| e.into_inner()).remove(&id);
            return Err(CallError::Crashed(kind.clone(), conn.exit_note.lock().unwrap_or_else(|e| e.into_inner()).clone()));
        }
        let req = Request { jsonrpc: crate::protocol::JSONRPC.into(), id: Some(crate::protocol::Id::Num(id)), method: m.into(), params };
        let write = async {
            let mut w = conn.stdin.lock().await;
            rpc::write_msg(&mut *w, &req).await
        };
        match tokio::time::timeout(left, write).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                conn.pending.lock().unwrap_or_else(|e| e.into_inner()).remove(&id);
                return Err(CallError::Crashed(kind.clone(), format!("write: {e}")));
            }
            Err(_) => {
                conn.pending.lock().unwrap_or_else(|e| e.into_inner()).remove(&id);
                return Err(CallError::Timeout(kind.clone(), m.into(), deadline));
            }
        }
        let left = deadline.saturating_sub(start.elapsed());
        let resp = match tokio::time::timeout(left, rx).await {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => return Err(CallError::Crashed(kind.clone(), conn.exit_note.lock().unwrap_or_else(|e| e.into_inner()).clone())),
            Err(_) => {
                conn.pending.lock().unwrap_or_else(|e| e.into_inner()).remove(&id);
                // `$/cancel`, best effort, then free the pipe slot.
                let cancel = Request { jsonrpc: crate::protocol::JSONRPC.into(), id: None, method: method::CANCEL.into(), params: serde_json::json!({"id": id}) };
                if let Ok(mut w) = conn.stdin.try_lock() {
                    let _ = tokio::time::timeout(Duration::from_millis(50), rpc::write_msg(&mut *w, &cancel)).await;
                }
                drop(permit);
                return Err(CallError::Timeout(kind.clone(), m.into(), deadline));
            }
        };
        drop(permit);
        match (resp.result, resp.error) {
            (_, Some(e)) => Err(CallError::Rpc { kind: kind.clone(), message: e.message, code: e.data.as_ref().map(|d| d.code), retryable: e.data.is_some_and(|d| d.retryable) }),
            (Some(v), None) => Ok(v),
            (None, None) => Ok(Value::Null),
        }
    }

    /// Call `m` with `deadline` (default: the spec's).
    pub async fn call(&self, m: &str, params: Value, deadline: Option<Duration>) -> Result<Value, CallError> {
        let deadline = deadline.unwrap_or(self.spec.deadline);
        let conn = self.connection().await?;
        let r = self.raw_call(&conn, m, params, deadline, true).await;
        match &r {
            Err(CallError::Timeout(..)) => {
                if conn.timeouts.fetch_add(1, Ordering::SeqCst) + 1 >= 2 {
                    tracing::warn!(plugin = %self.spec.kind, "two consecutive timeouts: restarting the plugin");
                    self.kill().await;
                }
            }
            Err(CallError::Crashed(..)) => {
                conn.alive.store(false, Ordering::SeqCst);
                self.note_failure();
            }
            _ => conn.timeouts.store(0, Ordering::SeqCst),
        }
        r
    }

    /// Typed call.
    pub async fn call_typed<P: Serialize, R: DeserializeOwned>(&self, m: &str, params: &P, deadline: Option<Duration>) -> Result<R, CallError> {
        let v = self.call(m, serde_json::to_value(params).unwrap_or_default(), deadline).await?;
        serde_json::from_value(v).map_err(|e| CallError::BadResult(self.spec.kind.clone(), m.into(), e.to_string()))
    }

    /// Kill the process (it is respawned on next use, after the backoff).
    pub async fn kill(&self) {
        let mut g = self.conn.lock().await;
        if let Some(c) = g.take() {
            // Other callers holding the connection see EOF once it dies.
            c.kill();
            self.note_failure();
        }
    }

    /// `shutdown` and wait for exit (kill after 1 s).
    pub async fn shutdown(&self) {
        let c = self.conn.lock().await.take();
        if let Some(c) = c {
            let _ = self.raw_call(&c, method::SHUTDOWN, Value::Null, Duration::from_millis(500), false).await;
            // Closing stdin is the second signal.
            let _ = tokio::io::AsyncWriteExt::shutdown(&mut *c.stdin.lock().await).await;
            let t = Instant::now();
            while t.elapsed() < Duration::from_secs(1) {
                if matches!(c.child.lock().unwrap_or_else(|e| e.into_inner()).try_wait(), Ok(Some(_))) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            c.kill();
        }
    }
}

/// An [`Adapter`] (and platform model) backed by a plugin process. Every
/// transport error becomes a [`PluginError`] (extract/render) or a
/// [`Rejection`] (validate), so the core keeps the prior (S1).
#[derive(Clone, Debug)]
pub struct PluginAdapter {
    client: Arc<PluginClient>,
    kind: String,
}

impl PluginAdapter {
    /// Wrap a client.
    pub fn new(client: Arc<PluginClient>) -> Self {
        let kind = client.spec().kind.clone();
        PluginAdapter { client, kind }
    }

    /// The client.
    pub fn client(&self) -> &Arc<PluginClient> {
        &self.client
    }

    fn deadline(env: Option<&AdapterEnv>, fallback: Duration) -> Duration {
        env.and_then(|e| e.deadline_ms).map(|d| Duration::from_millis(d as u64)).unwrap_or(fallback)
    }

    async fn has(&self, m: &str) -> bool {
        self.client.info().await.is_some_and(|i| i.methods.iter().any(|x| x == m))
    }

    /// `reference`, when the plugin implements it; else a bare reference.
    pub async fn reference_rpc(&self, unit: &Unit, deadline: Duration) -> PlatformRef {
        if self.client.info().await.is_none() {
            let _ = self.client.start().await;
        }
        if !self.has(method::REFERENCE).await {
            return PlatformRef::new(&self.kind);
        }
        self.client
            .call_typed::<_, PlatformRef>(method::REFERENCE, &crate::protocol::ReferenceParams { unit: unit.clone() }, Some(deadline))
            .await
            .unwrap_or_else(|_| PlatformRef::new(&self.kind))
    }

    /// `state`, when the plugin implements it; else defaults.
    pub async fn state_rpc(&self, tenant: &Tenant, env: &AdapterEnv, deadline: Duration) -> PlatformState {
        if !self.has(method::STATE).await {
            return PlatformState::default();
        }
        let p = StateParams { tenant: tenant.clone(), env: env.clone(), deadline_ms: None };
        self.client.call_typed::<_, PlatformState>(method::STATE, &p, Some(deadline)).await.unwrap_or_default()
    }
}

#[async_trait]
impl Adapter for PluginAdapter {
    fn kind(&self) -> &str {
        &self.kind
    }

    async fn extract(&self, native: &Native, env: &AdapterEnv) -> crate::Result<Extracted> {
        let d = Self::deadline(Some(env), self.client.spec().deadline);
        let p = ExtractParams { native: native.clone(), env: env.clone(), deadline_ms: None };
        Ok(self.client.call_typed(method::EXTRACT, &p, Some(d)).await?)
    }

    async fn render(&self, native: &Native, decisions: &[(UnitId, ResourceSpec)]) -> crate::Result<Patch> {
        let p = RenderParams { native: native.clone(), decisions: decisions.iter().map(|(u, s)| UnitSpec { unit: u.clone(), spec: s.clone() }).collect(), deadline_ms: None };
        Ok(self.client.call_typed(method::RENDER, &p, None).await?)
    }

    async fn validate(&self, native: &Native, patch: &Patch, env: &AdapterEnv) -> Result<(), Rejection> {
        let d = Self::deadline(Some(env), self.client.spec().deadline);
        let p = ValidateParams { native: native.clone(), patch: patch.clone(), env: env.clone(), deadline_ms: None };
        match self.client.call_typed::<_, ValidateResult>(method::VALIDATE, &p, Some(d)).await {
            Ok(ValidateResult { ok: true, .. }) => Ok(()),
            Ok(ValidateResult { rejection: Some(r), .. }) => Err(r),
            Ok(ValidateResult { rejection: None, .. }) => Err(Rejection { reason: "rejected".into(), detail: None, field: None }),
            Err(e) => Err(Rejection { reason: "plugin_error".into(), detail: Some(e.to_string()), field: None }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stderr_lines_are_bounded() {
        // 10 MiB without a newline, then a normal line.
        let mut data = vec![b'x'; 10 << 20];
        data.extend_from_slice(b"\nok\ntail");
        let mut r = BufReader::new(&data[..]);
        let first = read_log_line(&mut r, 1024).await.unwrap().unwrap();
        assert_eq!(first.len(), 1024);
        assert_eq!(read_log_line(&mut r, 1024).await.unwrap().as_deref(), Some("ok"));
        assert_eq!(read_log_line(&mut r, 1024).await.unwrap().as_deref(), Some("tail"));
        assert_eq!(read_log_line(&mut r, 1024).await.unwrap(), None);
    }
}
