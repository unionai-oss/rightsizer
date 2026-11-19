//! The serving side of `rightsizer-plugin/1` (plugins.md §3): one
//! connection loop for every transport (Unix socket, stdio, HTTP bodies),
//! and the Unix-socket listener that authenticates callers by their peer
//! credentials (`SO_PEERCRED` on Linux, `getpeereid`/`LOCAL_PEERCRED` on
//! macOS and the BSDs, both through `tokio::net::UnixStream::peer_cred`).
//!
//! Per connection:
//!
//! - requests run concurrently (up to [`ServerOptions::max_concurrent`]);
//!   responses are written as they finish, each on one line;
//! - `initialize` is answered by the loop itself (version negotiation:
//!   the highest common `rightsizer-plugin/<major>`, N/N-1; no overlap is an
//!   error with `data.code = unsupported`). It is optional for shims: a
//!   connection that starts with `decide` speaks the current version;
//! - every request may carry `deadline_ms`; on expiry the request is
//!   dropped and answered with a `-32001` error (`retryable: true`);
//! - `$/cancel {"id": …}` aborts an in-flight request, which is answered
//!   with `-32800` (`retryable: true`); cancelling an unknown or finished id
//!   is a no-op;
//! - a panic in a handler is an `internal` error, never a dropped connection;
//! - on shutdown the loop stops reading, lets in-flight requests finish
//!   (within the caller's grace period) and closes.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::FutureExt;
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncWrite, BufReader};
use tokio::sync::{Semaphore, mpsc, watch};

use crate::protocol::{ErrorCode, Id, InitializeParams, InitializeResult, Request, Response, RpcError, method};
use crate::rpc::{self, Incoming, Line, codes};

/// How a caller reached the server.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Transport {
    /// Unix socket (peer credentials known).
    Unix,
    /// HTTP (no peer credentials).
    Http,
    /// stdio.
    Stdio,
    /// In-process (tests).
    Memory,
}

/// The authenticated caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Caller {
    /// Peer uid (Unix socket).
    pub uid: Option<u32>,
    /// Peer gid.
    pub gid: Option<u32>,
    /// Peer pid (Linux only).
    pub pid: Option<i32>,
    /// Transport.
    pub transport: Transport,
}

impl Caller {
    /// A caller without credentials.
    pub fn anonymous(transport: Transport) -> Self {
        Caller { uid: None, gid: None, pid: None, transport }
    }
}

/// Per-request context.
#[derive(Clone, Debug)]
pub struct Ctx {
    /// Who called.
    pub caller: Caller,
    /// When the request's `deadline_ms` expires.
    pub deadline: Option<Instant>,
    /// The version negotiated on this connection.
    pub protocol: String,
}

impl Ctx {
    /// Remaining time, ms (`None`: no deadline).
    pub fn remaining_ms(&self) -> Option<u32> {
        self.deadline.map(|d| d.saturating_duration_since(Instant::now()).as_millis().min(u32::MAX as u128) as u32)
    }
}

/// What a server serves.
#[async_trait]
pub trait Handler: Send + Sync + 'static {
    /// The `initialize` result (the loop fills in the negotiated protocol).
    fn info(&self) -> InitializeResult;
    /// Handle one request. Unknown methods return [`method_not_found`].
    async fn call(&self, method: &str, params: Value, ctx: &Ctx) -> Result<Value, RpcError>;
}

/// The standard "unknown method" error.
pub fn method_not_found(m: &str) -> RpcError {
    rpc::err(codes::METHOD_NOT_FOUND, ErrorCode::Unsupported, format!("method not found: {m}"), false)
}

/// Bad params.
pub fn invalid_params(e: impl std::fmt::Display) -> RpcError {
    rpc::err(codes::INVALID_PARAMS, ErrorCode::ParseError, format!("invalid params: {e}"), false)
}

/// Connection options.
#[derive(Clone, Debug)]
pub struct ServerOptions {
    /// Requests in flight per connection.
    pub max_concurrent: usize,
    /// Longest line.
    pub max_line: usize,
    /// Deadline applied when a request carries none (`None`: no deadline).
    pub default_deadline_ms: Option<u32>,
    /// Upper bound on any request's deadline.
    pub max_deadline_ms: u32,
}

impl Default for ServerOptions {
    fn default() -> Self {
        ServerOptions { max_concurrent: 64, max_line: rpc::MAX_LINE, default_deadline_ms: Some(30_000), max_deadline_ms: 300_000 }
    }
}

type Inflight = Arc<Mutex<HashMap<Id, tokio::task::AbortHandle>>>;

/// Resolves when `rx` turns true (never, if the sender is gone).
pub async fn shutdown_signal(rx: &mut watch::Receiver<bool>) {
    loop {
        if *rx.borrow_and_update() {
            return;
        }
        if rx.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

/// Serve one connection until EOF or shutdown.
pub async fn serve_connection<R, W>(reader: R, writer: W, handler: Arc<dyn Handler>, caller: Caller, opts: ServerOptions, mut shutdown: watch::Receiver<bool>)
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (tx, mut rx) = mpsc::channel::<Response>(256);
    let writer_task = tokio::spawn(async move {
        let mut w = writer;
        while let Some(resp) = rx.recv().await {
            if rpc::write_msg(&mut w, &resp).await.is_err() {
                break;
            }
        }
    });
    let mut reader = BufReader::new(reader);
    let inflight: Inflight = Arc::new(Mutex::new(HashMap::new()));
    let sem = Arc::new(Semaphore::new(opts.max_concurrent.max(1)));
    let protocol = Arc::new(Mutex::new(rpc::protocol_of(rpc::CURRENT_MAJOR)));
    let mut tasks = tokio::task::JoinSet::new();
    loop {
        if *shutdown.borrow() {
            break;
        }
        let line = tokio::select! {
            l = rpc::read_line(&mut reader, opts.max_line) => l,
            _ = shutdown_signal(&mut shutdown) => break,
        };
        let v = match line {
            Ok(Line::Msg(v)) => v,
            Ok(Line::Bad(e)) => {
                let _ = tx.send(Response::err(None, rpc::err(codes::PARSE, ErrorCode::ParseError, e, false))).await;
                continue;
            }
            Ok(Line::Eof) | Err(_) => break,
        };
        let req = match rpc::classify(v) {
            Incoming::Request(r) => r,
            Incoming::Response(_) => continue, // the server sends no requests
            Incoming::Invalid(id, e) => {
                let _ = tx.send(Response::err(id, rpc::err(codes::INVALID_REQUEST, ErrorCode::ParseError, e, false))).await;
                continue;
            }
        };
        match req.method.as_str() {
            method::CANCEL => {
                if let Some(id) = req.params.get("id").and_then(|i| serde_json::from_value::<Id>(i.clone()).ok()) {
                    let handle = inflight.lock().unwrap_or_else(|e| e.into_inner()).remove(&id);
                    if let Some(h) = handle {
                        h.abort();
                        let _ = tx.send(Response::err(Some(id), rpc::err(codes::CANCELLED, ErrorCode::Internal, "cancelled", true))).await;
                    }
                }
                continue;
            }
            method::INITIALIZE => {
                let resp = initialize(&req, handler.as_ref(), &protocol);
                if req.id.is_some() {
                    let _ = tx.send(Response { id: req.id.clone(), ..resp }).await;
                }
                continue;
            }
            _ => {}
        }
        // Concurrency bound: wait for a slot (backpressure on the reader).
        let permit = tokio::select! {
            p = sem.clone().acquire_owned() => match p { Ok(p) => p, Err(_) => break },
            _ = shutdown_signal(&mut shutdown) => break,
        };
        let deadline_ms = rpc::deadline_of(&req.params).or(opts.default_deadline_ms).map(|d| d.min(opts.max_deadline_ms));
        let ctx = Ctx {
            caller: caller.clone(),
            deadline: deadline_ms.map(|d| Instant::now() + Duration::from_millis(d as u64)),
            protocol: protocol.lock().unwrap_or_else(|e| e.into_inner()).clone(),
        };
        let h = handler.clone();
        let txc = tx.clone();
        let infl = inflight.clone();
        let id = req.id.clone();
        let Request { method: m, params, .. } = req;
        let id2 = id.clone();
        // Hold the in-flight map while spawning so the task cannot finish
        // (and look itself up) before it is registered.
        let mut guard = inflight.lock().unwrap_or_else(|e| e.into_inner());
        let abort = tasks.spawn(async move {
            let _permit = permit;
            let fut = AssertUnwindSafe(h.call(&m, params, &ctx)).catch_unwind();
            let out = match deadline_ms {
                Some(d) => match tokio::time::timeout(Duration::from_millis(d as u64), fut).await {
                    Ok(r) => r,
                    Err(_) => Ok(Err(rpc::err(codes::DEADLINE, ErrorCode::Internal, format!("deadline of {d} ms exceeded"), true))),
                },
                None => fut.await,
            };
            let result = match out {
                Ok(r) => r,
                Err(_) => Err(rpc::err(-32000, ErrorCode::Internal, "internal error (panic)", true)),
            };
            let Some(id) = id2 else { return }; // notification
            let mine = infl.lock().unwrap_or_else(|e| e.into_inner()).remove(&id).is_some();
            if !mine {
                return; // cancelled: already answered
            }
            let resp = match result {
                Ok(v) => Response { jsonrpc: crate::protocol::JSONRPC.into(), id: Some(id), result: Some(v), error: None },
                Err(e) => Response::err(Some(id), e),
            };
            let _ = txc.send(resp).await;
        });
        if let Some(id) = id {
            // Duplicate in-flight ids: the newer request wins the id.
            guard.insert(id, abort);
        }
        drop(guard);
        // Reap finished tasks without blocking.
        while tasks.try_join_next().is_some() {}
    }
    // Let in-flight requests answer (the caller bounds this with its grace period).
    while tasks.join_next().await.is_some() {}
    drop(tx);
    let _ = writer_task.await;
}

fn initialize(req: &Request, handler: &dyn Handler, protocol: &Mutex<String>) -> Response {
    let params: InitializeParams = match serde_json::from_value(req.params.clone()) {
        Ok(p) => p,
        Err(e) => return Response::err(req.id.clone(), invalid_params(e)),
    };
    let offered = std::iter::once(params.protocol.as_str()).chain(params.protocols.iter().map(String::as_str));
    match rpc::negotiate(offered) {
        Some(p) => {
            *protocol.lock().unwrap_or_else(|e| e.into_inner()) = p.clone();
            let mut info = handler.info();
            info.protocol = p;
            Response::ok(req.id.clone(), &info)
        }
        None => Response::err(
            req.id.clone(),
            rpc::err(
                codes::VERSION,
                ErrorCode::Unsupported,
                format!("no common protocol version: offered {:?}, served {:?}", params.protocol, rpc::offered_protocols()),
                false,
            ),
        ),
    }
}

// ── Unix socket ─────────────────────────────────────────────────────────

/// Bind a Unix socket at `path`: a stale socket file (nothing listening)
/// is replaced; a live one is an error. `mode` is applied to the file.
pub fn bind_unix(path: &Path, mode: u32) -> std::io::Result<tokio::net::UnixListener> {
    use std::os::unix::fs::PermissionsExt;
    if path.exists() {
        match std::os::unix::net::UnixStream::connect(path) {
            Ok(_) => return Err(std::io::Error::new(std::io::ErrorKind::AddrInUse, format!("{} is in use by a running daemon", path.display()))),
            Err(_) => std::fs::remove_file(path)?,
        }
    }
    if let Some(dir) = path.parent()
        && !dir.as_os_str().is_empty()
    {
        std::fs::create_dir_all(dir)?;
    }
    let l = tokio::net::UnixListener::bind(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))?;
    Ok(l)
}

/// A uid's user name: `/etc/passwd`, else `id -nu UID` (macOS directory
/// services). No libc.
pub fn user_name(uid: u32) -> Option<String> {
    if let Ok(text) = std::fs::read_to_string("/etc/passwd") {
        for l in text.lines() {
            let f: Vec<&str> = l.split(':').collect();
            if f.len() > 2 && f[2].parse::<u32>().ok() == Some(uid) {
                return Some(f[0].to_string());
            }
        }
    }
    let o = std::process::Command::new("id").arg("-nu").arg(uid.to_string()).output().ok()?;
    let n = String::from_utf8_lossy(&o.stdout).trim().to_string();
    (o.status.success() && !n.is_empty()).then_some(n)
}

/// The current process's uid (`id -u`; no libc).
pub fn current_uid() -> Option<u32> {
    let o = std::process::Command::new("id").arg("-u").output().ok()?;
    String::from_utf8_lossy(&o.stdout).trim().parse().ok()
}

/// Peer credentials of a Unix stream.
pub fn peer_of(s: &tokio::net::UnixStream) -> Caller {
    match s.peer_cred() {
        Ok(c) => Caller { uid: Some(c.uid()), gid: Some(c.gid()), pid: c.pid(), transport: Transport::Unix },
        Err(_) => Caller::anonymous(Transport::Unix),
    }
}

/// Accept connections until `shutdown` turns true, then wait up to
/// `grace` for open connections to finish their in-flight requests.
/// Removes the socket file on exit.
pub async fn serve_unix(listener: tokio::net::UnixListener, path: PathBuf, handler: Arc<dyn Handler>, opts: ServerOptions, mut shutdown: watch::Receiver<bool>, grace: Duration) {
    let mut conns = tokio::task::JoinSet::new();
    loop {
        if *shutdown.borrow() {
            break;
        }
        tokio::select! {
            a = listener.accept() => match a {
                Ok((stream, _)) => {
                    let caller = peer_of(&stream);
                    let (r, w) = stream.into_split();
                    conns.spawn(serve_connection(r, w, handler.clone(), caller, opts.clone(), shutdown.clone()));
                }
                Err(e) => {
                    tracing::warn!(error = %e, "accept failed");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            },
            _ = shutdown_signal(&mut shutdown) => break,
        }
        while conns.try_join_next().is_some() {}
    }
    drop(listener);
    let _ = std::fs::remove_file(&path);
    let _ = tokio::time::timeout(grace, async { while conns.join_next().await.is_some() {} }).await;
    conns.abort_all();
}

/// Serve one request body (HTTP): one JSON-RPC message (or a batch array)
/// in, the response(s) out. Notifications get `None`.
pub async fn serve_body(body: &[u8], handler: Arc<dyn Handler>, caller: Caller, opts: ServerOptions) -> Option<Value> {
    let v: Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => return serde_json::to_value(Response::err(None, rpc::err(codes::PARSE, ErrorCode::ParseError, e.to_string(), false))).ok(),
    };
    // Reuse the connection loop over in-memory pipes: same semantics.
    let lines: Vec<Value> = match v {
        Value::Array(xs) => xs,
        v => vec![v],
    };
    let batch = lines.len() > 1;
    let mut input = Vec::new();
    for l in &lines {
        input.extend(serde_json::to_vec(l).unwrap_or_default());
        input.push(b'\n');
    }
    let (client, server) = tokio::io::duplex(rpc::MAX_LINE);
    let (sr, sw) = tokio::io::split(server);
    let (_tx, rx) = watch::channel(false);
    let task = tokio::spawn(serve_connection(sr, sw, handler, caller, opts, rx));
    let (mut cr, mut cw) = tokio::io::split(client);
    use tokio::io::AsyncWriteExt;
    let w = async move {
        let _ = cw.write_all(&input).await;
        let _ = cw.shutdown().await;
    };
    let r = async move {
        let mut out = Vec::new();
        let mut br = BufReader::new(&mut cr);
        while let Ok(Line::Msg(v)) = rpc::read_line(&mut br, rpc::MAX_LINE).await {
            out.push(v);
        }
        out
    };
    let ((), out) = tokio::join!(w, r);
    let _ = task.await;
    match (batch, out.len()) {
        (_, 0) => None,
        (false, _) => out.into_iter().next(),
        (true, _) => Some(Value::Array(out)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncBufReadExt;
    use tokio::io::AsyncWriteExt;

    struct Echo;

    #[async_trait]
    impl Handler for Echo {
        fn info(&self) -> InitializeResult {
            InitializeResult { protocol: String::new(), kind: "rightsizer".into(), version: "test".into(), methods: vec!["echo".into()], native_media_types: vec![], concurrency: Some(4) }
        }
        async fn call(&self, m: &str, params: Value, ctx: &Ctx) -> Result<Value, RpcError> {
            match m {
                "echo" => Ok(serde_json::json!({"params": params, "uid": ctx.caller.uid})),
                "sleep" => {
                    let ms = params.get("ms").and_then(Value::as_u64).unwrap_or(100);
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                    Ok(Value::from(ms))
                }
                "panic" => panic!("boom"),
                other => Err(method_not_found(other)),
            }
        }
    }

    async fn roundtrip(lines: &[&str], expect: usize) -> Vec<Value> {
        let (client, server) = tokio::io::duplex(1 << 20);
        let (sr, sw) = tokio::io::split(server);
        let (_tx, rx) = watch::channel(false);
        let t = tokio::spawn(serve_connection(sr, sw, Arc::new(Echo), Caller { uid: Some(7), gid: None, pid: None, transport: Transport::Memory }, ServerOptions::default(), rx));
        let (cr, mut cw) = tokio::io::split(client);
        for l in lines {
            cw.write_all(l.as_bytes()).await.unwrap();
            cw.write_all(b"\n").await.unwrap();
        }
        let mut out = Vec::new();
        let mut br = tokio::io::BufReader::new(cr);
        while out.len() < expect {
            let mut s = String::new();
            if br.read_line(&mut s).await.unwrap() == 0 {
                break;
            }
            out.push(serde_json::from_str(&s).unwrap());
        }
        drop(cw);
        let _ = tokio::time::timeout(Duration::from_secs(2), t).await;
        out
    }

    #[tokio::test]
    async fn initialize_negotiates_and_rejects() {
        let out = roundtrip(
            &[
                r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocol":"rightsizer-plugin/2","protocols":["rightsizer-plugin/1"]}}"#,
                r#"{"jsonrpc":"2.0","id":2,"method":"initialize","params":{"protocol":"rightsizer-plugin/9"}}"#,
            ],
            2,
        )
        .await;
        assert_eq!(out[0]["result"]["protocol"], "rightsizer-plugin/1");
        assert_eq!(out[0]["result"]["kind"], "rightsizer");
        assert_eq!(out[1]["error"]["data"]["code"], "unsupported");
    }

    #[tokio::test]
    async fn concurrency_cancel_deadline_panic_and_errors() {
        let out = roundtrip(
            &[
                r#"{"jsonrpc":"2.0","id":"slow","method":"sleep","params":{"ms":400}}"#,
                r#"{"jsonrpc":"2.0","id":"fast","method":"echo","params":{"x":1}}"#,
                r#"{"jsonrpc":"2.0","method":"$/cancel","params":{"id":"slow"}}"#,
                r#"{"jsonrpc":"2.0","id":"dl","method":"sleep","params":{"ms":500,"deadline_ms":50}}"#,
                r#"{"jsonrpc":"2.0","id":"p","method":"panic"}"#,
                r#"{"jsonrpc":"2.0","id":"u","method":"nope"}"#,
                r#"garbage"#,
            ],
            6,
        )
        .await;
        let by = |id: &str| out.iter().find(|v| v["id"] == id).cloned().unwrap_or_default();
        assert_eq!(by("fast")["result"]["uid"], 7);
        assert_eq!(by("slow")["error"]["code"], codes::CANCELLED);
        assert_eq!(by("dl")["error"]["code"], codes::DEADLINE);
        assert_eq!(by("dl")["error"]["data"]["retryable"], true);
        assert_eq!(by("p")["error"]["data"]["code"], "internal");
        assert_eq!(by("u")["error"]["code"], codes::METHOD_NOT_FOUND);
        assert!(out.iter().any(|v| v["error"]["code"] == codes::PARSE));
        // The fast request was not blocked behind the slow or the 50 ms one.
        let pos = |id: &str| out.iter().position(|v| v["id"] == id).unwrap();
        assert!(pos("fast") < pos("dl"));
    }

    #[tokio::test]
    async fn unix_socket_peer_credentials_and_shutdown() {
        let dir = tempfile_dir();
        let path = dir.join("d.sock");
        let l = bind_unix(&path, 0o666).unwrap();
        let (tx, rx) = watch::channel(false);
        let srv = tokio::spawn(serve_unix(l, path.clone(), Arc::new(Echo), ServerOptions::default(), rx, Duration::from_secs(2)));
        let mut s = tokio::net::UnixStream::connect(&path).await.unwrap();
        s.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"echo\"}\n").await.unwrap();
        let mut br = tokio::io::BufReader::new(s);
        let mut line = String::new();
        br.read_line(&mut line).await.unwrap();
        let v: Value = serde_json::from_str(&line).unwrap();
        let me = std::process::Command::new("id").arg("-u").output().unwrap();
        let me: u32 = String::from_utf8_lossy(&me.stdout).trim().parse().unwrap();
        assert_eq!(v["result"]["uid"], me);
        // A second bind on a live socket fails; shutdown removes the file.
        assert!(bind_unix(&path, 0o666).is_err());
        tx.send(true).unwrap();
        drop(br);
        tokio::time::timeout(Duration::from_secs(3), srv).await.unwrap().unwrap();
        assert!(!path.exists());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn http_bodies() {
        let v = serve_body(br#"{"jsonrpc":"2.0","id":3,"method":"echo","params":{"a":1}}"#, Arc::new(Echo), Caller::anonymous(Transport::Http), ServerOptions::default()).await.unwrap();
        assert_eq!(v["result"]["params"]["a"], 1);
        let b = serve_body(br#"[{"jsonrpc":"2.0","id":1,"method":"echo"},{"jsonrpc":"2.0","id":2,"method":"echo"}]"#, Arc::new(Echo), Caller::anonymous(Transport::Http), ServerOptions::default()).await.unwrap();
        assert_eq!(b.as_array().unwrap().len(), 2);
        assert!(serve_body(br#"{"jsonrpc":"2.0","method":"echo"}"#, Arc::new(Echo), Caller::anonymous(Transport::Http), ServerOptions::default()).await.is_none());
    }

    fn tempfile_dir() -> PathBuf {
        // Short path: macOS limits sun_path to 104 bytes.
        let d = PathBuf::from(format!("/tmp/rsz-srv-{}", ulid::Ulid::generate()));
        std::fs::create_dir_all(&d).unwrap();
        d
    }
}
