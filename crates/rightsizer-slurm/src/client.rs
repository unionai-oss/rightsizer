//! `rsz slurm decide-client`: the helper `cli_filter.lua` runs through
//! `io.popen`, because Slurm's Lua has no socket library (the 26.05 image
//! has neither luaposix nor luasocket).
//!
//! It reads one JSON-RPC 2.0 request from stdin, adds the script text when
//! the request names a script path (cli_filter runs as the user, so the
//! helper can read it; the daemon cannot), sends the request as one line to
//! the daemon's Unix socket, and prints the one-line response. The whole
//! exchange has one deadline (1 s by default): connect, write and read all
//! count against it. Any failure prints nothing and exits non-zero, which
//! cli_filter treats as "daemon unavailable" (S1, negative cache).

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde_json::Value;

/// Largest script sent (Slurm's default `max_script_size` is 4 MiB).
pub const MAX_SCRIPT: u64 = 4 << 20;
/// Largest response accepted.
pub const MAX_RESPONSE: usize = 8 << 20;

/// Client errors.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// I/O (connect, read, write, timeout).
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// Deadline passed.
    #[error("deadline exceeded")]
    Deadline,
    /// The request is not JSON-RPC.
    #[error("bad request: {0}")]
    BadRequest(String),
    /// The response is not one JSON line.
    #[error("bad response: {0}")]
    BadResponse(String),
}

/// Add `script` to a cli_filter `decide` request from `script_path`
/// (relative paths resolve against the request's `cwd`).
pub fn enrich_request(req: &mut Value) {
    let Some(v) = req.pointer_mut("/params/native/value") else { return };
    if v.get("kind").and_then(Value::as_str) != Some("cli_filter") || v.get("script").is_some_and(|s| !s.is_null()) {
        return;
    }
    let Some(path) = v.get("script_path").and_then(Value::as_str).filter(|p| !p.is_empty()) else { return };
    let mut p = PathBuf::from(path);
    if p.is_relative()
        && let Some(cwd) = v.get("cwd").and_then(Value::as_str)
    {
        p = Path::new(cwd).join(p);
    }
    let Ok(f) = std::fs::File::open(&p) else { return };
    let mut s = String::new();
    if f.take(MAX_SCRIPT).read_to_string(&mut s).is_ok()
        && let Some(o) = v.as_object_mut()
    {
        o.insert("script".into(), Value::String(s));
    }
}

/// One request/response exchange with the daemon.
pub fn exchange(socket: &Path, timeout: Duration, request: &[u8]) -> Result<Vec<u8>, ClientError> {
    let deadline = Instant::now() + timeout;
    let left = || deadline.checked_duration_since(Instant::now()).filter(|d| !d.is_zero()).ok_or(ClientError::Deadline);
    // connect(2) on a Unix socket blocks while the listener's backlog is
    // full (a wedged or stopped daemon), and std has no connect timeout:
    // connect on a thread and stop waiting at the deadline, so `sbatch`
    // is never held up for longer than the deadline (S1). The helper exits
    // right after, taking the thread with it.
    let (tx, rx) = std::sync::mpsc::channel();
    let path = socket.to_path_buf();
    std::thread::spawn(move || {
        let _ = tx.send(UnixStream::connect(path));
    });
    let mut s = match rx.recv_timeout(left()?) {
        Ok(r) => r?,
        Err(_) => return Err(ClientError::Deadline),
    };
    s.set_write_timeout(Some(left()?))?;
    let mut line = request.to_vec();
    while line.last().is_some_and(|b| *b == b'\n' || *b == b'\r') {
        line.pop();
    }
    if line.contains(&b'\n') {
        // Newline-delimited JSON: re-encode compactly.
        let v: Value = serde_json::from_slice(&line).map_err(|e| ClientError::BadRequest(e.to_string()))?;
        line = serde_json::to_vec(&v).map_err(|e| ClientError::BadRequest(e.to_string()))?;
    }
    line.push(b'\n');
    s.write_all(&line)?;
    s.flush()?;
    let mut out = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        s.set_read_timeout(Some(left()?))?;
        let n = match s.read(&mut buf) {
            Ok(n) => n,
            Err(e) if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut) => return Err(ClientError::Deadline),
            Err(e) => return Err(e.into()),
        };
        if n == 0 {
            break;
        }
        out.extend_from_slice(&buf[..n]);
        if let Some(i) = out.iter().position(|b| *b == b'\n') {
            out.truncate(i);
            break;
        }
        if out.len() > MAX_RESPONSE {
            return Err(ClientError::BadResponse("response too large".into()));
        }
    }
    serde_json::from_slice::<Value>(&out).map_err(|e| ClientError::BadResponse(e.to_string()))?;
    Ok(out)
}

/// The whole helper: read the request, enrich it, exchange, return the
/// response line.
pub fn run(socket: &Path, timeout: Duration, stdin: impl Read) -> Result<Vec<u8>, ClientError> {
    let mut raw = Vec::new();
    stdin.take(MAX_SCRIPT * 2).read_to_end(&mut raw)?;
    let mut req: Value = serde_json::from_slice(&raw).map_err(|e| ClientError::BadRequest(e.to_string()))?;
    if req.get("method").is_none() {
        return Err(ClientError::BadRequest("not a JSON-RPC request".into()));
    }
    enrich_request(&mut req);
    let body = serde_json::to_vec(&req).map_err(|e| ClientError::BadRequest(e.to_string()))?;
    exchange(socket, timeout, &body)
}

#[cfg(test)]
mod tests {
    use std::io::BufRead;
    use std::os::unix::net::UnixListener;

    use super::*;

    #[test]
    fn exchange_with_a_fake_daemon_and_enrichment() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("d.sock");
        let l = UnixListener::bind(&sock).unwrap();
        std::fs::write(dir.path().join("job.sh"), "#!/bin/sh\n#SBATCH --mem=1G\n").unwrap();
        let h = std::thread::spawn(move || {
            let (s, _) = l.accept().unwrap();
            let mut r = std::io::BufReader::new(s.try_clone().unwrap());
            let mut line = String::new();
            r.read_line(&mut line).unwrap();
            let v: Value = serde_json::from_str(&line).unwrap();
            let script = v.pointer("/params/native/value/script").and_then(Value::as_str).unwrap_or("").to_string();
            let mut w = s;
            writeln!(w, "{}", serde_json::json!({"jsonrpc":"2.0","id":1,"result":{"echo": script}})).unwrap();
        });
        let req = serde_json::json!({"jsonrpc":"2.0","id":1,"method":"decide","params":{"kind":"slurm","native":{"kind":"object","value":{
            "kind":"cli_filter","type":"sbatch","offset":0,"options":{},"script_path":"job.sh","cwd": dir.path().to_str().unwrap()}}}});
        let out = run(&sock, Duration::from_secs(2), serde_json::to_vec(&req).unwrap().as_slice()).unwrap();
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["result"]["echo"], "#!/bin/sh\n#SBATCH --mem=1G\n");
        h.join().unwrap();
    }

    #[test]
    fn deadline_and_missing_socket() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("slow.sock");
        let l = UnixListener::bind(&sock).unwrap();
        let h = std::thread::spawn(move || {
            let (_s, _) = l.accept().unwrap();
            std::thread::sleep(Duration::from_millis(500));
        });
        let t = Instant::now();
        let e = exchange(&sock, Duration::from_millis(150), br#"{"jsonrpc":"2.0","id":1,"method":"status"}"#).unwrap_err();
        assert!(matches!(e, ClientError::Deadline), "{e}");
        assert!(t.elapsed() < Duration::from_millis(450));
        h.join().unwrap();
        assert!(exchange(&dir.path().join("none.sock"), Duration::from_millis(100), b"{}").is_err());
        assert!(run(&sock, Duration::from_millis(100), &b"not json"[..]).is_err());
    }
}
