//! Newline-delimited JSON-RPC 2.0 framing (plugins.md §3.1) and the
//! protocol-version rules shared by the client and the server.
//!
//! - One message per line; a line is at most [`MAX_LINE`] bytes (longer
//!   lines are a `parse_error`, and the reader resynchronizes on the next
//!   newline).
//! - Versions are `rightsizer-plugin/<major>`. Both sides speak the current
//!   major and the previous one (SPEC §13.2 N/N-1, [`SUPPORTED_MAJORS`]);
//!   [`negotiate`] picks the highest common version.

use serde::Serialize;
use serde_json::Value;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

use crate::protocol::{ErrorCode, Id, PROTOCOL, Request, Response, RpcError};

/// Largest accepted line (16 MiB: a 4 MiB script, base64, plus decisions).
pub const MAX_LINE: usize = 16 << 20;

/// Protocol name prefix.
pub const PROTOCOL_PREFIX: &str = "rightsizer-plugin/";
/// The current major version.
pub const CURRENT_MAJOR: u32 = 1;
/// Majors spoken: current and previous (N/N-1). Major 0 never shipped, so
/// today only 1 is accepted; the rule is what [`negotiate`] implements.
pub const SUPPORTED_MAJORS: &[u32] = &[CURRENT_MAJOR];

/// JSON-RPC codes used besides the protocol's application code `-32000`.
pub mod codes {
    /// Invalid JSON.
    pub const PARSE: i64 = -32700;
    /// Not a valid request object.
    pub const INVALID_REQUEST: i64 = -32600;
    /// Unknown method.
    pub const METHOD_NOT_FOUND: i64 = -32601;
    /// Bad params.
    pub const INVALID_PARAMS: i64 = -32602;
    /// Cancelled by `$/cancel` (LSP's code).
    pub const CANCELLED: i64 = -32800;
    /// `deadline_ms` expired.
    pub const DEADLINE: i64 = -32001;
    /// The caller may not act for this tenant.
    pub const FORBIDDEN: i64 = -32003;
    /// No common protocol version.
    pub const VERSION: i64 = -32004;
}

/// Parse `rightsizer-plugin/<major>[.<minor>]` into the major.
pub fn major_of(protocol: &str) -> Option<u32> {
    let rest = protocol.strip_prefix(PROTOCOL_PREFIX)?;
    rest.split('.').next()?.parse().ok()
}

/// The protocol string of a major.
pub fn protocol_of(major: u32) -> String {
    format!("{PROTOCOL_PREFIX}{major}")
}

/// Pick the highest version both sides speak. `offered` is the peer's
/// preferred version plus any alternatives it lists.
pub fn negotiate<'a>(offered: impl IntoIterator<Item = &'a str>) -> Option<String> {
    let mut best: Option<u32> = None;
    for p in offered {
        if let Some(m) = major_of(p)
            && SUPPORTED_MAJORS.contains(&m)
            && best.is_none_or(|b| m > b)
        {
            best = Some(m);
        }
    }
    best.map(protocol_of)
}

/// The versions this side offers, preferred first.
pub fn offered_protocols() -> Vec<String> {
    let mut v: Vec<String> = SUPPORTED_MAJORS.iter().rev().map(|m| protocol_of(*m)).collect();
    if v.is_empty() {
        v.push(PROTOCOL.into());
    }
    v
}

/// Error helpers.
pub fn err(jsonrpc_code: i64, code: ErrorCode, message: impl Into<String>, retryable: bool) -> RpcError {
    RpcError { code: jsonrpc_code, message: message.into(), data: Some(crate::protocol::ErrorData { code, retryable }) }
}

/// A line read from the peer.
#[derive(Debug)]
pub enum Line {
    /// A message (request or response).
    Msg(Value),
    /// Not JSON / too long: answer with a parse error.
    Bad(String),
    /// End of stream.
    Eof,
}

/// Read one line (without the newline). Over-long lines are skipped up to
/// the next newline and reported as [`Line::Bad`].
pub async fn read_line<R: AsyncBufRead + Unpin>(r: &mut R, max: usize) -> std::io::Result<Line> {
    let mut buf = Vec::new();
    loop {
        let avail = r.fill_buf().await?;
        if avail.is_empty() {
            return Ok(if buf.is_empty() { Line::Eof } else { parse_line(&buf) });
        }
        if let Some(i) = avail.iter().position(|b| *b == b'\n') {
            let take = buf.len() + i <= max;
            if take {
                buf.extend_from_slice(&avail[..i]);
            }
            r.consume(i + 1);
            if !take {
                return Ok(Line::Bad(format!("line longer than {max} bytes")));
            }
            if buf.iter().all(u8::is_ascii_whitespace) {
                buf.clear();
                continue;
            }
            return Ok(parse_line(&buf));
        }
        let n = avail.len();
        if buf.len() + n > max {
            r.consume(n);
            // Skip to the next newline.
            loop {
                let a = r.fill_buf().await?;
                if a.is_empty() {
                    return Ok(Line::Bad(format!("line longer than {max} bytes")));
                }
                if let Some(i) = a.iter().position(|b| *b == b'\n') {
                    r.consume(i + 1);
                    return Ok(Line::Bad(format!("line longer than {max} bytes")));
                }
                let n = a.len();
                r.consume(n);
            }
        }
        buf.extend_from_slice(avail);
        r.consume(n);
    }
}

fn parse_line(buf: &[u8]) -> Line {
    let trimmed = buf.strip_suffix(b"\r").unwrap_or(buf);
    match serde_json::from_slice::<Value>(trimmed) {
        Ok(v) => Line::Msg(v),
        Err(e) => Line::Bad(e.to_string()),
    }
}

/// Write one message as a line and flush.
pub async fn write_msg<W: AsyncWrite + Unpin, T: Serialize>(w: &mut W, msg: &T) -> std::io::Result<()> {
    let mut line = serde_json::to_vec(msg).map_err(std::io::Error::other)?;
    line.push(b'\n');
    w.write_all(&line).await?;
    w.flush().await
}

/// A parsed incoming message.
#[derive(Debug)]
pub enum Incoming {
    /// A request or notification.
    Request(Request),
    /// A response to one of our requests.
    Response(Response),
    /// Neither: answer with `invalid_request` (when it has an id).
    Invalid(Option<Id>, String),
}

/// Classify a JSON value.
pub fn classify(v: Value) -> Incoming {
    let id = v.get("id").and_then(|i| serde_json::from_value::<Id>(i.clone()).ok());
    if v.get("method").is_some() {
        return match serde_json::from_value::<Request>(v) {
            Ok(r) if r.jsonrpc == crate::protocol::JSONRPC => Incoming::Request(r),
            Ok(_) => Incoming::Invalid(id, "jsonrpc must be \"2.0\"".into()),
            Err(e) => Incoming::Invalid(id, e.to_string()),
        };
    }
    if v.get("result").is_some() || v.get("error").is_some() {
        return match serde_json::from_value::<Response>(v) {
            Ok(r) => Incoming::Response(r),
            Err(e) => Incoming::Invalid(id, e.to_string()),
        };
    }
    Incoming::Invalid(id, "neither a request nor a response".into())
}

/// `deadline_ms` from a request's params, if any.
pub fn deadline_of(params: &Value) -> Option<u32> {
    params.get("deadline_ms").and_then(Value::as_u64).map(|d| d.min(u32::MAX as u64) as u32)
}

/// Set `deadline_ms` on params (objects only).
pub fn with_deadline(mut params: Value, deadline_ms: u32) -> Value {
    if let Some(o) = params.as_object_mut() {
        o.insert("deadline_ms".into(), Value::from(deadline_ms));
    }
    params
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_negotiation() {
        assert_eq!(major_of("rightsizer-plugin/1"), Some(1));
        assert_eq!(major_of("rightsizer-plugin/2.3"), Some(2));
        assert_eq!(major_of("other/1"), None);
        assert_eq!(negotiate(["rightsizer-plugin/1"]).as_deref(), Some("rightsizer-plugin/1"));
        assert_eq!(negotiate(["rightsizer-plugin/2", "rightsizer-plugin/1"]).as_deref(), Some("rightsizer-plugin/1"));
        assert_eq!(negotiate(["rightsizer-plugin/2"]), None);
        assert_eq!(negotiate(["rightsizer-plugin/0"]), None);
        assert_eq!(offered_protocols(), vec!["rightsizer-plugin/1".to_string()]);
    }

    #[tokio::test]
    async fn lines_and_limits() {
        let data = b"{\"a\":1}\n\n  \nnot json\n{\"b\":2}\r\n0123456789012345678901234567890\n{\"c\":3}".to_vec();
        let mut r = tokio::io::BufReader::with_capacity(8, &data[..]);
        assert!(matches!(read_line(&mut r, 20).await.unwrap(), Line::Msg(v) if v["a"] == 1));
        assert!(matches!(read_line(&mut r, 20).await.unwrap(), Line::Bad(_)));
        assert!(matches!(read_line(&mut r, 20).await.unwrap(), Line::Msg(v) if v["b"] == 2));
        assert!(matches!(read_line(&mut r, 20).await.unwrap(), Line::Bad(m) if m.contains("longer")));
        assert!(matches!(read_line(&mut r, 20).await.unwrap(), Line::Msg(v) if v["c"] == 3));
        assert!(matches!(read_line(&mut r, 20).await.unwrap(), Line::Eof));
    }

    #[test]
    fn classify_messages() {
        assert!(matches!(classify(serde_json::json!({"jsonrpc":"2.0","id":1,"method":"status"})), Incoming::Request(_)));
        assert!(matches!(classify(serde_json::json!({"jsonrpc":"2.0","method":"$/cancel","params":{"id":1}})), Incoming::Request(r) if r.id.is_none()));
        assert!(matches!(classify(serde_json::json!({"jsonrpc":"2.0","id":1,"result":{}})), Incoming::Response(_)));
        assert!(matches!(classify(serde_json::json!({"jsonrpc":"1.0","id":1,"method":"x"})), Incoming::Invalid(Some(_), _)));
        assert!(matches!(classify(serde_json::json!({"x":1})), Incoming::Invalid(None, _)));
    }
}
