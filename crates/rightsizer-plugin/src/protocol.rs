//! `rightsizer-plugin/1` (plugins.md §3): JSON-RPC 2.0, newline-delimited,
//! over stdio (core → plugin) or a Unix socket / HTTP (shim → daemon).
//!
//! | direction | methods |
//! |---|---|
//! | core → plugin | `initialize`, `extract`, `render`, `validate`, `reference`, `state`, `fetch_outcomes`, `shutdown` |
//! | shim → core | `initialize`, `decide`, `outcome`, `status` |
//!
//! Every request's params may carry `deadline_ms`; on expiry the core sends
//! `$/cancel` and falls back to the prior. Errors are JSON-RPC error objects
//! with `data: {code, retryable}`.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use rightsizer_core::breaker::BreakerState;
use rightsizer_core::{Decision, Mode, PlatformRef, PlatformState, RunRecord, Tenant, UnitId, WorkloadKey};
use serde::{Deserialize, Serialize};

use crate::{AdapterEnv, Extracted, Native, Patch, Rejection, ResourceSpec, Unit};

/// The protocol id.
pub const PROTOCOL: &str = "rightsizer-plugin/1";
/// JSON-RPC version string.
pub const JSONRPC: &str = "2.0";

/// Method names.
pub mod method {
    /// Handshake (both directions).
    pub const INITIALIZE: &str = "initialize";
    /// core → plugin: native → units.
    pub const EXTRACT: &str = "extract";
    /// core → plugin: decisions → patch.
    pub const RENDER: &str = "render";
    /// core → plugin: check a patch.
    pub const VALIDATE: &str = "validate";
    /// core → plugin: static platform facts for a unit.
    pub const REFERENCE: &str = "reference";
    /// core → plugin: live platform state.
    pub const STATE: &str = "state";
    /// core → plugin: run records.
    pub const FETCH_OUTCOMES: &str = "fetch_outcomes";
    /// core → plugin: exit.
    pub const SHUTDOWN: &str = "shutdown";
    /// shim → core: the composite decision.
    pub const DECIDE: &str = "decide";
    /// shim → core: report an outcome.
    pub const OUTCOME: &str = "outcome";
    /// shim → core: daemon status.
    pub const STATUS: &str = "status";
    /// Cancel an in-flight request.
    pub const CANCEL: &str = "$/cancel";
    /// shim → core (extension): open or close a ladder lease (`rsz slurm
    /// sbatch --watch` run as a user; SPEC §7.2).
    pub const LEASE: &str = "lease";
    /// shim → core (extension): sign a ladder resubmission of an earlier
    /// enforced decision (Slurm; the daemon checks it is a ladder step).
    pub const SIGN: &str = "sign";
}

/// A request id.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(untagged)]
pub enum Id {
    /// Numeric.
    Num(i64),
    /// String.
    Str(String),
}

/// A JSON-RPC request (a notification when `id` is absent).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Request {
    /// `"2.0"`.
    pub jsonrpc: String,
    /// Id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<Id>,
    /// Method.
    pub method: String,
    /// Params.
    #[serde(default)]
    pub params: serde_json::Value,
}

impl Request {
    /// A request with typed params.
    pub fn new<P: Serialize>(id: i64, method: &str, params: &P) -> Self {
        Request { jsonrpc: JSONRPC.into(), id: Some(Id::Num(id)), method: method.into(), params: serde_json::to_value(params).unwrap_or_default() }
    }
}

/// A JSON-RPC response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Response {
    /// `"2.0"`.
    pub jsonrpc: String,
    /// Id of the request.
    pub id: Option<Id>,
    /// Result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    /// Error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

impl Response {
    /// A success.
    pub fn ok<R: Serialize>(id: Option<Id>, result: &R) -> Self {
        Response { jsonrpc: JSONRPC.into(), id, result: Some(serde_json::to_value(result).unwrap_or_default()), error: None }
    }
    /// A failure.
    pub fn err(id: Option<Id>, error: RpcError) -> Self {
        Response { jsonrpc: JSONRPC.into(), id, result: None, error: Some(error) }
    }
}

/// A JSON-RPC error object.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct RpcError {
    /// JSON-RPC code (`-32700` parse, `-32601` method not found, `-32000` application).
    pub code: i64,
    /// Message.
    pub message: String,
    /// `{code, retryable}`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<ErrorData>,
}

impl RpcError {
    /// An application error with protocol data.
    pub fn app(code: ErrorCode, message: impl Into<String>, retryable: bool) -> Self {
        RpcError { code: -32000, message: message.into(), data: Some(ErrorData { code, retryable }) }
    }
}

impl From<&crate::PluginError> for RpcError {
    fn from(e: &crate::PluginError) -> Self {
        let retryable = matches!(e, crate::PluginError::PlatformUnavailable(_));
        RpcError::app(e.code(), e.to_string(), retryable)
    }
}

/// Error data (plugins.md §3.3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ErrorData {
    /// Protocol error code.
    pub code: ErrorCode,
    /// Whether a retry may succeed.
    pub retryable: bool,
}

/// Protocol error codes (plugins.md §3.3).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// Malformed message or native.
    ParseError,
    /// Not handled.
    Unsupported,
    /// Invalid native submission.
    InvalidNative,
    /// The platform could not be reached.
    PlatformUnavailable,
    /// Anything else.
    Internal,
}

/// `initialize` params.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct InitializeParams {
    /// `"rightsizer-plugin/1"`.
    pub protocol: String,
    /// Schema versions spoken (`{"context": "1", "decision": "1", "run": "1"}`).
    #[serde(default)]
    pub schemas: BTreeMap<String, String>,
    /// Feature schema (`"features-v1"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub features: Option<String>,
    /// Extension: other versions the caller also speaks (N/N-1), besides
    /// `protocol` (its preferred one).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub protocols: Vec<String>,
}

impl Default for InitializeParams {
    fn default() -> Self {
        InitializeParams {
            protocol: PROTOCOL.into(),
            schemas: [("context", "1"), ("decision", "1"), ("run", "1")].into_iter().map(|(k, v)| (k.into(), v.into())).collect(),
            features: Some("features-v1".into()),
            protocols: Vec::new(),
        }
    }
}

/// `initialize` result.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct InitializeResult {
    /// Protocol spoken.
    pub protocol: String,
    /// Platform kind (plugins) or `"rightsizer"` (daemon).
    pub kind: String,
    /// Implementation version.
    pub version: String,
    /// Methods implemented.
    #[serde(default)]
    pub methods: Vec<String>,
    /// Native media types accepted.
    #[serde(default)]
    pub native_media_types: Vec<String>,
    /// Concurrent requests per pipe (default 1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency: Option<u32>,
}

/// `extract` params.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ExtractParams {
    /// The submission.
    pub native: Native,
    /// Hosting environment.
    #[serde(default)]
    pub env: AdapterEnv,
    /// Deadline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_ms: Option<u32>,
}

/// `extract` result.
pub type ExtractResult = Extracted;

/// One unit's decision for `render`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct UnitSpec {
    /// Unit.
    pub unit: UnitId,
    /// Spec to render.
    pub spec: ResourceSpec,
}

/// `render` params.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct RenderParams {
    /// The submission.
    pub native: Native,
    /// Decisions.
    pub decisions: Vec<UnitSpec>,
    /// Deadline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_ms: Option<u32>,
}

/// `render` result.
pub type RenderResult = Patch;

/// `validate` params.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ValidateParams {
    /// The submission.
    pub native: Native,
    /// The patch.
    pub patch: Patch,
    /// Hosting environment.
    #[serde(default)]
    pub env: AdapterEnv,
    /// Deadline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_ms: Option<u32>,
}

/// `validate` result.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ValidateResult {
    /// Accepted.
    pub ok: bool,
    /// Why not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rejection: Option<Rejection>,
}

/// `reference` params.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ReferenceParams {
    /// The unit.
    pub unit: Unit,
}

/// `reference` result.
pub type ReferenceResult = PlatformRef;

/// `state` params.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct StateParams {
    /// Tenant.
    pub tenant: Tenant,
    /// Hosting environment.
    #[serde(default)]
    pub env: AdapterEnv,
    /// Deadline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_ms: Option<u32>,
}

/// `state` result.
pub type StateResult = PlatformState;

/// `fetch_outcomes` params.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct FetchOutcomesParams {
    /// Records since.
    pub since: DateTime<Utc>,
    /// Only these keys.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keys: Option<Vec<WorkloadKey>>,
    /// Continuation cursor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    /// Page size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

/// `fetch_outcomes` result (paged).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct FetchOutcomesResult {
    /// Records.
    pub records: Vec<RunRecord>,
    /// Next page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// `decide` params (shim → daemon).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct DecideParams {
    /// Adapter kind (built-in or plugin).
    pub kind: String,
    /// The submission.
    pub native: Native,
    /// Requested mode (capped by policy, breakers and `mode_ceiling`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<Mode>,
    /// Hosting environment. Over the daemon socket, `principal` and
    /// `namespace` are ignored: the daemon derives them from `SO_PEERCRED`.
    #[serde(default)]
    pub env: AdapterEnv,
    /// Deadline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_ms: Option<u32>,
    /// Extension: the attempt being sized (escalate-on-retry, SPEC §7.2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt: Option<rightsizer_core::AttemptContext>,
    /// Extension: a `Caller` ladder drives this submission (`rsz slurm
    /// sbatch --watch`), so reductions may rest on S3(a).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub watch: bool,
    /// Extension: decide but do not record.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub dry_run: bool,
}

/// `decide` result.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct DecideResult {
    /// One per unit.
    pub decisions: Vec<Decision>,
    /// The patch to apply (`none` below enforce).
    pub patch: Patch,
    /// Effective mode.
    pub mode: Mode,
    /// One-line human explanation.
    pub summary: String,
    /// Signed attribution token (slurm.md §4.1), Slurm batch submissions only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
}

/// `outcome` params.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct OutcomeParams {
    /// The record.
    pub record: RunRecord,
}

/// `outcome` result.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct OutcomeResult {
    /// Accepted (buffered or written).
    pub accepted: bool,
    /// Already recorded.
    #[serde(default)]
    pub duplicate: bool,
}

/// `status` result.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct StatusResult {
    /// Protocol.
    pub protocol: String,
    /// Daemon version.
    pub version: String,
    /// Fleet `mode_ceiling`, if readable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode_ceiling: Option<Mode>,
    /// Fleet breaker.
    pub fleet_breaker: BreakerState,
    /// Store reachable.
    pub store_reachable: bool,
    /// Loaded plugins by kind.
    #[serde(default)]
    pub plugins: Vec<String>,
    /// Extension: adapters served (built-in and plugins).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub kinds: Vec<String>,
    /// Extension: requests served since start.
    #[serde(default)]
    pub requests: u64,
    /// Extension: `decide` latency over the last requests, ms (p50, p99, max, count).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decide_latency_ms: Option<LatencySummary>,
    /// Extension: the caller as the daemon sees it (uid, tenants).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caller: Option<serde_json::Value>,
}

/// Latency percentiles.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct LatencySummary {
    /// Samples.
    pub count: u64,
    /// Median, ms.
    pub p50: f64,
    /// 99th percentile, ms.
    pub p99: f64,
    /// Max, ms.
    pub max: f64,
}

/// `lease` params (extension).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct LeaseParams {
    /// `open` (write the attempt record with an open lease if absent) or `close`.
    pub action: LeaseAction,
    /// The attempt record (open).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record: Option<rightsizer_core::AttemptRecord>,
    /// Key (close).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "schema", schemars(with = "Option<String>"))]
    pub key: Option<WorkloadKey>,
    /// Invocation (close).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invocation_id: Option<String>,
    /// Final status (close).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<rightsizer_core::RunStatus>,
}

/// Lease actions.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum LeaseAction {
    /// Open.
    Open,
    /// Close.
    Close,
}

/// `sign` params (extension).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct SignParams {
    /// Adapter kind (`slurm`).
    pub kind: String,
    /// The resubmission, already escalated.
    pub native: Native,
    /// The decisions it escalates.
    #[cfg_attr(feature = "schema", schemars(with = "Vec<String>"))]
    pub decision_ids: Vec<ulid::Ulid>,
}

/// `sign` result (extension).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct SignResult {
    /// The token (per component, joined with `.`).
    pub token: Option<String>,
}

/// `$/cancel` params.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct CancelParams {
    /// The request to cancel.
    pub id: Id,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handshake_matches_plugins_md() {
        let req = Request::new(1, method::INITIALIZE, &InitializeParams::default());
        let j = serde_json::to_value(&req).unwrap();
        assert_eq!(
            j,
            serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize",
                "params":{"protocol":"rightsizer-plugin/1","schemas":{"context":"1","decision":"1","run":"1"},"features":"features-v1"}})
        );
        let resp: Response = serde_json::from_str(
            r#"{"jsonrpc":"2.0","id":1,"result":{"protocol":"rightsizer-plugin/1","kind":"nomad","version":"0.3.0",
               "methods":["extract","render","validate","reference","state"],"native_media_types":["application/hcl","application/json"]}}"#,
        )
        .unwrap();
        let r: InitializeResult = serde_json::from_value(resp.result.unwrap()).unwrap();
        assert_eq!(r.kind, "nomad");
        assert_eq!(r.concurrency, None);
    }

    #[test]
    fn errors_carry_code_and_retryable() {
        let e = RpcError::from(&crate::PluginError::PlatformUnavailable("api down".into()));
        let j = serde_json::to_value(Response::err(Some(Id::Str("x".into())), e)).unwrap();
        assert_eq!(j["error"]["data"], serde_json::json!({"code": "platform_unavailable", "retryable": true}));
        assert_eq!(j["id"], "x");
    }
}
