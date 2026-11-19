//! Estimator Protocol v1 client (SPEC §6.3), feature `remote`.
//!
//! `GET /v1/info`, `POST /v1/estimate`, `POST /v1/estimate:batch`,
//! `GET /v1/health`; bearer token or mTLS; 10 s default deadline, one jittered
//! retry on retryable errors, a client circuit breaker (5 failures, 30 s
//! half-open), single-flight and 20 ms coalescing. `422` maps to
//! [`EstimateError::Insufficient`](crate::EstimateError::Insufficient).
//!
//! **Stub:** wire types only; the client lands next round.

use serde::{Deserialize, Serialize};

/// `POST /v1/estimate` body.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EstimateRequest {
    /// The (privacy-filtered) context.
    pub context: rightsizer_core::WorkloadContext,
    /// Axes requested.
    pub axes: Vec<crate::EstimateAxis>,
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

/// Client settings.
#[derive(Clone, Debug)]
pub struct RemoteConfig {
    /// Base URL (`https://host/v1`).
    pub url: String,
    /// Per-call deadline.
    pub timeout: std::time::Duration,
    /// Bearer token.
    pub token: Option<String>,
    /// Keeps the HTTP client dependency in this feature's build.
    pub client: Option<reqwest::Client>,
}
