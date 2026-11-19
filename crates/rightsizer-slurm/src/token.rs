//! Signed attribution tokens (slurm.md §4.1): HMAC-SHA256 over the origin
//! prior and decision id, carried in the job's comment / environment.
//!
//! Skeleton: the token payload type.

use serde::{Deserialize, Serialize};

/// What a token attests.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenPayload {
    /// Wire version (1).
    pub v: u16,
    /// Decision id.
    pub decision_id: String,
    /// Origin prior memory, bytes.
    pub prior_memory: Option<u64>,
    /// Origin prior CPUs, millicores.
    pub prior_cpu: Option<u32>,
}
