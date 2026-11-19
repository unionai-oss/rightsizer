//! Column extraction shared by the sqlite and postgres backends. Every
//! table keeps the artifact's full JSON in `body` (decoded with the N/N-1
//! rule on read) plus the indexed columns queries filter on.

use rightsizer_core::breaker::{BreakerScope, Reset, Trip};
use rightsizer_core::{AttemptRecord, Decision, DecisionSource, RunRecord};

use crate::{StoreError, ulid_time};

pub(crate) fn to_json<T: serde::Serialize>(v: &T) -> Result<String, StoreError> {
    serde_json::to_string(v).map_err(|e| StoreError::Other(e.to_string()))
}

/// Indexed columns of a run row.
pub(crate) struct RunCols {
    pub key: String,
    pub tenant: String,
    pub invocation_id: String,
    pub attempt: i64,
    pub source: String,
    pub cluster: Option<String>,
    pub started_at_ms: i64,
    pub v: i64,
    pub body: String,
}

pub(crate) fn run_cols(rec: &RunRecord, write_version: u16) -> Result<RunCols, StoreError> {
    let mut r = rec.clone();
    r.v = write_version;
    Ok(RunCols {
        key: r.key.to_string(),
        tenant: crate::layout::tenant_dir(&r.key),
        invocation_id: r.invocation_id.clone(),
        attempt: r.attempt as i64,
        source: serde_json::to_string(&r.source).unwrap_or_default(),
        cluster: r.cluster.clone(),
        started_at_ms: r.started_at.timestamp_millis(),
        v: write_version as i64,
        body: to_json(&r)?,
    })
}

/// Indexed columns of a decision row.
pub(crate) struct DecisionCols {
    pub id: String,
    pub key: String,
    pub tenant: String,
    pub at_ms: i64,
    pub mode: String,
    pub estimator_id: Option<String>,
    pub estimator_version: Option<String>,
    pub body: String,
}

pub(crate) fn decision_cols(d: &Decision, write_version: u16) -> Result<DecisionCols, StoreError> {
    let mut d2 = d.clone();
    d2.v = write_version;
    let (eid, ever) = match &d.source {
        DecisionSource::Estimator { id, version } | DecisionSource::Memo { id, version } => (Some(id.clone()), Some(version.clone())),
        DecisionSource::Prior { .. } => (None, None),
    };
    Ok(DecisionCols {
        id: d.id.to_string(),
        key: d.key.to_string(),
        tenant: crate::layout::tenant_dir(&d.key),
        at_ms: ulid_time(d.id).timestamp_millis(),
        mode: d.mode.as_str().to_string(),
        estimator_id: eid,
        estimator_version: ever,
        body: to_json(&d2)?,
    })
}

pub(crate) fn trip_body(t: &Trip, write_version: u16) -> Result<String, StoreError> {
    let mut t = t.clone();
    t.v = write_version;
    to_json(&t)
}

pub(crate) fn reset_body(r: &Reset, write_version: u16) -> Result<String, StoreError> {
    let mut r = r.clone();
    r.v = write_version;
    to_json(&r)
}

pub(crate) fn attempt_body(a: &AttemptRecord, write_version: u16) -> Result<String, StoreError> {
    let mut a = a.clone();
    a.v = write_version;
    to_json(&a)
}

pub(crate) fn scope_path(s: &BreakerScope) -> String {
    s.path()
}

/// Decode a body; `None` for unsupported versions; corrupt rows are skipped
/// with a warning (S1: one bad row never fails a read).
#[cfg_attr(not(feature = "sqlite"), allow(dead_code))]
pub(crate) fn decode<T: serde::de::DeserializeOwned>(body: &str, what: &str) -> Option<T> {
    match crate::decode_versioned::<T>(body.as_bytes(), what) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "skipping unreadable row");
            None
        }
    }
}
