//! `rsz history export --parquet` (SPEC §14.1): run records and decisions
//! as one Parquet file with the tuner's corpus columns
//! ([`crate::snapshot::COLUMNS`]). Exports what the store retains: for the
//! object store, the `latest` pointers (`keep_last` per key) plus live
//! shards; older history lives in the daily snapshots, which use the same
//! schema.

use std::path::Path;

use chrono::{DateTime, Utc};
use rightsizer_core::{Tenant, WorkloadKey};

use crate::snapshot::{SnapshotRow, write_snapshot};
use crate::{DecisionFilter, HistoryStore, StoreError};

/// What to export.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ExportFilter {
    /// Only these keys (default: every key the store knows).
    pub keys: Option<Vec<WorkloadKey>>,
    /// Only this `(platform, tenant)`.
    pub tenant: Option<(String, Tenant)>,
    /// Runs started / decisions made at or after.
    pub since: Option<DateTime<Utc>>,
    /// Before.
    pub until: Option<DateTime<Utc>>,
    /// Include decisions (default true via [`ExportFilter::all`]).
    pub decisions: bool,
}

impl ExportFilter {
    /// Everything, decisions included.
    pub fn all() -> Self {
        ExportFilter { decisions: true, ..Default::default() }
    }
}

/// What an export wrote.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExportSummary {
    /// Run rows.
    pub runs: usize,
    /// Decision rows.
    pub decisions: usize,
    /// File size.
    pub bytes: usize,
}

/// Collect export rows.
pub async fn export_rows(store: &dyn HistoryStore, f: &ExportFilter) -> Result<Vec<SnapshotRow>, StoreError> {
    let keys = match &f.keys {
        Some(k) => k.clone(),
        None => store.keys().await?,
    };
    let mut rows = Vec::new();
    for k in keys.iter().filter(|k| f.tenant.as_ref().is_none_or(|(p, t)| &k.platform == p && &k.tenant == t)) {
        for r in store.runs(k, usize::MAX).await? {
            if f.since.is_none_or(|s| r.started_at >= s) && f.until.is_none_or(|u| r.started_at < u) {
                rows.push(SnapshotRow::Run(Box::new(r)));
            }
        }
    }
    if f.decisions {
        let df = DecisionFilter { tenant: f.tenant.clone(), since: f.since, until: f.until, ..Default::default() };
        for d in store.decisions(&df).await? {
            if f.keys.as_ref().is_none_or(|ks| ks.contains(&d.key)) {
                rows.push(SnapshotRow::Decision(Box::new(d)));
            }
        }
    }
    Ok(rows)
}

/// Export as Parquet bytes.
pub async fn export_parquet_bytes(store: &dyn HistoryStore, f: &ExportFilter) -> Result<(Vec<u8>, ExportSummary), StoreError> {
    let rows = export_rows(store, f).await?;
    let runs = rows.iter().filter(|r| matches!(r, SnapshotRow::Run(_))).count();
    let decisions = rows.len() - runs;
    let bytes = write_snapshot(&rows).map_err(StoreError::Other)?;
    let n = bytes.len();
    Ok((bytes, ExportSummary { runs, decisions, bytes: n }))
}

/// Export as a Parquet file at `out`.
pub async fn export_parquet(store: &dyn HistoryStore, f: &ExportFilter, out: &Path) -> Result<ExportSummary, StoreError> {
    let (bytes, s) = export_parquet_bytes(store, f).await?;
    std::fs::write(out, bytes).map_err(|e| StoreError::Other(format!("{}: {e}", out.display())))?;
    Ok(s)
}
