//! The object-store backend (SPEC §8.1): immutable shards, `latest`
//! pointers with watermarks, conditional PUTs for trips, leases and
//! compaction; local FS, S3, GCS and Azure through `object_store`.
//!
//! **Stub:** the type and its constructor exist; every operation returns
//! [`StoreError::Unimplemented`] until the backend lands.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use object_store::ObjectStore;
use rightsizer_core::breaker::{BreakerObjects, BreakerScope, Control, Reset, Trip};
use rightsizer_core::{AttemptRecord, Decision, RunRecord, RunStatus, WorkloadKey};
use ulid::Ulid;

use crate::{Backend, CompactOptions, CompactReport, DecisionFilter, HistoryStore, PutOutcome, StoreError, StoreInfo};

/// A store over any `object_store` backend.
pub struct ObjectHistoryStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    uri: String,
    durable_shared: bool,
}

impl ObjectHistoryStore {
    /// Wrap an object store at `prefix`. `durable_shared` is false for local paths.
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>, uri: impl Into<String>, durable_shared: bool) -> Self {
        ObjectHistoryStore { store, prefix: prefix.into(), uri: uri.into(), durable_shared }
    }

    /// The underlying store.
    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    /// The prefix.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }
}

const TODO: StoreError = StoreError::Unimplemented("object-store backend");

#[async_trait]
impl HistoryStore for ObjectHistoryStore {
    fn info(&self) -> StoreInfo {
        StoreInfo { backend: Backend::ObjectStore, uri: self.uri.clone(), durable_shared: self.durable_shared }
    }
    async fn put_run(&self, _: &RunRecord) -> Result<PutOutcome, StoreError> {
        Err(TODO)
    }
    async fn put_decision(&self, _: &Decision) -> Result<(), StoreError> {
        Err(TODO)
    }
    async fn runs(&self, _: &WorkloadKey, _: usize) -> Result<Vec<RunRecord>, StoreError> {
        Err(TODO)
    }
    async fn decisions(&self, _: &DecisionFilter) -> Result<Vec<Decision>, StoreError> {
        Err(TODO)
    }
    async fn decision(&self, _: Ulid) -> Result<Option<Decision>, StoreError> {
        Err(TODO)
    }
    async fn breaker_get(&self, _: &BreakerScope) -> Result<BreakerObjects, StoreError> {
        Err(TODO)
    }
    async fn breaker_list(&self) -> Result<Vec<(BreakerScope, BreakerObjects)>, StoreError> {
        Err(TODO)
    }
    async fn breaker_trip(&self, _: &Trip) -> Result<(), StoreError> {
        Err(TODO)
    }
    async fn breaker_reset(&self, _: &Reset) -> Result<(), StoreError> {
        Err(TODO)
    }
    async fn control_get(&self) -> Result<Option<Control>, StoreError> {
        Err(TODO)
    }
    async fn control_set(&self, _: &Control) -> Result<(), StoreError> {
        Err(TODO)
    }
    async fn attempt_get(&self, _: &WorkloadKey, _: &str) -> Result<Option<AttemptRecord>, StoreError> {
        Err(TODO)
    }
    async fn attempt_put_if_absent(&self, _: &AttemptRecord) -> Result<bool, StoreError> {
        Err(TODO)
    }
    async fn attempt_close_lease(&self, _: &WorkloadKey, _: &str, _: DateTime<Utc>, _: RunStatus) -> Result<(), StoreError> {
        Err(TODO)
    }
    async fn compact(&self, _: &CompactOptions) -> Result<CompactReport, StoreError> {
        Err(TODO)
    }
}
