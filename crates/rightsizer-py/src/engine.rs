//! `rightsizer._core.Engine`: the engine, JSON in / JSON out.
//!
//! - Every blocking call releases the GIL while Rust works.
//! - `decide`, `decide_group`, `attempt_decide` (and their `a…` awaitables)
//!   never raise (S1): an unreadable request, a missing engine or a panic
//!   becomes a prior decision.
//! - Everything else raises `rightsizer.RightsizerError` on failure.
//! - The engine is rebuilt (from the same configuration) after `fork()` and
//!   after a Python estimator is registered.

use std::sync::{Arc, Mutex};

use pyo3::prelude::*;
use rightsizer_core::{AttemptContext, Decision, PriorReason, RunRecord};
use rightsizer_engine::{Config, DecideRequest};
use rightsizer_history::Role;

use crate::api;
use crate::errors::rs_err;
use crate::estimator;
use crate::runtime::{PyRt, block_on};

struct Built {
    pid: u32,
    generation: u64,
    engine: rightsizer_engine::Engine,
}

/// The engine handle shared by the Python object and its async calls.
pub struct Shared {
    config: Config,
    role: Role,
    built: Mutex<Option<Built>>,
    /// One build at a time; waiters wait asynchronously (no runtime thread
    /// is parked on it, so a fan-out of awaitables cannot starve the
    /// blocking pool the build may need).
    gate: tokio::sync::Mutex<()>,
}

impl Shared {
    /// A handle; nothing is built until first use.
    pub fn new(config: Config, role: Role) -> Arc<Self> {
        Arc::new(Shared { config, role, built: Mutex::new(None), gate: tokio::sync::Mutex::new(()) })
    }

    /// The current engine if it belongs to this process and estimator registry.
    fn current(&self) -> Option<rightsizer_engine::Engine> {
        let g = self.built.lock().unwrap_or_else(|e| e.into_inner());
        let b = g.as_ref()?;
        (b.pid == std::process::id() && b.generation == estimator::generation()).then(|| b.engine.clone())
    }

    /// The engine for this process and estimator registry, building it if
    /// needed (after `fork()` or a new Python estimator).
    pub async fn engine_async(self: Arc<Self>) -> Result<rightsizer_engine::Engine, String> {
        if let Some(e) = self.current() {
            return Ok(e);
        }
        let _gate = self.gate.lock().await;
        if let Some(e) = self.current() {
            return Ok(e);
        }
        let pid = std::process::id();
        let generation = estimator::generation();
        let (config, role, front) = (self.config.clone(), self.role, estimator::registered());
        let engine = api::build_engine(config, role, front).await?;
        let mut g = self.built.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(old) = g.take()
            && old.pid != pid
        {
            // Forked: the parent's engine holds tasks, clients and locks of a
            // runtime that does not exist here. Never touch or drop it.
            std::mem::forget(old);
        }
        *g = Some(Built { pid, generation, engine: engine.clone() });
        Ok(engine)
    }

    /// Blocking variant of [`Shared::engine_async`]; call without the GIL.
    pub fn engine(self: &Arc<Self>) -> Result<rightsizer_engine::Engine, String> {
        if let Some(e) = self.current() {
            return Ok(e);
        }
        block_on(self.clone().engine_async())?
    }
}

/// `Engine(config=None, *, store=None, role="client", env_overrides=True, storage_options=None)`.
///
/// `storage_options` (a `dict[str, str]`) are the object-store options of
/// the history store (`object_store` keys or fsspec spellings: credentials,
/// endpoint, region, …; SPEC §10.3). They override
/// `[history.storage_options]` key by key and are never written to the
/// process environment.
#[pyclass(name = "Engine", module = "rightsizer._core", frozen)]
pub struct PyEngine {
    shared: Arc<Shared>,
}

fn decide_or_prior(shared: &Arc<Shared>, req: DecideRequest) -> impl std::future::Future<Output = Decision> + Send + 'static {
    let shared = shared.clone();
    async move {
        match shared.engine_async().await {
            Ok(e) => e.decide(req).await,
            Err(msg) => api::fallback_decision(&req, PriorReason::ConfigError, msg),
        }
    }
}

fn run_decide(py: Python<'_>, shared: &Arc<Shared>, request_json: &str) -> String {
    let req = match api::parse_request(request_json) {
        Ok(r) => r,
        Err(d) => return api::to_json(&*d),
    };
    let shared = shared.clone();
    py.detach(move || {
        let fallback = || api::fallback_decision(&req, PriorReason::Panic, String::new());
        let engine = match shared.engine() {
            Ok(e) => e,
            Err(msg) => return api::to_json(&api::fallback_decision(&req, PriorReason::ConfigError, msg)),
        };
        let r2 = req.clone();
        match block_on(async move { engine.decide(r2).await }) {
            Ok(d) => api::to_json(&d),
            Err(panic) => {
                let mut d = fallback();
                d.explanation.push(format!("panic: {panic}"));
                api::to_json(&d)
            }
        }
    })
}

/// See [`PyEngine::import_runs`].
pub async fn import_runs(engine: &rightsizer_engine::Engine, recs: Vec<RunRecord>) -> Result<String, String> {
    use rightsizer_history::{Importer, PutOutcome};
    let read = recs.len();
    let mut imp = Importer::new();
    let mut fresh = Vec::new();
    let mut duplicates = 0usize;
    for r in recs {
        match imp.put_run(engine.store().as_ref(), &r).await.map_err(|e| e.to_string())? {
            PutOutcome::Inserted => fresh.push(r),
            PutOutcome::Duplicate => duplicates += 1,
        }
    }
    let inserted = fresh.len();
    let (mut joined, mut leases_closed) = (0usize, 0usize);
    let mut attributions = std::collections::BTreeMap::<String, usize>::new();
    let mut tripped = Vec::new();
    for r in fresh {
        // Already stored by the importer: process without writing again
        // (joins, attribution, breakers, lease closing).
        if let Ok(rep) = engine.outcome_with(r, rightsizer_engine::OutcomeOptions { prewritten: true }).await {
            joined += rep.joined as usize;
            leases_closed += rep.lease_closed as usize;
            if let Some(a) = rep.attribution {
                *attributions.entry(a).or_default() += 1;
            }
            tripped.extend(rep.tripped.into_iter().map(|s| s.to_string()));
        }
    }
    engine.flush().await;
    Ok(serde_json::json!({
        "read": read, "inserted": inserted, "duplicates": duplicates, "joined": joined,
        "attributions": attributions, "tripped": tripped, "leases_closed": leases_closed,
    })
    .to_string())
}

#[pymethods]
impl PyEngine {
    #[new]
    #[pyo3(signature = (config=None, *, store=None, role=None, env_overrides=true, storage_options=None))]
    fn new(
        py: Python<'_>,
        config: Option<String>,
        store: Option<String>,
        role: Option<String>,
        env_overrides: bool,
        storage_options: Option<std::collections::BTreeMap<String, String>>,
    ) -> PyResult<Self> {
        let role = api::parse_role(role.as_deref()).map_err(|e| rs_err(py, e))?;
        let mut cfg = py.detach(|| api::load_config(config.as_deref(), store.as_deref(), env_overrides)).map_err(|e| rs_err(py, e))?;
        for (k, v) in storage_options.unwrap_or_default() {
            cfg.history.storage_options.insert(k, v);
        }
        let shared = Shared::new(cfg, role);
        // Build now so `strict` configuration errors surface here.
        let s2 = shared.clone();
        py.detach(move || s2.engine()).map_err(|e| rs_err(py, e))?;
        Ok(PyEngine { shared })
    }

    /// The effective configuration as JSON.
    fn config_json(&self) -> String {
        api::to_json(&self.shared.config)
    }

    /// The configuration error that made this engine inert, if any.
    fn config_error(&self, py: Python<'_>) -> PyResult<Option<String>> {
        let s = self.shared.clone();
        let e = py.detach(move || s.engine()).map_err(|e| rs_err(py, e))?;
        Ok(e.config_error().map(str::to_string))
    }

    /// Ids of the chain's members, top-down.
    fn chain(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let s = self.shared.clone();
        let e = py.detach(move || s.engine()).map_err(|e| rs_err(py, e))?;
        Ok(e.chain().ids())
    }

    /// `DecideRequest` JSON → `Decision` JSON. Never raises.
    fn decide(&self, py: Python<'_>, request_json: &str) -> String {
        run_decide(py, &self.shared, request_json)
    }

    /// Awaitable `decide` (on the caller's event loop). Never raises.
    fn adecide<'py>(&self, py: Python<'py>, request_json: &str) -> PyResult<Bound<'py, PyAny>> {
        let parsed = api::parse_request(request_json);
        let shared = self.shared.clone();
        pyo3_async_runtimes::generic::future_into_py::<PyRt, _, _>(py, async move {
            Ok(match parsed {
                Err(d) => api::to_json(&*d),
                Ok(req) => api::to_json(&decide_or_prior(&shared, req).await),
            })
        })
    }

    /// JSON array of requests → JSON array of decisions (one decision per
    /// group, SPEC §4). Never raises.
    fn decide_group(&self, py: Python<'_>, requests_json: &str) -> String {
        let reqs: Vec<DecideRequest> = match serde_json::from_str(requests_json) {
            Ok(r) => r,
            Err(e) => return api::to_json(&vec![api::invalid_request_decision("null", &format!("group: {e}"))]),
        };
        let shared = self.shared.clone();
        py.detach(move || {
            let fallback = |reqs: &[DecideRequest], reason, msg: String| {
                api::to_json(&reqs.iter().map(|r| api::fallback_decision(r, reason, msg.clone())).collect::<Vec<_>>())
            };
            let engine = match shared.engine() {
                Ok(e) => e,
                Err(msg) => return fallback(&reqs, PriorReason::ConfigError, msg),
            };
            let r2 = reqs.clone();
            match block_on(async move { engine.decide_group(r2).await }) {
                Ok(ds) => api::to_json(&ds),
                Err(p) => fallback(&reqs, PriorReason::Panic, p),
            }
        })
    }

    /// Awaitable `decide_group`.
    fn adecide_group<'py>(&self, py: Python<'py>, requests_json: &str) -> PyResult<Bound<'py, PyAny>> {
        let reqs: Result<Vec<DecideRequest>, String> = serde_json::from_str(requests_json).map_err(|e| e.to_string());
        let shared = self.shared.clone();
        pyo3_async_runtimes::generic::future_into_py::<PyRt, _, _>(py, async move {
            let reqs = match reqs {
                Ok(r) => r,
                Err(e) => return Ok(api::to_json(&vec![api::invalid_request_decision("null", &format!("group: {e}"))])),
            };
            Ok(match shared.engine_async().await {
                Ok(e) => api::to_json(&e.decide_group(reqs).await),
                Err(msg) => api::to_json(&reqs.iter().map(|r| api::fallback_decision(r, PriorReason::ConfigError, msg.clone())).collect::<Vec<_>>()),
            })
        })
    }

    /// A decision for attempt k of an invocation (SPEC §2.3 S5 ladders).
    /// `attempt_json` is an `AttemptContext`. Never raises.
    fn attempt_decide(&self, py: Python<'_>, request_json: &str, attempt_json: &str) -> String {
        let req = match api::parse_request(request_json) {
            Ok(r) => r,
            Err(d) => return api::to_json(&*d),
        };
        let attempt: AttemptContext = match serde_json::from_str(attempt_json) {
            Ok(a) => a,
            Err(e) => return api::to_json(&api::fallback_decision(&req, PriorReason::Internal, format!("invalid attempt context: {e}"))),
        };
        let shared = self.shared.clone();
        py.detach(move || {
            let engine = match shared.engine() {
                Ok(e) => e,
                Err(msg) => return api::to_json(&api::fallback_decision(&req, PriorReason::ConfigError, msg)),
            };
            let r2 = req.clone();
            match block_on(async move { engine.attempt_decide(r2, attempt).await }) {
                Ok(d) => api::to_json(&d),
                Err(p) => api::to_json(&api::fallback_decision(&req, PriorReason::Panic, p)),
            }
        })
    }

    /// Awaitable `attempt_decide` (on the caller's event loop). Never raises.
    /// Flyte parents pass an attempt context on every call that may escalate
    /// (the default), so this is the common async path, not a retry-only one.
    fn aattempt_decide<'py>(&self, py: Python<'py>, request_json: &str, attempt_json: &str) -> PyResult<Bound<'py, PyAny>> {
        let parsed = api::parse_request(request_json);
        let attempt: Result<AttemptContext, String> = serde_json::from_str(attempt_json).map_err(|e| e.to_string());
        let shared = self.shared.clone();
        pyo3_async_runtimes::generic::future_into_py::<PyRt, _, _>(py, async move {
            let req = match parsed {
                Ok(r) => r,
                Err(d) => return Ok(api::to_json(&*d)),
            };
            let attempt = match attempt {
                Ok(a) => a,
                Err(e) => return Ok(api::to_json(&api::fallback_decision(&req, PriorReason::Internal, format!("invalid attempt context: {e}")))),
            };
            Ok(match shared.engine_async().await {
                Ok(e) => api::to_json(&e.attempt_decide(req, attempt).await),
                Err(msg) => api::to_json(&api::fallback_decision(&req, PriorReason::ConfigError, msg)),
            })
        })
    }

    /// Ingest a `RunRecord` (JSON). Returns the outcome report as JSON.
    /// Raises `RightsizerError` on an invalid record or a store error.
    fn outcome(&self, py: Python<'_>, record_json: &str) -> PyResult<String> {
        let rec: RunRecord = serde_json::from_str(record_json).map_err(|e| rs_err(py, format!("invalid run record: {e}")))?;
        let shared = self.shared.clone();
        let r = py.detach(move || -> Result<String, String> {
            let engine = shared.engine()?;
            let rep = block_on(async move { engine.outcome(rec).await })?.map_err(|e| e.to_string())?;
            Ok(api::outcome_report(&rep).to_string())
        });
        r.map_err(|e| rs_err(py, e))
    }

    /// Awaitable `outcome`.
    fn aoutcome<'py>(&self, py: Python<'py>, record_json: &str) -> PyResult<Bound<'py, PyAny>> {
        let rec: RunRecord = serde_json::from_str(record_json).map_err(|e| rs_err(py, format!("invalid run record: {e}")))?;
        let shared = self.shared.clone();
        pyo3_async_runtimes::generic::future_into_py::<PyRt, _, _>(py, async move {
            let r: Result<String, String> = async {
                let engine = shared.engine_async().await?;
                let rep = engine.outcome(rec).await.map_err(|e| e.to_string())?;
                Ok(api::outcome_report(&rep).to_string())
            }
            .await;
            r.map_err(|e| Python::attach(|py| rs_err(py, e)))
        })
    }

    /// Bulk import (`rsz history import …` backfills): `RunRecord`s (JSON
    /// array). Each record is deduplicated against its key's **whole**
    /// stored history (`rightsizer_history::Importer`, like the Rust
    /// importers), so a re-import of an overlapping window inserts nothing;
    /// attribution, breakers and lease closing run only for inserted
    /// records (after every record is stored, so a recovered OOM is seen as
    /// such). Returns JSON `{read, inserted, duplicates, joined,
    /// attributions: {attribution: n}, tripped: [scope], leases_closed}`.
    /// Raises `RightsizerError` on invalid records or a store error.
    fn import_runs(&self, py: Python<'_>, records_json: &str) -> PyResult<String> {
        let recs: Vec<RunRecord> = serde_json::from_str(records_json).map_err(|e| rs_err(py, format!("records: {e}")))?;
        let shared = self.shared.clone();
        py.detach(move || -> Result<String, String> {
            let engine = shared.engine()?;
            block_on(async move { import_runs(&engine, recs).await })?
        })
        .map_err(|e| rs_err(py, e))
    }

    /// Flush buffered writes; returns how many were written.
    fn flush(&self, py: Python<'_>) -> PyResult<usize> {
        let shared = self.shared.clone();
        py.detach(move || -> Result<usize, String> {
            let engine = shared.engine()?;
            block_on(async move { engine.flush().await })
        })
        .map_err(|e| rs_err(py, e))
    }

    /// Forget cached history and memoized estimates for `key` (a workload
    /// key string), so the next decision re-reads the store. Raises
    /// `RightsizerError` only on an unparseable key.
    fn invalidate(&self, py: Python<'_>, key: &str) -> PyResult<()> {
        let key: rightsizer_core::WorkloadKey = key.parse().map_err(|e| rs_err(py, format!("key: {e}")))?;
        let shared = self.shared.clone();
        py.detach(move || -> Result<(), String> {
            shared.engine()?.invalidate(&key);
            Ok(())
        })
        .map_err(|e| rs_err(py, e))
    }

    /// Breakers as JSON (`rsz breaker status`).
    fn breaker_status(&self, py: Python<'_>) -> PyResult<String> {
        let shared = self.shared.clone();
        py.detach(move || -> Result<String, String> {
            let engine = shared.engine()?;
            let s = block_on(async move { engine.breaker_status().await })?.map_err(|e| e.to_string())?;
            Ok(api::to_json(&s))
        })
        .map_err(|e| rs_err(py, e))
    }

    /// The fleet control object as JSON, or `None` when there is none.
    fn control_get(&self, py: Python<'_>) -> PyResult<Option<String>> {
        let shared = self.shared.clone();
        py.detach(move || -> Result<Option<String>, String> {
            let engine = shared.engine()?;
            let c = block_on(async move { engine.control_get().await })?.map_err(|e| e.to_string())?;
            Ok(c.map(|c| api::to_json(&c)))
        })
        .map_err(|e| rs_err(py, e))
    }

    /// Shadow / audit replay (`rsz shadow`): `RunRecord`s (JSON array) and
    /// options (JSON object: `since`, `platform`, `state`, `constraints`,
    /// `policy`, `bootstrap`, `seed`, `per_run`) → `ReplayReport` JSON.
    #[pyo3(signature = (records_json, options_json=None))]
    fn replay(&self, py: Python<'_>, records_json: &str, options_json: Option<&str>) -> PyResult<String> {
        let recs: Vec<RunRecord> = serde_json::from_str(records_json).map_err(|e| rs_err(py, format!("records: {e}")))?;
        let opts: api::ReplayOpts = match options_json {
            None => api::ReplayOpts::default(),
            Some(o) => serde_json::from_str(o).map_err(|e| rs_err(py, format!("replay options: {e}")))?,
        };
        let opts = opts.into_options();
        let shared = self.shared.clone();
        py.detach(move || -> Result<String, String> {
            let engine = shared.engine()?;
            let r = block_on(async move { engine.replay(recs, &opts).await })?;
            Ok(api::to_json(&r))
        })
        .map_err(|e| rs_err(py, e))
    }

    /// Prometheus text exposition of this engine's metrics.
    fn metrics(&self, py: Python<'_>) -> PyResult<String> {
        let shared = self.shared.clone();
        py.detach(move || shared.engine().map(|e| e.metrics().render())).map_err(|e| rs_err(py, e))
    }

    fn __repr__(&self) -> String {
        format!("<rightsizer._core.Engine history={:?}>", self.shared.config.history.uri.as_deref().map(rightsizer_history::redact_uri))
    }
}
