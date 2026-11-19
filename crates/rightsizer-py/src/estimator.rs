//! Python estimators in the Rust chain (SPEC §10.3).
//!
//! `rs.register_estimator(obj)` hands the object to [`register`] with its
//! `EstimatorInfo` (JSON) and whether `estimate` is a coroutine function.
//! Engines put every registered estimator, in registration order, in front
//! of the configured chain; an engine notices a new registration (a
//! generation counter) and rebuilds itself before its next call.
//!
//! Calls go through `rightsizer._estimator` (pure Python), which turns the
//! context JSON into a dict, calls `estimate`, and normalizes the result to
//! `(tag, payload)`: `("ok", estimate_json)`, `("insufficient", why)` or
//! `("error", message)`.
//!
//! - **Sync** `estimate` runs on the runtime's blocking pool with the GIL
//!   reacquired there; the deciding thread released it.
//! - **`async def` estimate** runs on the caller's event loop when the
//!   decision came from `Engine.adecide` (the awaitable carries the loop and
//!   the caller's contextvars as task locals). From the synchronous `decide`
//!   there is no loop to borrow, so the coroutine runs to completion with
//!   `asyncio.run` on a blocking-pool thread (its own short-lived loop).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3_async_runtimes::generic::ContextExt;
use rightsizer_core::{Estimate, WorkloadContext};
use rightsizer_estimators::{EstimateError, Estimator, EstimatorInfo};

use crate::runtime::PyRt;

/// A Python object with an `estimate(ctx_dict)` method.
pub struct PyEstimator {
    obj: Arc<Py<PyAny>>,
    info: EstimatorInfo,
    is_async: bool,
}

static REGISTRY: Mutex<Vec<Arc<PyEstimator>>> = Mutex::new(Vec::new());
static GENERATION: AtomicU64 = AtomicU64::new(0);

/// The registry's generation: bumped by every change.
pub fn generation() -> u64 {
    GENERATION.load(Ordering::Acquire)
}

/// Registered estimators, in order.
pub fn registered() -> Vec<Arc<dyn Estimator>> {
    REGISTRY.lock().unwrap_or_else(|e| e.into_inner()).iter().map(|e| e.clone() as Arc<dyn Estimator>).collect()
}

/// Register (or replace, by id) a Python estimator.
pub fn register(obj: Py<PyAny>, info: EstimatorInfo, is_async: bool) {
    let mut r = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
    let e = Arc::new(PyEstimator { obj: Arc::new(obj), info, is_async });
    match r.iter().position(|x| x.info.id == e.info.id) {
        Some(i) => r[i] = e,
        None => r.push(e),
    }
    GENERATION.fetch_add(1, Ordering::AcqRel);
}

/// Remove by id; returns whether one was removed.
pub fn unregister(id: &str) -> bool {
    let mut r = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
    let n = r.len();
    r.retain(|e| e.info.id != id);
    let removed = r.len() != n;
    if removed {
        GENERATION.fetch_add(1, Ordering::AcqRel);
    }
    removed
}

/// Remove every registered estimator.
pub fn clear() {
    REGISTRY.lock().unwrap_or_else(|e| e.into_inner()).clear();
    GENERATION.fetch_add(1, Ordering::AcqRel);
}

/// Ids, in order.
pub fn ids() -> Vec<String> {
    REGISTRY.lock().unwrap_or_else(|e| e.into_inner()).iter().map(|e| e.info.id.clone()).collect()
}

static HELPER: PyOnceLock<Py<PyModule>> = PyOnceLock::new();

fn helper(py: Python<'_>) -> PyResult<&Bound<'_, PyModule>> {
    HELPER.get_or_try_init(py, || py.import("rightsizer._estimator").map(Bound::unbind)).map(|m| m.bind(py))
}

fn failed(message: impl Into<String>) -> EstimateError {
    EstimateError::Failed { message: message.into(), retryable: false }
}

fn finish(tag: &str, payload: &str) -> Result<Estimate, EstimateError> {
    match tag {
        "ok" => serde_json::from_str::<Estimate>(payload).map_err(|e| EstimateError::Protocol(format!("estimate: {e}"))),
        "insufficient" => Err(EstimateError::Insufficient(payload.to_string())),
        "unavailable" => Err(EstimateError::Unavailable(payload.to_string())),
        _ => Err(failed(payload)),
    }
}

fn py_failed(py: Python<'_>, e: PyErr) -> EstimateError {
    failed(e.value(py).to_string())
}

impl PyEstimator {
    async fn call_sync(&self, ctx_json: String) -> Result<(String, String), EstimateError> {
        let obj = self.obj.clone();
        tokio::task::spawn_blocking(move || {
            Python::attach(|py| -> Result<(String, String), EstimateError> {
                let f = helper(py).and_then(|m| m.getattr("call_sync")).map_err(|e| py_failed(py, e))?;
                f.call1((obj.bind(py), ctx_json)).and_then(|r| r.extract()).map_err(|e| py_failed(py, e))
            })
        })
        .await
        .map_err(|e| failed(format!("python estimator task: {e}")))?
    }

    async fn call_async(&self, ctx_json: String) -> Result<(String, String), EstimateError> {
        // On the caller's loop when there is one (adecide).
        if let Some(locals) = PyRt::get_task_locals() {
            let fut = Python::attach(|py| -> Result<_, EstimateError> {
                let f = helper(py).and_then(|m| m.getattr("call_async")).map_err(|e| py_failed(py, e))?;
                let coro = f.call1((self.obj.bind(py), ctx_json)).map_err(|e| py_failed(py, e))?;
                pyo3_async_runtimes::into_future_with_locals(&locals, coro).map_err(|e| py_failed(py, e))
            })?;
            let out = fut.await;
            return Python::attach(|py| match out {
                Ok(v) => v.bind(py).extract::<(String, String)>().map_err(|e| py_failed(py, e)),
                Err(e) => Err(py_failed(py, e)),
            });
        }
        // Synchronous decide: no loop to borrow.
        let obj = self.obj.clone();
        tokio::task::spawn_blocking(move || {
            Python::attach(|py| -> Result<(String, String), EstimateError> {
                let f = helper(py).and_then(|m| m.getattr("call_async_blocking")).map_err(|e| py_failed(py, e))?;
                f.call1((obj.bind(py), ctx_json)).and_then(|r| r.extract()).map_err(|e| py_failed(py, e))
            })
        })
        .await
        .map_err(|e| failed(format!("python estimator task: {e}")))?
    }
}

#[async_trait]
impl Estimator for PyEstimator {
    fn info(&self) -> &EstimatorInfo {
        &self.info
    }

    async fn estimate(&self, ctx: &WorkloadContext) -> Result<Estimate, EstimateError> {
        let ctx_json = serde_json::to_string(ctx).map_err(|e| failed(format!("context: {e}")))?;
        let (tag, payload) = if self.is_async { self.call_async(ctx_json).await? } else { self.call_sync(ctx_json).await? };
        finish(&tag, &payload)
    }
}
