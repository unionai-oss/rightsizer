//! PyO3 module `rightsizer._core` (ARCHITECTURE.md §4.1, SPEC §10.2–§10.3):
//! JSON in, JSON out. The `rightsizer` package wraps it in typed dataclasses.
//!
//! | item | what |
//! |---|---|
//! | `Engine(config=None, *, store=None, role="client", env_overrides=True)` | the engine; config resolved like `Config::load` |
//! | `Engine.decide / adecide / decide_group / adecide_group / attempt_decide` | decisions (never raise, S1) |
//! | `Engine.outcome / aoutcome / flush / breaker_status / control_get / replay / metrics` | the rest (raise `RightsizerError`) |
//! | `normalize_quantity`, `format_quantity` | K8s / Flyte / Slurm / Ray units ⇄ bytes and millicores |
//! | `rs_grid_v1`, `grid`, `context_digest`, `feature_bucket`, `code_info`, `read_inputs` | pure helpers |
//! | `parse_gpu`, `accelerators`, `canonical_key`, `key_hash` | catalog and key helpers |
//! | `register_estimator`, `unregister_estimator`, `clear_estimators`, `registered_estimators` | Python estimators |
//!
//! Runtime: one tokio runtime per process, re-created after `fork()`
//! ([`runtime`]). Blocking calls release the GIL; awaitables use
//! `pyo3-async-runtimes` over that runtime.
//!
//! Build the wheel with maturin (feature `extension-module`); plain `cargo
//! test` links against libpython.

use pyo3::prelude::*;
use rightsizer_core::{AcceleratorId, GpuShape, InputRef, WorkloadKey};
use rightsizer_estimators::EstimatorInfo;

pub mod api;
pub mod engine;
pub mod errors;
pub mod estimator;
pub mod runtime;

use errors::rs_err;

/// The crate version.
#[pyfunction]
fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// `rs-grid-v1` as JSON.
#[pyfunction]
fn rs_grid_v1() -> String {
    api::to_json(&rightsizer_core::Grid::rs_grid_v1())
}

/// `rs-grid-v1` as JSON (old name).
#[pyfunction]
fn grid_json() -> String {
    rs_grid_v1()
}

/// A registered grid by id as JSON, or `None`.
#[pyfunction]
fn grid(id: &str) -> Option<String> {
    rightsizer_core::Grid::by_id(id).map(|g| api::to_json(&g))
}

fn qvalue(v: &Bound<'_, PyAny>) -> PyResult<api::QValue> {
    if v.is_instance_of::<pyo3::types::PyBool>() {
        return Err(pyo3::exceptions::PyTypeError::new_err("a quantity cannot be a bool"));
    }
    if let Ok(s) = v.extract::<String>() {
        return Ok(api::QValue::Str(s));
    }
    if let Ok(i) = v.extract::<i64>() {
        return Ok(api::QValue::Str(i.to_string()));
    }
    if let Ok(f) = v.extract::<f64>() {
        return Ok(api::QValue::Num(f));
    }
    Err(pyo3::exceptions::PyTypeError::new_err("a quantity must be a str, int or float"))
}

/// Normalize a platform quantity to bytes (memory-like axes) or millicores
/// (CPU), rounding up. Raises `RightsizerError` on an invalid quantity.
#[pyfunction]
#[pyo3(signature = (value, axis="memory", platform="k8s"))]
fn normalize_quantity(py: Python<'_>, value: &Bound<'_, PyAny>, axis: &str, platform: &str) -> PyResult<u64> {
    let v = qvalue(value)?;
    api::normalize_quantity(&v, axis, platform).map_err(|e| rs_err(py, e))
}

/// Format bytes / millicores for a platform (`"64Gi"`, `"1536M"`, `2.5`).
#[pyfunction]
#[pyo3(signature = (value, axis="memory", platform="k8s"))]
fn format_quantity(py: Python<'_>, value: u64, axis: &str, platform: &str) -> PyResult<Py<PyAny>> {
    use pyo3::IntoPyObjectExt;
    match api::format_quantity(value, axis, platform).map_err(|e| rs_err(py, e))? {
        serde_json::Value::String(s) => s.into_py_any(py),
        serde_json::Value::Number(n) if n.is_u64() => n.as_u64().unwrap_or_default().into_py_any(py),
        serde_json::Value::Number(n) => n.as_f64().unwrap_or_default().into_py_any(py),
        other => other.to_string().into_py_any(py),
    }
}

/// The context digest of a `WorkloadContext` (JSON).
#[pyfunction]
fn context_digest(py: Python<'_>, ctx_json: &str) -> PyResult<String> {
    api::context_digest(ctx_json).map_err(|e| rs_err(py, e))
}

/// The memo feature bucket of `InputMeta`s (JSON array).
#[pyfunction]
#[pyo3(signature = (inputs_json, grid=None))]
fn feature_bucket(py: Python<'_>, inputs_json: &str, grid: Option<&str>) -> PyResult<String> {
    api::feature_bucket(inputs_json, grid).map_err(|e| rs_err(py, e))
}

/// `CodeInfo` JSON (digest, `features-v1` tokens; source only if kept).
#[pyfunction]
#[pyo3(signature = (source, language=None, keep_source=false))]
fn code_info(source: &str, language: Option<&str>, keep_source: bool) -> String {
    api::code_info(source, language, keep_source)
}

/// Read input metadata for `InputRef`s (JSON array) within `budget_ms`;
/// returns `InputMeta`s (JSON array). Releases the GIL. Unreadable inputs
/// keep what is known (S1).
#[pyfunction]
#[pyo3(signature = (refs_json, budget_ms=1000, allow_hosts=None))]
fn read_inputs(py: Python<'_>, refs_json: &str, budget_ms: u64, allow_hosts: Option<Vec<String>>) -> PyResult<String> {
    let refs: Vec<InputRef> = serde_json::from_str(refs_json).map_err(|e| rs_err(py, format!("input refs: {e}")))?;
    let hosts = allow_hosts.unwrap_or_default();
    let out = py.detach(move || runtime::block_on(api::read_inputs(refs, budget_ms, hosts))).map_err(|e| rs_err(py, e))?;
    Ok(api::to_json(&out))
}

/// Parse a GPU shape (`"A100-80G:2"`, case-insensitive) → `{"accel", "count"}` JSON.
#[pyfunction]
fn parse_gpu(py: Python<'_>, s: &str) -> PyResult<String> {
    s.parse::<GpuShape>().map(|g| api::to_json(&g)).map_err(|e| rs_err(py, e.to_string()))
}

/// The accelerator catalog (SPEC §5.1.1) as JSON.
#[pyfunction]
fn accelerators() -> String {
    let v: Vec<serde_json::Value> = AcceleratorId::ALL
        .iter()
        .map(|a| {
            let i = a.info();
            serde_json::json!({"id": i.id, "nominal_gib": i.nominal_gib, "usable_gib": i.usable_gib, "family": i.family, "vendor": i.vendor})
        })
        .collect();
    serde_json::Value::Array(v).to_string()
}

/// The canonical string form of a workload key.
#[pyfunction]
fn canonical_key(py: Python<'_>, key: &str) -> PyResult<String> {
    WorkloadKey::parse(key).map(|k| k.to_string()).map_err(|e| rs_err(py, e.to_string()))
}

/// `WorkloadKey::hash()` (32 hex chars; store paths).
#[pyfunction]
fn key_hash(py: Python<'_>, key: &str) -> PyResult<String> {
    WorkloadKey::parse(key).map(|k| k.hash()).map_err(|e| rs_err(py, e.to_string()))
}

/// Register a Python estimator (front of every engine's chain, in order).
/// `info_json` is its `EstimatorInfo`; `is_async` whether `estimate` is a
/// coroutine function. Re-registering an id replaces it.
#[pyfunction]
fn register_estimator(py: Python<'_>, obj: Py<PyAny>, info_json: &str, is_async: bool) -> PyResult<()> {
    let info: EstimatorInfo = serde_json::from_str(info_json).map_err(|e| rs_err(py, format!("estimator info: {e}")))?;
    if info.id.is_empty() {
        return Err(rs_err(py, "estimator id must not be empty"));
    }
    estimator::register(obj, info, is_async);
    Ok(())
}

/// Unregister a Python estimator by id.
#[pyfunction]
fn unregister_estimator(id: &str) -> bool {
    estimator::unregister(id)
}

/// Unregister every Python estimator.
#[pyfunction]
fn clear_estimators() {
    estimator::clear()
}

/// Ids of the registered Python estimators, in chain order.
#[pyfunction]
fn registered_estimators() -> Vec<String> {
    estimator::ids()
}

/// The pid the process runtime was created in (`None` before first use).
#[pyfunction]
fn runtime_pid() -> Option<u32> {
    runtime::runtime_pid()
}

/// The `_core` module.
#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("RightsizerError", errors::class(m.py()))?;
    m.add_class::<engine::PyEngine>()?;
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(rs_grid_v1, m)?)?;
    m.add_function(wrap_pyfunction!(grid_json, m)?)?;
    m.add_function(wrap_pyfunction!(grid, m)?)?;
    m.add_function(wrap_pyfunction!(normalize_quantity, m)?)?;
    m.add_function(wrap_pyfunction!(format_quantity, m)?)?;
    m.add_function(wrap_pyfunction!(context_digest, m)?)?;
    m.add_function(wrap_pyfunction!(feature_bucket, m)?)?;
    m.add_function(wrap_pyfunction!(code_info, m)?)?;
    m.add_function(wrap_pyfunction!(read_inputs, m)?)?;
    m.add_function(wrap_pyfunction!(parse_gpu, m)?)?;
    m.add_function(wrap_pyfunction!(accelerators, m)?)?;
    m.add_function(wrap_pyfunction!(canonical_key, m)?)?;
    m.add_function(wrap_pyfunction!(key_hash, m)?)?;
    m.add_function(wrap_pyfunction!(register_estimator, m)?)?;
    m.add_function(wrap_pyfunction!(unregister_estimator, m)?)?;
    m.add_function(wrap_pyfunction!(clear_estimators, m)?)?;
    m.add_function(wrap_pyfunction!(registered_estimators, m)?)?;
    m.add_function(wrap_pyfunction!(runtime_pid, m)?)?;
    Ok(())
}
