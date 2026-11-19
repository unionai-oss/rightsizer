//! PyO3 module `rightsizer._core` (ARCHITECTURE.md §4.1): JSON in, JSON out.
//! `decide(request_json) -> decision_json`, `outcome(record_json)`,
//! `Engine(config_json)`; the `rightsizer` package wraps them in dataclasses.
//!
//! Status: stub module exposing `version()` and `grid_json()`; the engine
//! bindings land with M1. Build the wheel with maturin and the
//! `extension-module` feature; plain `cargo test` links against libpython.

use pyo3::prelude::*;

/// The crate version.
#[pyfunction]
fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// `rs-grid-v1` as JSON.
#[pyfunction]
fn grid_json() -> String {
    serde_json::to_string(&rightsizer_core::Grid::rs_grid_v1()).unwrap_or_default()
}

/// The `_core` module.
#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(grid_json, m)?)?;
    Ok(())
}
