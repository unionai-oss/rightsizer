//! `rightsizer.RightsizerError`.
//!
//! The class lives in pure Python (`rightsizer._errors`) so `except
//! rs.RightsizerError` works without importing `_core`. When that module is
//! not importable (the extension used on its own), `_core` raises its own
//! `rightsizer._core.RightsizerError`.

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyType;

create_exception!(_core, RightsizerError, PyException, "A rightsizer operation failed (never raised by decide).");

static CLASS: PyOnceLock<Py<PyType>> = PyOnceLock::new();

/// The exception class raised by `_core`.
pub fn class(py: Python<'_>) -> Bound<'_, PyType> {
    CLASS
        .get_or_init(py, || {
            py.import("rightsizer._errors")
                .and_then(|m| m.getattr("RightsizerError"))
                .and_then(|c| c.cast_into::<PyType>().map_err(Into::into))
                .map(Bound::unbind)
                .unwrap_or_else(|_| py.get_type::<RightsizerError>().unbind())
        })
        .bind(py)
        .clone()
}

/// A `RightsizerError` with `msg`.
pub fn rs_err(py: Python<'_>, msg: impl Into<String>) -> PyErr {
    PyErr::from_type(class(py), msg.into())
}
