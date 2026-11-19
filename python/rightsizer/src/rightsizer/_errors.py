"""Exceptions (standard library only, importable without ``_core``)."""

from __future__ import annotations


import asyncio as _asyncio

# Exceptions that are control flow, not rightsizer failures: S1 guards re-raise them and
# contain everything else, including pyo3's ``PanicException`` (a ``BaseException``).
CONTROL_FLOW = (KeyboardInterrupt, SystemExit, GeneratorExit, _asyncio.CancelledError)


class RightsizerError(Exception):
    """A rightsizer operation failed.

    Raised by ``_core`` (and the typed API) where the contract allows an
    error: configuration, outcomes, store operations, invalid helper input.
    Decisions never raise (SPEC §2.3 S1): they keep the prior instead.
    """


class Insufficient(Exception):
    """Raised (or ``None`` returned) by an estimator that cannot answer a
    context: the chain moves on to the next estimator, and it does not count
    as a failure (SPEC §6.1)."""


class Rejection(Exception):
    """A Python adapter's ``validate`` rejects a patch (plugins.md §2): the
    patch is dropped and the unit keeps its prior."""

    def __init__(self, reason: str, detail: str | None = None, field: str | None = None):
        super().__init__(f"rejected: {reason}" + (f" ({detail})" if detail else ""))
        self.reason = reason
        self.detail = detail
        self.field = field

    def to_dict(self) -> dict:
        out = {"reason": self.reason}
        if self.detail is not None:
            out["detail"] = self.detail
        if self.field is not None:
            out["field"] = self.field
        return out


__all__ = ["CONTROL_FLOW", "Insufficient", "Rejection", "RightsizerError"]
