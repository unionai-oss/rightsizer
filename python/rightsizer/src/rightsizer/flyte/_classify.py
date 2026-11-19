"""Parent-side outcome classification (flyte.md §8.2).

| child result | status |
|---|---|
| ``flyte.errors.OOMError`` (USER kind, code containing "OOM", e.g. ``OOMKilled``) | ``OomKilled`` |
| code ``OutOfMemoryError`` (a CUDA/ROCm OOM raised by the task), or a message matching ``CUDA out of memory`` / ``HIP out of memory`` | ``GpuOom`` |
| ``flyte.errors.TaskTimeoutError`` (any of its subclasses) | ``Timeout`` |
| other | ``Failed`` |

In SDK 2.13.6 a CUDA OOM reaches the parent as ``RuntimeUserError`` with
``code="OutOfMemoryError"`` (the child's task runner wraps the exception as
``CustomError(<class name>)``), not as ``RuntimeUnknownError``; both are
matched by code.
"""

from __future__ import annotations

import re
from typing import Any

from rightsizer_runtime.wire import RunStatus

_GPU_OOM_MESSAGE = re.compile(r"\b(CUDA|HIP) out of memory", re.IGNORECASE)
_GPU_OOM_CODES = frozenset({"OutOfMemoryError", "CudaOutOfMemoryError", "torch.cuda.OutOfMemoryError"})


def _code(exc: BaseException) -> str:
    c = getattr(exc, "code", None)
    return c if isinstance(c, str) else ""


def classify(exc: BaseException) -> RunStatus:
    """The ``RunStatus`` for a child failure as the parent sees it."""
    try:
        import flyte.errors as fe
    except Exception:  # pragma: no cover - flyte is always there when this runs
        fe = None  # type: ignore[assignment]
    code = _code(exc)
    msg = str(exc)
    if fe is not None and isinstance(exc, fe.OOMError):
        return RunStatus.OOM_KILLED
    if code in _GPU_OOM_CODES or _GPU_OOM_MESSAGE.search(msg):
        return RunStatus.GPU_OOM
    if fe is not None and isinstance(exc, fe.TaskTimeoutError):
        return RunStatus.TIMEOUT
    # An OOM kill whose error was not converted to OOMError (other kinds).
    if "OOMKILLED" in code.upper() or code.upper() == "OOM":
        return RunStatus.OOM_KILLED
    return RunStatus.FAILED


def is_oom(status: Any) -> bool:
    return RunStatus.parse(status) in (RunStatus.OOM_KILLED, RunStatus.GPU_OOM)


__all__ = ["classify", "is_oom"]
