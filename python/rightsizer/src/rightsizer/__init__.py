"""rightsizer: size AI workload resource requests before they run.

::

    import rightsizer as rs

    d = rs.recommend(rs.Context(
        key="llama-ft",
        prior=rs.Resources(cpu=8, memory="64Gi", gpu="A100-80G:1"),
        inputs=[rs.Input.from_uri("s3://bucket/train.parquet")],
        code=rs.Code.from_callable(train_fn),
    ))
    print(d.applied.requests.memory, d.held, d.explanation)
    rs.outcome(d, status="succeeded", memory_peak="41Gi")

``import rightsizer`` loads only the standard library and
``rightsizer-runtime``: the native module ``rightsizer._core`` is imported
at the first decision (SPEC §10.2), and neither Flyte nor Ray is ever
imported here (``rightsizer.flyte`` / ``rightsizer.ray`` import their
platform, extras ``rightsizer[flyte]`` / ``rightsizer[ray]``).
"""

from __future__ import annotations

import importlib
from typing import Any

from rightsizer_runtime import AttemptContext, Mode, RunRecord, RunStatus, attempt_context, policy
from rightsizer_runtime.env import current_attempt, set_attempt_context

from ._engine import (
    Engine,
    aoutcome,
    arecommend,
    build_record,
    build_request,
    configure,
    decide_group,
    default_engine,
    flush,
    outcome,
    recommend,
)
from ._errors import Insufficient, Rejection, RightsizerError
from ._estimator import Estimator, clear_estimators, register_estimator, registered_estimators, unregister_estimator
from ._storage import object_store_options
from .types import Code, Context, Decision, Estimate, Hold, Input, OutcomeReport, Prior, Resources, Source

__version__ = "0.1.0.dev0"

# Loaded on first attribute access, never by `import rightsizer`.
_LAZY_SUBMODULES = ("_core", "plugin", "serve", "models", "export", "flyte", "ray", "cli")


def register_adapter(adapter: Any) -> Any:
    """Register an in-process Python adapter (``rightsizer.plugin.Adapter``)."""
    from .plugin import register_adapter as _register

    return _register(adapter)


def __getattr__(name: str) -> Any:
    if name in _LAZY_SUBMODULES:
        mod = importlib.import_module(f"{__name__}.{name}")
        globals()[name] = mod
        return mod
    raise AttributeError(f"module 'rightsizer' has no attribute {name!r}")


def __dir__() -> list:
    return sorted(set(globals()) | set(_LAZY_SUBMODULES))


__all__ = [
    "AttemptContext",
    "Code",
    "Context",
    "Decision",
    "Engine",
    "Estimate",
    "Estimator",
    "Hold",
    "Input",
    "Insufficient",
    "Mode",
    "OutcomeReport",
    "Prior",
    "Rejection",
    "Resources",
    "RightsizerError",
    "RunRecord",
    "RunStatus",
    "Source",
    "aoutcome",
    "arecommend",
    "attempt_context",
    "build_record",
    "build_request",
    "clear_estimators",
    "configure",
    "current_attempt",
    "decide_group",
    "default_engine",
    "flush",
    "object_store_options",
    "outcome",
    "policy",
    "recommend",
    "register_adapter",
    "register_estimator",
    "registered_estimators",
    "set_attempt_context",
    "unregister_estimator",
]
