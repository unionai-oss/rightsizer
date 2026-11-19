"""Python estimators in the Rust chain (SPEC §10.3).

::

    class Mine(rs.Estimator):
        id = "mine"
        version = "1"

        def estimate(self, ctx):            # a WorkloadContext dict
            if not ctx["history"]:
                return None                  # or raise rs.Insufficient(...)
            return rs.Estimate.cells(probs, estimator=self.id, version=self.version)

    rs.register_estimator(Mine())

Registered estimators go in front of every engine's configured chain, in
registration order (re-registering an id replaces it). A sync ``estimate``
runs on the runtime's blocking pool (GIL reacquired there); an ``async def
estimate`` runs on the caller's event loop for ``rs.arecommend`` and under
its own ``asyncio.run`` for the synchronous ``rs.recommend``.

``call_sync`` / ``call_async`` / ``call_async_blocking`` are what ``_core``
calls; they never raise and return ``(tag, payload)``.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import re
from typing import Any, Callable, ClassVar, Dict, Mapping, Optional, Sequence, Tuple, Union

from ._errors import Insufficient, RightsizerError
from ._native import core

log = logging.getLogger("rightsizer.estimator")

_AXES = ("memory", "cpu_mean", "cpu_peak", "gpu_memory", "duration")


class Estimator:
    """Base class for Python estimators. Set ``id`` and ``version`` (they join
    the memo key) and implement ``estimate(ctx) -> Estimate | dict | None``.

    ``ctx`` is the ``WorkloadContext`` as a dict (in-process estimators see
    the whole context, SPEC §11). Return ``None`` or raise
    :class:`rightsizer.Insufficient` to pass to the next estimator; any other
    exception is an estimator failure (also passes on, S1, and is counted).
    A missing ``provenance`` is filled in from ``id`` / ``version``.
    """

    id: ClassVar[Optional[str]] = None
    version: ClassVar[str] = "0"
    axes: ClassVar[Sequence[str]] = ("memory", "cpu_mean")
    kind: ClassVar[str] = "demand"
    grids: ClassVar[Sequence[str]] = ("rs-grid-v1",)
    needs: ClassVar[Mapping[str, bool]] = {"code": False, "history": True, "input_bytes": True}
    features: ClassVar[str] = "features-v1"

    def estimate(self, ctx: Dict[str, Any]) -> Any:  # pragma: no cover - interface
        raise NotImplementedError

    def info(self) -> Dict[str, Any]:
        """``EstimatorInfo`` (SPEC §6.1)."""
        ident = self.id or _kebab(type(self).__name__)
        axes = [a if a != "duration_s" else "duration" for a in self.axes]
        for a in axes:
            if a not in _AXES:
                raise ValueError(f"estimator {ident}: unknown axis {a!r} ({', '.join(_AXES)})")
        return {
            "id": ident,
            "version": str(self.version),
            "grids": list(self.grids),
            "axes": axes,
            "kind": self.kind,
            "needs": dict(self.needs),
            "features": self.features,
        }


def _kebab(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "-", name).lower()


class FunctionEstimator(Estimator):
    """A plain callable ``ctx -> estimate`` as an estimator."""

    def __init__(self, fn: Callable[[Dict[str, Any]], Any], id: Optional[str] = None, version: str = "0",
                 axes: Optional[Sequence[str]] = None):
        self._fn = fn
        self._id = id or getattr(fn, "__name__", "python").replace("_", "-")
        self._version = version
        self._axes = tuple(axes) if axes is not None else Estimator.axes
        if inspect.iscoroutinefunction(fn):
            async def estimate(ctx: Dict[str, Any]) -> Any:
                return await fn(ctx)
        else:
            def estimate(ctx: Dict[str, Any]) -> Any:  # type: ignore[misc]
                return fn(ctx)
        self.estimate = estimate  # type: ignore[method-assign]

    def info(self) -> Dict[str, Any]:
        d = super().info()
        d.update(id=self._id, version=self._version, axes=[a if a != "duration_s" else "duration" for a in self._axes])
        return d


def _info_of(obj: Any) -> Dict[str, Any]:
    if hasattr(obj, "info") and callable(obj.info):
        info = dict(obj.info())
    else:
        info = {}
    if not info.get("id"):
        info["id"] = getattr(obj, "id", None) or _kebab(type(obj).__name__)
    info.setdefault("version", str(getattr(obj, "version", "0")))
    info.setdefault("axes", ["memory", "cpu_mean"])
    return info


def register_estimator(est: Any = None, *, id: Optional[str] = None, version: Optional[str] = None) -> Any:
    """Register a Python estimator: an :class:`Estimator` instance or class
    (instantiated with no arguments), an object with ``estimate(ctx)``, or a
    plain function ``ctx -> estimate``. Usable as a decorator; returns its
    argument. Raises :class:`RightsizerError` when ``_core`` is missing.
    """
    if est is None:
        return lambda e: register_estimator(e, id=id, version=version)
    obj = est() if isinstance(est, type) else est
    if not hasattr(obj, "estimate"):
        if not callable(obj):
            raise TypeError("register_estimator: expected an Estimator, an object with estimate(ctx), or a function")
        obj = FunctionEstimator(obj, id=id, version=version or "0")
    info = _info_of(obj)
    if id is not None:
        info["id"] = id
    if version is not None:
        info["version"] = str(version)
    is_async = inspect.iscoroutinefunction(getattr(obj, "estimate"))
    try:
        c = core()
    except ImportError as e:
        raise RightsizerError(str(e)) from e
    c.register_estimator(_Bound(obj, info), json.dumps(info), is_async)
    return est


def unregister_estimator(id: str) -> bool:
    try:
        return bool(core().unregister_estimator(id))
    except ImportError:
        return False


def clear_estimators() -> None:
    try:
        core().clear_estimators()
    except ImportError:
        pass


def registered_estimators() -> list:
    try:
        return list(core().registered_estimators())
    except ImportError:
        return []


class _Bound:
    """An estimator with its resolved info (for provenance defaults)."""

    __slots__ = ("obj", "info")

    def __init__(self, obj: Any, info: Dict[str, Any]):
        self.obj = obj
        self.info = info


def _declined(e: BaseException) -> bool:
    # rightsizer.Insufficient, or any exception class named Insufficient
    # (e.g. rightsizer.serve.Insufficient).
    return isinstance(e, Insufficient) or any(c.__name__ == "Insufficient" for c in type(e).__mro__)


def _finish(b: _Bound, out: Any) -> Tuple[str, str]:
    if out is None:
        return ("insufficient", f"{b.info['id']} returned None")
    if hasattr(out, "to_dict") and callable(out.to_dict):
        out = out.to_dict()
    if isinstance(out, (str, bytes)):
        out = json.loads(out)
    if not isinstance(out, dict):
        return ("error", f"{b.info['id']}: estimate must return an Estimate, a dict or None, not {type(out).__name__}")
    out = dict(out)
    prov = dict(out.get("provenance") or {})
    prov.setdefault("estimator", b.info["id"])
    prov.setdefault("version", b.info.get("version", "0"))
    out["provenance"] = prov
    out.setdefault("v", 1)
    return ("ok", json.dumps(out, allow_nan=False))


def _fail(b: _Bound, e: BaseException) -> Tuple[str, str]:
    if _declined(e):
        return ("insufficient", str(e) or f"{b.info['id']} declined")
    log.warning("rightsizer: estimator %s failed: %s: %s", b.info.get("id"), type(e).__name__, e)
    return ("error", f"{type(e).__name__}: {e}")


def call_sync(b: _Bound, ctx_json: str) -> Tuple[str, str]:
    try:
        out = b.obj.estimate(json.loads(ctx_json))
        if inspect.isawaitable(out):
            out = asyncio.run(_awaited(out))
        return _finish(b, out)
    except BaseException as e:  # noqa: BLE001 - S1: nothing escapes into Rust
        if isinstance(e, (KeyboardInterrupt, SystemExit)):
            raise
        return _fail(b, e)


async def _awaited(aw: Any) -> Any:
    return await aw


async def call_async(b: _Bound, ctx_json: str) -> Tuple[str, str]:
    try:
        return _finish(b, await b.obj.estimate(json.loads(ctx_json)))
    except BaseException as e:  # noqa: BLE001
        if isinstance(e, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
            raise
        return _fail(b, e)


def call_async_blocking(b: _Bound, ctx_json: str) -> Tuple[str, str]:
    """An ``async def estimate`` from the synchronous API: its own loop."""
    try:
        return asyncio.run(call_async(b, ctx_json))
    except BaseException as e:  # noqa: BLE001
        if isinstance(e, (KeyboardInterrupt, SystemExit)):
            raise
        return _fail(b, e)


EstimatorLike = Union[Estimator, Callable[[Dict[str, Any]], Any]]

__all__ = [
    "Estimator", "EstimatorLike", "FunctionEstimator", "clear_estimators", "register_estimator",
    "registered_estimators", "unregister_estimator",
]
