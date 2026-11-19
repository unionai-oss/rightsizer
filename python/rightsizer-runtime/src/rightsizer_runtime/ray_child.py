"""Ray child-side code: in-task measurement for ``rs.ray.remote`` (ray.md §6).

The driver (``rightsizer.ray``) wraps the user's function with
:func:`make_task_function`. Ray pickles the wrapper (by value) and runs it
in a worker; the wrapper runs the body inside a
:class:`~.measure.MeasurementScope` with process attribution (workers are
reused: sampled RSS every 0.5 s minus the RSS at task start, ``Exact`` with
``resolution_s = 0.5``; NVML per-PID device memory if ``pynvml`` and a GPU
are present; process CPU time) and writes one :class:`~.wire.RunRecord`
directly to the store.

How the decision reaches the worker: the driver adds one hidden keyword
argument, :data:`META_KWARG` (``_rightsizer``), to every call. The wrapper's
``__signature__`` is the user's signature plus that keyword-only parameter
(the same pattern Ray's tracing uses for ``_ray_trace_ctx``), so Ray keeps
validating the user's arguments at submission. Passing it per call, rather
than through a per-task ``runtime_env``, keeps workers shared with every
other task of the job (a different ``runtime_env`` would get its own worker
pool).

The meta payload (a small dict, wire version 1)::

    {"v": 1, "k": key, "d": decision_id | None, "m": mode, "r": requested ResourceSpec | None,
     "s": store URI | None, "tr": trust, "c": cluster | None, "i": [InputMeta, ...], "cd": code digest | None}

Without a store in the payload the worker uses ``RIGHTSIZER__HISTORY__URI``
(``rsz ray submit`` sets it in ``runtime_env.env_vars``). Only object-store
URIs (local paths, ``file://``, fsspec schemes) are written from workers;
``sqlite://`` / ``postgres://`` stores run unmeasured.

Status, child side: ``Succeeded``; ``GpuOom`` for ``torch.cuda.OutOfMemoryError``
(gpu peak ``AtLeast`` device total); ``Failed`` otherwise (a Python
``MemoryError`` keeps ``Failed`` with an ``AtLeast`` memory peak). OOM kills
and crashes never reach ``finally``: the driver classifies those
(``rightsizer.ray``).

**S1.** The wrapper itself references nothing but builtins and its closure,
and imports this module lazily: a worker without ``rightsizer-runtime`` runs
the function unmeasured, and no rightsizer error reaches the task. The
function's result and exceptions pass through unchanged.
"""

from __future__ import annotations

import functools
import inspect
import logging
import os
import threading
from typing import Any, Callable, Dict, Mapping, Optional, Tuple

log = logging.getLogger("rightsizer.runtime.ray")

META_KWARG = "_rightsizer"
META_VERSION = 1
STORE_ENV = "RIGHTSIZER__HISTORY__URI"
_UNWRITABLE_SCHEMES = ("sqlite", "postgres", "postgresql", "memory+rsz")

_writers: Dict[str, Any] = {}
_writers_lock = threading.Lock()
_nvml_state: Dict[str, Optional[bool]] = {"ok": None}
_warned: set = set()


# ---------------------------------------------------------------- wrapper (driver side)


def _add_meta_param(sig: inspect.Signature) -> inspect.Signature:
    params = list(sig.parameters.values())
    if any(p.name == META_KWARG for p in params):
        return sig
    new = inspect.Parameter(META_KWARG, inspect.Parameter.KEYWORD_ONLY, default=None)
    idx = len(params)
    for i, p in enumerate(params):
        if p.kind is inspect.Parameter.VAR_KEYWORD:
            idx = i
            break
    params.insert(idx, new)
    return sig.replace(parameters=params)


def make_task_function(fn: Callable[..., Any], key: Optional[str] = None) -> Callable[..., Any]:
    """The function Ray runs for a rightsized remote function.

    It accepts the user's arguments plus the hidden ``_rightsizer`` keyword,
    runs ``fn`` measured (:func:`run_task`) and returns or raises exactly
    what ``fn`` does. Generator functions stay generator functions (Ray
    checks ``inspect.isgeneratorfunction``); async generators are returned
    unwrapped (not measured).
    """
    if inspect.isasyncgenfunction(fn) or getattr(fn, "__rightsizer_task__", False):
        return fn
    static_key = key

    # The bodies reference only builtins and closure cells (no module
    # globals), so cloudpickle ships them by value and unpickling needs no
    # rightsizer module; the runtime is imported lazily, by name.
    if inspect.isgeneratorfunction(fn):

        def _rsz_task_gen(*args: Any, _rightsizer: Any = None, **kwargs: Any) -> Any:
            try:
                runner = getattr(__import__("rightsizer_runtime.ray_child", fromlist=["*"]), "run_task_gen")
            except Exception:
                runner = None
            if runner is None:
                return (yield from fn(*args, **kwargs))
            return (yield from runner(fn, static_key, _rightsizer, args, kwargs))

        wrapper: Callable[..., Any] = _rsz_task_gen
    else:

        def _rsz_task(*args: Any, _rightsizer: Any = None, **kwargs: Any) -> Any:
            try:
                runner = getattr(__import__("rightsizer_runtime.ray_child", fromlist=["*"]), "run_task")
            except Exception:
                runner = None
            if runner is None:
                return fn(*args, **kwargs)
            return runner(fn, static_key, _rightsizer, args, kwargs)

        wrapper = _rsz_task

    functools.update_wrapper(wrapper, fn)
    try:
        wrapper.__signature__ = _add_meta_param(inspect.signature(fn))  # type: ignore[attr-defined]
    except (TypeError, ValueError):
        pass
    wrapper.__rightsizer_task__ = True  # type: ignore[attr-defined]
    return wrapper


# ---------------------------------------------------------------- worker side


def _store_for(meta: Optional[Mapping[str, Any]], environ: Optional[Mapping[str, str]] = None) -> Optional[str]:
    env = os.environ if environ is None else environ
    s = (meta or {}).get("s") or env.get(STORE_ENV)
    if not s:
        return None
    scheme = s.split("://", 1)[0].lower() if "://" in s else ""
    if scheme in _UNWRITABLE_SCHEMES:
        if scheme not in _warned:
            _warned.add(scheme)
            log.warning("rightsizer: %s:// stores are not written from Ray workers; tasks run unmeasured", scheme)
        return None
    return s


def writer_for(uri: str) -> Any:
    """A process-wide :class:`~.record.RunRecordWriter` per store URI."""
    w = _writers.get(uri)
    if w is not None:
        return w
    with _writers_lock:
        w = _writers.get(uri)
        if w is None:
            from .record import RunRecordWriter

            w = RunRecordWriter(uri)
            _writers[uri] = w
    return w


def task_identity() -> Tuple[Optional[str], int]:
    """(Ray task id hex, 1-based attempt) of the running task; ``(None, 1)`` outside Ray."""
    try:
        import ray

        ctx = ray.get_runtime_context()
        tid = ctx.get_task_id()
        attempt = 1
        try:
            attempt = int(ray._private.worker.global_worker.core_worker.get_current_task_attempt_number()) + 1
        except Exception:
            pass
        return tid, max(1, attempt)
    except Exception:
        return None, 1


def is_torch_cuda_oom(exc: Optional[BaseException]) -> bool:
    """``torch.cuda.OutOfMemoryError`` (or a subclass), without importing torch."""
    if exc is None:
        return False
    for c in type(exc).__mro__:
        if c.__name__ == "OutOfMemoryError" and (c.__module__ or "").startswith("torch"):
            return True
    return False


def child_status(exc: Optional[BaseException]):
    """Child-side status for how the body ended."""
    from .child import status_for_exception
    from .wire import RunStatus

    if is_torch_cuda_oom(exc):
        return RunStatus.GPU_OOM
    return status_for_exception(exc)


INPUT_READ_BUDGET_MS = 250


def resolve_inputs(refs: Any) -> Tuple[Any, ...]:
    """``InputMeta``s for the driver's input refs (``{"name", "uri"?, "bytes"?, "format"?, "scalar"?}``).

    With ``rightsizer._core`` importable, its metadata readers resolve URIs
    (footers and sizes only, within :data:`INPUT_READ_BUDGET_MS`, salted URI
    hashes as at decision time). Without it, inputs of known size are kept
    as blobs and URIs are dropped. Never raises.
    """
    from .wire import InputMeta

    refs = [dict(r) for r in (refs or ()) if isinstance(r, Mapping)]
    if not refs:
        return ()
    try:
        import json

        core = __import__("rightsizer._core", fromlist=["*"])
        metas = json.loads(core.read_inputs(json.dumps(refs), INPUT_READ_BUDGET_MS))
        return tuple(InputMeta.from_dict(m, allow_unknown=True) for m in metas)
    except Exception:
        pass
    out = []
    for r in refs:
        try:
            if r.get("scalar") is not None:
                out.append(InputMeta.from_dict({"name": r["name"], "format": "scalar", "scalar": r["scalar"]}, allow_unknown=True))
            elif r.get("bytes") is not None:
                out.append(InputMeta.from_dict({"name": r["name"], "format": r.get("format") or "blob", "bytes": r["bytes"]},
                                               allow_unknown=True))
        except Exception:
            continue
    return tuple(out)


class _Run:
    """One measured task execution (guarded: never raises)."""

    def __init__(self, static_key: Optional[str], meta: Any):
        self.scope = None
        self.meta: Dict[str, Any] = dict(meta) if isinstance(meta, Mapping) else {}
        self.key = self.meta.get("k") or static_key
        self.store: Optional[str] = None

    def start(self) -> None:
        try:
            if self.meta.get("m") == "off" or not self.key:
                return
            self.store = _store_for(self.meta)
            if self.store is None:
                return
            from .measure import RAY_INTERVAL_S, MeasurementScope

            nv = _nvml_state["ok"]
            self.scope = MeasurementScope(RAY_INTERVAL_S, attribution="process", cgroup=None, nvml=nv is not False)
            self.scope.start()
            if nv is None:
                _nvml_state["ok"] = getattr(self.scope, "_nv", None) is not None
        except Exception as e:
            log.warning("rightsizer: measurement not started: %s", e)
            self.scope = None

    def end(self, exc: Optional[BaseException]) -> None:
        scope = self.scope
        if scope is None:
            return
        try:
            from dataclasses import replace

            from .wire import (
                Censor,
                Confidence,
                OutcomeSource,
                ResourceSpec,
                RunRecord,
                Trust,
                new_ulid,
                parse_ulid,
                utcnow,
            )

            status = child_status(exc)
            r = scope.stop(status)
            if isinstance(exc, MemoryError) and r is not None and r.memory_peak is not None:
                r = replace(r, memory_peak=r.memory_peak.with_censor(Censor.AT_LEAST))
            m = self.meta
            tid, attempt = task_identity()
            did = m.get("d")
            try:
                did = parse_ulid(did) if did else None
            except Exception:
                did = None
            req = m.get("r")
            rec = RunRecord(
                key=self.key,
                cluster=m.get("c"),
                invocation_id=tid or did or new_ulid(),
                attempt=attempt,
                decision_id=did,
                started_at=r.started_at if r is not None else utcnow(),
                inputs=resolve_inputs(m.get("i")),
                code_digest=m.get("cd"),
                requested=ResourceSpec.from_dict(req, allow_unknown=True) if req else None,
                status=status,
                status_confidence=Confidence.OBSERVED,
                memory_peak=r.memory_peak if r else None,
                cpu_mean=r.cpu_mean if r else None,
                cpu_peak_window=r.cpu_peak_window if r else None,
                gpu_memory_peak=r.gpu_memory_peak if r else None,
                gpu_util_mean=r.gpu_util_mean if r else None,
                t_peak_frac=r.t_peak_frac if (r and r.memory_peak is not None) else None,
                duration_s=r.duration_s if r else None,
                source=OutcomeSource.self_report(),
                trust=Trust.parse(m.get("tr") or "unverified"),
            )
            writer_for(self.store).write(rec)  # type: ignore[arg-type]
        except Exception as e:
            log.warning("rightsizer: RunRecord not written: %s", e)


def run_task(fn: Callable[..., Any], static_key: Optional[str], meta: Any, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Any:
    """Run ``fn(*args, **kwargs)`` measured; returns/raises exactly what ``fn`` does."""
    run = _Run(static_key, meta)
    run.start()
    exc: Optional[BaseException] = None
    try:
        return fn(*args, **kwargs)
    except BaseException as e:
        exc = e
        raise
    finally:
        run.end(exc)


def run_task_gen(fn: Callable[..., Any], static_key: Optional[str], meta: Any, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Any:
    """Generator variant of :func:`run_task` (measures until the generator is exhausted)."""
    run = _Run(static_key, meta)
    run.start()
    exc: Optional[BaseException] = None
    try:
        return (yield from fn(*args, **kwargs))
    except BaseException as e:
        exc = e
        raise
    finally:
        run.end(exc)


__all__ = [
    "META_KWARG",
    "META_VERSION",
    "STORE_ENV",
    "child_status",
    "is_torch_cuda_oom",
    "make_task_function",
    "run_task",
    "run_task_gen",
    "task_identity",
    "writer_for",
]
