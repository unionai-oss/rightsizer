"""Ray child-side code: in-task measurement for ``rs.ray.remote`` (ray.md §6).

The driver (``rightsizer.ray``) wraps the user's function with
:func:`make_task_function`. Ray pickles the wrapper (by value) and runs it
in a worker; the wrapper runs the body inside a
:class:`~.measure.MeasurementScope` with process attribution (workers are
reused: sampled RSS every 0.5 s minus the RSS at task start, ``Exact`` with
``resolution_s = 0.5``, re-based for reused workers, see
:func:`reused_worker_peak`; NVML per-PID device memory if ``pynvml`` and a GPU
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
     "s": store URI | None, "tr": trust, "c": cluster | None, "i": [InputRef, ...], "cd": code digest | None}

(``i`` holds the decision's input references; the worker turns them into
``InputMeta`` with the metadata readers when ``rightsizer._core`` is
importable, see :func:`resolve_inputs`.)

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
    if inspect.isasyncgenfunction(fn) or inspect.iscoroutinefunction(fn) or getattr(fn, "__rightsizer_task__", False):
        # ``async def``: returned unwrapped, so Ray's own definition-time
        # ``ValueError`` still applies (a sync wrapper would hide the coroutine
        # function from Ray and fail at run time instead).
        return fn
    try:
        if META_KWARG in inspect.signature(fn).parameters:
            # The user's own ``_rightsizer`` parameter: wrapping would swallow
            # its value. Such functions run unmeasured (and get no meta).
            return fn
    except (TypeError, ValueError):
        pass
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


class _MachFootprint:
    """macOS ``task_vm_info.phys_footprint`` (bytes).

    Reused Ray workers make ``resident_size`` useless as a per-task delta on
    macOS: the allocator keeps freed pages resident (``MADV_FREE``), so they
    land in the next task's baseline and its own allocations read as ~0.
    ``phys_footprint`` (what Activity Monitor shows) excludes them.
    """

    TASK_VM_INFO = 22

    def __init__(self) -> None:
        import ctypes

        class _Info(ctypes.Structure):
            _pack_ = 4
            _fields_ = [("virtual_size", ctypes.c_uint64), ("region_count", ctypes.c_int32),
                        ("page_size", ctypes.c_int32), ("resident_size", ctypes.c_uint64),
                        ("resident_size_peak", ctypes.c_uint64)]
            _fields_ += [(n, ctypes.c_uint64) for n in (
                "device", "device_peak", "internal", "internal_peak", "external", "external_peak", "reusable",
                "reusable_peak", "purgeable_volatile_pmap", "purgeable_volatile_resident",
                "purgeable_volatile_virtual", "compressed", "compressed_peak", "compressed_lifetime",
                "phys_footprint", "min_address", "max_address")]

        self._ct = ctypes
        self._Info = _Info
        libc = ctypes.CDLL(None)
        self._task = ctypes.c_uint32.in_dll(libc, "mach_task_self_").value
        self._task_info = libc.task_info
        self._task_info.argtypes = [ctypes.c_uint32, ctypes.c_int, ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint32)]
        self._task_info.restype = ctypes.c_int

    def __call__(self) -> Optional[int]:
        info = self._Info()
        count = self._ct.c_uint32(self._ct.sizeof(info) // 4)
        kr = self._task_info(self._task, self.TASK_VM_INFO, self._ct.byref(info), self._ct.byref(count))
        return int(info.phys_footprint) if kr == 0 and info.phys_footprint else None


_probe_cache: Dict[str, Any] = {}
_baseline: Dict[str, Optional[int]] = {"min": None}
# A task that starts within this much of the worker's lowest baseline has a clean baseline.
CLEAN_BASELINE_SLACK_BYTES = 16 << 20


def reused_worker_peak(scope: Any, r: Any) -> Any:
    """Re-base a task's memory peak for reused workers (ray.md §6).

    The scope subtracts the RSS at task start. On a reused worker that
    baseline can include memory a previous task freed but the allocator kept
    (glibc's dynamic mmap threshold, pymalloc arenas, macOS malloc's caches):
    a task that reuses it would read as ~0. So the peak is taken against the
    **lowest** start RSS this worker process has seen, and it is ``Exact``
    only when this task started within :data:`CLEAN_BASELINE_SLACK_BYTES` of
    that; otherwise it is ``AtMost`` (an upper bound: it may include
    recycled memory), which S3(b) still accepts and which never under-states
    the task's need. Never raises.
    """
    try:
        from dataclasses import replace

        from .wire import Censor, Measurement

        base = getattr(scope, "_rss_base", None)
        top = getattr(scope, "_rss_max", None)
        if r is None or r.memory_peak is None or base is None or top is None:
            return r
        lo = _baseline["min"]
        lo = base if lo is None else min(lo, base)
        _baseline["min"] = lo
        clean = base - lo <= CLEAN_BASELINE_SLACK_BYTES
        value = float(max(0, top - lo))
        mp = r.memory_peak
        censor = mp.censor
        if not clean and censor is Censor.EXACT:
            censor = Censor.AT_MOST
        return replace(r, memory_peak=Measurement(max(value, mp.value), censor, mp.resolution_s))
    except Exception as e:
        log.debug("rightsizer: peak re-basing skipped: %s", e)
        return r


def process_memory_probe() -> Any:
    """The per-process memory probe for Ray tasks: ``phys_footprint`` on macOS,
    else the runtime's default RSS probe (Linux ``/proc/self/statm``)."""
    if "p" in _probe_cache:
        return _probe_cache["p"]
    from .measure import _AUTO

    probe: Any = _AUTO
    import sys

    if sys.platform == "darwin":
        try:
            p = _MachFootprint()
            if p() is not None:
                probe = p
        except Exception as e:  # pragma: no cover - platform dependent
            log.debug("rightsizer: phys_footprint probe unavailable: %s", e)
    _probe_cache["p"] = probe
    return probe


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
                m = {"name": r["name"], "format": "scalar", "scalar": r["scalar"]}
                out.append(InputMeta.from_dict(m, allow_unknown=True))
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
            self.scope = MeasurementScope(RAY_INTERVAL_S, attribution="process", cgroup=None, nvml=nv is not False,
                                          rss_probe=process_memory_probe())
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
            r = reused_worker_peak(scope, r)
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


def run_task_gen(fn: Callable[..., Any], static_key: Optional[str], meta: Any, args: Tuple[Any, ...],
                 kwargs: Dict[str, Any]) -> Any:
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
