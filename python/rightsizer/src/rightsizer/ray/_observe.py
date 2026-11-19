"""Driver-side outcomes: how a rightsized task ended (ray.md §6 "Status").

**Classification** (:func:`classify`, pure):

- ``ray.exceptions.OutOfMemoryError`` (the raylet's memory monitor) →
  ``OomKilled`` (observed);
- ``WorkerCrashedError`` → ``Failed``, upgraded to ``OomKilled`` (inferred)
  when the State API or the error text shows a memory-monitor, kernel
  OOM-killer or cgroup kill;
- ``RayTaskError`` wrapping ``torch.cuda.OutOfMemoryError`` → ``GpuOom``;
  any other ``RayTaskError`` → ``Failed``;
- ``TaskCancelledError`` → ``Unknown``; node death → ``Preempted``.

**Observation without changing the user's semantics.** For every task it
sizes, the driver registers ``ObjectRef._on_ready`` (Ray 2.60: a callback
when the object *exists*, without fetching or deserializing it; unlike
``_on_completed``, which deserializes the value, and unlike ``ray.get``).
The callback only enqueues; a daemon thread then:

1. reads the object's size from the owner's reference table
   (``get_local_object_locations``, no RPC); a return larger than Ray's
   inline limit (100 KiB) lives in plasma and is a success (errors are
   stored inline), and is never touched;
2. otherwise reads the inlined entry from the owner's in-memory store
   (``core_worker.get_objects(timeout=0)``: a local lookup, no transfer)
   and looks only at its **metadata**: user values (``RAW`` / ``PYTHON``…)
   are never deserialized; a Ray error type is deserialized (it is Ray's
   own error object) and classified.

The ref is held only until the callback runs (the task's lifetime), then
dropped. Records the worker could not write (OOM kills, crashes) are
written here through ``engine.outcome`` (source ``ray_state``); user
exceptions are recorded by the worker itself (``self_report``), so the
driver skips them for measured functions.

Retries Ray absorbs (``max_retries``, ``task_oom_retries``) are not seen
here: only the final attempt reaches the owner.
"""

from __future__ import annotations

import logging
import queue
import re
import threading
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

log = logging.getLogger("rightsizer.ray")

INLINE_LIMIT_BYTES = 100 * 1024  # Ray's max_direct_call_object_size
# Evidence of a memory kill in a WORKER_DIED error / State API message. Ray's
# generic WORKER_DIED text lists "killed by the OOM killer" as a *possible*
# cause, so that phrase alone is not evidence.
_OOM_TEXT = re.compile(
    r"running low on memory|memory monitor|memory_usage_threshold|\bOOMKilled\b|oom-kill:|oom_kill_process|"
    r"[Mm]emory cgroup out of memory|cgroup[^\n]{0,40}memory\.(?:max|events)|Killed process \d+ \(",
)


@dataclass(frozen=True)
class Classified:
    status: str  # RunStatus value
    confidence: str  # "observed" | "inferred"
    reason: str
    worker_reported: bool  # the worker's own finally ran (user exception)


def _cause(exc: BaseException) -> Optional[BaseException]:
    c = getattr(exc, "cause", None)
    if isinstance(c, BaseException):
        return c
    return exc.__cause__


def classify(exc: Optional[BaseException], *, state_error_type: Optional[str] = None,
             state_error_message: Optional[str] = None) -> Classified:
    """Status for how a task ended, from the exception the owner holds
    (``None`` = success) and, optionally, the State API's ``error_type`` /
    ``error_message`` for that task."""
    from rightsizer_runtime.ray_child import is_torch_cuda_oom

    if exc is None:
        return Classified("succeeded", "observed", "ok", False)
    try:
        import ray.exceptions as rx
    except Exception:  # pragma: no cover
        rx = None  # type: ignore[assignment]
    name = type(exc).__name__
    ray_oom = rx is not None and isinstance(exc, rx.OutOfMemoryError)
    if ray_oom or (name == "OutOfMemoryError" and "ray" in (type(exc).__module__ or "")):
        return Classified("oom_killed", "observed", "ray_memory_monitor", False)
    if state_error_type == "OUT_OF_MEMORY":
        return Classified("oom_killed", "observed", "state_api_out_of_memory", False)
    if rx is not None and isinstance(exc, rx.RayTaskError):
        cause = _cause(exc)
        if is_torch_cuda_oom(cause) or is_torch_cuda_oom(exc):
            return Classified("gpu_oom", "observed", "torch_cuda_oom", True)
        return Classified("failed", "observed", f"task_error:{type(cause).__name__ if cause else name}", True)
    if rx is not None and isinstance(exc, rx.WorkerCrashedError):
        text = " ".join(t for t in (state_error_message, str(exc)) if t)
        if _OOM_TEXT.search(text or ""):
            return Classified("oom_killed", "inferred", "worker_died_oom", False)
        return Classified("failed", "observed", "worker_died", False)
    if rx is not None and isinstance(exc, getattr(rx, "TaskCancelledError", ())):
        return Classified("unknown", "observed", "cancelled", False)
    if rx is not None and isinstance(exc, getattr(rx, "NodeDiedError", ())):
        return Classified("preempted", "inferred", "node_died", False)
    if rx is not None and isinstance(exc, getattr(rx, "LocalRayletDiedError", ())):
        return Classified("preempted", "inferred", "raylet_died", False)
    return Classified("failed", "inferred", f"ray_error:{name}", False)


# ── the observer ─────────────────────────────────────────────────────────


@dataclass
class Tracked:
    ref: Any
    key: str
    decision: Any  # rightsizer.Decision
    measured: bool
    inputs: Tuple[Dict[str, Any], ...]
    code_digest: Optional[str]
    trust: str
    cluster: Optional[str]
    engine: Any


class Observer:
    """One per process: a queue fed by ``_on_ready`` callbacks and a daemon thread."""

    def __init__(self) -> None:
        self._q: "queue.Queue[Optional[Tracked]]" = queue.Queue()
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self.stats: Dict[str, int] = {"tracked": 0, "processed": 0, "written": 0, "skipped_plasma": 0, "errors": 0}
        self._pid: Optional[int] = None
        self._idle = threading.Condition()
        self._pending = 0

    def _ensure(self) -> None:
        import os

        if self._thread is not None and self._pid == os.getpid() and self._thread.is_alive():
            return
        with self._lock:
            if self._thread is not None and self._pid == os.getpid() and self._thread.is_alive():
                return
            self._pid = os.getpid()
            self._thread = threading.Thread(target=self._run, name="rightsizer-ray-observer", daemon=True)
            self._thread.start()

    def track(self, t: Tracked) -> bool:
        """Register ``_on_ready`` for ``t.ref``. Never raises; returns whether it is tracked."""
        try:
            self._ensure()
            q = self._q

            def _cb(_exc: Any = None, _t: Tracked = t) -> None:
                q.put(_t)

            with self._idle:
                self._pending += 1
            t.ref._on_ready(_cb)
            self.stats["tracked"] += 1
            return True
        except Exception as e:
            with self._idle:
                self._pending = max(0, self._pending - 1)
            log.debug("rightsizer.ray: not observing %s: %s", getattr(t, "key", "?"), e)
            return False

    def wait_idle(self, timeout: float = 30.0) -> bool:
        """Block until every tracked task was processed (tests, shutdown)."""
        with self._idle:
            return self._idle.wait_for(lambda: self._pending == 0, timeout)

    def _run(self) -> None:
        while True:
            t = self._q.get()
            if t is None:
                return
            try:
                self._process(t)
            except Exception as e:
                self.stats["errors"] += 1
                log.debug("rightsizer.ray: outcome for %s not recorded: %s", t.key, e)
            finally:
                self.stats["processed"] += 1
                t.ref = None  # drop our reference as soon as possible
                with self._idle:
                    self._pending = max(0, self._pending - 1)
                    self._idle.notify_all()

    # -- per task --

    def peek(self, ref: Any) -> Tuple[Optional[BaseException], str]:
        """``(exception or None, how)`` for a ready ref, never touching user values."""
        import ray
        from ray.experimental import get_local_object_locations

        loc = (get_local_object_locations([ref]) or {}).get(ref) or {}
        size = loc.get("object_size")
        # A plasma object (it has node locations, or is too big to be inlined)
        # is a success and is never read: a zero-timeout ``get_objects`` on it
        # would still start pulling it to this node.
        if (isinstance(size, int) and size > INLINE_LIMIT_BYTES) or loc.get("node_ids") or loc.get("did_spill"):
            self.stats["skipped_plasma"] += 1
            return None, "plasma"
        w = ray._private.worker.global_worker
        objs = w.core_worker.get_objects([ref], 0)
        data, meta, *_ = objs[0]
        if not meta:
            return None, "no_metadata"
        head = bytes(meta).split(b",", 1)[0]
        if not head.isdigit():
            return None, "value"  # RAW / PYTHON / ACTOR_HANDLE …: a user value, left alone
        vals = w.deserialize_objects(objs, [ref])
        v = vals[0]
        return (v if isinstance(v, BaseException) else None), "error"

    def _process(self, t: Tracked) -> None:
        exc, how = self.peek(t.ref)
        if exc is None:
            return  # successes are recorded by the worker (measured) or by importers
        state_type = state_msg = None
        if type(exc).__name__ == "WorkerCrashedError":
            state_type, state_msg = state_api_error(t.ref)
        c = classify(exc, state_error_type=state_type, state_error_message=state_msg)
        if c.worker_reported and t.measured:
            return
        record_outcome(t, c)


def state_api_error(ref: Any) -> Tuple[Optional[str], Optional[str]]:
    """``(error_type, error_message)`` from the State API (needs the dashboard). Never raises."""
    try:
        from ray.util.state import get_task

        st = get_task(ref.task_id().hex(), timeout=5)
        if isinstance(st, list):
            st = st[-1] if st else None
        if st is None:
            return None, None
        return getattr(st, "error_type", None), getattr(st, "error_message", None)
    except Exception as e:
        log.debug("rightsizer.ray: state API unavailable: %s", e)
        return None, None


def record_outcome(t: Tracked, c: Classified) -> None:
    """Write the driver's RunRecord for ``t`` through the engine (S1: logs on failure)."""
    from rightsizer_runtime.wire import Confidence, OutcomeSource, OutcomeSourceKind, RunStatus, Trust

    from .._engine import build_record

    d = t.decision
    attempt = 1
    rec = build_record(
        d,
        RunStatus.parse(c.status),
        invocation_id=t.ref.task_id().hex() if t.ref is not None else d.id,
        attempt=attempt,
        status_confidence=Confidence.parse(c.confidence),
        source=OutcomeSource(OutcomeSourceKind.RAY_STATE),
        trust=Trust.parse(t.trust),
        cluster=t.cluster,
    )
    t.engine.outcome(rec)
    _observer.stats["written"] += 1


_observer = Observer()


def observer() -> Observer:
    return _observer


__all__ = ["Classified", "Observer", "Tracked", "classify", "observer", "record_outcome", "state_api_error"]
