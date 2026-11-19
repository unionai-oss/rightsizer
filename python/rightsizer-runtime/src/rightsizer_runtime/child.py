"""Platform-neutral child-side helper (flyte.md §8.1, ray.md §6).

:func:`measured_execution` wraps a callable in a :class:`~.measure.MeasurementScope`
inside ``try/finally`` and writes one :class:`~.wire.RunRecord` with status
``Succeeded`` or ``Failed``. OOM kills skip ``finally`` entirely; the parent
side records those outcomes. Flyte and Ray glue (reading their contexts,
choosing tenants) lives in the ``rightsizer`` package and calls this.

S1: no failure of rightsizer's own (bad env, unwritable store, probe errors)
reaches the wrapped callable or its caller; the callable's own result and
exceptions pass through unchanged.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import logging
from typing import Any, Callable, Iterable, Mapping, Optional, Tuple, TypeVar, Union

from .env import DecisionEnv, current_attempt, read_decision
from .measure import FLYTE_INTERVAL_S, RAY_INTERVAL_S, MeasurementResult, MeasurementScope
from .record import RunRecordWriter
from .wire import (
    AttemptContext,
    Censor,
    Confidence,
    InputMeta,
    Mode,
    OutcomeSource,
    RunRecord,
    RunStatus,
    Trust,
    new_ulid,
    utcnow,
)

log = logging.getLogger("rightsizer.runtime.child")

# platform → (sampling interval, attribution)
PLATFORM_PRESETS = {
    "flyte": (FLYTE_INTERVAL_S, "container"),
    "k8s": (FLYTE_INTERVAL_S, "container"),
    "slurm": (FLYTE_INTERVAL_S, "container"),
    "ray": (RAY_INTERVAL_S, "process"),
}

_FROM_ENV = object()
F = TypeVar("F", bound=Callable[..., Any])


def status_for_exception(exc: Optional[BaseException]) -> RunStatus:
    """Child-side status: ``Succeeded`` / ``Failed`` (``Unknown`` for cancellation and interrupts)."""
    if exc is None:
        return RunStatus.SUCCEEDED
    if isinstance(exc, SystemExit):
        return RunStatus.SUCCEEDED if exc.code in (None, 0) else RunStatus.FAILED
    if isinstance(exc, (KeyboardInterrupt, asyncio.CancelledError, GeneratorExit)):
        return RunStatus.UNKNOWN
    return RunStatus.FAILED


class MeasuredExecution:
    """See :func:`measured_execution`."""

    def __init__(
        self,
        decision_env: Any = _FROM_ENV,
        writer: Optional[RunRecordWriter] = None,
        *,
        platform: str = "flyte",
        key: Optional[str] = None,
        tenant: Optional[str] = None,
        attempt: Any = _FROM_ENV,
        trust: Union[Trust, str] = Trust.UNVERIFIED,
        cluster: Optional[str] = None,
        code_digest: Optional[str] = None,
        inputs: Iterable[InputMeta] = (),
        reusable: bool = False,
        environ: Optional[Mapping[str, str]] = None,
        scope_factory: Optional[Callable[[], MeasurementScope]] = None,
        **scope_kwargs: Any,
    ):
        self.platform = platform
        self.writer = writer
        self.key = key
        self.tenant = tenant
        self.trust = trust
        self.cluster = cluster
        self.code_digest = code_digest
        self.inputs = tuple(inputs)
        self.reusable = reusable
        self._environ = environ
        self._decision_arg = decision_env
        self._attempt_arg = attempt
        interval, attribution = PLATFORM_PRESETS.get(platform, (FLYTE_INTERVAL_S, "container"))
        scope_kwargs.setdefault("interval_s", interval)
        scope_kwargs.setdefault("attribution", attribution)
        self._scope_kwargs = scope_kwargs
        self._scope_factory = scope_factory
        self.last_record: Optional[RunRecord] = None
        self.last_result: Optional[MeasurementResult] = None
        self._active: Optional[Tuple[Optional[MeasurementScope], Optional[DecisionEnv]]] = None

    # ---- resolution (guarded) ----

    def _decision(self) -> Optional[DecisionEnv]:
        try:
            if self._decision_arg is _FROM_ENV:
                return read_decision(self._environ)
            if isinstance(self._decision_arg, str):
                return DecisionEnv.from_json(self._decision_arg, allow_unknown=True)
            return self._decision_arg
        except Exception as e:
            log.warning("rightsizer: ignoring decision env: %s", e)
            return None

    def _attempt(self) -> Optional[AttemptContext]:
        try:
            if self._attempt_arg is _FROM_ENV:
                return current_attempt(self._environ)
            return self._attempt_arg
        except Exception as e:
            log.warning("rightsizer: ignoring attempt context: %s", e)
            return None

    def _begin(self) -> Tuple[Optional[MeasurementScope], Optional[DecisionEnv]]:
        try:
            d = self._decision()
            if d is not None and d.mode is Mode.OFF:
                return None, d
            if self.writer is None or (d is None and self.key is None):
                log.debug("rightsizer: no writer or key; running unmeasured")
                return None, d
            scope = self._scope_factory() if self._scope_factory else MeasurementScope(**self._scope_kwargs)
            scope.start()
            return scope, d
        except Exception as e:
            log.warning("rightsizer: measurement not started: %s", e)
            return None, None

    def _end(self, scope: Optional[MeasurementScope], d: Optional[DecisionEnv], exc: Optional[BaseException]) -> None:
        if scope is None:
            return
        try:
            status = status_for_exception(exc)
            result = scope.stop(status)
            if isinstance(exc, MemoryError) and result is not None and result.memory_peak is not None:
                # A Python MemoryError is OOM-like but not an OOM kill: keep
                # status Failed, but the peak is only a lower bound.
                from dataclasses import replace

                result = replace(result, memory_peak=result.memory_peak.with_censor(Censor.AT_LEAST))
            self.last_result = result
            rec = self.build_record(result, status, d)
            self.last_record = rec
            if rec is not None and self.writer is not None:
                self.writer.write(rec, tenant=self.tenant)
        except Exception as e:
            log.warning("rightsizer: RunRecord not written: %s", e)

    def build_record(self, result: Optional[MeasurementResult], status: RunStatus, d: Optional[DecisionEnv]) -> Optional[RunRecord]:
        key = d.key if d is not None else self.key
        if key is None:
            return None
        att = self._attempt()
        if att is not None:
            invocation_id, attempt = att.invocation_id, att.attempt
        else:
            invocation_id, attempt = (d.decision_id if d is not None else new_ulid()), 1
        r = result
        memory_peak = None if (r is None or self.reusable) else r.memory_peak
        return RunRecord(
            key=key,
            cluster=self.cluster,
            invocation_id=invocation_id,
            attempt=attempt,
            decision_id=d.decision_id if d is not None else None,
            started_at=r.started_at if r is not None else utcnow(),
            inputs=self.inputs,
            code_digest=self.code_digest,
            requested=d.requested if d is not None else None,
            status=status,
            status_confidence=Confidence.OBSERVED,
            memory_peak=memory_peak,
            cpu_mean=r.cpu_mean if r else None,
            cpu_peak_window=r.cpu_peak_window if r else None,
            cpu_throttled_frac=r.cpu_throttled_frac if r else None,
            gpu_memory_peak=r.gpu_memory_peak if r else None,
            gpu_util_mean=r.gpu_util_mean if r else None,
            t_peak_frac=(r.t_peak_frac if r and memory_peak is not None else None),
            duration_s=r.duration_s if r else None,
            source=OutcomeSource.cgroup() if (r is not None and r.from_cgroup) else OutcomeSource.self_report(),
            trust=self.trust,
        )

    # ---- entry points ----

    def run(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        scope, d = self._begin()
        exc: Optional[BaseException] = None
        try:
            return fn(*args, **kwargs)
        except BaseException as e:
            exc = e
            raise
        finally:
            self._end(scope, d, exc)

    async def arun(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        scope, d = self._begin()
        exc: Optional[BaseException] = None
        try:
            out = fn(*args, **kwargs)
            if inspect.isawaitable(out):
                out = await out
            return out
        except BaseException as e:
            exc = e
            raise
        finally:
            self._end(scope, d, exc)

    def __call__(self, fn: F) -> F:
        """Decorator form (sync or async)."""
        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def _aw(*a: Any, **k: Any) -> Any:
                return await self.arun(fn, *a, **k)

            return _aw  # type: ignore[return-value]

        @functools.wraps(fn)
        def _w(*a: Any, **k: Any) -> Any:
            return self.run(fn, *a, **k)

        return _w  # type: ignore[return-value]

    def __enter__(self) -> "MeasuredExecution":
        self._active = self._begin()
        return self

    def __exit__(self, et: Any, ev: Any, tb: Any) -> bool:
        scope, d = self._active or (None, None)
        self._active = None
        self._end(scope, d, ev)
        return False

    async def __aenter__(self) -> "MeasuredExecution":
        return self.__enter__()

    async def __aexit__(self, et: Any, ev: Any, tb: Any) -> bool:
        return self.__exit__(et, ev, tb)


def measured_execution(
    decision_env: Any = _FROM_ENV,
    writer: Optional[RunRecordWriter] = None,
    *,
    platform: str = "flyte",
    **kwargs: Any,
) -> MeasuredExecution:
    """Measure a callable and write its RunRecord.

    ``decision_env`` is a :class:`~.env.DecisionEnv`, its JSON, ``None`` (no
    decision; then ``key=`` is required to record), or omitted to read
    ``RIGHTSIZER_DECISION``. ``platform`` picks the presets: ``flyte``/``k8s``/
    ``slurm`` sample every 1 s with container attribution; ``ray`` every 0.5 s
    with process attribution. Use the result as a decorator, a (async)
    context manager, or via ``.run(fn, *a)`` / ``await .arun(fn, *a)``.
    """
    return MeasuredExecution(decision_env, writer, platform=platform, **kwargs)


__all__ = ["MeasuredExecution", "PLATFORM_PRESETS", "measured_execution", "status_for_exception"]
