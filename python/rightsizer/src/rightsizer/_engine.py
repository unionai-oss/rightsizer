"""Engines and the module-level decision API (SPEC §10.3).

``rs.recommend`` / ``rs.arecommend`` / ``rs.decide_group`` never raise
(S1): any failure, including a missing native module, a configuration
error or a bug, is logged and the decision keeps the prior.
``rs.outcome`` logs and returns ``None`` on failure. The methods of an
explicit :class:`Engine` raise :class:`RightsizerError` where the contract
allows (construction, outcomes, store operations); its decisions never do.
"""

from __future__ import annotations

import datetime as _dt
import functools
import json
import logging
import os
import threading
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Union

from rightsizer_runtime.env import current_attempt
from rightsizer_runtime.policy import current_stack, risk_to_wire
from rightsizer_runtime.wire import (
    AttemptContext,
    Censor,
    Confidence,
    InputMeta,
    Measurement,
    Mode,
    OutcomeSource,
    RunRecord,
    RunStatus,
    Trust,
    utcnow,
)

from . import _units
from ._errors import CONTROL_FLOW, RightsizerError
from ._native import core
from ._storage import apply_storage_options
from .types import AXES, Code, Context, Decision, OutcomeReport, Resources, _axes

log = logging.getLogger("rightsizer")

ConfigArg = Union[None, str, "os.PathLike[str]", Mapping[str, Any]]


def _config_text(config: ConfigArg) -> Optional[str]:
    if config is None:
        return None
    if isinstance(config, Mapping):
        return json.dumps(config)
    return os.fspath(config) if isinstance(config, os.PathLike) else str(config)


def _requests(r: Union[Resources, Mapping[str, Any], None]) -> Dict[str, Any]:
    if r is None:
        return {}
    if isinstance(r, Resources):
        return r.requests.to_dict()
    return dict(r)


# ── request building ─────────────────────────────────────────────────────


def _policy(mode: Any, max_mode: Any, risk: Any, estimator: Any, exploration: Any, allow_gpu_type_change: Any,
            enforce_without_prior: Any, can_classify: Any) -> Dict[str, Any]:
    st = current_stack()
    mode = mode if mode is not None else st.mode
    risk = risk if risk is not None else st.risk
    estimator = estimator if estimator is not None else st.estimator
    p: Dict[str, Any] = {}
    if mode is not None:
        p["mode"] = Mode.parse(mode, "mode").value
    if max_mode is not None:
        p["max_mode"] = Mode.parse(max_mode, "max_mode").value
    if risk is not None:
        p["risk"] = risk_to_wire(risk)
    if estimator is not None:
        p["chain"] = [estimator] if isinstance(estimator, str) else list(estimator)
    if exploration is not None:
        p["exploration"] = float(exploration)
    if allow_gpu_type_change is not None:
        p["allow_gpu_type_change"] = bool(allow_gpu_type_change)
    if enforce_without_prior is not None:
        p["enforce_without_prior"] = bool(enforce_without_prior)
    if can_classify is not None:
        p["can_classify"] = bool(can_classify)
    return p


def _constraints(constraints: Optional[Mapping[str, Any]], pins: Optional[Iterable[str]], floor: Any, ceiling: Any,
                 allowed_shapes: Optional[Sequence[Any]]) -> Dict[str, Any]:
    c: Dict[str, Any] = dict(constraints or {})
    if pins:
        c["pinned"] = sorted(set(_axes(c.get("pinned") or ())) | set(_axes(list(pins))), key=AXES.index)
    if floor is not None:
        c["floor"] = _requests(floor)
    if ceiling is not None:
        c["ceiling"] = _requests(ceiling)
    if allowed_shapes is not None:
        c["allowed_shapes"] = [_units.gpu_shape(g).to_dict() for g in allowed_shapes]
    return c


@functools.lru_cache(maxsize=1024)
def _code_info(code: Code) -> Optional[Dict[str, Any]]:
    c = core()
    if code.kind == "source":
        return json.loads(c.code_info(code.text or "", code.language, False))
    if code.kind == "path":
        try:
            with open(code.path or "", encoding="utf-8", errors="replace") as f:
                text = f.read(4 << 20)
        except OSError:
            return None
        lang = "python" if (code.path or "").endswith(".py") else None
        return json.loads(c.code_info(text, lang, False))
    return {"digest": code.digest or "", "tokens": list(code.tokens)}


def _input_metas(ctx: Context, budget_ms: int) -> List[Dict[str, Any]]:
    if not ctx.inputs:
        return []
    return json.loads(core().read_inputs(json.dumps([i.to_dict() for i in ctx.inputs]), int(budget_ms)))


def _history(ctx: Context) -> List[Dict[str, Any]]:
    out = []
    for r in ctx.history or ():
        out.append(r.to_dict() if isinstance(r, RunRecord) else dict(r))
    return out


def _context_input(ctx: Context, budget_ms: Optional[int]) -> Dict[str, Any]:
    if ctx.history is None:
        return ctx.context_input()
    # A complete context: inputs read and code digested here, history as given.
    full: Dict[str, Any] = {
        "v": 1,
        "key": ctx.workload_key,
        "unit": ctx.unit,
        "platform": ctx.platform_ref(),
        "inputs": _input_metas(ctx, budget_ms if budget_ms is not None else 250),
        "manifest": dict(ctx.manifest),
        "prior": ctx.origin_prior.to_dict(),
        "history": _history(ctx),
        "labels": dict(ctx.labels),
    }
    if ctx.code is not None:
        info = _code_info(ctx.code)
        if info is not None:
            full["code"] = info
    return {"kind": "full", "context": full}


def build_request(
    ctx: Context,
    *,
    mode: Any = None,
    max_mode: Any = None,
    risk: Any = None,
    estimator: Any = None,
    budget_ms: Optional[int] = None,
    dry_run: bool = False,
    pins: Optional[Iterable[str]] = None,
    floor: Any = None,
    ceiling: Any = None,
    allowed_shapes: Optional[Sequence[Any]] = None,
    constraints: Optional[Mapping[str, Any]] = None,
    state: Optional[Mapping[str, Any]] = None,
    exploration: Optional[float] = None,
    allow_gpu_type_change: Optional[bool] = None,
    enforce_without_prior: Optional[bool] = None,
    can_classify: Optional[bool] = None,
) -> Dict[str, Any]:
    """The engine's ``DecideRequest`` JSON for a context and policy keywords."""
    if not isinstance(ctx, Context):
        raise TypeError(f"expected rs.Context, not {type(ctx).__name__}")
    req: Dict[str, Any] = {
        "context": _context_input(ctx, budget_ms),
        "constraints": _constraints(constraints, pins, floor, ceiling, allowed_shapes),
        "state": dict(state or {}),
        "policy": _policy(mode, max_mode, risk, estimator, exploration, allow_gpu_type_change, enforce_without_prior, can_classify),
        "dry_run": bool(dry_run),
    }
    if budget_ms is not None:
        req["budget_ms"] = int(budget_ms)
    return req


# ── outcomes ─────────────────────────────────────────────────────────────


def _bytes_measure(v: Any, censor: Any, resolution_s: Optional[float]) -> Optional[Measurement]:
    if v is None:
        return None
    if isinstance(v, Measurement):
        return v
    b = float(_units.memory_bytes(v)) if isinstance(v, str) else float(v)
    return Measurement(value=b, censor=Censor.parse(censor or Censor.EXACT), resolution_s=resolution_s)


def _cores_measure(v: Any) -> Optional[Measurement]:
    if v is None:
        return None
    if isinstance(v, Measurement):
        return v
    cores = _units.cpu_millis(v) / 1000.0 if isinstance(v, str) else float(v)
    return Measurement(value=cores)


def build_record(
    decision: Decision,
    status: Union[str, RunStatus] = "succeeded",
    *,
    memory_peak: Any = None,
    memory_censor: Any = None,
    memory_resolution_s: Optional[float] = None,
    cpu_mean: Any = None,
    cpu_peak_window: Any = None,
    cpu_throttled_frac: Optional[float] = None,
    gpu_memory_peak: Any = None,
    gpu_util_mean: Optional[float] = None,
    t_peak_frac: Optional[float] = None,
    duration_s: Optional[float] = None,
    started_at: Optional[_dt.datetime] = None,
    invocation_id: Optional[str] = None,
    attempt: Optional[int] = None,
    status_confidence: Union[str, Confidence] = Confidence.OBSERVED,
    source: Optional[OutcomeSource] = None,
    trust: Union[str, Trust] = Trust.UNVERIFIED,
    cluster: Optional[str] = None,
    inputs: Optional[Sequence[Union[InputMeta, Mapping[str, Any]]]] = None,
    code_digest: Optional[str] = None,
) -> RunRecord:
    """A ``RunRecord`` for a decision (``self_report``, ``unverified`` by
    default). Inputs and the code digest come from the decision's context
    when not given (rules-sized anchors on input sizes, SPEC §6.2)."""
    att = current_attempt()
    ctx = decision.context
    if inputs is None and ctx is not None:
        try:
            inputs = [InputMeta.from_dict(m, allow_unknown=True) for m in _input_metas(ctx, 1000)]
        except Exception as e:  # S1: an outcome without input sizes is still an outcome
            log.debug("rightsizer: input metadata for the outcome: %s", e)
            inputs = []
    if code_digest is None and ctx is not None and ctx.code is not None:
        try:
            info = _code_info(ctx.code)
            code_digest = info.get("digest") if info else None
        except Exception:
            code_digest = None
    if started_at is None:
        started_at = utcnow() - _dt.timedelta(seconds=duration_s or 0.0)
    return RunRecord(
        key=decision.key,
        cluster=cluster,
        invocation_id=invocation_id or (att.invocation_id if att else decision.id),
        attempt=attempt if attempt is not None else (att.attempt if att else 1),
        decision_id=decision.id,
        started_at=started_at,
        inputs=tuple(i if isinstance(i, InputMeta) else InputMeta.from_dict(i, allow_unknown=True) for i in (inputs or ())),
        code_digest=code_digest,
        requested=decision.applied,
        origin_prior=decision.prior.spec.requests,
        status=RunStatus.parse(status, "status"),
        status_confidence=Confidence.parse(status_confidence, "status_confidence"),
        memory_peak=_bytes_measure(memory_peak, memory_censor, memory_resolution_s),
        cpu_mean=_cores_measure(cpu_mean),
        cpu_peak_window=_cores_measure(cpu_peak_window),
        cpu_throttled_frac=cpu_throttled_frac,
        gpu_memory_peak=_bytes_measure(gpu_memory_peak, None, None),
        gpu_util_mean=gpu_util_mean,
        t_peak_frac=t_peak_frac,
        duration_s=duration_s,
        source=source or OutcomeSource.self_report(),
        trust=Trust.parse(trust, "trust"),
    )


# ── engines ──────────────────────────────────────────────────────────────


class Engine:
    """A decision engine (``_core.Engine``) with the typed API.

    ``config``: ``None`` (resolved like ``rsz``: ``RIGHTSIZER_CONFIG``,
    ``./rightsizer.toml``, XDG, ``/etc``, then ``RIGHTSIZER__*``), a path,
    a TOML document, or a dict. ``store`` overrides ``history.uri``;
    ``storage_options`` (fsspec-style) are exported for Rust's store
    clients (:mod:`rightsizer._storage`). Raises :class:`RightsizerError`
    on a configuration error under ``strict`` or an unreadable config.
    """

    def __init__(self, config: ConfigArg = None, *, store: Optional[str] = None, role: Optional[str] = None,
                 env_overrides: bool = True, storage_options: Optional[Mapping[str, Any]] = None):
        if storage_options:
            apply_storage_options(store, storage_options)
        try:
            c = core()
        except ImportError as e:
            raise RightsizerError(str(e)) from e
        self._core = c.Engine(_config_text(config), store=store, role=role, env_overrides=env_overrides)

    @property
    def native(self) -> Any:
        return self._core

    @property
    def config(self) -> Dict[str, Any]:
        return json.loads(self._core.config_json())

    @property
    def config_error(self) -> Optional[str]:
        return self._core.config_error()

    def chain(self) -> List[str]:
        return list(self._core.chain())

    # decisions (never raise)

    def _attempt(self, attempt: Any) -> Optional[AttemptContext]:
        if attempt is None:
            return current_attempt()
        if isinstance(attempt, AttemptContext):
            return attempt
        if isinstance(attempt, Mapping):
            return AttemptContext.from_dict(attempt, allow_unknown=True)
        raise TypeError("attempt: expected rs.AttemptContext or a dict")

    def decide(self, ctx: Context, *, attempt: Any = None, **policy: Any) -> Decision:
        try:
            req = json.dumps(build_request(ctx, **policy))
            att = self._attempt(attempt)
            out = self._core.attempt_decide(req, att.to_json()) if att is not None else self._core.decide(req)
            return Decision.from_dict(json.loads(out), context=ctx)
        except BaseException as e:  # S1 (a pyo3 PanicException is a BaseException)
            if isinstance(e, CONTROL_FLOW):
                raise
            return _fallback(ctx, e, policy)

    async def adecide(self, ctx: Context, *, attempt: Any = None, **policy: Any) -> Decision:
        try:
            req = json.dumps(build_request(ctx, **policy))
            att = self._attempt(attempt)
            if att is not None:
                # Attempts are rare (retries); the sync path runs off the loop.
                import asyncio

                out = await asyncio.get_running_loop().run_in_executor(None, self._core.attempt_decide, req, att.to_json())
            else:
                out = await self._core.adecide(req)
            return Decision.from_dict(json.loads(out), context=ctx)
        except BaseException as e:  # S1 (a pyo3 PanicException is a BaseException)
            if isinstance(e, CONTROL_FLOW):
                raise
            return _fallback(ctx, e, policy)

    def decide_group(self, ctxs: Sequence[Context], **policy: Any) -> List[Decision]:
        ctxs = list(ctxs)
        try:
            out = self._core.decide_group(json.dumps(_group_requests(ctxs, policy)))
            return [Decision.from_dict(d, context=c) for d, c in zip(json.loads(out), ctxs)]
        except BaseException as e:  # S1 (a pyo3 PanicException is a BaseException)
            if isinstance(e, CONTROL_FLOW):
                raise
            return [_fallback(c, e, policy) for c in ctxs]

    async def adecide_group(self, ctxs: Sequence[Context], **policy: Any) -> List[Decision]:
        ctxs = list(ctxs)
        try:
            out = await self._core.adecide_group(json.dumps(_group_requests(ctxs, policy)))
            return [Decision.from_dict(d, context=c) for d, c in zip(json.loads(out), ctxs)]
        except BaseException as e:  # S1 (a pyo3 PanicException is a BaseException)
            if isinstance(e, CONTROL_FLOW):
                raise
            return [_fallback(c, e, policy) for c in ctxs]

    # everything else (raises RightsizerError)

    def outcome(self, decision_or_record: Union[Decision, RunRecord, Mapping[str, Any]], status: Any = "succeeded",
                **measurements: Any) -> OutcomeReport:
        rec = _record(decision_or_record, status, measurements)
        return OutcomeReport.from_dict(json.loads(self._core.outcome(rec.to_json())))

    async def aoutcome(self, decision_or_record: Union[Decision, RunRecord, Mapping[str, Any]], status: Any = "succeeded",
                       **measurements: Any) -> OutcomeReport:
        rec = _record(decision_or_record, status, measurements)
        return OutcomeReport.from_dict(json.loads(await self._core.aoutcome(rec.to_json())))

    def flush(self) -> int:
        return int(self._core.flush())

    def breaker_status(self) -> List[Dict[str, Any]]:
        return json.loads(self._core.breaker_status())

    def control(self) -> Optional[Dict[str, Any]]:
        c = self._core.control_get()
        return None if c is None else json.loads(c)

    def replay(self, records: Iterable[Union[RunRecord, Mapping[str, Any]]], **options: Any) -> Dict[str, Any]:
        recs = [r.to_dict() if isinstance(r, RunRecord) else dict(r) for r in records]
        if "since" in options and isinstance(options["since"], _dt.datetime):
            from rightsizer_runtime.wire import format_timestamp

            options["since"] = format_timestamp(options["since"])
        return json.loads(self._core.replay(json.dumps(recs), json.dumps(options) if options else None))

    def metrics(self) -> str:
        return self._core.metrics()

    def __repr__(self) -> str:
        return f"rightsizer.Engine({self._core!r})"


def _record(d: Any, status: Any, measurements: Mapping[str, Any]) -> RunRecord:
    if isinstance(d, RunRecord):
        return d
    if isinstance(d, Decision):
        return build_record(d, status, **measurements)
    if isinstance(d, Mapping):
        return RunRecord.from_dict(d, allow_unknown=True)
    raise TypeError("outcome: expected rs.Decision, rs.RunRecord or a RunRecord dict")


def _group_requests(ctxs: Sequence[Context], policy: Mapping[str, Any]) -> List[Dict[str, Any]]:
    from rightsizer_runtime.wire import new_ulid

    gid = next((c.group for c in ctxs if c.group), None) or f"group-{new_ulid()}"
    out = []
    for c in ctxs:
        if c.group is None:
            c.group = gid
        out.append(build_request(c, **policy))
    return out


def _fallback(ctx: Context, e: BaseException, policy: Mapping[str, Any]) -> Decision:
    if isinstance(e, ImportError):
        reason, detail = "config_error", str(e)
    elif isinstance(e, RightsizerError):
        reason, detail = "config_error", str(e)
    else:
        reason, detail = "internal", f"{type(e).__name__}: {e}"
        log.error("rightsizer: decision failed, keeping the prior (S1): %s", detail, exc_info=log.isEnabledFor(logging.DEBUG))
    try:
        mode = Mode.parse(policy.get("mode") or Mode.RECOMMEND)
    except Exception:
        mode = Mode.RECOMMEND
    try:
        return Decision.prior_for(ctx, reason, detail, mode)
    except Exception:  # an unusable context: still a decision
        from .types import Prior, Source
        from rightsizer_runtime.wire import ResourceSpec, new_ulid

        return Decision(id=new_ulid(), key="python://unknown/invalid-request", unit="main", mode=Mode.RECOMMEND,
                        recommended=ResourceSpec(), applied=ResourceSpec(), prior=Prior(),
                        source=Source(kind="prior", reason="internal", detail=detail), explanation=(detail,))


# ── the default engine ───────────────────────────────────────────────────

_lock = threading.Lock()
_settings: Dict[str, Any] = {"config": None, "store": None, "role": None, "env_overrides": True, "storage_options": None}
_default: Optional[Engine] = None
_default_error: Optional[BaseException] = None


def configure(config: ConfigArg = None, *, store: Optional[str] = None, role: Optional[str] = None,
              env_overrides: bool = True, storage_options: Optional[Mapping[str, Any]] = None) -> None:
    """Set what the default engine (``rs.recommend`` and friends) is built
    from; the next decision builds it. Nothing is imported here."""
    global _default, _default_error
    with _lock:
        _settings.update(config=config, store=store, role=role, env_overrides=env_overrides, storage_options=storage_options)
        _default, _default_error = None, None


def default_engine() -> Engine:
    """The process's default engine, built on first use (raises on failure;
    the module-level API turns that into prior decisions)."""
    global _default, _default_error
    e = _default
    if e is not None:
        return e
    with _lock:
        if _default is not None:
            return _default
        if _default_error is not None:
            raise _default_error
        try:
            _default = Engine(**_settings)
        except BaseException as ex:
            _default_error = ex if isinstance(ex, Exception) else RightsizerError(str(ex))
            log.error("rightsizer: cannot build the engine (%s); decisions keep the prior (S1)", ex)
            raise _default_error
        return _default


def recommend(ctx: Context, *, engine: Optional[Engine] = None, **policy: Any) -> Decision:
    """Decide a unit (sync; releases the GIL while Rust works). Never raises.

    Policy keywords: ``mode``, ``max_mode``, ``risk``, ``estimator``,
    ``budget_ms``, ``dry_run``, ``pins``, ``floor``, ``ceiling``,
    ``allowed_shapes``, ``constraints``, ``state``, ``exploration``,
    ``allow_gpu_type_change``, ``enforce_without_prior``, ``can_classify``,
    ``attempt``. Unset ``mode`` / ``risk`` / ``estimator`` come from the
    innermost ``rs.policy(...)`` block, then the configuration.
    """
    try:
        e = engine or default_engine()
    except Exception as ex:
        return _fallback(ctx, ex, policy)
    return e.decide(ctx, **policy)


async def arecommend(ctx: Context, *, engine: Optional[Engine] = None, **policy: Any) -> Decision:
    """``recommend`` as an awaitable (runs on the caller's event loop). Never raises."""
    try:
        e = engine or default_engine()
    except Exception as ex:
        return _fallback(ctx, ex, policy)
    return await e.adecide(ctx, **policy)


def decide_group(ctxs: Sequence[Context], *, engine: Optional[Engine] = None, **policy: Any) -> List[Decision]:
    """One decision for a group of units sized identically (SPEC §4). Never raises."""
    try:
        e = engine or default_engine()
    except Exception as ex:
        return [_fallback(c, ex, policy) for c in ctxs]
    return e.decide_group(ctxs, **policy)


def outcome(decision: Union[Decision, RunRecord, Mapping[str, Any]], status: Any = "succeeded", *,
            engine: Optional[Engine] = None, **measurements: Any) -> Optional[OutcomeReport]:
    """Report how a run went: ``rs.outcome(d, status="succeeded", memory_peak="41Gi")``.

    Measurements: ``memory_peak`` (bytes or ``"41Gi"``), ``memory_censor``
    (``exact`` / ``at_least`` / ``at_most``), ``cpu_mean`` and
    ``cpu_peak_window`` (cores), ``gpu_memory_peak``, ``gpu_util_mean``,
    ``duration_s``, ``t_peak_frac``, ``cpu_throttled_frac``; plus
    ``invocation_id``, ``attempt``, ``started_at``. Returns ``None`` (and
    logs) on failure.
    """
    try:
        e = engine or default_engine()
        return e.outcome(decision, status, **measurements)
    except BaseException as ex:  # S1
        if isinstance(ex, CONTROL_FLOW):
            raise
        log.warning("rightsizer: outcome not recorded: %s", ex)
        return None


async def aoutcome(decision: Union[Decision, RunRecord, Mapping[str, Any]], status: Any = "succeeded", *,
                   engine: Optional[Engine] = None, **measurements: Any) -> Optional[OutcomeReport]:
    try:
        e = engine or default_engine()
        return await e.aoutcome(decision, status, **measurements)
    except BaseException as ex:  # S1
        if isinstance(ex, CONTROL_FLOW):
            raise
        log.warning("rightsizer: outcome not recorded: %s", ex)
        return None


def flush() -> int:
    """Flush the default engine's buffered writes (0 when there is none)."""
    e = _default
    if e is None:
        return 0
    try:
        return e.flush()
    except Exception as ex:
        log.warning("rightsizer: flush failed: %s", ex)
        return 0


__all__ = [
    "Engine", "aoutcome", "arecommend", "build_record", "build_request", "configure", "decide_group",
    "default_engine", "flush", "outcome", "recommend",
]
