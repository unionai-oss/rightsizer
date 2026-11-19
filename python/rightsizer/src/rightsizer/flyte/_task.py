"""``RightsizedTask``, ``@rs.flyte.rightsize`` and ``rs.flyte.sized`` (flyte.md §4–5, §7–9).

A ``RightsizedTask`` is a dataclass subclass of the task's own template class,
created once per class and cached (so plugin task classes keep working). It
adds plain, picklable fields only::

    _rs_policy      DefinitionPolicy   what may change (task merged over environment)
    _rs_pins        frozenset[str]     axes pinned by explicit ``override(resources=...)``
    _rs_bypass      bool               set on the sized copy handed to the controller
    _rs_invocation  tuple[InvocationPolicy, ...]   from ``rs.flyte.sized(...)``
    _rs_code        rightsizer.Code    source captured once at decoration
    _rs_measured    bool               whether the child pod runs a RightsizedTask

The decision engine is a per-process singleton (:mod:`._engine`), never on
the task. Every rightsizer failure on the call path keeps the call unchanged
(S1); the task's own result and exceptions pass through.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import os
import time
from dataclasses import field
from typing import Any, Dict, FrozenSet, Optional, Tuple

from rightsizer_runtime.env import ATTEMPT_ENV, DECISION_ENV, DecisionEnv
from rightsizer_runtime.policy import InvocationPolicy, PolicyStack
from rightsizer_runtime.wire import AttemptContext, Confidence, Mode, OutcomeSource, RunStatus, Trust, new_ulid

from ..types import Code, Context, Decision, Input
from . import _engine as _eng
from ._classify import classify
from ._policy import CONTEXT_KEY, captured_stack, definition, invocation, resolve
from ._resources import axes_set, has_memory, memory_is_tuple, merge, pin_axes, requests_of, to_rs, to_spec

log = logging.getLogger("rightsizer.flyte")

PLATFORM = "flyte"
DECISION_BUDGET_MS = 250
OVERHEAD_MIB = ((0.5, 70.0), (0.9, 242.0))
CPU_WINDOW_S = 60
_RS_FIELD_NAMES = ("_rs_policy", "_rs_pins", "_rs_bypass", "_rs_invocation", "_rs_code", "_rs_measured")

_CLASS_CACHE: Dict[type, type] = {}


# ── helpers ──────────────────────────────────────────────────────────────


def _task_context() -> Any:
    from flyte._context import internal_ctx

    ctx = internal_ctx()
    if not ctx.is_task_context() or ctx.is_in_trace():
        return None
    return ctx.data.task_context


def _flyte_version() -> str:
    try:
        import flyte

        return str(flyte.__version__)
    except Exception:
        return ""


def _retries(task: Any) -> int:
    r = getattr(task, "retries", 0)
    return int(getattr(r, "count", r) or 0)


def walltime_s(timeout: Any) -> Optional[int]:
    """``Timeout`` / ``int`` / ``timedelta`` → seconds of ``max_runtime`` (``None`` = unlimited)."""
    import datetime as _dt

    if timeout is None:
        return None
    v = getattr(timeout, "max_runtime", timeout)
    if v is None:
        return None
    if isinstance(v, _dt.timedelta):
        s = int(v.total_seconds())
    elif isinstance(v, (int, float)) and not isinstance(v, bool):
        s = int(v)
    else:
        return None
    return s if s > 0 else None


def _cache_enabled(task: Any) -> bool:
    c = getattr(task, "cache", None)
    b = getattr(c, "behavior", c)
    return bool(b) and str(b).lower() not in ("disable", "disabled", "none", "false")


def _mode(m: Any) -> Mode:
    return Mode.parse(m)


def _tenant(action: Any) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    return getattr(action, "org", None), getattr(action, "project", None), getattr(action, "domain", None)


def task_key(task: Any, org: Optional[str], project: Optional[str], domain: Optional[str], platform: str = PLATFORM) -> str:
    from rightsizer_runtime.flyte_child import flyte_key

    suffix = task._rs_policy.key if getattr(task, "_rs_policy", None) is not None and task._rs_policy.key else task.name
    return flyte_key(org, project, domain, suffix, platform=platform)


_DF_FORMATS = {"parquet": "parquet", "csv": "csv", "jsonl": "jsonl", "arrow": "arrow_ipc"}


def _looks_like_uri(s: str) -> bool:
    return "://" in s[:32] and " " not in s and len(s) < 4096


def inputs_from_call(task: Any, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Tuple[Input, ...]:
    """flyte.md §4.2 inputs: ``File``/``Dir`` → path; ``DataFrame`` → uri + format;
    URI strings → reader; numbers and bools → scalars; anything else is skipped."""
    try:
        bound = task.interface.convert_to_kwargs(*args, **dict(kwargs))
    except Exception:
        bound = dict(kwargs)
    out = []
    for name, v in bound.items():
        try:
            mod = type(v).__module__ or ""
            if mod.startswith("flyte.io"):
                cls = type(v).__name__
                if cls in ("File", "Dir") and getattr(v, "path", None):
                    fmt = "dir" if cls == "Dir" else None
                    out.append(Input.from_uri(str(v.path), name, format=fmt))
                    continue
                if cls == "DataFrame" and getattr(v, "uri", None):
                    fmt = _DF_FORMATS.get(str(getattr(v, "format", "") or "").lower())
                    out.append(Input.from_uri(str(v.uri), name, format=fmt))
                    continue
            if isinstance(v, bool) or isinstance(v, (int, float)):
                out.append(Input.scalar(name, v))
            elif isinstance(v, str) and _looks_like_uri(v):
                out.append(Input.from_uri(v, name))
        except Exception as e:  # an unreadable argument is just not an input
            log.debug("rightsizer: input %s skipped: %s", name, e)
    return tuple(out)


# ── the mixin ────────────────────────────────────────────────────────────


@dataclasses.dataclass
class _Plan:
    ctx: Context
    policy: Dict[str, Any]
    mode: Mode
    escalate: bool
    max_escalations: int
    allow_gpu_type_change: bool
    measured: bool
    child_context: Optional[str]
    clamped: bool


class RightsizedMixin:
    """Methods shared by every ``RightsizedTask`` class (see the module docstring)."""

    # -- override: explicit resources pin their axes --------------------------------

    def override(self, **kw: Any) -> Any:
        new = super().override(**kw)  # type: ignore[misc]
        res = kw.get("resources")
        if res is not None and isinstance(new, RightsizedMixin):
            new._rs_pins = frozenset(self._rs_pins) | axes_set(res)  # type: ignore[attr-defined]
        return new

    # -- pickling: rebuild the cached class from its base --------------------------

    def __reduce_ex__(self, protocol: Any) -> Any:
        return (_rebuild_rightsized, (_base_class(type(self)),), self.__getstate__())  # type: ignore[attr-defined]

    # -- delegation ------------------------------------------------------------------

    def _rs_delegate_reason(self) -> Optional[str]:
        if self._rs_bypass:  # type: ignore[attr-defined]
            return "bypass"
        tctx = _task_context()
        if tctx is None:
            return "no task context"
        if getattr(tctx, "mode", "remote") == "local":
            return "local"
        if getattr(self, "reusable", None) is not None:
            return "reusable"
        return None

    def _rs_stack(self) -> PolicyStack:
        st = captured_stack()
        for p in self._rs_invocation:  # type: ignore[attr-defined]
            st = st.push(p)
        return st

    def aio(self, *args: Any, **kwargs: Any) -> Any:
        """Submit (sized). The invocation stack is captured *now*, so a coroutine
        created inside a ``policy()`` block keeps that block's policy."""
        base_aio = super().aio  # type: ignore[misc]
        try:
            reason = self._rs_delegate_reason()
            if reason is not None:
                return base_aio(*args, **kwargs)
            stack = self._rs_stack()
            inherited = captured_stack()
        except Exception as e:  # S1
            log.warning("rightsizer: %s: delegating unchanged: %s", getattr(self, "name", "?"), e)
            return base_aio(*args, **kwargs)
        return self._rs_arun(stack, inherited, args, kwargs)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        base_call = super().__call__  # type: ignore[misc]
        try:
            reason = self._rs_delegate_reason()
            if reason is not None:
                return base_call(*args, **kwargs)
            stack = self._rs_stack()
            inherited = captured_stack()
        except Exception as e:  # S1
            log.warning("rightsizer: %s: delegating unchanged: %s", getattr(self, "name", "?"), e)
            return base_call(*args, **kwargs)
        if getattr(self, "_call_as_synchronous", False):
            return self._rs_run_sync(stack, inherited, args, kwargs)
        return self._rs_arun(stack, inherited, args, kwargs)

    # -- planning (sync, cheap) ------------------------------------------------------

    def _rs_plan(self, stack: PolicyStack, inherited: PolicyStack, args: Tuple[Any, ...], kwargs: Dict[str, Any],
                 *, tenant: Optional[Tuple[Any, Any, Any]] = None, root: bool = False,
                 inputs: Optional[Tuple[Input, ...]] = None) -> Optional[_Plan]:
        """Policy, context and request keywords for one call.

        ``root``: the run's root action, which has no parent to catch its
        failure: ``PlatformRetry{ladder: None}`` (reductions need S3(b)).
        """
        defn = self._rs_policy.resolved()  # type: ignore[attr-defined]
        eff = resolve(defn, stack)
        if eff.mode is Mode.OFF:
            return None
        tctx = _task_context() if tenant is None else None
        org, project, domain = tenant if tenant is not None else _tenant(tctx.action)
        key = task_key(self, org, project, domain)
        cfg = _eng.flyte_config()
        prior = getattr(self, "resources", None)
        on_oom = defn.on_oom or "auto"
        retries = _retries(self)
        escalate = (not root) and (on_oom == "escalate" or (on_oom == "auto" and retries == 0)) and has_memory(prior)
        max_esc = int(defn.max_escalations if defn.max_escalations is not None else 2)
        escalate = escalate and max_esc > 0
        if memory_is_tuple(prior):
            enforced = True
        elif has_memory(prior):
            enforced = bool(cfg.get("scalar_memory_enforced", True))
        else:
            enforced = False
        retry: Dict[str, Any] = {"same_size": retries}
        if escalate:
            retry["ladder"] = {"memory_factor": 2.0, "max_steps": max_esc, "final_floor": {}, "driver": "caller"}
        platform: Dict[str, Any] = {
            "kind": PLATFORM,
            "version": _flyte_version(),
            "overhead": {"memory_mib": [list(q) for q in OVERHEAD_MIB], "cpu_millis": 0},
            "retry": retry,
            "cpu_window_s": CPU_WINDOW_S,
            "memory_enforced": enforced,
        }
        wt = walltime_s(getattr(self, "timeout", None))
        if wt is not None:
            platform["walltime_s"] = wt
        ctx = Context(
            key=key,
            prior=to_rs(prior),
            inputs=inputs if inputs is not None else inputs_from_call(self, args, kwargs),
            code=self._rs_code,  # type: ignore[attr-defined]
            platform=platform,
        )
        pins = set(pin_axes(defn.get("pins") or ())) | set(pin_axes(self._rs_pins))  # type: ignore[attr-defined]
        shapes = list(cfg.get("shapes") or [])
        ceiling = defn.ceiling
        clamped = False
        if not shapes:
            # S4 without platform state: increases clamped to the ceiling, or held.
            if ceiling is None:
                ceiling = requests_of(prior)
            clamped = True
        pol: Dict[str, Any] = {
            "mode": eff.mode,
            "max_mode": defn.max_mode,
            "risk": eff.risk,
            "estimator": eff.estimator,
            "budget_ms": DECISION_BUDGET_MS,
            "allow_gpu_type_change": bool(defn.allow_gpu_type_change),
        }
        if pins:
            pol["pins"] = sorted(pins)
        if defn.floor is not None:
            pol["floor"] = defn.floor.to_dict()
        if ceiling is not None:
            pol["ceiling"] = ceiling.to_dict()
        if shapes:
            pol["state"] = {"shapes": shapes}
        measured = bool(self._rs_measured)  # type: ignore[attr-defined]
        cb = getattr(tctx, "code_bundle", None) if tctx is not None else None
        if cb is not None and getattr(cb, "pkl", None):
            measured = True  # the child unpickles this very object
        child_context = None if len(inherited) == 0 else inherited.to_json()
        return _Plan(ctx=ctx, policy=pol, mode=eff.mode, escalate=escalate, max_escalations=max_esc,
                     allow_gpu_type_change=bool(defn.allow_gpu_type_change), measured=measured,
                     child_context=child_context, clamped=clamped)

    # -- the sized copy --------------------------------------------------------------

    def _rs_copy(self, d: Decision, plan: _Plan, at: Optional[AttemptContext]) -> Any:
        prior = getattr(self, "resources", None)
        enforce = _mode(d.mode) is Mode.ENFORCE
        resources = merge(prior, d.applied.requests, allow_gpu_type_change=plan.allow_gpu_type_change) if (enforce and prior is not None) else prior
        env = dict(getattr(self, "env_vars", None) or {})
        env[DECISION_ENV] = DecisionEnv(decision_id=d.id, key=d.key, mode=d.mode, requested=to_spec(resources),
                                        measured=plan.measured).to_json()
        if at is not None:
            env[ATTEMPT_ENV] = at.to_json()
        return self._replace(resources=resources, env_vars=env, _rs_bypass=True)  # type: ignore[attr-defined]

    def _rs_reduced(self, d: Decision) -> bool:
        a = d.applied.requests.memory
        p = d.prior.spec.requests.memory
        return _mode(d.mode) is Mode.ENFORCE and a is not None and p is not None and a < p

    # -- decisions -------------------------------------------------------------------

    async def _rs_adecide(self, plan: _Plan, at: Optional[AttemptContext]) -> Decision:
        from .._engine import _fallback

        _eng.counters["decisions"] += 1
        try:
            eng = _eng.get_engine()
        except Exception as e:
            _eng.counters["fallbacks"] += 1
            return _fallback(plan.ctx, e, {"mode": plan.mode})
        try:
            return await asyncio.wait_for(eng.adecide(plan.ctx, attempt=at, **plan.policy), timeout=(DECISION_BUDGET_MS + 750) / 1000)
        except asyncio.TimeoutError:
            _eng.counters["budget_expired"] += 1
            return Decision.prior_for(plan.ctx, "budget", "python-side deadline", plan.mode)
        except Exception as e:  # S1
            _eng.counters["fallbacks"] += 1
            return _fallback(plan.ctx, e, {"mode": plan.mode})

    def _rs_decide(self, plan: _Plan, at: Optional[AttemptContext]) -> Decision:
        from .._engine import _fallback

        _eng.counters["decisions"] += 1
        try:
            eng = _eng.get_engine()
            return eng.decide(plan.ctx, attempt=at, **plan.policy)
        except Exception as e:  # S1
            _eng.counters["fallbacks"] += 1
            return _fallback(plan.ctx, e, {"mode": plan.mode})

    def _rs_record(self, d: Decision, plan: _Plan, copy: Any, status: RunStatus, at: Optional[AttemptContext],
                   inv: str, attempt: int, t0: float, confidence: Confidence = Confidence.OBSERVED) -> Optional[Any]:
        try:
            from .._engine import build_record

            rec = build_record(
                d, status, duration_s=max(0.0, time.monotonic() - t0), invocation_id=inv, attempt=attempt,
                status_confidence=confidence, source=OutcomeSource.self_report(), trust=Trust.UNVERIFIED,
            )
            return dataclasses.replace(rec, requested=to_spec(getattr(copy, "resources", None)))
        except Exception as e:
            log.warning("rightsizer: outcome record not built: %s", e)
            return None

    def _rs_status_ok(self, d: Decision, plan: _Plan) -> Tuple[RunStatus, Confidence]:
        if plan.measured and _cache_enabled(self):
            found = _child_record_exists(d)
            if found is False:
                return RunStatus.CACHED, Confidence.INFERRED
        return RunStatus.SUCCEEDED, Confidence.OBSERVED

    def _rs_should_escalate(self, d: Decision, plan: _Plan, status: RunStatus, attempt: int) -> bool:
        if not plan.escalate or attempt > plan.max_escalations:
            return False
        if status is RunStatus.OOM_KILLED:
            return self._rs_reduced(d) or attempt > 1
        if status is RunStatus.GPU_OOM:
            return plan.allow_gpu_type_change
        return False

    async def _rs_arun(self, stack: PolicyStack, inherited: PolicyStack, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Any:
        base_aio = super().aio  # type: ignore[misc]
        try:
            plan = self._rs_plan(stack, inherited, args, kwargs)
        except Exception as e:  # S1
            log.warning("rightsizer: %s: delegating unchanged: %s", getattr(self, "name", "?"), e)
            plan = None
        if plan is None:
            return await base_aio(*args, **kwargs)
        inv = new_ulid()
        attempt = 1
        while True:
            at = AttemptContext(invocation_id=inv, attempt=attempt, max_attempts=1 + plan.max_escalations) if plan.escalate else None
            d = await self._rs_adecide(plan, at)
            try:
                copy = self._rs_copy(d, plan, at)
            except Exception as e:  # S1: run unchanged
                log.warning("rightsizer: %s: sized copy failed, delegating unchanged: %s", getattr(self, "name", "?"), e)
                return await base_aio(*args, **kwargs)
            t0 = time.monotonic()
            try:
                with _child_custom_context(plan.child_context):
                    out = await copy.aio(*args, **kwargs)
            except (asyncio.CancelledError, KeyboardInterrupt):
                raise
            except BaseException as exc:
                status = classify(exc)
                await _aoutcome(self._rs_record(d, plan, copy, status, at, inv, attempt, t0))
                if self._rs_should_escalate(d, plan, status, attempt):
                    log.info("rightsizer: %s attempt %d %s; escalating", getattr(self, "name", "?"), attempt, status.value)
                    attempt += 1
                    continue
                raise
            status, conf = self._rs_status_ok(d, plan)
            await _aoutcome(self._rs_record(d, plan, copy, status, at, inv, attempt, t0, conf))
            return out

    def _rs_run_sync(self, stack: PolicyStack, inherited: PolicyStack, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Any:
        base_call = super().__call__  # type: ignore[misc]
        try:
            plan = self._rs_plan(stack, inherited, args, kwargs)
        except Exception as e:  # S1
            log.warning("rightsizer: %s: delegating unchanged: %s", getattr(self, "name", "?"), e)
            plan = None
        if plan is None:
            return base_call(*args, **kwargs)
        inv = new_ulid()
        attempt = 1
        while True:
            at = AttemptContext(invocation_id=inv, attempt=attempt, max_attempts=1 + plan.max_escalations) if plan.escalate else None
            d = self._rs_decide(plan, at)
            try:
                copy = self._rs_copy(d, plan, at)
            except Exception as e:
                log.warning("rightsizer: %s: sized copy failed, delegating unchanged: %s", getattr(self, "name", "?"), e)
                return base_call(*args, **kwargs)
            t0 = time.monotonic()
            try:
                with _child_custom_context(plan.child_context):
                    out = copy(*args, **kwargs)
            except KeyboardInterrupt:
                raise
            except BaseException as exc:
                status = classify(exc)
                _outcome(self._rs_record(d, plan, copy, status, at, inv, attempt, t0))
                if self._rs_should_escalate(d, plan, status, attempt):
                    attempt += 1
                    continue
                raise
            status, conf = self._rs_status_ok(d, plan)
            _outcome(self._rs_record(d, plan, copy, status, at, inv, attempt, t0, conf))
            return out

    # -- child side ------------------------------------------------------------------

    async def execute(self, *args: Any, **kwargs: Any) -> Any:
        base_execute = super().execute  # type: ignore[misc]
        try:
            from rightsizer_runtime.flyte_child import measured_execute

            import flyte

            c = flyte.ctx()
            local = bool(c) and getattr(c, "mode", "remote") == "local"
            cfg_observe_local = os.environ.get("RIGHTSIZER__FLYTE__OBSERVE_LOCAL", "").lower() in ("1", "true", "yes")
            org, project, domain = _tenant(getattr(c, "action", None)) if c else (None, None, None)
            key = task_key(self, org, project, domain, platform="flyte-local" if local else PLATFORM)
            inputs, digest = _child_context_facts(self, args, kwargs)
        except Exception as e:  # S1
            log.warning("rightsizer: measurement unavailable: %s", e)
            return await base_execute(*args, **kwargs)
        return await measured_execute(
            base_execute, args, kwargs, key=key, requested=to_spec(getattr(self, "resources", None)),
            reusable=getattr(self, "reusable", None) is not None, local=local, observe_local=cfg_observe_local,
            inputs=inputs, code_digest=digest,
        )


def _child_context_facts(task: Any, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Tuple[Tuple[Any, ...], Optional[str]]:
    """Input metadata and the code digest for the child record, when ``_core`` is in the image."""
    from .._native import available

    if not available():
        return (), (task._rs_code.digest if task._rs_code is not None and task._rs_code.kind == "info" else None)
    import json

    from rightsizer_runtime.wire import InputMeta

    from .. import _native
    from .._engine import _code_info

    digest = None
    try:
        info = _code_info(task._rs_code) if task._rs_code is not None else None
        digest = info.get("digest") if info else None
    except Exception:
        digest = None
    metas: Tuple[Any, ...] = ()
    try:
        ins = inputs_from_call(task, args, kwargs)
        if ins:
            raw = json.loads(_native.core().read_inputs(json.dumps([i.to_dict() for i in ins]), 1000))
            metas = tuple(InputMeta.from_dict(m, allow_unknown=True) for m in raw)
    except Exception as e:
        log.debug("rightsizer: child input metadata: %s", e)
    return metas, digest


class _child_custom_context:
    """Re-enter the captured stack for the child's descendants (a no-op when unchanged)."""

    def __init__(self, payload: Optional[str]):
        self.payload = payload
        self.cm: Any = None

    def __enter__(self) -> None:
        if not self.payload:
            return
        try:
            from ._policy import inherited_json

            if inherited_json() == self.payload:
                return
            import flyte

            self.cm = flyte.custom_context(**{CONTEXT_KEY: self.payload})
            self.cm.__enter__()
        except Exception as e:  # S1
            log.debug("rightsizer: custom context not re-entered: %s", e)
            self.cm = None

    def __exit__(self, et: Any, ev: Any, tb: Any) -> bool:
        if self.cm is not None:
            try:
                self.cm.__exit__(et, ev, tb)
            except Exception:  # pragma: no cover
                pass
        return False


async def _aoutcome(rec: Any) -> None:
    if rec is None:
        return
    try:
        eng = _eng.get_engine()
        await eng.aoutcome(rec)
    except Exception as e:  # S1
        _log_once("outcome", "rightsizer: outcome not recorded: %s", e)


def _outcome(rec: Any) -> None:
    if rec is None:
        return
    try:
        _eng.get_engine().outcome(rec)
    except Exception as e:  # S1
        _log_once("outcome", "rightsizer: outcome not recorded: %s", e)


_LOGGED: set = set()


def _log_once(tag: str, msg: str, *a: Any) -> None:
    if tag in _LOGGED:
        log.debug(msg, *a)
    else:
        _LOGGED.add(tag)
        log.warning(msg, *a)


def _child_record_exists(d: Decision) -> Optional[bool]:
    """Whether the child wrote a record for ``d`` (``None``: unknown). Lists the
    key's newest shards; only used for cache-enabled tasks (flyte.md §8.2)."""
    try:
        import json

        from rightsizer_runtime.record import key_hash, split_key, tenant_path
        from rightsizer_runtime.wire import ulid_timestamp_ms

        store = _eng._state.get("store")
        if not store or "://" not in store:
            return None
        import flyte.storage as fs_mod

        platform, tenant, _ = split_key(d.key)
        prefix = f"{store.rstrip('/')}/v1/runs/{tenant_path(platform, tenant)}/{key_hash(d.key)}"
        fs = fs_mod.get_underlying_filesystem(path=store)
        t_min = ulid_timestamp_ms(d.id) - 1000
        names = [p for p in fs.ls(prefix, detail=False) if p.endswith(".json")]
        for p in sorted(names, reverse=True)[:64]:
            stem = p.rsplit("/", 1)[-1][:-5]
            try:
                if ulid_timestamp_ms(stem) < t_min:
                    break
            except Exception:
                continue
            rec = json.loads(fs.cat_file(p))
            if rec.get("decision_id") == d.id and (rec.get("source") or {}).get("kind") != "self_report":
                return True
        return False
    except Exception as e:
        log.debug("rightsizer: child record lookup: %s", e)
        return None


# ── classes ──────────────────────────────────────────────────────────────


def _base_class(cls: type) -> type:
    for b in cls.__mro__[1:]:
        if b is not RightsizedMixin and not issubclass(b, RightsizedMixin):
            return b
    raise TypeError(f"{cls!r} is not a RightsizedTask class")


def rightsized_class(base: type) -> type:
    """The cached ``RightsizedTask`` dataclass subclass of ``base``."""
    if issubclass(base, RightsizedMixin):
        return base
    cls = _CLASS_CACHE.get(base)
    if cls is not None:
        return cls
    from ._policy import DefinitionPolicy

    ns: Dict[str, Any] = {
        "__module__": __name__,
        "__qualname__": f"RightsizedTask[{base.__qualname__}]",
        "__annotations__": {
            "_rs_policy": Any,
            "_rs_pins": FrozenSet[str],
            "_rs_bypass": bool,
            "_rs_invocation": Tuple[InvocationPolicy, ...],
            "_rs_code": Optional[Code],
            "_rs_measured": bool,
        },
        "_rs_policy": field(default_factory=DefinitionPolicy),
        "_rs_pins": field(default=frozenset()),
        "_rs_bypass": field(default=False),
        "_rs_invocation": field(default=()),
        "_rs_code": field(default=None, compare=False),
        "_rs_measured": field(default=True),
    }
    cls = type(f"RightsizedTask[{base.__name__}]", (RightsizedMixin, base), ns)
    cls = dataclasses.dataclass(kw_only=True, repr=False)(cls)
    _CLASS_CACHE[base] = cls
    return cls


def _rebuild_rightsized(base: type) -> Any:
    cls = rightsized_class(base)
    return cls.__new__(cls)


def _init_fields(task: Any) -> Dict[str, Any]:
    return {f.name: getattr(task, f.name) for f in dataclasses.fields(task) if f.init and f.name not in _RS_FIELD_NAMES}


def _code_for(task: Any) -> Optional[Code]:
    fn = getattr(task, "func", None)
    if fn is None:
        return None
    try:
        return Code.from_callable(fn)
    except Exception:
        return None


def _as_rightsized(task: Any, policy: Any, *, measured: bool) -> Any:
    from flyte._task import TaskTemplate

    if not isinstance(task, TaskTemplate):
        raise TypeError(
            f"rs.flyte.rightsize must be the outermost decorator, over a Flyte task (@env.task); got {type(task).__name__}"
        )
    cls = rightsized_class(type(task))
    try:
        new = cls(**_init_fields(task))
    except TypeError:
        # A plugin class whose __init__ does not take its dataclass fields: rebuild the SDK's way.
        from flyte._task import _rebuild_as

        new = _rebuild_as(task, cls)
    new._rs_policy = policy
    new._rs_code = _code_for(task)
    new._rs_measured = measured
    return new


def _env_policy(task: Any) -> Any:
    env = None
    try:
        env = task.parent_env() if task.parent_env is not None else None
    except Exception:
        env = None
    pol = getattr(env, "_rs_definition", None)
    return pol


def _replace_in_registry(old: Any, new: Any) -> None:
    """Private API (flyte.md §11): ``TaskEnvironment._tasks`` holds the registered task."""
    try:
        env = old.parent_env() if old.parent_env is not None else None
    except Exception:
        env = None
    if env is None:
        return
    reg = getattr(env, "_tasks", None)
    if isinstance(reg, dict) and reg.get(old.name) is old:
        reg[old.name] = new


def rightsize(_task: Any = None, /, *args: Any, **kw: Any) -> Any:
    """Make a Flyte task rightsizable everywhere it is called (flyte.md §4).

    ``@rs.flyte.rightsize`` / ``@rs.flyte.rightsize("enforce")`` /
    ``@rs.flyte.rightsize(pins={"gpu"}, key="llama-ft", on_oom="escalate")``,
    outermost (over ``@env.task``). Returns a ``RightsizedTask``; the
    environment's registry gets the same object.
    """
    from flyte._task import TaskTemplate

    from collections.abc import Mapping

    from rightsizer_runtime.policy import DefinitionPolicy as _RtDefinition

    spec: Any = None
    if _task is not None and not isinstance(_task, TaskTemplate):
        if not isinstance(_task, (str, Mode, _RtDefinition, Mapping)):
            raise TypeError(
                "rs.flyte.rightsize must be the outermost decorator, over a Flyte task (@env.task); "
                f"got {type(_task).__name__}"
            )
        spec, _task = _task, None
    if args:
        raise TypeError("rightsize takes one positional spec (a mode string or DefinitionPolicy) and keywords")

    def deco(task: Any) -> Any:
        mine = definition(spec, **kw)
        if isinstance(task, RightsizedMixin):
            new = task._replace(_rs_policy=mine.merged_over(task._rs_policy))  # type: ignore[attr-defined]
            new._rs_measured = True
        else:
            env_pol = _env_policy(task)
            new = _as_rightsized(task, mine.merged_over(env_pol), measured=True)
        _replace_in_registry(task, new)
        return new

    if _task is not None:
        return deco(_task)
    return deco


def sized(task: Any, spec: Any = None, *, mode: Any = None, rightsize: Any = None, risk: Any = None,
          estimator: Optional[str] = None) -> Any:
    """An invocation policy for one call (or one ``flyte.map``) of any task (flyte.md §5).

    An undecorated task gets its environment's definition policy (or the
    default one); its child runs the plain template, so the decision carries
    ``measured = false``.
    """
    p = invocation(spec, mode=mode, rightsize=rightsize, risk=risk, estimator=estimator)
    if isinstance(task, RightsizedMixin):
        return task._replace(_rs_invocation=(*task._rs_invocation, p))  # type: ignore[attr-defined]
    env_pol = _env_policy(task)
    new = _as_rightsized(task, definition(None).merged_over(env_pol), measured=False)
    new._rs_invocation = (p,)
    return new


def is_rightsized(task: Any) -> bool:
    return isinstance(task, RightsizedMixin)


__all__ = ["RightsizedMixin", "is_rightsized", "rightsize", "rightsized_class", "sized", "task_key", "walltime_s"]
