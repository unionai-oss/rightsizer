"""The Flyte policy model on top of ``rightsizer_runtime.policy`` (flyte.md §2–3).

- :class:`DefinitionPolicy` — the runtime's, with ``ceiling`` / ``floor``
  accepted as ``flyte.Resources``.
- :func:`definition` — a definition-scope spec (``DefinitionPolicy``, a mode
  string, a dict or ``None``) as a policy: a string ``s`` sets
  ``default_mode = s`` and also ``max_mode = "off"`` when ``s == "off"``.
- :func:`invocation` — an invocation-scope spec: a string ``s`` means
  ``mode = s``; ``rightsize=`` is an alias of ``mode=``.
- :func:`captured_stack` — the invocation stack visible here: the stack
  inherited through ``custom_context["rightsizer"]`` (the run's policy and
  enclosing ``policy()`` blocks of ancestors) plus local pushes not yet
  mirrored into it.
- :class:`policy` — a sync **and** async context manager that pushes onto the
  contextvar stack and enters ``flyte.custom_context(rightsizer=<stack json>)``
  so descendants inherit it.
"""

from __future__ import annotations

import contextvars
import logging
from dataclasses import dataclass, fields
from typing import Any, Mapping, Optional, Union

import importlib as _importlib

_rt = _importlib.import_module("rightsizer_runtime.policy")
from rightsizer_runtime.policy import InvocationPolicy, PolicyStack
from rightsizer_runtime.wire import Mode, Requests, WireError

from ._resources import requests_of

log = logging.getLogger("rightsizer.flyte")

CONTEXT_KEY = "rightsizer"

# How many entries of the local contextvar stack are already mirrored into the
# task context's custom_context (by an enclosing `policy()` block).
_MIRRORED: contextvars.ContextVar[int] = contextvars.ContextVar("rightsizer_flyte_mirrored", default=0)


@dataclass(frozen=True)
class DefinitionPolicy(_rt.DefinitionPolicy):
    """flyte.md §3.1. ``ceiling`` / ``floor`` take ``flyte.Resources`` (or
    ``rightsizer_runtime.wire.Requests``); fields left ``None`` inherit
    (a task's decorator policy is merged over its environment's)."""

    def __post_init__(self) -> None:
        for k in ("ceiling", "floor"):
            v = getattr(self, k)
            if v is not None and not isinstance(v, Requests):
                object.__setattr__(self, k, requests_of(v))
        super().__post_init__()


def definition(spec: Any = None, **kw: Any) -> _rt.DefinitionPolicy:
    """A definition-scope spec as a :class:`DefinitionPolicy`."""
    if spec is None:
        return DefinitionPolicy(**kw) if kw else DefinitionPolicy()
    if isinstance(spec, _rt.DefinitionPolicy):
        if not kw:
            return spec
        base = {f.name: getattr(spec, f.name) for f in fields(spec)}
        base.update(kw)
        return DefinitionPolicy(**base)
    if isinstance(spec, (str, Mode)):
        m = Mode.parse(spec, "rightsize")
        d = {"default_mode": m}
        if m is Mode.OFF:
            d["max_mode"] = Mode.OFF
        d.update(kw)
        return DefinitionPolicy(**d)
    if isinstance(spec, Mapping):
        return DefinitionPolicy(**{**dict(spec), **kw})
    raise TypeError(f"rightsize: expected a DefinitionPolicy, a mode string or a dict, not {type(spec).__name__}")


def invocation(spec: Any = None, *, mode: Any = None, rightsize: Any = None, risk: Any = None,
               estimator: Optional[str] = None) -> InvocationPolicy:
    """An invocation-scope spec as an :class:`InvocationPolicy` (``rightsize=`` aliases ``mode=``)."""
    if isinstance(spec, InvocationPolicy):
        return spec
    if spec is not None:
        if mode is not None or rightsize is not None:
            raise TypeError("pass the mode once")
        mode = spec
    if rightsize is not None:
        if mode is not None:
            raise TypeError("mode= and rightsize= are aliases; pass one")
        mode = rightsize
    return InvocationPolicy(mode=mode, risk=risk, estimator=estimator)


def _task_context() -> Any:
    try:
        from flyte._context import internal_ctx

        return internal_ctx().data.task_context
    except Exception:
        return None


def inherited_json() -> Optional[str]:
    tctx = _task_context()
    if tctx is None:
        return None
    return (tctx.custom_context or {}).get(CONTEXT_KEY)


def captured_stack() -> PolicyStack:
    """The invocation stack here (outermost first), captured eagerly.

    An unreadable inherited payload is logged and ignored (S1)."""
    inherited: PolicyStack = PolicyStack()
    raw = inherited_json()
    if raw:
        try:
            inherited = PolicyStack.from_json(raw)
        except WireError as e:
            log.warning("rightsizer: ignoring custom_context[%r]: %s", CONTEXT_KEY, e)
    local = _rt._STACK.get()
    m = min(_MIRRORED.get(), len(local))
    return PolicyStack((*inherited, *local[m:]))


class policy:
    """``with rs.flyte.policy(mode="enforce", risk="lean"): ...`` (also ``async with``).

    Every rightsizable call in the block, and every descendant action it
    launches, sees this policy. Calls whose coroutines are *created* in the
    block keep it even when awaited after the block exits.
    """

    def __init__(self, spec: Any = None, *, mode: Any = None, rightsize: Any = None, risk: Any = None,
                 estimator: Optional[str] = None):
        self.policy = invocation(spec, mode=mode, rightsize=rightsize, risk=risk, estimator=estimator)
        self._tokens: list = []

    def __enter__(self) -> InvocationPolicy:
        t_stack = _rt._STACK.set(_rt._STACK.get().push(self.policy))
        cm = None
        t_m = None
        if _task_context() is not None:
            try:
                import flyte

                payload = captured_stack().to_json()
                cm = flyte.custom_context(**{CONTEXT_KEY: payload})
                cm.__enter__()
                t_m = _MIRRORED.set(len(_rt._STACK.get()))
            except Exception as e:  # S1: the local stack still applies
                log.warning("rightsizer: policy not propagated to descendants: %s", e)
                cm = None
        self._tokens.append((t_stack, cm, t_m))
        return self.policy

    def __exit__(self, et: Any, ev: Any, tb: Any) -> bool:
        t_stack, cm, t_m = self._tokens.pop()
        if t_m is not None:
            _MIRRORED.reset(t_m)
        if cm is not None:
            try:
                cm.__exit__(et, ev, tb)
            except Exception:  # pragma: no cover
                pass
        _rt._STACK.reset(t_stack)
        return False

    async def __aenter__(self) -> InvocationPolicy:
        return self.__enter__()

    async def __aexit__(self, et: Any, ev: Any, tb: Any) -> bool:
        return self.__exit__(et, ev, tb)


def run_context(spec: Any = None, *, mode: Any = None, rightsize: Any = None, risk: Any = None,
                estimator: Optional[str] = None) -> dict:
    """``custom_context`` for a run's invocation policy:
    ``flyte.with_runcontext(custom_context=rs.flyte.run_context(mode="enforce"))``."""
    p = invocation(spec, mode=mode, rightsize=rightsize, risk=risk, estimator=estimator)
    return {CONTEXT_KEY: PolicyStack(() if p.is_empty() else (p,)).to_json()}


def resolve(defn: Optional[_rt.DefinitionPolicy], stack: PolicyStack, config_risk: Any = None) -> _rt.EffectivePolicy:
    """flyte.md §3.3. ``mode_ceiling`` and breakers are applied by the engine."""
    return _rt.resolve(defn, stack, mode_ceiling=Mode.ENFORCE, config_risk=config_risk)


__all__ = [
    "CONTEXT_KEY",
    "DefinitionPolicy",
    "InvocationPolicy",
    "PolicyStack",
    "captured_stack",
    "definition",
    "invocation",
    "policy",
    "resolve",
    "run_context",
]
