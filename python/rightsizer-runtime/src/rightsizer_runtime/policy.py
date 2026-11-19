"""Platform-neutral policy model (flyte.md §3, SPEC §2.3 mode ceiling, §7.3 risk).

- :class:`DefinitionPolicy` — what the author of a task/function allows.
  Fields default to ``None`` meaning *inherit*; :meth:`DefinitionPolicy.merged_over`
  merges a task policy over its environment's field by field (the task wins),
  and :meth:`DefinitionPolicy.resolved` fills the spec defaults.
- :class:`InvocationPolicy` — what a caller asks for (mode, risk, estimator).
  Invocation policies form a stack, innermost last, held in a contextvar and
  serialized as compact JSON ``{"v":1,"s":[...]}`` (≤ 1 KiB).
- :func:`resolve` — ``effective.mode = min(stack.mode ?? default_mode,
  max_mode, mode_ceiling)``; ``effective.risk = stack.risk ?? definition.risk
  ?? config.risk``.
"""

from __future__ import annotations

import contextlib
import contextvars
import functools
import inspect
import json
import logging
from dataclasses import dataclass, fields, replace
from typing import Any, Awaitable, Callable, Dict, FrozenSet, Iterable, Iterator, Optional, Sequence, Tuple, TypeVar, Union

from .wire import Mode, Requests, WireError

log = logging.getLogger("rightsizer.runtime.policy")

STACK_MAX_BYTES = 1024
STACK_VERSION = 1

RiskProfile = str  # "lean" | "balanced" | "averse"
Risk = Union[str, float]  # profile name, or an explicit oom_penalty
RISK_PROFILES: Dict[str, Dict[str, float]] = {
    "lean": {"oom_penalty": 1.0, "delay_penalty": 1.0, "neighbour_penalty": 1.0},
    "balanced": {"oom_penalty": 4.0, "delay_penalty": 1.0, "neighbour_penalty": 2.0},
    "averse": {"oom_penalty": 16.0, "delay_penalty": 1.0, "neighbour_penalty": 4.0},
}
DEFAULT_RISK = "balanced"

# Axis names accepted in pins; aliases from SPEC §5.1.
AXES: FrozenSet[str] = frozenset({"memory", "cpu", "gpu", "gpu_count", "gpu_type", "disk", "ephemeral_storage", "shm"})
ON_OOM: FrozenSet[str] = frozenset({"auto", "fail", "escalate"})


def parse_mode(value: Any, where: str = "mode") -> Mode:
    return Mode.parse(value, where)  # type: ignore[return-value]


def parse_risk(value: Any, where: str = "risk") -> Risk:
    if isinstance(value, bool):
        raise WireError(f"{where}: expected profile name or number")
    if isinstance(value, str):
        if value not in RISK_PROFILES:
            raise WireError(f"{where}: {value!r} is not one of {sorted(RISK_PROFILES)}")
        return value
    if isinstance(value, (int, float)):
        v = float(value)
        if not (v > 0 and v < float("inf")):
            raise WireError(f"{where}: oom_penalty must be a positive finite number")
        return v
    raise WireError(f"{where}: expected profile name or number, got {type(value).__name__}")


def min_mode(*modes: Optional[Mode]) -> Mode:
    ms = [parse_mode(m) for m in modes if m is not None]
    if not ms:
        raise ValueError("min_mode needs at least one mode")
    return min(ms, key=lambda m: m.rank)


# --------------------------------------------------------------------------- definition


@dataclass(frozen=True)
class DefinitionPolicy:
    """flyte.md §3.1, platform-neutral. ``None`` means *inherit / default*."""

    max_mode: Optional[Mode] = None  # default enforce; "off" disables entirely
    default_mode: Optional[Mode] = None  # default recommend
    pins: Optional[FrozenSet[str]] = None  # default frozenset()
    allow_gpu_type_change: Optional[bool] = None  # default False
    ceiling: Optional[Requests] = None
    floor: Optional[Requests] = None
    on_oom: Optional[str] = None  # default "auto"
    max_escalations: Optional[int] = None  # default 2
    key: Optional[str] = None
    risk: Optional[Risk] = None

    DEFAULTS = {
        "max_mode": Mode.ENFORCE,
        "default_mode": Mode.RECOMMEND,
        "pins": frozenset(),
        "allow_gpu_type_change": False,
        "on_oom": "auto",
        "max_escalations": 2,
    }

    def __post_init__(self) -> None:
        if self.max_mode is not None:
            object.__setattr__(self, "max_mode", parse_mode(self.max_mode, "max_mode"))
        if self.default_mode is not None:
            object.__setattr__(self, "default_mode", parse_mode(self.default_mode, "default_mode"))
        if self.pins is not None:
            pins = frozenset([self.pins] if isinstance(self.pins, str) else self.pins)
            bad = pins - AXES
            if bad:
                raise ValueError(f"pins: unknown axes {sorted(bad)}; allowed {sorted(AXES)}")
            object.__setattr__(self, "pins", pins)
        if self.on_oom is not None and self.on_oom not in ON_OOM:
            raise ValueError(f"on_oom: {self.on_oom!r} not in {sorted(ON_OOM)}")
        if self.max_escalations is not None:
            if isinstance(self.max_escalations, bool) or not isinstance(self.max_escalations, int) or self.max_escalations < 0:
                raise ValueError("max_escalations: expected non-negative int")
        for k in ("ceiling", "floor"):
            v = getattr(self, k)
            if v is not None and not isinstance(v, Requests):
                raise ValueError(f"{k}: expected rightsizer_runtime.wire.Requests")
        if self.risk is not None:
            object.__setattr__(self, "risk", parse_risk(self.risk))

    def merged_over(self, base: Optional["DefinitionPolicy"]) -> "DefinitionPolicy":
        """``self`` (task) over ``base`` (environment), field by field."""
        if base is None:
            return self
        kw = {}
        for f in fields(self):
            mine = getattr(self, f.name)
            kw[f.name] = mine if mine is not None else getattr(base, f.name)
        return DefinitionPolicy(**kw)

    def resolved(self) -> "DefinitionPolicy":
        """A copy with every defaulted field filled (``ceiling``/``floor``/``key``/``risk`` stay optional)."""
        kw = {f.name: getattr(self, f.name) for f in fields(self)}
        for k, v in self.DEFAULTS.items():
            if kw[k] is None:
                kw[k] = v
        return DefinitionPolicy(**kw)

    def get(self, name: str) -> Any:
        v = getattr(self, name)
        return self.DEFAULTS.get(name) if v is None else v


# --------------------------------------------------------------------------- invocation


@dataclass(frozen=True)
class InvocationPolicy:
    mode: Optional[Mode] = None
    risk: Optional[Risk] = None
    estimator: Optional[str] = None

    def __post_init__(self) -> None:
        if self.mode is not None:
            object.__setattr__(self, "mode", parse_mode(self.mode))
        if self.risk is not None:
            object.__setattr__(self, "risk", parse_risk(self.risk))
        if self.estimator is not None and (not isinstance(self.estimator, str) or not self.estimator):
            raise ValueError("estimator: expected non-empty string")

    def is_empty(self) -> bool:
        return self.mode is None and self.risk is None and self.estimator is None

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        if self.mode is not None:
            out["mode"] = self.mode.value
        if self.risk is not None:
            out["risk"] = self.risk
        if self.estimator is not None:
            out["estimator"] = self.estimator
        return out

    @classmethod
    def from_dict(cls, d: Any, where: str = "policy") -> "InvocationPolicy":
        if not isinstance(d, dict):
            raise WireError(f"{where}: expected object")
        extra = set(d) - {"mode", "risk", "estimator"}
        if extra:
            raise WireError(f"{where}: unknown field(s) {sorted(extra)}")
        try:
            return cls(
                mode=None if d.get("mode") is None else parse_mode(d["mode"], f"{where}.mode"),
                risk=None if d.get("risk") is None else parse_risk(d["risk"], f"{where}.risk"),
                estimator=d.get("estimator"),
            )
        except ValueError as e:
            raise WireError(f"{where}: {e}") from None


class PolicyStack(tuple):
    """Immutable stack, outermost first (index 0 = the run's policy), innermost last."""

    def __new__(cls, items: Iterable[InvocationPolicy] = ()) -> "PolicyStack":
        items = tuple(items)
        for p in items:
            if not isinstance(p, InvocationPolicy):
                raise TypeError("PolicyStack items must be InvocationPolicy")
        return super().__new__(cls, items)

    def push(self, p: InvocationPolicy) -> "PolicyStack":
        return PolicyStack((*self, p))

    def _innermost(self, name: str) -> Any:
        for p in reversed(self):
            v = getattr(p, name)
            if v is not None:
                return v
        return None

    @property
    def mode(self) -> Optional[Mode]:
        return self._innermost("mode")

    @property
    def risk(self) -> Optional[Risk]:
        return self._innermost("risk")

    @property
    def estimator(self) -> Optional[str]:
        return self._innermost("estimator")

    def collapsed(self) -> "PolicyStack":
        """One entry carrying each field's innermost value; resolves identically."""
        p = InvocationPolicy(mode=self.mode, risk=self.risk, estimator=self.estimator)
        return PolicyStack(() if p.is_empty() else (p,))

    # ---- wire: {"v":1,"s":[...]} ----

    def to_json(self, max_bytes: int = STACK_MAX_BYTES) -> str:
        """Compact JSON, at most ``max_bytes``.

        Over the cap, the stack is collapsed to one equivalent entry; if that is
        still too large (a huge estimator name), the estimator is dropped. Each
        step is logged; resolution of mode and risk is unchanged.
        """

        def enc(stack: Sequence[InvocationPolicy]) -> str:
            return json.dumps(
                {"v": STACK_VERSION, "s": [p.to_dict() for p in stack if not p.is_empty()]},
                separators=(",", ":"),
            )

        s = enc(self)
        if len(s.encode()) <= max_bytes:
            return s
        log.warning("rightsizer: policy stack exceeds %d bytes; collapsing", max_bytes)
        c = self.collapsed()
        s = enc(c)
        if len(s.encode()) <= max_bytes:
            return s
        log.warning("rightsizer: collapsed policy stack still exceeds %d bytes; dropping estimator", max_bytes)
        s = enc([replace(p, estimator=None) for p in c])
        return s

    @classmethod
    def from_json(cls, s: Union[str, bytes, None], max_bytes: int = STACK_MAX_BYTES) -> "PolicyStack":
        if s is None or s == "" or s == b"":
            return cls()
        raw = s.encode() if isinstance(s, str) else s
        if len(raw) > max_bytes:
            raise WireError(f"policy stack: {len(raw)} bytes exceeds {max_bytes}")
        try:
            d = json.loads(raw)
        except ValueError as e:
            raise WireError(f"policy stack: invalid JSON: {e}") from None
        if not isinstance(d, dict) or set(d) - {"v", "s"}:
            raise WireError("policy stack: expected {'v':1,'s':[...]}")
        if d.get("v") != STACK_VERSION or isinstance(d.get("v"), bool):
            raise WireError(f"policy stack: unsupported version {d.get('v')!r}")
        items = d.get("s", [])
        if not isinstance(items, list):
            raise WireError("policy stack: 's' must be an array")
        return cls(InvocationPolicy.from_dict(x, f"policy stack.s[{i}]") for i, x in enumerate(items))


_STACK: contextvars.ContextVar[PolicyStack] = contextvars.ContextVar("rightsizer_policy_stack", default=PolicyStack())


def current_stack(inherited: Union[PolicyStack, str, None] = None) -> PolicyStack:
    """The stack visible here: ``inherited`` (e.g. from a parent's custom context) below the local one.

    ``inherited`` may be a :class:`PolicyStack` or its JSON. An unparseable
    JSON payload is logged and ignored (S1).
    """
    local = _STACK.get()
    if inherited is None:
        return local
    if isinstance(inherited, str):
        try:
            inherited = PolicyStack.from_json(inherited)
        except WireError as e:
            log.warning("rightsizer: ignoring inherited policy stack: %s", e)
            return local
    return PolicyStack((*inherited, *local))


@contextlib.contextmanager
def policy(mode: Optional[Union[Mode, str]] = None, risk: Optional[Risk] = None, estimator: Optional[str] = None) -> Iterator[InvocationPolicy]:
    """Push an invocation policy for the duration of the block (sync and async code alike)."""
    p = InvocationPolicy(mode=mode, risk=risk, estimator=estimator)  # type: ignore[arg-type]
    token = _STACK.set(_STACK.get().push(p))
    try:
        yield p
    finally:
        _STACK.reset(token)


@contextlib.contextmanager
def using_stack(stack: PolicyStack) -> Iterator[PolicyStack]:
    """Replace the local stack for the block (used to re-enter a captured stack)."""
    token = _STACK.set(PolicyStack(stack))
    try:
        yield stack
    finally:
        _STACK.reset(token)


F = TypeVar("F", bound=Callable[..., Any])


@dataclass(frozen=True)
class CapturedStack:
    """A stack captured eagerly, at call time, to be re-entered later or elsewhere.

    Use it so that a coroutine created inside a ``policy()`` block keeps that
    block's policy even if it is awaited after the block exits, and so that a
    thread or executor sees the caller's policy (contextvars do not cross into
    plain threads).
    """

    stack: PolicyStack

    def run(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        with using_stack(self.stack):
            return fn(*args, **kwargs)

    async def await_(self, aw: Awaitable[Any]) -> Any:
        with using_stack(self.stack):
            return await aw

    def wrap(self, fn: F) -> F:
        """A callable that always runs under this stack (sync or async)."""
        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def _aw(*a: Any, **k: Any) -> Any:
                with using_stack(self.stack):
                    return await fn(*a, **k)

            return _aw  # type: ignore[return-value]

        @functools.wraps(fn)
        def _w(*a: Any, **k: Any) -> Any:
            with using_stack(self.stack):
                return fn(*a, **k)

        return _w  # type: ignore[return-value]


def capture_stack(inherited: Union[PolicyStack, str, None] = None) -> CapturedStack:
    """Eagerly capture the current stack (see :class:`CapturedStack`)."""
    return CapturedStack(current_stack(inherited))


def eager(fn: F) -> F:
    """Decorator: capture the policy stack when the function is *called*, not when its coroutine runs.

    For ``async def`` functions, ``fn(...)`` returns a coroutine that runs
    under the stack active at the call. For sync functions it is a no-op.
    """
    if not inspect.iscoroutinefunction(fn):
        return fn

    @functools.wraps(fn)
    def _call(*a: Any, **k: Any) -> Awaitable[Any]:
        cap = capture_stack()
        return cap.await_(fn(*a, **k))

    return _call  # type: ignore[return-value]


# --------------------------------------------------------------------------- resolution


@dataclass(frozen=True)
class EffectivePolicy:
    mode: Mode
    risk: Risk
    estimator: Optional[str]
    definition: DefinitionPolicy  # resolved

    @property
    def oom_penalty(self) -> float:
        return risk_oom_penalty(self.risk)


def risk_oom_penalty(risk: Risk) -> float:
    r = parse_risk(risk)
    return float(r) if not isinstance(r, str) else RISK_PROFILES[r]["oom_penalty"]


def risk_to_wire(risk: Risk) -> Any:
    """Rust ``RiskProfile`` JSON: ``"balanced"`` or ``{"oom_penalty": x}`` (other penalties default)."""
    r = parse_risk(risk)
    return r if isinstance(r, str) else {"oom_penalty": float(r)}


def resolve_mode(stack_mode: Optional[Mode], default_mode: Optional[Mode], max_mode: Optional[Mode], mode_ceiling: Optional[Mode]) -> Mode:
    """``min(stack_mode ?? default_mode, max_mode, mode_ceiling)``; ``None`` → spec default."""
    requested = stack_mode if stack_mode is not None else (default_mode if default_mode is not None else Mode.RECOMMEND)
    return min_mode(
        parse_mode(requested),
        parse_mode(max_mode if max_mode is not None else Mode.ENFORCE),
        parse_mode(mode_ceiling if mode_ceiling is not None else Mode.ENFORCE),
    )


def resolve_risk(stack_risk: Optional[Risk], definition_risk: Optional[Risk], config_risk: Optional[Risk], default: Risk = DEFAULT_RISK) -> Risk:
    """``stack.risk ?? definition.risk ?? config.risk ?? default``.

    Integrations with no ladder and ``same_size = 0`` pass ``default="averse"`` (SPEC §7.3).
    """
    for r in (stack_risk, definition_risk, config_risk):
        if r is not None:
            return parse_risk(r)
    return parse_risk(default)


def resolve(
    definition: Optional[DefinitionPolicy] = None,
    stack: Optional[PolicyStack] = None,
    *,
    mode_ceiling: Optional[Union[Mode, str]] = Mode.ENFORCE,
    config_risk: Optional[Risk] = None,
    default_risk: Risk = DEFAULT_RISK,
) -> EffectivePolicy:
    """Resolve the effective policy (flyte.md §3.3).

    ``mode_ceiling`` is the operator's global switch, read from the store;
    pass what the store says (callers that cannot read it should pass
    ``"recommend"``, which still allows observing and recommending).
    """
    d = (definition or DefinitionPolicy()).resolved()
    st = stack if stack is not None else current_stack()
    mode = resolve_mode(st.mode, d.default_mode, d.max_mode, None if mode_ceiling is None else parse_mode(mode_ceiling))
    risk = resolve_risk(st.risk, d.risk, config_risk, default_risk)
    return EffectivePolicy(mode=mode, risk=risk, estimator=st.estimator, definition=d)


__all__ = [
    "AXES",
    "CapturedStack",
    "DEFAULT_RISK",
    "DefinitionPolicy",
    "EffectivePolicy",
    "InvocationPolicy",
    "Mode",
    "PolicyStack",
    "RISK_PROFILES",
    "STACK_MAX_BYTES",
    "capture_stack",
    "current_stack",
    "eager",
    "min_mode",
    "parse_mode",
    "parse_risk",
    "policy",
    "resolve",
    "resolve_mode",
    "resolve_risk",
    "risk_oom_penalty",
    "risk_to_wire",
    "using_stack",
]
