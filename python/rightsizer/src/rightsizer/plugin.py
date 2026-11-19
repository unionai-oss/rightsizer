"""Python adapters (plugins.md §3.5): ``rightsizer.plugin.Adapter`` mirrors
the Rust traits (``Adapter``: ``extract`` / ``render`` / ``validate``;
``PlatformModel``: ``reference`` / ``state`` / ``accelerators``) for
platforms whose submissions live in a Python process (Flyte, Ray, Modal,
SkyPilot, ...). A Python adapter registered with :func:`register_adapter`
runs in-process; it renders ``Patch.options`` only and never touches bytes.

:func:`size` is the in-process driver every Python integration shares::

    extract → per unit: reference + state → one decision per group
    (decide_group) or per unit (decide) → render (enforce only) → validate

It never raises (S1): any failure leaves the submission unchanged.

The shapes are the Rust ``rightsizer-plugin`` JSON shapes (``Unit``,
``Extracted``, ``Patch``, ``Rejection``, ``AdapterEnv``).
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union

from rightsizer_runtime.wire import Mode, ResourceSpec

from ._errors import Rejection
from .types import Code, Context, Decision, Input, Prior, Resources

log = logging.getLogger("rightsizer.plugin")


@dataclass
class AdapterEnv:
    """What the hosting surface tells an adapter."""

    cluster: Optional[str] = None
    principal: Optional[str] = None
    namespace: Optional[str] = None
    deadline_ms: Optional[int] = None
    settings: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {k: v for k, v in (("cluster", self.cluster), ("principal", self.principal),
                                                  ("namespace", self.namespace), ("deadline_ms", self.deadline_ms)) if v is not None}
        out["settings"] = dict(self.settings)
        return out


@dataclass
class Unit:
    """One resource-bearing thing in a submission.

    ``key_suffix`` is the user-overridable part of the key; ``key`` (a full
    workload key) is set only when it is not ``{kind}://{tenant}/{suffix}``
    and only from trusted data.
    """

    id: str
    key_suffix: str
    prior: Union[Resources, Prior, ResourceSpec, Mapping[str, Any], None] = None
    inputs: Sequence[Input] = ()
    code: Optional[Code] = None
    manifest: Mapping[str, Any] = field(default_factory=dict)
    labels: Mapping[str, str] = field(default_factory=dict)
    constraints: Mapping[str, Any] = field(default_factory=dict)
    group: Optional[str] = None
    distributed: bool = False
    key: Optional[str] = None
    defaulted: Sequence[str] = ()

    def to_dict(self) -> Dict[str, Any]:
        seed: Dict[str, Any] = {"inputs": [i.to_dict() for i in self.inputs], "manifest": dict(self.manifest),
                                "labels": dict(self.labels)}
        if self.code is not None:
            seed["code"] = self.code.to_dict()
        out: Dict[str, Any] = {"id": self.id, "key_suffix": self.key_suffix,
                               "prior": Prior.of(self.prior, self.defaulted).to_dict(), "seed": seed,
                               "constraints": dict(self.constraints), "distributed": self.distributed}
        if self.group is not None:
            out["group"] = self.group
        if self.key is not None:
            out["key"] = self.key
        return out


@dataclass
class Extracted:
    """What ``extract`` returns. ``tenant`` comes from authenticated data,
    never from user fields; ``skip`` (``{"kind": "already_sized", ...}``,
    ``"opted_out"``, ...) leaves the submission alone."""

    tenant: str
    units: List[Unit] = field(default_factory=list)
    skip: Optional[Mapping[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"tenant": self.tenant, "units": [u.to_dict() for u in self.units]}
        if self.skip is not None:
            out["skip"] = dict(self.skip)
        return out


@dataclass
class Patch:
    """A rendered change. Python adapters render options only
    (``{"kind": "options", "options": {...}}``) or nothing."""

    options: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_none(self) -> bool:
        return not self.options

    def to_dict(self) -> Dict[str, Any]:
        return {"kind": "none"} if self.is_none else {"kind": "options", "options": dict(self.options)}

    @classmethod
    def none(cls) -> "Patch":
        return cls()


class Adapter:
    """Base class for in-process Python adapters.

    Required: ``kind`` (the platform, e.g. ``"ray"``), :meth:`extract`,
    :meth:`render`. Optional: :meth:`validate` (raise
    :class:`~rightsizer.Rejection`), :meth:`reference` (static platform
    facts, part of the context digest), :meth:`state` (live shapes, prices,
    quota), :meth:`accelerators` (canonical id → native name).
    """

    kind: str = ""

    def extract(self, native: Any, env: AdapterEnv) -> Extracted:  # pragma: no cover - interface
        raise NotImplementedError

    def render(self, native: Any, decisions: Sequence[Tuple[str, ResourceSpec]]) -> Patch:  # pragma: no cover
        raise NotImplementedError

    def validate(self, native: Any, patch: Patch, env: AdapterEnv) -> None:
        return None

    def reference(self, unit: Unit) -> Dict[str, Any]:
        """``PlatformRef`` JSON; default: ``{"kind": self.kind}``."""
        return {"kind": self.kind}

    def state(self, tenant: str, env: AdapterEnv) -> Dict[str, Any]:
        """``PlatformState`` JSON; default: unknown (empty)."""
        return {}

    def accelerators(self) -> Dict[str, str]:
        """Canonical accelerator id → native name."""
        return {}


_registry: Dict[str, Adapter] = {}
_lock = threading.Lock()


def register_adapter(adapter: Union[Adapter, type]) -> Any:
    """Register an in-process adapter by its ``kind`` (instantiates a class).
    Usable as a class decorator; returns its argument."""
    obj = adapter() if isinstance(adapter, type) else adapter
    if not getattr(obj, "kind", ""):
        raise ValueError("an adapter needs a non-empty kind")
    for m in ("extract", "render"):
        if not callable(getattr(obj, m, None)):
            raise TypeError(f"adapter {obj.kind}: missing {m}()")
    with _lock:
        _registry[obj.kind] = obj
    return adapter


def unregister_adapter(kind: str) -> bool:
    with _lock:
        return _registry.pop(kind, None) is not None


def get_adapter(kind: str) -> Optional[Adapter]:
    with _lock:
        return _registry.get(kind)


def adapters() -> Dict[str, Adapter]:
    with _lock:
        return dict(_registry)


@dataclass
class Sized:
    """What :func:`size` did. ``patch`` is what to apply (none unless a
    decision is enforced and validates); ``decisions`` per unit id."""

    patch: Patch
    decisions: Dict[str, Decision]
    extracted: Optional[Extracted] = None
    rejection: Optional[Rejection] = None
    error: Optional[str] = None


def _context(adapter: Adapter, tenant: str, u: Unit) -> Context:
    platform = adapter.reference(u) or {"kind": adapter.kind}
    key = u.key or f"{adapter.kind}://{tenant}//{u.key_suffix}"
    return Context(key=key, prior=Prior.of(u.prior, u.defaulted), inputs=tuple(u.inputs), code=u.code, platform=platform,
                   unit=u.id, group=u.group, labels=dict(u.labels), manifest=dict(u.manifest))


def size(adapter: Union[Adapter, str], native: Any, env: Optional[AdapterEnv] = None, *, engine: Any = None,
         **policy: Any) -> Sized:
    """Extract, decide, render and validate one submission (never raises)."""
    from . import _engine

    env = env or AdapterEnv()
    try:
        a = get_adapter(adapter) if isinstance(adapter, str) else adapter
        if a is None:
            return Sized(Patch.none(), {}, error=f"no adapter registered for {adapter!r}")
        ex = a.extract(native, env)
        if ex.skip is not None or not ex.units:
            return Sized(Patch.none(), {}, extracted=ex)
        try:
            state = a.state(ex.tenant, env) or {}
        except Exception as e:  # unknown platform state: increases are clamped or held (S4)
            log.warning("rightsizer: %s state(): %s", a.kind, e)
            state = {}
        eng = engine or _engine.default_engine()
        decisions: Dict[str, Decision] = {}
        groups: Dict[str, List[Unit]] = {}
        for u in ex.units:
            if u.group is not None:
                groups.setdefault(u.group, []).append(u)
        policy = dict(policy)
        policy.setdefault("state", state)
        base = dict(policy.pop("constraints", None) or {})
        for u in ex.units:
            if u.group is not None:
                continue
            c = _context(a, ex.tenant, u)
            decisions[u.id] = eng.decide(c, constraints={**base, **u.constraints}, **policy)
        for members in groups.values():
            ctxs = [_context(a, ex.tenant, u) for u in members]
            # Group members share constraints (the first member's).
            for u, d in zip(members, eng.decide_group(ctxs, constraints={**base, **members[0].constraints}, **policy)):
                decisions[u.id] = d
        enforced = [(uid, d.applied) for uid, d in decisions.items() if Mode.parse(d.mode) == Mode.ENFORCE and d.changed]
        if not enforced:
            return Sized(Patch.none(), decisions, extracted=ex)
        patch = a.render(native, enforced)
        try:
            a.validate(native, patch, env)
        except Rejection as r:
            log.info("rightsizer: %s rejected the patch: %s", a.kind, r)
            return Sized(Patch.none(), decisions, extracted=ex, rejection=r)
        return Sized(patch, decisions, extracted=ex)
    except Exception as e:  # S1: the submission goes through unchanged
        log.error("rightsizer: sizing failed, submission unchanged (S1): %s: %s", type(e).__name__, e)
        return Sized(Patch.none(), {}, error=f"{type(e).__name__}: {e}")


__all__ = [
    "Adapter", "AdapterEnv", "Extracted", "Patch", "Rejection", "Sized", "Unit", "adapters", "get_adapter",
    "register_adapter", "size", "unregister_adapter",
]
