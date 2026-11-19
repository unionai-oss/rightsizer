"""``rs.ray.sized(ActorClass, key=...)`` (ray.md §3).

Ray blocks subclassing ``ActorClass`` (``ActorClass.__init__`` raises
``ActorClassInheritanceException``; ``actor.py`` lines 1562-1597 in Ray
2.60), so actors are sized through a thin proxy that exposes ``.remote``
and ``.options``. It decides **once, at creation**:
``Actor.options(**decided).remote(...)``. It is not an ``ActorClass``
(``isinstance`` checks fail). Actors are not measured in-task (their
lifetime spans many calls); their outcomes come from importers.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, FrozenSet, Optional

from ._sizing import SizingPolicy, decide_options, pins_from

log = logging.getLogger("rightsizer.ray")


class SizedActorClass:
    """Proxy for a Ray actor class whose instances are sized at creation."""

    def __init__(self, actor_cls: Any, *, policy: Optional[SizingPolicy] = None, options: Optional[Dict[str, Any]] = None,
                 pins: FrozenSet[str] = frozenset()):
        self._cls = actor_cls
        self._policy = policy or SizingPolicy()
        self._opts: Dict[str, Any] = dict(options or {})
        self._pins = frozenset(pins)
        self.last_decision: Any = None

    @property
    def actor_class(self) -> Any:
        """The wrapped ``ray.actor.ActorClass``."""
        return self._cls

    def _defaults(self) -> Dict[str, Any]:
        base = dict(getattr(self._cls, "_default_options", None) or {})
        base.update(self._opts)
        return base

    def options(self, **actor_options: Any) -> "SizedActorClass":
        """Like ``ActorClass.options``: the axes set here are pins."""
        merged = dict(self._opts)
        merged.update(actor_options)
        return SizedActorClass(self._cls, policy=self._policy, options=merged, pins=self._pins | pins_from(actor_options))

    def remote(self, *args: Any, **kwargs: Any) -> Any:
        """Create the actor: one decision, then ``ActorCls.options(**decided).remote(...)``."""
        opts = self._defaults()
        try:
            meta = getattr(self._cls, "__ray_metadata__", None)
            target = getattr(meta, "modified_class", None) or self._cls
            sized = decide_options(opts, self._policy, target=_orig_class(target), code=_code(target), args=args,
                                   kwargs=kwargs, pins=self._pins, actor=True)
            self.last_decision = sized.decision
            final = {k: v for k, v in sized.options.items() if k in self._opts or k in sized.changed}
        except Exception as e:  # S1
            log.warning("rightsizer.ray: actor sizing failed, creation unchanged (S1): %s", e)
            final = dict(self._opts)
        cls = self._cls.options(**final) if final else self._cls
        return cls.remote(*args, **kwargs)

    def bind(self, *args: Any, **kwargs: Any) -> Any:
        """Ray DAGs / Serve: passed through, not sized."""
        cls = self._cls.options(**self._opts) if self._opts else self._cls
        return cls.bind(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        return getattr(self._cls, name)

    def __repr__(self) -> str:
        return f"<rightsizer SizedActorClass {getattr(self._cls, '__ray_metadata__', self._cls)!r}>"


def _orig_class(target: Any) -> Any:
    # Ray's modified class keeps the user's name; prefer the user's class when reachable.
    for c in getattr(target, "__mro__", ())[1:2]:
        if getattr(c, "__qualname__", None) == getattr(target, "__qualname__", None):
            return c
    return target


def _code(target: Any) -> Any:
    try:
        from ..types import Code

        return Code.from_callable(_orig_class(target))
    except Exception:
        return None


def sized(actor_cls: Any, *, key: Optional[str] = None, policy: Optional[SizingPolicy] = None, **kw: Any) -> SizedActorClass:
    """Size a Ray actor class at creation: ``Embedder = rs.ray.sized(ray.remote(num_gpus=1)(Cls), key="embedder")``.

    Policy keywords as ``rs.ray.remote`` (``mode``, ``risk``, ``ceiling``, ``pin``…).
    """
    from ray.actor import ActorClass

    if isinstance(actor_cls, SizedActorClass):
        return actor_cls
    if not isinstance(actor_cls, ActorClass):
        raise TypeError(f"rs.ray.sized: expected a ray actor class (ray.remote(cls)), not {type(actor_cls).__name__}")
    if policy is None:
        from ._remote import _policy_from

        policy = _policy_from(dict(kw, key=key))
    elif key is not None:
        from dataclasses import replace

        policy = replace(policy, key=key)
    return SizedActorClass(actor_cls, policy=policy)


__all__ = ["SizedActorClass", "sized"]
