"""``rs.ray.remote`` / ``rs.ray.rightsize`` (ray.md §2).

``RightsizedRemoteFunction`` subclasses ``ray.remote_function.RemoteFunction``.
Ray 2.60's ``RemoteFunction.__init__`` assigns ``self.remote`` as an
instance attribute that proxies to ``self._remote`` (``remote_function.py``
lines 174-183), and ``.options(**kw)`` returns a ``FuncWrapper`` whose
``.remote`` calls ``func_cls._remote`` with the merged options (lines
335-357). So the subclass overrides **``_remote``**: it decides, merges the
decision into the task options, adds the in-task measurement payload and
calls the base ``_remote``; and it overrides ``options`` only to remember
which axes the call pinned (Ray merges the keywords into the defaults, so
``_remote`` alone cannot tell them apart).

Never changed: ``max_retries`` (no ladder in v1, ray.md §7),
``resources={…}``, ``num_gpus`` and every other option.
"""

from __future__ import annotations

import contextvars
import inspect
import logging
from typing import Any, Callable, Dict, FrozenSet, Optional

import ray
from ray import Language
from ray.remote_function import RemoteFunction

from ._observe import Tracked, observer
from ._sizing import SizingPolicy, decide_options, pins_from

log = logging.getLogger("rightsizer.ray")

# Pins of the `.options(...)` call whose `.remote` is running (set by _RsOptions.remote).
_call_pins: contextvars.ContextVar[FrozenSet[str]] = contextvars.ContextVar("rightsizer_ray_pins", default=frozenset())

# Keywords of rs.ray.remote that are not Ray options.
POLICY_KEYS = ("key", "mode", "risk", "estimator", "budget_ms", "ceiling", "floor", "pin", "scalars",
               "allow_gpu_type_change", "measure")


def _policy_from(kw: Dict[str, Any]) -> SizingPolicy:
    pin = kw.pop("pin", None) or ()
    if isinstance(pin, str):
        pin = (pin,)
    from ..types import _axes

    return SizingPolicy(
        key=kw.pop("key", None),
        mode=kw.pop("mode", None),
        risk=kw.pop("risk", None),
        estimator=kw.pop("estimator", None),
        budget_ms=kw.pop("budget_ms", None),
        ceiling=kw.pop("ceiling", None),
        floor=kw.pop("floor", None),
        pins=frozenset(_axes(list(pin))) if pin else frozenset(),
        scalars=tuple(kw.pop("scalars", None) or ()),
        allow_gpu_type_change=kw.pop("allow_gpu_type_change", None),
    )


class _RsOptions:
    """What ``RightsizedRemoteFunction.options(...)`` returns: Ray's
    ``FuncWrapper`` plus the pins of the call."""

    def __init__(self, inner: Any, pins: FrozenSet[str]):
        self._inner = inner
        self._pins = pins

    def remote(self, *args: Any, **kwargs: Any) -> Any:
        tok = _call_pins.set(self._pins)
        try:
            return self._inner.remote(*args, **kwargs)
        finally:
            _call_pins.reset(tok)

    def bind(self, *args: Any, **kwargs: Any) -> Any:
        """Ray DAGs / Serve: passed through, not sized (ray.md §2)."""
        return self._inner.bind(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class RightsizedRemoteFunction(RemoteFunction):
    """A Ray remote function whose ``.remote()`` calls are sized (ray.md §2).

    Built by ``rs.ray.remote`` (sizing + in-task measurement) and
    ``rs.ray.rightsize`` (sizing only). Picklable like its base (workers that
    unpickle it need ``rightsizer`` installed; its decisions there follow S1).
    """

    def __init__(self, language: Any, function: Callable[..., Any], function_descriptor: Any,
                 task_options: Dict[str, Any], *, policy: Optional[SizingPolicy] = None, measure: bool = True):
        user_fn = function
        self._rs_user_function = user_fn
        self._rs_policy = policy or SizingPolicy()
        self._rs_measure = bool(measure)
        self._rs_code: Any = None
        self._rs_code_digest: Optional[str] = None
        self._rs_code_ready = False
        if measure:
            from rightsizer_runtime.ray_child import make_task_function

            function = make_task_function(user_fn, None)
        super().__init__(language, function, function_descriptor, task_options)

    # -- the code reference, computed once per definition --

    def _rs_code_ref(self) -> Any:
        if not self._rs_code_ready:
            try:
                from ..types import Code

                self._rs_code = Code.from_callable(self._rs_user_function)
                try:
                    from .._engine import _code_info

                    info = _code_info(self._rs_code)
                    self._rs_code_digest = (info or {}).get("digest")
                except Exception:
                    self._rs_code_digest = None
            except Exception:
                self._rs_code = None
            self._rs_code_ready = True
        return self._rs_code

    # -- Ray overrides --

    def options(self, **task_options: Any) -> Any:  # noqa: D401 - Ray's docstring applies
        inner = super().options(**task_options)
        return _RsOptions(inner, pins_from(task_options))

    def _remote(self, args: Any = None, kwargs: Any = None, serialized_runtime_env_info: Optional[str] = None,
                **task_options: Any) -> Any:
        sized = None
        try:
            sized = self._rs_decide(args, kwargs, task_options)
        except Exception as e:  # S1
            log.warning("rightsizer.ray: sizing failed, call unchanged (S1): %s", e)
        if sized is not None:
            task_options = sized[0]
            kwargs = sized[1]
        refs = super()._remote(args=args, kwargs=kwargs, serialized_runtime_env_info=serialized_runtime_env_info,
                               **task_options)
        if sized is not None and sized[2] is not None:
            self._rs_observe(refs, sized[2])
        return refs

    # -- internals --

    def _rs_decide(self, args: Any, kwargs: Any, task_options: Dict[str, Any]):
        try:
            if not ray.is_initialized():
                from ray._private.auto_init_hook import auto_init_ray

                auto_init_ray()
        except Exception:
            pass
        s = decide_options(
            task_options,
            self._rs_policy,
            target=self._rs_user_function,
            code=self._rs_code_ref(),
            args=args or (),
            kwargs=kwargs or {},
            pins=_call_pins.get(),
            can_classify=None,
        )
        d = s.decision
        st = s.settings
        if d is None or st is None:
            return s.options, kwargs, None
        from rightsizer_runtime.wire import Mode

        mode = Mode.parse(d.mode)
        if mode is Mode.OFF:
            return s.options, kwargs, None
        if self._rs_measure and st.measure:
            meta = {
                "v": 1, "k": d.key, "d": d.id, "m": mode.value, "r": d.applied.to_dict(), "s": st.store,
                "tr": st.trust, "c": st.cluster, "i": s.inputs, "cd": self._rs_code_digest,
            }
            kwargs = dict(kwargs or {})
            kwargs["_rightsizer"] = meta
        info = (d, st, s.inputs) if st.observe else None
        return s.options, kwargs, info

    def _rs_observe(self, refs: Any, info: Any) -> None:
        d, st, inputs = info
        ref = refs[0] if isinstance(refs, list) and refs else refs
        if not isinstance(ref, ray.ObjectRef):
            return  # streaming generators, actor handles…: not observed
        observer().track(Tracked(ref=ref, key=d.key, decision=d, measured=self._rs_measure and st.measure,
                                 inputs=tuple(inputs), code_digest=self._rs_code_digest, trust=st.trust,
                                 cluster=st.cluster, engine=st.engine))

    def __repr__(self) -> str:
        return f"<rightsizer RightsizedRemoteFunction {self._function_name}>"


def _make(fn: Callable[..., Any], options: Dict[str, Any], policy: SizingPolicy, measure: bool) -> Any:
    from ray._common import ray_option_utils

    if inspect.isclass(fn):
        from ._actor import sized

        return sized(ray.remote(**options)(fn) if options else ray.remote(fn), policy=policy)
    if not callable(fn):
        raise TypeError("rs.ray.remote must be applied to a function or a class")
    if not getattr(fn, "__module__", None):
        fn.__module__ = "global"
    ray_option_utils.validate_task_options(
        options, in_options=False,
        is_generator_callable=inspect.isgeneratorfunction(fn) or inspect.isasyncgenfunction(fn),
    )
    return RightsizedRemoteFunction(Language.PYTHON, fn, None, dict(options), policy=policy, measure=measure)


def remote(*args: Any, **kwargs: Any) -> Any:
    """``@rs.ray.remote`` / ``@rs.ray.remote(num_cpus=4, memory=16 * 2**30, key="embed")``.

    Drop-in for ``@ray.remote``: same options, plus ``key``, ``mode``,
    ``risk``, ``estimator``, ``budget_ms``, ``ceiling``, ``floor``, ``pin``,
    ``scalars`` (argument names whose values join the context) and
    ``measure`` (in-task measurement, default on). A class becomes
    ``rs.ray.sized(ray.remote(...)(cls))``.
    """
    if len(args) == 1 and not kwargs and callable(args[0]):
        return _make(args[0], {}, SizingPolicy(), True)
    if args:
        raise TypeError("rs.ray.remote takes only keyword arguments (or a single function/class)")
    opts = dict(kwargs)
    measure = bool(opts.pop("measure", True))
    pol = _policy_from(opts)

    def deco(fn: Callable[..., Any]) -> Any:
        return _make(fn, dict(opts), pol, measure)

    return deco


def rightsize(fn: Any, *, key: Optional[str] = None, **policy: Any) -> Any:
    """Sizing only, for an existing remote function or actor class.

    ``rs.ray.rightsize(ray.remote(num_cpus=1)(tokenize_fn))`` builds a new
    ``RightsizedRemoteFunction`` from the function's underlying function and
    default options (copying the instance would keep its ``.remote`` closure
    bound to the original). Actor classes go to ``rs.ray.sized``.
    """
    pol = _policy_from(dict(policy, key=key))
    if isinstance(fn, RightsizedRemoteFunction):
        fn = fn._rs_original()
    if isinstance(fn, RemoteFunction):
        opts = dict(fn._default_options)
        under = getattr(fn, "_rs_user_function", None) or fn._function
        return RightsizedRemoteFunction(fn._language, under, None, opts, policy=pol, measure=False)
    from ray.actor import ActorClass

    if isinstance(fn, ActorClass):
        from ._actor import sized

        return sized(fn, policy=pol)
    raise TypeError(f"rs.ray.rightsize: expected a ray remote function or actor class, not {type(fn).__name__}")


def _original(self: RightsizedRemoteFunction) -> RemoteFunction:
    return RemoteFunction(self._language, self._rs_user_function, None, dict(self._default_options))


RightsizedRemoteFunction._rs_original = _original  # type: ignore[attr-defined]

__all__ = ["RightsizedRemoteFunction", "remote", "rightsize"]
