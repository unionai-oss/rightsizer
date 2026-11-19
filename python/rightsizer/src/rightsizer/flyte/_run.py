"""Run level (flyte.md §6.1): ``rs.flyte.with_runcontext(rightsize=..., **kw).run(task, ...)``.

``with_runcontext`` forwards every ``flyte.with_runcontext`` keyword and
merges the run's invocation policy into ``custom_context["rightsizer"]``, so
every descendant action inherits it. ``.run(task, *args)`` also sizes the
**root**: the decision is made here, with ``PlatformRetry{ladder: None}``
(the root has no parent to catch its failure, so root reductions need S3(b)
evidence), and the run is launched with ``flyte.run`` on the sized copy.

The run level is a policy modifier: an undecorated root is launched
unchanged (only the policy travels).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Tuple

from rightsizer_runtime.env import DECISION_ENV, DecisionEnv
from rightsizer_runtime.policy import PolicyStack
from rightsizer_runtime.wire import Mode, WireError

from . import _engine as _eng
from ._policy import CONTEXT_KEY, captured_stack, invocation
from ._resources import merge, to_spec
from ._task import is_rightsized

log = logging.getLogger("rightsizer.flyte")


def _tenant_from(kw: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    org = project = domain = None
    try:
        from flyte._initialize import _get_init_config

        cfg = _get_init_config()
        if cfg is not None:
            org, project, domain = getattr(cfg, "org", None), getattr(cfg, "project", None), getattr(cfg, "domain", None)
    except Exception:
        pass
    return org, kw.get("project") or project, kw.get("domain") or domain


def size_root(task: Any, stack: PolicyStack, tenant: Tuple[Any, Any, Any], args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Tuple[Any, Any]:
    """``(sized copy, decision)`` for a root action; ``(task, None)`` when it is not sized. Never raises."""
    if not is_rightsized(task):
        return task, None
    try:
        plan = task._rs_plan(stack, stack, args, kwargs, tenant=tenant, root=True)
        if plan is None:
            return task, None
        d = task._rs_decide(plan, None)
        prior = getattr(task, "resources", None)
        enforce = Mode.parse(d.mode) is Mode.ENFORCE
        resources = merge(prior, d.applied.requests, allow_gpu_type_change=plan.allow_gpu_type_change) if (enforce and prior is not None) else prior
        env = dict(getattr(task, "env_vars", None) or {})
        env[DECISION_ENV] = DecisionEnv(decision_id=d.id, key=d.key, mode=d.mode, requested=to_spec(resources),
                                        measured=plan.measured).to_json()
        return task._replace(resources=resources, env_vars=env), d
    except Exception as e:  # S1: the root runs unchanged
        log.warning("rightsizer: root %s not sized: %s", getattr(task, "name", "?"), e)
        return task, None


class Runner:
    """What ``rs.flyte.with_runcontext`` returns; ``.run`` is sync, ``.run.aio`` async."""

    def __init__(self, rightsize: Any = None, **kw: Any):
        self.policy = invocation(rightsize) if not isinstance(rightsize, dict) else invocation(**rightsize)
        cc = dict(kw.pop("custom_context", None) or {})
        base = PolicyStack()
        if cc.get(CONTEXT_KEY):
            try:
                base = PolicyStack.from_json(cc[CONTEXT_KEY])
            except WireError as e:
                log.warning("rightsizer: ignoring custom_context[%r]: %s", CONTEXT_KEY, e)
        local = captured_stack()
        self.stack = PolicyStack((*base, *local, *(() if self.policy.is_empty() else (self.policy,))))
        if len(self.stack):
            cc[CONTEXT_KEY] = self.stack.to_json()
        self.kw = dict(kw)
        if cc:
            self.kw["custom_context"] = cc
        self.last_decision: Any = None
        runner = self

        class _Run:
            def __call__(self, task: Any, *args: Any, **kwargs: Any) -> Any:
                import flyte

                t = runner._sized(task, args, kwargs)
                return flyte.with_runcontext(**runner.kw).run(t, *args, **kwargs)

            async def aio(self, task: Any, *args: Any, **kwargs: Any) -> Any:
                import flyte

                t = runner._sized(task, args, kwargs)
                return await flyte.with_runcontext(**runner.kw).run.aio(t, *args, **kwargs)

        self.run = _Run()

    def _sized(self, task: Any, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Any:
        if self.kw.get("mode") == "local":
            return task  # local runs are inert (flyte.md §4.5)
        t, d = size_root(task, self.stack, _tenant_from(self.kw), args, kwargs)
        self.last_decision = d
        return t


def with_runcontext(rightsize: Any = None, **kw: Any) -> Runner:
    """``flyte.with_runcontext`` plus the run's rightsizer policy; ``.run`` sizes the root."""
    return Runner(rightsize, **kw)


__all__ = ["Runner", "size_root", "with_runcontext"]
