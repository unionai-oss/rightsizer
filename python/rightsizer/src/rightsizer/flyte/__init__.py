"""rightsizer for Flyte v2 (flyte.md). Extra: ``pip install 'rightsizer[flyte]'``.

::

    import flyte, rightsizer as rs

    env = rs.flyte.RightsizedEnvironment(name="train", resources=flyte.Resources(cpu=8, memory="64Gi"))

    @env.task
    async def finetune(base_model: str, data: flyte.io.DataFrame) -> flyte.io.Dir: ...

    rs.flyte.with_runcontext(rightsize="enforce").run(main, cfg)

Levels (flyte.md §2): task (:func:`rightsize`), environment
(:class:`RightsizedEnvironment`), action (:func:`sized`, :class:`policy`),
run (:func:`with_runcontext`, :func:`run_context`, ``rsz flyte run``);
deploy-time sizing (:func:`deploy`, ``rsz flyte deploy`` / ``reconcile``);
backfill (``rsz history import flyte``).

Importing this package imports Flyte; ``import rightsizer`` does not. The
native module is imported at the first decision; without it every call runs
at the prior (S1).
"""

from __future__ import annotations

try:
    import flyte as _flyte  # noqa: F401
except ImportError as e:  # pragma: no cover - exercised by the isolation test
    raise ImportError(
        "rightsizer.flyte needs Flyte (flyte>=2.13,<2.14). Install the extra: pip install 'rightsizer[flyte]'"
    ) from e

from rightsizer_runtime.policy import InvocationPolicy

from ._classify import classify
from ._deploy import deploy, reconcile, reconcile_once
from ._env import RightsizedEnvironment
from ._policy import DefinitionPolicy, captured_stack, policy, run_context
from ._run import with_runcontext
from ._task import RightsizedMixin as RightsizedTask
from ._task import is_rightsized, rightsize, rightsized_class, sized

__all__ = [
    "DefinitionPolicy",
    "InvocationPolicy",
    "RightsizedEnvironment",
    "RightsizedTask",
    "captured_stack",
    "classify",
    "deploy",
    "is_rightsized",
    "policy",
    "reconcile",
    "reconcile_once",
    "rightsize",
    "rightsized_class",
    "run_context",
    "sized",
    "with_runcontext",
]
