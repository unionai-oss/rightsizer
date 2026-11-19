"""Fixtures for ``rightsizer.flyte`` unit tests: a fake remote task context and
controller (the SDK's private ``_ControllerState``), and a real engine on a
local store marked durable + shared so ``enforce`` is not demoted."""

from __future__ import annotations

import asyncio
import concurrent.futures
import os
import sys
from typing import Any, Callable, Dict, List, Optional

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.abspath(os.path.join(HERE, "..", "..", "src"))
if SRC not in sys.path:
    sys.path.insert(0, SRC)

flyte = pytest.importorskip("flyte")

GiB = 1 << 30
MiB = 1 << 20


class Submission:
    def __init__(self, task: Any, args: tuple, kwargs: dict, custom_context: Dict[str, str]):
        self.task = task
        self.args = args
        self.kwargs = kwargs
        self.custom_context = custom_context

    @property
    def env(self) -> Dict[str, str]:
        return dict(self.task.env_vars or {})

    @property
    def decision(self) -> Optional[Dict[str, Any]]:
        import json

        raw = self.env.get("RIGHTSIZER_DECISION")
        return json.loads(raw) if raw else None


class FakeController:
    """Records submissions; ``behavior(submission)`` returns the result or raises."""

    def __init__(self) -> None:
        self.submitted: List[Submission] = []
        self.behavior: Callable[[Submission], Any] = lambda s: "ok"

    def _record(self, task: Any, args: tuple, kwargs: dict) -> Submission:
        from flyte._context import internal_ctx

        tctx = internal_ctx().data.task_context
        s = Submission(task, args, kwargs, dict(tctx.custom_context or {}) if tctx else {})
        self.submitted.append(s)
        return s

    async def submit(self, task: Any, *args: Any, **kwargs: Any) -> Any:
        s = self._record(task, args, kwargs)
        await asyncio.sleep(0)
        return self.behavior(s)

    def submit_sync(self, task: Any, *args: Any, **kwargs: Any) -> concurrent.futures.Future:
        f: concurrent.futures.Future = concurrent.futures.Future()
        s = self._record(task, args, kwargs)
        try:
            f.set_result(self.behavior(s))
        except BaseException as e:
            f.set_exception(e)
        return f


@pytest.fixture(autouse=True)
def _isolated(monkeypatch, tmp_path):
    for k in list(os.environ):
        if k.startswith("RIGHTSIZER_") or k.startswith("FLYTE_AWS"):
            monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    monkeypatch.chdir(tmp_path)
    yield
    from rightsizer.flyte import _engine as eng

    eng.reset_for_tests()
    try:
        import rightsizer as rs

        rs.clear_estimators()
    except Exception:
        pass


@pytest.fixture
def store(tmp_path) -> str:
    return str(tmp_path / "store")


@pytest.fixture
def engine(store):
    pytest.importorskip("rightsizer._core")
    import rightsizer as rs
    from rightsizer.flyte import _engine as eng

    cfg = {
        "history": {"uri": store, "durable": True, "shared": True},
        "flyte": {"shapes": [{"name": "node", "cpu": 16000, "memory": 64 * GiB, "price_per_hour": 0.8}]},
    }
    e = rs.Engine(cfg, env_overrides=False)
    eng.reset_for_tests(engine=e)
    eng._state["store"] = store
    return e


def _tctx(tmp_path: Any, mode: str = "remote", custom_context: Optional[Dict[str, str]] = None, pkl: bool = False) -> Any:
    from flyte.models import ActionID, CodeBundle, RawDataPath, TaskContext
    from flyte.report import Report

    kw: Dict[str, Any] = {}
    if pkl:
        kw["code_bundle"] = CodeBundle(computed_version="v", destination=".", pkl="s3://b/x.pkl")
    return TaskContext(
        action=ActionID(name="a0", run_name="r0", project="proj", domain="dev", org="acme"),
        version="v1",
        raw_data_path=RawDataPath(path=str(tmp_path / "raw")),
        output_path=str(tmp_path / "out"),
        run_base_dir=str(tmp_path / "base"),
        report=Report(name="a0"),
        mode=mode,
        custom_context=dict(custom_context or {}),
        **kw,
    )


@pytest.fixture
def controller(monkeypatch):
    import flyte._internal.controllers as ctrls

    fake = FakeController()
    monkeypatch.setattr(ctrls._ControllerState, "controller", fake)
    return fake


@pytest.fixture
def task_context(tmp_path, controller):
    """A factory: ``with task_context(mode=..., custom_context=...)``."""
    import contextlib

    from flyte._context import internal_ctx

    @contextlib.contextmanager
    def make(mode: str = "remote", custom_context: Optional[Dict[str, str]] = None, pkl: bool = False):
        with internal_ctx().replace_task_context(_tctx(tmp_path, mode, custom_context, pkl)):
            yield controller

    return make


class PointEstimator:
    """A Python estimator answering a fixed memory demand (bytes)."""

    def __init__(self, demand: int, cpu_cores: float = 0.5):
        import rightsizer as rs

        outer = self

        class _E(rs.Estimator):
            id = "point-test"
            version = "1"

            def estimate(self, ctx):
                outer.calls += 1
                return {"memory": {"kind": "point", "value": float(outer.demand)},
                        "cpu_mean": {"kind": "point", "value": float(outer.cpu)}}

        self.demand = demand
        self.cpu = cpu_cores
        self.calls = 0
        self.estimator = _E()
        rs.register_estimator(self.estimator)


@pytest.fixture
def point_estimator():
    return PointEstimator
