"""Adversarial review: the Flyte call path must never break or change a user's task (S1)."""

from __future__ import annotations

import asyncio
import json
import pickle

import cloudpickle
import flyte
import flyte.errors as fe
import pytest

import rightsizer.flyte as rf
from rightsizer.flyte import _engine as eng_mod

GiB = 1 << 30
MiB = 1 << 20

aenv = flyte.TaskEnvironment(name="rsz_adv", resources=flyte.Resources(cpu=2, memory="4Gi"))


@rf.rightsize
@aenv.task
def adv_sync(x: int) -> int:
    return x


@rf.rightsize
@aenv.task
async def adv_varargs(a: int, b: int = 2, *, flag: bool = False, extra: int = 0) -> int:
    return a


def run(coro):
    return asyncio.run(coro)


def _container_task():
    from flyte.extras import ContainerTask

    ct = ContainerTask(name="adv_ct", image="alpine:3", command=["echo", "{{.inputs.x}}"], inputs={"x": int},
                       resources=flyte.Resources(cpu=1, memory="1Gi"))
    ct.parent_env_name = "rsz_adv"
    return ct


def test_sized_container_task_keeps_its_private_state():
    """`ContainerTask.__init__` takes command/arguments, not its dataclass fields. Building the
    RightsizedTask through the generated dataclass `__init__` dropped `_cmd`/`_args`/..., so the
    controller's serialization of the submitted task raised AttributeError in user code."""
    ct = _container_task()
    s = rf.sized(ct, mode="enforce")
    for attr in ("_cmd", "_args", "_inputs", "_outputs", "_input_data_dir", "_output_data_dir"):
        assert getattr(s, attr) == getattr(ct, attr), attr
    assert s.container_args(None) == ct.container_args(None)
    # The sized copy (what the controller serializes) too.
    from rightsizer.types import Decision

    copy = s._replace(env_vars={"A": "1"}, _rs_bypass=True)
    assert copy._cmd == ct._cmd and copy._rs_bypass
    # And through cloudpickle (pickled code bundles).
    back = cloudpickle.loads(cloudpickle.dumps(s))
    assert back._cmd == ct._cmd and rf.is_rightsized(back)


def test_sized_container_task_submits(engine, task_context, point_estimator):
    point_estimator(256 * MiB)
    s = rf.sized(_container_task(), mode="enforce")
    with task_context() as c:
        assert run(s.aio(x=1)) == "ok"
    (sub,) = c.submitted
    assert sub.task._cmd == ["echo", "{{.inputs.x}}"]
    from flyte._internal.runtime.task_serde import translate_task_to_wire
    from flyte.models import SerializationContext

    spec = translate_task_to_wire(sub.task, SerializationContext(version="v1", image_cache=None))
    env = {kv.key: kv.value for kv in spec.task_template.container.env}
    assert "RIGHTSIZER_DECISION" in env and list(spec.task_template.container.args)[:1] == ["echo"]


def test_sync_task_called_blocking_from_async_code_is_unchanged(engine, task_context, point_estimator):
    """The SDK refuses a blocking call of a sync task from async code. Rightsizer must raise the
    same error, without a decision made on (and blocking) the event loop or a Failed outcome."""
    est = point_estimator(256 * MiB)

    async def parent():
        return adv_sync(1)

    with task_context() as c:
        with pytest.raises(fe.SyncTaskCallInAsyncContextError):
            run(parent())
    assert c.submitted == []
    assert est.calls == 0 and eng_mod.counters["decisions"] == 0


def test_varargs_and_kwonly_tasks_submit_unchanged_arguments(engine, task_context, point_estimator):
    point_estimator(256 * MiB)
    with task_context() as c:
        run(adv_varargs.aio(1, 2, flag=True, extra=3))
    (s,) = c.submitted
    assert s.args == (1, 2) and s.kwargs == {"flag": True, "extra": 3}
    assert s.task.func is adv_varargs.func


def test_weird_scalar_inputs_do_not_disable_sizing(engine, task_context, point_estimator):
    """A NaN, an infinity or a big integer argument is just not an input feature; it must
    not turn the whole decision into an error fallback."""
    point_estimator(256 * MiB)

    @rf.rightsize
    @aenv.task
    async def adv_scalars(a: float, b: int) -> int:
        return 0

    with task_context(custom_context=rf.run_context(mode="enforce")) as c:
        run(adv_scalars.aio(float("nan"), 1 << 70))
        run(adv_scalars.aio(float("inf"), -(1 << 70)))
    assert eng_mod.counters["fallbacks"] == 0
    for s in c.submitted:
        assert s.decision["mode"] == "enforce"


def test_engine_panic_like_base_exception_is_contained(task_context, monkeypatch):
    """pyo3's PanicException derives from BaseException: `except Exception` does not stop it."""

    class PanicLike(BaseException):
        pass

    class Boom:
        config = {}

        def decide(self, *a, **k):
            raise PanicLike("rust panicked")

        async def adecide(self, *a, **k):
            raise PanicLike("rust panicked")

        def outcome(self, *a, **k):
            raise PanicLike("rust panicked")

        async def aoutcome(self, *a, **k):
            raise PanicLike("rust panicked")

    eng_mod.reset_for_tests(engine=Boom())
    with task_context(custom_context=rf.run_context(mode="enforce")) as c:
        assert run(rf.sized(adv_varargs).aio(1)) == "ok"
        assert adv_sync(1) == "ok"
    assert len(c.submitted) == 2


def test_child_error_with_broken_str_propagates_itself(engine, task_context, point_estimator):
    """`classify` reads `str(exc)`; an exception whose `__str__` raises must still be the one the user sees."""
    point_estimator(256 * MiB)

    class Weird(Exception):
        def __str__(self):
            raise RuntimeError("no str for you")

    err = Weird()

    def behave(s):
        raise err

    with task_context() as c:
        c.behavior = behave
        with pytest.raises(Weird) as ei:
            run(rf.sized(adv_varargs).aio(1))
    assert ei.value is err


calls = {"n": 0}


@rf.rightsize
@aenv.task
def adv_sync_fails(x: int) -> int:
    calls["n"] += 1
    raise ValueError("user bug")


def test_failing_sync_task_runs_once_outside_a_task_context():
    """`__call__` delegated inside its S1 try block: the task's own ValueError was caught as a
    'rightsizer error' and the task was run a second time."""
    calls["n"] = 0
    with pytest.raises(Exception, match="user bug"):  # the SDK wraps it as RuntimeUserError
        adv_sync_fails(1)
    assert calls["n"] == 1


def test_failing_sync_child_is_submitted_once(engine, task_context, point_estimator):
    point_estimator(256 * MiB)
    err = fe.RuntimeUserError(code="ValueError", message="boom")

    def behave(s):
        raise err

    with task_context() as c:
        c.behavior = behave
        with pytest.raises(fe.RuntimeUserError):
            adv_sync(1)
    assert len(c.submitted) == 1, [s.task._rs_bypass for s in c.submitted]


def test_deploy_lets_flyte_version_code_changes_and_records_reported_versions(engine, monkeypatch, point_estimator, store):
    """`rs.flyte.deploy` used to pass `version="rsz-<digest of the sizing>"`: a code change with
    the same sizing got the previous deploy's version (stale code or an AlreadyExists failure)."""
    from types import SimpleNamespace

    from rightsizer.flyte import _deploy as dep
    from rightsizer.flyte._store import Store

    denv = flyte.TaskEnvironment(name="rsz_adv_dep", resources=flyte.Resources(cpu=1, memory="1Gi"))

    @rf.rightsize
    @denv.task
    async def grow2(x: int) -> int:
        return x

    point_estimator(3 * GiB)
    seen = {}

    def fake_deploy(*envs, version=None, dry_run=False, **kw):
        seen["version"] = version
        tid = SimpleNamespace(name="rsz_adv_dep.grow2", version="flyte-derived-123")
        dt = SimpleNamespace(deployed_task=SimpleNamespace(task_template=SimpleNamespace(id=tid)))
        return [SimpleNamespace(envs={"rsz_adv_dep": SimpleNamespace(deployed_entities=[dt])})]

    monkeypatch.setattr(flyte, "deploy", fake_deploy)
    monkeypatch.setenv("RIGHTSIZER__HISTORY__URI", store)
    flyte.init(project="proj", domain="dev", org="acme")
    rf.deploy(denv)
    assert seen["version"] is None
    (rec,) = [r for r in dep.deploy_records(Store(store)) if r["task"] == "rsz_adv_dep.grow2"]
    assert rec["version"] == "flyte-derived-123"


def test_deploy_without_a_store_deploys_from_source(monkeypatch):
    from rightsizer.flyte import _deploy as dep

    denv = flyte.TaskEnvironment(name="rsz_adv_dep2", resources=flyte.Resources(cpu=1, memory="1Gi"))

    @rf.rightsize
    @denv.task
    async def t2(x: int) -> int:
        return x

    seen = []
    monkeypatch.setattr(flyte, "deploy", lambda *envs, **kw: seen.append(kw) or "ok")
    monkeypatch.setattr(dep, "_store", lambda: (_ for _ in ()).throw(RuntimeError("no history store")))
    assert rf.deploy(denv) == "ok" and len(seen) == 1


def test_deploy_record_path_cannot_escape_the_store():
    from rightsizer.flyte import _deploy as dep

    p = dep._deploy_record_path("flyte", "acme/proj/dev", "env.t", "../../../../etc/x")
    assert ".." not in p.split("/") and p.startswith("v1/flyte/deploys/flyte/acme/proj/dev/env.t/")
    assert dep._deploy_record_path("flyte", "a/b/c", "t", "..").endswith("/t/-.json")
