"""``RightsizedTask`` call path against a fake controller (flyte.md §4–5, §8–9)."""

from __future__ import annotations

import asyncio
import json

import cloudpickle
import flyte
import flyte.errors as fe
import pytest

from rightsizer_runtime.wire import Mode

import rightsizer as rs
import rightsizer.flyte as rf
from rightsizer.flyte import _engine as eng_mod

GiB = 1 << 30
MiB = 1 << 20

env = flyte.TaskEnvironment(name="rsz_unit", resources=flyte.Resources(cpu=2, memory=("4Gi", "4Gi")))


@env.task
async def plain(x: int) -> int:
    return x


@rf.rightsize
@env.task
async def leaf(x: int) -> int:
    return x


@rf.rightsize(on_oom="auto")
@env.task(retries=1)
async def leaf_retries(x: int) -> int:
    return x


@rf.rightsize
@env.task
def sync_leaf(x: int) -> int:
    return x


renv = rf.RightsizedEnvironment(name="rsz_unit_env", resources=flyte.Resources(cpu=1, memory="2Gi"),
                                rightsize="enforce", auto_image=False)


@renv.task
async def env_leaf(x: int) -> int:
    return x


reusable_env = rf.RightsizedEnvironment(name="rsz_unit_reuse", resources=flyte.Resources(cpu=1, memory="1Gi"),
                                        reusable=flyte.ReusePolicy(replicas=1), auto_image=False)


@reusable_env.task
async def reused(x: int) -> int:
    return x


def run(coro):
    return asyncio.run(coro)


# ── definition-time behaviour ───────────────────────────────────────────


def test_registry_replacement_and_class_cache():
    assert env.tasks["rsz_unit.leaf"] is leaf and rf.is_rightsized(leaf)
    assert renv.tasks["rsz_unit_env.env_leaf"] is env_leaf and rf.is_rightsized(env_leaf)
    assert type(leaf) is type(env_leaf) is rf.rightsized_class(type(plain)), "one subclass per template class"
    assert env.tasks["rsz_unit.plain"] is plain and not rf.is_rightsized(plain)
    assert leaf._rs_code is not None and leaf._rs_code.kind == "source" and "async def leaf" in leaf._rs_code.text


def test_rightsize_must_be_outermost():
    with pytest.raises(TypeError, match="outermost"):
        rf.rightsize(lambda x: x)


def test_environment_fields_and_clone_with():
    assert env_leaf._rs_policy.default_mode is Mode.ENFORCE
    c = renv.clone_with("rsz_unit_env2")
    assert isinstance(c, rf.RightsizedEnvironment) and c.rightsize == "enforce" and c.auto_image is False
    assert c._rs_definition.default_mode is Mode.ENFORCE and c.tasks == {}


def test_environment_injects_rightsizer_config(monkeypatch):
    monkeypatch.setenv("RIGHTSIZER__HISTORY__URI", "s3://bucket/rs")
    e = rf.RightsizedEnvironment(name="rsz_inject", env_vars={"A": "1"}, auto_image=False)
    assert e.env_vars == {"RIGHTSIZER__HISTORY__URI": "s3://bucket/rs", "A": "1"}


def test_auto_image_adds_rightsizer_once(tmp_path):
    e = rf.RightsizedEnvironment(name="rsz_img", image=flyte.Image.from_debian_base(python_version=(3, 12)))
    pkgs = [p for l in e.image._layers for p in (getattr(l, "packages", None) or ())]
    assert any(p.startswith("rightsizer==") for p in pkgs)
    e2 = e.clone_with("rsz_img2")
    assert sum(p.startswith("rightsizer==") for l in e2.image._layers for p in (getattr(l, "packages", None) or ())) == 1
    (tmp_path / "rightsizer-0.1-cp310-abi3-manylinux_2_17_aarch64.whl").write_bytes(b"")
    e3 = rf.RightsizedEnvironment(name="rsz_img3", image=flyte.Image.from_debian_base(python_version=(3, 12)),
                                  auto_image=str(tmp_path))
    assert any(getattr(l, "package_name", None) == "rightsizer" for l in e3.image._layers)
    with pytest.warns(UserWarning, match="string"):
        rf.RightsizedEnvironment(name="rsz_img4", image="ghcr.io/x/y:1")


def test_picklable_cloudpickle_round_trip():
    s = rf.sized(leaf, mode="enforce")
    bypass = s._replace(_rs_bypass=True)
    for t in (leaf, env_leaf, s, bypass, rf.sized(plain)):
        t2 = cloudpickle.loads(cloudpickle.dumps(t))
        assert type(t2) is type(t) and t2.name == t.name
        assert t2._rs_policy == t._rs_policy and t2._rs_invocation == t._rs_invocation
        assert t2._rs_bypass == t._rs_bypass and t2._rs_measured == t._rs_measured
        assert t2.parent_env is None  # as the SDK's own __getstate__ does
    # Plain pickle cannot pickle any Flyte task (the module attribute is the task, not its function);
    # the SDK cloudpickles pickled bundles.


def test_override_pins_axes():
    o = leaf.override(resources=flyte.Resources(memory=("3Gi", "3Gi")))
    assert rf.is_rightsized(o) and o._rs_pins == frozenset({"memory"})
    assert leaf._rs_pins == frozenset()


# ── call path ───────────────────────────────────────────────────────────


def test_no_task_context_runs_the_function(controller):
    # Outside a task context the SDK's aio returns the function's coroutine (it "stays out of the way").
    inner = run(leaf.aio(3))
    assert run(inner) == 3 and controller.submitted == []


def test_recommend_by_default_marks_but_does_not_change(engine, task_context, point_estimator):
    point_estimator(256 * MiB)
    with task_context() as c:
        assert run(leaf.aio(1)) == "ok"
    (s,) = c.submitted
    assert s.task is not leaf and s.task._rs_bypass
    assert s.task.resources == leaf.resources, "recommend changes nothing"
    d = s.decision
    assert d["mode"] == "recommend" and d["measured"] is True and d["key"] == "flyte://acme/proj/dev/rsz_unit.leaf"


def test_enforce_reduces_and_records(engine, task_context, point_estimator, store):
    point_estimator(256 * MiB)
    with task_context(custom_context=rf.run_context(mode="enforce")) as c:
        assert run(leaf.aio(1)) == "ok"
    (s,) = c.submitted
    assert s.decision["mode"] == "enforce"
    mem = s.task.resources.memory
    assert isinstance(mem, tuple) and mem[0] == mem[1], "a (request, limit) tuple keeps its ratio"
    from rightsizer import _units

    assert _units.memory_bytes(mem[0]) < 4 * GiB, s.task.resources
    assert json.loads(s.env["RIGHTSIZER_ATTEMPT"])["attempt"] == 1
    engine.flush()
    from rightsizer.flyte._store import Store

    recs = Store(store).runs("flyte://acme/proj/dev/rsz_unit.leaf")
    assert [r.status.value for r in recs] == ["succeeded"] and recs[0].decision_id == s.decision["decision_id"]


def test_sized_and_policy_blocks_with_eager_capture_across_gather(engine, task_context, point_estimator):
    point_estimator(256 * MiB)

    async def driver():
        with rf.policy(mode="observe"):
            inside = [leaf.aio(i) for i in range(3)]          # created in the block ...
            mapped = [rf.sized(plain, mode="enforce").aio(9)]
        outside = [leaf.aio(10)]
        return await asyncio.gather(*inside, *mapped, *outside)  # ... awaited after it exits

    with task_context() as c:
        run(driver())
    modes = {s.args[0]: s.decision["mode"] for s in c.submitted}
    assert modes == {0: "observe", 1: "observe", 2: "observe", 9: "enforce", 10: "recommend"}
    # Descendants of calls made in the block inherit it through custom_context.
    child_cc = {s.args[0]: s.custom_context.get("rightsizer") for s in c.submitted}
    assert json.loads(child_cc[0])["s"] == [{"mode": "observe"}]
    assert child_cc[10] is None
    sized_plain = next(s for s in c.submitted if s.args[0] == 9)
    assert sized_plain.decision["measured"] is False, "an undecorated child runs the plain template"


def test_pkl_bundle_children_are_measured(engine, task_context, point_estimator):
    point_estimator(256 * MiB)
    with task_context(pkl=True) as c:
        run(rf.sized(plain).aio(1))
    assert c.submitted[0].decision["measured"] is True


def test_pins_via_override_keep_the_axis(engine, task_context, point_estimator):
    point_estimator(256 * MiB)
    pinned = leaf.override(resources=flyte.Resources(cpu=2, memory=("3Gi", "3Gi")))
    with task_context(custom_context=rf.run_context(mode="enforce")) as c:
        run(pinned.aio(1))
    assert c.submitted[0].task.resources.memory == ("3Gi", "3Gi"), "pinned memory is never changed"


def test_sync_task_uses_the_blocking_path(engine, task_context, point_estimator):
    point_estimator(256 * MiB)
    with task_context(custom_context=rf.run_context(mode="enforce")) as c:
        assert sync_leaf(5) == "ok"
    assert c.submitted[0].decision["mode"] == "enforce"


def test_reusable_is_observe_only(engine, task_context, point_estimator):
    point_estimator(256 * MiB)
    with task_context(custom_context=rf.run_context(mode="enforce")) as c:
        run(reused.aio(1))
    (s,) = c.submitted
    assert s.task is reused and "RIGHTSIZER_DECISION" not in s.env, "reusable tasks are submitted unchanged"


def test_local_mode_is_inert(engine, task_context, point_estimator):
    est = point_estimator(256 * MiB)
    with task_context(mode="local", custom_context=rf.run_context(mode="enforce")) as c:
        run(leaf.aio(1))
    assert c.submitted[0].task is leaf and est.calls == 0


def test_off_delegates_without_marker(engine, task_context):
    t = rf.sized(leaf, mode="off")
    with task_context() as c:
        run(t.aio(1))
    assert "RIGHTSIZER_DECISION" not in c.submitted[0].env


def test_missing_core_falls_back_to_the_prior(task_context, monkeypatch):
    from rightsizer import _native

    monkeypatch.setattr(_native, "_core", None)
    monkeypatch.setattr(_native, "_error", ImportError("simulated: no rightsizer._core in this image"))
    eng_mod.reset_for_tests()
    with task_context(custom_context=rf.run_context(mode="enforce")) as c:
        assert run(leaf.aio(1)) == "ok"
    (s,) = c.submitted
    assert s.task.resources == leaf.resources
    assert s.decision["mode"] == "recommend", "S1: the prior, demoted"
    assert eng_mod.counters["fallbacks"] >= 1


def test_user_errors_pass_through_unchanged(engine, task_context, point_estimator):
    point_estimator(256 * MiB)
    err = fe.RuntimeUserError(code="ValueError", message="boom")

    def behave(s):
        raise err

    with task_context() as c:
        c.behavior = behave
        with pytest.raises(fe.RuntimeUserError) as ei:
            run(leaf.aio(1))
    assert ei.value is err and len(c.submitted) == 1


# ── §9 escalation ───────────────────────────────────────────────────────


def _oom_below(need: int):
    from rightsizer import _units

    def behave(s):
        mem = s.task.resources.memory
        req = _units.memory_bytes(mem[0] if isinstance(mem, tuple) else mem)
        if req < need:
            raise fe.OOMError(code="OOMKilled", message="Reason [OOMKilled]")
        return "ok"

    return behave


def test_forced_oom_escalates_and_records_both_attempts(engine, task_context, point_estimator, store):
    point_estimator(256 * MiB)  # far below the real need
    with task_context(custom_context=rf.run_context(mode="enforce")) as c:
        c.behavior = _oom_below(3 * GiB)  # the trust region allows 4Gi -> 2Gi; the task needs 3Gi
        assert run(leaf.aio(1)) == "ok"
    from rightsizer import _units

    mems = [_units.memory_bytes(s.task.resources.memory[0]) for s in c.submitted]
    print("escalation ladder (bytes):", mems)
    assert len(mems) >= 2 and mems[1] >= 2 * mems[0] - MiB, mems
    atts = [json.loads(s.env["RIGHTSIZER_ATTEMPT"]) for s in c.submitted]
    assert [a["attempt"] for a in atts] == list(range(1, len(mems) + 1))
    assert len({a["invocation_id"] for a in atts}) == 1
    engine.flush()
    from rightsizer.flyte._store import Store

    recs = Store(store).runs("flyte://acme/proj/dev/rsz_unit.leaf")
    by_attempt = sorted((r.attempt, r.status.value) for r in recs)
    assert by_attempt[0] == (1, "oom_killed") and by_attempt[-1][1] == "succeeded"
    assert not [b for b in engine.breaker_status() if b.get("open")], "a recovered ladder trips no breaker"


def test_retries_disable_auto_escalation(engine, task_context, point_estimator):
    point_estimator(256 * MiB)
    with task_context(custom_context=rf.run_context(mode="enforce")) as c:
        c.behavior = _oom_below(3 * GiB)
        try:
            run(leaf_retries.aio(1))
        except fe.OOMError:
            pass
    assert len(c.submitted) == 1, "retries > 0: the backend retries at the same size; no ladder"
    assert "RIGHTSIZER_ATTEMPT" not in c.submitted[0].env
    # Without a ladder (and no S3(b) evidence) the reduction is not enforced.
    assert c.submitted[0].task.resources.memory == ("4Gi", "4Gi")


# ── run level ───────────────────────────────────────────────────────────


def test_with_runcontext_sizes_the_root_and_merges_custom_context(engine, monkeypatch, point_estimator):
    point_estimator(256 * MiB)
    seen = {}

    class FakeRunner:
        def __init__(self, **kw):
            seen["kw"] = kw

        def run(self, task, *a, **k):
            seen["task"] = task
            return "run"

    monkeypatch.setattr(flyte, "with_runcontext", lambda **kw: FakeRunner(**kw))
    r = rf.with_runcontext(rightsize="enforce", project="proj", domain="dev", custom_context={"x": "1"})
    assert r.run(leaf, 1) == "run"
    assert seen["kw"]["custom_context"]["x"] == "1"
    assert json.loads(seen["kw"]["custom_context"]["rightsizer"]) == {"v": 1, "s": [{"mode": "enforce"}]}
    t = seen["task"]
    d = json.loads(t.env_vars["RIGHTSIZER_DECISION"])
    assert d["key"].endswith("/proj/dev/rsz_unit.leaf") and d["mode"] == "enforce"
    assert r.last_decision is not None
    assert "RIGHTSIZER_ATTEMPT" not in (t.env_vars or {}), "roots have no ladder"


def test_cli_run_passes_the_policy_only(capsys):
    from rightsizer.flyte.cli import main

    assert main(["run", "--mode", "enforce", "--print", "--config", "c.yaml", "--", "wf.py", "main", "--n", "3"]) == 0
    out = capsys.readouterr().out.split()
    assert out[1:4] == ["--config", "c.yaml", "run"]
    assert out[4:6] == ["--custom-context", 'rightsizer={"v":1,"s":[{"mode":"enforce"}]}']
    assert out[6:] == ["wf.py", "main", "--n", "3"]
