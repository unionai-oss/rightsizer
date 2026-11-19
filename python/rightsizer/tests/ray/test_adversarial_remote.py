"""Adversarial cases for ``rs.ray``: things that must behave exactly like Ray (S1)."""

from __future__ import annotations

import copy
import threading
import time

import cloudpickle
import pytest
import ray

import rightsizer.ray as rr

from rayfx import GiB, TENANT, seed

KEY = f"ray://{TENANT}/embed"
SHAPES = [{"cpu": 16000, "memory": 64 * GiB, "price_per_hour": 0.8}]


def _embed(x):
    return x


def _increase_engine(engine_factory):
    """History whose peaks sit just under a 1 GiB prior: enforce increases memory."""
    e = engine_factory(shapes=SHAPES)
    seed(e, KEY, [int(x * GiB) for x in (0.9, 0.95, 0.92, 0.97)], requested_mem=1 * GiB)
    return e


# ── async def ────────────────────────────────────────────────────────────


def test_async_def_is_rejected_at_definition_like_ray():
    async def af(x):
        return x

    with pytest.raises(ValueError, match="async def"):
        ray.remote(af)
    # A sync measurement wrapper used to hide the coroutine function from Ray:
    # the definition was accepted and every call returned an unpicklable coroutine.
    with pytest.raises(ValueError, match="async def"):
        rr.remote(num_cpus=1)(af)
    with pytest.raises(ValueError, match="async def"):
        rr.remote(af)


# ── .options(...) wrapper ────────────────────────────────────────────────


def test_options_wrapper_copies_and_pickles(engine_factory, captured):
    engine_factory(shapes=SHAPES)
    f = rr.remote(num_cpus=1, memory=4 * GiB, key="embed")(_embed)
    o = f.options(memory=6 * GiB)
    copy.copy(o)  # RecursionError before the fix (__getattr__ → self._inner → __getattr__ …)
    o2 = cloudpickle.loads(cloudpickle.dumps(o))
    assert o2._pins == o._pins
    o2.remote("x")
    assert captured[-1]["options"]["memory"] == 6 * GiB  # still pinned after the round trip
    with pytest.raises(AttributeError):
        o2.__no_such_dunder__  # noqa: B018


# ── placement groups ─────────────────────────────────────────────────────


def _pg_strategy():
    from ray._raylet import PlacementGroupID
    from ray.util.placement_group import PlacementGroup
    from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

    return PlacementGroupSchedulingStrategy(PlacementGroup(PlacementGroupID.from_random()), 0)


def test_no_increase_inside_an_explicit_placement_group(engine_factory, captured):
    _increase_engine(engine_factory)
    f = rr.remote(num_cpus=1, memory=1 * GiB, key="embed")(_embed)
    f.remote("x")
    assert captured[0]["options"]["memory"] > 1 * GiB  # the history really asks for more
    # A bundle is fixed: a larger request could never be scheduled (pending forever).
    f.options(scheduling_strategy=_pg_strategy()).remote("x")
    assert captured[1]["options"]["memory"] == 1 * GiB
    assert captured[1]["options"]["num_cpus"] <= 1  # reductions still fit the bundle


def test_no_increase_when_ray_captures_the_callers_placement_group(engine_factory, captured, monkeypatch):
    _increase_engine(engine_factory)
    from ray._raylet import PlacementGroupID

    w = ray._private.worker.global_worker
    pg_id = PlacementGroupID.from_random()
    monkeypatch.setattr(type(w), "placement_group_id", property(lambda self: pg_id))
    monkeypatch.setattr(type(w), "should_capture_child_tasks_in_placement_group", property(lambda self: True))
    f = rr.remote(num_cpus=1, memory=1 * GiB, key="embed")(_embed)
    f.remote("x")  # a child task of a task running in a capturing placement group
    assert captured[0]["options"]["memory"] == 1 * GiB
    # Explicitly opting out of the group (placement_group=None) sizes normally.
    f.options(placement_group=None).remote("x")
    assert captured[1]["options"]["memory"] > 1 * GiB


def test_placement_group_without_memory_prior_never_adds_memory(engine_factory, captured):
    from rightsizer.ray._sizing import _constraints

    c = _constraints(rr._sizing.SizingPolicy(), frozenset(), {"cpu": 1000}, "config", hold_increases=True)
    assert "memory" in c["pinned"] and c["ceiling"] == {"cpu": 1000}


# ── platform state refresh off the call path ─────────────────────────────


def test_expired_autoscaler_shapes_do_not_block_remote(monkeypatch, engine_factory):
    import rightsizer.ray._settings as st

    engine_factory()
    s = st.settings()
    calls = []
    release = threading.Event()

    def slow():
        calls.append(1)
        release.wait(5)
        return [{"name": "n", "cpu": 8000, "memory": 32 * GiB, "price_per_hour": 0.5}]

    monkeypatch.setattr(st, "autoscaler_shapes", slow)
    s._shapes = (time.monotonic() - 2 * st.AUTOSCALER_TTL_S, "autoscaler", [{"name": "old"}])
    t0 = time.monotonic()
    src, shapes = s.shapes()
    assert time.monotonic() - t0 < 0.5  # the stale value, without waiting for the GCS
    assert shapes == [{"name": "old"}] and src == "autoscaler"
    s.shapes()  # a second expired read starts no second refresh
    release.set()
    for _ in range(100):
        if s._shapes[2][0].get("name") == "n":
            break
        time.sleep(0.02)
    assert s.shapes()[1][0]["name"] == "n"
    assert len(calls) == 1


# ── the observer never reads plasma objects ──────────────────────────────


def test_observer_never_reads_objects_that_live_in_plasma(monkeypatch):
    import ray.experimental as rx

    from rightsizer.ray._observe import Observer

    ref = ray.ObjectRef.from_random()
    monkeypatch.setattr(rx, "get_local_object_locations",
                        lambda refs: {refs[0]: {"object_size": None, "node_ids": ["n1"], "did_spill": False}})

    class W:
        class core_worker:  # noqa: N801
            @staticmethod
            def get_objects(*a, **k):
                raise AssertionError("a plasma object was fetched")

    monkeypatch.setattr(ray._private.worker, "global_worker", W)
    assert Observer().peek(ref) == (None, "plasma")


# ── the hidden keyword only goes to functions that accept it ─────────────


def _ray_accepts(f, call):
    """Ray's own submission-time argument check (``flatten_args``) on what rightsizer sent."""
    from ray._common.signature import extract_signature, flatten_args

    flatten_args(extract_signature(f._function), list(call["args"] or ()), call["kwargs"])


def test_async_generator_gets_no_hidden_keyword(engine_factory, captured):
    engine_factory(shapes=SHAPES)

    async def agen(x):
        yield x

    f = rr.remote(num_cpus=1, memory=4 * GiB, key="embed")(agen)
    f.remote("x")
    assert "_rightsizer" not in captured[0]["kwargs"]
    _ray_accepts(f, captured[0])  # TypeError ("unexpected keyword argument") before the fix

    async def agen_kw(x, **kw):
        yield kw

    g = rr.remote(num_cpus=1, key="embed")(agen_kw)
    g.remote("x")
    assert captured[1]["kwargs"] == {}  # the user's **kwargs never see rightsizer's payload


def test_user_parameter_named_like_the_hidden_keyword_is_untouched(engine_factory, captured):
    engine_factory(shapes=SHAPES)

    def f(x, _rightsizer=5):
        return _rightsizer

    rf = rr.remote(num_cpus=1, key="embed")(f)
    assert rf._function is f  # not wrapped: a wrapper would swallow the user's value
    rf.remote(1, _rightsizer=7)
    assert captured[0]["kwargs"] == {"_rightsizer": 7}
