"""``rs.ray.remote`` / ``rs.ray.rightsize``: what reaches Ray (ray.md §2, §7)."""

from __future__ import annotations

import inspect

import pytest
import ray

import rightsizer as rs
import rightsizer.ray as rr
from rightsizer.ray import RightsizedRemoteFunction

from rayfx import GiB, MiB, TENANT, seed

KEY = f"ray://{TENANT}/embed"


def _embed(shard_uri, model="bge"):
    return shard_uri


def test_is_a_ray_remote_function_and_keeps_ray_semantics():
    f = rr.remote(num_cpus=2, memory=4 * GiB, key="embed")(_embed)
    assert isinstance(f, RightsizedRemoteFunction)
    assert isinstance(f, ray.remote_function.RemoteFunction)
    assert f._default_options["num_cpus"] == 2 and f._default_options["memory"] == 4 * GiB
    with pytest.raises(TypeError, match="cannot be called directly"):
        f("x")
    # Ray's own option validation still applies.
    with pytest.raises((ValueError, TypeError)):
        rr.remote(num_cpus=-1)(_embed)
    # The function Ray ships keeps the user's name and signature (plus the hidden keyword).
    sig = inspect.signature(f._function)
    assert list(sig.parameters)[:2] == ["shard_uri", "model"] and "_rightsizer" in sig.parameters
    assert f._function.__name__ == "_embed"


def test_enforce_reduces_memory_with_trusted_evidence(engine_factory, captured):
    e = engine_factory(shapes=[{"name": "cpu-16", "cpu": 16000, "memory": 64 * GiB, "price_per_hour": 0.8}])
    seed(e, KEY, [300 * MiB, 320 * MiB, 310 * MiB, 305 * MiB])
    f = rr.remote(num_cpus=1, memory=4 * GiB, key="embed")(_embed)
    assert f.remote("x") == "ref-1"
    opts = captured[0]["options"]
    assert 320 * MiB < opts["memory"] < 4 * GiB, opts
    meta = captured[0]["kwargs"]["_rightsizer"]
    assert meta["k"] == KEY and meta["m"] == "enforce" and meta["r"]["requests"]["memory"] == opts["memory"]
    assert captured[0]["args"] == ("x",)


def test_without_evidence_memory_is_held_s3b_only(engine_factory, captured):
    # Unverified self-reports without the operator opt-in are not evidence (SPEC §8.2).
    e = engine_factory(shapes=[{"cpu": 16000, "memory": 64 * GiB, "price_per_hour": 0.8}])
    seed(e, KEY, [300 * MiB] * 4, trust="unverified")
    f = rr.remote(num_cpus=1, memory=4 * GiB, key="embed")(_embed)
    f.remote("x")
    assert captured[0]["options"]["memory"] == 4 * GiB
    d = f._rs_last_decision if hasattr(f, "_rs_last_decision") else None  # noqa: F841
    # With the opt-in the same history allows the reduction.
    e2 = engine_factory(allow_unverified=True, shapes=[{"cpu": 16000, "memory": 64 * GiB, "price_per_hour": 0.8}])
    seed(e2, KEY, [300 * MiB] * 4, trust="unverified")
    f.remote("x")
    assert captured[1]["options"]["memory"] < 4 * GiB


def test_no_history_holds_and_ray_memory_is_unenforced(engine_factory):
    e = engine_factory(shapes=[{"cpu": 16000, "memory": 64 * GiB, "price_per_hour": 0.8}])
    seed(e, KEY, [300 * MiB, 300 * MiB])  # < min_history (3)
    from rightsizer.ray._sizing import SizingPolicy, decide_options

    s = decide_options({"num_cpus": 1, "memory": 4 * GiB}, SizingPolicy(key="embed"), target=_embed)
    assert s.options["memory"] == 4 * GiB
    held = {h.axis: h.reason for h in s.decision.held}
    assert held.get("memory") == "not_enforced", s.decision.held


def test_options_axes_are_pins(engine_factory, captured):
    e = engine_factory(shapes=[{"cpu": 16000, "memory": 64 * GiB, "price_per_hour": 0.8}])
    seed(e, KEY, [300 * MiB] * 4)
    # memory 8Gi on a 16-core/64Gi shape: memory dominates the packing, so it is worth reducing
    f = rr.remote(num_cpus=1, memory=8 * GiB, key="embed")(_embed)
    f.options(memory=6 * GiB).remote("x")
    assert captured[0]["options"]["memory"] == 6 * GiB  # pinned: as given
    f.options(num_cpus=1).remote("x")
    assert captured[1]["options"]["num_cpus"] == 1  # cpu pinned …
    assert captured[1]["options"]["memory"] < 8 * GiB  # … memory still rightsizable
    f.remote("x")  # pins do not leak into later calls
    assert captured[2]["options"]["memory"] < 8 * GiB


def test_resources_and_max_retries_never_touched(engine_factory, captured):
    e = engine_factory(shapes=[{"cpu": 16000, "memory": 64 * GiB, "price_per_hour": 0.8}])
    seed(e, KEY, [300 * MiB] * 4)
    res = {"custom_accel": 2.0}
    f = rr.remote(num_cpus=1, memory=4 * GiB, key="embed", resources=res, max_retries=5)(_embed)
    f.remote("x")
    o = captured[0]["options"]
    assert o["resources"] == {"custom_accel": 2.0} and o["max_retries"] == 5
    assert o["memory"] < 4 * GiB
    g = rr.remote(num_cpus=1, memory=4 * GiB, key="embed")(_embed)
    g.remote("x")
    assert "max_retries" not in captured[1]["options"]  # Ray's default stays Ray's
    g.options(max_retries=0, resources={"r": 1}).remote("x")
    assert captured[2]["options"]["max_retries"] == 0 and captured[2]["options"]["resources"] == {"r": 1}


def test_num_gpus_never_changes(engine_factory, captured):
    e = engine_factory(shapes=[{"cpu": 16000, "memory": 64 * GiB, "gpu": {"accel": "L4-24G", "count": 4},
                                "price_per_hour": 3.0}])
    seed(e, KEY, [300 * MiB] * 4)
    f = rr.remote(num_cpus=1, num_gpus=2, accelerator_type="L4", memory=4 * GiB, key="embed")(_embed)
    f.remote("x")
    o = captured[0]["options"]
    assert o["num_gpus"] == 2 and o["accelerator_type"] == "L4"


def test_no_platform_state_clamps_increases(engine_factory, captured):
    e = engine_factory()  # no autoscaler (no Ray), no [ray] shapes
    seed(e, KEY, [int(x * GiB) for x in (0.9, 0.95, 0.92, 0.97)], requested_mem=1 * GiB)
    f = rr.remote(num_cpus=1, memory=1 * GiB, key="embed")(_embed)
    f.remote("x")
    assert captured[0]["options"]["memory"] == 1 * GiB  # the increase is held at the prior (no definition ceiling)
    assert 'rightsizer_clamped_total{reason="no_platform_state"} 1' in e.metrics()
    # A definition ceiling lets the increase go up to it.
    g = rr.remote(num_cpus=1, memory=1 * GiB, key="embed", ceiling={"memory": 4 * GiB})(_embed)
    g.remote("x")
    assert 1 * GiB < captured[1]["options"]["memory"] <= 4 * GiB


def test_recommend_mode_changes_nothing(engine_factory, captured):
    e = engine_factory(mode="recommend", shapes=[{"cpu": 16000, "memory": 64 * GiB, "price_per_hour": 0.8}])
    seed(e, KEY, [300 * MiB] * 4)
    f = rr.remote(num_cpus=1, memory=4 * GiB, key="embed")(_embed)
    f.remote("x")
    o = captured[0]["options"]
    assert o["memory"] == 4 * GiB and o["num_cpus"] == 1
    assert captured[0]["kwargs"]["_rightsizer"]["m"] == "recommend"


def test_rightsize_rebuilds_from_the_underlying_function(engine_factory, captured):
    e = engine_factory(shapes=[{"cpu": 16000, "memory": 64 * GiB, "price_per_hour": 0.8}])
    seed(e, f"ray://{TENANT}/tok", [300 * MiB] * 4)

    def tokenize_fn(x):
        return x

    plain = ray.remote(num_cpus=1, memory=4 * GiB, max_retries=2)(tokenize_fn)
    tok = rr.rightsize(plain, key="tok")
    assert isinstance(tok, RightsizedRemoteFunction) and tok is not plain
    assert tok._function is tokenize_fn  # sizing only: no measurement wrapper
    assert tok.remote is not plain.remote
    tok.remote(1)
    o = captured[0]["options"]
    assert o["memory"] < 4 * GiB and o["max_retries"] == 2
    assert "_rightsizer" not in captured[0]["kwargs"]
    with pytest.raises(TypeError):
        rr.rightsize(tokenize_fn)


def test_bind_is_not_sized(engine_factory, captured):
    engine_factory()
    f = rr.remote(num_cpus=1, memory=4 * GiB, key="embed")(_embed)
    node = f.options(num_cpus=2).bind("x")
    assert type(node).__name__ == "FunctionNode"
    assert captured == []


def test_key_derivation(engine_factory, captured):
    engine_factory()
    from rightsizer.ray._sizing import default_suffix

    assert default_suffix(_embed) == f"{__name__}._embed"
    f = rr.remote(num_cpus=1)(_embed)
    f.remote("x")
    assert captured[0]["kwargs"]["_rightsizer"]["k"] == f"ray://{TENANT}/{__name__}._embed"
    g = rr.remote(num_cpus=1, key="custom.suffix")(_embed)
    g.remote("x")
    assert captured[1]["kwargs"]["_rightsizer"]["k"] == f"ray://{TENANT}/custom.suffix"


def test_tenant_resolution(monkeypatch, core, store):
    from rightsizer.ray import _settings as st

    assert st.kuberay_tenant({"RAY_CLUSTER_NAMESPACE": "ml-team", "RAY_CLUSTER_NAME": "rc1"}) == "ml-team/rc1"
    assert st.kuberay_tenant({"POD_NAMESPACE": "ml"}) == "ml"
    assert st.kuberay_tenant({}) is None
    # Config wins over KubeRay; without either: RIGHTSIZER_TENANT (conftest: alice).
    monkeypatch.setenv("RAY_CLUSTER_NAMESPACE", "ns1")
    e = rs.Engine({"history": {"uri": store}}, env_overrides=False)
    rr.configure(engine=e)
    assert rr.settings().tenant == "ns1"
    e = rs.Engine({"history": {"uri": store}, "ray": {"tenant": "team-z/c9"}}, env_overrides=False)
    rr.configure(engine=e)
    assert rr.settings().tenant == "team-z/c9"
    monkeypatch.delenv("RAY_CLUSTER_NAMESPACE")
    rr.configure(engine=rs.Engine({"history": {"uri": store}}, env_overrides=False))
    assert rr.settings().tenant == "alice"


def test_trust_follows_tenant_scoped_credentials(core, store):
    rr.configure(engine=rs.Engine({"history": {"uri": store}}, env_overrides=False))
    assert rr.settings().trust == "unverified"
    rr.configure(engine=rs.Engine({"history": {"uri": store}, "ray": {"tenant_scoped_credentials": True}}, env_overrides=False))
    assert rr.settings().trust == "authenticated"


def test_object_ref_sizes_are_guarded(monkeypatch):
    from rightsizer.ray import _sizing

    class FakeRef(ray.ObjectRef):
        pass

    ref = ray.ObjectRef.from_random()
    import ray.experimental as rx

    monkeypatch.setattr(rx, "get_local_object_locations", lambda refs: {refs[0]: {"object_size": 12345, "node_ids": []}})
    ins = _sizing.inputs_from_args((ref, "s3://bucket/a.parquet", 7), {"n": 3}, scalars=("n",))
    d = [i.to_dict() for i in ins]
    assert {"name": "arg1", "uri": "s3://bucket/a.parquet"} in d
    assert {"name": "arg0", "bytes": 12345} in d
    assert {"name": "n", "scalar": {"kind": "int", "value": 3}} in d
    assert not any(x.get("name") == "arg2" for x in d)  # unnamed scalars do not join the memo bucket

    def boom(*a, **k):
        raise RuntimeError("experimental API changed")

    monkeypatch.setattr(rx, "get_local_object_locations", boom)
    monkeypatch.setattr(rx, "get_object_locations", lambda refs, timeout_ms=-1: {refs[0]: {"object_size": None}})
    assert _sizing.inputs_from_args((ref,), {}) == []


def test_accelerator_rendering_only_when_allowed():
    from rightsizer.ray._settings import catalog_accelerator, ray_accelerator

    assert catalog_accelerator("A10G") == "A10G-24G" and catalog_accelerator("weird") == "unknown-gpu"
    assert ray_accelerator("A100-80G") == "A100" and ray_accelerator("A100-80G", labelled=True) == "A100-80G"
    assert ray_accelerator("H100-80G") == "H100" and ray_accelerator("unknown-gpu") is None
