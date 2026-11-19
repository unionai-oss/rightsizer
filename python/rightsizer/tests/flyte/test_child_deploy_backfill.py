"""Child-side measurement (§8.1), deploy-time sizing and reconcile (§6.3), backfill (§8.3)."""

from __future__ import annotations

import asyncio
import json

import flyte
import pytest

from rightsizer_runtime.env import DecisionEnv
from rightsizer_runtime.wire import AttemptContext, Mode, RunStatus, new_ulid

import rightsizer.flyte as rf
from rightsizer.flyte import _deploy as dep
from rightsizer.flyte._store import Store

GiB = 1 << 30
MiB = 1 << 20

env = rf.RightsizedEnvironment(name="rsz_unit_dep", resources=flyte.Resources(cpu=2, memory="4Gi"), auto_image=False)


@env.task
async def grow(mib: int) -> int:
    b = bytearray(mib * MiB)
    return len(b)


@env.task
async def boom(x: int) -> int:
    raise ValueError("boom")


def test_child_writes_a_record_joined_to_the_parent_decision(monkeypatch, store):
    from rightsizer_runtime import flyte_child

    did = new_ulid()
    key = "flyte://acme/proj/dev/rsz_unit_dep.grow"
    monkeypatch.setenv("RIGHTSIZER__HISTORY__URI", store)
    monkeypatch.setenv("RIGHTSIZER_DECISION", DecisionEnv(decision_id=did, key=key, mode=Mode.ENFORCE, measured=True).to_json())
    monkeypatch.setenv("RIGHTSIZER_ATTEMPT", AttemptContext(invocation_id="inv-1", attempt=2, max_attempts=3).to_json())

    async def body(mib):
        return len(bytearray(mib * MiB))

    assert asyncio.run(flyte_child.measured_execute(body, (8,), {}, key="ignored")) == 8 * MiB
    (r,) = Store(store).runs(key)
    assert (r.decision_id, r.invocation_id, r.attempt, r.status) == (did, "inv-1", 2, RunStatus.SUCCEEDED)
    assert r.memory_peak is not None and r.memory_peak.value >= 8 * MiB


def test_child_failure_is_recorded_and_reraised(monkeypatch, store):
    from rightsizer_runtime import flyte_child

    monkeypatch.setenv("RIGHTSIZER__HISTORY__URI", store)

    async def body():
        raise KeyError("x")

    with pytest.raises(KeyError):
        asyncio.run(flyte_child.measured_execute(body, (), {}, key="flyte://acme/proj/dev/rsz_unit_dep.boom",
                                                  requested=None))
    (r,) = Store(store).runs("flyte://acme/proj/dev/rsz_unit_dep.boom")
    assert r.status is RunStatus.FAILED and r.decision_id is None


def test_child_reusable_has_no_memory_peak_and_local_is_inert(monkeypatch, store):
    from rightsizer_runtime import flyte_child

    monkeypatch.setenv("RIGHTSIZER__HISTORY__URI", store)

    async def body():
        return 1

    k = "flyte://acme/proj/dev/rsz_unit_dep.r"
    asyncio.run(flyte_child.measured_execute(body, (), {}, key=k, reusable=True))
    (r,) = Store(store).runs(k)
    assert r.memory_peak is None
    asyncio.run(flyte_child.measured_execute(body, (), {}, key=k + "2", local=True))
    assert Store(store).runs(k + "2") == []


def test_store_uri_from_raw_data_path(monkeypatch):
    from rightsizer_runtime.flyte_child import store_uri

    assert store_uri("s3://flyte-data/ab/cd/raw", {}) == "s3://flyte-data/rightsizer"
    assert store_uri("/tmp/raw", {}) is None
    assert store_uri("s3://x/y", {"RIGHTSIZER__HISTORY__URI": "gs://b/p/"}) == "gs://b/p"


def test_reconcile_lease_is_a_singleton(store):
    s = Store(store)
    assert dep.acquire_lease(s, "a") and dep.acquire_lease(s, "a"), "the holder renews"
    assert not dep.acquire_lease(s, "b"), "another holder is refused while the lease is live"
    assert dep.reconcile_active(s)
    dep.release_lease(s, "a")
    assert not dep.reconcile_active(s) and dep.acquire_lease(s, "b")


def test_deploy_sizes_increases_only_and_restores_the_registry(engine, monkeypatch, point_estimator, store):
    point_estimator(6 * GiB)  # demand above the prior: an increase
    deployed = {}

    def fake_deploy(*envs, version=None, dry_run=False, **kw):
        deployed["version"] = version
        deployed["tasks"] = {n: t.resources for e in envs for n, t in e.tasks.items()}
        return ["deployment"]

    monkeypatch.setattr(flyte, "deploy", fake_deploy)
    monkeypatch.setenv("RIGHTSIZER__HISTORY__URI", store)
    flyte.init(project="proj", domain="dev", org="acme")
    out = rf.deploy(env)
    assert out == ["deployment"]
    # Flyte derives the version (code bundle + envs with the sized tasks + images): a version made
    # from the sizing alone would give a code change the previous deploy's version.
    assert deployed["version"] is None
    from rightsizer import _units

    mem = deployed["tasks"]["rsz_unit_dep.grow"].memory
    assert _units.memory_bytes(mem) > 4 * GiB, mem
    assert env.tasks["rsz_unit_dep.grow"].resources.memory == "4Gi", "the registry holds the source definition again"
    recs = dep.deploy_records(Store(store))
    assert {r["task"] for r in recs} == {"rsz_unit_dep.grow", "rsz_unit_dep.boom"}
    assert all(r["origin_prior"]["requests"]["memory"] == 4 * GiB for r in recs)
    # restore deploys from source with sizing disabled
    rf.deploy(env, restore=True)
    assert deployed["tasks"]["rsz_unit_dep.grow"].memory == "4Gi" and deployed["version"] is None


def test_deploy_without_reconcile_never_reduces(engine, monkeypatch, point_estimator, store):
    point_estimator(64 * MiB)
    monkeypatch.setenv("RIGHTSIZER__HISTORY__URI", store)
    flyte.init(project="proj", domain="dev", org="acme")
    plans = dep.plan_deploy(env, allow_reductions=False)
    assert plans == [], "a reduction needs reconcile; without it the deploy is increases-only"


def test_backfill_maps_phases_errors_and_requests():
    from flyteidl2.core import tasks_pb2

    from rightsizer.flyte._backfill import parse_since, requested_from_template, status_of

    class E:
        code = "OOMKilled"
        message = "Reason [OOMKilled]"

    assert status_of("ACTION_PHASE_FAILED", E()) is RunStatus.OOM_KILLED
    assert status_of("ACTION_PHASE_SUCCEEDED", None, "CACHE_HIT") is RunStatus.CACHED
    assert status_of("ACTION_PHASE_SUCCEEDED", None, "CACHE_MISS") is RunStatus.SUCCEEDED
    assert status_of("ACTION_PHASE_TIMED_OUT", None) is RunStatus.TIMEOUT

    class G:
        code = "OutOfMemoryError"
        message = "CUDA out of memory"

    assert status_of("ACTION_PHASE_FAILED", G()) is RunStatus.GPU_OOM
    R = tasks_pb2.Resources
    t = tasks_pb2.TaskTemplate(container=tasks_pb2.Container(resources=R(
        requests=[R.ResourceEntry(name=R.CPU, value="500m"), R.ResourceEntry(name=R.MEMORY, value="256Mi")],
        limits=[R.ResourceEntry(name=R.MEMORY, value="512Mi")])))
    spec = requested_from_template(t)
    assert (spec.requests.cpu, spec.requests.memory, spec.limits.memory) == (500, 256 * MiB, 512 * MiB)
    assert parse_since("30d").days == 30


def test_wheel_dir_image_forces_both_local_wheels(tmp_path):
    """M1 QA regression: the PythonWheels layer force-reinstalls only its own package, so a
    stale rightsizer-runtime (same dev version) from the builder's uv cache stayed in the image.
    Both local wheels get their own layer."""
    from rightsizer.flyte._env import rightsizer_image

    (tmp_path / "rightsizer-0.1.0-cp310-abi3-manylinux2014_aarch64.whl").write_bytes(b"x")
    base = flyte.Image.from_debian_base(python_version=(3, 12))
    only_core = rightsizer_image(base, str(tmp_path))
    assert [getattr(layer, "package_name", None) for layer in only_core._layers if hasattr(layer, "wheel_dir")] == ["rightsizer"]
    (tmp_path / "rightsizer_runtime-0.1.0-py3-none-any.whl").write_bytes(b"y")
    img = rightsizer_image(base, str(tmp_path))
    names = [getattr(layer, "package_name", None) for layer in img._layers if hasattr(layer, "wheel_dir")]
    assert names == ["rightsizer", "rightsizer-runtime"]
    assert rightsizer_image(img, str(tmp_path)) is img  # idempotent
