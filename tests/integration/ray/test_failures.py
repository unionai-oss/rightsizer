"""(e) Failures are classified: a task raising MemoryError (worker-side record,
Failed with an AtLeast peak) and a task killed by SIGKILL (driver-side record:
WorkerCrashedError → Failed, source ray_state). The memory monitor's
OutOfMemoryError is Linux-only: see tests/integration/ray/linux/."""

from __future__ import annotations

import time

import pytest

import rayit_tasks as T
from conftest import engine, record, run_records


def test_memory_error_and_sigkill_are_classified(ray_session, store):
    ray = ray_session
    import rightsizer.ray as rr
    from ray.util.state import get_task

    rr.configure(engine=engine(store, mode="recommend"))
    mem = rr.remote(num_cpus=1, memory=256 << 20, max_retries=0, key="it.fail.memerr")(T.raise_memory_error)
    kill = rr.remote(num_cpus=1, memory=256 << 20, max_retries=0, key="it.fail.sigkill")(T.sigkill_self)
    r1, r2 = mem.remote(), kill.remote()
    with pytest.raises(ray.exceptions.RayTaskError) as e1:
        ray.get(r1)
    with pytest.raises(ray.exceptions.WorkerCrashedError):
        ray.get(r2)
    assert isinstance(e1.value.cause, MemoryError)
    assert rr.observer().wait_idle(30)
    rr.settings().engine.flush()
    time.sleep(1.0)
    st1, st2 = get_task(r1.task_id().hex()), get_task(r2.task_id().hex())
    st1 = st1[-1] if isinstance(st1, list) else st1
    st2 = st2[-1] if isinstance(st2, list) else st2
    m = run_records(store, "it.fail.memerr")
    k = run_records(store, "it.fail.sigkill")
    data = {
        "state_api": {"memerr": [st1.state, st1.error_type], "sigkill": [st2.state, st2.error_type]},
        "memerr_records": [{k_: r.get(k_) for k_ in ("status", "status_confidence", "source", "memory_peak")} for r in m],
        "sigkill_records": [{k_: r.get(k_) for k_ in ("status", "status_confidence", "source", "invocation_id")} for r in k],
        "sigkill_task_id": r2.task_id().hex(),
        "observer": dict(rr.observer().stats),
    }
    record("failures", data)
    assert st1.error_type == "TASK_EXECUTION_EXCEPTION" and st2.error_type == "WORKER_DIED"
    # MemoryError: the worker's own record (Failed, peak only a lower bound); the driver does not duplicate it.
    assert [r["status"] for r in m] == ["failed"] and m[0]["source"] == {"kind": "self_report"}
    assert m[0]["memory_peak"]["censor"] == "at_least"
    # SIGKILL: no finally on the worker; the driver classifies it from the owner's error metadata.
    assert [r["status"] for r in k] == ["failed"] and k[0]["source"] == {"kind": "ray_state"}
    assert k[0]["invocation_id"] == r2.task_id().hex()


def test_driver_never_fetches_successful_values(ray_session, store):
    """The observer leaves user values alone (plasma-sized and inline)."""
    ray = ray_session
    import rightsizer.ray as rr

    rr.configure(engine=engine(store))
    f = rr.remote(num_cpus=0.5, key="it.values")(lambda n: b"x" * n)
    before = dict(rr.observer().stats)
    refs = [f.remote(10), f.remote(5_000_000)]
    assert rr.observer().wait_idle(30)
    after = rr.observer().stats
    assert after["processed"] - before["processed"] == 2 and after["written"] == before["written"]
    assert after["skipped_plasma"] - before["skipped_plasma"] == 1
    assert [len(x) for x in ray.get(refs)] == [10, 5_000_000]
