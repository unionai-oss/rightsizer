import asyncio
import json

import pytest

from rightsizer_runtime.child import measured_execution, status_for_exception
from rightsizer_runtime.env import DECISION_ENV, DecisionEnv
from rightsizer_runtime.measure import CgroupInfo
from rightsizer_runtime.record import RunRecordWriter
from rightsizer_runtime.wire import (
    AttemptContext,
    Censor,
    OutcomeSourceKind,
    Requests,
    ResourceSpec,
    RunRecord,
    RunStatus,
    Trust,
    new_ulid,
)


def dec(mode="enforce", key="flyte://acme/proj/dev/env.train"):
    return DecisionEnv(decision_id=new_ulid(), key=key, mode=mode,
                       requested=ResourceSpec(Requests(memory=1 << 30)), measured=True)


def records(root):
    return [RunRecord.from_json(p.read_text()) for p in sorted(root.rglob("*.json"))]


def test_success_writes_record_v2(tmp_path, cg2):
    store = tmp_path / "store"
    d = dec()
    me = measured_execution(d, RunRecordWriter(str(store)), platform="flyte", proc_root=str(cg2.proc), nvml=False,
                            attempt=AttemptContext("inv-9", 2, 3), trust=Trust.AUTHENTICATED, cluster="c1")
    assert me.run(lambda x: x * 2, 21) == 42
    [r] = records(store)
    assert r.status is RunStatus.SUCCEEDED and r.decision_id == d.decision_id and r.key == d.key
    assert (r.invocation_id, r.attempt) == ("inv-9", 2)
    assert r.requested == d.requested and r.trust is Trust.AUTHENTICATED and r.cluster == "c1"
    assert r.source.kind is OutcomeSourceKind.CGROUP
    assert r.memory_peak.censor is Censor.EXACT and r.duration_s >= 0
    assert me._scope_kwargs["interval_s"] == 1.0


def test_failure_records_failed_and_reraises(tmp_path, cg2):
    store = tmp_path / "store"
    me = measured_execution(dec(), RunRecordWriter(str(store)), proc_root=str(cg2.proc), nvml=False, attempt=None)

    @me
    def boom():
        raise ValueError("user bug")

    with pytest.raises(ValueError, match="user bug"):
        boom()
    [r] = records(store)
    assert r.status is RunStatus.FAILED and r.attempt == 1 and r.invocation_id == r.decision_id


def test_async_and_context_manager(tmp_path, cg2):
    store = tmp_path / "store"
    w = RunRecordWriter(str(store))

    @measured_execution(dec(), w, proc_root=str(cg2.proc), nvml=False)
    async def f():
        await asyncio.sleep(0.01)
        return "ok"

    assert asyncio.run(f()) == "ok"
    with measured_execution(dec(), w, proc_root=str(cg2.proc), nvml=False):
        pass
    assert [r.status for r in records(store)] == [RunStatus.SUCCEEDED] * 2


def test_reads_decision_from_env(tmp_path, cg2):
    store = tmp_path / "store"
    d = dec()
    me = measured_execution(writer=RunRecordWriter(str(store)), environ={DECISION_ENV: d.to_json()},
                            proc_root=str(cg2.proc), nvml=False)
    me.run(lambda: None)
    assert records(store)[0].decision_id == d.decision_id


def test_off_mode_and_no_key_run_unmeasured(tmp_path):
    store = tmp_path / "store"
    w = RunRecordWriter(str(store))
    assert measured_execution(dec(mode="off"), w).run(lambda: 1) == 1
    assert measured_execution(None, w, environ={}).run(lambda: 2) == 2
    assert not store.exists()


def test_key_without_decision(tmp_path):
    store = tmp_path / "store"
    me = measured_execution(None, RunRecordWriter(str(store), tenant="team-a"), platform="ray", key="ray://team-a//mod.fn",
                            environ={}, nvml=False)
    me.run(lambda: bytearray(10 << 20))
    [r] = records(store)
    assert r.decision_id is None and r.requested is None
    assert me._scope_kwargs == {"interval_s": 0.5, "attribution": "process", "nvml": False}
    assert r.source.kind is OutcomeSourceKind.SELF_REPORT


def test_reusable_drops_memory_peak(tmp_path, cg2):
    store = tmp_path / "store"
    measured_execution(dec(), RunRecordWriter(str(store)), reusable=True, proc_root=str(cg2.proc), nvml=False).run(lambda: 0)
    [r] = records(store)
    assert r.memory_peak is None and r.t_peak_frac is None and r.cpu_mean is not None


def test_memory_error_is_failed_at_least(tmp_path, cg2):
    store = tmp_path / "store"
    me = measured_execution(dec(), RunRecordWriter(str(store)), proc_root=str(cg2.proc), nvml=False)

    def oom():
        raise MemoryError()

    with pytest.raises(MemoryError):
        me.run(oom)
    [r] = records(store)
    assert r.status is RunStatus.FAILED and r.memory_peak.censor is Censor.AT_LEAST


def test_status_for_exception():
    assert status_for_exception(None) is RunStatus.SUCCEEDED
    assert status_for_exception(SystemExit(0)) is RunStatus.SUCCEEDED
    assert status_for_exception(SystemExit(3)) is RunStatus.FAILED
    assert status_for_exception(KeyboardInterrupt()) is RunStatus.UNKNOWN
    assert status_for_exception(asyncio.CancelledError()) is RunStatus.UNKNOWN
    assert status_for_exception(RuntimeError()) is RunStatus.FAILED
