"""S1: every public entry point swallows rightsizer's own failures (injected here)."""

import asyncio
import logging

import pytest

import rightsizer_runtime.measure as mmod
import rightsizer_runtime.record as rmod
from rightsizer_runtime.child import measured_execution
from rightsizer_runtime.env import DecisionEnv, current_attempt, parse_config_env, read_attempt, read_decided, read_decision
from rightsizer_runtime.measure import CgroupInfo, CgroupReader, MeasurementScope, measured
from rightsizer_runtime.policy import current_stack
from rightsizer_runtime.record import RunRecordWriter
from rightsizer_runtime.wire import RunStatus, new_ulid


def boom(*a, **k):
    raise RuntimeError("injected")


@pytest.fixture
def broken_probes(monkeypatch):
    for name in ("memory_peak", "memory_current", "memory_limit", "cpu_usage_usec", "cpu_throttle", "cpu_limit_millicores"):
        monkeypatch.setattr(CgroupReader, name, boom)
    monkeypatch.setattr(mmod, "rusage_maxrss_bytes", boom)
    monkeypatch.setattr(mmod, "process_cpu_seconds", boom)


def test_scope_survives_broken_probes(broken_probes):
    s = MeasurementScope(0.01, cgroup=CgroupInfo(version=2, v2_dir="/nonexistent"), rss_probe=boom, nvml=False)
    with s:
        x = 1 + 1
    assert x == 2  # user code ran; result may be None or empty


def test_scope_survives_detection_failure(monkeypatch):
    monkeypatch.setattr(mmod, "detect_cgroup", boom)
    with MeasurementScope(0.01, nvml=False) as s:
        pass
    assert s.result is None or s.result.memory_peak is None or s.result.memory_peak.value >= 0


def test_scope_survives_thread_start_failure(monkeypatch):
    class BadThread:
        def __init__(self, *a, **k):
            raise RuntimeError("can't start new thread")

    monkeypatch.setattr(mmod.threading, "Thread", BadThread)
    with MeasurementScope(0.01, cgroup=None, nvml=False):
        pass


def test_scope_survives_compute_failure(monkeypatch):
    monkeypatch.setattr(MeasurementScope, "_compute", boom)
    with MeasurementScope(0.01, cgroup=None, nvml=False) as s:
        pass
    assert s.result is None


def test_scope_user_exception_unchanged(broken_probes):
    with pytest.raises(KeyError):
        with MeasurementScope(0.01, cgroup=None, nvml=False):
            raise KeyError("mine")


def test_nvml_raising_everywhere():
    class EvilNvml:
        def __getattr__(self, name):
            raise RuntimeError("nvml exploded")

    with MeasurementScope(0.01, cgroup=None, nvml=EvilNvml()) as s:
        pass
    assert s.result.gpu_memory_peak is None


def test_nvml_sample_raising():
    class HalfNvml:
        def nvmlInit(self): pass
        def nvmlShutdown(self): raise RuntimeError("x")
        def nvmlDeviceGetCount(self): return 1
        def nvmlDeviceGetHandleByIndex(self, i): return i
        def nvmlDeviceGetMemoryInfo(self, h): raise RuntimeError("x")
        def nvmlDeviceGetUtilizationRates(self, h): raise RuntimeError("x")

    with MeasurementScope(0.01, cgroup=None, nvml=HalfNvml()) as s:
        pass
    assert s.result.gpu_memory_peak is None


def test_measured_decorator_callback_failure():
    @measured(interval_s=0.01, cgroup=None, nvml=False, on_result=boom)
    def f():
        return 7

    assert f() == 7


def test_writer_backend_failures(tmp_path, monkeypatch):
    monkeypatch.setattr(rmod, "make_backend", boom)
    w = RunRecordWriter(str(tmp_path))
    from test_record import rec

    assert w.write(rec()) is False
    assert w.flush() == 0
    monkeypatch.setattr(rmod.RunRecord, "to_json", boom)
    assert w.write(rec()) is False  # serialization failure → dropped, not raised


def test_writer_garbage_input(tmp_path):
    w = RunRecordWriter(str(tmp_path))
    assert w.write(None) is False
    assert w.write("not a record") is False


def test_child_survives_everything(tmp_path, monkeypatch, broken_probes):
    d = DecisionEnv(decision_id=new_ulid(), key="flyte://o/p/d/t", mode="enforce")
    w = RunRecordWriter(str(tmp_path))
    monkeypatch.setattr(w, "write", boom)
    monkeypatch.setattr(MeasurementScope, "start", boom)
    assert measured_execution(d, w).run(lambda: "ran") == "ran"

    @measured_execution(d, w)
    async def af():
        return "aran"

    assert asyncio.run(af()) == "aran"
    with measured_execution(d, w):
        pass


def test_child_survives_build_record_failure(tmp_path, monkeypatch):
    d = DecisionEnv(decision_id=new_ulid(), key="flyte://o/p/d/t", mode="enforce")
    me = measured_execution(d, RunRecordWriter(str(tmp_path)), cgroup=None, nvml=False)
    monkeypatch.setattr(me, "build_record", boom)
    with pytest.raises(ZeroDivisionError):
        me.run(lambda: 1 / 0)  # the user's exception, not ours
    assert me.run(lambda: 5) == 5


def test_child_bad_decision_inputs(tmp_path):
    w = RunRecordWriter(str(tmp_path))
    assert measured_execution("{{garbage", w).run(lambda: 1) == 1
    assert measured_execution(12345, w).run(lambda: 2) == 2
    assert measured_execution(environ={"RIGHTSIZER_DECISION": "x"}, writer=w).run(lambda: 3) == 3


def test_env_readers_never_raise():
    bad = {"RIGHTSIZER_DECISION": "\x00", "RIGHTSIZER_ATTEMPT": "[1,2]", "RIGHTSIZER_DECIDED": "!!",
           "RIGHTSIZER__A": "x", "RIGHTSIZER__A__B": "y"}
    assert read_decision(bad) is None and read_attempt(bad) is None and read_decided(bad) is None
    assert current_attempt(bad) is None
    assert parse_config_env(bad) == {"a": "x"}


def test_policy_inherited_garbage():
    assert current_stack("\x00\x01") == ()
    assert current_stack('{"v":1,"s":[{"mode":"bogus"}]}') == ()
