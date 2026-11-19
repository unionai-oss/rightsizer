"""Adversarial review of the measurement scope: it must never hang or break the task (S1)."""

from __future__ import annotations

import threading
import time
from types import SimpleNamespace

from rightsizer_runtime.measure import MeasurementScope, rusage_maxrss_bytes


class HangingNvml:
    """pynvml stand-in whose memory query hangs (a GPU fallen off the bus) after a few calls."""

    def __init__(self):
        self.release = threading.Event()
        self.calls = 0

    def nvmlInit(self):
        pass

    def nvmlShutdown(self):
        pass

    def nvmlDeviceGetCount(self):
        return 1

    def nvmlDeviceGetHandleByIndex(self, i):
        return i

    def nvmlDeviceGetUUID(self, h):
        return "GPU-0"

    def nvmlDeviceGetMemoryInfo(self, h):
        self.calls += 1
        if self.calls > 3:
            self.release.wait(30)
        return SimpleNamespace(total=16 << 30, used=1 << 30)

    def nvmlDeviceGetUtilizationRates(self, h):
        return SimpleNamespace(gpu=50)


def test_stop_returns_when_the_sampler_is_stuck_in_a_probe():
    nv = HangingNvml()
    scope = MeasurementScope(0.02, attribution="container", nvml=nv, cgroup=None).start()
    time.sleep(0.3)  # the sampler is now blocked inside NVML, holding the scope's lock
    done = threading.Event()
    out = {}

    def stopper():
        out["r"] = scope.stop()
        done.set()

    threading.Thread(target=stopper, daemon=True).start()
    try:
        assert done.wait(6), "MeasurementScope.stop() hung behind a stuck probe: the task never returns"
        assert out["r"] is not None
    finally:
        nv.release.set()


def test_rusage_is_bytes_on_every_platform():
    """ru_maxrss is KiB on Linux and bytes on macOS; a few-MiB Python process is between 4 MiB and 64 GiB."""
    b = rusage_maxrss_bytes()
    assert b is not None and (4 << 20) < b < (64 << 30), b


# ── RunRecord writes against a hung store ──────────────────────────────────


def _rec():
    import datetime as dt

    from rightsizer_runtime.wire import OutcomeSource, RunRecord, RunStatus

    return RunRecord(key="flyte://acme/proj/dev/env.t", invocation_id="inv", attempt=1,
                     started_at=dt.datetime(2026, 11, 18, tzinfo=dt.timezone.utc), status=RunStatus.SUCCEEDED,
                     source=OutcomeSource.self_report())


class HungFs:
    def __init__(self):
        self.release = threading.Event()
        self.calls = 0

    def pipe_file(self, path, data):
        self.calls += 1
        self.release.wait(30)


def test_writer_does_not_hang_the_task_on_a_hung_store():
    from rightsizer_runtime.record import RunRecordWriter

    fs = HungFs()
    w = RunRecordWriter("s3://b/p", fs=fs, put_timeout_s=0.2)
    t0 = time.monotonic()
    try:
        assert w.write(_rec()) is False
        assert time.monotonic() - t0 < 3, "the write blocked the task's return"
        assert w.pending == 1, "a timed-out record is kept for retry"
    finally:
        fs.release.set()


def test_flyte_child_write_does_not_hang_on_a_hung_flyte_storage(monkeypatch):
    import asyncio
    import sys
    import types

    import rightsizer_runtime.flyte_child as fc

    async def never(*a, **k):
        await asyncio.sleep(60)

    fake = types.ModuleType("flyte.storage")
    fake.put_stream = never
    monkeypatch.setitem(sys.modules, "flyte.storage", fake)
    monkeypatch.setattr(fc, "PUT_TIMEOUT_S", 0.2)
    t0 = time.monotonic()
    n = asyncio.run(fc.write_records("s3://b/p", [(_rec(), None)]))
    assert n == 0 and time.monotonic() - t0 < 3
