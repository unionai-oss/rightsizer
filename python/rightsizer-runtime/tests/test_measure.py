import sys
import threading
import time
import types

import pytest

from rightsizer_runtime.measure import (
    CgroupInfo,
    CgroupReader,
    MeasurementResult,
    MeasurementScope,
    apply_censoring,
    detect_cgroup,
    make_rss_probe,
    measured,
    rusage_maxrss_bytes,
    _parse_mountinfo,
    _unescape_mount,
)
from rightsizer_runtime.wire import Censor, Measurement, RunStatus, utcnow

MiB = 1 << 20
GiB = 1 << 30


# ---- detection


def test_detect_v2(cg2):
    info = detect_cgroup(str(cg2.proc))
    assert info == CgroupInfo(version=2, v2_dir=str(cg2.dir))


def test_detect_v2_with_mount_root(tmp_path):
    from conftest import FakeCgroupV2

    cg = FakeCgroupV2(tmp_path, path="/kubepods/pod1/ctr", mount_root="/kubepods")
    info = detect_cgroup(str(cg.proc))
    assert info.version == 2 and info.v2_dir == str(cg.dir)


def test_detect_v2_namespaced_root(tmp_path):
    from conftest import FakeCgroupV2

    cg = FakeCgroupV2(tmp_path, path="/")
    assert detect_cgroup(str(cg.proc)).v2_dir == str(cg.mount)


def test_detect_v1(cg1):
    info = detect_cgroup(str(cg1.proc))
    assert info.version == 1
    assert info.memory_dir == str(cg1.mem) and info.cpuacct_dir == str(cg1.cpu) and info.cpu_dir == str(cg1.cpu)


def test_detect_none(tmp_path):
    assert detect_cgroup(str(tmp_path / "nope")) is None


def test_mountinfo_escapes():
    assert _unescape_mount(r"/a\040b") == "/a b"
    ms = _parse_mountinfo(r"30 22 0:26 / /sys/fs/my\040cg rw shared:4 - cgroup2 cgroup2 rw" + "\ngarbage line\n")
    assert len(ms) == 1 and ms[0].mount_point == "/sys/fs/my cg" and ms[0].fstype == "cgroup2"


def test_reader_v2(cg2):
    cg2.set(current=50 * MiB, max_=str(GiB), cpu_usage=2_000_000, periods=100, throttled=10, throttled_usec=500, cpu_max="200000 100000")
    r = CgroupReader(detect_cgroup(str(cg2.proc)))
    assert r.memory_current() == 50 * MiB and r.memory_limit() == GiB and r.memory_peak() == 100 * MiB
    assert r.cpu_usage_usec() == 2_000_000 and r.cpu_throttle() == (100, 10, 500)
    assert r.cpu_limit_millicores() == 2000
    cg2.set(max_="max", cpu_max="max 100000")
    assert r.memory_limit() is None and r.cpu_limit_millicores() is None


def test_reader_v1(cg1):
    r = CgroupReader(detect_cgroup(str(cg1.proc)))
    assert r.memory_peak() == 300 * MiB and r.memory_current() == 200 * MiB
    assert r.memory_limit() is None  # the "unlimited" sentinel
    (cg1.mem / "memory.limit_in_bytes").write_text(str(GiB))
    (cg1.cpu / "cpu.cfs_quota_us").write_text("150000")
    assert r.memory_limit() == GiB and r.cpu_limit_millicores() == 1500


# ---- scopes on a fake cgroup tree


def _run_scope(scope, body):
    scope.start()
    try:
        body()
    finally:
        pass
    return scope


def test_v2_scope_peak_exact(cg2):
    s = MeasurementScope(0.02, proc_root=str(cg2.proc), nvml=False)
    s.start()
    cg2.set(current=300 * MiB, cpu_usage=500_000)
    time.sleep(0.1)
    cg2.write("memory.peak", 400 * MiB)
    cg2.set(current=120 * MiB, cpu_usage=1_000_000)
    time.sleep(0.05)
    r = s.stop(RunStatus.SUCCEEDED)
    assert r.memory_source == "cgroup_v2"
    assert r.memory_peak == Measurement(400 * MiB, Censor.EXACT, None)
    assert r.cpu_source == "cgroup" and r.cpu_mean.censor is Censor.EXACT
    assert r.cpu_mean.value == pytest.approx(1.0 / r.duration_s, rel=1e-6)
    assert 0 <= r.t_peak_frac <= 1
    assert r.from_cgroup


def test_v2_without_memory_peak_uses_sampled_exact_with_resolution(tmp_path):
    from conftest import FakeCgroupV2

    cg = FakeCgroupV2(tmp_path, peak=False)  # kernel < 5.19
    s = MeasurementScope(0.02, proc_root=str(cg.proc), nvml=False).start()
    cg.set(current=700 * MiB)
    time.sleep(0.1)
    cg.set(current=200 * MiB)
    time.sleep(0.05)
    r = s.stop(RunStatus.SUCCEEDED)
    assert r.memory_source == "sampled_cgroup"
    assert r.memory_peak == Measurement(700 * MiB, Censor.EXACT, 0.02)


def test_v1_scope(cg1):
    s = MeasurementScope(0.02, proc_root=str(cg1.proc), nvml=False).start()
    (cg1.cpu / "cpuacct.usage").write_text(str(3 * 10**9))
    time.sleep(0.05)
    r = s.stop(RunStatus.SUCCEEDED)
    assert r.memory_source == "cgroup_v1" and r.memory_peak == Measurement(300 * MiB, Censor.EXACT, None)
    assert r.cpu_mean.value > 0


def test_success_within_5pct_of_limit_is_at_least(cg2):
    cg2.set(max_=str(1000 * MiB))
    cg2.write("memory.peak", 960 * MiB)
    s = MeasurementScope(0.02, proc_root=str(cg2.proc), nvml=False).start()
    r = s.stop(RunStatus.SUCCEEDED)
    assert r.memory_limit == 1000 * MiB
    assert r.memory_peak.censor is Censor.AT_LEAST and r.memory_peak.value == 960 * MiB
    # below 95% stays exact
    cg2.write("memory.peak", 900 * MiB)
    r2 = MeasurementScope(0.02, proc_root=str(cg2.proc), nvml=False).start().stop(RunStatus.SUCCEEDED)
    assert r2.memory_peak.censor is Censor.EXACT


def test_throttling_over_5pct_is_at_least(cg2):
    cg2.set(cpu_usage=0, periods=1000, throttled=0)
    s = MeasurementScope(0.02, proc_root=str(cg2.proc), nvml=False).start()
    cg2.set(cpu_usage=2_000_000, periods=1100, throttled=20)
    time.sleep(0.05)
    r = s.stop(RunStatus.SUCCEEDED)
    assert r.cpu_throttled_frac == pytest.approx(0.2)
    assert r.cpu_mean.censor is Censor.AT_LEAST and r.cpu_peak_window.censor is Censor.AT_LEAST
    # 5% exactly is not "above"
    s = MeasurementScope(0.02, proc_root=str(cg2.proc), nvml=False).start()
    cg2.set(cpu_usage=3_000_000, periods=1200, throttled=25)
    r = s.stop(RunStatus.SUCCEEDED)
    assert r.cpu_throttled_frac == pytest.approx(0.05) and r.cpu_mean.censor is Censor.EXACT


def test_cpu_peak_window(cg2):
    s = MeasurementScope(0.01, proc_root=str(cg2.proc), nvml=False, cpu_window_s=0.05).start()
    usage = 0
    t_end = time.monotonic() + 0.3
    burst_until = time.monotonic() + 0.1
    while time.monotonic() < t_end:
        usage += 40_000 if time.monotonic() < burst_until else 1_000  # 4 cores, then 0.1
        cg2.set(cpu_usage=usage)
        time.sleep(0.01)
    r = s.stop(RunStatus.SUCCEEDED)
    assert r.cpu_peak_window.value > r.cpu_mean.value * 1.5
    assert r.cpu_peak_window.resolution_s == 0.01


# ---- censoring rules (pure)


def _res(**kw):
    base = dict(started_at=utcnow(), duration_s=10.0, attribution="container")
    base.update(kw)
    return MeasurementResult(**base)


def test_oom_records_at_least_limit():
    r = _res(memory_peak=Measurement(900 * MiB, Censor.EXACT, 1.0), memory_limit=GiB, last_memory_sample=800 * MiB)
    c = apply_censoring(r, RunStatus.OOM_KILLED)
    assert c.memory_peak == Measurement(GiB, Censor.AT_LEAST, 1.0)


def test_oom_without_limit_uses_last_sample():
    r = _res(memory_peak=None, last_memory_sample=800 * MiB)
    assert apply_censoring(r, RunStatus.OOM_KILLED).memory_peak == Measurement(800 * MiB, Censor.AT_LEAST, None)


def test_gpu_oom_at_least_device_memory():
    r = _res(gpu_memory_peak=Measurement(70 * GiB, Censor.EXACT, 1.0), gpu_memory_total=80 * GiB)
    c = apply_censoring(r, RunStatus.GPU_OOM)
    assert c.gpu_memory_peak == Measurement(80 * GiB, Censor.AT_LEAST, 1.0)


def test_cached_carries_no_measurements():
    r = _res(memory_peak=Measurement(1.0), cpu_mean=Measurement(1.0), gpu_memory_peak=Measurement(1.0), t_peak_frac=0.5)
    c = apply_censoring(r, RunStatus.CACHED)
    assert c.memory_peak is None and c.cpu_mean is None and c.gpu_memory_peak is None and c.t_peak_frac is None


def test_failed_keeps_exact_peak():
    r = _res(memory_peak=Measurement(990 * MiB, Censor.EXACT, None), memory_limit=GiB)
    assert apply_censoring(r, RunStatus.FAILED).memory_peak.censor is Censor.EXACT


# ---- host (macOS here; Linux CI too) fallbacks


def test_rusage_fallback_without_cgroup():
    s = MeasurementScope(0.05, cgroup=None, nvml=False).start()
    buf = bytearray(64 * MiB)
    time.sleep(0.1)
    r = s.stop(RunStatus.SUCCEEDED)
    del buf
    assert r.memory_source == "rusage"
    assert r.memory_peak.censor is Censor.AT_LEAST and r.memory_peak.value >= 64 * MiB
    assert r.memory_peak.resolution_s is None
    assert r.cpu_mean.censor is Censor.AT_LEAST and r.cpu_source == "process"
    assert not r.from_cgroup


def test_rusage_maxrss_units():
    v = rusage_maxrss_bytes()
    assert v is None or v > 1 * MiB  # a Python interpreter is bigger than 1 MiB in either unit


@pytest.mark.skipif(make_rss_probe() is None, reason="no RSS probe on this platform")
def test_process_attribution_rss_minus_baseline():
    s = MeasurementScope(0.02, attribution="process", nvml=False).start()
    buf = bytearray(128 * MiB)
    buf[:: 4096] = b"x" * len(buf[:: 4096])  # touch pages
    time.sleep(0.1)
    r = s.stop(RunStatus.SUCCEEDED)
    del buf
    assert r.memory_source == "process_rss"
    assert r.memory_peak.censor is Censor.EXACT and r.memory_peak.resolution_s == 0.02
    assert 100 * MiB <= r.memory_peak.value <= 400 * MiB
    assert r.cpu_mean.censor is Censor.EXACT


def test_process_attribution_ignores_cgroup(cg2):
    s = MeasurementScope(0.02, attribution="process", proc_root=str(cg2.proc), nvml=False,
                         rss_probe=iter([100 * MiB, 150 * MiB, 130 * MiB] + [120 * MiB] * 100).__next__).start()
    time.sleep(0.05)
    r = s.stop(RunStatus.SUCCEEDED)
    assert r.memory_source == "process_rss" and r.memory_peak.value == 50 * MiB
    assert r.cpu_throttled_frac is None


def test_ray_interval_preset():
    from rightsizer_runtime import RAY_INTERVAL_S, FLYTE_INTERVAL_S

    assert (RAY_INTERVAL_S, FLYTE_INTERVAL_S) == (0.5, 1.0)


# ---- NVML


def test_nvml_absent(monkeypatch):
    monkeypatch.setitem(sys.modules, "pynvml", None)  # import fails
    s = MeasurementScope(0.02, cgroup=None).start()
    r = s.stop(RunStatus.SUCCEEDED)
    assert r.gpu_memory_peak is None and r.gpu_util_mean is None


class FakeNvml:
    def __init__(self, used, total=80 * GiB, fail_init=False, count=None):
        self.used = used  # list of per-device lists of samples
        self.total = total
        self.fail_init = fail_init
        self.count = len(used) if count is None else count
        self.i = [0] * len(used)
        self.shutdown = False

    def nvmlInit(self):
        if self.fail_init:
            raise RuntimeError("NVML_ERROR_LIBRARY_NOT_FOUND")

    def nvmlShutdown(self):
        self.shutdown = True

    def nvmlDeviceGetCount(self):
        return self.count

    def nvmlDeviceGetHandleByIndex(self, i):
        return i

    def nvmlDeviceGetUUID(self, h):
        return f"GPU-{h:04d}-uuid"

    def nvmlDeviceGetMemoryInfo(self, h):
        seq = self.used[h]
        v = seq[min(self.i[h], len(seq) - 1)]
        self.i[h] += 1
        return types.SimpleNamespace(used=v, total=self.total)

    def nvmlDeviceGetUtilizationRates(self, h):
        return types.SimpleNamespace(gpu=50)


def test_nvml_busiest_device_peak():
    nv = FakeNvml([[1 * GiB, 5 * GiB, 2 * GiB], [3 * GiB, 9 * GiB, 4 * GiB]])
    s = MeasurementScope(0.02, cgroup=None, nvml=nv).start()
    time.sleep(0.1)
    r = s.stop(RunStatus.SUCCEEDED)
    assert r.gpu_memory_peak == Measurement(9 * GiB, Censor.EXACT, 0.02)
    assert r.gpu_util_mean == pytest.approx(0.5)
    assert r.gpu_memory_total == 80 * GiB and nv.shutdown


def test_nvml_shared_is_at_most():
    nv = FakeNvml([[2 * GiB]])
    r = MeasurementScope(0.02, cgroup=None, nvml=nv, gpu_shared=True).start().stop(RunStatus.SUCCEEDED)
    assert r.gpu_memory_peak.censor is Censor.AT_MOST


def test_nvml_no_gpu_or_init_failure():
    for nv in (FakeNvml([], count=0), FakeNvml([[1]], fail_init=True)):
        r = MeasurementScope(0.02, cgroup=None, nvml=nv).start().stop(RunStatus.SUCCEEDED)
        assert r.gpu_memory_peak is None


def test_nvml_respects_cuda_visible_devices(monkeypatch):
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "1")
    nv = FakeNvml([[50 * GiB], [3 * GiB]])
    r = MeasurementScope(0.02, cgroup=None, nvml=nv).start().stop(RunStatus.SUCCEEDED)
    assert r.gpu_memory_peak.value == 3 * GiB
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "GPU-0000")
    nv = FakeNvml([[50 * GiB], [3 * GiB]])
    r = MeasurementScope(0.02, cgroup=None, nvml=nv).start().stop(RunStatus.SUCCEEDED)
    assert r.gpu_memory_peak.value == 50 * GiB


# ---- context manager / decorator


def test_context_manager_status_and_passthrough(cg2):
    with pytest.raises(ZeroDivisionError):
        with MeasurementScope(0.02, proc_root=str(cg2.proc), nvml=False) as s:
            1 / 0
    assert s.result is not None


def test_decorator_sync_and_async(cg2):
    import asyncio

    seen = []

    @measured(interval_s=0.02, proc_root=str(cg2.proc), nvml=False, on_result=seen.append)
    def f(x):
        return x + 1

    @measured(interval_s=0.02, proc_root=str(cg2.proc), nvml=False, on_result=seen.append)
    async def g(x):
        await asyncio.sleep(0.01)
        return x * 2

    assert f(1) == 2 and asyncio.run(g(3)) == 6
    assert len(seen) == 2 and all(r.memory_source == "cgroup_v2" for r in seen)


def test_sampler_thread_stops(cg2):
    before = {t.name for t in threading.enumerate()}
    s = MeasurementScope(0.01, proc_root=str(cg2.proc), nvml=False).start()
    s.stop()
    time.sleep(0.05)
    assert not any(t.name == "rightsizer-sampler" and t.is_alive() for t in threading.enumerate())
    assert s.stop() is s.result  # idempotent
