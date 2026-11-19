import os
import sys
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class FakeCgroupV2:
    """A cgroup v2 tree under tmp_path, with /proc/self/{cgroup,mountinfo} pointing at it."""

    def __init__(self, root: Path, path: str = "/kubepods/pod1/ctr", mount_root: str = "/", peak: bool = True):
        self.proc = root / "proc"
        self.mount = root / "sys" / "fs" / "cgroup"
        (self.proc / "self").mkdir(parents=True)
        rel = path if mount_root == "/" else path[len(mount_root.rstrip("/")):]
        self.dir = self.mount / rel.lstrip("/")
        self.dir.mkdir(parents=True)
        (self.proc / "self" / "cgroup").write_text(f"0::{path}\n")
        (self.proc / "self" / "mountinfo").write_text(
            "22 1 0:21 / /proc rw,nosuid - proc proc rw\n"
            f"30 22 0:26 {mount_root} {self.mount} rw,nosuid,nodev,noexec shared:4 - cgroup2 cgroup2 rw,nsdelegate\n"
        )
        self.set(current=100 << 20, max_="max", cpu_usage=0, periods=0, throttled=0, throttled_usec=0)
        if peak:
            self.write("memory.peak", 100 << 20)

    def write(self, name, value):
        (self.dir / name).write_text(f"{value}\n")

    def set(self, current=None, max_=None, cpu_usage=None, periods=None, throttled=None, throttled_usec=None, cpu_max=None):
        if current is not None:
            self.write("memory.current", current)
        if max_ is not None:
            self.write("memory.max", max_)
        if cpu_max is not None:
            self.write("cpu.max", cpu_max)
        if cpu_usage is not None:
            self._cpu = dict(getattr(self, "_cpu", {}), usage_usec=cpu_usage)
        if periods is not None:
            self._cpu["nr_periods"] = periods
        if throttled is not None:
            self._cpu["nr_throttled"] = throttled
        if throttled_usec is not None:
            self._cpu["throttled_usec"] = throttled_usec
        if cpu_usage is not None or periods is not None:
            c = self._cpu
            (self.dir / "cpu.stat").write_text(
                f"usage_usec {c['usage_usec']}\nuser_usec {c['usage_usec']}\nsystem_usec 0\n"
                f"nr_periods {c.get('nr_periods', 0)}\nnr_throttled {c.get('nr_throttled', 0)}\n"
                f"throttled_usec {c.get('throttled_usec', 0)}\n"
            )


class FakeCgroupV1:
    def __init__(self, root: Path, path: str = "/kubepods/pod1/ctr"):
        self.proc = root / "proc"
        base = root / "sys" / "fs" / "cgroup"
        (self.proc / "self").mkdir(parents=True)
        self.mem = base / "memory" / path.lstrip("/")
        self.cpu = base / "cpu,cpuacct" / path.lstrip("/")
        self.mem.mkdir(parents=True)
        self.cpu.mkdir(parents=True)
        (self.proc / "self" / "cgroup").write_text(
            f"12:memory:{path}\n4:cpu,cpuacct:{path}\n1:name=systemd:{path}\n0::/\n"
        )
        (self.proc / "self" / "mountinfo").write_text(
            f"40 30 0:35 / {base / 'memory'} rw,nosuid shared:20 - cgroup cgroup rw,memory\n"
            f"41 30 0:36 / {base / 'cpu,cpuacct'} rw,nosuid shared:21 - cgroup cgroup rw,cpu,cpuacct\n"
        )
        (self.mem / "memory.max_usage_in_bytes").write_text(str(300 << 20))
        (self.mem / "memory.usage_in_bytes").write_text(str(200 << 20))
        (self.mem / "memory.limit_in_bytes").write_text(str(9223372036854771712))
        (self.cpu / "cpuacct.usage").write_text("0")
        (self.cpu / "cpu.stat").write_text("nr_periods 0\nnr_throttled 0\nthrottled_time 0\n")
        (self.cpu / "cpu.cfs_quota_us").write_text("-1")
        (self.cpu / "cpu.cfs_period_us").write_text("100000")


@pytest.fixture
def cg2(tmp_path):
    return FakeCgroupV2(tmp_path)


@pytest.fixture
def cg1(tmp_path):
    return FakeCgroupV1(tmp_path)
