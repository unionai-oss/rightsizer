"""In-task measurement (SPEC §5.7, flyte.md §8.1, ray.md §6).

A :class:`MeasurementScope` runs a background sampler thread while user code
runs and turns what it saw into censored :class:`~.wire.Measurement` values.

Two attributions:

- ``"container"`` (Flyte, K8s pods): the unit owns its cgroup. Memory peak is
  ``memory.peak`` (cgroup v2, kernel ≥ 5.19) | ``memory.max_usage_in_bytes``
  (v1) | the sampled maximum of ``memory.current`` / ``memory.usage_in_bytes``
  | ``max(ru_maxrss self, children)`` (``AtLeast``). CPU from ``cpu.stat`` /
  ``cpuacct.usage``; throttling from ``cpu.stat``.
- ``"process"`` (Ray workers, which are reused): lifetime counters are not
  attributable, so memory is the in-task maximum of sampled process RSS minus
  the RSS at scope start, ``Exact`` with ``resolution_s = interval``; CPU is the
  process's own CPU time.

GPU memory (both): NVML per-device used memory via ``pynvml`` if importable
and a GPU is present; the record keeps the busiest device's peak.

**S1:** nothing here raises into user code. Every failure is logged at debug
or warning level and yields ``None`` measurements.
"""

from __future__ import annotations

import collections
import ctypes
import datetime as _dt
import functools
import inspect
import logging
import os
import sys
import threading
import time
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple, TypeVar, Union

from .wire import Censor, Measurement, RunStatus, utcnow

log = logging.getLogger("rightsizer.runtime.measure")

FLYTE_INTERVAL_S = 1.0
RAY_INTERVAL_S = 0.5
DEFAULT_CPU_WINDOW_S = 60.0
NEAR_LIMIT_FRAC = 0.95  # success within 5% of the limit → AtLeast
THROTTLE_FRAC = 0.05  # throttled periods above 5% → AtLeast for CPU
V1_UNLIMITED = 1 << 62  # memory.limit_in_bytes at or above this means "no limit"

OOM_STATUSES = frozenset({RunStatus.OOM_KILLED})
GPU_OOM_STATUSES = frozenset({RunStatus.GPU_OOM})


# --------------------------------------------------------------------------- cgroup detection


@dataclass(frozen=True)
class CgroupInfo:
    """Where this process's cgroup files live."""

    version: int  # 1 or 2
    v2_dir: Optional[str] = None
    memory_dir: Optional[str] = None  # v1 memory controller
    cpuacct_dir: Optional[str] = None  # v1 cpuacct controller
    cpu_dir: Optional[str] = None  # v1 cpu controller (cpu.stat, cfs quota)


def _unescape_mount(s: str) -> str:
    # mountinfo escapes space, tab, newline and backslash as \ooo.
    out, i = [], 0
    while i < len(s):
        if s[i] == "\\" and i + 4 <= len(s) and s[i + 1 : i + 4].isdigit():
            out.append(chr(int(s[i + 1 : i + 4], 8)))
            i += 4
        else:
            out.append(s[i])
            i += 1
    return "".join(out)


@dataclass(frozen=True)
class _Mount:
    root: str
    mount_point: str
    fstype: str
    super_options: Tuple[str, ...]


def _parse_mountinfo(text: str) -> List[_Mount]:
    mounts = []
    for line in text.splitlines():
        parts = line.split()
        try:
            sep = parts.index("-")
        except ValueError:
            continue
        if len(parts) < sep + 3 or sep < 5:
            continue
        mounts.append(
            _Mount(
                root=_unescape_mount(parts[3]),
                mount_point=_unescape_mount(parts[4]),
                fstype=parts[sep + 1],
                super_options=tuple(parts[sep + 3].split(",")) if len(parts) > sep + 3 else (),
            )
        )
    return mounts


def _parse_proc_cgroup(text: str) -> Tuple[Optional[str], Dict[str, str]]:
    """Return (v2 path, {v1 controller: path})."""
    v2: Optional[str] = None
    v1: Dict[str, str] = {}
    for line in text.splitlines():
        parts = line.strip().split(":", 2)
        if len(parts) != 3:
            continue
        hid, ctrls, path = parts
        if hid == "0" and ctrls == "":
            v2 = path
        else:
            for c in ctrls.split(","):
                if c:
                    v1[c.replace("name=", "")] = path
    return v2, v1


def _join_cgroup(mount: _Mount, path: str) -> str:
    """Map a /proc/self/cgroup path onto a mount, honouring the mount's root."""
    root = mount.root.rstrip("/") or "/"
    if root == "/":
        rel = path
    elif path == root or path.startswith(root + "/"):
        rel = path[len(root) :]
    else:
        # cgroup namespace edge cases ("/../.."): the mount point is our cgroup.
        rel = "/"
    rel = rel.lstrip("/")
    if ".." in rel.split("/"):
        rel = ""
    return os.path.join(mount.mount_point, rel) if rel else mount.mount_point


def _read_text(path: str) -> Optional[str]:
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return f.read()
    except OSError:
        return None


def detect_cgroup(proc_root: str = "/proc", sys_fs_cgroup: str = "/sys/fs/cgroup") -> Optional[CgroupInfo]:
    """Find this process's cgroup (v2 preferred when it has the memory controller files).

    Reads ``{proc_root}/self/cgroup`` and ``{proc_root}/self/mountinfo``.
    Falls back to ``sys_fs_cgroup`` when mountinfo is unreadable. Returns
    ``None`` when there are no cgroups (macOS, or nothing readable).
    """
    try:
        cg_text = _read_text(os.path.join(proc_root, "self", "cgroup"))
        if cg_text is None:
            return None
        v2_path, v1_paths = _parse_proc_cgroup(cg_text)
        mi = _read_text(os.path.join(proc_root, "self", "mountinfo"))
        mounts = _parse_mountinfo(mi) if mi else []

        v2_dir: Optional[str] = None
        if v2_path is not None:
            cands = [m for m in mounts if m.fstype == "cgroup2"]
            for m in cands:
                d = _join_cgroup(m, v2_path)
                if os.path.isdir(d):
                    v2_dir = d
                    break
            if v2_dir is None and cands and os.path.isdir(cands[0].mount_point):
                v2_dir = cands[0].mount_point
            if v2_dir is None and not mounts:
                d = os.path.join(sys_fs_cgroup, v2_path.lstrip("/"))
                v2_dir = d if os.path.isdir(d) else (sys_fs_cgroup if os.path.isdir(sys_fs_cgroup) else None)

        def v1_dir(ctrl: str) -> Optional[str]:
            path = v1_paths.get(ctrl)
            if path is None:
                return None
            for m in mounts:
                if m.fstype == "cgroup" and ctrl in m.super_options:
                    d = _join_cgroup(m, path)
                    if os.path.isdir(d):
                        return d
                    if os.path.isdir(m.mount_point):
                        return m.mount_point
            if not mounts:
                for name in (ctrl, "cpu,cpuacct", "cpuacct,cpu"):
                    d = os.path.join(sys_fs_cgroup, name, path.lstrip("/"))
                    if os.path.isdir(d):
                        return d
            return None

        mem1, acct1, cpu1 = v1_dir("memory"), v1_dir("cpuacct"), v1_dir("cpu")
        v2_usable = v2_dir is not None and (
            os.path.exists(os.path.join(v2_dir, "memory.current")) or os.path.exists(os.path.join(v2_dir, "cpu.stat"))
        )
        if mem1 is None and v2_usable:
            return CgroupInfo(version=2, v2_dir=v2_dir)
        if mem1 is not None or acct1 is not None:
            return CgroupInfo(version=1, memory_dir=mem1, cpuacct_dir=acct1, cpu_dir=cpu1 or acct1)
        return None
    except Exception as e:  # pragma: no cover - defensive
        log.debug("rightsizer: cgroup detection failed: %s", e)
        return None


def _read_int(path: Optional[str]) -> Optional[int]:
    if path is None:
        return None
    t = _read_text(path)
    if t is None:
        return None
    t = t.strip()
    try:
        return int(t)
    except ValueError:
        return None


def _read_kv(path: Optional[str]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    if path is None:
        return out
    t = _read_text(path)
    if not t:
        return out
    for line in t.splitlines():
        parts = line.split()
        if len(parts) == 2:
            try:
                out[parts[0]] = int(parts[1])
            except ValueError:
                pass
    return out


class CgroupReader:
    """Reads one cgroup's counters. Every method returns ``None`` when unavailable."""

    def __init__(self, info: CgroupInfo):
        self.info = info

    def _p(self, d: Optional[str], name: str) -> Optional[str]:
        return os.path.join(d, name) if d else None

    @property
    def kind(self) -> str:
        return f"cgroup_v{self.info.version}"

    def memory_peak(self) -> Optional[int]:
        i = self.info
        if i.version == 2:
            return _read_int(self._p(i.v2_dir, "memory.peak"))
        return _read_int(self._p(i.memory_dir, "memory.max_usage_in_bytes"))

    def memory_current(self) -> Optional[int]:
        i = self.info
        if i.version == 2:
            return _read_int(self._p(i.v2_dir, "memory.current"))
        return _read_int(self._p(i.memory_dir, "memory.usage_in_bytes"))

    def memory_limit(self) -> Optional[int]:
        i = self.info
        if i.version == 2:
            t = _read_text(self._p(i.v2_dir, "memory.max") or "")
            if t is None or t.strip() == "max":
                return None
            try:
                return int(t.strip())
            except ValueError:
                return None
        v = _read_int(self._p(i.memory_dir, "memory.limit_in_bytes"))
        return None if v is None or v >= V1_UNLIMITED else v

    def cpu_usage_usec(self) -> Optional[int]:
        i = self.info
        if i.version == 2:
            return _read_kv(self._p(i.v2_dir, "cpu.stat")).get("usage_usec")
        ns = _read_int(self._p(i.cpuacct_dir, "cpuacct.usage"))
        return None if ns is None else ns // 1000

    def cpu_throttle(self) -> Optional[Tuple[int, int, int]]:
        """(nr_periods, nr_throttled, throttled_usec)."""
        i = self.info
        if i.version == 2:
            kv = _read_kv(self._p(i.v2_dir, "cpu.stat"))
            if "nr_periods" not in kv:
                return None
            return kv.get("nr_periods", 0), kv.get("nr_throttled", 0), kv.get("throttled_usec", 0)
        kv = _read_kv(self._p(i.cpu_dir, "cpu.stat"))
        if "nr_periods" not in kv:
            return None
        return kv.get("nr_periods", 0), kv.get("nr_throttled", 0), kv.get("throttled_time", 0) // 1000

    def cpu_limit_millicores(self) -> Optional[int]:
        i = self.info
        if i.version == 2:
            t = _read_text(self._p(i.v2_dir, "cpu.max") or "")
            if not t:
                return None
            parts = t.split()
            if len(parts) != 2 or parts[0] == "max":
                return None
            try:
                q, p = int(parts[0]), int(parts[1])
            except ValueError:
                return None
            return int(q * 1000 / p) if p > 0 else None
        q = _read_int(self._p(i.cpu_dir, "cpu.cfs_quota_us"))
        p = _read_int(self._p(i.cpu_dir, "cpu.cfs_period_us"))
        if q is None or p is None or q <= 0 or p <= 0:
            return None
        return int(q * 1000 / p)


# --------------------------------------------------------------------------- process probes


def _page_size() -> int:
    try:
        return os.sysconf("SC_PAGE_SIZE")
    except (ValueError, OSError, AttributeError):  # pragma: no cover
        return 4096


class _MachTaskBasicInfo(ctypes.Structure):
    _pack_ = 4
    _fields_ = [
        ("virtual_size", ctypes.c_uint64),
        ("resident_size", ctypes.c_uint64),
        ("resident_size_max", ctypes.c_uint64),
        ("user_time", ctypes.c_int32 * 2),
        ("system_time", ctypes.c_int32 * 2),
        ("policy", ctypes.c_int32),
        ("suspend_count", ctypes.c_int32),
    ]


class _MachRss:
    MACH_TASK_BASIC_INFO = 20

    def __init__(self) -> None:
        libc = ctypes.CDLL(None)
        self._task = ctypes.c_uint32.in_dll(libc, "mach_task_self_").value
        self._task_info = libc.task_info
        self._task_info.argtypes = [ctypes.c_uint32, ctypes.c_int, ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint32)]
        self._task_info.restype = ctypes.c_int

    def __call__(self) -> Optional[int]:
        info = _MachTaskBasicInfo()
        count = ctypes.c_uint32(ctypes.sizeof(info) // 4)
        kr = self._task_info(self._task, self.MACH_TASK_BASIC_INFO, ctypes.byref(info), ctypes.byref(count))
        return int(info.resident_size) if kr == 0 else None


def make_rss_probe(proc_root: str = "/proc") -> Optional[Callable[[], Optional[int]]]:
    """A callable returning this process's current RSS in bytes, or ``None`` if unsupported."""
    statm = os.path.join(proc_root, "self", "statm")
    if os.path.exists(statm):
        page = _page_size()

        def _linux() -> Optional[int]:
            t = _read_text(statm)
            if not t:
                return None
            parts = t.split()
            try:
                return int(parts[1]) * page
            except (IndexError, ValueError):
                return None

        return _linux
    if sys.platform == "darwin":
        try:
            probe = _MachRss()
            if probe() is not None:
                return probe
        except Exception as e:
            log.debug("rightsizer: mach RSS probe unavailable: %s", e)
    return None


def rusage_maxrss_bytes() -> Optional[int]:
    """``max(ru_maxrss self, ru_maxrss children)`` in bytes (KiB on Linux, bytes on macOS)."""
    try:
        import resource
    except ImportError:  # Windows
        return None
    try:
        s = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        c = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    except (OSError, ValueError):
        return None
    m = max(s, c)
    if m <= 0:
        return None
    return int(m) if sys.platform == "darwin" else int(m) * 1024


def process_cpu_seconds(include_children: bool = True) -> float:
    """CPU seconds of this process (all threads), plus reaped children if requested."""
    t = time.process_time()
    if include_children:
        try:
            import resource

            r = resource.getrusage(resource.RUSAGE_CHILDREN)
            t += r.ru_utime + r.ru_stime
        except (ImportError, OSError, ValueError):
            pass
    return t


# --------------------------------------------------------------------------- NVML


class _Nvml:
    """Thin guard around pynvml. ``open()`` returns ``None`` when there is no NVML or no GPU."""

    def __init__(self, nv: Any, handles: List[Any], per_process: bool):
        self.nv = nv
        self.handles = handles
        self.per_process = per_process
        self.pid = os.getpid()
        self.total: Optional[int] = None
        try:
            self.total = max(int(nv.nvmlDeviceGetMemoryInfo(h).total) for h in handles)
        except Exception:
            self.total = None

    @classmethod
    def open(cls, module: Any = None, per_process: bool = False) -> Optional["_Nvml"]:
        try:
            nv = module
            if nv is None:
                import pynvml as nv  # type: ignore[no-redef]
        except Exception as e:
            log.debug("rightsizer: pynvml not importable: %s", e)
            return None
        try:
            nv.nvmlInit()
        except Exception as e:
            log.debug("rightsizer: nvmlInit failed (no GPU/driver?): %s", e)
            return None
        try:
            n = int(nv.nvmlDeviceGetCount())
            handles = [nv.nvmlDeviceGetHandleByIndex(i) for i in range(n)]
            handles = cls._filter_visible(nv, handles)
            if not handles:
                nv.nvmlShutdown()
                return None
            return cls(nv, handles, per_process)
        except Exception as e:
            log.debug("rightsizer: NVML enumeration failed: %s", e)
            try:
                nv.nvmlShutdown()
            except Exception:
                pass
            return None

    @staticmethod
    def _filter_visible(nv: Any, handles: List[Any]) -> List[Any]:
        """NVML ignores CUDA_VISIBLE_DEVICES; honour it (indices or UUIDs) when set."""
        vis = os.environ.get("CUDA_VISIBLE_DEVICES")
        if vis is None or vis.strip() in ("", "all"):
            return handles
        want = [v.strip() for v in vis.split(",") if v.strip()]
        out = []
        for i, h in enumerate(handles):
            uuid = None
            try:
                uuid = nv.nvmlDeviceGetUUID(h)
                if isinstance(uuid, bytes):
                    uuid = uuid.decode()
            except Exception:
                pass
            for w in want:
                if w == str(i) or (uuid and w.startswith("GPU-") and uuid.startswith(w)) or (uuid and w == uuid):
                    out.append(h)
                    break
        return out

    def sample(self) -> Tuple[List[Optional[int]], List[float]]:
        used: List[Optional[int]] = []
        util: List[float] = []
        for h in self.handles:
            u: Optional[int] = None
            try:
                if self.per_process:
                    procs = self.nv.nvmlDeviceGetComputeRunningProcesses(h)
                    u = sum(int(p.usedGpuMemory or 0) for p in procs if int(p.pid) == self.pid)
                else:
                    u = int(self.nv.nvmlDeviceGetMemoryInfo(h).used)
            except Exception:
                u = None
            used.append(u)
            try:
                util.append(float(self.nv.nvmlDeviceGetUtilizationRates(h).gpu) / 100.0)
            except Exception:
                pass
        return used, util

    def close(self) -> None:
        try:
            self.nv.nvmlShutdown()
        except Exception:
            pass


# --------------------------------------------------------------------------- result


@dataclass(frozen=True)
class MeasurementResult:
    """What a scope measured. Units: bytes for memory, cores for CPU, fractions in [0, 1]."""

    started_at: _dt.datetime
    duration_s: float
    attribution: str
    memory_source: Optional[str] = None  # cgroup_v2 | cgroup_v1 | sampled_cgroup | process_rss | rusage
    cpu_source: Optional[str] = None  # cgroup | process
    memory_peak: Optional[Measurement] = None
    cpu_mean: Optional[Measurement] = None
    cpu_peak_window: Optional[Measurement] = None
    cpu_throttled_frac: Optional[float] = None
    gpu_memory_peak: Optional[Measurement] = None
    gpu_util_mean: Optional[float] = None
    t_peak_frac: Optional[float] = None
    memory_limit: Optional[int] = None
    cpu_limit_millicores: Optional[int] = None
    gpu_memory_total: Optional[int] = None
    last_memory_sample: Optional[int] = None
    samples: int = 0

    @property
    def from_cgroup(self) -> bool:
        return (self.memory_source or "").startswith(("cgroup", "sampled_cgroup")) or self.cpu_source == "cgroup"

    def censored(self, status: Union[RunStatus, str]) -> "MeasurementResult":
        """Apply the status-dependent censoring rules of SPEC §5.7."""
        try:
            return apply_censoring(self, RunStatus.parse(status))  # type: ignore[arg-type]
        except Exception as e:
            log.warning("rightsizer: censoring failed: %s", e)
            return self


def apply_censoring(r: MeasurementResult, status: RunStatus) -> MeasurementResult:
    """SPEC §5.7 rules that depend on the outcome.

    - OOM-class failure → ``memory_peak = AtLeast(limit or last sample)``;
    - success within 5% of the memory limit → ``AtLeast(peak)``;
    - CPU throttled above 5% → ``AtLeast`` for CPU;
    - GPU OOM → ``gpu_memory_peak = AtLeast(device memory or peak)``;
    - ``Cached`` → no measurements.
    """
    if status is RunStatus.CACHED:
        return replace(
            r, memory_peak=None, cpu_mean=None, cpu_peak_window=None, cpu_throttled_frac=None,
            gpu_memory_peak=None, gpu_util_mean=None, t_peak_frac=None,
        )
    mem = r.memory_peak
    if status in OOM_STATUSES:
        base = r.memory_limit if r.memory_limit is not None else (
            r.last_memory_sample if r.last_memory_sample is not None else (mem.value if mem else None)
        )
        if base is not None:
            v = max(float(base), mem.value if mem else 0.0)
            mem = Measurement(v, Censor.AT_LEAST, mem.resolution_s if mem else None)
    elif mem is not None and status is RunStatus.SUCCEEDED and r.memory_limit:
        if mem.value >= NEAR_LIMIT_FRAC * r.memory_limit and mem.censor is Censor.EXACT:
            mem = mem.with_censor(Censor.AT_LEAST)
    cpu_mean, cpu_peak = r.cpu_mean, r.cpu_peak_window
    if r.cpu_throttled_frac is not None and r.cpu_throttled_frac > THROTTLE_FRAC:
        if cpu_mean is not None and cpu_mean.censor is Censor.EXACT:
            cpu_mean = cpu_mean.with_censor(Censor.AT_LEAST)
        if cpu_peak is not None and cpu_peak.censor is Censor.EXACT:
            cpu_peak = cpu_peak.with_censor(Censor.AT_LEAST)
    gpu = r.gpu_memory_peak
    if status in GPU_OOM_STATUSES:
        base = r.gpu_memory_total if r.gpu_memory_total is not None else (gpu.value if gpu else None)
        if base is not None:
            gpu = Measurement(max(float(base), gpu.value if gpu else 0.0), Censor.AT_LEAST, gpu.resolution_s if gpu else None)
    return replace(r, memory_peak=mem, cpu_mean=cpu_mean, cpu_peak_window=cpu_peak, gpu_memory_peak=gpu)


# --------------------------------------------------------------------------- scope

_AUTO = object()


class MeasurementScope:
    """Measure the enclosed code. Context manager (sync and async) and reusable via :func:`measured`.

    Parameters
    ----------
    interval_s:
        Sampling interval: 1 s for Flyte (:data:`FLYTE_INTERVAL_S`), 0.5 s for
        Ray (:data:`RAY_INTERVAL_S`).
    attribution:
        ``"container"`` (the unit owns the cgroup) or ``"process"`` (reused
        worker: in-task RSS minus baseline).
    cgroup:
        ``"auto"`` (detect), a :class:`CgroupInfo` (inject, e.g. in tests), or
        ``None`` (disable cgroups).
    proc_root, sys_fs_cgroup:
        Roots for detection and process probes (tests point them at a tmp tree).
    nvml:
        ``True`` to try pynvml, ``False`` to skip, or a module object to use.
    gpu_shared:
        Devices are time-sliced / MPS: device memory includes other tenants →
        recorded ``AtMost``.
    memory_limit, cpu_limit_millicores:
        Override the limits read from the cgroup (used for the 5% rule).
    """

    def __init__(
        self,
        interval_s: float = FLYTE_INTERVAL_S,
        *,
        attribution: str = "container",
        cgroup: Any = "auto",
        proc_root: str = "/proc",
        sys_fs_cgroup: str = "/sys/fs/cgroup",
        nvml: Any = True,
        gpu_shared: bool = False,
        cpu_window_s: float = DEFAULT_CPU_WINDOW_S,
        memory_limit: Optional[int] = None,
        cpu_limit_millicores: Optional[int] = None,
        subtract_baseline: bool = True,
        rss_probe: Any = _AUTO,
    ):
        self.interval_s = max(0.01, float(interval_s)) if interval_s else FLYTE_INTERVAL_S
        self.attribution = attribution if attribution in ("container", "process") else "container"
        self._cgroup_arg = cgroup
        self.proc_root = proc_root
        self.sys_fs_cgroup = sys_fs_cgroup
        self._nvml_arg = nvml
        self.gpu_shared = gpu_shared
        self.cpu_window_s = max(float(cpu_window_s), self.interval_s)
        self._memory_limit_arg = memory_limit
        self._cpu_limit_arg = cpu_limit_millicores
        self.subtract_baseline = subtract_baseline
        self._rss_probe_arg = rss_probe

        self.result: Optional[MeasurementResult] = None
        self._lock = threading.Lock()  # sample state (held by the sampler while probing)
        self._life = threading.Lock()  # start/stop flags (never held across a probe)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._started = False
        self._stopped = False
        self._reset_state()

    # ---- state ----

    def _reset_state(self) -> None:
        self._cg: Optional[CgroupReader] = None
        self._rss: Optional[Callable[[], Optional[int]]] = None
        self._nv: Optional[_Nvml] = None
        self._t0 = 0.0
        self._started_at = utcnow()
        self._mem_max: Optional[int] = None
        self._mem_max_t: Optional[float] = None
        self._mem_last: Optional[int] = None
        self._rss_base: Optional[int] = None
        self._rss_max: Optional[int] = None
        self._rss_max_t: Optional[float] = None
        self._cpu0: Optional[float] = None  # seconds
        self._cpu_last: Optional[Tuple[float, float]] = None
        self._cpu_win: Deque[Tuple[float, float]] = collections.deque()
        self._cpu_peak_rate: Optional[float] = None
        self._thr0: Optional[Tuple[int, int, int]] = None
        self._gpu_max: Dict[int, int] = {}
        self._gpu_util_sum = 0.0
        self._gpu_util_n = 0
        self._samples = 0
        self._errors = 0

    def _cpu_now(self) -> Optional[float]:
        if self.attribution == "container" and self._cg is not None:
            u = self._cg.cpu_usage_usec()
            if u is not None:
                return u / 1e6
        if self.attribution == "container" and self._cg is not None:
            return None
        return process_cpu_seconds(include_children=True)

    # ---- lifecycle ----

    def start(self) -> "MeasurementScope":
        """Start sampling. Never raises."""
        try:
            with self._life:
                if self._started:
                    return self
                self._started = True
            self._t0 = time.monotonic()
            self._started_at = utcnow()
            if self.attribution == "container":
                info = self._cgroup_arg
                if isinstance(info, str) and info == "auto":
                    info = detect_cgroup(self.proc_root, self.sys_fs_cgroup)
                self._cg = CgroupReader(info) if isinstance(info, CgroupInfo) else None
            if self._rss_probe_arg is _AUTO:
                self._rss = make_rss_probe(self.proc_root)
            else:
                self._rss = self._rss_probe_arg
            if self._nvml_arg is not False and self._nvml_arg is not None:
                mod = None if self._nvml_arg is True else self._nvml_arg
                self._nv = _Nvml.open(mod, per_process=self.attribution == "process")
            if self._rss is not None:
                try:
                    self._rss_base = self._rss()
                except Exception:
                    self._rss_base = None
            try:
                self._cpu0 = self._cpu_now()
            except Exception:
                self._cpu0 = None
            if self._cg is not None:
                self._thr0 = self._safe(self._cg.cpu_throttle)
            self._sample()
            self._thread = threading.Thread(target=self._run, name="rightsizer-sampler", daemon=True)
            self._thread.start()
        except Exception as e:
            log.warning("rightsizer: measurement start failed: %s", e)
        return self

    def _safe(self, fn: Callable[[], Any]) -> Any:
        try:
            return fn()
        except Exception as e:
            self._errors += 1
            if self._errors <= 3:
                log.debug("rightsizer: probe failed: %s", e)
            return None

    def _run(self) -> None:
        while not self._stop.wait(self.interval_s):
            try:
                self._sample()
            except Exception as e:  # pragma: no cover - _sample guards itself
                log.debug("rightsizer: sample failed: %s", e)

    def _sample(self) -> None:
        now = time.monotonic()
        with self._lock:
            self._samples += 1
            if self._cg is not None:
                m = self._safe(self._cg.memory_current)
                if m is not None:
                    self._mem_last = m
                    if self._mem_max is None or m > self._mem_max:
                        self._mem_max, self._mem_max_t = m, now
            if self._rss is not None:
                r = self._safe(self._rss)
                if r is not None and (self._rss_max is None or r > self._rss_max):
                    self._rss_max, self._rss_max_t = r, now
            c = self._safe(self._cpu_now)
            if c is not None:
                self._cpu_last = (now, c)
                w = self._cpu_win
                w.append((now, c))
                while len(w) >= 2 and now - w[1][0] >= self.cpu_window_s:
                    w.popleft()
                t0, c0 = w[0]
                span = now - t0
                if span >= self.cpu_window_s * 0.999 and span > 0:
                    rate = max(0.0, (c - c0) / span)
                    if self._cpu_peak_rate is None or rate > self._cpu_peak_rate:
                        self._cpu_peak_rate = rate
            if self._nv is not None:
                res = self._safe(self._nv.sample)
                if res:
                    used, util = res
                    for i, u in enumerate(used):
                        if u is not None and u > self._gpu_max.get(i, -1):
                            self._gpu_max[i] = u
                    if util:
                        self._gpu_util_sum += max(util)
                        self._gpu_util_n += 1

    def stop(self, status: Optional[Union[RunStatus, str]] = None) -> Optional[MeasurementResult]:
        """Stop sampling and compute the result (censored for ``status`` if given). Never raises."""
        try:
            with self._life:
                if not self._started or self._stopped:
                    return self.result
                self._stopped = True
            self._stop.set()
            stuck = False
            if self._thread is not None:
                self._thread.join(timeout=max(2.0, 2 * self.interval_s))
                stuck = self._thread.is_alive()
            if stuck:
                # The sampler is blocked inside a probe (e.g. NVML on a failed GPU) and holds
                # the lock: a final sample would hang the task. Use what was sampled, and do
                # not shut NVML down under the blocked call.
                log.warning("rightsizer: sampler did not stop; measurement uses the samples so far")
                self._nv = None
            else:
                try:
                    self._sample()
                except Exception:
                    pass
            r = self._compute()
            if status is not None and r is not None:
                r = r.censored(status)
            self.result = r
        except Exception as e:
            log.warning("rightsizer: measurement stop failed: %s", e)
            self.result = None
        finally:
            if self._nv is not None:
                self._nv.close()
                self._nv = None
        return self.result

    def _compute(self) -> MeasurementResult:
        end = time.monotonic()
        dur = max(0.0, end - self._t0)
        res = self.interval_s
        mem: Optional[Measurement] = None
        mem_src: Optional[str] = None
        peak_t: Optional[float] = None
        mem_limit = self._memory_limit_arg
        cpu_limit = self._cpu_limit_arg

        if self.attribution == "container":
            cg = self._cg
            if cg is not None:
                if mem_limit is None:
                    mem_limit = self._safe(cg.memory_limit)
                if cpu_limit is None:
                    cpu_limit = self._safe(cg.cpu_limit_millicores)
                kpeak = self._safe(cg.memory_peak)
                if kpeak is not None:
                    v = max(kpeak, self._mem_max or 0)
                    mem, mem_src = Measurement(float(v), Censor.EXACT, None), cg.kind
                    peak_t = self._mem_max_t
                elif self._mem_max is not None:
                    mem, mem_src = Measurement(float(self._mem_max), Censor.EXACT, res), "sampled_cgroup"
                    peak_t = self._mem_max_t
            if mem is None:
                ru = rusage_maxrss_bytes()
                cand = max([x for x in (ru, self._rss_max) if x is not None], default=None)
                if cand is not None:
                    # A lower bound on the container: misses non-descendant
                    # processes, unreaped children and page cache.
                    mem, mem_src = Measurement(float(cand), Censor.AT_LEAST, None), "rusage"
                    peak_t = self._rss_max_t if self._rss_max is not None and cand == self._rss_max else None
        else:
            if self._rss_max is not None:
                base = self._rss_base if (self.subtract_baseline and self._rss_base is not None) else 0
                mem = Measurement(float(max(0, self._rss_max - base)), Censor.EXACT, res)
                mem_src, peak_t = "process_rss", self._rss_max_t
            else:
                # No RSS probe: rusage is process-lifetime (not attributable to the
                # task) and so is not even a valid lower bound for this task.
                mem = None

        # CPU
        cpu_mean = cpu_peak = None
        cpu_src = None
        if self._cpu0 is not None and self._cpu_last is not None and dur > 0:
            used = max(0.0, self._cpu_last[1] - self._cpu0)
            cgroup_cpu = self.attribution == "container" and self._cg is not None
            # Without a cgroup, container CPU is a lower bound (other processes).
            exact = cgroup_cpu or self.attribution == "process"
            censor = Censor.EXACT if exact else Censor.AT_LEAST
            cpu_src = "cgroup" if cgroup_cpu else "process"
            cpu_mean = Measurement(used / dur, censor, None)
            peak_rate = self._cpu_peak_rate if self._cpu_peak_rate is not None else used / dur
            cpu_peak = Measurement(peak_rate, censor, res)

        thr_frac = None
        if self._cg is not None and self._thr0 is not None:
            thr1 = self._safe(self._cg.cpu_throttle)
            if thr1 is not None:
                dp = thr1[0] - self._thr0[0]
                dt = thr1[1] - self._thr0[1]
                if dp > 0:
                    thr_frac = min(1.0, max(0.0, dt / dp))

        gpu = None
        gpu_total = None
        gpu_util = None
        if self._gpu_max:
            busiest = max(self._gpu_max.values())
            censor = Censor.AT_MOST if self.gpu_shared else Censor.EXACT
            gpu = Measurement(float(busiest), censor, res)
            gpu_total = self._nv.total if self._nv is not None else None
        if self._gpu_util_n:
            gpu_util = min(1.0, max(0.0, self._gpu_util_sum / self._gpu_util_n))

        tpf = None
        if peak_t is not None and dur > 0:
            tpf = min(1.0, max(0.0, (peak_t - self._t0) / dur))

        return MeasurementResult(
            started_at=self._started_at,
            duration_s=dur,
            attribution=self.attribution,
            memory_source=mem_src,
            cpu_source=cpu_src,
            memory_peak=mem,
            cpu_mean=cpu_mean,
            cpu_peak_window=cpu_peak,
            cpu_throttled_frac=thr_frac,
            gpu_memory_peak=gpu,
            gpu_util_mean=gpu_util,
            t_peak_frac=tpf,
            memory_limit=mem_limit,
            cpu_limit_millicores=cpu_limit,
            gpu_memory_total=gpu_total,
            last_memory_sample=self._mem_last if self._mem_last is not None else self._rss_max,
            samples=self._samples,
        )

    # ---- context manager ----

    def __enter__(self) -> "MeasurementScope":
        return self.start()

    def __exit__(self, et: Any, ev: Any, tb: Any) -> bool:
        self.stop(RunStatus.SUCCEEDED if et is None else RunStatus.FAILED)
        return False

    async def __aenter__(self) -> "MeasurementScope":
        return self.start()

    async def __aexit__(self, et: Any, ev: Any, tb: Any) -> bool:
        self.stop(RunStatus.SUCCEEDED if et is None else RunStatus.FAILED)
        return False


F = TypeVar("F", bound=Callable[..., Any])


def _call_guarded(cb: Optional[Callable[[Optional[MeasurementResult]], Any]], r: Optional[MeasurementResult]) -> None:
    if cb is None:
        return
    try:
        cb(r)
    except Exception as e:
        log.warning("rightsizer: on_result callback failed: %s", e)


def measured(
    fn: Optional[F] = None,
    *,
    on_result: Optional[Callable[[Optional[MeasurementResult]], Any]] = None,
    **scope_kwargs: Any,
) -> Any:
    """Decorator: run ``fn`` (sync or async) in a fresh :class:`MeasurementScope`.

    ``on_result(result)`` is called after each call (guarded; never raises).
    The function's own return value and exceptions pass through unchanged.
    """

    def deco(f: F) -> F:
        if inspect.iscoroutinefunction(f):

            @functools.wraps(f)
            async def _aw(*a: Any, **k: Any) -> Any:
                scope = MeasurementScope(**scope_kwargs)
                scope.start()
                status = RunStatus.FAILED
                try:
                    out = await f(*a, **k)
                    status = RunStatus.SUCCEEDED
                    return out
                finally:
                    _call_guarded(on_result, scope.stop(status))

            return _aw  # type: ignore[return-value]

        @functools.wraps(f)
        def _w(*a: Any, **k: Any) -> Any:
            scope = MeasurementScope(**scope_kwargs)
            scope.start()
            status = RunStatus.FAILED
            try:
                out = f(*a, **k)
                status = RunStatus.SUCCEEDED
                return out
            finally:
                _call_guarded(on_result, scope.stop(status))

        return _w  # type: ignore[return-value]

    return deco(fn) if fn is not None else deco


__all__ = [
    "CgroupInfo",
    "CgroupReader",
    "FLYTE_INTERVAL_S",
    "MeasurementResult",
    "MeasurementScope",
    "RAY_INTERVAL_S",
    "apply_censoring",
    "detect_cgroup",
    "make_rss_probe",
    "measured",
    "process_cpu_seconds",
    "rusage_maxrss_bytes",
]
