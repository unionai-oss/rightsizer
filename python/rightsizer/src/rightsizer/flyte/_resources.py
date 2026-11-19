"""``flyte.Resources`` ↔ rightsizer requests, and the merge rules (flyte.md §4.3).

Pure functions over duck-typed ``flyte.Resources`` (``cpu``, ``memory``,
``gpu``, ``disk``, ``shm``); ``flyte`` is imported only to build the merged
``flyte.Resources`` (and ``flyte.GPU`` on a type change).
"""

from __future__ import annotations

from typing import Any, Dict, FrozenSet, Iterable, Optional, Tuple

from rightsizer_runtime.wire import GpuShape, Limits, Requests, ResourceSpec

from .. import _units
from ..types import Resources as RsResources

# Flyte GPU device names → catalog ids (SPEC §5.1.1). Anything else is
# ``unknown-gpu``: its type is never changed.
FLYTE_TO_CATALOG: Dict[str, str] = {
    "T4": "T4-16G",
    "L4": "L4-24G",
    "A10G": "A10G-24G",
    "A10": "A10-24G",
    "L40s": "L40S-48G",
    "A100": "A100-40G",
    "A100_80G": "A100-80G",
    "A100 80G": "A100-80G",  # deprecated spelling, still accepted by flyte
    "H100": "H100-80G",
    "H200": "H200-141G",
    "B200": "B200-180G",
    "MI300X": "MI300X-192G",
    "V5E": "TPU-V5E",
    "V5P": "TPU-V5P",
    "V6E": "TPU-V6E",
    "Trn1": "TRN1",
    "Trn2": "TRN2",
    "Inf2": "INF2",
}
CATALOG_TO_FLYTE: Dict[str, str] = {v: k for k, v in FLYTE_TO_CATALOG.items() if k != "A100 80G"}

# flyte.md pin axes → rightsizer axes.
_PIN_AXES = {
    "memory": ("memory",),
    "cpu": ("cpu",),
    "gpu": ("gpu_count", "gpu_type"),
    "gpu_count": ("gpu_count",),
    "gpu_type": ("gpu_type",),
    "disk": ("ephemeral_storage",),
    "ephemeral_storage": ("ephemeral_storage",),
    "shm": ("shm",),
}


def pin_axes(pins: Iterable[str]) -> Tuple[str, ...]:
    out = []
    for p in pins:
        for a in _PIN_AXES.get(p, (p,)):
            if a not in out:
                out.append(a)
    return tuple(out)


def axes_set(resources: Any) -> FrozenSet[str]:
    """The flyte.md axes an explicit ``flyte.Resources`` sets (they become pins, §3.3)."""
    if resources is None:
        return frozenset()
    out = set()
    if getattr(resources, "memory", None) is not None:
        out.add("memory")
    if getattr(resources, "cpu", None) is not None:
        out.add("cpu")
    if getattr(resources, "gpu", None) is not None:
        out.update(("gpu", "gpu_type"))
    if getattr(resources, "disk", None) is not None:
        out.add("disk")
    if getattr(resources, "shm", None) is not None:
        out.add("shm")
    return frozenset(out)


def _pair(v: Any) -> Tuple[Any, Any]:
    if isinstance(v, tuple):
        return v[0], v[1]
    return v, None


def gpu_shape(gpu: Any) -> Optional[GpuShape]:
    """A ``flyte.Resources.gpu`` value as a catalog shape (``unknown-gpu`` when untyped or unknown)."""
    if gpu is None:
        return None
    if isinstance(gpu, bool):
        return None
    if isinstance(gpu, int):
        return None if gpu == 0 else GpuShape(accel="unknown-gpu", count=gpu)
    if isinstance(gpu, str):
        device, _, n = gpu.partition(":")
        count = int(n or 1)
        return GpuShape(accel=FLYTE_TO_CATALOG.get(device, "unknown-gpu"), count=count)
    # flyte.Device(quantity, device_class, device, partition)
    q = int(getattr(gpu, "quantity", 1) or 1)
    device = getattr(gpu, "device", None)
    if getattr(gpu, "partition", None):
        return GpuShape(accel="unknown-gpu", count=q)  # MIG / TPU slices: never changed
    return GpuShape(accel=FLYTE_TO_CATALOG.get(device or "", "unknown-gpu"), count=q)


def to_spec(resources: Any) -> ResourceSpec:
    """``flyte.Resources`` → ``ResourceSpec`` (requests, and limits where a tuple gives one)."""
    if resources is None:
        return ResourceSpec()
    cr, cl = _pair(getattr(resources, "cpu", None))
    mr, ml = _pair(getattr(resources, "memory", None))
    disk = getattr(resources, "disk", None)
    shm = getattr(resources, "shm", None)
    return ResourceSpec(
        requests=Requests(
            cpu=None if cr is None else _units.cpu_millis(cr),
            memory=None if mr is None else _units.memory_bytes(mr),
            gpu=gpu_shape(getattr(resources, "gpu", None)),
            ephemeral_storage=None if disk is None else _units.memory_bytes(disk),
            shm=None if shm in (None, "auto") else _units.memory_bytes(shm),
        ),
        limits=Limits(cpu=None if cl is None else _units.cpu_millis(cl), memory=None if ml is None else _units.memory_bytes(ml)),
    )


def to_rs(resources: Any) -> RsResources:
    s = to_spec(resources)
    return RsResources(requests=s.requests, limits=s.limits)


def requests_of(resources: Any) -> Optional[Requests]:
    """Requests only (for ceilings and floors)."""
    if resources is None:
        return None
    if isinstance(resources, Requests):
        return resources
    if isinstance(resources, RsResources):
        return resources.requests
    return to_spec(resources).requests


def memory_is_tuple(resources: Any) -> bool:
    return isinstance(getattr(resources, "memory", None), tuple)


def has_memory(resources: Any) -> bool:
    return getattr(resources, "memory", None) is not None


def fmt_memory(b: int) -> str:
    """Bytes as a Kubernetes quantity in whole MiB (rounded up), e.g. ``"1536Mi"``."""
    mib = -(-int(b) // (1 << 20))
    if mib % 1024 == 0 and mib >= 1024:
        return f"{mib // 1024}Gi"
    return f"{max(1, mib)}Mi"


def fmt_cpu(m: int) -> str:
    m = int(m)
    return str(m // 1000) if m % 1000 == 0 else f"{m}m"


def _scaled_limit(prior_req: Any, prior_lim: Any, new_req: int, parse: Any) -> int:
    """Keep the prior's limit/request ratio (S2: a limit only moves with its request)."""
    pr = parse(prior_req)
    pl = parse(prior_lim)
    if pr <= 0:
        return max(new_req, pl)
    return max(new_req, int(round(new_req * (pl / pr))))


def merge(prior: Any, applied: Requests, *, allow_gpu_type_change: bool = False) -> Any:
    """flyte.md §4.3: start from the prior; replace only the decided axes.

    - memory / cpu: a scalar stays a scalar (request only, no limit added, S2);
      a ``(request, limit)`` tuple stays a tuple and keeps its ratio;
    - gpu: kept, unless a type change is allowed and decided (then
      ``flyte.GPU(device, count)`` with Flyte's names); an ``int`` prior
      (``unknown-gpu``) is never retyped;
    - disk, shm: kept.

    Returns a new ``flyte.Resources`` (or ``prior`` itself when nothing changed).
    """
    import flyte

    if prior is None:
        return None
    cpu = getattr(prior, "cpu", None)
    mem = getattr(prior, "memory", None)
    gpu = getattr(prior, "gpu", None)
    changed = False
    if applied.memory is not None and mem is not None:
        mr, ml = _pair(mem)
        if _units.memory_bytes(mr) != applied.memory:
            if ml is None:
                mem = fmt_memory(applied.memory)
            else:
                new_lim = _scaled_limit(mr, ml, applied.memory, _units.memory_bytes)
                mem = (fmt_memory(applied.memory), fmt_memory(new_lim))
            changed = True
    if applied.cpu is not None and cpu is not None:
        cr, cl = _pair(cpu)
        if _units.cpu_millis(cr) != applied.cpu:
            if cl is None:
                cpu = fmt_cpu(applied.cpu)
            else:
                new_lim = _scaled_limit(cr, cl, applied.cpu, _units.cpu_millis)
                cpu = (fmt_cpu(applied.cpu), fmt_cpu(new_lim))
            changed = True
    if allow_gpu_type_change and applied.gpu is not None and gpu is not None and not isinstance(gpu, int):
        cur = gpu_shape(gpu)
        if cur is not None and cur.accel != "unknown-gpu" and applied.gpu.accel != cur.accel and applied.gpu.count == cur.count:
            name = CATALOG_TO_FLYTE.get(applied.gpu.accel)
            if name is not None:
                gpu = flyte.GPU(name, applied.gpu.count)  # type: ignore[arg-type]
                changed = True
    if not changed:
        return prior
    return flyte.Resources(cpu=cpu, memory=mem, gpu=gpu, disk=getattr(prior, "disk", None), shm=getattr(prior, "shm", None))


def scaled_memory(resources: Any, factor: float) -> Any:
    """``resources`` with memory (request and limit) multiplied by ``factor``."""
    import flyte

    mem = getattr(resources, "memory", None)
    if mem is None:
        return resources
    mr, ml = _pair(mem)
    nr = fmt_memory(int(_units.memory_bytes(mr) * factor))
    nm: Any = nr if ml is None else (nr, fmt_memory(int(_units.memory_bytes(ml) * factor)))
    return flyte.Resources(cpu=resources.cpu, memory=nm, gpu=resources.gpu, disk=resources.disk, shm=resources.shm)


__all__ = [
    "CATALOG_TO_FLYTE",
    "FLYTE_TO_CATALOG",
    "axes_set",
    "fmt_cpu",
    "fmt_memory",
    "gpu_shape",
    "has_memory",
    "memory_is_tuple",
    "merge",
    "pin_axes",
    "requests_of",
    "scaled_memory",
    "to_rs",
    "to_spec",
]
