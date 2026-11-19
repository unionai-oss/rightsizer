"""Grids (SPEC §5.3) as the models need them: ``rs-grid-v1`` and the tuner's
option lists, with conservative rebinning.

Mirrors ``crates/rightsizer-core/src/grid.rs`` (Rust is the source of truth):
a cell ``j`` of an axis with values ``v`` covers ``(v[j-1], v[j]]``; the GPU
axis' index 0 is "no GPU" (value 0). Pure Python, no NumPy.
"""

from __future__ import annotations

import re
from collections.abc import Sequence

RS_GRID_V1 = "rs-grid-v1"
MIB = 1 << 20
GIB = 1 << 30

#: The tuner's memory ladder, MiB (``tuner:bench/grid.py``).
TUNER_MEMORY_MIB = (256, 384, 512, 768, 1024, 1536, 2048, 3072, 4096, 6144, 8192, 12288, 16384, 24576, 32768,
                    49152, 65536)
#: The tuner's CPU steps, cores (``tuner:bench_cpu/grid.py``).
TUNER_CPU_CORES = (0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0, 2.5, 3.0, 3.5, 4.0, 5.0, 6.0, 7.0, 8.0)
#: The tuner's GPU memory cells, GiB (``tuner:bench_gpu/grid.py``).
TUNER_GPU_GIB = (1.0, 1.5, 2.0, 3.0, 4.0, 6.0, 8.0, 10.0, 12.0, 15.0, 22.0, 32.0, 44.0, 64.0, 88.0, 120.0, 176.0)

_EXTRA_MEMORY_GIB = (96, 128, 192, 256, 384, 512, 768, 1024, 1536, 2048)
_EXTRA_CPU_CORES = (10.0, 12.0, 16.0, 24.0, 32.0, 48.0, 64.0, 96.0, 128.0)
_EXTRA_GPU_GIB = (256.0, 352.0, 512.0, 640.0, 768.0, 1024.0, 1152.0, 1536.0)


def _gib_bytes(g: float) -> int:
    # Rust: Bytes::from_gib_f64 = round(g * 2^30)
    return int(round(g * GIB))


#: rs-grid-v1 axis values in natural units: memory and GPU memory in bytes, CPU in cores.
RS_MEMORY = tuple(m * MIB for m in TUNER_MEMORY_MIB) + tuple(g * GIB for g in _EXTRA_MEMORY_GIB)
RS_CPU = TUNER_CPU_CORES + _EXTRA_CPU_CORES
RS_GPU = (0,) + tuple(_gib_bytes(g) for g in TUNER_GPU_GIB + _EXTRA_GPU_GIB)

AXIS_VALUES = {"memory": RS_MEMORY, "cpu": RS_CPU, "gpu_memory": RS_GPU}


def covering(values: Sequence[float], v: float) -> int:
    """Index of the smallest value ``>= v`` (1e-9 relative tolerance), clamped to the top."""
    for i, x in enumerate(values):
        if v <= x * (1.0 + 1e-9) + 1e-12:
            return i
    return len(values) - 1


def rebin(src_values: Sequence[float], probs: Sequence[float], dst_values: Sequence[float]) -> list[float]:
    """Conservative rebinning (SPEC §5.3): each source cell's mass moves to the
    smallest target cell covering the source cell's upper edge."""
    out = [0.0] * len(dst_values)
    for i, p in enumerate(probs):
        if not p > 0.0:
            continue
        edge = src_values[i] if i < len(src_values) else float("inf")
        out[covering(dst_values, edge)] += float(p)
    return out


# ── option strings (the tuner's prompt spells cells this way) ───────────

_MEM = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(Ki|Mi|Gi|Ti|K|M|G|T|KiB|MiB|GiB|TiB|KB|MB|GB|TB)?\s*$", re.IGNORECASE)
_MEM_MULT = {"ki": 1 << 10, "mi": MIB, "gi": GIB, "ti": 1 << 40, "kib": 1 << 10, "mib": MIB, "gib": GIB,
             "tib": 1 << 40, "k": 1e3, "m": 1e6, "g": 1e9, "t": 1e12, "kb": 1e3, "mb": 1e6, "gb": 1e9, "tb": 1e12}


def parse_memory_bytes(s: str | float) -> float | None:
    """``"1.5Gi"`` / ``"768Mi"`` / ``"2GB"`` -> bytes; a bare number is MiB (the tuner's reading)."""
    if isinstance(s, bool):
        return None
    if isinstance(s, (int, float)):
        return float(s) * MIB
    m = _MEM.match(str(s))
    if not m:
        return None
    unit = (m.group(2) or "mi").lower()
    return float(m.group(1)) * _MEM_MULT[unit]


def parse_cpu_cores(s: str | float) -> float | None:
    """``"2"`` / ``2.5`` / ``"500m"`` / ``"2 cores"`` -> cores."""
    if isinstance(s, bool):
        return None
    if isinstance(s, (int, float)):
        return float(s)
    t = str(s).strip().lower()
    m = re.match(r"^(\d+(?:\.\d+)?)\s*m$", t)
    if m:
        return float(m.group(1)) / 1000.0
    m = re.search(r"\d+(?:\.\d+)?", t)
    return float(m.group(0)) if m else None


def parse_gpu_bytes(s: str | float | None) -> float | None:
    """``"none"`` -> 0; ``"22GiB"`` / ``"22"`` (GiB) / ``"512MiB"`` -> bytes."""
    if s is None:
        return 0.0
    t = str(s).strip().lower()
    if t in ("none", "no", "0", "", "null", "cpu", "false"):
        return 0.0
    m = re.search(r"(\d+(?:\.\d+)?)\s*([mg])?", t)
    if not m:
        return None
    x = float(m.group(1))
    return x * (MIB if (m.group(2) or "") == "m" else GIB)


def option_values(key: str, options: Sequence[str]) -> list[float]:
    """Natural-unit values of a tuner axis' option strings (bytes, cores, bytes)."""
    parse = {"memory": parse_memory_bytes, "cpu": parse_cpu_cores, "gpu": parse_gpu_bytes}[key]
    out = []
    for o in options:
        v = parse(o)
        if v is None:
            raise ValueError(f"cannot read {key} option {o!r}")
        out.append(v)
    return out


#: Tuner config key -> rs-grid-v1 axis.
KEY_AXIS = {"memory": "memory", "cpu": "cpu", "gpu": "gpu_memory"}


def to_rs_grid(key: str, options: Sequence[str], probs: Sequence[float]) -> list[float]:
    """Probabilities over a tuner axis' options -> probabilities over rs-grid-v1's cells.

    The tuner's options are prefixes of rs-grid-v1, so this is the identity
    padded with zeros; an option that is not a grid cell is rebinned
    conservatively (its mass goes to the smallest cell covering it)."""
    return rebin(option_values(key, options), probs, AXIS_VALUES[KEY_AXIS[key]])
