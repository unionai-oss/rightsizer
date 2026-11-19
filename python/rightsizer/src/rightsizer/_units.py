"""Quantities in pure Python, with the semantics of ``rightsizer_core::units``
(K8s quantity syntax, rounded up) and the accelerator catalog (SPEC §5.1.1).

Used to build typed objects (``rs.Resources``) without ``_core``; the wire
tests check every case against ``_core.normalize_quantity``.
"""

from __future__ import annotations

import math
import re
from fractions import Fraction
from typing import Optional, Tuple, Union

from rightsizer_runtime.wire import ACCELERATORS, GpuShape

U64_MAX = 2**64 - 1
U32_MAX = 2**32 - 1

_K8S = re.compile(r"^([+-]?)(\d*)(?:\.(\d*))?(Ki|Mi|Gi|Ti|Pi|Ei|[eE][+-]?\d+|n|u|m|k|M|G|T|P|E)?$")
_BIN = {"Ki": 10, "Mi": 20, "Gi": 30, "Ti": 40, "Pi": 50, "Ei": 60}
_DEC = {"n": -9, "u": -6, "m": -3, "k": 3, "M": 6, "G": 9, "T": 12, "P": 15, "E": 18}

Quantity = Union[str, int, float]


def k8s_quantity(s: str) -> Fraction:
    """The exact value of a K8s quantity string."""
    t = s.strip()
    if not t:
        raise ValueError("empty quantity")
    m = _K8S.match(t)
    if not m or (not m.group(2) and not m.group(3)):
        raise ValueError(f"invalid quantity {s!r}")
    sign, ip, fp, suf = m.group(1), m.group(2) or "", m.group(3) or "", m.group(4) or ""
    v = Fraction(int(ip + fp or "0"), 10 ** len(fp))
    if suf in _BIN:
        v *= 2 ** _BIN[suf]
    elif suf in _DEC:
        v *= Fraction(10) ** _DEC[suf]
    elif suf:
        e = int(suf[1:])
        if abs(e) > 60:
            raise ValueError(f"invalid quantity {s!r}")
        v *= Fraction(10) ** e
    return -v if sign == "-" else v


def _ceil(v: Fraction, src: str, hi: int) -> int:
    if v < 0:
        raise ValueError(f"negative quantity {src!r}")
    n = math.ceil(v)
    if n > hi:
        raise ValueError(f"quantity {src!r} overflows")
    return n


def memory_bytes(q: Quantity) -> int:
    """Bytes from ``"64Gi"`` / ``"129M"`` / an int or float number of bytes (rounded up)."""
    if isinstance(q, bool):
        raise TypeError("a quantity cannot be a bool")
    if isinstance(q, int):
        return _ceil(Fraction(q), str(q), U64_MAX)
    if isinstance(q, float):
        if not math.isfinite(q):
            raise ValueError(f"invalid quantity {q!r}")
        return _ceil(Fraction(repr(q)), repr(q), U64_MAX)  # decimal text, as Rust sees it
    if isinstance(q, str):
        return _ceil(k8s_quantity(q), q, U64_MAX)
    raise TypeError(f"a memory quantity must be str, int or float, not {type(q).__name__}")


def cpu_millis(q: Quantity) -> int:
    """Millicores from ``"500m"`` / ``"2"`` / a number of cores (rounded up)."""
    if isinstance(q, bool):
        raise TypeError("a quantity cannot be a bool")
    if isinstance(q, int):
        return _ceil(Fraction(q) * 1000, str(q), U32_MAX)
    if isinstance(q, float):
        if not math.isfinite(q):
            raise ValueError(f"invalid quantity {q!r}")
        return _ceil(Fraction(repr(q)) * 1000, repr(q), U32_MAX)
    if isinstance(q, str):
        return _ceil(k8s_quantity(q) * 1000, q, U32_MAX)
    raise TypeError(f"a CPU quantity must be str, int or float, not {type(q).__name__}")


_ACCEL_BY_LOWER = {a.lower(): a for a in ACCELERATORS}


def accelerator(name: str) -> str:
    """The canonical catalog id (case-insensitive); unknown ids are errors."""
    a = _ACCEL_BY_LOWER.get(name.strip().lower())
    if a is None:
        raise ValueError(f"unknown accelerator {name!r}; catalog ids: {', '.join(sorted(ACCELERATORS))}")
    return a


def gpu_shape(g: Union[str, int, GpuShape, Tuple[str, int]]) -> GpuShape:
    """``"A100-80G:2"``, ``"L4-24G"`` (one device), ``("H100-80G", 8)``, or a
    bare count (an ``unknown-gpu``: its type is never changed)."""
    if isinstance(g, GpuShape):
        return g
    if isinstance(g, bool):
        raise TypeError("a GPU cannot be a bool")
    if isinstance(g, int):
        if g < 1:
            raise ValueError("a GPU count must be ≥ 1")
        return GpuShape("unknown-gpu", g)
    if isinstance(g, tuple) and len(g) == 2:
        return GpuShape(accelerator(str(g[0])), _count(g[1], g))
    if isinstance(g, str):
        name, sep, n = g.rpartition(":")
        if not sep:
            name, n = g, "1"
        try:
            count = int(n.strip())
        except ValueError:
            raise ValueError(f"invalid GPU shape {g!r}") from None
        return GpuShape(accelerator(name), _count(count, g))
    raise TypeError(f"a GPU must be 'ID:count', (id, count) or a count, not {type(g).__name__}")


def _count(n: object, src: object) -> int:
    if not isinstance(n, int) or isinstance(n, bool) or n < 1 or n > U32_MAX:
        raise ValueError(f"invalid GPU count in {src!r}")
    return n


def format_bytes(b: Optional[int]) -> Optional[str]:
    """``64Gi``-style text for display (exact binary suffix when one divides)."""
    if b is None:
        return None
    for suf, p in (("Ei", 60), ("Pi", 50), ("Ti", 40), ("Gi", 30), ("Mi", 20), ("Ki", 10)):
        if b and b % (1 << p) == 0:
            return f"{b >> p}{suf}"
    for suf, p in (("Ti", 40), ("Gi", 30), ("Mi", 20)):
        if b >= (1 << p):
            return f"{b / (1 << p):.2f}{suf}"
    return str(b)


def format_cpu(m: Optional[int]) -> Optional[str]:
    if m is None:
        return None
    return str(m // 1000) if m % 1000 == 0 else f"{m}m"


__all__ = ["accelerator", "cpu_millis", "format_bytes", "format_cpu", "gpu_shape", "k8s_quantity", "memory_bytes"]
