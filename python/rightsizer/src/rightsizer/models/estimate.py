"""Per-axis option probabilities -> an ``Estimate`` (SPEC §5.5, models.md §4).

The JSON is exactly what serde writes for ``rightsizer_core::Estimate``:
``{"v": 1, "kind": "demand"|"action", "memory": {"kind": "cells", "grid":
"rs-grid-v1", "probs": [...]}, "cpu_mean": ..., "cpu_peak": ...,
"gpu_memory": ..., "provenance": {"estimator", "version", "grid", "features",
"latency_ms"}}``; absent axes are omitted (``skip_serializing_if``).

Read-outs:

- ``kind="demand"`` (default): the calibrated probabilities are the demand
  distribution and rightsizer's expected-cost decode picks the request. The GPU
  axis applies the release's ``gpu_threshold`` as the reference decode does
  (``tuner:bench_all/scoring.decode``): no GPU when P(none) >= threshold (a
  point mass at "none"), else P(gpu | some GPU) over the GPU cells.
- ``kind="action"``: a one-hot policy at each axis' argmax (the ``-argmax``
  rows; ``rightsize_pointer.request``), decoded by argmax over the feasible set.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from typing import Any

from .grids import AXIS_VALUES, MIB, RS_GRID_V1, option_values, to_rs_grid

#: Estimate axis names (``EstimateAxis`` serde spellings; ``duration_s`` accepted too).
ESTIMATE_AXES = ("memory", "cpu_mean", "cpu_peak", "gpu_memory", "duration")
#: The tuner's label headroom (``tuner:bench/grid.py:LABEL_HEADROOM``).
LABEL_HEADROOM = 1.05


def normalize_axes(axes: Sequence[str] | None) -> list[str] | None:
    if axes is None:
        return None
    return ["duration" if a == "duration_s" else a for a in axes]


def _norm(p: Sequence[float]) -> list[float]:
    """Normalize; NaN, negative and infinite entries are refused (an infinite one
    would turn every other cell into 0 or NaN)."""
    q = [float(x) for x in p]
    if any(math.isnan(x) or math.isinf(x) for x in q):
        raise ValueError("probabilities are not finite")
    s = float(sum(x for x in q if x > 0))
    if not s > 0:
        raise ValueError("probabilities sum to zero")
    return [max(x, 0.0) / s for x in q]


def _onehot(n: int, i: int) -> list[float]:
    out = [0.0] * n
    out[i] = 1.0
    return out


def _argmax(p: Sequence[float]) -> int:
    return max(range(len(p)), key=p.__getitem__)


def cells(probs: Sequence[float], grid: str = RS_GRID_V1) -> dict:
    return {"kind": "cells", "grid": grid, "probs": [float(x) for x in probs]}


def need_to_demand(probs: Sequence[float], option_bytes: Sequence[float], overhead_bytes: float,
                   headroom: float = LABEL_HEADROOM) -> list[float]:
    """Memory probabilities over *need* cells (the tuner's label: (peak +
    runtime) x headroom) -> over rs-grid-v1 *demand* cells (SPEC §5.2), with
    demand = need / headroom - overhead, spread log-uniformly within each cell
    (cell 0 from half its value), as rightsizer's normalization reads cells."""
    dst = AXIS_VALUES["memory"]
    out = [0.0] * len(dst)
    floor = 1.0 * MIB
    for j, p in enumerate(probs):
        if not p > 0:
            continue
        hi = option_bytes[j]
        lo = option_bytes[j - 1] if j else hi / 2
        a = max(lo / headroom - overhead_bytes, floor)
        b = max(hi / headroom - overhead_bytes, floor * 1.0001)
        if b <= a:
            a = b / 1.0001
        la, lb = math.log(a), math.log(b)
        for k, v in enumerate(dst):
            c_lo = dst[k - 1] if k else 0.0
            ov_lo, ov_hi = max(a, c_lo), min(b, v)
            if k == len(dst) - 1:
                ov_hi = b
            if ov_hi > ov_lo:
                out[k] += p * (math.log(ov_hi) - math.log(ov_lo)) / (lb - la)
    return out


def build_estimate(
    probs: Mapping[str, Sequence[float]],
    options: Mapping[str, Sequence[str]],
    *,
    estimator: str,
    version: str,
    kind: str = "demand",
    gpu_threshold: float | None = None,
    axes: Sequence[str] | None = None,
    cpu_fields: Sequence[str] = ("cpu_mean", "cpu_peak"),
    memory_basis: str = "demand",
    memory_overhead_bytes: float = 0.0,
    features: str = "features-v1",
    latency_ms: int = 0,
) -> dict:
    """An ``Estimate`` dict from per-key option probabilities (``memory``,
    ``cpu``, ``gpu``, each over its options as written in the prompt).

    ``memory_basis="demand"`` (the default) converts the tuner's need label,
    need = (peak + runtime) x 1.05, to the demand rightsizer estimators emit
    (SPEC §5.2; :func:`need_to_demand` with ``memory_overhead_bytes``), so the
    decision's own overhead and headroom are not counted twice; ``"need"``
    maps the memory cells as they are (models.md §4: "probabilities map cell
    to cell"; conservative by up to about a cell)."""
    if kind not in ("demand", "action"):
        raise ValueError(f"kind must be demand or action, not {kind!r}")
    want = set(normalize_axes(axes) or ESTIMATE_AXES)
    est: dict[str, Any] = {"v": 1, "kind": kind}
    if "memory" in probs and "memory" in want:
        p = _norm(probs["memory"])
        if kind == "action":
            p = _onehot(len(p), _argmax(p))
        if memory_basis == "demand" and kind == "demand":
            rs = need_to_demand(p, option_values("memory", options["memory"]), memory_overhead_bytes)
        elif memory_basis in ("need", "demand"):
            rs = to_rs_grid("memory", options["memory"], p)
        else:
            raise ValueError(f"memory_basis must be need or demand, not {memory_basis!r}")
        est["memory"] = cells(rs)
    if "cpu" in probs:
        p = _norm(probs["cpu"])
        if kind == "action":
            p = _onehot(len(p), _argmax(p))
        rs = to_rs_grid("cpu", options["cpu"], p)
        for f in cpu_fields:
            if f in want:
                est[f] = cells(rs)
    if "gpu" in probs and "gpu_memory" in want:
        p = _norm(probs["gpu"])
        vals = option_values("gpu", options["gpu"])
        none = [i for i, v in enumerate(vals) if v == 0.0]
        if kind == "action":
            p = _onehot(len(p), _argmax(p))
        elif gpu_threshold is not None and none:
            p_none = sum(p[i] for i in none)
            if p_none >= gpu_threshold:
                p = [1.0 if i == none[0] else 0.0 for i in range(len(p))]
            else:
                rest = [0.0 if i in none else x for i, x in enumerate(p)]
                p = _norm(rest)
        est["gpu_memory"] = cells(to_rs_grid("gpu", options["gpu"], p))
    est["provenance"] = {"estimator": estimator, "version": version, "grid": RS_GRID_V1, "features": features,
                         "latency_ms": int(latency_ms)}
    return est


def argmax_request(probs: Mapping[str, Sequence[float]], options: Mapping[str, Sequence[str]]) -> dict[str, str]:
    """``rightsize_pointer.request``: the most likely option on each axis."""
    return {k: options[k][_argmax(list(p))] for k, p in probs.items()}
