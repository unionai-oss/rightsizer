"""The generative read-out (models.md §1): ``n`` sampled answers -> an
empirical distribution over rs-grid-v1 cells per axis.

An answer is the last JSON-ish object in the text that has a usable
``memory``; values snap **up** onto the grid (the tuner's generous reading,
``tuner:bench_all/prompting.parse_answer``). A missing ``cpu`` reads as 1 core
and a missing ``gpu`` as none, as the tuner does. Parsing tolerates code
fences, single quotes, trailing commas, bare numbers and units (``"2Gi"``,
``"2 GiB"``, ``"2048Mi"``, ``2048`` = MiB, ``"500m"`` CPU, ``"22GiB"`` /
``"22"`` / ``"none"`` GPU).
"""

from __future__ import annotations

import json
import math
import re
import time
from collections.abc import Mapping, Sequence
from typing import Any

from .estimate import cells, normalize_axes
from .grids import AXIS_VALUES, RS_GRID_V1, covering, parse_cpu_cores, parse_gpu_bytes, parse_memory_bytes

_OBJ = re.compile(r"\{[^{}]*\}", re.DOTALL)


def _loads(s: str) -> Any:
    try:
        return json.loads(s)
    except ValueError:
        pass
    t = re.sub(r",\s*([}\]])", r"\1", s)            # trailing commas
    t = re.sub(r"(?<![\w\"])'([^'\"]*)'", r'"\1"', t)  # 'single' -> "double"
    t = re.sub(r"([{,]\s*)([A-Za-z_]\w*)\s*:", r'\1"\2":', t)  # bare keys
    try:
        return json.loads(t)
    except ValueError:
        return None


def parse_answer(text: str) -> dict[str, int] | None:
    """``text`` -> rs-grid-v1 cell indices ``{"memory", "cpu", "gpu_memory"}``, or None."""
    for m in reversed(_OBJ.findall(text or "")):
        obj = _loads(m)
        if not isinstance(obj, dict):
            continue
        obj = {str(k).strip().lower(): v for k, v in obj.items()}
        if "memory" not in obj:
            continue
        mem = parse_memory_bytes(obj["memory"])
        if mem is None or not mem > 0 or not math.isfinite(mem):
            continue
        cpu = parse_cpu_cores(obj.get("cpu", 1))
        if cpu is None or not cpu > 0:
            cpu = 1.0
        gpu = parse_gpu_bytes(obj.get("gpu", "none"))
        if gpu is None:
            gpu = 0.0
        return {"memory": covering(AXIS_VALUES["memory"], mem), "cpu": covering(AXIS_VALUES["cpu"], cpu),
                "gpu_memory": covering(AXIS_VALUES["gpu_memory"], gpu)}
    return None


def empirical_estimate(texts: Sequence[str], *, estimator: str, version: str, kind: str = "demand",
                       axes: Sequence[str] | None = None, smoothing: float = 0.0,
                       cpu_fields: Sequence[str] = ("cpu_mean", "cpu_peak"), latency_ms: int = 0) -> dict | None:
    """Samples -> an ``Estimate`` (counts / parsed samples, plus ``smoothing``
    pseudo-counts spread uniformly); None when no sample parses."""
    picks = [p for p in (parse_answer(t) for t in texts) if p is not None]
    if not picks:
        return None
    want = set(normalize_axes(axes) or ("memory", "cpu_mean", "cpu_peak", "gpu_memory"))

    def dist(axis: str, key: str) -> list[float]:
        n = len(AXIS_VALUES[axis])
        c = [smoothing] * n
        for p in picks:
            c[p[key]] += 1.0
        s = sum(c)
        return [x / s for x in c]

    est: dict[str, Any] = {"v": 1, "kind": kind}
    if "memory" in want:
        est["memory"] = cells(dist("memory", "memory"))
    cpu = dist("cpu", "cpu")
    for f in cpu_fields:
        if f in want:
            est[f] = cells(cpu)
    if "gpu_memory" in want:
        est["gpu_memory"] = cells(dist("gpu_memory", "gpu_memory"))
    est["provenance"] = {"estimator": estimator, "version": version, "grid": RS_GRID_V1, "features": "features-v1",
                         "latency_ms": int(latency_ms)}
    return est


class GenerativeModel:
    """Any chat model behind a ``generate`` backend, read out by sampling."""

    def __init__(self, backend: Any, *, model_id: str, version: str = "unknown", n: int = 8,
                 temperature: float = 1.0, max_tokens: int = 128, kind: str = "demand", smoothing: float = 0.0,
                 prompt_options: Mapping[str, Any] | None = None, extra: Mapping[str, Any] | None = None):
        self.backend, self.model_id, self.version = backend, model_id, version
        self.n, self.temperature, self.max_tokens = n, temperature, max_tokens
        self.kind, self.smoothing = kind, smoothing
        self.prompt_options = dict(prompt_options or {})
        self.extra = dict(extra or {})

    def estimate_batch(self, contexts: Sequence[Mapping[str, Any]], axes: Sequence[str] | None = None) -> list:
        from rightsizer.serve.errors import Insufficient, as_error

        from . import prompting
        from .pointer import _unrenderable

        out: list = []
        for c in contexts:
            t0 = time.monotonic()
            try:
                msgs = prompting.messages_from_context(c, **self.prompt_options)
            except Exception as e:  # noqa: BLE001 -- this context's slot only
                out.append(_unrenderable(e))
                continue
            try:
                texts = self.backend.generate(msgs, self.n, temperature=self.temperature,
                                              max_tokens=self.max_tokens, **self.extra)
            except Exception as e:  # noqa: BLE001
                out.append(as_error(e))
                continue
            est = empirical_estimate(texts, estimator=self.model_id, version=self.version, kind=self.kind, axes=axes,
                                     smoothing=self.smoothing,
                                     latency_ms=int((time.monotonic() - t0) * 1000))
            out.append(est if est is not None else Insufficient(f"none of {len(texts)} samples had a usable answer"))
        return out

    def info(self, max_batch: int = 8) -> dict:
        return {"id": self.model_id, "version": self.version, "grids": [RS_GRID_V1],
                "axes": ["memory", "cpu_mean", "cpu_peak", "gpu_memory"], "kind": self.kind,
                "needs": {"code": True, "history": True, "input_bytes": True}, "features": "features-v1",
                "max_batch": max_batch, "backend": getattr(self.backend, "name", "generate")}

    def ready(self) -> bool:
        r = getattr(self.backend, "ready", None)
        return bool(r()) if callable(r) else True
