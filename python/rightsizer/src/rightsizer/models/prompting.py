"""The RightSize Bench prompt, ported from the tuner, and the mapping from a
rightsizer ``WorkloadContext`` to it (models.md §3).

Two layers:

1. **The port** (:func:`messages`, :func:`user_prompt`, :func:`format_history`,
   :func:`describe_inputs`, ...) is a byte-for-byte copy of
   ``resource_tuner/bench_all/prompting.py`` (and the helpers it imports from
   ``bench/prompting.py``, ``bench/spec.py``, ``bench/workloads.py``) over the
   tuner's *row* dicts (``platform``, ``task_code``, ``manifest``,
   ``inputs_desc``, ``history_json``, ``hint``). Golden tests compare it with
   the tuner's own builder.
2. **The mapping** (:func:`row_from_context`, :func:`messages_from_context`)
   turns a rightsizer ``WorkloadContext`` (the JSON dict serde writes) into such
   a row. Where the context cannot carry what the tuner rendered, the gap is
   rendered in the same style and documented in ``docs/IMPLEMENTATION-NOTES.md``
   ("rightsizer.models / serve").

Pure Python; no NumPy.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from typing import Any

from .grids import MIB, TUNER_CPU_CORES, TUNER_GPU_GIB, TUNER_MEMORY_MIB

# ── the port ────────────────────────────────────────────────────────────

SYSTEM = (
    "You size batch jobs before they run: Flyte tasks, or Kubernetes Jobs given as a Python module and its "
    "manifest. Given the code, the metadata of this run's inputs, and what earlier runs of the same job "
    "used, choose the cheapest request that runs it: memory, CPU cores, and a GPU or none. Memory is a hard "
    "limit: below the peak the container is OOM-killed and retried with double. A Flyte task's memory "
    "includes the Flyte runtime; a Kubernetes container's includes whatever the job loads to fetch its "
    "inputs, and files written to a memory-backed emptyDir (medium: Memory) are charged to it. "
    "CPU below the busiest 15 seconds' use throttles the task and "
    "it finishes late. Thread pools the code does not size get one thread. gpu is the peak GPU memory "
    "to provision, or none for a task that does not use CUDA; it lands on the cheapest machine that holds "
    "it -- T4 (15 GiB, $0.53/h), L4 (22 GiB, $0.98/h), A10G (22 GiB, $1.21/h), L40S (44 GiB, $2.24/h), "
    "4xL4 (88 GiB, $4.60/h), 8xT4 (120 GiB, $7.82/h), 4xL40S (176 GiB, $10.49/h) "
    "-- and a GPU pod comes with its host memory and cores. A multi-GPU machine only "
    "helps code that spreads its model over every visible GPU. A GPU task given no GPU "
    "fails at start; a GPU given to a CPU task is paid for the whole run."
)


def format_memory(mib: int) -> str:
    """``tuner:bench/grid.py:format_memory``."""
    if mib % 1024 == 0:
        return f"{mib // 1024}Gi"
    if mib > 1024 and (mib * 2) % 1024 == 0:
        return f"{mib / 1024:g}Gi"
    return f"{mib}Mi"


KEYS = ("memory", "cpu", "gpu")
MEM_OPTIONS = tuple(format_memory(m) for m in TUNER_MEMORY_MIB)
CPU_OPTIONS = tuple(f"{c:g}" for c in TUNER_CPU_CORES)
GPU_OPTIONS = ("none",) + tuple(f"{g:g}GiB" for g in TUNER_GPU_GIB)
OPTIONS = (MEM_OPTIONS, CPU_OPTIONS, GPU_OPTIONS)

#: The Flyte runtime's footprint the tuner adds to a Flyte task's history peaks, MiB (``bench_all/k8s.py``).
FLYTE_RUNTIME_MIB = 70.0
#: Bound on the inputs description (``bench/prompting.py``).
MAX_DESC_CHARS = 1600
HINT_HEADER = "### A tree model's estimate"
ANSWER = ('Answer with JSON only: {"memory": "<one memory option>", "cpu": "<one cpu option>", '
          '"gpu": "<one gpu option>"}')

_SCHEMA = re.compile(r", schema=\[.*?\]")


def _short_inputs(desc: str) -> str:
    return "; ".join(_SCHEMA.sub("", line) for line in desc.splitlines())


def _clip(text: str, n: int) -> str:
    return text if len(text) <= n else text[: n // 2] + " ... " + text[-n // 2:]


def format_history(history: list[dict], platform: str = "flyte") -> str:
    if not history:
        return "none -- this is the first run of this task"
    overhead = FLYTE_RUNTIME_MIB if platform == "flyte" else 0.0
    lines = []
    for h in history:
        parts = []
        if "peak_mem_mib" in h:
            parts.append(f"peak memory {h['peak_mem_mib'] + overhead:,.0f} MiB")
        if "peak_cores" in h:
            parts.append(f"peak {h['peak_cores']:.2f} cores (busiest 15 s)")
        if "mean_cores" in h:
            parts.append(f"mean {h['mean_cores']:.2f} cores")
        if "peak_gpu_mib" in h:
            parts.append(f"peak GPU memory {h['peak_gpu_mib'] / 1024:.2f} GiB, "
                         f"GPU busy {h.get('mean_util', 0):.0f}%")
        lines.append(f"- inputs {_short_inputs(h['inputs_desc'])} -> {', '.join(parts)}, {h.get('wall_s', 0):.0f} s")
    return "\n".join(lines)


def options_text() -> str:
    return "\n".join(f"{k}: {', '.join(o)}" for k, o in zip(KEYS, OPTIONS))


def user_prompt(row: Mapping[str, Any]) -> str:
    history = json.loads(row.get("history_json") or "[]")
    platform = row.get("platform") or "flyte"
    if platform == "k8s":
        head = (f"### Job code (job.py)\n```python\n{row['task_code'].strip()}\n```\n\n"
                f"### Job manifest\n```yaml\n{row['manifest'].strip()}\n```\n\n")
        hist = "### Earlier runs of this job (container memory peaks, from the cluster's metrics)"
    else:
        head = f"### Task code\n```python\n{row['task_code'].strip()}\n```\n\n"
        hist = "### Earlier runs of this task (memory peaks include the Flyte runtime)"
    return (
        f"{head}### This run's inputs\n{_clip(row['inputs_desc'], MAX_DESC_CHARS)}\n\n"
        f"{hist}\n{format_history(history, platform)}\n\n"
        f"### Options\n{options_text()}\n\n{ANSWER}"
    )


def with_hint(prompt: str, hint: str) -> str:
    """A stacked arm's prompt: the hint just before the options (``bench/spec.py``)."""
    if not hint:
        return prompt
    i = prompt.rindex("### Options")
    return f"{prompt[:i]}{HINT_HEADER}\n{hint}\n\n{prompt[i:]}"


def messages(row: Mapping[str, Any]) -> list[dict]:
    user = with_hint(user_prompt(row), row.get("hint") or "")
    return [{"role": "system", "content": SYSTEM}, {"role": "user", "content": user}]


def _fmt_bytes(n: float) -> str:
    for unit, div in (("GiB", 2**30), ("MiB", 2**20), ("KiB", 2**10)):
        if n >= div:
            return f"{n / div:.2f} {unit}"
    return f"{int(n)} B"


_KIND_NAMES = {"f64": "double", "f32": "float", "i64": "int64", "i32": "int32",
               "cat": "string (dictionary)", "str": "string", "text": "string"}


def describe_input(spec: Mapping[str, Any], size_bytes: int | None) -> str:
    """``tuner:bench/workloads.py:describe_input`` over a tuner input spec."""
    size = _fmt_bytes(size_bytes) if size_bytes else "unknown size"
    name, fmt = spec["name"], spec["fmt"]
    if fmt == "npy":
        return f"{name}: npy, {size}, shape={tuple(spec['shape'])}, dtype={spec['dtype']}"
    names = [c["name"] for c in spec["columns"]]
    if fmt == "parquet":
        schema = ", ".join(f"{c['name']}: {_KIND_NAMES[c['kind']]}" for c in spec["columns"])
        return f"{name}: parquet, {size}, {spec['rows']:,} rows, schema=[{schema}]"
    if fmt == "csv":
        return f"{name}: csv, {size}, header: {','.join(names)}"
    return f"{name}: jsonl, {size}, first record keys: {names}"


def describe_inputs(inputs: Sequence[Mapping[str, Any]], sizes: Mapping[str, int] | None) -> str:
    sizes = sizes or {}
    return "\n".join(describe_input(s, sizes.get(s["name"])) for s in inputs)


# ── WorkloadContext -> row ──────────────────────────────────────────────

#: ``ManifestFeatures.extra`` key an adapter may use to pass the rendered
#: manifest (a ``{"kind": "str", "value": ...}`` scalar) for the K8s layout.
MANIFEST_EXTRA_KEY = "manifest_yaml"

_KIND_WORDS = (("numeric", "numeric"), ("string", "string"), ("nested", "nested"), ("other", "other"))


def _schema(cols: Mapping[str, Any] | None) -> str:
    """A parquet / Arrow schema. The tuner wrote ``name: kind`` per column;
    rightsizer's ``ColumnsInfo`` has names (when privacy allows) and per-kind
    *counts* only, so per-column kinds are known only when all columns share one."""
    if not cols:
        return "unknown"
    names = cols.get("names")
    counts = {k: int(cols.get(k) or 0) for k, _ in _KIND_WORDS}
    total = int(cols.get("total") or 0)
    if names is not None:
        single = [w for k, w in _KIND_WORDS if counts[k] == total and total > 0]
        return ", ".join(f"{n}: {single[0]}" for n in names) if single else ", ".join(names)
    parts = [f"{counts[k]} {w}" for k, w in _KIND_WORDS if counts[k]]
    return f"{total} columns" + (f": {', '.join(parts)}" if parts else "")


def _scalar_text(s: Mapping[str, Any] | None) -> str:
    if not s:
        return "unknown"
    v = s.get("value")
    if s.get("kind") == "bool":
        return "True" if v else "False"
    return repr(v) if s.get("kind") == "str" else str(v)


def describe_input_meta(i: Mapping[str, Any]) -> str:
    """One rightsizer ``InputMeta`` in the tuner's ``describe_input`` style.

    Exact for ``npy`` (shape, dtype), ``csv`` (header names) and ``jsonl``
    (first-record keys) when names are present; ``parquet`` differs in the
    schema (see :func:`_schema`). Formats the tuner never saw (safetensors,
    arrow_ipc, dir, blob, scalar, model_ref) are rendered in the same style."""
    name, fmt = i["name"], i.get("format", "blob")
    b = i.get("bytes")
    size = _fmt_bytes(b) if b else "unknown size"
    cols = i.get("columns")
    names = (cols or {}).get("names")
    if fmt == "npy":
        shape = tuple(i["shape"]) if i.get("shape") is not None else "unknown"
        return f"{name}: npy, {size}, shape={shape}, dtype={i.get('dtype') or 'unknown'}"
    if fmt == "parquet":
        rows = f"{i['rows']:,} rows" if i.get("rows") is not None else "unknown rows"
        return f"{name}: parquet, {size}, {rows}, schema=[{_schema(cols)}]"
    if fmt == "arrow_ipc":
        return f"{name}: arrow_ipc, {size}, schema=[{_schema(cols)}]"
    if fmt == "csv":
        header = ",".join(names) if names is not None else (f"{cols['total']} columns" if cols else "unknown")
        return f"{name}: csv, {size}, header: {header}"
    if fmt == "jsonl":
        keys = str(list(names)) if names is not None else (f"{cols['total']} keys" if cols else "unknown")
        return f"{name}: jsonl, {size}, first record keys: {keys}"
    if fmt == "scalar":
        return f"{name}: scalar, value={_scalar_text(i.get('scalar'))}"
    parts = [f"{name}: {fmt}", size]
    if i.get("shape") is not None:
        parts.append(f"shape={tuple(i['shape'])}")
    if i.get("dtype"):
        parts.append(f"dtype={i['dtype']}")
    if i.get("params") is not None:
        parts.append(f"{i['params']:,} parameters")
    if i.get("rows") is not None:
        parts.append(f"{i['rows']:,} rows")
    return ", ".join(parts)


def describe_input_metas(inputs: Sequence[Mapping[str, Any]]) -> str:
    return "\n".join(describe_input_meta(i) for i in inputs)


def tuner_platform(ctx: Mapping[str, Any]) -> str:
    """``flyte`` for Flyte tasks; every other platform uses the tuner's Kubernetes
    layout (a module plus its manifest, container peaks), the closer of the two."""
    kind = ((ctx.get("platform") or {}).get("kind") or "").lower()
    return "flyte" if kind == "flyte" else "k8s"


def synthesize_manifest(ctx: Mapping[str, Any]) -> str:
    """A minimal Job manifest in the tuner's flow style from ``ManifestFeatures``,
    for contexts that carry no rendered manifest. It keeps the signals the
    model reads (``kind: Job``, ``initContainers``, ``medium: Memory``), not the
    tuner's exact text."""
    m = ctx.get("manifest") or {}
    name = "batch-job"
    y = ["apiVersion: batch/v1", "kind: Job", "metadata:", f"  generateName: {name}-", "spec:", "  backoffLimit: 0",
         "  template:", "    spec:", "      restartPolicy: Never"]
    mount = "volumeMounts: [{name: data, mountPath: /data}]"
    for k in range(int(m.get("init_containers") or 0)):
        if k == 0:
            y.append("      initContainers:")
        y += [f"        - name: init-{k}", f"          {mount}"]
    y += ["      containers:", "        - name: main", "          command: [python, /app/job.py]",
          "          resources: {}  # to be decided", f"          {mount}"]
    for k in range(int(m.get("sidecars") or 0)):
        y += [f"        - name: sidecar-{k}"]
    y += ["      volumes:", "        - name: data",
          "          emptyDir: {medium: Memory}" if m.get("memory_backed_bytes") else "          emptyDir: {}"]
    return "\n".join(y) + "\n"


def _manifest_text(ctx: Mapping[str, Any]) -> str | None:
    extra = (ctx.get("manifest") or {}).get("extra") or {}
    v = extra.get(MANIFEST_EXTRA_KEY)
    if isinstance(v, Mapping) and v.get("kind") == "str":
        return str(v.get("value"))
    return None


def _measured(m: Mapping[str, Any] | None) -> float | None:
    if not m:
        return None
    v = m.get("value")
    return float(v) if isinstance(v, (int, float)) else None


def history_entry(rec: Mapping[str, Any], platform: str) -> dict | None:
    """One ``RunRecord`` as a tuner history entry, or ``None`` when it does not
    count (not a success, or nothing measured).

    Memory: rightsizer records the container peak as measured. The tuner's Flyte
    history held the workload's peak and added ``FLYTE_RUNTIME_MIB`` when
    rendering, so the entry stores ``peak - 70 MiB`` and the rendered number is
    the measured container peak. CPU: ``cpu_peak_window`` -> "peak ... (busiest
    15 s)" (whatever the platform's ``cpu_window_s``), ``cpu_mean`` -> "mean".
    GPU: ``gpu_memory_peak`` and ``gpu_util_mean`` (a fraction; the tuner wrote
    percent)."""
    if rec.get("status") != "succeeded":
        return None
    inputs = rec.get("inputs") or []
    e: dict[str, Any] = {"inputs_desc": describe_input_metas(inputs),
                         "input_bytes": {i["name"]: int(i["bytes"]) for i in inputs if i.get("bytes") is not None},
                         "wall_s": float(rec.get("duration_s") or 0.0)}
    measured = False
    mem = _measured(rec.get("memory_peak"))
    if mem is not None:
        e["peak_mem_mib"] = mem / MIB - (FLYTE_RUNTIME_MIB if platform == "flyte" else 0.0)
        measured = True
    peak = _measured(rec.get("cpu_peak_window"))
    if peak is not None:
        e["peak_cores"] = peak
        measured = True
    mean = _measured(rec.get("cpu_mean"))
    if mean is not None:
        e["mean_cores"] = mean
        measured = True
    gpu = _measured(rec.get("gpu_memory_peak"))
    if gpu is not None:
        e["peak_gpu_mib"] = gpu / MIB
        e["mean_util"] = float(rec.get("gpu_util_mean") or 0.0) * 100.0
        measured = True
    return e if measured else None


def row_from_context(ctx: Mapping[str, Any], *, manifest: str | None = None, hint: str = "",
                     max_history: int | None = None) -> dict:
    """A tuner row for a rightsizer ``WorkloadContext`` (JSON dict).

    - ``platform``: ``flyte`` or ``k8s`` (:func:`tuner_platform`).
    - ``task_code``: ``code.source``; empty when the context carries none (the
      default for remote estimators, SPEC §11) -- the model still answers.
    - ``manifest`` (K8s layout): the argument, else ``manifest.extra.manifest_yaml``,
      else :func:`synthesize_manifest`.
    - ``inputs_desc``: :func:`describe_input_meta` per input, in context order
      (rightsizer's canonical order is largest first, the tuner's the task's
      declaration order).
    - ``history_json``: successful records, oldest first (rightsizer keeps
      newest first), at most ``max_history`` of the newest.
    """
    platform = tuner_platform(ctx)
    code = (ctx.get("code") or {}).get("source") or ""
    recs = list(ctx.get("history") or [])
    entries = [e for e in (history_entry(r, platform) for r in recs) if e is not None]
    if max_history is not None:
        entries = entries[:max_history]
    entries.reverse()
    row = {"platform": platform, "task_code": code, "inputs_desc": describe_input_metas(ctx.get("inputs") or []),
           "history_json": json.dumps(entries), "hint": hint}
    if platform == "k8s":
        row["manifest"] = manifest if manifest is not None else (_manifest_text(ctx) or synthesize_manifest(ctx))
    else:
        row["manifest"] = ""
    return row


def messages_from_context(ctx: Mapping[str, Any], **kw: Any) -> list[dict]:
    """The chat messages the model reads for ``ctx`` (see :func:`row_from_context`)."""
    return messages(row_from_context(ctx, **kw))


__all__ = [
    "ANSWER", "CPU_OPTIONS", "FLYTE_RUNTIME_MIB", "GPU_OPTIONS", "KEYS", "MANIFEST_EXTRA_KEY", "MEM_OPTIONS",
    "OPTIONS", "SYSTEM", "describe_input", "describe_input_meta", "describe_inputs", "format_history",
    "history_entry", "messages", "messages_from_context", "options_text", "row_from_context",
    "synthesize_manifest", "user_prompt", "with_hint",
]
