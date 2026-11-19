"""Varied workloads in both representations: the tuner's rows and rightsizer
``WorkloadContext`` dicts (serde JSON shapes), built from the same facts."""

from __future__ import annotations

import json
import random

MIB = 1 << 20
FLYTE_RUNTIME_MIB = 70.0

_IMPORTS = {
    "npy": "import numpy as np",
    "csv": "import pandas as pd",
    "jsonl": "import json",
    "parquet": "import pandas as pd",
}
_BODIES = {
    "npy": "    a = np.load({n}_path, mmap_mode=\"r\")\n    total += float(a.sum())",
    "csv": "    df = pd.read_csv({n}_path, chunksize=100_000)\n    total += sum(len(c) for c in df)",
    "jsonl": "    with open({n}_path) as f:\n        for line in f:\n            total += len(json.loads(line))",
    "parquet": "    df = pd.read_parquet({n}_path, columns=[\"a\"])\n    total += float(df.groupby(\"a\").size().sum())",
}
_KINDS = ("f64", "f32", "i64", "i32", "cat", "str", "text")


def flyte_code(family: str, inputs: list[dict], gpu: bool = False) -> str:
    """A module in ``tuner:bench/workloads.py:render_task``'s layout."""
    imports = sorted({_IMPORTS[s["fmt"]] for s in inputs} | ({"import torch"} if gpu else set()))
    args = ", ".join(f"{s['name']}: File" for s in inputs)
    downloads = "\n".join(f"    {s['name']}_path = await {s['name']}.download()" for s in inputs)
    body = "\n".join(_BODIES[s["fmt"]].format(n=s["name"]) for s in inputs)
    if gpu:
        body += "\n    device = \"cuda\"\n    x = torch.zeros(1024, 1024).to(device)"
    return (
        "import flyte\nfrom flyte.io import File\n" + "\n".join(imports) + "\n\n"
        f'env = flyte.TaskEnvironment(name="{family.replace("_", "-")}", image=flyte.Image.from_debian_base())\n\n\n'
        f"@env.task\nasync def main({args}) -> dict:\n{downloads}\n    total = 0.0\n{body}\n    return {{\"total\": total}}\n"
    )


def _spec(rng: random.Random, name: str, fmt: str, wide: bool = False) -> dict:
    if fmt == "npy":
        dims = [rng.choice([1000, 50_000, 2_000_000])] + ([rng.choice([8, 64, 784])] if rng.random() < 0.8 else [])
        return {"name": name, "fmt": "npy", "shape": dims, "dtype": rng.choice(["float64", "float32", "uint8"])}
    n = rng.randint(30, 60) if wide else rng.randint(1, 8)
    cols = [{"name": f"c{i}_{rng.choice(['id', 'val', 'ts', 'tag'])}", "kind": rng.choice(_KINDS)} for i in range(n)]
    spec = {"name": name, "fmt": fmt, "columns": cols}
    if fmt == "parquet":
        spec["rows"] = rng.randint(1, 50_000_000)
    return spec


def _input_meta(spec: dict, size: int | None) -> dict:
    """A rightsizer ``InputMeta`` (serde JSON) for a tuner input spec, as the
    Rust readers would record it (column names allowed)."""
    m: dict = {"name": spec["name"], "format": spec["fmt"]}
    if size is not None:
        m["bytes"] = size
    if spec["fmt"] == "npy":
        m["shape"] = list(spec["shape"])
        m["dtype"] = spec["dtype"]
    else:
        names = [c["name"] for c in spec["columns"]]
        if spec["fmt"] == "parquet":
            m["rows"] = spec["rows"]
            num = sum(c["kind"] in ("f64", "f32", "i64", "i32") for c in spec["columns"])
            m["columns"] = {"total": len(names), "numeric": num, "string": len(names) - num, "nested": 0, "other": 0,
                            "names": names}
        else:
            m["columns"] = {"total": len(names), "numeric": 0, "string": 0, "nested": 0, "other": 0, "names": names}
    return m


def _run_record(key: str, platform: str, h: dict, specs: list[dict], i: int) -> dict:
    """A rightsizer ``RunRecord`` for a tuner history entry. Memory is the
    container peak: a Flyte entry's workload peak + the runtime."""
    inputs = [_input_meta(s, h["input_bytes"].get(s["name"])) for s in specs]
    rec: dict = {"v": 1, "key": key, "invocation_id": f"inv-{i}", "attempt": 1,
                 "started_at": f"2026-11-{10 + i:02d}T00:00:00.000000Z", "inputs": inputs, "status": "succeeded",
                 "status_confidence": "observed", "source": {"kind": "cgroup"}, "trust": "authenticated",
                 "duration_s": h["wall_s"]}
    if "peak_mem_mib" in h:
        over = FLYTE_RUNTIME_MIB if platform == "flyte" else 0.0
        rec["memory_peak"] = {"value": (h["peak_mem_mib"] + over) * MIB, "censor": "exact"}
    if "peak_cores" in h:
        rec["cpu_peak_window"] = {"value": h["peak_cores"], "censor": "exact"}
    if "mean_cores" in h:
        rec["cpu_mean"] = {"value": h["mean_cores"], "censor": "exact"}
    if "peak_gpu_mib" in h:
        rec["gpu_memory_peak"] = {"value": h["peak_gpu_mib"] * MIB, "censor": "exact"}
        rec["gpu_util_mean"] = h["mean_util"] / 100.0
    return rec


def make_case(seed: int, tuner_prompting, tuner_k8s=None, fmts: tuple = ("npy", "csv", "jsonl")) -> tuple[dict, dict]:
    """``(tuner row, WorkloadContext)`` for one random workload."""
    rng = random.Random(seed)
    describe = __import__("resource_tuner.bench.workloads", fromlist=["describe_inputs"]).describe_inputs
    n_in = rng.randint(1, 3)
    specs = [_spec(rng, f"in{k}", rng.choice(fmts), wide=rng.random() < 0.15) for k in range(n_in)]
    gpu = rng.random() < 0.25
    family = rng.choice(["np_reduce", "pd_groupby", "json_scan", "torch_train"])
    sizes = {s["name"]: rng.randint(10_000, 8 << 30) for s in specs}
    if rng.random() < 0.15:
        sizes.pop(specs[0]["name"])
    hist = []
    for i in range(rng.choice([0, 0, 1, 2, 4])):
        hs = {s["name"]: rng.randint(10_000, 8 << 30) for s in specs}
        e = {"inputs_desc": describe(specs, hs), "input_bytes": hs, "wall_s": round(rng.uniform(5, 900), 2)}
        if not gpu or rng.random() < 0.5:
            e["peak_mem_mib"] = round(rng.uniform(40, 40000), 2) + 0.0013
        if rng.random() < 0.7:
            e["peak_cores"] = round(rng.uniform(0.1, 7.9), 3) + 0.0003
        e["mean_cores"] = round(rng.uniform(0.05, 4), 3) + 0.0003
        if gpu:
            e["peak_gpu_mib"] = round(rng.uniform(300, 40000), 1) + 0.013
            e["mean_util"] = round(rng.uniform(5, 99), 1) + 0.013
        hist.append(e)
    row = {"run_id": f"r{seed}", "task_id": f"t{seed}", "run_idx": len(hist), "source": "gpu" if gpu else "mem",
           "task_code": flyte_code(family, specs, gpu), "inputs_json": json.dumps(specs),
           "input_bytes_json": json.dumps(sizes), "inputs_desc": describe(specs, sizes),
           "history_json": json.dumps(hist), "platform": "flyte", "manifest": "", "need_mib": 1000.0,
           "need_cores": 1.0, "mean_cores": 1.0, "t_peak_frac": 0.5, "wall_s": 60.0, "peak_gpu_gib": 3.0}
    if tuner_k8s is not None and rng.random() < 0.5:
        row = tuner_k8s.twin(row)
        hist = json.loads(row["history_json"])
    platform = row["platform"]
    key = f"{'flyte://acme/proj/dev/fam.main' if platform == 'flyte' else 'k8s://c1/ns/job/main'}"
    ctx = {
        "v": 1, "key": key, "unit": "main",
        "platform": {"kind": platform, "version": "", "cpu_window_s": 15, "memory_enforced": True},
        "code": {"digest": "sha256:x", "tokens": [], "source": row["task_code"]},
        "inputs": [_input_meta(s, sizes.get(s["name"])) for s in specs],
        "manifest": {"sidecars": 0, "init_containers": 0, "distributed": False},
        "prior": {"spec": {"requests": {}, "limits": {}}, "defaulted": []},
        "history": [_run_record(key, platform, h, specs, i) for i, h in enumerate(hist)][::-1],
        "labels": {},
    }
    if platform == "k8s":
        ctx["manifest"]["extra"] = {"manifest_yaml": {"kind": "str", "value": row["manifest"]}}
    return row, ctx
