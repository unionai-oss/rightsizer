"""Generate the M0 parity fixture from the resource tuner's own scorer.

The k8s-2 rows themselves (release ``rsk-k8s-2``, exported by
``paper.paper_export`` as ``rt-paper-k2s1-rows``) live in Union run outputs
and are not in any repository, release or CI artifact we can reach. So this
script builds a **deterministic synthetic row set shaped like k8s-2** (Job
pods: memory-bench rows with a mean-cores label, CPU-bench rows with a
per-second core series, GPU-bench rows with a device-memory peak, sharded
or not) and scores candidate requests with the tuner's code, imported
directly -- nothing is re-implemented here:

- ``resource_tuner.bench_all.scoring.cost`` (memory ladder x2 up to 8
  attempts, throttling stretch from the series or the mean, GPU device
  ladder, OOM and delay penalties against the oracle), at the tuner's
  default penalties (OOM 1, delay 1) and the round-23 retry-cost sweep
  (OOM 5 and 20);
- ``resource_tuner.bench_all.arms.fit_rules_sized`` /
  ``predict_rules_sized`` and ``scoring.score`` for the rules-sized regret
  on a synthetic task corpus with history.

Run (from the repo root; numpy only, in a throwaway env)::

    PYTHONPATH=<tuner checkout> uv run --no-project --with numpy \
        python crates/rightsizer-engine/tests/parity/gen_k8s2_fixture.py \
        crates/rightsizer-engine/tests/fixtures/parity_k8s2.json
"""

from __future__ import annotations

import json
import math
import random
import sys

import numpy as np

from resource_tuner.bench_all import arms, grid, scoring
from resource_tuner.bench_cpu.grid import peak_window
from resource_tuner.bench_gpu import grid as gpu_grid

SEED = 20261118
PENALTIES = (1.0, 5.0, 20.0)  # OOM_PENALTY values; DELAY_PENALTY stays 1


def sig(x: float) -> float:
    return float(f"{x:.12g}")


def mem_row(rng: random.Random, i: int) -> dict:
    peak = min(max(math.exp(rng.gauss(math.log(1500), 1.2)), 40.0), 50_000.0)
    staging = rng.choice(("init-disk", "init-tmpfs", "boto3-disk", "boto3-tmpfs"))
    extra = (48.0 if staging.startswith("boto3") else 0.0) + (rng.uniform(5, 400) if staging.endswith("tmpfs") else 0.0)
    need = (peak + extra) * 1.05
    mean = round(rng.uniform(0.05, 3.5), 3)
    return {"run_id": f"mem:{i}", "source": "mem", "platform": "k8s", "staging": staging, "need_mib": sig(need),
            "t_peak_frac": round(rng.uniform(0.05, 0.95), 3), "mean_cores": mean, "need_cores": mean,
            "cpu_series_json": "[]", "peak_gpu_gib": float("nan"), "sharded": False, "gpu_imbalance": 1.0,
            "label_mem": grid.mem_cell(need), "label_cpu": grid.cpu_cell(mean), "label_gpu": grid.NO_GPU}


def cpu_row(rng: random.Random, i: int) -> dict:
    n = rng.randint(60, 150)
    base = rng.uniform(0.2, 5.0)
    series = []
    for t in range(n):
        burst = rng.uniform(1.0, 2.5) if (t // 15) % 3 == 1 else 1.0
        series.append(round(max(0.0, base * burst + rng.gauss(0, 0.15)), 2))
    peak_cores = peak_window(series, 15)
    peak = min(max(math.exp(rng.gauss(math.log(900), 1.0)), 40.0), 30_000.0)
    need = peak * 1.05
    return {"run_id": f"cpu:{i}", "source": "cpu", "platform": "k8s", "staging": "init-disk", "need_mib": sig(need),
            "t_peak_frac": round(rng.uniform(0.05, 0.95), 3), "mean_cores": round(float(np.mean(series)), 6),
            "need_cores": round(peak_cores, 6), "cpu_series_json": json.dumps(series), "peak_gpu_gib": float("nan"),
            "sharded": False, "gpu_imbalance": 1.0, "label_mem": grid.mem_cell(need),
            "label_cpu": grid.cpu_cell(peak_cores), "label_gpu": grid.NO_GPU}


def gpu_row(rng: random.Random, i: int) -> dict:
    sharded = rng.random() < 0.3
    peak = rng.uniform(0.6, 150.0) if sharded else rng.uniform(0.6, 43.0)
    imb = round(rng.uniform(1.0, 1.5), 3) if sharded else gpu_grid.DEFAULT_IMBALANCE
    if sharded and gpu_grid.oracle_device(peak, True, imb) is None:
        peak = rng.uniform(0.6, 100.0)
    return {"run_id": f"gpu:{i}", "source": "gpu", "platform": "k8s", "staging": "", "need_mib": float("nan"),
            "t_peak_frac": round(rng.uniform(0.02, 0.4), 3), "mean_cores": 0.0, "need_cores": 0.0,
            "cpu_series_json": "[]", "peak_gpu_gib": round(peak, 4), "sharded": sharded, "gpu_imbalance": imb,
            "label_mem": -1, "label_cpu": -1, "label_gpu": 1 + gpu_grid.oracle_cell(peak, sharded, imb)}


def candidates(r: dict) -> list[tuple[int, int, int]]:
    if r["source"] == "gpu":
        lg = int(r["label_gpu"])
        cs = {(0, 0, lg), (0, 0, max(1, lg - 1)), (0, 0, max(1, lg - 3)), (0, 0, 1), (0, 0, min(grid.N_GPU - 1, lg + 2)),
              (0, 0, grid.NO_GPU)}
        return sorted(cs)
    lm, lc = int(r["label_mem"]), int(r["label_cpu"])
    cap = lambda v, n: max(0, min(n - 1, v))  # noqa: E731
    cs = {(lm, lc, 0), (cap(lm - 1, grid.N_MEM), lc, 0), (cap(lm - 3, grid.N_MEM), cap(lc - 3, grid.N_CPU), 0),
          (cap(lm + 2, grid.N_MEM), cap(lc + 2, grid.N_CPU), 0), (0, 0, 0), (lm, 0, 0), (lm, lc, 1)}
    return sorted(cs)


def cost_table(rows: list[dict]) -> list[dict]:
    out = []
    for r in rows:
        cands = []
        for (m, c, g) in candidates(r):
            costs, attempts, failed = [], None, None
            for pen in PENALTIES:
                scoring.OOM_PENALTY = pen
                r.pop("_oracle", None)
                o = scoring.cost(r, m, c, g)
                costs.append(sig(o.cost))
                attempts, failed = o.attempts, o.failed
            scoring.OOM_PENALTY = 1.0
            r.pop("_oracle", None)
            cands.append({"m": m, "c": c, "g": g, "cost": costs, "attempts": attempts, "failed": failed})
        oracle = scoring.oracle_action(r)
        r.pop("_oracle", None)
        r.pop("_series", None)
        row = {k: v for k, v in r.items() if not (isinstance(v, float) and math.isnan(v))}
        row["oracle"] = list(oracle)
        row["oracle_cost"] = sig(scoring.oracle_cost(r))
        r.pop("_oracle", None)
        row["candidates"] = cands
        out.append(row)
    return out


# ── rules-sized on a synthetic task corpus with history ─────────────────


def task_corpus(rng: random.Random, n_tasks: int = 80, runs_per_task: int = 6) -> list[dict]:
    rows = []
    for t in range(n_tasks):
        split = ("train", "train", "val", "test")[t % 4]
        base = math.exp(rng.gauss(math.log(300), 0.8))
        slope = rng.choice((0.5, 1.0, 2.0, 3.0, 6.0))
        mean_cores = round(rng.uniform(0.2, 3.0), 3)
        pools = rng.choice((None, 2, 4, 8))
        staging = rng.choice(("init-disk", "init-tmpfs", "boto3-disk", "boto3-tmpfs"))
        code = "import pandas as pd\n"
        if staging.startswith("boto3"):
            code += "import boto3\n"
        if pools:
            code += f"from concurrent.futures import ThreadPoolExecutor\npool = ThreadPoolExecutor(max_workers={pools})\n"
        manifest = "kind: Job\n" + ("volumes:\n- name: data\n  emptyDir:\n    medium: Memory\n" if staging.endswith("tmpfs") else "")
        hist = []
        for k in range(runs_per_task):
            size = int(math.exp(rng.uniform(math.log(5e6), math.log(2e9))))
            sizes = {"data": size}
            staged = (math.ceil(size / 4096) * 4096 + 4096) / 2**20 if staging.endswith("tmpfs") else 0.0
            workload = base + slope * size / 2**20 * rng.uniform(0.9, 1.1)
            peak = workload + (48.0 if staging.startswith("boto3") else 0.0) + staged  # the container's peak
            if peak * 1.05 > 60_000:
                continue
            need = peak * 1.05
            cores = round(mean_cores * rng.uniform(0.9, 1.1), 3)
            rows.append({
                "run_id": f"rs:{t}:{k}", "task_id": f"rs:{t}", "family": f"fam{t % 10}", "split": split,
                "source": "mem", "platform": "k8s", "staging": staging, "manifest": manifest, "task_code": code,
                "input_bytes_json": json.dumps(sizes), "n_history": len(hist), "history_json": json.dumps(list(hist)),
                "need_mib": sig(need), "t_peak_frac": round(rng.uniform(0.1, 0.9), 3), "mean_cores": cores,
                "need_cores": cores, "cpu_series_json": "[]", "sharded": False, "gpu_imbalance": 1.0,
                "label_mem": grid.mem_cell(need), "label_cpu": grid.cpu_cell(cores), "label_gpu": grid.NO_GPU,
            })
            hist.append({"peak_mem_mib": round(peak, 3), "input_bytes": sizes, "mean_cores": cores})
    return rows


def sized_one_scale_floor(row, k, static):
    """The tuner's ``arms._sized_one`` with SPEC §7.4's ``scale = max(1, b_now / b_hist)``:
    the one deliberate deviation of rightsizer's ``rules-sized`` from the tuner's
    (the tuner also scales the nearest run *down* for a smaller invocation)."""
    margin, base, slope = k
    h = arms._hist(row)
    sizes = json.loads(row.get("input_bytes_json") or "{}")
    total = sum(sizes.values()) or 1
    mems = [(x["peak_mem_mib"], sum((x.get("input_bytes") or {}).values()) or 1) for x in h if "peak_mem_mib" in x]
    plat = arms.platform_of(row)
    if not mems:
        return arms._sized_one(row, k, static)
    peak, hb = min(mems, key=lambda x: abs(np.log(x[1] / total)))
    m = grid.mem_cell(arms.need_from_peak(peak * max(1.0, total / hb), plat) * margin)
    c = arms._rules_one(row, (1.0, static["margin_c"], 1.0), static)["cpu_idx"]
    return arms._p(m, c, grid.NO_GPU)


def main(dst: str) -> None:
    rng = random.Random(SEED)
    rows = [mem_row(rng, i) for i in range(120)] + [cpu_row(rng, i) for i in range(110)] + [gpu_row(rng, i) for i in range(60)]
    table = cost_table(rows)

    corpus = task_corpus(random.Random(SEED + 1))
    train = [r for r in corpus if r["split"] == "train"]
    val = [r for r in corpus if r["split"] == "val"]
    test = [r for r in corpus if r["split"] == "test"]
    state = arms.fit_rules_sized(train, val)
    preds = arms.predict_rules_sized(state, test)
    s = scoring.score(test, preds)
    preds_floor = {r["run_id"]: sized_one_scale_floor(r, tuple(state["k"]), state["static"]) for r in test}
    s_floor = scoring.score(test, preds_floor)
    rs_rows = []
    for r in test:
        p = preds[r["run_id"]]
        keep = {k: r[k] for k in ("run_id", "task_id", "staging", "manifest", "task_code", "input_bytes_json", "n_history",
                                  "history_json", "need_mib", "t_peak_frac", "mean_cores", "need_cores", "label_mem",
                                  "label_cpu", "label_gpu", "source", "platform")}
        keep["pick"] = [int(p["mem_idx"]), int(p["cpu_idx"]), int(p["gpu_idx"])]
        pf = preds_floor[r["run_id"]]
        keep["pick_scale_floor"] = [int(pf["mem_idx"]), int(pf["cpu_idx"]), int(pf["gpu_idx"])]
        keep["pick_cost"] = sig(scoring.cost(r, *keep["pick"]).cost)
        r.pop("_oracle", None)
        keep["oracle_cost"] = sig(scoring.oracle_cost(r))
        r.pop("_oracle", None)
        rs_rows.append(keep)

    fixture = {
        "v": 1,
        "about": "Synthetic k8s-2-shaped rows scored by the resource tuner's own scorer (bench_all.scoring.cost); "
                 "see crates/rightsizer-engine/tests/parity/gen_k8s2_fixture.py",
        "seed": SEED,
        "penalties": list(PENALTIES),
        "delay_penalty": scoring.DELAY_PENALTY,
        "grids": {"memory_mib": list(grid.MEMORY_GRID_MIB), "cpu": list(grid.CPU_GRID),
                  "gpu_gib": list(gpu_grid.GPU_MEM_GRID_GIB),
                  "devices": [{"name": d.name, "n": d.n_gpus, "per_gpu_gib": d.per_gpu_gib, "price": d.price} for d in gpu_grid.DEVICES]},
        "rows": table,
        "rules_sized": {"state": state, "regret": sig(s["regret"]), "regret_scale_floor": sig(s_floor["regret"]),
                        "n": s["n"], "rows": rs_rows},
    }
    with open(dst, "w") as f:
        json.dump(fixture, f, separators=(",", ":"), sort_keys=True)
    print(f"{len(table)} rows, {sum(len(r['candidates']) for r in table)} candidates; rules-sized regret "
          f"{s['regret']:.4f} (scale >= 1: {s_floor['regret']:.4f}) on {s['n']} test rows -> {dst}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "parity_k8s2.json")
