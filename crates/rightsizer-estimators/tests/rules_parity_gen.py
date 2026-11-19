"""Expected numbers for ``tests/rules_parity.rs``, computed by running the
tuner's own ``rules-sized`` / ``size-rule`` code on hand-built rows.

Regenerate (needs a checkout of the resource tuner)::

    TUNER_PATH=/path/to/model-factory-resource-tuner \\
        .venv/bin/python crates/rightsizer-estimators/tests/rules_parity_gen.py

Writes ``tests/fixtures/rules_sized_parity.json``. Each case records the
tuner's *workload* value (what it passes to ``need_from_peak`` /
``mem_cell``), its memory and CPU cells, and how rightsizer should build the
same case (container peaks = tuner peaks + the platform runtime, which
rightsizer's overhead model subtracts again).
"""

from __future__ import annotations

import json
import math
import os
import sys
from pathlib import Path

sys.path.insert(0, os.environ.get("TUNER_PATH", "."))

from resource_tuner.bench import arms as bench_arms  # noqa: E402
from resource_tuner.bench_all import arms as all_arms  # noqa: E402
from resource_tuner.bench_all import grid as all_grid  # noqa: E402
from resource_tuner.bench_all.k8s import FLYTE_RUNTIME_MIB, need_from_peak  # noqa: E402

MIB = 2**20
OUT = Path(__file__).resolve().parent / "fixtures" / "rules_sized_parity.json"
STATIC = {"mem": 5, "cpu": 3, "gpu": 0, "margin_c": 1.25, "margin_g": 1.0}


def sized_case(name, platform, total, hist, k=(1.0, 256.0, 2.0), deviation=None):
    """bench_all ``_sized_one``: hist = [(peak_mib, input_bytes, mean_cores)]."""
    row = {
        "task_code": "import pandas as pd\n",
        "platform": platform,
        "manifest": "",
        "input_bytes_json": json.dumps({"a": total}),
        "history_json": json.dumps([{"peak_mem_mib": p, "input_bytes": {"a": b}, "mean_cores": c} for p, b, c in hist]),
    }
    out = all_arms._sized_one(row, k, STATIC)
    margin, base, slope = k
    if hist:
        peak, hb = min([(p, b or 1) for p, b, _ in hist], key=lambda x: abs(math.log(x[1] / total)))
        workload = peak * total / hb
        need = need_from_peak(workload, platform) * margin
    else:
        workload = base + slope * total / MIB
        need = need_from_peak(workload, platform)
    assert all_grid.mem_cell(need) == out["mem_idx"], (name, need, out)
    overhead = 0.0 if platform == "k8s" else FLYTE_RUNTIME_MIB
    return {
        "name": name,
        "kind": "sized",
        "platform": platform,
        "overhead_mib": overhead,
        "input_bytes": total,
        # rightsizer stores container peaks: the tuner's Flyte peaks exclude the runtime.
        "history": [{"container_peak_mib": p + overhead, "input_bytes": b, "mean_cores": c} for p, b, c in hist],
        "margin": margin, "base_mib": base, "slope": slope, "cpu_margin": STATIC["margin_c"],
        "tuner_workload_mib": workload,
        "tuner_need_mib": need,
        "tuner_mem_cell": out["mem_idx"],
        "tuner_cpu_cell": out["cpu_idx"],
        "deviation": deviation,
    }


def power_case(name, fmt, total, a, b):
    """bench ``_size_one`` cold start: ``2^(a + b·log2 bytes)`` MiB."""
    row = {"inputs_json": json.dumps([{"name": "a", "fmt": fmt}]), "input_bytes_json": json.dumps({"a": total}), "history_json": "[]"}
    out = bench_arms._size_one(row, {fmt: (a, b)}, 1.0)
    value = 2 ** (a + b * math.log2(total))
    return {"name": name, "kind": "power", "format": fmt, "input_bytes": total, "a": a, "b": b,
            "tuner_workload_mib": value, "tuner_need_mib": value, "tuner_mem_cell": out["mem_idx"], "deviation": None}


def main():
    G = 2**30
    cases = [
        sized_case("k8s cold 1 GiB", "k8s", 1 * G, []),
        sized_case("flyte cold 3 GiB, base 128 k 4", "flyte", 3 * G, [], k=(1.0, 128.0, 4.0)),
        sized_case("k8s warm exact size", "k8s", 2 * G, [(3000.0, 2 * G, 1.5)]),
        sized_case("k8s warm 2x larger, margin 1.25", "k8s", 4 * G, [(1500.0, 2 * G, 0.8)], k=(1.25, 256.0, 2.0)),
        sized_case("flyte warm nearest of three", "flyte", 6 * G,
                   [(900.0, 1 * G, 0.5), (4000.0, 5 * G, 2.0), (12000.0, 20 * G, 3.0)]),
        sized_case("flyte warm 1.5x", "flyte", 3 * G, [(2000.0, 2 * G, 1.0)]),
        sized_case("k8s warm smaller invocation (tuner scales down)", "k8s", 1 * G, [(4000.0, 4 * G, 1.0)],
                   deviation="SPEC §7.4: scale = max(1, b_now / b_hist); the tuner scales the peak down"),
        power_case("bench power law parquet", "parquet", 3 * G, 3.0, 0.4),
        power_case("bench default law (4 GiB flat)", "csv", 10 * G, math.log2(4096), 0.0),
    ]
    OUT.write_text(json.dumps({"generated_by": "rules_parity_gen.py", "cases": cases}, indent=1))
    print(f"wrote {len(cases)} cases to {OUT}")


if __name__ == "__main__":
    main()
