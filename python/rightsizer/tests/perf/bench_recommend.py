"""``rs.recommend`` from Python: memo-hit latency and single-flight fan-out.

    python tests/perf/bench_recommend.py [--n 10000] [--threads 2000] [--tasks 10000]

Memo hit: the same context (a Parquet input read by the engine, 32 history
records in an in-memory store, a walltime, 10 node shapes) decided ``n``
times after a warm-up; reported for the raw ``_core.Engine.decide`` (JSON in,
JSON out: the Rust engine plus the binding) and the typed ``rs.recommend``
(request building, JSON both ways, the ``Decision`` dataclass).

Fan-out: identical contexts from ``threads`` OS threads (``tasks`` calls in
all) and from ``tasks`` asyncio tasks, with a 200 ms Python estimator in
front of the chain: single-flight means one estimator call per wave.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import rightsizer as rs

GiB = 1 << 30


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))]


def summary(us):
    return {"p50_us": round(pct(us, 50), 1), "p99_us": round(pct(us, 99), 1), "mean_us": round(statistics.fmean(us), 1)}


def make_context(parquet):
    platform = {"kind": "python", "walltime_s": 7200, "memory_enforced": True}
    return rs.Context(key="bench", prior=rs.Resources(cpu=8, memory="64Gi"), inputs=[rs.Input.from_path(parquet)],
                      code=rs.Code.from_source("import pandas as pd\ndf = pd.read_parquet(p)\n"), platform=platform)


STATE = {"shapes": [{"name": f"n{i}", "cpu": (2 ** (i % 5 + 1)) * 1000, "memory": (2 ** (i % 5 + 3)) * GiB,
                     "price_per_hour": 0.05 * 2 ** (i % 5)} for i in range(10)]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=10_000)
    ap.add_argument("--threads", type=int, default=2000)
    ap.add_argument("--tasks", type=int, default=10_000)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    import pyarrow as pa
    import pyarrow.parquet as pq

    tmp = tempfile.mkdtemp()
    parquet = os.path.join(tmp, "train.parquet")
    pq.write_table(pa.table({"a": list(range(100_000)), "b": [str(i) for i in range(100_000)]}), parquet)
    eng = rs.Engine({"history": {"uri": "memory://"}, "policy": {"budget_ms": 5000}}, env_overrides=False)
    ctx = make_context(parquet)
    for i in range(32):
        d = eng.decide(ctx, state=STATE)
        eng.outcome(d, "succeeded", memory_peak=(20 + i % 7) * GiB, cpu_mean=3.0 + (i % 5) * 0.1, duration_s=1800 + i,
                    invocation_id=f"h{i}")
    # Warm the memo and the history cache.
    for _ in range(50):
        d = eng.decide(ctx, state=STATE, dry_run=a.dry_run)
    assert d.source.kind == "memo", d.source

    req = json.dumps(rs.build_request(ctx, state=STATE, dry_run=a.dry_run))
    native = eng.native
    raw = []
    for _ in range(a.n):
        t = time.perf_counter_ns()
        native.decide(req)
        raw.append((time.perf_counter_ns() - t) / 1000)
    typed = []
    for _ in range(a.n):
        t = time.perf_counter_ns()
        d = rs.recommend(ctx, engine=eng, state=STATE, dry_run=a.dry_run)
        typed.append((time.perf_counter_ns() - t) / 1000)
    assert d.source.kind == "memo"
    # Python-side work alone (request building + JSON + dataclass), no Rust call.
    out = native.decide(req)
    py = []
    for _ in range(a.n):
        t = time.perf_counter_ns()
        json.dumps(rs.build_request(ctx, state=STATE, dry_run=a.dry_run))
        rs.Decision.from_dict(json.loads(out), context=ctx)
        py.append((time.perf_counter_ns() - t) / 1000)

    # Fan-out with single-flight.
    calls = []
    lock = threading.Lock()

    class Slow(rs.Estimator):
        id = "slow-bench"

        def estimate(self, c):
            with lock:
                calls.append(1)
            time.sleep(0.2)
            return {"memory": {"kind": "point", "value": 24.0 * GiB}}

    rs.register_estimator(Slow())
    fan_ctx = rs.Context(key="fan", prior=rs.Resources(cpu=8, memory="64Gi"), inputs=[rs.Input.sized("x", 8 * GiB, "parquet")])
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=a.threads) as ex:
        ds = list(ex.map(lambda _: eng.decide(fan_ctx, state=STATE), range(a.tasks)))
    thread_wall = time.perf_counter() - t0
    thread_calls = len(calls)
    thread_sources = sorted({x.source.kind for x in ds})

    class SlowAsync(rs.Estimator):
        id = "slow-bench-async"

        async def estimate(self, c):
            calls.append(1)
            await asyncio.sleep(0.2)
            return {"memory": {"kind": "point", "value": 24.0 * GiB}}

    rs.unregister_estimator("slow-bench")
    rs.register_estimator(SlowAsync())
    afan = rs.Context(key="fan-async", prior=rs.Resources(cpu=8, memory="64Gi"), inputs=[rs.Input.sized("x", 8 * GiB, "parquet")])
    calls.clear()

    async def wave():
        t = time.perf_counter()
        r = await asyncio.gather(*[rs.arecommend(afan, engine=eng, state=STATE) for _ in range(a.tasks)])
        return r, time.perf_counter() - t

    ads, async_wall = asyncio.run(wave())
    print(json.dumps({
        "n": a.n,
        "dry_run": a.dry_run,
        "core_decide_memo_hit": summary(raw),
        "rs_recommend_memo_hit": summary(typed),
        "python_side_only": summary(py),
        "typed_overhead_over_core_p50_us": round(pct(typed, 50) - pct(raw, 50), 1),
        "threads": {"workers": a.threads, "calls": a.tasks, "estimator_calls": thread_calls, "wall_s": round(thread_wall, 3),
                    "sources": thread_sources},
        "async": {"tasks": a.tasks, "estimator_calls": len(calls), "wall_s": round(async_wall, 3),
                  "sources": sorted({x.source.kind for x in ads})},
    }, indent=2))


if __name__ == "__main__":
    main()
