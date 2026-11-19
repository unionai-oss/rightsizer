"""Decision overhead per call on the Flyte parent path, once memoized (SPEC §15 M1:
< 2 ms per call for a 10k-way fan-out).

    .venv/bin/python python/rightsizer/tests/flyte/bench_decision_path.py [N] [--full]

Runs ``N`` (default 10,000) calls of a ``RightsizedTask`` under a fake remote
task context and controller, concurrently (as ``flyte.map`` / ``gather`` do),
with a real engine on a ``memory://`` store. Reports the per-call cost of
the rightsizer part: planning (context, policy), the engine decision (a memo
hit after the first), building the sized copy, and — with ``--full`` — the
parent-side outcome record too. Prints one JSON line.
"""

from __future__ import annotations

import asyncio
import json
import os
import statistics
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "..", "src")))

import flyte  # noqa: E402

import rightsizer as rs  # noqa: E402
import rightsizer.flyte as rf  # noqa: E402
from rightsizer.flyte import _engine as eng  # noqa: E402

DATA = ""  # a readable Parquet input, written in main() (its footer is read once, then cached)

env = flyte.TaskEnvironment(name="rsz_bench", resources=flyte.Resources(cpu=1, memory=("2Gi", "2Gi")))


@rf.rightsize
@env.task
async def leaf(x: int, path: str) -> int:
    return x


def main() -> int:
    n = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 10_000
    full = "--full" in sys.argv
    global DATA
    tmp = tempfile.mkdtemp(prefix="rsz-bench-")
    import pyarrow as pa
    import pyarrow.parquet as pq

    pq.write_table(pa.table({"a": list(range(10_000)), "b": [float(i) for i in range(10_000)]}), f"{tmp}/part-0.parquet")
    DATA = f"file://{tmp}/part-0.parquet"
    sources: dict = {}
    e = rs.Engine({"history": {"uri": "memory://", "durable": True, "shared": True},
                   "flyte": {"shapes": [{"name": "n", "cpu": 16000, "memory": 64 << 30, "price_per_hour": 0.8}]}},
                  env_overrides=False)
    eng.reset_for_tests(engine=e)

    import flyte._internal.controllers as ctrls
    from flyte._context import internal_ctx
    from flyte.models import ActionID, RawDataPath, TaskContext
    from flyte.report import Report

    class Fake:
        async def submit(self, task, *a, **k):
            return 0

    ctrls._ControllerState.controller = Fake()
    tctx = TaskContext(action=ActionID(name="a0", run_name="r0", project="p", domain="d", org="o"), version="v",
                       raw_data_path=RawDataPath(path=tmp), output_path=tmp, run_base_dir=tmp, report=Report(name="a0"),
                       custom_context=rf.run_context(mode="enforce"))

    timings: list = []

    async def one(i: int) -> None:
        stack = leaf._rs_stack()
        t0 = time.perf_counter()
        plan = leaf._rs_plan(stack, stack, (1, DATA), {})
        d = await leaf._rs_adecide(plan, None)
        sources[d.source.kind] = sources.get(d.source.kind, 0) + 1
        copy = leaf._rs_copy(d, plan, None)
        if full:
            from rightsizer.flyte._task import _aoutcome
            from rightsizer_runtime.wire import RunStatus

            await _aoutcome(leaf._rs_record(d, plan, copy, RunStatus.SUCCEEDED, None, "inv", 1, t0))
        timings.append(time.perf_counter() - t0)

    async def run_all() -> float:
        await one(-1)  # warm: the miss that fills the memo
        timings.clear()
        t0 = time.perf_counter()
        await asyncio.gather(*(one(i) for i in range(n)))
        return time.perf_counter() - t0

    with internal_ctx().replace_task_context(tctx):
        wall = asyncio.run(run_all())
        # Sequential, for the per-call latency without queueing behind the fan-out.
        seq: list = []

        async def seq_run() -> None:
            for i in range(min(n, 2000)):
                t0 = time.perf_counter()
                await one(i)
                seq.append(time.perf_counter() - t0)

        timings_fanout = list(timings)
        asyncio.run(seq_run())
    q = sorted(seq)
    out = {
        "calls": n,
        "full": full,
        "fanout_wall_s": round(wall, 3),
        "fanout_per_call_ms": round(1000 * wall / n, 3),
        "sequential_p50_ms": round(1000 * statistics.median(q), 3),
        "sequential_p99_ms": round(1000 * q[int(0.99 * (len(q) - 1))], 3),
        "sequential_mean_ms": round(1000 * statistics.fmean(q), 3),
        "counters": dict(eng.counters),
        "decision_sources": sources,
    }
    print(json.dumps(out))
    return 0 if out["fanout_per_call_ms"] < 2.0 and out["sequential_p50_ms"] < 2.0 else 1


if __name__ == "__main__":
    sys.exit(main())
