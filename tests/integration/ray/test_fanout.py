"""(a) 10k-task fan-out: overhead of a rightsized ``.remote()`` with memoized
decisions vs plain ``ray.remote`` (target < 5 ms per task, ray.md §2)."""

from __future__ import annotations

import os
import statistics
import time

import rayit_tasks as T
from conftest import engine, record, run_records

N = int(os.environ.get("RSZ_IT_RAY_FANOUT", "10000"))


def _pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))]


def _submit(f, n):
    per = []
    t0 = time.perf_counter()
    refs = []
    for i in range(n):
        a = time.perf_counter()
        refs.append(f.remote(i))
        per.append(time.perf_counter() - a)
    submit_s = time.perf_counter() - t0
    return refs, per, submit_s


def test_fanout_overhead(ray_session, store):
    ray = ray_session
    import rightsizer.ray as rr
    from rightsizer.ray import _remote

    e = engine(store, ttl="10m")
    rr.configure(engine=e)

    # Time the decision path (decide + merge) inside .remote().
    decide_times = []
    orig = _remote.decide_options

    def timed(*a, **k):
        t = time.perf_counter()
        try:
            return orig(*a, **k)
        finally:
            decide_times.append(time.perf_counter() - t)

    _remote.decide_options = timed
    try:
        plain = ray.remote(num_cpus=0.25, memory=64 * 2**20)(T.noop)
        sized = rr.remote(num_cpus=0.25, memory=64 * 2**20, key="it.fanout")(T.noop)
        sizing_only = rr.rightsize(ray.remote(num_cpus=0.25, memory=64 * 2**20)(T.noop), key="it.fanout-sizing")
        # Warm-up: workers, function export, the memo.
        for f in (plain, sized, sizing_only):
            ray.get([f.remote(i) for i in range(200)])
        rr.observer().wait_idle(60)
        decide_times.clear()

        results = {}
        for name, f in (("plain", plain), ("rightsized", sized), ("sizing_only", sizing_only)):
            t0 = time.perf_counter()
            refs, per, submit_s = _submit(f, N)
            out = ray.get(refs)
            wall = time.perf_counter() - t0
            assert out == list(range(N))
            results[name] = {
                "submit_s": round(submit_s, 3),
                "wall_s": round(wall, 3),
                "per_call_p50_ms": round(statistics.median(per) * 1e3, 4),
                "per_call_p99_ms": round(_pct(per, 0.99) * 1e3, 4),
                "per_call_mean_ms": round(submit_s / N * 1e3, 4),
            }
        t_obs = time.perf_counter()
        rr.observer().wait_idle(120)
        obs_drain_s = time.perf_counter() - t_obs
    finally:
        _remote.decide_options = orig

    dec = [x for x in decide_times]
    rs_calls = dec[: 2 * N]
    p = results["plain"]
    for name in ("rightsized", "sizing_only"):
        r = results[name]
        r["added_per_call_mean_ms"] = round(r["per_call_mean_ms"] - p["per_call_mean_ms"], 4)
        r["added_per_call_p50_ms"] = round(r["per_call_p50_ms"] - p["per_call_p50_ms"], 4)
        r["added_wall_per_task_ms"] = round((r["wall_s"] - p["wall_s"]) / N * 1e3, 4)
    m = e.metrics()
    memo_hits = sum(float(ln.rsplit(" ", 1)[1]) for ln in m.splitlines()
                    if ln.startswith("rightsizer_decisions_total") and 'source="memo"' in ln)
    time.sleep(1.5)  # last worker writes
    recs = run_records(store, "it.fanout")
    measured = [r for r in recs if r["key"].endswith("/it.fanout")]
    data = {
        "tasks": N,
        "results": results,
        "decision_path_ms": {"p50": round(statistics.median(rs_calls) * 1e3, 4),
                             "p99": round(_pct(rs_calls, 0.99) * 1e3, 4),
                             "max": round(max(rs_calls) * 1e3, 3), "n": len(rs_calls)},
        "memo_hit_decisions": memo_hits,
        "observer": dict(rr.observer().stats, drain_after_get_s=round(obs_drain_s, 3)),
        "in_task_records_for_measured_fn": len(measured),
        "sample_record": measured[-1] if measured else None,
    }
    record("fanout", data)
    assert data["decision_path_ms"]["p99"] < 5.0
    assert results["rightsized"]["added_per_call_mean_ms"] < 5.0
    assert results["sizing_only"]["added_per_call_mean_ms"] < 5.0
    assert len(measured) >= N  # every measured task wrote its RunRecord
