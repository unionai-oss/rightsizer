"""Python estimators in the Rust chain, the runtime, GIL and fork behaviour (SPEC §10.3)."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import threading
import time

import pytest

import rightsizer as rs

GiB = 1 << 30


def _cells_at(cell: int, n: int = 22):
    p = [0.0] * n
    p[cell] = 1.0
    return p


class Fixed(rs.Estimator):
    id = "fixed"
    version = "1"

    def __init__(self):
        self.calls = 0
        self.threads = set()
        self.lock = threading.Lock()

    def estimate(self, ctx):
        with self.lock:
            self.calls += 1
            self.threads.add(threading.current_thread().name)
        assert ctx["key"].startswith("python://")
        return rs.Estimate.cells(_cells_at(9), estimator=self.id, version=self.version)  # rs-grid-v1 cell 9


def test_sync_estimator_answers(engine):
    est = Fixed()
    rs.register_estimator(est)
    assert rs.registered_estimators() == ["fixed"]
    assert engine.chain()[0] == "fixed", "registered estimators go in front of the configured chain"
    d = engine.decide(rs.Context(key="py", prior=rs.Resources(cpu=2, memory="16Gi")))
    assert d.source.kind == "estimator" and d.source.id == "fixed" and est.calls == 1
    assert est.threads and threading.current_thread().name not in est.threads, "runs on the blocking pool"
    # Memo hit: no second call.
    d2 = engine.decide(rs.Context(key="py", prior=rs.Resources(cpu=2, memory="16Gi")))
    assert d2.source.kind == "memo" and est.calls == 1


def test_declines_and_failures_pass_on(engine):
    @rs.register_estimator
    def declines(ctx):
        return None

    class Raises(rs.Estimator):
        id = "raises"

        def estimate(self, ctx):
            raise RuntimeError("boom")

    class Says(rs.Estimator):
        id = "says"

        def estimate(self, ctx):
            raise rs.Insufficient("no history")

    rs.register_estimator(Raises)
    rs.register_estimator(Says())
    assert rs.registered_estimators() == ["declines", "raises", "says"]
    d = engine.decide(rs.Context(key="pass", prior=rs.Resources(cpu=2, memory="16Gi"), inputs=[rs.Input.sized("x", GiB, "csv")]))
    assert d.source.kind == "estimator" and d.source.id == "rules-sized"
    m = engine.metrics()
    assert 'component="estimator"' in m or "raises" in m
    assert rs.unregister_estimator("raises") and not rs.unregister_estimator("raises")


def test_bad_estimates_are_estimator_errors(engine):
    rs.register_estimator(lambda ctx: {"memory": {"kind": "cells", "grid": "rs-grid-v1", "probs": "nope"}}, id="bad")
    rs.register_estimator(lambda ctx: 42, id="worse")
    d = engine.decide(rs.Context(key="bad", prior=rs.Resources(cpu=2, memory="16Gi"), inputs=[rs.Input.sized("x", GiB, "csv")]))
    assert d.source.id == "rules-sized"


def test_plain_dict_gets_provenance(engine):
    rs.register_estimator(lambda ctx: {"memory": {"kind": "point", "value": 2.0 * GiB}}, id="dict-est", version="7")
    d = engine.decide(rs.Context(key="dict", prior=rs.Resources(cpu=2, memory="16Gi")))
    assert (d.source.id, d.source.version) == ("dict-est", "7")
    assert d.estimate.provenance["estimator"] == "dict-est"


async def test_async_estimator_runs_on_the_callers_loop(engine):
    seen = {}
    loop = asyncio.get_running_loop()

    class Async(rs.Estimator):
        id = "async-est"

        async def estimate(self, ctx):
            seen["loop"] = asyncio.get_running_loop()
            seen["thread"] = threading.current_thread()
            await asyncio.sleep(0.01)
            return rs.Estimate.cells(_cells_at(8), estimator=self.id)

    rs.register_estimator(Async())
    d = await rs.arecommend(rs.Context(key="aio", prior=rs.Resources(cpu=2, memory="16Gi")), engine=engine)
    assert d.source.id == "async-est"
    assert seen["loop"] is loop and seen["thread"] is threading.current_thread()


def test_async_estimator_from_sync_api(engine):
    seen = {}

    class Async(rs.Estimator):
        id = "async-sync"

        async def estimate(self, ctx):
            seen["thread"] = threading.current_thread().name
            await asyncio.sleep(0)
            return rs.Estimate.cells(_cells_at(8), estimator=self.id)

    rs.register_estimator(Async)
    d = engine.decide(rs.Context(key="aio-sync", prior=rs.Resources(cpu=2, memory="16Gi")))
    assert d.source.id == "async-sync" and seen["thread"] != threading.current_thread().name


async def test_contextvars_reach_async_estimators(engine):
    import contextvars

    var = contextvars.ContextVar("rs_test_var", default=None)
    got = {}

    class Ctx(rs.Estimator):
        id = "ctxvar"

        async def estimate(self, ctx):
            got["v"] = var.get()
            return None

    rs.register_estimator(Ctx())
    var.set("from-caller")
    await engine.adecide(rs.Context(key="cv", prior=rs.Resources(cpu=1, memory="1Gi")))
    assert got["v"] == "from-caller"


def test_gil_is_released_during_decisions(engine):
    """A slow sync estimator runs on another thread; it needs the GIL, so the
    decision would deadlock if the caller held it. Meanwhile, a Python thread
    keeps running."""
    ticks = []
    stop = threading.Event()

    def ticker():
        while not stop.is_set():
            ticks.append(1)
            time.sleep(0.001)

    class Slow(rs.Estimator):
        id = "slow"

        def estimate(self, ctx):
            time.sleep(0.2)
            return None

    rs.register_estimator(Slow())
    t = threading.Thread(target=ticker)
    t.start()
    try:
        before = len(ticks)
        engine.decide(rs.Context(key="gil", prior=rs.Resources(cpu=1, memory="1Gi")), budget_ms=2000)
        during = len(ticks) - before
    finally:
        stop.set()
        t.join()
    assert during > 20, during


def test_budget_expiry_keeps_the_prior(engine):
    class Slower(rs.Estimator):
        id = "slower"

        def estimate(self, ctx):
            time.sleep(0.3)
            return rs.Estimate.cells(_cells_at(3), estimator=self.id)

    rs.register_estimator(Slower())
    t0 = time.perf_counter()
    d = engine.decide(rs.Context(key="budget", prior=rs.Resources(cpu=1, memory="4Gi")), budget_ms=50)
    assert time.perf_counter() - t0 < 0.25
    assert d.is_prior and d.source.reason == "budget" and d.applied.requests.memory == 4 * GiB


def test_fan_out_single_flight_threads(engine):
    est = Fixed()

    def slow_estimate(ctx, _orig=est.estimate):
        time.sleep(0.05)
        return _orig(ctx)

    est.estimate = slow_estimate
    rs.register_estimator(est)
    ctx = rs.Context(key="fan", prior=rs.Resources(cpu=2, memory="16Gi"))
    out = []
    lock = threading.Lock()

    def one():
        d = engine.decide(ctx, budget_ms=5000)
        with lock:
            out.append(d)

    threads = [threading.Thread(target=one) for _ in range(200)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(out) == 200 and est.calls == 1, est.calls
    assert {d.source.kind for d in out} <= {"estimator", "memo"}


async def test_fan_out_single_flight_async(engine):
    calls = []

    class A(rs.Estimator):
        id = "fan-async"

        async def estimate(self, ctx):
            calls.append(1)
            await asyncio.sleep(0.05)
            return rs.Estimate.cells(_cells_at(9), estimator=self.id)

    rs.register_estimator(A())
    ctx = rs.Context(key="fan-async", prior=rs.Resources(cpu=2, memory="16Gi"))
    ds = await asyncio.gather(*[engine.adecide(ctx, budget_ms=5000) for _ in range(3000)])
    assert len(calls) == 1 and all(d.source.id == "fan-async" for d in ds)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs fork")
def test_runtime_and_engine_rebuilt_after_fork(engine, core):
    engine.decide(rs.Context(key="fork", prior=rs.Resources(cpu=1, memory="1Gi")))
    parent = core.runtime_pid()
    assert parent == os.getpid()
    r, w = os.pipe()
    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)  # fork with threads (tokio workers)
        pid = os.fork()
    if pid == 0:  # child
        try:
            d = engine.decide(rs.Context(key="fork", prior=rs.Resources(cpu=1, memory="1Gi")))
            ok = d.source.kind in ("estimator", "memo") and core.runtime_pid() == os.getpid()
            os.write(w, json.dumps({"ok": ok, "pid": core.runtime_pid(), "src": d.source.kind}).encode())
        except BaseException as e:  # pragma: no cover
            os.write(w, json.dumps({"ok": False, "err": repr(e)}).encode())
        finally:
            os._exit(0)
    os.close(w)
    _, status = os.waitpid(pid, 0)
    msg = json.loads(os.read(r, 4096).decode())
    assert status == 0 and msg["ok"], msg
    assert msg["pid"] != parent and core.runtime_pid() == parent
    # The parent still works.
    assert engine.decide(rs.Context(key="fork", prior=rs.Resources(cpu=1, memory="1Gi"))).source.kind in ("estimator", "memo")


def test_estimator_calls_back_into_rightsizer(engine):
    """A Python estimator that itself asks for a decision (re-entrancy from a
    blocking-pool thread, where Runtime::block_on would panic)."""

    class Nested(rs.Estimator):
        id = "nested"

        def estimate(self, ctx):
            inner = engine.decide(rs.Context(key="inner", prior=rs.Resources(cpu=1, memory="2Gi")))
            assert inner.key.endswith("/inner")
            return None

    rs.register_estimator(Nested())
    d = engine.decide(rs.Context(key="outer", prior=rs.Resources(cpu=1, memory="2Gi"), inputs=[rs.Input.sized("x", GiB, "csv")]),
                      budget_ms=5000)
    assert d.source.id == "rules-sized"
