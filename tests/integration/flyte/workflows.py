"""Demo workflows for the Flyte devbox integration test (``e2e.py``; flyte.md §13).

Every task is a module-level attribute (the default resolver loads tasks by
name). Images: one task image with the locally built Linux wheels of
``rightsizer`` and ``rightsizer-runtime`` (``RIGHTSIZER_FLYTE_WHEELS`` on the
host; ``auto_image`` uses the SDK's ``PythonWheels`` layer) plus ``pyarrow``,
and a variant with the native module deleted (scenario h).

Environment (set on the host, injected into pods by ``RightsizedEnvironment``):
``RIGHTSIZER__HISTORY__URI`` (the devbox's object store) and
``RIGHTSIZER__FLYTE__SHAPES`` (the devbox node).
"""

from __future__ import annotations

import asyncio
import json
import os
import statistics
import time
from typing import Dict, List

import flyte
import flyte.io

import rightsizer as rs
import rightsizer.flyte as rf

WHEELS = os.environ.get("RIGHTSIZER_FLYTE_WHEELS") or True

BASE = flyte.Image.from_debian_base(python_version=(3, 12), platform=("linux/arm64",)).with_pip_packages("pyarrow")
# Scenario h: the same image without the native module (rightsizer's Python code and rightsizer-runtime only).
NO_NATIVE = rf._env.rightsizer_image(BASE, WHEELS).with_commands(
    ["rm -f /opt/venv/lib/python3.12/site-packages/rightsizer/_core*.so "
     "$(python -c 'import os, rightsizer; print(os.path.dirname(rightsizer.__file__))')/_core*.so"]
)

MiB = 1 << 20

# ── leaves ──────────────────────────────────────────────────────────────

leaf_env = rf.RightsizedEnvironment(
    name="rsz_it_leaf", image=BASE, resources=flyte.Resources(cpu="250m", memory="1Gi"), auto_image=WHEELS,
)


@leaf_env.task
async def etl(data: flyte.io.File) -> int:
    """Read a Parquet file and aggregate it (memory grows with the file)."""
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    path = await data.download()
    t = pq.read_table(path)
    total = 0
    for col in t.column_names:
        if str(t[col].type).startswith(("int", "double", "float")):
            total += int(pc.sum(t[col]).as_py() or 0)
    return total % 1_000_003


@leaf_env.task
async def hog(mib: int, seconds: float = 2.0) -> int:
    """Hold ``mib`` MiB for ``seconds``."""
    chunks = [bytearray(MiB) for _ in range(mib)]
    for c in chunks:
        c[0] = 1
    await asyncio.sleep(seconds)
    return len(chunks)


tight_env = rf.RightsizedEnvironment(
    name="rsz_it_tight", image=BASE, resources=flyte.Resources(cpu="250m", memory="400Mi"), auto_image=WHEELS,
)


@tight_env.task
async def hog_tight(mib: int) -> int:
    """Uses close to its 400Mi prior: the decision rule asks for more (an increase)."""
    chunks = [bytearray(MiB) for _ in range(mib)]
    for c in chunks:
        c[0] = 1
    await asyncio.sleep(1.0)
    return len(chunks)


@leaf_env.task
async def tiny(i: int) -> int:
    return i * 2


# ── forced OOM (scenario c) ─────────────────────────────────────────────

oom_env = rf.RightsizedEnvironment(
    name="rsz_it_oom", image=BASE, resources=flyte.Resources(cpu="250m", memory=("1Gi", "1Gi")), auto_image=WHEELS,
)


@oom_env.task
async def oom_victim(mib: int) -> int:
    chunks = [bytearray(MiB) for _ in range(mib)]
    for c in chunks:
        c[0] = 1
    await asyncio.sleep(1.0)
    return len(chunks)


@oom_env.task(retries=1)
async def oom_victim_retries(mib: int) -> int:
    chunks = [bytearray(MiB) for _ in range(mib)]
    for c in chunks:
        c[0] = 1
    return len(chunks)


class Lowball(rs.Estimator):
    """Forces a reduction for the OOM victims only: a demand far below what they use."""

    id = "it-lowball"
    version = "1"

    def estimate(self, ctx):
        if not str(ctx.get("key", "")).endswith(".oom_victim"):
            return None
        return {"memory": {"kind": "point", "value": float(64 * MiB)}, "cpu_mean": {"kind": "point", "value": 0.1}}


if os.environ.get("RSZ_IT_LOWBALL") == "1":
    rs.register_estimator(Lowball())

# ── reusable (scenario d) ───────────────────────────────────────────────

reuse_env = rf.RightsizedEnvironment(
    name="rsz_it_reuse", image=BASE, resources=flyte.Resources(cpu="250m", memory="512Mi"), auto_image=WHEELS,
    reusable=flyte.ReusePolicy(replicas=1, idle_ttl=60),
)


@reuse_env.task
async def reused(i: int) -> int:
    return i + 1


# ── no native module (scenario h) ───────────────────────────────────────

nonative_env = rf.RightsizedEnvironment(
    name="rsz_it_nonative", image=NO_NATIVE, resources=flyte.Resources(cpu="250m", memory="1Gi"), auto_image=False,
    env_vars={"RSZ_IT_LOWBALL": "0"},
)


@nonative_env.task
async def nn_leaf(i: int) -> int:
    return i + 100


@nonative_env.task
async def nn_driver(n: int) -> str:
    with rf.policy(mode="enforce"):
        outs = await asyncio.gather(*(nn_leaf(i) for i in range(n)))
    from rightsizer import _native
    from rightsizer.flyte import _engine as eng

    return json.dumps({"outs": list(outs), "native": _native.available(), "counters": dict(eng.counters)})


# ── deploy-time sizing (scenario g) ─────────────────────────────────────

deploy_env = rf.RightsizedEnvironment(
    name="rsz_it_deploy", image=BASE, resources=flyte.Resources(cpu="250m", memory="2Gi"), auto_image=WHEELS,
)


@deploy_env.task
async def deployed(i: int) -> int:
    chunks = [bytearray(MiB) for _ in range(64)]
    return i + len(chunks)


# ── drivers ─────────────────────────────────────────────────────────────

driver_env = rf.RightsizedEnvironment(
    name="rsz_it_driver", image=BASE, resources=flyte.Resources(cpu="1", memory="2Gi"), auto_image=WHEELS,
    depends_on=[leaf_env, tight_env, oom_env, reuse_env, deploy_env],
    env_vars={"RSZ_IT_LOWBALL": "1"},
)


def _stats() -> str:
    from rightsizer.flyte import _engine as eng

    lat = sorted(eng.latencies_ms)
    q = (lambda p: round(lat[min(len(lat) - 1, int(p * (len(lat) - 1)))], 3)) if lat else (lambda p: None)
    return {"counters": dict(eng.counters), "decision_ms_p50": q(0.5), "decision_ms_p99": q(0.99),
            "decision_ms_max": round(lat[-1], 3) if lat else None, "decisions_timed": len(lat)}


async def _parquet_files(sizes: List[int]) -> List[flyte.io.File]:
    import tempfile

    import pyarrow as pa
    import pyarrow.parquet as pq

    out = []
    d = tempfile.mkdtemp()
    for i, rows in enumerate(sizes):
        p = os.path.join(d, f"part-{i}-{rows}.parquet")
        pq.write_table(pa.table({"a": list(range(rows)), "b": [float(x) for x in range(rows)]}), p, compression=None)
        out.append(await flyte.io.File.from_local(p))
    return out


async def _chunked(coros_fns, chunk: int) -> list:
    results = []
    for i in range(0, len(coros_fns), chunk):
        results += await asyncio.gather(*(f() for f in coros_fns[i:i + chunk]), return_exceptions=True)
    return results


@driver_env.task
async def demo(round_no: int, n_etl: int = 40, n_hog: int = 40, n_tight: int = 10, chunk: int = 45) -> str:
    """One round of the demo workload: ETL over Parquet files, memory hogs, a tight task."""
    files = await _parquet_files([50_000, 200_000, 600_000, 1_200_000])
    calls = []
    for i in range(n_etl):
        f = files[i % len(files)]
        calls.append(lambda f=f: etl(f))
    for i in range(n_hog):
        m = (64, 128, 192)[i % 3]
        calls.append(lambda m=m: hog(m, 1.0))
    for i in range(n_tight):
        calls.append(lambda: hog_tight(300))
    t0 = time.monotonic()
    res = await _chunked(calls, chunk)
    errors = [f"{type(r).__name__}: {r}"[:300] for r in res if isinstance(r, BaseException)]
    return json.dumps({"round": round_no, "invocations": len(calls), "errors": errors,
                       "wall_s": round(time.monotonic() - t0, 1), **_stats()})


@driver_env.task
async def fanout(n: int = 1000, concurrency: int = 60) -> str:
    """A 1,000-way flyte.map under enforce (scenario b)."""
    t0 = time.monotonic()
    outs = []
    async for r in flyte.map.aio(rf.sized(tiny, mode="enforce"), list(range(n)), concurrency=concurrency,
                                 return_exceptions=True):
        outs.append(r)
    errors = [str(r)[:200] for r in outs if isinstance(r, BaseException)]
    return json.dumps({"invocations": n, "errors": errors, "ok": sum(1 for r in outs if not isinstance(r, BaseException)),
                       "wall_s": round(time.monotonic() - t0, 1), **_stats()})


@driver_env.task
async def oom_scenario(need_mib: int = 700, over_prior_mib: int = 1600) -> str:
    """Scenario c: a forced reduction OOMs and escalates; with retries > 0 there is no ladder."""
    out: Dict[str, object] = {}
    with rf.policy(mode="enforce"):
        try:
            out["auto"] = await oom_victim(need_mib)
        except Exception as e:  # must not happen: the ladder recovers it
            out["auto_error"] = f"{type(e).__name__}: {e}"[:300]
        try:
            out["retries"] = await oom_victim_retries(over_prior_mib)
        except Exception as e:  # expected: OOM at the prior itself, retried by the backend, not escalated
            out["retries_error"] = f"{type(e).__name__}: {getattr(e, 'code', '')}"[:300]
            out["retries_status"] = rf.classify(e).value
    return json.dumps({**out, **_stats()})


@driver_env.task
async def reuse_scenario(n: int = 3) -> str:
    with rf.policy(mode="enforce"):
        outs = [await reused(i) for i in range(n)]
    return json.dumps({"outs": outs, **_stats()})


@driver_env.task
async def small(n: int = 3) -> str:
    """For local mode (f) and pickled bundles (e)."""
    outs = await asyncio.gather(*(tiny(i) for i in range(n)), hog(32, 0.1))
    return json.dumps({"outs": list(outs), **_stats()})
