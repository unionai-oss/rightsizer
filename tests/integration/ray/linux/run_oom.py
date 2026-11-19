"""Runs inside rsz-it/ray-linux (Linux, Ray 2.60.0): the raylet's memory
monitor kills a rightsized task → the driver classifies OutOfMemoryError as
OomKilled; plus SIGKILL, MemoryError, and in-task peaks on a reused worker.

The macOS ``_core`` cannot load here, so decisions go through a stand-in
engine (``decide`` keeps the prior; ``outcome`` collects records): this
exercises rightsizer.ray's own code paths (wrapper, observer,
classification), not the decision rule.
"""

import json
import os
import signal
import sys
import time

os.environ.setdefault("RAY_memory_usage_threshold", "0.75")
os.environ.setdefault("RAY_memory_monitor_refresh_ms", "100")
os.environ.setdefault("RAY_task_oom_retries", "0")

import ray  # noqa: E402

import rightsizer as rs  # noqa: E402
import rightsizer.ray as rr  # noqa: E402

OUT = sys.argv[1] if len(sys.argv) > 1 else "/out"
STORE = os.path.join(OUT, "store")


class StandInEngine:
    config = {"ray": {"tenant": "it/linux"}, "history": {"uri": STORE}, "policy": {"mode": "recommend"}}

    def __init__(self):
        self.outcomes = []

    def decide(self, ctx, **kw):
        return rs.Decision.prior_for(ctx, "config_error", "stand-in engine (no _core for linux in this image)")

    def outcome(self, rec):
        self.outcomes.append(rec.to_dict())

    def flush(self):
        return 0


eng = StandInEngine()
rr.configure(engine=eng)
ray.init(num_cpus=1, object_store_memory=150 * 2**20, include_dashboard=True, dashboard_host="127.0.0.1",
         logging_level="ERROR")


@rr.remote(num_cpus=1, memory=256 << 20, max_retries=0, key="linux.hog")
def hog():
    chunks = []
    while True:
        b = bytearray(64 << 20)
        for j in range(0, len(b), 4096):
            b[j] = 1
        chunks.append(b)
        time.sleep(0.05)


@rr.remote(num_cpus=1, max_retries=0, key="linux.sigkill")
def sigkill():
    os.kill(os.getpid(), signal.SIGKILL)


@rr.remote(num_cpus=1, max_retries=0, key="linux.memerr")
def memerr():
    raise MemoryError("simulated")


@rr.remote(num_cpus=1, key="linux.alloc")
def alloc(mb):
    b = bytearray(mb << 20)
    for j in range(0, len(b), 4096):
        b[j] = 1
    time.sleep(1.2)
    return os.getpid()


def outcome(ref):
    try:
        v = ray.get(ref)
        return "ok", v
    except Exception as e:  # noqa: BLE001
        return type(e).__name__, str(e)[:400]


res = {"uname": os.uname().sysname, "ray": ray.__version__}
r_hog = hog.remote()
res["hog"] = outcome(r_hog)
r_kill = sigkill.remote()
res["sigkill"] = outcome(r_kill)
r_mem = memerr.remote()
res["memerr"] = outcome(r_mem)
res["alloc"] = [outcome(alloc.remote(m)) for m in (200, 200, 50, 200)]
rr.observer().wait_idle(30)
time.sleep(1.0)
try:
    from ray.util.state import get_task

    def et(r):
        st = get_task(r.task_id().hex(), timeout=10)
        st = st[-1] if isinstance(st, list) else st
        return [st.state, st.error_type]

    res["state_api"] = {"hog": et(r_hog), "sigkill": et(r_kill), "memerr": et(r_mem)}
except Exception as e:  # noqa: BLE001
    res["state_api"] = f"unavailable: {e}"
res["driver_records"] = [{k: o.get(k) for k in ("key", "status", "status_confidence", "source", "invocation_id")}
                         for o in eng.outcomes]
res["hog_task_id"] = r_hog.task_id().hex()
recs = []
for root, _, files in os.walk(STORE):
    for f in files:
        if f.endswith(".json"):
            r = json.load(open(os.path.join(root, f)))
            recs.append({"key": r["key"], "status": r["status"], "started_at": r["started_at"],
                         "peak_mib": None if not r.get("memory_peak") else round(r["memory_peak"]["value"] / 2**20, 1),
                         "censor": None if not r.get("memory_peak") else r["memory_peak"]["censor"]})
res["worker_records"] = sorted(recs, key=lambda r: r["started_at"])
res["observer"] = dict(rr.observer().stats)
os.makedirs(OUT, exist_ok=True)
json.dump(res, open(os.path.join(OUT, "result.json"), "w"), indent=2, default=str)
print(json.dumps(res, indent=2, default=str))
ray.shutdown()
