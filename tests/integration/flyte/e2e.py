"""Flyte devbox integration test for rightsizer.flyte (M1; flyte.md §13).

    RSZ_IT_FLYTE=1 tests/integration/flyte/run.sh [scenario ...]

Scenarios (default: all, in this order): smoke, a (demo rounds), b (1,000-way
fan-out), c (forced OOM), d (reusable), e (pickled bundles), f (local mode),
g (deploy round trip), h (no native module), i (request-only memory),
backfill. Evidence goes to ``tests/infra/.state/flyte-it-results.json``.

Needs: ``tests/infra/flyte.sh up``; wheels from ``tests/infra/build-wheels.sh``.
Everything talks to the isolated devbox only (its SDK config, its kubeconfig,
its object store on 127.0.0.1:30002).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from collections import Counter, defaultdict
from typing import Any, Dict, List

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
STATE = os.path.join(REPO, "tests", "infra", ".state")
FLYTE_CONFIG = os.environ.get("FLYTE_CONFIG", os.path.join(STATE, "flyte-config.yaml"))
KUBECONFIG = os.environ.get("KUBECONFIG_RSZ", os.path.join(STATE, "rsz-it-flyte.kubeconfig"))
RESULTS = os.path.join(STATE, "flyte-it-results.json")
PREFIX = os.environ.get("RSZ_IT_PREFIX") or f"rsz-it/{time.strftime('%Y%m%d-%H%M%S')}"
STORE = f"s3://flyte-data/{PREFIX}"

# Host-side store credentials (pods use FLYTE_AWS_* → AWS_*); never injected into pods.
os.environ.update({
    "AWS_ENDPOINT": "http://127.0.0.1:30002", "AWS_ACCESS_KEY_ID": "rustfs", "AWS_SECRET_ACCESS_KEY": "rustfsstorage",
    "AWS_ALLOW_HTTP": "true", "AWS_REGION": "us-east-1",
})
# rightsizer configuration, injected into every task pod by RightsizedEnvironment.
os.environ["RIGHTSIZER__HISTORY__URI"] = STORE
os.environ["RIGHTSIZER__FLYTE__SHAPES"] = json.dumps(
    [{"name": "devbox", "cpu": 18000, "memory": 33598169088, "price_per_hour": 0.5}])
os.environ.setdefault("RIGHTSIZER_FLYTE_WHEELS", os.path.join(STATE, "wheels"))
os.environ["KUBECONFIG"] = KUBECONFIG
sys.path.insert(0, HERE)

import flyte  # noqa: E402
import flyte.remote as remote  # noqa: E402

import rightsizer.flyte as rf  # noqa: E402
from rightsizer.flyte._store import Store  # noqa: E402

RESULT: Dict[str, Any] = {"store": STORE, "scenarios": {}}


def save() -> None:
    with open(RESULTS, "w") as f:
        json.dump(RESULT, f, indent=2, default=str)


def log(*a: Any) -> None:
    print(time.strftime("%H:%M:%S"), *a, flush=True)


class PodWatcher(threading.Thread):
    """Captures task pod specs (they are deleted soon after completion)."""

    def __init__(self) -> None:
        super().__init__(daemon=True)
        self.pods: Dict[str, Dict[str, Any]] = {}
        self.stop = threading.Event()

    def run(self) -> None:
        while not self.stop.is_set():
            try:
                out = subprocess.run(["kubectl", "get", "pods", "-n", "flyte", "-l", "flyte.org/run", "-o", "json"],
                                     capture_output=True, text=True, timeout=30, env={**os.environ, "KUBECONFIG": KUBECONFIG})
                for p in json.loads(out.stdout or "{}").get("items", []):
                    name = p["metadata"]["name"]
                    prev = self.pods.get(name)
                    if prev is None or p.get("status", {}).get("containerStatuses"):
                        self.pods[name] = p
            except Exception as e:  # noqa: BLE001
                log("watcher:", e)
            time.sleep(1.5)

    def for_run(self, run: str) -> List[Dict[str, Any]]:
        return [describe(p) for p in self.pods.values() if p["metadata"].get("labels", {}).get("flyte.org/run") == run]


def describe(p: Dict[str, Any]) -> Dict[str, Any]:
    c = p["spec"]["containers"][0]
    env = {e["name"]: e.get("value") for e in c.get("env", []) if "value" in e}
    st = (p.get("status", {}).get("containerStatuses") or [{}])[0]
    term = st.get("state", {}).get("terminated") or st.get("lastState", {}).get("terminated") or {}
    dec = json.loads(env["RIGHTSIZER_DECISION"]) if env.get("RIGHTSIZER_DECISION") else None
    return {"pod": p["metadata"]["name"], "task": p["metadata"].get("labels", {}).get("task-name"),
            "resources": c.get("resources", {}), "decision": dec, "attempt": env.get("RIGHTSIZER_ATTEMPT"),
            "reason": term.get("reason"), "exit": term.get("exitCode"), "image": c.get("image")}


WATCH = PodWatcher()


def run_and_wait(runner: Any, task: Any, *args: Any, **kwargs: Any) -> Dict[str, Any]:
    t0 = time.time()
    run = runner.run(task, *args, **kwargs)
    log(f"run {run.name} ({task.name}) {getattr(run, 'url', '')}")
    try:
        run.wait(quiet=True)
    except Exception as e:  # noqa: BLE001
        log("wait raised", type(e).__name__, e)
    r = remote.Run.get(run.name)
    out: Dict[str, Any] = {"run": run.name, "phase": str(r.phase), "wall_s": round(time.time() - t0, 1)}
    try:
        o = r.outputs()
        out["output"] = json.loads(o[0]) if o and isinstance(o[0], str) else o
    except Exception as e:  # noqa: BLE001
        out["output_error"] = f"{type(e).__name__}: {e}"[:300]
        try:
            ad = r.details().action_details
            ei = ad.error_info
            out["error"] = {"code": getattr(ei, "code", None), "message": (ad.error_message or "")[:400]}
        except Exception:
            pass
    return out


def store_summary(store: Store, task_filter: str = "") -> Dict[str, Any]:
    """Counts of run records and decisions under the store (all keys of the test tenant)."""
    recs = list(store.all_runs("flyte"))
    if task_filter:
        recs = [r for r in recs if task_filter in r.key]
    by_source = Counter((r.source.kind.value if hasattr(r.source.kind, "value") else str(r.source.kind)) for r in recs)
    by_status = Counter(r.status.value for r in recs)
    return {"records": len(recs), "by_source": dict(by_source), "by_status": dict(by_status)}


def decisions(store: Store) -> List[Dict[str, Any]]:
    return list(store.decisions("flyte"))


def enforce_runner(**kw: Any) -> Any:
    return rf.with_runcontext(rightsize="enforce", **kw)


# ── scenarios ───────────────────────────────────────────────────────────


RSZ = os.environ.get("RSZ_IT_RSZ") or os.path.join(STATE, "flyte-rsz")  # built with --features s3


def compact() -> Dict[str, Any]:
    """``rsz history compact`` (operator role): the scheduled singleton of SPEC §8.1."""
    p = subprocess.run([RSZ, "--store", STORE, "-o", "json", "history", "compact"], capture_output=True, text=True, timeout=600)
    try:
        return json.loads(p.stdout)
    except Exception:
        return {"rc": p.returncode, "stdout": p.stdout[-500:], "stderr": p.stderr[-500:]}


def s_smoke(w: Any) -> Dict[str, Any]:
    return run_and_wait(enforce_runner(), w.small, 3)


def s_a(w: Any, rounds: int = int(os.environ.get("RSZ_IT_ROUNDS", "10"))) -> Dict[str, Any]:
    out = {"rounds": []}
    for i in range(rounds):
        res = run_and_wait(enforce_runner(), w.demo, i)
        out["rounds"].append(res)
        res["compact"] = compact()
        log("round", i, res.get("phase"), {k: res.get("output", {}).get(k) for k in ("invocations", "wall_s", "counters")}
            if isinstance(res.get("output"), dict) else res)
        save()
    return out


def s_b(w: Any) -> Dict[str, Any]:
    return run_and_wait(enforce_runner(), w.fanout, 1000, 60)


def s_c(w: Any) -> Dict[str, Any]:
    res = run_and_wait(enforce_runner(), w.oom_scenario)
    time.sleep(3)
    res["pods"] = sorted(WATCH.for_run(res["run"]), key=lambda p: p["pod"])
    return res


def s_d(w: Any) -> Dict[str, Any]:
    res = run_and_wait(enforce_runner(), w.reuse_scenario, 3)
    time.sleep(3)
    res["pods"] = WATCH.for_run(res["run"])
    return res


def s_e(w: Any) -> Dict[str, Any]:
    return run_and_wait(enforce_runner(interactive_mode=True), w.small, 4)


def s_f(w: Any) -> Dict[str, Any]:
    store = Store(STORE)
    before = len(decisions(store))
    t0 = time.time()
    r = flyte.with_runcontext(mode="local", custom_context=rf.run_context(mode="enforce")).run(w.small, 2)
    after = len(decisions(store))
    out = json.loads(r.outputs()[0]) if hasattr(r, "outputs") else None
    return {"local_output": out, "decisions_before": before, "decisions_after": after, "wall_s": round(time.time() - t0, 1)}


def _remote_resources(name: str, version: str) -> Any:
    t = remote.Task.get(name, version=version).fetch()
    req, lim = t.resources
    from flyteidl2.core import tasks_pb2

    n = tasks_pb2.Resources.ResourceName.Name
    return {"requests": {n(e.name): e.value for e in req}, "limits": {n(e.name): e.value for e in lim}}


def s_g(w: Any) -> Dict[str, Any]:
    # History for the deploy task first (so a decision has evidence), then deploy / restore through rsz.
    run_and_wait(enforce_runner(), w.small, 1)
    rsz = RSZ
    env = {**os.environ, "RIGHTSIZER_PYTHON": sys.executable}
    out: Dict[str, Any] = {}
    for label, extra in (("sized", []), ("restore", ["--restore"])):
        cmd = [rsz, "flyte", "deploy", "--config", FLYTE_CONFIG, *extra, os.path.join(HERE, "workflows.py"), "deploy_env"]
        p = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=900)
        out[label] = {"rc": p.returncode, "stdout": p.stdout[-1500:], "stderr": p.stderr[-1500:]}
    store = Store(STORE)
    out["deploy_records"] = rf._deploy.deploy_records(store)
    return out


def s_h(w: Any) -> Dict[str, Any]:
    res = run_and_wait(enforce_runner(), w.nn_driver, 3)
    time.sleep(3)
    res["pods"] = WATCH.for_run(res["run"])
    return res


def s_i(w: Any) -> Dict[str, Any]:
    """Scalar memory (request only): the pod gets limit = request, and a reduction behind the ladder is enforced."""
    res = run_and_wait(enforce_runner(), w.demo, 99, 4, 4, 0, 10)
    time.sleep(3)
    res["pods"] = [p for p in WATCH.for_run(res["run"]) if p["task"] and ("etl" in p["task"] or "hog" in p["task"])]
    return res


SCENARIOS = {"smoke": s_smoke, "a": s_a, "b": s_b, "c": s_c, "d": s_d, "e": s_e, "f": s_f, "g": s_g, "h": s_h, "i": s_i}


def main(argv: List[str]) -> int:
    names = argv or list(SCENARIOS)
    # Bundle only this directory: the repo's editable rightsizer sources (with a macOS _core) must not ship.
    flyte.init_from_config(FLYTE_CONFIG, root_dir=__import__('pathlib').Path(HERE))
    import workflows as w

    # Pre-build the task images once (pushed to the devbox registry on 127.0.0.1:30500).
    for img in {id(e.image): e.image for e in (w.leaf_env, w.nonative_env)}.values():
        b = flyte.build(img)
        log("image", b.uri)
    WATCH.start()
    RESULT["prefix"] = PREFIX
    for n in names:
        log("=== scenario", n)
        t0 = time.time()
        try:
            RESULT["scenarios"][n] = SCENARIOS[n](w)
        except Exception as e:  # noqa: BLE001
            import traceback

            RESULT["scenarios"][n] = {"exception": f"{type(e).__name__}: {e}", "tb": traceback.format_exc()[-3000:]}
        RESULT["scenarios"][n]["elapsed_s"] = round(time.time() - t0, 1) if isinstance(RESULT["scenarios"][n], dict) else None
        save()
    store = Store(STORE)
    RESULT["store_summary"] = store_summary(store)
    RESULT["pods_seen"] = len(WATCH.pods)
    save()
    log("results ->", RESULTS)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
