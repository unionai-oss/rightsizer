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
PREFIX = os.environ.get("RSZ_IT_PREFIX") or f"rsz-it/{time.strftime('%Y%m%d-%H%M%S')}"
STORE = f"s3://flyte-data/{PREFIX}"
OUT_DIR = os.path.join(STATE, "flyte-it")
RESULTS = os.path.join(OUT_DIR, f"results-{PREFIX.replace('/', '_')}.json")

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
import faulthandler  # noqa: E402
import signal  # noqa: E402

faulthandler.register(signal.SIGUSR1, all_threads=True)  # `kill -USR1 <pid>` dumps every thread's stack (hang diagnosis)

import flyte  # noqa: E402
import flyte.remote as remote  # noqa: E402

import rightsizer.flyte as rf  # noqa: E402
from rightsizer.flyte._store import Store  # noqa: E402

RESULT: Dict[str, Any] = {"store": STORE, "scenarios": {}}
if os.path.exists(RESULTS):  # scenarios run in several invocations against one prefix accumulate
    with open(RESULTS) as _f:
        RESULT = json.load(_f)


def save() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
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
    # Poll instead of run.wait(): its watch stream once never returned after the run had
    # SUCCEEDED (the harness hung for 2.5 h in round 3 of scenario a).
    deadline = t0 + float(os.environ.get("RSZ_IT_RUN_TIMEOUT_S", "3600"))
    while True:
        try:
            r = remote.Run.get(run.name)
            if r.done():
                break
        except Exception as e:  # noqa: BLE001
            log("poll raised", type(e).__name__, str(e)[:200])
        if time.time() > deadline:
            log("run timed out in the harness", run.name)
            break
        time.sleep(3)
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


_CROCK = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def ulid_ms(u: str) -> int:
    v = 0
    for ch in u[:10].upper():
        v = v * 32 + _CROCK.index(ch)
    return v


RSZ = os.environ.get("RSZ_IT_RSZ") or os.path.join(STATE, "flyte-rsz")  # built with --features s3


def rsz(*args: str, timeout: int = 900) -> subprocess.CompletedProcess:
    return subprocess.run([RSZ, "--store", STORE, *args], capture_output=True, text=True, timeout=timeout)


def export_records() -> List[Dict[str, Any]]:
    """Every run record of the store: all run shards (the harness never deletes any).
    Not ``rsz history export --jsonl``: it reads each key's ``latest`` pointer (last K) plus the
    shards above its watermark, so after a same-day compaction it misses retained records
    (finding, M1 QA)."""
    seen: Dict[tuple, Dict[str, Any]] = {}
    st = Store(STORE)
    for p, _ in st.list("v1/runs"):
        if not p.endswith(".json"):
            continue
        raw = st.get(p)
        if not raw:
            continue
        r = json.loads(raw)
        seen[(r["key"], r["invocation_id"], r["attempt"], (r.get("source") or {}).get("kind"))] = r
    return list(seen.values())


def breaker_trips() -> Any:
    p = rsz("-o", "json", "breaker", "status")
    try:
        return json.loads(p.stdout)
    except Exception:
        return {"rc": p.returncode, "stdout": p.stdout[-500:], "stderr": p.stderr[-500:]}


def all_decisions() -> List[Dict[str, Any]]:
    return list(Store(STORE).decisions("flyte"))


def _mem(spec: Any) -> Any:
    return ((spec or {}).get("requests") or {}).get("memory")


def decision_summary(ds: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_mode, by_source, changes, held = Counter(), Counter(), Counter(), Counter()
    per_key: Dict[str, Counter] = defaultdict(Counter)
    for d in ds:
        by_mode[d.get("mode")] += 1
        src = d.get("source") or {}
        by_source[f"{src.get('kind')}:{src.get('reason') or src.get('id')}"] += 1
        a, p = _mem(d.get("applied")), _mem((d.get("prior") or {}).get("spec"))
        kind = "same"
        if d.get("mode") == "enforce" and a is not None and p is not None:
            kind = "reduction" if a < p else "increase" if a > p else "same"
        changes[kind] += 1
        short = d["key"].rsplit("/", 1)[-1]
        per_key[short][kind] += 1
        for h in d.get("held") or []:
            held[f"{h.get('axis')}:{h.get('reason')}"] += 1
    return {"decisions": len(ds), "by_mode": dict(by_mode), "by_source": dict(by_source), "memory_changes": dict(changes),
            "held": dict(held), "per_task": {k: dict(v) for k, v in sorted(per_key.items())}}


def record_summary(recs: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_source, by_status = Counter(), Counter()
    per_key: Dict[str, Counter] = defaultdict(Counter)
    failures = []
    for r in recs:
        src = (r.get("source") or {}).get("kind")
        by_source[src] += 1
        by_status[r.get("status")] += 1
        per_key[r["key"].rsplit("/", 1)[-1]][f"{src}:{r.get('status')}"] += 1
        if r.get("status") not in ("succeeded", "cached"):
            failures.append({k: r.get(k) for k in ("key", "invocation_id", "attempt", "status", "source", "decision_id", "requested", "origin_prior", "memory_peak")})
    return {"records": len(recs), "by_source": dict(by_source), "by_status": dict(by_status),
            "per_task": {k: dict(v) for k, v in sorted(per_key.items())}, "failures": failures[:50], "n_failures": len(failures)}


def window(items: List[Dict[str, Any]], t0: float, t1: float, idf: str = "id") -> List[Dict[str, Any]]:
    lo, hi = int(t0 * 1000) - 5000, int(t1 * 1000) + 120000  # child records land after the run ends
    return [x for x in items if x.get(idf) and lo <= ulid_ms(x[idf]) <= hi]


def store_summary(store: Store, task_filter: str = "") -> Dict[str, Any]:
    recs = export_records()
    if task_filter:
        recs = [r for r in recs if task_filter in r["key"]]
    return record_summary(recs)


def enforce_runner(**kw: Any) -> Any:
    return rf.with_runcontext(rightsize="enforce", **kw)


def compact() -> Dict[str, Any]:
    """``rsz history compact`` (operator role): the scheduled singleton of SPEC §8.1."""
    p = rsz("-o", "json", "history", "compact", timeout=600)
    try:
        return json.loads(p.stdout)
    except Exception:
        return {"rc": p.returncode, "stdout": p.stdout[-500:], "stderr": p.stderr[-500:]}


def oom_pods(run: str) -> int:
    return sum(1 for p in WATCH.for_run(run) if p.get("reason") == "OOMKilled")


# ── scenarios ───────────────────────────────────────────────────────────


def s_smoke(w: Any) -> Dict[str, Any]:
    return run_and_wait(enforce_runner(), w.small, 3)


def s_a(w: Any, rounds: int = int(os.environ.get("RSZ_IT_ROUNDS", "12"))) -> Dict[str, Any]:
    out: Dict[str, Any] = {"rounds": []}
    t_start = time.time()
    for i in range(rounds):
        t0 = time.time()
        res = run_and_wait(enforce_runner(), w.demo, i)
        res["t0"], res["t1"] = t0, time.time()
        res["oom_pods"] = oom_pods(res["run"])
        res["compact"] = compact()
        out["rounds"].append(res)
        o = res.get("output") if isinstance(res.get("output"), dict) else {}
        log("round", i, res.get("phase"), {k: o.get(k) for k in ("invocations", "errors", "wall_s", "counters", "decision_ms_p50", "decision_ms_p99")})
        save()
    time.sleep(20)  # the last children's records
    ds = all_decisions()
    recs = export_records()
    for r in out["rounds"]:
        rd = window(ds, r["t0"], r["t1"])
        r["store_decisions"] = decision_summary(rd)
    out["decisions"] = decision_summary(window(ds, t_start, time.time()))
    out["records"] = record_summary([r for r in recs if r["key"].endswith((".etl", ".hog", ".hog_tight", ".demo"))])
    out["breakers"] = breaker_trips()
    tot = Counter()
    for r in out["rounds"]:
        o = r.get("output") if isinstance(r.get("output"), dict) else {}
        tot["invocations"] += o.get("invocations", 0)
        tot["task_errors"] += len(o.get("errors") or [])
        tot["run_failed"] += 0 if "SUCCEEDED" in str(r.get("phase")) else 1
        tot["oom_pods"] += r.get("oom_pods", 0)
        for k, v in (o.get("counters") or {}).items():
            tot[f"counters.{k}"] += v
        for k, v in (o.get("metrics") or {}).items():
            if "caused_failures" in k or "budget_expired" in k:
                tot[k] += v
    out["totals"] = dict(tot)
    return out


def s_b(w: Any) -> Dict[str, Any]:
    t0 = time.time()
    res = run_and_wait(enforce_runner(), w.fanout, 1000, 60)
    time.sleep(20)
    res["store_decisions"] = decision_summary([d for d in window(all_decisions(), t0, time.time()) if d["key"].endswith(".tiny")])
    res["oom_pods"] = oom_pods(res["run"])
    return res


def s_c(w: Any) -> Dict[str, Any]:
    t0 = time.time()
    res = run_and_wait(enforce_runner(), w.oom_scenario)
    time.sleep(15)
    res["pods"] = sorted(WATCH.for_run(res["run"]), key=lambda p: p["pod"])
    recs = [r for r in export_records() if r["key"].endswith((".oom_victim", ".oom_victim_retries"))]
    res["records"] = [{k: r.get(k) for k in ("key", "invocation_id", "attempt", "status", "source", "trust", "requested", "memory_peak")}
                      for r in sorted(recs, key=lambda r: (r["key"], r["invocation_id"], r["attempt"], r["source"]["kind"]))]
    res["decisions"] = [{k: d.get(k) for k in ("id", "key", "mode", "applied", "prior", "source", "held")}
                        for d in window(all_decisions(), t0, time.time()) if "oom_victim" in d["key"]]
    res["breakers"] = breaker_trips()
    return res


def s_d(w: Any) -> Dict[str, Any]:
    t0 = time.time()
    res = run_and_wait(enforce_runner(), w.reuse_scenario, 3)
    time.sleep(10)
    res["pods"] = WATCH.for_run(res["run"])
    res["decisions_for_reused"] = len([d for d in window(all_decisions(), t0, time.time()) if d["key"].endswith(".reused")])
    res["records"] = [{k: r.get(k) for k in ("key", "status", "source", "memory_peak", "decision_id")}
                      for r in export_records() if r["key"].endswith(".reused")]
    return res


def s_e(w: Any) -> Dict[str, Any]:
    t0 = time.time()
    res = run_and_wait(enforce_runner(interactive_mode=True), w.small, 4)
    time.sleep(10)
    res["decisions"] = decision_summary(window(all_decisions(), t0, time.time()))
    return res


def s_f(w: Any) -> Dict[str, Any]:
    before_d = len(all_decisions())
    before_r = len(export_records())
    t0 = time.time()
    r = flyte.with_runcontext(mode="local", custom_context=rf.run_context(mode="enforce")).run(w.small, 2)
    time.sleep(3)
    out = json.loads(r.outputs()[0]) if hasattr(r, "outputs") else None
    return {"local_output": out, "decisions_before": before_d, "decisions_after": len(all_decisions()),
            "records_before": before_r, "records_after": len(export_records()), "wall_s": round(time.time() - t0, 1)}


def _remote_resources(name: str, version: str) -> Any:
    t = remote.Task.get(name, version=version).fetch()
    tmpl = t.pb2.spec.task_template
    from flyteidl2.core import tasks_pb2

    n = tasks_pb2.Resources.ResourceName.Name
    res = tmpl.container.resources
    return {"requests": {n(e.name): e.value for e in res.requests}, "limits": {n(e.name): e.value for e in res.limits}}


def s_g(w: Any) -> Dict[str, Any]:
    """Deploy-time sizing: root runs of ``deployed`` (history under the host's tenant), then
    ``rsz flyte deploy --reconcile`` (reductions allowed) and ``--restore``."""
    out: Dict[str, Any] = {"history_runs": []}
    for i in range(int(os.environ.get("RSZ_IT_DEPLOY_HISTORY", "6"))):
        r = run_and_wait(enforce_runner(), w.deployed, i)
        out["history_runs"].append({k: r.get(k) for k in ("run", "phase")})
    time.sleep(10)
    env = {**os.environ, "RIGHTSIZER_PYTHON": sys.executable}
    for label, extra in (("plan", ["--plan", "--reconcile"]), ("sized", ["--reconcile"]), ("restore", ["--restore"])):
        cmd = [RSZ, "flyte", "deploy", "--config", FLYTE_CONFIG, *extra, os.path.join(HERE, "workflows.py"), "deploy_env"]
        p = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=900)
        out[label] = {"rc": p.returncode, "stdout": p.stdout[-3000:], "stderr": p.stderr[-1500:]}
        if label != "plan":
            try:
                tasks = list(remote.Task.listall(by_task_name="rsz_it_deploy.deployed", sort_by=("created_at", "desc"), limit=1))
                v = tasks[0].version if tasks else None
                out[label]["latest_version"] = v
                out[label]["remote_resources"] = _remote_resources("rsz_it_deploy.deployed", v) if v else None
            except Exception as e:  # noqa: BLE001
                out[label]["remote_error"] = f"{type(e).__name__}: {e}"[:400]
    out["deploy_records"] = rf._deploy.deploy_records(Store(STORE))
    return out


def s_h(w: Any) -> Dict[str, Any]:
    res = run_and_wait(enforce_runner(), w.nn_driver, 3)
    time.sleep(10)
    res["pods"] = WATCH.for_run(res["run"])
    res["records"] = [{k: r.get(k) for k in ("key", "status", "source", "memory_peak", "decision_id")}
                      for r in export_records() if ".nn_" in r["key"]]
    return res


def s_i(w: Any) -> Dict[str, Any]:
    """Scalar memory (request only): the pod gets limit = request, and a reduction behind the ladder is enforced."""
    res = run_and_wait(enforce_runner(), w.demo, 99, 4, 4, 0, 10)
    time.sleep(3)
    res["pods"] = [p for p in WATCH.for_run(res["run"]) if p["task"] and ("etl" in p["task"] or "hog" in p["task"])]
    return res


def s_backfill(w: Any) -> Dict[str, Any]:
    """``rsz history import flyte`` (operator role → ``verified``), then again (inserts 0)."""
    env = {**os.environ, "RIGHTSIZER_PYTHON": sys.executable, "RIGHTSIZER__HISTORY__URI": STORE}
    out: Dict[str, Any] = {}
    before = export_records()
    for label in ("first", "again"):
        cmd = [RSZ, "history", "import", "flyte", "--config", FLYTE_CONFIG, "--project", "flytesnacks", "--domain",
               "development", "--since", os.environ.get("RSZ_IT_BACKFILL_SINCE", "6h"), "--limit", "1000"]
        t0 = time.time()
        p = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=3600)
        try:
            rep = json.loads(p.stdout[p.stdout.index("{"):])
        except Exception:
            rep = {"stdout": p.stdout[-1500:]}
        out[label] = {"rc": p.returncode, "report": rep, "stderr": p.stderr[-1500:], "wall_s": round(time.time() - t0, 1)}
    after = export_records()
    imported = [r for r in after if (r.get("source") or {}).get("kind") == "flyte_remote"]
    out["flyte_remote_records"] = len(imported)
    out["by_trust"] = dict(Counter(r.get("trust") for r in imported))
    out["by_status"] = dict(Counter(r.get("status") for r in imported))
    out["tenants"] = dict(Counter(r["key"].split("/")[2] for r in imported))
    out["records_before"], out["records_after"] = len(before), len(after)
    return out


def s_overhead(w: Any) -> Dict[str, Any]:
    return run_and_wait(enforce_runner(), w.overhead, int(os.environ.get("RSZ_IT_OVERHEAD_N", "10000")))


SCENARIOS = {"smoke": s_smoke, "a": s_a, "b": s_b, "c": s_c, "d": s_d, "e": s_e, "f": s_f, "g": s_g, "h": s_h, "i": s_i,
             "backfill": s_backfill, "overhead": s_overhead}


def main(argv: List[str]) -> int:
    names = argv or list(SCENARIOS)
    # Bundle only this directory: the repo's editable rightsizer sources (with a macOS _core) must not ship.
    flyte.init_from_config(FLYTE_CONFIG, root_dir=__import__('pathlib').Path(HERE))
    import workflows as w

    # Pre-build the task images once (pushed to the devbox registry on 127.0.0.1:30500).
    for img in {id(e.image): e.image for e in (w.leaf_env, w.nonative_env)}.values():
        b = flyte.build(img, force=os.environ.get("RSZ_IT_FORCE_BUILD") == "1")  # 1 after a fresh devbox registry
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
    try:
        RESULT["store_summary"] = store_summary(Store(STORE))
    except Exception as e:  # noqa: BLE001
        RESULT["store_summary"] = {"error": str(e)[:500]}
    RESULT["pods_seen"] = len(WATCH.pods)
    save()
    log("results ->", RESULTS)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
