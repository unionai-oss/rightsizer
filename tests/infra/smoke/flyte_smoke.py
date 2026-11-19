"""Flyte v2 devbox smoke test (run via `tests/infra/flyte.sh smoke`).

1. Runs trivial tasks with different `flyte.Resources(memory=...)` shapes and
   records the request/limit that land on the task pod (spec open question 5:
   does a request-only memory spec get limit = request?).
2. Runs a task that exceeds its memory limit and shows how the OOM surfaces in
   the pod status and in flyte.remote.
3. Lists runs/actions through flyte.remote.

Env: FLYTE_CONFIG (SDK config file), KUBECONFIG (devbox kubeconfig).
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import threading
import time

import flyte
import flyte.remote as remote

MEM_SHAPES = {
    "mem_request_only": flyte.Resources(cpu="100m", memory="200Mi"),
    "mem_request_and_limit": flyte.Resources(cpu="100m", memory=("200Mi", "400Mi")),
    "cpu_tuple_mem_scalar": flyte.Resources(cpu=("100m", "500m"), memory="300Mi"),
}

envs = {name: flyte.TaskEnvironment(name=f"rsz_it_{name}", resources=res) for name, res in MEM_SHAPES.items()}
oom_env = flyte.TaskEnvironment(name="rsz_it_oom", resources=flyte.Resources(cpu="100m", memory=("128Mi", "128Mi")))


# Tasks must be module-level attributes: the default resolver loads them by name.
@envs["mem_request_only"].task
async def sleep_mem_request_only(seconds: int) -> str:
    await asyncio.sleep(seconds)
    return "ok"


@envs["mem_request_and_limit"].task
async def sleep_mem_request_and_limit(seconds: int) -> str:
    await asyncio.sleep(seconds)
    return "ok"


@envs["cpu_tuple_mem_scalar"].task
async def sleep_cpu_tuple_mem_scalar(seconds: int) -> str:
    await asyncio.sleep(seconds)
    return "ok"


sleepers = {
    "mem_request_only": sleep_mem_request_only,
    "mem_request_and_limit": sleep_mem_request_and_limit,
    "cpu_tuple_mem_scalar": sleep_cpu_tuple_mem_scalar,
}


@oom_env.task
async def hog(mib: int) -> int:
    chunks = []
    for _ in range(mib // 16):
        chunks.append(bytearray(16 * 2**20))
    return len(chunks)


def run_definition_kind(kind: int) -> str:
    from flyteidl2.workflow import run_definition_pb2

    return run_definition_pb2.ErrorInfo.Kind.Name(kind)


def kubectl_json(*args: str) -> dict:
    out = subprocess.run(["kubectl", *args, "-o", "json"], check=True, capture_output=True, text=True).stdout
    return json.loads(out)


class PodWatcher(threading.Thread):
    """Polls all pods so specs are captured even if Flyte deletes them on completion."""

    def __init__(self) -> None:
        super().__init__(daemon=True)
        self.seen: dict[str, dict] = {}
        self.stop = threading.Event()

    def run(self) -> None:
        while not self.stop.is_set():
            try:
                for p in kubectl_json("get", "pods", "-A")["items"]:
                    if p["metadata"]["namespace"] in ("kube-system", "knative-serving", "kourier-system"):
                        continue
                    self.seen[p["metadata"]["name"]] = p
            except Exception as e:  # noqa: BLE001
                print(f"watcher: {e}", file=sys.stderr)
            time.sleep(1)


def pods_for_run(seen: dict[str, dict], run_name: str) -> list[dict]:
    # Task pods (devbox: namespace `flyte`) are named <run>-<action>-<n> and labelled flyte.org/run=<run>.
    return [p for p in seen.values() if p["metadata"].get("labels", {}).get("flyte.org/run") == run_name]


def describe_container(pod: dict) -> dict:
    c = pod["spec"]["containers"][0]
    statuses = pod.get("status", {}).get("containerStatuses") or [{}]
    st = statuses[0]
    term = (st.get("state", {}).get("terminated") or st.get("lastState", {}).get("terminated") or {})
    return {
        "pod": pod["metadata"]["name"],
        "namespace": pod["metadata"]["namespace"],
        "resources": c.get("resources", {}),
        "phase": pod.get("status", {}).get("phase"),
        "terminated_reason": term.get("reason"),
        "exit_code": term.get("exitCode"),
    }


def main() -> int:
    flyte.init_from_config(os.environ["FLYTE_CONFIG"])
    watcher = PodWatcher()
    watcher.start()
    results: dict[str, dict] = {}
    failures = 0

    for name, task in sleepers.items():
        run = flyte.run(task, seconds=15)
        print(f"[{name}] run {run.name} url={run.url}")
        run.wait(quiet=True)
        run = remote.Run.get(run.name)
        time.sleep(2)
        pods = [describe_container(p) for p in pods_for_run(watcher.seen, run.name)]
        results[name] = {"declared": repr(MEM_SHAPES[name]), "phase": run.phase, "pods": pods}
        print(f"[{name}] phase={run.phase} pods={json.dumps(pods)}")
        if "SUCCEEDED" not in str(run.phase).upper():
            failures += 1

    run = flyte.run(hog, mib=512)
    print(f"[oom] run {run.name}")
    try:
        run.wait(quiet=True)
    except Exception as e:  # noqa: BLE001
        print(f"[oom] wait raised {type(e).__name__}: {e}")
    time.sleep(3)
    run = remote.Run.get(run.name)
    details = run.details()
    action = details.action_details if hasattr(details, "action_details") else None
    err = None
    if action is not None:
        ei = action.error_info
        err = (
            {"kind": run_definition_kind(ei.kind), "code": ei.code, "message": (action.error_message or "")[:300]}
            if ei
            else None
        )
    pods = [describe_container(p) for p in pods_for_run(watcher.seen, run.name)]
    results["oom"] = {"phase": run.phase, "error": err, "attempts": action.attempts if action else None, "pods": pods}
    print(f"[oom] phase={run.phase} error={err} pods={json.dumps(pods)}")

    # flyte.remote listing APIs.
    runs = list(remote.Run.listall(limit=10))
    print(f"Run.listall -> {[(r.name, r.phase) for r in runs]}")
    actions = list(remote.Action.listall(for_run_name=run.name))
    print(f"Action.listall(for_run_name={run.name}) -> {[(a.name, a.phase) for a in actions]}")

    watcher.stop.set()
    out = os.path.join(os.path.dirname(os.environ["FLYTE_CONFIG"]), "flyte-smoke-results.json")
    with open(out, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"results -> {out}")
    print("FLYTE SMOKE: " + ("PASS" if failures == 0 else f"FAIL ({failures})"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
