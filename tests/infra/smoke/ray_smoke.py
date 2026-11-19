"""Ray smoke test: remote functions with `memory=`, then read task status back
through the State API (ray.util.state) and the node's Prometheus metrics.

Run via `tests/infra/ray.sh smoke` (sets RAY_ADDRESS / RAY_API_SERVER_ADDRESS).
"""

from __future__ import annotations

import os
import signal
import sys
import time
import urllib.request

import ray
from ray.util.state import get_task, list_tasks

MiB = 1024**2


@ray.remote(memory=256 * MiB, num_cpus=0.5)
def ok_task(x: int) -> int:
    return x * 2


@ray.remote(memory=128 * MiB, max_retries=0)
def raising_task() -> None:
    raise ValueError("boom")


@ray.remote(memory=128 * MiB, max_retries=0)
def killed_task() -> None:
    # Stand-in for an OOM kill: the worker dies by SIGKILL. (Ray's memory
    # monitor, which raises OutOfMemoryError, is Linux-only.)
    os.kill(os.getpid(), signal.SIGKILL)


TERMINAL = {"FINISHED", "FAILED"}


def wait_state(task_id: str, timeout: float = 30.0):
    deadline = time.time() + timeout
    while True:
        t = get_task(task_id)
        # get_task returns a TaskState, or a list of attempts in some versions.
        if isinstance(t, list):
            t = t[-1] if t else None
        if t is not None and t.state in TERMINAL:
            return t
        if time.time() > deadline:
            raise TimeoutError(f"task {task_id} not terminal: {t}")
        time.sleep(0.5)


def main() -> int:
    ray.init(address=os.environ["RAY_ADDRESS"], logging_level="WARNING")
    print(f"ray {ray.__version__}; cluster resources: {ray.cluster_resources()}")

    refs = {"ok": ok_task.remote(21), "raise": raising_task.remote(), "killed": killed_task.remote()}
    assert ray.get(refs["ok"]) == 42
    for name in ("raise", "killed"):
        try:
            ray.get(refs[name])
        except Exception as e:  # noqa: BLE001
            print(f"{name}: ray.get raised {type(e).__module__}.{type(e).__name__}")

    failures = 0
    expect = {"ok": ("FINISHED", None), "raise": ("FAILED", "TASK_EXECUTION_EXCEPTION"), "killed": ("FAILED", "WORKER_DIED")}
    for name, ref in refs.items():
        t = wait_state(ref.task_id().hex())
        print(
            f"{name}: state={t.state} error_type={t.error_type} "
            f"required_resources={t.required_resources} func={t.func_or_class_name} "
            f"worker_pid={t.worker_pid} attempt={t.attempt_number}"
        )
        want_state, want_err = expect[name]
        if t.state != want_state or (want_err and t.error_type != want_err):
            print(f"  UNEXPECTED: want state={want_state} error_type={want_err}", file=sys.stderr)
            failures += 1
        if name == "ok" and t.required_resources.get("memory") != 256 * MiB:
            print("  UNEXPECTED: memory= not reflected in required_resources", file=sys.stderr)
            failures += 1

    # Bulk listing with server-side filters (what an importer would use).
    failed = list_tasks(filters=[("state", "=", "FAILED")], detail=True, limit=100)
    print(f"list_tasks(state=FAILED): {[(t.func_or_class_name, t.error_type) for t in failed]}")

    # Node-level Prometheus metrics (ray_tasks{State=...}, ray_node_mem_used, ...).
    node = ray.nodes()[0]
    port = node.get("MetricsExportPort")
    try:
        body = urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5).read().decode()
        names = sorted({line.split("{")[0] for line in body.splitlines() if line.startswith("ray_")})
        print(f"metrics :{port}/metrics exposes {len(names)} ray_* series, e.g. "
              f"{[n for n in names if n.startswith(('ray_tasks', 'ray_node_mem', 'ray_component_rss'))][:6]}")
    except Exception as e:  # noqa: BLE001
        print(f"metrics endpoint on port {port} not readable: {e}")

    ray.shutdown()
    print("RAY SMOKE: " + ("PASS" if failures == 0 else f"FAIL ({failures})"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
