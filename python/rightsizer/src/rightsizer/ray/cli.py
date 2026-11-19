"""``python -m rightsizer.ray.cli`` (= ``rsz ray …`` = ``rightsizer ray …``).

::

    rsz ray submit [--address URL] [--runtime-env-json JSON] [--working-dir DIR] [--submission-id ID]
                   [--entrypoint-num-cpus N] [--entrypoint-num-gpus N] [--entrypoint-memory Q]
                   [--key KEY] [--mode MODE] [--no-wait] [--timeout S] -- python job.py ...
    rsz ray env                     # the RIGHTSIZER__* a submitted job would get

``submit`` waits for the job and prints its logs unless ``--no-wait``; the
exit code is 0 when the job succeeded, 1 when it failed or stopped, 2 on a
usage error. ``-o json`` prints machine-readable output.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import List, Optional

USAGE = "usage: rsz ray {submit,env} ... (rsz ray submit -- python job.py)"


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="rsz ray", description="rightsizer for Ray (ray.md)")
    p.add_argument("-o", "--output", choices=("text", "json"), default="text")
    sub = p.add_subparsers(dest="cmd")
    s = sub.add_parser("submit", help="submit a Ray job with rightsizer's store and policy (ray.md §5)")
    s.add_argument("--address", help="Ray dashboard / Jobs API address (default: [ray] address, RAY_API_SERVER_ADDRESS)")
    s.add_argument("--runtime-env-json", default=None, help="runtime_env as JSON (env_vars are merged)")
    s.add_argument("--working-dir", default=None, help="runtime_env working_dir")
    s.add_argument("--submission-id", default=None)
    s.add_argument("--entrypoint-num-cpus", type=float, default=None, help="the driver's CPU prior")
    s.add_argument("--entrypoint-num-gpus", type=float, default=None, help="the driver's GPU prior")
    s.add_argument("--entrypoint-memory", default=None, help="the driver's memory prior (bytes or 4Gi)")
    s.add_argument("--key", default=None, help="driver key suffix (default driver.<script>)")
    s.add_argument("--mode", default=None, help="mode of the driver decision")
    s.add_argument("--no-wait", action="store_true", help="return after submitting")
    s.add_argument("--timeout", type=float, default=3600.0, help="seconds to wait for the job")
    s.add_argument("entrypoint", nargs=argparse.REMAINDER, help="-- COMMAND ...")
    sub.add_parser("env", help="print the RIGHTSIZER__* variables a submitted job gets")
    return p


def _print(out: str, obj: dict, text: str) -> None:
    if out == "json":
        print(json.dumps(obj, indent=2, default=str))
    else:
        print(text)


def main(argv: Optional[List[str]] = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] in ("-h", "--help", "help"):
        print(USAGE, file=sys.stdout if argv else sys.stderr)
        _parser().print_help(sys.stdout if argv else sys.stderr)
        return 0 if argv else 2
    a = _parser().parse_args(argv)
    if a.cmd == "env":
        from ._jobs import propagated_env

        env = propagated_env()
        _print(a.output, env, "\n".join(f"{k}={v}" for k, v in sorted(env.items())))
        return 0
    if a.cmd != "submit":
        print(USAGE, file=sys.stderr)
        return 2
    ep = list(a.entrypoint)
    if ep and ep[0] == "--":
        ep = ep[1:]
    if not ep:
        print("rsz ray submit: missing the entrypoint (rsz ray submit -- python job.py)", file=sys.stderr)
        return 2
    rt = json.loads(a.runtime_env_json) if a.runtime_env_json else {}
    if a.working_dir:
        rt["working_dir"] = a.working_dir
    mem = None
    if a.entrypoint_memory is not None:
        from .._units import memory_bytes

        mem = memory_bytes(a.entrypoint_memory)
    from ._jobs import resolve_address, submit

    res = submit(ep, address=a.address, runtime_env=rt, submission_id=a.submission_id,
                 entrypoint_num_cpus=a.entrypoint_num_cpus, entrypoint_num_gpus=a.entrypoint_num_gpus,
                 entrypoint_memory=mem, key=a.key, mode=a.mode)
    sid = res["submission_id"]
    if a.no_wait:
        _print(a.output, res, f"submitted {sid}\n  env: {', '.join(res['env_vars'])}\n  driver: {res['driver'] or 'unchanged'}")
        return 0
    from ray.job_submission import JobStatus, JobSubmissionClient

    client = JobSubmissionClient(resolve_address(a.address))
    deadline = time.monotonic() + a.timeout
    status = client.get_job_status(sid)
    while status not in (JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.STOPPED) and time.monotonic() < deadline:
        time.sleep(1.0)
        status = client.get_job_status(sid)
    logs = client.get_job_logs(sid)
    res["status"] = str(status.value if hasattr(status, "value") else status)
    res["logs"] = logs
    if a.output == "json":
        _print("json", res, "")
    else:
        sys.stdout.write(logs)
        print(f"rsz ray: job {sid} {res['status']} (env: {', '.join(res['env_vars'])}; driver: {res['driver'] or 'unchanged'})")
    return 0 if status == JobStatus.SUCCEEDED else 1


if __name__ == "__main__":
    sys.exit(main())
