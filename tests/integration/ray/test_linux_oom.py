"""(e, Linux) Ray's memory monitor kills a rightsized task → OomKilled.

The memory monitor is Linux-only, so this runs a Ray 2.60.0 head inside a
Linux container (``linux/run.sh``: python:3.12-slim + ray[default], image
rsz-it/ray-linux:2.60.0, container memory 3 GiB, threshold 0.75). Opt-in:
``RSZ_IT_RAY_LINUX=1`` (needs Docker).
"""

from __future__ import annotations

import json
import os
import subprocess

import pytest

from conftest import HERE, RESULTS, record

pytestmark = pytest.mark.skipif(os.environ.get("RSZ_IT_RAY_LINUX") != "1", reason="opt-in: RSZ_IT_RAY_LINUX=1 (Docker)")


def test_memory_monitor_oom_is_classified_oom_killed():
    p = subprocess.run([os.path.join(HERE, "linux", "run.sh"), "run"], capture_output=True, text=True, timeout=900)
    assert p.returncode == 0, p.stderr[-3000:]
    res = json.load(open(os.path.join(RESULTS, "linux", "result.json")))
    record("linux_oom", {k: res[k] for k in ("uname", "ray", "state_api", "driver_records", "worker_records", "observer")})
    assert res["uname"] == "Linux"
    assert res["hog"][0] == "OutOfMemoryError" and res["state_api"]["hog"] == ["FAILED", "OUT_OF_MEMORY"]
    by_key = {r["key"]: r for r in res["driver_records"]}
    hog = by_key["ray://it/linux/linux.hog"]
    assert (hog["status"], hog["status_confidence"], hog["source"]) == ("oom_killed", "observed", {"kind": "ray_state"})
    assert by_key["ray://it/linux/linux.sigkill"]["status"] == "failed"
    assert "ray://it/linux/linux.memerr" not in by_key  # the worker recorded it itself
    allocs = [r for r in res["worker_records"] if r["key"].endswith("linux.alloc")]
    assert [round(r["peak_mib"] / 50) for r in allocs] == [4, 4, 1, 4]  # 200/200/50/200 MiB on one reused worker
