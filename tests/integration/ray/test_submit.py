"""(d) ``rsz ray submit`` propagates RIGHTSIZER__* through runtime_env.env_vars;
the job's rs.ray functions find the store and the policy, and their workers
write RunRecords there."""

from __future__ import annotations

import json
import os
import subprocess
import sys

from conftest import HERE, REPO, record, run_records


def test_rsz_ray_submit_propagates_store_and_policy(store):
    rsz = os.path.join(REPO, "target", "debug", "rsz")
    if not os.path.exists(rsz):
        subprocess.run(["cargo", "build", "-p", "rightsizer-cli"], cwd=REPO, check=True)
    env = dict(os.environ)
    env.update({
        "RIGHTSIZER_PYTHON": sys.executable,
        "RIGHTSIZER__HISTORY__URI": store,
        "RIGHTSIZER__HISTORY__DURABLE": "true",
        "RIGHTSIZER__HISTORY__SHARED": "true",
        "RIGHTSIZER__POLICY__MODE": "enforce",
        "RIGHTSIZER__RAY__TENANT": "it/submit",
    })
    p = subprocess.run(
        [rsz, "ray", "-o", "json", "submit", "--address", os.environ["RAY_API_SERVER_ADDRESS"],
         "--working-dir", os.path.join(HERE, "jobs"), "--entrypoint-num-cpus", "1", "--entrypoint-memory", "512Mi",
         "--", sys.executable, "job_rsz.py"],
        env=env, capture_output=True, text=True, timeout=300,
    )
    assert p.returncode == 0, p.stderr[-3000:] + p.stdout[-3000:]
    res = json.loads(p.stdout)
    line = next(ln for ln in res["logs"].splitlines() if ln.startswith("RSZ-JOB "))
    job = json.loads(line[len("RSZ-JOB "):])
    recs = run_records(store, "it.submit.work")
    data = {"submitted": {k: res[k] for k in ("submission_id", "entrypoint", "env_vars", "driver", "status")},
            "job_saw": job, "records_in_store": len(recs),
            "record_keys": sorted({r["key"] for r in recs}), "record_statuses": [r["status"] for r in recs]}
    record("submit", data)
    assert res["status"] == "SUCCEEDED"
    assert "RIGHTSIZER__HISTORY__URI" in res["env_vars"] and "RIGHTSIZER__POLICY__MODE" in res["env_vars"]
    assert job["store"] == store and job["mode"] == "enforce" and job["tenant"] == "it/submit"
    assert all(t["store_env"] == store for t in job["tasks"])  # workers see it too
    assert len(recs) == 2 and data["record_keys"] == ["ray://it/submit/it.submit.work"]
    assert res["driver"] == {"entrypoint_num_cpus": 1, "entrypoint_memory": 512 * 2**20}  # no evidence: the prior
