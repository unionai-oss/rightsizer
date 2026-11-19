"""A Ray job submitted with `rsz ray submit`: its rs.ray functions must find
the store and the policy from RIGHTSIZER__* in runtime_env.env_vars."""

import json
import os
import time

import ray

import rightsizer as rs


@rs.ray.remote(num_cpus=1, memory=1 << 30, key="it.submit.work")
def work(mb):
    b = bytearray(mb << 20)
    for j in range(0, len(b), 4096):
        b[j] = 1
    time.sleep(1.0)
    return {"pid": os.getpid(), "store_env": os.environ.get("RIGHTSIZER__HISTORY__URI"),
            "assigned": ray.get_runtime_context().get_assigned_resources()}


ray.init(logging_level="ERROR")
s = rs.ray.settings()
cfg = s.engine.config if s.engine is not None else {}
out = [ray.get(work.remote(64)) for _ in range(2)]
rs.ray.observer().wait_idle(10)
print("RSZ-JOB " + json.dumps({
    "store": s.store, "tenant": s.tenant, "trust": s.trust, "mode": cfg.get("policy", {}).get("mode"),
    "durable": cfg.get("history", {}).get("durable"), "env": sorted(k for k in os.environ if k.startswith("RIGHTSIZER")),
    "tasks": out,
}))
