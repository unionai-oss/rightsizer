"""Worker container resources and rayStartParams agree (num-cpus, memory).

usage: check_ray.py RESOURCES_JSON RAYSTARTPARAMS_JSON
"""

import json
import math
import sys

UNITS = {"Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "Ti": 2**40, "k": 10**3, "M": 10**6, "G": 10**9}


def cpu(q):
    q = str(q)
    return int(q[:-1]) / 1000 if q.endswith("m") else float(q)


def mem(q):
    q = str(q)
    for u, f in UNITS.items():
        if q.endswith(u):
            return int(float(q[: -len(u)]) * f)
    return int(q)


res, params = json.loads(sys.argv[1]), json.loads(sys.argv[2])
c = res.get("limits", {}).get("cpu") or res["requests"]["cpu"]
m = res.get("limits", {}).get("memory") or res["requests"]["memory"]
ok = int(params["num-cpus"]) == math.ceil(cpu(c)) and int(params["memory"]) == mem(m)
changed = cpu(res["requests"]["cpu"]) != 2 or mem(res["requests"]["memory"]) != 2 * 2**30
print(f"num-cpus={params['num-cpus']} cpu={c} memory={params['memory']} limit={m} changed={changed}")
sys.exit(0 if ok and changed else 1)
