"""Fire AdmissionReviews at the webhook (CREATE of a Job, unique uid/name each).

usage: load.py --url URL --ca CA --namespace NS --manifest FILE -n N -c CONCURRENCY
Prints client-side latency quantiles (JSON). Requests are dryRun=false, so
decisions are recorded (the realistic path).
"""

import argparse
import concurrent.futures as cf
import json
import ssl
import time
import urllib.request
import uuid

ap = argparse.ArgumentParser()
ap.add_argument("--url", required=True)
ap.add_argument("--ca", required=True)
ap.add_argument("--namespace", required=True)
ap.add_argument("--manifest", required=True)
ap.add_argument("-n", type=int, default=1000)
ap.add_argument("-c", type=int, default=8)
a = ap.parse_args()


def load_yaml_job(path):
    # The manifests here are simple; a JSON conversion via the job fixture keeps
    # this script dependency-free: we build the object directly.
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "train", "namespace": a.namespace},
        "spec": {
            "backoffLimit": 1,
            "template": {
                "spec": {
                    "restartPolicy": "Never",
                    "containers": [
                        {
                            "name": "main",
                            "image": "busybox:latest",
                            "command": ["sh", "-c", "sleep 1"],
                            "resources": {"requests": {"cpu": "1", "memory": "2Gi"}, "limits": {"memory": "2Gi"}},
                        }
                    ],
                }
            },
        },
    }


ctx = ssl.create_default_context(cafile=a.ca)
ctx.check_hostname = False
base = load_yaml_job(a.manifest)


def one(i):
    obj = json.loads(json.dumps(base))
    obj["metadata"]["name"] = f"train-{i}"
    review = {
        "apiVersion": "admission.k8s.io/v1",
        "kind": "AdmissionReview",
        "request": {
            "uid": str(uuid.uuid4()),
            "kind": {"group": "batch", "version": "v1", "kind": "Job"},
            "resource": {"group": "batch", "version": "v1", "resource": "jobs"},
            "operation": "CREATE",
            "namespace": a.namespace,
            "name": obj["metadata"]["name"],
            "userInfo": {"username": "load"},
            "dryRun": False,
            "object": obj,
        },
    }
    body = json.dumps(review).encode()
    t0 = time.perf_counter()
    req = urllib.request.Request(a.url, data=body, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, context=ctx, timeout=10) as r:
        resp = json.load(r)
    dt = time.perf_counter() - t0
    return dt, bool(resp["response"].get("patch"))


t0 = time.perf_counter()
with cf.ThreadPoolExecutor(a.c) as ex:
    res = list(ex.map(one, range(a.n)))
wall = time.perf_counter() - t0
lat = sorted(r[0] for r in res)
q = lambda p: round(lat[min(len(lat) - 1, int(p * len(lat)))], 4)
print(json.dumps({"n": len(lat), "patched": sum(r[1] for r in res), "client_p50": q(0.5), "client_p99": q(0.99), "rps": round(len(lat) / wall, 1)}))
