"""Seed history: N measured, Verified successes of a key (JSON lines on stdout).

usage: seed.py KEY N PEAK_MIB [STATUS] [REQUEST_MIB]
"""

import datetime as dt
import json
import random
import sys

key, n, peak = sys.argv[1], int(sys.argv[2]), float(sys.argv[3])
status = sys.argv[4] if len(sys.argv) > 4 else "succeeded"
req = float(sys.argv[5]) if len(sys.argv) > 5 else None
now = dt.datetime.now(dt.timezone.utc)
rng = random.Random(7)
for i in range(n):
    started = now - dt.timedelta(hours=2 * (n - i) + 1)
    p = peak * (0.95 + 0.1 * rng.random())
    rec = {
        "v": 1,
        "key": key,
        "cluster": "rsz-it",
        "invocation_id": f"seed-{i:03d}",
        "attempt": 1,
        "started_at": started.isoformat().replace("+00:00", "Z"),
        "status": status,
        "memory_peak": {"value": p * 1048576, "censor": "exact", "resolution_s": 1.0},
        "cpu_mean": {"value": 0.15, "censor": "exact", "resolution_s": 1.0},
        "cpu_peak_window": {"value": 0.3, "censor": "exact", "resolution_s": 1.0},
        "duration_s": 600.0,
        "source": {"kind": "watcher"},
        "trust": "verified",
    }
    if req:
        rec["requested"] = {"requests": {"memory": int(req * 1048576)}, "limits": {}}
    print(json.dumps(rec))
