"""p50/p90/p99 from a Prometheus histogram in a text exposition (all label sets summed).

usage: histogram.py METRICS_FILE NAME
"""

import json
import re
import sys

path, name = sys.argv[1], sys.argv[2]
buckets, count = {}, 0.0
for line in open(path):
    if line.startswith("#"):
        continue
    m = re.match(rf'{name}_bucket{{(.*)}} ([0-9.e+-]+)$', line.strip())
    if m:
        le = re.search(r'le="([^"]+)"', m.group(1)).group(1)
        b = float("inf") if le == "+Inf" else float(le)
        buckets[b] = buckets.get(b, 0.0) + float(m.group(2))
    m = re.match(rf'{name}_count(?:{{.*}})? ([0-9.e+-]+)$', line.strip())
    if m:
        count += float(m.group(1))
edges = sorted(buckets)


def q(p):
    target = p * count
    prev_b, prev_c = 0.0, 0.0
    for b in edges:
        c = buckets[b]
        if c >= target:
            if b == float("inf"):
                return prev_b
            frac = 0 if c == prev_c else (target - prev_c) / (c - prev_c)
            return prev_b + (b - prev_b) * frac
        prev_b, prev_c = b, c
    return prev_b


print(json.dumps({"count": int(count), "p50": round(q(0.5), 5), "p90": round(q(0.9), 5), "p99": round(q(0.99), 5)}))
