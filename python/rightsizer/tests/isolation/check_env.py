"""Run inside an isolated environment (see test_isolation.py).

argv: <mode: bare|flyte|ray> <parquet file> <work dir>
"""

import json
import sys

mode, parquet, work = sys.argv[1], sys.argv[2], sys.argv[3]
HEAVY = ("flyte", "ray", "torch", "numpy", "pyarrow", "pandas", "sklearn")

import rightsizer as rs  # noqa: E402

after_import = sorted(m for m in sys.modules if m.split(".")[0] in HEAVY)
assert "rightsizer._core" not in sys.modules, "rightsizer._core imported eagerly"
assert not after_import, f"import rightsizer pulled in {after_import}"

store = f"{work}/store"
eng = rs.Engine({"history": {"uri": store}}, env_overrides=False)


def train(path):
    import pandas as pd

    return pd.read_parquet(path)


ctx = rs.Context(key="iso", prior=rs.Resources(cpu=4, memory="16Gi"), inputs=[rs.Input.from_path(parquet)],
                 code=rs.Code.from_callable(train))
first = rs.recommend(ctx, engine=eng)
assert first.source.kind == "estimator", first
for i in range(4):
    d = rs.recommend(ctx, engine=eng)
    rep = rs.outcome(d, "succeeded", engine=eng, memory_peak="700Mi", cpu_mean=1.0, duration_s=30, invocation_id=f"i{i}")
    assert rep is not None and rep.joined, rep
# A fresh engine on the same store: history comes from the store.
eng2 = rs.Engine({"history": {"uri": store}}, env_overrides=False)
d = rs.recommend(ctx, engine=eng2)
assert d.source.kind == "estimator", d
assert any("demand p50 6" in line or "demand p50 7" in line for line in d.explanation), d.explanation
rs.configure({"history": {"uri": store}}, env_overrides=False)
assert rs.recommend(ctx).source.kind in ("estimator", "memo")

after_decide = sorted(m for m in sys.modules if m.split(".")[0] in HEAVY)
assert not after_decide, f"deciding pulled in {after_decide}"

results = {}
for name in ("flyte", "ray"):
    try:
        __import__(f"rightsizer.{name}")
        results[name] = "ok"
    except ImportError as e:
        results[name] = str(e)
if mode == "bare":
    for name in ("flyte", "ray"):
        assert f"rightsizer[{name}]" in results[name], results
elif mode == "flyte":
    assert results["flyte"] == "ok" and "rightsizer[ray]" in results["ray"], results
elif mode == "ray":
    assert results["ray"] == "ok" and "rightsizer[flyte]" in results["flyte"], results
print(json.dumps({"mode": mode, "after_import": after_import, "after_decide": after_decide, "platforms": results,
                  "memory": d.recommended.requests.memory, "explanation": d.explanation[:3]}))
