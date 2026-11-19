"""The typed API (SPEC §10.3) over ``_core``."""

from __future__ import annotations

import json
import sys
import textwrap

import pytest

import rightsizer as rs
from rightsizer import _units
from rightsizer.types import workload_key

GiB = 1 << 30


def train_fn(path):
    import pandas as pd  # noqa: F401 - tokens only

    return pd.read_parquet(path)


# ── building contexts (no _core) ─────────────────────────────────────────


def test_resources_normalize():
    r = rs.Resources(cpu=8, memory="64Gi", gpu="A100-80G:1")
    assert r.requests.cpu == 8000 and r.requests.memory == 64 * GiB
    assert (r.requests.gpu.accel, r.requests.gpu.count) == ("A100-80G", 1)
    r = rs.Resources(cpu=("500m", 2), memory=("1Gi", "2Gi"), gpu="l4-24g")
    assert r.requests.cpu == 500 and r.limits.cpu == 2000 and r.limits.memory == 2 * GiB
    assert r.requests.gpu.accel == "L4-24G" and r.requests.gpu.count == 1
    assert rs.Resources(gpu=2).requests.gpu.accel == "unknown-gpu"
    assert rs.Resources(cpu=0.1).requests.cpu == 100  # decimal text, not binary 0.1
    for bad in (dict(memory="lots"), dict(gpu="V100:1"), dict(gpu="A100-80G:0"), dict(cpu=True), dict(memory="-1Gi")):
        with pytest.raises((ValueError, TypeError)):
            rs.Resources(**bad)


@pytest.mark.parametrize("q", ["64Gi", "1.5Gi", "128974848", "129M", "1e9", "1E3", "1E", "123Mi", "0.5", " 64Gi ",
                               "100m", "1k", "2.5", "1.0001Ki", ".5Gi", "3e-2", "7Ti", "1Ei", "999999n"])
def test_units_match_rust(core, q):
    try:
        want = core.normalize_quantity(q, "memory", "k8s")
    except rs.RightsizerError:
        with pytest.raises(ValueError):
            _units.memory_bytes(q)
    else:
        assert _units.memory_bytes(q) == want
    try:
        want = core.normalize_quantity(q, "cpu", "k8s")
    except rs.RightsizerError:
        with pytest.raises(ValueError):
            _units.cpu_millis(q)
    else:
        assert _units.cpu_millis(q) == want


@pytest.mark.parametrize("v", [8, 0.25, 0.1, 1.5, 1e-07, 3])
def test_numeric_units_match_rust(core, v):
    assert _units.cpu_millis(v) == core.normalize_quantity(v, "cpu", "k8s")
    if float(v).is_integer():
        assert _units.memory_bytes(v) == core.normalize_quantity(v, "memory", "k8s")


def test_normalize_quantity_platforms(core):
    assert core.normalize_quantity("64G", "memory", "slurm") == 64 * GiB
    assert core.normalize_quantity(1536, "memory", "slurm") == 1536 << 20
    assert core.normalize_quantity(1.5, "cpu", "ray") == 1500
    assert core.normalize_quantity(2**30, "memory", "ray") == GiB
    assert core.format_quantity(64 * GiB, "memory", "k8s") == "64Gi"
    assert core.format_quantity(1536 << 20, "memory", "slurm") == "1536M"
    assert core.format_quantity(2500, "cpu", "ray") == 2.5
    with pytest.raises(rs.RightsizerError):
        core.normalize_quantity("1Gi", "memory", "nomad")
    with pytest.raises(rs.RightsizerError):
        core.normalize_quantity("x", "memory", "k8s")


def test_keys():
    assert workload_key("llama-ft") == "python://alice/llama-ft"
    assert workload_key("llama-ft", tenant="team-a") == "python://team-a/llama-ft"
    assert workload_key("flyte://acme/proj/dev/env.train") == "flyte://acme/proj/dev/env.train"
    assert rs.Context(key="x", platform="k8s", tenant="c/ns").workload_key == "k8s://c/ns/x"


def test_code_from_callable_cached_and_fallback():
    c = rs.Code.from_callable(train_fn)
    assert c.kind == "source" and "read_parquet" in c.text
    assert rs.Code.from_callable(train_fn) is c
    ns: dict = {}
    exec("def f(x):\n    return x * 2\n", ns)  # no source available
    d = rs.Code.from_callable(ns["f"])
    assert d.kind == "info" and d.digest.startswith("sha256:")
    ns2: dict = {}
    exec("def f(x):\n    return x * 2\n", ns2)
    assert rs.Code.from_callable(ns2["f"]).digest == d.digest, "digest of __code__ is content-based"


def test_inputs():
    i = rs.Input.from_uri("s3://bucket/data/train.parquet?versionId=3")
    assert i.name == "train.parquet" and i.to_dict() == {"name": "train.parquet", "uri": "s3://bucket/data/train.parquet?versionId=3"}
    assert rs.Input.scalar("epochs", 3).to_dict() == {"name": "epochs", "scalar": {"kind": "int", "value": 3}}
    assert rs.Input.sized("x", 10, "csv").to_dict() == {"name": "x", "bytes": 10, "format": "csv"}


def test_import_is_light():
    code = textwrap.dedent("""
        import sys, rightsizer
        bad = [m for m in ("rightsizer._core", "flyte", "ray", "torch", "numpy") if m in sys.modules]
        assert not bad, bad
        rightsizer.Context(key="k", prior=rightsizer.Resources(memory="1Gi"))
        assert "rightsizer._core" not in sys.modules
    """)
    import subprocess

    subprocess.run([sys.executable, "-c", code], check=True)


# ── decisions ────────────────────────────────────────────────────────────


def test_spec_example_verbatim(core, store):
    rs.configure({"history": {"uri": store}}, env_overrides=False)
    d = rs.recommend(rs.Context(
        key="llama-ft",
        prior=rs.Resources(cpu=8, memory="64Gi", gpu="A100-80G:1"),   # canonical accelerator ids (§5.1.1)
        inputs=[rs.Input.from_uri("s3://bucket/train.parquet")],
        code=rs.Code.from_callable(train_fn),
    ))
    print(d.applied.requests.memory, d.held, d.explanation)
    rs.outcome(d, status="succeeded", memory_peak="41Gi")
    assert d.key == "python://alice/llama-ft"
    assert d.applied.requests.gpu.accel == "A100-80G"
    assert isinstance(d.explanation, tuple) and d.explanation


def test_recommend_with_parquet_and_history(engine, parquet_file):
    ctx = rs.Context(key="etl", prior=rs.Resources(cpu=4, memory="16Gi"), inputs=[rs.Input.from_path(parquet_file)],
                     code=rs.Code.from_callable(train_fn))
    d1 = engine.decide(ctx)
    assert d1.source.kind == "estimator" and d1.source.id == "rules-sized"
    assert d1.estimate is not None and d1.context_digest.startswith("sha256:")
    for i in range(4):
        d = engine.decide(ctx)
        rep = engine.outcome(d, "succeeded", memory_peak="900Mi", cpu_mean=1.5, duration_s=60, invocation_id=f"inv{i}")
        assert rep.written and rep.joined
    d2 = engine.decide(ctx)
    assert any("demand p50 8" in line or "demand p50 9" in line for line in d2.explanation), d2.explanation
    assert d2.recommended.requests.cpu < 4000


def test_explicit_history_is_a_full_context(engine, parquet_file):
    base = rs.Context(key="etl", prior=rs.Resources(cpu=4, memory="16Gi"), inputs=[rs.Input.from_path(parquet_file)])
    d0 = engine.decide(base)
    recs = [rs.build_record(d0, "succeeded", memory_peak="2Gi", duration_s=30, invocation_id=f"i{i}") for i in range(3)]
    assert recs[0].inputs and recs[0].inputs[0].format.value == "parquet" and recs[0].inputs[0].rows == 50_000
    ctx = rs.Context(key="other", prior=rs.Resources(cpu=4, memory="16Gi"), inputs=[rs.Input.from_path(parquet_file)],
                     history=recs)
    req = rs.build_request(ctx)
    assert req["context"]["kind"] == "full" and len(req["context"]["context"]["history"]) == 3
    d = engine.decide(ctx)
    assert d.source.kind == "estimator"
    assert any("demand p50 2" in line or "demand p50 1.9" in line for line in d.explanation), d.explanation


def test_policy_keywords_and_stack(engine):
    ctx = rs.Context(key="p", prior=rs.Resources(cpu=2, memory="4Gi", gpu="A100-80G:1"))
    req = rs.build_request(ctx, mode="enforce", risk=5, pins={"gpu"}, ceiling=rs.Resources(memory="8Gi"), budget_ms=50,
                           estimator="history-peak")
    assert req["policy"] == {"mode": "enforce", "risk": {"oom_penalty": 5.0}, "chain": ["history-peak"]}
    assert req["constraints"]["pinned"] == ["gpu_count", "gpu_type"] and req["constraints"]["ceiling"] == {"memory": 8 * GiB}
    assert req["budget_ms"] == 50
    with rs.policy(mode="observe"):
        assert rs.build_request(ctx)["policy"]["mode"] == "observe"
        d = engine.decide(ctx)
    assert d.mode == rs.Mode.OBSERVE and d.is_prior and d.source.reason == "mode"
    d = engine.decide(ctx, mode="off")
    assert d.mode == rs.Mode.OFF


def test_recommend_never_raises(core, monkeypatch):
    ctx = rs.Context(key="s1", prior=rs.Resources(memory="1Gi"))
    rs.configure("/nonexistent/rightsizer.toml")
    d = rs.recommend(ctx)
    assert d.is_prior and d.source.reason == "config_error" and d.applied.requests.memory == GiB
    rs.configure({"strict": True, "estimator": {"chain": ["nope"]}}, env_overrides=False)
    d = rs.recommend(ctx)
    assert d.is_prior and d.source.reason == "config_error"
    rs.configure({"history": {"uri": "memory://"}}, env_overrides=False)
    d = rs.recommend(ctx, mode="enforc")  # a typo in a keyword: logged, prior kept
    assert d.is_prior and d.source.reason == "internal"
    assert rs.outcome(d, status="not-a-status") is None


def test_missing_native_module(monkeypatch):
    from rightsizer import _native

    monkeypatch.setattr(_native, "_core", None)
    monkeypatch.setattr(_native, "_error", ImportError("no wheel for this platform"))
    rs.configure()
    d = rs.recommend(rs.Context(key="nowheel", prior=rs.Resources(cpu=1, memory="2Gi")))
    assert d.is_prior and d.source.reason == "config_error" and "no wheel" in d.source.detail
    assert d.applied.requests.memory == 2 * GiB
    with pytest.raises(rs.RightsizerError):
        rs.Engine()
    with pytest.raises(rs.RightsizerError):
        rs.register_estimator(lambda ctx: None)


def test_core_decide_never_raises_on_bad_json(core):
    e = core.Engine('{"history": {"uri": "memory://"}}', env_overrides=False)
    d = json.loads(e.decide("{not json"))
    assert d["source"]["kind"] == "prior" and d["source"]["reason"] == "internal"
    d = json.loads(e.decide(json.dumps({"context": {"kind": "seed", "key": "k8s://c/ns/x", "unit": "main",
                                                     "prior": {"spec": {"requests": {"memory": 5}}}}, "budget_ms": "x"})))
    assert d["key"] == "k8s://c/ns/x" and d["applied"]["requests"]["memory"] == 5
    assert json.loads(e.decide_group("[1, 2]"))[0]["source"]["reason"] == "internal"
    with pytest.raises(rs.RightsizerError):
        e.outcome("{}")
    with pytest.raises(rs.RightsizerError):
        core.Engine('{"bogus": 1}')
    with pytest.raises(rs.RightsizerError):
        core.Engine("{}", role="root")


def test_engine_config_resolution(core, tmp_path, monkeypatch):
    (tmp_path / "rightsizer.toml").write_text('[policy]\nmode = "observe"\n[history]\nuri = "memory://"\n')
    e = rs.Engine()
    assert e.config["policy"]["mode"] == "observe"
    monkeypatch.setenv("RIGHTSIZER__POLICY__MODE", "recommend")
    assert rs.Engine().config["policy"]["mode"] == "recommend"
    p = tmp_path / "other.toml"
    p.write_text('[decision]\nmemo_ttl = "1m"\n')
    monkeypatch.setenv("RIGHTSIZER_CONFIG", str(p))
    assert rs.Engine().config["decision"]["memo_ttl"] in ("1m", "60s")
    assert rs.Engine(str(p), store="memory://").config["history"]["uri"] == "memory://"


def test_decide_group(engine):
    ctxs = [rs.Context(key="g", unit=f"w{i}", prior=rs.Resources(cpu=2, memory="8Gi"),
                       inputs=[rs.Input.sized("x", (i + 1) * GiB, "parquet")]) for i in range(3)]
    ds = engine.decide_group(ctxs)
    assert len(ds) == 3 and len({d.id for d in ds}) == 1 and len({d.group for d in ds}) == 1
    assert len({d.recommended.requests.memory for d in ds}) == 1


def test_attempts(engine):
    ctx = rs.Context(key="att", prior=rs.Resources(cpu=2, memory="8Gi"))
    tok = rs.set_attempt_context(rs.AttemptContext(invocation_id="inv-1", attempt=2, max_attempts=3), environ={})
    try:
        d = engine.decide(ctx)
        rec = rs.build_record(d, "oom_killed", memory_peak=GiB, memory_censor="at_least")
    finally:
        rs.attempt_context.reset(tok)
    assert d.key == "python://alice/att"
    assert (rec.invocation_id, rec.attempt) == ("inv-1", 2)
    assert engine.outcome(rec).written
    # Explicit attempt keyword.
    d3 = engine.decide(ctx, attempt={"invocation_id": "inv-1", "attempt": 3, "max_attempts": 3})
    assert d3.applied.requests.memory >= d.applied.requests.memory


async def test_arecommend(engine, parquet_file):
    ctx = rs.Context(key="async", prior=rs.Resources(cpu=4, memory="16Gi"), inputs=[rs.Input.from_path(parquet_file)])
    d = await rs.arecommend(ctx, engine=engine)
    assert d.source.kind == "estimator"
    ds = await engine.adecide_group([ctx, rs.Context(key="async", unit="b", prior=rs.Resources(cpu=4, memory="16Gi"))])
    assert len(ds) == 2
    rep = await rs.aoutcome(d, "succeeded", engine=engine, memory_peak="1Gi")
    assert rep is not None and rep.joined


def test_store_operations(core, store):
    e = rs.Engine({"history": {"uri": store}}, env_overrides=False, role="operator")
    assert e.breaker_status() == []
    assert e.control() is None
    assert e.flush() >= 0
    assert "rightsizer_decisions_total" in e.metrics() or "rightsizer" in e.metrics()
    ctx = rs.Context(key="r", prior=rs.Resources(cpu=2, memory="8Gi"))
    recs = []
    import datetime as dt

    for i in range(6):
        d = e.decide(ctx)
        recs.append(rs.build_record(d, "succeeded", memory_peak="1Gi", duration_s=600, invocation_id=f"r{i}",
                                    started_at=dt.datetime(2026, 11, 1, i, tzinfo=dt.timezone.utc)))
    rep = e.replay(recs, per_run=True, bootstrap=50)
    assert rep["runs"] >= 1 and rep["keys"][0]["key"] == "python://alice/r"
    with pytest.raises(rs.RightsizerError):
        e.replay(recs, bogus=1)


def test_helpers(core):
    g = json.loads(core.rs_grid_v1())
    assert g["id"] == "rs-grid-v1"
    assert json.loads(core.parse_gpu("h100-80g:8")) == {"accel": "H100-80G", "count": 8}
    assert any(a["id"] == "A100-80G" for a in json.loads(core.accelerators()))
    assert core.canonical_key("python://alice//x") == "python://alice/x"
    assert core.key_hash("flyte://acme/proj/dev/env.train") == "eef92d58436c80afb2670dc464a4b4f2"
    info = json.loads(core.code_info("import pandas as pd\npd.read_parquet(x)\n", "python"))
    assert info["digest"].startswith("sha256:") and "tok:read_parquet" in info["tokens"] and "source" not in info
    metas = [{"name": "a", "format": "parquet", "bytes": 3 * GiB}, {"name": "n", "format": "scalar", "scalar": {"kind": "int", "value": 4}}]
    assert core.feature_bucket(json.dumps(metas)) == core.feature_bucket(json.dumps(metas[::-1]))
    ctx = json.load(open(__import__("os").path.join(__import__("os").path.dirname(__file__), "..", "..", "..", "..",
                                                     "tests", "fixtures", "wire", "decision_enforce_request.json")))["context"]["context"]
    assert core.context_digest(json.dumps(ctx)).startswith("sha256:")


def test_read_inputs(core, parquet_file, tmp_path):
    out = json.loads(core.read_inputs(json.dumps([{"name": "t", "uri": parquet_file}, {"name": "missing", "uri": str(tmp_path / "nope.csv")},
                                                  {"name": "local", "uri": "http://127.0.0.1:1/x.parquet"}]), 500))
    by = {m["name"]: m for m in out}
    assert by["t"]["format"] == "parquet" and by["t"]["rows"] == 50_000 and by["t"]["columns"]["total"] == 2
    assert by["t"]["uri_hash"].startswith("sha256:")
    assert by["missing"].get("bytes") is None and by["local"].get("bytes") is None, "unreadable / SSRF-guarded inputs keep what is known"


def test_decision_marker(engine):
    d = engine.decide(rs.Context(key="m", prior=rs.Resources(cpu=1, memory="1Gi")))
    env = d.marker(measured=True)
    assert env.decision_id == d.id and env.key == d.key and env.measured
