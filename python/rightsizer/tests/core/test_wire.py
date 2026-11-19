"""Cross-language wire tests (ARCHITECTURE.md §4): Rust-produced fixtures in
``tests/fixtures/wire`` (written by ``cargo test -p rightsizer-py --test
wire_fixtures`` with ``RIGHTSIZER_WRITE_FIXTURES=1``) round-trip through the
runtime's dataclasses and the typed API, validate against ``schemas/``, and
Python-produced records are accepted by Rust."""

from __future__ import annotations

import datetime as dt
import glob
import json
import os

import pytest

import rightsizer as rs
from rightsizer_runtime.wire import (
    Censor,
    InputFormat,
    InputMeta,
    Measurement,
    OutcomeSource,
    Requests,
    ResourceSpec,
    RunRecord,
    RunStatus,
    Trust,
    new_ulid,
    parse_timestamp,
)

from conftest import SCHEMAS, WIRE

jsonschema = pytest.importorskip("jsonschema")

GiB = 1 << 30


def _load(name):
    with open(os.path.join(WIRE, f"{name}.json")) as f:
        return json.load(f)


def _names(prefix):
    return sorted(os.path.basename(p)[:-5] for p in glob.glob(os.path.join(WIRE, f"{prefix}*.json")) if not p.endswith("_request.json"))


def _validator(schema_name):
    with open(os.path.join(SCHEMAS, "core", "1", f"{schema_name}.json")) as f:
        schema = json.load(f)
    return jsonschema.Draft202012Validator(schema)


def test_fixtures_exist():
    assert len(_names("run_record")) >= 4 and len(_names("decision")) >= 4 and len(_names("estimate")) >= 2


@pytest.mark.parametrize("name", _names("run_record"))
def test_run_records_round_trip_and_validate(name):
    raw = _load(name)
    rec = RunRecord.from_dict(raw)  # strict: unknown fields are errors
    back = json.loads(rec.to_json())
    # Python always writes microseconds (IMPLEMENTATION-NOTES, runtime item 4): same instant.
    assert parse_timestamp(back.pop("started_at")) == parse_timestamp(raw["started_at"])
    assert back == {k: v for k, v in raw.items() if k != "started_at"}
    assert RunRecord.from_dict(json.loads(rec.to_json())) == rec
    _validator("run_record").validate(raw)
    _validator("run_record").validate(json.loads(rec.to_json()))


@pytest.mark.parametrize("name", _names("decision"))
def test_decisions_round_trip_and_validate(name):
    raw = _load(name)
    d = rs.Decision.from_dict(raw, allow_unknown=False)
    assert d.to_dict() == raw
    _validator("decision").validate(raw)
    _validator("decision").validate(d.to_dict())
    if d.estimate is not None:
        _validator("estimate").validate(d.estimate.to_dict())


@pytest.mark.parametrize("name", _names("estimate"))
def test_estimates_round_trip_and_validate(name):
    raw = _load(name)
    e = rs.Estimate.from_dict(raw)
    assert e.to_dict() == raw
    _validator("estimate").validate(raw)


def test_enforce_fixture_typed_view():
    d = rs.Decision.from_dict(_load("decision_enforce"))
    assert d.key == "flyte://acme/proj/dev/env.train" and d.source.kind == "estimator"
    assert {h.axis for h in d.held} >= {"gpu_count", "gpu_type"}
    assert d.applied.requests.gpu.accel == "A100-80G" and d.estimate.memory["kind"] == "cells"
    assert d.prior.spec.requests.memory == 64 * GiB


@pytest.mark.parametrize("name", ["decision_enforce", "decision_seed", "decision_off"])
def test_rust_requests_decide_from_python(core, name):
    e = core.Engine('{"history": {"uri": "memory://"}}', env_overrides=False)
    d = json.loads(e.decide(json.dumps(_load(f"{name}_request"))))
    want = _load(name)
    for k in ("key", "unit", "mode", "recommended", "applied", "prior", "held", "source", "context_digest"):
        assert d[k] == want[k], k
    _validator("decision").validate(d)


def _python_records(decision_id):
    t0 = dt.datetime(2026, 11, 18, 12, tzinfo=dt.timezone.utc)
    meta = InputMeta(name="data", format=InputFormat.PARQUET, bytes=8 * GiB, rows=10)
    base = dict(key="flyte://acme/proj/dev/env.train", invocation_id="py-1", attempt=1, started_at=t0,
                source=OutcomeSource.cgroup(), trust=Trust.AUTHENTICATED, decision_id=decision_id)
    return [
        RunRecord(status=RunStatus.SUCCEEDED, memory_peak=Measurement(20.5 * GiB, Censor.EXACT, 1.0), inputs=(meta,),
                  cpu_mean=Measurement(3.25), duration_s=1800.0, t_peak_frac=0.5, gpu_util_mean=0.75,
                  requested=ResourceSpec(Requests(cpu=8000, memory=64 * GiB)), origin_prior=Requests(cpu=8000, memory=64 * GiB),
                  code_digest="sha256:abc", cluster="c1", staged_bytes=128 << 20, **base),
        RunRecord(status=RunStatus.OOM_KILLED, memory_peak=Measurement(64 * GiB, Censor.AT_LEAST), **{**base, "attempt": 2}),
        RunRecord(status=RunStatus.SUCCEEDED, **{**base, "invocation_id": "py-2", "decision_id": None,
                                                 "source": OutcomeSource.importer("flyte"), "trust": Trust.UNVERIFIED}),
    ]


def test_python_records_accepted_by_rust(core):
    e = core.Engine('{"history": {"uri": "memory://"}}', env_overrides=False)
    for rec in _python_records(new_ulid()):
        raw = json.loads(rec.to_json())
        _validator("run_record").validate(raw)
        rep = json.loads(e.outcome(rec.to_json()))
        assert rep["written"] is True


def test_python_records_survive_a_rust_store(core, tmp_path):
    """Python-written records come back out of Rust's store unchanged in meaning."""
    eng = rs.Engine({"history": {"uri": str(tmp_path / "s")}}, env_overrides=False)
    ctx = rs.Context(key="flyte://acme/proj/dev/env.train", platform="flyte", prior=rs.Resources(cpu=8, memory="64Gi"))
    d = eng.decide(ctx)
    for rec in _python_records(d.id)[:1]:
        assert eng.outcome(rec).joined
    report = eng.replay(_python_records(d.id)[:1])
    assert report["keys"][0]["key"] == "flyte://acme/proj/dev/env.train"


def test_typed_api_records_validate(engine):
    d = engine.decide(rs.Context(key="w", prior=rs.Resources(cpu=2, memory="8Gi"), inputs=[rs.Input.sized("x", GiB, "csv")]))
    rec = rs.build_record(d, "succeeded", memory_peak="3Gi", cpu_mean="1500m", duration_s=12.5)
    raw = json.loads(rec.to_json())
    _validator("run_record").validate(raw)
    assert raw["memory_peak"] == {"value": 3.0 * GiB, "censor": "exact"} and raw["cpu_mean"]["value"] == 1.5
    assert raw["requested"] == d.applied.to_dict() and raw["origin_prior"] == d.prior.spec.requests.to_dict()
    assert raw["inputs"] == [{"name": "x", "format": "csv", "bytes": GiB}]


def test_typed_requests_validate_against_schemas(engine, parquet_file):
    ctx = rs.Context(key="v", prior=rs.Resources(cpu=2, memory=("8Gi", "16Gi"), gpu="L4-24G:1"),
                     inputs=[rs.Input.from_path(parquet_file), rs.Input.scalar("epochs", 3)],
                     code=rs.Code.from_source("print(1)\n"), labels={"team": "ml"})
    req = rs.build_request(ctx, pins=["gpu"], ceiling=rs.Resources(memory="32Gi"), allowed_shapes=["L4-24G:1"])
    ci = req["context"]
    _validator("context_seed").validate(ci["seed"])
    _validator("prior").validate(ci["prior"])
    _validator("platform_ref").validate(ci["platform"])
    _validator("constraints").validate(req["constraints"])
    full = rs.build_request(rs.Context(key="v", prior=ctx.prior, inputs=ctx.inputs, history=[]))["context"]["context"]
    _validator("workload_context").validate(full)
