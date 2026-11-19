import datetime as dt
import json
import threading

import pytest

from rightsizer_runtime.wire import (
    AttemptContext,
    Censor,
    ColumnsInfo,
    Confidence,
    GpuShape,
    InputFormat,
    InputMeta,
    Limits,
    Measurement,
    Mode,
    OutcomeSource,
    Requests,
    ResourceSpec,
    RunRecord,
    RunStatus,
    ScalarValue,
    Trust,
    WireError,
    canonical_key,
    parse_workload_key,
    format_timestamp,
    new_ulid,
    parse_timestamp,
    parse_ulid,
    ulid_timestamp_ms,
)

T0 = dt.datetime(2026, 11, 18, 12, 0, 0, 123456, tzinfo=dt.timezone.utc)
GiB = 1 << 30


def full_record(**kw):
    base = dict(
        key="flyte://org/proj/dev/env.train",
        cluster="c1",
        invocation_id="inv-1",
        attempt=2,
        decision_id=new_ulid(),
        started_at=T0,
        inputs=(
            InputMeta(name="data", format=InputFormat.PARQUET, uri_hash="ab12", bytes=10 * GiB, rows=1000,
                      columns=ColumnsInfo(total=5, numeric=5), shape=(1000, 5), dtype="f32", params=None),
            InputMeta(name="lr", format="scalar", scalar=0.1),
            InputMeta(name="flag", format="scalar", scalar=True),
        ),
        code_digest="sha256:abc",
        requested=ResourceSpec(
            Requests(cpu=2000, memory=8 * GiB, gpu=GpuShape("A100-80G", 1), ephemeral_storage=GiB, shm=GiB // 2),
            Limits(cpu=4000, memory=8 * GiB, ephemeral_storage=2 * GiB),
        ),
        status=RunStatus.SUCCEEDED,
        status_confidence=Confidence.OBSERVED,
        memory_peak=Measurement(5.5 * GiB, Censor.EXACT, 1.0),
        cpu_mean=Measurement(1.25, Censor.EXACT),
        cpu_peak_window=Measurement(1.9, Censor.AT_LEAST, 1.0),
        cpu_throttled_frac=0.07,
        gpu_memory_peak=Measurement(40 * GiB, Censor.AT_MOST, 1.0),
        gpu_util_mean=0.8,
        t_peak_frac=0.6,
        duration_s=123.4,
        source=OutcomeSource.cgroup(),
        trust=Trust.AUTHENTICATED,
    )
    base.update(kw)
    return RunRecord(**base)


def test_runrecord_roundtrip_full():
    r = full_record()
    s = r.to_json()
    d = json.loads(s)
    assert d["v"] == 1
    assert d["status"] == "succeeded"
    assert d["status_confidence"] == "observed"
    assert d["source"] == {"kind": "cgroup"}
    assert d["trust"] == "authenticated"
    assert d["started_at"] == "2026-11-18T12:00:00.123456Z"
    assert d["requested"]["requests"]["gpu"] == {"accel": "A100-80G", "count": 1}
    assert d["memory_peak"] == {"value": 5.5 * GiB, "censor": "exact", "resolution_s": 1.0}
    assert d["cpu_peak_window"]["censor"] == "at_least"
    assert d["inputs"][0]["format"] == "parquet"
    assert d["inputs"][0]["columns"] == {"total": 5, "numeric": 5, "string": 0, "nested": 0, "other": 0}
    assert d["inputs"][1]["scalar"] == {"kind": "float", "value": 0.1}
    assert d["inputs"][2]["scalar"] == {"kind": "bool", "value": True}
    assert isinstance(d["requested"]["requests"]["memory"], int)
    r2 = RunRecord.from_json(s)
    assert r2 == r
    assert r2.to_json() == s


def test_runrecord_minimal_omits_none():
    r = RunRecord(key="ray://team-a//mod.fn", invocation_id="x", attempt=1, started_at=T0,
                  status="failed", source=OutcomeSource.self_report())
    d = r.to_dict()
    assert set(d) == {"v", "key", "invocation_id", "attempt", "started_at", "inputs", "status",
                      "status_confidence", "source", "trust"}
    assert d["trust"] == "unverified"
    assert d["source"] == {"kind": "self_report"}
    assert RunRecord.from_dict(d) == r


def test_null_reads_as_none_and_missing_inputs_ok():
    d = full_record().to_dict()
    d["cluster"] = None
    del d["inputs"]
    r = RunRecord.from_dict(d)
    assert r.cluster is None and r.inputs == ()


def test_all_enum_spellings():
    assert [s.value for s in RunStatus] == [
        "succeeded", "oom_killed", "gpu_oom", "evicted", "timeout", "preempted", "failed", "cached", "unknown"]
    assert [c.value for c in Censor] == ["exact", "at_least", "at_most"]
    assert [t.value for t in Trust] == ["verified", "authenticated", "unverified"]
    assert [c.value for c in Confidence] == ["observed", "inferred"]
    assert [m.value for m in Mode] == ["off", "observe", "recommend", "enforce"]
    assert [f.value for f in InputFormat] == [
        "parquet", "csv", "jsonl", "npy", "safetensors", "arrow_ipc", "dir", "blob", "scalar", "model_ref"]


def test_importer_source():
    s = OutcomeSource.importer("flyte")
    assert s.to_dict() == {"kind": "importer", "platform": "flyte"}
    assert OutcomeSource.from_dict(s.to_dict()) == s
    with pytest.raises(WireError):
        OutcomeSource.from_dict({"kind": "importer"})
    with pytest.raises(WireError):
        OutcomeSource.from_dict({"kind": "cgroup", "platform": "x"})
    with pytest.raises(WireError):
        OutcomeSource.from_dict("cgroup")


@pytest.mark.parametrize(
    "mutate",
    [
        lambda d: d.update(v=2),
        lambda d: d.update(v="1"),
        lambda d: d.update(origin_prior={"memory": "1Gi"}),
        lambda d: d["inputs"][1].update(scalar=0.1),
        lambda d: d["inputs"][1].update(scalar={"kind": "decimal", "value": 1}),
        lambda d: d["inputs"][1].update(scalar={"kind": "int", "value": 1.5}),
        lambda d: d["inputs"][0].update(columns={"numeric": 3}),
        lambda d: d.update(key="Flyte://o/p/d/t"),
        lambda d: d.update(key="k8s://a b/c/d"),
        lambda d: d.update(key="k8s://a/b/"),
        lambda d: d.update(key="slurm://c/acct//"),
        lambda d: d.update(extra=1),
        lambda d: d.update(status="OomKilled"),
        lambda d: d.update(status="oom"),
        lambda d: d.update(trust="trusted"),
        lambda d: d.update(attempt=-1),
        lambda d: d.update(attempt=1.0),
        lambda d: d.update(attempt=True),
        lambda d: d.update(attempt=2**32),
        lambda d: d.update(key="no-scheme"),
        lambda d: d.update(key="flyte://onlytenant"),
        lambda d: d.update(decision_id="not-a-ulid"),
        lambda d: d.update(started_at="2026-11-18 12:00:00"),
        lambda d: d.update(started_at="2026-11-18T12:00:00"),
        lambda d: d.update(t_peak_frac=1.5),
        lambda d: d.update(cpu_throttled_frac=-0.1),
        lambda d: d.update(duration_s=-1),
        lambda d: d["requested"]["requests"].update(memory=1.5),
        lambda d: d["requested"]["requests"].update(memory="8Gi"),
        lambda d: d["requested"]["requests"].update(cpu=2**32),
        lambda d: d["requested"]["requests"].update(gpu={"accel": "A100", "count": 1}),
        lambda d: d["requested"]["requests"].update(bogus=1),
        lambda d: d["requested"]["limits"].update(gpu={"accel": "A100-80G", "count": 1}),
        lambda d: d["memory_peak"].update(censor="AtLeast"),
        lambda d: d["memory_peak"].update(value=float("nan")) if False else d["memory_peak"].update(value=-1),
        lambda d: d["memory_peak"].pop("value"),
        lambda d: d["inputs"][0].update(format="parquet2"),
        lambda d: d["inputs"][0].update(shape=[1, -2]),
        lambda d: d.update(source={"kind": "carrier_pigeon"}),
        lambda d: d.update(inputs={}),
    ],
)
def test_strict_validation(mutate):
    d = json.loads(full_record().to_json())
    mutate(d)
    with pytest.raises(WireError):
        RunRecord.from_dict(d)


def test_unknown_fields_allowed_when_asked():
    d = full_record().to_dict()
    d["future_field"] = 1
    d["memory_peak"]["future"] = True
    r = RunRecord.from_dict(d, allow_unknown=True)
    assert r.memory_peak.value == 5.5 * GiB


def test_invalid_json():
    with pytest.raises(WireError):
        RunRecord.from_json("{not json")
    with pytest.raises(WireError):
        RunRecord.from_json("[]")


def test_nan_rejected_on_construct():
    with pytest.raises(WireError):
        Measurement(float("nan"))
    with pytest.raises(WireError):
        Measurement(float("inf"))


def test_timestamps():
    assert parse_timestamp("2026-11-18T12:00:00Z") == dt.datetime(2026, 11, 18, 12, tzinfo=dt.timezone.utc)
    assert parse_timestamp("2026-11-18T14:00:00.5+02:00") == dt.datetime(2026, 11, 18, 12, 0, 0, 500000, tzinfo=dt.timezone.utc)
    assert parse_timestamp("2026-11-18T12:00:00.123456789Z").microsecond == 123456
    with pytest.raises(WireError):
        parse_timestamp("2026-13-18T12:00:00Z")
    with pytest.raises(WireError):
        format_timestamp(dt.datetime(2026, 1, 1))
    assert format_timestamp(dt.datetime(2026, 1, 1, 1, tzinfo=dt.timezone(dt.timedelta(hours=1)))) == "2026-01-01T00:00:00.000000Z"


def test_attempt_context():
    a = AttemptContext("dag/run/task/0", 2, 4)
    assert json.loads(a.to_json()) == {"v": 1, "invocation_id": "dag/run/task/0", "attempt": 2, "max_attempts": 4}
    assert AttemptContext.from_json(a.to_json()) == a
    b = AttemptContext("x")
    assert "max_attempts" not in b.to_dict()
    for bad in ('{"v":1,"invocation_id":"x","attempt":0}', '{"v":1,"invocation_id":"","attempt":1}',
                '{"invocation_id":"x","attempt":1}', '{"v":1,"invocation_id":"x","attempt":1,"max_attempts":0}'):
        with pytest.raises(WireError):
            AttemptContext.from_json(bad)


def test_resource_spec_empty_roundtrip():
    s = ResourceSpec()
    assert s.to_dict() == {"requests": {}, "limits": {}}
    assert ResourceSpec.from_dict({}) == s


# ---- ULID


def test_ulid_format_and_parse():
    u = new_ulid()
    assert len(u) == 26 and parse_ulid(u.lower()) == u
    with pytest.raises(WireError):
        parse_ulid("8" + u[1:])  # overflow first char
    with pytest.raises(WireError):
        parse_ulid(u[:-1] + "U")  # U not in Crockford


def test_ulid_timestamp_roundtrip():
    u = new_ulid(now_ms=1_700_000_000_123)
    assert ulid_timestamp_ms(u) >= 1_700_000_000_123


def test_ulid_monotonic_within_ms():
    us = [new_ulid(now_ms=1_800_000_000_000) for _ in range(1000)]
    assert us == sorted(us) and len(set(us)) == 1000


def test_ulid_monotonic_clock_backwards():
    a = new_ulid(now_ms=1_900_000_000_000)
    b = new_ulid(now_ms=1_899_999_999_000)
    assert b > a


def test_ulid_ordering_across_ms_and_threads():
    out = []
    lock = threading.Lock()

    def gen():
        local = [new_ulid() for _ in range(500)]
        with lock:
            out.extend(local)
        assert local == sorted(local)

    ts = [threading.Thread(target=gen) for _ in range(4)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert len(set(out)) == 2000


def test_mode_ordering_and_str_eq():
    assert Mode.OFF < Mode.OBSERVE < Mode.RECOMMEND < Mode.ENFORCE
    assert min(Mode.ENFORCE, Mode.OBSERVE) is Mode.OBSERVE
    assert Mode("enforce") == "enforce"


def test_rust_serde_defaults_accepted():
    # Rust: v, status_confidence, censor and inputs have serde defaults
    d = full_record().to_dict()
    for k in ("v", "status_confidence", "inputs"):
        d.pop(k)
    d["memory_peak"].pop("censor")
    r = RunRecord.from_dict(d)
    assert r.status_confidence is Confidence.OBSERVED and r.memory_peak.censor is Censor.EXACT and r.inputs == ()


def test_origin_prior_roundtrip():
    r = full_record(origin_prior=Requests(memory=16 * GiB, cpu=4000))
    d = json.loads(r.to_json())
    assert d["origin_prior"] == {"cpu": 4000, "memory": 16 * GiB}
    assert RunRecord.from_json(r.to_json()) == r


def test_workload_keys_mirror_rust():
    k = parse_workload_key("k8s://prod/ml/pod/trainer/main")
    assert (k.platform, k.tenant, k.suffix) == ("k8s", "prod/ml", "pod/trainer/main")
    k = parse_workload_key("flyte://acme/ml/development/env.train")
    assert (k.tenant, k.suffix) == ("acme/ml/development", "env.train")
    assert canonical_key("nomad://dc1/ns//job/web") == "nomad://dc1/ns//job/web"
    assert canonical_key("ray://team-a//f") == "ray://team-a//f"
    assert canonical_key("flyte://a/b/c//x") == "flyte://a/b/c/x"
    for bad in ["k8s:/a/b/c", "K8S://a/b/c", "k8s://a/b", "k8s://a/b/", "k8s://a b/c/d", "k8s://a/b/c d",
                "k8s:///b/c", "slurm://c/acct//"]:
        with pytest.raises(WireError):
            parse_workload_key(bad)


def test_scalar_value_forms():
    assert ScalarValue.of(3).to_dict() == {"kind": "int", "value": 3}
    assert ScalarValue.of("x").to_dict() == {"kind": "str", "value": "x"}
    assert ScalarValue.of(True).kind == "bool"
    with pytest.raises(WireError):
        ScalarValue("int", 2**63)
