import datetime as dt
import hashlib
import json
import os
import threading

import pytest

from rightsizer_runtime.record import (
    LocalBackend,
    RunRecordWriter,
    key_hash,
    shard_path,
    split_key,
    tenant_path,
)
from rightsizer_runtime.wire import OutcomeSource, RunRecord, RunStatus, WireError, new_ulid, parse_ulid

T0 = dt.datetime(2026, 11, 18, 12, tzinfo=dt.timezone.utc)


def rec(key="flyte://acme/proj/dev/env.train", **kw):
    return RunRecord(key=key, invocation_id=kw.pop("invocation_id", "inv"), attempt=1, started_at=T0,
                     status=RunStatus.SUCCEEDED, source=OutcomeSource.self_report(), **kw)


def test_key_hash_contract():
    k = "flyte://acme/proj/dev/env.train"
    assert key_hash(k) == hashlib.sha256(k.encode()).hexdigest()[:32]
    assert len(key_hash(k)) == 32 and key_hash(k) == key_hash(k).lower()
    # pinned value: rightsizer_core::WorkloadKey::hash must produce the same string
    assert key_hash("flyte://acme/proj/dev/env.train") == "eef92d58436c80afb2670dc464a4b4f2"
    # hashed over the canonical form
    assert key_hash("flyte://acme/proj/dev//env.train") == key_hash(k)


def test_split_key_and_tenant_path():
    assert split_key("flyte://acme/proj/dev/env.train") == ("flyte", "acme/proj/dev", "env.train")
    assert split_key("k8s://c1/ns/job.trainer") == ("k8s", "c1/ns", "job.trainer")
    assert split_key("slurm://c1/acct/train.sh") == ("slurm", "c1/acct", "train.sh")
    assert split_key("ray://team-a/cluster-1/mod.fn") == ("ray", "team-a/cluster-1", "mod.fn")
    assert split_key("ray://team-a//mod.fn") == ("ray", "team-a", "mod.fn")
    assert split_key("ray://team-a//mod.fn", tenant="team-a") == ("ray", "team-a", "mod.fn")
    with pytest.raises(WireError):
        split_key("ray://team-a/mod.fn")  # arity 2: too few segments
    with pytest.raises(WireError):
        split_key("flyte://acme/proj/env.train")  # too few segments
    with pytest.raises(WireError):
        split_key("ray://team-a//mod.fn", tenant="team-b")
    assert tenant_path("flyte", "acme/proj/dev") == "flyte/acme/proj/dev"
    assert tenant_path("k8s", "c1/n@s") == "k8s/c1/n@s"
    for bad in ("c1/../x", "c 1/x", "c1//x", "c1/%2e"):
        with pytest.raises(WireError):
            tenant_path("k8s", bad)


def test_shard_layout():
    r = rec()
    u = new_ulid()
    assert shard_path(r, u) == f"v1/runs/flyte/acme/proj/dev/{key_hash(r.key)}/{u}.json"


def test_local_write_and_layout(tmp_path):
    w = RunRecordWriter(str(tmp_path))
    r = rec()
    assert w.write(r) is True
    files = list(tmp_path.rglob("*.json"))
    assert len(files) == 1
    rel = files[0].relative_to(tmp_path).as_posix()
    parts = rel.split("/")
    assert parts[:6] == ["v1", "runs", "flyte", "acme", "proj", "dev"]
    assert parts[6] == key_hash(r.key)
    parse_ulid(parts[7][:-5])
    assert RunRecord.from_json(files[0].read_text()) == r
    assert w.stats.written == 1


def test_file_uri(tmp_path):
    w = RunRecordWriter(tmp_path.as_uri())
    assert w.write(rec())
    assert len(list(tmp_path.rglob("*.json"))) == 1


def test_atomic_write_leaves_no_partial(tmp_path, monkeypatch):
    real_replace = os.replace

    def boom(a, b):
        raise OSError("disk full at rename")

    monkeypatch.setattr(os, "replace", boom)
    w = RunRecordWriter(str(tmp_path))
    assert w.write(rec()) is False
    assert list(tmp_path.rglob("*.json")) == []
    assert [p for p in tmp_path.rglob("*") if p.is_file()] == []  # temp file cleaned up
    assert w.pending == 1
    monkeypatch.setattr(os, "replace", real_replace)
    assert w.flush() == 1 and w.pending == 0
    assert len(list(tmp_path.rglob("*.json"))) == 1


def test_temp_name_not_json(tmp_path):
    # readers LIST *.json; a temp file must never match
    seen = []
    real = os.replace

    def spy(a, b):
        seen.append(os.path.basename(a))
        return real(a, b)

    import rightsizer_runtime.record as rmod

    orig = rmod.os.replace
    rmod.os.replace = spy
    try:
        RunRecordWriter(str(tmp_path)).write(rec())
    finally:
        rmod.os.replace = orig
    assert seen and all(not s.endswith(".json") and s.startswith(".") for s in seen)


class FlakyBackend:
    def __init__(self):
        self.fail = True
        self.objects = {}

    def put(self, rel, data):
        if self.fail:
            raise ConnectionError("store down")
        self.objects[rel] = data


def test_buffer_retry_and_overflow_counting(tmp_path):
    w = RunRecordWriter(str(tmp_path), buffer_size=3)
    fb = FlakyBackend()
    w._backend = fb
    for i in range(5):
        assert w.write(rec(invocation_id=f"i{i}")) is False
    assert w.pending == 3 and w.stats.dropped == 2 and w.stats.errors >= 5
    fb.fail = False
    assert w.write(rec(invocation_id="i5")) is True  # flushes buffer first
    assert w.pending == 0 and len(fb.objects) == 4 and w.stats.retried == 3
    ids = sorted(json.loads(v)["invocation_id"] for v in fb.objects.values())
    assert ids == ["i0", "i1", "i2", "i5"]


def test_retry_keeps_same_shard_name(tmp_path):
    w = RunRecordWriter(str(tmp_path))
    fb = FlakyBackend()
    w._backend = fb
    w.write(rec())
    rel = w._buffer[0][0]
    fb.fail = False
    w.flush()
    assert list(fb.objects) == [rel]


def test_default_buffer_is_10k():
    assert RunRecordWriter("/tmp/x").buffer_size == 10_000


def test_tenant_mismatch_dropped_not_raised(tmp_path):
    w = RunRecordWriter(str(tmp_path), tenant="team-b")
    assert w.write(rec(key="ray://team-a//mod.fn")) is False
    assert w.stats.dropped == 1
    w2 = RunRecordWriter(str(tmp_path), tenant="team-a")
    assert w2.write(rec(key="ray://team-a//mod.fn")) is True
    assert list((tmp_path / "v1" / "runs" / "ray" / "team-a").iterdir())


def test_concurrent_writes(tmp_path):
    w = RunRecordWriter(str(tmp_path))
    ts = [threading.Thread(target=lambda i=i: [w.write(rec(invocation_id=f"{i}-{j}")) for j in range(25)]) for i in range(4)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert len(list(tmp_path.rglob("*.json"))) == 100 and w.stats.written == 100


def test_fsspec_memory_backend():
    fsspec = pytest.importorskip("fsspec")
    fs = fsspec.filesystem("memory")
    prefix = f"memory://rs-test-{new_ulid()}/hist"
    w = RunRecordWriter(prefix, storage_options={})
    r = rec()
    assert w.write(r) is True
    root = prefix.split("://", 1)[1]
    found = fs.find("/" + root) or fs.find(root)
    assert len(found) == 1
    path = found[0]
    assert f"/v1/runs/flyte/acme/proj/dev/{key_hash(r.key)}/" in path and path.endswith(".json")
    assert RunRecord.from_json(fs.cat_file(path)) == r


def test_fsspec_missing_buffers(monkeypatch, tmp_path):
    import sys

    monkeypatch.setitem(sys.modules, "fsspec", None)
    w = RunRecordWriter("s3://bucket/prefix")
    assert w.write(rec()) is False
    assert w.pending == 1 and w.stats.errors == 1
