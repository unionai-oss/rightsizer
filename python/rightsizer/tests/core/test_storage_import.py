"""Store options without environment pollution (SPEC §10.3) and the bulk
importer (``Engine.import_runs``: dedup against the key's whole history)."""

from __future__ import annotations

import datetime as dt
import json
import os

import pytest

import rightsizer as rs
from rightsizer_runtime.wire import OutcomeSource, RunRecord, RunStatus, Trust

T0 = dt.datetime(2026, 11, 1, 12, tzinfo=dt.timezone.utc)
SECRETS = {"access_key_id": "AKIAEXAMPLE", "secret_access_key": "s3cr3t-value", "endpoint": "http://minio:9000", "region": "eu-west-1"}


def _rec(i: int, attempt: int = 1, status: RunStatus = RunStatus.SUCCEEDED) -> RunRecord:
    return RunRecord(key="flyte://acme/proj/dev/env.train", invocation_id=f"run{i}/a0", attempt=attempt,
                     started_at=T0 + dt.timedelta(minutes=i), status=status, duration_s=60.0,
                     source=OutcomeSource(kind="flyte_remote"), trust=Trust.VERIFIED)


def test_engine_storage_options_leave_os_environ_unchanged(core, tmp_path):
    before = dict(os.environ)
    store = f"file://{tmp_path / 'store'}"
    e = rs.Engine({"history": {"uri": store}}, env_overrides=False, storage_options={"key": "AKIAEXAMPLE", "secret": "s3cr3t-value",
                                                                                    "client_kwargs": {"endpoint_url": "http://minio:9000"}})
    assert dict(os.environ) == before, "storage options must never be exported to the environment"
    assert e.config_error is None
    # The (local) store still reads and writes.
    assert e.outcome(_rec(0)).written
    assert e.import_runs([_rec(0)])["duplicates"] == 1
    # The options reach the config, redacted.
    shown = json.dumps(e.config)
    assert "s3cr3t-value" not in shown and "AKIAEXAMPLE" not in shown
    assert e.config["history"]["storage_options"]["endpoint"] == "http://minio:9000"
    assert e.config["history"]["storage_options"]["secret_access_key"] == "***"


def test_core_engine_takes_storage_options(core, tmp_path):
    before = dict(os.environ)
    e = core.Engine(json.dumps({"history": {"uri": str(tmp_path / "s")}}), env_overrides=False, storage_options=SECRETS)
    assert dict(os.environ) == before
    assert "s3cr3t" not in e.config_json() and "s3cr3t" not in repr(e)


def test_flyte_engine_does_not_export_flyte_credentials(core, tmp_path, monkeypatch):
    pytest.importorskip("flyte")
    from rightsizer.flyte import _engine as fe

    monkeypatch.setenv("FLYTE_AWS_ENDPOINT", "http://rustfs:9000")
    monkeypatch.setenv("FLYTE_AWS_ACCESS_KEY_ID", "rustfs")
    monkeypatch.setenv("FLYTE_AWS_SECRET_ACCESS_KEY", "rustfsstorage")
    for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_ENDPOINT", "AWS_ENDPOINT_URL", "AWS_REGION", "AWS_ALLOW_HTTP", "AWS_DEFAULT_REGION"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("RIGHTSIZER__HISTORY__URI", f"file://{tmp_path / 'fs'}")
    before = dict(os.environ)
    fe.reset_for_tests()
    try:
        eng = fe.get_engine()
        assert eng.config_error is None
        assert dict(os.environ) == before, "FLYTE_AWS_* must not be copied to AWS_*"
        opts = eng.config["history"]["storage_options"]
        assert opts["endpoint"] == "http://rustfs:9000" and opts["access_key_id"] == "***"
    finally:
        fe.reset_for_tests()


def test_import_runs_dedupes_against_the_whole_history(engine):
    first = [_rec(i) for i in range(5)] + [_rec(1, attempt=2, status=RunStatus.OOM_KILLED)]
    rep = engine.import_runs(first)
    assert (rep["read"], rep["inserted"], rep["duplicates"]) == (6, 6, 0)
    # Re-import of an overlapping window: nothing new, no attribution re-run.
    again = engine.import_runs(first + [_rec(5)])
    assert (again["read"], again["inserted"], again["duplicates"]) == (7, 1, 6)
    assert again["attributions"] == {} and again["tripped"] == []
    # Idempotent: a third pass inserts nothing.
    third = engine.import_runs(first + [_rec(5)])
    assert (third["inserted"], third["duplicates"]) == (0, 7)
    # Duplicates inside one batch count once.
    assert engine.import_runs([_rec(9), _rec(9)])["inserted"] == 1


def test_flyte_backfill_reimport_is_idempotent(engine, monkeypatch):
    pytest.importorskip("flyte")
    from rightsizer.flyte import _backfill as bf

    recs = [_rec(i) for i in range(4)]
    monkeypatch.setattr(bf, "iter_action_details", lambda *a, **k: iter(range(len(recs))))
    monkeypatch.setattr(bf, "records_for", lambda i: [recs[i]])
    out = bf.backfill("proj", "dev", "30d", engine=engine)
    assert (out["actions"], out["read"], out["inserted"], out["duplicates"]) == (4, 4, 4, 0)
    out = bf.backfill("proj", "dev", "30d", engine=engine)
    assert (out["inserted"], out["duplicates"], out["written"]) == (0, 4, 0)
    dry = bf.backfill("proj", "dev", "30d", dry_run=True)
    assert dry["inserted"] == 0 and dry["read"] == 4
