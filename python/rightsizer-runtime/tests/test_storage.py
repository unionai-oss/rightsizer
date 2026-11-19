"""Store options are passed explicitly, never through ``os.environ`` (SPEC §10.3)."""

import datetime as dt
import os

import pytest

from rightsizer_runtime.record import RunRecordWriter
from rightsizer_runtime.storage import flyte_storage_options, fsspec_options, object_store_options
from rightsizer_runtime.wire import OutcomeSource, RunRecord, RunStatus

T0 = dt.datetime(2026, 11, 18, 12, tzinfo=dt.timezone.utc)


def test_object_store_options_normalize_fsspec_spellings():
    o = object_store_options("s3://b/p", {"key": "AK", "secret": "SK", "token": "ST", "client_kwargs": {"endpoint_url": "http://minio:9000", "region_name": "eu-west-1"},
                                          "use_ssl": False, "anon": False, "config_kwargs": {"s3": {"addressing_style": "path"}}})
    assert o == {"access_key_id": "AK", "secret_access_key": "SK", "session_token": "ST", "endpoint": "http://minio:9000",
                 "region": "eu-west-1", "skip_signature": "false", "virtual_hosted_style_request": "false", "allow_http": "true"}
    assert object_store_options("gs://b", {"token": "/sa.json"}) == {"service_account": "/sa.json"}
    assert object_store_options("az://c", {"account_name": "a", "account_key": "k"}) == {"account_name": "a", "account_key": "k"}
    assert object_store_options("s3://b", None) == {}


def test_fsspec_options_accept_canonical_keys():
    o = fsspec_options("s3://b/p", {"access_key_id": "AK", "secret_access_key": "SK", "endpoint": "http://minio:9000", "region": "us-east-1",
                                    "allow_http": "true", "virtual_hosted_style_request": "false", "use_ssl": False})
    assert o == {"key": "AK", "secret": "SK", "use_ssl": False, "client_kwargs": {"endpoint_url": "http://minio:9000", "region_name": "us-east-1"},
                 "config_kwargs": {"s3": {"addressing_style": "path"}}}
    # fsspec spellings pass through unchanged.
    assert fsspec_options("s3://b", {"key": "AK", "client_kwargs": {"endpoint_url": "x"}}) == {"key": "AK", "client_kwargs": {"endpoint_url": "x"}}
    assert fsspec_options("gs://b", {"service_account": "/sa.json"}) == {"token": "/sa.json"}
    assert fsspec_options("memory://x", {"a": 1}) == {"a": 1}


def test_flyte_storage_options_read_but_never_write_the_environment():
    env = {"FLYTE_AWS_ENDPOINT": "http://rustfs:9000", "FLYTE_AWS_ACCESS_KEY_ID": "rustfs", "FLYTE_AWS_SECRET_ACCESS_KEY": "pw"}
    before = dict(env)
    o = flyte_storage_options(env)
    assert o == {"endpoint": "http://rustfs:9000", "access_key_id": "rustfs", "secret_access_key": "pw", "allow_http": "true", "region": "us-east-1"}
    assert env == before
    # Ambient AWS_* settings take precedence (the clients read them themselves).
    o = flyte_storage_options({**env, "AWS_ACCESS_KEY_ID": "mine", "AWS_REGION": "eu-west-1"})
    assert "access_key_id" not in o and "secret_access_key" not in o and "region" not in o
    assert flyte_storage_options({}) == {}


def test_record_writer_takes_the_same_options_without_env_changes(monkeypatch):
    fsspec = pytest.importorskip("fsspec")
    seen = {}

    def url_to_fs(uri, **kw):
        seen.update(kw)
        return fsspec.filesystem("memory"), "rs-opts/hist"

    monkeypatch.setattr(fsspec.core, "url_to_fs", url_to_fs)
    before = dict(os.environ)
    w = RunRecordWriter("s3://bucket/hist", storage_options={"access_key_id": "AK", "secret_access_key": "SK", "endpoint": "http://minio:9000"})
    r = RunRecord(key="flyte://acme/proj/dev/env.t", invocation_id="i", attempt=1, started_at=T0, status=RunStatus.SUCCEEDED,
                  source=OutcomeSource.self_report())
    assert w.write(r) is True
    assert seen == {"key": "AK", "secret": "SK", "client_kwargs": {"endpoint_url": "http://minio:9000"}}
    assert dict(os.environ) == before
