"""Share object-store credentials between Python writes and Rust reads
(SPEC §10.3).

Rust's object-store backends read credentials from the environment
(``AmazonS3Builder::from_env`` and friends). Integrations resolve storage
options for their own writes (Flyte's ``flyte.storage``, fsspec
``storage_options``); :func:`apply_storage_options` exports the same
options into the environment variables Rust reads, so both sides use the
same identity. Call it before the engine is built (``rs.configure`` does).
"""

from __future__ import annotations

import os
from typing import Any, Dict, Mapping, MutableMapping, Optional

# fsspec / s3fs / gcsfs / adlfs option → environment variable read by object_store.
_S3 = {
    "key": "AWS_ACCESS_KEY_ID",
    "access_key_id": "AWS_ACCESS_KEY_ID",
    "aws_access_key_id": "AWS_ACCESS_KEY_ID",
    "secret": "AWS_SECRET_ACCESS_KEY",
    "secret_access_key": "AWS_SECRET_ACCESS_KEY",
    "aws_secret_access_key": "AWS_SECRET_ACCESS_KEY",
    "token": "AWS_SESSION_TOKEN",
    "session_token": "AWS_SESSION_TOKEN",
    "aws_session_token": "AWS_SESSION_TOKEN",
    "endpoint_url": "AWS_ENDPOINT",
    "endpoint": "AWS_ENDPOINT",
    "region": "AWS_REGION",
    "region_name": "AWS_REGION",
    "profile": "AWS_PROFILE",
}
_GCS = {"token": "GOOGLE_SERVICE_ACCOUNT", "service_account": "GOOGLE_SERVICE_ACCOUNT", "project": "GOOGLE_CLOUD_PROJECT"}
_AZURE = {
    "account_name": "AZURE_STORAGE_ACCOUNT_NAME",
    "account_key": "AZURE_STORAGE_ACCOUNT_KEY",
    "sas_token": "AZURE_STORAGE_SAS_KEY",
    "client_id": "AZURE_CLIENT_ID",
    "client_secret": "AZURE_CLIENT_SECRET",
    "tenant_id": "AZURE_TENANT_ID",
}


def env_for(uri: Optional[str], options: Mapping[str, Any]) -> Dict[str, str]:
    """Environment variables equivalent to fsspec ``storage_options`` for ``uri``'s scheme."""
    scheme = (uri or "").split("://", 1)[0].lower() if uri and "://" in uri else "s3"
    flat: Dict[str, Any] = {}
    for k, v in options.items():
        if k in ("client_kwargs", "config_kwargs") and isinstance(v, Mapping):
            flat.update(v)
        else:
            flat[k] = v
    out: Dict[str, str] = {}
    table = _S3 if scheme in ("s3", "s3a") else _GCS if scheme in ("gs", "gcs") else _AZURE if scheme in ("az", "azure", "abfs", "abfss") else {}
    for k, v in flat.items():
        name = table.get(k)
        if name and v is not None and not isinstance(v, (dict, list)):
            out[name] = str(v)
    if scheme in ("s3", "s3a"):
        if flat.get("anon"):
            out["AWS_SKIP_SIGNATURE"] = "true"
        ep = out.get("AWS_ENDPOINT", "")
        if ep.startswith("http://"):
            out["AWS_ALLOW_HTTP"] = "true"
    return out


def apply_storage_options(uri: Optional[str], options: Optional[Mapping[str, Any]], environ: Optional[MutableMapping[str, str]] = None) -> Dict[str, str]:
    """Export ``options`` into the environment (``os.environ`` by default);
    returns what was set. Explicit options win over existing variables."""
    if not options:
        return {}
    env = os.environ if environ is None else environ
    vals = env_for(uri, options)
    for k, v in vals.items():
        env[k] = v
    return vals


__all__ = ["apply_storage_options", "env_for"]
