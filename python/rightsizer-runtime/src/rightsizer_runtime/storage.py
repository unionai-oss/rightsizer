"""Object-store options shared by Python writes and Rust reads (SPEC §10.3).

Integrations resolve storage options for their own writes (Flyte's
object-store settings, fsspec ``storage_options``). The same options are
passed **explicitly** to every rightsizer store client, never through the
process environment (exporting ``AWS_*`` would leak rightsizer's store
identity into the user's own boto3/s3fs clients):

- :func:`object_store_options`: any spelling → the ``object_store`` keys the
  Rust engine takes (``rightsizer._core.Engine(storage_options=...)``,
  ``[history.storage_options]``);
- :func:`fsspec_options`: the same options → fsspec/s3fs/gcsfs/adlfs
  keyword arguments (:class:`~rightsizer_runtime.record.RunRecordWriter`);
- :func:`flyte_storage_options`: Flyte's own settings in a task pod
  (``FLYTE_AWS_*``), read from the environment, never written back.

Standard library only.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Mapping, Optional

# Every accepted spelling → the canonical (object_store) key, per scheme family.
_S3 = {
    "key": "access_key_id", "access_key_id": "access_key_id", "aws_access_key_id": "access_key_id",
    "secret": "secret_access_key", "secret_access_key": "secret_access_key", "aws_secret_access_key": "secret_access_key",
    "token": "session_token", "session_token": "session_token", "aws_session_token": "session_token",
    "endpoint_url": "endpoint", "endpoint": "endpoint", "aws_endpoint": "endpoint", "aws_endpoint_url": "endpoint",
    "region": "region", "region_name": "region", "aws_region": "region",
    "allow_http": "allow_http", "aws_allow_http": "allow_http",
    "virtual_hosted_style_request": "virtual_hosted_style_request",
    "anon": "skip_signature", "skip_signature": "skip_signature",
}
_GCS = {
    "token": "service_account", "service_account": "service_account", "google_service_account": "service_account",
    "service_account_key": "service_account_key", "google_service_account_key": "service_account_key",
    "anon": "skip_signature", "skip_signature": "skip_signature",
}
_AZURE = {
    "account_name": "account_name", "azure_storage_account_name": "account_name",
    "account_key": "account_key", "azure_storage_account_key": "account_key", "access_key": "account_key",
    "sas_token": "sas_token", "azure_storage_sas_token": "sas_token",
    "client_id": "client_id", "client_secret": "client_secret", "tenant_id": "tenant_id",
    "anon": "skip_signature", "skip_signature": "skip_signature",
}


def _family(uri: Optional[str]) -> str:
    scheme = uri.split("://", 1)[0].lower() if uri and "://" in uri else ""
    if scheme in ("s3", "s3a"):
        return "s3"
    if scheme in ("gs", "gcs"):
        return "gcs"
    if scheme in ("az", "azure", "abfs", "abfss", "adl"):
        return "azure"
    return ""


def _flatten(options: Mapping[str, Any]) -> Dict[str, Any]:
    flat: Dict[str, Any] = {}
    for k, v in options.items():
        if k in ("client_kwargs", "config_kwargs") and isinstance(v, Mapping):
            for k2, v2 in v.items():
                if k2 == "s3" and isinstance(v2, Mapping) and "addressing_style" in v2:
                    flat["virtual_hosted_style_request"] = v2["addressing_style"] == "virtual"
                elif not isinstance(v2, Mapping):
                    flat[k2] = v2
        else:
            flat[str(k).lower()] = v
    return flat


def _text(v: Any) -> str:
    return ("true" if v else "false") if isinstance(v, bool) else str(v)


def object_store_options(uri: Optional[str], options: Optional[Mapping[str, Any]]) -> Dict[str, str]:
    """fsspec / object_store spellings → the canonical keys for ``uri``'s scheme.

    Unknown keys of a known scheme are dropped (fsspec-only settings such as
    ``use_ssl``); for an unknown scheme (or no URI) S3 spellings are assumed.
    ``None`` values and nested objects are skipped.
    """
    if not options:
        return {}
    fam = _family(uri) or "s3"
    table = {"s3": _S3, "gcs": _GCS, "azure": _AZURE}[fam]
    out: Dict[str, str] = {}
    for k, v in _flatten(options).items():
        name = table.get(k)
        if name is None or v is None or isinstance(v, (dict, list, tuple)):
            continue
        out[name] = _text(v)
    if fam == "s3" and out.get("endpoint", "").startswith("http://"):
        out.setdefault("allow_http", "true")
    return out


def fsspec_options(uri: Optional[str], options: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """The same options as fsspec keyword arguments for ``uri``'s filesystem.

    Accepts canonical (object_store) keys and fsspec keys alike; keys that are
    neither (fsspec-only settings) pass through unchanged.
    """
    if not options:
        return {}
    fam = _family(uri)
    if not fam:
        return dict(options)
    table = {"s3": _S3, "gcs": _GCS, "azure": _AZURE}[fam]
    out: Dict[str, Any] = {}
    client: Dict[str, Any] = dict(options.get("client_kwargs") or {}) if isinstance(options.get("client_kwargs"), Mapping) else {}
    config: Dict[str, Any] = dict(options.get("config_kwargs") or {}) if isinstance(options.get("config_kwargs"), Mapping) else {}
    for k, v in options.items():
        if k in ("client_kwargs", "config_kwargs") or v is None:
            continue
        name = table.get(str(k).lower())
        if name is None:
            out[k] = v
            continue
        if fam == "s3":
            if name == "access_key_id":
                out["key"] = v
            elif name == "secret_access_key":
                out["secret"] = v
            elif name == "session_token":
                out["token"] = v
            elif name == "endpoint":
                client["endpoint_url"] = v
            elif name == "region":
                client["region_name"] = v
            elif name == "skip_signature":
                out["anon"] = _truthy(v)
            elif name == "virtual_hosted_style_request":
                config.setdefault("s3", {})["addressing_style"] = "virtual" if _truthy(v) else "path"
            # allow_http: s3fs follows the endpoint's scheme.
        elif fam == "gcs":
            if name in ("service_account", "service_account_key"):
                out["token"] = v
            elif name == "skip_signature" and _truthy(v):
                out["token"] = "anon"
        else:
            if name == "skip_signature":
                out["anon"] = _truthy(v)
            else:
                out[name] = v
    if client:
        out["client_kwargs"] = client
    if config:
        out["config_kwargs"] = config
    return out


def _truthy(v: Any) -> bool:
    return v if isinstance(v, bool) else str(v).strip().lower() in ("1", "true", "yes", "on")


def flyte_storage_options(environ: Optional[Mapping[str, str]] = None) -> Dict[str, str]:
    """Flyte's object-store settings in a task pod (``FLYTE_AWS_ENDPOINT``,
    ``FLYTE_AWS_ACCESS_KEY_ID``, ``FLYTE_AWS_SECRET_ACCESS_KEY``) as canonical
    options. Reads ``environ`` (default ``os.environ``); writes nothing.
    ``AWS_ENDPOINT``/``AWS_ENDPOINT_URL`` and ``AWS_ACCESS_KEY_ID``, when set,
    take precedence (the clients read those themselves).

    A custom endpoint gets ``allow_http`` (``http://``) and a default region
    (``us-east-1``, which S3-compatible stores ignore) unless ``AWS_REGION`` /
    ``AWS_DEFAULT_REGION`` is set.
    """
    env = os.environ if environ is None else environ
    out: Dict[str, str] = {}
    # Explicit AWS_* settings in the environment win (the store clients read them themselves).
    if env.get("FLYTE_AWS_ENDPOINT") and not (env.get("AWS_ENDPOINT") or env.get("AWS_ENDPOINT_URL")):
        out["endpoint"] = env["FLYTE_AWS_ENDPOINT"]
    if env.get("FLYTE_AWS_ACCESS_KEY_ID") and not env.get("AWS_ACCESS_KEY_ID"):
        out["access_key_id"] = env["FLYTE_AWS_ACCESS_KEY_ID"]
        if env.get("FLYTE_AWS_SECRET_ACCESS_KEY"):
            out["secret_access_key"] = env["FLYTE_AWS_SECRET_ACCESS_KEY"]
    ep = out.get("endpoint", "")
    if ep:
        if ep.startswith("http://"):
            out["allow_http"] = "true"
        if not env.get("AWS_REGION") and not env.get("AWS_DEFAULT_REGION"):
            out["region"] = "us-east-1"
    return out


__all__ = ["flyte_storage_options", "fsspec_options", "object_store_options"]
