"""Direct reads (and leases) on the history store's object layout, for the parts
of the Flyte integration that run outside the engine: deploy-time sizing
(recent input sizes), ``reconcile`` (its singleton lease and deploy
records) and test tooling.

Uses ``obstore`` (a Flyte dependency) with the same credentials Rust's object
store reads (``AWS_*``; ``FLYTE_AWS_*`` in task pods via
:func:`._engine.export_flyte_storage_env`). Local paths and ``file://`` use
the filesystem. Layout: SPEC §8.1 (``v1/runs/{platform}/{tenant}/{key-hash}/{ulid}.json``).
"""

from __future__ import annotations

import json
import os
import pathlib
import urllib.parse
from typing import Any, Dict, Iterator, List, Optional, Tuple

from rightsizer_runtime.record import key_hash, split_key, tenant_path
from rightsizer_runtime.wire import RunRecord

from ._engine import export_flyte_storage_env


class Store:
    """A store prefix (``s3://bucket/prefix``, ``file:///dir`` or a path)."""

    def __init__(self, uri: str):
        self.uri = uri.rstrip("/")
        p = urllib.parse.urlparse(self.uri)
        self.local: Optional[pathlib.Path] = None
        if p.scheme in ("", "file"):
            self.local = pathlib.Path(urllib.parse.unquote(p.path) if p.scheme else self.uri)
            self.prefix = ""
            self._store = None
            return
        export_flyte_storage_env()
        from obstore.store import from_url

        self.prefix = p.path.strip("/")
        base = f"{p.scheme}://{p.netloc}"
        cfg: Dict[str, Any] = {}
        client: Dict[str, Any] = {}
        if p.scheme in ("s3", "s3a"):
            env = os.environ
            for k, e in (("endpoint", "AWS_ENDPOINT"), ("access_key_id", "AWS_ACCESS_KEY_ID"),
                         ("secret_access_key", "AWS_SECRET_ACCESS_KEY"), ("session_token", "AWS_SESSION_TOKEN")):
                if env.get(e):
                    cfg[k] = env[e]
            cfg["region"] = env.get("AWS_REGION") or env.get("AWS_DEFAULT_REGION") or "us-east-1"
            if cfg.get("endpoint"):
                cfg["virtual_hosted_style_request"] = False
                if str(cfg["endpoint"]).startswith("http://"):
                    client["allow_http"] = True
        self._store = from_url(base, config=cfg or None, client_options=client or None)

    def _path(self, rel: str) -> str:
        return f"{self.prefix}/{rel}" if self.prefix else rel

    # -- primitives --

    def list(self, rel_prefix: str) -> List[Tuple[str, Any]]:
        """``(relative path, etag-ish)`` under ``rel_prefix``."""
        if self.local is not None:
            root = self.local / rel_prefix
            if not root.exists():
                return []
            return [(str(p.relative_to(self.local)), p.stat().st_mtime_ns) for p in root.rglob("*") if p.is_file()]
        import obstore

        out = []
        n = len(self.prefix) + 1 if self.prefix else 0
        for batch in obstore.list(self._store, self._path(rel_prefix).rstrip("/") + "/"):
            for m in batch:
                out.append((m["path"][n:], m.get("e_tag")))
        return out

    def get(self, rel: str) -> Optional[bytes]:
        if self.local is not None:
            p = self.local / rel
            return p.read_bytes() if p.exists() else None
        import obstore

        try:
            return bytes(obstore.get(self._store, self._path(rel)).bytes())
        except FileNotFoundError:
            return None

    def head_etag(self, rel: str) -> Optional[str]:
        if self.local is not None:
            p = self.local / rel
            return str(p.stat().st_mtime_ns) if p.exists() else None
        import obstore

        try:
            return obstore.head(self._store, self._path(rel)).get("e_tag")
        except FileNotFoundError:
            return None

    def put(self, rel: str, data: bytes, *, create: bool = False, etag: Optional[str] = None) -> bool:
        """Write; ``create``: only if absent; ``etag``: only if unchanged. Returns whether it was written."""
        if self.local is not None:
            p = self.local / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            if create:
                try:
                    with open(p, "xb") as f:
                        f.write(data)
                    return True
                except FileExistsError:
                    return False
            if etag is not None and (not p.exists() or str(p.stat().st_mtime_ns) != etag):
                return False
            p.write_bytes(data)
            return True
        import obstore

        mode: Any = None
        if create:
            mode = "create"
        elif etag is not None:
            mode = {"e_tag": etag}
        try:
            obstore.put(self._store, self._path(rel), data, mode=mode)
            return True
        except Exception as e:  # AlreadyExists / Precondition
            name = type(e).__name__
            if "AlreadyExists" in name or "Precondition" in name or "exists" in str(e).lower() or "precondition" in str(e).lower():
                return False
            raise

    # -- layout --

    def runs(self, key: str, limit: int = 256) -> List[RunRecord]:
        """Newest-first run records of ``key`` (shards only; compacted pointers included when present)."""
        platform, tenant, _ = split_key(key)
        base = f"v1/runs/{tenant_path(platform, tenant)}/{key_hash(key)}"
        names = sorted((p for p, _ in self.list(base) if p.endswith(".json")), reverse=True)
        out: List[RunRecord] = []
        for p in names[:limit]:
            raw = self.get(p)
            if raw:
                try:
                    out.append(RunRecord.from_json(raw.decode(), allow_unknown=True))
                except Exception:
                    continue
        latest = self.get(f"v1/latest/{tenant_path(platform, tenant)}/{key_hash(key)}.json")
        if latest:
            try:
                d = json.loads(latest)
                for r in d.get("records", d.get("runs", [])) or []:
                    out.append(RunRecord.from_dict(r, allow_unknown=True))
            except Exception:
                pass
        return out

    def all_runs(self, tenant_prefix: str = "") -> Iterator[RunRecord]:
        """Every run shard under ``v1/runs/{tenant_prefix}``."""
        for p, _ in self.list(f"v1/runs/{tenant_prefix}".rstrip("/")):
            if not p.endswith(".json"):
                continue
            raw = self.get(p)
            if raw:
                try:
                    yield RunRecord.from_json(raw.decode(), allow_unknown=True)
                except Exception:
                    continue

    def decisions(self, tenant_prefix: str = "") -> Iterator[Dict[str, Any]]:
        for p, _ in self.list(f"v1/decisions/{tenant_prefix}".rstrip("/")):
            if p.endswith(".json"):
                raw = self.get(p)
                if raw:
                    try:
                        yield json.loads(raw)
                    except Exception:
                        continue


__all__ = ["Store"]
