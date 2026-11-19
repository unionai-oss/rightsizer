"""RunRecord writer (SPEC §8.1 object-store layout, §8.4 availability).

Layout (contract shared with ``rightsizer-history``)::

    {prefix}/v1/runs/{tenant-path}/{key-hash}/{ulid}.json

- ``key-hash`` = ``sha256(canonical_key.encode("utf-8")).hexdigest()[:32]`` —
  lower-case hex, first 32 hex chars, exactly ``rightsizer_core::WorkloadKey::hash``
  (the key canonicalized as Rust's ``Display`` writes it).
- ``tenant-path`` = ``{platform}/{tenant}``: the qualified tenant
  ``{platform}://{tenant}`` with ``://`` replaced by ``/``. The tenant is the
  one ``WorkloadKey::parse`` extracts (per-platform arity, or the explicit
  ``//`` separator); its charset needs no escaping.
- ``ulid`` is a fresh ULID per shard (not the decision id), so shards sort by
  write time; a record keeps its ULID across retries, so a retry rewrites the
  same object.

Backends: a local path or ``file://`` URI (atomic: temp file in the same
directory, fsync, ``os.replace``), and fsspec for every other scheme
(``s3://``, ``gs://``, ``abfs://``, ``memory://`` …), imported lazily.

S1: :meth:`RunRecordWriter.write` never raises. Failed writes are buffered
in memory (bounded, 10k records) and retried on the next write and on
:meth:`RunRecordWriter.flush`; overflow is dropped and counted.
"""

from __future__ import annotations

import collections
import hashlib
import logging
import os
import tempfile
import threading
import urllib.parse
from dataclasses import dataclass
from typing import Any, Callable, Deque, Dict, Mapping, Optional, Tuple

from .wire import RunRecord, WireError, canonical_key, new_ulid, parse_ulid, parse_workload_key

log = logging.getLogger("rightsizer.runtime.record")

DEFAULT_BUFFER_SIZE = 10_000
DEFAULT_PUT_TIMEOUT_S = 15.0  # one remote PUT; a hung store must not hang the task (S1)
KEY_HASH_HEX_LEN = 32  # rightsizer_core::WorkloadKey::hash
LAYOUT_VERSION = "v1"


def key_hash(key: str) -> str:
    """Lower-case hex sha256 of the canonical key string, first 32 hex chars.

    Same as ``rightsizer_core::WorkloadKey::hash`` (``sha256_hex(key.to_string())[..32]``):
    the key is first canonicalized as Rust's ``Display`` writes it.
    """
    return hashlib.sha256(canonical_key(key).encode("utf-8")).hexdigest()[:KEY_HASH_HEX_LEN]


def split_key(key: str, tenant: Optional[str] = None) -> Tuple[str, str, str]:
    """Split a key into (platform, tenant, suffix), as ``rightsizer_core::WorkloadKey::parse``.

    ``tenant``, if given, is a cross-check: the parsed tenant must equal it.
    """
    k = parse_workload_key(key)
    if tenant is not None and tenant.strip("/") != k.tenant:
        raise WireError(f"key {key!r} has tenant {k.tenant!r}, not {tenant!r}")
    return k.platform, k.tenant, k.suffix


def tenant_path(platform: str, tenant: str) -> str:
    """``{platform}://{tenant}`` → ``{platform}/{tenant}``.

    Platform and tenant are validated with the Rust charsets
    (``[a-z][a-z0-9-]*``; segments ``[A-Za-z0-9._@+=,-]+``), so no escaping
    is needed and ``.``/``..`` segments are rejected.
    """
    parse_workload_key(f"{platform}://{tenant}//x")
    if any(seg in (".", "..") for seg in tenant.split("/")):
        raise WireError(f"invalid tenant segment in {tenant!r}")
    return f"{platform}/{tenant}"


def shard_path(record: RunRecord, ulid: str, tenant: Optional[str] = None) -> str:
    """Relative shard path ``v1/runs/{platform}/{tenant}/{key-hash}/{ulid}.json``."""
    platform, ten, _ = split_key(record.key, tenant)
    return f"{LAYOUT_VERSION}/runs/{tenant_path(platform, ten)}/{key_hash(record.key)}/{parse_ulid(ulid)}.json"


# --------------------------------------------------------------------------- backends


class LocalBackend:
    """Local filesystem; atomic create via temp file + ``os.replace``."""

    def __init__(self, root: str):
        self.root = root

    def put(self, rel: str, data: bytes) -> None:
        path = os.path.join(self.root, *rel.split("/"))
        d = os.path.dirname(path)
        os.makedirs(d, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=".rs-", suffix=".tmp", dir=d)
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
        try:  # make the rename durable; best effort
            dfd = os.open(d, os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
        except OSError:
            pass


class FsspecBackend:
    """Any fsspec URL. One ``pipe_file`` per shard: object stores PUT atomically."""

    def __init__(self, uri: str, storage_options: Optional[Mapping[str, Any]] = None, fs: Any = None):
        if fs is None:
            import fsspec  # lazy: optional dependency

            from .storage import fsspec_options

            # Explicit options only (canonical object_store keys or fsspec
            # spellings); nothing is exported to the environment (SPEC §10.3).
            fs, root = fsspec.core.url_to_fs(uri, **fsspec_options(uri, storage_options))
        else:
            root = uri.split("://", 1)[1] if "://" in uri else uri
        self.fs = fs
        self.root = root.rstrip("/")

    def put(self, rel: str, data: bytes) -> None:
        path = f"{self.root}/{rel}" if self.root else rel
        self.fs.pipe_file(path, data)


def make_backend(uri: str, storage_options: Optional[Mapping[str, Any]] = None, fs: Any = None) -> Any:
    if fs is None:
        parsed = urllib.parse.urlparse(uri)
        if parsed.scheme == "file":
            if parsed.netloc not in ("", "localhost"):
                raise ValueError(f"file URI with host is not supported: {uri!r}")
            return LocalBackend(urllib.parse.unquote(parsed.path))
        if "://" not in uri:
            return LocalBackend(os.path.abspath(os.path.expanduser(uri)))
    return FsspecBackend(uri, storage_options, fs)


# --------------------------------------------------------------------------- writer


@dataclass
class WriterStats:
    written: int = 0
    buffered: int = 0  # currently buffered
    dropped: int = 0  # dropped on overflow (or invalid)
    errors: int = 0  # failed PUT attempts
    retried: int = 0  # buffered records later written


class RunRecordWriter:
    """Writes immutable RunRecord shards. Never raises into user code (S1).

    Parameters
    ----------
    uri:
        Store prefix: a local path, ``file://…``, or any fsspec URL.
    storage_options:
        Store options (credentials, endpoint, …): canonical ``object_store``
        keys (``access_key_id``, ``endpoint``, ``region``, … — the same
        options the Rust engine takes) or fsspec spellings; translated by
        :func:`rightsizer_runtime.storage.fsspec_options`. Never exported to
        the environment.
    tenant:
        Optional cross-check: every key written must have this tenant
        (without platform). Per-call ``tenant=`` overrides it.
    buffer_size:
        Maximum buffered records awaiting retry (SPEC §8.4: 10k).
    put_timeout_s:
        Longest a remote (fsspec) PUT may block the caller; a PUT still running
        then is treated as failed and buffered (rewriting a shard is idempotent).
        ``None`` waits forever. Local writes are not bounded.
    fs:
        An fsspec filesystem to use instead of resolving ``uri`` (tests).
    """

    def __init__(
        self,
        uri: str,
        *,
        storage_options: Optional[Mapping[str, Any]] = None,
        tenant: Optional[str] = None,
        buffer_size: int = DEFAULT_BUFFER_SIZE,
        fs: Any = None,
        ulid_factory: Callable[[], str] = new_ulid,
        put_timeout_s: Optional[float] = DEFAULT_PUT_TIMEOUT_S,
    ):
        self.uri = uri
        self.tenant = tenant
        self.buffer_size = max(0, int(buffer_size))
        self.stats = WriterStats()
        self._ulid = ulid_factory
        self._lock = threading.Lock()
        self._buffer: Deque[Tuple[str, bytes]] = collections.deque()
        self._backend: Any = None
        self._backend_error: Optional[str] = None
        self._storage_options = storage_options
        self._fs = fs
        self.put_timeout_s = put_timeout_s

    def _get_backend(self) -> Any:
        if self._backend is None:
            self._backend = make_backend(self.uri, self._storage_options, self._fs)
        return self._backend

    def path_for(self, record: RunRecord, ulid: str, tenant: Optional[str] = None) -> str:
        return shard_path(record, ulid, tenant if tenant is not None else self.tenant)

    def write(self, record: RunRecord, tenant: Optional[str] = None) -> bool:
        """Write one record. Returns ``True`` if persisted now; ``False`` if buffered or dropped."""
        try:
            rel = self.path_for(record, self._ulid(), tenant)
            data = record.to_json().encode("utf-8")
        except Exception as e:
            log.warning("rightsizer: RunRecord not written (invalid): %s", e)
            with self._lock:
                self.stats.dropped += 1
            return False
        try:
            self.flush()
            return self._put_or_buffer(rel, data)
        except Exception as e:  # pragma: no cover - defensive
            log.warning("rightsizer: RunRecord write failed: %s", e)
            return False

    def _put(self, rel: str, data: bytes) -> None:
        backend = self._get_backend()
        if self.put_timeout_s is None or isinstance(backend, LocalBackend):
            backend.put(rel, data)
            return
        # A daemon thread bounds how long a hung object store can block the task's own
        # return (the record is written in the task's ``finally``).
        box: Dict[str, Any] = {}

        def run() -> None:
            try:
                backend.put(rel, data)
            except BaseException as e:  # noqa: BLE001 - reported to the caller below
                box["error"] = e

        t = threading.Thread(target=run, name="rightsizer-record-put", daemon=True)
        t.start()
        t.join(self.put_timeout_s)
        if t.is_alive():
            raise TimeoutError(f"RunRecord PUT did not finish within {self.put_timeout_s:g} s")
        if "error" in box:
            raise box["error"]

    def _put_or_buffer(self, rel: str, data: bytes) -> bool:
        try:
            self._put(rel, data)
            with self._lock:
                self.stats.written += 1
            return True
        except Exception as e:
            with self._lock:
                self.stats.errors += 1
                if self.stats.errors <= 3 or self.stats.errors % 100 == 0:
                    log.warning("rightsizer: RunRecord write failed (buffered for retry): %s", e)
                self._enqueue(rel, data)
            return False

    def _enqueue(self, rel: str, data: bytes) -> None:
        if len(self._buffer) >= self.buffer_size:
            self.stats.dropped += 1
            if self.stats.dropped == 1 or self.stats.dropped % 1000 == 0:
                log.warning("rightsizer: RunRecord buffer full (%d); dropped %d", self.buffer_size, self.stats.dropped)
            return
        self._buffer.append((rel, data))
        self.stats.buffered = len(self._buffer)

    def flush(self) -> int:
        """Retry buffered records in order; stop at the first failure. Returns how many were written."""
        n = 0
        try:
            while True:
                with self._lock:
                    if not self._buffer:
                        break
                    rel, data = self._buffer[0]
                try:
                    self._put(rel, data)
                except Exception as e:
                    with self._lock:
                        self.stats.errors += 1
                    log.debug("rightsizer: retry failed: %s", e)
                    break
                with self._lock:
                    if self._buffer and self._buffer[0][0] == rel:
                        self._buffer.popleft()
                    self.stats.written += 1
                    self.stats.retried += 1
                    self.stats.buffered = len(self._buffer)
                n += 1
        except Exception as e:  # pragma: no cover - defensive
            log.warning("rightsizer: flush failed: %s", e)
        return n

    @property
    def pending(self) -> int:
        return len(self._buffer)


__all__ = [
    "DEFAULT_BUFFER_SIZE",
    "DEFAULT_PUT_TIMEOUT_S",
    "FsspecBackend",
    "LocalBackend",
    "RunRecordWriter",
    "WriterStats",
    "key_hash",
    "make_backend",
    "shard_path",
    "split_key",
    "tenant_path",
]
