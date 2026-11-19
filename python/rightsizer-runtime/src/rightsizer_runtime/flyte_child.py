"""Flyte child side (flyte.md §8.1): measure a task's ``execute`` in its pod and
write one :class:`~.wire.RunRecord` shard.

``rightsizer.flyte.RightsizedTask.execute`` calls :func:`measured_execute`.
Only the standard library and Flyte (already imported in a task pod) are
used, so leaf images need ``rightsizer-runtime`` alone; ``flyte`` is imported
lazily inside the functions.

- **Store.** ``RIGHTSIZER__HISTORY__URI`` (injected by
  ``RightsizedEnvironment``), else ``{scheme}://{bucket}/rightsizer`` derived
  from the action's ``raw_data_path`` (:func:`store_uri`).
- **Writes** go through ``flyte.storage.put_stream`` when Flyte has a
  configured remote store (the pod's own credentials), else through
  :class:`~.record.RunRecordWriter` (local paths, fsspec).
- **Key and attempt** come from ``RIGHTSIZER_DECISION`` / ``RIGHTSIZER_ATTEMPT``
  (set by the deciding parent); a task run without a parent decision (a root
  launched elsewhere) is keyed by its own definition and recorded with
  ``requested`` = its declared resources.
- **Reusable containers** record ``memory_peak = None`` (not attributable).
- An OOM kill skips everything here; the parent records that outcome.

S1: nothing here raises into the task. The task's own result and exceptions
pass through unchanged.
"""

from __future__ import annotations

import logging
import os
import urllib.parse
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Mapping, Optional, Tuple

from .child import MeasuredExecution
from .env import read_decision
from .measure import FLYTE_INTERVAL_S
from .record import RunRecordWriter, shard_path
from .wire import InputMeta, ResourceSpec, RunRecord, Trust, new_ulid

log = logging.getLogger("rightsizer.runtime.flyte")

HISTORY_URI_ENV = "RIGHTSIZER__HISTORY__URI"
DEFAULT_PREFIX = "rightsizer"
CPU_WINDOW_S = 60.0


def store_uri(raw_data_path: Optional[str] = None, environ: Optional[Mapping[str, str]] = None) -> Optional[str]:
    """The history store prefix: ``RIGHTSIZER__HISTORY__URI``, else
    ``{scheme}://{bucket}/rightsizer`` from the raw data path (object stores
    only; a local raw data path gives ``None``)."""
    env = os.environ if environ is None else environ
    uri = env.get(HISTORY_URI_ENV)
    if uri:
        return uri.rstrip("/")
    if not raw_data_path or "://" not in raw_data_path:
        return None
    p = urllib.parse.urlparse(raw_data_path)
    if p.scheme in ("", "file") or not p.netloc:
        return None
    return f"{p.scheme}://{p.netloc}/{DEFAULT_PREFIX}"


def flyte_tenant(org: Optional[str], project: Optional[str], domain: Optional[str]) -> str:
    """The Flyte tenant ``{org}/{project}/{domain}`` (arity 3), with empty parts as ``default``."""

    def seg(s: Optional[str]) -> str:
        s = (s or "").strip()
        out = "".join(ch if (ch.isalnum() or ch in "._@+=,-") else "-" for ch in s)
        return out or "default"

    return f"{seg(org)}/{seg(project)}/{seg(domain)}"


def flyte_key(org: Optional[str], project: Optional[str], domain: Optional[str], suffix: str, platform: str = "flyte") -> str:
    """``flyte://{org}/{project}/{domain}/{suffix}`` (``platform="flyte-local"`` keys use ``//``)."""
    t = flyte_tenant(org, project, domain)
    if platform == "flyte":
        return f"flyte://{t}/{suffix}"
    return f"{platform}://{t}//{suffix}"


class _Collector:
    """A writer stand-in for :class:`MeasuredExecution`: keeps the record, so
    the async caller can write it with ``flyte.storage`` afterwards."""

    def __init__(self) -> None:
        self.records: List[Tuple[RunRecord, Optional[str]]] = []

    def write(self, record: RunRecord, tenant: Optional[str] = None) -> bool:
        self.records.append((record, tenant))
        return True


def _flyte_remote_storage() -> bool:
    try:
        from flyte._initialize import get_storage  # type: ignore

        return get_storage() is not None
    except Exception:
        return False


async def write_records(uri: str, records: Iterable[Tuple[RunRecord, Optional[str]]]) -> int:
    """Write records under ``uri``; returns how many were written. Never raises."""
    n = 0
    fallback: Optional[RunRecordWriter] = None
    for rec, tenant in records:
        try:
            rel = shard_path(rec, new_ulid(), tenant)
            data = rec.to_json().encode("utf-8")
        except Exception as e:
            log.warning("rightsizer: RunRecord not written (invalid): %s", e)
            continue
        written = False
        if "://" in uri and not uri.startswith("file://"):
            try:
                import flyte.storage as storage  # type: ignore

                await storage.put_stream(data, to_path=f"{uri.rstrip('/')}/{rel}")
                written = True
            except Exception as e:
                log.warning("rightsizer: RunRecord write through flyte.storage failed (%s); trying fsspec", e)
        if not written:
            try:
                if fallback is None:
                    fallback = RunRecordWriter(uri)
                written = fallback.write(rec, tenant=tenant)
            except Exception as e:
                log.warning("rightsizer: RunRecord not written: %s", e)
        n += int(bool(written))
    return n


async def measured_execute(
    execute: Callable[..., Awaitable[Any]],
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
    *,
    key: Optional[str] = None,
    requested: Optional[ResourceSpec] = None,
    reusable: bool = False,
    local: bool = False,
    observe_local: bool = False,
    inputs: Iterable[InputMeta] = (),
    code_digest: Optional[str] = None,
    environ: Optional[Mapping[str, str]] = None,
    scope_kwargs: Optional[Dict[str, Any]] = None,
) -> Any:
    """Run ``await execute(*args, **kwargs)`` inside the measurement scope (flyte.md §8.1).

    ``key`` is the task's own key and ``requested`` its declared resources,
    used when no parent decision is present (a root launched elsewhere).
    In local mode nothing is measured unless ``observe_local``; local records
    use the ``flyte-local`` platform and ``requested = None``.
    """
    env = os.environ if environ is None else environ
    try:
        if local and not observe_local:
            return await execute(*args, **kwargs)
        decision = read_decision(env)
        raw = None
        try:
            import flyte  # type: ignore

            c = flyte.ctx()
            raw = c.raw_data_path.path if c and c.raw_data_path is not None else None
        except Exception:
            raw = None
        uri = store_uri(raw, env)
        if uri is None:
            log.debug("rightsizer: no history store (set %s); running unmeasured", HISTORY_URI_ENV)
            return await execute(*args, **kwargs)
        if local:
            decision = None  # local records are never joined to decisions
        collector = _Collector()
        me = MeasuredExecution(
            decision if decision is not None else None,
            collector,
            platform="flyte",
            key=key,
            reusable=reusable,
            environ=env,
            trust=Trust.UNVERIFIED,
            code_digest=code_digest,
            inputs=inputs,
            interval_s=FLYTE_INTERVAL_S,
            cpu_window_s=CPU_WINDOW_S,
            **(scope_kwargs or {}),
        )
    except Exception as e:  # S1: measurement setup failed
        log.warning("rightsizer: measurement not set up: %s", e)
        return await execute(*args, **kwargs)
    try:
        return await me.arun(execute, *args, **kwargs)
    finally:
        try:
            recs = []
            for rec, tenant in collector.records:
                if decision is None and not local:
                    from dataclasses import replace

                    rec = replace(rec, requested=requested)
                recs.append((rec, tenant))
            if recs:
                await write_records(uri, recs)
        except Exception as e:  # S1
            log.warning("rightsizer: RunRecord not written: %s", e)


__all__ = [
    "CPU_WINDOW_S",
    "HISTORY_URI_ENV",
    "flyte_key",
    "flyte_tenant",
    "measured_execute",
    "store_uri",
    "write_records",
]
