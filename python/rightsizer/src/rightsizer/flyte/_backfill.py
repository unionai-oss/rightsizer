"""``rsz history import flyte`` (flyte.md §8.3): backfill statuses from ``flyte.remote``.

Walks ``Run.listall`` → ``Action.listall`` → ``ActionDetails`` and writes one
``RunRecord`` per attempt through the engine (``outcome``: dedup, joins,
attribution, lease closing):

- key ``flyte://{org}/{project}/{domain}/{task name}``;
- requested resources from the task template proto
  (``container.resources``, ``extended_resources.gpu_accelerator``);
- status from the attempt phase and ``error_info`` (``code == "OOMKilled"``
  → ``oom_killed``; a CUDA/HIP OOM → ``gpu_oom``; timed out → ``timeout``;
  a cache hit → ``cached``);
- duration from the attempt's start/end;
- source ``flyte_remote``, trust ``verified`` (the tenant is the Flyte
  project/domain, an authenticated identity), no peaks (open-source Flyte has
  no metrics API; child records are the only peak source).

``invocation_id`` is ``{run}/{action}``: stable across re-imports, so a
re-run inserts duplicates the store drops.
"""

from __future__ import annotations

import datetime as _dt
import logging
import re
from typing import Any, Dict, Iterator, List, Optional, Tuple

from rightsizer_runtime.wire import (
    Confidence,
    GpuShape,
    Limits,
    OutcomeSource,
    Requests,
    ResourceSpec,
    RunRecord,
    RunStatus,
    Trust,
)

from .. import _units
from ._resources import FLYTE_TO_CATALOG

log = logging.getLogger("rightsizer.flyte")

_DUR = re.compile(r"^(\d+)([smhdw])$")


def parse_since(s: str) -> _dt.timedelta:
    m = _DUR.match(s.strip())
    if not m:
        raise ValueError(f"--since: expected e.g. 30d, 12h, 90m; got {s!r}")
    n, u = int(m.group(1)), m.group(2)
    return _dt.timedelta(**{{"s": "seconds", "m": "minutes", "h": "hours", "d": "days", "w": "weeks"}[u]: n})


def _ts(t: Any) -> Optional[_dt.datetime]:
    try:
        if t is None or (t.seconds == 0 and t.nanos == 0):
            return None
        return t.ToDatetime(tzinfo=_dt.timezone.utc)
    except Exception:
        return None


def requested_from_template(tmpl: Any) -> Optional[ResourceSpec]:
    """``ResourceSpec`` from a ``flyteidl2.core.TaskTemplate`` proto (None when it has no container)."""
    try:
        from flyteidl2.core import tasks_pb2
    except Exception:  # pragma: no cover
        return None
    if tmpl is None or not tmpl.HasField("container"):
        return None
    names = tasks_pb2.Resources.ResourceName

    def entries(xs: Any) -> Dict[str, str]:
        return {names.Name(e.name): e.value for e in xs}

    req = entries(tmpl.container.resources.requests)
    lim = entries(tmpl.container.resources.limits)
    gpu = None
    n = req.get("GPU") or lim.get("GPU")
    if n:
        accel = "unknown-gpu"
        try:
            if tmpl.HasField("extended_resources") and tmpl.extended_resources.HasField("gpu_accelerator"):
                dev = tmpl.extended_resources.gpu_accelerator.device
                accel = FLYTE_TO_CATALOG.get(dev, "unknown-gpu")
        except Exception:
            pass
        gpu = GpuShape(accel=accel, count=int(float(n)))

    def mem(v: Optional[str]) -> Optional[int]:
        return None if not v else _units.memory_bytes(v)

    def cpu(v: Optional[str]) -> Optional[int]:
        return None if not v else _units.cpu_millis(v)

    return ResourceSpec(
        requests=Requests(cpu=cpu(req.get("CPU")), memory=mem(req.get("MEMORY")), gpu=gpu,
                          ephemeral_storage=mem(req.get("EPHEMERAL_STORAGE"))),
        limits=Limits(cpu=cpu(lim.get("CPU")), memory=mem(lim.get("MEMORY"))),
    )


def status_of(phase_name: str, error_info: Any, cache_status_name: str = "") -> RunStatus:
    p = phase_name.upper()
    if "SUCCEEDED" in p:
        return RunStatus.CACHED if "HIT" in cache_status_name.upper() else RunStatus.SUCCEEDED
    if "TIMED_OUT" in p or "TIMEOUT" in p:
        return RunStatus.TIMEOUT
    if "ABORTED" in p:
        return RunStatus.UNKNOWN
    code = getattr(error_info, "code", "") or ""
    msg = getattr(error_info, "message", "") or ""
    if code == "OOMKilled" or "OOMKILLED" in code.upper():
        return RunStatus.OOM_KILLED
    if code == "OutOfMemoryError" or re.search(r"\b(CUDA|HIP) out of memory", msg, re.IGNORECASE):
        return RunStatus.GPU_OOM
    if "FAILED" in p:
        return RunStatus.FAILED
    return RunStatus.UNKNOWN


def records_for(details: Any) -> List[RunRecord]:
    """One record per attempt of an ``ActionDetails`` (terminal attempts only)."""
    from flyteidl2.common import phase_pb2

    try:
        from flyteidl2.core import catalog_pb2

        cache_name = lambda v: catalog_pb2.CatalogCacheStatus.Name(v)  # noqa: E731
    except Exception:  # pragma: no cover
        cache_name = lambda v: str(v)  # noqa: E731
    pb = details.pb2
    rid = pb.id.run
    task_name = details.task_name or ""
    if not task_name:
        return []
    from rightsizer_runtime.flyte_child import flyte_key

    key = flyte_key(rid.org, rid.project, rid.domain, task_name)
    tmpl = pb.task.task_template if pb.HasField("task") else None
    requested = requested_from_template(tmpl)
    out = []
    for i, a in enumerate(pb.attempts):
        phase = phase_pb2.ActionPhase.Name(a.phase)
        if not any(x in phase for x in ("SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED")):
            continue
        err = a.error_info if a.HasField("error_info") else (pb.error_info if pb.HasField("error_info") else None)
        st = status_of(phase, err, cache_name(a.cache_status))
        start, end = _ts(a.start_time), _ts(a.end_time)
        dur = (end - start).total_seconds() if (start and end) else None
        out.append(RunRecord(
            key=key,
            invocation_id=f"{rid.name}/{pb.id.name}",
            attempt=int(a.attempt or (i + 1)),
            started_at=start or end or _dt.datetime.now(_dt.timezone.utc),
            requested=requested,
            status=st,
            status_confidence=Confidence.OBSERVED,
            duration_s=dur,
            source=OutcomeSource(kind="flyte_remote"),
            trust=Trust.VERIFIED,
        ))
    return out


def iter_action_details(project: Optional[str], domain: Optional[str], since: _dt.timedelta,
                        limit: int = 1000) -> Iterator[Any]:
    import flyte.remote as remote
    from flyte.remote._common import TimeFilter

    after = _dt.datetime.now(_dt.timezone.utc) - since
    for run in remote.Run.listall(project=project, domain=domain, created_at=TimeFilter(after=after), limit=limit):
        try:
            for act in remote.Action.listall(for_run_name=run.name):
                try:
                    yield remote.ActionDetails.get(run_name=run.name, name=act.name)
                except Exception as e:
                    log.warning("rightsizer: %s/%s details: %s", run.name, act.name, e)
        except Exception as e:
            log.warning("rightsizer: actions of %s: %s", run.name, e)


def backfill(project: Optional[str] = None, domain: Optional[str] = None, since: str = "30d", *, limit: int = 1000,
             dry_run: bool = False, engine: Any = None) -> Dict[str, Any]:
    """Import statuses; returns counts by status and how many records were written."""
    from . import _engine as _eng

    eng = engine
    if eng is None and not dry_run:
        eng = _eng.get_engine()
    counts: Dict[str, int] = {}
    written = joined = actions = 0
    for det in iter_action_details(project, domain, parse_since(since), limit=limit):
        actions += 1
        for rec in records_for(det):
            counts[rec.status.value] = counts.get(rec.status.value, 0) + 1
            if dry_run:
                continue
            try:
                rep = eng.outcome(rec)
                written += int(bool(rep.written))
                joined += int(bool(rep.joined))
            except Exception as e:
                log.warning("rightsizer: record for %s not written: %s", rec.key, e)
    if eng is not None:
        try:
            eng.flush()
        except Exception:
            pass
    return {"actions": actions, "statuses": counts, "written": written, "joined": joined, "dry_run": dry_run}


__all__ = ["backfill", "parse_since", "records_for", "requested_from_template", "status_of"]
