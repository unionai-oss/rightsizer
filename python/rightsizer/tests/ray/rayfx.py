"""Shared helpers for the rightsizer.ray tests (imported as a plain module)."""

from __future__ import annotations

import datetime as dt

GiB = 1 << 30
MiB = 1 << 20
TENANT = "team-a/c1"


def make_engine(store, *, mode="enforce", allow_unverified=False, shapes=None, scoped=False, **extra):
    import rightsizer as rs

    cfg = {
        "history": {"uri": store, "durable": True, "shared": True, "allow_unverified_for_reductions": allow_unverified},
        "policy": {"mode": mode},
        "ray": {"tenant": TENANT, "tenant_scoped_credentials": scoped, "shapes": shapes or []},
    }
    for k, v in extra.items():
        cfg.setdefault(k, {}).update(v)
    return rs.Engine(cfg, env_overrides=False)


def seed(engine, key, peaks_bytes, *, trust="authenticated", status="succeeded", requested_mem=4 * GiB):
    """Write measured RunRecords for ``key`` (in-task style: Exact, 0.5 s)."""
    from rightsizer_runtime.wire import Censor, Measurement, OutcomeSource, Requests, ResourceSpec, RunRecord, RunStatus, Trust

    now = dt.datetime.now(dt.timezone.utc)
    for i, p in enumerate(peaks_bytes):
        rec = RunRecord(
            key=key, invocation_id=f"seed-{i}", attempt=1, started_at=now - dt.timedelta(minutes=30 - i),
            requested=ResourceSpec(requests=Requests(cpu=1000, memory=requested_mem)),
            status=RunStatus.parse(status), memory_peak=Measurement(float(p), Censor.EXACT, 0.5),
            cpu_mean=Measurement(0.2), duration_s=20.0, source=OutcomeSource.self_report(), trust=Trust.parse(trust),
        )
        engine.outcome(rec)
    engine.flush()
