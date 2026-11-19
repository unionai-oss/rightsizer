"""(b) History accumulates from in-task measurement; after ``min_history``
trusted measured successes, enforce reduces ``memory``; without trusted
evidence it holds (Ray memory is unenforced → S3(b) only)."""

from __future__ import annotations

import time

import rayit_tasks as T
from conftest import engine, record, run_records

GiB = 1 << 30
MiB = 1 << 20
SHAPES = [{"name": "it-node", "cpu": 4000, "memory": 8 * GiB, "price_per_hour": 0.2}]


def _run_series(ray, f, n):
    out = []
    for _ in range(n):
        ref = f.remote(200)
        res = ray.get(ref)
        out.append({"task_id": ref.task_id().hex(), "assigned": res["assigned"]})
        time.sleep(0.3)  # the worker writes its record right after the body
    return out


def _state_memory(task_id):
    from ray.util.state import get_task

    st = get_task(task_id, timeout=10)
    st = st[-1] if isinstance(st, list) else st
    return (getattr(st, "required_resources", None) or {}).get("memory")


def _scenario(ray, store, *, scoped, allow_unverified, key):
    import rightsizer.ray as rr

    rr.configure(engine=engine(store, mode="enforce", scoped=scoped, allow_unverified=allow_unverified),
                 shapes=SHAPES)
    f = rr.remote(num_cpus=1, memory=2 * GiB, key=key)(T.alloc)
    calls = _run_series(ray, f, 4)  # 4 measured successes (min_history = 3)
    time.sleep(1.5)  # history.cache_ttl / memo_ttl are 1 s in these tests
    ref = f.remote(200)
    after = ray.get(ref)
    d = rr.observer()  # noqa: F841
    recs = run_records(store, key)
    last_decision = rr.settings().engine.decide  # noqa: F841
    return {
        "first_calls_memory": [c["assigned"].get("memory") for c in calls],
        "after_memory_assigned": after["assigned"].get("memory"),
        "after_memory_state_api": _state_memory(ref.task_id().hex()),
        "records": [{"status": r["status"], "trust": r["trust"], "peak_mib": round(r["memory_peak"]["value"] / MiB, 1),
                     "censor": r["memory_peak"]["censor"], "resolution_s": r["memory_peak"]["resolution_s"],
                     "requested_memory": r["requested"]["requests"].get("memory")} for r in recs],
    }


def test_enforce_reduces_memory_after_trusted_history(ray_session, store):
    import rightsizer.ray as rr
    from rightsizer.ray._sizing import SizingPolicy, decide_options

    res = _scenario(ray_session, store, scoped=True, allow_unverified=False, key="it.hist.scoped")
    d = decide_options({"num_cpus": 1, "memory": 2 * GiB}, SizingPolicy(key="it.hist.scoped"), target=T.alloc).decision
    res["decision"] = {"applied_memory": d.applied.requests.memory, "held": [h.to_dict() for h in d.held],
                       "explanation": list(d.explanation)}
    record("history_scoped", res)
    assert all(m == 2 * GiB for m in res["first_calls_memory"][:3])  # cold: held at the prior
    assert len(res["records"]) >= 4 and all(r["trust"] == "authenticated" for r in res["records"])
    assert all(150 < r["peak_mib"] < 400 and r["censor"] == "exact" for r in res["records"])
    assert res["after_memory_assigned"] < 2 * GiB  # enforce reduced memory
    assert res["after_memory_state_api"] == res["after_memory_assigned"]
    assert rr.settings().trust == "authenticated"


def test_unverified_history_holds_without_opt_in(ray_session, store):
    from rightsizer.ray._sizing import SizingPolicy, decide_options

    res = _scenario(ray_session, store, scoped=False, allow_unverified=False, key="it.hist.unverified")
    d = decide_options({"num_cpus": 1, "memory": 2 * GiB}, SizingPolicy(key="it.hist.unverified"), target=T.alloc).decision
    res["decision"] = {"applied_memory": d.applied.requests.memory, "recommended_memory": d.recommended.requests.memory,
                       "held": [h.to_dict() for h in d.held], "explanation": list(d.explanation)}
    record("history_unverified", res)
    assert all(r["trust"] == "unverified" for r in res["records"])
    assert res["after_memory_assigned"] == 2 * GiB  # held: no trusted S3(b) evidence
    assert any(h["axis"] == "memory" and h["reason"] == "not_enforced" for h in res["decision"]["held"])
    assert res["decision"]["recommended_memory"] < 2 * GiB  # still reported as a recommendation


def test_operator_opt_in_allows_unverified(ray_session, store):
    res = _scenario(ray_session, store, scoped=False, allow_unverified=True, key="it.hist.optin")
    record("history_optin", res)
    assert res["after_memory_assigned"] < 2 * GiB
