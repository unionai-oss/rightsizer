"""Deploy-time sizing and ``reconcile`` (flyte.md §6.2–6.3).

``rs.flyte.deploy(env)`` / ``rsz flyte deploy FILE ENV``:

1. per rightsizable, non-reusable task: an **input-independent** decision
   (``b_now`` = the p90 of recent same-key input sizes; S3(b) evidence only:
   a deployed default has no ladder);
2. **increases-only** unless a ``reconcile`` singleton holds a live lease
   (then reductions are allowed too);
3. each changed task is deployed as ``t._replace(resources=merged)`` (not
   ``override``, which would pin every axis), swapped into the environment's
   registry for the deploy (private API, flyte.md §11) and swapped back after;
4. the engine logs the decision (with the origin prior) to
   ``{prefix}/v1/decisions/``; a deploy record
   ``{prefix}/v1/flyte/deploys/{tenant}/{task}/{version}.json`` maps the task
   name and deploy version to it, so run-time sizing and the webhook can
   attribute against the origin prior;
5. ``flyte.deploy``. The version is the caller's or, by default, the one Flyte
   derives (code bundle, environments with the sized tasks swapped in, images),
   so a code change always deploys a new version; the deploy records use the
   versions Flyte reports. ``restore=True`` deploys from source unchanged.

``reconcile`` (``rsz flyte reconcile --schedule 5m``) is a singleton (a
lease object created with a conditional PUT, renewed each round). Each round
it reads the mode ceiling and breakers; when the ceiling is below enforce, or
a deploy-sized task's key has an open breaker, it redeploys the affected
environments at the prior (``restore``), well within 10 minutes.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
import logging
import math
import os
import socket
import time
from typing import Any, Dict, List, Optional, Tuple

from rightsizer_runtime.policy import InvocationPolicy, PolicyStack
from rightsizer_runtime.wire import Mode, format_timestamp, new_ulid, parse_timestamp, utcnow

from ..types import Input
from . import _engine as _eng
from ._resources import merge, requests_of, to_spec
from ._task import is_rightsized

log = logging.getLogger("rightsizer.flyte")

LEASE = "v1/flyte/reconcile/lease.json"
LEASE_TTL_S = 15 * 60
DEPLOYS = "v1/flyte/deploys"


def _store() -> Any:
    from ._store import Store

    try:
        _eng.get_engine()
    except Exception:
        pass
    uri = _eng._state.get("store") or os.environ.get("RIGHTSIZER__HISTORY__URI")
    if not uri:
        raise RuntimeError("no history store: set RIGHTSIZER__HISTORY__URI (or [history] uri)")
    return Store(uri)


def _tenant() -> Tuple[Optional[str], Optional[str], Optional[str]]:
    from ._run import _tenant_from

    return _tenant_from({})


# ── reconcile lease ──────────────────────────────────────────────────────


def lease_state(store: Any = None) -> Optional[Dict[str, Any]]:
    """The reconcile lease, if one is live."""
    store = store or _store()
    raw = store.get(LEASE)
    if not raw:
        return None
    try:
        d = json.loads(raw)
        if parse_timestamp(d["expires_at"]) > utcnow():
            return d
    except Exception:
        return None
    return None


def reconcile_active(store: Any = None) -> bool:
    try:
        return lease_state(store) is not None
    except Exception:
        return False


def acquire_lease(store: Any, holder: str, ttl_s: int = LEASE_TTL_S) -> bool:
    """Take or renew the singleton lease (conditional PUTs). Returns whether we hold it."""
    now = utcnow()
    body = json.dumps({"holder": holder, "renewed_at": format_timestamp(now),
                       "expires_at": format_timestamp(now + _dt.timedelta(seconds=ttl_s))}).encode()
    raw = store.get(LEASE)
    if raw is None:
        return store.put(LEASE, body, create=True)
    try:
        cur = json.loads(raw)
        live = parse_timestamp(cur["expires_at"]) > now
    except Exception:
        cur, live = {}, False
    if live and cur.get("holder") != holder:
        return False
    etag = store.head_etag(LEASE)
    return store.put(LEASE, body, etag=etag)


def release_lease(store: Any, holder: str) -> None:
    raw = store.get(LEASE)
    if not raw:
        return
    try:
        cur = json.loads(raw)
    except Exception:
        return
    if cur.get("holder") == holder:
        cur["expires_at"] = format_timestamp(utcnow())
        store.put(LEASE, json.dumps(cur).encode(), etag=store.head_etag(LEASE))


# ── deploy-time decisions ────────────────────────────────────────────────


def recent_input_bytes(store: Any, key: str, limit: int = 32) -> Optional[int]:
    """p90 of the total input bytes of the key's recent runs (SPEC §7.4 ``b_now``)."""
    sizes = []
    for r in store.runs(key, limit=limit * 2)[: limit * 2]:
        b = sum(int(i.bytes or 0) for i in (r.inputs or ()) if getattr(i, "bytes", None))
        if b > 0:
            sizes.append(b)
    if not sizes:
        return None
    sizes.sort()
    return sizes[min(len(sizes) - 1, int(math.ceil(0.9 * len(sizes))) - 1)]


def deploy_decision(task: Any, *, allow_reductions: bool, store: Any = None, tenant: Any = None) -> Tuple[Any, Any]:
    """``(decision, merged resources)`` for one task; ``merged is task.resources`` when unchanged."""
    tenant = tenant or _tenant()
    from ._task import task_key

    key = task_key(task, *tenant)
    b = None
    try:
        b = recent_input_bytes(store or _store(), key)
    except Exception as e:
        log.debug("rightsizer: recent input sizes for %s: %s", key, e)
    inputs = (Input.sized("b_now", b),) if b else ()
    stack = PolicyStack((InvocationPolicy(mode=Mode.ENFORCE),))
    plan = task._rs_plan(stack, PolicyStack(), (), {}, tenant=tenant, root=True, inputs=inputs)
    if plan is None:
        return None, task.resources
    if not allow_reductions:
        prior_req = requests_of(task.resources)
        if prior_req is not None:
            plan.policy["floor"] = prior_req.to_dict()
    d = task._rs_decide(plan, None)
    if Mode.parse(d.mode) is not Mode.ENFORCE or task.resources is None:
        return d, task.resources
    return d, merge(task.resources, d.applied.requests, allow_gpu_type_change=plan.allow_gpu_type_change)


def _digest(plans: List[Tuple[str, Any, Any, Any]]) -> str:
    h = hashlib.sha256(b"rsz-deploy/1\0")
    for name, _t, _d, merged in sorted(plans, key=lambda x: x[0]):
        h.update(name.encode())
        h.update(json.dumps(to_spec(merged).to_dict(), sort_keys=True).encode())
    return h.hexdigest()[:12]


def plan_deploy(*envs: Any, allow_reductions: Optional[bool] = None) -> List[Tuple[Any, str, Any, Any, Any]]:
    """``[(env, task name, original, decision, merged)]`` for every changed task
    (none, logged, when there is no history store: the deploy is then from source, S1)."""
    try:
        store = _store()
    except Exception as e:  # S1: deploy unchanged
        log.warning("rightsizer: deploy-time sizing unavailable, deploying from source: %s", e)
        return []
    if allow_reductions is None:
        allow_reductions = reconcile_active(store)
    out = []
    for env in envs:
        for name, t in list(getattr(env, "tasks", {}).items()):
            if not is_rightsized(t):
                continue
            if getattr(t, "reusable", None) is not None:
                log.info("rightsizer: %s is reusable: recommend-only at deploy (see rsz shadow)", name)
                continue
            try:
                d, merged = deploy_decision(t, allow_reductions=allow_reductions, store=store)
            except Exception as e:  # S1: deploy unchanged
                log.warning("rightsizer: %s deploys unchanged: %s", name, e)
                continue
            if d is not None and merged is not t.resources and merged != t.resources:
                out.append((env, name, t, d, merged))
    return out


def deploy(*envs: Any, version: Optional[str] = None, restore: bool = False, reconcile: Optional[bool] = None,
           dry_run: bool = False, **kw: Any) -> Any:
    """Deploy ``envs`` with deploy-time sizing (or from source with ``restore=True``)."""
    import flyte

    if restore:
        return flyte.deploy(*envs, version=version, dry_run=dry_run, **kw)
    plans = plan_deploy(*envs, allow_reductions=reconcile)
    if not plans:
        log.info("rightsizer: no task changes at deploy; deploying from source")
        return flyte.deploy(*envs, version=version, dry_run=dry_run, **kw)
    # Not a version of our own: a digest of the sizing alone would give a code change the
    # version of the previous deploy (Flyte's derived version covers code, envs and images).
    label = version or f"rsz-{_digest([(n, t, d, m) for _e, n, t, d, m in plans])}"
    swapped = []
    try:
        for env, name, t, d, merged in plans:
            env._tasks[name] = t._replace(resources=merged)  # private registry (flyte.md §11)
            swapped.append((env, name, t))
        result = flyte.deploy(*envs, version=version, dry_run=dry_run, **kw)
    finally:
        for env, name, t in swapped:
            env._tasks[name] = t
    if not dry_run:
        try:
            store = _store()
            from rightsizer_runtime.record import split_key, tenant_path

            versions = _deployed_versions(result)
            for _env, name, t, d, merged in plans:
                version = versions.get(name) or label
                platform, tenant, _ = split_key(d.key)
                rec = {"v": 1, "task": name, "version": version, "decision_id": d.id, "key": d.key,
                       "origin_prior": to_spec(t.resources).to_dict(), "deployed": to_spec(merged).to_dict(),
                       "at": format_timestamp(utcnow()), "reductions": any(
                           (to_spec(merged).requests.memory or 0) < (to_spec(t.resources).requests.memory or 0) for _ in [0])}
                store.put(_deploy_record_path(platform, tenant, name, version), json.dumps(rec).encode())
        except Exception as e:  # the deploy happened; attribution falls back to the deployed value
            log.warning("rightsizer: deploy record not written: %s", e)
    return result


def _path_segment(s: Any) -> str:
    """One store path segment: a version or task name may not climb out of the prefix."""
    out = "".join(ch if (ch.isalnum() or ch in "._-") else "-" for ch in str(s))
    return out if out.strip(".") else "-"


def _deploy_record_path(platform: str, tenant: str, task: Any, version: Any) -> str:
    from rightsizer_runtime.record import tenant_path

    return f"{DEPLOYS}/{tenant_path(platform, tenant)}/{_path_segment(task)}/{_path_segment(version)}.json"


def _deployed_versions(result: Any) -> Dict[str, str]:
    """``{task name: version}`` from what ``flyte.deploy`` returned (``DeployedTask`` specs)."""
    out: Dict[str, str] = {}
    seen: set = set()

    def walk(o: Any, depth: int = 0) -> None:
        if o is None or depth > 6 or id(o) in seen or isinstance(o, (str, bytes, int, float)):
            return
        seen.add(id(o))
        spec = getattr(o, "deployed_task", None)
        tid = getattr(getattr(spec, "task_template", None), "id", None)
        if tid is not None and getattr(tid, "name", None) and getattr(tid, "version", None):
            out[str(tid.name)] = str(tid.version)
            return
        if isinstance(o, dict):
            items: Any = o.values()
        elif isinstance(o, (list, tuple, set)):
            items = o
        else:
            items = [getattr(o, a, None) for a in ("envs", "deployed_entities")]
        for x in items:
            walk(x, depth + 1)

    try:
        walk(result)
    except Exception as e:
        log.debug("rightsizer: deployed versions: %s", e)
    return out


def deploy_records(store: Any = None) -> List[Dict[str, Any]]:
    store = store or _store()
    out = []
    for p, _ in store.list(DEPLOYS):
        if p.endswith(".json"):
            raw = store.get(p)
            if raw:
                try:
                    out.append(json.loads(raw))
                except Exception:
                    pass
    return sorted(out, key=lambda r: r.get("at", ""))


# ── reconcile ────────────────────────────────────────────────────────────


def reconcile_once(envs: List[Any], *, holder: str, store: Any = None, dry_run: bool = False) -> Dict[str, Any]:
    """One reconcile round; returns what it saw and did."""
    store = store or _store()
    if not acquire_lease(store, holder):
        return {"lease": False}
    eng = _eng.get_engine()
    ceiling = None
    try:
        c = eng.control() or {}
        ceiling = (c.get("mode_ceiling") or c.get("mode") or "enforce") if isinstance(c, dict) else "enforce"
    except Exception:
        ceiling = "unknown"
    open_keys = set()
    try:
        for b in eng.breaker_status() or []:
            if b.get("open") or b.get("state") in ("open", "half_open"):
                scope = b.get("scope") or {}
                k = scope.get("key") if isinstance(scope, dict) else None
                open_keys.add(k or "*")
    except Exception:
        open_keys.add("*unknown*")
    latest: Dict[str, Dict[str, Any]] = {}
    for r in deploy_records(store):
        latest[r["task"]] = r
    demoted = ceiling not in ("enforce",)
    affected = [r for r in latest.values() if demoted or "*" in open_keys or r.get("key") in open_keys]
    restored = []
    if affected:
        names = {r["task"] for r in affected}
        todo = [e for e in envs if any(n in getattr(e, "tasks", {}) for n in names)]
        if todo and not dry_run:
            deploy(*todo, restore=True)
            restored = sorted(names)
            for r in affected:
                r2 = dict(r, restored_at=format_timestamp(utcnow()))
                from rightsizer_runtime.record import split_key, tenant_path

                platform, tenant, _ = split_key(r["key"])
                store.put(_deploy_record_path(platform, tenant, r["task"], r["version"]), json.dumps(r2).encode())
    return {"lease": True, "mode_ceiling": ceiling, "open_breakers": sorted(k for k in open_keys if k), "restored": restored}


def reconcile(envs: List[Any], *, every_s: float = 300.0, rounds: Optional[int] = None, holder: Optional[str] = None,
              dry_run: bool = False) -> None:
    """Run reconcile rounds every ``every_s`` (the lease TTL covers three missed rounds)."""
    holder = holder or f"{socket.gethostname()}:{os.getpid()}:{new_ulid()}"
    store = _store()
    n = 0
    try:
        while rounds is None or n < rounds:
            out = reconcile_once(envs, holder=holder, store=store, dry_run=dry_run)
            log.info("rightsizer reconcile: %s", out)
            print(json.dumps(out), flush=True)
            n += 1
            if rounds is not None and n >= rounds:
                break
            time.sleep(every_s)
    finally:
        try:
            release_lease(store, holder)
        except Exception:
            pass


__all__ = ["acquire_lease", "deploy", "deploy_decision", "deploy_records", "lease_state", "plan_deploy",
           "reconcile", "reconcile_active", "reconcile_once", "release_lease"]
