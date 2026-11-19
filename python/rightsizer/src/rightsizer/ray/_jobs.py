"""``rsz ray submit`` / ``rightsizer ray submit`` (ray.md §5).

Submits a Ray job through the Jobs SDK with:

- ``RIGHTSIZER__*`` in ``runtime_env.env_vars``, so the job's ``rs.ray``
  functions (and their workers) find the store and the policy. What is
  propagated: every ``RIGHTSIZER__*`` / ``RIGHTSIZER_TENANT`` variable of
  the submitting environment, plus every configuration value that differs
  from the defaults (so a ``rightsizer.toml`` on the submitting machine
  reaches the cluster), plus the resolved tenant. A local store path is made
  absolute. Variables the user's own ``runtime_env`` sets win.
- the driver sized: ``entrypoint_num_cpus`` / ``_num_gpus`` / ``_memory``
  from a decision for ``ray://{tenant}/driver.{name}`` whose prior is the
  ``--entrypoint-*`` flags (drivers are usually light: without a prior,
  nothing is set).
"""

from __future__ import annotations

import json
import logging
import os
import shlex
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

log = logging.getLogger("rightsizer.ray")

PASS_THROUGH = ("RIGHTSIZER_TENANT",)


def _diff(cur: Any, base: Any) -> Any:
    if isinstance(cur, dict) and isinstance(base, dict):
        out = {}
        for k, v in cur.items():
            if k not in base:
                if v not in (None, [], {}):
                    out[k] = v
                continue
            d = _diff(v, base[k])
            if d is not _SAME:
                out[k] = d
        return out if out else _SAME
    return _SAME if cur == base else cur


_SAME = object()


def _absolute_store(uri: Optional[str]) -> Optional[str]:
    if not uri or "://" in uri:
        return uri
    return os.path.abspath(os.path.expanduser(uri))


def propagated_env(environ: Optional[Mapping[str, str]] = None) -> Dict[str, str]:
    """The ``RIGHTSIZER__*`` variables a submitted job gets (see the module docstring)."""
    from rightsizer_runtime.env import config_env_vars

    from ._settings import settings

    env = os.environ if environ is None else environ
    out = {k: v for k, v in env.items() if k.startswith("RIGHTSIZER__") or k in PASS_THROUGH}
    s = settings()
    if s.engine is not None:
        try:
            from .._engine import Engine

            cur = s.engine.config
            base = Engine({}, env_overrides=False).config
            d = _diff(cur, base)
            if d is not _SAME:
                hist = d.get("history") if isinstance(d, dict) else None
                if isinstance(hist, dict) and hist.get("uri"):
                    hist["uri"] = _absolute_store(hist["uri"])
                for k, v in config_env_vars(d).items():
                    out.setdefault(k, v)
        except Exception as e:
            log.warning("rightsizer.ray: configuration not propagated: %s", e)
    if "RIGHTSIZER__HISTORY__URI" in out:
        out["RIGHTSIZER__HISTORY__URI"] = _absolute_store(out["RIGHTSIZER__HISTORY__URI"]) or ""
    out.setdefault("RIGHTSIZER__RAY__TENANT", s.tenant)
    return out


def driver_name(entrypoint: Sequence[str]) -> str:
    """``train`` for ``python train.py``; ``mymod`` for ``python -m mymod``; else the command."""
    toks = list(entrypoint)
    for i, t in enumerate(toks):
        if t == "-m" and i + 1 < len(toks):
            return toks[i + 1]
        if t.endswith(".py"):
            return os.path.splitext(os.path.basename(t))[0]
    return os.path.basename(toks[0]) if toks else "driver"


def size_driver(entrypoint: Sequence[str], *, key: Optional[str] = None, num_cpus: Optional[float] = None,
                num_gpus: Optional[float] = None, memory: Optional[int] = None, mode: Optional[str] = None) -> Tuple[Dict[str, Any], Any]:
    """``(submit_job entrypoint_* kwargs, decision)``. Never raises."""
    from ._sizing import SizingPolicy, decide_options

    opts: Dict[str, Any] = {}
    if num_cpus is not None:
        opts["num_cpus"] = num_cpus
    if num_gpus is not None:
        opts["num_gpus"] = num_gpus
    if memory is not None:
        opts["memory"] = memory
    pol = SizingPolicy(key=key or f"driver.{driver_name(entrypoint)}", mode=mode)
    sized = decide_options(opts, pol, target=None, code=None, inputs=[], actor=True,
                           labels={"ray.unit": "driver"})
    out: Dict[str, Any] = {}
    names = {"num_cpus": "entrypoint_num_cpus", "num_gpus": "entrypoint_num_gpus", "memory": "entrypoint_memory"}
    for k, name in names.items():
        v = sized.options.get(k)
        if v is not None:
            out[name] = int(v) if k == "memory" else v
    return out, sized.decision


def submit(entrypoint: Sequence[str], *, address: Optional[str] = None, runtime_env: Optional[Mapping[str, Any]] = None,
           submission_id: Optional[str] = None, metadata: Optional[Mapping[str, str]] = None,
           entrypoint_num_cpus: Optional[float] = None, entrypoint_num_gpus: Optional[float] = None,
           entrypoint_memory: Optional[int] = None, key: Optional[str] = None, mode: Optional[str] = None,
           client: Any = None) -> Dict[str, Any]:
    """Submit ``entrypoint`` (argv) as a Ray job with rightsizer's env; returns what was submitted."""
    from ._settings import settings

    rt: Dict[str, Any] = json.loads(json.dumps(dict(runtime_env or {})))
    env_vars = dict(rt.get("env_vars") or {})
    prop = propagated_env()
    for k, v in prop.items():
        env_vars.setdefault(k, v)
    rt["env_vars"] = env_vars
    driver, decision = size_driver(entrypoint, key=key, num_cpus=entrypoint_num_cpus, num_gpus=entrypoint_num_gpus,
                                   memory=entrypoint_memory, mode=mode)
    if client is None:
        from ray.job_submission import JobSubmissionClient

        client = JobSubmissionClient(resolve_address(address))
    cmd = " ".join(shlex.quote(t) for t in entrypoint)
    sid = client.submit_job(entrypoint=cmd, runtime_env=rt, submission_id=submission_id, metadata=dict(metadata or {}) or None,
                            **driver)
    return {"submission_id": sid, "entrypoint": cmd, "env_vars": sorted(prop), "driver": driver,
            "decision": None if decision is None else str(decision), "tenant": settings().tenant}


def resolve_address(address: Optional[str]) -> Optional[str]:
    """``--address``, else ``[ray] address``, else ``RAY_API_SERVER_ADDRESS`` / ``RAY_DASHBOARD_ADDRESS``,
    else an ``http(s)://`` ``RAY_ADDRESS``, else the SDK's default."""
    if address:
        return address
    from ._settings import settings

    s = settings()
    try:
        a = ((s.engine.config if s.engine is not None else {}).get("ray") or {}).get("address")
        if a:
            return a
    except Exception:
        pass
    for var in ("RAY_API_SERVER_ADDRESS", "RAY_DASHBOARD_ADDRESS"):
        if os.environ.get(var):
            return os.environ[var]
    ra = os.environ.get("RAY_ADDRESS", "")
    return ra if ra.startswith(("http://", "https://")) else None


__all__ = ["driver_name", "propagated_env", "resolve_address", "size_driver", "submit"]


def _main_env_listing(env: Mapping[str, str]) -> List[str]:  # pragma: no cover - CLI helper
    return [f"{k}={v}" for k, v in sorted(env.items())]
