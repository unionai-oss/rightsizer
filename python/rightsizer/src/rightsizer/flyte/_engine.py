"""The per-process decision engine for Flyte parents (flyte.md §4.1, §7).

One engine per process (rebuilt after ``fork``), built at the first
decision. Nothing lives on tasks: they stay plain, picklable data.

- **Store**: ``RIGHTSIZER__HISTORY__URI``, else ``{scheme}://{bucket}/rightsizer``
  from the action's raw data path (flyte.md §8.1).
- **Credentials**: Flyte's own object-store settings (``FLYTE_AWS_*`` in task
  pods) are passed to the engine as explicit store options
  (``storage_options``), never exported to ``os.environ`` — the user's own
  boto3/s3fs clients in this process keep their identity (SPEC §10.3).
  ``AWS_*`` variables already set take precedence.
- **Failures**: a missing native module or a config error is remembered;
  every decision then keeps the prior (S1).
"""

from __future__ import annotations

import collections
import logging
import os
import threading
from typing import Any, Dict, Mapping, Optional

log = logging.getLogger("rightsizer.flyte")

_lock = threading.Lock()
_state: Dict[str, Any] = {"pid": None, "engine": None, "error": None, "store": None}

# Per-process counters (decisions made here, S1 fallbacks, Python-side deadline expiries, decisions
# under the S4 "no platform state" clamp, decisions whose engine source was a prior with reason budget)
# and the latest decision latencies (ms), for tests and logs.
counters: Dict[str, int] = {"clamped_no_platform_state": 0, "budget_expired": 0, "fallbacks": 0, "decisions": 0,
                            "prior_budget": 0, "enforced_changes": 0}
latencies_ms: "collections.deque[float]" = collections.deque(maxlen=100_000)


def flyte_storage_options(environ: Optional[Mapping[str, str]] = None) -> Dict[str, str]:
    """Flyte's object-store settings (``FLYTE_AWS_*``) as store options for
    the engine and the record writer. Reads the environment; writes nothing."""
    from rightsizer_runtime.storage import flyte_storage_options as _opts

    return _opts(environ)


def resolve_store(raw_data_path: Optional[str] = None) -> Optional[str]:
    from rightsizer_runtime.flyte_child import store_uri

    return store_uri(raw_data_path)


def _raw_data_path() -> Optional[str]:
    try:
        import flyte

        c = flyte.ctx()
        return c.raw_data_path.path if c and c.raw_data_path is not None else None
    except Exception:
        return None


def importer_engine() -> Any:
    """A fresh engine for backfills (``rsz history import flyte``): operator
    role, so records read from ``flyte.remote`` — platform accounting with the
    platform's own tenant — are stored as ``verified`` (SPEC §8.2) instead of
    being capped at the client ceiling (``authenticated``)."""
    from .._engine import Engine

    store = None if os.environ.get("RIGHTSIZER__HISTORY__URI") else resolve_store(_raw_data_path())
    return Engine(None, store=store, role="operator", storage_options=flyte_storage_options() or None)


def get_engine() -> Any:
    """The process's engine (raises the remembered error; callers fall back to the prior)."""
    pid = os.getpid()
    st = _state
    if st["pid"] == pid:
        if st["engine"] is not None:
            return st["engine"]
        if st["error"] is not None:
            raise st["error"]
    with _lock:
        if st["pid"] == pid and st["engine"] is not None:
            return st["engine"]
        if st["pid"] == pid and st["error"] is not None:
            raise st["error"]
        st.update(pid=pid, engine=None, error=None)
        try:
            from .._engine import Engine

            store = None if os.environ.get("RIGHTSIZER__HISTORY__URI") else resolve_store(_raw_data_path())
            st["store"] = store or os.environ.get("RIGHTSIZER__HISTORY__URI")
            st["engine"] = Engine(None, store=store, storage_options=flyte_storage_options() or None)
            err = st["engine"].config_error
            if err:
                log.warning("rightsizer: engine configuration error (decisions keep the prior): %s", err)
            return st["engine"]
        except BaseException as e:
            st["error"] = e if isinstance(e, Exception) else RuntimeError(str(e))
            log.error("rightsizer: no decision engine in this process (%s); rightsizable calls run at the prior (S1)", e)
            raise st["error"]


def flyte_config() -> Dict[str, Any]:
    """The engine's ``[flyte]`` section (defaults when there is no engine)."""
    try:
        return dict(get_engine().config.get("flyte") or {})
    except Exception:
        return {"scalar_memory_enforced": _env_bool("RIGHTSIZER__FLYTE__SCALAR_MEMORY_ENFORCED", True),
                "observe_local": _env_bool("RIGHTSIZER__FLYTE__OBSERVE_LOCAL", False), "shapes": []}


def _env_bool(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


def reset_for_tests(engine: Any = None, error: Optional[BaseException] = None) -> None:
    """Install ``engine`` (or an ``error``) as this process's engine; ``reset_for_tests()`` clears it."""
    with _lock:
        if engine is None and error is None:
            _state.update(pid=None, engine=None, error=None, store=None)
        else:
            _state.update(pid=os.getpid(), engine=engine, error=error)
    for k in counters:
        counters[k] = 0
    latencies_ms.clear()


__all__ = ["counters", "latencies_ms", "flyte_storage_options", "flyte_config", "get_engine", "reset_for_tests", "resolve_store"]
