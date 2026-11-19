"""The per-process decision engine for Flyte parents (flyte.md §4.1, §7).

One engine per process (rebuilt after ``fork``), built at the first
decision. Nothing lives on tasks: they stay plain, picklable data.

- **Store**: ``RIGHTSIZER__HISTORY__URI``, else ``{scheme}://{bucket}/rightsizer``
  from the action's raw data path (flyte.md §8.1).
- **Credentials**: Flyte's own object-store settings (``FLYTE_AWS_*`` in task
  pods) are exported as the ``AWS_*`` variables Rust's object store reads,
  unless they are already set (SPEC §10.3).
- **Failures**: a missing native module or a config error is remembered;
  every decision then keeps the prior (S1).
"""

from __future__ import annotations

import logging
import os
import threading
from typing import Any, Dict, Optional

log = logging.getLogger("rightsizer.flyte")

_lock = threading.Lock()
_state: Dict[str, Any] = {"pid": None, "engine": None, "error": None, "store": None}

# Counters for the S4 "no platform state" clamp and budget expiries seen in Python.
counters: Dict[str, int] = {"clamped_no_platform_state": 0, "budget_expired": 0, "fallbacks": 0, "decisions": 0}


def export_flyte_storage_env(environ: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """``FLYTE_AWS_*`` → ``AWS_*`` (only unset variables). Returns what was set."""
    env = os.environ if environ is None else environ
    out: Dict[str, str] = {}
    pairs = (
        ("FLYTE_AWS_ENDPOINT", "AWS_ENDPOINT"),
        ("FLYTE_AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID"),
        ("FLYTE_AWS_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY"),
    )
    for src, dst in pairs:
        v = env.get(src)
        if v and not env.get(dst):
            env[dst] = v
            out[dst] = v
    ep = env.get("AWS_ENDPOINT", "")
    if ep:
        if ep.startswith("http://") and not env.get("AWS_ALLOW_HTTP"):
            env["AWS_ALLOW_HTTP"] = "true"
            out["AWS_ALLOW_HTTP"] = "true"
        if not env.get("AWS_REGION") and not env.get("AWS_DEFAULT_REGION"):
            env["AWS_REGION"] = "us-east-1"
            out["AWS_REGION"] = "us-east-1"
    return out


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

            export_flyte_storage_env()
            store = None if os.environ.get("RIGHTSIZER__HISTORY__URI") else resolve_store(_raw_data_path())
            st["store"] = store or os.environ.get("RIGHTSIZER__HISTORY__URI")
            st["engine"] = Engine(None, store=store)
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


__all__ = ["counters", "export_flyte_storage_env", "flyte_config", "get_engine", "reset_for_tests", "resolve_store"]
