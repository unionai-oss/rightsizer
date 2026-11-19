"""Environment-variable contracts.

- ``RIGHTSIZER_DECISION`` — set by the deciding layer on a sized unit (every
  mode except ``off``): compact JSON
  ``{"v":1,"decision_id":…,"key":…,"mode":…,"requested":{…},"measured":bool}``.
  Also the double-sizing marker (plugins.md §2): any layer that sees it skips.
- ``RIGHTSIZER_ATTEMPT`` — :class:`~.wire.AttemptContext` JSON, mirrored from
  the ``attempt_context`` contextvar so submitting subprocesses see it.
- ``RIGHTSIZER_DECIDED`` — a decision id: an upstream integration already
  decided this unit; downstream hooks add only the marker.
- ``RIGHTSIZER__SECTION__KEY`` — config overrides, parsed into a nested dict.

Readers never raise: a malformed value is logged and read as ``None``.
"""

from __future__ import annotations

import contextvars
import json
import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, Mapping, MutableMapping, Optional

from .wire import (
    WIRE_VERSION,
    AttemptContext,
    Mode,
    ResourceSpec,
    WireError,
    WireModel,
    _bool,
    _check_keys,
    _check_version,
    _obj,
    parse_ulid,
    validate_workload_key,
)

log = logging.getLogger("rightsizer.runtime.env")

DECISION_ENV = "RIGHTSIZER_DECISION"
ATTEMPT_ENV = "RIGHTSIZER_ATTEMPT"
DECIDED_ENV = "RIGHTSIZER_DECIDED"
CONFIG_PREFIX = "RIGHTSIZER__"


@dataclass(frozen=True)
class DecisionEnv(WireModel):
    decision_id: str
    key: str
    mode: Mode
    requested: Optional[ResourceSpec] = None
    measured: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "decision_id", parse_ulid(self.decision_id, "DecisionEnv.decision_id"))
        validate_workload_key(self.key, "DecisionEnv.key")
        object.__setattr__(self, "mode", Mode.parse(self.mode, "DecisionEnv.mode"))
        if self.requested is not None and not isinstance(self.requested, ResourceSpec):
            raise WireError("DecisionEnv.requested: expected ResourceSpec")
        _bool(self.measured, "DecisionEnv.measured")

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"v": WIRE_VERSION, "decision_id": self.decision_id, "key": self.key, "mode": self.mode.value}
        if self.requested is not None:
            out["requested"] = self.requested.to_dict()
        out["measured"] = self.measured
        return out

    @classmethod
    def from_dict(cls, d: Any, *, allow_unknown: bool = False, where: str = "DecisionEnv") -> "DecisionEnv":
        d = _obj(d, where)
        _check_keys(d, frozenset({"v", "decision_id", "key", "mode", "requested", "measured"}), where, allow_unknown)
        _check_version(d, where)
        for k in ("decision_id", "key", "mode"):
            if k not in d:
                raise WireError(f"{where}: missing {k!r}")
        req = d.get("requested")
        return cls(
            decision_id=d["decision_id"],
            key=d["key"],
            mode=Mode.parse(d["mode"], f"{where}.mode"),
            requested=None if req is None else ResourceSpec.from_dict(req, allow_unknown=allow_unknown, where=f"{where}.requested"),
            measured=_bool(d.get("measured", False), f"{where}.measured"),
        )


def _environ(environ: Optional[Mapping[str, str]]) -> Mapping[str, str]:
    return os.environ if environ is None else environ


# ---- RIGHTSIZER_DECISION


def read_decision(environ: Optional[Mapping[str, str]] = None) -> Optional[DecisionEnv]:
    raw = _environ(environ).get(DECISION_ENV)
    if not raw:
        return None
    try:
        return DecisionEnv.from_json(raw, allow_unknown=True)
    except Exception as e:
        log.warning("rightsizer: ignoring malformed %s: %s", DECISION_ENV, e)
        return None


def decision_env_vars(decision: DecisionEnv) -> Dict[str, str]:
    """``{"RIGHTSIZER_DECISION": <compact json>}`` to merge into a unit's env."""
    return {DECISION_ENV: decision.to_json()}


def has_decision_marker(environ: Optional[Mapping[str, str]] = None) -> bool:
    """True if a layer already sized this unit (double-sizing rule)."""
    return bool(_environ(environ).get(DECISION_ENV))


# ---- RIGHTSIZER_ATTEMPT

attempt_context: contextvars.ContextVar[Optional[AttemptContext]] = contextvars.ContextVar("rightsizer_attempt_context", default=None)


def read_attempt(environ: Optional[Mapping[str, str]] = None) -> Optional[AttemptContext]:
    raw = _environ(environ).get(ATTEMPT_ENV)
    if not raw:
        return None
    try:
        return AttemptContext.from_json(raw, allow_unknown=True)
    except Exception as e:
        log.warning("rightsizer: ignoring malformed %s: %s", ATTEMPT_ENV, e)
        return None


def set_attempt_context(ctx: Optional[AttemptContext], environ: Optional[MutableMapping[str, str]] = None) -> contextvars.Token:
    """Set the contextvar and mirror it into the environment (``None`` clears both)."""
    env = os.environ if environ is None else environ
    if ctx is None:
        env.pop(ATTEMPT_ENV, None)
    else:
        env[ATTEMPT_ENV] = ctx.to_json()
    return attempt_context.set(ctx)


def current_attempt(environ: Optional[Mapping[str, str]] = None) -> Optional[AttemptContext]:
    """The contextvar if set, else ``RIGHTSIZER_ATTEMPT``."""
    ctx = attempt_context.get()
    return ctx if ctx is not None else read_attempt(environ)


# ---- RIGHTSIZER_DECIDED


def read_decided(environ: Optional[Mapping[str, str]] = None) -> Optional[str]:
    raw = _environ(environ).get(DECIDED_ENV)
    if not raw:
        return None
    try:
        return parse_ulid(raw.strip(), DECIDED_ENV)
    except WireError as e:
        log.warning("rightsizer: ignoring malformed %s: %s", DECIDED_ENV, e)
        return None


def mark_decided(decision_id: str, environ: Optional[MutableMapping[str, str]] = None) -> None:
    env = os.environ if environ is None else environ
    env[DECIDED_ENV] = parse_ulid(decision_id, DECIDED_ENV)


# ---- RIGHTSIZER__SECTION__KEY

_INT_RE = re.compile(r"^[+-]?\d+$")
_FLOAT_RE = re.compile(r"^[+-]?(\d+\.\d*|\.\d+|\d+)([eE][+-]?\d+)?$")


def parse_config_value(raw: str) -> Any:
    """TOML-like scalar parsing: bools, ints, floats, JSON arrays/objects/strings; else the raw string."""
    s = raw.strip()
    low = s.lower()
    if low in ("true", "false"):
        return low == "true"
    if _INT_RE.match(s):
        try:
            return int(s)
        except ValueError:  # pragma: no cover
            return raw
    if _FLOAT_RE.match(s):
        try:
            return float(s)
        except ValueError:  # pragma: no cover
            return raw
    if s[:1] in ("[", "{", '"'):
        try:
            return json.loads(s)
        except ValueError:
            return raw
    return raw


def parse_config_env(environ: Optional[Mapping[str, str]] = None, *, parse_values: bool = True) -> Dict[str, Any]:
    """``RIGHTSIZER__HISTORY__URI=s3://b/p`` → ``{"history": {"uri": "s3://b/p"}}``.

    Names are lower-cased; nesting is unlimited. Variables are applied in
    sorted order; a conflict between a table and a scalar at the same path is
    logged and the later (sorted) variable is skipped.
    """
    env = _environ(environ)
    out: Dict[str, Any] = {}
    for name in sorted(env):
        if not name.startswith(CONFIG_PREFIX):
            continue
        parts = [p.lower() for p in name[len(CONFIG_PREFIX) :].split("__")]
        if not parts or any(not p for p in parts):
            log.warning("rightsizer: ignoring malformed config variable %s", name)
            continue
        value: Any = parse_config_value(env[name]) if parse_values else env[name]
        node = out
        ok = True
        for p in parts[:-1]:
            nxt = node.setdefault(p, {})
            if not isinstance(nxt, dict):
                ok = False
                break
            node = nxt
        if not ok or isinstance(node.get(parts[-1]), dict):
            log.warning("rightsizer: config variable %s conflicts with another; ignored", name)
            continue
        node[parts[-1]] = value
    return out


def config_env_vars(config: Mapping[str, Any], prefix: str = CONFIG_PREFIX) -> Dict[str, str]:
    """Inverse of :func:`parse_config_env` (for injecting config into a child's env)."""
    out: Dict[str, str] = {}

    def walk(node: Mapping[str, Any], path: list) -> None:
        for k, v in node.items():
            if isinstance(v, Mapping):
                walk(v, path + [k])
            else:
                name = prefix + "__".join(p.upper() for p in path + [k])
                if isinstance(v, bool):
                    out[name] = "true" if v else "false"
                elif isinstance(v, (int, float, str)):
                    out[name] = str(v)
                else:
                    out[name] = json.dumps(v, separators=(",", ":"))

    walk(config, [])
    return out


__all__ = [
    "ATTEMPT_ENV",
    "CONFIG_PREFIX",
    "DECIDED_ENV",
    "DECISION_ENV",
    "DecisionEnv",
    "attempt_context",
    "config_env_vars",
    "current_attempt",
    "decision_env_vars",
    "has_decision_marker",
    "mark_decided",
    "parse_config_env",
    "parse_config_value",
    "read_attempt",
    "read_decided",
    "read_decision",
    "set_attempt_context",
]
