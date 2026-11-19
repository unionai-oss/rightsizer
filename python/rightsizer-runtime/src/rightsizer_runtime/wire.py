"""Wire types mirroring the Rust JSON contract (`rightsizer-core`).

Conventions (docs/ARCHITECTURE.md §4, SPEC §5):

- keys are ``snake_case``; every top-level artifact carries ``"v": 1``;
- quantities are integers: bytes (u64) and millicores (u32);
- enums are ``snake_case`` strings; tagged enums are ``{"kind": ...}``;
- timestamps are RFC 3339 UTC; ids are ULIDs (Crockford base32);
- ``None`` fields are omitted on output; ``null`` and a missing key both read
  as ``None``.

``from_dict``/``from_json`` validate strictly and raise :class:`WireError`.
Unknown keys are rejected unless ``allow_unknown=True`` is passed (readers of
newer minor versions may want that).

The Rust structs are the source of truth. Every naming choice made here
before they existed is listed in docs/IMPLEMENTATION-NOTES.md.
"""

from __future__ import annotations

import datetime as _dt
import enum
import json
import math
import os
import re
import threading
import time
from dataclasses import dataclass, field, fields
from typing import Any, ClassVar, Dict, List, Mapping, Optional, Tuple, Type, TypeVar, Union

WIRE_VERSION = 1

U32_MAX = 2**32 - 1
U64_MAX = 2**64 - 1


class WireError(ValueError):
    """A JSON payload does not match the wire contract."""


# --------------------------------------------------------------------------- enums


class WireEnum(str, enum.Enum):
    """A string enum whose wire value is its ``snake_case`` value."""

    @classmethod
    def parse(cls, value: Any, where: str = "") -> "WireEnum":
        if isinstance(value, cls):
            return value
        if not isinstance(value, str):
            raise WireError(f"{where or cls.__name__}: expected string, got {type(value).__name__}")
        try:
            return cls(value)
        except ValueError:
            allowed = ", ".join(m.value for m in cls)
            raise WireError(f"{where or cls.__name__}: {value!r} is not one of {allowed}") from None

    def __str__(self) -> str:  # pragma: no cover - cosmetic
        return self.value


class Mode(WireEnum):
    """``off < observe < recommend < enforce`` (SPEC §4)."""

    OFF = "off"
    OBSERVE = "observe"
    RECOMMEND = "recommend"
    ENFORCE = "enforce"

    @property
    def rank(self) -> int:
        return _MODE_ORDER[self]

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, Mode):
            return NotImplemented
        return self.rank < other.rank

    def __le__(self, other: object) -> bool:
        if not isinstance(other, Mode):
            return NotImplemented
        return self.rank <= other.rank

    def __gt__(self, other: object) -> bool:
        if not isinstance(other, Mode):
            return NotImplemented
        return self.rank > other.rank

    def __ge__(self, other: object) -> bool:
        if not isinstance(other, Mode):
            return NotImplemented
        return self.rank >= other.rank

    # str's __hash__/__eq__ are kept, so Mode("off") == "off".
    __hash__ = str.__hash__


_MODE_ORDER = {Mode.OFF: 0, Mode.OBSERVE: 1, Mode.RECOMMEND: 2, Mode.ENFORCE: 3}


class Censor(WireEnum):
    EXACT = "exact"
    AT_LEAST = "at_least"
    AT_MOST = "at_most"


class RunStatus(WireEnum):
    SUCCEEDED = "succeeded"
    OOM_KILLED = "oom_killed"
    GPU_OOM = "gpu_oom"
    EVICTED = "evicted"
    TIMEOUT = "timeout"
    PREEMPTED = "preempted"
    FAILED = "failed"
    CACHED = "cached"
    UNKNOWN = "unknown"


class Confidence(WireEnum):
    OBSERVED = "observed"
    INFERRED = "inferred"


class Trust(WireEnum):
    VERIFIED = "verified"
    AUTHENTICATED = "authenticated"
    UNVERIFIED = "unverified"


class InputFormat(WireEnum):
    PARQUET = "parquet"
    CSV = "csv"
    JSONL = "jsonl"
    NPY = "npy"
    SAFETENSORS = "safetensors"
    ARROW_IPC = "arrow_ipc"
    DIR = "dir"
    BLOB = "blob"
    SCALAR = "scalar"
    MODEL_REF = "model_ref"


class OutcomeSourceKind(WireEnum):
    SELF_REPORT = "self_report"
    CGROUP = "cgroup"
    PROMETHEUS = "prometheus"
    SACCT = "sacct"
    FLYTE_REMOTE = "flyte_remote"
    RAY_STATE = "ray_state"
    WATCHER = "watcher"
    IMPORTER = "importer"


# SPEC §5.1.1 accelerator catalog.
ACCELERATORS: frozenset = frozenset(
    {
        "T4-16G", "L4-24G", "A10G-24G", "A10-24G", "L40S-48G", "A100-40G", "A100-80G",
        "H100-80G", "H200-141G", "B200-180G", "MI300X-192G", "TPU-V5E", "TPU-V5P",
        "TPU-V6E", "TRN1", "TRN2", "INF2", "unknown-gpu",
    }
)

# --------------------------------------------------------------------------- ULID

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_CROCKFORD_DECODE = {c: i for i, c in enumerate(_CROCKFORD)}
_ULID_RE = re.compile(r"^[0-7][0-9A-HJKMNP-TV-Z]{25}$")


class _UlidGenerator:
    """ULID generator, monotonic within a millisecond (and across clock steps back)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last_ms = -1
        self._last_rand = 0
        self._pid = os.getpid()

    def new_int(self, now_ms: Optional[int] = None) -> int:
        with self._lock:
            if self._pid != os.getpid():  # after fork: do not continue the parent's sequence
                self._pid = os.getpid()
                self._last_ms = -1
            ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
            if ms <= self._last_ms:
                ms = self._last_ms
                rand = self._last_rand + 1
                if rand >= 1 << 80:  # random part exhausted within one ms: borrow the next ms
                    ms += 1
                    rand = int.from_bytes(os.urandom(10), "big") >> 1
            else:
                # Leave headroom so increments within a ms rarely overflow.
                rand = int.from_bytes(os.urandom(10), "big") >> 1
            if ms >= 1 << 48:
                raise OverflowError("ULID timestamp overflow")
            self._last_ms, self._last_rand = ms, rand
            return (ms << 80) | rand


_GEN = _UlidGenerator()


def _encode_ulid(value: int) -> str:
    out = []
    for _ in range(26):
        out.append(_CROCKFORD[value & 31])
        value >>= 5
    return "".join(reversed(out))


def new_ulid(now_ms: Optional[int] = None) -> str:
    """A new ULID string (26 chars, Crockford base32, upper case)."""
    return _encode_ulid(_GEN.new_int(now_ms))


def parse_ulid(value: Any, where: str = "ulid") -> str:
    """Validate a ULID and return its canonical (upper case) form."""
    if not isinstance(value, str):
        raise WireError(f"{where}: expected ULID string, got {type(value).__name__}")
    s = value.upper()
    if not _ULID_RE.match(s):
        raise WireError(f"{where}: {value!r} is not a ULID")
    return s


def ulid_timestamp_ms(value: str) -> int:
    n = 0
    for c in parse_ulid(value):
        n = (n << 5) | _CROCKFORD_DECODE[c]
    return n >> 80


# --------------------------------------------------------------------------- timestamps

_RFC3339_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[Tt](\d{2}):(\d{2}):(\d{2})(\.\d{1,9})?(Z|z|[+-]\d{2}:\d{2})$"
)


def format_timestamp(t: _dt.datetime) -> str:
    """RFC 3339 UTC with microseconds and a ``Z`` suffix."""
    if t.tzinfo is None:
        raise WireError("timestamp must be timezone-aware")
    t = t.astimezone(_dt.timezone.utc)
    return t.strftime("%Y-%m-%dT%H:%M:%S.") + f"{t.microsecond:06d}Z"


def parse_timestamp(value: Any, where: str = "timestamp") -> _dt.datetime:
    if isinstance(value, _dt.datetime):
        if value.tzinfo is None:
            raise WireError(f"{where}: naive datetime")
        return value.astimezone(_dt.timezone.utc)
    if not isinstance(value, str):
        raise WireError(f"{where}: expected RFC 3339 string")
    m = _RFC3339_RE.match(value)
    if not m:
        raise WireError(f"{where}: {value!r} is not RFC 3339")
    y, mo, d, h, mi, s, frac, tz = m.groups()
    us = 0
    if frac:
        us = int((frac[1:] + "000000")[:6])
    if tz in ("Z", "z"):
        tzinfo = _dt.timezone.utc
    else:
        sign = 1 if tz[0] == "+" else -1
        tzinfo = _dt.timezone(sign * _dt.timedelta(hours=int(tz[1:3]), minutes=int(tz[4:6])))
    try:
        t = _dt.datetime(int(y), int(mo), int(d), int(h), int(mi), int(s), us, tzinfo=tzinfo)
    except ValueError as e:
        raise WireError(f"{where}: {e}") from None
    return t.astimezone(_dt.timezone.utc)


def utcnow() -> _dt.datetime:
    return _dt.datetime.now(_dt.timezone.utc)


# --------------------------------------------------------------------------- validators


def _int(value: Any, where: str, lo: int = 0, hi: int = U64_MAX) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise WireError(f"{where}: expected integer, got {type(value).__name__}")
    if not lo <= value <= hi:
        raise WireError(f"{where}: {value} out of range [{lo}, {hi}]")
    return value


def _u32(value: Any, where: str) -> int:
    return _int(value, where, 0, U32_MAX)


def _u64(value: Any, where: str) -> int:
    return _int(value, where, 0, U64_MAX)


def _float(value: Any, where: str, lo: Optional[float] = None, hi: Optional[float] = None) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise WireError(f"{where}: expected number, got {type(value).__name__}")
    v = float(value)
    if not math.isfinite(v):
        raise WireError(f"{where}: not finite")
    if lo is not None and v < lo:
        raise WireError(f"{where}: {v} < {lo}")
    if hi is not None and v > hi:
        raise WireError(f"{where}: {v} > {hi}")
    return v


def _str(value: Any, where: str, nonempty: bool = False) -> str:
    if not isinstance(value, str):
        raise WireError(f"{where}: expected string, got {type(value).__name__}")
    if nonempty and not value:
        raise WireError(f"{where}: empty string")
    return value


def _bool(value: Any, where: str) -> bool:
    if not isinstance(value, bool):
        raise WireError(f"{where}: expected bool, got {type(value).__name__}")
    return value


def _obj(value: Any, where: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise WireError(f"{where}: expected object, got {type(value).__name__}")
    for k in value:
        if not isinstance(k, str):
            raise WireError(f"{where}: non-string key {k!r}")
    return value


def _check_keys(d: Mapping[str, Any], allowed: frozenset, where: str, allow_unknown: bool) -> None:
    if allow_unknown:
        return
    extra = set(d) - allowed
    if extra:
        raise WireError(f"{where}: unknown field(s) {sorted(extra)}")


def _check_version(d: Mapping[str, Any], where: str, required: bool = True) -> None:
    if "v" not in d:
        if required:
            raise WireError(f"{where}: missing 'v'")
        return  # Rust: #[serde(default = "wire_v")]
    v = _int(d["v"], f"{where}.v", 0, 65535)
    if v != WIRE_VERSION:
        raise WireError(f"{where}: unsupported version v={v} (supported: {WIRE_VERSION})")


def _opt(d: Mapping[str, Any], k: str) -> Any:
    v = d.get(k)
    return v


T = TypeVar("T", bound="WireModel")


class WireModel:
    """Mixin: JSON helpers on top of ``to_dict``/``from_dict``."""

    _where: ClassVar[str] = ""

    def to_dict(self) -> Dict[str, Any]:  # pragma: no cover - overridden
        raise NotImplementedError

    @classmethod
    def from_dict(cls: Type[T], d: Any, *, allow_unknown: bool = False) -> T:  # pragma: no cover
        raise NotImplementedError

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"), sort_keys=False, allow_nan=False)

    @classmethod
    def from_json(cls: Type[T], s: Union[str, bytes], *, allow_unknown: bool = False) -> T:
        try:
            d = json.loads(s)
        except (TypeError, ValueError) as e:
            raise WireError(f"{cls.__name__}: invalid JSON: {e}") from None
        return cls.from_dict(d, allow_unknown=allow_unknown)


def _put(out: Dict[str, Any], k: str, v: Any) -> None:
    if v is not None:
        out[k] = v


# --------------------------------------------------------------------------- resources


@dataclass(frozen=True)
class GpuShape(WireModel):
    accel: str
    count: int

    def __post_init__(self) -> None:
        if self.accel not in ACCELERATORS:
            raise WireError(f"GpuShape.accel: {self.accel!r} is not a catalog id")
        _u32(self.count, "GpuShape.count")

    def to_dict(self) -> Dict[str, Any]:
        return {"accel": self.accel, "count": self.count}

    @classmethod
    def from_dict(cls, d: Any, *, allow_unknown: bool = False, where: str = "GpuShape") -> "GpuShape":
        d = _obj(d, where)
        _check_keys(d, frozenset({"accel", "count"}), where, allow_unknown)
        for k in ("accel", "count"):
            if k not in d:
                raise WireError(f"{where}: missing {k!r}")
        return cls(accel=_str(d["accel"], f"{where}.accel"), count=_u32(d["count"], f"{where}.count"))


@dataclass(frozen=True)
class Requests(WireModel):
    cpu: Optional[int] = None  # millicores (u32)
    memory: Optional[int] = None  # bytes (u64)
    gpu: Optional[GpuShape] = None
    ephemeral_storage: Optional[int] = None  # bytes
    shm: Optional[int] = None  # bytes

    _KEYS: ClassVar[frozenset] = frozenset({"cpu", "memory", "gpu", "ephemeral_storage", "shm"})

    def __post_init__(self) -> None:
        if self.cpu is not None:
            _u32(self.cpu, "Requests.cpu")
        for k in ("memory", "ephemeral_storage", "shm"):
            v = getattr(self, k)
            if v is not None:
                _u64(v, f"Requests.{k}")
        if self.gpu is not None and not isinstance(self.gpu, GpuShape):
            raise WireError("Requests.gpu: expected GpuShape")

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        _put(out, "cpu", self.cpu)
        _put(out, "memory", self.memory)
        if self.gpu is not None:
            out["gpu"] = self.gpu.to_dict()
        _put(out, "ephemeral_storage", self.ephemeral_storage)
        _put(out, "shm", self.shm)
        return out

    @classmethod
    def from_dict(cls, d: Any, *, allow_unknown: bool = False, where: str = "Requests") -> "Requests":
        d = _obj(d, where)
        _check_keys(d, cls._KEYS, where, allow_unknown)
        gpu = d.get("gpu")
        return cls(
            cpu=None if d.get("cpu") is None else _u32(d["cpu"], f"{where}.cpu"),
            memory=None if d.get("memory") is None else _u64(d["memory"], f"{where}.memory"),
            gpu=None if gpu is None else GpuShape.from_dict(gpu, allow_unknown=allow_unknown, where=f"{where}.gpu"),
            ephemeral_storage=None
            if d.get("ephemeral_storage") is None
            else _u64(d["ephemeral_storage"], f"{where}.ephemeral_storage"),
            shm=None if d.get("shm") is None else _u64(d["shm"], f"{where}.shm"),
        )


@dataclass(frozen=True)
class Limits(WireModel):
    cpu: Optional[int] = None  # millicores
    memory: Optional[int] = None  # bytes
    ephemeral_storage: Optional[int] = None  # bytes

    _KEYS: ClassVar[frozenset] = frozenset({"cpu", "memory", "ephemeral_storage"})

    def __post_init__(self) -> None:
        if self.cpu is not None:
            _u32(self.cpu, "Limits.cpu")
        for k in ("memory", "ephemeral_storage"):
            v = getattr(self, k)
            if v is not None:
                _u64(v, f"Limits.{k}")

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        _put(out, "cpu", self.cpu)
        _put(out, "memory", self.memory)
        _put(out, "ephemeral_storage", self.ephemeral_storage)
        return out

    @classmethod
    def from_dict(cls, d: Any, *, allow_unknown: bool = False, where: str = "Limits") -> "Limits":
        d = _obj(d, where)
        _check_keys(d, cls._KEYS, where, allow_unknown)
        return cls(
            cpu=None if d.get("cpu") is None else _u32(d["cpu"], f"{where}.cpu"),
            memory=None if d.get("memory") is None else _u64(d["memory"], f"{where}.memory"),
            ephemeral_storage=None
            if d.get("ephemeral_storage") is None
            else _u64(d["ephemeral_storage"], f"{where}.ephemeral_storage"),
        )


@dataclass(frozen=True)
class ResourceSpec(WireModel):
    requests: Requests = field(default_factory=Requests)
    limits: Limits = field(default_factory=Limits)

    def to_dict(self) -> Dict[str, Any]:
        # Both members are always present (Rust: plain structs, not Options).
        return {"requests": self.requests.to_dict(), "limits": self.limits.to_dict()}

    @classmethod
    def from_dict(cls, d: Any, *, allow_unknown: bool = False, where: str = "ResourceSpec") -> "ResourceSpec":
        d = _obj(d, where)
        _check_keys(d, frozenset({"requests", "limits"}), where, allow_unknown)
        req = d.get("requests")
        lim = d.get("limits")
        return cls(
            requests=Requests() if req is None else Requests.from_dict(req, allow_unknown=allow_unknown, where=f"{where}.requests"),
            limits=Limits() if lim is None else Limits.from_dict(lim, allow_unknown=allow_unknown, where=f"{where}.limits"),
        )


# --------------------------------------------------------------------------- measurement


@dataclass(frozen=True)
class Measurement(WireModel):
    value: float
    censor: Censor = Censor.EXACT
    resolution_s: Optional[float] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "value", _float(self.value, "Measurement.value", lo=0.0))
        object.__setattr__(self, "censor", Censor.parse(self.censor, "Measurement.censor"))
        if self.resolution_s is not None:
            object.__setattr__(self, "resolution_s", _float(self.resolution_s, "Measurement.resolution_s", lo=0.0))

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"value": self.value, "censor": self.censor.value}
        _put(out, "resolution_s", self.resolution_s)
        return out

    @classmethod
    def from_dict(cls, d: Any, *, allow_unknown: bool = False, where: str = "Measurement") -> "Measurement":
        d = _obj(d, where)
        _check_keys(d, frozenset({"value", "censor", "resolution_s"}), where, allow_unknown)
        if "value" not in d:
            raise WireError(f"{where}: missing 'value'")
        censor = d.get("censor")  # Rust: #[serde(default)] → exact
        return cls(
            value=_float(d["value"], f"{where}.value", lo=0.0),
            censor=Censor.EXACT if censor is None else Censor.parse(censor, f"{where}.censor"),
            resolution_s=None if d.get("resolution_s") is None else _float(d["resolution_s"], f"{where}.resolution_s", lo=0.0),
        )

    def with_censor(self, censor: Censor) -> "Measurement":
        return Measurement(self.value, censor, self.resolution_s)


@dataclass(frozen=True)
class OutcomeSource(WireModel):
    """Tagged enum ``{"kind": "self_report"}`` / ``{"kind": "importer", "platform": "flyte"}``."""

    kind: OutcomeSourceKind
    platform: Optional[str] = None  # only for kind == importer

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", OutcomeSourceKind.parse(self.kind, "OutcomeSource.kind"))
        if self.kind is OutcomeSourceKind.IMPORTER:
            _str(self.platform, "OutcomeSource.platform", nonempty=True)
        elif self.platform is not None:
            raise WireError(f"OutcomeSource: 'platform' only valid for kind=importer, not {self.kind.value}")

    @classmethod
    def self_report(cls) -> "OutcomeSource":
        return cls(OutcomeSourceKind.SELF_REPORT)

    @classmethod
    def cgroup(cls) -> "OutcomeSource":
        return cls(OutcomeSourceKind.CGROUP)

    @classmethod
    def importer(cls, platform: str) -> "OutcomeSource":
        return cls(OutcomeSourceKind.IMPORTER, platform)

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"kind": self.kind.value}
        _put(out, "platform", self.platform)
        return out

    @classmethod
    def from_dict(cls, d: Any, *, allow_unknown: bool = False, where: str = "OutcomeSource") -> "OutcomeSource":
        d = _obj(d, where)
        _check_keys(d, frozenset({"kind", "platform"}), where, allow_unknown)
        if "kind" not in d:
            raise WireError(f"{where}: missing 'kind'")
        plat = d.get("platform")
        return cls(
            kind=OutcomeSourceKind.parse(d["kind"], f"{where}.kind"),
            platform=None if plat is None else _str(plat, f"{where}.platform"),
        )


# --------------------------------------------------------------------------- inputs

@dataclass(frozen=True)
class ColumnsInfo(WireModel):
    """Column counts by kind; names only if privacy allows (Rust ``ColumnsInfo``)."""

    total: int
    numeric: int = 0
    string: int = 0
    nested: int = 0
    other: int = 0
    names: Optional[Tuple[str, ...]] = None

    _KEYS: ClassVar[frozenset] = frozenset({"total", "numeric", "string", "nested", "other", "names"})

    def __post_init__(self) -> None:
        for k in ("total", "numeric", "string", "nested", "other"):
            _u32(getattr(self, k), f"ColumnsInfo.{k}")
        if self.names is not None:
            object.__setattr__(self, "names", tuple(_str(n, "ColumnsInfo.names[]") for n in self.names))

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "total": self.total, "numeric": self.numeric, "string": self.string,
            "nested": self.nested, "other": self.other,
        }
        if self.names is not None:
            out["names"] = list(self.names)
        return out

    @classmethod
    def from_dict(cls, d: Any, *, allow_unknown: bool = False, where: str = "ColumnsInfo") -> "ColumnsInfo":
        d = _obj(d, where)
        _check_keys(d, cls._KEYS, where, allow_unknown)
        if "total" not in d:
            raise WireError(f"{where}: missing 'total'")
        names = d.get("names")
        if names is not None and not isinstance(names, list):
            raise WireError(f"{where}.names: expected array")
        return cls(
            total=_u32(d["total"], f"{where}.total"),
            **{k: _u32(d.get(k, 0), f"{where}.{k}") for k in ("numeric", "string", "nested", "other")},
            names=None if names is None else tuple(_str(n, f"{where}.names[]") for n in names),
        )


_SCALAR_KINDS = ("int", "float", "bool", "str")
I64_MIN, I64_MAX = -(2**63), 2**63 - 1


@dataclass(frozen=True)
class ScalarValue(WireModel):
    """Adjacently tagged scalar: ``{"kind": "int", "value": 3}`` (Rust ``ScalarValue``)."""

    kind: str
    value: Union[bool, int, float, str]

    def __post_init__(self) -> None:
        k, v = self.kind, self.value
        if k == "int":
            _int(v, "ScalarValue.value", I64_MIN, I64_MAX)
        elif k == "float":
            object.__setattr__(self, "value", _float(v, "ScalarValue.value"))
        elif k == "bool":
            _bool(v, "ScalarValue.value")
        elif k == "str":
            _str(v, "ScalarValue.value")
        else:
            raise WireError(f"ScalarValue.kind: {k!r} is not one of {', '.join(_SCALAR_KINDS)}")

    @classmethod
    def of(cls, v: Union[bool, int, float, str]) -> "ScalarValue":
        if isinstance(v, bool):
            return cls("bool", v)
        if isinstance(v, int):
            return cls("int", v)
        if isinstance(v, float):
            return cls("float", v)
        if isinstance(v, str):
            return cls("str", v)
        raise WireError(f"ScalarValue: unsupported type {type(v).__name__}")

    def to_dict(self) -> Dict[str, Any]:
        return {"kind": self.kind, "value": self.value}

    @classmethod
    def from_dict(cls, d: Any, *, allow_unknown: bool = False, where: str = "ScalarValue") -> "ScalarValue":
        d = _obj(d, where)
        _check_keys(d, frozenset({"kind", "value"}), where, allow_unknown)
        for k in ("kind", "value"):
            if k not in d:
                raise WireError(f"{where}: missing {k!r}")
        try:
            return cls(_str(d["kind"], f"{where}.kind"), d["value"])
        except WireError as e:
            raise WireError(f"{where}: {e}") from None


@dataclass(frozen=True)
class InputMeta(WireModel):
    name: str
    format: InputFormat
    uri_hash: Optional[str] = None
    bytes: Optional[int] = None
    rows: Optional[int] = None
    columns: Optional[ColumnsInfo] = None
    shape: Optional[Tuple[int, ...]] = None
    dtype: Optional[str] = None
    params: Optional[int] = None
    scalar: Optional[ScalarValue] = None

    _KEYS: ClassVar[frozenset] = frozenset(
        {"name", "uri_hash", "format", "bytes", "rows", "columns", "shape", "dtype", "params", "scalar"}
    )

    def __post_init__(self) -> None:
        _str(self.name, "InputMeta.name")
        object.__setattr__(self, "format", InputFormat.parse(self.format, "InputMeta.format"))
        if self.shape is not None:
            object.__setattr__(self, "shape", tuple(_u64(x, "InputMeta.shape[]") for x in self.shape))
        for k in ("bytes", "rows", "params"):
            v = getattr(self, k)
            if v is not None:
                _u64(v, f"InputMeta.{k}")
        if self.scalar is not None and not isinstance(self.scalar, ScalarValue):
            object.__setattr__(self, "scalar", ScalarValue.of(self.scalar))
        if self.columns is not None and not isinstance(self.columns, ColumnsInfo):
            raise WireError("InputMeta.columns: expected ColumnsInfo")

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"name": self.name}
        _put(out, "uri_hash", self.uri_hash)
        out["format"] = self.format.value
        _put(out, "bytes", self.bytes)
        _put(out, "rows", self.rows)
        if self.columns is not None:
            out["columns"] = self.columns.to_dict()
        if self.shape is not None:
            out["shape"] = list(self.shape)
        _put(out, "dtype", self.dtype)
        _put(out, "params", self.params)
        if self.scalar is not None:
            out["scalar"] = self.scalar.to_dict()
        return out

    @classmethod
    def from_dict(cls, d: Any, *, allow_unknown: bool = False, where: str = "InputMeta") -> "InputMeta":
        d = _obj(d, where)
        _check_keys(d, cls._KEYS, where, allow_unknown)
        for k in ("name", "format"):
            if k not in d:
                raise WireError(f"{where}: missing {k!r}")
        shape = d.get("shape")
        if shape is not None and not isinstance(shape, list):
            raise WireError(f"{where}.shape: expected array")
        cols = d.get("columns")
        sc = d.get("scalar")
        return cls(
            name=_str(d["name"], f"{where}.name"),
            format=InputFormat.parse(d["format"], f"{where}.format"),
            uri_hash=None if d.get("uri_hash") is None else _str(d["uri_hash"], f"{where}.uri_hash"),
            bytes=None if d.get("bytes") is None else _u64(d["bytes"], f"{where}.bytes"),
            rows=None if d.get("rows") is None else _u64(d["rows"], f"{where}.rows"),
            columns=None if cols is None else ColumnsInfo.from_dict(cols, allow_unknown=allow_unknown, where=f"{where}.columns"),
            shape=None if shape is None else tuple(_u64(x, f"{where}.shape[]") for x in shape),
            dtype=None if d.get("dtype") is None else _str(d["dtype"], f"{where}.dtype"),
            params=None if d.get("params") is None else _u64(d["params"], f"{where}.params"),
            scalar=None if sc is None else ScalarValue.from_dict(sc, allow_unknown=allow_unknown, where=f"{where}.scalar"),
        )


# --------------------------------------------------------------------------- AttemptContext


@dataclass(frozen=True)
class AttemptContext(WireModel):
    """SPEC §7.2. ``attempt`` is 1-based; ``max_attempts=None`` means unknown (no ladder)."""

    invocation_id: str
    attempt: int = 1
    max_attempts: Optional[int] = None

    def __post_init__(self) -> None:
        _str(self.invocation_id, "AttemptContext.invocation_id", nonempty=True)
        _int(self.attempt, "AttemptContext.attempt", 1, U32_MAX)
        if self.max_attempts is not None:
            _int(self.max_attempts, "AttemptContext.max_attempts", 1, U32_MAX)

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"v": WIRE_VERSION, "invocation_id": self.invocation_id, "attempt": self.attempt}
        _put(out, "max_attempts", self.max_attempts)
        return out

    @classmethod
    def from_dict(cls, d: Any, *, allow_unknown: bool = False, where: str = "AttemptContext") -> "AttemptContext":
        d = _obj(d, where)
        _check_keys(d, frozenset({"v", "invocation_id", "attempt", "max_attempts"}), where, allow_unknown)
        _check_version(d, where)
        for k in ("invocation_id", "attempt"):
            if k not in d:
                raise WireError(f"{where}: missing {k!r}")
        return cls(
            invocation_id=_str(d["invocation_id"], f"{where}.invocation_id", nonempty=True),
            attempt=_int(d["attempt"], f"{where}.attempt", 1, U32_MAX),
            max_attempts=None if d.get("max_attempts") is None else _int(d["max_attempts"], f"{where}.max_attempts", 1, U32_MAX),
        )


# --------------------------------------------------------------------------- RunRecord

# Mirrors rightsizer_core::types::key (Rust is the source of truth).
_PLATFORM_RE = re.compile(r"^[a-z][a-z0-9-]{0,31}$")
_SEGMENT_RE = re.compile(r"^[A-Za-z0-9._@+=,-]{1,128}$")

# Tenant segment count per platform (rightsizer_core::tenant_arity); unknown
# platforms default to 1. Other shapes use the explicit "//" separator:
# "{platform}://{tenant}//{suffix}".
TENANT_ARITY: Dict[str, int] = {
    "k8s": 2, "slurm": 2, "ray": 2, "sky": 2, "dstack": 2, "aws": 2, "gcp": 2, "modal": 2, "databricks": 2,
    "flyte": 3, "azure": 3,
    "airflow": 1, "metaflow": 1,
}


def validate_tenant(tenant: Any, where: str = "tenant") -> str:
    t = _str(tenant, where, nonempty=True)
    if len(t) > 256 or not all(_SEGMENT_RE.match(seg) for seg in t.split("/")):
        raise WireError(f"{where}: invalid tenant {t!r}")
    return t


def _valid_suffix(s: str) -> bool:
    return (
        0 < len(s) <= 512
        and not s.startswith("/")
        and not s.endswith("/")
        and "//" not in s
        and not any(c.isspace() or not c.isprintable() for c in s)
    )


@dataclass(frozen=True)
class WorkloadKey:
    platform: str
    tenant: str
    suffix: str

    def __str__(self) -> str:
        n = TENANT_ARITY.get(self.platform)
        segs = self.tenant.count("/") + 1
        sep = "/" if (n == segs if n is not None else segs == 1) else "//"
        return f"{self.platform}://{self.tenant}{sep}{self.suffix}"


def parse_workload_key(key: Any, where: str = "key") -> WorkloadKey:
    """Parse ``{platform}://{tenant}/{suffix}`` or ``{platform}://{tenant}//{suffix}``."""
    s = _str(key, where, nonempty=True)
    if "://" not in s:
        raise WireError(f"{where}: {s!r}: missing '://'")
    platform, rest = s.split("://", 1)
    if not _PLATFORM_RE.match(platform):
        raise WireError(f"{where}: {s!r}: platform must match [a-z][a-z0-9-]{{0,31}}")
    if "//" in rest:
        tenant, suffix = rest.split("//", 1)
    else:
        n = TENANT_ARITY.get(platform, 1)
        parts = rest.split("/", n)
        if len(parts) != n + 1:
            raise WireError(f"{where}: {s!r}: too few segments for tenant and suffix")
        tenant, suffix = "/".join(parts[:n]), parts[n]
    try:
        validate_tenant(tenant)
    except WireError:
        raise WireError(f"{where}: {s!r}: invalid tenant") from None
    if not _valid_suffix(suffix):
        raise WireError(f"{where}: {s!r}: suffix must be non-empty, without whitespace, '//' or leading/trailing '/'")
    return WorkloadKey(platform, tenant, suffix)


def canonical_key(key: Any) -> str:
    """The key as Rust's ``Display`` writes it (``"/"`` when the tenant has the platform's arity)."""
    return str(parse_workload_key(key))


def validate_workload_key(key: Any, where: str = "key") -> str:
    """Validate a workload key; returns it unchanged."""
    parse_workload_key(key, where)
    return key


def key_platform(key: str) -> str:
    return parse_workload_key(key).platform


_MEASUREMENT_FIELDS = ("memory_peak", "cpu_mean", "cpu_peak_window", "gpu_memory_peak")
_FRAC_FIELDS = ("cpu_throttled_frac", "gpu_util_mean", "t_peak_frac")


@dataclass(frozen=True)
class RunRecord(WireModel):
    """SPEC §5.7. Dedup key: ``(key, invocation_id, attempt, source)``.

    Units: ``memory_peak`` and ``gpu_memory_peak`` in bytes; ``cpu_mean`` and
    ``cpu_peak_window`` in cores; fractions in ``[0, 1]``.
    """

    key: str
    invocation_id: str
    attempt: int
    started_at: _dt.datetime
    status: RunStatus
    source: OutcomeSource
    trust: Trust = Trust.UNVERIFIED
    status_confidence: Confidence = Confidence.OBSERVED
    cluster: Optional[str] = None
    decision_id: Optional[str] = None
    inputs: Tuple[InputMeta, ...] = ()
    code_digest: Optional[str] = None
    requested: Optional[ResourceSpec] = None
    origin_prior: Optional[Requests] = None  # Rust extension to SPEC §5.7 (S5 attribution)
    memory_peak: Optional[Measurement] = None
    cpu_mean: Optional[Measurement] = None
    cpu_peak_window: Optional[Measurement] = None
    cpu_throttled_frac: Optional[float] = None
    gpu_memory_peak: Optional[Measurement] = None
    gpu_util_mean: Optional[float] = None
    t_peak_frac: Optional[float] = None
    duration_s: Optional[float] = None

    _KEYS: ClassVar[frozenset] = frozenset(
        {
            "v", "key", "cluster", "invocation_id", "attempt", "decision_id", "started_at", "inputs",
            "code_digest", "requested", "origin_prior", "status", "status_confidence", "memory_peak", "cpu_mean",
            "cpu_peak_window", "cpu_throttled_frac", "gpu_memory_peak", "gpu_util_mean", "t_peak_frac",
            "duration_s", "source", "trust",
        }
    )

    def __post_init__(self) -> None:
        validate_workload_key(self.key, "RunRecord.key")
        _str(self.invocation_id, "RunRecord.invocation_id", nonempty=True)
        _u32(self.attempt, "RunRecord.attempt")
        object.__setattr__(self, "started_at", parse_timestamp(self.started_at, "RunRecord.started_at"))
        object.__setattr__(self, "status", RunStatus.parse(self.status, "RunRecord.status"))
        object.__setattr__(self, "status_confidence", Confidence.parse(self.status_confidence, "RunRecord.status_confidence"))
        object.__setattr__(self, "trust", Trust.parse(self.trust, "RunRecord.trust"))
        if not isinstance(self.source, OutcomeSource):
            raise WireError("RunRecord.source: expected OutcomeSource")
        if self.decision_id is not None:
            object.__setattr__(self, "decision_id", parse_ulid(self.decision_id, "RunRecord.decision_id"))
        object.__setattr__(self, "inputs", tuple(self.inputs))
        for i in self.inputs:
            if not isinstance(i, InputMeta):
                raise WireError("RunRecord.inputs: expected InputMeta items")
        for k in _MEASUREMENT_FIELDS:
            v = getattr(self, k)
            if v is not None and not isinstance(v, Measurement):
                raise WireError(f"RunRecord.{k}: expected Measurement")
        for k in _FRAC_FIELDS:
            v = getattr(self, k)
            if v is not None:
                object.__setattr__(self, k, _float(v, f"RunRecord.{k}", 0.0, 1.0))
        if self.duration_s is not None:
            object.__setattr__(self, "duration_s", _float(self.duration_s, "RunRecord.duration_s", lo=0.0))
        if self.requested is not None and not isinstance(self.requested, ResourceSpec):
            raise WireError("RunRecord.requested: expected ResourceSpec")
        if self.origin_prior is not None and not isinstance(self.origin_prior, Requests):
            raise WireError("RunRecord.origin_prior: expected Requests")

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"v": WIRE_VERSION, "key": self.key}
        _put(out, "cluster", self.cluster)
        out["invocation_id"] = self.invocation_id
        out["attempt"] = self.attempt
        _put(out, "decision_id", self.decision_id)
        out["started_at"] = format_timestamp(self.started_at)
        out["inputs"] = [i.to_dict() for i in self.inputs]
        _put(out, "code_digest", self.code_digest)
        if self.requested is not None:
            out["requested"] = self.requested.to_dict()
        if self.origin_prior is not None:
            out["origin_prior"] = self.origin_prior.to_dict()
        out["status"] = self.status.value
        out["status_confidence"] = self.status_confidence.value
        for k in ("memory_peak", "cpu_mean", "cpu_peak_window"):
            v = getattr(self, k)
            if v is not None:
                out[k] = v.to_dict()
        _put(out, "cpu_throttled_frac", self.cpu_throttled_frac)
        if self.gpu_memory_peak is not None:
            out["gpu_memory_peak"] = self.gpu_memory_peak.to_dict()
        _put(out, "gpu_util_mean", self.gpu_util_mean)
        _put(out, "t_peak_frac", self.t_peak_frac)
        _put(out, "duration_s", self.duration_s)
        out["source"] = self.source.to_dict()
        out["trust"] = self.trust.value
        return out

    @classmethod
    def from_dict(cls, d: Any, *, allow_unknown: bool = False, where: str = "RunRecord") -> "RunRecord":
        d = _obj(d, where)
        _check_keys(d, cls._KEYS, where, allow_unknown)
        _check_version(d, where, required=False)
        for k in ("key", "invocation_id", "attempt", "started_at", "status", "source", "trust"):
            if k not in d:
                raise WireError(f"{where}: missing {k!r}")
        inputs = d.get("inputs")
        if inputs is None:
            inputs = []
        if not isinstance(inputs, list):
            raise WireError(f"{where}.inputs: expected array")

        def meas(k: str) -> Optional[Measurement]:
            v = d.get(k)
            return None if v is None else Measurement.from_dict(v, allow_unknown=allow_unknown, where=f"{where}.{k}")

        def frac(k: str) -> Optional[float]:
            v = d.get(k)
            return None if v is None else _float(v, f"{where}.{k}", 0.0, 1.0)

        req = d.get("requested")
        op = d.get("origin_prior")
        conf = d.get("status_confidence")
        return cls(
            key=validate_workload_key(d["key"], f"{where}.key"),
            cluster=None if d.get("cluster") is None else _str(d["cluster"], f"{where}.cluster"),
            invocation_id=_str(d["invocation_id"], f"{where}.invocation_id", nonempty=True),
            attempt=_u32(d["attempt"], f"{where}.attempt"),
            decision_id=None if d.get("decision_id") is None else parse_ulid(d["decision_id"], f"{where}.decision_id"),
            started_at=parse_timestamp(d["started_at"], f"{where}.started_at"),
            inputs=tuple(
                InputMeta.from_dict(x, allow_unknown=allow_unknown, where=f"{where}.inputs[{n}]") for n, x in enumerate(inputs)
            ),
            code_digest=None if d.get("code_digest") is None else _str(d["code_digest"], f"{where}.code_digest"),
            requested=None if req is None else ResourceSpec.from_dict(req, allow_unknown=allow_unknown, where=f"{where}.requested"),
            origin_prior=None if op is None else Requests.from_dict(op, allow_unknown=allow_unknown, where=f"{where}.origin_prior"),
            status=RunStatus.parse(d["status"], f"{where}.status"),
            status_confidence=Confidence.OBSERVED if conf is None else Confidence.parse(conf, f"{where}.status_confidence"),
            memory_peak=meas("memory_peak"),
            cpu_mean=meas("cpu_mean"),
            cpu_peak_window=meas("cpu_peak_window"),
            cpu_throttled_frac=frac("cpu_throttled_frac"),
            gpu_memory_peak=meas("gpu_memory_peak"),
            gpu_util_mean=frac("gpu_util_mean"),
            t_peak_frac=frac("t_peak_frac"),
            duration_s=None if d.get("duration_s") is None else _float(d["duration_s"], f"{where}.duration_s", lo=0.0),
            source=OutcomeSource.from_dict(d["source"], allow_unknown=allow_unknown, where=f"{where}.source"),
            trust=Trust.parse(d["trust"], f"{where}.trust"),
        )


__all__ = [
    "ACCELERATORS",
    "AttemptContext",
    "Censor",
    "ColumnsInfo",
    "Confidence",
    "GpuShape",
    "InputFormat",
    "InputMeta",
    "Limits",
    "Measurement",
    "Mode",
    "OutcomeSource",
    "OutcomeSourceKind",
    "Requests",
    "ResourceSpec",
    "RunRecord",
    "RunStatus",
    "ScalarValue",
    "TENANT_ARITY",
    "Trust",
    "WorkloadKey",
    "canonical_key",
    "parse_workload_key",
    "validate_tenant",
    "WIRE_VERSION",
    "WireError",
    "format_timestamp",
    "key_platform",
    "new_ulid",
    "parse_timestamp",
    "parse_ulid",
    "ulid_timestamp_ms",
    "utcnow",
    "validate_workload_key",
]
