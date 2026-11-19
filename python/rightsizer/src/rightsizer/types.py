"""The typed API (SPEC §10.3): contexts in, decisions out.

Building these objects needs neither ``_core`` nor any platform package.
The wire shapes are the Rust serde shapes (ARCHITECTURE.md §4); the
``to_dict`` / ``from_dict`` pairs round-trip Rust JSON exactly
(``tests/fixtures/wire``).
"""

from __future__ import annotations

import getpass
import hashlib
import inspect
import marshal
import os
import re
import textwrap
import threading
import weakref
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

from rightsizer_runtime.wire import (
    WIRE_VERSION,
    GpuShape,
    InputFormat,
    InputMeta,
    Limits,
    Mode,
    Requests,
    ResourceSpec,
    RunRecord,
    ScalarValue,
    WireError,
    canonical_key,
    new_ulid,
)

from . import _units

Quantity = Union[str, int, float]
MemorySpec = Union[Quantity, Tuple[Quantity, Quantity], None]
GpuSpec = Union[str, int, GpuShape, Tuple[str, int], None]

AXES = ("memory", "cpu", "gpu_count", "gpu_type", "ephemeral_storage", "shm")
_AXIS_ALIASES = {"mem": "memory", "gpu": "gpu", "disk": "ephemeral_storage", "cpus": "cpu"}


# ── resources ────────────────────────────────────────────────────────────


def _req_lim(v: Any, conv: Callable[[Any], int]) -> Tuple[Optional[int], Optional[int]]:
    if v is None:
        return None, None
    if isinstance(v, tuple):
        if len(v) != 2:
            raise ValueError("a (request, limit) pair has two elements")
        req, lim = v
        return (None if req is None else conv(req)), (None if lim is None else conv(lim))
    return conv(v), None


@dataclass(frozen=True, init=False)
class Resources:
    """What a unit asks for: ``Resources(cpu=8, memory="64Gi", gpu="A100-80G:1")``.

    - ``cpu``: cores (``8``, ``0.5``) or a K8s quantity (``"500m"``);
    - ``memory``, ``ephemeral_storage``, ``shm``: bytes or a K8s quantity
      (``"64Gi"``);
    - ``cpu`` and ``memory`` also take a ``(request, limit)`` pair, as
      ``flyte.Resources`` does;
    - ``gpu``: a canonical accelerator id with a count (``"A100-80G:1"``,
      SPEC §5.1.1), ``(id, count)``, or a bare count (``unknown-gpu``).

    Values are normalized on construction (bytes, millicores) and validated.
    """

    requests: Requests
    limits: Limits

    def __init__(
        self,
        cpu: MemorySpec = None,
        memory: MemorySpec = None,
        gpu: GpuSpec = None,
        ephemeral_storage: Optional[Quantity] = None,
        shm: Optional[Quantity] = None,
        *,
        requests: Optional[Requests] = None,
        limits: Optional[Limits] = None,
    ):
        if requests is not None or limits is not None:
            if any(x is not None for x in (cpu, memory, gpu, ephemeral_storage, shm)):
                raise TypeError("pass either quantities or requests=/limits=, not both")
            object.__setattr__(self, "requests", requests or Requests())
            object.__setattr__(self, "limits", limits or Limits())
            return
        cpu_r, cpu_l = _req_lim(cpu, _units.cpu_millis)
        mem_r, mem_l = _req_lim(memory, _units.memory_bytes)
        object.__setattr__(
            self,
            "requests",
            Requests(
                cpu=cpu_r,
                memory=mem_r,
                gpu=None if gpu is None else _units.gpu_shape(gpu),
                ephemeral_storage=None if ephemeral_storage is None else _units.memory_bytes(ephemeral_storage),
                shm=None if shm is None else _units.memory_bytes(shm),
            ),
        )
        object.__setattr__(self, "limits", Limits(cpu=cpu_l, memory=mem_l))

    @classmethod
    def from_spec(cls, spec: Union[ResourceSpec, Mapping[str, Any]]) -> "Resources":
        if not isinstance(spec, ResourceSpec):
            spec = ResourceSpec.from_dict(spec, allow_unknown=True)
        return cls(requests=spec.requests, limits=spec.limits)

    @property
    def spec(self) -> ResourceSpec:
        return ResourceSpec(requests=self.requests, limits=self.limits)

    # Convenience views.
    @property
    def cpu(self) -> Optional[int]:
        """Requested millicores."""
        return self.requests.cpu

    @property
    def memory(self) -> Optional[int]:
        """Requested bytes."""
        return self.requests.memory

    @property
    def gpu(self) -> Optional[GpuShape]:
        return self.requests.gpu

    def to_dict(self) -> Dict[str, Any]:
        return self.spec.to_dict()

    def __repr__(self) -> str:
        parts = []
        r = self.requests
        if r.cpu is not None:
            parts.append(f"cpu={_units.format_cpu(r.cpu)!r}")
        if r.memory is not None:
            parts.append(f"memory={_units.format_bytes(r.memory)!r}")
        if r.gpu is not None:
            parts.append(f"gpu='{r.gpu.accel}:{r.gpu.count}'")
        if self.limits.to_dict():
            parts.append(f"limits={self.limits.to_dict()}")
        return f"Resources({', '.join(parts)})"


@dataclass(frozen=True)
class Prior:
    """The origin prior (SPEC §5.1): the unit's own request before sizing."""

    spec: ResourceSpec = field(default_factory=ResourceSpec)
    defaulted: Tuple[str, ...] = ()
    qos: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"spec": self.spec.to_dict(), "defaulted": list(self.defaulted)}
        if self.qos is not None:
            out["qos"] = self.qos
        return out

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "Prior":
        return cls(
            spec=ResourceSpec.from_dict(d.get("spec") or {}, allow_unknown=True),
            defaulted=tuple(_axes(d.get("defaulted") or ())),
            qos=d.get("qos"),
        )

    @classmethod
    def of(cls, r: Union["Resources", ResourceSpec, Mapping[str, Any], "Prior", None], defaulted: Iterable[str] = ()) -> "Prior":
        if r is None:
            return cls(defaulted=tuple(_axes(defaulted)))
        if isinstance(r, Prior):
            return r
        if isinstance(r, Resources):
            return cls(spec=r.spec, defaulted=tuple(_axes(defaulted)))
        if isinstance(r, ResourceSpec):
            return cls(spec=r, defaulted=tuple(_axes(defaulted)))
        if isinstance(r, Mapping) and "spec" in r:
            return cls.from_dict(r)
        return cls(spec=ResourceSpec.from_dict(r, allow_unknown=True), defaulted=tuple(_axes(defaulted)))


def _axes(axes: Union[str, Iterable[str]]) -> List[str]:
    if isinstance(axes, str):
        axes = [a for a in axes.split(",") if a.strip()]
    out: List[str] = []
    for a in axes:
        a = _AXIS_ALIASES.get(a.strip(), a.strip())
        names = ["gpu_count", "gpu_type"] if a == "gpu" else [a]
        for n in names:
            if n not in AXES:
                raise ValueError(f"unknown axis {a!r}; axes: {', '.join(AXES)} (aliases: gpu, mem, disk)")
            if n not in out:
                out.append(n)
    return sorted(out, key=AXES.index)


# ── inputs and code ──────────────────────────────────────────────────────


@dataclass(frozen=True)
class Input:
    """One input reference (``InputRef``): only metadata is ever read
    (SPEC §5.2 "no peeking": footers, headers, sizes)."""

    name: str
    uri: Optional[str] = None
    bytes: Optional[int] = None
    format: Optional[InputFormat] = None
    scalar_value: Optional[ScalarValue] = None

    @classmethod
    def from_uri(cls, uri: str, name: Optional[str] = None, *, format: Optional[Union[str, InputFormat]] = None,
                 bytes: Optional[int] = None) -> "Input":
        """A file, directory, object-store URI or ``hf://org/model`` ref."""
        if not isinstance(uri, str) or not uri:
            raise ValueError("an input URI must be a non-empty string")
        if name is None:
            base = re.split(r"[?#]", uri, maxsplit=1)[0].rstrip("/")
            name = base.rsplit("/", 1)[-1] or uri
        return cls(name=name, uri=uri, bytes=bytes, format=None if format is None else InputFormat.parse(format, "format"))

    @classmethod
    def from_path(cls, path: Union[str, "os.PathLike[str]"], name: Optional[str] = None, **kw: Any) -> "Input":
        return cls.from_uri(os.path.abspath(os.fspath(path)), name, **kw)

    @classmethod
    def scalar(cls, name: str, value: Union[bool, int, float, str]) -> "Input":
        """A scalar argument (numeric scalars join the memo bucket as given)."""
        return cls(name=name, scalar_value=ScalarValue.of(value))

    @classmethod
    def sized(cls, name: str, bytes: int, format: Optional[Union[str, InputFormat]] = None) -> "Input":
        """An input whose size is already known (no reads)."""
        return cls(name=name, bytes=bytes, format=None if format is None else InputFormat.parse(format, "format"))

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"name": self.name}
        if self.uri is not None:
            out["uri"] = self.uri
        if self.bytes is not None:
            out["bytes"] = self.bytes
        if self.format is not None:
            out["format"] = InputFormat.parse(self.format).value
        if self.scalar_value is not None:
            out["scalar"] = self.scalar_value.to_dict()
        return out


_SOURCE_CACHE: "weakref.WeakKeyDictionary[Any, Code]" = weakref.WeakKeyDictionary()
_SOURCE_LOCK = threading.Lock()


@dataclass(frozen=True)
class Code:
    """A code reference (``CodeRef``): source text (digest and tokens are
    computed in Rust), a file path, or a precomputed digest."""

    kind: str  # "source" | "path" | "info"
    text: Optional[str] = None
    language: Optional[str] = None
    path: Optional[str] = None
    digest: Optional[str] = None
    tokens: Tuple[str, ...] = ()

    @classmethod
    def from_source(cls, text: str, language: Optional[str] = "python") -> "Code":
        return cls(kind="source", text=text, language=language)

    @classmethod
    def from_path(cls, path: Union[str, "os.PathLike[str]"]) -> "Code":
        return cls(kind="path", path=os.path.abspath(os.fspath(path)))

    @classmethod
    def from_digest(cls, digest: str, tokens: Iterable[str] = ()) -> "Code":
        return cls(kind="info", digest=digest, tokens=tuple(sorted(set(tokens))))

    @classmethod
    def from_callable(cls, fn: Callable[..., Any]) -> "Code":
        """``inspect.getsource(fn)``, captured once per function. When the
        source is unavailable (pickled bundles, REPL), a digest of
        ``fn.__code__`` instead."""
        target = inspect.unwrap(fn) if callable(fn) else fn
        try:
            with _SOURCE_LOCK:
                hit = _SOURCE_CACHE.get(target)
            if hit is not None:
                return hit
        except TypeError:
            hit = None
        try:
            src = textwrap.dedent(inspect.getsource(target))
            code = cls.from_source(src, "python")
        except (OSError, TypeError):
            code = cls.from_digest(_code_digest(target))
        try:
            with _SOURCE_LOCK:
                _SOURCE_CACHE[target] = code
        except TypeError:
            pass
        return code

    def to_dict(self) -> Dict[str, Any]:
        if self.kind == "source":
            out: Dict[str, Any] = {"kind": "source", "text": self.text or ""}
            if self.language is not None:
                out["language"] = self.language
            return out
        if self.kind == "path":
            return {"kind": "path", "path": self.path}
        return {"kind": "info", "info": {"digest": self.digest or "", "tokens": list(self.tokens)}}


def _code_digest(fn: Any) -> str:
    co = getattr(fn, "__code__", None)
    if co is None:
        co = getattr(getattr(fn, "__call__", None), "__code__", None)
    h = hashlib.sha256(b"rightsizer-pycode/1\0")
    if co is not None:
        try:
            h.update(marshal.dumps(co))
        except ValueError:
            h.update(repr(co).encode())
    else:
        h.update(f"{getattr(fn, '__module__', '')}.{getattr(fn, '__qualname__', repr(fn))}".encode())
    return "sha256:" + h.hexdigest()


# ── contexts ─────────────────────────────────────────────────────────────

_TENANT_BAD = re.compile(r"[^A-Za-z0-9._@+=,-]")


def default_tenant() -> str:
    """``RIGHTSIZER_TENANT``, else the user name (sanitized to the tenant charset)."""
    t = os.environ.get("RIGHTSIZER_TENANT")
    if not t:
        try:
            t = getpass.getuser()
        except Exception:
            t = "default"
    return _TENANT_BAD.sub("-", t) or "default"


def workload_key(key: str, platform: str = "python", tenant: Optional[str] = None) -> str:
    """A full key: ``key`` as given when it has ``://``, else
    ``{platform}://{tenant}/{key}``."""
    if "://" in key:
        return canonical_key(key)
    t = tenant or default_tenant()
    return canonical_key(f"{platform}://{t}//{key}")


@dataclass
class Context:
    """What is known about a unit before it runs (a ``ContextSeed`` plus key,
    prior and platform facts; SPEC §5.2).

    ``key`` is a full workload key (``flyte://org/proj/dev/env.task``) or a
    suffix, which becomes ``python://{tenant}/{key}`` (tenant:
    ``RIGHTSIZER_TENANT`` or the user). History comes from the store unless
    ``history`` is given (then the context is complete as built here).
    """

    key: str
    prior: Union[Resources, Prior, ResourceSpec, Mapping[str, Any], None] = None
    inputs: Sequence[Input] = ()
    code: Optional[Code] = None
    platform: Union[str, Mapping[str, Any]] = "python"
    unit: str = "main"
    group: Optional[str] = None
    labels: Mapping[str, str] = field(default_factory=dict)
    manifest: Mapping[str, Any] = field(default_factory=dict)
    history: Optional[Sequence[Union[RunRecord, Mapping[str, Any]]]] = None
    tenant: Optional[str] = None
    defaulted: Sequence[str] = ()

    def __post_init__(self) -> None:
        self.inputs = tuple(self.inputs)
        for i in self.inputs:
            if not isinstance(i, Input):
                raise TypeError("Context.inputs: expected rs.Input items (rs.Input.from_uri, rs.Input.scalar)")
        if self.code is not None and not isinstance(self.code, Code):
            if callable(self.code):
                self.code = Code.from_callable(self.code)
            elif isinstance(self.code, str):
                self.code = Code.from_source(self.code)
            else:
                raise TypeError("Context.code: expected rs.Code, a callable or source text")
        self._key = workload_key(self.key, self.platform_ref()["kind"], self.tenant)
        self._prior = Prior.of(self.prior, self.defaulted)

    @property
    def workload_key(self) -> str:
        return self._key

    @property
    def origin_prior(self) -> Prior:
        return self._prior

    def platform_ref(self) -> Dict[str, Any]:
        if isinstance(self.platform, str):
            return {"kind": self.platform}
        p = dict(self.platform)
        p.setdefault("kind", "python")
        return p

    def seed(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"inputs": [i.to_dict() for i in self.inputs]}
        if self.code is not None:
            out["code"] = self.code.to_dict()
        out["manifest"] = dict(self.manifest)
        out["labels"] = dict(self.labels)
        return out

    def context_input(self) -> Dict[str, Any]:
        """``ContextInput::Seed`` JSON (history from the store)."""
        out: Dict[str, Any] = {
            "kind": "seed",
            "key": self._key,
            "unit": self.unit,
            "seed": self.seed(),
            "prior": self._prior.to_dict(),
            "platform": self.platform_ref(),
        }
        if self.group is not None:
            out["group"] = self.group
        return out


# ── estimates and decisions ──────────────────────────────────────────────

_EST_AXES = ("memory", "cpu_mean", "cpu_peak", "gpu_memory", "parallelism", "duration_s")


@dataclass(frozen=True)
class Estimate:
    """An estimate (SPEC §5.5). Axis estimates stay in their wire form:
    ``{"kind": "cells", "grid", "probs"}``, ``{"kind": "quantiles", "q": [[q, v], ...]}``
    or ``{"kind": "point", "value"}``; ``parallelism`` is ``{"mode", "imbalance"}``."""

    provenance: Mapping[str, Any]
    kind: str = "demand"
    memory: Optional[Mapping[str, Any]] = None
    cpu_mean: Optional[Mapping[str, Any]] = None
    cpu_peak: Optional[Mapping[str, Any]] = None
    gpu_memory: Optional[Mapping[str, Any]] = None
    parallelism: Optional[Mapping[str, Any]] = None
    duration_s: Optional[Mapping[str, Any]] = None
    t_peak_frac: Optional[float] = None
    novelty: Optional[float] = None
    v: int = WIRE_VERSION

    _KEYS = frozenset({"v", "kind", "provenance", "t_peak_frac", "novelty", *_EST_AXES})

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"v": self.v, "kind": self.kind}
        for k in _EST_AXES:
            v = getattr(self, k)
            if v is not None:
                out[k] = _plain(v)
        if self.t_peak_frac is not None:
            out["t_peak_frac"] = self.t_peak_frac
        if self.novelty is not None:
            out["novelty"] = self.novelty
        out["provenance"] = dict(self.provenance)
        return out

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "Estimate":
        unknown = set(d) - cls._KEYS
        if unknown:
            raise WireError(f"Estimate: unknown fields {sorted(unknown)}")
        if "provenance" not in d:
            raise WireError("Estimate: missing 'provenance'")
        kind = d.get("kind", "demand")
        if kind not in ("demand", "action"):
            raise WireError(f"Estimate.kind: {kind!r}")
        for k in _EST_AXES:
            v = d.get(k)
            if v is not None and k != "parallelism" and (not isinstance(v, Mapping) or v.get("kind") not in ("cells", "quantiles", "point")):
                raise WireError(f"Estimate.{k}: expected a cells / quantiles / point distribution")
        return cls(
            v=int(d.get("v", WIRE_VERSION)),
            kind=kind,
            provenance=dict(d["provenance"]),
            t_peak_frac=d.get("t_peak_frac"),
            novelty=d.get("novelty"),
            **{k: d.get(k) for k in _EST_AXES},
        )

    @classmethod
    def cells(cls, probs: Sequence[float], *, estimator: str, version: str = "0", axis: str = "memory",
              grid: str = "rs-grid-v1", **axes: Any) -> "Estimate":
        """A one-axis ``Cells`` estimate (more axes as keyword dicts)."""
        prov = {"estimator": estimator, "version": version, "grid": grid, "features": "features-v1", "latency_ms": 0}
        return cls(provenance=prov, **{axis: {"kind": "cells", "grid": grid, "probs": list(probs)}}, **axes)


def _plain(v: Any) -> Any:
    if isinstance(v, Mapping):
        return {k: _plain(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_plain(x) for x in v]
    return v


@dataclass(frozen=True)
class Hold:
    """An axis the decision did not change, and why (SPEC §5.6)."""

    axis: str
    reason: str
    detail: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        out = {"axis": self.axis, "reason": self.reason}
        if self.detail is not None:
            out["detail"] = self.detail
        return out

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "Hold":
        return cls(axis=d["axis"], reason=d["reason"], detail=d.get("detail"))


@dataclass(frozen=True)
class Source:
    """Where a decision came from: ``estimator`` / ``memo`` (``id``,
    ``version``) or ``prior`` (``reason``, ``detail``)."""

    kind: str
    id: Optional[str] = None
    version: Optional[str] = None
    reason: Optional[str] = None
    detail: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        if self.kind == "prior":
            out: Dict[str, Any] = {"kind": "prior", "reason": self.reason}
            if self.detail is not None:
                out["detail"] = self.detail
            return out
        return {"kind": self.kind, "id": self.id, "version": self.version}

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "Source":
        return cls(kind=d["kind"], id=d.get("id"), version=d.get("version"), reason=d.get("reason"), detail=d.get("detail"))


_DECISION_KEYS = frozenset(
    {"v", "id", "key", "unit", "group", "mode", "recommended", "applied", "shape", "prior", "held", "source",
     "estimate", "propensity", "expected_cost", "p_fail", "advisories", "explanation", "context_digest"}
)


@dataclass(frozen=True)
class Decision:
    """A decision (SPEC §5.6).

    - ``applied``: what to run with (the prior unless the mode is ``enforce``
      and the safety contract allows a change);
    - ``recommended``: what rightsizer would run with;
    - ``held``: axes not changed and why; ``explanation``: human-readable lines.
    """

    id: str
    key: str
    unit: str
    mode: Mode
    recommended: ResourceSpec
    applied: ResourceSpec
    prior: Prior
    source: Source
    propensity: float = 1.0
    held: Tuple[Hold, ...] = ()
    explanation: Tuple[str, ...] = ()
    advisories: Tuple[Mapping[str, Any], ...] = ()
    context_digest: str = ""
    group: Optional[str] = None
    shape: Optional[str] = None
    estimate: Optional[Estimate] = None
    expected_cost: Optional[float] = None
    p_fail: Optional[float] = None
    v: int = WIRE_VERSION
    # The context it was made for (not on the wire; used by rs.outcome).
    context: Optional[Context] = field(default=None, compare=False, repr=False)

    @property
    def is_prior(self) -> bool:
        """The decision kept the prior (fallback, mode, insufficient context…)."""
        return self.source.kind == "prior"

    @property
    def changed(self) -> bool:
        """``applied`` differs from the prior."""
        return self.applied != self.prior.spec

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"v": self.v, "id": self.id, "key": self.key, "unit": self.unit}
        if self.group is not None:
            out["group"] = self.group
        out["mode"] = Mode.parse(self.mode).value
        out["recommended"] = self.recommended.to_dict()
        out["applied"] = self.applied.to_dict()
        if self.shape is not None:
            out["shape"] = self.shape
        out["prior"] = self.prior.to_dict()
        out["held"] = [h.to_dict() for h in self.held]
        out["source"] = self.source.to_dict()
        if self.estimate is not None:
            out["estimate"] = self.estimate.to_dict()
        out["propensity"] = self.propensity
        if self.expected_cost is not None:
            out["expected_cost"] = self.expected_cost
        if self.p_fail is not None:
            out["p_fail"] = self.p_fail
        out["advisories"] = [_plain(a) for a in self.advisories]
        out["explanation"] = list(self.explanation)
        out["context_digest"] = self.context_digest
        return out

    @classmethod
    def from_dict(cls, d: Mapping[str, Any], *, context: Optional[Context] = None, allow_unknown: bool = True) -> "Decision":
        if not allow_unknown:
            unknown = set(d) - _DECISION_KEYS
            if unknown:
                raise WireError(f"Decision: unknown fields {sorted(unknown)}")
        est = d.get("estimate")
        return cls(
            v=int(d.get("v", WIRE_VERSION)),
            id=d["id"],
            key=d["key"],
            unit=d["unit"],
            group=d.get("group"),
            mode=Mode.parse(d["mode"], "Decision.mode"),
            recommended=ResourceSpec.from_dict(d["recommended"], allow_unknown=allow_unknown),
            applied=ResourceSpec.from_dict(d["applied"], allow_unknown=allow_unknown),
            shape=d.get("shape"),
            prior=Prior.from_dict(d["prior"]),
            held=tuple(Hold.from_dict(h) for h in d.get("held") or ()),
            source=Source.from_dict(d["source"]),
            estimate=None if est is None else Estimate.from_dict(est),
            propensity=float(d.get("propensity", 1.0)),
            expected_cost=d.get("expected_cost"),
            p_fail=d.get("p_fail"),
            advisories=tuple(d.get("advisories") or ()),
            explanation=tuple(d.get("explanation") or ()),
            context_digest=d.get("context_digest", ""),
            context=context,
        )

    @classmethod
    def prior_for(cls, ctx: Context, reason: str, detail: Optional[str] = None, mode: Union[Mode, str] = Mode.RECOMMEND) -> "Decision":
        """A decision that keeps the prior, built without ``_core`` (S1)."""
        prior = ctx.origin_prior
        expl = [f"kept the prior: {reason}"] + ([detail] if detail else [])
        m = Mode.parse(mode)
        return cls(
            id=new_ulid(), key=ctx.workload_key, unit=ctx.unit, group=ctx.group,
            mode=m if m <= Mode.RECOMMEND else Mode.RECOMMEND,
            recommended=prior.spec, applied=prior.spec, prior=prior,
            source=Source(kind="prior", reason=reason, detail=detail), explanation=tuple(expl), context=ctx,
        )

    def marker(self, measured: bool = False):
        """The ``RIGHTSIZER_DECISION`` payload for the sized unit (plugins.md §2)."""
        from rightsizer_runtime.env import DecisionEnv

        return DecisionEnv(decision_id=self.id, key=self.key, mode=self.mode, requested=self.applied, measured=measured)

    def __str__(self) -> str:
        a = self.applied.requests
        return (f"Decision({self.key}#{self.unit} mode={Mode.parse(self.mode).value} "
                f"memory={_units.format_bytes(a.memory)} cpu={_units.format_cpu(a.cpu)} source={self.source.kind})")


@dataclass(frozen=True)
class OutcomeReport:
    """What ``rs.outcome`` did (the engine's ``OutcomeReport``)."""

    written: bool
    joined: bool
    attribution: Optional[str] = None
    tripped: Tuple[Mapping[str, Any], ...] = ()
    lease_closed: bool = False

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "OutcomeReport":
        return cls(written=bool(d.get("written")), joined=bool(d.get("joined")), attribution=d.get("attribution"),
                   tripped=tuple(d.get("tripped") or ()), lease_closed=bool(d.get("lease_closed")))


__all__ = [
    "AXES", "Code", "Context", "Decision", "Estimate", "Hold", "Input", "OutcomeReport", "Prior", "Resources",
    "Source", "default_tenant", "workload_key",
]
