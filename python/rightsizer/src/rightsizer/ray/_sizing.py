"""Deciding Ray options (ray.md §2-§5): prior from options, context from
metadata, decision, and the merge back into options.

**What is read.** Only metadata (ray.md §2 "Arguments"): URI strings go to
the engine's metadata readers; ``ObjectRef`` sizes come from
``get_local_object_locations`` (no RPC), then a guarded
``get_object_locations`` with a short timeout (it is experimental, may
return ``None`` sizes, and waits forever by default); objects are never
fetched. Other arguments contribute nothing, except scalars named in
``scalars=``.

**What is written** (``Patch::Options``): ``num_cpus`` and ``memory``
(bytes) when an enforced decision changed them; ``accelerator_type`` only
when GPU type changes are allowed (off in v1). ``num_gpus`` never changes
(GPU count is held in v1). Axes set in ``.options(**kw)`` are pins.
``resources={…}``, ``max_retries`` and every other option are never touched.

**S1.** :func:`decide_options` never raises: any failure returns the
options unchanged (and no decision).
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet, Iterable, List, Mapping, Optional, Sequence, Tuple

from ._settings import RaySettings, catalog_accelerator, ray_accelerator, settings

log = logging.getLogger("rightsizer.ray")

PLATFORM = "ray"
_URI_RE = re.compile(r"^(?:[A-Za-z][A-Za-z0-9+.-]*://|/)\S+$")
MAX_URI_INPUTS = 16
OBJECT_LOCATION_TIMEOUT_MS = 50

# Ray option → axes it pins.
_PIN_AXES = {
    "num_cpus": ("cpu",),
    "memory": ("memory",),
    "num_gpus": ("gpu_count", "gpu_type"),
    "accelerator_type": ("gpu_type",),
}


def pins_from(options: Mapping[str, Any]) -> FrozenSet[str]:
    """Axes pinned by explicit ``.options(**kw)`` keys."""
    out: set = set()
    for k in options:
        out.update(_PIN_AXES.get(k, ()))
    return frozenset(out)


@dataclass(frozen=True)
class SizingPolicy:
    """Per-definition policy (decorator keywords)."""

    key: Optional[str] = None
    mode: Optional[str] = None
    risk: Any = None
    estimator: Any = None
    budget_ms: Optional[int] = None
    ceiling: Any = None
    floor: Any = None
    pins: FrozenSet[str] = frozenset()
    scalars: Tuple[str, ...] = ()
    allow_gpu_type_change: Optional[bool] = None


@dataclass
class Sized:
    """The outcome of :func:`decide_options`."""

    options: Dict[str, Any]
    decision: Any = None  # rightsizer.Decision or None
    changed: Tuple[str, ...] = ()
    settings: Optional[RaySettings] = None
    inputs: List[Dict[str, Any]] = field(default_factory=list)


def default_suffix(obj: Any) -> str:
    """``{module}.{qualname}`` of a function or class (ray.md §8)."""
    target = obj
    for attr in ("_function", "__ray_actor_class__"):
        inner = getattr(target, attr, None)
        if inner is not None:
            target = inner
    mod = getattr(target, "__module__", None) or "global"
    qn = getattr(target, "__qualname__", None) or getattr(target, "__name__", None) or type(target).__name__
    return f"{mod}.{qn}".replace("<", "").replace(">", "")


def workload_key(suffix: str, tenant: str) -> str:
    from ..types import workload_key as wk

    return wk(suffix, PLATFORM, tenant)


# ── prior ────────────────────────────────────────────────────────────────


def _num(v: Any) -> Optional[float]:
    if v is None or isinstance(v, bool):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def prior_from_options(options: Mapping[str, Any], *, actor: bool = False) -> Tuple[Dict[str, Any], List[str]]:
    """``(Requests dict, defaulted axes)`` from Ray options.

    Ray's defaults: tasks reserve 1 CPU, actors 0 (1 to schedule); no
    ``memory`` means no memory reservation (no prior: memory is held unless
    ``enforce_without_prior``). Fractional GPUs are not modelled.
    """
    req: Dict[str, Any] = {}
    defaulted: List[str] = []
    cpu = _num(options.get("num_cpus"))
    if cpu is None:
        defaulted.append("cpu")
        if not actor:
            req["cpu"] = 1000
    else:
        req["cpu"] = int(round(cpu * 1000))
    mem = _num(options.get("memory"))
    if mem is None or mem <= 0:
        defaulted.append("memory")
    else:
        req["memory"] = int(mem)
    gpus = _num(options.get("num_gpus"))
    if gpus and gpus >= 1 and float(gpus).is_integer():
        req["gpu"] = {"accel": catalog_accelerator(options.get("accelerator_type")), "count": int(gpus)}
    return req, defaulted


# ── inputs ───────────────────────────────────────────────────────────────


def _object_sizes(refs: Sequence[Any]) -> Dict[Any, int]:
    """Sizes of ``ObjectRef``s without fetching them (guarded)."""
    out: Dict[Any, int] = {}
    if not refs:
        return out
    try:
        from ray.experimental import get_local_object_locations

        for r, loc in (get_local_object_locations(list(refs)) or {}).items():
            size = (loc or {}).get("object_size")
            if isinstance(size, int) and size >= 0:
                out[r] = size
    except Exception as e:
        log.debug("rightsizer.ray: get_local_object_locations: %s", e)
    missing = [r for r in refs if r not in out]
    if missing:
        try:
            from ray.experimental import get_object_locations

            for r, loc in (get_object_locations(missing, timeout_ms=OBJECT_LOCATION_TIMEOUT_MS) or {}).items():
                size = (loc or {}).get("object_size")
                if isinstance(size, int) and size >= 0:
                    out[r] = size
        except Exception as e:
            log.debug("rightsizer.ray: get_object_locations: %s", e)
    return out


def inputs_from_args(args: Sequence[Any], kwargs: Mapping[str, Any], scalars: Iterable[str] = ()) -> List[Any]:
    """``rs.Input``s from call arguments: URI strings, ``ObjectRef`` sizes,
    and the scalars named in ``scalars`` (positional ``argN`` or keyword names)."""
    from ..types import Input

    try:
        from ray import ObjectRef
    except Exception:  # pragma: no cover - ray is installed when this runs
        ObjectRef = ()  # type: ignore[assignment]
    want = set(scalars)
    items: List[Tuple[str, Any]] = [(f"arg{i}", a) for i, a in enumerate(args)] + list(kwargs.items())
    out: List[Any] = []
    refs: List[Tuple[str, Any]] = []
    n_uri = 0
    for name, v in items:
        if isinstance(v, str):
            if n_uri < MAX_URI_INPUTS and _URI_RE.match(v):
                out.append(Input.from_uri(v, name))
                n_uri += 1
            elif name in want:
                out.append(Input.scalar(name, v))
        elif ObjectRef and isinstance(v, ObjectRef):
            refs.append((name, v))
        elif name in want and isinstance(v, (bool, int, float)):
            out.append(Input.scalar(name, v))
    if refs:
        sizes = _object_sizes([r for _, r in refs])
        for name, r in refs:
            if r in sizes:
                out.append(Input.sized(name, sizes[r]))
    return out


# ── deciding ─────────────────────────────────────────────────────────────


def in_placement_group(options: Mapping[str, Any]) -> bool:
    """Whether a task/actor with these options is scheduled into a placement group.

    Explicitly (``PlacementGroupSchedulingStrategy`` or the deprecated
    ``placement_group=`` option) or by Ray capturing the caller's group
    (``placement_group_capture_child_tasks``, Ray's
    ``_configure_placement_group_based_on_context``). A request larger than
    its bundle can never be scheduled (the task stays pending forever), so
    rightsizer never increases it. Never raises (unknown = not in a group).
    """
    try:
        ss = options.get("scheduling_strategy")
        try:
            from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

            if isinstance(ss, PlacementGroupSchedulingStrategy):
                return getattr(ss, "placement_group", None) is not None
        except Exception:  # pragma: no cover - ray is installed when this runs
            pass
        pg = options.get("placement_group", "default")
        if pg is not None and not (isinstance(pg, str) and pg == "default"):
            return True
        if ss is not None or pg is None:
            return False
        capture = options.get("placement_group_capture_child_tasks")
        import ray

        w = ray._private.worker.global_worker
        if capture is None:
            capture = bool(getattr(w, "should_capture_child_tasks_in_placement_group", False))
        if not capture:
            return False
        pg_id = getattr(w, "placement_group_id", None)
        return pg_id is not None and not pg_id.is_nil()
    except Exception as e:
        log.debug("rightsizer.ray: placement group unknown: %s", e)
        return False


def _constraints(pol: SizingPolicy, pins: FrozenSet[str], prior: Mapping[str, Any], shapes_source: str,
                 hold_increases: bool = False) -> Dict[str, Any]:
    from .._engine import _requests

    c: Dict[str, Any] = {}
    all_pins = set(pol.pins) | set(pins)
    if all_pins:
        from ..types import AXES

        c["pinned"] = sorted(all_pins, key=AXES.index)
    if pol.floor is not None:
        c["floor"] = _requests(pol.floor)
    ceiling = _requests(pol.ceiling) if pol.ceiling is not None else {}
    if shapes_source == "none":
        # S4 without platform state: clamp increases to the definition
        # ceiling, or hold them at the prior.
        for ax in ("cpu", "memory"):
            if ceiling.get(ax) is None and prior.get(ax) is not None:
                ceiling[ax] = prior[ax]
        c["ceiling_reason"] = "no_platform_state"
    if hold_increases:
        # Placement-group bundles are fixed: an increase could never be
        # scheduled, and an axis the task does not request (no prior) is not
        # in the bundle either.
        for ax in ("cpu", "memory"):
            if prior.get(ax) is not None:
                cap = ceiling.get(ax)
                ceiling[ax] = prior[ax] if cap is None else min(cap, prior[ax])
            else:
                all_pins.add(ax)
        if all_pins:
            from ..types import AXES

            c["pinned"] = sorted(all_pins, key=AXES.index)
        c.setdefault("ceiling_reason", "placement_group")
    if ceiling:
        c["ceiling"] = ceiling
    return c


def platform_ref(max_retries: Any = None) -> Dict[str, Any]:
    ver = ""
    try:
        import ray

        ver = getattr(ray, "__version__", "") or ""
    except Exception:
        pass
    p: Dict[str, Any] = {"kind": PLATFORM, "version": ver, "memory_enforced": False}
    try:
        same = int(max_retries) if max_retries is not None else 3
        p["retry"] = {"same_size": max(0, same)}
    except (TypeError, ValueError):
        pass
    return p


def decide_options(
    options: Mapping[str, Any],
    pol: SizingPolicy,
    *,
    target: Any,
    code: Any = None,
    args: Sequence[Any] = (),
    kwargs: Optional[Mapping[str, Any]] = None,
    inputs: Optional[Sequence[Any]] = None,
    pins: FrozenSet[str] = frozenset(),
    actor: bool = False,
    can_classify: Optional[bool] = None,
    labels: Optional[Mapping[str, str]] = None,
) -> Sized:
    """Decide and merge into a copy of ``options``. Never raises (S1)."""
    opts = dict(options)
    try:
        s = settings()
        if s.engine is None:
            return Sized(options=opts, settings=s)
        from ..types import Context

        prior, defaulted = prior_from_options(opts, actor=actor)
        src, shapes = s.shapes()
        ins = list(inputs) if inputs is not None else inputs_from_args(args, kwargs or {}, pol.scalars)
        ctx = Context(
            key=workload_key(pol.key or default_suffix(target), s.tenant),
            prior={"requests": prior},
            defaulted=defaulted,
            inputs=ins,
            code=code,
            platform=platform_ref(opts.get("max_retries")),
            labels=dict(labels or {}),
        )
        policy: Dict[str, Any] = {}
        mode = pol.mode or s.mode
        if mode is not None:
            policy["mode"] = mode
        for k in ("risk", "estimator", "budget_ms", "allow_gpu_type_change"):
            v = getattr(pol, k)
            if v is not None:
                policy[k] = v
        if can_classify is not None:
            policy["can_classify"] = can_classify
        state: Dict[str, Any] = {"shapes": shapes} if shapes else {}
        cons = _constraints(pol, pins, prior, src, hold_increases=in_placement_group(opts))
        d = s.engine.decide(ctx, constraints=cons, state=state, **policy)
        changed = merge(opts, d, prior, set(pol.pins) | set(pins), s.labelled_accelerators)
        return Sized(options=opts, decision=d, changed=changed, settings=s, inputs=[i.to_dict() for i in ins])
    except Exception as e:  # S1: the call proceeds unchanged
        log.warning("rightsizer.ray: sizing failed, options unchanged (S1): %s", e)
        return Sized(options=dict(options))


def merge(opts: Dict[str, Any], d: Any, prior: Mapping[str, Any], pins: Iterable[str], labelled: bool = False) -> Tuple[str, ...]:
    """Write an enforced decision into Ray options (in place); returns the changed option names."""
    from rightsizer_runtime.wire import Mode

    if d is None or Mode.parse(d.mode) is not Mode.ENFORCE:
        return ()
    pins = set(pins)
    a = d.applied.requests
    changed: List[str] = []
    if "cpu" not in pins and a.cpu is not None and a.cpu != prior.get("cpu"):
        cores = a.cpu / 1000.0
        opts["num_cpus"] = int(cores) if float(cores).is_integer() else cores
        changed.append("num_cpus")
    if "memory" not in pins and a.memory is not None and a.memory != prior.get("memory"):
        opts["memory"] = int(a.memory)
        changed.append("memory")
    pg = prior.get("gpu") or {}
    if "gpu_type" not in pins and a.gpu is not None and pg and a.gpu.accel != pg.get("accel"):
        rt = ray_accelerator(a.gpu.accel, labelled)
        if rt is not None:  # GPU type change (opt-in, M4); count never changes in v1
            opts["accelerator_type"] = rt
            changed.append("accelerator_type")
    return tuple(changed)


__all__ = ["SizingPolicy", "Sized", "decide_options", "default_suffix", "inputs_from_args", "merge", "pins_from",
           "platform_ref", "prior_from_options", "workload_key"]
