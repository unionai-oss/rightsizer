"""``rs.ray.data.map_batches`` (ray.md §4).

One decision per operator, from the operator's history and the metadata of
the files given to the read (parquet footers of its paths, read by the
engine's bounded metadata readers). Datasets are never executed for it:
``Dataset.size_bytes()`` would execute the plan. Per-task variation is not
supported: ``ray_remote_args_fn`` takes no arguments and is deprecated in
Ray 2.60 ("will be removed in Ray 2.64", ``data/dataset.py`` lines 215-224).

The read's paths: ``paths=`` when given; otherwise the ``ListFiles``
operator's ``source_paths`` in the dataset's logical plan (Ray 2.60's
datasource-v2 file reads), else ``Dataset.input_files()``. Both are plan
introspection (guarded): no paths → the decision uses history alone.
"""

from __future__ import annotations

import logging
from typing import Any, List, Optional, Sequence

from ._sizing import MAX_URI_INPUTS, SizingPolicy, decide_options, default_suffix

log = logging.getLogger("rightsizer.ray")

_RESOURCE_KEYS = ("num_cpus", "num_gpus", "memory")


def read_paths(ds: Any) -> List[str]:
    """Input paths of ``ds``'s read operators, from its logical plan (never executes it)."""
    out: List[str] = []
    try:
        dag = ds._logical_plan.dag
        for op in dag.post_order_iter():
            for attr in ("source_paths", "paths", "_paths"):
                v = getattr(op, attr, None)
                if isinstance(v, str):
                    v = [v]
                if isinstance(v, (list, tuple)) and v and all(isinstance(p, str) for p in v):
                    out.extend(v)
                    break
    except Exception as e:
        log.debug("rightsizer.ray.data: no paths from the logical plan: %s", e)
    if not out:
        try:
            out = list(ds._logical_plan.input_files() or [])
        except Exception:
            out = []
    seen, uniq = set(), []
    for p in out:
        if p not in seen:
            seen.add(p)
            uniq.append(p)
    return uniq


def map_batches(ds: Any, fn: Any, *, key: Optional[str] = None, paths: Optional[Sequence[str]] = None,
                mode: Optional[str] = None, risk: Any = None, estimator: Any = None, budget_ms: Optional[int] = None,
                ceiling: Any = None, floor: Any = None, pin: Any = None, **kwargs: Any) -> Any:
    """``ds.map_batches(fn, **kwargs)`` with ``num_cpus`` / ``memory`` decided once for the operator.

    ``num_cpus``, ``num_gpus`` and ``memory`` in ``kwargs`` are the prior
    (the operator's definition); ``pin=`` pins axes. Every other keyword
    (``batch_size``, ``concurrency``, ``resources``, ``ray_remote_args_fn``…)
    is passed through untouched. The decision is available as
    ``map_batches.last_decision``. Never raises for rightsizer's sake (S1).
    """
    from ..types import Input, _axes

    try:
        pol = SizingPolicy(key=key or default_suffix(fn), mode=mode, risk=risk, estimator=estimator, budget_ms=budget_ms,
                           ceiling=ceiling, floor=floor,
                           pins=frozenset(_axes([pin] if isinstance(pin, str) else list(pin or ()))))
        ps = list(paths) if paths is not None else read_paths(ds)
        inputs = [Input.from_uri(p, f"path{i}") for i, p in enumerate(ps[:MAX_URI_INPUTS])]
        prior_opts = {k: kwargs[k] for k in _RESOURCE_KEYS if kwargs.get(k) is not None}
        actor = not callable(fn) or isinstance(fn, type)
        code = None
        try:
            from ..types import Code

            code = Code.from_callable(fn)
        except Exception:
            pass
        sized = decide_options(prior_opts, pol, target=fn, code=code, inputs=inputs, actor=actor,
                               labels={"ray.operator": "map_batches"})
        map_batches.last_decision = sized.decision  # type: ignore[attr-defined]
        for k in sized.changed:
            kwargs[k] = sized.options[k]
    except Exception as e:  # S1
        log.warning("rightsizer.ray.data: sizing failed, operator unchanged (S1): %s", e)
    return ds.map_batches(fn, **kwargs)


map_batches.last_decision = None  # type: ignore[attr-defined]

__all__ = ["map_batches", "read_paths"]
