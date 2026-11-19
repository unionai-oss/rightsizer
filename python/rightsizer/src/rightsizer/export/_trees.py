"""Exporters to ``rightsizer-trees/1`` and a pure-Python reference evaluator.

The format is documented in ``crates/rightsizer-estimators/src/trees.rs``. In
short: per axis, a target transform and one tree ensemble per quantile level;
each tree is a flat node list (root first, children after parents); a split
sends ``x <= threshold`` left and missing values to ``default_left`` under a
``missing`` rule (``"nan"``, ``"zero"`` or ``"none"``).

Thresholds
----------
sklearn's ``HistGradientBoostingRegressor`` bins features for training, but
its predictors also store each split's threshold in **raw feature space**
(``nodes["num_threshold"]``, the bin's upper edge), and ``predict`` on raw
``X`` compares ``x <= num_threshold``; NaN follows ``missing_go_to_left``
(set even for features that had no NaN in training: they go to the child
that saw more samples). ``bin_threshold`` is only for already-binned data, so
the exporter writes ``num_threshold``. The raw score is
``_baseline_prediction + Σ leaf values`` (leaves already include the
learning rate); the quantile loss has an identity link.

LightGBM's ``dump_model()`` gives raw thresholds, ``decision_type``,
``default_left`` and ``missing_type`` per split; the init score is folded
into the first tree's leaves, so ``base`` is 0.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

TREES_FORMAT = "rightsizer-trees/1"
AXES = ("memory", "cpu_mean", "cpu_peak", "gpu_memory", "duration_s")
TRANSFORMS = ("identity", "log2", "ln", "log10")
#: The tuner's memory target: log2(MiB).
LOG2_MIB = ("log2", 1048576.0)

_LGB_MISSING = {"None": "none", "Zero": "zero", "NaN": "nan"}


def finite_threshold(t: float) -> float:
    """A split threshold JSON can carry. sklearn writes ``+inf`` for a split that sends
    every non-missing value left (NaN alone goes right); the largest finite double
    keeps that for every finite ``x``. NaN thresholds are refused."""
    t = float(t)
    if math.isnan(t):
        raise ExportError("NaN split threshold")
    if math.isinf(t):
        return math.copysign(1.7976931348623157e308, t)
    return t


class ExportError(ValueError):
    """The model cannot be represented in ``rightsizer-trees/1``."""


def _by_axis(models: Mapping[Any, Any], axis: str) -> dict[str, dict[float, Any]]:
    """``{q: model}`` (one axis) or ``{axis: {q: model}}``."""
    if not models:
        raise ExportError("no models")
    if all(isinstance(k, str) for k in models):
        out = {a: {float(q): m for q, m in qs.items()} for a, qs in models.items()}
    else:
        out = {axis: {float(q): m for q, m in models.items()}}
    for a, qs in out.items():
        if a not in AXES:
            raise ExportError(f"unknown axis {a!r}; expected one of {AXES}")
        for q in qs:
            if not 0.0 < q < 1.0:
                raise ExportError(f"{a}: quantile level {q} outside (0, 1)")
    return out


def _targets(targets: Mapping[str, Any] | None, axes: Sequence[str]) -> dict[str, dict[str, Any]]:
    out = {}
    for a in axes:
        t = (targets or {}).get(a, LOG2_MIB if a in ("memory", "gpu_memory") else ("identity", 1.0))
        transform, scale = (t["transform"], t.get("scale", 1.0)) if isinstance(t, Mapping) else t
        if transform not in TRANSFORMS:
            raise ExportError(f"{a}: unknown transform {transform!r}")
        if not (isinstance(scale, (int, float)) and scale > 0 and math.isfinite(scale)):
            raise ExportError(f"{a}: target scale must be positive")
        out[a] = {"transform": transform, "scale": float(scale)}
    return out


def _doc(axes: dict, feature_names: Sequence[str], version: str, features: str, source: str, id: str | None) -> dict:
    doc = {
        "format": TREES_FORMAT,
        "version": version,
        "features": features,
        "feature_names": list(feature_names),
        "source": source,
        "axes": axes,
    }
    if id is not None:
        doc["id"] = id
    return doc


def _write(doc: dict, path: str | Path | None) -> dict:
    if path is not None:
        Path(path).write_text(json.dumps(doc, separators=(",", ":"), allow_nan=False))
    return doc


# ── sklearn ─────────────────────────────────────────────────────────────


def _sklearn_trees(model: Any, n_features: int, where: str) -> tuple[float, list[dict]]:
    from sklearn.ensemble import HistGradientBoostingRegressor

    if not isinstance(model, HistGradientBoostingRegressor):
        raise ExportError(f"{where}: expected HistGradientBoostingRegressor, got {type(model).__name__}")
    if not hasattr(model, "_predictors"):
        raise ExportError(f"{where}: model is not fitted")
    link = type(getattr(model._loss, "link", None)).__name__
    if link != "IdentityLink":
        raise ExportError(f"{where}: loss {model.loss!r} has a {link}; only identity links are exportable")
    if getattr(model, "_preprocessor", None) is not None:
        raise ExportError(f"{where}: categorical features are not supported by {TREES_FORMAT}")
    cats = getattr(model, "is_categorical_", None)
    if cats is not None and bool(cats.any()):
        raise ExportError(f"{where}: categorical features are not supported by {TREES_FORMAT}")
    if model.n_trees_per_iteration_ != 1:
        raise ExportError(f"{where}: {model.n_trees_per_iteration_} trees per iteration; expected 1")
    if model.n_features_in_ != n_features:
        raise ExportError(f"{where}: model has {model.n_features_in_} features, feature_names has {n_features}")
    base = float(model._baseline_prediction.ravel()[0])
    trees = []
    for it in model._predictors:
        nodes = it[0].nodes
        names = nodes.dtype.names
        thr = "num_threshold" if "num_threshold" in names else "threshold"
        out = []
        for i, n in enumerate(nodes):
            if n["is_leaf"]:
                out.append({"leaf": float(n["value"])})
                continue
            if "is_categorical" in names and n["is_categorical"]:
                raise ExportError(f"{where}: categorical split; not supported by {TREES_FORMAT}")
            left, right = int(n["left"]), int(n["right"])
            if left <= i or right <= i:
                raise ExportError(f"{where}: node {i} has a child before it")
            out.append({
                "feature": int(n["feature_idx"]),
                "threshold": finite_threshold(n[thr]),
                "left": left,
                "right": right,
                "default_left": bool(n["missing_go_to_left"]),
                "missing": "nan",
            })
        trees.append({"nodes": out})
    return base, trees


def export_sklearn_hgb(
    models_by_quantile: Mapping[Any, Any],
    feature_names: Sequence[str],
    path: str | Path | None = None,
    *,
    axis: str = "memory",
    targets: Mapping[str, Any] | None = None,
    version: str = "0",
    features: str = "features-v1",
    id: str | None = None,
) -> dict:
    """Export fitted ``HistGradientBoostingRegressor`` models.

    ``models_by_quantile`` is ``{q: model}`` for one ``axis`` or
    ``{axis: {q: model}}``. ``targets`` maps an axis to ``(transform, scale)``
    (default ``("log2", 2**20)`` — log2 MiB — for memory axes, identity
    otherwise). Writes compact JSON to ``path`` if given; returns the document.
    """
    by = _by_axis(models_by_quantile, axis)
    tg = _targets(targets, list(by))
    axes = {}
    for a, qs in by.items():
        quantiles = []
        for q in sorted(qs):
            m = qs[q]
            if getattr(m, "loss", None) == "quantile" and not math.isclose(float(m.quantile), q, abs_tol=1e-9):
                raise ExportError(f"{a}: model under key {q} was trained at quantile {m.quantile}")
            base, trees = _sklearn_trees(m, len(feature_names), f"{a} q={q}")
            quantiles.append({"q": q, "base": base, "trees": trees})
        axes[a] = {"target": tg[a], "quantiles": quantiles}
    return _write(_doc(axes, feature_names, version, features, "sklearn-hgb", id), path)


# ── LightGBM ────────────────────────────────────────────────────────────


def _lgb_flatten(tree: dict, where: str) -> list[dict]:
    nodes: list[dict] = []

    def visit(n: dict) -> int:
        i = len(nodes)
        if "leaf_value" in n:
            nodes.append({"leaf": float(n["leaf_value"])})
            return i
        if n.get("decision_type", "<=") != "<=":
            raise ExportError(f"{where}: categorical split ({n.get('decision_type')}); not supported by {TREES_FORMAT}")
        missing = _LGB_MISSING.get(n.get("missing_type", "None"))
        if missing is None:
            raise ExportError(f"{where}: unknown missing_type {n.get('missing_type')!r}")
        node = {
            "feature": int(n["split_feature"]),
            "threshold": finite_threshold(n["threshold"]),
            "left": -1,
            "right": -1,
            "default_left": bool(n.get("default_left", True)),
            "missing": missing,
        }
        nodes.append(node)
        node["left"] = visit(n["left_child"])
        node["right"] = visit(n["right_child"])
        return i

    visit(tree)
    return nodes


def _lgb_trees(booster: Any, n_features: int, where: str) -> list[dict]:
    b = getattr(booster, "booster_", booster)
    d = b.dump_model()
    if d.get("num_tree_per_iteration", 1) != 1 or d.get("num_class", 1) != 1:
        raise ExportError(f"{where}: multi-output models are not supported")
    if d.get("average_output"):
        raise ExportError(f"{where}: random-forest (average_output) models are not supported")
    if len(d.get("feature_names", [])) != n_features:
        raise ExportError(f"{where}: model has {len(d.get('feature_names', []))} features, feature_names has {n_features}")
    obj = str(d.get("objective", "")).split()[0] if d.get("objective") else ""
    if obj and obj not in ("quantile", "regression", "regression_l1", "huber", "fair", "l1", "l2", "mape"):
        raise ExportError(f"{where}: objective {obj!r} has a non-identity output transform")
    trees = []
    for t in d["tree_info"]:
        if t.get("is_linear"):
            raise ExportError(f"{where}: linear trees are not supported")
        trees.append({"nodes": _lgb_flatten(t["tree_structure"], where)})
    return trees


def export_lightgbm(
    booster_by_quantile: Mapping[Any, Any],
    feature_names: Sequence[str] | None = None,
    path: str | Path | None = None,
    *,
    axis: str = "memory",
    targets: Mapping[str, Any] | None = None,
    version: str = "0",
    features: str = "features-v1",
    id: str | None = None,
) -> dict:
    """Export LightGBM boosters (``lightgbm.Booster`` or ``LGBMRegressor``)
    trained with ``objective="quantile"`` (``alpha = q``), from
    ``dump_model()``. Arguments as in :func:`export_sklearn_hgb`;
    ``feature_names`` defaults to the booster's."""
    by = _by_axis(booster_by_quantile, axis)
    if feature_names is None:
        first = next(iter(next(iter(by.values())).values()))
        feature_names = list(getattr(first, "booster_", first).feature_name())
    tg = _targets(targets, list(by))
    axes = {}
    for a, qs in by.items():
        quantiles = [{"q": q, "base": 0.0, "trees": _lgb_trees(qs[q], len(feature_names), f"{a} q={q}")} for q in sorted(qs)]
        axes[a] = {"target": tg[a], "quantiles": quantiles}
    return _write(_doc(axes, feature_names, version, features, "lightgbm", id), path)


# ── reference evaluator ─────────────────────────────────────────────────


def _eval_tree(nodes: list[dict], x: Sequence[float]) -> float:
    i = 0
    while True:
        n = nodes[i]
        if "leaf" in n:
            return n["leaf"]
        v = x[n["feature"]] if n["feature"] < len(x) else math.nan
        v = math.nan if v is None else float(v)
        m = n.get("missing", "nan")
        if m != "nan" and math.isnan(v):
            v = 0.0
        missing = math.isnan(v) if m == "nan" else (abs(v) <= 1e-35 if m == "zero" else False)
        left = n.get("default_left", False) if missing else v <= n["threshold"]
        i = n["left"] if left else n["right"]


def _inverse(target: dict, raw: float) -> float:
    t = target.get("transform", "identity")
    v = {"identity": raw, "log2": 2.0 ** raw, "ln": math.exp(raw), "log10": 10.0 ** raw}[t]
    return v * target.get("scale", 1.0)


def raw_scores(doc: Mapping, X: Sequence[Sequence[float]]) -> dict[str, dict[float, list[float]]]:
    """``{axis: {q: [raw score per row]}}``: base + Σ trees, in file order.
    ``None`` / NaN entries are missing."""
    out: dict[str, dict[float, list[float]]] = {}
    for a, ax in doc["axes"].items():
        out[a] = {}
        for qe in ax["quantiles"]:
            rows = []
            for x in X:
                s = qe.get("base", 0.0)
                for t in qe["trees"]:
                    s += _eval_tree(t["nodes"], x)
                rows.append(s)
            out[a][qe["q"]] = rows
    return out


def evaluate_trees(doc: Mapping, X: Sequence[Sequence[float]]) -> dict[str, dict[float, list[float]]]:
    """``{axis: {q: [value per row]}}`` in natural units (after the target
    transform), as the Rust evaluator computes them."""
    raw = raw_scores(doc, X)
    return {a: {q: [_inverse(doc["axes"][a].get("target", {}), r) for r in rs] for q, rs in qs.items()} for a, qs in raw.items()}
