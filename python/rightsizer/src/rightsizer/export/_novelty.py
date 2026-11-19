"""``rightsizer-novelty/1``: the kNN novelty companion of a trees file.

Format and evaluation are documented in
``crates/rightsizer-estimators/src/novelty.rs``. Standardization follows the
tuner's ``novelty_knn``: ``z = (x - mean) / sd`` on the reference (training)
rows, with a feature whose sd is ≤ 1e-6 keeping its raw unit (scale 1), and
missing features filled with a raw 0.
"""

from __future__ import annotations

import bisect
import json
import math
from collections.abc import Sequence
from pathlib import Path

import numpy as np

NOVELTY_FORMAT = "rightsizer-novelty/1"
SD_FLOOR = 1e-6


def _clean(X, fill: float) -> np.ndarray:
    a = np.array([[fill if v is None else v for v in row] for row in X], dtype=float)
    a[np.isnan(a)] = fill
    return a


def standardization(reference_matrix, fill: float = 0.0) -> tuple[np.ndarray, np.ndarray]:
    """``(mean, scale)`` of the raw reference rows."""
    R = _clean(reference_matrix, fill)
    mu, sd = R.mean(0), R.std(0)
    return mu, np.where(sd > SD_FLOOR, sd, 1.0)


def mean_knn_distances(reference_matrix, X, k: int, *, mean=None, scale=None, fill: float = 0.0) -> np.ndarray:
    """Mean Euclidean distance from each raw row of ``X`` to its ``k`` nearest
    raw reference rows, in the reference's standardized space."""
    R = _clean(reference_matrix, fill)
    if mean is None or scale is None:
        mean, scale = standardization(R, fill)
    Z = (R - mean) / scale
    Q = (_clean(X, fill) - mean) / scale
    d = np.sqrt(((Q[:, None, :] - Z[None, :, :]) ** 2).sum(-1))
    k = min(k, len(Z))
    return np.sort(d, axis=1)[:, :k].mean(1)


def export_novelty(
    reference_matrix,
    k: int,
    heldout_distances: Sequence[float],
    path: str | Path | None = None,
    *,
    feature_names: Sequence[str],
    mean=None,
    scale=None,
    fill: float = 0.0,
    needs_code: bool | None = None,
    features: str = "features-v1",
) -> dict:
    """Write a ``rightsizer-novelty/1`` document.

    ``reference_matrix`` holds raw (unstandardized) training rows;
    ``heldout_distances`` are mean kNN distances of held-out in-distribution
    rows (:func:`mean_knn_distances`), whose ECDF the evaluator returns.
    ``needs_code`` defaults to "some feature is a code token" (``tok.*``).
    """
    R = _clean(reference_matrix, fill)
    if R.ndim != 2 or R.shape[1] != len(feature_names) or len(R) == 0:
        raise ValueError("reference_matrix must be a non-empty (n, len(feature_names)) matrix")
    if k < 1 or len(heldout_distances) == 0:
        raise ValueError("k must be ≥ 1 and heldout_distances non-empty")
    if mean is None or scale is None:
        mean, scale = standardization(R, fill)
    mean, scale = np.asarray(mean, dtype=float), np.asarray(scale, dtype=float)
    if (scale <= 0).any():
        raise ValueError("scale must be positive")
    knots = sorted(float(d) for d in heldout_distances)
    if not all(math.isfinite(d) for d in knots):
        raise ValueError("heldout_distances must be finite")
    doc = {
        "format": NOVELTY_FORMAT,
        "features": features,
        "feature_names": list(feature_names),
        "mean": mean.tolist(),
        "scale": scale.tolist(),
        "fill": float(fill),
        "reference": ((R - mean) / scale).tolist(),
        "k": int(k),
        "ecdf": knots,
        "needs_code": bool(any(n.startswith("tok.") for n in feature_names) if needs_code is None else needs_code),
    }
    if path is not None:
        Path(path).write_text(json.dumps(doc, separators=(",", ":"), allow_nan=False))
    return doc


def novelty_distance(doc, x: Sequence[float | None]) -> float:
    """Mean distance of one raw row to its ``k`` nearest reference rows."""
    fill = doc.get("fill", 0.0)
    z = [((fill if (v is None or (isinstance(v, float) and math.isnan(v))) else v) - m) / s for v, m, s in zip(x, doc["mean"], doc["scale"])]
    d = sorted(math.sqrt(sum((a - b) ** 2 for a, b in zip(r, z))) for r in doc["reference"])
    k = min(doc["k"], len(d))
    return sum(d[:k]) / k


def evaluate_novelty(doc, x: Sequence[float | None]) -> float:
    """ECDF value in [0, 1]: the fraction of knots ≤ the row's distance."""
    knots = doc["ecdf"]
    return bisect.bisect_right(knots, novelty_distance(doc, x)) / len(knots)
