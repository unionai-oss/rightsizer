"""Train small sklearn / LightGBM quantile models on synthetic data, export
them, and write the parity fixtures the Rust evaluator checks
(``crates/rightsizer-estimators/tests/trees_parity.rs``).

Regenerate (deterministic; keep the output under 200 KB)::

    .venv/bin/python python/rightsizer/tests/export/make_fixtures.py \\
        [crates/rightsizer-estimators/tests/fixtures]

Files written:

- ``trees_sklearn.json``, ``trees_lightgbm.json`` — ``rightsizer-trees/1`` models;
- ``trees_cases.json`` — input rows (``null`` = missing) and each library's own
  raw predictions per axis and quantile;
- ``novelty.json``, ``novelty_cases.json`` — a ``rightsizer-novelty/1`` file and
  numpy-computed distances / ECDF values for probe rows.
"""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[2] / "src"))

from rightsizer.export import export_lightgbm, export_novelty, export_sklearn_hgb  # noqa: E402
from rightsizer.export._novelty import mean_knn_distances  # noqa: E402

DEFAULT_OUT = HERE.parents[4] / "crates" / "rightsizer-estimators" / "tests" / "fixtures"

FEATURES = [
    "inputs.log2_bytes_total",
    "fmt.parquet.log2_bytes",
    "tok.groupby",
    "hist.log2_peak_max_mib",
    "scalar.batch",
]
QUANTILES = (0.1, 0.5, 0.9)


def data(n: int = 400, seed: int = 0):
    rng = np.random.default_rng(seed)
    X = np.empty((n, len(FEATURES)))
    X[:, 0] = rng.uniform(20, 36, n)                      # log2 bytes
    X[:, 1] = np.where(rng.random(n) < 0.5, X[:, 0], 0.0)  # parquet share (zeros)
    X[:, 2] = (rng.random(n) < 0.3).astype(float)         # a code token
    X[:, 3] = rng.uniform(8, 16, n)                       # history peak
    X[:, 3][rng.random(n) < 0.25] = np.nan                # cold starts: missing
    X[:, 4] = rng.integers(1, 64, n).astype(float)        # a scalar
    y = 0.6 * X[:, 0] - 4 + 1.5 * X[:, 2] + 0.3 * np.nan_to_num(X[:, 3], nan=10.0) + rng.normal(0, 0.4, n)
    cpu = 0.5 + 0.05 * X[:, 4] + rng.gamma(2.0, 0.2, n)
    return X, y, cpu


def probes(X: np.ndarray) -> np.ndarray:
    """Rows that exercise every branch kind: training rows, all-NaN, zeros,
    NaN in a feature that had none in training, extremes."""
    P = [X[i].copy() for i in range(0, len(X), 25)]
    P.append(np.full(len(FEATURES), np.nan))
    P.append(np.zeros(len(FEATURES)))
    r = X[3].copy()
    r[0] = np.nan
    P.append(r)
    r = X[4].copy()
    r[4] = np.nan
    r[1] = 0.0
    P.append(r)
    P.append(np.array([40.0, 40.0, 1.0, 20.0, 1000.0]))
    P.append(np.array([-5.0, -1e-40, 0.0, -3.0, 0.0]))
    return np.array(P)


def train_sklearn(X, y, cpu):
    from sklearn.ensemble import HistGradientBoostingRegressor

    def fit(q, target):
        return HistGradientBoostingRegressor(loss="quantile", quantile=q, max_iter=8, max_leaf_nodes=6,
                                             learning_rate=0.2, random_state=0).fit(X, target)

    return {"memory": {q: fit(q, y) for q in QUANTILES}, "cpu_mean": {0.7: fit(0.7, cpu)}}


def train_lightgbm(X, y, cpu):
    import lightgbm as lgb

    def fit(q, target, zero_as_missing=False):
        params = {"objective": "quantile", "alpha": q, "num_leaves": 6, "learning_rate": 0.2, "min_data_in_leaf": 10,
                  "verbose": -1, "seed": 0, "deterministic": True, "num_threads": 1, "zero_as_missing": zero_as_missing}
        return lgb.train(params, lgb.Dataset(X, target, feature_name=[f"f{i}" for i in range(X.shape[1])]), num_boost_round=8)

    # cpu_mean with zero_as_missing exercises LightGBM's "Zero" missing type.
    return {"memory": {q: fit(q, y) for q in QUANTILES}, "cpu_mean": {0.7: fit(0.7, cpu, zero_as_missing=True)}}


def predictions_sklearn(models, P):
    return {a: {str(q): m.predict(P).tolist() for q, m in qs.items()} for a, qs in models.items()}


def predictions_lightgbm(models, P):
    return {a: {str(q): m.predict(P, raw_score=True).tolist() for q, m in qs.items()} for a, qs in models.items()}


TARGETS = {"memory": ("log2", 1048576.0), "cpu_mean": ("identity", 1.0)}


def build(out: Path | None = None) -> dict:
    """Train, export and (when ``out`` is given) write every fixture; returns
    the documents and expected values."""
    X, y, cpu = data()
    P = probes(X)
    sk = train_sklearn(X, y, cpu)
    lg = train_lightgbm(X, y, cpu)
    w = (lambda name: out / name) if out is not None else (lambda name: None)
    sk_doc = export_sklearn_hgb(sk, FEATURES, w("trees_sklearn.json"), targets=TARGETS, version="fixture-sklearn")
    lg_doc = export_lightgbm(lg, FEATURES, w("trees_lightgbm.json"), targets=TARGETS, version="fixture-lightgbm")
    rows = [[None if math.isnan(v) else float(v) for v in r] for r in P]
    cases = {"feature_names": FEATURES, "rows": rows,
             "expected_raw": {"sklearn": predictions_sklearn(sk, P), "lightgbm": predictions_lightgbm(lg, P)}}

    # Novelty: reference = first 300 rows, held out = the rest.
    ref, held = X[:300], X[300:]
    k = 5
    nov = export_novelty(ref, k, mean_knn_distances(ref, held, k), w("novelty.json"), feature_names=FEATURES)
    probe_rows = P[:8]
    d = mean_knn_distances(ref, probe_rows, k)
    knots = np.array(nov["ecdf"])
    ecdf = [float(np.searchsorted(knots, v, side="right") / len(knots)) for v in d]
    nov_cases = {"rows": [[None if math.isnan(v) else float(v) for v in r] for r in probe_rows],
                 "distance": d.tolist(), "ecdf": ecdf}
    if out is not None:
        (out / "trees_cases.json").write_text(json.dumps(cases, separators=(",", ":")))
        (out / "novelty_cases.json").write_text(json.dumps(nov_cases, separators=(",", ":")))
    return {"sklearn": sk_doc, "lightgbm": lg_doc, "cases": cases, "novelty": nov, "novelty_cases": nov_cases,
            "models": {"sklearn": sk, "lightgbm": lg}, "probes": P}


if __name__ == "__main__":
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_OUT
    out.mkdir(parents=True, exist_ok=True)
    build(out)
    total = sum(p.stat().st_size for p in out.glob("*.json"))
    print(f"wrote fixtures to {out} ({total / 1024:.1f} KiB)")
