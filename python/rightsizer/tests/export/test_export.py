"""Parity of the rightsizer-trees/1 and rightsizer-novelty/1 exporters.

- the pure-Python reference evaluator against sklearn's / LightGBM's own
  predictions (≤ 1e-9 relative here; the spec asks ≤ 1e-6);
- the committed fixtures against the reference evaluator, so the Rust test
  (``cargo test -p rightsizer-estimators --test trees_parity``) checks the
  same numbers;
- optionally (``RIGHTSIZER_RUST_PARITY=1``) the Rust evaluator on freshly
  exported models, through ``cargo test`` with ``RIGHTSIZER_TREES_FIXTURES``.
"""

from __future__ import annotations

import json
import math
import os
import shutil
import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "src"))
sys.path.insert(0, str(HERE))

import make_fixtures as mf  # noqa: E402
from rightsizer.export import (  # noqa: E402
    ExportError,
    evaluate_novelty,
    evaluate_trees,
    export_lightgbm,
    export_sklearn_hgb,
    raw_scores,
)
from rightsizer.export._novelty import novelty_distance  # noqa: E402

REPO = HERE.parents[3]
FIXTURES = REPO / "crates" / "rightsizer-estimators" / "tests" / "fixtures"


def close(a, b, rel=1e-9):
    return abs(a - b) <= rel * max(1.0, abs(b))


@pytest.fixture(scope="module")
def built():
    return mf.build()


@pytest.mark.parametrize("lib", ["sklearn", "lightgbm"])
def test_reference_evaluator_matches_library(built, lib):
    doc, P = built[lib], built["probes"]
    rows = [[None if math.isnan(v) else v for v in r] for r in P]
    got = raw_scores(doc, rows)
    for axis, qs in built["models"][lib].items():
        for q, m in qs.items():
            want = m.predict(P) if lib == "sklearn" else m.predict(P, raw_score=True)
            for g, w in zip(got[axis][q], want):
                assert close(g, w), (lib, axis, q, g, w)


def test_lightgbm_uses_every_missing_type(built):
    kinds = {n.get("missing") for ax in built["lightgbm"]["axes"].values() for qe in ax["quantiles"]
             for t in qe["trees"] for n in t["nodes"] if "feature" in n}
    assert {"nan", "zero", "none"} <= kinds, kinds


def test_transforms_and_document_shape(built):
    doc = built["sklearn"]
    assert doc["format"] == "rightsizer-trees/1" and doc["features"] == "features-v1"
    assert doc["feature_names"] == mf.FEATURES
    assert doc["axes"]["memory"]["target"] == {"transform": "log2", "scale": 1048576.0}
    assert [qe["q"] for qe in doc["axes"]["memory"]["quantiles"]] == [0.1, 0.5, 0.9]
    rows = built["cases"]["rows"][:3]
    raw = raw_scores(doc, rows)["memory"][0.5]
    nat = evaluate_trees(doc, rows)["memory"][0.5]
    for r, v in zip(raw, nat):
        assert close(v, 2.0 ** r * 1048576.0)
    for t in doc["axes"]["memory"]["quantiles"][0]["trees"]:
        for i, n in enumerate(t["nodes"]):
            if "feature" in n:
                assert n["left"] > i and n["right"] > i


def test_committed_fixtures_match_reference_evaluator():
    cases = json.loads((FIXTURES / "trees_cases.json").read_text())
    for lib in ("sklearn", "lightgbm"):
        doc = json.loads((FIXTURES / f"trees_{lib}.json").read_text())
        got = raw_scores(doc, cases["rows"])
        for axis, qs in cases["expected_raw"][lib].items():
            for q, want in qs.items():
                for g, w in zip(got[axis][float(q)], want):
                    assert close(g, w), (lib, axis, q)
    nov = json.loads((FIXTURES / "novelty.json").read_text())
    nc = json.loads((FIXTURES / "novelty_cases.json").read_text())
    for row, d, e in zip(nc["rows"], nc["distance"], nc["ecdf"]):
        assert close(novelty_distance(nov, row), d, 1e-9)
        assert evaluate_novelty(nov, row) == e
    assert sum(p.stat().st_size for p in FIXTURES.glob("*.json")) < 200 * 1024


def test_novelty_in_and_out_of_distribution(built):
    nov = built["novelty"]
    assert nov["needs_code"] is True  # a tok.* feature is present
    inside = evaluate_novelty(nov, list(built["probes"][0]))
    far = evaluate_novelty(nov, [1e3] * len(mf.FEATURES))
    assert 0.0 <= inside < 1.0 and far == 1.0


def test_categorical_is_refused():
    from sklearn.ensemble import HistGradientBoostingRegressor

    X = np.column_stack([np.arange(60) % 3, np.arange(60.0)])
    m = HistGradientBoostingRegressor(loss="quantile", quantile=0.5, max_iter=3, categorical_features=[0]).fit(X, X[:, 1])
    with pytest.raises(ExportError, match="categorical"):
        export_sklearn_hgb({0.5: m}, ["c", "x"])


def test_lightgbm_categorical_is_refused():
    import lightgbm as lgb

    rng = np.random.default_rng(1)
    X = np.column_stack([rng.integers(0, 4, 300), rng.normal(size=300)])
    y = X[:, 0] * 3 + X[:, 1]
    b = lgb.train({"objective": "quantile", "alpha": 0.5, "verbose": -1, "min_data_per_group": 5, "cat_smooth": 1},
                  lgb.Dataset(X, y, categorical_feature=[0]), num_boost_round=3)
    with pytest.raises(ExportError, match="categorical"):
        export_lightgbm({0.5: b}, ["c", "x"])


def test_rejects_mismatches():
    from sklearn.ensemble import HistGradientBoostingRegressor

    X = np.random.default_rng(0).normal(size=(50, 2))
    m = HistGradientBoostingRegressor(loss="quantile", quantile=0.9, max_iter=2).fit(X, X[:, 0])
    with pytest.raises(ExportError, match="trained at quantile"):
        export_sklearn_hgb({0.5: m}, ["a", "b"])
    with pytest.raises(ExportError, match="features"):
        export_sklearn_hgb({0.9: m}, ["a"])
    with pytest.raises(ExportError, match="axis"):
        export_sklearn_hgb({"nope": {0.9: m}}, ["a", "b"])
    sq = HistGradientBoostingRegressor(loss="poisson", max_iter=2).fit(X, np.abs(X[:, 0]))
    with pytest.raises(ExportError, match="link"):
        export_sklearn_hgb({0.5: sq}, ["a", "b"])


@pytest.mark.skipif(os.environ.get("RIGHTSIZER_RUST_PARITY") != "1" or shutil.which("cargo") is None,
                    reason="set RIGHTSIZER_RUST_PARITY=1 to run the Rust evaluator on fresh exports")
def test_rust_evaluator_on_fresh_exports(tmp_path):
    mf.build(tmp_path)
    env = {**os.environ, "RIGHTSIZER_TREES_FIXTURES": str(tmp_path)}
    r = subprocess.run(["cargo", "test", "-q", "-p", "rightsizer-estimators", "--test", "trees_parity"],
                       cwd=REPO, env=env, capture_output=True, text=True)
    assert r.returncode == 0, r.stdout + r.stderr
