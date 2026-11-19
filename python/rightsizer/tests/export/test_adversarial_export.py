"""Adversarial tests for ``rightsizer.export``: NaN in training, constant
targets and features, file round trips with ``allow_nan=False``."""

from __future__ import annotations

import json
import math

import pytest

np = pytest.importorskip("numpy")
pytest.importorskip("sklearn")

from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor  # noqa: E402

from rightsizer.export import export_sklearn_hgb, raw_scores  # noqa: E402


def _nan_training_data(seed: int = 0):
    rng = np.random.default_rng(seed)
    X = rng.normal(size=(400, 2))
    y = X[:, 0] * 2 + rng.normal(size=400) * 0.1
    X[:100, 0] = np.nan  # missing values with their own target: splits "NaN vs everything" appear
    y[:100] = 10.0
    return X, y


def test_nan_in_training_exports_to_a_file(tmp_path):
    X, y = _nan_training_data()
    m = HistGradientBoostingRegressor(loss="quantile", quantile=0.5, max_iter=20).fit(X, y)
    out = tmp_path / "trees.json"
    doc = export_sklearn_hgb({0.5: m}, ["a", "b"], out)  # used to raise: inf thresholds are not JSON
    back = json.loads(out.read_text())
    for t in back["axes"]["memory"]["quantiles"][0]["trees"]:
        for n in t["nodes"]:
            if "threshold" in n:
                assert math.isfinite(n["threshold"])
    Xt = np.vstack([X[:20], [[np.nan, 0.0], [np.nan, np.nan], [1e30, -1e30], [-1e30, 1e30]]])
    got = np.array(raw_scores(back, Xt.tolist())["memory"][0.5])
    assert np.max(np.abs(got - m.predict(Xt))) < 1e-9
    assert doc == back


def test_constant_target_and_constant_feature():
    X, _ = _nan_training_data(1)
    X[:, 1] = 3.0
    m = HistGradientBoostingRegressor(loss="quantile", quantile=0.9, max_iter=5).fit(X, np.full(400, 7.0))
    doc = export_sklearn_hgb({0.9: m}, ["a", "b"])
    json.dumps(doc, allow_nan=False)
    assert raw_scores(doc, [[0.0, 3.0], [np.nan, np.nan]])["memory"][0.9] == pytest.approx([7.0, 7.0])


def test_trees_classifier_doc_has_finite_thresholds():
    from rightsizer.models.trees import classifier_doc, gpu_probability

    X, y = _nan_training_data(2)
    clf = HistGradientBoostingClassifier(max_iter=10).fit(X, (y > 5).astype(int))
    gdoc = {"classifier": classifier_doc(clf)}
    json.dumps(gdoc, allow_nan=False)
    # finite inputs agree with sklearn (the GPU companion reads missing as 0 by design)
    Xt = X[100:130]
    want = clf.predict_proba(Xt)[:, 1]
    got = [gpu_probability(gdoc, list(x)) for x in Xt]
    assert np.max(np.abs(np.array(got) - want)) < 1e-9
