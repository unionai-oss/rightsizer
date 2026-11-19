"""``rightsizer.models.trees.convert`` on a small skops release built with the
published structure (``tuner:bench_all/hub.py:stage_trees``)."""

from __future__ import annotations

import json
import math
import os

import pytest

np = pytest.importorskip("numpy")
pytest.importorskip("sklearn")
sio = pytest.importorskip("skops.io")

from rightsizer.models import trees

LEVELS = [0.1, 0.5, 0.9]


@pytest.fixture(scope="module")
def names(tuner):
    n = tuner["features"].feature_names()
    assert len(n) == 209
    return n


@pytest.fixture(scope="module")
def release(tmp_path_factory, names):
    from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor

    rng = np.random.default_rng(0)
    n = 600
    X = rng.normal(size=(n, len(names))).astype(np.float32)
    X[:, rng.choice(len(names), 120, replace=False)] = (rng.random((n, 120)) < 0.3).astype(np.float32)  # tokens
    idx = {k: i for i, k in enumerate(names)}
    y_mem = 10 + 1.5 * X[:, idx["log_total_bytes"]] + X[:, idx["m_tok_read_parquet"]] + 0.3 * rng.normal(size=n)
    y_cpu = 0.5 * X[:, idx["c_max_pool"]] + X[:, idx["hist_mean_cores"]] + 0.2 * rng.normal(size=n)
    is_gpu = X[:, idx['cuda_"cuda"']] > 0.5
    y_gpu = 2 + X[:, idx["g_log_max_linear"]] + 0.2 * rng.normal(size=n)

    def q(y, rows=slice(None)):
        return [HistGradientBoostingRegressor(loss="quantile", quantile=lv, max_iter=25, max_leaf_nodes=8,
                                              random_state=0).fit(X[rows], y[rows]) for lv in LEVELS]

    state = {"clf": HistGradientBoostingClassifier(max_iter=20, random_state=0).fit(X, is_gpu),
             "mem": q(y_mem, ~is_gpu), "cpu": q(y_cpu, ~is_gpu), "gpu": q(y_gpu, is_gpu)}
    d = tmp_path_factory.mktemp("trees")
    sio.dump(state, str(d / "model.skops"))
    cfg = {"release": "test-1", "approach": "gbt-dist", "levels": LEVELS, "gpu_threshold": 0.55, "features": names,
           "targets": {"mem": "log2(memory need, MiB)", "cpu": "log2(busiest-15 s cores, floor 0.05)",
                       "gpu": "log2(peak GPU memory, GiB, floor 0.1)"}, "n_train": n, "refit_matches_published": 1.0}
    (d / "rightsize_config.json").write_text(json.dumps(cfg))
    return str(d), state, X


def test_skops_types_are_allowlisted(release):
    d, _, _ = release
    untrusted = sio.get_untrusted_types(file=os.path.join(d, "model.skops"))
    assert set(untrusted) <= set(trees.TRUSTED_TYPES), sorted(set(untrusted) - set(trees.TRUSTED_TYPES))


def test_unknown_types_are_refused(tmp_path):
    import collections

    sio.dump({"x": collections.Counter("ab")}, str(tmp_path / "m.skops"))
    with pytest.raises(ValueError, match="allowlist"):
        trees.load_skops(str(tmp_path / "m.skops"), trusted=())


def test_feature_mapping(names):
    written, mapped, unmapped = trees.feature_mapping(names)
    assert len(written) == 209 and len(mapped) + len(unmapped) == 209
    assert mapped["log_total_bytes"] == "inputs.log2_bytes_total"
    assert mapped["m_tok_read_parquet"] == "tok.read_parquet" and mapped["c_tok_groupby"] == "tok.groupby"
    assert mapped['cuda_"cuda"'] == 'tok."cuda"' and mapped["k8s_kind: Job"] == "tok.kind: Job"
    assert "g_log_max_linear" in unmapped and "ncol_f64" in unmapped and "hist_peak_cores" in unmapped
    assert all(w.startswith(trees.UNMAPPED_PREFIX) for n, w in zip(names, written) if n in unmapped)
    print(f"mapped {len(mapped)}, unmapped {len(unmapped)}: {unmapped}")


def test_convert_matches_sklearn(release, names, tmp_path):
    from rightsizer.export import evaluate_trees

    d, state, X = release
    out = str(tmp_path / "trees.json")
    rep = trees.convert(d, None, out)
    assert rep["files"] == [out, str(tmp_path / "trees.gpu.json")]
    doc = json.load(open(out))
    assert doc["format"] == "rightsizer-trees/1" and set(doc["axes"]) == {"memory", "cpu_mean"}
    assert doc["feature_names"][names.index("log_total_bytes")] == "inputs.log2_bytes_total"
    unm = set(rep["unmapped"])
    Xz = X[:40].astype(np.float64).copy()
    Xz[:, [i for i, n in enumerate(names) if n in unm]] = 0.0  # unmapped -> missing -> 0 ("none")
    rows = [[None if (names[j] in unm) else float(v) for j, v in enumerate(r)] for r in Xz]
    got = evaluate_trees(doc, rows)
    for axis, key, scale in (("memory", "mem", 2**20 / 1.05), ("cpu_mean", "cpu", 1.0)):
        for lv, m in zip(LEVELS, state[key]):
            want = 2.0 ** m.predict(Xz.astype(np.float32)) * scale
            assert np.allclose(got[axis][lv], want, rtol=1e-6), axis
    gdoc = json.load(open(tmp_path / "trees.gpu.json"))
    assert gdoc["format"] == trees.GPU_FORMAT and gdoc["gpu_threshold"] == 0.55
    p = [trees.gpu_probability(gdoc, r) for r in rows]
    assert np.allclose(p, state["clf"].predict_proba(Xz.astype(np.float32))[:, 1], atol=1e-6)
    g = evaluate_trees(gdoc, rows)["gpu_memory"]
    for lv, m in zip(LEVELS, state["gpu"]):
        assert np.allclose(g[lv], 2.0 ** m.predict(Xz.astype(np.float32)) * 2**30, rtol=1e-6)
    assert all(n.get("missing") == "none" for a in doc["axes"].values() for q in a["quantiles"]
               for t in q["trees"] for n in t["nodes"] if "leaf" not in n)
    assert not math.isnan(sum(got["memory"][0.5]))
    # memory_basis="need" keeps the trained label (no headroom division).
    need = str(tmp_path / "need.json")
    assert trees.convert(d, None, need, gpu=False, memory_basis="need")["memory_basis"] == "need"
    got_need = evaluate_trees(json.load(open(need)), rows)["memory"]
    for lv in LEVELS:
        assert np.allclose(got_need[lv], np.array(got["memory"][lv]) * 1.05, rtol=1e-9)


def test_cli_convert(release, tmp_path, capsys):
    from rightsizer.models.cli import main

    d, _, _ = release
    assert main(["convert", d, "--out", str(tmp_path / "t.json"), "--no-gpu"]) == 0
    rep = json.loads(capsys.readouterr().out)
    assert rep["files"] == [str(tmp_path / "t.json")] and rep["n_features"] == 209
