"""Convert the published trees (``unionai/rightsize-trees``) to
``rightsizer-trees/1`` for the Rust ``gbt`` estimator (models.md §1, SPEC §6.2).

The repo holds ``model.skops`` -- a dict ``{"clf", "mem", "cpu", "gpu"}``: the
GPU classifier (``HistGradientBoostingClassifier``, P(task uses a GPU)) and one
``HistGradientBoostingRegressor(loss="quantile")`` per level in
``rightsize_config.json["levels"]`` for each target -- and
``rightsize_config.json`` (``features``: the 209 tuner feature names,
``levels``, ``gpu_threshold``, ``targets``)
(``tuner:bench_all/hub.py:stage_trees``, ``bench_all/arms.py:fit_gbt_dist``).

What :func:`convert` writes:

- ``out`` (``rightsizer-trees/1``): axes ``memory`` and ``cpu_mean``.
  - ``memory``'s tuner target is ``log2(need MiB)``, need = (peak + runtime) x
    1.05; the file's scale ``2^20 / 1.05`` removes the headroom, so the axis
    reads as the container peak (rightsizer adds runtime overhead and headroom
    itself; on Flyte the 70 MiB runtime is still counted twice -- conservative).
    This is ``memory_basis="demand"``, the default; ``"need"`` keeps the
    trained label (scale ``2^20``).
  - ``cpu_mean``'s tuner target is ``log2(cores)`` of the busiest 15 s window
    (RightSizeCPU rows) or the mean (RightSizeMem rows).
- ``<out stem>.gpu.json`` (``rightsizer-trees-gpu/1``, when ``gpu=True``): the
  classifier as a logistic tree ensemble, the ``gpu_memory`` quantile axis
  (``log2(GiB)``), and the release's threshold. The Rust ``gbt`` estimator has
  no classifier yet, and a ``gpu_memory`` axis without one would put every task
  on a GPU, so GPU trees are kept out of the main file.

Features are mapped by name to ``features-v1`` (:data:`FEATURE_MAP`); a tuner
feature with no counterpart is written as ``tuner.<name>``, which the
evaluator does not know, so it is missing. Every split is written with
``missing: "none"`` (a missing value is read as 0 and compared): the tuner
computed absent features as 0, never NaN.
"""

from __future__ import annotations

import json
import math
import os
from collections.abc import Mapping, Sequence
from typing import Any

MODEL_FILE = "model.skops"
CONFIG_FILE = "rightsize_config.json"
GPU_FORMAT = "rightsizer-trees-gpu/1"

#: The types ``model.skops`` may contain (anything else is refused).
TRUSTED_TYPES = (
    "sklearn.ensemble._hist_gradient_boosting.gradient_boosting.HistGradientBoostingRegressor",
    "sklearn.ensemble._hist_gradient_boosting.gradient_boosting.HistGradientBoostingClassifier",
    "sklearn.ensemble._hist_gradient_boosting.predictor.TreePredictor",
    "sklearn.ensemble._hist_gradient_boosting.binning._BinMapper",
    "sklearn._loss.loss.PinballLoss",
    "sklearn._loss.loss.HalfBinomialLoss",
    "sklearn._loss.link.IdentityLink",
    "sklearn._loss.link.LogitLink",
    "sklearn._loss.link.Interval",
    "sklearn._loss._loss.CyPinballLoss",
    "sklearn._loss._loss.CyHalfBinomialLoss",
    "sklearn.utils._bunch.Bunch",
    "numpy.dtype",
    "numpy.random.mtrand.RandomState",
    "numpy.random._mt19937.MT19937",
    "numpy.random._pickle.__randomstate_ctor",
    "numpy.random._pickle.__bit_generator_ctor",
    "numpy.random.bit_generator.__pyx_unpickle_SeedSequence",
    "numpy.random.bit_generator.SeedSequence",
    "builtins.slice",
    "collections.OrderedDict",
)

# ── feature names: tuner -> features-v1 ─────────────────────────────────

_DIRECT = {
    "log_total_bytes": "inputs.log2_bytes_total",
    "n_inputs": "inputs.count",
    "log_bytes_parquet": "fmt.parquet.log2_bytes",
    "log_bytes_csv": "fmt.csv.log2_bytes",
    "log_bytes_jsonl": "fmt.jsonl.log2_bytes",
    "log_bytes_npy": "fmt.npy.log2_bytes",
    "log_rows_visible": "inputs.log2_rows",
    "ncols": "cols.total",
    "npy_log_n": "npy.log2_n",
    "npy_log_row": "npy.log2_row",
    "npy_itemsize": "npy.itemsize",
    "m_code_lines": "code.lines",
    "c_code_lines": "code.lines",
    "g_code_lines": "code.lines",
    # The tuner's log2(max integer literal > 1), else 0: Rust's `max_literal=` token.
    "m_log_max_literal": "code.max_literal_log2",
    "c_log_max_literal": "code.max_literal_log2",
    "g_log_max_literal": "code.max_literal_log2",
    "n_hist": "hist.n_success",
    "log_hist_mem": "hist.log2_peak_max_mib",
    "log_hist_mem_scaled": "hist.log2_scaled_mib",
    "log_hist_size_ratio": "hist.log2_size_ratio",
    "hist_mean_cores": "hist.cpu_mean_max",
}
# Token features are named `tok.<raw token>` (the tuner's suffix verbatim); the
# Rust `features-v1` vector reads `CodeInfo.tokens` entries `tok:<raw token>`
# (crates/rightsizer-estimators/src/features_v1.rs, rightsizer_features::tokens).
_TOKEN_PREFIXES = ("m_tok_", "c_tok_", "g_tok_", "cuda_", "k8s_")
TOKEN_FEATURE_PREFIX = "tok."
UNMAPPED_PREFIX = "tuner."


def map_feature(name: str) -> str | None:
    """The ``features-v1`` name for a tuner tree feature, or None."""
    if name in _DIRECT:
        return _DIRECT[name]
    for p in _TOKEN_PREFIXES:
        if name.startswith(p):
            return TOKEN_FEATURE_PREFIX + name[len(p):]
    return None


def feature_mapping(names: Sequence[str]) -> tuple[list[str], dict[str, str], list[str]]:
    """``(names as written, {tuner: features-v1}, unmapped tuner names)``."""
    written, mapped, unmapped = [], {}, []
    for n in names:
        m = map_feature(n)
        if m is None:
            unmapped.append(n)
            written.append(UNMAPPED_PREFIX + n)
        else:
            mapped[n] = m
            written.append(m)
    return written, mapped, unmapped


FEATURE_MAP = _DIRECT  # the non-token part, for documentation

# ── loading ─────────────────────────────────────────────────────────────


def resolve(repo_or_dir: str, revision: str | None = None) -> tuple[str, str, str]:
    """``(local dir, repo id, version)``; the version is the commit sha for a Hub repo."""
    if os.path.isdir(repo_or_dir):
        import hashlib

        h = hashlib.sha256()
        for name in (MODEL_FILE, CONFIG_FILE):
            with open(os.path.join(repo_or_dir, name), "rb") as f:
                h.update(f.read())
        return os.path.abspath(repo_or_dir), os.path.basename(os.path.abspath(repo_or_dir)), "local-" + h.hexdigest()[:16]
    from huggingface_hub import snapshot_download

    path = snapshot_download(repo_or_dir, revision=revision, allow_patterns=[MODEL_FILE, CONFIG_FILE, "README.md"])
    commit = os.path.basename(os.path.normpath(path))
    return path, repo_or_dir, commit


def load_skops(path: str, trusted: Sequence[str] = TRUSTED_TYPES) -> Any:
    import skops.io as sio

    untrusted = sio.get_untrusted_types(file=path)
    bad = sorted(set(untrusted) - set(trusted))
    if bad:
        raise ValueError(f"{path} contains types outside the allowlist: {bad}")
    return sio.load(path, trusted=list(untrusted))


# ── trees ───────────────────────────────────────────────────────────────


def _nodes(predictor: Any) -> list[dict]:
    nodes = predictor.nodes
    names = nodes.dtype.names
    thr = "num_threshold" if "num_threshold" in names else "threshold"
    out = []
    for i, n in enumerate(nodes):
        if n["is_leaf"]:
            out.append({"leaf": float(n["value"])})
            continue
        if "is_categorical" in names and n["is_categorical"]:
            raise ValueError("categorical split; not supported by rightsizer-trees/1")
        out.append({"feature": int(n["feature_idx"]), "threshold": float(n[thr]), "left": int(n["left"]),
                    "right": int(n["right"]), "default_left": bool(n["missing_go_to_left"]), "missing": "none"})
    return out


def classifier_doc(clf: Any) -> dict:
    """A binary ``HistGradientBoostingClassifier`` as ``{"link": "logistic", "base", "trees"}``:
    P(class 1) = sigmoid(base + sum of trees)."""
    if getattr(clf, "n_trees_per_iteration_", 1) != 1:
        raise ValueError("only binary classifiers are supported")
    return {"link": "logistic", "positive_class": str(clf.classes_[1]),
            "base": float(clf._baseline_prediction.ravel()[0]),
            "trees": [{"nodes": _nodes(it[0])} for it in clf._predictors]}


def _set_missing_none(doc: dict) -> None:
    for axis in doc["axes"].values():
        for q in axis["quantiles"]:
            for t in q["trees"]:
                for n in t["nodes"]:
                    if "leaf" not in n:
                        n["missing"] = "none"


def _export_regressors(models: Mapping[str, Mapping[float, Any]], names: list[str], targets: Mapping[str, tuple],
                       version: str, id: str) -> dict:
    """Through ``rightsizer.export`` (the reference exporter); quantile levels
    are checked against each model's own ``quantile``."""
    from rightsizer.export import export_sklearn_hgb

    doc = export_sklearn_hgb(models, names, None, targets=targets, version=version, features="features-v1", id=id)
    _set_missing_none(doc)
    doc["source"] = "sklearn-hgb (rightsize-trees)"
    return doc


def convert(repo_or_dir: str, revision: str | None, out: str, *, gpu: bool = True, id: str = "gbt",
            memory_basis: str = "demand") -> dict:
    """Convert a trees release; returns a report (files, feature mapping, unmapped features).

    ``memory_basis="demand"`` (default) divides the need label by its
    headroom (scale ``2^20 / 1.05``), the demand rightsizer estimators emit;
    the runtime overhead (70 MiB on Flyte, 0 on K8s) cannot be expressed in
    ``rightsizer-trees/1`` targets and stays in. ``"need"`` keeps the label
    as trained (scale ``2^20``)."""
    if memory_basis not in ("demand", "need"):
        raise ValueError(f"memory_basis must be demand or need, not {memory_basis!r}")
    path, repo, version = resolve(repo_or_dir, revision)
    with open(os.path.join(path, CONFIG_FILE)) as f:
        cfg = json.load(f)
    state = load_skops(os.path.join(path, MODEL_FILE))
    levels = [float(q) for q in cfg["levels"]]
    for k in ("mem", "cpu", "gpu"):
        if len(state[k]) != len(levels):
            raise ValueError(f"{k}: {len(state[k])} models for {len(levels)} levels")
    names, mapped, unmapped = feature_mapping(cfg["features"])
    headroom = float(cfg.get("label_headroom", 1.05)) if memory_basis == "demand" else 1.0
    doc = _export_regressors(
        {"memory": dict(zip(levels, state["mem"])), "cpu_mean": dict(zip(levels, state["cpu"]))}, names,
        {"memory": ("log2", 2**20 / headroom), "cpu_mean": ("log2", 1.0)}, version, id)
    doc["provenance"] = {"repo": repo, "revision": revision, "commit": version, "release": cfg.get("release"),
                         "approach": cfg.get("approach")}
    os.makedirs(os.path.dirname(os.path.abspath(out)) or ".", exist_ok=True)
    with open(out, "w") as f:
        json.dump(doc, f, separators=(",", ":"), allow_nan=False)
    files = [out]
    if gpu:
        g = _export_regressors({"gpu_memory": dict(zip(levels, state["gpu"]))}, names,
                               {"gpu_memory": ("log2", float(2**30))}, version, id)
        gdoc = {"format": GPU_FORMAT, "version": version, "features": "features-v1", "feature_names": names,
                "classifier": classifier_doc(state["clf"]), "gpu_threshold": float(cfg.get("gpu_threshold", 0.5)),
                "axes": g["axes"], "provenance": doc["provenance"]}
        for t in gdoc["classifier"]["trees"]:
            for n in t["nodes"]:
                if "leaf" not in n:
                    n["missing"] = "none"
        gout = os.path.splitext(out)[0] + ".gpu.json"
        with open(gout, "w") as f:
            json.dump(gdoc, f, separators=(",", ":"), allow_nan=False)
        files.append(gout)
    return {"files": files, "repo": repo, "version": version, "levels": levels, "memory_basis": memory_basis, "mapped": mapped,
            "unmapped": unmapped, "n_features": len(names)}


# ── a reference evaluator for the GPU companion (tests; Rust TODO) ──────


def _eval(nodes: list[dict], x: Sequence[float]) -> float:
    i = 0
    while "leaf" not in nodes[i]:
        n = nodes[i]
        v = x[n["feature"]]
        if v is None or (isinstance(v, float) and math.isnan(v)):
            v = 0.0  # missing: "none"
        i = n["left"] if v <= n["threshold"] else n["right"]
    return nodes[i]["leaf"]


def gpu_probability(gdoc: Mapping, x: Sequence[float]) -> float:
    c = gdoc["classifier"]
    raw = c["base"] + sum(_eval(t["nodes"], x) for t in c["trees"])
    return 1.0 / (1.0 + math.exp(-raw))
