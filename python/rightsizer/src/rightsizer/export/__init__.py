"""Model exporters for rightsizer's built-in ``gbt`` estimator (SPEC §6.2).

- :func:`export_sklearn_hgb` — ``HistGradientBoostingRegressor(loss="quantile")``
  models (one per quantile level) to ``rightsizer-trees/1``;
- :func:`export_lightgbm` — LightGBM quantile boosters to ``rightsizer-trees/1``;
- :func:`export_novelty` — the ``rightsizer-novelty/1`` kNN companion.

:func:`evaluate_trees` and :func:`evaluate_novelty` are pure-Python reference
evaluators with the Rust evaluator's semantics. sklearn and LightGBM are
imported only by the exporters that need them; numpy only by the novelty
helpers.
"""

from ._trees import (
    LOG2_MIB,
    TREES_FORMAT,
    ExportError,
    evaluate_trees,
    export_lightgbm,
    export_sklearn_hgb,
    raw_scores,
)


def __getattr__(name):  # numpy is imported lazily
    if name in ("export_novelty", "evaluate_novelty", "novelty_distance", "mean_knn_distances", "standardization", "NOVELTY_FORMAT"):
        from . import _novelty

        return getattr(_novelty, name)
    raise AttributeError(name)


__all__ = [
    "LOG2_MIB",
    "NOVELTY_FORMAT",
    "TREES_FORMAT",
    "ExportError",
    "evaluate_novelty",
    "evaluate_trees",
    "export_lightgbm",
    "export_novelty",
    "export_sklearn_hgb",
    "mean_knn_distances",
    "novelty_distance",
    "raw_scores",
    "standardization",
]
