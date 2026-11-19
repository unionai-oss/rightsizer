"""rightsizer for Ray (ray.md). Extra: ``pip install 'rightsizer[ray]'``.

::

    import rightsizer as rs

    @rs.ray.remote(num_cpus=4, memory=16 * 2**30, key="embed")   # sizing + in-task measurement
    def embed(shard_uri: str) -> str: ...

    tokenize = rs.ray.rightsize(ray.remote(num_cpus=1)(tokenize_fn))   # sizing only
    Embedder = rs.ray.sized(ray.remote(num_gpus=1)(EmbedderCls), key="embedder")   # actors: once at creation
    ds = rs.ray.data.map_batches(ds, Embedder, key="embed-op", batch_size=256)    # one decision per operator

Ray memory is logical (``memory_enforced = false``): reductions need S3(b)
evidence (``min_history`` measured successes, trusted), and the cost model
adds a neighbour-risk premium. ``max_retries`` is never changed and there is
no escalation ladder (ray.md §7). Importing this package imports Ray;
``import rightsizer`` does not.
"""

from __future__ import annotations

try:
    import ray as _ray  # noqa: F401
except ImportError as e:  # pragma: no cover - exercised by the isolation test
    raise ImportError("rightsizer.ray needs Ray (ray>=2.60). Install the extra: pip install 'rightsizer[ray]'") from e

from . import data
from ._actor import SizedActorClass, sized
from ._jobs import propagated_env, submit
from ._observe import Classified, classify, observer
from ._remote import RightsizedRemoteFunction, remote, rightsize
from ._settings import RaySettings, configure, settings

__all__ = [
    "Classified",
    "RaySettings",
    "RightsizedRemoteFunction",
    "SizedActorClass",
    "classify",
    "configure",
    "data",
    "observer",
    "propagated_env",
    "remote",
    "rightsize",
    "settings",
    "sized",
    "submit",
]
