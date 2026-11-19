"""The Estimator Protocol v1 reference server (SPEC §6.3).

``rightsizer.serve`` wraps any Python callable ``ctx -> Estimate`` (or a batch
callable) as a protocol server; ``rightsizer serve`` plugs in a model-factory
model (models.md). Standard library only.

    from rightsizer.serve import EstimatorServer, wrap

    def estimate(ctx):            # a WorkloadContext dict -> an Estimate dict
        ...

    EstimatorServer(wrap(estimate, id="mine", version="1"), port=8095).serve_forever()
"""

from .errors import (
    BadRequest,
    EstimatorError,
    Insufficient,
    Internal,
    RateLimited,
    Unauthorized,
    Unavailable,
    Unsupported,
)
from .server import PROTOCOL, Coalescer, Estimator, EstimatorServer, serve, wrap

__all__ = [
    "PROTOCOL", "BadRequest", "Coalescer", "Estimator", "EstimatorError", "EstimatorServer", "Insufficient",
    "Internal", "RateLimited", "Unauthorized", "Unavailable", "Unsupported", "serve", "wrap",
]
