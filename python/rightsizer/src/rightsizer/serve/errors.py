"""Estimator Protocol v1 errors (SPEC §6.3): every error body is
``{"error": {"code", "message", "retryable"}}``.

| class | status | code | retryable | the client reads it as |
|---|---|---|---|---|
| :class:`BadRequest` | 400 | ``malformed`` | no | protocol error |
| :class:`Unauthorized` | 401 | ``unauthorized`` | no | failure (next estimator) |
| :class:`Unsupported` | 409 | ``unsupported`` | no | unsupported grid / features |
| :class:`Insufficient` | 422 | ``insufficient`` | no | ``EstimateError::Insufficient`` (pass on) |
| :class:`RateLimited` | 429 | ``rate_limited`` | yes | back off |
| :class:`Unavailable` | 503 | ``unavailable`` | yes | back off |
| :class:`Internal` | 500 | ``internal`` | yes | failure (next estimator) |

The codes are the ones ``rightsizer-estimators``' remote client maps per batch
item (``crates/rightsizer-estimators/src/remote.rs``).
"""

from __future__ import annotations


class EstimatorError(Exception):
    status = 500
    code = "internal"
    retryable = False

    def __init__(self, message: str = "", retryable: bool | None = None):
        super().__init__(message or self.code)
        self.message = message or self.code
        if retryable is not None:
            self.retryable = retryable

    def body(self) -> dict:
        return {"error": {"code": self.code, "message": self.message, "retryable": bool(self.retryable)}}


class BadRequest(EstimatorError):
    status, code, retryable = 400, "malformed", False


class Unauthorized(EstimatorError):
    status, code, retryable = 401, "unauthorized", False


class NotFound(EstimatorError):
    status, code, retryable = 404, "not_found", False


class Unsupported(EstimatorError):
    status, code, retryable = 409, "unsupported", False


class Insufficient(EstimatorError):
    status, code, retryable = 422, "insufficient", False


class RateLimited(EstimatorError):
    status, code, retryable = 429, "rate_limited", True


class Unavailable(EstimatorError):
    status, code, retryable = 503, "unavailable", True


class Internal(EstimatorError):
    status, code, retryable = 500, "internal", True


def as_error(e: BaseException) -> EstimatorError:
    """Any exception as a protocol error (a backend's ``retryable`` flag is kept)."""
    if isinstance(e, EstimatorError):
        return e
    retryable = getattr(e, "retryable", None)
    if retryable is True and type(e).__name__ == "BackendError":
        return Unavailable(f"backend: {e}")
    return Internal(f"{type(e).__name__}: {e}", retryable=True if retryable is None else bool(retryable))
