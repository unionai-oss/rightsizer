"""rightsizer-runtime: the pure-Python half of rightsizer.

Imports with the standard library only. Optional dependencies (``fsspec``,
``pynvml``) are imported lazily where they are used.
"""

from __future__ import annotations

from .child import MeasuredExecution, measured_execution
from .env import (
    DecisionEnv,
    attempt_context,
    current_attempt,
    parse_config_env,
    read_attempt,
    read_decided,
    read_decision,
    set_attempt_context,
)
from .measure import FLYTE_INTERVAL_S, RAY_INTERVAL_S, MeasurementResult, MeasurementScope, measured
from .policy import (
    DefinitionPolicy,
    EffectivePolicy,
    InvocationPolicy,
    PolicyStack,
    capture_stack,
    current_stack,
    eager,
    policy,
    resolve,
)
from .record import RunRecordWriter, key_hash, shard_path
from .wire import (
    AttemptContext,
    Censor,
    Confidence,
    GpuShape,
    InputFormat,
    InputMeta,
    Limits,
    Measurement,
    Mode,
    OutcomeSource,
    Requests,
    ResourceSpec,
    RunRecord,
    RunStatus,
    Trust,
    WireError,
    new_ulid,
)

__version__ = "0.1.0.dev0"

__all__ = [
    "AttemptContext",
    "Censor",
    "Confidence",
    "DecisionEnv",
    "DefinitionPolicy",
    "EffectivePolicy",
    "FLYTE_INTERVAL_S",
    "GpuShape",
    "InputFormat",
    "InputMeta",
    "InvocationPolicy",
    "Limits",
    "MeasuredExecution",
    "Measurement",
    "MeasurementResult",
    "MeasurementScope",
    "Mode",
    "OutcomeSource",
    "PolicyStack",
    "RAY_INTERVAL_S",
    "Requests",
    "ResourceSpec",
    "RunRecord",
    "RunRecordWriter",
    "RunStatus",
    "Trust",
    "WireError",
    "attempt_context",
    "capture_stack",
    "current_attempt",
    "current_stack",
    "eager",
    "key_hash",
    "measured",
    "measured_execution",
    "new_ulid",
    "parse_config_env",
    "policy",
    "read_attempt",
    "read_decided",
    "read_decision",
    "resolve",
    "set_attempt_context",
    "shard_path",
]
