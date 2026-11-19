"""Adversarial tests for ``rightsizer.models``: one bad context in a coalesced
batch, non-finite probabilities, odd generative answers."""

from __future__ import annotations

import json
import math
from types import SimpleNamespace

import pytest

from rightsizer.models import estimate, generate, prompting
from rightsizer.models.pointer import PointerModel, softmax_t
from rightsizer.serve.errors import EstimatorError


def ctx(name: str = "a", **kw) -> dict:
    c = {"v": 1, "key": f"flyte://o/p/d/{name}", "unit": "main", "platform": {"kind": "flyte"},
         "prior": {"spec": {"requests": {}, "limits": {}}}, "inputs": [], "history": []}
    c.update(kw)
    return c


# A context that passes the server's checks but that the prompt builder cannot read:
# an input with no name, and a history record whose bytes is not a number.
BAD_INPUT = ctx("bad", inputs=[{"format": "csv", "bytes": 10}])
BAD_HISTORY = ctx("bad2", history=[{"status": "succeeded", "inputs": [{"name": "x", "bytes": "s3://secret/path"}],
                                    "memory_peak": {"value": 1.0e9}}])


def _probs():
    return {"memory": [1.0] + [0.0] * (len(prompting.MEM_OPTIONS) - 1),
            "cpu": [1.0] + [0.0] * (len(prompting.CPU_OPTIONS) - 1),
            "gpu": [1.0] + [0.0] * (len(prompting.GPU_OPTIONS) - 1)}


def _pointer() -> PointerModel:
    m = PointerModel.__new__(PointerModel)
    m.release = SimpleNamespace(options={"memory": list(prompting.MEM_OPTIONS), "cpu": list(prompting.CPU_OPTIONS),
                                         "gpu": list(prompting.GPU_OPTIONS)},
                                repo="r", commit="c", config={"gpu_threshold": 0.5}, keys=["memory", "cpu", "gpu"])
    m.kind, m.memory_basis, m.prompt_options = "demand", "demand", {}
    m.seen = []

    def probabilities_batch(msgs):
        m.seen.append(len(msgs))
        return [_probs() for _ in msgs]

    m.probabilities_batch = probabilities_batch
    return m


@pytest.mark.parametrize("bad", [BAD_INPUT, BAD_HISTORY])
def test_pointer_bad_context_fails_only_its_slot(bad):
    m = _pointer()
    out = m.estimate_batch([ctx("a"), bad, ctx("b")])
    assert len(out) == 3
    assert isinstance(out[0], dict) and isinstance(out[2], dict)
    assert isinstance(out[1], EstimatorError) and out[1].status == 400
    # The slot's message must not quote another tenant's context values.
    assert "secret" not in out[1].message
    assert m.seen == [2]  # the good contexts still share one backend call


def test_generative_backend_error_fails_only_its_slot():
    class Backend:
        name = "g"

        def generate(self, messages, n, **kw):
            if "boom" in messages[-1]["content"]:
                raise RuntimeError("backend exploded")
            return ['{"memory": "2Gi", "cpu": "1", "gpu": "none"}'] * n

    g = generate.GenerativeModel(Backend(), model_id="m", n=2)
    out = g.estimate_batch([ctx("a"), ctx("b", code={"kind": "source", "source": "boom()"}), BAD_INPUT, ctx("c")])
    assert isinstance(out[0], dict) and isinstance(out[3], dict)
    assert isinstance(out[1], EstimatorError) and isinstance(out[2], EstimatorError)


def test_non_finite_probabilities_never_reach_the_estimate():
    opts = _pointer().release.options
    for bad in (float("nan"), float("inf")):
        p = _probs()
        p["memory"] = [bad, 0.5, 0.5] + [0.0] * (len(prompting.MEM_OPTIONS) - 3)
        # inf used to give an all-zero memory distribution; NaN was dropped silently.
        with pytest.raises(ValueError):
            estimate.build_estimate(p, opts, estimator="e", version="1")
    est = estimate.build_estimate(_probs(), opts, estimator="e", version="1")
    json.dumps(est, allow_nan=False)


def test_softmax_with_an_infinite_logit_is_one_hot_not_nan():
    p = softmax_t([1.0, float("inf"), 0.0], 1.0)
    assert list(p) == [0.0, 1.0, 0.0]


@pytest.mark.parametrize("text", ['{"memory": NaN}', '{"memory": Infinity}', '{"memory": "-2Gi"}',
                                  '{"memory": [1, 2]}', '{"memory": {"a": 1}}', '{"memory": true}'])
def test_generative_unusable_memory_is_skipped(text):
    assert generate.parse_answer(text) is None


def test_generative_nan_gpu_reads_as_none():
    got = generate.parse_answer('{"memory": "2Gi", "cpu": NaN, "gpu": NaN}')
    assert got is not None and got["cpu"] == 3 and got["gpu_memory"] == 0  # 1 core; no GPU
