"""Policy resolution (flyte.md §3), merge rules (§4.3), classification (§8.2)."""

from __future__ import annotations

import datetime as _dt

import flyte
import flyte.errors as fe
import pytest

from rightsizer_runtime.policy import InvocationPolicy, PolicyStack
from rightsizer_runtime.wire import GpuShape, Mode, Requests, RunStatus

import rightsizer.flyte as rf
from rightsizer.flyte import _policy as pol
from rightsizer.flyte._resources import merge, to_spec
from rightsizer.flyte._task import walltime_s

GiB = 1 << 30
MiB = 1 << 20


# ── §3 policy resolution ────────────────────────────────────────────────

S = PolicyStack


def st(*modes, risk=None):
    return S(tuple(InvocationPolicy(mode=m, risk=risk) for m in modes))


@pytest.mark.parametrize(
    "definition, stack, mode",
    [
        (None, st(), "recommend"),                              # default_mode recommend
        ("enforce", st(), "enforce"),                           # definition string = default_mode
        ("off", st("enforce"), "off"),                          # "off" also caps max_mode
        (None, st("enforce"), "enforce"),                       # run policy
        (None, st("enforce", "observe"), "observe"),            # innermost wins
        (None, st("observe", None), "observe"),                 # None inherits
        (rf.DefinitionPolicy(max_mode="recommend"), st("enforce"), "recommend"),   # never relaxes the definition
        (rf.DefinitionPolicy(default_mode="observe", max_mode="enforce"), st(), "observe"),
        (rf.DefinitionPolicy(default_mode="enforce"), st("recommend"), "recommend"),
        ({"default_mode": "enforce", "max_mode": "observe"}, st(), "observe"),
    ],
)
def test_policy_resolution_table(definition, stack, mode):
    eff = pol.resolve(pol.definition(definition), stack)
    assert eff.mode is Mode.parse(mode)


def test_risk_resolution_and_aliases():
    d = pol.definition(rf.DefinitionPolicy(risk="lean"))
    assert pol.resolve(d, st()).risk == "lean"
    assert pol.resolve(d, st("enforce", risk="averse")).risk == "averse"
    assert pol.resolve(pol.definition(None), st()).risk == "balanced"
    assert pol.invocation("enforce").mode is Mode.ENFORCE
    assert pol.invocation(rightsize="observe").mode is Mode.OBSERVE
    with pytest.raises(TypeError):
        pol.invocation(mode="enforce", rightsize="observe")


def test_definition_merge_task_over_environment():
    env = pol.definition(rf.DefinitionPolicy(default_mode="enforce", pins=frozenset({"gpu"}), max_escalations=1))
    task = pol.definition(rf.DefinitionPolicy(key="llama-ft", max_escalations=3))
    m = task.merged_over(env)
    assert (m.default_mode, m.pins, m.max_escalations, m.key) == (Mode.ENFORCE, frozenset({"gpu"}), 3, "llama-ft")


def test_definition_ceiling_accepts_flyte_resources():
    d = rf.DefinitionPolicy(ceiling=flyte.Resources(cpu=4, memory="8Gi"), floor=flyte.Resources(memory="1Gi"))
    assert d.ceiling == Requests(cpu=4000, memory=8 * GiB) and d.floor.memory == GiB


def test_run_context_payload():
    cc = rf.run_context(mode="enforce", risk="lean")
    assert cc == {"rightsizer": '{"v":1,"s":[{"mode":"enforce","risk":"lean"}]}'}
    assert PolicyStack.from_json(cc["rightsizer"]).mode is Mode.ENFORCE


# ── §4.3 merge rules ────────────────────────────────────────────────────


def test_scalar_memory_stays_scalar_request_only():
    prior = flyte.Resources(cpu=2, memory="4Gi")
    out = merge(prior, Requests(cpu=1000, memory=1536 * MiB))
    assert out.memory == "1536Mi" and out.cpu == "1"
    assert not isinstance(out.memory, tuple), "no limit added (S2)"


def test_tuple_memory_keeps_its_ratio():
    prior = flyte.Resources(cpu=(1, 2), memory=("2Gi", "4Gi"))
    out = merge(prior, Requests(cpu=500, memory=GiB))
    assert out.memory == ("1Gi", "2Gi")
    assert out.cpu == ("500m", "1")


def test_unchanged_axes_and_disk_shm_kept():
    prior = flyte.Resources(cpu=2, memory="4Gi", disk="10Gi", shm="1Gi")
    assert merge(prior, Requests(cpu=2000, memory=4 * GiB)) is prior
    out = merge(prior, Requests(memory=2 * GiB, ephemeral_storage=GiB, shm=2 * GiB))
    assert (out.disk, out.shm, out.cpu, out.memory) == ("10Gi", "1Gi", 2, "2Gi")


def test_gpu_kept_unless_type_change_allowed():
    prior = flyte.Resources(cpu=8, memory="64Gi", gpu="A100:1")
    shape = Requests(gpu=GpuShape(accel="A100-80G", count=1))
    assert merge(prior, shape).gpu == "A100:1" if merge(prior, shape) is not prior else True
    out = merge(prior, shape, allow_gpu_type_change=True)
    assert out.gpu.device == "A100_80G" and out.gpu.quantity == 1, "flyte names, not the deprecated 'A100 80G'"


def test_int_gpu_is_unknown_and_never_retyped():
    prior = flyte.Resources(cpu=8, memory="64Gi", gpu=1)
    assert to_spec(prior).requests.gpu == GpuShape(accel="unknown-gpu", count=1)
    out = merge(prior, Requests(gpu=GpuShape(accel="H100-80G", count=1)), allow_gpu_type_change=True)
    assert out is prior


def test_gpu_names_map_to_the_catalog():
    assert to_spec(flyte.Resources(gpu="A100_80G:2")).requests.gpu == GpuShape(accel="A100-80G", count=2)
    assert to_spec(flyte.Resources(gpu="L40s:1")).requests.gpu == GpuShape(accel="L40S-48G", count=1)
    assert to_spec(flyte.Resources(gpu=flyte.GPU("A100", 1, partition="1g.5gb"))).requests.gpu.accel == "unknown-gpu"


def test_walltime_from_timeout():
    assert walltime_s(None) is None and walltime_s(0) is None
    assert walltime_s(600) == 600
    assert walltime_s(_dt.timedelta(minutes=5)) == 300
    assert walltime_s(flyte.Timeout(max_runtime=_dt.timedelta(hours=1), deadline=10)) == 3600
    assert walltime_s(flyte.Timeout(deadline=10)) is None


# ── §8.2 classification ─────────────────────────────────────────────────

CUDA_OOM = (
    "OutOfMemoryError: CUDA out of memory. Tried to allocate 2.00 GiB. GPU 0 has a total capacity of 79.15 GiB "
    "of which 1.06 GiB is free. Including non-PyTorch memory, this process has 78.08 GiB memory in use."
)


@pytest.mark.parametrize(
    "exc, status",
    [
        (fe.OOMError(code="OOMKilled", message="Reason [OOMKilled]"), RunStatus.OOM_KILLED),
        # SDK 2.13.6: a torch.cuda.OutOfMemoryError in the child arrives as a user error coded by class name.
        (fe.RuntimeUserError(code="OutOfMemoryError", message=CUDA_OOM), RunStatus.GPU_OOM),
        # flyte.md §1 / older SDKs: RuntimeUnknownError with the same code.
        (fe.RuntimeUnknownError(code="OutOfMemoryError", message=CUDA_OOM), RunStatus.GPU_OOM),
        (fe.RuntimeUserError(code="RuntimeError", message="HIP out of memory. Tried to allocate 1 GiB"), RunStatus.GPU_OOM),
        (fe.MaxRuntimeExceededError("max runtime exceeded"), RunStatus.TIMEOUT),
        (fe.DeadlineExceededError("deadline"), RunStatus.TIMEOUT),
        (fe.RuntimeUserError(code="MemoryError", message="MemoryError"), RunStatus.FAILED),
        (fe.RuntimeUserError(code="ValueError", message="bad"), RunStatus.FAILED),
        (fe.RuntimeSystemError(code="OOMKilled", message="killed"), RunStatus.OOM_KILLED),
        (ValueError("x"), RunStatus.FAILED),
    ],
)
def test_classification(exc, status):
    assert rf.classify(exc) is status
