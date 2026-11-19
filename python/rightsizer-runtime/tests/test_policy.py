import asyncio
import itertools
import json
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from rightsizer_runtime.policy import (
    STACK_MAX_BYTES,
    DefinitionPolicy,
    InvocationPolicy,
    PolicyStack,
    capture_stack,
    current_stack,
    eager,
    policy,
    resolve,
    resolve_mode,
    resolve_risk,
    risk_oom_penalty,
    risk_to_wire,
)
from rightsizer_runtime.wire import Mode, Requests, WireError

MODES = list(Mode)
OPT_MODES = [None] + MODES


@pytest.mark.parametrize(
    "stack_mode,default_mode,max_mode,ceiling",
    list(itertools.product(OPT_MODES, OPT_MODES, OPT_MODES, OPT_MODES)),
)
def test_mode_resolution_exhaustive(stack_mode, default_mode, max_mode, ceiling):
    requested = stack_mode or default_mode or Mode.RECOMMEND
    expected = min([requested, max_mode or Mode.ENFORCE, ceiling or Mode.ENFORCE], key=lambda m: m.rank)
    assert resolve_mode(stack_mode, default_mode, max_mode, ceiling) is expected
    d = DefinitionPolicy(max_mode=max_mode, default_mode=default_mode)
    st = PolicyStack([InvocationPolicy(mode=stack_mode)])
    assert resolve(d, st, mode_ceiling=ceiling).mode is expected


def test_resolution_examples():
    assert resolve().mode is Mode.RECOMMEND
    assert resolve(DefinitionPolicy(max_mode="off"), PolicyStack([InvocationPolicy(mode="enforce")])).mode is Mode.OFF
    assert resolve(None, PolicyStack([InvocationPolicy(mode="enforce")]), mode_ceiling="observe").mode is Mode.OBSERVE
    # an invocation never relaxes a definition: max_mode caps an enforce request
    assert resolve(DefinitionPolicy(max_mode="recommend"), PolicyStack([InvocationPolicy(mode="enforce")])).mode is Mode.RECOMMEND


@pytest.mark.parametrize(
    "stack,defn,config,default,expected",
    [
        (None, None, None, "balanced", "balanced"),
        (None, None, None, "averse", "averse"),
        (None, None, "lean", "balanced", "lean"),
        (None, "averse", "lean", "balanced", "averse"),
        ("lean", "averse", "balanced", "balanced", "lean"),
        (8.0, "averse", None, "balanced", 8.0),
        (None, 3, None, "balanced", 3.0),
    ],
)
def test_risk_resolution(stack, defn, config, default, expected):
    assert resolve_risk(stack, defn, config, default) == expected
    eff = resolve(DefinitionPolicy(risk=defn), PolicyStack([InvocationPolicy(risk=stack)]), config_risk=config, default_risk=default)
    assert eff.risk == expected


def test_risk_penalties_and_validation():
    assert risk_oom_penalty("lean") == 1 and risk_oom_penalty("balanced") == 4 and risk_oom_penalty("averse") == 16
    assert risk_oom_penalty(2.5) == 2.5
    for bad in ("reckless", 0, -1, float("inf"), True):
        with pytest.raises((WireError, ValueError)):
            InvocationPolicy(risk=bad)


def test_innermost_wins_per_field():
    st = PolicyStack([
        InvocationPolicy(mode="enforce", risk="lean", estimator="a"),
        InvocationPolicy(risk="averse"),
        InvocationPolicy(estimator="b"),
    ])
    assert (st.mode, st.risk, st.estimator) == (Mode.ENFORCE, "averse", "b")
    assert st.collapsed() == PolicyStack([InvocationPolicy(mode="enforce", risk="averse", estimator="b")])


def test_definition_merge_and_defaults():
    env = DefinitionPolicy(max_mode="recommend", pins={"gpu"}, ceiling=Requests(memory=64 << 30), risk="averse")
    task = DefinitionPolicy(default_mode="observe", pins=frozenset({"memory"}), key="ft")
    m = task.merged_over(env)
    assert m.max_mode is Mode.RECOMMEND and m.default_mode is Mode.OBSERVE
    assert m.pins == frozenset({"memory"}) and m.key == "ft" and m.risk == "averse"
    assert m.ceiling == Requests(memory=64 << 30)
    r = DefinitionPolicy().resolved()
    assert (r.max_mode, r.default_mode, r.pins, r.allow_gpu_type_change, r.on_oom, r.max_escalations) == (
        Mode.ENFORCE, Mode.RECOMMEND, frozenset(), False, "auto", 2)
    with pytest.raises(ValueError):
        DefinitionPolicy(pins={"vram"})
    with pytest.raises(ValueError):
        DefinitionPolicy(on_oom="retry")


# ---- serialization


def test_stack_json_compact_roundtrip():
    st = PolicyStack([InvocationPolicy(mode="enforce"), InvocationPolicy(risk=4.0, estimator="gate")])
    s = st.to_json()
    assert s == '{"v":1,"s":[{"mode":"enforce"},{"risk":4.0,"estimator":"gate"}]}'
    assert PolicyStack.from_json(s) == st
    assert PolicyStack.from_json(None) == PolicyStack() == PolicyStack.from_json("")
    assert PolicyStack().to_json() == '{"v":1,"s":[]}'


def test_stack_1kib_cap_collapses():
    st = PolicyStack([InvocationPolicy(mode=m, estimator=f"chain-{i}") for i, m in zip(range(200), itertools.cycle(Mode))])
    s = st.to_json()
    assert len(s.encode()) <= STACK_MAX_BYTES
    back = PolicyStack.from_json(s)
    assert (back.mode, back.risk, back.estimator) == (st.mode, st.risk, st.estimator)


def test_stack_cap_drops_huge_estimator():
    st = PolicyStack([InvocationPolicy(mode="observe", risk="lean", estimator="x" * 2000)])
    s = st.to_json()
    assert len(s.encode()) <= STACK_MAX_BYTES
    back = PolicyStack.from_json(s)
    assert back.mode is Mode.OBSERVE and back.risk == "lean" and back.estimator is None


@pytest.mark.parametrize(
    "bad",
    ['{"v":2,"s":[]}', '{"s":[]}', '{"v":1,"s":{}}', '{"v":1,"s":[{"mode":"max"}]}', '{"v":1,"s":[{"x":1}]}',
     '[1]', 'nope', '{"v":true,"s":[]}', '{"v":1,"s":[],"x":0}', '{"v":1,"s":[' + ",".join(['{"mode":"off"}'] * 100) + ']}'],
)
def test_stack_from_json_strict(bad):
    with pytest.raises(WireError):
        PolicyStack.from_json(bad)


# ---- contextvar stack and eager capture


def test_policy_block_push_pop():
    assert current_stack() == PolicyStack()
    with policy(mode="enforce"):
        with policy(risk="lean"):
            st = current_stack()
            assert st.mode is Mode.ENFORCE and st.risk == "lean" and len(st) == 2
        assert len(current_stack()) == 1
    assert current_stack() == PolicyStack()


def test_inherited_stack_below_local():
    inherited = PolicyStack([InvocationPolicy(mode="observe", risk="averse")]).to_json()
    with policy(mode="enforce"):
        st = current_stack(inherited)
        assert st.mode is Mode.ENFORCE and st.risk == "averse"
    # malformed inherited payload is ignored (S1)
    assert current_stack("{garbage") == PolicyStack()


def test_eager_capture_across_gather():
    @eager
    async def task(i):
        await asyncio.sleep(0.01 * (3 - i))
        return current_stack().mode

    async def main():
        coros = []
        with policy(mode="enforce"):
            coros.append(task(0))
            with policy(mode="observe"):
                coros.append(task(1))
        coros.append(task(2))  # created outside any block
        # awaited after every block has exited
        return await asyncio.gather(*coros)

    assert asyncio.run(main()) == [Mode.ENFORCE, Mode.OBSERVE, None]


def test_without_eager_gather_loses_policy():
    async def task():
        return current_stack().mode

    async def main():
        with policy(mode="enforce"):
            c = task()
        return await asyncio.gather(c)

    assert asyncio.run(main()) == [None]


def test_captured_stack_in_threads():
    with policy(mode="enforce", estimator="e1"):
        cap = capture_stack()
    seen = {}

    def worker(name):
        seen[name + "_plain"] = current_stack().mode
        seen[name] = cap.run(lambda: current_stack().mode)

    t = threading.Thread(target=worker, args=("t",))
    t.start(); t.join()
    with ThreadPoolExecutor(2) as ex:
        fut = ex.submit(cap.wrap(lambda: (current_stack().mode, current_stack().estimator)))
        assert fut.result() == (Mode.ENFORCE, "e1")
    assert seen == {"t_plain": None, "t": Mode.ENFORCE}


def test_captured_stack_await_and_wrap_async():
    with policy(mode="observe"):
        cap = capture_stack()

    async def inner():
        return current_stack().mode

    async def main():
        a = await cap.await_(inner())
        b = await cap.wrap(inner)()
        return a, b, current_stack().mode

    assert asyncio.run(main()) == (Mode.OBSERVE, Mode.OBSERVE, None)


def test_threads_do_not_leak_between_each_other():
    barrier = threading.Barrier(2)
    out = {}

    def w(mode):
        with policy(mode=mode):
            barrier.wait()
            out[mode] = current_stack().mode

    ts = [threading.Thread(target=w, args=(m,)) for m in ("enforce", "observe")]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert out == {"enforce": Mode.ENFORCE, "observe": Mode.OBSERVE}


def test_risk_to_rust_wire():
    assert risk_to_wire("averse") == "averse"
    assert risk_to_wire(8) == {"oom_penalty": 8.0}
