"""The in-task wrapper (rightsizer_runtime.ray_child) and picklability for workers."""

from __future__ import annotations

import glob
import inspect
import json
import os
import subprocess
import sys

import cloudpickle
import pytest
import ray
from ray._common.signature import extract_signature, flatten_args, recover_args

import rightsizer.ray as rr
from rightsizer_runtime import ray_child
from rightsizer_runtime.ray_child import META_KWARG, make_task_function, run_task

from rayfx import GiB, TENANT

KEY = f"ray://{TENANT}/work"


def work(n, scale=1, *, label="x", **extra):
    return n * scale


def gen(k):
    for i in range(k):
        yield i


def _meta(store, **kw):
    m = {"v": 1, "k": KEY, "d": "01M7S9N0TKZGHADFN67T47D0VC", "m": "enforce",
         "r": {"requests": {"cpu": 500, "memory": 2 * GiB}, "limits": {}}, "s": store, "tr": "authenticated",
         "c": None, "i": [{"name": "arg0", "bytes": 4096}], "cd": "sha256:abc"}
    m.update(kw)
    return m


def _records(store):
    return [json.loads(open(p).read()) for p in sorted(glob.glob(f"{store}/v1/runs/**/*.json", recursive=True))]


def test_signature_keeps_user_args_and_adds_the_hidden_keyword():
    w = make_task_function(work)
    sig = inspect.signature(w)
    names = list(sig.parameters)
    assert names == ["n", "scale", "label", META_KWARG, "extra"]
    assert sig.parameters[META_KWARG].kind is inspect.Parameter.KEYWORD_ONLY
    # Ray validates the user's arguments at submission exactly as before.
    params = extract_signature(w)
    flat = flatten_args(params, (2,), {"scale": 3, META_KWARG: {"k": KEY}})
    assert recover_args(flat) == ([2], {"scale": 3, META_KWARG: {"k": KEY}})
    with pytest.raises(TypeError):
        flatten_args(params, (1, 2, 3), {})
    assert w.__name__ == "work" and w.__module__ == __name__
    assert inspect.isgeneratorfunction(make_task_function(gen))
    assert make_task_function(w) is w  # idempotent


def test_run_task_writes_a_process_attributed_record(store):
    out = run_task(work, None, _meta(store), (3,), {"scale": 2})
    assert out == 6
    (rec,) = _records(store)
    assert rec["key"] == KEY and rec["status"] == "succeeded" and rec["trust"] == "authenticated"
    assert rec["decision_id"] == "01M7S9N0TKZGHADFN67T47D0VC"
    assert rec["requested"]["requests"]["memory"] == 2 * GiB
    assert rec["memory_peak"]["censor"] == "exact" and rec["memory_peak"]["resolution_s"] == 0.5
    assert rec["source"] == {"kind": "self_report"} and rec["code_digest"] == "sha256:abc"
    assert rec["inputs"][0]["bytes"] == 4096


def test_wrapper_passes_results_and_exceptions_through(store):
    w = make_task_function(work)
    assert w(4, scale=2, _rightsizer=_meta(store)) == 8
    assert w(4) == 4  # no payload: measured only if the env names a store

    def boom():
        raise ValueError("user error")

    with pytest.raises(ValueError, match="user error"):
        make_task_function(boom)(_rightsizer=_meta(store))
    statuses = [r["status"] for r in _records(store)]
    assert statuses.count("failed") == 1 and statuses.count("succeeded") == 1
    g = make_task_function(gen)
    assert list(g(3, _rightsizer=_meta(store))) == [0, 1, 2]


def test_child_status_gpu_oom_and_memory_error(store):
    TorchOOM = type("OutOfMemoryError", (RuntimeError,), {"__module__": "torch.cuda"})

    def cuda_oom():
        raise TorchOOM("CUDA out of memory")

    def py_oom():
        raise MemoryError()

    for fn in (cuda_oom, py_oom):
        with pytest.raises((TorchOOM, MemoryError)):
            run_task(fn, None, _meta(store), (), {})
    recs = {r["status"]: r for r in _records(store)}
    assert set(recs) == {"gpu_oom", "failed"}
    assert recs["failed"]["memory_peak"]["censor"] == "at_least"


def test_off_and_unwritable_stores_run_unmeasured(store, monkeypatch):
    assert run_task(work, None, _meta(store, m="off"), (1,), {}) == 1
    assert run_task(work, None, _meta("sqlite:///tmp/x.db"), (1,), {}) == 1
    assert _records(store) == []
    monkeypatch.setenv("RIGHTSIZER__HISTORY__URI", store)
    assert run_task(work, KEY, None, (5,), {}) == 5  # bind()/DAG path: key + env store
    (rec,) = _records(store)
    assert rec.get("decision_id") is None and rec["trust"] == "unverified"


def test_a_broken_store_never_reaches_the_task(tmp_path):
    bad = tmp_path / "file"
    bad.write_text("not a dir")
    assert run_task(work, None, _meta(str(bad / "store")), (7,), {}) == 7


def test_rightsized_function_pickles_for_workers(core, store):
    rr.configure(engine=None)
    f = rr.remote(num_cpus=1, memory=2 * GiB, key="work")(work)
    blob = cloudpickle.dumps(f)
    g = cloudpickle.loads(blob)
    assert isinstance(g, rr.RightsizedRemoteFunction)
    assert g._rs_policy.key == "work" and g._default_options["memory"] == 2 * GiB
    assert g._function(2, scale=5) == 10
    # The function Ray ships (pickled by value) runs where rightsizer is not installed.
    def local_work(n, scale=1):
        return n * scale

    fn_blob = cloudpickle.dumps(rr.remote(key="work")(local_work)._function)
    code = (
        "import sys, cloudpickle\n"
        "class Block:\n"
        "    def find_spec(self, name, path=None, target=None):\n"
        "        if name.split('.')[0] in ('rightsizer', 'rightsizer_runtime'):\n"
        "            raise ImportError('blocked: ' + name)\n"
        "sys.meta_path.insert(0, Block())\n"
        "fn = cloudpickle.loads(sys.stdin.buffer.read())\n"
        "assert fn(3, scale=4, _rightsizer={'k': 'ray://a/b//c', 's': '/tmp/nowhere'}) == 12\n"
        "assert not [m for m in sys.modules if m.startswith('rightsizer')], sorted(sys.modules)\n"
        "print('ok')\n"
    )
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    p = subprocess.run([sys.executable, "-c", code], input=fn_blob, capture_output=True, env=env, cwd="/")
    assert p.returncode == 0 and b"ok" in p.stdout, p.stderr.decode()[-2000:]


def test_rs_remote_on_a_class_returns_the_actor_proxy():
    class Model:
        def __init__(self, n):
            self.n = n

    A = rr.remote(num_cpus=2, key="model")(Model)
    assert isinstance(A, rr.SizedActorClass)
    assert not isinstance(A, ray.actor.ActorClass)


def test_identity_outside_ray():
    assert ray_child.task_identity() == (None, 1)
