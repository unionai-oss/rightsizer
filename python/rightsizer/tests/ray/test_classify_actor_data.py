"""Status classification fixtures, the actor proxy, Ray Data, S1, ``submit``."""

from __future__ import annotations

import json

import pytest
import ray
import ray.exceptions as rx

import rightsizer as rs
import rightsizer.ray as rr
from rightsizer.ray import classify
from rightsizer.ray._sizing import SizingPolicy, decide_options

from rayfx import GiB, MiB, TENANT, seed

SHAPES = [{"name": "cpu-16", "cpu": 16000, "memory": 64 * GiB, "price_per_hour": 0.8}]


# ── classification ───────────────────────────────────────────────────────


def _task_error(cause: BaseException) -> rx.RayTaskError:
    return rx.RayTaskError("work", "Traceback ...", cause)


def test_classify_fixtures():
    assert classify(None).status == "succeeded"
    oom = rx.OutOfMemoryError("Task was killed due to the node running low on memory. Memory on the node ... "
                              "memory_usage_threshold 0.95")
    c = classify(oom)
    assert (c.status, c.confidence, c.worker_reported) == ("oom_killed", "observed", False)
    crash = rx.WorkerCrashedError()
    c = classify(crash)
    assert (c.status, c.confidence) == ("failed", "observed")
    # Upgraded when the State API (or the error text) shows a memory-monitor / cgroup / kernel OOM kill.
    assert classify(crash, state_error_type="OUT_OF_MEMORY").status == "oom_killed"
    c = classify(crash, state_error_message="oom-kill:constraint=CONSTRAINT_MEMCG ... Memory cgroup out of memory: "
                                            "Killed process 4242 (ray::work)")
    assert (c.status, c.confidence) == ("oom_killed", "inferred")
    # Ray's generic WORKER_DIED text names the OOM killer only as a possible cause: not evidence.
    generic = ("Worker unexpectedly exits with a connection error code 2. End of file. Some common causes include: "
               "(1) the process was killed by the OOM killer due to high memory usage, (2) ray stop --force was called")
    assert classify(crash, state_error_message=generic).status == "failed"
    TorchOOM = type("OutOfMemoryError", (RuntimeError,), {"__module__": "torch.cuda"})
    c = classify(_task_error(TorchOOM("CUDA out of memory. Tried to allocate 2.00 GiB")))
    assert (c.status, c.worker_reported) == ("gpu_oom", True)
    c = classify(_task_error(ValueError("bad row")))
    assert (c.status, c.worker_reported) == ("failed", True) and c.reason == "task_error:ValueError"
    c = classify(_task_error(MemoryError()))
    assert c.status == "failed"  # a Python MemoryError is not an OOM kill
    assert classify(rx.TaskCancelledError()).status == "unknown"
    assert classify(rx.NodeDiedError("node died")).status == "preempted"


# ── actors ───────────────────────────────────────────────────────────────


class FakeActorClass:
    """Stands in for ray.actor.ActorClass (creation needs a cluster)."""

    def __init__(self, defaults, log):
        self._default_options = dict(defaults)
        self.log = log

    def options(self, **kw):
        log = self.log

        class W:
            def remote(self, *a, **k):
                log.append(("options.remote", dict(kw), a, k))
                return "handle"

        return W()

    def remote(self, *a, **k):
        self.log.append(("remote", {}, a, k))
        return "handle"


class Embedder:
    def __init__(self, model="bge"):
        self.model = model


def test_actor_proxy_decides_once_at_creation(engine_factory):
    e = engine_factory(shapes=SHAPES)
    seed(e, f"ray://{TENANT}/embedder", [300 * MiB] * 4)
    log = []
    A = rr.SizedActorClass(FakeActorClass({"num_cpus": 1, "memory": 8 * GiB, "max_restarts": 2}, log),
                           policy=SizingPolicy(key="embedder"))
    assert A.remote(model="x") == "handle"
    (call,) = log
    assert call[0] == "options.remote" and call[1]["memory"] < 8 * GiB and call[3] == {"model": "x"}
    assert "max_restarts" not in call[1]  # defaults stay Ray's: only what changed is passed
    assert A.last_decision is not None and A.last_decision.key == f"ray://{TENANT}/embedder"
    # .options axes are pins
    log.clear()
    A.options(memory=6 * GiB, name="e1").remote()
    assert log[0][1]["memory"] == 6 * GiB and log[0][1]["name"] == "e1"
    # isinstance checks against ActorClass fail by design (ray.md §3)
    assert not isinstance(A, ray.actor.ActorClass)


def test_sized_requires_an_actor_class():
    with pytest.raises(TypeError):
        rr.sized(Embedder)
    real = ray.remote(num_cpus=1)(Embedder)
    s = rr.sized(real, key="embedder")
    assert s.actor_class is real and rr.sized(s) is s


# ── Ray Data ─────────────────────────────────────────────────────────────


class FakeDataset:
    def __init__(self, paths=None):
        self.calls = []
        self._paths = paths

        class Op:
            def __init__(self, paths):
                self.source_paths = paths

            def post_order_iter(self):
                return [self]

        class Plan:
            def __init__(self, paths):
                self.dag = Op(paths)

            def input_files(self):
                return []

        self._logical_plan = Plan(paths)

    def map_batches(self, fn, **kw):
        self.calls.append(kw)
        return self


def test_map_batches_one_decision_per_operator(engine_factory, parquet):
    e = engine_factory(shapes=SHAPES)
    ds = FakeDataset([parquet])
    seen = []
    orig = e.decide

    def counting(ctx, **kw):
        seen.append(ctx)
        return orig(ctx, **kw)

    e.decide = counting  # type: ignore[method-assign]
    seed(e, f"ray://{TENANT}/embed-op", [300 * MiB] * 4)
    out = rr.data.map_batches(ds, Embedder, key="embed-op", batch_size=256, concurrency=8, num_cpus=1,
                              memory=8 * GiB, resources={"r": 1})
    assert out is ds and len(seen) == 1
    kw = ds.calls[0]
    assert kw["batch_size"] == 256 and kw["concurrency"] == 8 and kw["resources"] == {"r": 1}
    assert kw["memory"] < 8 * GiB
    assert [i.uri for i in seen[0].inputs] == [parquet]  # from the plan's read paths, never executed
    d = rr.data.map_batches.last_decision
    assert d is not None and d.key == f"ray://{TENANT}/embed-op"


@pytest.fixture
def parquet(tmp_path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    p = tmp_path / "part-0.parquet"
    pq.write_table(pa.table({"a": list(range(1000))}), p)
    return str(p)


# ── S1 ───────────────────────────────────────────────────────────────────


def test_s1_core_missing_call_proceeds_unchanged(monkeypatch, captured):
    from rightsizer import _native

    monkeypatch.setattr(_native, "_core", None)
    monkeypatch.setattr(_native, "_error", ImportError("no wheel for this platform"))
    rs.configure()  # default engine rebuilt → fails → None
    rr.configure(observe=False)
    f = rr.remote(num_cpus=1, memory=4 * GiB, key="embed")(lambda x: x)
    assert f.remote("x") == "ref-1"
    assert captured[0]["options"] == {"num_cpus": 1, "memory": 4 * GiB}
    assert "_rightsizer" not in captured[0]["kwargs"]


def test_s1_estimator_failure_keeps_the_prior(engine_factory, captured):
    engine_factory(shapes=SHAPES)

    class Broken(rs.Estimator):
        id = "broken"
        version = "1"

        def estimate(self, ctx):
            raise RuntimeError("estimator exploded")

    rs.register_estimator(Broken())
    f = rr.remote(num_cpus=1, memory=4 * GiB, key="embed")(lambda x: x)
    f.remote("x")
    assert captured[0]["options"] == {"num_cpus": 1, "memory": 4 * GiB}


def test_s1_engine_raising_inside_decide(captured):
    class Exploding:
        config = {"ray": {"tenant": TENANT}, "history": {}}

        def decide(self, *a, **k):
            raise RuntimeError("bug")

    rr.configure(engine=Exploding(), observe=False)
    f = rr.remote(num_cpus=2, key="embed")(lambda x: x)
    f.options(memory=GiB).remote("x")
    assert captured[0]["options"] == {"num_cpus": 2, "memory": GiB}
    s = decide_options({"num_cpus": 2}, SizingPolicy(key="k"), target=None)
    assert s.options == {"num_cpus": 2} and s.decision is None


# ── submit ───────────────────────────────────────────────────────────────


class FakeJobs:
    def __init__(self):
        self.calls = []

    def submit_job(self, **kw):
        self.calls.append(kw)
        return "raysubmit_test"


def test_submit_propagates_store_policy_and_sizes_the_driver(core, store, monkeypatch, tmp_path):
    monkeypatch.setenv("RIGHTSIZER__POLICY__MODE", "enforce")
    rel = "rel-store"
    e = rs.Engine({"history": {"uri": rel, "durable": True, "shared": True}, "ray": {"tenant": TENANT}})
    rr.configure(engine=e)
    jobs = FakeJobs()
    res = rr.submit(["python", "train.py", "--epochs", "1"], runtime_env={"env_vars": {"MINE": "1"}, "pip": ["x"]},
                    entrypoint_num_cpus=2, entrypoint_memory=4 * GiB, client=jobs)
    (call,) = jobs.calls
    env = call["runtime_env"]["env_vars"]
    assert env["MINE"] == "1" and call["runtime_env"]["pip"] == ["x"]
    assert env["RIGHTSIZER__POLICY__MODE"] == "enforce"
    assert env["RIGHTSIZER__HISTORY__URI"] == str(tmp_path / rel)  # made absolute for the cluster
    assert env["RIGHTSIZER__RAY__TENANT"] == TENANT and env["RIGHTSIZER__HISTORY__DURABLE"] == "true"
    assert call["entrypoint"] == "python train.py --epochs 1"
    assert call["entrypoint_num_cpus"] == 2 and call["entrypoint_memory"] == 4 * GiB  # no evidence: held
    assert res["submission_id"] == "raysubmit_test"
    # The user's own runtime_env wins.
    jobs2 = FakeJobs()
    rr.submit(["python", "-m", "pkg.job"], runtime_env={"env_vars": {"RIGHTSIZER__POLICY__MODE": "recommend"}}, client=jobs2)
    assert jobs2.calls[0]["runtime_env"]["env_vars"]["RIGHTSIZER__POLICY__MODE"] == "recommend"
    assert "entrypoint_num_cpus" not in jobs2.calls[0]  # no prior: nothing set


def test_cli_usage_and_env(core, store, capsys, monkeypatch):
    from rightsizer.ray import cli

    assert cli.main([]) == 2
    rr.configure(engine=rs.Engine({"history": {"uri": store}, "ray": {"tenant": TENANT}}, env_overrides=False))
    assert cli.main(["-o", "json", "env"]) == 0
    env = json.loads(capsys.readouterr().out)
    assert env["RIGHTSIZER__RAY__TENANT"] == TENANT and env["RIGHTSIZER__HISTORY__URI"] == store
    assert cli.main(["submit"]) == 2
