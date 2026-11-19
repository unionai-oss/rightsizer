"""(c) Actors are sized once at creation; a Ray Data ``map_batches`` operator gets one decision."""

from __future__ import annotations

import time

import rayit_tasks as T
from conftest import engine, record

GiB = 1 << 30
MiB = 1 << 20
SHAPES = [{"name": "it-node", "cpu": 4000, "memory": 8 * GiB, "price_per_hour": 0.2}]


def _seed(e, key, peak_bytes, n=4):
    import datetime as dt

    from rightsizer_runtime.wire import Censor, Measurement, OutcomeSource, Requests, ResourceSpec, RunRecord, Trust

    now = dt.datetime.now(dt.timezone.utc)
    for i in range(n):
        e.outcome(RunRecord(key=key, invocation_id=f"seed-{i}", attempt=1, started_at=now - dt.timedelta(minutes=20 - i),
                            requested=ResourceSpec(requests=Requests(cpu=1000, memory=2 * GiB)), status="succeeded",
                            memory_peak=Measurement(float(peak_bytes), Censor.EXACT, 0.5), cpu_mean=Measurement(0.3),
                            duration_s=30.0, source=OutcomeSource.self_report(), trust=Trust.AUTHENTICATED))
    e.flush()


class _Counting:
    def __init__(self, e):
        self.e, self.n = e, 0

    def __getattr__(self, k):
        return getattr(self.e, k)

    def decide(self, ctx, **kw):
        self.n += 1
        return self.e.decide(ctx, **kw)


def test_actor_sized_once_at_creation(ray_session, store):
    ray = ray_session
    import rightsizer.ray as rr
    from ray.util.state import list_actors

    e = engine(store, mode="enforce", scoped=True)
    _seed(e, "ray://it/local/it.embedder", 150 * MiB)
    counting = _Counting(e)
    rr.configure(engine=counting, shapes=SHAPES)
    Embedder = rr.sized(ray.remote(num_cpus=1, memory=2 * GiB)(T.Embedder), key="it.embedder")
    a = Embedder.remote(model="bge-large")
    after_create = counting.n
    res = [ray.get(a.resources.remote()) for _ in range(5)]  # method calls are not sized
    d = Embedder.last_decision
    time.sleep(1.0)
    st = [x for x in list_actors(filters=[("class_name", "=", "Embedder"), ("state", "=", "ALIVE")], detail=True)]
    data = {
        "decisions_at_creation": after_create,
        "decisions_after_5_method_calls": counting.n,
        "assigned_in_actor": res[0],
        "decision": {"applied_memory": d.applied.requests.memory, "applied_cpu": d.applied.requests.cpu,
                     "mode": str(d.mode), "key": d.key},
        "state_api_required_resources": [getattr(x, "required_resources", None) for x in st][-1:] if st else None,
    }
    record("actor", data)
    assert after_create == 1 and counting.n == 1
    assert res[0].get("memory") == d.applied.requests.memory < 2 * GiB
    ray.kill(a)


def test_map_batches_one_decision_per_operator(ray_session, store, tmp_path):
    ray = ray_session
    import pyarrow as pa
    import pyarrow.parquet as pq

    import rightsizer.ray as rr
    from ray.util.state import list_tasks

    paths = []
    for i in range(4):
        p = tmp_path / f"part-{i}.parquet"
        pq.write_table(pa.table({"a": list(range(20_000 * (i + 1)))}), p)
        paths.append(str(p))
    e = engine(store, mode="enforce", scoped=True)
    _seed(e, "ray://it/local/it.double-op", 100 * MiB)
    counting = _Counting(e)
    rr.configure(engine=counting, shapes=SHAPES)
    ds = ray.data.read_parquet(str(tmp_path))
    plan_paths = rr.data.read_paths(ds)
    ds2 = rr.data.map_batches(ds, T.double, key="it.double-op", batch_size=1000, num_cpus=1, memory=2 * GiB)
    decisions_at_plan = counting.n
    rows = ds2.take_all()
    d = rr.data.map_batches.last_decision
    time.sleep(2.0)
    job = ray.get_runtime_context().get_job_id()
    tasks = [t for t in list_tasks(filters=[("job_id", "=", job)], detail=True, limit=10_000)
             if "MapBatches(double)" in (t.name or "")]
    mem = sorted({(t.required_resources or {}).get("memory") for t in tasks})
    data = {
        "plan_paths": plan_paths,
        "decisions": counting.n,
        "decisions_at_plan_time": decisions_at_plan,
        "rows": len(rows),
        "decision_inputs": [i.uri for i in d.context.inputs] if d is not None and d.context is not None else None,
        "applied_memory": d.applied.requests.memory if d else None,
        "operator_tasks": len(tasks),
        "operator_task_names": sorted({t.name for t in tasks}),
        "operator_task_memory": mem,
        "explanation": list(d.explanation) if d else None,
    }
    record("ray_data", data)
    assert counting.n == 1 and decisions_at_plan == 1  # one decision, before execution
    assert len(rows) == sum(20_000 * (i + 1) for i in range(4))
    assert plan_paths  # the read's paths came from the plan without executing it
    assert d.applied.requests.memory < 2 * GiB
    assert tasks and mem == [float(d.applied.requests.memory)]
