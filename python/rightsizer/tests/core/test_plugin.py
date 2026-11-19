"""In-process Python adapters (plugins.md §3.5) and the CLI dispatcher."""

from __future__ import annotations

import subprocess
import sys

import pytest

import rightsizer as rs
from rightsizer import plugin

GiB = 1 << 30


class ToyRay(plugin.Adapter):
    """A Ray-like option dict: {"num_cpus", "memory", "key", "args"}."""

    kind = "toy"

    def __init__(self, reject: bool = False):
        self.reject = reject
        self.rendered = []

    def extract(self, native, env):
        if native.get("sized"):
            return plugin.Extracted(tenant="team-a", skip={"kind": "already_sized", "decision_id": None})
        u = plugin.Unit(id="task", key_suffix=native["key"],
                        prior=rs.Resources(cpu=native["num_cpus"], memory=native["memory"]),
                        inputs=[rs.Input.sized("shard", native["bytes"], "parquet")],
                        constraints={"pinned": ["cpu"]} if native.get("pin_cpu") else {})
        return plugin.Extracted(tenant=env.namespace or "team-a", units=[u])

    def render(self, native, decisions):
        (uid, spec), = decisions
        self.rendered.append(uid)
        return plugin.Patch({"num_cpus": spec.requests.cpu / 1000, "memory": spec.requests.memory})

    def validate(self, native, patch, env):
        if self.reject:
            raise rs.Rejection("exceeds_node_shape", "too big")

    def reference(self, unit):
        return {"kind": "toy", "memory_enforced": True}

    def state(self, tenant, env):
        return {"shapes": [{"cpu": 16000, "memory": 64 * GiB, "price_per_hour": 0.8}]}


class Grouped(ToyRay):
    kind = "toy-group"

    def extract(self, native, env):
        units = [plugin.Unit(id=f"r{i}", key_suffix="replica", group="replicas", prior=rs.Resources(cpu=2, memory="8Gi"),
                             inputs=[rs.Input.sized("x", (i + 1) * GiB, "csv")]) for i in range(3)]
        return plugin.Extracted(tenant="t", units=units)

    def render(self, native, decisions):
        return plugin.Patch({uid: spec.requests.memory for uid, spec in decisions})


def test_register_and_size(engine):
    rs.register_adapter(ToyRay)
    assert "toy" in plugin.adapters() and plugin.get_adapter("toy").kind == "toy"
    native = {"key": "embed", "num_cpus": 4, "memory": 16 * GiB, "bytes": GiB}
    s = plugin.size("toy", native, plugin.AdapterEnv(namespace="team-b"), engine=engine)
    assert s.error is None and set(s.decisions) == {"task"}
    d = s.decisions["task"]
    assert d.key == "toy://team-b/embed" and d.mode == rs.Mode.RECOMMEND
    assert s.patch.is_none, "nothing is rendered below enforce"
    assert plugin.unregister_adapter("toy")


def test_skip_and_rejection_and_errors(engine):
    a = ToyRay(reject=True)
    s = plugin.size(a, {"sized": True}, engine=engine)
    assert s.patch.is_none and s.decisions == {} and s.extracted.skip["kind"] == "already_sized"
    s = plugin.size(a, {"bogus": 1}, engine=engine)  # extract raises KeyError: S1
    assert s.patch.is_none and s.error and "KeyError" in s.error
    assert plugin.size("nope", {}).error


class _Enforcing:
    """Wraps an engine and marks its decisions enforced (store durability aside)."""

    def __init__(self, eng):
        self.eng = eng

    def decide(self, ctx, **kw):
        import dataclasses

        d = self.eng.decide(ctx, **kw)
        return dataclasses.replace(d, mode=rs.Mode.ENFORCE, applied=d.recommended)

    def decide_group(self, ctxs, **kw):
        return [self.decide(c, **kw) for c in ctxs]


def test_enforced_patch_and_validate(core, tmp_path):
    eng = rs.Engine({"history": {"uri": str(tmp_path / "s"), "durable": True, "shared": True}}, env_overrides=False)
    native = {"key": "embed", "num_cpus": 4, "memory": 512 << 20, "bytes": 8 * GiB}
    s = plugin.size(ToyRay(), native, engine=eng, mode="enforce")
    d = s.decisions["task"]
    assert d.mode == rs.Mode.ENFORCE and d.changed, d.explanation
    assert s.patch.options["memory"] == d.applied.requests.memory > 512 << 20
    a = ToyRay()
    s = plugin.size(a, native, engine=_Enforcing(eng), mode="enforce")
    d = s.decisions["task"]
    assert s.error is None and d.changed and d.applied.requests.memory > 512 << 20
    assert s.patch.options == {"num_cpus": d.applied.requests.cpu / 1000, "memory": d.applied.requests.memory}
    assert a.rendered == ["task"]
    r = plugin.size(ToyRay(reject=True), native, engine=_Enforcing(eng), mode="enforce")
    assert r.patch.is_none and r.rejection is not None and r.rejection.reason == "exceeds_node_shape"


def test_groups_decided_once(engine):
    s = plugin.size(Grouped(), {}, engine=engine)
    ids = {d.id for d in s.decisions.values()}
    assert len(s.decisions) == 3 and len(ids) == 1


def test_unit_wire_shape():
    u = plugin.Unit(id="u", key_suffix="s", prior=rs.Resources(memory="1Gi"), inputs=[rs.Input.sized("x", 5)],
                    code=rs.Code.from_source("x=1"), group="g")
    d = u.to_dict()
    assert d["prior"]["spec"]["requests"] == {"memory": GiB} and d["seed"]["code"]["kind"] == "source" and d["group"] == "g"
    assert plugin.Patch().to_dict() == {"kind": "none"}
    assert plugin.Patch({"memory": 1}).to_dict() == {"kind": "options", "options": {"memory": 1}}
    assert rs.Rejection("quota", field="memory").to_dict() == {"reason": "quota", "field": "memory"}


def test_cli():
    from rightsizer import cli

    assert cli.main(["version"]) == 0
    assert cli.main([]) == 2
    assert cli.main(["bogus"]) == 2
    out = subprocess.run([sys.executable, "-m", "rightsizer.cli", "models", "--help"], capture_output=True, text=True)
    assert out.returncode == 0 and "convert" in out.stdout
    out = subprocess.run([sys.executable, "-m", "rightsizer.cli", "serve", "--help"], capture_output=True, text=True)
    assert out.returncode == 0 and "--memory-basis" in out.stdout
    # Platform hooks: delegate to rightsizer.<platform>.cli, or print the extra.
    rc = cli.main(["flyte", "--help"])
    assert rc == 2  # placeholder round: no rightsizer.flyte.cli yet


@pytest.mark.parametrize("name", ["flyte", "ray"])
def test_platform_packages_import_their_platform(name):
    pytest.importorskip(name)
    out = subprocess.run([sys.executable, "-c", f"import sys, rightsizer.{name}; assert '{name}' in sys.modules"])
    assert out.returncode == 0
