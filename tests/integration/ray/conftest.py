"""Integration tests for ``rightsizer.ray`` on the local Ray head
(``tests/infra/ray.sh up``). Opt-in: ``RSZ_IT_RAY=1``.

    tests/infra/ray.sh up
    RSZ_IT_RAY=1 .venv/bin/python -m pytest tests/integration/ray -v -s

Evidence (numbers, records, decisions) is written as JSON to
``tests/infra/.state/ray-it/`` and printed with ``-s``.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
STATE = os.path.join(REPO, "tests", "infra", ".state")
RESULTS = os.path.join(STATE, "ray-it")
if HERE not in sys.path:
    sys.path.insert(0, HERE)

if os.environ.get("RSZ_IT_RAY") != "1":
    pytest.skip("Ray integration tests are opt-in: RSZ_IT_RAY=1 (and tests/infra/ray.sh up)", allow_module_level=True)


def _load_env() -> None:
    path = os.path.join(STATE, "ray.env")
    if not os.path.exists(path):
        pytest.skip("no local Ray head: run tests/infra/ray.sh up", allow_module_level=True)
    for line in open(path, encoding="utf-8"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        line = line[len("export "):] if line.startswith("export ") else line
        k, _, v = line.partition("=")
        os.environ.setdefault(k, v)


_load_env()


def record(name: str, data: dict) -> None:
    os.makedirs(RESULTS, exist_ok=True)
    with open(os.path.join(RESULTS, f"{name}.json"), "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    print(f"\n[{name}] " + json.dumps(data, indent=2, default=str))


@pytest.fixture(scope="session")
def ray_session():
    import ray

    # Workers import the task module from this directory.
    ray.init(address=os.environ["RSZ_IT_RAY_ADDRESS"], logging_level="ERROR",
             runtime_env={"env_vars": {"PYTHONPATH": HERE}})
    yield ray
    ray.shutdown()


@pytest.fixture
def store():
    d = tempfile.mkdtemp(prefix="rsz-ray-it-")
    return d


@pytest.fixture(autouse=True)
def _reset():
    import rightsizer as rs
    import rightsizer.ray._settings as st

    st._reset_for_tests()
    yield
    rs.ray.observer().wait_idle(30)
    st._reset_for_tests()
    rs.clear_estimators()


def engine(store, *, mode="recommend", scoped=False, allow_unverified=False, ttl="1s", **extra):
    """An engine on a local store that counts as durable and shared (one host runs every surface)."""
    import rightsizer as rs

    cfg = {
        "history": {"uri": store, "durable": True, "shared": True, "cache_ttl": ttl,
                    "allow_unverified_for_reductions": allow_unverified},
        "decision": {"memo_ttl": ttl},
        "policy": {"mode": mode},
        "ray": {"tenant": "it/local", "tenant_scoped_credentials": scoped},
    }
    for k, v in extra.items():
        cfg.setdefault(k, {}).update(v)
    return rs.Engine(cfg, env_overrides=False)


def run_records(store, key_part=""):
    import glob

    out = []
    for p in glob.glob(f"{store}/v1/runs/**/*.json", recursive=True):
        try:
            r = json.loads(open(p, encoding="utf-8").read())
        except Exception:
            continue
        if key_part in r.get("key", ""):
            out.append(r)
    return sorted(out, key=lambda r: r.get("started_at", ""))
