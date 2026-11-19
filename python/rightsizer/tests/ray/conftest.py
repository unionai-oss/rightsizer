"""Fixtures for ``rightsizer.ray`` unit tests (need Ray 2.60 in the venv and
the built ``_core``). No Ray cluster: Ray's base ``RemoteFunction._remote``
and actor creation are replaced by recorders, so the tests see exactly the
options rightsizer hands to Ray."""

from __future__ import annotations

import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.abspath(os.path.join(HERE, "..", "..", "src"))
if SRC not in sys.path:
    sys.path.insert(0, SRC)

ray = pytest.importorskip("ray")
if HERE not in sys.path:
    sys.path.insert(0, HERE)
from rayfx import make_engine  # noqa: E402


@pytest.fixture(scope="session")
def core():
    try:
        from rightsizer import _core
    except ImportError as e:  # pragma: no cover
        pytest.skip(f"rightsizer._core not built ({e})")
    return _core


@pytest.fixture(autouse=True)
def _isolated(monkeypatch, tmp_path):
    for k in list(os.environ):
        if k.startswith("RIGHTSIZER_") or k in ("RAY_CLUSTER_NAMESPACE", "POD_NAMESPACE", "RAY_CLUSTER_NAME"):
            monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("RIGHTSIZER_TENANT", "alice")
    import ray._private.auto_init_hook as aih

    monkeypatch.setattr(aih, "enable_auto_connect", False)  # never start a local Ray from a unit test
    import rightsizer.ray._settings as st

    st._reset_for_tests()
    yield
    import rightsizer as rs

    st._reset_for_tests()
    rs.configure()
    rs.clear_estimators()


@pytest.fixture
def store(tmp_path):
    return str(tmp_path / "store")


@pytest.fixture
def engine_factory(core, store):
    import rightsizer.ray as rr

    def f(**kw):
        e = make_engine(store, **kw)
        rr.configure(engine=e, observe=False)
        return e

    return f


@pytest.fixture
def captured(monkeypatch):
    """Replace Ray's ``RemoteFunction._remote`` with a recorder."""
    from ray.remote_function import RemoteFunction

    calls = []

    def fake(self, args=None, kwargs=None, serialized_runtime_env_info=None, **task_options):
        calls.append({"args": args, "kwargs": dict(kwargs or {}), "options": dict(task_options)})
        return f"ref-{len(calls)}"

    monkeypatch.setattr(RemoteFunction, "_remote", fake)
    return calls
