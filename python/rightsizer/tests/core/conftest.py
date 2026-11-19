"""Fixtures for the typed API / ``_core`` tests (need the built extension:
``maturin develop`` in ``python/rightsizer``)."""

from __future__ import annotations

import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.abspath(os.path.join(HERE, "..", "..", "src"))
if SRC not in sys.path:
    sys.path.insert(0, SRC)
REPO = os.path.abspath(os.path.join(HERE, "..", "..", "..", ".."))
WIRE = os.path.join(REPO, "tests", "fixtures", "wire")
SCHEMAS = os.path.join(REPO, "schemas")


@pytest.fixture(scope="session")
def core():
    try:
        from rightsizer import _core
    except ImportError as e:  # pragma: no cover
        pytest.skip(f"rightsizer._core not built ({e}); run `maturin develop` in python/rightsizer")
    return _core


@pytest.fixture(autouse=True)
def _isolated(monkeypatch, tmp_path):
    # No user / system config or overrides leak into tests.
    for k in list(os.environ):
        if k.startswith("RIGHTSIZER_"):
            monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("RIGHTSIZER_TENANT", "alice")
    yield
    import rightsizer as rs

    rs.configure()
    try:
        rs.clear_estimators()
    except Exception:
        pass


@pytest.fixture
def store(tmp_path):
    return str(tmp_path / "store")


@pytest.fixture
def engine(core, store):
    import rightsizer as rs

    return rs.Engine({"history": {"uri": store}}, env_overrides=False)


@pytest.fixture
def parquet_file(tmp_path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    p = tmp_path / "train.parquet"
    pq.write_table(pa.table({"a": list(range(50_000)), "b": [str(i) for i in range(50_000)]}), p)
    return str(p)
