"""Import isolation (ARCHITECTURE.md §1): fresh uv environments with ONLY
the built wheel and rightsizer-runtime (no flyte, ray, torch, numpy), then
with Flyte only, then with Ray only.

Opt-in (it builds environments): ``RIGHTSIZER_ISOLATION_WHEEL=/path/to/rightsizer-*.whl``
(``maturin build`` in python/rightsizer). Flyte/Ray versions follow the repo
``.venv`` (``RIGHTSIZER_ISOLATION_FLYTE`` / ``_RAY`` override them).
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
RUNTIME = os.path.abspath(os.path.join(HERE, "..", "..", "..", "rightsizer-runtime"))
WHEEL = os.environ.get("RIGHTSIZER_ISOLATION_WHEEL")

pytestmark = pytest.mark.skipif(not WHEEL or not shutil.which("uv"), reason="set RIGHTSIZER_ISOLATION_WHEEL (and have uv)")

EXTRA = {
    "bare": [],
    "flyte": [f"flyte=={os.environ.get('RIGHTSIZER_ISOLATION_FLYTE', '2.13.6')}"],
    "ray": [f"ray=={os.environ.get('RIGHTSIZER_ISOLATION_RAY', '2.60.0')}"],
}


@pytest.fixture(scope="module")
def parquet(tmp_path_factory):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    p = tmp_path_factory.mktemp("data") / "train.parquet"
    pq.write_table(pa.table({"a": list(range(200_000)), "b": [f"s{i}" for i in range(200_000)]}), p)
    return str(p)


@pytest.mark.parametrize("mode", ["bare", "flyte", "ray"])
def test_isolated_environment(mode, parquet, tmp_path):
    env_dir = tmp_path / "venv"
    subprocess.run(["uv", "venv", "--quiet", "--python", "3.12", str(env_dir)], check=True)
    py = str(env_dir / "bin" / "python")
    subprocess.run(["uv", "pip", "install", "--quiet", "--python", py, WHEEL, RUNTIME, *EXTRA[mode]], check=True)
    listed = subprocess.run(["uv", "pip", "list", "--python", py], check=True, capture_output=True, text=True).stdout
    names = {line.split()[0].lower() for line in listed.splitlines()[2:] if line.strip()}
    if mode == "bare":
        assert names == {"rightsizer", "rightsizer-runtime"}, names
    for heavy in ("torch", "numpy") if mode != "ray" else ("torch",):
        assert heavy not in names or mode != "bare"
    if mode != "flyte":
        assert "flyte" not in names
    if mode != "ray":
        assert "ray" not in names
    work = tmp_path / "work"
    work.mkdir()
    env = {k: v for k, v in os.environ.items() if not k.startswith(("RIGHTSIZER_", "PYTHON", "VIRTUAL_ENV"))}
    env["RIGHTSIZER_TENANT"] = "iso"
    out = subprocess.run([py, "-I", os.path.join(HERE, "check_env.py"), mode, parquet, str(work)], cwd=str(work),
                         env=env, capture_output=True, text=True)
    assert out.returncode == 0, out.stderr[-4000:]
    res = json.loads(out.stdout.strip().splitlines()[-1])
    print(json.dumps(res))
    assert res["after_import"] == [] and res["after_decide"] == []
