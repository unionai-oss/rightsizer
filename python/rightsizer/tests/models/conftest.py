"""Test setup for rightsizer.models.

- ``src/`` goes on ``sys.path`` (the package is assembled later).
- The tuner checkout (for golden tests) is ``$RIGHTSIZER_TUNER_PATH`` or the
  model-factory clone used during development; tests that need it skip
  without it.
- The Qwen3 tokenizer comes from ``$RIGHTSIZER_TEST_TOKENIZER`` (a directory
  with ``tokenizer.json`` + ``tokenizer_config.json``) or the Hub
  (``Qwen/Qwen3-1.7B``, tokenizer files only, ~11 MB).
"""

from __future__ import annotations

import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.abspath(os.path.join(HERE, "..", "..", "src"))
if SRC not in sys.path:
    sys.path.insert(0, SRC)

_DEFAULT_TUNER = ("/private/tmp/claude-502/-Users-nielsbantilan-git-rightsizer/a3735c47-1f4a-42e4-9832-2a3282dcc6c0/"
                  "scratchpad/model-factory/model-factory-resource-tuner")
TUNER = os.environ.get("RIGHTSIZER_TUNER_PATH", _DEFAULT_TUNER)


def tuner_available() -> bool:
    return os.path.isdir(os.path.join(TUNER, "resource_tuner", "bench_all"))


@pytest.fixture(scope="session")
def tuner():
    """The tuner's modules: ``prompting``, ``k8s``, ``hub_pointer``, ``features``."""
    if not tuner_available():
        pytest.skip("tuner checkout not found (set RIGHTSIZER_TUNER_PATH)")
    pytest.importorskip("numpy")
    if TUNER not in sys.path:
        sys.path.insert(0, TUNER)
    import importlib

    mods = {}
    for name in ("prompting", "k8s", "hub_pointer", "features"):
        try:
            mods[name] = importlib.import_module(f"resource_tuner.bench_all.{name}")
        except ImportError as e:  # features needs nothing heavy; hub_pointer imports lazily
            pytest.skip(f"cannot import the tuner's {name}: {e}")
    return mods


@pytest.fixture(scope="session")
def qwen3_tokenizer_dir(tmp_path_factory):
    d = os.environ.get("RIGHTSIZER_TEST_TOKENIZER")
    if d and os.path.isdir(d):
        return d
    pytest.importorskip("huggingface_hub")
    from huggingface_hub import snapshot_download

    try:
        return snapshot_download("Qwen/Qwen3-1.7B", allow_patterns=["tokenizer.json", "tokenizer_config.json",
                                                                    "vocab.json", "merges.txt"])
    except Exception as e:  # noqa: BLE001 -- offline
        pytest.skip(f"Qwen3 tokenizer unavailable: {e}")
