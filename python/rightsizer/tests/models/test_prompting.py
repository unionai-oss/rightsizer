"""Golden tests: rightsizer.models.prompting against the tuner's own builder."""

from __future__ import annotations

import json
import re

import pytest
from cases import make_case

from rightsizer.models import prompting


def test_constants_match_tuner(tuner):
    tp = tuner["prompting"]
    assert prompting.SYSTEM == tp.SYSTEM
    assert prompting.KEYS == tp.KEYS
    assert prompting.OPTIONS == tuple(tuple(o) for o in tp.OPTIONS)
    assert prompting.ANSWER == tp.ANSWER
    assert prompting.options_text() == tp.options_text()
    assert prompting.FLYTE_RUNTIME_MIB == tuner["k8s"].FLYTE_RUNTIME_MIB


@pytest.mark.parametrize("seed", range(60))
def test_port_is_byte_for_byte(tuner, seed):
    """The port over tuner rows (Flyte and K8s twins, with and without hints)."""
    row, _ = make_case(seed, tuner["prompting"], tuner["k8s"], fmts=("npy", "csv", "jsonl", "parquet"))
    if seed % 5 == 0:
        row["hint"] = "Gradient-boosted trees fit on other tasks' runs estimate memory 2Gi 60%, 3Gi 30%."
    assert prompting.messages(row) == tuner["prompting"].messages(row)


def test_clipped_inputs_desc(tuner):
    row, _ = make_case(7, tuner["prompting"])
    row["inputs_desc"] = "\n".join(f"x{i}: csv, 1.00 GiB, header: " + ",".join(f"c{j}" for j in range(40))
                                   for i in range(12))
    assert prompting.messages(row) == tuner["prompting"].messages(row)


@pytest.mark.parametrize("seed", range(80))
def test_context_renders_like_the_tuner(tuner, seed):
    """A WorkloadContext built from the same facts renders the tuner's prompt
    exactly (npy / csv / jsonl inputs; Flyte and K8s; code, history, manifest)."""
    row, ctx = make_case(seed, tuner["prompting"], tuner["k8s"])
    assert prompting.messages_from_context(ctx) == tuner["prompting"].messages(row)


@pytest.mark.parametrize("seed", range(20))
def test_parquet_differs_only_in_schema(tuner, seed):
    """Parquet: rightsizer keeps column kinds as counts, so the current input's
    schema cannot be the tuner's ``name: kind`` list. Everything else matches,
    including history lines (the tuner strips schemas there)."""
    row, ctx = make_case(seed, tuner["prompting"], tuner["k8s"], fmts=("parquet",))
    ours = prompting.messages_from_context(ctx)[1]["content"]
    theirs = tuner["prompting"].messages(row)[1]["content"]
    strip = re.compile(r"schema=\[[^\]]*\]")
    if len(row["inputs_desc"]) <= prompting.MAX_DESC_CHARS:
        assert strip.sub("schema=[]", ours) == strip.sub("schema=[]", theirs)
    hist = lambda s: s[s.index("### Earlier runs"):]
    assert hist(ours) == hist(theirs)


def test_absent_fields_render_like_the_tuner_absent_values():
    ctx = {"v": 1, "key": "k8s://c/ns/job/x", "unit": "main", "platform": {"kind": "k8s"},
           "prior": {"spec": {"requests": {}, "limits": {}}}, "inputs": [], "history": []}
    user = prompting.messages_from_context(ctx)[1]["content"]
    assert user.startswith("### Job code (job.py)\n```python\n\n```\n\n### Job manifest\n```yaml\napiVersion: batch/v1")
    assert "### This run's inputs\n\n\n" in user
    assert "none -- this is the first run of this task" in user
    assert user.endswith(prompting.ANSWER)


def test_history_filters_and_orders():
    def rec(status, mem, t):
        return {"status": status, "memory_peak": {"value": mem * 2**20}, "duration_s": t, "inputs": []}

    ctx = {"platform": {"kind": "flyte"}, "history": [rec("succeeded", 300, 3), rec("oom_killed", 9000, 2),
                                                      rec("succeeded", 200, 1), {"status": "cached", "inputs": []}]}
    row = prompting.row_from_context(ctx)
    h = json.loads(row["history_json"])
    assert [round(e["peak_mem_mib"] + 70) for e in h] == [200, 300]  # oldest first; failures, cached dropped
    assert json.loads(prompting.row_from_context(ctx, max_history=1)["history_json"])[0]["wall_s"] == 3


def test_other_formats_render():
    ins = [{"name": "w", "format": "safetensors", "bytes": 3 << 30, "params": 1_500_000_000, "dtype": "BF16"},
           {"name": "n", "format": "scalar", "scalar": {"kind": "int", "value": 3}},
           {"name": "p", "format": "parquet", "bytes": 5 << 20, "rows": 1000,
            "columns": {"total": 3, "numeric": 3, "string": 0, "nested": 0, "other": 0}},
           {"name": "c", "format": "csv", "bytes": 100}]
    d = prompting.describe_input_metas(ins).splitlines()
    assert d[0] == "w: safetensors, 3.00 GiB, dtype=BF16, 1,500,000,000 parameters"
    assert d[1] == "n: scalar, value=3"
    assert d[2] == "p: parquet, 5.00 MiB, 1,000 rows, schema=[3 columns: 3 numeric]"
    assert d[3] == "c: csv, 100 B, header: unknown"
