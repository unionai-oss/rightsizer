"""Light tests: no torch. Imports, grids, estimates, the generative read-out,
NumPy heads against a hand computation, offsets with tokenizers + jinja."""

from __future__ import annotations

import importlib
import json
import os
import subprocess
import sys

import pytest

from rightsizer.models import estimate, generate, grids, prompting

SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
MODULES = ["rightsizer.models", "rightsizer.models.prompting", "rightsizer.models.grids", "rightsizer.models.chat",
           "rightsizer.models.estimate", "rightsizer.models.pointer", "rightsizer.models.generate",
           "rightsizer.models.trees", "rightsizer.models.merge", "rightsizer.models.load", "rightsizer.models.cli",
           "rightsizer.models.backends", "rightsizer.models.backends.http", "rightsizer.models.backends.vllm",
           "rightsizer.models.backends.sglang", "rightsizer.models.backends.openai",
           "rightsizer.models.backends.launch", "rightsizer.models.backends.transformers_backend",
           "rightsizer.serve", "rightsizer.serve.server", "rightsizer.serve.errors", "rightsizer.serve.cli"]


def test_imports_without_heavy_dependencies():
    """Every module imports with the standard library alone: no torch,
    transformers, vllm, sglang, numpy, flyte or ray gets imported."""
    code = ("import sys, importlib\n"
            f"sys.path.insert(0, {SRC!r})\n"
            "class Block:\n"
            "    def find_spec(self, name, path=None, target=None):\n"
            "        if name.split('.')[0] in ('torch','transformers','vllm','sglang','numpy','flyte','ray','peft',"
            "'safetensors','tokenizers','jinja2','huggingface_hub','sklearn','skops'):\n"
            "            raise ImportError('blocked ' + name)\n"
            "sys.meta_path.insert(0, Block())\n"
            f"for m in {MODULES!r}: importlib.import_module(m)\n"
            "print('ok')\n")
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert r.returncode == 0 and r.stdout.strip() == "ok", r.stderr


def test_rs_grid_matches_rust():
    assert len(grids.RS_MEMORY) == 27 and len(grids.RS_CPU) == 25 and len(grids.RS_GPU) == 26
    assert grids.RS_MEMORY[16] == 64 << 30 and grids.RS_MEMORY[-1] == 2048 << 30
    assert grids.RS_GPU[0] == 0 and grids.RS_GPU[2] == int(1.5 * 2**30)


def test_tuner_options_are_rs_prefixes():
    for key, opts in zip(prompting.KEYS, prompting.OPTIONS):
        vals = grids.option_values(key, opts)
        axis = grids.AXIS_VALUES[grids.KEY_AXIS[key]]
        assert [float(v) for v in axis[: len(vals)]] == vals
        p = [1.0 / len(opts)] * len(opts)
        rs = grids.to_rs_grid(key, opts, p)
        assert rs[: len(opts)] == p and sum(rs[len(opts):]) == 0


def test_foreign_option_rebins_conservatively():
    rs = grids.to_rs_grid("memory", ["300Mi", "1Gi", "100Gi"], [0.2, 0.3, 0.5])
    assert rs[1] == 0.2 and rs[4] == 0.3 and rs[18] == 0.5  # 384Mi, 1Gi, 128Gi


def _probs():
    m = [0.0] * 17
    m[6], m[7] = 0.7, 0.3
    c = [0.0] * 16
    c[3] = 1.0
    g = [0.3] + [0.7 / 17] * 17
    return {"memory": m, "cpu": c, "gpu": g}


OPTS = dict(zip(prompting.KEYS, prompting.OPTIONS))


def test_build_estimate_demand_and_threshold():
    # memory_basis="need": the tuner's cells as labelled.
    e = estimate.build_estimate(_probs(), OPTS, estimator="r", version="abc", gpu_threshold=0.5, memory_basis="need")
    assert e["kind"] == "demand" and e["memory"]["probs"][6] == pytest.approx(0.7)
    # The default converts need -> demand: the same mass, never higher on the grid.
    d = estimate.build_estimate(_probs(), OPTS, estimator="r", version="abc", gpu_threshold=0.5)
    assert sum(d["memory"]["probs"]) == pytest.approx(1.0)
    assert d["memory"]["probs"][6] < 0.7 and sum(d["memory"]["probs"][:7]) >= sum(e["memory"]["probs"][:7])
    assert d["memory"] == estimate.build_estimate(_probs(), OPTS, estimator="r", version="abc", gpu_threshold=0.5,
                                                  memory_basis="demand")["memory"]
    assert e["cpu_mean"] == e["cpu_peak"] and e["cpu_mean"]["probs"][3] == 1.0
    assert e["gpu_memory"]["probs"][0] == 0.0 and sum(e["gpu_memory"]["probs"]) == pytest.approx(1.0)
    e = estimate.build_estimate(_probs(), OPTS, estimator="r", version="abc", gpu_threshold=0.25)
    assert e["gpu_memory"]["probs"][0] == 1.0
    assert e["provenance"] == {"estimator": "r", "version": "abc", "grid": "rs-grid-v1", "features": "features-v1",
                               "latency_ms": 0}


def test_build_estimate_action_and_axes():
    e = estimate.build_estimate(_probs(), OPTS, estimator="r", version="v", kind="action", axes=["memory", "gpu_memory"])
    assert set(e) == {"v", "kind", "memory", "gpu_memory", "provenance"}
    assert e["memory"]["probs"][6] == 1.0 and e["gpu_memory"]["probs"][0] == 1.0  # argmax, no threshold


def test_need_to_demand_moves_mass_down():
    p = [0.0] * 17
    p[4] = 1.0  # 1Gi need
    vals = grids.option_values("memory", prompting.MEM_OPTIONS)
    d = estimate.need_to_demand(p, vals, 70 * 2**20)
    assert sum(d) == pytest.approx(1.0)
    # demand in (768/1.05 - 70, 1024/1.05 - 70] MiB = (661, 905]: cells 768Mi and 1Gi
    assert d[3] > 0 and d[4] > 0 and d[3] + d[4] == pytest.approx(1.0)


@pytest.mark.parametrize("text,want", [
    ('{"memory": "2Gi", "cpu": "1.5", "gpu": "none"}', (6, 5, 0)),
    ('```json\n{"memory": "3 GiB", "cpu": 2, "gpu": "22GiB"}\n```', (7, 7, 11)),
    ("Sure: {'memory': '700Mi', 'cpu': '500m', 'gpu': 'none',}", (3, 1, 0)),
    ('{memory: 2048, cpu: 3}', (6, 9, 0)),
    ('thinking {"x": 1} then {"memory": "100Gi"}', (18, 3, 0)),
    ('{"memory": "1.5Gi", "gpu": "5"}', (5, 3, 6)),
    ("no json here", None),
])
def test_parse_answer(text, want):
    got = generate.parse_answer(text)
    assert (None if got is None else (got["memory"], got["cpu"], got["gpu_memory"])) == want


def test_empirical_estimate():
    texts = ['{"memory": "2Gi", "cpu": 1}'] * 3 + ['{"memory": "4Gi", "cpu": 2, "gpu": "15GiB"}', "garbage"]
    e = generate.empirical_estimate(texts, estimator="m", version="1")
    assert e["memory"]["probs"][6] == 0.75 and e["memory"]["probs"][8] == 0.25
    assert e["gpu_memory"]["probs"][0] == 0.75 and e["gpu_memory"]["probs"][10] == 0.25
    assert generate.empirical_estimate(["nope"], estimator="m", version="1") is None


def test_openai_backend_against_mock():
    from mock_backends import openai_mock

    from rightsizer.models.backends.openai import OpenAIBackend

    srv = openai_mock(['{"memory": "2Gi", "cpu": 1, "gpu": "none"}', 'I think {"memory": "3Gi", "cpu": "2"}'])
    try:
        be = OpenAIBackend(srv.url + "/v1", "m", extra_body={"chat_template_kwargs": {"enable_thinking": False}})
        assert be.ready()
        model = generate.GenerativeModel(be, model_id="m", n=4)
        ctx = {"platform": {"kind": "flyte"}, "key": "flyte://a/b/c/d", "unit": "u", "prior": {}}
        (est,) = model.estimate_batch([ctx])
        assert est["memory"]["probs"][6] == 0.5 and est["memory"]["probs"][7] == 0.5
        req = srv.requests[-1]
        assert req["n"] == 4 and req["chat_template_kwargs"] == {"enable_thinking": False}
        assert req["messages"][0]["content"] == prompting.SYSTEM
    finally:
        srv.close()


def test_numpy_heads_by_hand():
    np = pytest.importorskip("numpy")
    from rightsizer.models.pointer import PointerHeads

    rng = np.random.default_rng(1)
    h, d = 8, 4
    st = {"norm.weight": rng.normal(size=h), "norm.bias": rng.normal(size=h), "off0": np.array([3, 2, 1])}
    st |= {"q.0.weight": rng.normal(size=(d, h)), "q.0.bias": rng.normal(size=d),
           "k.0.weight": rng.normal(size=(d, h)), "k.0.bias": rng.normal(size=d)}
    heads = PointerHeads(st, 1)
    a, o = rng.normal(size=h), rng.normal(size=(3, h))

    def ln(x):
        return (x - x.mean()) / np.sqrt(x.var() + 1e-5) * st["norm.weight"] + st["norm.bias"]

    q = st["q.0.weight"] @ ln(a) + st["q.0.bias"]
    want = [float(q @ (st["k.0.weight"] @ ln(r) + st["k.0.bias"])) / 2.0 for r in o]
    assert np.allclose(heads.logits(a, [o])[0], want)
    assert heads.offsets == [[3, 2, 1]]


def test_offsets_with_tokenizers_and_jinja(qwen3_tokenizer_dir):
    pytest.importorskip("tokenizers")
    pytest.importorskip("jinja2")
    from rightsizer.models.chat import ChatTokenizer, option_offsets

    tok = ChatTokenizer.from_pretrained(qwen3_tokenizer_dir)
    msgs = prompting.messages({"task_code": "x = 1", "inputs_desc": "x: npy, 1 KiB, shape=(1,), dtype=uint8",
                               "history_json": "[]"})
    text = tok.apply_chat_template(msgs)
    assert text.startswith("<|im_start|>system\n") and text.endswith("<|im_start|>assistant\n<think>\n\n</think>\n\n")
    offs = option_offsets(tok, text, prompting.KEYS, prompting.OPTIONS)
    assert [len(o) for o in offs] == [17, 16, 18]
    assert all(a > b for o in offs for a, b in zip(o, o[1:]))  # earlier options sit further back


def test_cli_prompt(tmp_path, capsys):
    from rightsizer.models.cli import main

    p = tmp_path / "ctx.json"
    p.write_text(json.dumps({"platform": {"kind": "flyte"}, "code": {"digest": "d", "source": "print(1)"}}))
    assert main(["prompt", str(p)]) == 0
    msgs = json.loads(capsys.readouterr().out)
    assert "print(1)" in msgs[1]["content"]


def test_reimport():
    for m in MODULES:
        importlib.import_module(m)


def test_launcher_start_health_teardown(tmp_path):
    from rightsizer.models.backends.launch import Launcher, free_port

    port = free_port()
    script = tmp_path / "fake_server.py"
    script.write_text(
        "import http.server, sys, time\n"
        "time.sleep(0.5)\n"
        "class H(http.server.BaseHTTPRequestHandler):\n"
        "    def do_GET(self):\n"
        "        self.send_response(200 if self.path == '/health' else 404); self.end_headers()\n"
        "    def log_message(self, *a): pass\n"
        f"http.server.HTTPServer(('127.0.0.1', {port}), H).serve_forever()\n")
    la = Launcher([sys.executable, str(script)], f"http://127.0.0.1:{port}/health", timeout=20, poll=0.1,
                  log_path=str(tmp_path / "log"))
    with la:
        assert la.healthy()
        pid = la.proc.pid
    assert la.proc is None
    with pytest.raises(OSError):
        os.kill(pid, 0)


def test_launcher_reports_early_exit(tmp_path):
    from rightsizer.models.backends.launch import Launcher

    la = Launcher([sys.executable, "-c", "raise SystemExit(3)"], "http://127.0.0.1:9/health", timeout=10, poll=0.1)
    with pytest.raises(RuntimeError, match="exited with 3"):
        la.start()


def test_launch_commands():
    from rightsizer.models.backends.sglang import sglang_command
    from rightsizer.models.backends.vllm import vllm_command

    v = vllm_command("Qwen/Qwen3-1.7B", adapter="/a", lora_rank=16, port=8001, max_model_len=2112)
    assert v[:3] == ["vllm", "serve", "Qwen/Qwen3-1.7B"]
    assert json.loads(v[v.index("--pooler-config") + 1]) == {"task": "token_embed", "pooling_type": "ALL",
                                                              "use_activation": False}
    assert v[v.index("--lora-modules") + 1] == "rightsize=/a" and "--enable-lora" in v
    m = vllm_command("/merged", port=8001)
    assert "--enable-lora" not in m and m[m.index("--served-model-name") + 1] == "rightsize"
    s = sglang_command("Qwen/Qwen3-1.7B", adapter="/a", python="python")
    assert s[:3] == ["python", "-m", "sglang.launch_server"] and "--disable-radix-cache" in s
    assert s[s.index("--return-hidden-states-mode") + 1] == "full" and s[s.index("--lora-paths") + 1] == "rightsize=/a"


def test_build_estimator_picks_the_readout(tmp_path):
    from rightsizer.models.load import ServeOptions, build_estimator

    est = build_estimator(ServeOptions(model=str(tmp_path), backend="vllm", backend_url="http://127.0.0.1:9"))
    assert isinstance(est, generate.GenerativeModel) and est.backend.name == "openai"
    est = build_estimator(ServeOptions(model="Qwen/Qwen3-8B", backend="openai", backend_url="http://127.0.0.1:9/v1",
                                       samples=3))
    assert est.n == 3 and est.info()["id"] == "Qwen/Qwen3-8B"
    with pytest.raises(ValueError, match="backend-url"):
        build_estimator(ServeOptions(model=str(tmp_path), backend="sglang"))


def test_memory_basis_defaults_to_demand():
    import inspect

    from rightsizer.models import load, pointer
    from rightsizer.serve.cli import parser

    assert estimate.build_estimate.__kwdefaults__["memory_basis"] == "demand"
    assert inspect.signature(pointer.PointerModel.__init__).parameters["memory_basis"].default == "demand"
    assert load.ServeOptions(model="m").memory_basis == "demand"
    assert parser().parse_args(["--model", "m", "--backend", "vllm"]).memory_basis == "demand"
    assert parser().parse_args(["--model", "m", "--backend", "vllm", "--memory-basis", "need"]).memory_basis == "need"
    from rightsizer.models import cli, trees

    assert inspect.signature(trees.convert).parameters["memory_basis"].default == "demand"
    assert cli._parser().parse_args(["convert", "r", "--out", "o"]).memory_basis == "demand"
