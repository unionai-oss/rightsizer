"""End-to-end parity on a tiny local release (models.md §2):

(a) the tuner's own ``rightsize_pointer.RightSizePointer`` vs the ``transformers``
    backend + NumPy heads: probabilities within 1e-5 (fp32, CPU);
(b) the vLLM / SGLang paths against mock servers answering in the documented
    shapes with the reference states: equal;
(c) the Estimate JSON validates against ``schemas/core/1/estimate.json``.

Needs torch, transformers, peft (the ``torch`` extra) and the tuner checkout.
"""

from __future__ import annotations

import json
import os

import pytest

torch = pytest.importorskip("torch")
pytest.importorskip("transformers")
pytest.importorskip("peft")
np = pytest.importorskip("numpy")

from cases import make_case
from mock_backends import sglang_mock, vllm_mock

from rightsizer.models import prompting
from rightsizer.models.backends.sglang import SGLangBackend
from rightsizer.models.backends.transformers_backend import TransformersBackend
from rightsizer.models.backends.vllm import VLLMBackend
from rightsizer.models.chat import ChatTokenizer
from rightsizer.models.pointer import PointerModel, load_release

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
SCHEMA = os.path.join(REPO, "schemas", "core", "1", "estimate.json")
N_CASES = 12


@pytest.fixture(scope="module")
def release_dir(tmp_path_factory, tuner, qwen3_tokenizer_dir):
    from tiny_release import build

    return build(str(tmp_path_factory.mktemp("tiny")), qwen3_tokenizer_dir, tuner["prompting"], tuner["hub_pointer"])


@pytest.fixture(scope="module")
def cases(tuner):
    return [make_case(s, tuner["prompting"], tuner["k8s"]) for s in range(N_CASES)]


@pytest.fixture(scope="module")
def reference(release_dir, tuner):
    return tuner["hub_pointer"].RightSizePointer.from_pretrained(release_dir, device="cpu")


@pytest.fixture(scope="module")
def tf_backend(release_dir):
    rel = load_release(release_dir)
    return TransformersBackend(rel.config["base_model"], rel.path, device="cpu", dtype="float32")


@pytest.fixture(scope="module")
def model(release_dir, tf_backend):
    return PointerModel(load_release(release_dir), tf_backend)


def _max_diff(a: dict, b: dict) -> float:
    return max(float(np.max(np.abs(np.asarray(a[k]) - np.asarray(b[k])))) for k in a)


def test_truncation_is_exercised(model, cases):
    lens = [len(model.tok.ids(model.tok.apply_chat_template(prompting.messages_from_context(c))))
            for _, c in cases]
    assert max(lens) > model.release.config["max_len"] > min(lens)


def test_offsets_match_the_head_buffers(model):
    assert model.offsets == model.heads.offsets


def test_tokenizer_matches_transformers(model, reference, cases):
    for _, ctx in cases:
        msgs = prompting.messages_from_context(ctx)
        ours = model.tok.apply_chat_template(msgs, add_generation_prompt=True, enable_thinking=False)
        theirs = reference._chat_text(msgs)
        assert ours == theirs
        assert model.tok.ids(ours) == reference.tok(theirs, add_special_tokens=False)["input_ids"]


def test_reference_parity(model, reference, cases, record_property):
    worst = 0.0
    for _row, ctx in cases:
        msgs = prompting.messages_from_context(ctx)
        ours = model.probabilities(msgs)
        theirs = reference.probabilities(msgs)
        d = _max_diff(ours, theirs)
        worst = max(worst, d)
        assert d < 1e-5, d
        assert {k: model.release.options[k][int(np.argmax(v))] for k, v in ours.items()} == reference.request(msgs)
    record_property("max_abs_prob_diff_transformers", worst)
    print(f"transformers backend vs RightSizePointer: max |dP| = {worst:.3g}")


def test_batch_equals_single(model, cases):
    msgs = [prompting.messages_from_context(c) for _, c in cases[:4]]
    batch = model.probabilities_batch(msgs)
    for m, p in zip(msgs, batch):
        assert _max_diff(p, model.probabilities(m)) < 1e-12


@pytest.fixture(scope="module")
def mock_vllm(tf_backend):
    srv = vllm_mock(lambda ids: tf_backend.full_hidden_states(list(ids)))
    yield srv
    srv.close()


@pytest.fixture(scope="module")
def mock_sglang(tf_backend):
    srv = sglang_mock(lambda ids: tf_backend.full_hidden_states(list(ids)))
    yield srv
    srv.close()


@pytest.mark.parametrize("encoding", ["base64", "float"])
def test_vllm_mock_parity(release_dir, model, mock_vllm, cases, encoding):
    m = PointerModel(load_release(release_dir), VLLMBackend(mock_vllm.url, encoding_format=encoding, max_batch=5),
                     model.tok)
    assert m.ready()
    msgs = [prompting.messages_from_context(c) for _, c in cases]
    got = m.probabilities_batch(msgs)
    want = model.probabilities_batch(msgs)
    worst = max(_max_diff(a, b) for a, b in zip(got, want))
    assert worst < 1e-6, worst
    req = mock_vllm.requests[-1]
    assert req["task"] == "token_embed" and req["use_activation"] is False and req["model"] == "rightsize"
    assert isinstance(req["input"][0], list) and req["add_special_tokens"] is False


def test_sglang_mock_parity(release_dir, model, mock_sglang, cases):
    m = PointerModel(load_release(release_dir), SGLangBackend(mock_sglang.url, max_batch=5), model.tok)
    msgs = [prompting.messages_from_context(c) for _, c in cases]
    got = m.probabilities_batch(msgs)
    want = model.probabilities_batch(msgs)
    worst = max(_max_diff(a, b) for a, b in zip(got, want))
    assert worst < 1e-6, worst
    req = mock_sglang.requests[-1]
    assert req["return_hidden_states"] is True and req["sampling_params"]["max_new_tokens"] == 1
    assert req["lora_path"] == ["rightsize"] * len(req["input_ids"])


def test_estimates_validate_against_the_schema(model, cases):
    jsonschema = pytest.importorskip("jsonschema")
    with open(SCHEMA) as f:
        schema = json.load(f)
    ctxs = [c for _, c in cases]
    for kind in ("demand", "action"):
        model.kind = kind
        try:
            for est in model.estimate_batch(ctxs):
                jsonschema.validate(est, schema)
                assert est["kind"] == kind
                assert est["provenance"]["version"] == model.release.commit
                for axis, n in (("memory", 27), ("cpu_mean", 25), ("cpu_peak", 25), ("gpu_memory", 26)):
                    p = est[axis]["probs"]
                    assert est[axis]["grid"] == "rs-grid-v1" and len(p) == n
                    assert abs(sum(p) - 1) < 1e-9
                if kind == "action":
                    assert all(max(est[a]["probs"]) == 1.0 for a in ("memory", "cpu_mean", "gpu_memory"))
        finally:
            model.kind = "demand"


def test_gpu_threshold(model, cases):
    msgs = [prompting.messages_from_context(c) for _, c in cases]
    for p, est in zip(model.probabilities_batch(msgs), model.estimate_batch([c for _, c in cases])):
        g = est["gpu_memory"]["probs"]
        if p["gpu"][0] >= model.release.config["gpu_threshold"]:
            assert g[0] == 1.0
        else:
            assert g[0] == 0.0 and abs(sum(g[1:]) - 1) < 1e-9


def test_relayout_adapter_for_causal_lm(release_dir, tf_backend, tmp_path, cases):
    """The CausalLM-layout adapter on ``Qwen3ForCausalLM`` gives the reference states
    (what vLLM / SGLang load with ``--enable-lora``)."""
    from peft import PeftModel
    from transformers import AutoModelForCausalLM

    from rightsizer.models.merge import causal_lm_adapter

    rel = load_release(release_dir)
    ad = causal_lm_adapter(rel.path, str(tmp_path / "adapter"))
    lm = AutoModelForCausalLM.from_pretrained(rel.config["base_model"], dtype=torch.float32)
    lm = PeftModel.from_pretrained(lm, ad).eval()
    n_lora = sum("lora_A" in n for n, _ in lm.named_parameters())
    assert n_lora == 2 * 7
    tok = ChatTokenizer.from_pretrained(rel.config["base_model"])
    ids = tok.ids(tok.apply_chat_template(prompting.messages_from_context(cases[0][1])))[-256:]
    x = torch.tensor([ids])
    with torch.no_grad():
        H = lm(input_ids=x, output_hidden_states=True).hidden_states[-1][0].numpy()
        H2 = lm.base_model.model.model(input_ids=x).last_hidden_state[0].numpy()
    ref = tf_backend.full_hidden_states(ids)
    assert np.max(np.abs(H2 - ref)) < 1e-5
    assert np.max(np.abs(H - ref)) < 1e-5  # HF's last hidden_states entry is post-norm too


def test_merge(release_dir, tf_backend, tmp_path, cases):
    from transformers import AutoModel

    from rightsizer.models.merge import merge

    rel = load_release(release_dir)
    out = merge(rel.path, str(tmp_path / "merged"), dtype="float32")
    m = AutoModel.from_pretrained(out, dtype=torch.float32).eval()
    tok = ChatTokenizer.from_pretrained(out)
    ids = tok.ids(tok.apply_chat_template(prompting.messages_from_context(cases[1][1])))[-256:]
    with torch.no_grad():
        H = m(input_ids=torch.tensor([ids])).last_hidden_state[0].numpy()
    assert np.max(np.abs(H - tf_backend.full_hidden_states(ids))) < 1e-4


def test_serve_end_to_end_over_mock_vllm(release_dir, model, mock_vllm, cases):
    """``rightsizer serve --backend vllm --backend-url ...`` assembled by
    ``build_estimator``, behind the protocol server; estimates equal the
    transformers backend's and validate against the schema."""
    import urllib.request

    from rightsizer.models.load import ServeOptions, build_estimator
    from rightsizer.serve import EstimatorServer

    est = build_estimator(ServeOptions(model=release_dir, backend="vllm", backend_url=mock_vllm.url + "/v1"))
    ctxs = [c for _, c in cases[:6]]
    with EstimatorServer(est, port=0) as srv:
        req = urllib.request.Request(srv.url + "/estimate:batch", data=json.dumps({"contexts": ctxs}).encode(),
                                     method="POST", headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=60) as r:
            body = json.loads(r.read())
        with urllib.request.urlopen(srv.url + "/info", timeout=10) as r:
            info = json.loads(r.read())
    assert info["id"] == os.path.basename(release_dir) and info["version"].startswith("local-")
    assert info["backend"] == "vllm" and info["kind"] == "demand"
    want = model.estimate_batch(ctxs)
    with open(SCHEMA) as f:
        schema = json.load(f)
    jsonschema = pytest.importorskip("jsonschema")
    for item, w in zip(body["results"], want):
        e = item["estimate"]
        jsonschema.validate(e, schema)
        for axis in ("memory", "cpu_mean", "cpu_peak", "gpu_memory"):
            assert np.allclose(e[axis]["probs"], w[axis]["probs"], atol=1e-6)
