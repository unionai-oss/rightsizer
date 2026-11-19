# Serving model-factory models (`rightsizer.models`, `rightsizer.serve`)

Status: **draft v1**. Core concepts: [SPEC.md](SPEC.md) §6 (estimators,
Estimator Protocol). Model sources: the RightSize Bench release of
[`unionai-oss/model-factory`](https://github.com/unionai-oss/model-factory)
(`model-factory-resource-tuner/docs/huggingface-release.md`), published under
`huggingface.co/unionai`, each release tagged (`k8s-2`).

## 1. What is published, and how each is served

| repo | what it is | served by rightsizer as |
|---|---|---|
| `unionai/rightsize-trees` | quantile trees + GPU classifier (`model.skops`), `rightsize_config.json` (feature names, quantile levels, threshold) | **converted**, not served: `rightsizer models convert` writes `rightsizer-trees/1` (+ novelty companion when present) and the Rust `gbt` estimator runs it in-process (§6.2). No Python at decision time. |
| `unionai/rightsize-decision-qwen3-1.7b`, `-rl-qwen3-1.7b` (+ `-job-only`, `-flyte-only`; seeds as branches `seed-2`, `seed-3`) | Qwen3-1.7B torso + LoRA adapter + three pointer heads (`decision_head.safetensors`), `rightsize_config.json` (`base_model`, `max_len`, `head_dim`, `keys`, `options`, `temperatures`, `gpu_threshold`), reference `rightsize_pointer.py` | an **Estimator Protocol server** (`rightsizer serve`) that renders the context into the model's prompt, gets hidden states from a **serving backend**, applies the heads and calibration, and returns per-axis cell probabilities |
| any chat model (zero-shot teachers, SFT models that write `{"memory": …}`) | text generation | the same server with the `generate` read-out: `n` samples → empirical distribution |

## 2. Serving backends

The torso is the expensive part, so it runs in a popular serving framework and
rightsizer applies only the heads (a LayerNorm and two linear maps per axis,
in NumPy). A backend answers one question: *the last-layer hidden states at a
few token positions of a prompt*.

| backend | how | notes |
|---|---|---|
| `vllm` | an OpenAI-compatible vLLM server running the base model with the LoRA adapter, queried through its pooling API for per-token hidden states | rightsizer can launch it (`--launch`) or attach to a URL |
| `sglang` | an SGLang server with the LoRA adapter, `/generate` with `return_hidden_states` | same |
| `transformers` | in-process `transformers` + `peft`, the reference implementation | CPU or one GPU; used for parity tests |
| `openai` (generate only) | any OpenAI-compatible chat endpoint (vLLM, SGLang, llama.cpp, Ollama) | for generative read-outs |

Each backend is verified against the `transformers` reference on the same
prompts: option probabilities within 1e-3 absolute in fp32, and the same argmax
on the release's validation prompts (the release itself accepts 5 of 6 under
bf16 near-ties).

## 3. From a `WorkloadContext` to the model's prompt

- The prompt is rendered **exactly** as the model was trained
  (`resource_tuner/bench_all/prompting.py`): system message, the task source,
  input metadata within the information boundary (SPEC §5.2), run history,
  then the fixed `### Options` block and answer line. A port lives in
  `rightsizer.models.prompting` with golden tests against the tuner's
  builder.
- Fields the context lacks are rendered the way the tuner renders absent
  values. A context with no code is still answered; the gate (§6.5, M4)
  decides whether to trust it.
- Privacy (SPEC §11) applies when the server is remote: the client redacts
  before sending, and the server never logs contexts by default.

## 4. From option probabilities to an `Estimate`

- Each axis's options are the tuner grid's cells, which are prefixes of
  `rs-grid-v1` (SPEC §5.3). Probabilities map cell to cell; foreign cells are
  rebinned conservatively.
- The default `kind` is **`demand`**: the calibrated probabilities are read
  as a demand distribution, and rightsizer's expected-cost decode (SPEC §7)
  picks the request. This matches the paper's cost-aware read-out, and lets
  constraints, S3 and the platform's retry semantics apply. `kind = action`
  reproduces the `-argmax` rows.
- The GPU axis uses `gpu_threshold` for "no GPU", as the reference does.
- Provenance records the repo, revision (commit sha, not just the tag),
  backend and base model.

## 5. Running it

```bash
pip install 'rightsizer[serve]'                        # no torch; for vllm/sglang backends
rightsizer serve --model unionai/rightsize-decision-qwen3-1.7b --revision k8s-2 \
    --backend vllm --launch --port 8095                 # starts vllm, then the protocol server
rightsizer serve --model ... --backend sglang --backend-url http://gpu-box:30000
rightsizer models convert unionai/rightsize-trees --revision k8s-2 --out /etc/rightsizer/trees.json
```

```toml
[estimator]
chain = ["decision", "rules-sized", "history-peak", "prior"]
[estimator.members.decision]
url = "http://localhost:8095/v1"
```

The server is the reference server of SPEC §6.3 (`rightsizer.serve`) with a
model plugged in. It implements `/v1/info` (`id` = repo, `version` = commit
sha, `grids`, `axes`, `kind`, `needs`, `features`, `max_batch`),
`/v1/estimate`, `/v1/estimate:batch` (batched into one backend call) and
`/v1/health` (503 until the backend answers). A Flyte app template serves it
on a GPU.
