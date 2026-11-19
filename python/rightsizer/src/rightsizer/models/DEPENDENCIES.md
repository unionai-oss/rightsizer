# Dependencies of `rightsizer.models` and `rightsizer.serve`

For whoever assembles `python/rightsizer/pyproject.toml`. Every module imports
with the standard library alone (`tests/models/test_light.py::test_imports_without_heavy_dependencies`
blocks torch, transformers, vllm, sglang, numpy, peft, safetensors, tokenizers,
jinja2, huggingface_hub, sklearn, skops, flyte and ray); heavy packages are
imported inside the functions that use them.

## Extras

```toml
[project.optional-dependencies]
# rightsizer serve with --backend vllm | sglang | openai: NumPy heads, no torch
serve = [
  "numpy>=1.26",
  "safetensors>=0.6",          # decision_head.safetensors (safetensors.numpy); adapter re-layout
  "huggingface_hub>=1.1,<2",   # snapshot_download of releases and tokenizer files
  "tokenizers>=0.22",          # the base model's tokenizer.json (ids + offsets)
  "jinja2>=3.1",               # the chat template (enable_thinking=False), as transformers renders it
]
# the in-process reference backend (--backend transformers), rightsizer models merge
torch = [
  "rightsizer[serve]",
  "torch>=2.4",
  "transformers>=4.51",        # Qwen3; tested with 5.23.0
  "peft>=0.15",                # tested with 0.21.3
]
# rightsizer models convert (unionai/rightsize-trees)
trees = [
  "numpy>=1.26",
  "scikit-learn>=1.5",         # tested with 1.9.1; the skops file holds HistGradientBoosting* models
  "skops>=0.13",               # tested with 0.16.0
  "huggingface_hub>=1.1,<2",
]
```

The protocol server itself (`rightsizer.serve`) is standard library only
(`http.server.ThreadingHTTPServer`); `rightsizer.serve.wrap(my_fn)` needs no extra.
vLLM and SGLang are **not** dependencies: they run as separate servers
(`--backend-url`) or are launched as subprocesses (`--launch`, which needs
`vllm` / `python -m sglang.launch_server` on `PATH` / in the environment).

Test-only: `pytest`, `jsonschema` (Estimate schema validation). The golden
prompt tests import the tuner checkout (`RIGHTSIZER_TUNER_PATH`) and need
`numpy`.

## Entry points

```toml
[project.scripts]
rightsizer = "rightsizer.cli:main"   # rightsizer serve ... | models ... | flyte ... | ray ... | version
```

`python -m rightsizer.serve` and `python -m rightsizer.models` work without it;
`rightsizer.models.cli:rightsizer` (serve / models only) is kept for compatibility.
These extras are declared in `python/rightsizer/pyproject.toml`.

## Tests

```bash
cd python/rightsizer
# light (no torch): prompts, grids, estimates, generate read-out, server
uv run --isolated --no-project --python 3.12 --with numpy --with tokenizers --with jinja2 \
    --with huggingface_hub --with jsonschema --with pytest pytest tests/models tests/serve
# everything (parity on a tiny local release, trees conversion)
uv run --isolated --no-project --python 3.12 --with torch --with transformers --with peft \
    --with safetensors --with huggingface_hub --with numpy --with tokenizers --with jinja2 \
    --with jsonschema --with scikit-learn --with skops --with pytest pytest tests/models tests/serve
```
