"""Serving model-factory models (models.md).

- :mod:`.prompting` -- the tuner's prompt, ported, and ``WorkloadContext`` -> prompt;
- :mod:`.pointer` -- pointer releases: NumPy heads, calibration, option offsets, Estimates;
- :mod:`.backends` -- hidden states / generation from transformers, vLLM, SGLang, OpenAI-compatible servers;
- :mod:`.generate` -- the generative read-out (samples -> empirical cells);
- :mod:`.trees` -- ``rightsize-trees`` (skops) -> ``rightsizer-trees/1``;
- :mod:`.merge` -- LoRA re-layout and offline merge for serving frameworks;
- :mod:`.load` -- what ``rightsizer serve`` builds.

Importing this package (or any submodule) needs neither torch, transformers,
vllm nor sglang; heavy dependencies are imported where they are used.
"""
