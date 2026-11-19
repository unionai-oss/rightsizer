"""Make a release's LoRA adapter servable by vLLM / SGLang.

The tuner trains the adapter on ``AutoModel`` (``Qwen3Model``, peft
``task_type="FEATURE_EXTRACTION"``), so its tensors are named
``base_model.model.layers.N...``. Serving frameworks load the base checkpoint
as ``Qwen3ForCausalLM``, whose decoder is under ``model.``: they expect
``base_model.model.model.layers.N...``. Two ways out:

- :func:`causal_lm_adapter` rewrites the adapter's tensor names (and
  ``task_type``) into the CausalLM layout, so the framework serves the base
  model with ``--enable-lora`` (pure safetensors, no torch);
- :func:`merge` folds the adapter into the base weights offline (peft
  ``merge_and_unload``) and writes a full ``Qwen3ForCausalLM`` checkpoint plus
  tokenizer, served without LoRA (``rightsizer models merge``; needs torch).
"""

from __future__ import annotations

import json
import os
import shutil

ADAPTER_WEIGHTS = "adapter_model.safetensors"
ADAPTER_CONFIG = "adapter_config.json"


def causal_lm_adapter(src: str, dst: str, prefix: str = "model.") -> str:
    """Copy the adapter in ``src`` to ``dst`` with tensor names in the CausalLM
    layout. Idempotent: names already under ``base_model.model.model.`` are kept."""
    from safetensors.numpy import load_file, save_file

    os.makedirs(dst, exist_ok=True)
    state = load_file(os.path.join(src, ADAPTER_WEIGHTS))
    head = "base_model.model."
    out = {}
    for k, v in state.items():
        if k.startswith(head) and not k.startswith(head + prefix) and not k.startswith(head + "lm_head"):
            k = head + prefix + k[len(head):]
        out[k] = v
    save_file(out, os.path.join(dst, ADAPTER_WEIGHTS))
    with open(os.path.join(src, ADAPTER_CONFIG)) as f:
        cfg = json.load(f)
    cfg["task_type"] = "CAUSAL_LM"
    with open(os.path.join(dst, ADAPTER_CONFIG), "w") as f:
        json.dump(cfg, f, indent=2)
    return dst


def lora_rank(adapter_dir: str, default: int = 16) -> int:
    try:
        with open(os.path.join(adapter_dir, ADAPTER_CONFIG)) as f:
            return int(json.load(f).get("r") or default)
    except (OSError, ValueError):
        return default


def merge(release_dir: str, out: str, *, base_model: str | None = None, dtype: str = "bfloat16") -> str:
    """Fold the release's adapter into its base model; write a ``Qwen3ForCausalLM``
    checkpoint (tied LM head as in the base) and the tokenizer to ``out``."""
    import torch
    from peft import PeftModel
    from transformers import AutoModel, AutoModelForCausalLM, AutoTokenizer

    with open(os.path.join(release_dir, "rightsize_config.json")) as f:
        base_model = base_model or json.load(f)["base_model"]
    dt = {"float32": torch.float32, "bfloat16": torch.bfloat16, "float16": torch.float16}[dtype]
    torso = AutoModel.from_pretrained(base_model, dtype=torch.float32)
    torso = PeftModel.from_pretrained(torso, release_dir).merge_and_unload()
    lm = AutoModelForCausalLM.from_pretrained(base_model, dtype=torch.float32)
    lm.model.load_state_dict(torso.state_dict(), strict=True)
    lm = lm.to(dt)
    os.makedirs(out, exist_ok=True)
    lm.save_pretrained(out, safe_serialization=True)
    AutoTokenizer.from_pretrained(base_model).save_pretrained(out)
    for name in ("rightsize_config.json",):
        shutil.copy(os.path.join(release_dir, name), os.path.join(out, name))
    return out
