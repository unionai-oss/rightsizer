"""A tiny local pointer "release" in the published layout (models.md §1):

    base/                 a random 2-layer Qwen3 (hidden 64 by default) + the real Qwen3 tokenizer
    release/
      rightsize_config.json      the tuner's keys / options; base_model -> base/
      decision_head.safetensors  random TriPointer heads with the exact state-dict keys (via the tuner's head_module)
      adapter_config.json, adapter_model.safetensors   a random LoRA (peft, FEATURE_EXTRACTION on AutoModel)

Needs torch, transformers, peft, safetensors and the tuner checkout (for
``head_module`` and ``option_offsets``).
"""

from __future__ import annotations

import json
import os
import shutil

HIDDEN = 64
HEAD_DIM = 32
TEMPERATURES = [1.3, 0.8, 1.1]
GPU_THRESHOLD = 0.6
LORA_TARGETS = ["q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"]


def build(root: str, tokenizer_dir: str, tuner_prompting, tuner_hub_pointer, max_len: int = 1200, seed: int = 0,
          hidden: int = HIDDEN, heads: int = 4, kv_heads: int = 2, head_dim: int = 16) -> str:
    import torch
    from peft import LoraConfig, get_peft_model
    from safetensors.torch import save_file
    from transformers import AutoModel, AutoTokenizer, Qwen3Config, Qwen3ForCausalLM

    torch.manual_seed(seed)
    base = os.path.join(root, "base")
    rel = os.path.join(root, "release")
    os.makedirs(base, exist_ok=True)
    os.makedirs(rel, exist_ok=True)
    for f in ("tokenizer.json", "tokenizer_config.json", "vocab.json", "merges.txt"):
        if os.path.exists(os.path.join(tokenizer_dir, f)):
            shutil.copy(os.path.join(tokenizer_dir, f), os.path.join(base, f))
    tok = AutoTokenizer.from_pretrained(base)
    cfg = Qwen3Config(vocab_size=len(tok), hidden_size=hidden, intermediate_size=2 * hidden, num_hidden_layers=2,
                      num_attention_heads=heads, num_key_value_heads=kv_heads, head_dim=head_dim,
                      max_position_embeddings=4096,
                      tie_word_embeddings=True, rms_norm_eps=1e-6, bos_token_id=151643, eos_token_id=151645)
    lm = Qwen3ForCausalLM(cfg)
    with torch.no_grad():  # random norms, so the final RMSNorm matters
        for n, p in lm.named_parameters():
            if "norm" in n:
                p.copy_(1.0 + 0.2 * torch.randn_like(p))
    lm.save_pretrained(base, safe_serialization=True)

    torso = AutoModel.from_pretrained(base, dtype=torch.float32)
    peft = get_peft_model(torso, LoraConfig(r=4, lora_alpha=8, lora_dropout=0.0, target_modules=LORA_TARGETS,
                                            task_type="FEATURE_EXTRACTION"))
    with torch.no_grad():
        for n, p in peft.named_parameters():
            if "lora_B" in n:
                p.copy_(0.1 * torch.randn_like(p))
    peft.save_pretrained(rel)

    head = tuner_hub_pointer.head_module(hidden, 3, HEAD_DIM)
    probe = tuner_prompting.messages({"task_code": "x = 1", "inputs_desc": "x: npy, 1 KiB, shape=(1,), dtype=uint8",
                                      "history_json": "[]"})
    text = tok.apply_chat_template(probe, tokenize=False, add_generation_prompt=True, enable_thinking=False)
    offs = tuner_hub_pointer.option_offsets(tok, text, tuner_prompting.KEYS, tuner_prompting.OPTIONS)
    state = head.state_dict()
    for i, o in enumerate(offs):
        state[f"off{i}"] = torch.tensor(o, dtype=torch.long)
    with torch.no_grad():
        for k in list(state):
            if k.startswith(("q.", "k.", "norm.")) and k.endswith("weight"):
                state[k] = state[k] + 0.3 * torch.randn_like(state[k])
            if k.endswith("bias"):
                state[k] = 0.1 * torch.randn_like(state[k])
    save_file({k: v.contiguous() for k, v in state.items()}, os.path.join(rel, "decision_head.safetensors"))
    shutil.copy(tuner_hub_pointer.__file__, os.path.join(rel, "rightsize_pointer.py"))
    config = {"release": "test-1", "approach": "jev-pointer", "seed": 1, "base_model": base, "max_len": max_len,
              "head_dim": HEAD_DIM, "keys": list(tuner_prompting.KEYS),
              "options": [list(o) for o in tuner_prompting.OPTIONS], "temperatures": TEMPERATURES,
              "gpu_threshold": GPU_THRESHOLD, "train_platforms": "all", "lora": {"r": 4, "alpha": 8}}
    with open(os.path.join(rel, "rightsize_config.json"), "w") as f:
        json.dump(config, f, indent=2)
    return rel
