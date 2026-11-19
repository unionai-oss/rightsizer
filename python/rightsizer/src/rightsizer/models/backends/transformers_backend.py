"""The reference backend: in-process ``transformers`` + ``peft`` (models.md §2).

It loads the torso exactly as ``rightsize_pointer.RightSizePointer`` does:
``AutoModel.from_pretrained(base_model, dtype=..., attn_implementation="sdpa")``
with the release's LoRA adapter (``PeftModel.from_pretrained``), and reads
``last_hidden_state`` (after the final norm). CPU fp32 by default (parity
tests); one GPU with ``device="cuda"``. Needs the ``torch`` extra.
"""

from __future__ import annotations

from typing import Any

from . import select_positions


def _dtype(torch, dtype: str | None, device: str):
    if dtype in (None, "auto"):
        return torch.bfloat16 if device.startswith("cuda") else torch.float32
    return {"float32": torch.float32, "fp32": torch.float32, "bfloat16": torch.bfloat16, "bf16": torch.bfloat16,
            "float16": torch.float16, "fp16": torch.float16}[dtype]


class TransformersBackend:
    name = "transformers"

    def __init__(self, base_model: str, adapter: str | None = None, *, device: str = "cpu",
                 dtype: str | None = "float32", revision: str | None = None, torso: bool = True):
        import torch
        from transformers import AutoModel

        self._torch = torch
        self.base_model, self.adapter, self.device = base_model, adapter, device
        self.dtype = _dtype(torch, dtype, device)
        self.revision = revision
        self.model = None
        if torso:
            model = AutoModel.from_pretrained(base_model, dtype=self.dtype, attn_implementation="sdpa",
                                              revision=revision)
            if adapter is not None:
                from peft import PeftModel

                model = PeftModel.from_pretrained(model, adapter)
            self.model = model.to(device).eval()
        self._tok = None
        self._lm = None

    @property
    def hidden_size(self) -> int:
        return int(self.model.config.hidden_size)

    def _ids(self, p: Any) -> list[int]:
        if isinstance(p, str):
            if self._tok is None:
                from transformers import AutoTokenizer

                self._tok = AutoTokenizer.from_pretrained(self.base_model)
            return self._tok(p, add_special_tokens=False)["input_ids"]
        return list(p)

    def full_hidden_states(self, ids: list[int]):
        """``last_hidden_state[0]`` for one prompt: ``float32 [n_tokens, hidden]``."""
        torch = self._torch
        x = torch.tensor([ids], device=self.device)
        with torch.no_grad():
            H = self.model(input_ids=x, attention_mask=torch.ones_like(x)).last_hidden_state
        return H[0].float().cpu().numpy()

    def hidden_states(self, prompts: list, positions: list[list[int]]) -> list:
        return [select_positions(self.full_hidden_states(self._ids(p)), pos) for p, pos in zip(prompts, positions)]

    def generate(self, messages: list[dict], n: int = 1, *, max_new_tokens: int = 64, temperature: float = 1.0,
                 enable_thinking: bool = False, seed: int | None = None, max_tokens: int | None = None) -> list[str]:
        """Sample ``n`` completions with the base model's LM head (no adapter:
        generative read-outs use chat models, not the pointer adapter)."""
        torch = self._torch
        from transformers import AutoModelForCausalLM, AutoTokenizer

        max_new_tokens = max_tokens or max_new_tokens
        if self._lm is None:
            self._lm = AutoModelForCausalLM.from_pretrained(self.base_model, dtype=self.dtype,
                                                            revision=self.revision).to(self.device).eval()
        if self._tok is None:
            self._tok = AutoTokenizer.from_pretrained(self.base_model, revision=self.revision)
        text = self._tok.apply_chat_template(messages, tokenize=False, add_generation_prompt=True,
                                             enable_thinking=enable_thinking)
        ids = self._tok(text, add_special_tokens=False, return_tensors="pt")["input_ids"].to(self.device)
        if seed is not None:
            torch.manual_seed(seed)
        with torch.no_grad():
            out = self._lm.generate(ids, attention_mask=torch.ones_like(ids), do_sample=temperature > 0,
                                    temperature=max(temperature, 1e-5), max_new_tokens=max_new_tokens,
                                    num_return_sequences=n, pad_token_id=self._tok.eos_token_id)
        return [self._tok.decode(o[ids.shape[1]:], skip_special_tokens=True) for o in out]

    def ready(self) -> bool:
        return True
