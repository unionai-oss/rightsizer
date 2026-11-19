"""SGLang backend: hidden states from the native ``/generate`` endpoint.

Checked against SGLang v0.5.23 (2026-11-02); sources in
``docs/IMPLEMENTATION-NOTES.md`` ("rightsizer.models / serve").

Server (what :func:`sglang_command` launches)::

    python -m sglang.launch_server --model-path <base_model> --return-hidden-states-mode full \\
        --enable-lora --lora-paths rightsize=<adapter in CausalLM layout> --max-loras-per-batch 1 \\
        --max-lora-rank <r> --disable-radix-cache --chunked-prefill-size -1

``--disable-radix-cache``: a prefix-cache hit shrinks the prefill block (to
nothing on a full hit); ``--chunked-prefill-size -1``: chunked prefill keeps
only the last chunk's rows. Both would drop the rows we need.

Request (``POST /generate``; a list of prompts is one batch)::

    {"input_ids": [[151644, ...], ...], "sampling_params": {"max_new_tokens": 1, "temperature": 0},
     "return_hidden_states": true, "lora_path": ["rightsize", ...]}

Response (a list for a batch, an object for one prompt)::

    [{"text": "...", "output_ids": [...],
      "meta_info": {"id": "...", "prompt_tokens": N, "completion_tokens": 1,
                    "hidden_states": [[[h_0...], ..., [h_{N-1}...]]], ...}}, ...]

``meta_info.hidden_states[0]`` is the prefill block, ``[N][hidden]``, the
model's output after the final norm (the states the logits processor
receives); later elements are one ``[hidden]`` row per decode step.
"""

from __future__ import annotations

import json
import sys
from typing import Any

from . import BackendError, select_positions
from .http import JsonClient
from .openai import OpenAIBackend

LORA_NAME = "rightsize"


class SGLangBackend:
    name = "sglang"

    def __init__(self, url: str, lora: str | None = LORA_NAME, *, api_key: str | None = None,
                 timeout: float = 300.0, max_batch: int = 16, launcher: Any = None, model: str = "default"):
        self.client = JsonClient(url, api_key, timeout)
        self.lora = lora
        self.max_batch = max_batch
        self.launcher = launcher
        self.model = model

    def request_body(self, ids: list[list[int]]) -> dict:
        body: dict[str, Any] = {"input_ids": ids, "sampling_params": {"max_new_tokens": 1, "temperature": 0},
                                "return_hidden_states": True}
        if self.lora:
            body["lora_path"] = [self.lora] * len(ids)
        return body

    def hidden_states(self, prompts: list, positions: list[list[int]]) -> list:
        if any(isinstance(p, str) for p in prompts):
            raise BackendError("the sglang backend takes token ids (render prompts with the base tokenizer)", False)
        out = []
        for s in range(0, len(prompts), self.max_batch):
            chunk = [list(map(int, p)) for p in prompts[s:s + self.max_batch]]
            r = self.client.post("/generate", self.request_body(chunk))
            items = r if isinstance(r, list) else [r]
            if len(items) != len(chunk):
                raise BackendError(f"/generate returned {len(items)} results for {len(chunk)} prompts", False)
            for ids, pos, item in zip(chunk, positions[s:s + self.max_batch], items):
                try:
                    prefill = item["meta_info"]["hidden_states"][0]
                except (TypeError, KeyError, IndexError) as e:
                    raise BackendError("no meta_info.hidden_states in the /generate response; start the server "
                                       f"with --return-hidden-states-mode full: {json.dumps(item)[:300]}",
                                       retryable=False) from e
                out.append(select_positions(prefill, pos, len(ids)))
        return out

    def generate(self, messages: list[dict], n: int = 1, **kw: Any) -> list[str]:
        return OpenAIBackend(self.client.url, self.model, api_key=self.client.api_key).generate(messages, n, **kw)

    def ready(self) -> bool:
        return self.client.ok("/health")

    def close(self) -> None:
        if self.launcher is not None:
            self.launcher.stop()


def sglang_command(base_model: str, *, adapter: str | None = None, lora_rank: int = 16, port: int = 30000,
                   host: str = "127.0.0.1", dtype: str = "auto", context_length: int | None = None,
                   extra: list[str] | None = None, python: str | None = None) -> list[str]:
    cmd = [python or sys.executable, "-m", "sglang.launch_server", "--model-path", base_model,
           "--return-hidden-states-mode", "full", "--disable-radix-cache", "--chunked-prefill-size", "-1",
           "--host", host, "--port", str(port), "--dtype", dtype]
    if context_length:
        cmd += ["--context-length", str(context_length)]
    if adapter:
        cmd += ["--enable-lora", "--lora-paths", f"{LORA_NAME}={adapter}", "--max-loras-per-batch", "1",
                "--max-lora-rank", str(lora_rank)]
    return cmd + list(extra or [])
