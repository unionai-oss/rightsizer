"""vLLM backend: per-token last-layer hidden states through the pooling API.

Checked against vLLM v0.34.0 (2026-11-16); see ``docs/IMPLEMENTATION-NOTES.md``
("rightsizer.models / serve") for the sources.

Server (what :func:`vllm_command` launches)::

    vllm serve <base_model> --runner pooling --convert embed \\
        --pooler-config '{"task": "token_embed", "pooling_type": "ALL", "use_activation": false}' \\
        --enable-lora --lora-modules rightsize=<adapter in CausalLM layout> --max-lora-rank <r> \\
        --served-model-name <base_model> --dtype <dtype> --max-model-len <max_len + 64>

``token_embed`` with ``pooling_type: ALL`` returns one row per prompt token: the
model's output after the final RMSNorm (the LM head is skipped), i.e. HF's
``last_hidden_state``. ``use_activation: false`` turns off the L2 normalization
``token_embed`` applies by default.

Request (``POST /pooling``)::

    {"model": "rightsize", "input": [[151644, 8948, ...], ...], "task": "token_embed",
     "use_activation": false, "encoding_format": "base64", "embed_dtype": "float32",
     "endianness": "little", "add_special_tokens": false}

Response::

    {"id": "pool-...", "object": "list", "created": 1763500000, "model": "rightsize",
     "data": [{"index": 0, "object": "pooling", "data": "<base64 of float32 [n_tokens * hidden]>"}, ...],
     "usage": {"prompt_tokens": ..., "total_tokens": ...}}

With ``encoding_format: "float"`` each ``data`` is a ``[n_tokens][hidden]`` list.
The adapter is chosen by ``"model"`` (a ``--lora-modules`` name). A merged
checkpoint (``rightsizer models merge``) is served without ``--enable-lora`` and
addressed by its served name.
"""

from __future__ import annotations

import base64
import json
from typing import Any

from . import BackendError, select_positions
from .http import JsonClient
from .openai import OpenAIBackend

POOLER_CONFIG = {"task": "token_embed", "pooling_type": "ALL", "use_activation": False}
LORA_NAME = "rightsize"


def decode_pooling_data(data: Any, n_tokens: int, dtype: str = "float32", endianness: str = "little"):
    """One ``data[i].data`` item -> ``float32 [n_tokens, hidden]``."""
    import numpy as np

    if isinstance(data, str):
        dt = {"float32": "f4", "float16": "f2"}.get(dtype)
        if dt is None:
            raise BackendError(f"unsupported embed_dtype {dtype}", retryable=False)
        arr = np.frombuffer(base64.b64decode(data), dtype=("<" if endianness == "little" else ">") + dt)
        if n_tokens <= 0 or arr.size % n_tokens:
            raise BackendError(f"pooling output of {arr.size} values does not split into {n_tokens} tokens",
                               retryable=False)
        return arr.reshape(n_tokens, -1).astype(np.float32)
    return np.asarray(data, dtype=np.float32)


class VLLMBackend:
    name = "vllm"

    def __init__(self, url: str, model: str = LORA_NAME, *, api_key: str | None = None, timeout: float = 300.0,
                 encoding_format: str = "base64", max_batch: int = 16, launcher: Any = None):
        self.client = JsonClient(url, api_key, timeout)
        self.model = model
        self.encoding_format = encoding_format
        self.max_batch = max_batch
        self.launcher = launcher

    def request_body(self, ids: list[list[int]]) -> dict:
        return {"model": self.model, "input": ids, "task": "token_embed", "use_activation": False,
                "encoding_format": self.encoding_format, "embed_dtype": "float32", "endianness": "little",
                "add_special_tokens": False}

    def hidden_states(self, prompts: list, positions: list[list[int]]) -> list:
        if any(isinstance(p, str) for p in prompts):
            raise BackendError("the vllm backend takes token ids (render prompts with the base tokenizer)", False)
        out = []
        for s in range(0, len(prompts), self.max_batch):
            chunk = [list(map(int, p)) for p in prompts[s:s + self.max_batch]]
            r = self.client.post("/pooling", self.request_body(chunk))
            try:
                items = sorted(r["data"], key=lambda d: d["index"])
            except (TypeError, KeyError) as e:
                raise BackendError(f"unexpected /pooling response: {json.dumps(r)[:300]}", retryable=False) from e
            if len(items) != len(chunk):
                raise BackendError(f"/pooling returned {len(items)} results for {len(chunk)} prompts", False)
            for ids, pos, item in zip(chunk, positions[s:s + self.max_batch], items):
                H = decode_pooling_data(item["data"], len(ids))
                out.append(select_positions(H, pos, len(ids)))
        return out

    def generate(self, messages: list[dict], n: int = 1, **kw: Any) -> list[str]:
        return OpenAIBackend(self.client.url, self.model, api_key=self.client.api_key).generate(messages, n, **kw)

    def ready(self) -> bool:
        return self.client.ok("/health")

    def close(self) -> None:
        if self.launcher is not None:
            self.launcher.stop()


def vllm_command(base_model: str, *, adapter: str | None = None, lora_rank: int = 16, port: int = 8000,
                 host: str = "127.0.0.1", max_model_len: int | None = None, dtype: str = "auto",
                 served_name: str | None = None, extra: list[str] | None = None,
                 executable: str | list[str] = "vllm") -> list[str]:
    """``vllm serve`` for hidden states. ``adapter`` must be in the CausalLM
    layout (:func:`rightsizer.models.merge.causal_lm_adapter`; vLLM v0.34 also
    accepts the tuner's own layout); without it, ``base_model`` is a merged
    checkpoint served as ``served_name``. ``executable`` may be a command prefix
    (e.g. ``["docker", "run", ..., "--entrypoint", "vllm", "<image>"]``)."""
    prefix = [executable] if isinstance(executable, str) else list(executable)
    cmd = prefix + ["serve", base_model, "--runner", "pooling", "--convert", "embed",
           "--pooler-config", json.dumps(POOLER_CONFIG), "--host", host, "--port", str(port), "--dtype", dtype]
    if max_model_len:
        cmd += ["--max-model-len", str(max_model_len)]
    if adapter:
        cmd += ["--enable-lora", "--lora-modules", f"{LORA_NAME}={adapter}", "--max-lora-rank", str(lora_rank)]
    else:
        cmd += ["--served-model-name", served_name or LORA_NAME]
    return cmd + list(extra or [])
