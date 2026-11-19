"""Generate-only backend: any OpenAI-compatible chat endpoint (vLLM, SGLang,
llama.cpp, Ollama, a hosted API).

``POST {url}/v1/chat/completions`` with ``{"model", "messages", "n",
"temperature", "max_tokens", **extra_body}``; the answer texts are
``choices[i].message.content``. Servers that ignore ``n`` (llama.cpp, Ollama)
are asked repeatedly until ``n`` samples are in. For Qwen3 chat models pass
``extra_body={"chat_template_kwargs": {"enable_thinking": False}}`` (vLLM and
SGLang honour it).
"""

from __future__ import annotations

import os
from typing import Any

from . import BackendError
from .http import JsonClient


class OpenAIBackend:
    name = "openai"

    def __init__(self, url: str, model: str, *, api_key: str | None = None, timeout: float = 120.0,
                 extra_body: dict | None = None, max_requests: int = 64):
        self.client = JsonClient(url, api_key or os.environ.get("OPENAI_API_KEY"), timeout)
        self.model = model
        self.extra_body = dict(extra_body or {})
        self.max_requests = max_requests

    def generate(self, messages: list[dict], n: int = 1, *, temperature: float = 1.0, max_tokens: int = 128,
                 seed: int | None = None, **kw: Any) -> list[str]:
        out: list[str] = []
        for _ in range(self.max_requests):
            body = {"model": self.model, "messages": messages, "n": n - len(out), "temperature": temperature,
                    "max_tokens": max_tokens, **self.extra_body, **kw}
            if seed is not None:
                body["seed"] = seed + len(out)
            r = self.client.post("/v1/chat/completions", body)
            try:
                choices = r["choices"]
            except (TypeError, KeyError) as e:
                raise BackendError(f"unexpected chat completion response: {str(r)[:300]}", retryable=False) from e
            out += [((c.get("message") or {}).get("content") or c.get("text") or "") for c in choices]
            if len(out) >= n or not choices:
                break
        return out[:n]

    def ready(self) -> bool:
        return self.client.ok("/v1/models") or self.client.ok("/health")
