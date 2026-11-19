"""Serving backends: where the torso runs (models.md §2).

A hidden-state backend answers one question: the last-layer (post final norm)
hidden states at a few token positions of each prompt::

    hidden_states(prompts: list[list[int] | str], positions: list[list[int]]) -> list[np.ndarray]

Each result is ``float32 [len(positions[i]), hidden]``. Prompts are token ids
(the pointer model renders and truncates them) or text (tokenized without
special tokens). Backends that can sample text also implement
``generate(messages, n, ...) -> list[str]``.

| backend | module | hidden states | generate |
|---|---|---|---|
| ``transformers`` | :mod:`.transformers_backend` | in-process ``AutoModel`` + ``peft`` (reference) | ``AutoModelForCausalLM`` |
| ``vllm`` | :mod:`.vllm` | ``POST /pooling``, ``task=token_embed`` | ``/v1/chat/completions`` |
| ``sglang`` | :mod:`.sglang` | ``POST /generate``, ``return_hidden_states`` | ``/v1/chat/completions`` |
| ``openai`` | :mod:`.openai` | -- | ``/v1/chat/completions`` |

Nothing here imports torch, transformers or an HTTP library at import time.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

BACKENDS = ("transformers", "vllm", "sglang", "openai")


@runtime_checkable
class HiddenStateBackend(Protocol):
    name: str

    def hidden_states(self, prompts: list, positions: list[list[int]]) -> list: ...

    def ready(self) -> bool: ...


@runtime_checkable
class GenerateBackend(Protocol):
    name: str

    def generate(self, messages: list[dict], n: int = 1, **kw: Any) -> list[str]: ...

    def ready(self) -> bool: ...


class BackendError(RuntimeError):
    """A backend call failed. ``retryable`` says whether a retry may help."""

    def __init__(self, message: str, retryable: bool = True):
        super().__init__(message)
        self.retryable = retryable


def select_positions(states: Any, positions: list[int], n_tokens: int | None = None):
    """Rows ``positions`` of a ``[n_tokens, hidden]`` array, as float32."""
    import numpy as np

    H = np.asarray(states, dtype=np.float32)
    if H.ndim != 2:
        raise BackendError(f"expected a [tokens, hidden] array, got shape {H.shape}", retryable=False)
    if n_tokens is not None and H.shape[0] != n_tokens:
        raise BackendError(f"backend returned {H.shape[0]} token states for a {n_tokens}-token prompt "
                           "(prefix caching or chunked prefill dropping rows?)", retryable=False)
    return H[np.asarray(positions, dtype=np.int64)]
