"""Pointer decision models (``unionai/rightsize-decision-qwen3-1.7b`` and kin):
load a release, apply its heads in NumPy to hidden states from a backend, and
turn the calibrated probabilities into an ``Estimate`` (models.md §1-§4).

A release directory holds ``rightsize_config.json`` (``base_model``,
``max_len``, ``head_dim``, ``keys``, ``options``, ``temperatures``,
``gpu_threshold``), ``decision_head.safetensors`` (the ``TriPointer`` state
dict: ``norm.{weight,bias}``, ``q.{i}.{weight,bias}``, ``k.{i}.{weight,bias}``
and the option-offset buffers ``off{i}``) and the LoRA adapter
(``adapter_config.json``, ``adapter_model.safetensors``), which only the
backend reads.

The heads, per axis ``i`` (``rightsize_pointer.head_module``)::

    a      = LayerNorm(H[last])                     # the answer slot
    o_j    = LayerNorm(H[last - off_i[j]])          # option j's last token
    z_ij   = (q_i(a) . k_i(o_j)) / sqrt(head_dim)
    P_i    = softmax(z_i / temperature_i)
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from .chat import ChatTokenizer, option_offsets
from .estimate import build_estimate

CONFIG_FILE = "rightsize_config.json"
HEAD_FILE = "decision_head.safetensors"
LAYERNORM_EPS = 1e-5  # torch.nn.LayerNorm's default, as TriPointer uses it


@dataclass
class Release:
    """A pointer-model release on disk."""

    path: str
    repo: str
    revision: str | None
    commit: str
    config: dict
    head: dict = field(repr=False)

    @property
    def keys(self) -> list[str]:
        return list(self.config["keys"])

    @property
    def options(self) -> dict[str, list[str]]:
        return {k: list(o) for k, o in zip(self.config["keys"], self.config["options"])}


def _local_version(path: str) -> str:
    h = hashlib.sha256()
    for name in (CONFIG_FILE, HEAD_FILE, "adapter_model.safetensors"):
        p = os.path.join(path, name)
        if os.path.exists(p):
            with open(p, "rb") as f:
                for chunk in iter(lambda: f.read(1 << 20), b""):
                    h.update(chunk)
    return "local-" + h.hexdigest()[:16]


def resolve_release(repo_or_path: str, revision: str | None = None) -> tuple[str, str, str]:
    """``(local path, repo id, commit)``. A Hub repo is fetched with
    ``snapshot_download`` (the snapshot directory is named by its commit sha);
    a local directory's version is a content hash."""
    if os.path.isdir(repo_or_path):
        path = os.path.abspath(repo_or_path)
        return path, os.path.basename(path.rstrip("/")), _local_version(path)
    from huggingface_hub import snapshot_download

    path = snapshot_download(repo_or_path, revision=revision)
    commit = os.path.basename(os.path.normpath(path))
    if len(commit) != 40:  # not a snapshots/<sha> layout: ask the Hub
        from huggingface_hub import HfApi

        commit = HfApi().model_info(repo_or_path, revision=revision).sha or commit
    return path, repo_or_path, commit


def load_release(repo_or_path: str, revision: str | None = None) -> Release:
    from safetensors.numpy import load_file

    path, repo, commit = resolve_release(repo_or_path, revision)
    with open(os.path.join(path, CONFIG_FILE)) as f:
        config = json.load(f)
    for k in ("base_model", "max_len", "keys", "options", "temperatures"):
        if k not in config:
            raise ValueError(f"{path}/{CONFIG_FILE}: missing {k!r}")
    head = load_file(os.path.join(path, HEAD_FILE))
    return Release(path, repo, revision, commit, config, head)


class PointerHeads:
    """The ``TriPointer`` heads in NumPy (float64 internally)."""

    def __init__(self, state: Mapping[str, Any], n_axes: int, head_dim: int | None = None):
        import numpy as np

        f = lambda k: np.asarray(state[k], dtype=np.float64)
        self.norm_w, self.norm_b = f("norm.weight"), f("norm.bias")
        self.q = [(f(f"q.{i}.weight"), f(f"q.{i}.bias")) for i in range(n_axes)]
        self.k = [(f(f"k.{i}.weight"), f(f"k.{i}.bias")) for i in range(n_axes)]
        d = head_dim or self.q[0][0].shape[0]
        self.scale = d ** -0.5
        self.offsets = [[int(x) for x in np.asarray(state[f"off{i}"]).reshape(-1)] if f"off{i}" in state else None
                        for i in range(n_axes)]

    @property
    def hidden_size(self) -> int:
        return int(self.norm_w.shape[0])

    def _ln(self, x):
        import numpy as np

        mu = x.mean(-1, keepdims=True)
        var = ((x - mu) ** 2).mean(-1, keepdims=True)
        return (x - mu) / np.sqrt(var + LAYERNORM_EPS) * self.norm_w + self.norm_b

    def logits(self, last_state, option_states: Sequence[Any]) -> list:
        """``last_state``: ``[hidden]``; ``option_states[i]``: ``[n_options_i, hidden]``."""
        import numpy as np

        a = self._ln(np.asarray(last_state, dtype=np.float64))
        out = []
        for (qw, qb), (kw, kb), opts in zip(self.q, self.k, option_states):
            o = self._ln(np.asarray(opts, dtype=np.float64))
            out.append(((o @ kw.T + kb) @ (qw @ a + qb)) * self.scale)
        return out


def softmax_t(z, t: float):
    import numpy as np

    x = np.asarray(z, dtype=np.float64) / t
    x = x - x.max()
    e = np.exp(x)
    return e / e.sum()


@dataclass
class Prepared:
    ids: list[int]
    positions: list[int]  # [last, *axis0 options, *axis1 options, ...]
    sizes: list[int]


class PointerModel:
    """A release + a hidden-state backend = per-axis probabilities and Estimates.

    ``backend`` implements ``hidden_states(prompts, positions)``
    (:mod:`rightsizer.models.backends`). Prompts are sent as token ids, already
    rendered with the base model's chat template (``enable_thinking=False``)
    and truncated from the left to ``max_len``, as the reference does."""

    def __init__(self, release: Release, backend: Any, tokenizer: ChatTokenizer | None = None, *,
                 kind: str = "demand", memory_basis: str = "need", prompt_options: Mapping[str, Any] | None = None):
        self.release = release
        self.backend = backend
        self.tok = tokenizer or ChatTokenizer.from_pretrained(release.config["base_model"])
        self.heads = PointerHeads(release.head, len(release.keys), release.config.get("head_dim"))
        self.kind = kind
        self.memory_basis = memory_basis
        self.prompt_options = dict(prompt_options or {})
        self._offsets: list[list[int]] | None = None

    # ── offsets (constant per tokenizer; cached) ────────────────────────

    @property
    def offsets(self) -> list[list[int]]:
        if self._offsets is None:
            from . import prompting

            probe = prompting.messages({"task_code": "x = 1", "inputs_desc": "x: npy, 1 KiB, shape=(1,), dtype=uint8",
                                        "history_json": "[]"})
            text = self.tok.apply_chat_template(probe, add_generation_prompt=True, enable_thinking=False)
            computed = option_offsets(self.tok, text, self.release.keys, self.release.config["options"])
            for i, stored in enumerate(self.heads.offsets):
                if stored is not None and stored != computed[i]:
                    raise ValueError(f"option offsets for {self.release.keys[i]!r} differ from the head's "
                                     f"({computed[i]} != {stored}): the prompt or tokenizer changed")
            self._offsets = computed
        return self._offsets

    def prepare(self, messages: list[dict]) -> Prepared:
        text = self.tok.apply_chat_template(messages, add_generation_prompt=True, enable_thinking=False)
        if "### Options\n" not in text:
            raise ValueError("prompt has no options block")
        ids = self.tok.ids(text)[-int(self.release.config["max_len"]):]
        last = len(ids) - 1
        positions = [last]
        sizes = []
        for offs in self.offsets:
            positions += [last - o for o in offs]
            sizes.append(len(offs))
        if min(positions) < 0:
            raise ValueError("prompt shorter than its options block")
        return Prepared(ids, positions, sizes)

    def _logits(self, preps: Sequence[Prepared]) -> list[list]:
        states = self.backend.hidden_states([p.ids for p in preps], [p.positions for p in preps])
        out = []
        for p, H in zip(preps, states):
            last, rest = H[0], H[1:]
            chunks, at = [], 0
            for n in p.sizes:
                chunks.append(rest[at:at + n])
                at += n
            out.append(self.heads.logits(last, chunks))
        return out

    def logits_batch(self, batch: Sequence[list[dict]]) -> list[list[list[float]]]:
        """Uncalibrated scores, per prompt, per axis."""
        return [[z.tolist() for z in zs] for zs in self._logits([self.prepare(m) for m in batch])]

    def probabilities_batch(self, batch: Sequence[list[dict]]) -> list[dict[str, list[float]]]:
        """``rightsize_pointer.probabilities`` for a batch of message lists."""
        temps = self.release.config["temperatures"]
        out = []
        for zs in self._logits([self.prepare(m) for m in batch]):
            out.append({k: softmax_t(z, t).tolist() for k, z, t in zip(self.release.keys, zs, temps)})
        return out

    def probabilities(self, messages: list[dict]) -> dict[str, list[float]]:
        return self.probabilities_batch([messages])[0]

    # ── estimates ───────────────────────────────────────────────────────

    def estimate_batch(self, contexts: Sequence[Mapping[str, Any]], axes: Sequence[str] | None = None) -> list[dict]:
        from . import prompting

        t0 = time.monotonic()
        msgs = [prompting.messages_from_context(c, **self.prompt_options) for c in contexts]
        probs = self.probabilities_batch(msgs)
        ms = int(math.ceil((time.monotonic() - t0) * 1000 / max(1, len(contexts))))
        out = []
        for c, p in zip(contexts, probs):
            platform = prompting.tuner_platform(c)
            overhead = prompting.FLYTE_RUNTIME_MIB * 2**20 if platform == "flyte" else 0.0
            out.append(build_estimate(
                p, self.release.options, estimator=self.release.repo, version=self.release.commit, kind=self.kind,
                gpu_threshold=self.release.config.get("gpu_threshold"), axes=axes, memory_basis=self.memory_basis,
                memory_overhead_bytes=overhead, latency_ms=ms))
        return out

    def info(self, max_batch: int = 32) -> dict:
        """The ``/v1/info`` body (without ``protocol``)."""
        return {
            "id": self.release.repo, "version": self.release.commit, "grids": ["rs-grid-v1"],
            "axes": ["memory", "cpu_mean", "cpu_peak", "gpu_memory"], "kind": self.kind,
            "needs": {"code": True, "history": True, "input_bytes": True}, "features": "features-v1",
            "max_batch": max_batch,
            "backend": getattr(self.backend, "name", type(self.backend).__name__),
            "base_model": self.release.config["base_model"],
        }

    def ready(self) -> bool:
        r = getattr(self.backend, "ready", None)
        return bool(r()) if callable(r) else True
