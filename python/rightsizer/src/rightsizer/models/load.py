"""Build a servable estimator from a model id and a backend choice (what
``rightsizer serve --model ... --backend ...`` does)."""

from __future__ import annotations

import hashlib
import logging
import os
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger("rightsizer.models")


@dataclass
class ServeOptions:
    model: str
    revision: str | None = None
    backend: str = "transformers"
    backend_url: str | None = None
    launch: bool = False
    kind: str = "demand"
    readout: str = "auto"                 # auto | pointer | generate
    memory_basis: str = "need"            # need | demand
    device: str = "cpu"
    dtype: str | None = None
    merge: bool = False                   # --launch: serve a merged checkpoint instead of base + LoRA
    launch_port: int | None = None
    launch_args: list[str] = field(default_factory=list)
    launch_timeout: float = 900.0
    launch_log: str | None = None
    backend_model: str | None = None      # served model / LoRA name on an attached server
    api_key: str | None = None
    samples: int = 8                      # generate read-out
    temperature: float = 1.0
    max_tokens: int = 128
    cache_dir: str | None = None


def _cache_dir(opts: ServeOptions, commit: str) -> str:
    root = opts.cache_dir or os.path.join(os.path.expanduser("~"), ".cache", "rightsizer", "models")
    d = os.path.join(root, hashlib.sha256(f"{opts.model}@{commit}".encode()).hexdigest()[:16])
    os.makedirs(d, exist_ok=True)
    return d


def _is_pointer(model: str, revision: str | None = None) -> bool:
    """Whether ``model`` (a directory or Hub repo) is a pointer release, without
    downloading a chat model's weights to find out."""
    from .pointer import CONFIG_FILE, HEAD_FILE

    if os.path.isdir(model):
        return os.path.exists(os.path.join(model, CONFIG_FILE)) and os.path.exists(os.path.join(model, HEAD_FILE))
    from huggingface_hub import file_exists

    return file_exists(model, CONFIG_FILE, revision=revision) and file_exists(model, HEAD_FILE, revision=revision)


def _launch(opts: ServeOptions, release: Any) -> tuple[str, Any, str | None]:
    """Start vllm / sglang for ``release``; ``(url, launcher, lora name or None)``."""
    from .backends.launch import Launcher, free_port
    from .backends.sglang import LORA_NAME as SG_LORA
    from .backends.sglang import sglang_command
    from .backends.vllm import LORA_NAME as VL_LORA
    from .backends.vllm import vllm_command
    from .merge import causal_lm_adapter, lora_rank, merge

    port = opts.launch_port or free_port()
    cache = _cache_dir(opts, release.commit)
    base = release.config["base_model"]
    max_len = int(release.config.get("max_len") or 2048) + 64
    dtype = opts.dtype or "auto"
    if opts.merge:
        merged = os.path.join(cache, "merged")
        if not os.path.exists(os.path.join(merged, "config.json")):
            merge(release.path, merged, dtype="bfloat16" if dtype in ("auto", "bfloat16") else dtype)
        model_path, adapter = merged, None
    else:
        model_path = base
        adapter = causal_lm_adapter(release.path, os.path.join(cache, "adapter-causal-lm"))
    rank = lora_rank(release.path)
    if opts.backend == "vllm":
        cmd = vllm_command(model_path, adapter=adapter, lora_rank=rank, port=port, max_model_len=max_len,
                           dtype=dtype, extra=opts.launch_args)
        lora = VL_LORA
    else:
        cmd = sglang_command(model_path, adapter=adapter, lora_rank=rank, port=port, context_length=max_len,
                             dtype=dtype, extra=opts.launch_args)
        lora = SG_LORA if adapter else None
    url = f"http://127.0.0.1:{port}"
    log.info("launching %s", " ".join(cmd))
    launcher = Launcher(cmd, url + "/health", timeout=opts.launch_timeout, log_path=opts.launch_log).start()
    return url, launcher, lora


def build_estimator(opts: ServeOptions) -> Any:
    """A ``PointerModel`` (a release with ``decision_head.safetensors``) or a
    ``GenerativeModel`` (any chat model, or ``--readout generate``)."""
    if opts.backend not in ("transformers", "vllm", "sglang", "openai"):
        raise ValueError(f"unknown backend {opts.backend!r}")
    if opts.backend == "openai" or opts.readout == "generate":
        return _generative(opts)
    from .pointer import PointerModel, load_release

    if opts.readout == "auto" and not _is_pointer(opts.model, opts.revision):
        return _generative(opts)
    release = load_release(opts.model, opts.revision)
    backend: Any
    if opts.backend == "transformers":
        from .backends.transformers_backend import TransformersBackend

        backend = TransformersBackend(release.config["base_model"], release.path, device=opts.device,
                                      dtype=opts.dtype or "float32")
    elif opts.backend == "vllm":
        from .backends.vllm import VLLMBackend

        if opts.launch:
            url, launcher, lora = _launch(opts, release)
            backend = VLLMBackend(url, lora or "rightsize", launcher=launcher)
        else:
            if not opts.backend_url:
                raise ValueError("--backend vllm needs --backend-url or --launch")
            backend = VLLMBackend(opts.backend_url, opts.backend_model or "rightsize", api_key=opts.api_key)
    else:
        from .backends.sglang import SGLangBackend

        if opts.launch:
            url, launcher, lora = _launch(opts, release)
            backend = SGLangBackend(url, lora, launcher=launcher)
        else:
            if not opts.backend_url:
                raise ValueError("--backend sglang needs --backend-url or --launch")
            lora = None if opts.backend_model in ("", "none") else (opts.backend_model or "rightsize")
            backend = SGLangBackend(opts.backend_url, lora, api_key=opts.api_key)
    return PointerModel(release, backend, kind=opts.kind, memory_basis=opts.memory_basis)


def _generative(opts: ServeOptions) -> Any:
    from .generate import GenerativeModel

    if opts.backend == "transformers":
        from .backends.transformers_backend import TransformersBackend

        backend = TransformersBackend(opts.model, None, device=opts.device, dtype=opts.dtype or "float32",
                                      revision=opts.revision, torso=False)
        extra: dict = {}
    else:
        from .backends.openai import OpenAIBackend

        if not opts.backend_url:
            raise ValueError(f"--backend {opts.backend} with a generative read-out needs --backend-url")
        backend = OpenAIBackend(opts.backend_url, opts.backend_model or opts.model, api_key=opts.api_key,
                                extra_body={"chat_template_kwargs": {"enable_thinking": False}})
        extra = {}
    return GenerativeModel(backend, model_id=opts.model, version=opts.revision or "unknown", n=opts.samples,
                           temperature=opts.temperature, max_tokens=opts.max_tokens, kind=opts.kind, extra=extra)
