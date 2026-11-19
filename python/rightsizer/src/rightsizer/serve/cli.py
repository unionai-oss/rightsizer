"""``rightsizer serve`` / ``python -m rightsizer.serve``: a model-factory model
behind the Estimator Protocol v1 (models.md §5).

::

    rightsizer serve --model unionai/rightsize-decision-qwen3-1.7b --revision k8s-2 \\
        --backend vllm --launch --port 8095
    rightsizer serve --model ... --backend sglang --backend-url http://gpu-box:30000
    rightsizer serve --model Qwen/Qwen3-8B --backend openai --backend-url http://localhost:8000 --samples 16
"""

from __future__ import annotations

import argparse
import logging
import os
import sys


def _token(args: argparse.Namespace) -> str | None:
    if args.token_file:
        with open(args.token_file) as f:
            return f.read().strip() or None
    if args.token_env:
        v = os.environ.get(args.token_env)
        if not v:
            raise SystemExit(f"--token-env {args.token_env}: the variable is empty or unset")
        return v
    return os.environ.get("RIGHTSIZER_SERVE_TOKEN") or None


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="rightsizer serve", description=__doc__.split("\n\n")[0],
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--model", required=True, help="Hub repo id or local directory")
    p.add_argument("--revision", help="branch, tag or commit (e.g. k8s-2)")
    p.add_argument("--backend", required=True, choices=["transformers", "vllm", "sglang", "openai"])
    g = p.add_mutually_exclusive_group()
    g.add_argument("--backend-url", help="an already running vLLM / SGLang / OpenAI-compatible server")
    g.add_argument("--launch", action="store_true", help="start vllm / sglang as a subprocess")
    p.add_argument("--backend-model", help="model or LoRA name on the backend (default: rightsize)")
    p.add_argument("--api-key-env", help="env var with the backend's API key")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8095)
    p.add_argument("--kind", default="demand", choices=["demand", "action"])
    p.add_argument("--readout", default="auto", choices=["auto", "pointer", "generate"])
    p.add_argument("--memory-basis", default="need", choices=["need", "demand"],
                   help="need: pass the tuner's memory cells through; demand: convert need -> demand")
    p.add_argument("--device", default="cpu", help="transformers backend: cpu, cuda, cuda:1, mps")
    p.add_argument("--dtype", help="float32 (transformers default), bfloat16, auto (launch default)")
    p.add_argument("--merge", action="store_true", help="--launch: serve a merged checkpoint (no LoRA)")
    p.add_argument("--launch-port", type=int)
    p.add_argument("--launch-arg", action="append", default=[], help="extra flag for the launched server")
    p.add_argument("--launch-timeout", type=float, default=900.0)
    p.add_argument("--launch-log")
    p.add_argument("--samples", type=int, default=8, help="generate read-out: samples per context")
    p.add_argument("--temperature", type=float, default=1.0)
    p.add_argument("--max-tokens", type=int, default=128)
    p.add_argument("--token-env", help="env var holding the bearer token clients must send")
    p.add_argument("--token-file", help="file holding the bearer token")
    p.add_argument("--batch-window-ms", type=float, default=10.0)
    p.add_argument("--max-batch", type=int, default=32)
    p.add_argument("--log-contexts", action="store_true", help="log request contexts (off by default, SPEC §11)")
    p.add_argument("--log-level", default="INFO")
    return p


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    from rightsizer.models.load import ServeOptions, build_estimator

    from .server import serve

    opts = ServeOptions(
        model=args.model, revision=args.revision, backend=args.backend, backend_url=args.backend_url,
        launch=args.launch, kind=args.kind, readout=args.readout, memory_basis=args.memory_basis, device=args.device,
        dtype=args.dtype, merge=args.merge, launch_port=args.launch_port, launch_args=args.launch_arg,
        launch_timeout=args.launch_timeout, launch_log=args.launch_log, backend_model=args.backend_model,
        api_key=os.environ.get(args.api_key_env) if args.api_key_env else None, samples=args.samples,
        temperature=args.temperature, max_tokens=args.max_tokens)
    if args.launch and args.backend not in ("vllm", "sglang"):
        raise SystemExit("--launch is for --backend vllm or sglang")
    serve(None, factory=lambda: build_estimator(opts), host=args.host, port=args.port, token=_token(args),
          batch_window_ms=args.batch_window_ms, max_batch=args.max_batch, log_contexts=args.log_contexts)
    return 0


if __name__ == "__main__":
    sys.exit(main())
