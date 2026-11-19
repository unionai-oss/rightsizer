"""``rightsizer models ...`` and the ``rightsizer`` dispatcher for ``serve`` / ``models``.

::

    rightsizer models convert unionai/rightsize-trees --revision k8s-2 --out trees.json [--no-gpu]
    rightsizer models merge unionai/rightsize-decision-qwen3-1.7b --revision k8s-2 --out merged/ [--dtype bfloat16]
    rightsizer models adapter unionai/rightsize-decision-qwen3-1.7b --out adapter/   # LoRA in the CausalLM layout
    rightsizer models prompt context.json                                         # print the rendered messages
"""

from __future__ import annotations

import argparse
import json
import sys


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="rightsizer models", description="Convert and prepare model-factory models.")
    sub = p.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("convert", help="trees release -> rightsizer-trees/1")
    c.add_argument("repo")
    c.add_argument("--revision")
    c.add_argument("--out", required=True)
    c.add_argument("--no-gpu", action="store_true", help="do not write the <out>.gpu.json companion")
    c.add_argument("--id", default="gbt")
    m = sub.add_parser("merge", help="fold a pointer release's LoRA into its base model (needs torch)")
    m.add_argument("repo")
    m.add_argument("--revision")
    m.add_argument("--out", required=True)
    m.add_argument("--dtype", default="bfloat16", choices=["bfloat16", "float16", "float32"])
    a = sub.add_parser("adapter", help="rewrite a pointer release's LoRA into the CausalLM layout (vLLM/SGLang)")
    a.add_argument("repo")
    a.add_argument("--revision")
    a.add_argument("--out", required=True)
    r = sub.add_parser("prompt", help="render a WorkloadContext (JSON file, '-' for stdin) as the model's messages")
    r.add_argument("context")
    return p


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.cmd == "convert":
        from .trees import convert

        report = convert(args.repo, args.revision, args.out, gpu=not args.no_gpu, id=args.id)
        print(json.dumps(report, indent=2))
        return 0
    if args.cmd in ("merge", "adapter"):
        from .merge import causal_lm_adapter, merge
        from .pointer import resolve_release

        path, _repo, _commit = resolve_release(args.repo, args.revision)
        out = merge(path, args.out, dtype=args.dtype) if args.cmd == "merge" else causal_lm_adapter(path, args.out)
        print(out)
        return 0
    if args.cmd == "prompt":
        from .prompting import messages_from_context

        if args.context == "-":
            ctx = json.load(sys.stdin)
        else:
            with open(args.context) as f:
                ctx = json.load(f)
        print(json.dumps(messages_from_context(ctx), indent=2))
        return 0
    return 2


def rightsizer(argv: list[str] | None = None) -> int:
    """``rightsizer serve ...`` / ``rightsizer models ...`` (console-script entry point)."""
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "serve":
        from rightsizer.serve.cli import main as serve_main

        return serve_main(argv[1:])
    if argv and argv[0] == "models":
        return main(argv[1:])
    print("usage: rightsizer {serve,models} ...", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
