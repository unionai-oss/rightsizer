"""``python -m rightsizer.flyte.cli`` — what ``rsz flyte …`` and
``rsz history import flyte …`` run (flyte.md §6, §8.3).

::

    run [--mode M] [--risk R] [--estimator E] [--config CFG] -- FLYTE_RUN_ARGS...
        ≡ flyte [--config CFG] run --custom-context rightsizer='{"v":1,"s":[...]}' FLYTE_RUN_ARGS...
        (passes the run's policy only; the root is not sized here)
    deploy [--restore] [--version V] [--reconcile/--no-reconcile] [--dry-run] [--config CFG] FILE ENV...
    reconcile [--schedule 5m] [--rounds N] [--dry-run] [--config CFG] FILE ENV...
    import [--project P] [--domain D] [--since 30d] [--limit N] [--dry-run] [--config CFG]
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import shutil
import sys
from typing import Any, List, Optional

from ._backfill import parse_since


def _init(config: Optional[str]) -> None:
    import flyte

    if config:
        flyte.init_from_config(config)
    else:
        flyte.init_from_config()


def _load_envs(file: str, names: List[str]) -> List[Any]:
    path = os.path.abspath(file)
    d, base = os.path.split(path)
    mod_name = os.path.splitext(base)[0]
    if d not in sys.path:
        sys.path.insert(0, d)
    mod = importlib.import_module(mod_name)
    envs = []
    for n in names:
        e = getattr(mod, n, None)
        if e is None:
            raise SystemExit(f"rsz flyte: {file} has no attribute {n!r}")
        envs.append(e)
    return envs


def _flyte_bin() -> str:
    cand = os.path.join(os.path.dirname(sys.executable), "flyte")
    if os.path.exists(cand):
        return cand
    return shutil.which("flyte") or "flyte"


def cmd_run(a: argparse.Namespace, rest: List[str]) -> int:
    from ._policy import run_context

    cc = run_context(mode=a.mode, risk=float(a.risk) if a.risk and a.risk.replace(".", "", 1).isdigit() else a.risk,
                     estimator=a.estimator)
    argv = [_flyte_bin()]
    if a.config:
        argv += ["--config", a.config]
    argv += ["run", "--custom-context", f"rightsizer={cc['rightsizer']}", *rest]
    if a.print:
        print(" ".join(argv))
        return 0
    os.execvp(argv[0], argv)
    return 0  # pragma: no cover


def cmd_deploy(a: argparse.Namespace) -> int:
    from ._deploy import deploy, plan_deploy

    _init(a.config)
    envs = _load_envs(a.file, a.env)
    if a.plan:
        out = [{"task": n, "decision": d.id, "mode": str(getattr(d.mode, "value", d.mode)), "prior": repr(t.resources),
                "deployed": repr(m)} for _e, n, t, d, m in plan_deploy(*envs, allow_reductions=a.reconcile)]
        print(json.dumps(out, indent=2))
        return 0
    res = deploy(*envs, version=a.version, restore=a.restore, reconcile=a.reconcile, dry_run=a.dry_run)
    print(res)
    return 0


def cmd_reconcile(a: argparse.Namespace) -> int:
    from ._deploy import reconcile

    _init(a.config)
    envs = _load_envs(a.file, a.env)
    every = parse_since(a.schedule).total_seconds()
    reconcile(envs, every_s=every, rounds=a.rounds, dry_run=a.dry_run)
    return 0


def cmd_import(a: argparse.Namespace) -> int:
    from ._backfill import backfill

    _init(a.config)
    out = backfill(a.project, a.domain, a.since, limit=a.limit, dry_run=a.dry_run)
    print(json.dumps(out, indent=2))
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    rest: List[str] = []
    if "--" in argv:
        i = argv.index("--")
        argv, rest = argv[:i], argv[i + 1:]
    p = argparse.ArgumentParser(prog="rsz flyte", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd")

    r = sub.add_parser("run", help="flyte run with the run's rightsizer policy")
    r.add_argument("--mode", "--rightsize", dest="mode")
    r.add_argument("--risk")
    r.add_argument("--estimator")
    r.add_argument("--config")
    r.add_argument("--print", action="store_true", help="print the flyte command instead of running it")

    d = sub.add_parser("deploy", help="deploy with deploy-time sizing")
    d.add_argument("file")
    d.add_argument("env", nargs="+")
    d.add_argument("--restore", action="store_true", help="redeploy from source with sizing disabled")
    d.add_argument("--version")
    g = d.add_mutually_exclusive_group()
    g.add_argument("--reconcile", dest="reconcile", action="store_true", default=None,
                   help="allow reductions (a reconcile singleton is scheduled)")
    g.add_argument("--no-reconcile", dest="reconcile", action="store_false", help="increases only")
    d.add_argument("--dry-run", action="store_true")
    d.add_argument("--plan", action="store_true", help="print the per-task plan and exit")
    d.add_argument("--config")

    c = sub.add_parser("reconcile", help="singleton: redeploy at the prior after breaker trips or a mode-ceiling drop")
    c.add_argument("file")
    c.add_argument("env", nargs="+")
    c.add_argument("--schedule", default="5m")
    c.add_argument("--rounds", type=int)
    c.add_argument("--dry-run", action="store_true")
    c.add_argument("--config")

    i = sub.add_parser("import", help="backfill statuses from flyte.remote")
    i.add_argument("--project")
    i.add_argument("--domain")
    i.add_argument("--since", default="30d")
    i.add_argument("--limit", type=int, default=1000)
    i.add_argument("--dry-run", action="store_true")
    i.add_argument("--config")

    a = p.parse_args(argv)
    if a.cmd is None:
        p.print_help()
        return 2
    if a.cmd == "run":
        return cmd_run(a, rest)
    if rest:
        p.error(f"unexpected arguments after --: {rest}")
    return {"deploy": cmd_deploy, "reconcile": cmd_reconcile, "import": cmd_import}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
