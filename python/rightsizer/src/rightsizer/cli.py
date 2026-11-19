"""The ``rightsizer`` command.

::

    rightsizer serve ...          # Estimator Protocol v1 server (rightsizer.serve)
    rightsizer models ...         # convert / merge / adapter / prompt (rightsizer.models)
    rightsizer flyte ...          # rightsizer.flyte.cli (extra: rightsizer[flyte])
    rightsizer ray ...            # rightsizer.ray.cli (extra: rightsizer[ray])
    rightsizer version

``rsz flyte`` / ``rsz ray`` delegate to ``python -m rightsizer.<platform>.cli``
(ARCHITECTURE.md §1); this is the same entry point from Python.
"""

from __future__ import annotations

import importlib
import sys
from typing import List, Optional

_PLATFORMS = {"flyte": "rightsizer[flyte]", "ray": "rightsizer[ray]"}

USAGE = "usage: rightsizer {serve,models,flyte,ray,version} ..."


def _platform(name: str, argv: List[str]) -> int:
    try:
        mod = importlib.import_module(f"rightsizer.{name}.cli")
    except ImportError as e:
        print(f"rightsizer {name}: not available ({e}). Install it with: pip install '{_PLATFORMS[name]}'", file=sys.stderr)
        return 2
    main = getattr(mod, "main", None)
    if main is None:
        print(f"rightsizer {name}: rightsizer.{name}.cli has no main()", file=sys.stderr)
        return 2
    return int(main(argv) or 0)


def main(argv: Optional[List[str]] = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] in ("-h", "--help", "help"):
        print(USAGE, file=sys.stderr if not argv else sys.stdout)
        return 0 if argv else 2
    cmd, rest = argv[0], argv[1:]
    if cmd == "serve":
        from rightsizer.serve.cli import main as serve_main

        return int(serve_main(rest) or 0)
    if cmd == "models":
        from rightsizer.models.cli import main as models_main

        return int(models_main(rest) or 0)
    if cmd in _PLATFORMS:
        return _platform(cmd, rest)
    if cmd in ("version", "--version"):
        from rightsizer import __version__

        try:
            from rightsizer import _core

            native = _core.version()
        except ImportError:
            native = "unavailable"
        print(f"rightsizer {__version__} (native {native})")
        return 0
    print(USAGE, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
