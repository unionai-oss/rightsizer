"""Make ``rightsizer.export`` importable from the source tree (the wheel's
build config lives with the rest of the package)."""

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
for p in (HERE.parents[1] / "src", HERE):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))
