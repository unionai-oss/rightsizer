"""Lazy access to the native module ``rightsizer._core`` (SPEC §10.2).

``import rightsizer`` never imports ``_core``. The first decision does; an
image without a wheel for its platform fails there, the error is logged once
and every decision keeps the prior (S1).
"""

from __future__ import annotations

import importlib
import logging
import threading
from types import ModuleType
from typing import Optional

log = logging.getLogger("rightsizer")

_lock = threading.Lock()
_core: Optional[ModuleType] = None
_error: Optional[BaseException] = None


def core() -> ModuleType:
    """The native module; raises ``ImportError`` (cached) when it is missing."""
    global _core, _error
    if _core is not None:
        return _core
    if _error is not None:
        raise ImportError(str(_error)) from _error
    with _lock:
        if _core is not None:
            return _core
        try:
            _core = importlib.import_module("rightsizer._core")
            return _core
        except BaseException as e:  # ImportError, or an OSError from a bad wheel
            _error = e
            log.error(
                "rightsizer: the native module rightsizer._core cannot be imported (%s); "
                "decisions keep the prior. Install a rightsizer wheel for this platform.",
                e,
            )
            raise ImportError(f"rightsizer._core is not available: {e}") from e


def available() -> bool:
    """Whether ``_core`` can be imported (imports it)."""
    try:
        core()
        return True
    except ImportError:
        return False


def _reset_for_tests() -> None:
    global _core, _error
    _core, _error = None, None
