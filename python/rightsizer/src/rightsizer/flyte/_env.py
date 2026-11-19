"""``RightsizedEnvironment`` (flyte.md §10, §4.4).

A ``@dataclass`` subclass of ``flyte.TaskEnvironment`` with two ``kw_only``
fields (``rightsize``, ``auto_image``); ``.task`` returns ``RightsizedTask``\\ s
with the environment's definition policy; ``clone_with`` keeps both fields
(it uses ``dataclasses.replace``). The creating process's ``RIGHTSIZER__*``
configuration is injected into ``env_vars``, so task pods (parents deciding,
children measuring) share it.

``auto_image``:

- ``True`` (default): ``flyte.Image`` images get
  ``with_pip_packages("rightsizer==<version>")`` (``"auto"`` becomes the
  default Debian image first); string images get a warning (an error under
  ``strict``);
- a directory of wheels (or ``$RIGHTSIZER_FLYTE_WHEELS``): the ``rightsizer``
  and ``rightsizer-runtime`` wheels found there are installed through the
  SDK's ``PythonWheels`` layer (local builds, e.g. the integration tests);
- ``False``: the image is left alone.
"""

from __future__ import annotations

import dataclasses
import logging
import os
import pathlib
import warnings
from typing import Any, Dict, Union

import flyte

from ._policy import definition
from ._task import _as_rightsized, _replace_in_registry, is_rightsized

log = logging.getLogger("rightsizer.flyte")

WHEELS_ENV = "RIGHTSIZER_FLYTE_WHEELS"
_INJECT_PREFIX = "RIGHTSIZER__"


def _strict() -> bool:
    return os.environ.get("RIGHTSIZER__STRICT", "").strip().lower() in ("1", "true", "yes")


def rightsizer_image(image: Any, auto_image: Union[bool, str, os.PathLike, None] = True) -> Any:
    """``image`` with rightsizer installed (see the module docstring)."""
    if auto_image is False or auto_image is None:
        return image
    wheels = os.environ.get(WHEELS_ENV) if auto_image is True else os.fspath(auto_image)
    if image == "auto" or image is None:
        image = flyte.Image.from_debian_base()
    if isinstance(image, str):
        msg = (f"rightsizer: the image {image!r} is a string; rightsizer cannot add itself to it. Parents need "
               "the rightsizer wheel and leaf tasks rightsizer-runtime, or decisions keep the prior (S1)")
        if _strict():
            raise ValueError(msg)
        warnings.warn(msg, stacklevel=3)
        return image
    if not isinstance(image, flyte.Image):
        return image
    if has_rightsizer(image):
        return image  # idempotent (clone_with re-runs __post_init__)
    if wheels:
        from flyte._image import PythonWheels  # private: the SDK's local-wheel layer (with_local_v2 uses it)

        d = pathlib.Path(wheels).expanduser().resolve()
        if not any(d.glob("rightsizer-*.whl")):
            raise FileNotFoundError(f"{WHEELS_ENV}={d}: no rightsizer-*.whl there")
        return image.clone(addl_layer=PythonWheels(wheel_dir=d, package_name="rightsizer"))
    from .. import __version__

    return image.with_pip_packages(f"rightsizer=={__version__}")


def has_rightsizer(image: Any) -> bool:
    """Whether an image already installs rightsizer (a wheel layer or a pip pin)."""
    for layer in getattr(image, "_layers", ()) or ():
        if getattr(layer, "package_name", None) == "rightsizer":
            return True
        for p in getattr(layer, "packages", None) or ():
            if isinstance(p, str) and p.replace(" ", "").startswith(("rightsizer==", "rightsizer>=", "rightsizer[")):
                return True
    return False


def injected_env(env_vars: Any = None, environ: Any = None) -> Dict[str, str]:
    """``env_vars`` plus the ``RIGHTSIZER__*`` configuration of ``environ`` (explicit values win)."""
    src = os.environ if environ is None else environ
    out = {k: v for k, v in src.items() if k.startswith(_INJECT_PREFIX)}
    out.update(dict(env_vars or {}))
    return out


@dataclasses.dataclass(init=True, repr=True)
class RightsizedEnvironment(flyte.TaskEnvironment):
    """``flyte.TaskEnvironment`` whose tasks are rightsizable (flyte.md §10).

    ``rightsize``: a ``DefinitionPolicy`` or a mode string (``"off"`` also
    caps ``max_mode``); the default mode is ``recommend``, which changes
    nothing.
    """

    rightsize: Any = dataclasses.field(default=None, kw_only=True)
    auto_image: Any = dataclasses.field(default=True, kw_only=True)

    def __post_init__(self) -> None:
        self._rs_definition = definition(self.rightsize)
        self.env_vars = injected_env(self.env_vars)
        try:
            self.image = rightsizer_image(self.image, self.auto_image)
        except (FileNotFoundError, ValueError):
            raise
        except Exception as e:  # S1 for anything unexpected: keep the user's image
            log.warning("rightsizer: image not extended: %s", e)
        super().__post_init__()

    def task(self, _func: Any = None, **kwargs: Any) -> Any:
        """``@env.task`` returning a ``RightsizedTask`` (the registry holds the same object)."""
        inner = super().task(**kwargs)

        def deco(func: Any) -> Any:
            tmpl = inner(func)
            if is_rightsized(tmpl):
                return tmpl
            new = _as_rightsized(tmpl, self._rs_definition, measured=True)
            _replace_in_registry(tmpl, new)
            return new

        if _func is not None:
            return deco(_func)
        return deco


__all__ = ["RightsizedEnvironment", "WHEELS_ENV", "has_rightsizer", "injected_env", "rightsizer_image"]
