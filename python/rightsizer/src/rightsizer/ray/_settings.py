"""Deployment settings for the Ray integration (ray.md §2 "Platform state", §8).

Resolved once per engine from the configuration (``[ray]``, ``[history]``)
and the environment, then cached; :func:`configure` overrides them.

- **Tenant** (ray.md §8): ``configure(tenant=)``, else ``[ray] tenant``,
  else the KubeRay namespace (``RAY_CLUSTER_NAMESPACE`` / ``POD_NAMESPACE`` /
  the service-account namespace file, plus ``RAY_CLUSTER_NAME``), else
  ``RIGHTSIZER_TENANT`` or the user name.
- **Trust** (SPEC §8.2): ``authenticated`` only when ``[ray]
  tenant_scoped_credentials = true`` (the operator scoped the store
  credentials to the tenant's prefix), else ``unverified``.
- **Shapes** (S4): the autoscaler's node types when the cluster has an
  autoscaler, else ``[ray] shapes``; neither → no platform state (increases
  are clamped to the definition ceiling or held, reason
  ``no_platform_state``).
"""

from __future__ import annotations

import logging
import os
import re
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

log = logging.getLogger("rightsizer.ray")

SA_NAMESPACE_FILE = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
AUTOSCALER_TTL_S = 60.0
_TENANT_BAD = re.compile(r"[^A-Za-z0-9._@+=,-]")
# Nominal $/h for shapes the autoscaler reports without prices (m6i-like
# linear price: per core and per GiB). Only relative prices matter.
_PRICE_PER_CORE = 0.0475
_PRICE_PER_GIB = 0.0063
_PRICE_PER_GPU = 1.0

# Ray accelerator_type constants → catalog ids (SPEC §5.1.1). "A100" is
# ambiguous (40G or 80G) and maps to A100-40G only for the prior; rendering
# goes the other way (``ray_accelerator``).
_RAY_TO_CATALOG = {
    "T4": "T4-16G",
    "A10G": "A10G-24G",
    "L4": "L4-24G",
    "L40S": "L40S-48G",
    "A100": "A100-40G",
    "A100-40G": "A100-40G",
    "A100-80G": "A100-80G",
    "H100": "H100-80G",
    "H200": "H200-141G",
    "B200": "B200-180G",
    "AMD-Instinct-MI300X-OAM": "MI300X-192G",
    "TPU-V5P": "TPU-V5P",
    "TPU-V6E": "TPU-V6E",
    "TPU-V5LITEPOD": "TPU-V5E",
}
_CATALOG_TO_RAY = {
    "T4-16G": "T4",
    "A10G-24G": "A10G",
    "L4-24G": "L4",
    "L40S-48G": "L40S",
    "A100-40G": "A100",
    "A100-80G": "A100",
    "H100-80G": "H100",
    "H200-141G": "H200",
    "B200-180G": "B200",
    "MI300X-192G": "AMD-Instinct-MI300X-OAM",
    "TPU-V5P": "TPU-V5P",
    "TPU-V6E": "TPU-V6E",
    "TPU-V5E": "TPU-V5LITEPOD",
}


def catalog_accelerator(ray_type: Optional[str]) -> str:
    """A Ray ``accelerator_type`` as a catalog id (``unknown-gpu`` when unknown)."""
    if not ray_type:
        return "unknown-gpu"
    return _RAY_TO_CATALOG.get(ray_type, "unknown-gpu")


def ray_accelerator(catalog_id: str, labelled: bool = False) -> Optional[str]:
    """The ``accelerator_type`` to render for a catalog id: only constants
    nodes carry automatically; ``A100-40G`` / ``A100-80G`` only when the
    operator labelled nodes that way (``[ray] labelled_accelerators``)."""
    if labelled and catalog_id in ("A100-40G", "A100-80G"):
        return catalog_id
    return _CATALOG_TO_RAY.get(catalog_id)


@dataclass
class Overrides:
    engine: Any = None
    tenant: Optional[str] = None
    shapes: Optional[List[Dict[str, Any]]] = None
    trust: Optional[str] = None
    measure: Optional[bool] = None
    observe: Optional[bool] = None
    mode: Optional[str] = None


@dataclass
class RaySettings:
    """What every Ray decision in this process uses."""

    engine: Any  # rightsizer.Engine, or None when it cannot be built (S1)
    tenant: str
    trust: str
    store: Optional[str]
    cluster: Optional[str]
    labelled_accelerators: bool
    measure: bool
    observe: bool
    mode: Optional[str]
    config_shapes: List[Dict[str, Any]] = field(default_factory=list)
    _shapes: Optional[Tuple[float, str, List[Dict[str, Any]]]] = None
    _lock: threading.Lock = field(default_factory=threading.Lock)

    _refreshing: bool = False

    def shapes(self) -> Tuple[str, List[Dict[str, Any]]]:
        """``(source, shapes)``: source is ``autoscaler``, ``config`` or ``none``.

        Reading the autoscaler can take GCS round trips (``get_cluster_status``
        waits up to 2 s), which must not land on a ``.remote()`` call: once a
        value is cached, an expired one is returned as is while one background
        thread refreshes it (only the very first resolution is inline).
        """
        now = time.monotonic()
        cached = self._shapes
        if cached is not None:
            if now - cached[0] >= AUTOSCALER_TTL_S:
                self._refresh_in_background()
            return cached[1], cached[2]
        with self._lock:
            cached = self._shapes
            if cached is not None:
                return cached[1], cached[2]
            return self._resolve_shapes()

    def _resolve_shapes(self) -> Tuple[str, List[Dict[str, Any]]]:
        now = time.monotonic()
        auto = autoscaler_shapes()
        if auto:
            out = ("autoscaler", auto)
        elif self.config_shapes:
            out = ("config", list(self.config_shapes))
        else:
            out = ("none", [])
        # Cache "none" only once Ray is up (a later call may see the autoscaler).
        if out[0] != "none" or _ray_initialized():
            self._shapes = (now, out[0], out[1])
        return out

    def _refresh_in_background(self) -> None:
        with self._lock:
            if self._refreshing:
                return
            self._refreshing = True

        def run() -> None:
            try:
                self._resolve_shapes()
            except Exception as e:  # pragma: no cover - autoscaler_shapes never raises
                log.debug("rightsizer.ray: shapes refresh failed: %s", e)
            finally:
                self._refreshing = False

        try:
            threading.Thread(target=run, name="rightsizer-ray-shapes", daemon=True).start()
        except Exception:  # pragma: no cover
            self._refreshing = False


_lock = threading.Lock()
_overrides = Overrides()
_settings: Optional[RaySettings] = None
_settings_engine: Any = None


def configure(
    *,
    engine: Any = None,
    tenant: Optional[str] = None,
    shapes: Optional[Sequence[Mapping[str, Any]]] = None,
    trust: Optional[str] = None,
    measure: Optional[bool] = None,
    observe: Optional[bool] = None,
    mode: Optional[str] = None,
) -> None:
    """Override what the Ray integration uses (``None`` = keep resolving).

    ``engine``: a ``rightsizer.Engine`` (default: ``rs.default_engine()``);
    ``tenant``: the tenant of every key; ``shapes``: ``NodeShape`` dicts
    (``{"cpu": millicores, "memory": bytes, "gpu"?, "price_per_hour"}``);
    ``trust``: ``"authenticated"`` / ``"unverified"`` for in-task records;
    ``measure`` / ``observe``: in-task measurement / driver-side status
    classification; ``mode``: the default mode of Ray decisions.
    """
    global _overrides, _settings
    with _lock:
        _overrides = Overrides(engine=engine, tenant=tenant, shapes=None if shapes is None else [dict(s) for s in shapes],
                               trust=trust, measure=measure, observe=observe, mode=mode)
        _settings = None


def _ray_initialized() -> bool:
    try:
        import ray

        return bool(ray.is_initialized())
    except Exception:
        return False


def kuberay_tenant(environ: Optional[Mapping[str, str]] = None, sa_file: str = SA_NAMESPACE_FILE) -> Optional[str]:
    """``{namespace}/{cluster}`` (or ``{namespace}``) on KubeRay, else ``None``."""
    env = os.environ if environ is None else environ
    ns = env.get("RAY_CLUSTER_NAMESPACE") or env.get("POD_NAMESPACE")
    if not ns and (env.get("RAY_CLUSTER_NAME") or env.get("KUBERAY_GEN_RAY_START_CMD")):
        try:
            with open(sa_file, encoding="utf-8") as f:
                ns = f.read().strip()
        except OSError:
            ns = None
    if not ns:
        return None
    ns = _TENANT_BAD.sub("-", ns)
    cluster = env.get("RAY_CLUSTER_NAME")
    return f"{ns}/{_TENANT_BAD.sub('-', cluster)}" if cluster else ns


def _resolve_engine(ov: Overrides) -> Any:
    if ov.engine is not None:
        return ov.engine
    from .._engine import default_engine

    try:
        return default_engine()
    except Exception as e:  # S1: decisions keep the prior
        log.debug("rightsizer.ray: no engine: %s", e)
        return None


def settings() -> RaySettings:
    """The cached settings (rebuilt when the default engine changes)."""
    global _settings, _settings_engine
    s = _settings
    if s is not None:
        if _overrides.engine is not None:
            return s
        from .. import _engine as eng_mod

        if eng_mod._default is _settings_engine and _settings_engine is not None:
            return s
    with _lock:
        ov = _overrides
        engine = _resolve_engine(ov)
        cfg: Dict[str, Any] = {}
        if engine is not None:
            try:
                cfg = engine.config
            except Exception as e:
                log.debug("rightsizer.ray: config unreadable: %s", e)
        ray_cfg = cfg.get("ray") or {}
        hist = cfg.get("history") or {}
        tenant = ov.tenant or ray_cfg.get("tenant") or kuberay_tenant()
        if not tenant:
            from ..types import default_tenant

            tenant = default_tenant()
        trust = ov.trust or ("authenticated" if ray_cfg.get("tenant_scoped_credentials") else "unverified")
        s = RaySettings(
            engine=engine,
            tenant=tenant,
            trust=trust,
            store=hist.get("uri"),
            cluster=hist.get("cluster"),
            labelled_accelerators=bool(ray_cfg.get("labelled_accelerators")),
            measure=True if ov.measure is None else bool(ov.measure),
            observe=True if ov.observe is None else bool(ov.observe),
            mode=ov.mode,
            config_shapes=list(ov.shapes if ov.shapes is not None else (ray_cfg.get("shapes") or [])),
        )
        _settings = s
        _settings_engine = engine if ov.engine is None else None
        return s


# ── autoscaler node types ────────────────────────────────────────────────


def shape_from_resources(name: Optional[str], res: Mapping[str, float]) -> Optional[Dict[str, Any]]:
    """A ``NodeShape`` dict from a Ray resource map (``CPU``, ``memory``, ``GPU``,
    ``accelerator_type:X``). ``memory`` is Ray's schedulable memory."""
    cpu = float(res.get("CPU") or 0)
    mem = float(res.get("memory") or 0)
    if cpu <= 0 or mem <= 0:
        return None
    out: Dict[str, Any] = {"cpu": int(round(cpu * 1000)), "memory": int(mem)}
    if name:
        out["name"] = name
    gpus = int(res.get("GPU") or 0)
    price = cpu * _PRICE_PER_CORE + mem / 2**30 * _PRICE_PER_GIB
    if gpus > 0:
        accel = next((k.split(":", 1)[1] for k in res if k.startswith("accelerator_type:")), None)
        out["gpu"] = {"accel": catalog_accelerator(accel), "count": gpus}
        price += gpus * _PRICE_PER_GPU
    out["price_per_hour"] = round(price, 6)
    return out


def _bootstrap_config_shapes(path: str = "~/ray_bootstrap_config.yaml") -> List[Dict[str, Any]]:
    p = os.path.expanduser(path)
    if not os.path.exists(p):
        return []
    try:
        import yaml  # type: ignore[import-not-found]

        with open(p, encoding="utf-8") as f:
            conf = yaml.safe_load(f) or {}
    except Exception:
        return []
    out = []
    for name, nt in (conf.get("available_node_types") or {}).items():
        s = shape_from_resources(name, (nt or {}).get("resources") or {})
        if s is not None:
            out.append(s)
    return out


def autoscaler_shapes() -> List[Dict[str, Any]]:
    """Node types of the cluster's autoscaler, when it has one (else ``[]``).

    An autoscaler is present when it registered ``AutoscalerMetricsAddress``
    or ``__autoscaling_status`` in the GCS KV. A fixed cluster started with
    ``ray start`` runs the monitor with the read-only provider, whose "node
    types" are the nodes themselves (``node_<node id>``): those are not node
    types and are ignored. Types come from the head's
    ``~/ray_bootstrap_config.yaml`` when readable, else from the nodes the
    autoscaler reports (``get_cluster_status``: active, idle, pending), one
    shape per ``ray_node_type_name``. Never raises.
    """
    if not _ray_initialized():
        return []
    try:
        import ray
        from ray.experimental.internal_kv import _internal_kv_get, _internal_kv_initialized

        if not _internal_kv_initialized():
            return []
        if not (_internal_kv_get(b"AutoscalerMetricsAddress") or _internal_kv_get(b"__autoscaling_status")):
            return []
        shapes = _bootstrap_config_shapes()
        if shapes:
            return shapes
        from ray.autoscaler.v2.sdk import get_cluster_status

        st = get_cluster_status(ray.get_runtime_context().gcs_address, timeout=2)
        by_type: Dict[str, Dict[str, Any]] = {}
        for n in list(st.active_nodes) + list(st.idle_nodes) + list(getattr(st, "pending_nodes", []) or []):
            name = getattr(n, "ray_node_type_name", None) or ""
            if not name or name == f"node_{getattr(n, 'node_id', '')}":
                continue  # the read-only provider of a fixed cluster (`ray start`): no node types
            res = getattr(getattr(n, "resource_usage", None), "usage", None)
            total = {u.resource_name: u.total for u in res} if res else dict(getattr(n, "total_resources", lambda: {})() or {})
            s = shape_from_resources(name, total)
            if s is not None and name not in by_type:
                by_type[name] = s
        return list(by_type.values())
    except Exception as e:
        log.debug("rightsizer.ray: autoscaler node types unavailable: %s", e)
        return []


def _reset_for_tests() -> None:
    global _settings, _settings_engine, _overrides
    with _lock:
        _settings, _settings_engine, _overrides = None, None, Overrides()


__all__ = ["RaySettings", "autoscaler_shapes", "catalog_accelerator", "configure", "kuberay_tenant", "ray_accelerator",
           "settings", "shape_from_resources"]
