"""The Estimator Protocol v1 reference server (SPEC §6.3).

::

    GET  /v1/info             -> {protocol: "estimator/1", id, version, grids, axes, kind, needs, features,
                                  novelty, max_batch}
    POST /v1/estimate         {context, axes}     -> {estimate}
    POST /v1/estimate:batch   {contexts, axes}    -> {results: [{estimate} | {error}]}
    GET  /v1/health           -> 200 | 503 (503 until the estimator is loaded and its backend answers)

Built on the standard library (``http.server.ThreadingHTTPServer``): the
server only parses JSON and queues work; the cost is in the backend (a NumPy
head over hidden states from vLLM/SGLang, or an in-process model), so an
ASGI stack would add dependencies to the ``serve`` extra without buying
throughput. One thread per connection reads the request; a coalescer thread
gathers requests for up to ``batch_window_ms`` (or ``max_batch`` contexts)
and makes **one** backend call per batch, so concurrent ``/v1/estimate``
calls and a whole ``/v1/estimate:batch`` share a forward pass.

Requests may name ``grid`` (or ``grids``) and ``features``; a value the
estimator does not serve is a ``409``, as is a context ``v`` above 1 or a set
of ``axes`` with nothing the estimator estimates. Contexts are never logged
unless ``log_contexts=True``. Bearer auth (``token``) guards every endpoint
but ``/v1/health`` (for liveness probes).
"""

from __future__ import annotations

import hmac
import inspect
import json
import logging
import threading
import math
import time
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import Future
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .errors import (
    BadRequest,
    EstimatorError,
    Insufficient,
    Internal,
    NotFound,
    RateLimited,
    Unauthorized,
    Unavailable,
    Unsupported,
    as_error,
)

log = logging.getLogger("rightsizer.serve")

PROTOCOL = "estimator/1"
#: ``EstimateAxis`` spellings (``duration_s`` accepted as the Estimate field name).
AXES = ("memory", "cpu_mean", "cpu_peak", "gpu_memory", "duration")
_AXIS_FIELD = {"duration": "duration_s"}
_CTX_REQUIRED = ("key", "unit", "platform", "prior")


def _axis(a: str) -> str:
    return "duration" if a == "duration_s" else a


# ── wrapping a callable ─────────────────────────────────────────────────


class Estimator:
    """Wraps ``fn(ctx) -> Estimate`` or ``batch(ctxs, axes) -> [Estimate | error]``
    (dicts in the serde JSON shape) as something the server can call.

    ``fn`` and ``batch`` may take an ``axes`` keyword. A callable may raise
    :class:`~rightsizer.serve.errors.Insufficient` (or any ``EstimatorError``);
    a batch callable may return one in a result slot."""

    def __init__(self, fn: Callable | None = None, *, batch: Callable | None = None, info: Mapping[str, Any],
                 ready: Callable[[], bool] | None = None):
        if fn is None and batch is None:
            raise ValueError("give fn or batch")
        self._fn, self._batch, self._ready = fn, batch, ready
        self._info = dict(info)
        self._fn_axes = fn is not None and "axes" in inspect.signature(fn).parameters
        self._batch_axes = batch is not None and "axes" in inspect.signature(batch).parameters

    def info(self) -> dict:
        return dict(self._info)

    def ready(self) -> bool:
        return bool(self._ready()) if self._ready is not None else True

    def estimate_batch(self, contexts: Sequence[Mapping], axes: Sequence[str] | None = None) -> list:
        if self._batch is not None:
            return list(self._batch(contexts, axes=axes) if self._batch_axes else self._batch(contexts))
        out: list = []
        for c in contexts:
            try:
                out.append(self._fn(c, axes=axes) if self._fn_axes else self._fn(c))
            except Exception as e:  # noqa: BLE001 -- one context's error is that slot's error
                out.append(as_error(e))
        return out


def wrap(fn: Callable | None = None, *, batch: Callable | None = None, ready: Callable[[], bool] | None = None,
         id: str = "python", version: str = "0", grids: Sequence[str] = ("rs-grid-v1",),
         axes: Sequence[str] = ("memory", "cpu_mean"), kind: str = "demand", needs: Mapping[str, bool] | None = None,
         features: str = "features-v1", novelty: Mapping | None = None, max_batch: int = 32) -> Estimator:
    """``wrap(my_fn, id="mine", version="1", axes=["memory"])``."""
    info = {"id": id, "version": version, "grids": list(grids), "axes": list(axes), "kind": kind,
            "needs": dict(needs or {"code": True, "history": True, "input_bytes": True}), "features": features,
            "novelty": dict(novelty) if novelty is not None else None, "max_batch": int(max_batch)}
    return Estimator(fn, batch=batch, info=info, ready=ready)


def as_estimator(obj: Any) -> Any:
    """Anything with ``estimate_batch`` and ``info`` is used as is (``PointerModel``,
    ``GenerativeModel``, :class:`Estimator`)."""
    if hasattr(obj, "estimate_batch") and hasattr(obj, "info"):
        return obj
    if callable(obj):
        return wrap(obj)
    raise TypeError(f"not an estimator: {obj!r}")


# ── coalescing ──────────────────────────────────────────────────────────


class _Job:
    __slots__ = ("axes", "contexts", "future")

    def __init__(self, contexts: list, axes: tuple | None):
        self.contexts, self.axes, self.future = contexts, axes, Future()


class Coalescer:
    """Gathers jobs for up to ``window_s`` (or ``max_batch`` contexts) and calls
    ``estimate_batch`` once per distinct ``axes`` in the gathered set."""

    def __init__(self, estimator: Any, window_s: float, max_batch: int, max_pending: int):
        self.estimator, self.window_s, self.max_batch, self.max_pending = estimator, window_s, max_batch, max_pending
        self._cv = threading.Condition()
        self._queue: list[_Job] = []
        self._pending = 0
        self._stop = False
        self.calls = 0  # backend calls made (tests)
        self._thread = threading.Thread(target=self._run, name="rightsizer-serve-coalescer", daemon=True)
        self._thread.start()

    def submit(self, contexts: list, axes: Sequence[str] | None) -> Future:
        job = _Job(contexts, tuple(axes) if axes is not None else None)
        with self._cv:
            if self._pending + len(contexts) > self.max_pending:
                raise RateLimited(f"{self._pending} contexts pending (limit {self.max_pending})")
            self._pending += len(contexts)
            self._queue.append(job)
            self._cv.notify()
        return job.future

    def close(self) -> None:
        with self._cv:
            self._stop = True
            self._cv.notify()

    def _take(self) -> list[_Job]:
        with self._cv:
            while not self._queue and not self._stop:
                self._cv.wait()
            if self._stop:
                return []
            deadline = time.monotonic() + self.window_s
            while True:
                n = sum(len(j.contexts) for j in self._queue)
                left = deadline - time.monotonic()
                if n >= self.max_batch or left <= 0 or self._stop:
                    break
                self._cv.wait(left)
            jobs, total = [], 0
            while self._queue and (not jobs or total + len(self._queue[0].contexts) <= self.max_batch):
                j = self._queue.pop(0)
                jobs.append(j)
                total += len(j.contexts)
            return jobs

    def _run(self) -> None:
        while True:
            jobs = self._take()
            if not jobs:
                if self._stop:
                    return
                continue
            groups: dict[tuple | None, list[_Job]] = {}
            for j in jobs:
                groups.setdefault(j.axes, []).append(j)
            for axes, js in groups.items():
                ctxs = [c for j in js for c in j.contexts]
                try:
                    self.calls += 1
                    res = list(self.estimator.estimate_batch(ctxs, list(axes) if axes is not None else None))
                    if len(res) != len(ctxs):
                        raise RuntimeError(f"estimator returned {len(res)} results for {len(ctxs)} contexts")
                    at = 0
                    for j in js:
                        j.future.set_result(res[at:at + len(j.contexts)])
                        at += len(j.contexts)
                except BaseException as e:  # noqa: BLE001 -- the coalescer thread must outlive any backend failure
                    err = as_error(e) if isinstance(e, Exception) else Internal(f"{type(e).__name__} in the estimator")
                    log.warning("estimate batch of %d failed: %s", len(ctxs), type(e).__name__)
                    for j in js:
                        j.future.set_exception(err)
                finally:
                    with self._cv:
                        self._pending -= len(ctxs)


# ── the server ──────────────────────────────────────────────────────────


def _json_default(o: Any) -> Any:
    """NumPy scalars / arrays (an estimator's probabilities) as plain JSON values."""
    if hasattr(o, "tolist"):
        return o.tolist()
    if hasattr(o, "item"):
        return o.item()
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")


def _dumps(payload: Any) -> bytes:
    return json.dumps(payload, allow_nan=False, default=_json_default).encode()


class _Server(ThreadingHTTPServer):
    """``ThreadingHTTPServer`` with a bound on concurrent connections (one thread each):
    a connection over ``max_connections`` is closed at once instead of getting a thread."""

    daemon_threads = True

    def __init__(self, addr: Any, handler: Any, max_connections: int):
        self._slots = threading.BoundedSemaphore(max_connections)
        super().__init__(addr, handler)

    def process_request(self, request: Any, client_address: Any) -> None:
        if not self._slots.acquire(blocking=False):
            log.warning("connection limit reached; closing a new connection")
            self.shutdown_request(request)
            return
        t = threading.Thread(target=self._serve_one, args=(request, client_address),
                             name="rightsizer-serve-conn", daemon=True)
        t.start()

    def _serve_one(self, request: Any, client_address: Any) -> None:
        try:
            self.process_request_thread(request, client_address)
        finally:
            self._slots.release()


def _filter_axes(est: Mapping, axes: Sequence[str] | None) -> dict:
    out = dict(est)
    if axes is not None:
        want = {_AXIS_FIELD.get(a, a) for a in axes}
        for f in ("memory", "cpu_mean", "cpu_peak", "gpu_memory", "duration_s"):
            if f not in want:
                out.pop(f, None)
    return out


class EstimatorServer:
    """Serve an estimator: an object with ``estimate_batch``/``info``, a
    :class:`Estimator`, or a plain ``ctx -> Estimate`` callable. With
    ``factory=`` (zero arguments) it is loaded in the background and
    ``/v1/health`` is 503 until it returns and its backend is ready."""

    def __init__(self, estimator: Any = None, *, factory: Callable[[], Any] | None = None, host: str = "127.0.0.1", port: int = 8095, token: str | None = None,
                 batch_window_ms: float = 10.0, max_batch: int | None = None, max_pending: int = 1024,
                 log_contexts: bool = False, max_body_bytes: int = 64 << 20, request_timeout_s: float = 600.0,
                 idle_timeout_s: float = 60.0, max_connections: int = 256, ready_ttl_s: float = 1.0):
        self.token = token
        self.batch_window_s = batch_window_ms / 1000.0
        self._max_batch = max_batch
        self.max_pending = max_pending
        self.log_contexts = log_contexts
        self.max_body_bytes = max_body_bytes
        self.request_timeout_s = request_timeout_s
        self.idle_timeout_s = idle_timeout_s
        self.ready_ttl_s = ready_ttl_s
        self._ready_until = 0.0
        self._lock = threading.Lock()
        self._estimator: Any = None
        self._coalescer: Coalescer | None = None
        self.load_error: str | None = None
        if (estimator is None) == (factory is None):
            raise ValueError("give an estimator or a factory")
        if factory is None:
            self._install(as_estimator(estimator))
        else:
            threading.Thread(target=self._load, args=(factory,), name="rightsizer-serve-load", daemon=True).start()
        self.httpd = _Server((host, port), self._handler(), max_connections)
        self._thread: threading.Thread | None = None

    # lifecycle

    def _install(self, est: Any) -> None:
        mb = int(self._max_batch or est.info().get("max_batch") or 32)
        with self._lock:
            self._estimator = est
            self._coalescer = Coalescer(est, self.batch_window_s, mb, self.max_pending)

    def _load(self, factory: Callable[[], Any]) -> None:
        try:
            self._install(as_estimator(factory()))
        except Exception as e:
            self.load_error = f"{type(e).__name__}: {e}"
            log.exception("loading the estimator failed")

    @property
    def estimator(self) -> Any:
        return self._estimator

    @property
    def coalescer(self) -> Coalescer | None:
        return self._coalescer

    @property
    def max_batch(self) -> int:
        c = self._coalescer
        return c.max_batch if c is not None else int(self._max_batch or 32)

    @property
    def port(self) -> int:
        return self.httpd.server_address[1]

    @property
    def url(self) -> str:
        host = self.httpd.server_address[0]
        return f"http://{host}:{self.port}/v1"

    def start(self) -> EstimatorServer:
        self._thread = threading.Thread(target=self.httpd.serve_forever, kwargs={"poll_interval": 0.05},
                                        name="rightsizer-serve", daemon=True)
        self._thread.start()
        return self

    def serve_forever(self) -> None:
        self.httpd.serve_forever()

    def close(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        if self._coalescer is not None:
            self._coalescer.close()
        for name in ("close", "stop"):
            f = getattr(self._estimator, name, None) or getattr(getattr(self._estimator, "backend", None), name, None)
            if callable(f):
                try:
                    f()
                except Exception:
                    log.exception("closing the estimator failed")
                break

    def __enter__(self) -> EstimatorServer:
        return self.start()

    def __exit__(self, *exc) -> None:
        self.close()

    # protocol

    def is_ready(self, cached: bool = False) -> bool:
        """Whether the estimator is loaded and its backend answers. ``cached``: reuse a
        positive answer for ``ready_ttl_s`` (the estimate path; ``/v1/health`` always probes),
        so a request does not cost an extra backend health call."""
        est = self._estimator
        if est is None:
            return False
        if cached and time.monotonic() < self._ready_until:
            return True
        r = getattr(est, "ready", None)
        try:
            ok = bool(r()) if callable(r) else True
        except Exception:  # noqa: BLE001
            ok = False
        self._ready_until = time.monotonic() + self.ready_ttl_s if ok else 0.0
        return ok

    def info(self) -> dict:
        est = self._estimator
        if est is None:
            raise Unavailable(self.load_error or "loading")
        info = est.info() if callable(getattr(est, "info", None)) else dict(est.info)
        info.setdefault("novelty", None)
        info["max_batch"] = self.max_batch
        return {"protocol": PROTOCOL, **info}

    def _check(self, body: Any, many: bool) -> tuple[list, list[str] | None]:
        if not isinstance(body, dict):
            raise BadRequest("request body must be a JSON object")
        info = self.info()
        ctxs = body.get("contexts") if many else [body.get("context")]
        if many and not isinstance(ctxs, list):
            raise BadRequest("'contexts' must be an array")
        if many and len(ctxs) > self.max_batch:
            raise BadRequest(f"{len(ctxs)} contexts exceed max_batch {self.max_batch}")
        for i, c in enumerate(ctxs):
            where = f"contexts[{i}]" if many else "context"
            if not isinstance(c, dict):
                raise BadRequest(f"'{where}' must be a WorkloadContext object")
            missing = [k for k in _CTX_REQUIRED if k not in c]
            if missing:
                raise BadRequest(f"'{where}' lacks {', '.join(missing)}")
            if not isinstance(c.get("platform"), dict) or not isinstance(c.get("key"), str):
                raise BadRequest(f"'{where}': 'key' must be a string and 'platform' an object")
            v = c.get("v", 1)
            if v is None:
                v = 1
            if isinstance(v, bool) or not isinstance(v, int):
                raise BadRequest(f"'{where}': 'v' must be an integer")
            if v > 1:
                raise Unsupported(f"'{where}': WorkloadContext v{v} (this server reads v1)")
        grids = body.get("grids") or ([body["grid"]] if body.get("grid") else [])
        if not isinstance(grids, list) or not all(isinstance(g, str) for g in grids):
            raise BadRequest("'grids' must be an array of strings and 'grid' a string")
        feats = body.get("features")
        if feats is not None and not isinstance(feats, str):
            raise BadRequest("'features' must be a string")
        bad = [g for g in grids if g not in info.get("grids", [])]
        if bad:
            raise Unsupported(f"grid {bad[0]!r} not served (grids: {info.get('grids')})")
        feats = body.get("features")
        if feats and info.get("features") and feats != info["features"]:
            raise Unsupported(f"features {feats!r} not served (features: {info['features']!r})")
        axes = body.get("axes")
        if axes is not None:
            if not isinstance(axes, list) or not all(isinstance(a, str) for a in axes):
                raise BadRequest("'axes' must be an array of strings")
            axes = [_axis(a) for a in axes]
            unknown = [a for a in axes if a not in AXES]
            if unknown:
                raise BadRequest(f"unknown axis {unknown[0]!r} (axes: {list(AXES)})")
            served = {_axis(a) for a in info.get("axes", AXES)}
            if axes and not served.intersection(axes):
                raise Unsupported(f"none of axes {axes} is estimated (axes: {sorted(served)})")
        return ctxs, axes

    def authorize(self, headers: Mapping[str, str]) -> None:
        """Bearer auth; a no-op without a token. An empty token matches nothing."""
        if self.token is None:
            return
        auth = headers.get("Authorization") or headers.get("authorization") or ""
        given = auth[7:].strip() if auth.lower().startswith("bearer ") else ""
        if not given or not self.token or not hmac.compare_digest(given.encode(), self.token.encode()):
            raise Unauthorized("missing or wrong bearer token")

    def handle(self, method: str, path: str, headers: Mapping[str, str], raw: bytes | None) -> tuple[int, Any]:
        path = path.split("?", 1)[0].rstrip("/")
        if method == "GET" and path == "/v1/health":
            if self.is_ready():
                return 200, {"status": "ok"}
            return 503, Unavailable(self.load_error or ("loading" if self._estimator is None else
                                                        "backend not ready")).body()
        routes = {("GET", "/v1/info"), ("POST", "/v1/estimate"), ("POST", "/v1/estimate:batch")}
        if (method, path) not in routes:
            known = {p for _, p in routes} | {"/v1/health"}
            if path in known:
                return 405, BadRequest(f"{method} not allowed on {path}").body()
            raise NotFound(f"no route {method} {path}")
        self.authorize(headers)
        if path == "/v1/info":
            return 200, self.info()
        if not self.is_ready(cached=True):
            raise Unavailable(self.load_error or "backend not ready")
        try:
            body = json.loads(raw or b"")
        except ValueError as e:
            raise BadRequest(f"invalid JSON: {e}") from e
        many = path.endswith(":batch")
        ctxs, axes = self._check(body, many)
        if self.log_contexts:
            log.info("contexts: %s", json.dumps(ctxs))
        coal = self._coalescer
        assert coal is not None
        results = coal.submit(ctxs, axes).result(timeout=self.request_timeout_s)
        items = [as_error(r) if isinstance(r, BaseException) else _filter_axes(r, axes) for r in results]
        if not many:
            r = items[0]
            if isinstance(r, EstimatorError):
                raise r
            return 200, {"estimate": r}
        return 200, {"results": [r.body() if isinstance(r, EstimatorError) else {"estimate": r} for r in items]}

    def _handler(self):
        server = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"
            server_version = "rightsizer-serve/1"
            timeout = server.idle_timeout_s  # socket timeout: slow or idle clients do not hold a thread

            def _send(self, status: int, payload: Any) -> None:
                try:
                    data = _dumps(payload)
                except (TypeError, ValueError) as e:  # NaN/inf or a non-JSON value from the estimator
                    log.warning("response not serializable: %s", e)
                    status = 500
                    data = _dumps(Internal(f"estimator returned a value that is not JSON: {e}").body())
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                if status in (429, 503):
                    self.send_header("Retry-After", "1")
                self.end_headers()
                self.wfile.write(data)

            def _dispatch(self, method: str) -> None:
                t0 = time.monotonic()
                status = 500
                try:
                    raw = None
                    if method == "POST":
                        if self.headers.get("Transfer-Encoding"):
                            self.close_connection = True
                            raise BadRequest("Transfer-Encoding is not supported; send Content-Length")
                        cl = (self.headers.get("Content-Length") or "0").strip()
                        if not cl.isdigit():
                            self.close_connection = True
                            raise BadRequest(f"invalid Content-Length {cl[:32]!r}")
                        n = int(cl)
                        if n > server.max_body_bytes:
                            self.close_connection = True
                            raise BadRequest(f"body of {n} bytes exceeds {server.max_body_bytes}")
                        path = self.path.split("?", 1)[0].rstrip("/")
                        if path != "/v1/health":
                            try:  # before reading the body: unauthenticated clients cost no read
                                server.authorize(dict(self.headers.items()))
                            except Unauthorized:
                                self.close_connection = True
                                raise
                        raw = self.rfile.read(n) if n else b""
                    status, payload = server.handle(method, self.path, dict(self.headers.items()), raw)
                except EstimatorError as e:
                    status, payload = e.status, e.body()
                except Exception as e:
                    err = as_error(e)
                    log.error("request failed: %s", type(e).__name__)  # no message: it may quote a context
                    log.debug("request failed", exc_info=True)
                    status, payload = err.status, err.body()
                self._send(status, payload)
                log.info("%s %s %d %.1f ms", method, self.path.split("?", 1)[0], status,
                         (time.monotonic() - t0) * 1000)

            def do_GET(self) -> None:
                self._dispatch("GET")

            def do_POST(self) -> None:
                self._dispatch("POST")

            def log_message(self, format: str, *args: Any) -> None:
                pass

        return Handler


def serve(estimator: Any, **kw: Any) -> None:
    """Serve until interrupted."""
    srv = EstimatorServer(estimator, **kw)
    log.info("rightsizer estimator server on %s", srv.url)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.close()


__all__ = ["AXES", "PROTOCOL", "Coalescer", "Estimator", "EstimatorServer", "Insufficient", "serve", "wrap"]
