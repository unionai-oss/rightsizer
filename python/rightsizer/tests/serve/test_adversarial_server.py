"""Adversarial tests for ``rightsizer.serve``: framing, auth ordering, non-JSON
results, slow clients, readiness probing and the CLI's token sources."""

from __future__ import annotations

import json
import socket
import threading
import time

import pytest

from rightsizer.serve import EstimatorServer, wrap
from rightsizer.serve import cli as serve_cli


def ctx(name: str = "a", **kw) -> dict:
    c = {"v": 1, "key": f"k8s://c/ns/job/{name}", "unit": "main", "platform": {"kind": "k8s"},
         "prior": {"spec": {"requests": {}, "limits": {}}}, "inputs": [], "history": []}
    c.update(kw)
    return c


def good(c) -> dict:
    p = [0.0] * 27
    p[4] = 1.0
    return {"v": 1, "kind": "demand", "memory": {"kind": "cells", "grid": "rs-grid-v1", "probs": p},
            "provenance": {"estimator": "t", "version": "1", "grid": "rs-grid-v1", "features": "features-v1",
                           "latency_ms": 0}}


def raw_http(port: int, data: bytes, *, half_close: bool = False, timeout: float = 3.0) -> bytes:
    """Send raw bytes, return what the server answers (b"" if it closes without a response)."""
    s = socket.create_connection(("127.0.0.1", port), timeout=timeout)
    try:
        s.sendall(data)
        if half_close:
            s.shutdown(socket.SHUT_WR)
        out = b""
        try:
            while True:
                chunk = s.recv(65536)
                if not chunk:
                    break
                out += chunk
                if b"\r\n\r\n" in out:
                    head, _, body = out.partition(b"\r\n\r\n")
                    for line in head.split(b"\r\n"):
                        if line.lower().startswith(b"content-length:") and len(body) >= int(line.split(b":")[1]):
                            return out
        except socket.timeout:
            pass
        return out
    finally:
        s.close()


def status_of(resp: bytes) -> int | None:
    if not resp.startswith(b"HTTP/"):
        return None
    return int(resp.split(b" ", 2)[1])


@pytest.fixture
def srv():
    s = EstimatorServer(wrap(good, axes=["memory"]), port=0, batch_window_ms=1, max_body_bytes=200).start()
    yield s
    s.close()


def test_negative_content_length_does_not_bypass_body_limit(srv):
    body = json.dumps({"context": ctx(), "pad": "x" * 5000}).encode()
    req = (b"POST /v1/estimate HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n"
           b"Content-Length: -1\r\n\r\n" + body)
    resp = raw_http(srv.port, req, half_close=True)
    assert status_of(resp) == 400, resp[:200]


def test_non_numeric_content_length_is_a_bad_request(srv):
    resp = raw_http(srv.port, b"POST /v1/estimate HTTP/1.1\r\nHost: x\r\nContent-Length: ten\r\n\r\n")
    assert status_of(resp) == 400, resp[:200]


def test_chunked_body_is_refused_not_misparsed(srv):
    req = (b"POST /v1/estimate HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"
           b"5\r\nhello\r\n0\r\n\r\n")
    resp = raw_http(srv.port, req)
    assert status_of(resp) == 400, resp[:200]
    assert resp.count(b"HTTP/1.1") == 1  # the chunk bytes were not read as a second request


def test_auth_is_checked_before_the_body_is_read():
    s = EstimatorServer(wrap(good, axes=["memory"]), port=0, token="secret", max_body_bytes=1 << 20).start()
    try:
        # Announce a large body but never send it: an unauthenticated client must be
        # turned away without the server waiting for (or buffering) the body.
        t0 = time.monotonic()
        resp = raw_http(s.port, b"POST /v1/estimate HTTP/1.1\r\nHost: x\r\nContent-Length: 500000\r\n\r\n",
                        timeout=2.0)
        assert status_of(resp) == 401, resp[:200]
        assert time.monotonic() - t0 < 1.5
    finally:
        s.close()


def test_nan_estimate_gets_a_response_not_a_dropped_connection():
    def nan_est(c):
        e = good(c)
        e["memory"]["probs"][0] = float("nan")
        return e

    s = EstimatorServer(wrap(nan_est, axes=["memory"]), port=0, batch_window_ms=1).start()
    try:
        body = json.dumps({"context": ctx()}).encode()
        req = (b"POST /v1/estimate HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n"
               b"Content-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body)
        resp = raw_http(s.port, req)
        assert status_of(resp) == 500, resp[:200]
        assert b'"internal"' in resp
    finally:
        s.close()


def test_numpy_scalars_in_estimates_serialize():
    np = pytest.importorskip("numpy")

    def np_est(c):
        e = good(c)
        e["memory"]["probs"] = list(np.asarray(e["memory"]["probs"], dtype=np.float32))
        return e

    s = EstimatorServer(wrap(np_est, axes=["memory"]), port=0, batch_window_ms=1).start()
    try:
        body = json.dumps({"context": ctx()}).encode()
        req = (b"POST /v1/estimate HTTP/1.1\r\nHost: x\r\nContent-Length: " + str(len(body)).encode()
               + b"\r\n\r\n" + body)
        resp = raw_http(s.port, req)
        assert status_of(resp) == 200, resp[:300]
    finally:
        s.close()


def test_bad_context_version_is_400_not_500(srv):
    for v in ("abc", [1], {"x": 1}):
        body = json.dumps({"context": ctx(v=v)}).encode()
        req = (b"POST /v1/estimate HTTP/1.1\r\nHost: x\r\nContent-Length: " + str(len(body)).encode()
               + b"\r\n\r\n" + body)
        assert status_of(raw_http(srv.port, req)) == 400


def test_grids_must_be_a_list(srv):
    # "rs-grid-v1" as a string used to be iterated character by character (a 409).
    status, _ = _handle(srv, {"context": ctx(), "grids": "rs-grid-v1"})
    assert status == 400


def _handle(srv, body):
    from rightsizer.serve.errors import EstimatorError

    try:
        return srv.handle("POST", "/v1/estimate", {}, json.dumps(body).encode())
    except EstimatorError as e:
        return e.status, e.body()


def test_idle_connections_time_out():
    s = EstimatorServer(wrap(good, axes=["memory"]), port=0, idle_timeout_s=0.3).start()
    try:
        c = socket.create_connection(("127.0.0.1", s.port), timeout=3.0)
        c.sendall(b"POST /v1/estimate HTTP/1.1\r\n")  # a slowloris header, never finished
        t0 = time.monotonic()
        assert c.recv(1024) == b""  # the server hangs up
        assert time.monotonic() - t0 < 2.0
        c.close()
    finally:
        s.close()


def test_connection_cap_bounds_handler_threads():
    s = EstimatorServer(wrap(good, axes=["memory"]), port=0, max_connections=4, idle_timeout_s=5).start()
    socks = []
    try:
        for _ in range(12):
            c = socket.create_connection(("127.0.0.1", s.port), timeout=3.0)
            c.sendall(b"GET /v1/info HTTP/1.1\r\n")  # unfinished: holds a handler
            socks.append(c)
        time.sleep(0.3)
        handlers = [t for t in threading.enumerate() if t.name.startswith("rightsizer-serve-conn")]
        assert len(handlers) <= 4
    finally:
        for c in socks:
            c.close()
        s.close()


def test_backend_readiness_is_not_probed_per_request():
    probes = []

    def ready():
        probes.append(1)
        return True

    s = EstimatorServer(wrap(good, axes=["memory"], ready=ready), port=0, batch_window_ms=1).start()
    try:
        for _ in range(20):
            status, _ = _handle(s, {"context": ctx()})
            assert status == 200
        assert len(probes) <= 3
    finally:
        s.close()


def test_health_reprobes_the_backend():
    state = {"ok": True}
    s = EstimatorServer(wrap(good, axes=["memory"], ready=lambda: state["ok"]), port=0).start()
    try:
        assert s.handle("GET", "/v1/health", {}, None)[0] == 200
        state["ok"] = False
        assert s.handle("GET", "/v1/health", {}, None)[0] == 503  # liveness probes see it at once
    finally:
        s.close()


def test_empty_token_file_refuses_to_start_unauthenticated(tmp_path):
    p = tmp_path / "token"
    p.write_text("\n")
    args = serve_cli.parser().parse_args(["--model", "m", "--backend", "openai", "--token-file", str(p)])
    with pytest.raises(SystemExit):
        serve_cli._token(args)


def test_empty_token_is_never_accepted():
    s = EstimatorServer(wrap(good, axes=["memory"]), port=0, token="")
    try:
        from rightsizer.serve.errors import Unauthorized

        with pytest.raises(Unauthorized):
            s.handle("GET", "/v1/info", {"Authorization": "Bearer "}, None)
    finally:
        s.httpd.server_close()


def test_coalescer_survives_a_base_exception():
    calls = {"n": 0}

    def batch(ctxs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise SystemExit("backend called sys.exit")
        return [good(c) for c in ctxs]

    s = EstimatorServer(wrap(batch=batch, axes=["memory"]), port=0, batch_window_ms=1, request_timeout_s=3).start()
    try:
        first, _ = _handle(s, {"context": ctx()})
        assert first == 500
        second, _ = _handle(s, {"context": ctx()})
        assert second == 200
    finally:
        s.close()
