"""Estimator Protocol v1 conformance of ``rightsizer.serve`` (SPEC §6.3):
every endpoint and error code, batching, auth, health before ready."""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import urllib.error
import urllib.request

import pytest

from rightsizer.serve import EstimatorServer, Insufficient, Unavailable, wrap

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
SCHEMA = os.path.join(REPO, "schemas", "core", "1", "estimate.json")


def ctx(name: str = "a", **kw) -> dict:
    c = {"v": 1, "key": f"k8s://c/ns/job/{name}", "unit": "main", "platform": {"kind": "k8s"},
         "prior": {"spec": {"requests": {}, "limits": {}}}, "inputs": [], "history": []}
    c.update(kw)
    return c


def est_for(c: dict) -> dict:
    p = [0.0] * 27
    p[4 if c["key"].endswith("a") else 6] = 1.0
    return {"v": 1, "kind": "demand", "memory": {"kind": "cells", "grid": "rs-grid-v1", "probs": p},
            "cpu_mean": {"kind": "point", "value": 1.5},
            "provenance": {"estimator": "test", "version": "1", "grid": "rs-grid-v1", "features": "features-v1",
                           "latency_ms": 0}}


def call(url: str, method: str = "GET", body=None, token: str | None = None, raw: bytes | None = None):
    data = raw if raw is not None else (None if body is None else json.dumps(body).encode())
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"null")


class Batchy:
    """A batch estimator that records its calls."""

    def __init__(self, delay: float = 0.0):
        self.calls: list[int] = []
        self.delay = delay
        self.is_ready = True

    def estimate_batch(self, contexts, axes=None):
        self.calls.append(len(contexts))
        time.sleep(self.delay)
        out = []
        for c in contexts:
            if c["key"].endswith("insufficient"):
                out.append(Insufficient("no code"))
            elif c["key"].endswith("boom"):
                raise RuntimeError("backend exploded")
            else:
                out.append(est_for(c))
        return out

    def info(self):
        return {"id": "batchy", "version": "7", "grids": ["rs-grid-v1"], "axes": ["memory", "cpu_mean"],
                "kind": "demand", "needs": {"code": True, "history": True, "input_bytes": True},
                "features": "features-v1", "max_batch": 8}

    def ready(self):
        return self.is_ready


@pytest.fixture
def server():
    est = Batchy()
    srv = EstimatorServer(est, port=0, batch_window_ms=30).start()
    yield srv, est
    srv.close()


def test_info(server):
    srv, _ = server
    s, b = call(srv.url + "/info")
    assert s == 200 and b["protocol"] == "estimator/1" and b["id"] == "batchy" and b["version"] == "7"
    assert b["grids"] == ["rs-grid-v1"] and b["kind"] == "demand" and b["max_batch"] == 8
    assert b["novelty"] is None and b["needs"] == {"code": True, "history": True, "input_bytes": True}


def test_estimate_and_schema(server):
    srv, _ = server
    s, b = call(srv.url + "/estimate", "POST", {"context": ctx(), "axes": ["memory", "cpu_mean"]})
    assert s == 200 and b["estimate"]["memory"]["probs"][4] == 1.0
    jsonschema = pytest.importorskip("jsonschema")
    jsonschema.validate(b["estimate"], json.load(open(SCHEMA)))


def test_axes_filter(server):
    srv, _ = server
    s, b = call(srv.url + "/estimate", "POST", {"context": ctx(), "axes": ["memory"]})
    assert s == 200 and "cpu_mean" not in b["estimate"] and "memory" in b["estimate"]


def test_batch_endpoint_one_backend_call(server):
    srv, est = server
    ctxs = [ctx("a"), ctx("b"), ctx("insufficient"), ctx("a")]
    s, b = call(srv.url + "/estimate:batch", "POST", {"contexts": ctxs, "axes": ["memory"]})
    assert s == 200 and len(b["results"]) == 4
    assert b["results"][0]["estimate"]["memory"]["probs"][4] == 1.0
    assert b["results"][1]["estimate"]["memory"]["probs"][6] == 1.0
    assert b["results"][2] == {"error": {"code": "insufficient", "message": "no code", "retryable": False}}
    assert est.calls == [4]


def test_concurrent_requests_coalesce(server):
    srv, est = server
    results = []

    def one(i):
        results.append(call(srv.url + "/estimate", "POST", {"context": ctx("a" if i % 2 else "b"), "axes": ["memory"]}))

    ts = [threading.Thread(target=one, args=(i,)) for i in range(6)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert all(s == 200 for s, _ in results)
    assert sum(est.calls) == 6 and len(est.calls) < 6  # fewer backend calls than requests


def test_max_batch_split(server):
    srv, est = server
    s, b = call(srv.url + "/estimate:batch", "POST", {"contexts": [ctx()] * 9})
    assert s == 400 and b["error"]["code"] == "malformed" and "max_batch" in b["error"]["message"]


@pytest.mark.parametrize("body,status,code", [
    (None, 400, "malformed"),
    ([1, 2], 400, "malformed"),
    ({"context": {"key": "x"}}, 400, "malformed"),
    ({"context": ctx(), "axes": ["bogus"]}, 400, "malformed"),
    ({"context": ctx(), "axes": "memory"}, 400, "malformed"),
    ({"context": ctx(), "grid": "tuner-v1"}, 409, "unsupported"),
    ({"context": ctx(), "features": "features-v2"}, 409, "unsupported"),
    ({"context": ctx(v=2)}, 409, "unsupported"),
    ({"context": ctx(), "axes": ["gpu_memory"]}, 409, "unsupported"),
    ({"context": ctx("insufficient")}, 422, "insufficient"),
    ({"context": ctx("boom")}, 500, "internal"),
])
def test_error_codes(server, body, status, code):
    srv, _ = server
    raw = b"{not json" if body is None else None
    s, b = call(srv.url + "/estimate", "POST", body, raw=raw)
    assert s == status and b["error"]["code"] == code
    assert set(b["error"]) == {"code", "message", "retryable"}


def test_backend_unavailable_maps_to_503():
    class Down(Batchy):
        def estimate_batch(self, contexts, axes=None):
            raise Unavailable("vllm restarting")

    srv = EstimatorServer(Down(), port=0).start()
    try:
        s, b = call(srv.url + "/estimate", "POST", {"context": ctx()})
        assert s == 503 and b["error"] == {"code": "unavailable", "message": "vllm restarting", "retryable": True}
    finally:
        srv.close()


def test_backend_error_is_retryable_503():
    from rightsizer.models.backends import BackendError

    class Flaky(Batchy):
        def estimate_batch(self, contexts, axes=None):
            raise BackendError("connection refused", retryable=True)

    srv = EstimatorServer(Flaky(), port=0).start()
    try:
        s, b = call(srv.url + "/estimate:batch", "POST", {"contexts": [ctx()]})
        assert s == 503 and b["error"]["retryable"] is True
    finally:
        srv.close()


def test_rate_limited():
    est = Batchy(delay=0.5)
    srv = EstimatorServer(est, port=0, max_pending=1, batch_window_ms=1).start()
    try:
        t = threading.Thread(target=call, args=(srv.url + "/estimate", "POST", {"context": ctx()}))
        t.start()
        time.sleep(0.15)
        s, b = call(srv.url + "/estimate", "POST", {"context": ctx()})
        t.join()
        assert s == 429 and b["error"]["code"] == "rate_limited" and b["error"]["retryable"] is True
    finally:
        srv.close()


def test_routes(server):
    srv, _ = server
    assert call(srv.url + "/nope")[0] == 404
    assert call(srv.url + "/estimate")[0] == 405
    s, b = call(srv.url + "/health")
    assert s == 200


def test_health_before_ready():
    gate = threading.Event()
    est = Batchy()

    def factory():
        gate.wait(5)
        return est

    srv = EstimatorServer(factory=factory, port=0).start()
    try:
        s, b = call(srv.url + "/health")
        assert s == 503 and b["error"]["code"] == "unavailable"
        assert call(srv.url + "/info")[0] == 503
        assert call(srv.url + "/estimate", "POST", {"context": ctx()})[0] == 503
        gate.set()
        for _ in range(100):
            if call(srv.url + "/health")[0] == 200:
                break
            time.sleep(0.02)
        assert call(srv.url + "/health")[0] == 200
        est.is_ready = False  # backend went away
        assert call(srv.url + "/health")[0] == 503
        assert call(srv.url + "/estimate", "POST", {"context": ctx()})[0] == 503
        est.is_ready = True
        assert call(srv.url + "/estimate", "POST", {"context": ctx()})[0] == 200
    finally:
        srv.close()


def test_load_failure_reported():
    def factory():
        raise FileNotFoundError("no such repo")

    srv = EstimatorServer(factory=factory, port=0).start()
    try:
        time.sleep(0.1)
        s, b = call(srv.url + "/health")
        assert s == 503 and "no such repo" in b["error"]["message"]
    finally:
        srv.close()


def test_bearer_auth():
    srv = EstimatorServer(Batchy(), port=0, token="s3cret").start()
    try:
        assert call(srv.url + "/health")[0] == 200  # probes need no token
        s, b = call(srv.url + "/info")
        assert s == 401 and b["error"]["code"] == "unauthorized"
        assert call(srv.url + "/info", token="wrong")[0] == 401
        assert call(srv.url + "/info", token="s3cret")[0] == 200
        assert call(srv.url + "/estimate", "POST", {"context": ctx()})[0] == 401
        assert call(srv.url + "/estimate", "POST", {"context": ctx()}, token="s3cret")[0] == 200
    finally:
        srv.close()


def test_token_file_and_env(tmp_path, monkeypatch):
    from rightsizer.serve.cli import _token, parser

    f = tmp_path / "tok"
    f.write_text("abc\n")
    assert _token(parser().parse_args(["--model", "m", "--backend", "openai", "--token-file", str(f)])) == "abc"
    monkeypatch.setenv("MY_TOKEN", "xyz")
    assert _token(parser().parse_args(["--model", "m", "--backend", "openai", "--token-env", "MY_TOKEN"])) == "xyz"


def test_wrap_plain_callable():
    srv = EstimatorServer(wrap(lambda c: est_for(c), id="fn", version="2", axes=["memory"]), port=0).start()
    try:
        s, b = call(srv.url + "/estimate:batch", "POST", {"contexts": [ctx("a"), ctx("b")]})
        assert s == 200 and [r["estimate"]["memory"]["probs"].index(1.0) for r in b["results"]] == [4, 6]
        s, b = call(srv.url + "/info")
        assert b["id"] == "fn" and b["axes"] == ["memory"]
    finally:
        srv.close()


def test_wrap_raising_callable():
    def fn(c):
        raise Insufficient("nothing to go on")

    srv = EstimatorServer(wrap(fn), port=0).start()
    try:
        s, b = call(srv.url + "/estimate", "POST", {"context": ctx()})
        assert s == 422 and b["error"]["message"] == "nothing to go on"
    finally:
        srv.close()


def test_contexts_not_logged_by_default(server, caplog):
    srv, _ = server
    secret = "SECRET_SOURCE_TEXT"
    with caplog.at_level(logging.DEBUG, logger="rightsizer.serve"):
        call(srv.url + "/estimate", "POST", {"context": ctx(code={"digest": "d", "source": secret})})
    assert caplog.records and secret not in caplog.text
