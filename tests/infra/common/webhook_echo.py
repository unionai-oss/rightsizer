#!/usr/bin/env python3
"""Throwaway HTTPS admission-webhook server for checking host reachability.

Answers every AdmissionReview with allowed=true, except objects named
``rsz-it-deny-me``, which it denies with a recognisable message so a test can
prove the API server really called the host. ``GET /healthz`` returns ``ok``.

Usage: webhook_echo.py --port N --cert CERT --key KEY [--bind 127.0.0.1]
Stdlib only, so it runs on any host python3.
"""

from __future__ import annotations

import argparse
import json
import ssl
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DENY_NAME = "rsz-it-deny-me"
DENY_MESSAGE = "denied-by-rsz-it-host-webhook"


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args: object) -> None:  # noqa: D401
        sys.stderr.write("webhook_echo: " + (fmt % args) + "\n")

    def _send(self, code: int, body: bytes, ctype: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        self._send(200, b"ok\n", "text/plain")

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        review = json.loads(self.rfile.read(length) or b"{}")
        req = review.get("request", {})
        name = (req.get("object") or {}).get("metadata", {}).get("name") or req.get("name")
        kind = (req.get("kind") or {}).get("kind")
        sys.stderr.write(f"webhook_echo: AdmissionReview kind={kind} name={name} op={req.get('operation')}\n")
        allowed = name != DENY_NAME
        resp = {
            "apiVersion": review.get("apiVersion", "admission.k8s.io/v1"),
            "kind": "AdmissionReview",
            "response": {"uid": req.get("uid", ""), "allowed": allowed},
        }
        if not allowed:
            resp["response"]["status"] = {"code": 403, "message": DENY_MESSAGE}
        self._send(200, json.dumps(resp).encode(), "application/json")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--cert", required=True)
    ap.add_argument("--key", required=True)
    ap.add_argument("--bind", default="127.0.0.1")
    a = ap.parse_args()
    srv = ThreadingHTTPServer((a.bind, a.port), Handler)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(a.cert, a.key)
    srv.socket = ctx.wrap_socket(srv.socket, server_side=True)
    sys.stderr.write(f"webhook_echo: listening on https://{a.bind}:{a.port}\n")
    srv.serve_forever()


if __name__ == "__main__":
    main()
