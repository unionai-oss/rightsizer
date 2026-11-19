"""A small JSON-over-HTTP client on the standard library (no requests/httpx)."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any

from . import BackendError


def base_url(url: str) -> str:
    """``http://host:8000/v1/`` -> ``http://host:8000`` (vLLM's ``/pooling`` and
    SGLang's ``/generate`` live at the root; OpenAI routes under ``/v1``)."""
    u = url.rstrip("/")
    return u.removesuffix("/v1")


class JsonClient:
    def __init__(self, url: str, api_key: str | None = None, timeout: float = 120.0):
        self.url = base_url(url)
        self.api_key = api_key
        self.timeout = timeout

    def _headers(self) -> dict[str, str]:
        h = {"Content-Type": "application/json", "Accept": "application/json"}
        if self.api_key:
            h["Authorization"] = f"Bearer {self.api_key}"
        return h

    def request(self, method: str, path: str, body: Any = None, timeout: float | None = None) -> tuple[int, Any]:
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(self.url + path, data=data, method=method, headers=self._headers())
        try:
            with urllib.request.urlopen(req, timeout=timeout or self.timeout) as r:
                raw = r.read()
                return r.status, (json.loads(raw) if raw else None)
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                payload = json.loads(raw) if raw else None
            except ValueError:
                payload = raw.decode(errors="replace")
            return e.code, payload
        except (urllib.error.URLError, OSError, TimeoutError) as e:
            raise BackendError(f"{method} {self.url}{path}: {e}", retryable=True) from e

    def post(self, path: str, body: Any) -> Any:
        status, payload = self.request("POST", path, body)
        if status != 200:
            raise BackendError(f"POST {self.url}{path}: HTTP {status}: {str(payload)[:500]}",
                               retryable=status in (408, 429, 500, 502, 503, 504))
        return payload

    def ok(self, path: str, timeout: float = 5.0) -> bool:
        try:
            status, _ = self.request("GET", path, None, timeout=timeout)
        except BackendError:
            return False
        return status == 200
