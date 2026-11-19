"""Mock vLLM and SGLang servers that answer in the documented shapes
(vLLM v0.34.0 ``POST /pooling`` with ``task=token_embed``; SGLang v0.5.23
``POST /generate`` with ``return_hidden_states``), computing the states with a
reference function ``ids -> float32 [n_tokens, hidden]``."""

from __future__ import annotations

import base64
import json
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class MockServer:
    def __init__(self, handler_cls):
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
        self.httpd.daemon_threads = True
        self.requests: list[dict] = []
        handler_cls.mock = self
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        self.healthy = True

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.httpd.server_address[1]}"

    def close(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()


class _Base(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    mock: MockServer

    def log_message(self, *a):
        pass

    def _json(self, status, payload):
        data = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/health":
            if self.mock.healthy:
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()
            else:
                self._json(503, {"error": "starting"})
        else:
            self._json(404, {"detail": "Not Found"})

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        b = json.loads(self.rfile.read(n))
        self.mock.requests.append(b)
        return b


def vllm_mock(reference, lora_name: str = "rightsize", base_name: str = "base") -> MockServer:
    class H(_Base):
        def do_POST(self):
            if self.path != "/pooling":
                return self._json(404, {"detail": "Not Found"})
            b = self._body()
            if b.get("model") not in (lora_name, base_name):
                return self._json(404, {"error": {"message": f"The model `{b.get('model')}` does not exist.",
                                                  "type": "NotFoundError", "code": 404}})
            if b.get("task", "token_embed") != "token_embed" or b.get("use_activation", True):
                return self._json(400, {"error": {"message": "expected task token_embed, use_activation false",
                                                  "type": "BadRequestError", "code": 400}})
            inputs = b["input"]
            if inputs and isinstance(inputs[0], int):
                inputs = [inputs]
            data, n_tok = [], 0
            for i, ids in enumerate(inputs):
                Hs = reference(ids).astype("<f4")
                n_tok += len(ids)
                enc = (base64.b64encode(Hs.tobytes()).decode() if b.get("encoding_format") == "base64"
                       else Hs.tolist())
                data.append({"index": i, "object": "pooling", "data": enc})
            self._json(200, {"id": f"pool-{uuid.uuid4().hex}", "object": "list", "created": int(time.time()),
                             "model": b["model"], "data": data,
                             "usage": {"prompt_tokens": n_tok, "total_tokens": n_tok, "completion_tokens": 0,
                                       "prompt_tokens_details": None}})

    return MockServer(H)


def sglang_mock(reference, lora_name: str = "rightsize") -> MockServer:
    class H(_Base):
        def do_POST(self):
            if self.path != "/generate":
                return self._json(404, {"detail": "Not Found"})
            b = self._body()
            if not b.get("return_hidden_states"):
                return self._json(400, {"error": {"message": "return_hidden_states required"}})
            ids_list = b["input_ids"]
            single = bool(ids_list) and isinstance(ids_list[0], int)
            if single:
                ids_list = [ids_list]
            loras = b.get("lora_path")
            loras = loras if isinstance(loras, list) else [loras] * len(ids_list)
            if any(lp not in (lora_name, None) for lp in loras):
                return self._json(400, {"error": {"message": "unknown lora_path"}})
            out = []
            for ids in ids_list:
                Hs = reference(ids)
                out.append({"text": "{", "output_ids": [90],
                            "meta_info": {"id": uuid.uuid4().hex, "finish_reason": {"type": "length", "length": 1},
                                          "prompt_tokens": len(ids), "completion_tokens": 1, "cached_tokens": 0,
                                          "hidden_states": [Hs.tolist()]}})
            self._json(200, out[0] if single else out)

    return MockServer(H)


def openai_mock(answers: list[str]) -> MockServer:
    """Chat completions that cycle through ``answers`` (honours ``n``)."""
    state = {"i": 0}

    class H(_Base):
        def do_GET(self):
            if self.path == "/v1/models":
                return self._json(200, {"object": "list", "data": [{"id": "m", "object": "model"}]})
            return super().do_GET()

        def do_POST(self):
            if self.path != "/v1/chat/completions":
                return self._json(404, {"detail": "Not Found"})
            b = self._body()
            n = int(b.get("n") or 1)
            choices = []
            for k in range(n):
                choices.append({"index": k, "finish_reason": "stop",
                                "message": {"role": "assistant", "content": answers[state["i"] % len(answers)]}})
                state["i"] += 1
            self._json(200, {"id": "chatcmpl-1", "object": "chat.completion", "created": int(time.time()),
                             "model": b.get("model"), "choices": choices,
                             "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}})

    return MockServer(H)
