"""Start a serving framework as a subprocess, wait until it is healthy, tear it down."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request


def free_port(host: str = "127.0.0.1") -> int:
    import socket

    with socket.socket() as s:
        s.bind((host, 0))
        return s.getsockname()[1]


class Launcher:
    """``Launcher(cmd, health_url).start()`` runs ``cmd`` in its own process group
    and blocks until ``GET health_url`` returns 200; ``stop()`` sends SIGTERM to
    the group, then SIGKILL after ``grace`` seconds. A context manager."""

    def __init__(self, cmd: list[str], health_url: str, *, timeout: float = 900.0, env: dict | None = None,
                 log_path: str | None = None, grace: float = 20.0, poll: float = 1.0):
        self.cmd, self.health_url, self.timeout = list(cmd), health_url, timeout
        self.env = {**os.environ, **(env or {})}
        self.log_path, self.grace, self.poll = log_path, grace, poll
        self.proc: subprocess.Popen | None = None
        self._log = None

    def start(self) -> Launcher:
        out = sys.stderr
        if self.log_path:
            self._log = open(self.log_path, "ab")
            out = self._log
        self.proc = subprocess.Popen(self.cmd, env=self.env, stdout=out, stderr=subprocess.STDOUT,
                                     start_new_session=True)
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                code = self.proc.returncode
                self.stop()
                raise RuntimeError(f"{self.cmd[0]} exited with {code} before becoming healthy"
                                   + (f"; see {self.log_path}" if self.log_path else ""))
            if self.healthy():
                return self
            time.sleep(self.poll)
        self.stop()
        raise TimeoutError(f"{self.cmd[0]} not healthy at {self.health_url} after {self.timeout:.0f} s")

    def healthy(self) -> bool:
        try:
            with urllib.request.urlopen(self.health_url, timeout=2.0) as r:
                return r.status == 200
        except (urllib.error.URLError, OSError):
            return False

    def stop(self) -> None:
        p, self.proc = self.proc, None
        if p is not None and p.poll() is None:
            try:
                os.killpg(p.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                p.wait(self.grace)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(p.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                p.wait(5)
        if self._log is not None:
            self._log.close()
            self._log = None

    def __enter__(self) -> Launcher:
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()
