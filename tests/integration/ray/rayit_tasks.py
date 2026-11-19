"""Task bodies for the Ray integration tests (importable by workers via PYTHONPATH)."""

from __future__ import annotations

import os
import signal
import time


def noop(i):
    return i


def alloc(mb, hold_s=1.2):
    """Allocate ``mb`` MiB (touched) and hold it across a few 0.5 s samples."""
    b = bytearray(mb * 2**20)
    for j in range(0, len(b), 4096):
        b[j] = 1
    time.sleep(hold_s)
    import ray

    return {"bytes": len(b), "assigned": ray.get_runtime_context().get_assigned_resources()}


def raise_memory_error():
    raise MemoryError("simulated allocation failure")


def sigkill_self():
    os.kill(os.getpid(), signal.SIGKILL)


def assigned():
    import ray

    return ray.get_runtime_context().get_assigned_resources()


class Embedder:
    def __init__(self, model="bge"):
        self.model = model

    def resources(self):
        import ray

        return ray.get_runtime_context().get_assigned_resources()


def double(batch):
    batch["a"] = batch["a"] * 2
    return batch
