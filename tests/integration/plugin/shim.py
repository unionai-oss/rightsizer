#!/usr/bin/env python3
"""A minimal rightsizer-plugin/1 shim (Python stdlib only): what a Nomad (or
any other) integration would run to ask the daemon for a decision.

    shim.py --socket PATH initialize
    shim.py --socket PATH status
    shim.py --socket PATH decide --kind nomad-demo --namespace ml [--mode enforce] [--memory-mb 8192]
    shim.py --http http://127.0.0.1:PORT decide ...

Prints the JSON-RPC response (one JSON object) on stdout.
"""

import argparse
import json
import socket
import sys
import urllib.request


def jobspec(namespace, memory_mb, cpu):
    return {
        "kind": "object",
        "value": {"Job": {
            "ID": "train", "Namespace": namespace,
            "TaskGroups": [{"Name": "trainer", "Count": 1,
                            "Tasks": [{"Name": "main", "Driver": "docker",
                                       "Resources": {"CPU": cpu, "MemoryMB": memory_mb}}]}],
        }},
    }


def call_socket(path, msgs, timeout=10.0):
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(timeout)
    s.connect(path)
    f = s.makefile("rwb")
    out = []
    for m in msgs:
        f.write((json.dumps(m) + "\n").encode())
        f.flush()
        line = f.readline()
        if not line:
            raise SystemExit("daemon closed the connection")
        out.append(json.loads(line))
    s.close()
    return out


def call_http(url, msg, timeout=10.0):
    req = urllib.request.Request(url, data=json.dumps(msg).encode(), headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--socket")
    ap.add_argument("--http")
    ap.add_argument("method", choices=["initialize", "status", "decide"])
    ap.add_argument("--kind", default="nomad-demo")
    ap.add_argument("--namespace", default="ml")
    ap.add_argument("--mode")
    ap.add_argument("--memory-mb", type=int, default=8192)
    ap.add_argument("--cpu", type=int, default=2000)
    ap.add_argument("--protocol", default="rightsizer-plugin/1")
    a = ap.parse_args()
    init = {"jsonrpc": "2.0", "id": 0, "method": "initialize",
            "params": {"protocol": a.protocol, "schemas": {"context": "1", "decision": "1", "run": "1"}, "features": "features-v1"}}
    if a.method == "initialize":
        msg = init
    elif a.method == "status":
        msg = {"jsonrpc": "2.0", "id": 1, "method": "status", "params": {}}
    else:
        params = {"kind": a.kind, "native": jobspec(a.namespace, a.memory_mb, a.cpu), "deadline_ms": 2000}
        if a.mode:
            params["mode"] = a.mode
        msg = {"jsonrpc": "2.0", "id": 1, "method": "decide", "params": params}
    if a.http:
        print(json.dumps(call_http(a.http, msg)))
    else:
        # Handshake first on the socket, then the request on the same connection.
        if a.method == "initialize":
            print(json.dumps(call_socket(a.socket, [init])[0]))
        else:
            hs, resp = call_socket(a.socket, [init, msg])
            if "error" in hs:
                print(json.dumps(hs))
                sys.exit(0)
            print(json.dumps(resp))


if __name__ == "__main__":
    main()
