#!/usr/bin/env python3
"""End-to-end M3 Slurm checks on the Docker cluster (tests/infra/slurm.sh).

Gated by RSZ_IT_SLURM=1 (run through ./run.sh). The linux `rsz` binary must
be in tests/infra/.state/bin/ (tests/infra/build-linux.sh), which the
cluster mounts at /opt/rsz/bin; RSZ_IT_RSZ_NAME selects the file (default
`rsz`).

Layout inside rsz-it-slurm-slurmctld (the "login node"):
  daemon      `rsz serve` as the service user `rightsizer` (uid 991), socket
              /run/rightsizer/rightsizer.sock, config /etc/rightsizer/config.toml,
              key /var/lib/rightsizer/rightsizer.key (0600 rightsizer),
              history file:///var/lib/rightsizer/history
  slurm       cli_filter.lua + job_submit.lua (+ modules) installed by
              `rsz slurm install` into /etc/slurm-extra (linked into /etc/slurm),
              job_submit's key copy /etc/slurm/rightsizer.key (0600 slurm)
  users       alice, bob (account mlteam); tenant slurm://rsz-it/mlteam

Steps: a (recommend), b (enforce + tampering + daemon down), c (key
rotation), d (sbatch --watch ladder), e (sacct import + shadow),
f (throughput), g (install --disable). Prints PASS/FAIL lines with evidence
and a JSON summary at the end.
"""

import base64
import json
import os
import statistics
import subprocess
import sys
import time

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
CTLD = "rsz-it-slurm-slurmctld"
NODES = ["rsz-it-slurm-c1", "rsz-it-slurm-c2"]
RSZ = "/opt/rsz/bin/" + os.environ.get("RSZ_IT_RSZ_NAME", "rsz")
SOCK = "/run/rightsizer/rightsizer.sock"
CONF = "/etc/rightsizer/config.toml"
KEY = "/var/lib/rightsizer/rightsizer.key"
SLURM_KEY = "/etc/slurm/rightsizer.key"
EXTRA = "/etc/slurm-extra"
TENANT = "slurm://rsz-it/mlteam"
THROUGHPUT_N = int(os.environ.get("RSZ_IT_THROUGHPUT_N", "1000"))
ONLY = set(filter(None, os.environ.get("RSZ_IT_STEPS", "a,b,c,d,e,f,g").split(",")))

results = []
evidence = {}


def log(msg):
    print(msg, flush=True)


def check(name, cond, detail):
    results.append({"name": name, "passed": bool(cond), "detail": detail})
    log(f"{'PASS' if cond else 'FAIL'} {name}: {detail}")
    return cond


def dexec(cmd, user="root", inp=None, env=None, check_rc=True, container=CTLD, timeout=600):
    args = ["docker", "exec", "-i", "-u", user, "-w", "/data"]
    for k, v in (env or {}).items():
        args += ["-e", f"{k}={v}"]
    args += [container, "bash", "-c", cmd]
    p = subprocess.run(args, input=inp, capture_output=True, text=True, timeout=timeout)
    if check_rc and p.returncode != 0:
        raise RuntimeError(f"{cmd!r} as {user}: rc={p.returncode}\nstdout: {p.stdout}\nstderr: {p.stderr}")
    return p


def put(path, text, user="root", mode="0644"):
    dexec(f"cat > {path} && chmod {mode} {path}", user=user, inp=text)


def host(cmd, tries=3):
    for i in range(tries):
        p = subprocess.run(["bash", "-c", cmd], cwd=ROOT, capture_output=True, text=True)
        if p.returncode == 0:
            return p.stdout
        time.sleep(1)
    raise RuntimeError(f"{cmd}: rc={p.returncode}: {p.stdout[-300:]} {p.stderr[-500:]}")


def rsz(args, user="rightsizer", check_rc=True, env=None):
    e = {"RIGHTSIZER_CONFIG": CONF}
    e.update(env or {})
    return dexec(f"{RSZ} {args}", user=user, env=e, check_rc=check_rc)


def job(jid):
    out = dexec(f"scontrol show job -o {jid}", check_rc=False).stdout
    f = {}
    for tok in out.split():
        if "=" in tok:
            k, v = tok.split("=", 1)
            f[k] = v
    return f


def sacct_field(jid, field):
    return dexec(f"sacct -n -X -P -j {jid} -o {field}", check_rc=False).stdout.strip()


def wait_done(jid, timeout=180):
    t = time.time()
    while time.time() - t < timeout:
        st = sacct_field(jid, "State")
        if st and not any(s in st for s in ("PENDING", "RUNNING", "COMPLETING", "CONFIGURING")):
            return st
        time.sleep(1)
    return "TIMEOUT-WAITING"


def mem_mib(v):
    # scontrol MinMemoryNode=1G / 512M
    v = v.strip()
    mult = {"K": 1 / 1024, "M": 1, "G": 1024, "T": 1024 * 1024}
    if v and v[-1] in mult:
        return float(v[:-1]) * mult[v[-1]]
    return float(v)


def token_kid(tok):
    raw = base64.urlsafe_b64decode(tok + "=" * (-len(tok) % 4))
    n = raw[1]
    return raw[2:2 + n].decode()


# ── setup ─────────────────────────────────────────────────────────────


CONFIG = """# rsz e2e (tests/integration/slurm/e2e.py)
[policy]
mode = "{mode}"
max_mode = "enforce"
[history]
uri = "file:///var/lib/rightsizer/history"
durable = true
shared = true
cluster = "rsz-it"
cache_ttl = "3s"
[slurm]
cluster = "rsz-it"
token_key_file = "{key}"
ladder_memory_factor = 2.0
ladder_max_steps = 2
prices = {{ default = 1.0 }}
[serve]
socket = "{sock}"
"""


def setup():
    log("== setup")
    dexec("sacctmgr -i add account mlteam >/dev/null 2>&1 || true; "
          "id bob >/dev/null 2>&1 || useradd -m -u 1001 bob; "
          "id rightsizer >/dev/null 2>&1 || useradd -r -m -d /var/lib/rightsizer -u 991 rightsizer; "
          "sacctmgr -i add user alice account=mlteam >/dev/null 2>&1 || true; "
          "sacctmgr -i add user bob account=mlteam >/dev/null 2>&1 || true; "
          "mkdir -p /etc/rightsizer /run/rightsizer /var/lib/rightsizer; chown rightsizer: /run/rightsizer /var/lib/rightsizer; chmod 0755 /run/rightsizer; "
          "rm -f /tmp/rightsizer-cli-filter-*.neg")
    for n in NODES:
        dexec("id bob >/dev/null 2>&1 || useradd -m -u 1001 bob; id rightsizer >/dev/null 2>&1 || useradd -r -m -d /var/lib/rightsizer -u 991 rightsizer", container=n)
    v = dexec(f"{RSZ} --version").stdout.strip()
    log(f"rsz: {v} ({RSZ})")
    write_config("recommend")
    # Keys: the daemon's copy (rightsizer, 0600) and job_submit's (slurm, 0600).
    dexec(f"rm -f {KEY} {SLURM_KEY}")
    out = rsz(f"slurm keygen --out {KEY}").stdout.strip()
    dexec(f"cp {KEY} {SLURM_KEY} && chown slurm:slurm {SLURM_KEY} && chmod 0600 {SLURM_KEY}")
    perms = dexec(f"stat -c '%a %U' {KEY} {SLURM_KEY}").stdout.split("\n")
    check("setup.keygen", perms[0].startswith("600 rightsizer") and perms[1].startswith("600 slurm"), f"{out} | {KEY}: {perms[0]} | {SLURM_KEY}: {perms[1]}")
    install()
    put("/data/seed.py", SEED_PY, mode="0755")


def install(disable=False):
    flag = " --disable" if disable else ""
    out = dexec(f"{RSZ} slurm install --cluster rsz-it --cli-filter {EXTRA}/cli_filter.lua --job-submit {EXTRA}/job_submit.lua "
                f"--rsz {RSZ} --socket {SOCK} --key-file {SLURM_KEY}{flag}").stdout
    host("tests/infra/slurm.sh reconfigure")
    # slurmctld re-reads job_submit.lua when its mtime changes (checked per submission).
    dexec(f"touch {EXTRA}/job_submit.lua")
    return out


def write_config(mode, extra=""):
    put(CONF, CONFIG.format(mode=mode, key=KEY, sock=SOCK) + extra)


def start_daemon(mode, extra=""):
    stop_daemon()
    write_config(mode, extra)
    dexec(f"rm -f /tmp/rightsizer-cli-filter-*.neg; : > /var/lib/rightsizer/serve.log; chown rightsizer: /var/lib/rightsizer/serve.log")
    subprocess.run(["docker", "exec", "-d", "-u", "rightsizer", "-e", f"RIGHTSIZER_CONFIG={CONF}", "-e", "RSZ_LOG=info", CTLD,
                    "bash", "-c", f"exec {RSZ} serve >> /var/lib/rightsizer/serve.log 2>&1"], check=True)
    for _ in range(100):
        if dexec(f"test -S {SOCK}", check_rc=False).returncode == 0:
            break
        time.sleep(0.1)
    st = status()
    return st


def stop_daemon(sig="TERM"):
    # Anchored: an unanchored pattern also matches this very `bash -c` command line.
    wait = f"for i in $(seq 1 50); do pgrep -f '^{RSZ} serve' >/dev/null || break; sleep 0.1; done" if sig in ("TERM", "KILL") else "true"
    dexec(f"pkill -{sig} -f '^{RSZ} serve' || true; {wait}", check_rc=False)


def status(user="alice"):
    req = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "status", "params": {}})
    p = dexec(f"{RSZ} slurm decide-client --socket {SOCK} --timeout-ms 3000", user=user, inp=req, check_rc=False)
    try:
        return json.loads(p.stdout)["result"]
    except Exception:  # noqa: BLE001
        return None


def cli_opts(name, mem="1G"):
    """What slurm.json_cli_options sends for `sbatch -A mlteam /data/<name>.sh` (non-empty keys)."""
    return {"account": "mlteam", "cpus-per-task": "1", "job-name": f"rsz-{name}", "mem": mem}


def decide_client(script_path, options, user="alice", extra=None):
    params = {"kind": "slurm", "native": {"kind": "object", "value": {
        "kind": "cli_filter", "type": "sbatch", "offset": 0, "options": dict(options, argv=[script_path]),
        "script_path": script_path, "cwd": "/data"}}}
    params.update(extra or {})
    req = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "decide", "params": params})
    p = dexec(f"{RSZ} slurm decide-client --socket {SOCK} --timeout-ms 3000", user=user, inp=req, check_rc=False)
    return json.loads(p.stdout) if p.stdout.strip() else {"error": {"message": f"no response (rc {p.returncode})"}}


SEED_PY = r'''
import json, sys, datetime
key, n, peak_mib, req_mib = sys.argv[1], int(sys.argv[2]), float(sys.argv[3]), int(sys.argv[4])
now = datetime.datetime.now(datetime.timezone.utc)
for i in range(n):
    t = (now - datetime.timedelta(hours=10 - i)).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(json.dumps({"v": 1, "key": key, "cluster": "rsz-it", "invocation_id": f"seed-{key}-{i}", "attempt": 1, "started_at": t,
        "requested": {"requests": {"memory": req_mib << 20, "cpu": 1000}}, "status": "succeeded", "status_confidence": "observed",
        "memory_peak": {"value": peak_mib * 2**20, "censor": "exact", "resolution_s": 5.0},
        "cpu_mean": {"value": 0.5, "censor": "exact"}, "duration_s": 60.0, "source": {"kind": "sacct"}, "trust": "verified"}))
'''


def seed(key, n=5, peak_mib=100, req_mib=1024):
    dexec(f"python3 /data/seed.py {key} {n} {peak_mib} {req_mib} > /data/seed-$$.jsonl && {RSZ} history import jsonl /data/seed-$$.jsonl; rm -f /data/seed-$$.jsonl",
          user="rightsizer", env={"RIGHTSIZER_CONFIG": CONF})


def script(name, body, mem="1G", extra=""):
    put(f"/data/{name}.sh", f"#!/bin/bash\n#SBATCH --job-name=rsz-{name}\n#SBATCH --mem={mem}\n#SBATCH -c 1\n{extra}{body}\n")
    return f"/data/{name}.sh"


def sbatch(args, user="alice", env=None):
    t = time.time()
    p = dexec(f"sbatch --parsable {args}", user=user, env=env, check_rc=False)
    dt = time.time() - t
    jid = p.stdout.strip().split(";")[0] if p.returncode == 0 else None
    return jid, p.stderr.strip(), dt


# ── a: recommend ──────────────────────────────────────────────────────


def step_a():
    log("== a: recommend mode")
    st = start_daemon("recommend")
    check("a.daemon", st is not None and st["caller"]["uid"] == 1000, f"status as alice: caller={st and st['caller']} kinds={st and st['kinds']}")
    s = script("a", 'python3 -c "b=bytearray(50*2**20)"; sleep 1')
    jid, err, dt = sbatch(f"-A mlteam -o /data/a-%j.out {s}")
    j = job(jid)
    check("a.unchanged", j.get("MinMemoryNode") == "1G", f"job {jid} MinMemoryNode={j.get('MinMemoryNode')} ({dt * 1000:.0f} ms)")
    check("a.summary_on_stderr", "rightsizer:" in err and "[recommend]" in err, f"stderr: {err}")
    wait_done(jid)
    ac = sacct_field(jid, "AdminComment")
    check("a.admin_comment", ac == "rightsizer::user", f"AdminComment={ac!r} (no token in recommend mode)")
    evidence["a"] = {"job": jid, "stderr": err, "admin_comment": ac}


# ── b: enforce ────────────────────────────────────────────────────────


def step_b():
    log("== b: enforce with S3(b) history evidence")
    seed(f"{TENANT}/rsz-b", n=5, peak_mib=100, req_mib=1024)
    st = start_daemon("enforce")
    check("b.daemon", st is not None, f"enforce daemon up: {st and st.get('kinds')}")
    s = script("b", 'python3 -c "b=bytearray(60*2**20)"; sleep 1')
    jid, err, dt = sbatch(f"-A mlteam -o /data/b-%j.out {s}")
    j = job(jid)
    mem = mem_mib(j.get("MinMemoryNode", "0"))
    check("b.memory_reduced", 0 < mem < 1024, f"job {jid} MinMemoryNode={j.get('MinMemoryNode')} (prior 1G), sbatch {dt * 1000:.0f} ms")
    wait_done(jid)
    ac = sacct_field(jid, "AdminComment")
    did = ac.split(":")[1] if ac.count(":") == 2 else ""
    check("b.verified", ac.startswith("rightsizer:") and ac.endswith(":verified") and len(did) == 26, f"AdminComment={ac}")
    st = sacct_field(jid, "State")
    check("b.completed", st == "COMPLETED", f"state {st}")
    evidence["b"] = {"job": jid, "min_memory_node": j.get("MinMemoryNode"), "admin_comment": ac}

    # Tampering 1: a forged token in the environment (cli_filter steps aside, job_submit rejects it).
    forged = base64.urlsafe_b64encode(b"\x01\x02k1" + b"0" * 26 + b"\x00" * 8 + b"\x11" * 32).decode().rstrip("=")
    jid2, _, _ = sbatch(f"--hold -A mlteam {s}", env={"RIGHTSIZER_TOKEN": forged})
    ac2 = job(jid2).get("AdminComment", "")
    check("b.tamper_forged_env_token", ac2.endswith(":user") and job(jid2).get("MinMemoryNode") == "1G", f"job {jid2}: AdminComment={ac2} MinMemoryNode={job(jid2).get('MinMemoryNode')}")
    # Tampering 2: a real token replayed with other resources.
    r = decide_client(s, cli_opts("b"))
    tok = (r.get("result") or {}).get("token")
    pmem = ((r.get("result") or {}).get("patch") or {}).get("options", {}).get("mem")
    jid3, _, _ = sbatch(f"--hold -A mlteam --mem=900M {s}", env={"RIGHTSIZER_TOKEN": tok or ""})
    ac3 = job(jid3).get("AdminComment", "")
    jid4, _, _ = sbatch(f"--hold -A mlteam --mem={pmem} {s}", env={"RIGHTSIZER_TOKEN": tok or ""})
    ac4 = job(jid4).get("AdminComment", "")
    check("b.tamper_replayed_token_other_resources", tok and ac3.endswith(":user"), f"token for --mem={pmem} replayed with --mem=900M: job {jid3} AdminComment={ac3}")
    check("b.replay_same_resources_control", ac4.endswith(":verified"), f"same token with --mem={pmem}: job {jid4} AdminComment={ac4}")
    # Tampering 3: scontrol update after verification (logged; attribution dropped at import, step e).
    jid5, _, _ = sbatch(f"--hold -A mlteam {s}")
    ac5 = job(jid5).get("AdminComment", "")
    upd = dexec(f"scontrol update jobid={jid5} MinMemoryNode=200", user="alice", check_rc=False)
    logl = dexec(f"grep 'job {jid5}: resources modified' /var/log/slurm/slurmctld.log | tail -1", check_rc=False).stdout.strip()
    check("b.tamper_scontrol_update_logged", ac5.endswith(":verified") and upd.returncode == 0 and "after verification" in logl,
          f"job {jid5} {ac5} -> scontrol update MinMemoryNode=200 rc={upd.returncode}; slurmctld: {logl[-150:]}")
    dexec(f"scontrol release {jid5}", check_rc=False)
    evidence["b_tamper"] = {"scontrol_updated_job": jid5}
    dexec(f"scancel {jid2} {jid3} {jid4}", check_rc=False)

    # Daemon down: frozen (the 1 s deadline, then the negative cache) and stopped.
    dexec("rm -f /tmp/rightsizer-cli-filter-*.neg")
    stop_daemon("STOP")
    j6, _, t6 = sbatch(f"--hold -A mlteam {s}")
    j7, _, t7 = sbatch(f"--hold -A mlteam {s}")
    neg = dexec("ls /tmp/rightsizer-cli-filter-*.neg && cat /tmp/rightsizer-cli-filter-*.neg", check_rc=False).stdout.strip().replace(chr(10), " ")
    m6, m7 = job(j6).get("MinMemoryNode"), job(j7).get("MinMemoryNode")
    check("b.daemon_frozen_unchanged", m6 == "1G" and m7 == "1G", f"jobs {j6},{j7} MinMemoryNode={m6},{m7}; AdminComment={job(j6).get('AdminComment')}")
    check("b.negative_cache", t6 >= 0.9 and t7 < 0.6 and neg != "", f"first sbatch {t6 * 1000:.0f} ms (waits the 1 s deadline), second {t7 * 1000:.0f} ms (skipped: negative cache {neg})")
    stop_daemon("CONT")
    stop_daemon("TERM")
    dexec("rm -f /tmp/rightsizer-cli-filter-*.neg")
    j8, _, t8 = sbatch(f"--hold -A mlteam {s}")
    j9, _, t9 = sbatch(f"--hold -A mlteam {s}")
    check("b.daemon_stopped_fast", job(j8).get("MinMemoryNode") == "1G" and t8 < 0.6 and t9 < 0.6, f"daemon stopped: jobs {j8},{j9} unchanged in {t8 * 1000:.0f} / {t9 * 1000:.0f} ms")
    dexec(f"scancel {j6} {j7} {j8} {j9}", check_rc=False)
    dexec("rm -f /tmp/rightsizer-cli-filter-*.neg")
    evidence["b_down"] = {"frozen_ms": [round(t6 * 1000), round(t7 * 1000)], "stopped_ms": [round(t8 * 1000), round(t9 * 1000)]}


# ── c: key rotation ───────────────────────────────────────────────────


def rotate(step):
    out = rsz(f"slurm rotate-key {step} --key {KEY} --copy-to {SLURM_KEY}", user="root").stdout
    dexec(f"touch {EXTRA}/job_submit.lua")
    time.sleep(1.5)  # the daemon re-reads the key file at most once a second
    return out


def step_c():
    log("== c: key rotation")
    seed(f"{TENANT}/rsz-c", n=5, peak_mib=100, req_mib=1024)
    start_daemon("enforce")
    s = script("c", "sleep 1")

    def token():
        r = decide_client(s, cli_opts("c"))
        res = r.get("result") or {}
        return res.get("token"), (res.get("patch") or {}).get("options", {}).get("mem")

    def replay(tok, mem):
        jid, _, _ = sbatch(f"--hold -A mlteam --mem={mem} {s}", env={"RIGHTSIZER_TOKEN": tok})
        return job(jid).get("AdminComment", ""), jid

    t_old, mem = token()
    k_old = token_kid(t_old)
    jid, _, _ = sbatch(f"--hold -A mlteam {s}")
    check("c.before", job(jid).get("AdminComment", "").endswith(":verified"), f"key {k_old}: cli_filter job {jid} {job(jid).get('AdminComment')}")
    out = rotate("stage")
    t_staged, _ = token()
    jid, _, _ = sbatch(f"--hold -A mlteam {s}")
    check("c.staged", token_kid(t_staged) == k_old and job(jid).get("AdminComment", "").endswith(":verified"),
          f"stage: daemon still signs with {token_kid(t_staged)}; job {jid} {job(jid).get('AdminComment')} | {out.strip().replace(chr(10), '; ')}")
    out = rotate("promote")
    t_new, mem2 = token()
    k_new = token_kid(t_new)
    jid, _, _ = sbatch(f"--hold -A mlteam {s}")
    ac_old, j_old = replay(t_old, mem)
    ac_new, j_new = replay(t_new, mem2)
    check("c.promoted_overlap", k_new != k_old and job(jid).get("AdminComment", "").endswith(":verified") and ac_old.endswith(":verified") and ac_new.endswith(":verified"),
          f"promote: daemon signs with {k_new}; cli_filter job {jid} {job(jid).get('AdminComment')}; old-key token -> job {j_old} {ac_old}; new-key token -> job {j_new} {ac_new}")
    out = rotate("retire")
    ac_old2, j_old2 = replay(t_old, mem)
    ac_new2, j_new2 = replay(t_new, mem2)
    check("c.retired", ac_old2.endswith(":user") and ac_new2.endswith(":verified"), f"retire: old-key token -> job {j_old2} {ac_old2}; new-key token -> job {j_new2} {ac_new2} | {out.strip().replace(chr(10), '; ')}")
    dexec("scancel -u alice --state=PENDING", check_rc=False)
    evidence["c"] = {"old_kid": k_old, "new_kid": k_new}


# ── d: sbatch --watch ladder ──────────────────────────────────────────


def step_d():
    log("== d: rsz slurm sbatch --watch ladder")
    seed(f"{TENANT}/rsz-d", n=5, peak_mib=100, req_mib=1024)
    start_daemon("enforce")
    s = script("d", 'python3 -c "import os,time; b=bytearray(int(os.environ.get(\'ALLOC_MB\',\'200\'))*2**20); b[::4096]=b\'x\'*len(b[::4096]); time.sleep(12)"')
    probe = decide_client(s, cli_opts("d"), extra={"watch": True, "dry_run": True})
    res = probe.get("result") or {}
    d0 = (res.get("decisions") or [{}])[0]
    applied = (d0.get("applied") or {}).get("requests", {}).get("memory", 0) / 2**20
    alloc = int(applied * 1.45)
    log(f"   decided memory {applied:.0f} MiB (watch); the job allocates {alloc} MiB: OOM at step 0, fits at step 1 ({2 * applied:.0f} MiB)")
    t = time.time()
    p = dexec(f"{RSZ} slurm sbatch --watch --poll 2s -A mlteam -o /data/d-%j.out {s}", user="alice",
              env={"RIGHTSIZER_CONFIG": CONF, "ALLOC_MB": str(alloc)}, check_rc=False, timeout=900)
    dt = time.time() - t
    ids = [l.split()[-1] for l in p.stdout.splitlines() if l.startswith("Submitted batch job")] or [x for x in p.stdout.split() if x.isdigit()]
    log("   " + p.stderr.strip().replace("\n", "\n   "))
    states = [(i, sacct_field(i, "State"), sacct_field(i, "ReqMem"), sacct_field(i, "AdminComment")) for i in ids]
    check("d.ladder", p.returncode == 0 and len(ids) == 2 and states[0][1] == "OUT_OF_MEMORY" and states[-1][1] == "COMPLETED",
          f"exit {p.returncode} in {dt:.0f} s; attempts {states}")
    acs = {s_[3] for s_ in states}
    check("d.attempts_signed", len(acs) == 1 and next(iter(acs)).endswith(":verified"), f"both attempts attributed to one decision: {acs}")
    did = next(iter(acs)).split(":")[1] if acs else ""
    rec = dexec(f"grep -rl '{did}' /var/lib/rightsizer/history/v1/attempts 2>/dev/null | head -1 | xargs -r cat", check_rc=False).stdout
    try:
        a = json.loads(rec)
    except Exception:  # noqa: BLE001
        a = {}
    lease = a.get("lease") or {}
    check("d.lease_closed", a.get("invocation_id") == f"rsz-sbatch:{did}" and lease.get("closed_at") and lease.get("outcome") == "succeeded" and a.get("reduced") is True,
          f"attempt record {a.get('invocation_id')} reduced={a.get('reduced')} applied={a.get('applied')} lease={lease}")
    evidence["d"] = {"jobs": ids, "states": states, "decision": did, "attempt_record": a}


# ── e: sacct import + shadow ──────────────────────────────────────────


def step_e():
    log("== e: rsz history import slurm, rsz shadow")
    j5 = (evidence.get("b_tamper") or {}).get("scontrol_updated_job")
    if j5:
        wait_done(j5)
    time.sleep(2)
    p = rsz("-o json history import slurm --since 3h")
    imp = json.loads(p.stdout)
    log(f"   import: jobs={imp['jobs']} records={imp['records']} inserted={imp['inserted']} statuses={imp['by_status']} linked={imp['ladder_attempts_linked']} "
        f"attributions={imp['attributions']} tripped={imp['tripped']} detached={imp['detached_after_modification']}")
    check("e.import", imp["inserted"] > 0, f"{imp['inserted']} records imported from {imp['jobs']} jobs (resolution {imp['resolution_s']} s)")
    d = evidence.get("d") or {}
    if d:
        h = json.loads(rsz(f"-o json history show {TENANT}/rsz-d --limit 20").stdout)
        runs = h if isinstance(h, list) else h.get("runs", h)
        lad = sorted([r for r in runs if r.get("invocation_id") == f"rsz-sbatch:{d['decision']}"], key=lambda r: r["attempt"])
        desc = [(r["attempt"], r["status"], (r.get("memory_peak") or {}).get("censor"), round((r.get("memory_peak") or {}).get("value", 0) / 2**20), (r.get("requested") or {}).get("requests", {}).get("memory", 0) >> 20) for r in lad]
        ok = len(lad) == 2 and lad[0]["status"] == "oom_killed" and lad[0]["memory_peak"]["censor"] == "at_least" and lad[1]["status"] == "succeeded" and lad[1]["memory_peak"]["censor"] == "exact"
        check("e.ladder_records", ok and imp["ladder_attempts_linked"] >= 2, f"(attempt, status, censor, peak MiB, requested MiB): {desc}; no breaker tripped: {imp['tripped'] == []}")
        evidence["e_ladder"] = desc
    if j5:
        check("e.scontrol_update_not_attributed", any(f":{j5} " in x or x.startswith(f"rsz-it:{j5} ") for x in imp["detached_after_modification"]),
              f"detached: {imp['detached_after_modification']}")
    sh = rsz(f"shadow --tenant {TENANT} --since 1d").stdout
    keys = [k for k in ("rsz-a", "rsz-b", "rsz-c", "rsz-d") if k in sh]
    check("e.shadow", len(keys) >= 3, f"rsz shadow --tenant {TENANT} lists {keys}; first lines: {' | '.join(sh.strip().splitlines()[:6])}")
    evidence["e"] = {"import": {k: imp[k] for k in ("jobs", "records", "inserted", "by_status", "ladder_attempts_linked", "attributions", "tripped")}}


# ── f: throughput ─────────────────────────────────────────────────────


RTT_PY = r'''
import json, socket, sys, time
sock, n, script = sys.argv[1], int(sys.argv[2]), sys.argv[3]
body = open(script).read()
req = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "decide", "params": {"kind": "slurm", "native": {"kind": "object", "value": {
    "kind": "cli_filter", "type": "sbatch", "offset": 0, "options": {"account": "mlteam", "cpus-per-task": "1", "job-name": "rsz-f", "mem": "1G", "argv": [script]},
    "script": body, "script_path": script, "cwd": "/data"}}}}).encode() + b"\n"
lat = []
for i in range(n):
    t = time.perf_counter()
    s = socket.socket(socket.AF_UNIX); s.connect(sock); s.sendall(req)
    buf = b""
    while not buf.endswith(b"\n"):
        c = s.recv(65536)
        if not c: break
        buf += c
    s.close()
    lat.append((time.perf_counter() - t) * 1000)
    assert b'"result"' in buf, buf[:300]
lat.sort()
print(json.dumps({"n": n, "p50": lat[n // 2], "p99": lat[int(n * 0.99) - 1], "max": lat[-1], "mean": sum(lat) / n}))
'''


def step_f():
    log(f"== f: throughput ({THROUGHPUT_N} sbatch through cli_filter -> daemon, memoized decision)")
    seed(f"{TENANT}/rsz-f", n=5, peak_mib=100, req_mib=1024)
    start_daemon("enforce")
    s = script("f", "true")
    put("/data/rtt.py", RTT_PY)
    # Warm the memo.
    jid, _, _ = sbatch(f"--hold -A mlteam {s}")
    check("f.enforced", job(jid).get("AdminComment", "").endswith(":verified"), f"warm-up job {jid} MinMemoryNode={job(jid).get('MinMemoryNode')} {job(jid).get('AdminComment')}")
    rtt = json.loads(dexec(f"python3 /data/rtt.py {SOCK} {THROUGHPUT_N} {s}", user="alice").stdout)
    helper = dexec(f"python3 - <<'EOF'\nimport json,subprocess,time\nreq=json.dumps({{'jsonrpc':'2.0','id':1,'method':'decide','params':{{'kind':'slurm','native':{{'kind':'object','value':{{'kind':'cli_filter','type':'sbatch','offset':0,'options':{{'account':'mlteam','cpus-per-task':'1','job-name':'rsz-f','mem':'1G','argv':['{s}']}},'script_path':'{s}','cwd':'/data'}}}}}}}})\nlat=[]\nfor i in range(200):\n    t=time.perf_counter(); p=subprocess.run(['{RSZ}','slurm','decide-client','--socket','{SOCK}','--timeout-ms','1000'],input=req.encode(),capture_output=True); lat.append((time.perf_counter()-t)*1000)\n    assert b'result' in p.stdout\nlat.sort(); print(json.dumps({{'n':200,'p50':lat[100],'p99':lat[197],'max':lat[-1]}}))\nEOF", user="alice")
    helper = json.loads(helper.stdout)
    loop = f"t=$(date +%s.%N); for i in $(seq 1 {THROUGHPUT_N}); do sbatch --hold --parsable -A mlteam {s} >/dev/null 2>&1; done; echo $t $(date +%s.%N)"
    # Fresh daemon statistics: only the sbatch-driven decisions (memo warmed by one job).
    start_daemon("enforce")
    sbatch(f"--hold -A mlteam {s}")
    a, b = map(float, dexec(loop, user="alice", timeout=1800).stdout.split())
    with_rsz = (b - a) / THROUGHPUT_N * 1000
    n_queued = int(dexec("squeue -u alice -h -o '%i' | wc -l").stdout.strip())
    sample = dexec("squeue -u alice -h -o %i | tail -1", check_rc=False).stdout.strip()
    ac = job(sample).get("AdminComment", "")
    st = status()
    daemon_lat = st.get("decide_latency_ms") if st else None
    dexec("scancel -u alice --state=PENDING", check_rc=False)
    # Socket round trip while sbatch submissions load the login node.
    m = max(200, THROUGHPUT_N // 3)
    loaded = dexec(f"(for i in $(seq 1 {m}); do sbatch --hold --parsable -A mlteam {s} >/dev/null 2>&1; done) & "
                   f"python3 /data/rtt.py {SOCK} {m} {s}; wait", user="alice", timeout=1800)
    rtt_loaded = json.loads(loaded.stdout.strip().splitlines()[-1])
    dexec("scancel -u alice --state=PENDING", check_rc=False)
    install(disable=True)
    a2, b2 = map(float, dexec(loop, user="alice", timeout=1800).stdout.split())
    baseline = (b2 - a2) / THROUGHPUT_N * 1000
    dexec("scancel -u alice --state=PENDING", check_rc=False)
    install(disable=False)
    added = with_rsz - baseline
    detail = (f"daemon round trip, idle (socket, new connection per call, n={rtt['n']}): p50 {rtt['p50']:.2f} ms, p99 {rtt['p99']:.2f} ms, max {rtt['max']:.1f} ms; "
              f"under sbatch load (n={rtt_loaded['n']}): p50 {rtt_loaded['p50']:.2f} ms, p99 {rtt_loaded['p99']:.2f} ms, max {rtt_loaded['max']:.1f} ms; "
              f"daemon-side decide for the {THROUGHPUT_N} sbatch calls: {daemon_lat}; "
              f"decide-client process incl. spawn (n=200, idle): p50 {helper['p50']:.1f} ms, p99 {helper['p99']:.1f} ms; "
              f"sbatch with rightsizer {with_rsz:.1f} ms vs disabled {baseline:.1f} ms -> added {added:.1f} ms per sbatch "
              f"({n_queued} jobs queued; last {sample} {ac})")
    check("f.p99_round_trip_under_20ms", rtt["p99"] < 20 and rtt_loaded["p99"] < 20, detail)
    evidence["f"] = {"rtt_idle": rtt, "rtt_under_load": rtt_loaded, "decide_client": helper, "daemon_side": daemon_lat, "sbatch_ms_with": with_rsz, "sbatch_ms_disabled": baseline, "added_ms": added}


# ── g: install --disable ──────────────────────────────────────────────


def step_g():
    log("== g: rsz slurm install --disable")
    start_daemon("enforce")
    s = script("b", 'python3 -c "b=bytearray(60*2**20)"; sleep 1')  # key rsz-b: enforced in b
    out = install(disable=True)
    hdr = dexec(f"grep -h RIGHTSIZER_DISABLED {EXTRA}/cli_filter.lua {EXTRA}/job_submit.lua").stdout.split()
    jid, err, _ = sbatch(f"--hold -A mlteam {s}")
    j = job(jid)
    check("g.disabled", j.get("MinMemoryNode") == "1G" and j.get("AdminComment", "") in ("", "(null)") and "rightsizer" not in err,
          f"job {jid}: MinMemoryNode={j.get('MinMemoryNode')} AdminComment={j.get('AdminComment')!r}; headers {hdr}; {out.strip().splitlines()[-3:]}")
    dexec(f"scancel {jid}", check_rc=False)
    install(disable=False)
    jid, _, _ = sbatch(f"--hold -A mlteam {s}")
    j = job(jid)
    check("g.reenabled", j.get("AdminComment", "").endswith(":verified"), f"after re-install: job {jid} MinMemoryNode={j.get('MinMemoryNode')} {j.get('AdminComment')}")
    dexec(f"scancel {jid}", check_rc=False)


def main():
    if os.environ.get("RSZ_IT_SLURM") != "1":
        print("RSZ_IT_SLURM != 1: skipping the Slurm e2e")
        return 0
    setup()
    steps = [("a", step_a), ("b", step_b), ("c", step_c), ("d", step_d), ("e", step_e), ("f", step_f), ("g", step_g)]
    for name, fn in steps:
        if name not in ONLY:
            continue
        try:
            fn()
        except Exception as e:  # noqa: BLE001
            check(f"{name}.error", False, repr(e)[:2000])
    stop_daemon()
    log(dexec("tail -5 /var/lib/rightsizer/serve.log", check_rc=False).stdout)
    n = sum(r["passed"] for r in results)
    print(json.dumps({"passed": n, "failed": len(results) - n, "results": results, "evidence": evidence}, indent=1, default=str), file=open(os.path.join(ROOT, "tests/infra/.state/slurm-e2e-results.json"), "w"))
    log(f"== {n}/{len(results)} checks passed (details: tests/infra/.state/slurm-e2e-results.json)")
    return 0 if n == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
