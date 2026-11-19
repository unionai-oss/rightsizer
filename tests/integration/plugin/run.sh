#!/usr/bin/env bash
# End-to-end: `rsz serve` + the nomad-demo plugin (configured by absolute
# path with a sha256 pin) + a stdlib Python shim calling `decide` over the
# Unix socket and HTTP. Runs on the host (no containers):
#
#   tests/integration/plugin/run.sh          # builds target/debug/rsz unless RSZ is set
#
# Checks:
#   1. handshake (and N/N-1 rejection of an unknown major)
#   2. status shows the caller's peer uid and mapped tenants
#   3. decide (recommend) for a mapped tenant -> decisions, patch none
#   4. decide (enforce) after seeding history -> a JSON Patch from the plugin's render, validated
#   5. a tenant the uid is not mapped to -> an error, not a decision
#   6. HTTP caller without an "http" mapping -> an error; with one -> a decision
#   7. pin mismatch -> plugin refused, decide errors (submission unchanged)
#   8. uid not mapped at all -> an error
#   9. SIGTERM -> graceful exit, socket removed
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
HERE="$ROOT/tests/integration/plugin"
if [[ -z "${RSZ:-}" ]]; then
  (cd "$ROOT" && cargo build -q -p rightsizer-cli)
  RSZ="$ROOT/target/debug/rsz"
fi
DEMO="$ROOT/examples/plugins/rsz-plugin-nomad-demo"
SHA="$(shasum -a 256 "$DEMO" | cut -d' ' -f1)"
D="$(mktemp -d /tmp/rsz-plug-XXXXXX)"   # short: sun_path is 104 bytes on macOS
SOCK="$D/d.sock"
ME="$(id -u)"
PORT="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')"
PID=""
pass=0; fail=0
ok() { echo "PASS $*"; pass=$((pass+1)); }
bad() { echo "FAIL $*"; fail=$((fail+1)); }
cleanup() { [[ -n "$PID" ]] && kill "$PID" 2>/dev/null || true; rm -rf "$D"; }
trap cleanup EXIT
shim() { python3 "$HERE/shim.py" --socket "$SOCK" "$@"; }
jq_() { python3 -c "import json,sys; v=json.load(sys.stdin); print(eval(sys.argv[1], {'v': v}))" "$1"; }

config() { # $1 = sha256, $2 = tenants for my uid (TOML array), $3 = http tenants
  cat >"$D/rightsizer.toml" <<EOF
[policy]
max_mode = "enforce"
[decision]
memo_ttl = "1s"
[history]
uri = "file://$D/history"
durable = true
shared = true
cache_ttl = "1s"
[plugins.nomad-demo]
path = "$DEMO"
sha256 = "$1"
deadline_ms = 1500
[serve]
tenants_by_uid = { "$ME" = $2, "http" = $3 }
EOF
}

start() {
  RIGHTSIZER_CONFIG="$D/rightsizer.toml" RSZ_LOG=warn "$RSZ" serve --socket "$SOCK" --listen "127.0.0.1:$PORT" >"$D/serve.log" 2>&1 &
  PID=$!
  for _ in $(seq 1 100); do [[ -S "$SOCK" ]] && break; sleep 0.05; done
  [[ -S "$SOCK" ]] || { cat "$D/serve.log"; echo "daemon did not start"; exit 1; }
}
stop() { kill -TERM "$PID"; wait "$PID" 2>/dev/null; local rc=$?; PID=""; return $rc; }

echo "== daemon with the demo plugin pinned to sha256 ${SHA:0:12}…, uid $ME -> nomad-demo://ml"
config "$SHA" '["nomad-demo://ml"]' '[]'
start

# 1. handshake
r="$(shim initialize)"
[[ "$(jq_ 'v["result"]["protocol"]' <<<"$r")" == "rightsizer-plugin/1" ]] && ok "handshake: $(jq_ 'v["result"]["kind"], v["result"]["protocol"], v["result"]["methods"]' <<<"$r")" || bad "handshake: $r"
r="$(python3 "$HERE/shim.py" --socket "$SOCK" initialize --protocol rightsizer-plugin/9)"
[[ "$(jq_ 'v["error"]["data"]["code"]' <<<"$r")" == "unsupported" ]] && ok "unknown major rejected: $(jq_ 'v["error"]["message"]' <<<"$r")" || bad "version: $r"

# 2. status
r="$(shim status)"
[[ "$(jq_ 'v["result"]["caller"]["uid"]' <<<"$r")" == "$ME" ]] && ok "status: caller $(jq_ 'v["result"]["caller"]' <<<"$r"); kinds $(jq_ 'v["result"]["kinds"]' <<<"$r")" || bad "status: $r"

# 3. recommend
r="$(shim decide --namespace ml --mode recommend)"
KEY="$(jq_ 'v["result"]["decisions"][0]["key"]' <<<"$r")"
[[ "$(jq_ 'v["result"]["mode"]' <<<"$r")" == "recommend" && "$(jq_ 'v["result"]["patch"]["kind"]' <<<"$r")" == "none" ]] \
  && ok "decide recommend: key $KEY; summary: $(jq_ 'v["result"]["summary"]' <<<"$r")" || bad "decide recommend: $r"

# 4. enforce with history (5 verified successes peaking at 1 GiB under 8 GiB)
python3 - "$KEY" >"$D/seed.jsonl" <<'EOF'
import json, sys, datetime
key = sys.argv[1]
now = datetime.datetime.now(datetime.timezone.utc)
for i in range(5):
    t = (now - datetime.timedelta(hours=6 - i)).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(json.dumps({"v": 1, "key": key, "invocation_id": f"seed-{i}", "attempt": 1, "started_at": t,
        "requested": {"requests": {"memory": 8 << 30, "cpu": 2000}}, "status": "succeeded", "status_confidence": "observed",
        "memory_peak": {"value": float(1 << 30), "censor": "exact"}, "duration_s": 600.0, "source": {"kind": "self_report"}, "trust": "verified"}))
EOF
RIGHTSIZER_CONFIG="$D/rightsizer.toml" "$RSZ" history import jsonl "$D/seed.jsonl" >/dev/null
sleep 1.2
r="$(shim decide --namespace ml --mode enforce)"
if [[ "$(jq_ 'v["result"]["mode"]' <<<"$r")" == "enforce" && "$(jq_ 'v["result"]["patch"]["kind"]' <<<"$r")" == "json_patch" ]]; then
  ok "decide enforce: patch $(jq_ 'v["result"]["patch"]["ops"]' <<<"$r"); summary: $(jq_ 'v["result"]["summary"]' <<<"$r")"
else
  bad "decide enforce: $r"
fi

# 5. a tenant this uid is not mapped to
r="$(shim decide --namespace other --mode enforce)"
[[ "$(jq_ 'v.get("error",{}).get("code")' <<<"$r")" == "-32003" ]] && ok "unmapped tenant -> error: $(jq_ 'v["error"]["message"]' <<<"$r")" || bad "unmapped tenant: $r"

# 6. HTTP (no peer credentials): not mapped
r="$(python3 "$HERE/shim.py" --http "http://127.0.0.1:$PORT/" decide --namespace ml)"
[[ "$(jq_ 'v.get("error",{}).get("code")' <<<"$r")" == "-32003" ]] && ok "http caller without mapping -> error: $(jq_ 'v["error"]["message"]' <<<"$r")" || bad "http: $r"

stop && ok "SIGTERM: daemon exited 0" || bad "SIGTERM: exit code $?"
[[ ! -e "$SOCK" ]] && ok "socket removed on shutdown" || bad "socket left behind"
grep -q 'shutting down' "$D/serve.log" && grep -q 'stopped' "$D/serve.log" && ok "graceful shutdown log: $(grep -E 'shutting down|stopped' "$D/serve.log" | tr '\n' ' ')" || bad "shutdown log: $(cat "$D/serve.log")"

echo "== HTTP mapped to nomad-demo://ml"
config "$SHA" '["nomad-demo://ml"]' '["nomad-demo://ml"]'
start
r="$(python3 "$HERE/shim.py" --http "http://127.0.0.1:$PORT/" decide --namespace ml --mode recommend)"
[[ "$(jq_ 'v["result"]["mode"]' <<<"$r")" == "recommend" ]] && ok "http caller with mapping -> decision $(jq_ 'v["result"]["decisions"][0]["id"]' <<<"$r")" || bad "http mapped: $r"
stop || true

echo "== pin mismatch"
config "$(printf '0%.0s' $(seq 1 64))" '["nomad-demo://ml"]' '[]'
start
r="$(shim decide --namespace ml --mode enforce)"
[[ "$(jq_ 'v.get("error",{}).get("data",{}).get("code")' <<<"$r")" == "platform_unavailable" ]] && ok "pin mismatch -> plugin refused, no decision: $(jq_ 'v["error"]["message"]' <<<"$r")" || bad "pin mismatch: $r"
r="$(shim status)"
ok "status after refusal: refused=$(jq_ 'v["result"].get("refused")' <<<"$r") plugins=$(jq_ 'v["result"]["plugins"]' <<<"$r")"
grep -q 'sha256 mismatch' "$D/serve.log" && ok "daemon log: $(grep -m1 'sha256 mismatch' "$D/serve.log" | sed 's/.*plugin refused/plugin refused/' | cut -c1-160)" || bad "no refusal in log"
stop || true

echo "== uid $ME not mapped to any tenant"
config "$SHA" '[]' '[]'
start
r="$(shim decide --namespace ml --mode enforce)"
[[ "$(jq_ 'v.get("error",{}).get("code")' <<<"$r")" == "-32003" ]] && ok "unmapped uid -> error: $(jq_ 'v["error"]["message"]' <<<"$r")" || bad "unmapped uid: $r"
stop || true

echo "== server mode: a group-writable plugin path is refused (pin matches)"
mkdir -p "$D/plug"
cp "$DEMO" "$D/plug/rsz-plugin-nomad-demo"
chmod 0775 "$D/plug/rsz-plugin-nomad-demo"
DEMO_ORIG="$DEMO"; DEMO="$D/plug/rsz-plugin-nomad-demo"
config "$SHA" '["nomad-demo://ml"]' '[]'
start
r="$(shim decide --namespace ml --mode recommend)"
if [[ "$(jq_ 'v.get("error",{}).get("data",{}).get("code")' <<<"$r")" == "platform_unavailable" ]] && grep -q 'group- or world-writable' "$D/serve.log"; then
  ok "group-writable plugin (0775) refused: $(grep -m1 'writable' "$D/serve.log" | sed 's/.*plugin refused/plugin refused/' | cut -c1-160)"
else
  bad "group-writable plugin: $r | $(tail -3 "$D/serve.log")"
fi
stop || true
chmod 0755 "$D/plug/rsz-plugin-nomad-demo"
start
r="$(shim decide --namespace ml --mode recommend)"
[[ "$(jq_ 'v.get("result",{}).get("mode")' <<<"$r")" == "recommend" ]] && ok "control: the same copy at 0755 is accepted" || bad "control 0755: $r"
stop || true
DEMO="$DEMO_ORIG"

echo "== $pass passed, $fail failed"
[[ $fail == 0 ]]
