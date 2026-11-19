# Helpers for the K8s integration suite (sourced by run.sh).
# Everything runs against the k3d cluster `rsz-it` with an explicit
# KUBECONFIG; nothing touches ~/.kube/config or other clusters.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
IT_DIR="${ROOT}/tests/integration/k8s"
INFRA="${ROOT}/tests/infra"
STATE="${INFRA}/.state/k8s-it"
export KUBECONFIG="${INFRA}/.state/rsz-it.kubeconfig"
RSZ="${RSZ_BIN:-${ROOT}/target/debug/rsz}"
WEBHOOK_PORT="${RSZ_IT_WEBHOOK_PORT:-18443}"
HTTP_PORT="${RSZ_IT_WEBHOOK_HTTP_PORT:-18080}"
PG_PORT="${RSZ_IT_PG_PORT:-15432}"
PG_CONTAINER="rsz-it-k8s-pg"
PG_URI="postgres://rsz:rsz@127.0.0.1:${PG_PORT}/rsz"
RESULTS="${STATE}/results.tsv"
mkdir -p "${STATE}"

kc() { kubectl --kubeconfig "${KUBECONFIG}" "$@"; }
log() { printf '\033[36m[k8s-it]\033[0m %s\n' "$*" >&2; }
pass() { printf 'PASS\t%s\t%s\n' "$1" "${2:-}" >>"${RESULTS}"; printf '\033[32mPASS\033[0m %s %s\n' "$1" "${2:-}" >&2; }
fail() { printf 'FAIL\t%s\t%s\n' "$1" "${2:-}" >>"${RESULTS}"; printf '\033[31mFAIL\033[0m %s %s\n' "$1" "${2:-}" >&2; }
check() { # check NAME EVIDENCE CMD... : PASS when CMD succeeds
  local name="$1" ev="$2"; shift 2
  if "$@"; then pass "$name" "$ev"; else fail "$name" "$ev"; fi
}

rsz() { RIGHTSIZER_CONFIG="${STATE}/rightsizer.toml" RSZ_LOG="${RSZ_LOG:-warn}" "${RSZ}" "$@"; }

wait_for() { # wait_for SECONDS DESC CMD...
  local t="$1" desc="$2"; shift 2
  local end=$((SECONDS + t))
  until "$@" >/dev/null 2>&1; do
    if ((SECONDS > end)); then log "timeout: ${desc}"; return 1; fi
    sleep 1
  done
}

# ---------------------------------------------------------------- setup
setup_pg() {
  if ! docker inspect "${PG_CONTAINER}" >/dev/null 2>&1; then
    docker run -d --name "${PG_CONTAINER}" -p "127.0.0.1:${PG_PORT}:5432" \
      -e POSTGRES_PASSWORD=rsz -e POSTGRES_USER=rsz -e POSTGRES_DB=rsz postgres:16-alpine >/dev/null
  fi
  docker start "${PG_CONTAINER}" >/dev/null
  wait_for 60 "postgres" docker exec "${PG_CONTAINER}" pg_isready -U rsz
}

reset_pg() {
  docker exec "${PG_CONTAINER}" psql -U rsz -d rsz -qc 'DROP SCHEMA IF EXISTS rightsizer CASCADE' >/dev/null 2>&1 || true
}

write_config() { # write_config [extra toml]
  cat >"${STATE}/rightsizer.toml" <<EOF
[history]
uri = "${PG_URI}"
cluster = "rsz-it"

[k8s]
cluster = "rsz-it"
signing_key_file = "${STATE}/signing.key"
webhook_listen = "127.0.0.1:${WEBHOOK_PORT}"
tls_cert = "${STATE}/certs/tls.crt"
tls_key = "${STATE}/certs/tls.key"
prometheus_url = "http://127.0.0.1:19090"
exclude_namespaces = ["kube-system", "kueue-system", "jobset-system", "rsz-it-monitoring", "rsz-it-kuberay"]

[k8s.inputs.allow]
"rsz-it-a" = ["file://${STATE}/inputs/"]

[k8s.adapter.flyte]
default_org = "testorg"
${1:-}
EOF
}

setup_certs_and_keys() {
  [[ -s "${STATE}/certs/tls.crt" ]] || "${RSZ}" webhook certs --dns host.k3d.internal --dns localhost --out "${STATE}/certs" 2>/dev/null
  [[ -s "${STATE}/signing.key" ]] || "${RSZ}" webhook rotate-key --key-file "${STATE}/signing.key" 2>/dev/null
}

register_webhook() { # register_webhook URL CAFILE
  rsz webhook manifests --url "$1" --ca-file "$2" --name rightsizer-it | kc apply -f - >/dev/null
}

start_host_webhook() { # start the webhook on the host (dev loop)
  stop_host_webhook
  RIGHTSIZER_CONFIG="${STATE}/rightsizer.toml" RSZ_LOG="${RSZ_LOG:-info,kube=warn,hyper=warn}" \
    nohup "${RSZ}" webhook serve --http-listen "127.0.0.1:${HTTP_PORT}" --no-leader-election \
      --scrape-interval 15 --peak-delay "${RSZ_IT_PEAK_DELAY:-35}" --view-ttl 2 ${RSZ_IT_WEBHOOK_EXTRA:-} \
      >"${STATE}/webhook.log" 2>&1 &
  echo $! >"${STATE}/webhook.pid"
  wait_for 30 "webhook ready" curl -sf "http://127.0.0.1:${HTTP_PORT}/readyz"
}

stop_host_webhook() {
  if [[ -f "${STATE}/webhook.pid" ]]; then
    kill "$(cat "${STATE}/webhook.pid")" 2>/dev/null || true
    rm -f "${STATE}/webhook.pid"
    sleep 1
  fi
}

metric() { # metric REGEX : sum of matching sample values from the host webhook
  curl -sf "http://127.0.0.1:${HTTP_PORT}/metrics" | awk -v re="$1" '$0 !~ /^#/ && $0 ~ re {s += $NF} END {print s + 0}'
}

ns_enable() { # ns_enable NS MODE
  kc create namespace "$1" --dry-run=client -o yaml | kc apply -f - >/dev/null
  kc label ns "$1" rightsizer.io/enabled=true "rightsizer.io/mode=$2" --overwrite >/dev/null
}

ann() { # ann KIND/NAME NS KEY
  kc -n "$2" get "$1" -o json | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["metadata"].get("annotations",{}).get(sys.argv[1],""))' "$3"
}

jp() { # jp KIND/NAME NS JSONPATH
  kc -n "$2" get "$1" -o "jsonpath=$3"
}

# seed_history KEY N PEAK_MIB [STATUS] : N measured Verified successes of KEY
seed_history() {
  python3 -I "${IT_DIR}/seed.py" "$@" >"${STATE}/seed.jsonl"
  rsz history import jsonl "${STATE}/seed.jsonl" >/dev/null
}

# rec_for KEY INVOCATION : the newest RunRecord of KEY with that invocation id (JSON, or empty)
rec_for() {
  rsz history show "$1" -o json 2>/dev/null | python3 -I -c '
import json,sys
rs=[r for r in json.load(sys.stdin) if r["invocation_id"]==sys.argv[1]]
print(json.dumps(rs[0]) if rs else "")' "$2"
}

# wait_rec SECONDS KEY INVOCATION : wait for rec_for to be non-empty, print it
wait_rec() {
  local end=$((SECONDS + $1)) r=""
  while ((SECONDS < end)); do
    r="$(rec_for "$2" "$3")"
    [[ -n "$r" ]] && { echo "$r"; return 0; }
    sleep 3
  done
  return 1
}
