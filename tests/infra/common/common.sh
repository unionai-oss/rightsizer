# shellcheck shell=bash
# Shared helpers for tests/infra/*.sh. Source, don't execute.
#
# Isolation contract (docs/ARCHITECTURE.md §6): everything these scripts create
# is named rsz-it*, every kubectl/helm call uses an explicit KUBECONFIG under
# .state/, and nothing touches ~/.kube/config or pre-existing containers.

INFRA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "${INFRA_DIR}/../.." && pwd)"
STATE_DIR="${RSZ_IT_STATE_DIR:-${INFRA_DIR}/.state}"
mkdir -p "${STATE_DIR}"

if [[ -t 2 ]]; then
  _c_red=$'\033[31m'; _c_grn=$'\033[32m'; _c_ylw=$'\033[33m'; _c_dim=$'\033[2m'; _c_off=$'\033[0m'
else
  _c_red=""; _c_grn=""; _c_ylw=""; _c_dim=""; _c_off=""
fi

log()  { printf '%s[%s]%s %s\n' "${_c_dim}" "${RSZ_COMPONENT:-infra}" "${_c_off}" "$*" >&2; }
ok()   { printf '%s[%s] OK%s %s\n' "${_c_grn}" "${RSZ_COMPONENT:-infra}" "${_c_off}" "$*" >&2; }
warn() { printf '%s[%s] WARN%s %s\n' "${_c_ylw}" "${RSZ_COMPONENT:-infra}" "${_c_off}" "$*" >&2; }
die()  { printf '%s[%s] ERROR%s %s\n' "${_c_red}" "${RSZ_COMPONENT:-infra}" "${_c_off}" "$*" >&2; exit 1; }

need() {
  local c
  for c in "$@"; do
    command -v "$c" >/dev/null 2>&1 || die "required command not found on PATH: $c"
  done
}

need_docker() {
  need docker
  docker info >/dev/null 2>&1 || die "docker daemon is not reachable (is Docker Desktop running?)"
}

# port_free PORT -> 0 if nothing on the host listens on TCP PORT.
port_free() {
  ! lsof -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1
}

# require_ports_free PORT... -> die listing the busy ones.
require_ports_free() {
  local p busy=()
  for p in "$@"; do
    port_free "$p" || busy+=("$p")
  done
  if ((${#busy[@]})); then
    die "host port(s) already in use: ${busy[*]} (override via the RSZ_IT_* env vars documented in tests/infra/README.md)"
  fi
}

# free_port [START] -> first free TCP port >= START (default 20000).
free_port() {
  local p="${1:-20000}"
  while ! port_free "$p"; do p=$((p + 1)); done
  echo "$p"
}

container_running() { [[ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null || true)" == "true" ]]; }
container_exists()  { docker inspect "$1" >/dev/null 2>&1; }

# Refuse to act on any docker object whose name is not ours.
assert_ours() {
  case "$1" in
    rsz-it*|k3d-rsz-it*) ;;
    *) die "refusing to touch '$1': not an rsz-it* object" ;;
  esac
}

# wait_until TIMEOUT_S DESCRIPTION CMD... -> poll CMD every 2s.
wait_until() {
  local timeout="$1" desc="$2"; shift 2
  local deadline=$((SECONDS + timeout))
  until "$@" >/dev/null 2>&1; do
    ((SECONDS < deadline)) || die "timed out after ${timeout}s waiting for: ${desc}"
    sleep 2
  done
}

disk_free() { df -h "$HOME" | awk 'NR==2 {print $4 " free on " $9}'; }

usage_and_exit() {
  printf '%s\n' "$1" >&2
  exit "${2:-2}"
}
