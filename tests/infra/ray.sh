#!/usr/bin/env bash
# Local single-node Ray head for integration tests, using the repo .venv.
#
#   ray.sh up | down | status | smoke
#
# `down` only kills processes belonging to this head (matched by its private
# temp dir), never `ray stop`, which would kill every Ray on the machine.
set -euo pipefail
RSZ_COMPONENT=ray
# shellcheck source=common/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common/common.sh"

VENV="${RSZ_IT_VENV:-${REPO_ROOT}/.venv}"
RAY="${VENV}/bin/ray"
PY="${VENV}/bin/python"
# Unix-socket paths must stay < 108 chars, so the temp dir cannot live under .state/.
RAY_TMP="${RSZ_IT_RAY_TMP:-/tmp/rsz-it-ray}"
ENV_FILE="${STATE_DIR}/ray.env"
# Ray >= 2.5x enables token auth by default and would otherwise share the
# user's ~/.ray/auth_token; give this head its own token.
TOKEN_FILE="${STATE_DIR}/ray-auth-token"
export RAY_AUTH_MODE=token RAY_AUTH_TOKEN_PATH="${TOKEN_FILE}"

load_env() { [[ -f "$ENV_FILE" ]] && source "$ENV_FILE"; return 0; }

head_alive() {
  load_env
  [[ -n "${RSZ_IT_RAY_GCS_PORT:-}" ]] || return 1
  [[ -s "$TOKEN_FILE" ]] || return 1
  curl -sf -m 3 -H "Authorization: Bearer $(cat "$TOKEN_FILE")" \
    "http://127.0.0.1:${RSZ_IT_RAY_DASHBOARD_PORT}/api/version" >/dev/null
}

cmd_up() {
  [[ -x "$RAY" ]] || die "ray not in ${VENV}; run tests/infra/python-env.sh up"
  if head_alive; then ok "Ray head already up at ${RSZ_IT_RAY_ADDRESS}"; return 0; fi
  [[ -f "$ENV_FILE" ]] && { warn "stale ${ENV_FILE}; cleaning up old head"; cmd_down; }
  local gcs dash client agent metrics
  gcs="${RSZ_IT_RAY_GCS_PORT:-$(free_port 26379)}"
  dash="${RSZ_IT_RAY_DASHBOARD_PORT:-$(free_port 28265)}"
  client="${RSZ_IT_RAY_CLIENT_PORT:-$(free_port 20001)}"
  agent="$(free_port 28365)"
  metrics="$(free_port 28080)"
  require_ports_free "$gcs" "$dash" "$client" "$agent" "$metrics"
  mkdir -p "$RAY_TMP"
  [[ -s "$TOKEN_FILE" ]] || { openssl rand -hex 32 >"$TOKEN_FILE"; chmod 600 "$TOKEN_FILE"; }
  log "starting Ray head: gcs=127.0.0.1:${gcs} dashboard=127.0.0.1:${dash} client=${client} temp=${RAY_TMP}"
  # The dashboard is required: the State API (ray.util.state, `ray list tasks`) is served by it.
  RAY_USAGE_STATS_ENABLED=0 "$RAY" start --head \
    --node-ip-address 127.0.0.1 \
    --port "$gcs" \
    --include-dashboard=true --dashboard-host 127.0.0.1 --dashboard-port "$dash" \
    --ray-client-server-port "$client" \
    --dashboard-agent-listen-port "$agent" \
    --metrics-export-port "$metrics" \
    --min-worker-port 21000 --max-worker-port 21999 \
    --num-cpus "${RSZ_IT_RAY_CPUS:-4}" \
    --temp-dir "$RAY_TMP" \
    --disable-usage-stats >"${STATE_DIR}/ray-start.log" 2>&1 \
    || { tail -30 "${STATE_DIR}/ray-start.log" >&2; die "ray start failed"; }
  cat >"$ENV_FILE" <<EOF
RSZ_IT_RAY_GCS_PORT=${gcs}
RSZ_IT_RAY_DASHBOARD_PORT=${dash}
RSZ_IT_RAY_CLIENT_PORT=${client}
RSZ_IT_RAY_METRICS_PORT=${metrics}
RSZ_IT_RAY_ADDRESS=127.0.0.1:${gcs}
export RAY_ADDRESS=127.0.0.1:${gcs}
export RAY_API_SERVER_ADDRESS=http://127.0.0.1:${dash}
export RAY_AUTH_MODE=token
export RAY_AUTH_TOKEN_PATH=${TOKEN_FILE}
EOF
  wait_until 60 "Ray dashboard /api/version" head_alive
  ok "Ray head up: RAY_ADDRESS=127.0.0.1:${gcs}, dashboard/state API http://127.0.0.1:${dash} (env: ${ENV_FILE})"
}

ray_pids() { pgrep -f -- "${RAY_TMP}" || true; }

cmd_down() {
  local pids; pids="$(ray_pids)"
  if [[ -n "$pids" ]]; then
    log "stopping Ray processes under ${RAY_TMP}: $(echo "$pids" | wc -l | tr -d ' ')"
    # shellcheck disable=SC2086
    kill $pids 2>/dev/null || true
    local i; for i in $(seq 1 15); do [[ -z "$(ray_pids)" ]] && break; sleep 1; done
    pids="$(ray_pids)"; [[ -n "$pids" ]] && kill -9 $pids 2>/dev/null || true
  fi
  # Process names that do not carry the temp dir (e.g. idle workers) die with their raylet.
  rm -f "$ENV_FILE" "$TOKEN_FILE"
  rm -rf "$RAY_TMP"
  ok "Ray head down"
}

cmd_status() {
  if head_alive; then
    echo "ray: head UP at ${RSZ_IT_RAY_ADDRESS}, dashboard http://127.0.0.1:${RSZ_IT_RAY_DASHBOARD_PORT} (source ${ENV_FILE})"
  else
    echo "ray: head DOWN"
  fi
}

cmd_smoke() {
  head_alive || die "Ray head is not up; run tests/infra/ray.sh up"
  RAY_ADDRESS="$RSZ_IT_RAY_ADDRESS" RAY_API_SERVER_ADDRESS="http://127.0.0.1:${RSZ_IT_RAY_DASHBOARD_PORT}" \
    "$PY" "${INFRA_DIR}/smoke/ray_smoke.py"
}

case "${1:-}" in
  up) cmd_up ;;
  down) cmd_down ;;
  status) cmd_status ;;
  smoke) cmd_smoke ;;
  *) usage_and_exit "usage: ray.sh up|down|status|smoke" ;;
esac
