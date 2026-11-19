#!/usr/bin/env bash
# Isolated Flyte v2 devbox for integration tests.
#
#   flyte.sh up | down | status | smoke | clean
#
# Why not `flyte start devbox`: in SDK 2.13 it hard-codes the container and
# volume name `flyte-devbox`, publishes host ports 6443/30000-30003/30080/30081
# (30000 and 30003 are taken by the existing k3d `devbox` cluster on this
# machine), merges its kubeconfig into ~/.kube/config and switches the current
# context to `flyte-devbox`. This script runs the same devbox image the same
# way (see flyte/cli/_devbox.py::_run_container) but with rsz-it names, its own
# kubeconfig and only the ports the SDK needs.
set -euo pipefail
RSZ_COMPONENT=flyte
# shellcheck source=common/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common/common.sh"

NAME=rsz-it-flyte
VOLUME=rsz-it-flyte
IMAGE="${RSZ_IT_FLYTE_IMAGE:-cr.flyte.org/flyteorg/flyte-devbox:latest}"
API_PORT="${RSZ_IT_FLYTE_K8S_API_PORT:-26443}"   # host -> container 6443 (k8s API)
# The devbox bakes these into its config (console/API on 30080, object store
# endpoint http://localhost:30002 for signed URLs), so they map 1:1.
CONSOLE_PORT=30080
STORAGE_PORT=30002
KUBE_DIR="${STATE_DIR}/flyte-kube"
CONFIG_DIR="${STATE_DIR}/flyte-devbox-config"
export KUBECONFIG="${STATE_DIR}/${NAME}.kubeconfig"
FLYTE_CONFIG="${STATE_DIR}/flyte-config.yaml"
VENV="${RSZ_IT_VENV:-${REPO_ROOT}/.venv}"
FLYTE_BIN="${VENV}/bin/flyte"
[[ -x "$FLYTE_BIN" ]] || FLYTE_BIN="$(command -v flyte || true)"

healthy() { curl -sf -m 3 "http://localhost:${CONSOLE_PORT}/healthz" >/dev/null; }

cmd_up() {
  need_docker
  if container_running "$NAME" && healthy; then ok "devbox ${NAME} already up"; write_config; return 0; fi
  if container_exists "$NAME"; then
    assert_ours "$NAME"
    log "removing stale container ${NAME}"
    docker rm -f "$NAME" >/dev/null
  fi
  require_ports_free "$API_PORT" "$CONSOLE_PORT" "$STORAGE_PORT"
  docker image inspect "$IMAGE" >/dev/null 2>&1 || warn "$IMAGE not present; docker will pull it (~1.9 GB). $(disk_free)"
  docker volume inspect "$VOLUME" >/dev/null 2>&1 || docker volume create "$VOLUME" >/dev/null
  mkdir -p "$KUBE_DIR" "$CONFIG_DIR"
  rm -f "$KUBE_DIR/kubeconfig"
  log "starting ${NAME} (${IMAGE}); ports: 127.0.0.1:${CONSOLE_PORT} (API/UI), 127.0.0.1:${STORAGE_PORT} (object store), 127.0.0.1:${API_PORT}->6443"
  docker run --detach --privileged \
    --name "$NAME" \
    --add-host host.docker.internal:host-gateway \
    --env FLYTE_DEV=False \
    --env K3S_KUBECONFIG_OUTPUT=/.kube/kubeconfig \
    --volume "${KUBE_DIR}:/.kube" \
    --volume "${CONFIG_DIR}:/var/lib/flyte/config" \
    --volume "${VOLUME}:/var/lib/flyte/storage" \
    --publish "127.0.0.1:${API_PORT}:6443" \
    --publish "127.0.0.1:${CONSOLE_PORT}:30080" \
    --publish "127.0.0.1:${STORAGE_PORT}:30002" \
    "$IMAGE" >/dev/null
  wait_until 120 "devbox kubeconfig" test -s "$KUBE_DIR/kubeconfig"
  sed -E "s#server: https://[^:]+:6443#server: https://127.0.0.1:${API_PORT}#" "$KUBE_DIR/kubeconfig" >"$KUBECONFIG"
  chmod 600 "$KUBECONFIG"
  log "waiting for Flyte at http://localhost:${CONSOLE_PORT}/healthz (first start takes a few minutes)"
  wait_until 900 "Flyte devbox healthy" healthy
  write_config
  ok "devbox up: FLYTECTL_CONFIG=${FLYTE_CONFIG}  KUBECONFIG=${KUBECONFIG}  UI http://localhost:${CONSOLE_PORT}/v2"
}

write_config() {
  [[ -n "$FLYTE_BIN" ]] || die "no flyte CLI (run tests/infra/python-env.sh up)"
  "$FLYTE_BIN" create config --endpoint "localhost:${CONSOLE_PORT}" --insecure \
    --project flytesnacks --domain development --builder local \
    --output "$FLYTE_CONFIG" --force >/dev/null
}

cmd_down() {
  need_docker
  if container_exists "$NAME"; then docker rm -f "$NAME" >/dev/null; log "removed container ${NAME}"; fi
  rm -f "$KUBECONFIG"
  ok "devbox down (volume ${VOLUME} kept for fast restart; 'flyte.sh clean' removes it)"
}

cmd_clean() {
  cmd_down
  docker volume rm "$VOLUME" >/dev/null 2>&1 && log "removed volume ${VOLUME}" || true
  rm -rf "$KUBE_DIR" "$CONFIG_DIR" "$FLYTE_CONFIG"
  ok "clean"
}

cmd_status() {
  if container_running "$NAME"; then
    if healthy; then echo "flyte: devbox ${NAME} UP (http://localhost:${CONSOLE_PORT}, config ${FLYTE_CONFIG})"
    else echo "flyte: devbox ${NAME} STARTING/UNHEALTHY"; fi
  else
    echo "flyte: devbox ${NAME} DOWN"
  fi
}

cmd_smoke() {
  healthy || die "devbox not healthy; run tests/infra/flyte.sh up"
  [[ -x "${VENV}/bin/python" ]] || die "no ${VENV}; run tests/infra/python-env.sh up"
  KUBECONFIG="$KUBECONFIG" FLYTE_CONFIG="$FLYTE_CONFIG" "${VENV}/bin/python" "${INFRA_DIR}/smoke/flyte_smoke.py" "$@"
}

case "${1:-}" in
  up) cmd_up ;;
  down) cmd_down ;;
  status) cmd_status ;;
  smoke) shift; cmd_smoke "$@" ;;
  clean) cmd_clean ;;
  *) usage_and_exit "usage: flyte.sh up|down|status|smoke|clean" ;;
esac
