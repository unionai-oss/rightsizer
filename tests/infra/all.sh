#!/usr/bin/env bash
# Summary across every rsz-it integration environment.
#
#   all.sh status    one line per environment
#   all.sh down      bring every environment down (images/caches kept)
set -euo pipefail
RSZ_COMPONENT=all
# shellcheck source=common/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common/common.sh"

cmd_status() {
  "${INFRA_DIR}/k3s.sh" status || true
  "${INFRA_DIR}/flyte.sh" status || true
  "${INFRA_DIR}/slurm.sh" status || true
  "${INFRA_DIR}/ray.sh" status || true
  "${INFRA_DIR}/python-env.sh" status || true
  "${INFRA_DIR}/build-linux.sh" status || true
  echo "host: $(disk_free)"
  local other
  other="$(docker ps --format '{{.Names}}' | grep -E '^(k3d-)?rsz-it' | grep -vE '^(k3d-rsz-it-|rsz-it-flyte$|rsz-it-slurm-)' || true)"
  [[ -n "$other" ]] && echo "other rsz-it containers: $(echo "$other" | tr '\n' ' ')"
  return 0
}

cmd_down() {
  "${INFRA_DIR}/ray.sh" down || true
  "${INFRA_DIR}/slurm.sh" down || true
  "${INFRA_DIR}/flyte.sh" down || true
  "${INFRA_DIR}/k3s.sh" down || true
}

case "${1:-status}" in
  status) cmd_status ;;
  down) cmd_down ;;
  *) usage_and_exit "usage: all.sh status|down" ;;
esac
