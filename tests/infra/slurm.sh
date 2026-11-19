#!/usr/bin/env bash
# Slurm test cluster in Docker: mariadb + slurmdbd + slurmctld + 2 slurmd
# (c1, c2), Lua job_submit + cli_filter, cgroup v2 memory enforcement.
#
#   slurm.sh build | up | down | status | smoke | reconfigure | exec [-u USER] CMD... | clean
#
# Host dirs mounted into every Slurm container:
#   .state/slurm-etc-extra -> /etc/slurm-extra  (job_submit.lua, cli_filter.lua,
#       slurm-extra.conf, any tables; each file is symlinked into /etc/slurm)
#   .state/bin             -> /opt/rsz/bin (ro, on PATH)  e.g. the linux `rsz`
set -euo pipefail
RSZ_COMPONENT=slurm
# shellcheck source=common/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common/common.sh"

SLURM_TAG="${RSZ_IT_SLURM_TAG:-slurm-26-05-5-1}"   # SchedMD git tag (26.05.5)
export RSZ_IT_SLURM_IMAGE="${RSZ_IT_SLURM_IMAGE:-rsz-it/slurm:${SLURM_TAG#slurm-}}"
export RSZ_IT_MARIADB_IMAGE="${RSZ_IT_MARIADB_IMAGE:-mariadb:11.8}"
export RSZ_IT_SLURM_EXTRA="${STATE_DIR}/slurm-etc-extra"
export RSZ_IT_BIN_DIR="${STATE_DIR}/bin"
BUILDER=rsz-it-builder
SLURM_DIR="${INFRA_DIR}/slurm"
CTLD=rsz-it-slurm-slurmctld

dc() { docker compose -f "${SLURM_DIR}/docker-compose.yml" "$@"; }

ensure_builder() {
  docker buildx inspect "$BUILDER" >/dev/null 2>&1 \
    || docker buildx create --name "$BUILDER" --driver docker-container >/dev/null
}

cmd_build() {
  need_docker
  ensure_builder
  log "building ${RSZ_IT_SLURM_IMAGE} from ${SLURM_TAG} with buildx builder ${BUILDER} ($(disk_free))"
  # A dedicated builder keeps the (large) build cache out of the default
  # builder; `slurm.sh clean` removes it.
  docker buildx build --builder "$BUILDER" --load \
    --build-arg "SLURM_TAG=${SLURM_TAG}" \
    -t "${RSZ_IT_SLURM_IMAGE}" "${SLURM_DIR}"
  ok "built ${RSZ_IT_SLURM_IMAGE} ($(docker image inspect -f '{{.Size}}' "${RSZ_IT_SLURM_IMAGE}" | awk '{printf "%.0f MB", $1/1e6}'))"
}

seed_extra() {
  mkdir -p "${RSZ_IT_SLURM_EXTRA}" "${RSZ_IT_BIN_DIR}"
  local f
  for f in job_submit.lua cli_filter.lua; do
    [[ -e "${RSZ_IT_SLURM_EXTRA}/$f" ]] || cp "${SLURM_DIR}/lua/$f" "${RSZ_IT_SLURM_EXTRA}/$f"
  done
  [[ -e "${RSZ_IT_SLURM_EXTRA}/slurm-extra.conf" ]] \
    || echo "# Extra slurm.conf lines (included last). Apply with: tests/infra/slurm.sh reconfigure" \
      >"${RSZ_IT_SLURM_EXTRA}/slurm-extra.conf"
}

nodes_idle() {
  docker exec "$CTLD" sinfo -h -N -o '%N %t' 2>/dev/null | awk '$2 ~ /^idle/ {n++} END {exit !(n == 2)}'
}

cmd_up() {
  need_docker
  docker image inspect "${RSZ_IT_SLURM_IMAGE}" >/dev/null 2>&1 || cmd_build
  seed_extra
  dc up -d
  wait_until 180 "slurmctld responding" docker exec "$CTLD" scontrol ping
  # Nodes may come up DOWN/unknown on first boot; resume them.
  local i
  for i in $(seq 1 60); do
    nodes_idle && break
    docker exec "$CTLD" scontrol update nodename=c[1-2] state=resume >/dev/null 2>&1 || true
    sleep 2
  done
  nodes_idle || { docker exec "$CTLD" sinfo -N -l >&2 || true; die "nodes c1,c2 not idle"; }
  wait_until 60 "slurmdbd registered cluster" sh -c "docker exec $CTLD sacctmgr -n -P list cluster format=cluster | grep -qx rsz-it"
  ok "Slurm up: $(docker exec "$CTLD" sinfo -h -o '%P %a %D %t' | tr '\n' ' ')"
}

cmd_down() {
  need_docker
  dc down -v --remove-orphans
  ok "Slurm down (images kept; 'slurm.sh clean' removes them)"
}

cmd_status() {
  if container_running "$CTLD"; then
    echo "slurm: UP ($(docker exec "$CTLD" sinfo -h -N -o '%N:%t' 2>/dev/null | tr '\n' ' '))"
  else
    echo "slurm: DOWN"
  fi
  docker image inspect "${RSZ_IT_SLURM_IMAGE}" >/dev/null 2>&1 && echo "  image ${RSZ_IT_SLURM_IMAGE} present"
  return 0
}

cmd_reconfigure() {
  local c
  for c in rsz-it-slurm-slurmctld rsz-it-slurm-c1 rsz-it-slurm-c2 rsz-it-slurm-slurmdbd; do
    docker exec "$c" bash -c 'for f in /etc/slurm-extra/*; do ln -sfn "$f" "/etc/slurm/$(basename "$f")"; done'
  done
  docker exec "$CTLD" scontrol reconfigure
  ok "reconfigured"
}

cmd_exec() {
  local user=root
  if [[ "${1:-}" == -u ]]; then user="$2"; shift 2; fi
  docker exec -i -u "$user" -w /data "$CTLD" "$@"
}

cmd_smoke() {
  container_running "$CTLD" || die "Slurm is not up; run tests/infra/slurm.sh up"
  local x
  log "plugins built and configured:"
  docker exec "$CTLD" bash -c 'ls /usr/lib64/slurm | grep -E "^(job_submit_lua|cli_filter_lua|cgroup_v2|accounting_storage_mysql|serializer_json)\.so$"' | sed 's/^/  /' >&2
  docker exec "$CTLD" scontrol show config | grep -iE 'lua|^CliFilter|^JobSubmit|^ProctrackType|^TaskPlugin |^JobAcctGatherType|^CgroupPlugin|ConstrainRAMSpace' | sed 's/^/  /' >&2

  log "1) Lua plugins really run: temporary cli_filter/job_submit that tag the job"
  cp "${RSZ_IT_SLURM_EXTRA}/job_submit.lua" "${STATE_DIR}/job_submit.lua.bak"
  cp "${RSZ_IT_SLURM_EXTRA}/cli_filter.lua" "${STATE_DIR}/cli_filter.lua.bak"
  restore_lua() {
    mv -f "${STATE_DIR}/job_submit.lua.bak" "${RSZ_IT_SLURM_EXTRA}/job_submit.lua" 2>/dev/null || true
    mv -f "${STATE_DIR}/cli_filter.lua.bak" "${RSZ_IT_SLURM_EXTRA}/cli_filter.lua" 2>/dev/null || true
  }
  trap restore_lua EXIT
  cat >"${RSZ_IT_SLURM_EXTRA}/cli_filter.lua" <<'EOF'
function slurm_cli_setup_defaults(options, early_pass) return slurm.SUCCESS end
function slurm_cli_pre_submit(options, pack_offset)
  options["comment"] = "cli_filter-ran"
  return slurm.SUCCESS
end
function slurm_cli_post_submit(offset, job_id, step_id) return slurm.SUCCESS end
EOF
  cat >"${RSZ_IT_SLURM_EXTRA}/job_submit.lua" <<'EOF'
function slurm_job_submit(job_desc, part_list, submit_uid)
  job_desc.admin_comment = "job_submit-ran;comment=" .. tostring(job_desc.comment)
  return slurm.SUCCESS
end
function slurm_job_modify(job_desc, job_rec, part_list, modify_uid) return slurm.SUCCESS end
return slurm.SUCCESS
EOF
  local jid
  jid="$(cmd_exec -u alice sbatch --parsable --mem=100M -o /data/lua-%j.out --wrap 'echo hi')"
  wait_job_done "$jid"
  x="$(docker exec "$CTLD" sacct -j "$jid" --json | jq -r '.jobs[0] | "\(.comment.job) | \(.comment.administrator)"')"
  [[ "$x" == "cli_filter-ran | job_submit-ran;comment=cli_filter-ran" ]] \
    || die "Lua plugins did not tag job $jid as expected: '$x'"
  ok "job $jid comment/admin_comment: $x"
  restore_lua; trap - EXIT

  log "2) a job completes and appears in sacct --json"
  jid="$(cmd_exec -u alice sbatch --parsable --mem=200M -o /data/ok-%j.out --wrap 'python3 -c "b=bytearray(50*2**20); print(len(b))"')"
  wait_job_done "$jid"
  x="$(docker exec "$CTLD" sacct -j "$jid" --json | jq -c '[.jobs[0].state.current, .jobs[0].exit_code.status, (.jobs[0].required.memory_per_node.number // .jobs[0].required.memory_per_node)]')"
  log "  sacct --json [state, exit_code.status, required.memory_per_node]: $x"
  grep -q COMPLETED <<<"$x" || die "job $jid did not COMPLETE: $x"
  ok "job $jid COMPLETED"

  log "3) a job that exceeds --mem is OOM-killed by cgroup v2 (ConstrainRAMSpace=yes)"
  jid="$(cmd_exec -u alice sbatch --parsable --mem=100M -o /data/oom-%j.out --wrap 'python3 -c "
chunks = []
for i in range(40):
    chunks.append(bytearray(10 * 2**20))
    print(i * 10, \"MiB\", flush=True)
"')"
  wait_job_done "$jid"
  x="$(docker exec "$CTLD" sacct -j "$jid" --json | jq -c '[.jobs[0].state.current, .jobs[0].exit_code.status, .jobs[0].exit_code.signal.name, [.jobs[0].steps[] | {step: .step.name, state: .state, maxrss: ([.tres.requested.max[]? | select(.type=="mem") | .count] | first)}]]')"
  log "  sacct --json: $x"
  if grep -q OUT_OF_MEMORY <<<"$x"; then ok "job $jid state OUT_OF_MEMORY (cgroup enforcement works)"; else warn "job $jid not OUT_OF_MEMORY: $x"; return 1; fi
  docker exec "$CTLD" sacct -j "$jid" -o JobID,State,ExitCode,ReqMem,MaxRSS -P | sed 's/^/  /' >&2
}

wait_job_done() {
  local jid="$1"
  wait_until 120 "job $1 to finish" sh -c "docker exec $CTLD squeue -h -j $jid 2>/dev/null | grep -q . && exit 1 || exit 0"
  # accounting is asynchronous: wait until slurmdbd has the end record.
  wait_until 60 "job $1 in sacct" sh -c "docker exec $CTLD sacct -n -X -j $jid -o State | grep -vqE 'PENDING|RUNNING|^\s*$'"
}

cmd_clean() {
  cmd_down
  docker image rm "${RSZ_IT_SLURM_IMAGE}" >/dev/null 2>&1 && log "removed ${RSZ_IT_SLURM_IMAGE}" || true
  docker buildx rm "$BUILDER" >/dev/null 2>&1 && log "removed buildx builder ${BUILDER} and its cache" || true
  ok "clean (left ${RSZ_IT_MARIADB_IMAGE}, which other projects may share)"
}

case "${1:-}" in
  build) cmd_build ;;
  up) cmd_up ;;
  down) cmd_down ;;
  status) cmd_status ;;
  smoke) cmd_smoke ;;
  reconfigure) cmd_reconfigure ;;
  exec) shift; cmd_exec "$@" ;;
  clean) cmd_clean ;;
  *) usage_and_exit "usage: slurm.sh build|up|down|status|smoke|reconfigure|exec [-u USER] CMD...|clean" ;;
esac
