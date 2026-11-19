#!/bin/bash
# Entrypoint for the rsz-it Slurm containers: mysql-less roles slurmdbd,
# slurmctld, slurmd. Keeps every daemon in the foreground under tini-less bash.
set -euo pipefail
role="${1:-}"

link_extra() {
  # Host-mounted, live-editable files (Lua scripts, tables, slurm-extra.conf).
  local f
  for f in /etc/slurm-extra/*; do
    [[ -e "$f" ]] || continue
    case "$(basename "$f")" in slurm.conf|slurmdbd.conf|cgroup.conf) continue ;; esac
    ln -sfn "$f" "/etc/slurm/$(basename "$f")"
  done
  [[ -e /etc/slurm/slurm-extra.conf ]] || : >/etc/slurm/slurm-extra.conf
}

wait_tcp() { until (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null; do echo "waiting for $1:$2"; sleep 2; done; }

setup_cgroup_v2() {
  # Move every process out of the container's root cgroup so controllers can
  # be delegated to children (cgroup v2 "no internal processes" rule).
  [[ -f /sys/fs/cgroup/cgroup.controllers ]] || { echo "cgroup v2 not mounted"; return 0; }
  mkdir -p /sys/fs/cgroup/init
  local p
  while read -r p; do echo "$p" >/sys/fs/cgroup/init/cgroup.procs 2>/dev/null || true; done </sys/fs/cgroup/cgroup.procs
  local c
  for c in $(cat /sys/fs/cgroup/cgroup.controllers); do
    echo "+$c" >/sys/fs/cgroup/cgroup.subtree_control 2>/dev/null || echo "could not enable cgroup controller $c"
  done
  echo "cgroup v2 subtree_control: $(cat /sys/fs/cgroup/cgroup.subtree_control)"
}

link_extra
runuser -u munge -- /usr/sbin/munged

case "$role" in
  slurmdbd)
    wait_tcp mysql 3306
    exec runuser -u slurm -- /usr/sbin/slurmdbd -D
    ;;
  slurmctld)
    wait_tcp slurmdbd 6819
    exec runuser -u slurm -- /usr/sbin/slurmctld -D -i
    ;;
  slurmd)
    setup_cgroup_v2
    wait_tcp slurmctld 6817
    exec /usr/sbin/slurmd -D -N "$(hostname -s)"
    ;;
  *) exec "$@" ;;
esac
