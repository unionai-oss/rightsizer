#!/usr/bin/env bash
# Run the Lua unit tests with every available interpreter: host lua/luajit
# and Lua 5.4 in the Slurm test image (tests/infra/slurm.sh build).
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
IMAGE="${RSZ_IT_SLURM_IMAGE:-rsz-it/slurm:26-05-5-1}"
rc=0; ran=0
for lua in luajit lua lua5.1 lua5.2 lua5.3 lua5.4; do
  command -v "$lua" >/dev/null 2>&1 || continue
  for t in test_*.lua; do "$lua" "$t" || rc=1; done; ran=1
done
if command -v docker >/dev/null 2>&1 && docker image inspect "$IMAGE" >/dev/null 2>&1; then
  docker run --rm --entrypoint bash -v "$(cd .. && pwd)":/w -w /w/tests "$IMAGE" \
    -c 'for t in test_*.lua; do lua "$t" || exit 1; done' || rc=1; ran=1
fi
[[ $ran == 1 ]] || { echo "no Lua interpreter found" >&2; exit 2; }
exit $rc
