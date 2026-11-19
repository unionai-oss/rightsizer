#!/usr/bin/env bash
# Memory-monitor OOM test in a Linux container (Ray's memory monitor is Linux-only).
#   tests/integration/ray/linux/run.sh [build|run|clean]
# Image rsz-it/ray-linux:2.60.0 (python:3.12-slim + ray[default]==2.60.0, ~0.5 GB);
# container rsz-it-ray-linux (removed after the run); results in
# tests/infra/.state/ray-it/linux/result.json. Nothing outside rsz-it* is touched.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "${HERE}/../../../.." && pwd)"
IMAGE="${RSZ_IT_RAY_LINUX_IMAGE:-rsz-it/ray-linux:2.60.0}"
OUT="${REPO}/tests/infra/.state/ray-it/linux"
MEM="${RSZ_IT_RAY_LINUX_MEMORY:-3g}"

build() { docker build -t "$IMAGE" "$HERE"; }
run() {
  docker image inspect "$IMAGE" >/dev/null 2>&1 || build
  rm -rf "$OUT"; mkdir -p "$OUT"
  # The repo's pure-Python packages are mounted read-only; the macOS _core is
  # not loadable on Linux, so rightsizer falls back to its S1 path (a stand-in
  # engine in run_oom.py makes the decisions).
  docker run --rm --name rsz-it-ray-linux --memory "$MEM" --shm-size 512m \
    -v "${REPO}/python/rightsizer/src:/src/rightsizer:ro" \
    -v "${REPO}/python/rightsizer-runtime/src:/src/runtime:ro" \
    -v "${HERE}:/work:ro" -v "${OUT}:/out" \
    -e PYTHONPATH=/src/rightsizer:/src/runtime \
    "$IMAGE" python /work/run_oom.py /out
}
clean() { docker image rm "$IMAGE" 2>/dev/null || true; }
case "${1:-run}" in
  build) build ;;
  run) run ;;
  clean) clean ;;
  *) echo "usage: run.sh [build|run|clean]" >&2; exit 2 ;;
esac
