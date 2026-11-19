#!/usr/bin/env bash
# Slurm end-to-end (M3) on the Docker cluster. Opt-in:
#
#   tests/infra/slurm.sh up
#   tests/infra/build-linux.sh -- -p rightsizer-cli     # -> tests/infra/.state/bin/rsz
#   RSZ_IT_SLURM=1 tests/integration/slurm/run.sh       # RSZ_IT_RSZ_NAME=<file in .state/bin> to pick another binary
#   RSZ_IT_STEPS=a,b RSZ_IT_THROUGHPUT_N=200 ...         # a subset / a shorter throughput run
#
# See e2e.py for what each step checks. Results: tests/infra/.state/slurm-e2e-results.json.
set -euo pipefail
if [[ "${RSZ_IT_SLURM:-}" != 1 ]]; then echo "RSZ_IT_SLURM != 1: skipping"; exit 0; fi
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
[[ "$("$ROOT/tests/infra/slurm.sh" status)" == *UP* ]] || { echo "Slurm is not up: tests/infra/slurm.sh up" >&2; exit 2; }
[[ -x "$ROOT/tests/infra/.state/bin/${RSZ_IT_RSZ_NAME:-rsz}" ]] || { echo "no linux rsz: tests/infra/build-linux.sh" >&2; exit 2; }
exec python3 "$ROOT/tests/integration/slurm/e2e.py"
