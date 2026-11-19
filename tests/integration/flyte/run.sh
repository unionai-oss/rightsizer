#!/usr/bin/env bash
# Flyte v2 devbox integration test for rightsizer.flyte (flyte.md §13, SPEC §15 M1).
#
#   RSZ_IT_FLYTE=1 tests/integration/flyte/run.sh [scenario ...]
#
# Gated: does nothing unless RSZ_IT_FLYTE=1. Brings up the isolated devbox
# (tests/infra/flyte.sh), builds the Linux wheels (tests/infra/build-wheels.sh)
# and an s3-enabled rsz, runs e2e.py, and leaves the devbox up (flyte.sh down
# tears it down). Results: tests/infra/.state/flyte-it-results.json.
set -euo pipefail
if [[ "${RSZ_IT_FLYTE:-0}" != "1" ]]; then
  echo "skipped: set RSZ_IT_FLYTE=1 to run the Flyte devbox integration test" >&2
  exit 0
fi
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../../.." && pwd)"
STATE="$REPO/tests/infra/.state"
"$REPO/tests/infra/flyte.sh" up
if [[ "${RSZ_IT_SKIP_BUILD:-0}" != "1" ]]; then
  "$REPO/tests/infra/build-wheels.sh" all
  (cd "$REPO" && cargo build -p rightsizer-cli --features s3)
  cp "$REPO/target/debug/rsz" "$STATE/flyte-rsz"
fi
exec "$REPO/.venv/bin/python" "$HERE/e2e.py" "$@"
