#!/usr/bin/env bash
# Build Linux wheels of `rightsizer` (manylinux2014, abi3) and `rightsizer-runtime`
# (py3-none-any) for task images, inside ghcr.io/pyo3/maturin.
#
#   build-wheels.sh [--platform linux/arm64]     # -> tests/infra/.state/wheels/*.whl
#   build-wheels.sh runtime                       # only the pure-Python runtime wheel
#   build-wheels.sh status | clean
#
# The toolchain comes from rust-toolchain.toml, installed by rustup into the
# shared `rsz-it-rustup` volume (the one build-linux.sh uses); the cargo
# registry/git volumes are shared too. The target dir is its own volume
# (`rsz-it-cargo-target-wheel`): manylinux2014's devtoolset gcc builds the C
# dependencies differently from bookworm's, so the two must not share objects.
# The Python sources are copied without any host-built `_core*.so` first, so
# the macOS extension from `maturin develop` never lands in a Linux wheel.
set -euo pipefail
RSZ_COMPONENT=build-wheels
# shellcheck source=common/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common/common.sh"

IMAGE="${RSZ_IT_MATURIN_IMAGE:-ghcr.io/pyo3/maturin:latest}"
PLATFORM="${RSZ_IT_PLATFORM:-linux/arm64}"
VOL_REGISTRY=rsz-it-cargo-registry
VOL_GIT=rsz-it-cargo-git
VOL_RUSTUP=rsz-it-rustup
VOL_TARGET="${RSZ_IT_WHEEL_TARGET_VOLUME:-rsz-it-cargo-target-wheel}"
OUT_DIR="${STATE_DIR}/wheels"
VENV="${RSZ_IT_VENV:-${REPO_ROOT}/.venv}"

build_runtime() {
  mkdir -p "$OUT_DIR"
  rm -f "$OUT_DIR"/rightsizer_runtime-*.whl
  local tmp
  tmp="$(mktemp -d "${STATE_DIR}/rt-build.XXXXXX")"
  # hatchling via uv (no network once cached); fall back to pip wheel.
  if command -v uv >/dev/null 2>&1; then
    (cd "${REPO_ROOT}/python/rightsizer-runtime" && uv build --wheel --out-dir "$tmp" >/dev/null)
  else
    "${VENV}/bin/python" -m pip wheel --no-deps -w "$tmp" "${REPO_ROOT}/python/rightsizer-runtime" >/dev/null
  fi
  mv "$tmp"/rightsizer_runtime-*.whl "$OUT_DIR"/
  rm -rf "$tmp"
  ok "runtime wheel: $(ls "$OUT_DIR"/rightsizer_runtime-*.whl)"
}

build_native() {
  need_docker
  docker image inspect "$IMAGE" >/dev/null 2>&1 || warn "$IMAGE not present; pulling (~0.7 GB). $(disk_free)"
  local v
  for v in "$VOL_REGISTRY" "$VOL_GIT" "$VOL_RUSTUP" "$VOL_TARGET"; do
    docker volume inspect "$v" >/dev/null 2>&1 || docker volume create "$v" >/dev/null
  done
  mkdir -p "$OUT_DIR"
  rm -f "$OUT_DIR"/rightsizer-*.whl
  log "building rightsizer (abi3, manylinux2014, ${PLATFORM}) in ${IMAGE}"
  docker run --rm --platform "$PLATFORM" \
    --name "rsz-it-wheel-$$" \
    --entrypoint bash \
    -v "${REPO_ROOT}:/src" \
    -v "${OUT_DIR}:/out" \
    -v "${VOL_REGISTRY}:/root/.cargo/registry" \
    -v "${VOL_GIT}:/root/.cargo/git" \
    -v "${VOL_RUSTUP}:/usr/local/rustup" \
    -v "${VOL_TARGET}:/target" \
    -e RUSTUP_HOME=/usr/local/rustup \
    -e CARGO_TARGET_DIR=/target \
    -e CARGO_TERM_COLOR=never \
    "$IMAGE" -euo pipefail -c '
      # A copy of the Python package without host build products, next to the
      # crates it points at (manifest-path = ../../crates/rightsizer-py/Cargo.toml).
      mkdir -p /build/python
      cp -r /src/python/rightsizer /build/python/rightsizer
      find /build/python/rightsizer -name "*.so" -delete
      find /build/python/rightsizer -name "__pycache__" -type d -prune -exec rm -rf {} +
      rm -rf /build/python/rightsizer/.pytest_cache /build/python/rightsizer/.ruff_cache
      ln -s /src/crates /build/crates
      ln -s /src/Cargo.toml /build/Cargo.toml
      ln -s /src/Cargo.lock /build/Cargo.lock
      ln -s /src/rust-toolchain.toml /build/rust-toolchain.toml
      [[ -d /src/.cargo ]] && ln -s /src/.cargo /build/.cargo
      [[ -d /src/xtask ]] && ln -s /src/xtask /build/xtask
      cd /build/python/rightsizer
      rustup toolchain install --no-self-update 2>&1 | grep -v "unchanged\|syncing" || true
      echo "toolchain: $(rustc --version)" >&2
      maturin build --release --compatibility manylinux2014 -i python3.12 --out /out
    '
  ok "native wheel: $(ls "$OUT_DIR"/rightsizer-*.whl)"
}

cmd_status() {
  ls -la "$OUT_DIR" 2>/dev/null || echo "build-wheels: no wheels yet"
  docker volume inspect "$VOL_TARGET" >/dev/null 2>&1 && echo "build-wheels: volume $VOL_TARGET present"
  return 0
}

cmd_clean() {
  assert_ours "$VOL_TARGET"
  docker volume rm "$VOL_TARGET" >/dev/null 2>&1 && log "removed volume $VOL_TARGET" || true
  rm -rf "$OUT_DIR"
  ok "clean"
}

case "${1:-all}" in
  all) build_runtime; build_native ;;
  native) build_native ;;
  runtime) build_runtime ;;
  status) cmd_status ;;
  clean) cmd_clean ;;
  *) usage_and_exit "usage: build-wheels.sh [all|native|runtime|status|clean]" ;;
esac
