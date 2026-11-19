#!/usr/bin/env bash
# Build the workspace's `rsz` binary for linux/arm64 inside a container, with
# the cargo registry, rustup toolchains and target dir in named docker volumes
# (incremental across runs; nothing written to the host except the binary).
#
#   build-linux.sh [bin] [--musl] [--src DIR] [--bin NAME] [--profile P] [-- extra cargo args]
#   build-linux.sh image            # docker image rsz-it/rsz:dev from .state/bin/rsz
#   build-linux.sh status | clean   # show / delete the cache volumes
#
# The toolchain comes from DIR/rust-toolchain.toml (installed by rustup into
# the rsz-it-rustup volume on first use), so the base image's Rust version
# does not matter.
set -euo pipefail
RSZ_COMPONENT=build-linux
# shellcheck source=common/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common/common.sh"

BUILDER_IMAGE="${RSZ_IT_BUILDER_IMAGE:-rust:1.91-bookworm}"
PLATFORM="${RSZ_IT_PLATFORM:-linux/arm64}"
IMAGE_TAG="${RSZ_IT_RSZ_IMAGE:-rsz-it/rsz:dev}"
RUNTIME_BASE="${RSZ_IT_RUNTIME_BASE:-debian:bookworm-slim}"
VOL_REGISTRY=rsz-it-cargo-registry
VOL_GIT=rsz-it-cargo-git
VOL_RUSTUP=rsz-it-rustup
VOL_TARGET="${RSZ_IT_TARGET_VOLUME:-rsz-it-cargo-target}"
VOLUMES=("$VOL_REGISTRY" "$VOL_GIT" "$VOL_RUSTUP" "$VOL_TARGET")
OUT_DIR="${STATE_DIR}/bin"

cmd_bin() {
  local src="${REPO_ROOT}" bin=rsz profile=release musl=0 extra=()
  while (($#)); do
    case "$1" in
      --src) src="$(cd "$2" && pwd)"; shift 2 ;;
      --bin) bin="$2"; shift 2 ;;
      --profile) profile="$2"; shift 2 ;;
      --musl) musl=1; shift ;;
      --) shift; extra=("$@"); break ;;
      *) die "unknown option: $1" ;;
    esac
  done
  need_docker
  [[ -f "$src/Cargo.toml" ]] || die "no Cargo.toml in $src"
  docker image inspect "$BUILDER_IMAGE" >/dev/null 2>&1 \
    || warn "$BUILDER_IMAGE not present locally; pulling (~2 GB). Check: df -h ~ ($(disk_free))"
  local v
  for v in "${VOLUMES[@]}"; do docker volume inspect "$v" >/dev/null 2>&1 || docker volume create "$v" >/dev/null; done
  mkdir -p "$OUT_DIR"

  local triple=aarch64-unknown-linux-gnu
  [[ "$PLATFORM" == linux/amd64 ]] && triple=x86_64-unknown-linux-gnu
  ((musl)) && triple="${triple/-gnu/-musl}"
  local profile_dir="$profile"; [[ "$profile" == dev ]] && profile_dir=debug

  log "building --bin ${bin} (${profile}, ${triple}) from ${src} in ${BUILDER_IMAGE}"
  # Mount the source read-write (cargo may write Cargo.lock) at /src; target dir is a volume.
  docker run --rm --platform "$PLATFORM" \
    --name "rsz-it-build-$$" \
    -v "$src:/src" \
    -v "$OUT_DIR:/out" \
    -v "$VOL_REGISTRY:/usr/local/cargo/registry" \
    -v "$VOL_GIT:/usr/local/cargo/git" \
    -v "$VOL_RUSTUP:/usr/local/rustup" \
    -v "$VOL_TARGET:/target" \
    -e CARGO_TARGET_DIR=/target \
    -e CARGO_TERM_COLOR=always \
    -e MUSL="$musl" -e TRIPLE="$triple" -e BIN="$bin" -e PROFILE="$profile" -e PROFILE_DIR="$profile_dir" \
    -w /src \
    "$BUILDER_IMAGE" \
    bash -euo pipefail -c '
      if [[ -f rust-toolchain.toml || -f rust-toolchain ]]; then
        rustup toolchain install --no-self-update 2>&1 | grep -v "^info: syncing\|unchanged" || true
      fi
      if [[ "$MUSL" == 1 ]]; then
        rustup target add "$TRIPLE" >/dev/null
        command -v musl-gcc >/dev/null || { apt-get update -qq && apt-get install -y -qq musl-tools >/dev/null; }
      fi
      echo "toolchain: $(rustc --version)" >&2
      cargo build --profile "$PROFILE" --bin "$BIN" --target "$TRIPLE" "$@"
      install -m 0755 "/target/$TRIPLE/$PROFILE_DIR/$BIN" "/out/$BIN"
    ' bash "${extra[@]+"${extra[@]}"}"
  ok "built ${OUT_DIR}/${bin} ($(file -b "${OUT_DIR}/${bin}" | cut -d, -f1-2))"
}

cmd_image() {
  need_docker
  local bin="${OUT_DIR}/rsz"
  [[ -x "$bin" ]] || die "$bin missing; run tests/infra/build-linux.sh first"
  log "building ${IMAGE_TAG} (FROM ${RUNTIME_BASE})"
  docker build --platform "$PLATFORM" -t "$IMAGE_TAG" \
    --build-arg "BASE=${RUNTIME_BASE}" \
    -f "${INFRA_DIR}/docker/rsz.Dockerfile" "${OUT_DIR}"
  ok "image ${IMAGE_TAG}; load into k3d with: tests/infra/k3s.sh import ${IMAGE_TAG}"
}

cmd_status() {
  local v
  for v in "${VOLUMES[@]}"; do
    if docker volume inspect "$v" >/dev/null 2>&1; then echo "build-linux: volume $v present"; else echo "build-linux: volume $v absent"; fi
  done
  [[ -x "${OUT_DIR}/rsz" ]] && echo "build-linux: ${OUT_DIR}/rsz ($(file -b "${OUT_DIR}/rsz" | cut -d, -f1-2))"
  docker image inspect "$IMAGE_TAG" >/dev/null 2>&1 && echo "build-linux: image ${IMAGE_TAG} present"
  return 0
}

cmd_clean() {
  local v
  for v in "${VOLUMES[@]}"; do
    assert_ours "$v"
    docker volume rm "$v" >/dev/null 2>&1 && log "removed volume $v" || true
  done
  docker image rm "$IMAGE_TAG" >/dev/null 2>&1 && log "removed image $IMAGE_TAG" || true
  ok "clean"
}

case "${1:-bin}" in
  bin) shift || true; cmd_bin "$@" ;;
  --*) cmd_bin "$@" ;;
  image) cmd_image ;;
  status) cmd_status ;;
  clean) cmd_clean ;;
  *) usage_and_exit "usage: build-linux.sh [bin [--musl] [--src DIR] [--bin NAME] [--profile P] [-- cargo args]] | image | status | clean" ;;
esac
