#!/usr/bin/env bash
set -euo pipefail

# Build a static musl `cc-lb` binary inside an Alpine `tonistiigi/xx` container
# with host-persisted `target/` and `CARGO_HOME`, eliminating BuildKit cache tar
# shuttles and unsafe `node_modules`/root-UID leaks.

usage() {
  cat <<'USAGE' >&2
Usage: TARGET_PLATFORM=<platform> OUT_BIN=<path> scripts/docker-build-musl.sh

Environment variables:
  TARGET_PLATFORM                      Required, e.g. linux/amd64, linux/arm64, linux/arm/v7, linux/ppc64le
  OUT_BIN                              Required output binary path on host
  FEATURES                             Cargo features for cc-lb-server (default: sqlite,postgres)
  SKIP_SPA                             Set to 1 to skip prebuilt SPA validation/embedding (default: 0)
  GIT_SHA                              Git revision passed to cc-lb-server build.rs
  SOURCE_DATE_EPOCH                    Unix epoch passed to build.rs (unset if empty)
  CARGO_HOME_DIR                       Host Cargo home dir to bind-mount (default: $HOME/.cargo)
  CARGO_TARGET_DIR_HOST                Host Cargo target dir to bind-mount (default: $PWD/target)
  CARGO_PROFILE_RELEASE_OPT_LEVEL      Optional Cargo profile override
  CARGO_PROFILE_RELEASE_LTO            Optional Cargo profile override
  CARGO_PROFILE_RELEASE_CODEGEN_UNITS  Optional Cargo profile override
  CARGO_INCREMENTAL                    Optional Cargo incremental setting
  CARGO_BUILD_JOBS                     Optional Cargo parallel jobs setting
  CARGO_TIMINGS                        Set to 1 to emit Cargo HTML/JSON timings in target/cargo-timings
USAGE
}

if [ -z "${TARGET_PLATFORM:-}" ] || [ -z "${OUT_BIN:-}" ]; then
  usage
  exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
cd "$repo_root"

FEATURES="${FEATURES:-sqlite,postgres}"
SKIP_SPA="${SKIP_SPA:-0}"
CARGO_HOME_DIR="${CARGO_HOME_DIR:-$HOME/.cargo}"
CARGO_TARGET_DIR_HOST="${CARGO_TARGET_DIR_HOST:-$repo_root/target}"
GIT_SHA="${GIT_SHA:-$(git -C "$repo_root" rev-parse HEAD 2>/dev/null || true)}"
SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-}"
CARGO_TIMINGS="${CARGO_TIMINGS:-0}"

# Parse base image versions from Dockerfile so rust-toolchain bumps update once.
rust_version="$(sed -n -E 's/^FROM .* rust:([0-9]+\.[0-9]+\.[0-9]+)-alpine AS builder$/\1/p' "$repo_root/Dockerfile")"
xx_version="$(sed -n -E 's/^FROM .* tonistiigi\/xx:([^[:space:]]+) AS xx$/\1/p' "$repo_root/Dockerfile")"
toolchain_version="$(sed -n -E 's/^[[:space:]]*channel[[:space:]]*=[[:space:]]*"([^"]+)".*/\1/p' "$repo_root/rust-toolchain.toml")"

if [ -z "$rust_version" ] || [ -z "$xx_version" ] || [ -z "$toolchain_version" ]; then
  echo "ERROR: failed to parse Rust ($rust_version), xx ($xx_version), or toolchain ($toolchain_version) version" >&2
  exit 1
fi
if [ "$rust_version" != "$toolchain_version" ]; then
  echo "ERROR: Dockerfile Rust $rust_version does not match rust-toolchain.toml $toolchain_version" >&2
  exit 1
fi

start_ts=$(date +%s)
echo "=== docker-build-musl: platform=${TARGET_PLATFORM} rust=${rust_version} xx=${xx_version} ==="
df -h / "$CARGO_TARGET_DIR_HOST" 2>/dev/null || df -h /

tmp_dir=$(mktemp -d "${RUNNER_TEMP:-/tmp}/docker-build-musl.XXXXXX")
cleanup() {
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

# Build ephemeral local Alpine cross-builder image using host Docker BuildKit.
builder_tag="cc-lb-musl-builder:$(echo "${TARGET_PLATFORM}-${rust_version}-${xx_version}" | tr '/:' '--')"
cat > "$tmp_dir/Dockerfile" <<EOF
# syntax=docker/dockerfile:1
FROM tonistiigi/xx:${xx_version} AS xx
FROM rust:${rust_version}-alpine
RUN apk add --no-cache clang lld git make su-exec
COPY --from=xx / /
ARG TARGETPLATFORM
RUN xx-apk add --no-cache musl-dev gcc && xx-cargo --setup-target-triple
EOF

builder_t0=$(date +%s)
DOCKER_BUILDKIT=1 docker build \
  --build-arg "TARGETPLATFORM=${TARGET_PLATFORM}" \
  -t "$builder_tag" \
  "$tmp_dir"
builder_t1=$(date +%s)
echo "=== timing: builder image ready in $((builder_t1 - builder_t0))s ==="

triple="$(docker run --rm -e "TARGETPLATFORM=${TARGET_PLATFORM}" "$builder_tag" xx-cargo --print-target-triple)"
if [ -z "$triple" ]; then
  echo "ERROR: failed to determine target triple for ${TARGET_PLATFORM}" >&2
  exit 1
fi
echo "Resolved target triple: ${triple}"

target_profile_dir="${CARGO_TARGET_DIR_HOST}/${triple}/release"
mkdir -p "$CARGO_HOME_DIR" "$target_profile_dir"

# Guard against stale rust-embed assets in cached target/ because
# crates/cc-lb-admin/build.rs skips web/dist rerun-if-changed directives when
# CC_LB_ADMIN_PREBUILT_SPA=1.
dist_dir="$repo_root/crates/cc-lb-admin/web/dist"
spa_hash_file="$target_profile_dir/.web-dist-hash"
spa_hash=""
if [ "$SKIP_SPA" != "1" ]; then
  if [ ! -s "$dist_dir/index.html" ] || [ -z "$(find "$dist_dir/assets" -type f -print -quit 2>/dev/null)" ]; then
    echo "ERROR: prebuilt Admin SPA missing or incomplete under $dist_dir" >&2
    exit 1
  fi
  spa_hash=$(
    python3 - "$dist_dir" <<'PY'
import hashlib, os, sys

dist_dir = os.path.abspath(sys.argv[1])
h = hashlib.sha256()
files = []
for root, _, names in os.walk(dist_dir):
    for name in names:
        full = os.path.join(root, name)
        rel = os.path.relpath(full, dist_dir).replace(os.sep, "/")
        files.append((rel, full))
files.sort(key=lambda x: x[0])
for rel, full in files:
    h.update(rel.encode("utf-8"))
    h.update(b"\0")
    with open(full, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    h.update(b"\0")
print(h.hexdigest())
PY
  )
  prev_hash=""
  if [ -f "$spa_hash_file" ]; then
    prev_hash=$(tr -d '[:space:]' < "$spa_hash_file")
  fi
  if [ "$spa_hash" != "$prev_hash" ]; then
    echo "Admin SPA digest changed (${prev_hash:-none} -> ${spa_hash}); invalidating cc-lb-admin cache"
    rm -f "$spa_hash_file"
    rm -rf "${target_profile_dir}/.fingerprint/cc-lb-admin-"* "${target_profile_dir}/deps/libcc_lb_admin-"*
  else
    echo "Admin SPA digest unchanged (${spa_hash})"
  fi
else
  rm -f "$spa_hash_file"
fi

# Strip the host-specific x86_64 musl override in a mounted copy of
# .cargo/config.toml so proc-macros link dynamically without mutating git tree.
cargo_config_src="$repo_root/.cargo/config.toml"
cargo_config_mount="$tmp_dir/cargo-config.toml"
section_count="$(grep -cx '\[target\.x86_64-unknown-linux-musl\]' "$cargo_config_src" || true)"
if [ "$section_count" -gt 1 ]; then
  echo "ERROR: expected at most one x86_64 musl target section in $cargo_config_src" >&2
  exit 1
fi
sed '/^\[target\.x86_64-unknown-linux-musl\]$/,/^$/d' "$cargo_config_src" > "$cargo_config_mount"
if grep -q '^\[target\.x86_64-unknown-linux-musl\]$' "$cargo_config_mount"; then
  echo "ERROR: failed to strip x86_64-unknown-linux-musl section from mounted cargo config" >&2
  exit 1
fi

mkdir -p "$(dirname "$OUT_BIN")"
out_dir="$(cd "$(dirname "$OUT_BIN")" && pwd)"
out_name="$(basename "$OUT_BIN")"

host_uid="$(id -u)"
host_gid="$(id -g)"

env_args=(
  -e "HOME=/tmp"
  -e "HOST_UID=${host_uid}"
  -e "HOST_GID=${host_gid}"
  -e "TARGETPLATFORM=${TARGET_PLATFORM}"
  -e "TARGET_TRIPLE=${triple}"
  -e "FEATURES=${FEATURES}"
  -e "SKIP_SPA=${SKIP_SPA}"
  -e "GIT_SHA=${GIT_SHA}"
  -e "SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH}"
  -e "OUT_NAME=${out_name}"
  -e "CARGO_TIMINGS=${CARGO_TIMINGS}"
  -e "XX_CARGO_NOLOCK=1"
  -e "CARGO_HOME=/cargo-home"
  -e "CARGO_TARGET_DIR=/target"
  -e "CC_LB_SKIP_WASM_FIXTURE_BUILD=1"
)

for var in \
  CARGO_PROFILE_RELEASE_OPT_LEVEL \
  CARGO_PROFILE_RELEASE_LTO \
  CARGO_PROFILE_RELEASE_CODEGEN_UNITS \
  CARGO_INCREMENTAL \
  CARGO_BUILD_JOBS; do
  if [ -n "${!var:-}" ]; then
    env_args+=(-e "${var}=${!var}")
  fi
done

compile_t0=$(date +%s)
docker run --rm \
  "${env_args[@]}" \
  -v "$repo_root:/src" \
  -v "$cargo_config_mount:/src/.cargo/config.toml:ro" \
  -v "$CARGO_HOME_DIR:/cargo-home" \
  -v "$CARGO_TARGET_DIR_HOST:/target" \
  -v "$out_dir:/out" \
  -w /src \
  "$builder_tag" \
  su-exec "${host_uid}:${host_gid}" /bin/ash -euo pipefail -c '
    if [ -z "${SOURCE_DATE_EPOCH:-}" ]; then unset SOURCE_DATE_EPOCH; fi
    if [ "${SKIP_SPA}" = "1" ]; then
      rm -rf /src/crates/cc-lb-admin/web/dist
      export CC_LB_ADMIN_SKIP_SPA=1
    else
      export CC_LB_ADMIN_PREBUILT_SPA=1
    fi
    RUSTC="$(rustup which rustc)"
    export RUSTC
    c_target_var="CFLAGS_$(printf "%s" "$TARGET_TRIPLE" | tr "-" "_")"
    export "${c_target_var}=--target=${TARGET_TRIPLE}"
    git config --global --add safe.directory /src || true

    cargo metadata --no-deps --format-version 1 > /target/.cargo-metadata.json

    cargo_args=(
      build --release --locked
      -p cc-lb-server --bin cc-lb
      --no-default-features --features "${FEATURES}"
      --config '\''target."cfg(all())".rustflags = ["-C", "link-arg=-Wl,--threads=2"]'\''
      --target-dir /target
    )
    if [ "${CARGO_TIMINGS}" = "1" ]; then
      cargo_args+=(--timings)
    fi

    xx-cargo "${cargo_args[@]}"
    install -Dm0755 "/target/${TARGET_TRIPLE}/release/cc-lb" "/out/${OUT_NAME}"
    xx-verify --static "/out/${OUT_NAME}"
  '
compile_t1=$(date +%s)

if [ "$SKIP_SPA" != "1" ]; then
  printf '%s\n' "$spa_hash" > "$spa_hash_file"
fi

end_ts=$(date +%s)
echo "=== timing: compile+verify=$((compile_t1 - compile_t0))s total=$((end_ts - start_ts))s ==="
ls -lh "$out_dir/$out_name"
df -h / "$CARGO_TARGET_DIR_HOST" 2>/dev/null || df -h /
