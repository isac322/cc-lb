#!/bin/sh
# Compile the static musl `cc-lb` server binary.
#
# Single source of truth for the compile, shared by two callers:
#   * the Dockerfile `compiled` stage (in-BuildKit build for cd / local), and
#   * the docker-build CI job's `docker run` (compiles on the self-hosted runner
#     with a host-bind sccache cache on the node's local disk).
#
# Cache-agnostic: the caller supplies caches (BuildKit cache mounts, or a
# `docker run -v /opt/sccache:/sccache` host bind). Reads its knobs from the
# environment so both callers behave identically:
#   OUT_DIR (default /out)   staged binary + passwd/group land here
#   FEATURES (default both)  cc-lb-server cargo features
#   SKIP_SPA (0/1)           skip the dashboard SPA build (placeholder page)
#   GIT_SHA, SOURCE_DATE_EPOCH   build metadata passthrough
#   SCCACHE_DIR              when set + sccache present -> local-disk cache
set -eu

: "${OUT_DIR:=/out}"
: "${FEATURES:=sqlite,postgres}"
: "${SKIP_SPA:=0}"

# An empty SOURCE_DATE_EPOCH makes ring's cc/clang C build abort; drop it unless
# a real value was passed.
if [ -z "${SOURCE_DATE_EPOCH:-}" ]; then unset SOURCE_DATE_EPOCH; fi
# cc-lb-runtime-wasmtime/build.rs would otherwise force a wasm32 fixture build.
export CC_LB_SKIP_WASM_FIXTURE_BUILD=1
export GIT_SHA="${GIT_SHA:-}"
if [ "${SKIP_SPA}" = "1" ]; then export CC_LB_ADMIN_SKIP_SPA=1; fi

# Local-disk sccache when a cache dir is provided and sccache is installed. On
# the self-hosted runner SCCACHE_DIR binds the node hostPath /opt/sccache, so the
# compiler cache persists across CI runs. Fail-open: no dir / no sccache binary
# (cd, local) -> compile uncached rather than fail.
if [ -n "${SCCACHE_DIR:-}" ] && command -v sccache >/dev/null 2>&1; then
  mkdir -p "${SCCACHE_DIR}"
  export RUSTC_WRAPPER=sccache CARGO_INCREMENTAL=0
  # Disable the 600s server idle-timeout: the fat-LTO final link runs >10min with
  # no compiler calls and would otherwise reap the server mid-build.
  export SCCACHE_IDLE_TIMEOUT=0
  echo "sccache: local-disk cache at ${SCCACHE_DIR}"
else
  echo "sccache: disabled, compiling uncached"
fi

xx-cargo build --release --locked \
  -p cc-lb-server \
  --no-default-features --features "${FEATURES}" \
  --target-dir /src/target
if [ -n "${RUSTC_WRAPPER:-}" ]; then sccache --show-stats; fi

triple="$(xx-cargo --print-target-triple)"
install -Dm0755 "/src/target/${triple}/release/cc-lb" "${OUT_DIR}/cc-lb"
xx-verify --static "${OUT_DIR}/cc-lb"

# Minimal passwd/group so the scratch image can run as a real nonroot user.
install -d "${OUT_DIR}/etc"
echo 'nonroot:x:65532:65532:nonroot:/home/nonroot:/sbin/nologin' > "${OUT_DIR}/etc/passwd"
echo 'nonroot:x:65532:' > "${OUT_DIR}/etc/group"
