# syntax=docker/dockerfile:1
#
# Multi-stage, multi-arch, cross-compiling build for cc-lb binaries:
#   * `distroless` (default): monolith `cc-lb` binary (proxy + scheduler + admin/SPA)
#   * `proxy`: split data-plane `cc-lb-proxy` binary (no admin server, no SPA)
#   * `admin`: split control-plane `cc-lb-admin` binary (DB-only admin API + optional SPA)
#   * `smoke`: runs `/usr/local/bin/cc-lb --version` on Alpine in BuildKit
#
# Design:
#   * tonistiigi/xx drives Rust cross-compilation. The compiler always runs on
#     the native BUILDPLATFORM and cross-compiles to TARGETPLATFORM — no QEMU
#     emulation of the toolchain, so `linux/amd64,linux/arm64` builds are fast.
#   * The builder is Alpine (musl-native), so binaries link statically with zero
#     shared libraries and run on distroless-static.
#   * The dashboard SPA is built once on BUILDPLATFORM, then copied only into
#     the stages that embed it (`builder` and `builder-admin`, never `builder-proxy`).
#   * TLS uses rustls + webpki-roots (CA roots compiled into the binary), so the
#     final images need no system CA certificates.
#
# Build (single arch, load locally):
#   docker buildx build --load --build-arg GIT_SHA=$(git rev-parse HEAD) -t cc-lb .
#   docker buildx build --load --target proxy --build-arg GIT_SHA=$(git rev-parse HEAD) -t cc-lb-proxy .
#   docker buildx build --load --target admin --build-arg GIT_SHA=$(git rev-parse HEAD) -t cc-lb-admin .
# Build (multi-arch, push — requires a docker-container builder):
#   docker buildx build --platform linux/amd64,linux/arm64 \
#     --build-arg GIT_SHA=$(git rev-parse HEAD) -t <registry>/cc-lb:<tag> --push .

# ---- xx cross-compilation helper scripts (shared across all target platforms) ----
FROM --platform=$BUILDPLATFORM tonistiigi/xx:1.10.0 AS xx

# ---- Architecture-independent cc-lb-admin dashboard SPA build ----
FROM --platform=$BUILDPLATFORM oven/bun:1.4.4-alpine AS admin-spa
SHELL ["/bin/ash", "-exuo", "pipefail", "-c"]

ARG BUILDOS
ARG BUILDARCH
ARG SKIP_SPA="0"
WORKDIR /src/web

# Resolve dependencies separately so source-only changes retain the frozen
# install layer. Bun's cache contains native packages, so key it by the native
# build platform even though this stage is shared by every target platform.
# SKIP_SPA retains its no-Bun behavior while omitting embed-spa from Rust compilation.
COPY web/package.json web/bun.lock ./
RUN --mount=type=cache,id=bun-${BUILDOS}-${BUILDARCH},target=/root/.bun/install/cache \
    if [ "${SKIP_SPA}" = "1" ]; then \
      mkdir -p dist; \
    else \
      bun install --frozen-lockfile; \
    fi

COPY web/ ./
COPY Cargo.toml /src/Cargo.toml
RUN if [ "${SKIP_SPA}" != "1" ]; then \
      cc_lb_version="$(sed -n '/^\[workspace.package\]$/,/^\[/s/^version = "\([^"]*\)"$/\1/p' /src/Cargo.toml)"; \
      test -n "${cc_lb_version}"; \
      VITE_CC_LB_VERSION="${cc_lb_version}" bun run --shell=bun build; \
    fi

# ---- Shared Rust cross-compilation base (no admin SPA assets) ----
FROM --platform=$BUILDPLATFORM rust:1.99.1-alpine AS builder-base
SHELL ["/bin/ash", "-exuo", "pipefail", "-c"]

# clang/lld: xx uses clang as the cross linker driver for every target
#            architecture.
# git:       derives CC_LB_GIT_SHA fallback when GIT_SHA is empty.
# make:      tikv-jemalloc-sys builds its vendored jemalloc via autotools (runs
#            `make` on BUILDPLATFORM); the rust:alpine image does not ship it.
# sccache: Rust compiler cache when external cache config/creds are passed.
# hadolint ignore=DL3018
RUN apk add --no-cache clang lld git make sccache

# xx scripts (xx-cargo, xx-apk, xx-verify, ...).
# hadolint ignore=DL3067
COPY --from=xx / /

ARG BUILDPLATFORM
ARG TARGETPLATFORM

# Target C toolchain for the crates that compile C for the *target* arch
# (ring, zstd, brotli, libsqlite3-sys).
RUN xx-apk add --no-cache musl-dev gcc

WORKDIR /src

# Keep Cargo's package-cache lock in the same shared mount as its registry and
# git data. /usr/local/cargo/bin remains on PATH; this only relocates CARGO_HOME.
ENV CARGO_HOME=/cargo-home

COPY . .

# The repository's x86_64-musl linker is for standalone Cargo builds. When an
# x86_64 Alpine builder also targets linux/amd64, Cargo applies that target
# linker to host proc-macros and build scripts. Docker builds use xx-cargo for
# every target, so remove the conflicting local override inside this image only.
RUN config=.cargo/config.toml; \
    section_count="$(grep -cx '\[target\.x86_64-unknown-linux-musl\]' "$config" || true)"; \
    if [ "$section_count" -gt 1 ]; then \
      echo "expected at most one x86_64 musl target section in $config" >&2; \
      exit 1; \
    fi; \
    if [ "$section_count" = 1 ]; then \
      sed -i '/^\[target\.x86_64-unknown-linux-musl\]$/,/^$/d' "$config"; \
    fi; \
    ! grep -q '^\[target\.x86_64-unknown-linux-musl\]$' "$config"

# Shared parameterized Cargo + sccache cross-compile driver invoked by the
# per-binary builder stages below.
RUN <<'EOF'
cat <<'SCRIPT' > /usr/local/bin/docker-cargo-build.sh
#!/bin/ash
set -exuo pipefail

target_pkg="$1"
target_bin="$2"
spa_mode="$3"

if [ -n "${GIT_SHA:-}" ]; then
  CC_LB_GIT_SHA="$(printf '%s' "${GIT_SHA}" | cut -c1-7)"
else
  CC_LB_GIT_SHA="$(git rev-parse --short=7 HEAD 2>/dev/null || true)"
fi
if [ -n "${CC_LB_GIT_SHA:-}" ]; then
  export CC_LB_GIT_SHA
else
  unset CC_LB_GIT_SHA
fi
build_time_epoch="${BUILD_TIME_EPOCH:-${SOURCE_DATE_EPOCH:-}}"
if [ -n "${build_time_epoch}" ]; then
  CC_LB_BUILD_TIME="$(date -u -d "@${build_time_epoch}" '+%Y-%m-%dT%H:%M:%SZ')"
else
  CC_LB_BUILD_TIME="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
fi
export CC_LB_BUILD_TIME
# Never leak SOURCE_DATE_EPOCH into xx-cargo/cc 1.10.0 builds.
unset SOURCE_DATE_EPOCH
# cc-lb-runtime-wasmtime/build.rs would otherwise force a wasm32 fixture build.
export CC_LB_SKIP_WASM_FIXTURE_BUILD=1
if [ -z "${CARGO_PROFILE_RELEASE_OPT_LEVEL:-}" ]; then
  unset CARGO_PROFILE_RELEASE_OPT_LEVEL
else
  export CARGO_PROFILE_RELEASE_OPT_LEVEL="${CARGO_PROFILE_RELEASE_OPT_LEVEL}"
fi
if [ -z "${CARGO_PROFILE_RELEASE_LTO:-}" ]; then
  unset CARGO_PROFILE_RELEASE_LTO
else
  export CARGO_PROFILE_RELEASE_LTO="${CARGO_PROFILE_RELEASE_LTO}"
fi
if [ -z "${CARGO_PROFILE_RELEASE_CODEGEN_UNITS:-}" ]; then
  unset CARGO_PROFILE_RELEASE_CODEGEN_UNITS
else
  export CARGO_PROFILE_RELEASE_CODEGEN_UNITS="${CARGO_PROFILE_RELEASE_CODEGEN_UNITS}"
fi
if [ -z "${CARGO_BUILD_JOBS:-}" ]; then
  unset CARGO_BUILD_JOBS
else
  export CARGO_BUILD_JOBS
fi

effective_features="${FEATURES}"
if [ "${spa_mode}" = "spa" ]; then
  if [ "${SKIP_SPA:-0}" = "1" ]; then
    rm -rf /src/web/dist
    export CC_LB_ADMIN_SKIP_SPA=1
  else
    export CC_LB_ADMIN_PREBUILT_SPA=1
    case ",${effective_features}," in
      *,embed-spa,*) ;;
      *) effective_features="${effective_features:+${effective_features},}embed-spa" ;;
    esac
  fi
fi

# Install and verify the target in the same snapshot Cargo uses. Resolve rustc
# to its concrete binary before sccache receives it: the daemon must not
# re-execute rustup's cwd-sensitive proxy outside /src.
target="$(xx-cargo --print-target-triple)"
rustup target add "$target"
sysroot="$(rustc --print sysroot)"
test -n "$(find "$sysroot/lib/rustlib/$target/lib" -maxdepth 1 -type f -name 'libcore-*.rlib' -print -quit)"
RUSTC="$(rustup which rustc)"
export RUSTC
# Re-state the full triple in CFLAGS_<triple> so Alpine clang finds armv7 gcc runtime.
export "CFLAGS_$(echo "$target" | tr '-' '_')=--target=$target"
if [ "${SCCACHE_LOCAL_MOUNT:-0}" = "1" ]; then
  export SCCACHE_DIR=/sccache
  export SCCACHE_CACHE_SIZE="1G"
  export RUSTC_WRAPPER=sccache SCCACHE_IDLE_TIMEOUT=0 CARGO_INCREMENTAL=0
  export SCCACHE_SERVER_UDS=/tmp/sccache.sock
  export SCCACHE_LOG=warn SCCACHE_ERROR_LOG=/tmp/sccache-error.log
  mkdir -p "${SCCACHE_DIR}"
  sccache --start-server
  test -S "${SCCACHE_SERVER_UDS}"
  echo "sccache: local BuildKit mount backend enabled (dir=${SCCACHE_DIR})"
else
  echo "sccache: disabled - compiling uncached"
  if [ "${REQUIRE_SCCACHE:-0}" = "1" ]; then
    echo "sccache is required for this build" >&2
    exit 1
  fi
fi

build_status=0
xx-cargo build --release --locked \
  -p "${target_pkg}" --bin "${target_bin}" \
  --no-default-features --features "${effective_features}" \
  --config 'target."cfg(all())".rustflags = ["-C", "link-arg=-Wl,--threads=2"]' \
  --target-dir /src/target || build_status=$?

cache_status=0
if [ -n "${RUSTC_WRAPPER:-}" ]; then
  if ! sccache --show-stats; then
    cache_status=1
  fi
  if ! timeout 900 sccache --stop-server | tee /tmp/sccache-stats.txt; then
    cache_status=1
  fi
  cache_timeouts="$(awk '$1 == "Cache" && $2 == "timeouts" { print $3; exit }' /tmp/sccache-stats.txt)"
  if [ "${cache_timeouts:-0}" -gt 0 ]; then
    echo "::warning::sccache reported ${cache_timeouts} cache timeouts; affected compilations fell back to local compilation"
  fi
  cache_errors="$(awk '$1 == "Cache" && $2 == "errors" && $3 ~ /^[0-9]+$/ { print $3; exit }' /tmp/sccache-stats.txt)"
  dump_error_log=0
  if [ "${cache_errors:-0}" -gt 8 ]; then
    echo "::warning::sccache reported ${cache_errors} cache errors on ${target}, above the 8 expected autoconf conftest probes"
    dump_error_log=1
  fi
  if grep -Eq '^Cache (read errors|write errors)[[:space:]]+[1-9][0-9]*$' /tmp/sccache-stats.txt; then
    echo "sccache reported a cache storage read or write error" >&2
    cache_status=1
  fi
  unset RUSTC_WRAPPER
  if { [ "$cache_status" -ne 0 ] || [ "$build_status" -ne 0 ] || [ "$dump_error_log" -ne 0 ]; } \
    && [ -s "${SCCACHE_ERROR_LOG}" ]; then
    tail -n 200 "${SCCACHE_ERROR_LOG}" >&2
  fi
fi
if [ "$build_status" -ne 0 ]; then exit "$build_status"; fi
if [ "$cache_status" -ne 0 ]; then exit "$cache_status"; fi
install -Dm0755 "/src/target/${target}/release/${target_bin}" "/out/${target_bin}"
xx-verify --static "/out/${target_bin}"
SCRIPT
chmod 0755 /usr/local/bin/docker-cargo-build.sh
EOF

# ---- Builder: monolith `cc-lb` server (preserves `builder` target for CI) ----
FROM builder-base AS builder
SHELL ["/bin/ash", "-exuo", "pipefail", "-c"]

ARG GIT_SHA=""
ARG BUILD_TIME_EPOCH=""
ARG SOURCE_DATE_EPOCH=""
ARG CARGO_BUILD_JOBS
ARG FEATURES="sqlite,postgres"
ARG SKIP_SPA="0"
ARG SCCACHE_LOCAL_MOUNT="0"
ARG REQUIRE_SCCACHE="0"
ARG CARGO_PROFILE_RELEASE_OPT_LEVEL=""
ARG CARGO_PROFILE_RELEASE_LTO=""
ARG CARGO_PROFILE_RELEASE_CODEGEN_UNITS=""

COPY --link --from=admin-spa /src/web/dist /src/web/dist
RUN --mount=type=cache,id=cargo-home,sharing=shared,target=/cargo-home \
    --mount=type=cache,id=sccache,sharing=shared,target=/sccache \
    /usr/local/bin/docker-cargo-build.sh cc-lb-server cc-lb spa

# ---- Builder: split `cc-lb-proxy` data-plane binary (never receives SPA assets) ----
FROM builder-base AS builder-proxy
SHELL ["/bin/ash", "-exuo", "pipefail", "-c"]

ARG GIT_SHA=""
ARG BUILD_TIME_EPOCH=""
ARG SOURCE_DATE_EPOCH=""
ARG CARGO_BUILD_JOBS
ARG FEATURES="sqlite,postgres"
ARG SCCACHE_LOCAL_MOUNT="0"
ARG REQUIRE_SCCACHE="0"
ARG CARGO_PROFILE_RELEASE_OPT_LEVEL=""
ARG CARGO_PROFILE_RELEASE_LTO=""
ARG CARGO_PROFILE_RELEASE_CODEGEN_UNITS=""

RUN --mount=type=cache,id=cargo-home,sharing=shared,target=/cargo-home \
    --mount=type=cache,id=sccache,sharing=shared,target=/sccache \
    /usr/local/bin/docker-cargo-build.sh cc-lb-proxy cc-lb-proxy no-spa

# ---- Builder: split `cc-lb-admin` control-plane binary ----
FROM builder-base AS builder-admin
SHELL ["/bin/ash", "-exuo", "pipefail", "-c"]

ARG GIT_SHA=""
ARG BUILD_TIME_EPOCH=""
ARG SOURCE_DATE_EPOCH=""
ARG CARGO_BUILD_JOBS
ARG FEATURES="sqlite,postgres"
ARG SKIP_SPA="0"
ARG SCCACHE_LOCAL_MOUNT="0"
ARG REQUIRE_SCCACHE="0"
ARG CARGO_PROFILE_RELEASE_OPT_LEVEL=""
ARG CARGO_PROFILE_RELEASE_LTO=""
ARG CARGO_PROFILE_RELEASE_CODEGEN_UNITS=""

COPY --link --from=admin-spa /src/web/dist /src/web/dist
RUN --mount=type=cache,id=cargo-home,sharing=shared,target=/cargo-home \
    --mount=type=cache,id=sccache,sharing=shared,target=/sccache \
    /usr/local/bin/docker-cargo-build.sh cc-lb-admin-server cc-lb-admin spa

# ---- CI smoke: run the static binary on BuildKit without a local Docker daemon ----
FROM alpine:3.24 AS smoke
ARG EXPECTED_VERSION=
COPY --link --from=builder /out/cc-lb /usr/local/bin/cc-lb
SHELL ["/bin/ash", "-o", "pipefail", "-c"]
RUN /usr/local/bin/cc-lb --version && \
    if [ -n "$EXPECTED_VERSION" ]; then \
      /usr/local/bin/cc-lb --version | grep -Fq "$EXPECTED_VERSION"; \
    fi

# ---- Seed nonroot-owned runtime data directory (for fresh Docker volumes) ----
FROM --platform=$BUILDPLATFORM alpine:3.24 AS runtime-dirs
RUN mkdir -p /rootfs/var/lib/cc-lb && \
    chown 65532:65532 /rootfs/var/lib/cc-lb && \
    chmod 0700 /rootfs/var/lib/cc-lb

# ---- Shared distroless nonroot runtime image ----
FROM gcr.io/distroless/static-debian13:nonroot@sha256:25ac8f0c3b2334a9eb05452266bd4697864a8c8ef0ef71e5e6480d7f92868329 AS distroless-runtime
COPY --link --from=runtime-dirs /rootfs/var/lib /var/lib
USER 65532:65532

# ---- Split runtime target: proxy/worker data plane ----
FROM distroless-runtime AS proxy
# cc-lb binds listeners to 127.0.0.1 by default; pass CC_LB_LISTENER__PROXY_ADDR=0.0.0.0:8080 (or configure listener.proxy_addr) when publishing ports.
EXPOSE 8080 9091
COPY --link --from=builder-proxy /out/cc-lb-proxy /usr/local/bin/cc-lb-proxy
ENTRYPOINT ["/usr/local/bin/cc-lb-proxy"]
CMD ["serve", "--config", "/etc/cc-lb/cc-lb.toml"]

# ---- Split runtime target: admin control plane ----
FROM distroless-runtime AS admin
# cc-lb binds listeners to 127.0.0.1 by default; pass CC_LB_LISTENER__ADMIN_ADDR=0.0.0.0:9090 (or configure listener.admin_addr) when publishing ports.
EXPOSE 9090 9091
COPY --link --from=builder-admin /out/cc-lb-admin /usr/local/bin/cc-lb-admin
ENTRYPOINT ["/usr/local/bin/cc-lb-admin"]
CMD ["serve", "--config", "/etc/cc-lb/cc-lb.toml"]

# ---- Final base: distroless monolith runtime metadata ----
FROM distroless-runtime AS distroless-base
# cc-lb binds listeners to 127.0.0.1 by default; set CC_LB_LISTENER__PROXY_ADDR / CC_LB_LISTENER__ADMIN_ADDR (or listener config) when publishing container ports.
EXPOSE 8080 9090 9091
ENTRYPOINT ["/usr/local/bin/cc-lb"]
CMD ["serve", "--config", "/etc/cc-lb/cc-lb.toml"]

# ---- Prebuilt static binary packaging (used by hosted CI/CD workflows) ----
FROM distroless-base AS distroless-prebuilt
COPY --link cc-lb /usr/local/bin/cc-lb

# ---- Final: distroless static monolith (DEFAULT target) ----
FROM distroless-base AS distroless
COPY --link --from=builder /out/cc-lb /usr/local/bin/cc-lb
