# syntax=docker/dockerfile:1
#
# Multi-stage, multi-arch, cross-compiling build for the `cc-lb` server binary.
#
# Design:
#   * tonistiigi/xx drives Rust cross-compilation. The compiler always runs on
#     the native BUILDPLATFORM and cross-compiles to TARGETPLATFORM — no QEMU
#     emulation of the toolchain, so `linux/amd64,linux/arm64` builds are fast.
#   * The builder is Alpine (musl-native), so `cc-lb` links a fully static musl
#     binary that runs on `scratch`/distroless-static with zero shared libs.
#   * The dashboard SPA is built once on BUILDPLATFORM, then copied into every
#     Rust target build for validation and embedding by `cc-lb-admin/build.rs`.
#   * TLS uses rustls + webpki-roots (CA roots compiled into the binary), so the
#     final image needs no system CA certificates.
#
# Build (single arch, load locally):
#   docker buildx build --load --build-arg GIT_SHA=$(git rev-parse HEAD) -t cc-lb .
# Build (multi-arch, push — requires a docker-container builder):
#   docker buildx build --platform linux/amd64,linux/arm64 \
#     --build-arg GIT_SHA=$(git rev-parse HEAD) -t <registry>/cc-lb:<tag> --push .

# ---- xx cross-compilation helper scripts (shared across all target platforms) ----
FROM --platform=$BUILDPLATFORM tonistiigi/xx:1.9.0 AS xx

# ---- Architecture-independent cc-lb-admin dashboard SPA build ----
FROM --platform=$BUILDPLATFORM oven/bun:1.4.2-alpine AS admin-spa
SHELL ["/bin/ash", "-exuo", "pipefail", "-c"]

ARG BUILDOS
ARG BUILDARCH
ARG SKIP_SPA="0"
WORKDIR /src/crates/cc-lb-admin/web

# Resolve dependencies separately so source-only changes retain the frozen
# install layer. Bun's cache contains native packages, so key it by the native
# build platform even though this stage is shared by every target platform.
# SKIP_SPA retains its no-Bun behavior while leaving an empty dist handoff for
# the Rust build script to replace with the established placeholder.
COPY crates/cc-lb-admin/web/package.json crates/cc-lb-admin/web/bun.lock ./
RUN --mount=type=cache,id=bun-${BUILDOS}-${BUILDARCH},target=/root/.bun/install/cache \
    if [ "${SKIP_SPA}" = "1" ]; then \
      mkdir -p dist; \
    else \
      bun install --frozen-lockfile; \
    fi

COPY crates/cc-lb-admin/web/ ./
COPY Cargo.toml /src/Cargo.toml
RUN if [ "${SKIP_SPA}" != "1" ]; then \
      cc_lb_version="$(sed -n '/^\[workspace.package\]$/,/^\[/s/^version = "\([^"]*\)"$/\1/p' /src/Cargo.toml)"; \
      test -n "${cc_lb_version}"; \
      VITE_CC_LB_VERSION="${cc_lb_version}" bun run --shell=bun build; \
    fi

# ---- Builder: cross toolchain, source, and the compile ----
FROM --platform=$BUILDPLATFORM rust:1.98.1-alpine AS builder
SHELL ["/bin/ash", "-exuo", "pipefail", "-c"]

# clang/lld: xx uses clang as the cross linker driver for every target
#            architecture.
# git:       cc-lb-server/build.rs reads `git rev-parse` (falls back gracefully).
# make:      tikv-jemalloc-sys builds its vendored jemalloc via autotools (runs
#            `make` on BUILDPLATFORM); the rust:alpine image does not ship it.
# sccache: Rust compiler cache; uses the S3/garage backend when creds are passed.
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

# The cross target is installed alongside the compile below. That gives Cargo
# and sccache the same immutable Rust toolchain snapshot.
WORKDIR /src

# Keep Cargo's package-cache lock in the same shared mount as its registry and
# git data. /usr/local/cargo/bin remains on PATH; this only relocates CARGO_HOME.
ENV CARGO_HOME=/cargo-home

# Build-time metadata / knobs.
ARG GIT_SHA=""
ARG SOURCE_DATE_EPOCH=""
# The default ships BOTH storage backends in one binary; the backend is chosen at
# runtime via `[storage] kind` ("sqlite" or "postgres"). Override to slim, e.g.
# `--build-arg FEATURES=sqlite`.
# Optional Cargo parallelism override; unset lets Cargo size itself to the builder.
ARG CARGO_BUILD_JOBS
ARG FEATURES="sqlite,postgres"
# Set to 1 to embed the existing placeholder page instead of the built dashboard.
ARG SKIP_SPA="0"
# sccache S3 backend. Self-hosted CI and release builds require it so cache
# failures are visible; local builds retain an explicit uncached fallback.
ARG SCCACHE_BUCKET=""
ARG SCCACHE_ENDPOINT=""
ARG SCCACHE_REGION=""
ARG SCCACHE_S3_USE_SSL=""
ARG REQUIRE_SCCACHE="0"
ARG SCCACHE_S3_KEY_PREFIX=""
ARG CARGO_PROFILE_RELEASE_LTO=""

COPY . .

# The BUILDPLATFORM SPA is a target-independent input. BuildKit evaluates its
# stage once, then reuses this dist tree across the multi-platform Rust fan-out.
COPY --link --from=admin-spa /src/crates/cc-lb-admin/web/dist /src/crates/cc-lb-admin/web/dist

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

# Cargo's package-cache lock lives at $CARGO_HOME/.package-cache. Mounting the
# whole CARGO_HOME lets Cargo serialize only registry/git mutation while the
# four target builds compile concurrently. This cache contains target-neutral
# package sources; target-specific compiler output remains in sccache.
# The target dir is intentionally NOT cache-mounted: the prebuilt SPA is copied
# into the fresh source tree before each compile, avoiding stale build-script
# fingerprints that could omit its embedded assets.
RUN --mount=type=cache,id=cargo-home,sharing=shared,target=/cargo-home \
    --mount=type=secret,id=AWS_ACCESS_KEY_ID,required=false \
    --mount=type=secret,id=AWS_SECRET_ACCESS_KEY,required=false <<'EOF'
# An empty SOURCE_DATE_EPOCH makes ring's cc/clang C build abort; drop it unless
# a real value was passed.
if [ -z "${SOURCE_DATE_EPOCH:-}" ]; then unset SOURCE_DATE_EPOCH; fi
# cc-lb-runtime-wasmtime/build.rs would otherwise force a wasm32 fixture build.
export CC_LB_SKIP_WASM_FIXTURE_BUILD=1
export GIT_SHA="${GIT_SHA}"
if [ -z "${CARGO_PROFILE_RELEASE_LTO:-}" ]; then
  unset CARGO_PROFILE_RELEASE_LTO
else
  export CARGO_PROFILE_RELEASE_LTO="${CARGO_PROFILE_RELEASE_LTO}"
fi
if [ -z "${CARGO_BUILD_JOBS:-}" ]; then
  unset CARGO_BUILD_JOBS
else
  export CARGO_BUILD_JOBS
fi
if [ "${SKIP_SPA}" = "1" ]; then
  rm -rf /src/crates/cc-lb-admin/web/dist
  export CC_LB_ADMIN_SKIP_SPA=1
else
  export CC_LB_ADMIN_PREBUILT_SPA=1
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
# The `cc` crate canonicalises the Rust triple before putting it in CFLAGS:
# armv7-unknown-linux-musleabihf becomes --target=arm-unknown-linux-musleabihf.
# That later flag overrides the one in xx's clang .cfg, and Alpine's clang 22
# then looks for the gcc runtime under the arm-* triple and misses the armv7
# sysroot copy, so anything that *links* during a build script dies with
# "cannot open crtbeginS.o" / "unable to find library -lgcc". tikv-jemalloc-sys
# hits this because its autotools configure link-tests the compiler. cc appends
# CFLAGS_<triple> after its own flags, so re-stating the real triple there wins.
# No-op on targets whose Rust and LLVM triples already agree.
export "CFLAGS_$(echo "$target" | tr '-' '_')=--target=$target"
# Read + export the creds with the shell -x trace OFF so it never prints them.
set +x
AWS_ACCESS_KEY_ID="$(cat /run/secrets/AWS_ACCESS_KEY_ID 2>/dev/null || true)"
AWS_SECRET_ACCESS_KEY="$(cat /run/secrets/AWS_SECRET_ACCESS_KEY 2>/dev/null || true)"
if [ -n "${AWS_ACCESS_KEY_ID}" ] && [ -n "${AWS_SECRET_ACCESS_KEY}" ] && [ -n "${SCCACHE_BUCKET}" ]; then
  export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
  export SCCACHE_BUCKET SCCACHE_ENDPOINT SCCACHE_REGION
  if [ -n "${SCCACHE_S3_USE_SSL}" ]; then export SCCACHE_S3_USE_SSL; fi
  cache_prefix="${SCCACHE_S3_KEY_PREFIX:-cc-lb}"
  export SCCACHE_S3_KEY_PREFIX="${cache_prefix%/}/${target}"
  export RUSTC_WRAPPER=sccache SCCACHE_IDLE_TIMEOUT=0 CARGO_INCREMENTAL=0
  # Docker's host network shares 127.0.0.1 across concurrent target containers.
  # A filesystem socket is isolated by each container's mount namespace, keeping
  # its sccache server bound to the matching Rust sysroot and installed target.
  export SCCACHE_SERVER_UDS=/tmp/sccache.sock
  export SCCACHE_LOG=warn SCCACHE_ERROR_LOG=/tmp/sccache-error.log
  sccache --start-server
  test -S "${SCCACHE_SERVER_UDS}"
  set -x
  echo "sccache: S3 backend enabled (bucket=${SCCACHE_BUCKET})"
else
  set -x
  echo "sccache: disabled (no creds/bucket) - compiling uncached"
  if [ "${REQUIRE_SCCACHE}" = "1" ]; then
    echo "sccache is required for this build" >&2
    exit 1
  fi
fi
build_status=0
xx-cargo build --release --locked \
  -p cc-lb-server \
  --no-default-features --features "${FEATURES}" \
  --config 'target."cfg(all())".rustflags = ["-C", "link-arg=-Wl,--threads=2"]' \
  --target-dir /src/target || build_status=$?
 # Finish all asynchronous S3 writes before the container exits. Bound the flush
 # so a cold-cache backlog fails this build instead of consuming the CI cap.
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
  # Generic "Cache errors" is deliberately NOT gated. It counts compile requests
  # whose hash-key preprocessing exited non-zero (sccache server.rs returns
  # CompileResult::Error only from the generate_hash_key ProcessError arm), and
  # every occurrence here is an autoconf feature probe: instrumented runs
  # 35638161308 and 35641747730 captured exactly 8 per target on all four
  # targets, every one of them compiling `conftest.c` with jemalloc's
  # `-Werror -herror_on_warning` configure flags. Those probes are supposed to
  # fail. They are counted in neither hits nor misses, which is why
  # "Cache hits rate (C/C++)" stays at 100%. Gating them would fail every build.
  # Storage faults are a different counter and stay fail-closed below. See #565.
  cache_errors="$(awk '$1 == "Cache" && $2 == "errors" && $3 ~ /^[0-9]+$/ { print $3; exit }' /tmp/sccache-stats.txt)"
  dump_error_log=0
  if [ "${cache_errors:-0}" -gt 8 ]; then
    echo "::warning::sccache reported ${cache_errors} cache errors on ${target}, above the 8 expected autoconf conftest probes"
    # The individual entries log at debug only, so the dump below shows them
    # only after SCCACHE_LOG above is raised to warn,sccache::server=debug.
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
triple="$target"
install -Dm0755 "/src/target/${triple}/release/cc-lb" /out/cc-lb
xx-verify --static /out/cc-lb
EOF

# ---- CI smoke: run the static binary on BuildKit without a local Docker daemon ----
FROM alpine:3.24 AS smoke
ARG EXPECTED_VERSION=
COPY --link --from=builder /out/cc-lb /usr/local/bin/cc-lb
SHELL ["/bin/ash", "-o", "pipefail", "-c"]
RUN /usr/local/bin/cc-lb --version && \
    if [ -n "$EXPECTED_VERSION" ]; then \
      /usr/local/bin/cc-lb --version | grep -Fq "$EXPECTED_VERSION"; \
    fi

# ---- Final: distroless static (DEFAULT target) ----
# Ships passwd/group metadata, /tmp, and CA certificates in a small operational
# base around the static binary. Digest-pinned for reproducible builds
# (:nonroot is a rolling tag); bump alongside the other base images
# (freshen-deps).
FROM gcr.io/distroless/static-debian13:nonroot@sha256:e2e927ec666bae08560abb3c55d0659eceabb657f56b6782ab500a9fc7f555e3 AS distroless
COPY --link --from=builder /out/cc-lb /usr/local/bin/cc-lb
USER 65532:65532
EXPOSE 8080 9090 9091
ENTRYPOINT ["/usr/local/bin/cc-lb"]
CMD ["serve", "--config", "/etc/cc-lb/cc-lb.toml"]
