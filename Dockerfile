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
#   * `cc-lb-admin/build.rs` shells out to Bun to build + embed the dashboard
#     SPA; build scripts run on BUILDPLATFORM, so a BUILDPLATFORM Bun is enough.
#   * TLS uses rustls + webpki-roots (CA roots compiled into the binary), so the
#     final image needs no system CA certificates.
#
# Build (single arch, load locally):
#   docker buildx build --load --build-arg GIT_SHA=$(git rev-parse HEAD) -t cc-lb .
# Build (multi-arch, push — requires a docker-container builder):
#   docker buildx build --platform linux/amd64,linux/arm64 \
#     --build-arg GIT_SHA=$(git rev-parse HEAD) -t <registry>/cc-lb:<tag> --push .
# Smaller `scratch` variant:  add `--target runtime-scratch`

# ---- xx cross-compilation helper scripts (shared across all target platforms) ----
FROM --platform=$BUILDPLATFORM tonistiigi/xx:1.9.0 AS xx

# ---- Bun (musl) for the cc-lb-admin dashboard SPA build; runs on BUILDPLATFORM ----
FROM --platform=$BUILDPLATFORM oven/bun:1.3.14-alpine AS bun

# ---- Builder: cross toolchain, source, and the compile ----
FROM --platform=$BUILDPLATFORM rust:1.97.0-alpine AS builder
SHELL ["/bin/ash", "-exuo", "pipefail", "-c"]

# clang/lld: xx uses clang as the cross linker driver for every target
#            architecture.
# git:       cc-lb-server/build.rs reads `git rev-parse` (falls back gracefully).
# libstdc++/libgcc: Bun's runtime dependencies on Alpine.
# make:      tikv-jemalloc-sys builds its vendored jemalloc via autotools (runs
#            `make` on BUILDPLATFORM); the rust:alpine image does not ship it.
# sccache: Rust compiler cache; uses the S3/garage backend when creds are passed.
# hadolint ignore=DL3018
RUN apk add --no-cache clang lld git libstdc++ libgcc make sccache

# xx scripts (xx-cargo, xx-apk, xx-verify, ...).
COPY --from=xx / /

# BUILDPLATFORM Bun + a `bunx` alias (package.json build script calls `bunx`).
COPY --from=bun /usr/local/bin/bun /usr/local/bin/bun
RUN ln -sf /usr/local/bin/bun /usr/local/bin/bunx

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
ARG FEATURES="sqlite,postgres"
# Set to 1 to skip building the dashboard SPA (ships a placeholder page).
ARG SKIP_SPA="0"
# sccache S3 backend. Self-hosted CI and release builds require it so cache
# failures are visible; local builds retain an explicit uncached fallback.
ARG SCCACHE_BUCKET=""
ARG SCCACHE_ENDPOINT=""
ARG SCCACHE_REGION=""
ARG SCCACHE_S3_USE_SSL=""
ARG REQUIRE_SCCACHE="0"
ARG SCCACHE_S3_KEY_PREFIX=""
ARG SCCACHE_MIN_HIT_RATE="0"

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

# Cargo's package-cache lock lives at $CARGO_HOME/.package-cache. Mounting the
# whole CARGO_HOME lets Cargo serialize only registry/git mutation while the
# four target builds compile concurrently.
# Bun does not have that established contract here: give each target its own
# locked cache so a concurrent SPA install cannot serialize the other targets.
# The target dir is intentionally NOT cache-mounted: cc-lb-admin/build.rs
# regenerates its embedded SPA (web/dist) into the freshly COPYed source tree
# every build, and a persisted target dir makes cargo skip that build script on
# a warm rebuild -> missing web/dist -> fail.
RUN --mount=type=cache,id=cargo-home,sharing=shared,target=/cargo-home \
    --mount=type=cache,id=bun-${TARGETPLATFORM},sharing=locked,target=/root/.bun/install/cache \
    --mount=type=secret,id=AWS_ACCESS_KEY_ID,required=false \
    --mount=type=secret,id=AWS_SECRET_ACCESS_KEY,required=false <<'EOF'
# An empty SOURCE_DATE_EPOCH makes ring's cc/clang C build abort; drop it unless
# a real value was passed.
if [ -z "${SOURCE_DATE_EPOCH:-}" ]; then unset SOURCE_DATE_EPOCH; fi
# cc-lb-runtime-wasmtime/build.rs would otherwise force a wasm32 fixture build.
export CC_LB_SKIP_WASM_FIXTURE_BUILD=1
export GIT_SHA="${GIT_SHA}"
if [ "${SKIP_SPA}" = "1" ]; then export CC_LB_ADMIN_SKIP_SPA=1; fi
# Install and verify the target in the same snapshot Cargo uses. Resolve rustc
# to its concrete binary before sccache receives it: the daemon must not
# re-execute rustup's cwd-sensitive proxy outside /src.
target="$(xx-cargo --print-target-triple)"
rustup target add "$target"
sysroot="$(rustc --print sysroot)"
test -n "$(find "$sysroot/lib/rustlib/$target/lib" -maxdepth 1 -type f -name 'libcore-*.rlib' -print -quit)"
RUSTC="$(rustup which rustc)"
export RUSTC
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
  if grep -Eq '^Cache (timeouts|read errors|write errors)[[:space:]]+[1-9][0-9]*$' /tmp/sccache-stats.txt; then
    echo "sccache reported a cache storage error or timeout" >&2
    cache_status=1
  fi
  if [ "${SCCACHE_MIN_HIT_RATE}" != "0" ]; then
    for language in Rust 'C/C++'; do
      rate="$(awk -v language="$language" '$1 == "Cache" && $2 == "hits" && $3 == "rate" && $4 == "(" language ")" { print $(NF - 1); exit }' /tmp/sccache-stats.txt)"
      if ! awk -v rate="$rate" -v minimum="${SCCACHE_MIN_HIT_RATE}" 'BEGIN { exit !(rate + 0 >= minimum) }'; then
        echo "sccache ${language} hit rate ${rate:-missing}% is below ${SCCACHE_MIN_HIT_RATE}%" >&2
        cache_status=1
      fi
    done
  fi
  unset RUSTC_WRAPPER
  if { [ "$cache_status" -ne 0 ] || [ "$build_status" -ne 0 ]; } && [ -s "${SCCACHE_ERROR_LOG}" ]; then
    tail -n 200 "${SCCACHE_ERROR_LOG}" >&2
  fi
fi
if [ "$build_status" -ne 0 ]; then exit "$build_status"; fi
if [ "$cache_status" -ne 0 ]; then exit "$cache_status"; fi
triple="$target"
install -Dm0755 "/src/target/${triple}/release/cc-lb" /out/cc-lb
xx-verify --static /out/cc-lb
# Minimal passwd/group so the scratch image can run as a real nonroot user.
install -d /out/etc
echo 'nonroot:x:65532:65532:nonroot:/home/nonroot:/sbin/nologin' > /out/etc/passwd
echo 'nonroot:x:65532:' > /out/etc/group
EOF

# ---- Final: scratch (opt-in via `--target runtime-scratch`; smallest image) ----
FROM scratch AS runtime-scratch
COPY --link --from=builder /out/etc/passwd /etc/passwd
COPY --link --from=builder /out/etc/group /etc/group
COPY --link --from=builder /out/cc-lb /usr/local/bin/cc-lb
USER 65532:65532
EXPOSE 8080 9090 9091
ENTRYPOINT ["/usr/local/bin/cc-lb"]
CMD ["serve", "--config", "/etc/cc-lb/cc-lb.toml"]

# ---- Final: distroless static (DEFAULT target) ----
# Ships /etc/passwd, a nonroot user (65532), /tmp, and CA certs — a safe,
# debuggable base ~2 MB over the static binary. Digest-pinned for reproducible
# builds (:nonroot is a rolling tag); bump alongside the other base images
# (freshen-deps).
FROM gcr.io/distroless/static-debian13:nonroot@sha256:963fa6c544fe5ce420f1f54fb88b6fb01479f054c8056d0f74cc2c6000df5240 AS distroless
COPY --link --from=builder /out/cc-lb /usr/local/bin/cc-lb
USER 65532:65532
EXPOSE 8080 9090 9091
ENTRYPOINT ["/usr/local/bin/cc-lb"]
CMD ["serve", "--config", "/etc/cc-lb/cc-lb.toml"]
