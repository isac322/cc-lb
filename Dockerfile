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
FROM --platform=$BUILDPLATFORM rust:1.96.0-alpine AS builder
SHELL ["/bin/ash", "-exuo", "pipefail", "-c"]

# clang/lld: xx uses clang as the cross linker driver (overrides the repo's
#            .cargo/config.toml rust-lld, which is expected and correct).
# git:       cc-lb-server/build.rs reads `git rev-parse` (falls back gracefully).
# libstdc++/libgcc: Bun's runtime dependencies on Alpine.
# hadolint ignore=DL3018
RUN apk add --no-cache clang lld git libstdc++ libgcc

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

# Ensure the resolved Rust target triple is installed for cross builds.
RUN rustup target add "$(xx-cargo --print-target-triple)"

# Build-time metadata / knobs.
ARG GIT_SHA=""
ARG SOURCE_DATE_EPOCH=""
# The default ships BOTH storage backends in one binary; the backend is chosen at
# runtime via `[storage] kind` ("sqlite" or "postgres"). Override to slim, e.g.
# `--build-arg FEATURES=sqlite`.
ARG FEATURES="sqlite,postgres"
# Set to 1 to skip building the dashboard SPA (ships a placeholder page).
ARG SKIP_SPA="0"

WORKDIR /src
COPY . .

# Caches reused across builds: the cargo download caches (registry/git) and the
# Bun install cache. The target dir is intentionally NOT cache-mounted:
# cc-lb-admin/build.rs regenerates its embedded SPA (web/dist) into the freshly
# COPYed source tree every build, and a persisted target dir makes cargo skip
# that build script on a warm rebuild -> missing web/dist -> fail.
RUN --mount=type=cache,id=cargo-registry,sharing=locked,target=/usr/local/cargo/registry \
    --mount=type=cache,id=cargo-git,sharing=locked,target=/usr/local/cargo/git \
    --mount=type=cache,id=bun-${BUILDPLATFORM},sharing=locked,target=/root/.bun/install/cache <<'EOF'
# An empty SOURCE_DATE_EPOCH makes ring's cc/clang C build abort; drop it unless
# a real value was passed.
if [ -z "${SOURCE_DATE_EPOCH:-}" ]; then unset SOURCE_DATE_EPOCH; fi
# cc-lb-runtime-wasmtime/build.rs would otherwise force a wasm32 fixture build.
export CC_LB_SKIP_WASM_FIXTURE_BUILD=1
export GIT_SHA="${GIT_SHA}"
if [ "${SKIP_SPA}" = "1" ]; then export CC_LB_ADMIN_SKIP_SPA=1; fi
xx-cargo build --release --locked \
  -p cc-lb-server \
  --no-default-features --features "${FEATURES}" \
  --target-dir /src/target
triple="$(xx-cargo --print-target-triple)"
install -Dm0755 "/src/target/${triple}/release/cc-lb" /out/cc-lb
xx-verify --static /out/cc-lb
# Minimal passwd/group so the scratch image can run as a real nonroot user.
install -d /out/etc
echo 'nonroot:x:65532:65532:nonroot:/home/nonroot:/sbin/nologin' > /out/etc/passwd
echo 'nonroot:x:65532:' > /out/etc/group
EOF

# ---- Runtime base: distroless config shared by the final images ----
# Ships /etc/passwd, a nonroot user (65532), /tmp, and CA certs — a safe,
# debuggable base while staying ~2 MB over the static binary. Digest-pinned for
# reproducible builds (:nonroot is a rolling tag); bump it alongside the other
# base images (freshen-deps).
FROM gcr.io/distroless/static-debian13:nonroot@sha256:963fa6c544fe5ce420f1f54fb88b6fb01479f054c8056d0f74cc2c6000df5240 AS runtime-base
USER 65532:65532
EXPOSE 8080 9090 9091
ENTRYPOINT ["/usr/local/bin/cc-lb"]
CMD ["serve", "--config", "/etc/cc-lb/cc-lb.toml"]

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
FROM runtime-base AS distroless
COPY --link --from=builder /out/cc-lb /usr/local/bin/cc-lb
