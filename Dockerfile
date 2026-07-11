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
#   * The actual compile lives in scripts/build-cc-lb.sh (one source of truth):
#     the `compiled` stage runs it in BuildKit (cd / local), and the docker-build
#     CI job runs the SAME script via `docker run` off the `builder` image with a
#     host-bind sccache cache — a BuildKit RUN can't bind a writable host dir, so
#     the cached compile has to happen outside BuildKit.
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

# ---- Builder: cross toolchain only, NO source, NO compile ----
# Kept compile-free on purpose: the docker-build CI job builds this stage as an
# image and `docker run`s scripts/build-cc-lb.sh against a bind-mounted workspace
# with a host-bind sccache cache. The in-BuildKit `compiled` stage below is the
# cd / local path.
FROM --platform=$BUILDPLATFORM rust:1.96.0-alpine AS builder
SHELL ["/bin/ash", "-exuo", "pipefail", "-c"]

# clang/lld: xx uses clang as the cross linker driver (overrides the repo's
#            .cargo/config.toml rust-lld, which is expected and correct).
# git:       cc-lb-server/build.rs reads `git rev-parse` (falls back gracefully).
# libstdc++/libgcc: Bun's runtime dependencies on Alpine.
# sccache:   compiler cache; enabled by the docker-run CI path via SCCACHE_DIR.
# hadolint ignore=DL3018
RUN apk add --no-cache clang lld git libstdc++ libgcc sccache

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

# ---- Compiled: run the shared build script in BuildKit (cd / local path) ----
FROM builder AS compiled
ARG BUILDPLATFORM
# Build-time metadata / knobs (consumed by scripts/build-cc-lb.sh via the env).
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

# The cargo download caches (registry/git) and the Bun install cache are reused
# across builds. The target dir is intentionally NOT cache-mounted:
# cc-lb-admin/build.rs regenerates its embedded SPA (web/dist) into the freshly
# COPYed source tree every build, and a persisted target dir makes cargo skip
# that build script on a warm rebuild -> missing web/dist -> fail. The docker-run
# CI path caches at the compiler level (sccache) instead.
RUN --mount=type=cache,id=cargo-registry,sharing=locked,target=/usr/local/cargo/registry \
    --mount=type=cache,id=cargo-git,sharing=locked,target=/usr/local/cargo/git \
    --mount=type=cache,id=bun-${BUILDPLATFORM},sharing=locked,target=/root/.bun/install/cache \
    sh /src/scripts/build-cc-lb.sh

# ---- Runtime base: distroless config shared by the distroless + package images ----
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
COPY --link --from=compiled /out/etc/passwd /etc/passwd
COPY --link --from=compiled /out/etc/group /etc/group
COPY --link --from=compiled /out/cc-lb /usr/local/bin/cc-lb
USER 65532:65532
EXPOSE 8080 9090 9091
ENTRYPOINT ["/usr/local/bin/cc-lb"]
CMD ["serve", "--config", "/etc/cc-lb/cc-lb.toml"]

# ---- Package: distroless image from a prebuilt binary (docker-build CI path) ----
# The CI job compiles via `docker run` (host-bind sccache) then packages the
# result with `--build-context bin=<out> --target package`, reusing runtime-base
# so the runtime config isn't duplicated in the workflow. Only built when the
# `bin` build-context is supplied.
FROM runtime-base AS package
# `bin` is a named build-context (docker buildx --build-context bin=<dir>), not a
# stage alias, which hadolint DL3022 can't see.
# hadolint ignore=DL3022
COPY --link --from=bin cc-lb /usr/local/bin/cc-lb

# ---- Final: distroless static (DEFAULT target; cd / local in-BuildKit build) ----
FROM runtime-base AS distroless
COPY --link --from=compiled /out/cc-lb /usr/local/bin/cc-lb
