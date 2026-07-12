# syntax=docker/dockerfile:1.25
#
# Self-hosted GitHub Actions runner image, based on falcondev-oss/actions-runner.
# Adds clang + mold for fast Rust builds. Deliberately does NOT install rustup,
# language runtimes, or project-specific libraries — workflows stay portable
# (they still use dtolnay/rust-toolchain, actions/setup-node, etc.).

ARG BASE_IMAGE=ghcr.io/falcondev-oss/actions-runner
ARG BASE_TAG=latest

FROM ${BASE_IMAGE}:${BASE_TAG}

USER root

# General-purpose Rust build prerequisites only:
#   clang / clang++   : C/C++ compiler used by cc-rs build scripts (replaces gcc)
#   mold              : fast linker; large wins on link-heavy Rust workloads
#   libc6-dev         : libc headers needed by clang
#   pkg-config        : nearly every *-sys crate uses it
#   libssl-dev        : openssl-sys, extremely common transitive dep
#   ca-certificates, curl, git, make, xz-utils : baseline build/network tools
#
# Deliberately NOT installed (keep workflows portable):
#   rustup / rust toolchain     -> managed by dtolnay/rust-toolchain action
#   gcc                         -> clang replaces it (CC=clang below)
#   project-specific libs (libpq-dev, node, python, ...)
#
# `postgresql-client` (psql) is intentionally included: the postgres-conformance
# workflow's E2E smoke step shells out to `psql` against the service container
# for direct meta-table assertions. Without it the script silently falls back
# to empty values (psql 127 -> `|| echo ""`) and step 4 misreports the failure.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
      clang \
      mold \
      libc6-dev \
      pkg-config \
      libssl-dev \
      ca-certificates \
      curl \
      git \
      make \
      xz-utils \
      zstd \
      postgresql-client \
    ; \
    rm -rf /var/lib/apt/lists/*; \
    clang --version; \
    mold --version; \
    psql --version

# Make clang the default C/C++ compiler for cc-rs and similar Rust build scripts.
ENV CC=clang \
    CXX=clang++

# For the two main Linux glibc Rust targets, drive linking through clang and
# use mold as the actual linker. Scoped per-target on purpose: cross-compile
# to musl, wasi, windows, etc. is left untouched so workflows stay portable.
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=clang \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS="-C link-arg=-fuse-ld=mold" \
    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=clang \
    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_RUSTFLAGS="-C link-arg=-fuse-ld=mold"

# Restore the unprivileged runner user defined by the base image.
USER runner
