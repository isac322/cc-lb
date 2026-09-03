# syntax=docker/dockerfile:1.25
#
# Actions Runner Controller image for cc-lb. GitHub-hosted Ubuntu 24.04
# already ships clang; ghcr.io/actions/actions-runner does not, so rustc
# and cc-rs fail with `linker `cc` not found` on the official ARC image.
#
# Start from the official runner and add only general-purpose Rust build
# tools. Do not install rustup, language runtimes, or project libraries —
# workflows stay portable (dtolnay/rust-toolchain, actions/setup-node, …).
#
# Keep the FROM tag and digest in lockstep. CI reads the tag for GHCR
# image names; Dependabot updates this line.

FROM ghcr.io/actions/actions-runner:2.337.0@sha256:e5496277be5d09bc968b3d64911b74e219ac4a3f2edce956a3ecf9271bea1ef4

ARG SCCACHE_VERSION=0.17.0
ARG TARGETARCH

USER root

# General-purpose Rust build prerequisites only:
#   clang / clang++   : C/C++ compiler used by cc-rs (and as `cc`)
#   mold              : fast linker for link-heavy Rust workloads
#   libc6-dev         : libc headers needed by clang
#   pkg-config        : nearly every *-sys crate uses it
#   libssl-dev        : openssl-sys, extremely common transitive dep
#   ca-certificates, curl, git, make, xz-utils, zstd : baseline tools
#
# Deliberately NOT installed:
#   rustup / rust toolchain     -> dtolnay/rust-toolchain
#   gcc                         -> clang replaces it
#   project-specific libs (libpq-dev, node, python, ...)
#
# `postgresql-client` (psql) is included because the postgres-conformance
# E2E smoke step shells out to `psql` against the service container.
# Without it the script falls back to empty values (psql 127 -> `|| echo ""`)
# and misreports the failure.
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
    ln -sfn "$(command -v clang)" /usr/local/bin/cc; \
    ln -sfn "$(command -v clang++)" /usr/local/bin/c++; \
    clang --version; \
    cc --version; \
    mold --version; \
    psql --version

# Preinstall the static upstream sccache binary for both runner architectures.
# Digests are from the SHA-256 files published with the v0.17.0 release assets.
RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64) \
        sccache_target="x86_64-unknown-linux-musl"; \
        sccache_sha256="67c4a96dd237c1f518f6b36083f270f9976d516f1e57fce891755ea782e50006" \
        ;; \
      arm64) \
        sccache_target="aarch64-unknown-linux-musl"; \
        sccache_sha256="821a86343191aa1cbab74bd42f9e93c9a63bf85e4742945f40d3ae84193c1c77" \
        ;; \
      *) \
        echo "unsupported TARGETARCH: ${TARGETARCH}" >&2; \
        exit 1 \
        ;; \
    esac; \
    archive="sccache-v${SCCACHE_VERSION}-${sccache_target}.tar.gz"; \
    archive_path="/tmp/${archive}"; \
    install_root="/tmp/sccache-v${SCCACHE_VERSION}-${sccache_target}"; \
    curl --fail --silent --show-error --location \
      "https://github.com/mozilla/sccache/releases/download/v${SCCACHE_VERSION}/${archive}" \
      --output "${archive_path}"; \
    printf '%s  %s\n' "${sccache_sha256}" "${archive_path}" > /tmp/sccache.sha256; \
    sha256sum --check /tmp/sccache.sha256; \
    tar -xzf "${archive_path}" -C /tmp; \
    install -m 0755 "${install_root}/sccache" /usr/local/bin/sccache; \
    rm -rf "${archive_path}" "${install_root}" /tmp/sccache.sha256; \
    sccache --version

# Make clang the default C/C++ compiler for cc-rs and similar Rust build scripts.
ENV CC=clang \
    CXX=clang++

# Keep the mold choice in Cargo's linker executable rather than target
# RUSTFLAGS, which tools such as cargo-llvm-cov replace.
RUN printf '%s\n' \
      '#!/bin/sh' \
      'exec clang -fuse-ld=mold "$@"' \
      > /usr/local/bin/clang-mold; \
    chmod 0755 /usr/local/bin/clang-mold

# For the two main Linux glibc Rust targets, drive linking through the
# clang-mold wrapper. Scoped per-target on purpose: cross-compile to musl,
# wasi, windows, etc. is left untouched so workflows stay portable.
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=/usr/local/bin/clang-mold \
    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=/usr/local/bin/clang-mold

# Restore the unprivileged runner user defined by the base image.
USER runner
