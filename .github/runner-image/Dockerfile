# syntax=docker/dockerfile:1.25
#
# cc-lb Actions Runner Controller image.
#
# ARC does not require this FROM line. It needs a glibc image with the runner
# at /home/runner/run.sh (and k8s hooks for Kubernetes mode). The published
# actions-runner image is Ubuntu 24.04 (dotnet/runtime-deps noble) plus Docker
# CLI for DinD, so we layer on top instead of re-shipping the runner.
#
# Do not bake rustc/cargo. rust-toolchain.toml moves independently; putting
# the toolchain here would force a runner rebuild on every bump.
# dtolnay/rust-toolchain and taiki-e/install-action stay in the workflows.
#
# Alpine is a poor fit (musl runner/Node, gnu rust target). Wolfi is glibc and
# smaller than Ubuntu, but a GitHub Actions runner still needs a shell, git,
# sudo, Docker CLI, and then clang — that last layer dominates size. Stay on
# the official image until a base swap actually shrinks the job.
#
# Keep the FROM tag and digest in lockstep. CI reads the tag for GHCR names.

FROM ghcr.io/actions/actions-runner:2.337.0@sha256:e5496277be5d09bc968b3d64911b74e219ac4a3f2edce956a3ecf9271bea1ef4

ARG SCCACHE_VERSION=0.17.0
ARG TARGETARCH

USER root

# clang / mold / cc  : cc-rs and rustc (official ARC image has neither)
# libc6-dev          : clang headers
# pkg-config, libssl-dev : *-sys / openssl-sys
# postgresql-client  : postgres-conformance E2E smoke (psql 127 -> empty fallback)
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

# Preinstall the static upstream sccache binary. Digests are from the SHA-256
# files published with the v0.17.0 release assets.
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

ENV CC=clang \
    CXX=clang++

RUN printf '%s\n' \
      '#!/bin/sh' \
      'exec clang -fuse-ld=mold "$@"' \
      > /usr/local/bin/clang-mold; \
    chmod 0755 /usr/local/bin/clang-mold

ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=/usr/local/bin/clang-mold \
    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=/usr/local/bin/clang-mold

USER runner
