# syntax=docker/dockerfile:1.25
#
# cc-lb Actions Runner Controller image.
#
# The official ghcr.io/actions/actions-runner image is not required. ARC only
# needs a glibc image that can run the runner at /home/runner/run.sh (and, in
# Kubernetes mode, hooks under /home/runner/k8s). GitHub's own example starts
# from mcr.microsoft.com/dotnet/runtime-deps. The published actions-runner
# image is already that stack on Ubuntu 24.04 plus Docker CLI for DinD.
#
# Alpine is a poor fit: the runner is a glibc/.NET binary, JS actions use the
# runner's glibc Node, and cc-lb builds aarch64-unknown-linux-gnu. A musl
# rootfs would need gcompat or a GNU toolchain and would not shrink the job.
#
# This image is only for cc-lb, so it bakes the tools every ephemeral pod
# would otherwise download: clang/mold/cc, sccache, rustc 1.97.1, clippy,
# rustfmt, llvm-tools, wasm targets, cargo-nextest, cargo-llvm-cov, and
# cargo-deny. Do not add packages that only shave a few seconds off a job.
#
# Keep the FROM tag and digest in lockstep. CI reads the tag for GHCR names.

FROM ghcr.io/actions/actions-runner:2.337.0@sha256:e5496277be5d09bc968b3d64911b74e219ac4a3f2edce956a3ecf9271bea1ef4

ARG TARGETARCH
ARG SCCACHE_VERSION=0.17.0
ARG RUSTUP_VERSION=1.28.2
# Keep in sync with rust-toolchain.toml. CI asserts the match.
ARG RUST_TOOLCHAIN=1.97.1
ARG NEXTEST_VERSION=0.9.143
ARG LLVM_COV_VERSION=0.9.0
ARG CARGO_DENY_VERSION=0.20.2

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

RUN printf '%s\n' \
      '#!/bin/sh' \
      'exec clang -fuse-ld=mold "$@"' \
      > /usr/local/bin/clang-mold; \
    chmod 0755 /usr/local/bin/clang-mold

ENV CC=clang \
    CXX=clang++ \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=/usr/local/bin/clang-mold \
    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=/usr/local/bin/clang-mold

# Static upstream binaries. Digests are from the published SHA-256 files
# (sccache, nextest, cargo-deny) or a local hash of the release tarball
# (cargo-llvm-cov, which does not publish a .sha256 asset).
RUN set -eux; \
    case "${TARGETARCH}" in \
      arm64) \
        sccache_target="aarch64-unknown-linux-musl"; \
        sccache_sha256="821a86343191aa1cbab74bd42f9e93c9a63bf85e4742945f40d3ae84193c1c77"; \
        nextest_target="aarch64-unknown-linux-musl"; \
        nextest_sha256="0560ce0ce017f368c54b5db86588370fc03b470985490fa81200e62a657dda05"; \
        llvm_cov_target="aarch64-unknown-linux-musl"; \
        llvm_cov_sha256="3c299780e109d59fd77044e64734421d43c0067b92937a49be302fee66d04727"; \
        deny_target="aarch64-unknown-linux-musl"; \
        deny_sha256="995c82be0defc7a025cae49a2aa2644ce8245c9a3318fc4103907c6a285e8c7d" \
        ;; \
      *) \
        echo "unsupported TARGETARCH: ${TARGETARCH}" >&2; \
        exit 1 \
        ;; \
    esac; \
    install_tar() { \
      url="$1"; sha="$2"; bin="$3"; \
      archive="/tmp/${bin}.tar.gz"; \
      dest="/tmp/${bin}-extract"; \
      curl --fail --silent --show-error --location "${url}" --output "${archive}"; \
      printf '%s  %s\n' "${sha}" "${archive}" | sha256sum --check; \
      mkdir -p "${dest}"; \
      tar -xzf "${archive}" -C "${dest}"; \
      src="$(find "${dest}" -type f -name "${bin}" | head -n 1)"; \
      test -n "${src}"; \
      install -m 0755 "${src}" "/usr/local/bin/${bin}"; \
      rm -rf "${archive}" "${dest}"; \
    }; \
    install_tar \
      "https://github.com/mozilla/sccache/releases/download/v${SCCACHE_VERSION}/sccache-v${SCCACHE_VERSION}-${sccache_target}.tar.gz" \
      "${sccache_sha256}" \
      sccache; \
    install_tar \
      "https://github.com/nextest-rs/nextest/releases/download/cargo-nextest-${NEXTEST_VERSION}/cargo-nextest-${NEXTEST_VERSION}-${nextest_target}.tar.gz" \
      "${nextest_sha256}" \
      cargo-nextest; \
    install_tar \
      "https://github.com/taiki-e/cargo-llvm-cov/releases/download/v${LLVM_COV_VERSION}/cargo-llvm-cov-${llvm_cov_target}.tar.gz" \
      "${llvm_cov_sha256}" \
      cargo-llvm-cov; \
    install_tar \
      "https://github.com/EmbarkStudios/cargo-deny/releases/download/${CARGO_DENY_VERSION}/cargo-deny-${CARGO_DENY_VERSION}-${deny_target}.tar.gz" \
      "${deny_sha256}" \
      cargo-deny; \
    sccache --version; \
    cargo-nextest --version; \
    test -x /usr/local/bin/cargo-llvm-cov; \
    cargo-deny --version

USER runner
WORKDIR /home/runner

ENV HOME=/home/runner \
    USER=runner \
    CARGO_HOME=/home/runner/.cargo \
    RUSTUP_HOME=/home/runner/.rustup \
    PATH=/home/runner/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# rustup + the workspace toolchain. Ephemeral ARC pods start empty; putting
# rustc in the image avoids a multi-minute download on every clippy/nextest job.
RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64) \
        rustup_host="x86_64-unknown-linux-gnu"; \
        rustup_sha256="20a06e644b0d9bd2fbdbfd52d42540bdde820ea7df86e92e533c073da0cdd43c" \
        ;; \
      arm64) \
        rustup_host="aarch64-unknown-linux-gnu"; \
        rustup_sha256="e3853c5a252fca15252d07cb23a1bdd9377a8c6f3efa01531109281ae47f841c" \
        ;; \
      *) \
        echo "unsupported TARGETARCH: ${TARGETARCH}" >&2; \
        exit 1 \
        ;; \
    esac; \
    curl --fail --silent --show-error --location \
      "https://static.rust-lang.org/rustup/archive/${RUSTUP_VERSION}/${rustup_host}/rustup-init" \
      --output /tmp/rustup-init; \
    printf '%s  %s\n' "${rustup_sha256}" /tmp/rustup-init | sha256sum --check; \
    chmod 0755 /tmp/rustup-init; \
    /tmp/rustup-init -y --no-modify-path \
      --default-toolchain "${RUST_TOOLCHAIN}" \
      --component clippy,rustfmt,llvm-tools-preview; \
    rm -f /tmp/rustup-init; \
    rustup target add wasm32-wasip1 wasm32-unknown-unknown; \
    rustc --version; \
    cargo --version; \
    rustup show
