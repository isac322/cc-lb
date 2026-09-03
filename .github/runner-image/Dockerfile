# syntax=docker/dockerfile:1.25
#
# cc-lb Actions Runner Controller image on Wolfi (glibc).
#
# ARC needs /home/runner/run.sh, uid 1001, and Docker CLI for DinD. This image
# installs those plus clang 22 / mold 2.42 from Wolfi apk. Do not bake rustc;
# rust-toolchain.toml moves independently.
#
# Keep ARG RUNNER_VERSION in lockstep with the runner tarball digest. CI reads
# that ARG for GHCR tags.

FROM cgr.dev/chainguard/wolfi-base:latest@sha256:103eb3f4444c68ea2453bf3aad09d860eaa5a698effb3e656cd607f630f0e46d

ARG TARGETARCH
ARG RUNNER_VERSION=2.337.0
ARG RUNNER_CONTAINER_HOOKS_VERSION=0.7.0
ARG DOCKER_VERSION=29.7.2
ARG BUILDX_VERSION=0.36.1
ARG SCCACHE_VERSION=0.17.0

# linux-arm64 runner tarball SHA-256 from the v2.337.0 release notes.
ARG RUNNER_SHA256_ARM64=9b1dc70626422526e3c94767cf024896beb15da5342a3f4819bf2feac13e0393

ENV RUNNER_MANUALLY_TRAP_SIG=1 \
    ACTIONS_RUNNER_PRINT_LOG_TO_STDOUT=1

RUN set -eux; \
    apk add --no-cache \
      bash \
      busybox \
      ca-certificates-bundle \
      clang-22 \
      compiler-rt-22 \
      coreutils \
      curl \
      findutils \
      git \
      glibc-2.44-dev \
      icu \
      jq \
      libstdc++ \
      make \
      mold \
      openssl \
      openssl-dev \
      pkgconf \
      postgresql-18-client \
      shadow \
      sudo \
      unzip \
      xz \
      zlib \
      zstd; \
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

RUN set -eux; \
    case "${TARGETARCH}" in \
      arm64) \
        runner_arch=arm64; \
        runner_sha256="${RUNNER_SHA256_ARM64}"; \
        docker_arch=aarch64; \
        sccache_target="aarch64-unknown-linux-musl"; \
        sccache_sha256="821a86343191aa1cbab74bd42f9e93c9a63bf85e4742945f40d3ae84193c1c77" \
        ;; \
      *) \
        echo "unsupported TARGETARCH: ${TARGETARCH}" >&2; \
        exit 1 \
        ;; \
    esac; \
    addgroup docker; \
    adduser -D -u 1001 -h /home/runner -s /bin/bash runner; \
    addgroup runner docker; \
    echo 'runner ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/runner; \
    chmod 0440 /etc/sudoers.d/runner; \
    mkdir -p /home/runner /usr/local/lib/docker/cli-plugins; \
    curl --fail --silent --show-error --location \
      "https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/actions-runner-linux-${runner_arch}-${RUNNER_VERSION}.tar.gz" \
      --output /tmp/runner.tar.gz; \
    printf '%s  %s\n' "${runner_sha256}" /tmp/runner.tar.gz | sha256sum --check; \
    tar -xzf /tmp/runner.tar.gz -C /home/runner; \
    rm -f /tmp/runner.tar.gz; \
    curl --fail --silent --show-error --location \
      "https://github.com/actions/runner-container-hooks/releases/download/v${RUNNER_CONTAINER_HOOKS_VERSION}/actions-runner-hooks-k8s-${RUNNER_CONTAINER_HOOKS_VERSION}.zip" \
      --output /tmp/hooks.zip; \
    unzip -q /tmp/hooks.zip -d /home/runner/k8s; \
    rm -f /tmp/hooks.zip; \
    curl --fail --silent --show-error --location \
      "https://download.docker.com/linux/static/stable/${docker_arch}/docker-${DOCKER_VERSION}.tgz" \
      --output /tmp/docker.tgz; \
    tar -xzf /tmp/docker.tgz -C /tmp; \
    install -m 0755 /tmp/docker/docker /usr/bin/docker; \
    rm -rf /tmp/docker /tmp/docker.tgz; \
    curl --fail --silent --show-error --location \
      "https://github.com/docker/buildx/releases/download/v${BUILDX_VERSION}/buildx-v${BUILDX_VERSION}.linux-${TARGETARCH}" \
      --output /usr/local/lib/docker/cli-plugins/docker-buildx; \
    chmod 0755 /usr/local/lib/docker/cli-plugins/docker-buildx; \
    curl --fail --silent --show-error --location \
      "https://github.com/mozilla/sccache/releases/download/v${SCCACHE_VERSION}/sccache-v${SCCACHE_VERSION}-${sccache_target}.tar.gz" \
      --output /tmp/sccache.tar.gz; \
    printf '%s  %s\n' "${sccache_sha256}" /tmp/sccache.tar.gz | sha256sum --check; \
    tar -xzf /tmp/sccache.tar.gz -C /tmp; \
    install -m 0755 /tmp/sccache-v${SCCACHE_VERSION}-${sccache_target}/sccache /usr/local/bin/sccache; \
    rm -rf /tmp/sccache.tar.gz /tmp/sccache-v${SCCACHE_VERSION}-${sccache_target}; \
    chown -R runner:runner /home/runner; \
    chmod 0777 /home/runner; \
    docker --version; \
    sccache --version; \
    test -x /home/runner/run.sh

WORKDIR /home/runner
USER runner
