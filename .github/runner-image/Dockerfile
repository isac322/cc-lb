# syntax=docker/dockerfile:1.25
#
# cc-lb Actions Runner Controller image on Wolfi (glibc).
#
# ARC needs /home/runner/run.sh and uid 1001. Clang 22 / mold link Rust;
# gcc remains the default cc so autotools (jemalloc) configure correctly.
# Do not bake rustc; rust-toolchain.toml moves independently.
#
# Keep ARG RUNNER_VERSION in lockstep with the runner tarball digest. CI reads
# that ARG for GHCR tags.
#
# gnutar must stay installed: busybox tar rejects the -P flag that
# actions/cache passes, so cache restore and save both fail on this image.

FROM cgr.dev/chainguard/wolfi-base:latest@sha256:1d95114038f76513a9ace6fca107d5582b08c65981f81f61cb56bf7fd2ef216d
LABEL org.opencontainers.image.source="https://github.com/isac322/cc-lb"

ARG TARGETARCH
ARG RUNNER_VERSION=2.337.0
ARG RUNNER_CONTAINER_HOOKS_VERSION=0.8.1
ARG DOCKER_VERSION=29.8.1
ARG BUILDX_VERSION=0.37.1
ARG SCCACHE_VERSION=0.18.0
ARG GH_VERSION=2.101.0
ARG PROMETHEUS_VERSION=3.14.0
ARG HADOLINT_VERSION=2.15.1

# linux-arm64 runner tarball SHA-256 from the v2.337.0 release notes.
ARG RUNNER_SHA256_ARM64=9b1dc70626422526e3c94767cf024896beb15da5342a3f4819bf2feac13e0393
ARG GH_SHA256_ARM64=b57e8063f18862647c9d22727c32e9da1b963f8bf9db648fe123a6975695640f
ARG PROMETHEUS_SHA256_ARM64=077f3781ab7245dc04c9a3c9b78ba120fc8e41aa0dc97489b0af67247e50ba83
ARG HADOLINT_SHA256_ARM64=f6198ef8090f404dbb771abfee086eb8c48ac177f30da7fd3510aca35b344b5d

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
      gcc \
      git \
      glibc-2.44-dev \
      gnutar \
      grep \
      icu \
      jq \
      libstdc++ \
      make \
      mold \
      openssl \
      openssl-dev \
      pkgconf \
      posix-libc-utils \
      postgresql-18-client \
      python-3.14 \
      ripgrep \
      shadow \
      sudo \
      unzip \
      xz \
      zlib \
      zstd; \
    if ! command -v python3 >/dev/null; then \
      ln -sfn "$(command -v python3.14)" /usr/local/bin/python3; \
    fi; \
    clang --version; \
    gcc --version; \
    cc --version; \
    rg --version; \
    grep --version; \
    case "$(tar --version)" in "tar (GNU tar)"*) ;; *) exit 1 ;; esac; \
    mold --version; \
    psql --version; \
    python3 --version

RUN printf '%s\n' \
      '#!/bin/sh' \
      'exec clang -fuse-ld=mold "$@"' \
      > /usr/local/bin/clang-mold; \
    chmod 0755 /usr/local/bin/clang-mold

# The generated wrapper intentionally preserves shell expressions, and the
# build-time probes are checked immediately in this same shell.
# hadolint ignore=DL4006,SC2016
RUN set -eux; \
    real_ldd="$(command -v ldd)"; \
    ldd_version="$("${real_ldd}" --version 2>&1 | awk 'NR == 1 { print $NF; exit }')"; \
    printf '%s\n' \
      '#!/bin/sh' \
      'case "${1:-}" in' \
      '  --version|-v)' \
      "    printf '%s\n' 'ldd (GNU libc) ${ldd_version}'" \
      '    ;;' \
      '  *)' \
      "    exec ${real_ldd} \"\$@\"" \
      '    ;;' \
      'esac' \
      > /usr/local/bin/ldd; \
    chmod 0755 /usr/local/bin/ldd; \
    ldd --version | grep -Eq '^ldd \(GNU libc\) [0-9]+(\.[0-9]+)+$'; \
    printf '%s\n' 'int main(void) { return 0; }' > /tmp/ldd-smoke.c; \
    cc -o /tmp/ldd-smoke /tmp/ldd-smoke.c; \
    ldd /tmp/ldd-smoke | grep -q 'libc\.so\.6'; \
    rm -f /tmp/ldd-smoke /tmp/ldd-smoke.c

ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=/usr/local/bin/clang-mold \
    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=/usr/local/bin/clang-mold

RUN set -eux; \
    case "${TARGETARCH}" in \
      arm64) \
        runner_arch=arm64; \
        runner_sha256="${RUNNER_SHA256_ARM64}"; \
        gh_sha256="${GH_SHA256_ARM64}"; \
        docker_arch=aarch64; \
        sccache_target="aarch64-unknown-linux-musl"; \
        sccache_sha256="2b3284d5da3b46a47dc4229e75bb7b88ac4aa99c8d754fb7d2f84997e5a4354a"; \
        prometheus_sha256="${PROMETHEUS_SHA256_ARM64}"; \
        hadolint_sha256="${HADOLINT_SHA256_ARM64}" \
        ;; \
      *) \
        echo "unsupported TARGETARCH: ${TARGETARCH}" >&2; \
        exit 1 \
        ;; \
    esac; \
    groupmod -g 124 ntp; \
    addgroup -g 123 docker; \
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
    sed -i 's/\x41\x00\x43\x00\x54\x00\x49\x00\x4F\x00\x4E\x00\x53\x00\x5F\x00\x52\x00\x45\x00\x53\x00\x55\x00\x4C\x00\x54\x00\x53\x00\x5F\x00\x55\x00\x52\x00\x4C\x00/\x41\x00\x43\x00\x54\x00\x49\x00\x4F\x00\x4E\x00\x53\x00\x5F\x00\x52\x00\x45\x00\x53\x00\x55\x00\x4C\x00\x54\x00\x53\x00\x5F\x00\x4F\x00\x52\x00\x4C\x00/g' /home/runner/bin/Runner.Worker.dll; \
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
      "https://github.com/cli/cli/releases/download/v${GH_VERSION}/gh_${GH_VERSION}_linux_${runner_arch}.tar.gz" \
      --output /tmp/gh.tar.gz; \
    printf '%s  %s\n' "${gh_sha256}" /tmp/gh.tar.gz | sha256sum --check; \
    tar -xzf /tmp/gh.tar.gz -C /tmp; \
    install -m 0755 /tmp/gh_${GH_VERSION}_linux_${runner_arch}/bin/gh /usr/local/bin/gh; \
    rm -rf /tmp/gh.tar.gz /tmp/gh_${GH_VERSION}_linux_${runner_arch}; \
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
    curl --fail --silent --show-error --location \
      "https://github.com/prometheus/prometheus/releases/download/v${PROMETHEUS_VERSION}/prometheus-${PROMETHEUS_VERSION}.linux-${runner_arch}.tar.gz" \
      --output /tmp/prometheus.tar.gz; \
    printf '%s  %s\n' "${prometheus_sha256}" /tmp/prometheus.tar.gz | sha256sum --check; \
    tar -xzf /tmp/prometheus.tar.gz -C /tmp; \
    install -m 0755 /tmp/prometheus-${PROMETHEUS_VERSION}.linux-${runner_arch}/promtool /usr/local/bin/promtool; \
    rm -rf /tmp/prometheus.tar.gz /tmp/prometheus-${PROMETHEUS_VERSION}.linux-${runner_arch}; \
    curl --fail --silent --show-error --location \
      "https://github.com/hadolint/hadolint/releases/download/v${HADOLINT_VERSION}/hadolint-linux-${runner_arch}" \
      --output /tmp/hadolint; \
    printf '%s  %s\n' "${hadolint_sha256}" /tmp/hadolint | sha256sum --check; \
    install -m 0755 /tmp/hadolint /usr/local/bin/hadolint; \
    rm -f /tmp/hadolint; \
    chown -R runner:runner /home/runner; \
    chmod 0777 /home/runner; \
    command -v gh; \
    command -v ldd; \
    docker --version; \
    sccache --version; \
    promtool --version; \
    hadolint --version; \
    gh --version; \
    ldd --version | grep -Eq '^ldd \(GNU libc\) [0-9]+(\.[0-9]+)+$'; \
    test -x /home/runner/run.sh

WORKDIR /home/runner
USER runner
RUN set -eux; \
    test "$(id -u)" = 1001; \
    test -x /home/runner/run.sh; \
    command -v cc; \
    command -v gcc; \
    command -v clang; \
    command -v mold; \
    command -v sccache; \
    command -v psql; \
    command -v promtool; \
    command -v hadolint; \
    command -v docker; \
    command -v gh; \
    command -v ldd; \
    command -v git; \
    command -v sudo; \
    command -v python3; \
    command -v rg; \
    test -x /usr/local/bin/clang-mold; \
    test ! -e /home/runner/.rustup; \
    test "$(grep ^docker: /etc/group | cut -d: -f3)" = 123; \
    gcc --version; \
    clang --version; \
    mold --version; \
    sccache --version; \
    psql --version; \
    promtool --version; \
    hadolint --version; \
    docker --version; \
    gh --version; \
    ldd --version | grep -Eq '^ldd \(GNU libc\) [0-9]+(\.[0-9]+)+$'; \
    python3 --version; \
    printf '%s\n' 'int main(void) { return 0; }' > /tmp/t.c; \
    clang -fuse-ld=mold -o /tmp/t /tmp/t.c; \
    /tmp/t
