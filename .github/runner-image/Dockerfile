# syntax=docker/dockerfile:1.27
#
# cc-lb Actions Runner Controller image on Wolfi (glibc).
#
# ARC needs /home/runner/run.sh and uid 1001. Clang 22 / mold link Rust;
# gcc remains the default cc so autotools (jemalloc) configure correctly.
# Bake only Rustup and its official proxies; jobs install their own toolchains.
#
# Keep ARG RUNNER_VERSION in lockstep with the runner tarball digest. CI reads
# that ARG for GHCR tags.
#
# gnutar must stay installed: busybox tar rejects the -P flag that
# actions/cache passes, so cache restore and save both fail on this image.

FROM cgr.dev/chainguard/wolfi-base:latest@sha256:88a20e2d017bd20cdc0cb3910de603e7bea9512a6c351a8dd3c9df212ab03d73
LABEL org.opencontainers.image.source="https://github.com/isac322/cc-lb"

ARG TARGETARCH
ARG RUNNER_VERSION=2.338.2
ARG RUNNER_CONTAINER_HOOKS_VERSION=0.8.1
ARG DOCKER_VERSION=29.9.2
ARG BUILDX_VERSION=0.38.1
ARG SCCACHE_VERSION=0.19.0
ARG GH_VERSION=2.103.3
ARG PROMETHEUS_VERSION=3.15.1
ARG HADOLINT_VERSION=2.15.1
ARG RUSTUP_VERSION=1.29.2

# linux-arm64 runner tarball SHA-256 from the v2.338.2 release notes.
ARG RUNNER_SHA256_ARM64=670adb2f268fe21dcbb58b3db0e0aa9d5b3bddfe2c651481336e561ff86de945
ARG GH_SHA256_ARM64=ee32df2323c8847f5fac6cb6b4b69ca75828e9329a8f22f677021f7b3306ce63
ARG PROMETHEUS_SHA256_ARM64=ceb6d21493c37ca0b4513b147642797c68cf39a0f50a444d50e1e80c74f21b26
ARG HADOLINT_SHA256_ARM64=f6198ef8090f404dbb771abfee086eb8c48ac177f30da7fd3510aca35b344b5d
ARG RUSTUP_SHA256_ARM64=a9c168dbea81a84671c6b7a5c1795cb0eb699020110d107ddfca427bd797e697

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
        sccache_sha256="3e47fc4a71b1ddd1b4294a0de9d34498922fdf41e3cd0bda5df3954e3d6deb9d"; \
        rustup_target="aarch64-unknown-linux-gnu"; \
        rustup_sha256="${RUSTUP_SHA256_ARM64}"; \
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
    rustup_build="$(mktemp -d)"; \
    curl --fail --silent --show-error --location \
      "https://static.rust-lang.org/rustup/archive/${RUSTUP_VERSION}/${rustup_target}/rustup-init" \
      --output "${rustup_build}/rustup-init"; \
    printf '%s  %s\n' "${rustup_sha256}" "${rustup_build}/rustup-init" | sha256sum --check; \
    chmod 0755 "${rustup_build}/rustup-init"; \
    ( \
      export HOME="${rustup_build}/home" \
        CARGO_HOME="${rustup_build}/cargo" \
        RUSTUP_HOME="${rustup_build}/rustup"; \
      mkdir -p "${HOME}" "${CARGO_HOME}" "${RUSTUP_HOME}"; \
      env --chdir="${rustup_build}" "${rustup_build}/rustup-init" -y --no-modify-path --default-toolchain none; \
      install -m 0755 "${CARGO_HOME}/bin/rustup" /usr/local/bin/rustup; \
      for proxy in "${CARGO_HOME}"/bin/*; do \
        test "${proxy}" -ef "${CARGO_HOME}/bin/rustup"; \
        proxy_name="${proxy##*/}"; \
        if [ "${proxy_name}" != rustup ]; then \
          ln -s rustup "/usr/local/bin/${proxy_name}"; \
          test "/usr/local/bin/${proxy_name}" -ef /usr/local/bin/rustup; \
        fi; \
      done; \
      test "$(command -v rustup)" = /usr/local/bin/rustup; \
      case "$(env --chdir="${rustup_build}" rustup --version)" in "rustup ${RUSTUP_VERSION} "*) ;; *) exit 1 ;; esac; \
      test "$(env --chdir="${rustup_build}" rustup toolchain list)" = "no installed toolchains"; \
    ); \
    rm -rf "${rustup_build}"; \
    test ! -e "${rustup_build}"; \
    test ! -e /root/.cargo; \
    test ! -e /root/.rustup; \
    test ! -e /home/runner/.cargo; \
    test ! -e /home/runner/.rustup; \
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
    test "$(command -v rustup)" = /usr/local/bin/rustup; \
    test /usr/local/bin/rustc -ef /usr/local/bin/rustup; \
    test -x /usr/local/bin/clang-mold; \
    test ! -e /home/runner/.cargo; \
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
    rustup_smoke="$(mktemp -d)"; \
    ( \
      export HOME="${rustup_smoke}/home" \
        CARGO_HOME="${rustup_smoke}/cargo" \
        RUSTUP_HOME="${rustup_smoke}/rustup"; \
      mkdir -p "${HOME}" "${CARGO_HOME}" "${RUSTUP_HOME}"; \
      case "$(env --chdir="${rustup_smoke}" rustup --version)" in "rustup ${RUSTUP_VERSION} "*) ;; *) exit 1 ;; esac; \
      test "$(env --chdir="${rustup_smoke}" rustup toolchain list)" = "no installed toolchains"; \
    ); \
    rm -rf "${rustup_smoke}"; \
    test ! -e "${rustup_smoke}"; \
    test ! -e /home/runner/.cargo; \
    test ! -e /home/runner/.rustup; \
    printf '%s\n' 'int main(void) { return 0; }' > /tmp/t.c; \
    clang -fuse-ld=mold -o /tmp/t /tmp/t.c; \
    /tmp/t
