#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
toolchain_version="$(grep -E '^[[:space:]]*channel[[:space:]]*=' "$repo_root/rust-toolchain.toml" | sed -E 's/.*"([^"]+)".*/\1/')"
docker_version="$(grep -E '^FROM .* rust:[0-9]+\.[0-9]+\.[0-9]+-alpine AS builder$' "$repo_root/Dockerfile" | sed -E 's/.*rust:([^[:space:]]+)-alpine.*/\1/')"

if [ "$docker_version" != "$toolchain_version" ]; then
  echo "ERROR: Dockerfile Rust $docker_version does not match rust-toolchain.toml $toolchain_version" >&2
  exit 1
fi

echo "Dockerfile Rust version matches rust-toolchain.toml: $toolchain_version"
