#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
TARGET_DIR="$ROOT_DIR/target/test-bins"
mkdir -p "$TARGET_DIR"

# A non-auto preference is also part of the cached install identity. If the
# existing binary was installed by another package manager, rebuild its
# generated target/test-bins directory before reusing it.

installed_packages_match() {
  local client_dir=$1
  shift
  python3 - "$client_dir" "$@" <<'PY'
import json
import sys
from pathlib import Path

client_dir = Path(sys.argv[1])
for spec in sys.argv[2:]:
    name, expected = spec.rsplit("@", 1)
    package_json = client_dir / "node_modules" / name / "package.json"
    try:
        actual = json.loads(package_json.read_text())["version"]
    except (FileNotFoundError, KeyError, json.JSONDecodeError):
        sys.exit(1)
    if actual != expected:
        sys.exit(1)
PY
}

install_client() {
  local client=$1
  local package=$2
  local version=$3
  local binary=$4
  local env_name=$5
  local installer_preference=${6:-auto}
  local package_specs=("$package@$version")
  if [ "$#" -gt 6 ]; then
    package_specs+=("${@:7}")
  fi
  local client_dir="$TARGET_DIR/$client"
  local status_file="$client_dir/install-status"
  local bin_path="$client_dir/node_modules/.bin/$binary"
  mkdir -p "$client_dir"

  if [ -x "$bin_path" ] &&
    installed_packages_match "$client_dir" "${package_specs[@]}" && {
    [ "$installer_preference" = "auto" ] ||
      grep -q "installer=$installer_preference" "$status_file" 2>/dev/null
  }; then
    if [ "$installer_preference" = "auto" ]; then
      printf 'PASS %s %s@%s binary=%s\n' "$client" "$package" "$version" "$bin_path" > "$status_file"
    else
      printf 'PASS %s %s@%s binary=%s installer=%s\n' \
        "$client" "$package" "$version" "$bin_path" "$installer_preference" > "$status_file"
    fi
    printf -v "$env_name" '%s' "$bin_path"
    export "$env_name"
    return 0
  fi

  if [ "$installer_preference" != "auto" ]; then
    rm -rf "$client_dir"
    mkdir -p "$client_dir"
  fi

  printf 'installing %s from %s@%s into %s\n' "$client" "$package" "$version" "$client_dir"
  local installer
  local rc
  if [ "$installer_preference" = "npm" ]; then
    if ! command -v npm >/dev/null 2>&1 || ! npm --version >/dev/null 2>&1; then
      printf 'FAIL reason: %s: npm is required to install %s@%s reproducibly\n' \
        "$client" "$package" "$version" > "$status_file"
      printf 'FAIL reason: %s: npm is required to install %s@%s reproducibly\n' \
        "$client" "$package" "$version" >&2
      return 1
    fi
    installer=npm
    if (cd "$client_dir" && npm install "${package_specs[@]}"); then
      rc=0
    else
      rc=$?
    fi
  elif command -v bun >/dev/null 2>&1 && bun --version >/dev/null 2>&1; then
    installer=bun
    if (cd "$client_dir" && bun add "${package_specs[@]}"); then
      rc=0
    else
      rc=$?
    fi
  elif command -v npm >/dev/null 2>&1 && npm --version >/dev/null 2>&1; then
    installer=npm
    if (cd "$client_dir" && npm install "${package_specs[@]}"); then
      rc=0
    else
      rc=$?
    fi
  else
    printf 'FAIL reason: %s: neither bun nor npm is available to install %s@%s\n' \
      "$client" "$package" "$version" > "$status_file"
    printf 'FAIL reason: %s: neither bun nor npm is available to install %s@%s\n' \
      "$client" "$package" "$version" >&2
    return 1
  fi
  if [ "$rc" -eq 0 ]; then
    if [ -x "$bin_path" ] &&
      installed_packages_match "$client_dir" "${package_specs[@]}"; then
      printf 'PASS %s %s@%s binary=%s installer=%s\n' "$client" "$package" "$version" "$bin_path" "$installer" > "$status_file"
      printf -v "$env_name" '%s' "$bin_path"
      export "$env_name"
      return 0
    fi
    printf 'FAIL reason: %s: %s install did not resolve the pinned package set\n' \
      "$client" "$installer" > "$status_file"
    printf 'FAIL reason: %s: %s install did not resolve the pinned package set\n' \
      "$client" "$installer" >&2
    return 1
  fi

  printf 'FAIL reason: %s: %s install failed for %s@%s\n' \
    "$client" "$installer" "$package" "$version" > "$status_file"
  printf 'FAIL reason: %s: %s install failed for %s@%s\n' \
    "$client" "$installer" "$package" "$version" >&2
  return 1
}
install_requested() {
  local client=$1
  shift
  if [ -n "${REAL_CLIENT_ONLY:-}" ] && [ "$REAL_CLIENT_ONLY" != "$client" ]; then
    return 0
  fi
  install_client "$client" "$@"
}

install_requested claude-code @anthropic-ai/claude-code 2.1.207 claude CLAUDE_CODE_BIN
install_requested opencode opencode-ai 1.17.18 opencode OPENCODE_BIN
# pi-coding-agent's ^0.80.6 companion ranges currently resolve to incompatible
# 0.80.10 packages. Pin the published 0.80.6 package set explicitly.
install_requested pi @earendil-works/pi-coding-agent 0.80.6 pi PI_BIN npm \
  @earendil-works/pi-ai@0.80.6 \
  @earendil-works/pi-agent-core@0.80.6 \
  @earendil-works/pi-tui@0.80.6
install_requested senpi @code-yeongyu/senpi 2026.7.26 senpi SENPI_BIN
