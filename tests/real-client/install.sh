#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
TARGET_DIR="$ROOT_DIR/target/test-bins"
mkdir -p "$TARGET_DIR"

install_client() {
  local client=$1
  local package=$2
  local version=$3
  local binary=$4
  local env_name=$5
  local client_dir="$TARGET_DIR/$client"
  local status_file="$client_dir/install-status"
  local bin_path="$client_dir/node_modules/.bin/$binary"
  mkdir -p "$client_dir"

  if [ -x "$bin_path" ]; then
    printf 'PASS %s %s@%s binary=%s\n' "$client" "$package" "$version" "$bin_path" > "$status_file"
    printf -v "$env_name" '%s' "$bin_path"
    export "$env_name"
    return 0
  fi

  printf 'installing %s from %s@%s into %s\n' "$client" "$package" "$version" "$client_dir"
  local installer
  local rc
  if command -v bun >/dev/null 2>&1 && bun --version >/dev/null 2>&1; then
    installer=bun
    (cd "$client_dir" && bun add "$package@$version")
    rc=$?
  elif command -v npm >/dev/null 2>&1 && npm --version >/dev/null 2>&1; then
    installer=npm
    (cd "$client_dir" && npm install "$package@$version")
    rc=$?
  else
    printf 'SKIP %s reason: neither bun nor npm is available to install %s@%s\n' "$client" "$package" "$version" > "$status_file"
    return 0
  fi
  if [ "$rc" -eq 0 ]; then
    if [ -x "$bin_path" ]; then
      printf 'PASS %s %s@%s binary=%s installer=%s\n' "$client" "$package" "$version" "$bin_path" "$installer" > "$status_file"
      printf -v "$env_name" '%s' "$bin_path"
      export "$env_name"
      return 0
    fi
    printf 'SKIP %s reason: expected binary missing after %s install: %s\n' "$client" "$installer" "$bin_path" > "$status_file"
    return 0
  fi

  printf 'SKIP %s reason: %s install failed for %s@%s\n' "$client" "$installer" "$package" "$version" > "$status_file"
  return 0
}

install_client claude-code @anthropic-ai/claude-code 2.1.195 claude CLAUDE_CODE_BIN
install_client opencode opencode-ai 1.17.11 opencode OPENCODE_BIN
install_client pi @earendil-works/pi-coding-agent 0.80.2 pi PI_BIN
