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
  if npm install --prefix "$client_dir" "$package@$version"; then
    if [ -x "$bin_path" ]; then
      printf 'PASS %s %s@%s binary=%s\n' "$client" "$package" "$version" "$bin_path" > "$status_file"
      printf -v "$env_name" '%s' "$bin_path"
      export "$env_name"
      return 0
    fi
    printf 'SKIP %s reason: expected binary missing after npm install: %s\n' "$client" "$bin_path" > "$status_file"
    return 0
  fi

  printf 'SKIP %s reason: npm install failed for %s@%s\n' "$client" "$package" "$version" > "$status_file"
  return 0
}

install_client claude-code @anthropic-ai/claude-code 2.1.146 claude CLAUDE_CODE_BIN
install_client opencode opencode-ai 1.15.6 opencode OPENCODE_BIN
install_client pi @earendil-works/pi-coding-agent 0.75.4 pi PI_BIN
