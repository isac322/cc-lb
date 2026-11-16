#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 || $1 != /* ]]; then
  printf 'Usage: bash %s /absolute/patched/source-directory [/absolute/new/install-path/codex]\n' "$0" >&2
  exit 2
fi
source_dir=$1
script_dir=$(cd -- "$(dirname -- "$0")" && pwd -P)
[[ $(git -C "$source_dir" rev-parse HEAD) == d32063f96c2dff1dc96af839b3952abb6acfeda4 ]]
git -C "$source_dir" apply --reverse --check "$script_dir/cc-lb-local-session.patch"
if [[ $# == 2 ]]; then
  if [[ $2 != /* || -e $2 || -L $2 ]]; then
    printf 'Install target must be a new absolute file path; existing binaries are never overwritten.\n' >&2
    exit 2
  fi
  install_target=$2
fi
(
  cd -- "$source_dir/codex-rs"
  cargo build --locked --release -p codex-cli --bin codex
)
if [[ $# == 2 ]]; then
  target_dir=${CARGO_TARGET_DIR:-"$source_dir/codex-rs/target"}
  if [[ $target_dir != /* ]]; then
    target_dir="$source_dir/codex-rs/$target_dir"
  fi
  install -m 755 "$target_dir/release/codex" "$install_target"
  printf 'Installed the maintained patched binary at %s\n' "$install_target"
fi
