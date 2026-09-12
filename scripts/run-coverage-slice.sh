#!/usr/bin/env bash
set -euo pipefail

if [[ $# -eq 0 ]]; then
  echo "usage: $0 <package> [<package> ...]" >&2
  exit 2
fi

: "${LLVM_COV_ENV_FILE:?LLVM_COV_ENV_FILE must point to cargo llvm-cov show-env output}"
# shellcheck disable=SC1090
source "$LLVM_COV_ENV_FILE"
export CARGO_TARGET_DIR="$CARGO_LLVM_COV_TARGET_DIR"

args=(
  cargo nextest run
  --all-features
  --test-threads="${CARGO_BUILD_JOBS:-4}"
)

for package in "$@"; do
  args+=(--package "$package")
done

"${args[@]}"
