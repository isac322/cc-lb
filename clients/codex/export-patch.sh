#!/usr/bin/env bash
# Regenerates cc-lb-local-session.patch from a patched checkout of the pinned upstream commit.
set -euo pipefail

if [[ $# != 1 || $1 != /* ]]; then
  printf 'Usage: bash %s /absolute/patched/source-directory\n' "$0" >&2
  exit 2
fi
source_dir=$1
script_dir=$(cd -- "$(dirname -- "$0")" && pwd -P)
commit=d32063f96c2dff1dc96af839b3952abb6acfeda4

[[ $(git -C "$source_dir" rev-parse HEAD) == "$commit" ]]
# Intent-to-add makes new files visible to the diff without staging their content.
git -C "$source_dir" add --intent-to-add -- .
output=$(mktemp "$script_dir/.cc-lb-local-session.patch.XXXXXX")
trap 'rm -f -- "$output"' EXIT
git -C "$source_dir" -c core.quotePath=false diff --binary --full-index --no-color --no-ext-diff \
  --src-prefix=a/ --dst-prefix=b/ "$commit" -- . >"$output"
[[ -s $output ]]
chmod 644 -- "$output"
mv -- "$output" "$script_dir/cc-lb-local-session.patch"
trap - EXIT
printf 'Wrote %s\n' "$script_dir/cc-lb-local-session.patch"
