#!/usr/bin/env bash
set -euo pipefail

if [[ $# != 1 || $1 != /* ]]; then
  printf 'Usage: bash %s /absolute/new/source-directory\n' "$0" >&2
  exit 2
fi
source_dir=$1
if [[ -e "$source_dir" || -L "$source_dir" ]]; then
  printf 'Refusing to overwrite an existing source path.\n' >&2
  exit 2
fi
script_dir=$(cd -- "$(dirname -- "$0")" && pwd -P)
commit=d32063f96c2dff1dc96af839b3952abb6acfeda4
tree=89a3702732fed79957e519b5464ba7e9ac0e42d4
tag=a41fe5b443652cbdbee57717fbb9fd78d8d8b32a

git clone --filter=blob:none --depth=1 --no-checkout --branch rust-v0.169.0 \
  https://github.com/openai/codex.git "$source_dir"
[[ $(git -C "$source_dir" rev-parse refs/tags/rust-v0.169.0) == "$tag" ]]
[[ $(git -C "$source_dir" rev-parse 'refs/tags/rust-v0.169.0^{commit}') == "$commit" ]]
[[ $(git -C "$source_dir" rev-parse "$commit^{tree}") == "$tree" ]]
git -C "$source_dir" checkout --detach "$commit"
git -C "$source_dir" apply --check "$script_dir/cc-lb-local-session.patch"
git -C "$source_dir" apply "$script_dir/cc-lb-local-session.patch"
printf 'Prepared pinned Codex source with the maintained cc-lb patch at %s\n' "$source_dir"
printf 'Build with: bash %q/build.sh %q\n' "$script_dir" "$source_dir"
