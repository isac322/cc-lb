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
  companion_target="$(dirname -- "$install_target")/codex-code-mode-host"
  if [[ "$install_target" == "$companion_target" || -e "$companion_target" || -L "$companion_target" ]]; then
    printf 'Companion install target conflicts with an existing or requested binary: %s\n' "$companion_target" >&2
    exit 2
  fi
fi
target_dir=${CARGO_TARGET_DIR:-"$source_dir/codex-rs/target"}
if [[ $target_dir != /* ]]; then
  target_dir="$source_dir/codex-rs/$target_dir"
fi
# Follow the pinned upstream setup-rusty-v8 action, including its manifest trust root.
version=$(cd "$source_dir" && python3 .github/scripts/rusty_v8_bazel.py resolved-v8-crate-version)
target=${CARGO_BUILD_TARGET:-$(rustc -vV | awk '/^host:/ {print $2}')}
v8_dir="$target_dir/cc-lb-v8/$version/$target"
mkdir -p "$v8_dir"
profile=ptrcomp_sandbox_release
archive="librusty_v8_${profile}_${target}.a.gz"
binding="src_binding_${profile}_${target}.rs"
checksums="rusty_v8_${profile}_${target}.sha256"
base_url="https://github.com/openai/codex/releases/download/rusty-v8-v$version"
for artifact in "$checksums" "$archive" "$binding"; do
  if [[ ! -f "$v8_dir/$artifact" ]]; then
    curl -fsSL "$base_url/$artifact" -o "$v8_dir/$artifact.download"
    mv "$v8_dir/$artifact.download" "$v8_dir/$artifact"
  fi
done
expected=$(grep -F "  $checksums" "$source_dir/third_party/v8/rusty_v8_${version//./_}_release_manifests.sha256" | cut -d ' ' -f 1)
actual=$(shasum -a 256 "$v8_dir/$checksums" | cut -d ' ' -f 1)
[[ -n "$expected" && "$actual" == "$expected" ]]
[[ $(wc -l < "$v8_dir/$checksums") -eq 2 ]]
(cd "$v8_dir" && tr -d '\r' < "$checksums" | shasum -a 256 --check -)
export RUSTY_V8_ARCHIVE="$v8_dir/$archive"
export RUSTY_V8_SRC_BINDING_PATH="$v8_dir/$binding"
(
  cd -- "$source_dir/codex-rs"
  cargo build --locked --release -p codex-cli --bin codex -p codex-code-mode-host --bin codex-code-mode-host
)
if [[ $# == 2 ]]; then
  install -m 755 "$target_dir/release/codex" "$install_target"
  install -m 755 "$target_dir/release/codex-code-mode-host" "$companion_target"
  printf 'Installed the maintained patched binary at %s\n' "$install_target"
fi
