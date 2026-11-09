#!/usr/bin/env bash
# Regression smoke test for the relocated coverage runtime archive.
#
# Usage:
#   scripts/test-coverage-runtime.sh path/to/coverage-runtime.tar.gz
#
# The archive must have been produced by coverage-runtime.sh pack on the
# builder. This test deliberately runs from the repository root, where the
# repository rust-toolchain.toml is visible, to prove that the restored
# absolute toolchain wins without a rustup component download.
set -euo pipefail

usage() {
  printf 'usage: %s path/to/coverage-runtime.tar.gz\n' "$0" >&2
  exit 2
}

fail() {
  printf 'coverage-runtime smoke: %s\n' "$*" >&2
  exit 1
}

[[ $# -eq 1 ]] || usage
archive=$1
[[ -f "$archive" ]] || fail "runtime archive not found: $archive"

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
helper="$script_dir/coverage-runtime.sh"
[[ -x "$helper" ]] || fail "runtime helper is not executable: $helper"

# Keep every generated file below one ephemeral directory owned by this test.
scratch=$(mktemp -d "${TMPDIR:-/tmp}/coverage-runtime-smoke.XXXXXX")
trap 'rm -rf "$scratch"' EXIT

# Rustup must have no usable distribution or update endpoint. The restore is
# deliberately invoked from a cold caller environment: a helper that probes
# rustup before selecting the bundled absolute toolchain must fail here rather
# than silently using an inherited installation.
export RUSTUP_DIST_SERVER=http://127.0.0.1:9/
export RUSTUP_UPDATE_ROOT=http://127.0.0.1:9/rustup/archive/
export CARGO_NET_OFFLINE=true
unset RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER RUSTUP_TOOLCHAIN CARGO_HOME
cold_rustup_home="$scratch/cold-rustup-home"
mkdir -m 700 "$cold_rustup_home"
export RUSTUP_HOME="$cold_rustup_home"
export RUSTUP_AUTO_INSTALL=1

archive_whitelist() {
  local entry clean
  while IFS= read -r entry; do
    clean=${entry#./}
    clean=${clean%/}
    [[ -n "$clean" ]] || continue
    case "$clean" in
      /*|../*|*/../*|*/..)
        fail "archive contains an unsafe path: $entry"
        ;;
    esac
    case "$clean" in
      toolchain|toolchain/*|bin|bin/*|metadata.env|checksums.sha256)
        ;;
      *)
        fail "archive contains an unapproved top-level path: $entry"
        ;;
    esac
    case "/$clean/" in
      */.cargo/*|*/.rustup/*|*/credentials*|*/config*|*/registry/*|*/git/*)
        fail "archive contains a Cargo/Rustup home or credential path: $entry"
        ;;
    esac
  done < <(tar -tzf "$archive")
}

archive_whitelist

apply_runtime_environment() {
  local env_file=$1
  local path_file=$2
  local assignment key value line
  [[ -s "$env_file" ]] || fail "restore did not write GITHUB_ENV"
  [[ -s "$path_file" ]] || fail "restore did not write GITHUB_PATH"

  while IFS= read -r assignment || [[ -n "$assignment" ]]; do
    [[ -n "$assignment" ]] || continue
    [[ "$assignment" == *=* ]] || fail "invalid GITHUB_ENV assignment: $assignment"
    key=${assignment%%=*}
    value=${assignment#*=}
    [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || fail "invalid GITHUB_ENV name: $key"
    export "$key=$value"
  done < "$env_file"

  # GITHUB_PATH prepends each line in GitHub's order. Read all entries and
  # prepend in reverse so the helper's declared order is retained locally.
  local -a path_entries=()
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ -n "$line" ]] || continue
    path_entries+=("$line")
  done < "$path_file"
  local index
  for ((index = ${#path_entries[@]} - 1; index >= 0; index--)); do
    PATH="${path_entries[index]}${PATH:+:$PATH}"
  done
  export PATH
}

invoke_restore() {
  local payload=$1
  local destination=$2
  local env_file=$3
  local path_file=$4
  : > "$env_file"
  : > "$path_file"
  env -u RUSTUP_TOOLCHAIN -u CARGO_HOME \
    GITHUB_ENV="$env_file" GITHUB_PATH="$path_file" \
    RUSTUP_HOME="$cold_rustup_home" \
    RUSTUP_DIST_SERVER="$RUSTUP_DIST_SERVER" \
    RUSTUP_UPDATE_ROOT="$RUSTUP_UPDATE_ROOT" \
    RUSTUP_AUTO_INSTALL=1 \
    "$helper" restore "$payload" "$destination"
}

restore_runtime() {
  local payload=$1
  local destination=$2
  local env_file=$3
  local path_file=$4
  invoke_restore "$payload" "$destination" "$env_file" "$path_file"
  apply_runtime_environment "$env_file" "$path_file"
  export RUSTUP_DIST_SERVER RUSTUP_UPDATE_ROOT
  export RUSTUP_AUTO_INSTALL=0
  export CARGO_NET_OFFLINE=true
}

assert_cold_rustup_home_empty() {
  [[ -z "$(find "$cold_rustup_home" -mindepth 1 -print -quit)" ]] || \
    fail "restore touched the caller's cold Rustup home"
}

assert_no_restore_staging() {
  local path
  for path in "$scratch"/.coverage-runtime.restore.* "$scratch"/.coverage-runtime.validate.*; do
    [[ ! -e "$path" ]] || fail "restore left staging state: $path"
  done
}

assert_runtime_command() {
  local command_name=$1
  local resolved
  resolved=$(command -v "$command_name") || fail "runtime command is unavailable: $command_name"
  case "$resolved" in
    "$runtime_destination/bin/"*|"$runtime_destination/toolchain/bin/"*) ;;
    *) fail "$command_name resolved outside restored runtime: $resolved" ;;
  esac
}

runtime_destination="$scratch/runtime"
env_file="$scratch/github-env"
path_file="$scratch/github-path"
restore_runtime "$archive" "$runtime_destination" "$env_file" "$path_file"
assert_cold_rustup_home_empty
assert_no_restore_staging
[[ "${RUSTUP_TOOLCHAIN:-}" == "$runtime_destination/toolchain" ]] || \
  fail "restore did not select its absolute toolchain"
[[ "${RUSTUP_HOME:-}" == "$runtime_destination/rustup" ]] || \
  fail "restore did not select its isolated Rustup home"
[[ "${CARGO_HOME:-}" == "$runtime_destination/cargo" ]] || \
  fail "restore did not select its isolated Cargo home"
[[ -d "$RUSTUP_HOME" && -z "$(find "$RUSTUP_HOME" -mindepth 1 -print -quit)" ]] || \
  fail "restored Rustup home is not empty"
[[ -d "$CARGO_HOME" && -z "$(find "$CARGO_HOME" -mindepth 1 -print -quit)" ]] || \
  fail "restored Cargo home is not empty"
export CARGO_TARGET_DIR="$scratch/cargo-target"

for command_name in rustc cargo rustup cargo-llvm-cov cargo-nextest sqlx; do
  assert_runtime_command "$command_name"
done

llvm_cov_relative=$(awk -F= '$1 == "llvm_cov_relative" { print substr($0, index($0, "=") + 1); exit }' "$runtime_destination/metadata.env")
llvm_profdata_relative=$(awk -F= '$1 == "llvm_profdata_relative" { print substr($0, index($0, "=") + 1); exit }' "$runtime_destination/metadata.env")
[[ "$llvm_cov_relative" == lib/rustlib/*/bin/llvm-cov ]] || fail "invalid restored llvm-cov path"
[[ "$llvm_profdata_relative" == lib/rustlib/*/bin/llvm-profdata ]] || fail "invalid restored llvm-profdata path"
llvm_cov_path="$runtime_destination/toolchain/$llvm_cov_relative"
llvm_profdata_path="$runtime_destination/toolchain/$llvm_profdata_relative"
[[ -x "$llvm_cov_path" && -x "$llvm_profdata_path" ]] || fail "restored LLVM tools are unavailable"

# Execute every consumer binary, rather than trusting command -v or a mocked
# version string. The calls also verify that the copied LLVM tools are usable.
rustc_version=$(rustc --version)
cargo_version=$(cargo --version)
rustup_version=$(rustup --version)
llvm_cov_version=$("$llvm_cov_path" --version)
llvm_profdata_version=$("$llvm_profdata_path" --version)
cargo_llvm_cov_version=$(cargo llvm-cov --version)
cargo_nextest_version=$(cargo nextest --version)
sqlx_version=$(sqlx --version)
[[ "$rustc_version" == rustc\ * ]] || fail "unexpected rustc output: $rustc_version"
[[ "$cargo_version" == cargo\ * ]] || fail "unexpected cargo output: $cargo_version"
[[ "$rustup_version" == rustup\ * ]] || fail "unexpected rustup output: $rustup_version"
[[ -n "$llvm_cov_version" && -n "$llvm_profdata_version" ]] || \
  fail "a relocated LLVM tool returned no version output"
[[ -n "$cargo_llvm_cov_version" && -n "$cargo_nextest_version" && -n "$sqlx_version" ]] || \
  fail "a bundled coverage CLI returned no version output"

sysroot=$(rustc --print sysroot)
[[ "$sysroot" == "$runtime_destination/toolchain" ]] || \
  fail "rustc selected an unexpected sysroot: $sysroot"

pack_failure_smoke() {
  local fault_dir="$scratch/pack-fault-bin"
  local fault_tmp="$fault_dir/tmp"
  local real_sqlx failed_archive
  real_sqlx=$(command -v sqlx)
  mkdir -p "$fault_tmp"
  cat > "$fault_dir/sqlx" <<EOF
#!/usr/bin/env bash
if [[ "\${1:-}" == "--version" ]]; then
  printf '%s\n' 'intentional sqlx version failure' >&2
  exit 47
fi
exec "$real_sqlx" "\$@"
EOF
  chmod 0755 "$fault_dir/sqlx"
  failed_archive="$scratch/pack-failure.tar.gz"
  [[ ! -e "$failed_archive" ]] || fail "pack-failure archive already exists"
  if (TMPDIR="$fault_tmp" PATH="$fault_dir:$PATH" "$helper" pack "$failed_archive"); then
    fail "pack accepted a CLI that failed its real --version probe"
  fi
  [[ ! -e "$failed_archive" ]] || \
    fail "pack left an archive after a tool-version failure"
  [[ -z "$(find "$fault_tmp" -mindepth 1 -print -quit)" ]] || \
    fail "pack left temporary probe state after a tool-version failure"
}

pack_failure_smoke

project="$scratch/offline-project"
mkdir -p "$project/src"
cat > "$project/Cargo.toml" <<'EOF'
[package]
name = "coverage-runtime-offline-smoke"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "coverage-runtime-offline-smoke"
path = "src/main.rs"
EOF
cat > "$project/src/main.rs" <<'EOF'
fn main() {
    println!("coverage-runtime-offline-ok");
}
EOF

# Use --manifest-path while staying in the repository root. This catches an
# accidental dependency on the checkout's rust-toolchain.toml or a network
# component install and uses no registry dependencies.
run_output="$scratch/offline-run.txt"
cargo run --offline --manifest-path "$project/Cargo.toml" > "$run_output"
grep -Fxq 'coverage-runtime-offline-ok' "$run_output" || \
  fail "relocated cargo did not run the dependency-free project"

make_mutated_archive() {
  local mutation=$1
  local output=$2
  local unpack="$scratch/unpack-$mutation"
  mkdir -p "$unpack"
  tar -xzf "$archive" -C "$unpack"
  case "$mutation" in
    missing-toolchain)
      rm -rf "$unpack/toolchain"
      ;;
    missing-llvm)
      local llvm_payload
      llvm_payload=$(find "$unpack/toolchain/lib/rustlib" -type f -name llvm-cov -print -quit)
      [[ -n "$llvm_payload" ]] || fail "prepared archive has no llvm-cov payload to mutate"
      rm -f "$llvm_payload"
      ;;
    missing-tool)
      [[ -e "$unpack/bin/sqlx" ]] || fail "prepared archive has no sqlx payload to mutate"
      rm -f "$unpack/bin/sqlx"
      ;;
    metadata-mismatch)
      local metadata_file metadata mutated
      metadata_file="$unpack/metadata.env"
      [[ -f "$metadata_file" ]] || fail "prepared archive has no metadata.env payload to mutate"
      metadata=$(cat "$metadata_file")
      mutated=$(printf '%s' "$metadata" | sed -E '0,/[0-9]+\.[0-9]+\.[0-9]+/s//0.0.0/')
      [[ "$mutated" != "$metadata" ]] || fail "metadata.env has no version token to mutate"
      printf '%s\n' "$mutated" > "$metadata_file"
      ;;
    *)
      fail "unknown archive mutation: $mutation"
      ;;
  esac
  local -a members=(metadata.env checksums.sha256 bin)
  [[ -e "$unpack/toolchain" ]] && members+=(toolchain)
  tar -czf "$output" -C "$unpack" "${members[@]}"
}

expect_restore_failure() {
  local payload=$1
  local mutation=$2
  local destination="$scratch/reject-$mutation"
  local failed_env="$scratch/reject-$mutation.env"
  local failed_path="$scratch/reject-$mutation.path"
  if (
    invoke_restore "$payload" "$destination" "$failed_env" "$failed_path"
  ); then
    fail "$mutation payload was accepted"
  fi
  assert_cold_rustup_home_empty
  assert_no_restore_staging
  [[ ! -e "$destination/toolchain/bin/rustc" ]] || \
    fail "$mutation payload installed a compiler before failing: $mutation"
}

for mutation in missing-toolchain missing-llvm missing-tool metadata-mismatch; do
  mutated_archive="$scratch/$mutation.tar.gz"
  make_mutated_archive "$mutation" "$mutated_archive"
  expect_restore_failure "$mutated_archive" "$mutation"
done

# A nonempty destination is unowned state. Restore must reject it rather than
# replacing or partially overwriting it.
occupied="$scratch/occupied"
mkdir -p "$occupied"
printf 'must-survive\n' > "$occupied/sentinel"
if (
  occupied_env="$scratch/occupied.env"
  occupied_path="$scratch/occupied.path"
  invoke_restore "$archive" "$occupied" "$occupied_env" "$occupied_path"
); then
  fail "restore overwrote a nonempty destination"
fi
assert_cold_rustup_home_empty
assert_no_restore_staging
[[ -f "$occupied/sentinel" ]] || fail "restore removed unowned destination state"
grep -Fxq 'must-survive' "$occupied/sentinel" || fail "restore changed unowned destination state"

printf 'coverage-runtime smoke: PASS (offline consumer and fail-closed payload checks)\n'
