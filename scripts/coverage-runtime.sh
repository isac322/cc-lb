#!/usr/bin/env bash
set -euo pipefail

# The coverage archive is deliberately a small, self-contained runtime. It is
# consumed by jobs that must not ask rustup, cargo, or a package manager to
# contact the network again.

FORMAT_VERSION=1

fail() {
  echo "coverage-runtime: $*" >&2
  exit 1
}

usage() {
  cat >&2 <<'EOF'
usage:
  scripts/coverage-runtime.sh pack <archive.tar.gz>
  scripts/coverage-runtime.sh restore <archive.tar.gz> <fresh-destination>
EOF
  exit 2
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "required command is unavailable: $1"
}

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum -- "$1" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 -- "$1" | awk '{print $1}'
  else
    fail "required checksum command is unavailable (sha256sum or shasum)"
  fi
}

sha256_text() {
  local text=$1
  if command -v sha256sum >/dev/null 2>&1; then
    printf '%s' "$text" | sha256sum | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    printf '%s' "$text" | shasum -a 256 | awk '{print $1}'
  else
    fail "required checksum command is unavailable (sha256sum or shasum)"
  fi
}

real_file() {
  local path=$1
  [[ -f "$path" && -x "$path" ]] || fail "expected executable file: $path"
  realpath -e -- "$path"
}

resolve_executable() {
  local name=$1
  local path
  path="$(type -P -- "$name" || true)"
  [[ -n "$path" ]] || fail "required executable is unavailable: $name"
  real_file "$(realpath -e -- "$path")"
}
runtime_command() {
  local rustup_home=$1
  local toolchain=$2
  local runtime_path=$3
  local executable=$4
  shift 4
  local effective_path=${PATH:-}
  if [[ -n "$runtime_path" ]]; then
    effective_path="$runtime_path${effective_path:+:$effective_path}"
  fi
  RUSTUP_HOME="$rustup_home" \
    RUSTUP_TOOLCHAIN="$toolchain" \
    RUSTUP_AUTO_INSTALL=0 \
    RUSTUP_DIST_SERVER=http://127.0.0.1:9/ \
    RUSTUP_UPDATE_ROOT=http://127.0.0.1:9/rustup/archive/ \
    CARGO_HOME="$rustup_home/cargo" \
    CARGO_NET_OFFLINE=true \
    PATH="$effective_path" \
    "$executable" "$@"
}

runtime_version_hash() {
  local rustup_home=$1
  local toolchain=$2
  local runtime_path=$3
  local executable=$4
  shift 4
  local output
  output="$(runtime_command "$rustup_home" "$toolchain" "$runtime_path" "$executable" "$@")" || \
    fail "could not query version: $executable"
  sha256_text "$output"
}

first_llvm_tool() {
  local name=$1
  local path
  while IFS= read -r path; do
    [[ -x "$path" ]] || continue
    printf '%s\n' "$path"
    return 0
  done < <(find "$2/lib/rustlib" \( -type f -o -type l \) -name "$name" -print | LC_ALL=C sort)
  fail "Rust toolchain is missing executable LLVM tool: $name"
}

absolute_destination() {
  local destination=$1
  local parent base
  [[ "$destination" != */ ]] || fail "destination must not have a trailing slash"
  parent=$(dirname -- "$destination")
  base=$(basename -- "$destination")
  [[ "$base" != . && "$base" != .. && -n "$base" ]] || fail "invalid destination: $destination"
  [[ -d "$parent" ]] || fail "destination parent does not exist: $parent"
  printf '%s/%s\n' "$(cd -- "$parent" && pwd -P)" "$base"
}

write_runtime_environment() {
  local destination=$1
  local rustup_home="$destination/rustup"
  local cargo_home="$destination/cargo"
  mkdir -m 700 -p -- "$rustup_home" "$cargo_home"

  if [[ -n "${GITHUB_ENV:-}" ]]; then
    printf 'RUSTUP_TOOLCHAIN=%s\n' "$destination/toolchain" >>"$GITHUB_ENV"
    printf 'RUSTUP_HOME=%s\n' "$rustup_home" >>"$GITHUB_ENV"
    printf 'RUSTUP_AUTO_INSTALL=0\n' >>"$GITHUB_ENV"
    printf 'CARGO_HOME=%s\n' "$cargo_home" >>"$GITHUB_ENV"
  fi
  if [[ -n "${GITHUB_PATH:-}" ]]; then
    printf '%s\n' "$destination/bin" >>"$GITHUB_PATH"
    printf '%s\n' "$destination/toolchain/bin" >>"$GITHUB_PATH"
  fi

  echo "coverage-runtime: restored runtime at $destination"
  echo "coverage-runtime: PATH entries: $destination/bin and $destination/toolchain/bin"
}

pack_runtime() {
  [[ $# -eq 1 ]] || usage
  local archive=$1
  require_command rustc
  require_command rustup
  require_command tar
  require_command realpath
  require_command find
  [[ ! -e "$archive" && ! -L "$archive" ]] || fail "refusing to overwrite existing archive: $archive"
  [[ -d "$(dirname -- "$archive")" ]] || fail "archive parent does not exist: $(dirname -- "$archive")"

  local sysroot sysroot_real
  sysroot=$(RUSTUP_AUTO_INSTALL=0 RUSTUP_DIST_SERVER=http://127.0.0.1:9/ RUSTUP_UPDATE_ROOT=http://127.0.0.1:9/rustup/archive/ rustc --print sysroot) || fail "could not resolve the active Rust sysroot"
  sysroot_real=$(realpath -e -- "$sysroot")
  [[ -d "$sysroot_real" ]] || fail "Rust sysroot is not a directory: $sysroot_real"
  [[ -f "$sysroot_real/bin/rustc" && -x "$sysroot_real/bin/rustc" ]] || fail "Rust sysroot has no compiler: $sysroot_real/bin/rustc"
  [[ -f "$sysroot_real/bin/cargo" && -x "$sysroot_real/bin/cargo" ]] || fail "Rust sysroot has no cargo: $sysroot_real/bin/cargo"

  local llvm_cov llvm_profdata llvm_cov_rel llvm_profdata_rel
  llvm_cov=$(first_llvm_tool llvm-cov "$sysroot_real")
  llvm_profdata=$(first_llvm_tool llvm-profdata "$sysroot_real")
  llvm_cov_rel=${llvm_cov#"$sysroot_real"/}
  llvm_profdata_rel=${llvm_profdata#"$sysroot_real"/}
  [[ "$llvm_cov_rel" != "$llvm_cov" && "$llvm_profdata_rel" != "$llvm_profdata" ]] || fail "LLVM tool is outside the Rust sysroot"

  local cargo_llvm_cov cargo_nextest sqlx rustup_bin
  cargo_llvm_cov=$(resolve_executable cargo-llvm-cov)
  cargo_nextest=$(resolve_executable cargo-nextest)
  sqlx=$(resolve_executable sqlx)
  rustup_bin=$(resolve_executable rustup)

  local probe_home
  probe_home=$(mktemp -d "${TMPDIR:-/tmp}/coverage-runtime.pack-probe.XXXXXX")
  local cleanup_trap
  printf -v cleanup_trap 'rm -rf -- %q' "$probe_home"
  trap "$cleanup_trap" EXIT

  local rustc_vv rustc_release rustc_host rustc_vv_sha256
  rustc_vv=$("$sysroot_real/bin/rustc" -vV) || fail "could not query the active compiler"
  rustc_release=$(awk '$1 == "release:" { print $2; exit }' <<<"$rustc_vv")
  rustc_host=$(awk '$1 == "host:" { print $2; exit }' <<<"$rustc_vv")
  [[ -n "$rustc_release" && -n "$rustc_host" ]] || fail "rustc -vV did not provide release and host metadata"
  rustc_vv_sha256=$(sha256_text "$rustc_vv")

  # Probe every CLI before copying the potentially large sysroot. cargo-llvm-cov
  # and cargo-nextest are cargo subcommands, not standalone --version CLIs.
  local cargo_llvm_cov_version_sha cargo_nextest_version_sha
  local sqlx_version_sha rustup_version_sha
  local probe_path
  probe_path="$(dirname -- "$cargo_llvm_cov"):$sysroot_real/bin"
  cargo_llvm_cov_version_sha=$(runtime_version_hash "$probe_home" "$sysroot_real" "$probe_path" "$cargo_llvm_cov" llvm-cov --version)
  cargo_nextest_version_sha=$(runtime_version_hash "$probe_home" "$sysroot_real" "$probe_path" "$cargo_nextest" nextest --version)
  sqlx_version_sha=$(runtime_version_hash "$probe_home" "$sysroot_real" "$probe_path" "$sqlx" --version)
  rustup_version_sha=$(runtime_version_hash "$probe_home" "$sysroot_real" "$probe_path" "$rustup_bin" --version)

  local staging
  staging=$(mktemp -d "${TMPDIR:-/tmp}/coverage-runtime.pack.XXXXXX")
  printf -v cleanup_trap 'rm -rf -- %q %q' "$staging" "$probe_home"
  trap "$cleanup_trap" EXIT
  mkdir -p -- "$staging/bin" "$staging/toolchain"

  # Dereference the sysroot while copying. The archive must not contain links
  # that point into the builder's Rustup/Cargo homes or outside the payload.
  cp -RL -- "$sysroot_real/." "$staging/toolchain/"
  if find "$staging/toolchain" -type l -print -quit | grep -q .; then
    fail "copied Rust toolchain still contains a symlink"
  fi
  install -m 0755 -- "$cargo_llvm_cov" "$staging/bin/cargo-llvm-cov"
  install -m 0755 -- "$cargo_nextest" "$staging/bin/cargo-nextest"
  install -m 0755 -- "$sqlx" "$staging/bin/sqlx"
  install -m 0755 -- "$rustup_bin" "$staging/bin/rustup"

  local llvm_cov_sha llvm_profdata_sha
  local cargo_llvm_cov_sha cargo_nextest_sha sqlx_sha rustup_sha
  rustc_vv_sha256=$(sha256_text "$rustc_vv")
  llvm_cov_sha=$(sha256_file "$staging/toolchain/$llvm_cov_rel")
  llvm_profdata_sha=$(sha256_file "$staging/toolchain/$llvm_profdata_rel")
  cargo_llvm_cov_sha=$(sha256_file "$staging/bin/cargo-llvm-cov")
  cargo_nextest_sha=$(sha256_file "$staging/bin/cargo-nextest")
  sqlx_sha=$(sha256_file "$staging/bin/sqlx")
  rustup_sha=$(sha256_file "$staging/bin/rustup")

  cat >"$staging/metadata.env" <<EOF
format_version=$FORMAT_VERSION
rustc_release=$rustc_release
rustc_host=$rustc_host
rustc_vv_sha256=$rustc_vv_sha256
llvm_cov_relative=$llvm_cov_rel
llvm_cov_sha256=$llvm_cov_sha
llvm_profdata_relative=$llvm_profdata_rel
llvm_profdata_sha256=$llvm_profdata_sha
cargo_llvm_cov_sha256=$cargo_llvm_cov_sha
cargo_llvm_cov_version_sha256=$cargo_llvm_cov_version_sha
cargo_nextest_sha256=$cargo_nextest_sha
cargo_nextest_version_sha256=$cargo_nextest_version_sha
sqlx_sha256=$sqlx_sha
sqlx_version_sha256=$sqlx_version_sha
rustup_sha256=$rustup_sha
rustup_version_sha256=$rustup_version_sha
EOF

  local rustc_checksum llvm_cov_checksum llvm_profdata_checksum
  local cargo_llvm_cov_checksum cargo_nextest_checksum sqlx_checksum rustup_checksum
  rustc_checksum=$(sha256_file "$staging/toolchain/bin/rustc")
  llvm_cov_checksum=$(sha256_file "$staging/toolchain/$llvm_cov_rel")
  llvm_profdata_checksum=$(sha256_file "$staging/toolchain/$llvm_profdata_rel")
  cargo_llvm_cov_checksum=$(sha256_file "$staging/bin/cargo-llvm-cov")
  cargo_nextest_checksum=$(sha256_file "$staging/bin/cargo-nextest")
  sqlx_checksum=$(sha256_file "$staging/bin/sqlx")
  rustup_checksum=$(sha256_file "$staging/bin/rustup")
  {
    printf '%s  %s\n' "$cargo_llvm_cov_checksum" bin/cargo-llvm-cov
    printf '%s  %s\n' "$cargo_nextest_checksum" bin/cargo-nextest
    printf '%s  %s\n' "$sqlx_checksum" bin/sqlx
    printf '%s  %s\n' "$rustup_checksum" bin/rustup
    printf '%s  %s\n' "$rustc_checksum" toolchain/bin/rustc
    printf '%s  %s\n' "$llvm_cov_checksum" "toolchain/$llvm_cov_rel"
    printf '%s  %s\n' "$llvm_profdata_checksum" "toolchain/$llvm_profdata_rel"
  } >"$staging/checksums.sha256"

  tar -czf "$archive" -C "$staging" metadata.env checksums.sha256 bin toolchain
  trap - EXIT
  rm -rf -- "$staging" "$probe_home"
  echo "coverage-runtime: packed $archive"
}

metadata_value() {
  local key=$1
  local metadata=$2
  awk -F= -v wanted="$key" '$1 == wanted { print substr($0, index($0, "=") + 1); found=1 } END { if (!found) exit 1 }' "$metadata"
}

validate_archive_names() {
  local archive=$1
  local entry normalized
  while IFS= read -r entry; do
    [[ -n "$entry" ]] || fail "archive contains an empty path"
    normalized=${entry#./}
    [[ "$normalized" != /* ]] || fail "archive contains an absolute path: $entry"
    case "$normalized" in
      .|..|../*|*/../*|*/..|*\\*) fail "archive contains an unsafe path: $entry" ;;
      metadata.env|checksums.sha256|bin|bin/*|toolchain|toolchain/*) ;;
      *) fail "archive contains an unexpected path: $entry" ;;
    esac
  done < <(LC_ALL=C tar -tzf "$archive")

  while IFS= read -r entry; do
    case "${entry:0:1}" in
      l|h|b|c|p|s) fail "archive contains a non-regular link or device: $entry" ;;
    esac
  done < <(LC_ALL=C tar -tvzf "$archive")
}

validate_metadata() {
  local metadata=$1
  declare -A values=()
  local line key value
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ "$line" =~ ^([a-z][a-z0-9_]*)=([A-Za-z0-9._+:/-]+)$ ]] || fail "invalid metadata line"
    key=${BASH_REMATCH[1]}
    value=${BASH_REMATCH[2]}
    [[ -z "${values[$key]+present}" ]] || fail "duplicate metadata key: $key"
    values[$key]=$value
  done <"$metadata"

  local required
  for required in \
    format_version rustc_release rustc_host rustc_vv_sha256 \
    llvm_cov_relative llvm_cov_sha256 llvm_profdata_relative llvm_profdata_sha256 \
    cargo_llvm_cov_sha256 cargo_llvm_cov_version_sha256 \
    cargo_nextest_sha256 cargo_nextest_version_sha256 \
    sqlx_sha256 sqlx_version_sha256 rustup_sha256 rustup_version_sha256; do
    [[ -n "${values[$required]+present}" ]] || fail "metadata is missing key: $required"
  done
  [[ "${values[format_version]}" == "$FORMAT_VERSION" ]] || fail "unsupported archive format version"
  [[ "${values[llvm_cov_relative]}" == lib/rustlib/*/bin/llvm-cov && "${values[llvm_cov_relative]}" != *..* ]] || fail "invalid llvm-cov metadata path"
  [[ "${values[llvm_profdata_relative]}" == lib/rustlib/*/bin/llvm-profdata && "${values[llvm_profdata_relative]}" != *..* ]] || fail "invalid llvm-profdata metadata path"
  printf '%s\n' "${values[rustc_release]}" "${values[rustc_host]}" "${values[rustc_vv_sha256]}" \
    "${values[llvm_cov_relative]}" "${values[llvm_cov_sha256]}" \
    "${values[llvm_profdata_relative]}" "${values[llvm_profdata_sha256]}" \
    "${values[cargo_llvm_cov_sha256]}" "${values[cargo_llvm_cov_version_sha256]}" \
    "${values[cargo_nextest_sha256]}" "${values[cargo_nextest_version_sha256]}" \
    "${values[sqlx_sha256]}" "${values[sqlx_version_sha256]}" \
    "${values[rustup_sha256]}" "${values[rustup_version_sha256]}"
}

restore_runtime() {
  [[ $# -eq 2 ]] || usage
  local archive=$1
  local destination
  destination=$(absolute_destination "$2")
  require_command tar
  require_command realpath
  require_command find
  [[ -f "$archive" ]] || fail "runtime archive does not exist: $archive"
  validate_archive_names "$archive"

  if [[ -L "$destination" ]]; then
    fail "refusing to restore through a destination symlink: $destination"
  fi
  if [[ -e "$destination" ]]; then
    [[ -d "$destination" ]] || fail "destination is not a directory: $destination"
    [[ -z "$(find "$destination" -mindepth 1 -print -quit)" ]] || fail "destination is not empty: $destination"
    [[ -O "$destination" ]] || fail "destination is not owned by the current user: $destination"
  fi

  local parent staging validation_home cleanup_trap
  parent=$(dirname -- "$destination")
  staging=$(mktemp -d "$parent/.coverage-runtime.restore.XXXXXX")
  printf -v cleanup_trap 'rm -rf -- %q' "$staging"
  trap "$cleanup_trap" EXIT
  validation_home=$(mktemp -d "$parent/.coverage-runtime.validate.XXXXXX")
  mkdir -m 700 -p -- "$validation_home/cargo"
  printf -v cleanup_trap 'rm -rf -- %q %q' "$staging" "$validation_home"
  trap "$cleanup_trap" EXIT
  tar -xzf "$archive" --no-same-owner --no-same-permissions -C "$staging"
  find "$staging" -type l -print -quit | grep -q . && fail "restored archive contains a symlink"

  [[ -f "$staging/metadata.env" && -f "$staging/checksums.sha256" ]] || fail "archive is missing required metadata"
  [[ -d "$staging/bin" && -d "$staging/toolchain" ]] || fail "archive is missing required runtime directories"
  local metadata_values
  metadata_values=$(validate_metadata "$staging/metadata.env")

  local rustc_release rustc_host rustc_vv_sha256
  rustc_release=$(sed -n '1p' <<<"$metadata_values")
  rustc_host=$(sed -n '2p' <<<"$metadata_values")
  rustc_vv_sha256=$(sed -n '3p' <<<"$metadata_values")
  local llvm_cov_rel llvm_cov_sha llvm_profdata_rel llvm_profdata_sha
  llvm_cov_rel=$(sed -n '4p' <<<"$metadata_values")
  llvm_cov_sha=$(sed -n '5p' <<<"$metadata_values")
  llvm_profdata_rel=$(sed -n '6p' <<<"$metadata_values")
  llvm_profdata_sha=$(sed -n '7p' <<<"$metadata_values")
  local cargo_llvm_cov_sha cargo_llvm_cov_version_sha
  local cargo_nextest_sha cargo_nextest_version_sha sqlx_sha sqlx_version_sha
  local rustup_sha rustup_version_sha
  cargo_llvm_cov_sha=$(metadata_value cargo_llvm_cov_sha256 "$staging/metadata.env")
  cargo_llvm_cov_version_sha=$(metadata_value cargo_llvm_cov_version_sha256 "$staging/metadata.env")
  cargo_nextest_sha=$(metadata_value cargo_nextest_sha256 "$staging/metadata.env")
  cargo_nextest_version_sha=$(metadata_value cargo_nextest_version_sha256 "$staging/metadata.env")
  sqlx_sha=$(metadata_value sqlx_sha256 "$staging/metadata.env")
  sqlx_version_sha=$(metadata_value sqlx_version_sha256 "$staging/metadata.env")
  rustup_sha=$(metadata_value rustup_sha256 "$staging/metadata.env")
  rustup_version_sha=$(metadata_value rustup_version_sha256 "$staging/metadata.env")

  local name expected actual
  for name in cargo-llvm-cov cargo-nextest sqlx rustup; do
    [[ -f "$staging/bin/$name" && -x "$staging/bin/$name" ]] || fail "archive is missing executable: bin/$name"
  done
  [[ -f "$staging/toolchain/bin/rustc" && -x "$staging/toolchain/bin/rustc" ]] || fail "archive is missing compiler"
  [[ -f "$staging/toolchain/bin/cargo" && -x "$staging/toolchain/bin/cargo" ]] || fail "archive is missing cargo"
  [[ -f "$staging/toolchain/$llvm_cov_rel" && -x "$staging/toolchain/$llvm_cov_rel" ]] || fail "archive is missing llvm-cov"
  [[ -f "$staging/toolchain/$llvm_profdata_rel" && -x "$staging/toolchain/$llvm_profdata_rel" ]] || fail "archive is missing llvm-profdata"

  while read -r expected path; do
    [[ "$expected" =~ ^[0-9a-fA-F]{64}$ && "$path" != /* && "$path" != *..* ]] || fail "invalid checksum manifest entry"
    [[ -f "$staging/$path" ]] || fail "checksum manifest references missing file: $path"
    actual=$(sha256_file "$staging/$path")
    [[ "$actual" == "$expected" ]] || fail "checksum mismatch: $path"
  done <"$staging/checksums.sha256"

  local runtime_path="$staging/bin:$staging/toolchain/bin"
  local runtime_vv runtime_release runtime_host
  runtime_vv=$(runtime_command "$validation_home" "$staging/toolchain" "$runtime_path" "$staging/toolchain/bin/rustc" -vV) || \
    fail "restored compiler cannot execute"
  runtime_release=$(awk '$1 == "release:" { print $2; exit }' <<<"$runtime_vv")
  runtime_host=$(awk '$1 == "host:" { print $2; exit }' <<<"$runtime_vv")
  [[ "$runtime_release" == "$rustc_release" ]] || fail "compiler release does not match metadata"
  [[ "$runtime_host" == "$rustc_host" ]] || fail "compiler host does not match metadata"
  [[ "$(sha256_text "$runtime_vv")" == "$rustc_vv_sha256" ]] || fail "compiler version metadata mismatch"
  [[ "$(sha256_file "$staging/toolchain/$llvm_cov_rel")" == "$llvm_cov_sha" ]] || fail "llvm-cov metadata mismatch"
  [[ "$(sha256_file "$staging/toolchain/$llvm_profdata_rel")" == "$llvm_profdata_sha" ]] || fail "llvm-profdata metadata mismatch"

  # The preceding manifest checks tool identity and metadata. Querying each
  # binary here also rejects a payload that has the right filename but cannot
  # execute on this runner. Every probe uses the isolated bundle context so
  # rustup cannot consult or mutate the caller's home.
  local tool_version_sha
  [[ "$(sha256_file "$staging/bin/cargo-llvm-cov")" == "$cargo_llvm_cov_sha" ]] || fail "cargo-llvm-cov metadata mismatch"
  tool_version_sha=$(runtime_version_hash "$validation_home" "$staging/toolchain" "$runtime_path" "$staging/bin/cargo-llvm-cov" llvm-cov --version)
  [[ "$tool_version_sha" == "$cargo_llvm_cov_version_sha" ]] || fail "cargo-llvm-cov version metadata mismatch"
  [[ "$(sha256_file "$staging/bin/cargo-nextest")" == "$cargo_nextest_sha" ]] || fail "cargo-nextest metadata mismatch"
  tool_version_sha=$(runtime_version_hash "$validation_home" "$staging/toolchain" "$runtime_path" "$staging/bin/cargo-nextest" nextest --version)
  [[ "$tool_version_sha" == "$cargo_nextest_version_sha" ]] || fail "cargo-nextest version metadata mismatch"
  [[ "$(sha256_file "$staging/bin/sqlx")" == "$sqlx_sha" ]] || fail "sqlx metadata mismatch"
  tool_version_sha=$(runtime_version_hash "$validation_home" "$staging/toolchain" "$runtime_path" "$staging/bin/sqlx" --version)
  [[ "$tool_version_sha" == "$sqlx_version_sha" ]] || fail "sqlx version metadata mismatch"
  [[ "$(sha256_file "$staging/bin/rustup")" == "$rustup_sha" ]] || fail "rustup metadata mismatch"
  tool_version_sha=$(runtime_version_hash "$validation_home" "$staging/toolchain" "$runtime_path" "$staging/bin/rustup" --version)
  [[ "$tool_version_sha" == "$rustup_version_sha" ]] || fail "rustup version metadata mismatch"

  rm -rf -- "$validation_home"
  if [[ -e "$destination" ]]; then
    rmdir -- "$destination" || fail "destination changed while validating runtime"
  fi
  mv -- "$staging" "$destination"
  trap - EXIT
  write_runtime_environment "$destination"
}

[[ $# -ge 1 ]] || usage
case "$1" in
  pack) shift; pack_runtime "$@" ;;
  restore) shift; restore_runtime "$@" ;;
  *) usage ;;
esac
