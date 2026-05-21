#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
binary_path="$root_dir/target/x86_64-unknown-linux-musl/release/cc-lb"
evidence_dir="$root_dir/.omo/evidence"
verify_log="$evidence_dir/task-50-musl-verify.log"
file_ldd_log="$evidence_dir/task-50-file-ldd.txt"
nm_log="$evidence_dir/task-50-nm.txt"
size_log="$evidence_dir/task-50-size.txt"
version_log="$evidence_dir/task-50-version.txt"
config_log="$evidence_dir/task-50-config-validate.log"
build_log="$evidence_dir/task-50-musl-static-build.log"
temp_dir="$(mktemp -d)"

cleanup() {
    rm -rf "$temp_dir"
}

trap cleanup EXIT

mkdir -p "$evidence_dir"
: > "$verify_log"

build_failed=0
build_reused_existing=0
file_failed=0
ldd_failed=0
nm_failed=0
version_failed=0
config_failed=0
size_failed=0

run_step() {
    local label="$1"
    shift
    printf '== %s ==\n' "$label" | tee -a "$verify_log"
    "$@" 2>&1 | tee -a "$verify_log"
}

capture_step() {
    local log_file="$1"
    local tee_mode="$2"
    shift 2
    set +e
    if [[ "$tee_mode" == "append" ]]; then
        run_step "$@" | tee -a "$log_file" >/dev/null
    else
        run_step "$@" | tee "$log_file" >/dev/null
    fi
    local status="${PIPESTATUS[0]}"
    set -e
    return "$status"
}

if ! run_step "build" cargo build --release --target x86_64-unknown-linux-musl -p cc-lb-server; then
    build_failed=1
    if grep -q "can't find crate for \`core\`" "$verify_log" || grep -q "can't find crate for \`std\`" "$verify_log"; then
        printf 'ERROR: x86_64-unknown-linux-musl std/core is missing locally. Use the musl container workflow or install the target in a local Rust toolchain.\n' | tee -a "$verify_log" >&2
    fi
    if [[ -x "$binary_path" ]]; then
        build_reused_existing=1
        printf 'NOTE: using existing artifact at %s for smoke evidence.\n' "$binary_path" | tee -a "$verify_log"
    else
        exit 1
    fi
fi

if [[ ! -x "$binary_path" ]]; then
    printf 'ERROR: expected binary not found at %s\n' "$binary_path" | tee -a "$verify_log" >&2
    exit 1
fi

if ! capture_step "$file_ldd_log" overwrite file file "$binary_path"; then
    file_failed=1
fi

if ! capture_step "$file_ldd_log" append ldd ldd "$binary_path"; then
    ldd_failed=1
fi

if ! grep -Eq 'static|not a dynamic executable|statically linked' "$file_ldd_log"; then
    printf 'ERROR: binary is not static according to file/ldd.\n' | tee -a "$verify_log" >&2
    file_failed=1
    ldd_failed=1
fi

if ! capture_step "$nm_log" overwrite nm nm -u "$binary_path"; then
    nm_failed=1
fi
if grep -q 'GLIBC_' "$nm_log"; then
    printf 'ERROR: GLIBC symbols found in undefined symbol table.\n' | tee -a "$verify_log" >&2
    nm_failed=1
fi

cp "$binary_path" "$temp_dir/cc-lb"
strip --strip-unneeded "$temp_dir/cc-lb"
temp_size="$(stat -c '%s' "$temp_dir/cc-lb")"
artifact_size="$(stat -c '%s' "$binary_path")"
{
    printf 'artifact %s %s bytes\n' "$binary_path" "$artifact_size"
    printf 'temp-copy %s %s bytes\n' "$temp_dir/cc-lb" "$temp_size"
} | tee "$size_log"

if (( temp_size >= 50 * 1024 * 1024 )); then
    printf 'ERROR: stripped copy exceeds 50 MiB budget.\n' | tee -a "$verify_log" >&2
    size_failed=1
fi

if ! capture_step "$version_log" overwrite version "$binary_path" --version; then
    version_failed=1
fi
if ! grep -Eq '^cc-lb [0-9]+\.[0-9]+\.[0-9]+ \(([0-9a-fA-F]{7,}|unknown)\)$' "$version_log"; then
    printf 'ERROR: --version must print semver + git SHA.\n' | tee -a "$verify_log" >&2
    version_failed=1
fi

if ! CC_LB_MASTER_KEY=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef \
CC_LB_ADMIN_TOKEN=test capture_step "$config_log" overwrite "config validate" "$binary_path" config validate --config "$root_dir/examples/cc-lb.toml"; then
    config_failed=1
fi

if [[ ! -s "$config_log" ]]; then
    printf 'ERROR: config validate produced no evidence output.\n' | tee -a "$verify_log" >&2
    config_failed=1
fi

if (( build_failed || file_failed || ldd_failed || nm_failed || version_failed || config_failed || size_failed )); then
    if (( build_failed )); then
        printf 'ERROR: fresh musl build is blocked in this environment; see build log above.\n' | tee -a "$verify_log" >&2
    fi
    exit 1
fi

printf 'PASS: musl smoke checks completed.\n' | tee -a "$verify_log"
printf 'ALL CHECKS PASSED\n' | tee -a "$verify_log"
