#!/usr/bin/env bash
# Guard against silently-dropped integration tests.
#
# Consolidated crates set `autotests = false` and compile every top-level
# integration test (crates/<c>/tests/*.rs) as one binary via tests/all.rs. The
# downside of `autotests = false` is that a NEWLY added tests/*.rs is no longer
# auto-discovered: if it is not also wired into tests/all.rs it is compiled by
# nobody and run by nobody — a silent loss of test coverage. This check fails
# CI when that happens, so the consolidation cannot rot.
#
# Usage: scripts/check-integration-test-consolidation.sh
# Exit:  0 = every consolidated crate is fully wired; 1 = unreferenced file(s).
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

fail=0
while IFS= read -r manifest; do
  tests_dir="$(dirname "$manifest")/tests"
  all_rs="$tests_dir/all.rs"
  [ -d "$tests_dir" ] || continue

  if [ ! -f "$all_rs" ]; then
    echo "ERROR: $(dirname "$manifest") sets autotests=false but has no tests/all.rs" >&2
    fail=1
    continue
  fi

  for f in "$tests_dir"/*.rs; do
    [ -e "$f" ] || continue
    base="$(basename "$f")"
    [ "$base" = "all.rs" ] && continue
    if ! grep -qF "\"$base\"" "$all_rs"; then
      echo "ERROR: $f is not referenced in $all_rs (autotests=false would drop it)" >&2
      fail=1
    fi
  done
done < <(grep -rlE '^[[:space:]]*autotests[[:space:]]*=[[:space:]]*false' --include=Cargo.toml crates tests || true)

if [ "$fail" -ne 0 ]; then
  {
    echo ""
    echo "Integration-test consolidation guard FAILED."
    echo "Wire each file above into its crate's tests/all.rs, e.g.:"
    echo '  #[path = "my_new_test.rs"]'
    echo '  mod my_new_test;'
  } >&2
  exit 1
fi

echo "Integration-test consolidation guard OK: all consolidated crates fully wired."
