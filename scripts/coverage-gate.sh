#!/usr/bin/env bash
# coverage-gate.sh — per-package line-coverage gate for cc-lb.
#
# Reads a cargo-llvm-cov lcov tracefile and enforces a minimum line-coverage
# percentage per workspace package. Replaces 11 sequential
# `cargo llvm-cov report --package <p> --fail-under-lines <n>` calls (each of
# which re-runs a full llvm-cov export over the profile data) with a single
# pass over the lcov the coverage job already generates. The per-package numbers
# are byte-identical to those report calls (lcov line semantics == the report's
# line coverage), so this is a drop-in with no threshold recalibration.
#
# The PACKAGE -> MIN table below is the source of truth for the coverage policy.
#
# Exit status:
#   0  every gated package meets its threshold
#   1  at least one gated package is below its threshold
#   2  usage error, or a gated package has no coverage data (fail loud rather
#      than silently pass)
#
# Usage:
#   scripts/coverage-gate.sh [path/to/coverage.lcov]   # default: target/coverage.lcov
set -euo pipefail

lcov="${1:-target/coverage.lcov}"

if [[ ! -f "$lcov" ]]; then
  echo "coverage-gate: lcov tracefile not found: $lcov" >&2
  exit 2
fi

awk '
function set(p, n) { thr[p] = n; order[++np] = p }
BEGIN {
  # PACKAGE -> minimum line coverage %. Mirrors the previous per-package gates.
  set("cc-lb-engine", 60);                 set("cc-lb-dialect-anthropic", 80)
  set("cc-lb-admin", 60);                  set("cc-lb-config", 60)
  set("cc-lb-observability", 60);          set("cc-lb-signer-anthropic-key", 60)
  set("cc-lb-signer-anthropic-oauth", 60); set("cc-lb-server", 60)
  set("cc-lb-storage-sqlite", 60);         set("cc-lb-storage-postgres", 60)
  set("cc-lb-scheduler", 60)
}

# lcov record: SF:<file> ... LF:<lines found> LH:<lines hit> ... end_of_record.
# Attribute each source file to its workspace package via the /crates/<pkg>/
# path segment (the tracefile is already filtered by --ignore-filename-regex).
/^SF:/ {
  cur = ""
  # String regexp: busybox awk treats /\/crates\/[^/]+\// as an unterminated
  # pattern because / inside [^/] closes the slash-delimited regex.
  if (match($0, "/crates/[^/]+/")) {
    cur = substr($0, RSTART + 8, RLENGTH - 9)
  }
  next
}
/^LF:/ { if (cur != "") lf[cur] += substr($0, 4) + 0; next }
/^LH:/ { if (cur != "") lh[cur] += substr($0, 4) + 0; next }
$0 == "end_of_record" { cur = ""; next }

END {
  fail = 0; nodata = 0
  printf "%-32s %8s %8s %9s %5s  %s\n", "package", "hit", "found", "line%", "min", "verdict"
  for (i = 1; i <= np; i++) {
    p = order[i]
    if (!(p in lf) || lf[p] == 0) {
      printf "%-32s %8s %8s %9s %5d  %s\n", p, "-", "0", "NO DATA", thr[p], "ERROR"
      nodata = 1
      continue
    }
    pct = 100.0 * lh[p] / lf[p]
    v = (pct + 1e-9 >= thr[p]) ? "PASS" : "FAIL"
    if (v == "FAIL") fail = 1
    printf "%-32s %8d %8d %8.2f%% %5d  %s\n", p, lh[p], lf[p], pct, thr[p], v
  }
  if (nodata) { print "coverage-gate: a gated package produced no coverage data (see NO DATA above)" > "/dev/stderr"; exit 2 }
  if (fail)   { print "coverage-gate: one or more packages are below their line-coverage threshold" > "/dev/stderr"; exit 1 }
  printf "coverage-gate: all %d gated packages meet their line-coverage thresholds\n", np
}
' "$lcov"
