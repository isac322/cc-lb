#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
EVIDENCE_PATH="${EVIDENCE_PATH:-$REPO_ROOT/.omo/evidence/task-48-crash-recovery.log}"

command mkdir -p "$(dirname -- "$EVIDENCE_PATH")"

(
  cd "$REPO_ROOT"
  printf 'task-48 crash recovery start\n'
  printf 'command=cargo test -p cc-lb-crash-recovery-tests --release -- --nocapture --test-threads=1\n'
  cargo test -p cc-lb-crash-recovery-tests --release -- --nocapture --test-threads=1
) 2>&1 | tee "$EVIDENCE_PATH"

pass_count="$(command grep -cE '^PASS scenario=' "$EVIDENCE_PATH" || true)"
error_count="$(command grep -ciE 'corruption|invalid' "$EVIDENCE_PATH" || true)"

{
  printf 'summary pass_count=%s expected_pass_count=300 error_matches=%s\n' "$pass_count" "$error_count"
  printf 'evidence=%s\n' "$EVIDENCE_PATH"
} | tee -a "$EVIDENCE_PATH"

if [[ "$pass_count" != "300" ]]; then
  printf 'expected 300 PASS lines, got %s\n' "$pass_count" >&2
  exit 1
fi

if [[ "$error_count" != "0" ]]; then
  printf 'expected zero redb error matches, got %s\n' "$error_count" >&2
  exit 1
fi
