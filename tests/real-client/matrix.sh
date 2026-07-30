#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
EVIDENCE_DIR="$ROOT_DIR/.omo/evidence"
SUMMARY="$EVIDENCE_DIR/task-36-real-client-matrix.txt"
mkdir -p "$EVIDENCE_DIR"
: > "$SUMMARY"

clients='claude-code opencode pi senpi'
upstreams='anthropic-direct custom'
failures=0

export CC_LB_ADMIN_SKIP_SPA="${CC_LB_ADMIN_SKIP_SPA:-1}"
cargo build \
  -p cc-lb-server \
  -p fake-anthropic

for client in $clients; do
  for upstream in $upstreams; do
    log="$EVIDENCE_DIR/task-36-real-$client-$upstream.log"
    printf 'running %s/%s\n' "$client" "$upstream" > "$log"
    set +e
    "$SCRIPT_DIR/run.sh" "$client" "$upstream" >> "$log" 2>&1
    code=$?
    set -e
    if [ "$code" -eq 0 ]; then
      printf 'PASS %s/%s\n' "$client" "$upstream" | tee -a "$SUMMARY"
    elif [ "$code" -eq 77 ]; then
      reason=$(grep -E 'SKIP reason:' "$log" | tail -n 1 | sed 's/^SKIP reason: //' || true)
      [ -n "$reason" ] || reason="run.sh exited 77 without a reason"
      printf 'SKIP %s/%s (reason: %s)\n' "$client" "$upstream" "$reason" | tee -a "$SUMMARY"
    else
      failures=$((failures + 1))
      reason=$(grep -E 'FAIL reason:' "$log" | tail -n 1 | sed 's/^FAIL reason: //' || true)
      [ -n "$reason" ] || reason="run.sh exited $code"
      printf 'FAIL %s/%s (reason: %s)\n' "$client" "$upstream" "$reason" | tee -a "$SUMMARY"
    fi
  done
done

if [ "${RUN_MULTI_REPLICA:-0}" = "1" ]; then
  log="$EVIDENCE_DIR/task-37-multi-replica-postgres.log"
  printf 'running multi-replica-postgres\n' > "$log"
  set +e
  "$SCRIPT_DIR/../multi-replica/multi-replica-postgres.sh" >> "$log" 2>&1
  code=$?
  set -e
  if [ "$code" -eq 0 ]; then
    printf 'PASS multi-replica-postgres\n' | tee -a "$SUMMARY"
  else
    failures=$((failures + 1))
    reason=$(grep -E 'FAIL multi-replica postgres:' "$log" | tail -n 1 | sed 's/^FAIL multi-replica postgres: //' || true)
    [ -n "$reason" ] || reason="multi-replica-postgres.sh exited $code"
    printf 'FAIL multi-replica-postgres (reason: %s)\n' "$reason" | tee -a "$SUMMARY"
  fi
else
  printf 'SKIP multi-replica-postgres (reason: RUN_MULTI_REPLICA not set)\n' | tee -a "$SUMMARY"
fi

if [ "$failures" -ne 0 ]; then
  exit 1
fi
