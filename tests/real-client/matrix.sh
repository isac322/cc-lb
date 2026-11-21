#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
EVIDENCE_DIR="$ROOT_DIR/target/test-evidence/real-client"
SUMMARY="$EVIDENCE_DIR/matrix.txt"
mkdir -p "$EVIDENCE_DIR"
: > "$SUMMARY"

MAX_JOBS="${REAL_CLIENT_JOBS:-4}"
RC_DIR=$(mktemp -d "${TMPDIR:-/tmp}/cc-lb-matrix-rc.XXXXXX")
cleanup_matrix() {
  rm -rf "$RC_DIR"
}
trap cleanup_matrix EXIT INT TERM

failures=0

export CC_LB_ADMIN_SKIP_SPA="${CC_LB_ADMIN_SKIP_SPA:-1}"
if [ "${REAL_CLIENT_SKIP_BUILD:-0}" != "1" ]; then
  cargo build \
    -p cc-lb-server \
    -p fake-anthropic
fi

# Install and verify all 4 client binaries once upfront (in parallel) so
# concurrent run.sh invocations do not repeat checks or contend on node_modules.
if [ "${REAL_CLIENT_SKIP_INSTALL:-0}" != "1" ]; then
  "$SCRIPT_DIR/install.sh"
fi

# Canonical matrix ordering preserved in matrix.txt.
canonical_cases=(
  "claude-code anthropic-direct"
  "claude-code custom"
  "opencode anthropic-direct"
  "opencode custom"
  "pi anthropic-direct"
  "pi custom"
  "senpi anthropic-direct"
  "senpi custom"
  "senpi anthropic-affinity"
)

# Run longest cases first to minimize tail latency under MAX_JOBS parallelism.
scheduled_cases=(
  "senpi custom"
  "senpi anthropic-affinity"
  "senpi anthropic-direct"
  "opencode anthropic-direct"
  "opencode custom"
  "claude-code anthropic-direct"
  "claude-code custom"
  "pi anthropic-direct"
  "pi custom"
)

# Pre-allocate 9 * 5 = 45 distinct ports in one Python call so parallel runs
# cannot race on ephemeral port assignment.
preallocated_ports=()
while IFS= read -r port_group; do
  preallocated_ports+=("$port_group")
done < <(python3 - "${#scheduled_cases[@]}" <<'PY'
import socket, sys
count = int(sys.argv[1])
socks = []
for _ in range(count * 5):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('127.0.0.1', 0))
    socks.append(s)
ports = [str(s.getsockname()[1]) for s in socks]
for s in socks:
    s.close()
for i in range(0, len(ports), 5):
    print(" ".join(ports[i:i + 5]))
PY
)

running_keys=()
running_pids=()

reap_finished() {
  local new_keys=()
  local new_pids=()
  local i key pid
  for i in "${!running_keys[@]}"; do
    key="${running_keys[$i]}"
    pid="${running_pids[$i]}"
    if [ -s "$RC_DIR/$key.rc" ]; then
      wait "$pid" || true
    else
      new_keys+=("$key")
      new_pids+=("$pid")
    fi
  done
  running_keys=("${new_keys[@]+"${new_keys[@]}"}")
  running_pids=("${new_pids[@]+"${new_pids[@]}"}")
}

for idx in "${!scheduled_cases[@]}"; do
  read -r client upstream <<<"${scheduled_cases[$idx]}"
  key="${client}--${upstream}"
  log="$EVIDENCE_DIR/$client-$upstream.log"
  ports_line="${preallocated_ports[$idx]}"

  while [ "${#running_pids[@]}" -ge "$MAX_JOBS" ]; do
    reap_finished
    if [ "${#running_pids[@]}" -ge "$MAX_JOBS" ]; then
      sleep 0.05
    fi
  done

  printf 'running %s/%s\n' "$client" "$upstream" > "$log"
  (
    set +e
    CC_LB_REAL_CLIENT_PORTS="$ports_line" \
    REAL_CLIENT_SKIP_INSTALL=1 \
      "$SCRIPT_DIR/run.sh" "$client" "$upstream" >> "$log" 2>&1
    rc=$?
    printf '%s\n' "$rc" > "$RC_DIR/$key.rc"
    exit "$rc"
  ) &
  running_keys+=("$key")
  running_pids+=("$!")
done

for pid in "${running_pids[@]+"${running_pids[@]}"}"; do
  wait "$pid" || true
done

for entry in "${canonical_cases[@]}"; do
  read -r client upstream <<<"$entry"
  key="${client}--${upstream}"
  log="$EVIDENCE_DIR/$client-$upstream.log"
  code=1
  if [ -s "$RC_DIR/$key.rc" ]; then
    code=$(tr -d '\r\n' < "$RC_DIR/$key.rc")
  fi
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

if [ "${RUN_MULTI_REPLICA:-0}" = "1" ]; then
  log="$EVIDENCE_DIR/multi-replica-postgres.log"
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
