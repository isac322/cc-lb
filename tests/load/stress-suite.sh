#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)

usage() {
  cat <<'USAGE'
usage: tests/load/stress-suite.sh <command> [arguments]

Commands:
  plan                 Materialize a seeded manifest.
  run                  Execute a materialized run or seeded smoke run.
  replay               Verify and dry-run a materialized manifest.
  compare              Compare evidence with a recorded baseline.
  establish-baseline   Record a baseline from evidence.
  preflight            Check local prerequisites or run the T1 exercise.
  smoke                Alias for `run --profile smoke`; still requires run inputs.
  full                 Reserved for the final manual full-profile gate.
  elevated             Render elevated-netns commands only; no elevated execution occurs.

Examples:
  tests/load/stress-suite.sh plan --seed 42 --profile smoke --output /tmp/stress.json
  tests/load/stress-suite.sh replay --manifest /tmp/stress.json --dry-run
  tests/load/stress-suite.sh preflight --exercise-t1-chaos --output /tmp/t1.json
USAGE
}

[ "$#" -gt 0 ] || {
  usage >&2
  exit 2
}

case "$1" in
  -h|--help)
    usage
    ;;
  plan|run|replay|compare|establish-baseline|preflight)
    command=$1
    shift
    exec cargo run -q -p cc-lb-stress-suite -- "$command" "$@"
    ;;
  smoke)
    shift
    exec cargo run -q -p cc-lb-stress-suite -- run --profile smoke "$@"
    ;;
  full)
    printf 'full is reserved for the final manual profile gate; use smoke or an explicit manifest today\n' >&2
    exit 2
    ;;
  elevated)
    shift
    exec cargo run -q -p cc-lb-stress-suite -- preflight --tier elevated-netns "$@"
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
