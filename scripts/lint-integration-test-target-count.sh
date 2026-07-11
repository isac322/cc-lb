#!/usr/bin/env bash
set -euo pipefail

readonly BASELINE_TARGET_COUNT=332
readonly MAX_PERCENT=8
readonly DEFAULT_MAX_TARGET_COUNT=26
readonly MAX_TARGET_COUNT="${CC_LB_MAX_INTEGRATION_TEST_TARGETS:-$DEFAULT_MAX_TARGET_COUNT}"

metadata_file="$(mktemp)"
trap 'rm -f "$metadata_file"' EXIT

cargo metadata --format-version 1 --no-deps >"$metadata_file"

python3 - "$metadata_file" "$MAX_TARGET_COUNT" "$BASELINE_TARGET_COUNT" "$MAX_PERCENT" <<'PY'
import json
import pathlib
import sys

metadata_path = pathlib.Path(sys.argv[1])
try:
    maximum = int(sys.argv[2])
    baseline = int(sys.argv[3])
    max_percent = int(sys.argv[4])
except ValueError as error:
    raise SystemExit(f"integration-test target limit must be an integer: {error}") from error

with metadata_path.open(encoding="utf-8") as metadata_file:
    metadata = json.load(metadata_file)

targets = sorted(
    (package["name"], target["name"])
    for package in metadata["packages"]
    for target in package["targets"]
    if "test" in target["kind"]
)
count = len(targets)
print(
    f"integration-test targets: {count}/{maximum} "
    f"(baseline {baseline}, cap {max_percent}%)"
)

if count <= maximum:
    raise SystemExit(0)

print("integration-test target cap exceeded; consolidate targets before adding more:", file=sys.stderr)
for package, target in targets:
    print(f"  {package}: {target}", file=sys.stderr)
raise SystemExit(1)
PY
