#!/usr/bin/env bash
# validate-grafana-dashboard.sh — offline validator for cc-lb Grafana dashboards.
#
# Checks (fail on first failure):
#   1. Dashboard JSON is syntactically valid (jq .)
#   2. Required top-level fields exist (uid, title, panels, schemaVersion)
#   3. Panel IDs are unique (including panels nested inside row `panels[]`)
#   4. Every Prometheus target has a non-empty `expr`
#   5. Every PromQL expression parses under `promtool check rules`
#      (we wrap the exprs as synthetic recording rules and hand them to promtool).
#   6. Every dashboard panel that references an alert in its description or links
#      is matched to an alert rule name that actually exists in deploy/alerts/*.yml.
#
# Usage:
#   scripts/validate-grafana-dashboard.sh [dashboard.json ...]
#
# Defaults to deploy/grafana/*.json when invoked without arguments.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if ! command -v jq >/dev/null 2>&1; then
  echo "ERROR: jq is required." >&2
  exit 2
fi

if ! command -v promtool >/dev/null 2>&1; then
  echo "ERROR: promtool is required. Install prometheus via package manager or grab it from https://github.com/prometheus/prometheus/releases" >&2
  exit 2
fi

targets=("$@")
if [[ ${#targets[@]} -eq 0 ]]; then
  mapfile -t targets < <(find "$REPO_ROOT/deploy/grafana" -maxdepth 1 -type f -name '*.json' | sort)
  if [[ ${#targets[@]} -eq 0 ]]; then
    echo "No dashboard JSON files found under deploy/grafana/" >&2
    exit 2
  fi
fi

# Collect the set of alert names declared in deploy/alerts/*.yml so we can
# cross-check dashboard panel descriptions/links against real alert rules.
alert_names_file="$(mktemp)"
trap 'rm -f "$alert_names_file"' EXIT
grep -h -R -E '^\s*-\s*alert:\s*\S+' "$REPO_ROOT/deploy/alerts" 2>/dev/null \
  | sed -E 's/^\s*-\s*alert:\s*//' \
  | sort -u > "$alert_names_file"

fail=0

for dash in "${targets[@]}"; do
  echo "=== $dash ==="

  if ! jq -e . "$dash" >/dev/null 2>&1; then
    echo "  [FAIL] not valid JSON" >&2
    fail=1
    continue
  fi

  missing=$(jq -r 'del(.uid, .title, .schemaVersion, .panels) | if (.uid // empty) == "" or (.title // empty) == "" or (.schemaVersion // empty) == null or (.panels | length) == 0 then "missing" else "" end' "$dash" 2>/dev/null || echo missing)
  for field in uid title schemaVersion panels; do
    val=$(jq -r ".${field} // empty" "$dash")
    if [[ -z "$val" ]]; then
      echo "  [FAIL] missing required top-level field: $field" >&2
      fail=1
    fi
  done

  # Panel IDs across the whole dashboard, flattening any row.panels[] into the pool.
  duplicate_ids=$(jq -r '
    def gather:
      (.panels // [])
        | map(select(.type == "row") | (.panels // [])) as $nested
        | (.panels // []) + ([($nested // [])[] // []] | add // [])
      ;
    [.panels // [], (.panels[]? | select(.type == "row") | (.panels // []))] | flatten | map(.id) | group_by(.) | map(select(length > 1) | .[0]) | .[]?
  ' "$dash")
  if [[ -n "$duplicate_ids" ]]; then
    echo "  [FAIL] duplicate panel IDs: $duplicate_ids" >&2
    fail=1
  fi

  # Every prometheus target must have expr.
  bad_targets=$(jq -r '
    [.panels // [], (.panels[]? | select(.type == "row") | (.panels // []))]
      | flatten
      | map(select(.type != "row"))
      | map({id, title, targets: (.targets // [])})
      | map(select((.targets | length) > 0))
      | map(select(any(.targets[]; (.expr // "") == "")))
      | .[]
      | "panel id=\(.id) title=\(.title // "?") has empty expr"
  ' "$dash")
  if [[ -n "$bad_targets" ]]; then
    echo "  [FAIL] $bad_targets" >&2
    fail=1
  fi

  # Extract every expr, wrap in a synthetic recording-rules file, promtool check.
  rules_tmp="$(mktemp)"
  {
    echo "groups:"
    echo "  - name: dashboard_extracted"
    echo "    rules:"
    jq -r '
      [.panels // [], (.panels[]? | select(.type == "row") | (.panels // []))]
        | flatten
        | map(select(.type != "row"))
        | map(.targets // [])
        | flatten
        | map(.expr // empty)
        | map(select(length > 0))
        | to_entries[]
        | "      - record: dash:expr:\(.key)\n        expr: \(.value | @json)"
    ' "$dash"
  } > "$rules_tmp"
  # 1. Convert jq-emitted JSON-quoted exprs to YAML single-quoted scalars
  #    (jq @json wraps the string in double quotes with \-escapes, which is
  #    fine YAML but promtool then reads it as a literal, and PromQL still
  #    sees the escape sequences).
  # 2. Substitute Grafana template variables that promtool cannot parse
  #    (`$rate_window`, `$__rate_interval`, `$__interval`, `${var}`) with
  #    a fixed 5m stand-in so the surrounding PromQL parses.
  python3 - "$rules_tmp" <<'PY'
import re, sys, pathlib
p = pathlib.Path(sys.argv[1])
lines = p.read_text().splitlines()
grafana_var_re = re.compile(r'\$\{?[A-Za-z_][A-Za-z_0-9]*\}?')
out = []
for line in lines:
    m = re.match(r'^(\s*expr:\s*)"(.*)"$', line)
    if m:
        prefix, val = m.groups()
        val = val.encode().decode('unicode_escape')
        val = grafana_var_re.sub('5m', val)
        val = val.replace("'", "''")
        line = f"{prefix}'{val}'"
    out.append(line)
p.write_text("\n".join(out) + "\n")
PY

  if ! promtool check rules "$rules_tmp" >/dev/null 2>&1; then
    echo "  [FAIL] promtool rejected extracted PromQL:" >&2
    promtool check rules "$rules_tmp" >&2 || true
    fail=1
  else
    exprs=$(grep -c '^\s*expr:' "$rules_tmp" || true)
    echo "  [OK] $exprs PromQL expressions parse"
  fi
  rm -f "$rules_tmp"

  # Alert cross-check: pull alert names mentioned in panel description/links, ensure they exist.
  referenced=$(jq -r '
    [.panels // [], (.panels[]? | select(.type == "row") | (.panels // []))]
      | flatten
      | map(select(.type != "row"))
      | map((.description // "") + " " + ((.links // []) | map(.title // "") | join(" ")))
      | join(" ")
  ' "$dash" | grep -oE 'LiveTail[A-Za-z0-9_]+' | sort -u || true)
  missing_alerts=""
  while IFS= read -r name; do
    [[ -z "$name" ]] && continue
    if ! grep -q -x "$name" "$alert_names_file"; then
      missing_alerts+="$name\n"
    fi
  done <<< "$referenced"
  if [[ -n "$missing_alerts" ]]; then
    echo "  [FAIL] dashboard references alert names that do not exist in deploy/alerts/*.yml:" >&2
    printf "$missing_alerts" >&2
    fail=1
  fi

  if [[ $fail -eq 0 ]]; then
    echo "  [OK] $dash"
  fi
done

if [[ $fail -ne 0 ]]; then
  echo "FAIL: one or more dashboards failed validation" >&2
  exit 1
fi

echo "OK: all dashboards passed validation"
