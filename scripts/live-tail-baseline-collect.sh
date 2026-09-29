#!/usr/bin/env bash
# live-tail-baseline-collect.sh
#
# Periodic /metrics scraper for a single cc-lb instance. Filters to the
# live-tail metric family and appends timestamped JSONL records to an output
# file. Intended to run under a systemd timer (or cron) for the duration of
# the baseline window — Task 0.3 in .omo/plans/live-tail-followups.md.
#
# Explicit caveat: this collects a **single-operator personal-prod baseline**
# from ONE cc-lb instance running on the operator's own machine. Do not
# extrapolate the resulting p95/p99 values to fleet or multi-instance behavior.
#
# Usage:
#   live-tail-baseline-collect.sh <metrics_url> <output_jsonl>
#
# Example (single-shot):
#   ./scripts/live-tail-baseline-collect.sh http://127.0.0.1:52253/metrics \
#     /var/log/cc-lb/live-tail-baseline.jsonl
#
# Example (systemd timer): register OnCalendar=*:0/5 (every 5 min) to sample
# often enough for percentile calculations but cheaply enough to run for 7d.
set -euo pipefail

METRICS_URL="${1:-http://127.0.0.1:52253/metrics}"
OUTPUT_JSONL="${2:-live-tail-baseline.jsonl}"

TIMESTAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
# `hostname` is not portable across minimal Linux/Docker/BSD images; prefer
# uname -n which is POSIX-defined.
HOST_NAME="$(uname -n)"

# Extract cc-lb identification. Reading directly from the running process so
# the snapshot record documents the exact binary + transport at scrape time.
CC_LB_PID="$(pgrep -f 'cc-lb.*serve' | head -1 || echo '')"
CC_LB_CMD=""
if [ -n "$CC_LB_PID" ] && [ -r "/proc/$CC_LB_PID/cmdline" ]; then
    CC_LB_CMD="$(tr '\0' ' ' < "/proc/$CC_LB_PID/cmdline" | sed 's/ $//')"
fi

# Fetch metrics; abort the sample if the endpoint is unreachable so we don't
# poison the JSONL with empty-value records.
METRICS_BODY="$(curl -sS --max-time 5 "$METRICS_URL")" || {
    echo "warn: $METRICS_URL unreachable at $TIMESTAMP" >&2
    exit 0
}

# Whitelist of live-tail metrics from docs/metrics-live-tail.md. Keeps the
# JSONL compact — full /metrics is ~10 KB per scrape × ~2000 scrapes per week
# = 20 MB unfiltered. Filtered to live-tail we stay under 2 MB.
LIVE_TAIL_METRICS='^(sse_partials_published_total|sse_partials_throttled_total|cc_lb_lifecycle_assembler_rows_total|sse_backfill_pages_total|sse_backfill_rows_total|sse_reset_events_sent_total|sse_reconnects_total|sse_malformed_frames_total|sse_storage_tail_polls_total|sse_storage_tail_lag_ms|sse_storage_tail_backlog_rows|sse_partial_notify_sent_total|sse_partial_notify_dropped_total|sse_notify_http_fetches_total|sse_notify_queue_usage_ratio|cc_lb_dropped_events_total)'

# Strip HELP/TYPE lines, keep only sample lines matching the whitelist.
# Sample line format: `metric_name{label1="v1",label2="v2"} <value> [<timestamp>]`
SAMPLES_JSON="$(echo "$METRICS_BODY" \
    | grep -v '^#' \
    | grep -E "$LIVE_TAIL_METRICS" \
    | awk '
        BEGIN { first = 1 }
        {
            n = split($0, parts, " ")
            metric_and_labels = parts[1]
            value = parts[2]
            gsub(/\\/, "\\\\", metric_and_labels)
            gsub(/"/, "\\\"", metric_and_labels)
            if (!first) printf ","
            first = 0
            printf "{\"metric\":\"%s\",\"value\":\"%s\"}", metric_and_labels, value
        }
    ')"

printf '{"ts":"%s","host":"%s","cc_lb_cmd":"%s","samples":[%s]}\n' \
    "$TIMESTAMP" "$HOST_NAME" "$CC_LB_CMD" "$SAMPLES_JSON" \
    >> "$OUTPUT_JSONL"
