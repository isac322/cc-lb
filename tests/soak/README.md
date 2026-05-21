# Soak Harness

`tests/soak/soak.sh` runs local-only RSS soak tests against `fake-anthropic` through the `cc-lb` proxy. It builds and starts release binaries on OS-assigned loopback ports, renders a temporary custom-upstream config, runs the T40 fallback load generator, samples `VmRSS` from `/proc/<cc-lb-pid>/status`, and tears all child processes down when finished.

Usage:

```sh
bash tests/soak/soak.sh 1h
bash tests/soak/soak.sh 1m
bash tests/soak/soak.sh 1s
```

The load path is non-streaming `POST /v1/messages` with the small T40 request body. The harness uses `cc-lb-loadgen` at about one request per second per worker with 10 workers by default. Override worker count with `CC_LB_SOAK_CONCURRENCY` if needed.

RSS samples are written as CSV rows with `unix_time,rss_kib`. The default output path is `.omo/evidence/task-41-soak-<duration>.csv`; set `CC_LB_SOAK_CSV` to override it. The sampling interval scales as `max(5, duration_secs / 12)`, so short local runs sample every 5 seconds and a 1h run samples every 5 minutes. At the end, the script appends `# PASS delta_kib=<N>` or `# FAIL delta_kib=<N>` using the 16 MiB sample-run threshold.

Summarize a CSV with only Python stdlib:

```sh
python3 tests/soak/plot.py .omo/evidence/task-41-soak-1h.csv
```

CI runs the 24h variant nightly and uploads the CSV artifact. The nightly job is non-blocking with `continue-on-error: true` so regressions preserve evidence without blocking other CI work.
