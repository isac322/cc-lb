# PostgreSQL pool telemetry

## Purpose

Verify the PostgreSQL storage SQLx pool is represented by live `store="postgres"` size, idle, and in-use samples, not just startup `store="unknown"` placeholders. These gauges cover the storage pool, not all CNPG backends or the scheduler's separate pool. There is no storage row, admin API payload, or UI for this scenario; the metrics listener is the user-facing surface.

## 0. Isolated environment

Run a throwaway cc-lb instance configured with `[storage] kind = "postgres"` against a disposable local PostgreSQL database and off-production listener ports. Use `tests/real-client/postgres-smoke.sh` for an existing instance recipe. Never use shared production to exercise connection transitions. Keep the PostgreSQL URL private. The metrics listener is intentionally public.

## Point-in-time

1. Start the instance; GET `http://127.0.0.1:<metrics_port>/metrics`.
2. Assert exactly one `store="postgres"` sample for each of `cc_lb_sqlx_pool_size`, `cc_lb_sqlx_pool_idle`, and `cc_lb_sqlx_pool_in_use`. Ignore the legitimate `store="unknown"` placeholders.
3. Assert `size >= idle`, `size >= in_use`, and `in_use = size - idle`. Compare with the instance's SQLx pool counters when running the automated test; PostgreSQL's `pg_stat_activity` is an independent connection sanity check, not a one-to-one pool count.

## State transition

1. In a disposable instance with maximum pool size 2 and minimum size 1, acquire and retain one SQLx connection.
2. Acquire a second connection and run `SELECT 1` through it while retaining both; assert the pool's independent `size = 2, idle = 0` and, within one sampler interval (1 second), rendered metrics show `size = 2, idle = 0, in_use = 2`.
3. Release both connections and close the pool; assert the independent pool counters become `size = 0, idle = 0` and, within one sampler interval, rendered metrics show `in_use = 0`.
4. Stop the instance and remove the temporary database and files.

## Coverage and verdict

`cargo test -p cc-lb-server --features postgres --lib postgres_pool_metrics_track_live_connections` covers rendered Prometheus values against SQLx pool counters across acquire/query/release when `CI_POSTGRES_URL` points at isolated PostgreSQL. Periodic sampling on the running metrics listener is manual-only. SQLite pool metric observations are unchanged.

| Check | Evidence | Verdict |
|---|---|---|
| Startup store labels and invariants | scrape + pool counters | PENDING |
| Acquire/query transition | scrape + pool counters | PENDING |
| Release transition | scrape + pool counters | PENDING |
