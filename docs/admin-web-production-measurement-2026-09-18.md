# Production Admin Web measurement, 2026-09-18

Target: `https://cc-lb.runbear.io`. Every request in this report was issued read-only from a
real authenticated browser session through Cloudflare Access, or from a localhost replica of
the same binary. No production write was executed.

## Deployment as measured

The workload no longer runs in the `runbear-operation` cluster. `llm-proxy` keeps only the
Postgres cluster and an `ExternalName` service; the admin listener answers from the operator
workstation.

| Property | Value |
|---|---|
| Process | `target/debug/cc-lb serve`, **debug profile**, binary built 2026-09-14 |
| Binary source worktree | `01f6e654` (4 days behind the master measured here) |
| Config | `/data/data/cc-lb/master-9c420b4a/cc-lb.toml` |
| Storage | SQLite 14.9 MB with a 5.8 MB WAL, pool size 3 |
| Listeners | proxy `127.0.0.1:8080`, admin `10.222.0.7:9090`, metrics `127.0.0.1:9091` |
| Exposure | PSC endpoint `10.20.0.44:30080` → `cc-lb-local-psc` → Cloudflare tunnel |
| Tunnel edge | `iad03` / `iad19` (us-east4) while the browser edge is `ICN` |

## Browser-side results

Three sweeps of 39 read endpoints, plus two saturating hold loops on the slowest endpoint.

| Endpoint | Median | Max | Body |
|---|---|---|---|
| `/admin/v1/subscription-quotas/aggregate?range=24h` | 660 ms | 770 ms | 10.5 KB |
| `/admin/v1/subscription-quotas/analysis` | 641 ms | 643 ms | 2.0 KB |
| `/admin/v1/subscription-quotas/series` | 635 ms | 641 ms | 6.8 KB |
| `/admin/v1/principals/{id}/keys` | 525 ms | 547 ms | 0.4 KB |
| `/admin/v1/status` | 357 ms | 390 ms | 1.6 KB |
| `/admin/v1/subscription-quotas/pool-history?range=24h` | 334 ms | 335 ms | 7.0 KB |
| `/admin/dashboard/summary?range=6h` | 272 ms | 280 ms | 190 KB |
| `/admin/v1/events/recent?limit=50` | 251 ms | 260 ms | 93 KB |
| `/admin/health` | 232 ms | 259 ms | 74 B |

Hold loops on the aggregate endpoint: 95 requests in 60.5 s (median 622 ms, p95 674 ms) and
123 requests in 75.6 s (median 620 ms, p95 667 ms). All 218 responses were 200.

Every top-level page rendered without an error state: Overview, Upstreams, Principals, Logs,
Plugins, Audit, Settings.

Four non-2xx results, all explained: `/admin/v1/auth/session` and
`/admin/v1/scheduler/status` return 404 because the deployed binary predates them,
`/admin/v1/plugins/wasm` returns 405 because the sweep probes a POST-only route with GET, and
`/admin/config/diff` returns 400 because it requires `from` and `to`.

## Where the time is not

Each of these was measured, not assumed.

| Layer | Measurement | Result |
|---|---|---|
| Origin handler | same binary, copy of the live database, `127.0.0.1:19090` | 1.2-9.3 ms for every endpoint, 7.0 ms for the aggregate |
| Database | every table the aggregate reads, against the live file opened read-only | ≤5.8 ms total, dominated by 3,037 checkpoint rows |
| Connection pool | `cc_lb_sqlx_pool` sampled every 2 s through a 60 s saturating loop | `in_use` 1 of 3, `idle` 2, never queued |
| Process CPU | `cputime` delta across a 59 s window carrying ~95 requests | 870 ms total, ~9 ms per request |
| Scheduler jobs | `Jobs.lock_at`/`done_at` in the scheduler database | 0-1 s per job, not the 21-55 s that `run_at` suggests |
| Edge cache | response headers on every endpoint | `cf-cache-status: DYNAMIC`, `content-encoding: br`, always `content-length` |
| Payload size | 190 KB summary at 257 ms against 10.5 KB aggregate at 620 ms | latency is not monotonic in size |
| Application locks | scheduler jobs audited for a guard held across `.await` | none found |

## Where the time is

Two components, one certain and one still open.

**Path overhead, 218-257 ms on every request.** A 74-byte health response takes 218 ms from
the browser while the same handler answers in 1.2 ms on localhost. An unauthenticated request
to the same hostname returns its Access redirect in 36 ms, so the cost is not the Cloudflare
edge itself: it is the authenticated hop chain. The browser sits in Seoul, the tunnel
terminates in us-east4, and the origin is back in Korea, so each admin request crosses the
Pacific twice. This is the dominant cost for 35 of 39 endpoints.

**An unattributed ~370-400 ms on the subscription-quota endpoints.** The aggregate endpoint is
uniformly 620-660 ms in production and 7.0 ms on the replica. CPU, SQL, pool acquisition,
application locks, payload size, response framing, and edge caching are each excluded by the
measurements above. The replica could not reproduce it because the live in-memory quota cache
is not reconstructible from the database alone: the replica returns 2.6 KB where production
returns 10.5 KB, so it does not execute the same branch.

## The observation that closes the gap

The deployed binary emits no `x-request-id` and no `Server-Timing` header; current master does,
from `crates/cc-lb-server/src/app.rs`. Deploying a current build and repeating this sweep splits
origin handler time from path time for each request, which resolves the open ~400 ms without
further guessing. That deployment also carries ten merged fixes and would replace a debug binary
with a release build.

## Recommendations, in measured order of value

1. Deploy current master as a release build. This is the prerequisite for attributing the
   remaining ~400 ms, and it removes a debug-profile origin from the serving path.
2. Terminate the tunnel near the origin. The 218-257 ms floor is geography, not code: the
   browser and origin are both in Korea while the tunnel runs in us-east4.
3. Leave the database alone. At 15 MB with a 3-connection pool that never queued, SQLite is
   not a bottleneck for the admin read path.

## Artifacts

- `qa/admin-web/runs/2026-09-18/production-read-sweep.json`: the 39-endpoint sweep, hold loops,
  edge headers, and deployment facts.
- `qa/admin-web/runs/2026-09-18/origin-local-timings.json`: same-binary localhost timings.
- `qa/admin-web/runs/2026-09-18/default-limits-paired-ledger.json`: the 81-cell paired write
  verification from the isolated fixture.

Per-page screenshots and the raw browser sweep files stay on the measurement host under
`/data/tmp/cc-lb-prod-sweep/`; they are not committed.
