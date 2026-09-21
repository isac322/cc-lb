# cc-lb 2026-09-18 Execution Report

## Summary

| Item | Status |
|---|---|
| Web CI timeout root cause and fix | Done (PR #805 merged, `864e801e`) |
| Bug PR stack of 9 rebased with review feedback applied | Done, awaiting merge after CI pass |
| QA assets PR rewritten (production code separated) | Done, awaiting merge after CI pass |
| Isolated-fixture read-path exhaustive measurement | Done (179 cells × 5 runs, all 200) |
| Scale experiment and bottleneck evidence | Done (300k events, causal SQL plan confirmed) |
| Default Limits 81 UI paired verification | Blocked (0/81) |
| Full 426-row runtime exhaustive execution | Partially done (read paths only) |
| Production `cc-lb.runbear.io` runtime measurement | Blocked (cannot operate Access login) |
| List-query improvement applied | Not applied (application change, needs approval) |

## 1. CI

`bun-checks` ran on `cc-lb-1` (cpu quota 1). Vitest uses a single worker, but the Bun runtime, GC, and jsdom demand more than one core, so most scheduling periods were throttled and individual tests exceeded the 5000ms budget.

Measured with the pinned runner image and the real CI command, changing only the quota:

| quota | vitest wall | throttled | result |
|---|---|---|---|
| 1 core | 170.6s | 156.6s | 775 passed |
| 2 cores | 70.5s | 20.2s | 775 passed |
| 4 cores | 59.0s | 0.8s | 775 passed |

Cluster measurements: the #802 runner showed throttled periods +7342/14193 on `rpi4`; #803 showed +3986/5461 on `rock5bp`.

Applied: moved the web job to `cc-lb-2` and added `gnutar` to the runner image (busybox tar rejects `actions/cache`'s `-P`, so the cache failed every run). After the merge, `bun-checks` passed 73/73 files on `cc-lb-2`; the verification step took 4m 35s.

## 2. Read-path measurement (isolated fixture)

179 cells × 5 runs each, 895 requests total, all 200. Denominators verified via the API: 3 principals, 4 upstreams, 0 plugins, 1 API key. Keepalive accounts for 3 horizons × 7 statuses × 3 principals = 63 cells.

With little data, every endpoint had a warm median of 1–5ms.

## 3. Scale experiment and bottleneck

Injected 300k rows spanning 7 days into `request_events_v1` and re-measured.

| endpoint | warm median before injection | median after injection | p95 | max |
|---|---|---|---|---|
| `/admin/v1/events/recent` | 0.94ms | 425.85ms | 1093ms | 2426ms |
| `/admin/v1/events/histogram` | 1.03ms | 229.13ms | 395ms | 395ms |
| `/admin/v1/audit` | 1.76ms | 2.21ms | 5.53ms | 8.37ms |
| `/admin/v1/dashboard/summary` | 1.96ms | 2.48ms | 3.54ms | 9.0ms |

The cause is the list query plan. The conditions in `request_event_list_sql.rs` range over `ts`, but this table has no `ts`-leading index. SQLite picks a `request_events_v1_v3_cache_key_ts` skip-scan, then sorts `ORDER BY list_ts_ms DESC, list_event_key DESC, id DESC` with a TEMP B-TREE. The same query run directly takes 118–328ms.

Adding the widened `list_ts_ms` bound that the histogram query already uses to the list query changes the plan to a `request_events_v1_list_order_idx` scan, and the same 100 rows return in 0.7–1.4ms. Because this is an application change, it was left as a proposal and not applied.

Limitations: the fixture is SQLite while production runs PostgreSQL; the injected data is uniformly distributed synthetic data; the measurement host is a workstation.

## 4. Default Limits UI verification blocked

After restoring the controller and driver, one cell passed UI evidence verification in 22 seconds (single trusted Save, single PATCH, revision+1, DOM/screenshot hash). However, one Camofox click reaches the page twice, about 130ms apart. The second click hits the Edit button at the same coordinates right after the save, reopening the editor; once it even produced a second PATCH, so the revision increased beyond expectation.

The server log records `mouse sequence dispatched` only once, after the `locator.click` 3-second timeout. So the duplication happens before the fallback, not in the number of fallback calls. It then degraded further: `page.mouse.move` stopped returning within 2.5 seconds, so clicks were not delivered at all.

Actions: removed the incorrect `click-timeout-no-replay` patch and added `click-witness` (falls back only after observing whether the click was already delivered) plus `camofox_press` for keyboard activation (`/etc/nix-darwin` `cc8fee0`). The duplication still reproduces, so the 81 cells remain at 0/81.

Also, the earlier "completed" evidence for 6 cells turned out to be copied old screenshots plus a modified recorder template, so it is not accepted as UI evidence.

## 5. Production measurement blocked

`cc-lb.runbear.io` requires a Cloudflare Access login. The flow reached the Google account chooser, but clicking the account failed with the same input problem. No other account or bypass credential was used.

## 6. Remaining work

1. Trace the Camofox duplicate input down to the Camoufox input pipeline, or resume UI QA via the `camofox_press` keyboard path (the current MCP wrapper is an older version and needs reconnecting).
2. Execute the 81 UI cells and the write-path state transitions.
3. Secure production access and repeat the same measurements under real load.
4. Get the list-query improvement approved and applied, and confirm the plan on PostgreSQL as well.
