# cc-lb Admin Web & API/Query QA Inventory

> **Source base commit:** `ef70b347`  
> **Source state:** `approved_uncommitted_candidate` — Source hashes and references describe the approved uncommitted candidate worktree; source_commit identifies its base, not an updated production deployment.  
> **Source reconciliation:** `source_reconciled`  
> **Runtime status:** `runtime_pending`  
> **Production applicability:** `reconciled_from_deployed_delta`  
> **Count basis:** Rows are rendered from source contracts and independently checked source/route denominators; counts alone are not completion evidence.

Static reconciliation is not browser proof. Runtime entities, cursor termination, poll cycles, SSE behavior, production availability, and latency remain pending until the execution harness records them.

Candidate server Admin responses expose `x-request-id`; `admin_response_head_ready` records response-head generation, not stream completion. This does not assert that the deployed production version has this instrumentation.

`Server-Timing` metric `rid` exposes the same opaque ID to Resource Timing. Server-Timing rid.description repeats x-request-id without a duration. It is a backend join key, not proof of fresh execution: cached responses may retain an earlier ID.

## 1. Reconciliation Summary

| Dimension | Count / Status |
|---|---|
| UI parent actions | 205 |
| UI atomic request occurrences | 149 |
| Registered API method/path rows | 115 |
| Independent backend route scan | 115 |
| Production UI source denominator | 93 files |
| UI route denominator | 8 routes |
| Unknown UI requests | 0 |
| Runtime | runtime_pending |

## 2. Risk Tiers

- `read`: no source-backed side effect.
- `read_with_audit`: read path that also appends an audit record.
- `reversible_write`: fixture-scoped mutation with a source-backed restore path.
- `destructive_write`: deletion, revocation, or irreversible mutation requiring explicit isolation.
- `external_action`: calls an external system, reloads a service, performs OAuth exchange, or can consume billable upstream resources.

## 3. Master Interaction Index

| ID | Type | Parent | Page / Component | Action or HTTP | Risk | Production |
|---|---|---|---|---|---|---|
| **UI-SRC-E63DB0A6963C** | `ui_action` | — | Global / AuthRequiredGate | Auto Session Verification on Mount/Storage Change | `read` | `not_deployed` |
| **UI-ROOT-01** | `network_request` | UI-SRC-E63DB0A6963C | Global / AuthRequiredGate | GET /admin/v1/auth/session | `read` | `not_deployed` |
| **UI-SRC-7EF8F6882B60** | `ui_action` | — | Global / AuthRequiredGate | Submit Static Admin Bearer Token | `read` | `not_deployed` |
| **UI-ROOT-02** | `network_request` | UI-SRC-7EF8F6882B60 | Global / AuthRequiredGate | GET /admin/v1/auth/session | `read` | `not_deployed` |
| **UI-SRC-15F5F1954403** | `ui_action` | — | Global / AuthRequiredGate | Admin Token Input Empty Validation | `read` | `available` |
| **UI-SRC-9D6A6AA71D0F** | `ui_action` | — | Global / AuthRequiredGate | External Auth Required Retry Click | `read` | `not_deployed` |
| **REQ-SRC-D9664AAE1B32** | `network_request` | UI-SRC-9D6A6AA71D0F | Global / AuthRequiredGate | GET /admin/v1/auth/session | `read` | `not_deployed` |
| **UI-SRC-EB957D16B51D** | `ui_action` | — | Global / AuthRequiredGate | Auth Verification Error Retry Click | `read` | `not_deployed` |
| **REQ-SRC-B038CDBE84D7** | `network_request` | UI-SRC-EB957D16B51D | Global / AuthRequiredGate | GET /admin/v1/auth/session | `read` | `not_deployed` |
| **UI-SRC-F05A6A69408A** | `ui_action` | — | Global / AppShell / Topbar | Admin Server Health Poll | `read` | `available` |
| **UI-ROOT-03** | `network_request` | UI-SRC-F05A6A69408A | Global / AppShell / Topbar | GET /admin/health | `read` | `available` |
| **UI-TOP-01** | `ui_action` | — | Global / AppShell / Topbar | Desktop Sidebar Collapse Toggle Button | `read` | `available` |
| **UI-TOP-03** | `ui_action` | — | Global / AppShell | Keyboard Shortcut Toggle Sidebar (Cmd+B / Ctrl+B) | `read` | `available` |
| **UI-TOP-04** | `ui_action` | — | Global / AppShell / Topbar | Mobile Hamburger Navigation Drawer Trigger | `read` | `available` |
| **UI-SRC-7963BABBB70A** | `ui_action` | — | Global / AppShell / Topbar | Command Palette Topbar Trigger Button | `read` | `available` |
| **UI-TOP-05** | `ui_action` | — | Global / Sidebar / SidebarNav | Primary Navigation Links | `read` | `available` |
| **UI-TOP-02** | `ui_action` | — | Global / ThemeToggle | Theme Mode Selection Dropdown | `read` | `available` |
| **UI-CMD-01** | `ui_action` | — | Global / CommandPalette | Command Palette Keyboard Shortcut (Cmd+K / Ctrl+K) | `read` | `available` |
| **UI-SRC-FD9DEB65904F** | `ui_action` | — | Global / CommandPalette | Command Palette Upstreams Background Query | `read` | `available` |
| **UI-CMD-03** | `network_request` | UI-SRC-FD9DEB65904F | Global / CommandPalette | GET /admin/v1/upstreams | `read` | `available` |
| **UI-SRC-036489E5DD6A** | `ui_action` | — | Global / CommandPalette | Command Palette Principals Background Query | `read` | `available` |
| **UI-CMD-04** | `network_request` | UI-SRC-036489E5DD6A | Global / CommandPalette | GET /admin/v1/principals | `read` | `available` |
| **UI-CMD-05** | `ui_action` | — | Global / CommandPalette | Command Palette Live Text Filter | `read` | `available` |
| **UI-CMD-02** | `ui_action` | — | Global / CommandPalette | Command Palette Page or Action Navigation | `read` | `available` |
| **UI-SRC-7B67C1B1E413** | `ui_action` | — | Global / CommandPalette | Command Palette Entity Deep Link Selection | `read` | `available` |
| **UI-CMD-06** | `ui_action` | — | Global / CommandPalette | Command Palette Dismiss / Close | `read` | `available` |
| **UI-SRC-014AD24226CE** | `ui_action` | — | Overview / KPI Tiles & Header | Overview Dashboard Summary & Sparkline Metrics Query | `read` | `available` |
| **UI-OV-01** | `network_request` | UI-SRC-014AD24226CE | Overview / KPI Tiles & Header | GET /admin/dashboard/summary | `read` | `available` |
| **UI-SRC-6899B83C35F4** | `ui_action` | — | Overview / Top Principals Card | Overview Dashboard Principal Usage Query | `read` | `available` |
| **UI-OV-02** | `network_request` | UI-SRC-6899B83C35F4 | Overview / Top Principals Card | GET /admin/usage | `read` | `available` |
| **UI-SRC-6A947BA8B48A** | `ui_action` | — | Overview / Pool Quota Card / Stacked Bars | Subscription Quota Aggregate Snapshot Query | `read` | `available` |
| **UI-OV-03** | `network_request` | UI-SRC-6A947BA8B48A | Overview / Pool Quota Card / Stacked Bars | GET /admin/v1/subscription-quotas/aggregate | `read` | `available` |
| **UI-SRC-F52C6DD98101** | `ui_action` | — | Overview / Pool Quota Card / Themed Area Chart | Subscription Quota Pool History Time Series Query | `read` | `available` |
| **UI-OV-04** | `network_request` | UI-SRC-F52C6DD98101 | Overview / Pool Quota Card / Themed Area Chart | GET /admin/v1/subscription-quotas/pool-history | `read` | `available` |
| **UI-SRC-BAA6CE610FCA** | `ui_action` | — | Overview / OverviewPage / Data Layer | Principal Name Resolution Query | `read` | `available` |
| **UI-OV-13** | `network_request` | UI-SRC-BAA6CE610FCA | Overview / OverviewPage / Data Layer | GET /admin/v1/principals | `read` | `available` |
| **UI-SRC-1BEA80590008** | `ui_action` | — | Overview / OverviewPage / Data Layer | Upstream Name Resolution Query | `read` | `available` |
| **UI-OV-14** | `network_request` | UI-SRC-1BEA80590008 | Overview / OverviewPage / Data Layer | GET /admin/v1/upstreams | `read` | `available` |
| **UI-OV-07** | `ui_action` | — | Overview / Overview Header / Time Range Toggle Bar | Time Range Toggle Action | `read` | `available` |
| **UI-OV-08** | `ui_action` | — | Overview / KPI Tiles / Sparkline | Synchronized KPI Sparkline Hover | `read` | `available` |
| **UI-OV-09** | `ui_action` | — | Overview / TopPrincipalsCard / PrincipalCostMeter | Principal Cost Breakdown Popover | `read` | `available` |
| **UI-OV-10** | `ui_action` | — | Overview / PoolQuotaCard / PoolQuotaStackedBar | Pool Quota Stacked Bar Breakdown Popover | `read` | `available` |
| **UI-SRC-8C3BDB37C763** | `ui_action` | — | Overview / Recent Requests Table | Recent Requests Initial Page Query | `read` | `available` |
| **UI-OV-05** | `network_request` | UI-SRC-8C3BDB37C763 | Overview / Recent Requests Table | GET /admin/events/recent | `read` | `available` |
| **UI-SRC-2305B3BC43A4** | `ui_action` | — | Overview / Recent Requests Table / Infinite Scroll | Recent Requests Infinite Scroll Next Page | `read` | `available` |
| **UI-OV-11** | `network_request` | UI-SRC-2305B3BC43A4 | Overview / Recent Requests Table / Infinite Scroll | GET /admin/events/recent | `read` | `available` |
| **UI-SRC-40B1F1E07B91** | `ui_action` | — | Overview / Recent Requests / Live Event Stream | Live Tail SSE Connection | `read` | `available` |
| **UI-OV-06** | `network_request` | UI-SRC-40B1F1E07B91 | Overview / Recent Requests / Live Event Stream | GET /admin/events/stream | `read` | `available` |
| **UI-SRC-FDFB5195AB54** | `ui_action` | — | Overview / Recent Requests / Live Stream Reconnection | Live Stream Reconnect Hybrid Delta Backfill | `read` | `available` |
| **UI-OV-15** | `network_request` | UI-SRC-FDFB5195AB54 | Overview / Recent Requests / Live Stream Reconnection | GET /admin/v1/events/delta | `read` | `available` |
| **UI-OV-16** | `ui_action` | — | Overview / LiveTailFailureBanner | Live Tail Failure Banner Manual Retry | `read` | `available` |
| **UI-SRC-CFF84088D66A** | `ui_action` | — | Overview / LiveTailFailureBanner | Live Tail Failure Banner Dismiss | `read` | `available` |
| **UI-OV-17** | `ui_action` | — | Overview / Recent Requests / Section Header | Recent Requests 'See all' Navigation Link | `read` | `available` |
| **UI-SRC-1667E85E2450** | `ui_action` | — | Overview / Recent Requests Table / RequestEventRow | Recent Request Row Selection | `read` | `available` |
| **UI-SRC-7BBB835587EA** | `ui_action` | — | Overview / RequestEventDrawer / RequestDetail | Request Event Full Detail Query | `read` | `available` |
| **UI-OV-12** | `network_request` | UI-SRC-7BBB835587EA | Overview / RequestEventDrawer / RequestDetail | GET /admin/v1/events/detail/{event_id} | `read_with_audit` | `available` |
| **UI-SRC-FE19ED30AFDB** | `ui_action` | — | Overview / RequestEventDrawer / RequestDetail | Request Detail Copy Actions (ID, Session, JSON) | `read` | `available` |
| **UI-SRC-25F058F5759A** | `ui_action` | — | Overview / RequestEventDrawer | Request Detail Drawer Close | `read` | `available` |
| **UI-SRC-35CD806FA378** | `ui_action` | — | Logs / Logs Table Historical Events | Historical Events Page Query on Mount | `read` | `available` |
| **UI-LOG-01** | `network_request` | UI-SRC-35CD806FA378 | Logs / Logs Table Historical Events | GET /admin/events/recent | `read` | `available` |
| **UI-SRC-8290B2982481** | `ui_action` | — | Logs / Logs Live Event Stream (SSE) | SSE Live Event Stream Subscription | `read` | `available` |
| **UI-LOG-02** | `network_request` | UI-SRC-8290B2982481 | Logs / Logs Live Event Stream (SSE) | GET /admin/events/stream | `read` | `available` |
| **UI-SRC-694A5E8030DA** | `ui_action` | — | Logs / Events Histogram Strip | Events Histogram Query | `read` | `available` |
| **UI-LOG-03** | `network_request` | UI-SRC-694A5E8030DA | Logs / Events Histogram Strip | GET /admin/v1/events/histogram | `read` | `available` |
| **UI-LOG-04** | `ui_action` | — | Logs / Toggle Live Tail Button | Toggle Live Tail Button Click | `read` | `available` |
| **UI-LOG-05** | `ui_action` | — | Logs / Principal Filter Dropdown | Select Principal Filter | `read` | `available` |
| **UI-LOG-05A** | `network_request` | UI-LOG-05 | Logs / Principal Filter Dropdown | GET /admin/events/recent | `read` | `available` |
| **UI-LOG-05B** | `network_request` | UI-LOG-05 | Logs / Principal Filter Dropdown | GET /admin/events/stream | `read` | `available` |
| **UI-LOG-05C** | `network_request` | UI-LOG-05 | Logs / Principal Filter Dropdown | GET /admin/v1/events/histogram | `read` | `available` |
| **UI-LOG-06** | `ui_action` | — | Logs / Upstream Filter Dropdown | Select Upstream Filter | `read` | `available` |
| **UI-LOG-06A** | `network_request` | UI-LOG-06 | Logs / Upstream Filter Dropdown | GET /admin/events/recent | `read` | `available` |
| **UI-LOG-06B** | `network_request` | UI-LOG-06 | Logs / Upstream Filter Dropdown | GET /admin/events/stream | `read` | `available` |
| **UI-LOG-06C** | `network_request` | UI-LOG-06 | Logs / Upstream Filter Dropdown | GET /admin/v1/events/histogram | `read` | `available` |
| **UI-LOG-07** | `ui_action` | — | Logs / Session Filter Dropdown | Select Session Filter | `read` | `available` |
| **UI-LOG-07A** | `network_request` | UI-LOG-07 | Logs / Session Filter Dropdown | GET /admin/events/recent | `read` | `available` |
| **UI-LOG-07B** | `network_request` | UI-LOG-07 | Logs / Session Filter Dropdown | GET /admin/events/stream | `read` | `available` |
| **UI-LOG-07C** | `network_request` | UI-LOG-07 | Logs / Session Filter Dropdown | GET /admin/v1/events/histogram | `read` | `available` |
| **UI-LOG-08** | `ui_action` | — | Logs / Model Filter Input | Model Input Debounced Filter Change | `read` | `available` |
| **UI-LOG-08A** | `network_request` | UI-LOG-08 | Logs / Model Filter Input | GET /admin/events/recent | `read` | `available` |
| **UI-LOG-08B** | `network_request` | UI-LOG-08 | Logs / Model Filter Input | GET /admin/events/stream | `read` | `available` |
| **UI-LOG-08C** | `network_request` | UI-LOG-08 | Logs / Model Filter Input | GET /admin/v1/events/histogram | `read` | `available` |
| **UI-LOG-09** | `ui_action` | — | Logs / Status Class Filter Dropdown | Select Status Class Filter | `read` | `available` |
| **UI-LOG-09A** | `network_request` | UI-LOG-09 | Logs / Status Class Filter Dropdown | GET /admin/events/recent | `read` | `available` |
| **UI-LOG-09B** | `network_request` | UI-LOG-09 | Logs / Status Class Filter Dropdown | GET /admin/events/stream | `read` | `available` |
| **UI-LOG-09C** | `network_request` | UI-LOG-09 | Logs / Status Class Filter Dropdown | GET /admin/v1/events/histogram | `read` | `available` |
| **UI-LOG-10** | `ui_action` | — | Logs / Source Kind Filter Dropdown | Select Source Kind Filter | `read` | `available` |
| **UI-LOG-10A** | `network_request` | UI-LOG-10 | Logs / Source Kind Filter Dropdown | GET /admin/events/recent | `read` | `available` |
| **UI-LOG-10B** | `network_request` | UI-LOG-10 | Logs / Source Kind Filter Dropdown | GET /admin/events/stream | `read` | `available` |
| **UI-LOG-10C** | `network_request` | UI-LOG-10 | Logs / Source Kind Filter Dropdown | GET /admin/v1/events/histogram | `read` | `available` |
| **UI-LOG-11** | `ui_action` | — | Logs / Histogram Strip Canvas Interaction | Histogram View Pan/Zoom and Selection Drag | `read` | `available` |
| **UI-LOG-11B** | `network_request` | UI-LOG-11 | Logs / Histogram Strip Canvas Interaction | GET /admin/events/recent | `read` | `available` |
| **UI-LOG-11A** | `network_request` | UI-LOG-11 | Logs / Histogram Strip Canvas Interaction | GET /admin/v1/events/histogram | `read` | `available` |
| **UI-LOG-12** | `ui_action` | — | Logs / TimeRangeBounds Input and Apply | Absolute Time Bounds Entry and Calendar Pick | `read` | `available` |
| **UI-LOG-12A** | `network_request` | UI-LOG-12 | Logs / TimeRangeBounds Input and Apply | GET /admin/events/recent | `read` | `available` |
| **UI-LOG-12B** | `network_request` | UI-LOG-12 | Logs / TimeRangeBounds Input and Apply | GET /admin/v1/events/histogram | `read` | `available` |
| **UI-LOG-13** | `ui_action` | — | Logs / Clear Filters Button | Clear All Filters Button Click | `read` | `available` |
| **UI-SRC-2874D8E9F106** | `ui_action` | — | Logs / Refresh Button | Manual Refresh Button Click | `read` | `available` |
| **UI-LOG-14** | `network_request` | UI-SRC-2874D8E9F106 | Logs / Refresh Button | GET /admin/events/recent | `read` | `available` |
| **UI-LOG-15** | `ui_action` | — | Logs / Export Logs Button | Export Visible Logs to JSON | `read` | `available` |
| **UI-SRC-A6BE3F64E803** | `ui_action` | — | Logs / Logs Pagination Controls Next Button | Navigate to Next Historical Page | `read` | `available` |
| **UI-LOG-16** | `network_request` | UI-SRC-A6BE3F64E803 | Logs / Logs Pagination Controls Next Button | GET /admin/events/recent | `read` | `available` |
| **UI-SRC-888A1F4D1DD9** | `ui_action` | — | Logs / Logs Pagination Controls Prev Button | Navigate to Previous Historical Page | `read` | `available` |
| **UI-LOG-17** | `ui_action` | — | Logs / Log Row Anchor Zoom Buttons | Row Anchor Range Buttons (±1m, ±5m, ±30m) | `read` | `available` |
| **UI-LOG-17A** | `network_request` | UI-LOG-17 | Logs / Log Row Anchor Zoom Buttons | GET /admin/events/recent | `read` | `available` |
| **UI-LOG-17B** | `network_request` | UI-LOG-17 | Logs / Log Row Anchor Zoom Buttons | GET /admin/v1/events/histogram | `read` | `available` |
| **UI-SRC-091345AD21A8** | `ui_action` | — | Logs / RequestEventsTable Row Click | Row Click Open Request Detail Drawer | `read` | `available` |
| **UI-SRC-C59FB001EF22** | `ui_action` | — | Logs / Request Event Detail Query | Fetch Full Event Diagnostics Detail | `read` | `available` |
| **UI-LOG-18** | `network_request` | UI-SRC-C59FB001EF22 | Logs / Request Event Detail Query | GET /admin/v1/events/detail/{event_id} | `read_with_audit` | `available` |
| **UI-SRC-6793FF938733** | `ui_action` | — | Logs / Upstream Options Query | Fetch Upstreams for Options and Names | `read` | `available` |
| **UI-LOG-19** | `network_request` | UI-SRC-6793FF938733 | Logs / Upstream Options Query | GET /admin/v1/upstreams | `read` | `available` |
| **UI-SRC-476EE1297D09** | `ui_action` | — | Logs / Principal Options Query | Fetch Principals for Options and Names | `read` | `available` |
| **UI-LOG-20** | `network_request` | UI-SRC-476EE1297D09 | Logs / Principal Options Query | GET /admin/v1/principals | `read` | `available` |
| **UI-SRC-2B0EFC709378** | `ui_action` | — | Logs / Live Stream Reconnect Delta Backfill | SSE Reconnect Delta Catch-Up Query | `read` | `available` |
| **UI-LOG-21** | `network_request` | UI-SRC-2B0EFC709378 | Logs / Live Stream Reconnect Delta Backfill | GET /admin/v1/events/delta | `read` | `available` |
| **UI-LOG-22** | `ui_action` | — | Logs / Live Tail Failure Banner Retry | Force Reconnect Button Click on Terminal SSE Failure | `read` | `available` |
| **REQ-SRC-B1BE83CDD51E** | `network_request` | UI-LOG-22 | Logs / Live Tail Failure Banner Retry | GET /admin/events/stream | `read` | `available` |
| **UI-LOG-25** | `ui_action` | — | Logs / Live Tail Failure Banner Dismiss | Dismiss Failure Banner | `read` | `available` |
| **UI-LOG-23** | `ui_action` | — | Logs / Request Drawer Identity Copy Buttons | Copy Identity Fields to Clipboard | `read` | `available` |
| **UI-LOG-28** | `ui_action` | — | Logs / Request Detail Drawer Close | Close Detail Drawer | `read` | `available` |
| **UI-LOG-29** | `ui_action` | — | Logs / Latency Timeline Stage Breakdown | Latency Stage Hover Popover and Sticky Click | `read` | `available` |
| **UI-LOG-30** | `ui_action` | — | Logs / Streaming Timeline Markers | Streaming Event Markers (TTFT / Message Start / Deltas / Stop) | `read` | `available` |
| **UI-LOG-31** | `ui_action` | — | Logs / Token & Cost Donut Charts | Pie Slice Hover and Sticky Click | `read` | `available` |
| **UI-LOG-26** | `ui_action` | — | Logs / Legacy Preset URL Normalization | Rewrite Legacy ?time_range= Preset to Absolute Unix Bounds | `read` | `available` |
| **UI-SRC-00BD57E4D253** | `ui_action` | — | Principals / PrincipalsPage | Load Principals List | `read` | `available` |
| **UI-PR-01** | `network_request` | UI-SRC-00BD57E4D253 | Principals / PrincipalsPage | GET /admin/v1/principals | `read` | `available` |
| **UI-SRC-241AEA7F7B55** | `ui_action` | — | Principals / PrincipalsPage | Desktop Auto-Select First Principal | `read` | `available` |
| **UI-PR-02** | `ui_action` | — | Principals / PrincipalsPage | Select Principal from Sidebar | `read` | `available` |
| **UI-SRC-B7B21779622C** | `ui_action` | — | Principals / PrincipalsPage | Action New URL Query Parameter Trigger | `read` | `available` |
| **UI-SRC-783B9A8424A8** | `ui_action` | — | Principals / PrincipalsPage | Open Create Principal Modal via New Button | `read` | `available` |
| **UI-SRC-E9A473CD5D19** | `ui_action` | — | Principals / CreatePrincipalModal | Create Principal Submit | `reversible_write` | `available` |
| **UI-PR-23** | `network_request` | UI-SRC-E9A473CD5D19 | Principals / CreatePrincipalModal | POST /admin/v1/principals | `reversible_write` | `available` |
| **UI-PR-03** | `ui_action` | — | Principals / PrincipalDetail | Toggle Principal Enabled State | `reversible_write` | `available` |
| **REQ-SRC-C31B76D697EB** | `network_request` | UI-PR-03 | Principals / PrincipalDetail | POST /admin/v1/principals/{id}/enable | `reversible_write` | `available` |
| **REQ-SRC-39C6706B40BF** | `network_request` | UI-PR-03 | Principals / PrincipalDetail | POST /admin/v1/principals/{id}/disable | `reversible_write` | `available` |
| **UI-SRC-B1990946C344** | `ui_action` | — | Principals / PrincipalDetail | Delete Principal via Confirm Dialog | `destructive_write` | `available` |
| **UI-PR-04** | `network_request` | UI-SRC-B1990946C344 | Principals / PrincipalDetail | DELETE /admin/v1/principals/{id} | `destructive_write` | `available` |
| **UI-SRC-B0C8E4E556BC** | `ui_action` | — | Principals / PrincipalDetail | Mobile Back to Master List | `read` | `available` |
| **UI-SRC-74652446E668** | `ui_action` | — | Principals / AllowedModelsCard | Update Allowed Models | `reversible_write` | `available` |
| **UI-PR-05** | `network_request` | UI-SRC-74652446E668 | Principals / AllowedModelsCard | PUT /admin/v1/principals/{id}/allowed_models | `reversible_write` | `available` |
| **UI-SRC-D74A22B571BC** | `ui_action` | — | Principals / DefaultLimitsCard | Update Principal Default Limits | `reversible_write` | `available` |
| **UI-PR-06** | `network_request` | UI-SRC-D74A22B571BC | Principals / DefaultLimitsCard | PATCH /admin/v1/principals/{id} | `reversible_write` | `available` |
| **UI-SRC-5EED9F4C1CB5** | `ui_action` | — | Principals / RecentRequestsCard | Recent Requests Principal Polling | `read` | `available` |
| **UI-PR-24** | `network_request` | UI-SRC-5EED9F4C1CB5 | Principals / RecentRequestsCard | GET /admin/events/recent | `read` | `available` |
| **UI-SRC-7FA58E7543A9** | `ui_action` | — | Principals / RouterSlotEditor | Load Router Plugin Chain | `read` | `available` |
| **UI-PR-13A** | `network_request` | UI-SRC-7FA58E7543A9 | Principals / RouterSlotEditor | GET /admin/v1/principals/{id}/plugin-chain | `read` | `available` |
| **UI-SRC-F380C424AEC6** | `ui_action` | — | Principals / RouterSlotEditor | Load Router Terminal Strategy | `read` | `available` |
| **UI-PR-13B** | `network_request` | UI-SRC-F380C424AEC6 | Principals / RouterSlotEditor | GET /admin/v1/principals/{id}/router-terminal | `read` | `available` |
| **UI-SRC-2C8DBDF42A9A** | `ui_action` | — | Principals / RouterSlotEditor | Router Editor Tab Switching | `read` | `available` |
| **UI-SRC-72D80A37A7FC** | `ui_action` | — | Principals / RouterSlotEditor | Toggle Subscription Preference Plugin (Basic Tab) | `reversible_write` | `available` |
| **UI-PR-15** | `network_request` | UI-SRC-72D80A37A7FC | Principals / RouterSlotEditor | POST /admin/v1/principals/{id}/plugin-chain | `reversible_write` | `available` |
| **UI-PR-15B** | `network_request` | UI-SRC-72D80A37A7FC | Principals / RouterSlotEditor | DELETE /admin/v1/plugin-chain-entries/{id} | `destructive_write` | `available` |
| **UI-SRC-12BD8F041AC6** | `ui_action` | — | Principals / RouterSlotEditor | Change Router Terminal Strategy | `reversible_write` | `available` |
| **UI-PR-14** | `network_request` | UI-SRC-12BD8F041AC6 | Principals / RouterSlotEditor | PUT /admin/v1/principals/{id}/router-terminal | `reversible_write` | `available` |
| **UI-SRC-D4D3CDAFC561** | `ui_action` | — | Principals / RouterSlotEditor | Add Router Filter from Popover (Advanced Tab) | `reversible_write` | `available` |
| **UI-PR-16B** | `network_request` | UI-SRC-D4D3CDAFC561 | Principals / RouterSlotEditor | POST /admin/v1/principals/{id}/plugin-chain | `reversible_write` | `available` |
| **UI-SRC-C4E25AD91ABF** | `ui_action` | — | Principals / RouterSlotEditor | Reorder Router Filters via Move Up/Down | `reversible_write` | `available` |
| **UI-PR-16** | `network_request` | UI-SRC-C4E25AD91ABF | Principals / RouterSlotEditor | POST /admin/v1/principals/{id}/plugin-chain/reorder | `reversible_write` | `available` |
| **UI-SRC-797468122201** | `ui_action` | — | Principals / RouterSlotEditor | Remove Router Filter Step | `destructive_write` | `available` |
| **UI-PR-16C** | `network_request` | UI-SRC-797468122201 | Principals / RouterSlotEditor | DELETE /admin/v1/plugin-chain-entries/{id} | `destructive_write` | `available` |
| **UI-SRC-9E71353DC28E** | `ui_action` | — | Principals / PluginDetailDrawer | View Plugin Details Drawer | `read` | `available` |
| **UI-PR-18** | `ui_action` | — | Principals / ShapeSlotEditor | Select Shape Slot Plugin | `reversible_write` | `available` |
| **UI-PR-18A** | `network_request` | UI-PR-18 | Principals / ShapeSlotEditor | DELETE /admin/v1/plugin-chain-entries/{id} | `destructive_write` | `available` |
| **UI-PR-18B** | `network_request` | UI-PR-18 | Principals / ShapeSlotEditor | POST /admin/v1/principals/{id}/plugin-chain | `reversible_write` | `available` |
| **UI-SRC-4A31DF64FCC2** | `ui_action` | — | Principals / ObservabilityHookEditor | Reorder Observability Hooks via Drag-and-Drop | `reversible_write` | `available` |
| **UI-PR-19B** | `network_request` | UI-SRC-4A31DF64FCC2 | Principals / ObservabilityHookEditor | POST /admin/v1/principals/{id}/plugin-chain/reorder | `reversible_write` | `available` |
| **UI-SRC-8AE44ADD2C3E** | `ui_action` | — | Principals / ObservabilityHookEditor | Remove Observability Hook | `destructive_write` | `available` |
| **UI-PR-19C** | `network_request` | UI-SRC-8AE44ADD2C3E | Principals / ObservabilityHookEditor | DELETE /admin/v1/plugin-chain-entries/{id} | `destructive_write` | `available` |
| **UI-SRC-5DE97D4F1CFC** | `ui_action` | — | Principals / ApiKeysCard | Load Principal API Keys | `read` | `available` |
| **UI-PR-20** | `network_request` | UI-SRC-5DE97D4F1CFC | Principals / ApiKeysCard | GET /admin/v1/principals/{id}/keys | `read_with_audit` | `available` |
| **UI-SRC-911C0971EF03** | `ui_action` | — | Principals / ApiKeysCard | Issue New API Key | `reversible_write` | `available` |
| **UI-PR-21** | `network_request` | UI-SRC-911C0971EF03 | Principals / ApiKeysCard | POST /admin/v1/principals/{id}/keys | `reversible_write` | `available` |
| **UI-SRC-B4272A92476E** | `ui_action` | — | Principals / ApiKeysCard | Revoke API Key | `destructive_write` | `available` |
| **UI-PR-22** | `network_request` | UI-SRC-B4272A92476E | Principals / ApiKeysCard | POST /admin/v1/principals/{id}/keys/{key_id}/revoke | `destructive_write` | `available` |
| **UI-SRC-A3E7AE6357CE** | `ui_action` | — | Principals / CacheKeepaliveCard | Cache Keepalive Card Summary Polling (limit=0) | `read` | `available` |
| **UI-PR-07** | `network_request` | UI-SRC-A3E7AE6357CE | Principals / CacheKeepaliveCard | GET /admin/v1/principals/{id}/cache-keepalive | `read` | `available` |
| **UI-SRC-921EE69973B8** | `ui_action` | — | Principals / CacheKeepaliveCard | Toggle Principal Cache Keepalive Enabled | `reversible_write` | `available` |
| **UI-PR-08** | `network_request` | UI-SRC-921EE69973B8 | Principals / CacheKeepaliveCard | PATCH /admin/v1/principals/{id} | `reversible_write` | `available` |
| **UI-PR-09A** | `ui_action` | — | Principals / CacheKeepaliveSettingsDrawer | Save Cache Keepalive Advanced Settings | `reversible_write` | `available` |
| **UI-PR-09** | `network_request` | UI-PR-09A | Principals / CacheKeepaliveSettingsDrawer | PATCH /admin/v1/principals/{id} | `reversible_write` | `available` |
| **UI-PR-10** | `ui_action` | — | Principals / CacheKeepaliveSessionsDrawer | Cache Keepalive Sessions List Query & Polling (limit omitted) | `read` | `available` |
| **UI-PR-10A** | `network_request` | UI-PR-10 | Principals / CacheKeepaliveSessionsDrawer | GET /admin/v1/principals/{id}/cache-keepalive | `read` | `available` |
| **UI-SRC-88A40F094821** | `ui_action` | — | Principals / CacheKeepaliveSessionsDrawer | Horizon Toggle Selection & Cursor Invalidation | `read` | `available` |
| **REQ-SRC-68354F29C6F2** | `network_request` | UI-SRC-88A40F094821 | Principals / CacheKeepaliveSessionsDrawer | GET /admin/v1/principals/{id}/cache-keepalive | `read` | `available` |
| **UI-SRC-9E180B20857B** | `ui_action` | — | Principals / CacheKeepaliveSessionsDrawer | Status Filter Selection (All / State / Error) | `read` | `available` |
| **UI-PR-11** | `network_request` | UI-SRC-9E180B20857B | Principals / CacheKeepaliveSessionsDrawer | GET /admin/v1/principals/{id}/cache-keepalive | `read` | `available` |
| **UI-SRC-B75FF5FEE2B9** | `ui_action` | — | Principals / CacheKeepaliveSessionsDrawer | Sessions Infinite Pagination via Cursor | `read` | `available` |
| **UI-PR-11B** | `network_request` | UI-SRC-B75FF5FEE2B9 | Principals / CacheKeepaliveSessionsDrawer | GET /admin/v1/principals/{id}/cache-keepalive | `read` | `available` |
| **UI-SRC-BACAC4FD9C6A** | `ui_action` | — | Principals / SessionDetailPane | Cache Keepalive Session Detail Polling | `read` | `available` |
| **UI-PR-12** | `network_request` | UI-SRC-BACAC4FD9C6A | Principals / SessionDetailPane | GET /admin/v1/principals/{id}/cache-keepalive/{session_id} | `read` | `available` |
| **UI-SRC-2DEF2F42BB04** | `ui_action` | — | Principals / SessionDetailPane | Session Detail Collapsible Sections | `read` | `available` |
| **UI-SRC-D94B76752475** | `ui_action` | — | Plugins / PluginsPage | Plugins Page Mount & Registry Fetch | `read` | `available` |
| **UI-PLUG-01** | `network_request` | UI-SRC-D94B76752475 | Plugins / PluginsPage | GET /admin/v1/plugins/registry | `read` | `available` |
| **UI-PLUG-05** | `ui_action` | — | Plugins / PluginCatalog | Catalog Row Inspect Click | `read` | `available` |
| **UI-PLUG-11** | `ui_action` | — | Plugins / PluginCatalog | Catalog Row Copy SHA256 | `read` | `available` |
| **UI-PLUG-10** | `ui_action` | — | Plugins / PluginCatalog | Catalog Row Delete Button Click | `read` | `available` |
| **UI-SRC-42AEDACD7B73** | `ui_action` | — | Plugins / PluginCatalog | Garbage Collection (Clean Orphaned Uploads) | `destructive_write` | `available` |
| **UI-PLUG-04** | `network_request` | UI-SRC-42AEDACD7B73 | Plugins / PluginCatalog | POST /admin/v1/plugins/wasm/gc | `destructive_write` | `available` |
| **UI-SRC-F8C17F59AB34** | `ui_action` | — | Plugins / PluginUploadCard | WASM Plugin File Upload (Browse or Drop) | `reversible_write` | `available` |
| **UI-PLUG-02** | `network_request` | UI-SRC-F8C17F59AB34 | Plugins / PluginUploadCard | POST /admin/v1/plugins/wasm | `reversible_write` | `available` |
| **UI-SRC-19B510951607** | `ui_action` | — | Plugins / PluginUploadCard | Confirm Plugin Replacement (409 Conflict Resolution) | `destructive_write` | `available` |
| **UI-PLUG-03** | `network_request` | UI-SRC-19B510951607 | Plugins / PluginUploadCard | POST /admin/v1/plugins/wasm | `reversible_write` | `available` |
| **UI-PLUG-20** | `ui_action` | — | Plugins / PluginUploadCard | Cancel Plugin Replacement | `read` | `available` |
| **UI-PLUG-14** | `ui_action` | — | Plugins / PluginsPage | Upload Action Modal Lifecycle | `read` | `available` |
| **UI-SRC-FBE359698524** | `ui_action` | — | Plugins / PluginDeleteDialog | Delete Dialog References Fetch | `read` | `available` |
| **UI-PLUG-09** | `network_request` | UI-SRC-FBE359698524 | Plugins / PluginDeleteDialog | GET /admin/v1/plugins/registry/{id}/references | `read` | `available` |
| **UI-SRC-9693EBA2D460** | `ui_action` | — | Plugins / PluginDeleteDialog | Plugin Deletion Confirm (Simple & Cascade) | `destructive_write` | `available` |
| **UI-PLUG-08** | `network_request` | UI-SRC-9693EBA2D460 | Plugins / PluginDeleteDialog | DELETE /admin/v1/plugins/registry/{id} | `destructive_write` | `available` |
| **UI-PLUG-19** | `ui_action` | — | Plugins / PluginDeleteDialog | Cancel Plugin Deletion | `read` | `available` |
| **UI-PLUG-13** | `ui_action` | — | Plugins / PluginDetail | Back to Catalog Button Click | `read` | `available` |
| **UI-PLUG-12** | `ui_action` | — | Plugins / PluginDetailIntegrity | Detail Card Copy SHA256 | `read` | `available` |
| **UI-PLUG-16** | `ui_action` | — | Plugins / PluginDetailOperate | Inline Label Edit Form Toggle (Open & Cancel) | `read` | `available` |
| **UI-SRC-824B79F88848** | `ui_action` | — | Plugins / PluginDetailOperate | Save Plugin Label Mutation | `reversible_write` | `available` |
| **UI-PLUG-07** | `network_request` | UI-SRC-824B79F88848 | Plugins / PluginDetailOperate | PATCH /admin/v1/plugins/registry/{id} | `reversible_write` | `available` |
| **UI-PLUG-18** | `ui_action` | — | Plugins / PluginDetailOperate | Detail View Delete Button Click | `read` | `available` |
| **UI-PLUG-21** | `ui_action` | — | Plugins / PluginDetail | Used By Reference Link Click | `read` | `available` |
| **UI-PLUG-22** | `ui_action` | — | Plugins / PluginDetailApply | Apply Plugin Target Navigation Links | `read` | `available` |
| **UI-SRC-DF3F46767D73** | `ui_action` | — | Settings / Version Card | System Status & Version Telemetry | `read` | `available` |
| **UI-SET-01** | `network_request` | UI-SRC-DF3F46767D73 | Settings / Version Card | GET /admin/v1/status | `read` | `available` |
| **UI-SRC-0744A60F49D3** | `ui_action` | — | Settings / Admin Token Card | Admin Token Guidance View | `read` | `available` |
| **UI-SET-10** | `ui_action` | — | Settings / Localization Card | Change Locale Preference | `read` | `available` |
| **UI-SET-11** | `ui_action` | — | Settings / Localization Card | Change Timezone Preference | `read` | `available` |
| **UI-SRC-5A139160911F** | `ui_action` | — | Settings / Localization Card | Localization Live Preview Clock Tick | `read` | `available` |
| **UI-SET-03** | `ui_action` | — | Settings / ConfigDraftSection | Load Configuration Draft Pipeline Data | `read` | `available` |
| **UI-SET-03B** | `network_request` | UI-SET-03 | Settings / ConfigDraftSection | GET /admin/config/draft | `read_with_audit` | `available` |
| **UI-SET-03A** | `network_request` | UI-SET-03 | Settings / ConfigDraftSection | GET /admin/config/current | `read_with_audit` | `available` |
| **UI-SET-03C** | `network_request` | UI-SET-03 | Settings / ConfigDraftSection | GET /admin/config/schema | `read` | `available` |
| **UI-SET-14** | `ui_action` | — | Settings / ConfigDraftSection | Edit Draft Textarea | `read` | `available` |
| **UI-SRC-67C497520AA2** | `ui_action` | — | Settings / ConfigDraftSection | Save Draft | `reversible_write` | `available` |
| **UI-SET-04** | `network_request` | UI-SRC-67C497520AA2 | Settings / ConfigDraftSection | PUT /admin/config/draft | `reversible_write` | `available` |
| **UI-SRC-BFD8C956C3C5** | `ui_action` | — | Settings / ConfigDraftSection | Validate Draft | `reversible_write` | `available` |
| **UI-SET-05** | `network_request` | UI-SRC-BFD8C956C3C5 | Settings / ConfigDraftSection | POST /admin/config/draft/validate | `reversible_write` | `available` |
| **UI-SRC-EDE5449D42BD** | `ui_action` | — | Settings / ConfigDraftSection | Apply Draft Revision | `reversible_write` | `available` |
| **UI-SET-06** | `network_request` | UI-SRC-EDE5449D42BD | Settings / ConfigDraftSection | POST /admin/config/apply | `destructive_write` | `available` |
| **UI-SRC-EA3DE5874C4D** | `ui_action` | — | Settings / ConfigDraftSection | Trigger Daemon Configuration Hot Reload | `external_action` | `available` |
| **UI-SET-07** | `network_request` | UI-SRC-EA3DE5874C4D | Settings / ConfigDraftSection | POST /admin/config/reload | `external_action` | `available` |
| **UI-SET-12** | `ui_action` | — | Settings / ConfigDraftSection | Retry Draft Editor Loading | `read` | `available` |
| **UI-SET-12B** | `network_request` | UI-SET-12 | Settings / ConfigDraftSection | GET /admin/config/draft | `read_with_audit` | `available` |
| **UI-SET-12A** | `network_request` | UI-SET-12 | Settings / ConfigDraftSection | GET /admin/config/current | `read_with_audit` | `available` |
| **UI-SET-13** | `ui_action` | — | Settings / ConfigDraftSection | Toggle Schema Coverage Checklist | `read` | `available` |
| **UI-SRC-C9DD3E291979** | `ui_action` | — | Settings / ConfigHistorySection | Load Configuration History | `read` | `available` |
| **UI-SET-08** | `network_request` | UI-SRC-C9DD3E291979 | Settings / ConfigHistorySection | GET /admin/config/history | `read_with_audit` | `available` |
| **UI-SRC-625C96B0933F** | `ui_action` | — | Settings / RestartRequiredMatrix | Restart-Required Fields Matrix View | `read` | `available` |
| **UI-SRC-49B5404AC804** | `ui_action` | — | Settings / Configuration Export Card | Download Configuration Export Snapshot | `read` | `available` |
| **UI-SET-02** | `network_request` | UI-SRC-49B5404AC804 | Settings / Configuration Export Card | GET /admin/v1/export | `read_with_audit` | `available` |
| **UI-SRC-2FC5E3F9B148** | `ui_action` | — | Audit / AuditPage | Load Audit Trail & Supporting Entity Maps | `read` | `available` |
| **UI-AUD-01** | `network_request` | UI-SRC-2FC5E3F9B148 | Audit / AuditPage | GET /admin/audit | `read_with_audit` | `available` |
| **UI-AUD-07** | `network_request` | UI-SRC-2FC5E3F9B148 | Audit / AuditPage | GET /admin/v1/principals | `read` | `available` |
| **UI-AUD-08** | `network_request` | UI-SRC-2FC5E3F9B148 | Audit / AuditPage | GET /admin/v1/upstreams | `read` | `available` |
| **UI-SRC-8A3FB89087F5** | `ui_action` | — | Audit / AuditPage | Refresh Audit Trail | `read` | `available` |
| **UI-AUD-05** | `network_request` | UI-SRC-8A3FB89087F5 | Audit / AuditPage | GET /admin/audit | `read_with_audit` | `available` |
| **UI-SRC-DD556FCB4A54** | `ui_action` | — | Audit / AuditPage Filter Bar | Filter by Principal ID | `read` | `available` |
| **UI-AUD-02** | `network_request` | UI-SRC-DD556FCB4A54 | Audit / AuditPage Filter Bar | GET /admin/audit | `read_with_audit` | `available` |
| **UI-SRC-00FD191B0EB1** | `ui_action` | — | Audit / TimeRangeBounds | Edit Range Start (From) Field | `read` | `available` |
| **UI-SRC-C7A6EA2F6025** | `ui_action` | — | Audit / TimeRangeBounds | Edit Range End (To) Field | `read` | `available` |
| **UI-SRC-FA47104074F4** | `ui_action` | — | Audit / CalendarPopover | Pick From Date in Calendar Popover | `read` | `available` |
| **REQ-SRC-0829EA9261F6** | `network_request` | UI-SRC-FA47104074F4 | Audit / CalendarPopover | GET /admin/audit | `read_with_audit` | `available` |
| **UI-SRC-38E343D2A4C1** | `ui_action` | — | Audit / CalendarPopover | Pick To Date in Calendar Popover | `read` | `available` |
| **REQ-SRC-66D17D1EF13A** | `network_request` | UI-SRC-38E343D2A4C1 | Audit / CalendarPopover | GET /admin/audit | `read_with_audit` | `available` |
| **UI-SRC-D309B057EC9E** | `ui_action` | — | Audit / TimeRangeBounds | Apply Time Range Bounds | `read` | `available` |
| **UI-AUD-03** | `network_request` | UI-SRC-D309B057EC9E | Audit / TimeRangeBounds | GET /admin/audit | `read_with_audit` | `available` |
| **UI-AUD-04** | `ui_action` | — | Audit / AuditPage Filter Bar | Clear Filter Bar Filters | `read` | `available` |
| **REQ-SRC-CAE9CDF1B8F6** | `network_request` | UI-AUD-04 | Audit / AuditPage Filter Bar | GET /admin/audit | `read_with_audit` | `available` |
| **UI-AUD-09** | `ui_action` | — | Audit / AuditPage EmptyState | Clear Filters from Empty State Action | `read` | `available` |
| **REQ-SRC-91FBA8F4830F** | `network_request` | UI-AUD-09 | Audit / AuditPage EmptyState | GET /admin/audit | `read_with_audit` | `available` |
| **UI-AUD-06** | `ui_action` | — | Audit / Audit Table Row | Open Audit Detail Modal | `read` | `available` |
| **UI-AUD-10** | `ui_action` | — | Audit / Audit Detail Modal | Close Audit Detail Modal | `read` | `available` |
| **UI-SRC-BDC81AB9B863** | `ui_action` | — | Upstreams / Upstreams Master List | Upstreams Master List Mount & Polling | `read` | `available` |
| **UI-UP-01** | `network_request` | UI-SRC-BDC81AB9B863 | Upstreams / Upstreams Master List | GET /admin/v1/upstreams | `read` | `available` |
| **UI-SRC-3FC895176F17** | `ui_action` | — | Upstreams / Upstream Quota Latest Snapshots | Subscription Quota Latest Polling | `read` | `available` |
| **UI-UP-02** | `network_request` | UI-SRC-3FC895176F17 | Upstreams / Upstream Quota Latest Snapshots | GET /admin/v1/subscription-quotas/latest | `read` | `available` |
| **UI-SRC-F25206CE1E1B** | `ui_action` | — | Upstreams / Upstream 7d Usage Bar | Upstream 7d Usage Summary Query | `read` | `available` |
| **UI-UP-03** | `network_request` | UI-SRC-F25206CE1E1B | Upstreams / Upstream 7d Usage Bar | GET /admin/usage | `read` | `available` |
| **UI-SRC-E4C913E07FEC** | `ui_action` | — | Upstreams / Runtime Apply Status | Runtime Status Query | `read` | `available` |
| **UI-UP-04** | `network_request` | UI-SRC-E4C913E07FEC | Upstreams / Runtime Apply Status | GET /admin/v1/status | `read` | `available` |
| **UI-UP-05** | `ui_action` | — | Upstreams / Upstream Item Click in Sidebar | Select Upstream in Sidebar | `read` | `available` |
| **UI-SRC-CD87608DA5D5** | `ui_action` | — | Upstreams / Upstream Subscription Metadata | Upstream Subscription Metadata Query | `read` | `available` |
| **UI-UP-06** | `network_request` | UI-SRC-CD87608DA5D5 | Upstreams / Upstream Subscription Metadata | GET /admin/v1/upstreams/{id}/subscription-metadata | `read` | `available` |
| **UI-SRC-1EEBA1898F34** | `ui_action` | — | Upstreams / Refresh Metadata Button | Trigger Subscription Metadata Refresh | `reversible_write` | `available` |
| **UI-UP-07** | `network_request` | UI-SRC-1EEBA1898F34 | Upstreams / Refresh Metadata Button | POST /admin/v1/upstreams/{id}/subscription-metadata/refresh | `external_action` | `available` |
| **UI-SRC-224914630CC5** | `ui_action` | — | Upstreams / Quota Series Query | Subscription Quota Series Query | `read` | `available` |
| **UI-UP-08A** | `network_request` | UI-SRC-224914630CC5 | Upstreams / Quota Series Query | GET /admin/v1/subscription-quotas/series | `read` | `available` |
| **UI-SRC-2351FFDF6073** | `ui_action` | — | Upstreams / Quota Analysis Query | Subscription Quota Analysis Query | `read` | `available` |
| **UI-UP-08B** | `network_request` | UI-SRC-2351FFDF6073 | Upstreams / Quota Analysis Query | GET /admin/v1/subscription-quotas/analysis | `read` | `available` |
| **UI-UP-09** | `ui_action` | — | Upstreams / Quota History Range Toggle | Toggle Quota History Range | `read` | `available` |
| **UI-UP-09A** | `network_request` | UI-UP-09 | Upstreams / Quota History Range Toggle | GET /admin/v1/subscription-quotas/series | `read` | `available` |
| **UI-UP-09B** | `network_request` | UI-UP-09 | Upstreams / Quota History Range Toggle | GET /admin/v1/subscription-quotas/analysis | `read` | `available` |
| **UI-UP-10** | `ui_action` | — | Upstreams / Quota History Legend Window Isolation | Isolate Quota Window in Legend | `read` | `available` |
| **UI-SRC-0D929B471D7A** | `ui_action` | — | Upstreams / Inline Name Editor | Rename Upstream | `reversible_write` | `available` |
| **UI-UP-11** | `network_request` | UI-SRC-0D929B471D7A | Upstreams / Inline Name Editor | PUT /admin/v1/upstreams/{id} | `reversible_write` | `available` |
| **UI-SRC-C72B3D4F216B** | `ui_action` | — | Upstreams / Toggle Upstream Enabled | Toggle Upstream Enabled Switch | `reversible_write` | `available` |
| **UI-UP-12** | `network_request` | UI-SRC-C72B3D4F216B | Upstreams / Toggle Upstream Enabled | PATCH /admin/v1/upstreams/{id} | `reversible_write` | `available` |
| **UI-SRC-9FD3FEC04C3D** | `ui_action` | — | Upstreams / SettingsCard (Base URL / API Key) | Update Non-OAuth Upstream Settings | `reversible_write` | `available` |
| **UI-UP-13** | `network_request` | UI-SRC-9FD3FEC04C3D | Upstreams / SettingsCard (Base URL / API Key) | PUT /admin/v1/upstreams/{id} | `reversible_write` | `available` |
| **UI-SRC-7F9359EA0052** | `ui_action` | — | Upstreams / ApiUsageCard Range Toggle | Toggle API Usage Range (24h / 7d) | `read` | `available` |
| **UI-UP-14** | `network_request` | UI-SRC-7F9359EA0052 | Upstreams / ApiUsageCard Range Toggle | GET /admin/usage | `read` | `available` |
| **UI-SRC-6D1976AC76E2** | `ui_action` | — | Upstreams / WarmupCardMinimal Toggle Enabled | Toggle Warmup Enabled | `reversible_write` | `available` |
| **UI-UP-15** | `network_request` | UI-SRC-6D1976AC76E2 | Upstreams / WarmupCardMinimal Toggle Enabled | PATCH /admin/v1/upstreams/{id} | `reversible_write` | `available` |
| **UI-SRC-1B467A3726A3** | `ui_action` | — | Upstreams / Warmup Dialect Plugin Dropdown | Select Warmup Shape Plugin | `reversible_write` | `available` |
| **UI-UP-16** | `network_request` | UI-SRC-1B467A3726A3 | Upstreams / Warmup Dialect Plugin Dropdown | PATCH /admin/v1/upstreams/{id} | `reversible_write` | `available` |
| **UI-SRC-329B12DA3BC3** | `ui_action` | — | Upstreams / Warmup Clear Dialect Plugin | Clear Warmup Shape Plugin | `reversible_write` | `available` |
| **UI-UP-17** | `network_request` | UI-SRC-329B12DA3BC3 | Upstreams / Warmup Clear Dialect Plugin | DELETE /admin/v1/upstreams/{id}/warmup-dialect-plugin | `reversible_write` | `available` |
| **UI-SRC-C41372BCA4B0** | `ui_action` | — | Upstreams / Warmup Fire Now Button | Fire Warmup Now | `external_action` | `available` |
| **UI-UP-18** | `network_request` | UI-SRC-C41372BCA4B0 | Upstreams / Warmup Fire Now Button | POST /admin/v1/upstreams/{id}/warmup/fire-now | `external_action` | `available` |
| **UI-SRC-4851B3D30282** | `ui_action` | — | Upstreams / Warmup History Drawer Open | Open Warmup History Drawer | `read` | `available` |
| **UI-UP-19** | `network_request` | UI-SRC-4851B3D30282 | Upstreams / Warmup History Drawer Open | GET /admin/v1/upstreams/{id}/warmup/attempts | `read` | `available` |
| **UI-UP-20** | `ui_action` | — | Upstreams / Warmup History Attempt Click | Select Warmup Attempt in Drawer | `read` | `available` |
| **UI-SRC-49171C2D2202** | `ui_action` | — | Upstreams / Re-authenticate OAuth Upstream | Start OAuth Authorization Flow | `external_action` | `available` |
| **UI-UP-22** | `network_request` | UI-SRC-49171C2D2202 | Upstreams / Re-authenticate OAuth Upstream | POST /admin/v1/upstreams/{id}/oauth/start | `reversible_write` | `available` |
| **UI-SRC-0D6108CB1DBC** | `ui_action` | — | Upstreams / Paste OAuth Callback Code | Complete OAuth Token Exchange | `external_action` | `available` |
| **UI-UP-23** | `network_request` | UI-SRC-0D6108CB1DBC | Upstreams / Paste OAuth Callback Code | POST /admin/v1/upstreams/{id}/oauth/complete | `external_action` | `available` |
| **UI-SRC-9D5B9DACBF9A** | `ui_action` | — | Upstreams / Delete Upstream Button | Delete Upstream | `reversible_write` | `available` |
| **UI-UP-24** | `network_request` | UI-SRC-9D5B9DACBF9A | Upstreams / Delete Upstream Button | DELETE /admin/v1/upstreams/{id} | `destructive_write` | `available` |
| **UI-UP-25** | `ui_action` | — | Upstreams / Create Upstream Modal Open | Open Create Upstream Modal | `read` | `available` |
| **UI-SRC-5A9E89C6EEB0** | `ui_action` | — | Upstreams / Submit API Key Upstream Form | Create Non-OAuth Upstream | `reversible_write` | `available` |
| **UI-UP-26** | `network_request` | UI-SRC-5A9E89C6EEB0 | Upstreams / Submit API Key Upstream Form | POST /admin/v1/upstreams | `reversible_write` | `available` |
| **UI-SRC-427756CE86BB** | `ui_action` | — | Upstreams / Start OAuth Draft for New Upstream | Start OAuth Draft Flow | `external_action` | `available` |
| **UI-UP-27** | `network_request` | UI-SRC-427756CE86BB | Upstreams / Start OAuth Draft for New Upstream | POST /admin/v1/oauth/draft/start | `reversible_write` | `available` |
| **UI-SRC-CB056EC17B20** | `ui_action` | — | Upstreams / Submit OAuth Code for Draft | Verify OAuth Draft Code | `external_action` | `available` |
| **UI-UP-28** | `network_request` | UI-SRC-CB056EC17B20 | Upstreams / Submit OAuth Code for Draft | POST /admin/v1/oauth/draft/complete | `external_action` | `available` |
| **UI-SRC-2533F30C81A8** | `ui_action` | — | Upstreams / Create Upstream after OAuth Verified | Confirm and Create OAuth Upstream | `reversible_write` | `available` |
| **UI-UP-29** | `network_request` | UI-SRC-2533F30C81A8 | Upstreams / Create Upstream after OAuth Verified | POST /admin/v1/upstreams/from-oauth-draft | `reversible_write` | `available` |
| **UI-SRC-057C5139E406** | `ui_action` | — | Upstreams / Upstream OAuth Status | Upstream OAuth Status Query | `external_action` | `available` |
| **UI-UP-30** | `network_request` | UI-SRC-057C5139E406 | Upstreams / Upstream OAuth Status | GET /admin/v1/upstreams/{id}/oauth/status | `read` | `available` |
| **UI-SRC-83D0030A34AF** | `ui_action` | — | Upstreams / Warmup Summary | Warmup Summary Query | `read` | `available` |
| **UI-UP-31** | `network_request` | UI-SRC-83D0030A34AF | Upstreams / Warmup Summary | GET /admin/v1/upstreams/{id}/warmup | `read` | `available` |
| **UI-SRC-AB584B8FD224** | `ui_action` | — | Upstreams / Warmup Shape Plugin Registry | Shape Plugin Registry Query | `read` | `available` |
| **UI-UP-32** | `network_request` | UI-SRC-AB584B8FD224 | Upstreams / Warmup Shape Plugin Registry | GET /admin/v1/plugins/registry | `read` | `available` |
| **UI-SRC-C28C003E8881** | `ui_action` | — | Upstreams / Selected Upstream Recent Requests | Recent Requests for Upstream | `read` | `available` |
| **UI-UP-33** | `network_request` | UI-SRC-C28C003E8881 | Upstreams / Selected Upstream Recent Requests | GET /admin/events/recent | `read` | `available` |
| **UI-SRC-9CDF9EE839AE** | `ui_action` | — | Upstreams / Warmup History Status Filter | Filter Warmup History by Status | `read` | `available` |
| **UI-UP-34** | `network_request` | UI-SRC-9CDF9EE839AE | Upstreams / Warmup History Status Filter | GET /admin/v1/upstreams/{id}/warmup/attempts | `read` | `available` |
| **UI-SRC-35D5E99A460E** | `ui_action` | — | Upstreams / Warmup History Pagination | Load Older Warmup Attempts | `read` | `available` |
| **UI-UP-35** | `network_request` | UI-SRC-35D5E99A460E | Upstreams / Warmup History Pagination | GET /admin/v1/upstreams/{id}/warmup/attempts | `read` | `available` |
| **UI-UP-36** | `ui_action` | — | Upstreams / Warmup History Horizon Filter | Toggle Warmup History Horizon (24h / 7d / All) | `read` | `available` |
| **UI-UP-37** | `ui_action` | — | Upstreams / Warmup History Collapsible Sections | Toggle Plugin Snapshot and Raw Record Collapsibles | `read` | `available` |
| **UI-UP-38** | `ui_action` | — | Upstreams / ApiUsageCard Metric Toggle | Toggle API Usage Metric (Tokens / Cost) | `read` | `available` |
| **UI-UP-39** | `ui_action` | — | Upstreams / Metadata Strip Expansion | Expand/Collapse Metadata Strip | `read` | `available` |
| **UI-UP-40** | `ui_action` | — | Upstreams / Mobile Back Button | Mobile Back Navigation | `read` | `available` |
| **UI-SRC-517760F07481** | `ui_action` | — | Upstreams / Attempt Detail Close | Close Attempt Detail in Drawer | `read` | `available` |
| **UI-SRC-668F412D6C0E** | `ui_action` | — | Upstreams / Warmup History Drawer Close | Close Warmup History Drawer | `read` | `available` |
| **UI-SRC-D96DA7353940** | `ui_action` | — | Upstreams / WarmupConfigModal Copy JSON | Copy Warmup Plugin Config JSON | `read` | `available` |
| **UI-SRC-D61F3782A082** | `ui_action` | — | Plugins / PluginDetail | Load Selected Plugin References | `read` | `available` |
| **UI-PLUG-06** | `network_request` | UI-SRC-D61F3782A082 | Plugins / PluginDetail | GET /admin/v1/plugins/registry/{id}/references | `read` | `available` |
| **UI-SRC-7BDECC53787E** | `ui_action` | — | Principals / ObservabilityHookEditor | Add Observability Hook | `reversible_write` | `available` |
| **UI-PR-19** | `network_request` | UI-SRC-7BDECC53787E | Principals / ObservabilityHookEditor | POST /admin/v1/principals/{id}/plugin-chain | `reversible_write` | `available` |
| **UI-SRC-D64907F28D4C** | `ui_action` | — | Principals / PrincipalDetail | Load Observability Plugin Chain | `read` | `available` |
| **UI-PR-13D** | `network_request` | UI-SRC-D64907F28D4C | Principals / PrincipalDetail | GET /admin/v1/principals/{id}/plugin-chain | `read` | `available` |
| **UI-SRC-30AC7206755A** | `ui_action` | — | Principals / PrincipalDetail | Load Shape Plugin Chain | `read` | `available` |
| **UI-PR-13C** | `network_request` | UI-SRC-30AC7206755A | Principals / PrincipalDetail | GET /admin/v1/principals/{id}/plugin-chain | `read` | `available` |
| **UI-SRC-2092158E84C1** | `ui_action` | — | Principals / Router, Shape, and Observability Editors | Load Plugin Registry for Principal Editors | `read` | `available` |
| **UI-PR-13E** | `network_request` | UI-SRC-2092158E84C1 | Principals / Router, Shape, and Observability Editors | GET /admin/v1/plugins/registry | `read` | `available` |
| **UI-SRC-18907E842823** | `ui_action` | — | Global / Topbar | Render Authenticated Administrator Identity Badge | `read` | `not_deployed` |
| **UI-SRC-453C347CE4F4** | `ui_action` | — | Audit / AuditPage | Render Audit Actor Metadata | `read` | `not_deployed` |
| **UI-SRC-F0FA0033252E** | `ui_action` | — | Shared Request Tables / LatencyCell | Inspect Latency Responsibility Breakdown | `read` | `not_deployed` |
| **PROD-UI-A5000384379C** | `ui_action` | — | Credentials / CredentialsPage | Load Credentials and OAuth Status | `read` | `available` |
| **PROD-REQ-AE097B1BD13F** | `network_request` | PROD-UI-A5000384379C | Credentials / CredentialsPage | GET /admin/credentials | `read` | `available` |
| **PROD-REQ-186B043F6662** | `network_request` | PROD-UI-A5000384379C | Credentials / CredentialsPage | GET /admin/oauth/status | `read` | `available` |
| **PROD-REQ-8DB3CB46F307** | `network_request` | PROD-UI-A5000384379C | Credentials / CredentialsPage | GET /admin/v1/principals | `read` | `available` |
| **PROD-UI-76168321F814** | `ui_action` | — | Credentials / CredentialsPage | Rotate Credential / Force Refresh OAuth Token | `reversible_write` | `available` |
| **PROD-REQ-B33C09B92A03** | `network_request` | PROD-UI-76168321F814 | Credentials / CredentialsPage | POST /admin/credentials/{provider}/{cred_id}/rotate | `reversible_write` | `available` |
| **PROD-UI-6011B5C8167A** | `ui_action` | — | Credentials / CredentialsPage | Revoke Credential | `destructive_write` | `available` |
| **PROD-REQ-32A95A5C0307** | `network_request` | PROD-UI-6011B5C8167A | Credentials / CredentialsPage | POST /admin/credentials/{provider}/{cred_id}/revoke | `destructive_write` | `available` |
| **PROD-UI-E68AB274C4E9** | `ui_action` | — | Status / StatusPage | Load System Status, Killswitch, and Observed Credentials | `read` | `available` |
| **PROD-REQ-6B11261782D1** | `network_request` | PROD-UI-E68AB274C4E9 | Status / StatusPage | GET /admin/v1/status | `read` | `available` |
| **PROD-REQ-0F9F7B032EC5** | `network_request` | PROD-UI-E68AB274C4E9 | Status / StatusPage | GET /admin/credentials | `read` | `available` |
| **PROD-REQ-26ECB56C1696** | `network_request` | PROD-UI-E68AB274C4E9 | Status / StatusPage | GET /admin/oauth/status | `read` | `available` |
| **PROD-REQ-AE0413AAFCDF** | `network_request` | PROD-UI-E68AB274C4E9 | Status / StatusPage | GET /admin/v1/principals | `read` | `available` |
| **PROD-UI-168BF3B193F4** | `ui_action` | — | Status / StatusPage | Engage or Disengage Global Killswitch | `destructive_write` | `available` |
| **PROD-REQ-817B78681D90** | `network_request` | PROD-UI-168BF3B193F4 | Status / StatusPage | POST /admin/killswitch | `destructive_write` | `available` |
| **PROD-REQ-841024D77392** | `network_request` | PROD-UI-168BF3B193F4 | Status / StatusPage | DELETE /admin/killswitch | `destructive_write` | `available` |
| **PROD-UI-3DF6CC4224CD** | `ui_action` | — | Global / Sidebar | Sidebar Navigation - Credentials | `read` | `available` |
| **PROD-UI-F22C53DA973A** | `ui_action` | — | Global / Sidebar | Sidebar Navigation - Status | `read` | `available` |
| **PROD-UI-7075CE908A3D** | `ui_action` | — | Global / CommandPalette | Command Palette - Go to Status | `read` | `available` |
| **PROD-UI-3BA367988C76** | `ui_action` | — | Plugins / PluginDetailOperate | Plugin Detail Global Killswitch Indicator | `read` | `available` |
| **PROD-REQ-341C216AF806** | `network_request` | PROD-UI-3BA367988C76 | Plugins / PluginDetailOperate | GET /admin/v1/status | `read` | `available` |
| **PROD-UI-01A4AC001FED** | `ui_action` | — | Plugins / PluginDetailOperate | Plugin Detail Global Chain Usage Summary | `read` | `available` |
| **PROD-REQ-448A5BA63A6D** | `network_request` | PROD-UI-01A4AC001FED | Plugins / PluginDetailOperate | GET /admin/v1/status | `read` | `available` |
| **API-SRC-C767025D0EDC** | `backend_endpoint` | — | Backend-Only / serve_index | GET / | `read` | `available` |
| **API-SRC-276F2E8B8B8D** | `backend_endpoint` | — | Backend-Only / serve_asset | GET /{*file} | `read` | `available` |
| **API-SRC-7A787CD5C3CF** | `backend_endpoint` | — | Backend-Only / admin_server_state | GET /admin/health/state | `read` | `available` |
| **API-SYS-02** | `backend_endpoint` | — | Backend-Only / handle_internal_partial_fetch | GET /internal/v1/partials/{event_id} | `read` | `available` |
| **API-SRC-E5535B2DF6A8** | `backend_endpoint` | — | Backend-Only / get_config | GET /admin/v1/config/current | `read_with_audit` | `available` |
| **API-SRC-CA5516D80DC5** | `backend_endpoint` | — | Backend-Only / get_config_schema | GET /admin/v1/config/schema | `read` | `available` |
| **API-SRC-51C67C36397F** | `backend_endpoint` | — | Backend-Only / get_config_draft | GET /admin/v1/config/draft | `read_with_audit` | `available` |
| **API-SRC-E9BB1496F09E** | `backend_endpoint` | — | Backend-Only / get_config_history | GET /admin/v1/config/history | `read_with_audit` | `available` |
| **API-BACKEND-CONFIG-DIFF** | `backend_endpoint` | — | Backend-Only / get_config_diff | GET /admin/config/diff | `read_with_audit` | `available` |
| **API-SRC-903AA90CBF0D** | `backend_endpoint` | — | Backend-Only / get_config_diff | GET /admin/v1/config/diff | `read_with_audit` | `available` |
| **API-SRC-15D90407F542** | `backend_endpoint` | — | Backend-Only / query_audit | GET /admin/v1/audit | `read_with_audit` | `available` |
| **API-SRC-C2E19B96F128** | `backend_endpoint` | — | Backend-Only / crate::dashboard_routes::handle_dashboard_summary | GET /admin/v1/dashboard/summary | `read` | `available` |
| **API-SRC-6272C4C4303C** | `backend_endpoint` | — | Backend-Only / crate::dashboard_routes::handle_dashboard_usage | GET /admin/v1/dashboard/usage | `read` | `available` |
| **API-SRC-17E768052C98** | `backend_endpoint` | — | Backend-Only / handle_dashboard_usage | GET /admin/dashboard/usage | `read` | `available` |
| **API-SRC-393CB1B46201** | `backend_endpoint` | — | Backend-Only / crate::events_routes::handle_recent_events | GET /admin/v1/events/recent | `read` | `available` |
| **API-SRC-220A728FB03F** | `backend_endpoint` | — | Backend-Only / crate::events_routes::handle_events_histogram | GET /admin/events/histogram | `read` | `available` |
| **API-SRC-149FAC449949** | `backend_endpoint` | — | Backend-Only / handle_events_delta | GET /admin/events/delta | `read` | `available` |
| **API-SRC-07C82F02A23B** | `backend_endpoint` | — | Backend-Only / crate::events_routes::handle_events_stream | GET /admin/v1/events/stream | `read` | `available` |
| **API-SRC-FC8E60B994F7** | `backend_endpoint` | — | Backend-Only / handle_latest | GET /admin/subscription-quotas/latest | `read` | `available` |
| **API-SRC-063C7455BC4D** | `backend_endpoint` | — | Backend-Only / handle_series | GET /admin/subscription-quotas/series | `read` | `available` |
| **API-SRC-B19EB1F130F2** | `backend_endpoint` | — | Backend-Only / handle_aggregate | GET /admin/subscription-quotas/aggregate | `read` | `available` |
| **API-SRC-1CD1FD9DEBA0** | `backend_endpoint` | — | Backend-Only / handle_analysis | GET /admin/subscription-quotas/analysis | `read` | `available` |
| **API-SRC-155F19B11A2E** | `backend_endpoint` | — | Backend-Only / handle_pool_history | GET /admin/subscription-quotas/pool-history | `read` | `available` |
| **API-SYS-04** | `backend_endpoint` | — | Backend-Only / status | GET /admin/scheduler/status | `read` | `available` |
| **API-SYS-03** | `backend_endpoint` | — | Backend-Only / failures | GET /admin/scheduler/failures | `read` | `available` |
| **API-SRC-BD965DDC38E0** | `backend_endpoint` | — | Backend-Only / get_registry | GET /admin/v1/plugins/registry/{id} | `read` | `available` |
| **API-SRC-8895068281DE** | `backend_endpoint` | — | Backend-Only / get_chain | GET /admin/v1/plugin-chain-entries/{id} | `read` | `available` |
| **API-SRC-166291C6FB8A** | `backend_endpoint` | — | Backend-Only / get_principal | GET /admin/v1/principals/{id} | `read` | `available` |
| **API-SRC-4B568BB20E22** | `backend_endpoint` | — | Backend-Only / get_allowed_models | GET /admin/v1/principals/{id}/allowed_models | `read` | `available` |
| **API-SRC-5E670B98E5A2** | `backend_endpoint` | — | Backend-Only / principal_usage | GET /admin/principals/{id}/usage | `read` | `available` |
| **API-SRC-BC21CB4F90EC** | `backend_endpoint` | — | Backend-Only / principal_usage | GET /admin/v1/principals/{id}/usage | `read` | `available` |
| **API-SRC-B7C3EDE0843A** | `backend_endpoint` | — | Backend-Only / principal_limits | GET /admin/principals/{id}/limits | `read` | `available` |
| **API-SRC-7E55E35928A2** | `backend_endpoint` | — | Backend-Only / principal_limits | GET /admin/v1/principals/{id}/limits | `read` | `available` |
| **API-SRC-92FD477750A5** | `backend_endpoint` | — | Backend-Only / get_api_key | GET /admin/principals/{id}/keys/{key_id} | `read` | `available` |
| **API-SRC-7383936FE537** | `backend_endpoint` | — | Backend-Only / principal_key_usage | GET /admin/principals/{id}/keys/{key_id}/usage | `read` | `available` |
| **API-SRC-7C0B903CAB5B** | `backend_endpoint` | — | Backend-Only / get_upstream | GET /admin/v1/upstreams/{id} | `read` | `available` |
| **API-SRC-5E2D298E32DE** | `backend_endpoint` | — | Backend-Only / revoke_api_key | POST /admin/principals/{id}/keys/{key_id}/revoke | `destructive_write` | `available` |
| **API-SRC-D05CE36481FC** | `backend_endpoint` | — | Backend-Only / disable_api_key | POST /admin/principals/{id}/keys/{key_id}/disable | `reversible_write` | `available` |
| **API-SRC-EECEBC08D3B2** | `backend_endpoint` | — | Backend-Only / enable_api_key | POST /admin/principals/{id}/keys/{key_id}/enable | `reversible_write` | `available` |
| **API-SRC-EE67A2692EE2** | `backend_endpoint` | — | Backend-Only / put_config_draft | PUT /admin/v1/config/draft | `reversible_write` | `available` |
| **API-SRC-EEA580024B08** | `backend_endpoint` | — | Backend-Only / validate_config_draft | POST /admin/v1/config/draft/validate | `reversible_write` | `available` |
| **API-SRC-B9B9B5853D97** | `backend_endpoint` | — | Backend-Only / apply_config_draft | POST /admin/v1/config/apply | `destructive_write` | `available` |
| **API-SRC-D6B9EEC29AA6** | `backend_endpoint` | — | Backend-Only / reload_config | POST /admin/v1/config/reload | `external_action` | `available` |
| **API-SYS-06** | `backend_endpoint` | — | Backend-Only / rebalance_chain | POST /admin/v1/principals/{principal_id}/plugin-chain/rebalance | `reversible_write` | `available` |
| **API-SRC-B624778A38FE** | `backend_endpoint` | — | Backend-Only / update_chain | PUT /admin/v1/plugin-chain-entries/{id} | `reversible_write` | `available` |
| **API-SRC-B1B43DF36688** | `backend_endpoint` | — | Backend-Only / update_principal | PUT /admin/v1/principals/{id} | `reversible_write` | `available` |
| **API-SYS-05** | `backend_endpoint` | — | Backend-Only / preview_route | POST /admin/v1/router/preview | `read` | `available` |
| **API-SRC-FA895179691B** | `backend_endpoint` | — | Backend-Only / enable_upstream | POST /admin/v1/upstreams/{id}/enable | `reversible_write` | `available` |
| **API-SRC-F00657C8F5E0** | `backend_endpoint` | — | Backend-Only / disable_upstream | POST /admin/v1/upstreams/{id}/disable | `reversible_write` | `available` |
| **PROD-API-5234FA674D91** | `backend_endpoint` | — | Production Backend-Only / crate::v1::status::status | GET /admin/status | `read` | `available` |
| **PROD-API-2B1E133DA8BF** | `backend_endpoint` | — | Production Backend-Only / get_killswitch | GET /admin/killswitch | `destructive_write` | `available` |
| **PROD-API-DCBF12986EB5** | `backend_endpoint` | — | Production Backend-Only / get_killswitch | GET /admin/v1/killswitch | `destructive_write` | `available` |
| **PROD-API-8AA0DB7F3093** | `backend_endpoint` | — | Production Backend-Only / set_killswitch | POST /admin/v1/killswitch | `destructive_write` | `available` |
| **PROD-API-C482408C3112** | `backend_endpoint` | — | Production Backend-Only / clear_killswitch | DELETE /admin/v1/killswitch | `destructive_write` | `available` |
| **PROD-API-6B655BAB2645** | `backend_endpoint` | — | Production Backend-Only / crate::credentials::list_credentials | GET /admin/v1/credentials | `read` | `available` |
| **PROD-API-D8B657F6FCA4** | `backend_endpoint` | — | Production Backend-Only / crate::credentials::list_oauth_status | GET /admin/v1/oauth/status | `read` | `available` |

## 4. Detailed Execution Specifications

### [UI-SRC-E63DB0A6963C] Global — Auto Session Verification on Mount/Storage Change

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-ROOT-01`
- **Source:** `crates/cc-lb-admin/web/src/components/AuthRequiredGate.tsx:45#loadSession`
- **Preconditions:** None
- **Steps:** Component mounts in __root.tsx → loadSession(true) is invoked → Checks getAdminToken() in localStorage['cc-lb-admin-token'] → Dispatches GET /admin/v1/auth/session
- **Scope:** `once_and_event_driven` — Component mount & window storage/auth events
- **Risk:** `read`
- **Production applicability:** `not_deployed` — Added after deployed commit e56d029e
- **Expected UI:** Shows Spinner while loading. Transitions to children (authenticated view) on 200, or auth form/error screen on 401/error.
- **Runtime result:** `PENDING`

### [UI-ROOT-01] Global — Auto Session Verification on Mount/Storage Change — getAuthSession

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-E63DB0A6963C`
- **Source:** `crates/cc-lb-admin/web/src/lib/api.ts:74#getAuthSession`
- **Preconditions:** None
- **Steps:** Component mounts in __root.tsx → loadSession(true) is invoked → Checks getAdminToken() in localStorage['cc-lb-admin-token'] → Dispatches GET /admin/v1/auth/session
- **Scope:** `once_and_event_driven` — Component mount & window storage/auth events
- **Risk:** `read`
- **Production applicability:** `not_deployed` — Added after deployed commit e56d029e
- **HTTP:** `GET /admin/v1/auth/session`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `auth_session`
- **Storage operations:** None
- **Cache / no-query path:** In-process reads only: AdminIdentity is extracted from the auth middleware request extension, while auth_mode is read separately from state.admin_auth.mode().
- **Side effects:** None
- **Expected UI:** Shows Spinner while loading. Transitions to children (authenticated view) on 200, or auth form/error screen on 401/error.
- **Runtime result:** `PENDING`

### [UI-SRC-7EF8F6882B60] Global — Submit Static Admin Bearer Token

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-ROOT-02`
- **Source:** `crates/cc-lb-admin/web/src/components/AuthRequiredGate.tsx:192#handleSubmit`
- **Preconditions:** Gate in 'required' status (static_token auth mode)
- **Steps:** User inputs non-empty Bearer token into password input → User clicks 'Sign in' button or presses Enter → handleSubmit validates non-empty trimmed value → setAdminToken(trimmed) writes to localStorage['cc-lb-admin-token'] → Dispatches GET /admin/v1/auth/session via loadSession(false)
- **Scope:** `on_user_action` — User form submission
- **Risk:** `read`
- **Production applicability:** `not_deployed` — Every atomic request for this action is absent from the deployed API.
- **Expected UI:** Loading spinner on submit button; on success renders authenticated shell; on failure shows field error
- **Runtime result:** `PENDING`

### [UI-ROOT-02] Global — Submit Static Admin Bearer Token — getAuthSession

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-7EF8F6882B60`
- **Source:** `crates/cc-lb-admin/web/src/lib/api.ts:74#getAuthSession`
- **Preconditions:** Gate in 'required' status (static_token auth mode)
- **Steps:** User inputs non-empty Bearer token into password input → User clicks 'Sign in' button or presses Enter → handleSubmit validates non-empty trimmed value → setAdminToken(trimmed) writes to localStorage['cc-lb-admin-token'] → Dispatches GET /admin/v1/auth/session via loadSession(false)
- **Scope:** `on_user_action` — User form submission
- **Risk:** `read`
- **Production applicability:** `not_deployed` — Endpoint added after deployed commit e56d029e
- **HTTP:** `GET /admin/v1/auth/session`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `auth_session`
- **Storage operations:** None
- **Cache / no-query path:** In-process reads only: AdminIdentity is extracted from the auth middleware request extension, while auth_mode is read separately from state.admin_auth.mode().
- **Side effects:** None
- **Expected UI:** Loading spinner on submit button; on success renders authenticated shell; on failure shows field error
- **Runtime result:** `PENDING`

### [UI-SRC-15F5F1954403] Global — Admin Token Input Empty Validation

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/AuthRequiredGate.tsx:194#handleSubmit`
- **Preconditions:** Auth form visible
- **Steps:** User submits empty or whitespace-only token
- **Scope:** `on_user_action` — User form submission
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Renders inline error: 'Token cannot be empty' below input
- **Client-only reason:** Client-side form input validation guard prior to network dispatch
- **Runtime result:** `PENDING`

### [UI-SRC-9D6A6AA71D0F] Global — External Auth Required Retry Click

- **Entry type:** `ui_action`
- **Atomic requests:** `REQ-SRC-D9664AAE1B32`
- **Source:** `crates/cc-lb-admin/web/src/components/AuthRequiredGate.tsx:182#loadSession`
- **Preconditions:** Gate in 'external_required' status
- **Steps:** Admin server configured with external auth (e.g. IdP/SSO) → Gate shows 'External authentication required' → User clicks 'Retry' button
- **Scope:** `on_user_action` — User button click
- **Risk:** `read`
- **Production applicability:** `not_deployed` — Every atomic request for this action is absent from the deployed API.
- **Expected UI:** Triggers loadSession(true) to re-verify session with admin server
- **Runtime result:** `PENDING`

### [REQ-SRC-D9664AAE1B32] Global — External Auth Required Retry Click — getAuthSession

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-9D6A6AA71D0F`
- **Source:** `crates/cc-lb-admin/web/src/lib/api.ts:74#getAuthSession`
- **Preconditions:** Gate in 'external_required' status
- **Steps:** Admin server configured with external auth (e.g. IdP/SSO) → Gate shows 'External authentication required' → User clicks 'Retry' button
- **Scope:** `on_user_action` — User button click
- **Risk:** `read`
- **Production applicability:** `not_deployed` — Endpoint added after deployed commit e56d029e
- **HTTP:** `GET /admin/v1/auth/session`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `auth_session`
- **Storage operations:** None
- **Cache / no-query path:** In-process reads only: AdminIdentity is extracted from the auth middleware request extension, while auth_mode is read separately from state.admin_auth.mode().
- **Side effects:** None
- **Expected UI:** Triggers loadSession(true) to re-verify session with admin server
- **Runtime result:** `PENDING`

### [UI-SRC-EB957D16B51D] Global — Auth Verification Error Retry Click

- **Entry type:** `ui_action`
- **Atomic requests:** `REQ-SRC-B038CDBE84D7`
- **Source:** `crates/cc-lb-admin/web/src/components/AuthRequiredGate.tsx:165#loadSession`
- **Preconditions:** Gate in 'error' status
- **Steps:** Network or admin server is unreachable → Gate displays 'Unable to verify admin session' → User clicks 'Retry' button
- **Scope:** `on_user_action` — User button click
- **Risk:** `read`
- **Production applicability:** `not_deployed` — Every atomic request for this action is absent from the deployed API.
- **Expected UI:** Renders loading spinner and re-dispatches loadSession(true)
- **Runtime result:** `PENDING`

### [REQ-SRC-B038CDBE84D7] Global — Auth Verification Error Retry Click — getAuthSession

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-EB957D16B51D`
- **Source:** `crates/cc-lb-admin/web/src/lib/api.ts:74#getAuthSession`
- **Preconditions:** Gate in 'error' status
- **Steps:** Network or admin server is unreachable → Gate displays 'Unable to verify admin session' → User clicks 'Retry' button
- **Scope:** `on_user_action` — User button click
- **Risk:** `read`
- **Production applicability:** `not_deployed` — Endpoint added after deployed commit e56d029e
- **HTTP:** `GET /admin/v1/auth/session`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `auth_session`
- **Storage operations:** None
- **Cache / no-query path:** In-process reads only: AdminIdentity is extracted from the auth middleware request extension, while auth_mode is read separately from state.admin_auth.mode().
- **Side effects:** None
- **Expected UI:** Renders loading spinner and re-dispatches loadSession(true)
- **Runtime result:** `PENDING`

### [UI-SRC-F05A6A69408A] Global — Admin Server Health Poll

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-ROOT-03`
- **Source:** `crates/cc-lb-admin/web/src/components/layout/AppShell.tsx:23#useHealth`
- **Preconditions:** Authenticated session
- **Steps:** AppShell mounts after authentication → useHealth query executes GET /admin/health → Polls periodically every 15,000ms
- **Scope:** `continuous_poll` — Timer every 15s
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Status dot in topbar header updates tone and title tooltip
- **Runtime result:** `PENDING`

### [UI-ROOT-03] Global — Admin Server Health Poll — useHealth

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-F05A6A69408A`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:348#useHealth`
- **Preconditions:** Authenticated session
- **Steps:** AppShell mounts after authentication → useHealth query executes GET /admin/health → Polls periodically every 15,000ms
- **Scope:** `continuous_poll` — Timer every 15s
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/health`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `health`
- **Storage operations:** None
- **Cache / no-query path:** In-memory state: reads state.start_time and cargo compile-time env variables
- **Side effects:** None
- **Expected UI:** Status dot in topbar header updates tone and title tooltip
- **Runtime result:** `PENDING`

### [UI-TOP-01] Global — Desktop Sidebar Collapse Toggle Button

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/layout/AppShell.tsx:128#onToggleSidebar`
- **Preconditions:** Desktop viewport (>= 1024px)
- **Steps:** User clicks menu toggle button in Topbar (desktop viewport lg:inline-flex)
- **Scope:** `on_user_action` — User click
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Sidebar width transitions smoothly, localStorage['cclb.sidebar.collapsed'] updated ('1' or '0')
- **Client-only reason:** Client-side layout collapse state management
- **Runtime result:** `PENDING`

### [UI-TOP-03] Global — Keyboard Shortcut Toggle Sidebar (Cmd+B / Ctrl+B)

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/layout/AppShell.tsx:31#keydown`
- **Preconditions:** Window focused
- **Steps:** User presses Cmd+B (macOS) or Ctrl+B (Windows/Linux) anywhere in the application
- **Scope:** `on_user_action` — Global keyboard event
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Prevents default browser bookmark behavior, toggles collapsed state and updates localStorage
- **Client-only reason:** Client-side keyboard shortcut handler
- **Runtime result:** `PENDING`

### [UI-TOP-04] Global — Mobile Hamburger Navigation Drawer Trigger

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/layout/AppShell.tsx:120#onMobileMenu`
- **Preconditions:** Mobile / tablet viewport (< 1024px)
- **Steps:** User clicks hamburger menu icon in Topbar on mobile viewport (< 1024px)
- **Scope:** `on_user_action` — User click
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Opens BaseDialog.Portal drawer from left with brand, full nav links, and footer
- **Client-only reason:** Client-side modal dialog state
- **Runtime result:** `PENDING`

### [UI-SRC-7963BABBB70A] Global — Command Palette Topbar Trigger Button

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/layout/AppShell.tsx:140#onCommandPalette`
- **Preconditions:** None
- **Steps:** User clicks desktop 'Search... ⌘K' button or mobile Command icon button in Topbar
- **Scope:** `on_user_action` — User click
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Invokes onCommandPalette() callback, opening CommandPalette modal dialog
- **Client-only reason:** Client-side modal trigger
- **Runtime result:** `PENDING`

### [UI-TOP-05] Global — Primary Navigation Links

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/layout/Sidebar.tsx:41#SidebarNav`
- **Preconditions:** None
- **Steps:** User clicks any of the 7 primary navigation links in sidebar or mobile drawer
- **Scope:** `on_user_action` — User navigation click
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** TanStack Router transitions route; active link receives 'bg-overlay-6 text-text' styling; on mobile closes drawer
- **Client-only reason:** Client-side SPA route navigation
- **Runtime result:** `PENDING`

### [UI-TOP-02] Global — Theme Mode Selection Dropdown

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/ThemeToggle.tsx:28#onValueChange`
- **Preconditions:** None
- **Steps:** User clicks ThemeToggle trigger in Topbar → BaseMenu popup opens displaying Light, Dark, System radio options → User clicks one of the theme options
- **Scope:** `on_user_action` — User menu selection
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Writes selection to localStorage['cclb.theme'], sets documentElement data-theme attribute, updates theme colors immediately and syncs across tabs
- **Client-only reason:** Client-side theme attribute and local storage management
- **Runtime result:** `PENDING`

### [UI-CMD-01] Global — Command Palette Keyboard Shortcut (Cmd+K / Ctrl+K)

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/CommandPalette.tsx:29#keydown`
- **Preconditions:** Window focused
- **Steps:** User presses Cmd+K or Ctrl+K anywhere in the application
- **Scope:** `on_user_action` — Global keyboard event
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Toggles CommandPalette open state
- **Client-only reason:** Client-side keyboard shortcut handler
- **Runtime result:** `PENDING`

### [UI-SRC-FD9DEB65904F] Global — Command Palette Upstreams Background Query

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-CMD-03`
- **Source:** `crates/cc-lb-admin/web/src/components/CommandPalette.tsx:25#useUpstreams`
- **Preconditions:** Authenticated session
- **Steps:** CommandPalette component mounts in root layout → useUpstreams hook executes GET /admin/v1/upstreams → Polls every 30,000ms
- **Scope:** `continuous_poll` — Component mount & 30s timer
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Populates Upstreams command group in palette
- **Runtime result:** `PENDING`

### [UI-CMD-03] Global — Command Palette Upstreams Background Query — useUpstreams

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-FD9DEB65904F`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:418#useUpstreams`
- **Preconditions:** Authenticated session
- **Steps:** CommandPalette component mounts in root layout → useUpstreams hook executes GET /admin/v1/upstreams → Polls every 30,000ms
- **Scope:** `continuous_poll` — Component mount & 30s timer
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/upstreams`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `list_upstreams`
- **Storage operations:** `sqlite:UpstreamStore::list:SqliteStorage::list`, `postgres:UpstreamStore::list:PostgresStorage::list`
- **Cache / no-query path:** Sets X-Total-Count header; in-memory keyset pagination on ?after (UUID) UpstreamResponse now serializes base_url: Option<Url> (crates/cc-lb-admin/src/v1/upstreams.rs:167,1587), so the field is present and null when the upstream has no custom base URL.
- **Side effects:** None
- **Expected UI:** Populates Upstreams command group in palette
- **Runtime result:** `PENDING`

### [UI-SRC-036489E5DD6A] Global — Command Palette Principals Background Query

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-CMD-04`
- **Source:** `crates/cc-lb-admin/web/src/components/CommandPalette.tsx:26#usePrincipals`
- **Preconditions:** Authenticated session
- **Steps:** CommandPalette component mounts in root layout → usePrincipals hook executes GET /admin/v1/principals
- **Scope:** `once` — Component mount
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Populates Principals command group in palette
- **Runtime result:** `PENDING`

### [UI-CMD-04] Global — Command Palette Principals Background Query — usePrincipals

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-036489E5DD6A`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:427#usePrincipals`
- **Preconditions:** Authenticated session
- **Steps:** CommandPalette component mounts in root layout → usePrincipals hook executes GET /admin/v1/principals
- **Scope:** `once` — Component mount
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/principals`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `list_principals`
- **Storage operations:** `sqlite:PrincipalStore::list:SqliteStorage::list`, `postgres:PrincipalStore::list:PostgresStorage::list`
- **Cache / no-query path:** Executes PrincipalStore::list twice: first with offset=0 and limit=1000 to derive the capped X-Total-Count, then with the requested offset/limit for the page; no COUNT SQL is executed.
- **Side effects:** None
- **Expected UI:** Populates Principals command group in palette
- **Runtime result:** `PENDING`

### [UI-CMD-05] Global — Command Palette Live Text Filter

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/CommandPalette.tsx:59#Command.Input`
- **Preconditions:** Palette open
- **Steps:** User types text query into Command.Input
- **Scope:** `on_user_action` — User keystrokes
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** cmdk filters items in real time without network requests
- **Client-only reason:** In-memory client filtering of cached items
- **Runtime result:** `PENDING`

### [UI-CMD-02] Global — Command Palette Page or Action Navigation

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/CommandPalette.tsx:39#go`
- **Preconditions:** Palette open
- **Steps:** User selects an item in Pages or Actions group via Enter or Click
- **Scope:** `on_user_action` — User item selection
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Closes palette dialog, navigates to target path
- **Client-only reason:** Client SPA router navigation
- **Runtime result:** `PENDING`

### [UI-SRC-7B67C1B1E413] Global — Command Palette Entity Deep Link Selection

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/CommandPalette.tsx:39#go`
- **Preconditions:** Palette open, query loaded
- **Steps:** User selects an upstream or principal entity from search results
- **Scope:** `on_user_action` — User item selection
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Closes palette dialog, navigates to entity detail view
- **Client-only reason:** Client SPA router navigation with query parameters
- **Runtime result:** `PENDING`

### [UI-CMD-06] Global — Command Palette Dismiss / Close

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/CommandPalette.tsx:67#BaseDialog.Close`
- **Preconditions:** Palette open
- **Steps:** User presses ESC, clicks Close (X) button, or clicks modal backdrop
- **Scope:** `on_user_action` — User interaction
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Closes CommandPalette dialog and restores focus
- **Client-only reason:** Client modal dismiss action
- **Runtime result:** `PENDING`

### [UI-SRC-014AD24226CE] Overview — Overview Dashboard Summary & Sparkline Metrics Query

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-OV-01`
- **Source:** `crates/cc-lb-admin/web/src/routes/index.tsx:1354#useSummary`
- **Preconditions:** Overview route active, authenticated
- **Steps:** OverviewPage mounts or time range toggle changes (1h, 6h, 24h, 7d) → useSummary hook executes GET /admin/dashboard/summary?range={range} → Polls periodically every 5,000ms while window visible
- **Scope:** `continuous_poll` — Mount, 5s poll interval, and range state changes
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Updates 5 KPI tiles (avg req/s, tokens, equiv $, avg latency, err rate) and their respective sparklines and subtotals
- **Runtime result:** `PENDING`

### [UI-OV-01] Overview — Overview Dashboard Summary & Sparkline Metrics Query — useSummary

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-014AD24226CE`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:370#useSummary`
- **Preconditions:** Overview route active, authenticated
- **Steps:** OverviewPage mounts or time range toggle changes (1h, 6h, 24h, 7d) → useSummary hook executes GET /admin/dashboard/summary?range={range} → Polls periodically every 5,000ms while window visible
- **Scope:** `continuous_poll` — Mount, 5s poll interval, and range state changes
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/dashboard/summary`
  - Query: `range`
  - Body: None
  - Headers: `if-none-match`
- **Handler:** `handle_dashboard_summary`
- **Storage operations:** `sqlite:UsageRollupStore::usage_rollup_checkpoint:SqliteStorage::usage_rollup_checkpoint`, `postgres:UsageRollupStore::usage_rollup_checkpoint:PostgresStorage::usage_rollup_checkpoint`, `sqlite:UsageRollupStore::query_usage_rollups_in_range:SqliteStorage::query_usage_rollups_in_range`, `postgres:UsageRollupStore::query_usage_rollups_in_range:PostgresStorage::query_usage_rollups_in_range`, `sqlite:UsageRollupStore::query_overview_excluded_error_buckets_in_range:SqliteStorage::query_overview_excluded_error_buckets_in_range`, `postgres:UsageRollupStore::query_overview_excluded_error_buckets_in_range:PostgresStorage::query_overview_excluded_error_buckets_in_range`
- **Cache / no-query path:** Reads usage-rollup checkpoint for weak ETag/304, then usage rollups and overview-excluded error buckets when a body is required.
- **Side effects:** None
- **Expected UI:** Updates 5 KPI tiles (avg req/s, tokens, equiv $, avg latency, err rate) and their respective sparklines and subtotals
- **Runtime result:** `PENDING`

### [UI-SRC-6899B83C35F4] Overview — Overview Dashboard Principal Usage Query

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-OV-02`
- **Source:** `crates/cc-lb-admin/web/src/routes/index.tsx:1355#useUsage`
- **Preconditions:** Overview route active, authenticated
- **Steps:** OverviewPage mounts or time range toggle changes → useUsage executes GET /admin/usage?range={range}&step={step}&group_by=principal&projection=totals → Polls every 5,000ms while window visible
- **Scope:** `continuous_poll` — Mount, 5s poll interval, and range state changes
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Renders Top Principals Card with top 5 principals sorted by cost, showing req, tok, cache hit ratio, share %, and segmented cost meters
- **Runtime result:** `PENDING`

### [UI-OV-02] Overview — Overview Dashboard Principal Usage Query — useUsage

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-6899B83C35F4`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:382#useUsage`
- **Preconditions:** Overview route active, authenticated
- **Steps:** OverviewPage mounts or time range toggle changes → useUsage executes GET /admin/usage?range={range}&step={step}&group_by=principal&projection=totals → Polls every 5,000ms while window visible
- **Scope:** `continuous_poll` — Mount, 5s poll interval, and range state changes
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/usage`
  - Query: `range`, `step`, `group_by`, `projection`
  - Body: None
  - Headers: `if-none-match`
- **Handler:** `handle_dashboard_usage`
- **Storage operations:** `sqlite:UsageRollupStore::usage_rollup_checkpoint:SqliteStorage::usage_rollup_checkpoint`, `postgres:UsageRollupStore::usage_rollup_checkpoint:PostgresStorage::usage_rollup_checkpoint`, `sqlite:UsageRollupStore::query_usage_rollups_in_range:SqliteStorage::query_usage_rollups_in_range`, `postgres:UsageRollupStore::query_usage_rollups_in_range:PostgresStorage::query_usage_rollups_in_range`, `sqlite:RequestEventStore::request_event_principal_costs:SqliteStorage::request_event_principal_costs`, `postgres:RequestEventStore::request_event_principal_costs:PostgresStorage::request_event_principal_costs`
- **Cache / no-query path:** Principal totals projection uses a short-TTL single-flight cache; other branches use checkpoint-based weak ETag/304. Body construction reads usage rollups and conditionally principal costs for group_by=principal.
- **Side effects:** None
- **Expected UI:** Renders Top Principals Card with top 5 principals sorted by cost, showing req, tok, cache hit ratio, share %, and segmented cost meters
- **Runtime result:** `PENDING`

### [UI-SRC-6A947BA8B48A] Overview — Subscription Quota Aggregate Snapshot Query

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-OV-03`
- **Source:** `crates/cc-lb-admin/web/src/routes/index.tsx:1365#useSubscriptionQuotaAggregate`
- **Preconditions:** Overview route active, authenticated
- **Steps:** OverviewPage mounts → useSubscriptionQuotaAggregate executes GET /admin/v1/subscription-quotas/aggregate?windows=5h%2C7d%2C7d_fable&source=merged → Polls every 30,000ms while window visible
- **Scope:** `continuous_poll` — Mount & 30s poll interval
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Populates Pool Quota Card Snapshot section with 3 stacked meters and plan-weighted upstream contribution percentages
- **Runtime result:** `PENDING`

### [UI-OV-03] Overview — Subscription Quota Aggregate Snapshot Query — useSubscriptionQuotaAggregate

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-6A947BA8B48A`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:917#useSubscriptionQuotaAggregate`
- **Preconditions:** Overview route active, authenticated
- **Steps:** OverviewPage mounts → useSubscriptionQuotaAggregate executes GET /admin/v1/subscription-quotas/aggregate?windows=5h%2C7d%2C7d_fable&source=merged → Polls every 30,000ms while window visible
- **Scope:** `continuous_poll` — Mount & 30s poll interval
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/subscription-quotas/aggregate`
  - Query: `windows`, `source`
  - Body: None
  - Headers: None
- **Handler:** `handle_aggregate`
- **Storage operations:** `sqlite:UpstreamStore::list:SqliteStorage::list`, `postgres:UpstreamStore::list:PostgresStorage::list`, `sqlite:UpstreamSubscriptionMetadataStore::list_upstream_subscription_metadata:SqliteStorage::list_upstream_subscription_metadata`, `postgres:UpstreamSubscriptionMetadataStore::list_upstream_subscription_metadata:PostgresStorage::list_upstream_subscription_metadata`, `sqlite:OrganizationMetadataStore::list_organization_metadata:SqliteStorage::list_organization_metadata`, `postgres:OrganizationMetadataStore::list_organization_metadata:PostgresStorage::list_organization_metadata`, `sqlite:UpstreamSubscriptionQuotaAggregateStore::list_subscription_quota_provider_lots:SqliteStorage::list_subscription_quota_provider_lots`, `postgres:UpstreamSubscriptionQuotaAggregateStore::list_subscription_quota_provider_lots:PostgresStorage::list_subscription_quota_provider_lots`, `sqlite:UsageTokenIntervalStore::sum_usage_tokens_for_intervals:SqliteStorage::sum_usage_tokens_for_intervals`, `postgres:UsageTokenIntervalStore::sum_usage_tokens_for_intervals:PostgresStorage::sum_usage_tokens_for_intervals`
- **Cache / no-query path:** Reads upstreams, subscription/organization metadata, provider-lot checkpoint history, and usage-rollup token sums for generated intervals; current quota candidates also come from the in-memory dynamic cache.
- **Side effects:** None
- **Expected UI:** Populates Pool Quota Card Snapshot section with 3 stacked meters and plan-weighted upstream contribution percentages
- **Runtime result:** `PENDING`

### [UI-SRC-F52C6DD98101] Overview — Subscription Quota Pool History Time Series Query

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-OV-04`
- **Source:** `crates/cc-lb-admin/web/src/routes/index.tsx:1371#useSubscriptionQuotaPoolHistory`
- **Preconditions:** Overview route active, authenticated
- **Steps:** OverviewPage mounts or time range toggle changes → useSubscriptionQuotaPoolHistory executes GET /admin/v1/subscription-quotas/pool-history with computed time bounds → Polls every 30,000ms while window visible
- **Scope:** `continuous_poll` — Mount, 30s poll interval, and range state changes
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Renders Pool Quota Themed Chart with 5h, 7d (and optional 7d_fable) area series, reference lines (80% Warn, 95% Critical), and latest legend stats
- **Runtime result:** `PENDING`

### [UI-OV-04] Overview — Subscription Quota Pool History Time Series Query — useSubscriptionQuotaPoolHistory

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-F52C6DD98101`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:951#useSubscriptionQuotaPoolHistory`
- **Preconditions:** Overview route active, authenticated
- **Steps:** OverviewPage mounts or time range toggle changes → useSubscriptionQuotaPoolHistory executes GET /admin/v1/subscription-quotas/pool-history with computed time bounds → Polls every 30,000ms while window visible
- **Scope:** `continuous_poll` — Mount, 30s poll interval, and range state changes
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/subscription-quotas/pool-history`
  - Query: `series_projection`, `windows`, `since_unix_secs`, `until_unix_secs`, `max_points_per_series`
  - Body: None
  - Headers: None
- **Handler:** `handle_pool_history`
- **Storage operations:** `sqlite:PoolQuotaHistoryStore::list_latest_pool_quota_snapshot_summaries:SqliteStorage::list_latest_pool_quota_snapshot_summaries`, `postgres:PoolQuotaHistoryStore::list_latest_pool_quota_snapshot_summaries:PostgresStorage::list_latest_pool_quota_snapshot_summaries`, `sqlite:PoolQuotaHistoryStore::list_pool_quota_snapshot_summaries_in_range:SqliteStorage::list_pool_quota_snapshot_summaries_in_range`, `postgres:PoolQuotaHistoryStore::list_pool_quota_snapshot_summaries_in_range:PostgresStorage::list_pool_quota_snapshot_summaries_in_range`, `sqlite:PoolQuotaHistoryStore::list_pool_quota_chart_points_in_range:SqliteStorage::list_pool_quota_chart_points_in_range`, `postgres:PoolQuotaHistoryStore::list_pool_quota_chart_points_in_range:PostgresStorage::list_pool_quota_chart_points_in_range`
- **Cache / no-query path:** Always reads latest pooled summaries; series_projection=full reads raw summary rows, while chart reads raw or bucketed chart projections.
- **Side effects:** None
- **Expected UI:** Renders Pool Quota Themed Chart with 5h, 7d (and optional 7d_fable) area series, reference lines (80% Warn, 95% Critical), and latest legend stats
- **Runtime result:** `PENDING`

### [UI-SRC-BAA6CE610FCA] Overview — Principal Name Resolution Query

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-OV-13`
- **Source:** `crates/cc-lb-admin/web/src/routes/index.tsx:1362#usePrincipalNameMap`
- **Preconditions:** Authenticated session
- **Steps:** OverviewPage mounts → usePrincipalNameMap hook executes usePrincipals() -> GET /admin/v1/principals
- **Scope:** `once` — Component mount
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Translates raw principal IDs into human-readable names across Top Principals card and Recent Requests table
- **Runtime result:** `PENDING`

### [UI-OV-13] Overview — Principal Name Resolution Query — usePrincipals

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-BAA6CE610FCA`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:427#usePrincipals`
- **Preconditions:** Authenticated session
- **Steps:** OverviewPage mounts → usePrincipalNameMap hook executes usePrincipals() -> GET /admin/v1/principals
- **Scope:** `once` — Component mount
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/principals`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `list_principals`
- **Storage operations:** `sqlite:PrincipalStore::list:SqliteStorage::list`, `postgres:PrincipalStore::list:PostgresStorage::list`
- **Cache / no-query path:** Executes PrincipalStore::list twice: first with offset=0 and limit=1000 to derive the capped X-Total-Count, then with the requested offset/limit for the page; no COUNT SQL is executed.
- **Side effects:** None
- **Expected UI:** Translates raw principal IDs into human-readable names across Top Principals card and Recent Requests table
- **Runtime result:** `PENDING`

### [UI-SRC-1BEA80590008] Overview — Upstream Name Resolution Query

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-OV-14`
- **Source:** `crates/cc-lb-admin/web/src/routes/index.tsx:1363#useUpstreamNameMap`
- **Preconditions:** Authenticated session
- **Steps:** OverviewPage mounts → useUpstreamNameMap hook executes useUpstreams() -> GET /admin/v1/upstreams → Polls every 30,000ms
- **Scope:** `continuous_poll` — Component mount & 30s timer
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Translates upstream IDs into display names across Recent Requests table and Pool Quota Popovers
- **Runtime result:** `PENDING`

### [UI-OV-14] Overview — Upstream Name Resolution Query — useUpstreams

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-1BEA80590008`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:418#useUpstreams`
- **Preconditions:** Authenticated session
- **Steps:** OverviewPage mounts → useUpstreamNameMap hook executes useUpstreams() -> GET /admin/v1/upstreams → Polls every 30,000ms
- **Scope:** `continuous_poll` — Component mount & 30s timer
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/upstreams`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `list_upstreams`
- **Storage operations:** `sqlite:UpstreamStore::list:SqliteStorage::list`, `postgres:UpstreamStore::list:PostgresStorage::list`
- **Cache / no-query path:** Sets X-Total-Count header; in-memory keyset pagination on ?after (UUID) UpstreamResponse now serializes base_url: Option<Url> (crates/cc-lb-admin/src/v1/upstreams.rs:167,1587), so the field is present and null when the upstream has no custom base URL.
- **Side effects:** None
- **Expected UI:** Translates upstream IDs into display names across Recent Requests table and Pool Quota Popovers
- **Runtime result:** `PENDING`

### [UI-OV-07] Overview — Time Range Toggle Action

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/routes/index.tsx:1350#selectRange`
- **Preconditions:** Overview page active
- **Steps:** User clicks one of the range toggles: '1h', '6h', '24h', or '7d'
- **Scope:** `on_user_action` — User click
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Resets activeKpiIndex to null, updates range state, triggers query param updates across summary, usage, and pool-history queries
- **Client-only reason:** State update driving query re-evaluations (network triggered via dependent hooks UI-OV-07A/B/C)
- **Runtime result:** `PENDING`

### [UI-OV-08] Overview — Synchronized KPI Sparkline Hover

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/routes/index.tsx:286#handleMove`
- **Preconditions:** KPI sparkline rendered with >0 points
- **Steps:** User moves cursor horizontally over any of the 5 KPI sparkline charts
- **Scope:** `on_user_action` — Mouse move / mouse leave
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Calculates normalized X ratio, sets shared activeKpiIndex, displaying synchronized tooltips (timestamp + value) on all 5 KPI cards simultaneously
- **Client-only reason:** Pure client-side hover state coordination
- **Runtime result:** `PENDING`

### [UI-OV-09] Overview — Principal Cost Breakdown Popover

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/routes/index.tsx:450#PrincipalCostMeter`
- **Preconditions:** Top principal row rendered
- **Steps:** User hovers (delay 200ms) or focuses on a principal's segmented cost meter bar
- **Scope:** `on_user_action` — Hover or focus event
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Opens BasePopover showing BreakdownPopover with formatted dollar figures per category and total
- **Client-only reason:** Client popover displaying already-aggregated series bucket data
- **Runtime result:** `PENDING`

### [UI-OV-10] Overview — Pool Quota Stacked Bar Breakdown Popover

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/routes/index.tsx:779#PoolQuotaStackedBar`
- **Preconditions:** Aggregate data loaded
- **Steps:** User hovers (desktop) or clicks/touches (mobile) on a 5h, 7d, or 7d_fable stacked meter bar
- **Scope:** `on_user_action` — Hover, click, or touch event
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Opens BasePopover showing PoolQuotaPopoverContent table: Upstream name, Util %, Weight (capacity_ratio x), and Impact (weighted contribution %)
- **Client-only reason:** Client popover displaying already-fetched aggregate window data
- **Runtime result:** `PENDING`

### [UI-SRC-8C3BDB37C763] Overview — Recent Requests Initial Page Query

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-OV-05`
- **Source:** `crates/cc-lb-admin/web/src/routes/index.tsx:1361#useRecentEventsInfinite`
- **Preconditions:** Overview route active, authenticated
- **Steps:** OverviewPage mounts → useRecentEventsInfinite initiates first page query
- **Scope:** `once` — Component mount
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Renders initial table rows (up to 200 items), status dots, timestamps, models, outcomes, latencies, tokens, costs
- **Runtime result:** `PENDING`

### [UI-OV-05] Overview — Recent Requests Initial Page Query — fetchRecentEventsPage

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-8C3BDB37C763`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:581#fetchRecentEventsPage`
- **Preconditions:** Overview route active, authenticated
- **Steps:** OverviewPage mounts → useRecentEventsInfinite initiates first page query
- **Scope:** `once` — Component mount
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/events/recent`
  - Query: `limit`
  - Body: None
  - Headers: None
- **Handler:** `handle_recent_events`
- **Storage operations:** `sqlite:RequestEventStore::list_request_events:SqliteStorage::list_request_events`, `postgres:RequestEventStore::list_request_events:PostgresStorage::list_request_events`
- **Cache / no-query path:** Dynamic filter queries
- **Side effects:** None
- **Expected UI:** Renders initial table rows (up to 200 items), status dots, timestamps, models, outcomes, latencies, tokens, costs
- **Runtime result:** `PENDING`

### [UI-SRC-2305B3BC43A4] Overview — Recent Requests Infinite Scroll Next Page

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-OV-11`
- **Source:** `crates/cc-lb-admin/web/src/routes/index.tsx:1386#IntersectionObserver`
- **Preconditions:** events.hasNextPage is true, not currently fetching
- **Steps:** User scrolls down the Recent Requests table container → Sentinel row at the bottom of the table becomes visible (IntersectionObserver triggers) → events.fetchNextPage() is called
- **Scope:** `on_user_action` — User scroll intersection
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Sentinel displays 'Loading…' then appends next page of events seamlessly to recentRows
- **Runtime result:** `PENDING`

### [UI-OV-11] Overview — Recent Requests Infinite Scroll Next Page — fetchRecentEventsPage

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-2305B3BC43A4`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:581#fetchRecentEventsPage`
- **Preconditions:** events.hasNextPage is true, not currently fetching
- **Steps:** User scrolls down the Recent Requests table container → Sentinel row at the bottom of the table becomes visible (IntersectionObserver triggers) → events.fetchNextPage() is called
- **Scope:** `on_user_action` — User scroll intersection
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/events/recent`
  - Query: `limit`, `until_ts_ms`, `until_event_id`
  - Body: None
  - Headers: None
- **Handler:** `handle_recent_events`
- **Storage operations:** `sqlite:RequestEventStore::list_request_events:SqliteStorage::list_request_events`, `postgres:RequestEventStore::list_request_events:PostgresStorage::list_request_events`
- **Cache / no-query path:** Dynamic filter queries
- **Side effects:** None
- **Expected UI:** Sentinel displays 'Loading…' then appends next page of events seamlessly to recentRows
- **Runtime result:** `PENDING`

### [UI-SRC-40B1F1E07B91] Overview — Live Tail SSE Connection

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-OV-06`
- **Source:** `crates/cc-lb-admin/web/src/routes/index.tsx:1378#useLiveEventStream`
- **Preconditions:** Authenticated session, window online and active
- **Steps:** OverviewPage mounts → useLiveEventStream opens EventSource to /admin/events/stream
- **Scope:** `streaming` — Component mount & connection lifecycle
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** New events stream in real time and merge into table rows with green flash-in animation
- **Runtime result:** `PENDING`

### [UI-OV-06] Overview — Live Tail SSE Connection — createEventSource

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-40B1F1E07B91`
- **Source:** `crates/cc-lb-admin/web/src/lib/useLiveEventStream.ts:236#connect`
- **Preconditions:** Authenticated session, window online and active
- **Steps:** OverviewPage mounts → useLiveEventStream opens EventSource to /admin/events/stream
- **Scope:** `streaming` — Component mount & connection lifecycle
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/events/stream`
  - Query: None
  - Body: None
  - Headers: `last-event-id`
- **Handler:** `handle_events_stream`
- **Storage operations:** `sqlite:RequestEventStore::current_request_event_cursor:SqliteStorage::current_request_event_cursor`, `postgres:RequestEventStore::current_request_event_cursor:PostgresStorage::current_request_event_cursor`, `sqlite:RequestEventStore::query_request_events_between_cursors:SqliteStorage::query_request_events_between_cursors`, `postgres:RequestEventStore::query_request_events_between_cursors:PostgresStorage::query_request_events_between_cursors`
- **Cache / no-query path:** SSE: reads the current cursor and performs one reconnect backfill bounded by BACKFILL_MAX_EVENTS=500; reaching the cap emits a backfill-cap reset, otherwise delivery continues from in-memory event-bus/storage-tail broadcasts with heartbeats.
- **Side effects:** None
- **Expected UI:** New events stream in real time and merge into table rows with green flash-in animation
- **Runtime result:** `PENDING`

### [UI-SRC-FDFB5195AB54] Overview — Live Stream Reconnect Hybrid Delta Backfill

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-OV-15`
- **Source:** `crates/cc-lb-admin/web/src/lib/useLiveEventStream.ts:194#connect`
- **Preconditions:** Prior SSE stream received events and recorded a cursor
- **Steps:** SSE stream disconnects and attempts reconnection → lastCursorRef has a valid cursor string → connect(true) executes delta backfill request before reopening SSE
- **Scope:** `on_reconnect` — Stream reconnection event
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Seamlessly fills gaps in event stream without duplicate rows or missed events
- **Runtime result:** `PENDING`

### [UI-OV-15] Overview — Live Stream Reconnect Hybrid Delta Backfill — getJson

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-FDFB5195AB54`
- **Source:** `crates/cc-lb-admin/web/src/lib/useLiveEventStream.ts:199#connect`
- **Preconditions:** Prior SSE stream received events and recorded a cursor
- **Steps:** SSE stream disconnects and attempts reconnection → lastCursorRef has a valid cursor string → connect(true) executes delta backfill request before reopening SSE
- **Scope:** `on_reconnect` — Stream reconnection event
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/events/delta`
  - Query: `since_cursor`, `limit`
  - Body: None
  - Headers: None
- **Handler:** `crate::events_routes::handle_events_delta`
- **Storage operations:** `sqlite:RequestEventStore::current_request_event_cursor:SqliteStorage::current_request_event_cursor`, `postgres:RequestEventStore::current_request_event_cursor:PostgresStorage::current_request_event_cursor`, `sqlite:RequestEventStore::query_request_events_between_cursors:SqliteStorage::query_request_events_between_cursors`, `postgres:RequestEventStore::query_request_events_between_cursors:PostgresStorage::query_request_events_between_cursors`
- **Cache / no-query path:** Reads a storage cursor and cursor-bounded page. build_delta_events_payload in the admin layer then applies residual filters in memory (including excluding renewal source_kind by default); exhausted is computed from the pre-filter row count.
- **Side effects:** None
- **Expected UI:** Seamlessly fills gaps in event stream without duplicate rows or missed events
- **Runtime result:** `PENDING`

### [UI-OV-16] Overview — Live Tail Failure Banner Manual Retry

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/LiveTailFailureBanner.tsx:40#onRetry`
- **Preconditions:** live.permanentFailure is true
- **Steps:** SSE stream experiences permanent failure (>5 min unreachable) → LiveTailFailureBanner renders at top of Overview page → User clicks 'Retry now' button
- **Scope:** `on_user_action` — User click
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Calls live.forceReconnect(), resetting reconnect counters and immediately initiating stream connect()
- **Client-only reason:** Client connection state trigger (leads to GET /admin/events/stream)
- **Runtime result:** `PENDING`

### [UI-SRC-CFF84088D66A] Overview — Live Tail Failure Banner Dismiss

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/LiveTailFailureBanner.tsx:46#setDismissed`
- **Preconditions:** Banner visible
- **Steps:** User clicks Dismiss (X) button on failure banner
- **Scope:** `on_user_action` — User click
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Hides banner without affecting background reconnect attempts
- **Client-only reason:** Client banner visibility state
- **Runtime result:** `PENDING`

### [UI-OV-17] Overview — Recent Requests 'See all' Navigation Link

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/routes/index.tsx:1761#OverviewPage`
- **Preconditions:** None
- **Steps:** User clicks 'See all' link in Recent Requests section header
- **Scope:** `on_user_action` — User click
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Navigates browser to /logs page for full Logs Explorer view
- **Client-only reason:** Client anchor/router navigation
- **Runtime result:** `PENDING`

### [UI-SRC-1667E85E2450] Overview — Recent Request Row Selection

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/ui/RequestEventsTable.tsx:105#RequestEventRow`
- **Preconditions:** Table has loaded events
- **Steps:** User clicks any request row in the table, or focuses row and presses Enter or Space
- **Scope:** `on_user_action` — User row interaction
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Sets selectedId in RequestEventsTable, opening RequestEventDrawer sliding in from right
- **Client-only reason:** Client drawer state trigger (leads to detail query UI-OV-12-DETAIL)
- **Runtime result:** `PENDING`

### [UI-SRC-7BBB835587EA] Overview — Request Event Full Detail Query

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-OV-12`
- **Source:** `crates/cc-lb-admin/web/src/components/ui/RequestEventDrawer.tsx:86#useRequestEventDetail`
- **Preconditions:** Selected event is in 'final' phase with non-null event_id or request_id
- **Steps:** User clicks a finalized request event row in RequestEventsTable → RequestEventDrawer opens with non-null detailId → useRequestEventDetail executes GET /admin/v1/events/detail/{eventId}
- **Scope:** `on_user_action` — Row selection in table
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Populates full diagnostics in drawer: status badge, model, principal, upstream, session ID, latency timeline breakdown, token usage pie chart, cost pie chart, upstream failure message
- **Runtime result:** `PENDING`

### [UI-OV-12] Overview — Request Event Full Detail Query — useRequestEventDetail

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-7BBB835587EA`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:526#useRequestEventDetail`
- **Preconditions:** Selected event is in 'final' phase with non-null event_id or request_id
- **Steps:** User clicks a finalized request event row in RequestEventsTable → RequestEventDrawer opens with non-null detailId → useRequestEventDetail executes GET /admin/v1/events/detail/{eventId}
- **Scope:** `on_user_action` — Row selection in table
- **Risk:** `read_with_audit`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/events/detail/{event_id}`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `handle_event_detail`
- **Storage operations:** `sqlite:RequestEventStore::get_request_event:SqliteStorage::get_request_event`, `postgres:RequestEventStore::get_request_event:PostgresStorage::get_request_event`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Fetches raw unredacted event payload JSON from DB
- **Side effects:** Audit log write: record_admin_audit("request_event_detail_read", "/admin/v1/events/detail/{event_id}")
- **Expected UI:** Populates full diagnostics in drawer: status badge, model, principal, upstream, session ID, latency timeline breakdown, token usage pie chart, cost pie chart, upstream failure message
- **Runtime result:** `PENDING`

### [UI-SRC-FE19ED30AFDB] Overview — Request Detail Copy Actions (ID, Session, JSON)

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/ui/RequestEventDrawer.tsx:139#copy`
- **Preconditions:** RequestEventDrawer open
- **Steps:** User clicks copy icon next to Request ID, Principal ID, Session ID, or bottom 'Copy raw JSON' button
- **Scope:** `on_user_action` — User button click
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Copies string to clipboard via navigator.clipboard.writeText, displays success toast via Sonner
- **Client-only reason:** Client clipboard copy action
- **Runtime result:** `PENDING`

### [UI-SRC-25F058F5759A] Overview — Request Detail Drawer Close

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/ui/RequestEventDrawer.tsx:149#onClose`
- **Preconditions:** Drawer open
- **Steps:** User clicks Close (X) button, presses ESC, or clicks drawer backdrop
- **Scope:** `on_user_action` — User interaction
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Drawer slides out to right, resets selectedId to null
- **Client-only reason:** Client dialog dismissal
- **Runtime result:** `PENDING`

### [UI-SRC-35CD806FA378] Logs — Historical Events Page Query on Mount

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-LOG-01`
- **Source:** `crates/cc-lb-admin/web/src/routes/logs.tsx:218#LogsPage`
- **Preconditions:** User is authenticated with admin role
- **Steps:** Navigate to /logs with or without filter search params
- **Scope:** `once` — Route mount / URL filter changes
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Renders loading skeleton rows in RequestEventsTable, then displays up to 50 rows matching historical filters
- **Runtime result:** `PENDING`

### [UI-LOG-01] Logs — Historical Events Page Query on Mount — useRecentEventsPage

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-35CD806FA378`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:622#useRecentEventsPage`
- **Preconditions:** User is authenticated with admin role
- **Steps:** Navigate to /logs with or without filter search params
- **Scope:** `once` — Route mount / URL filter changes
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/events/recent`
  - Query: `limit`, `since_unix_secs`, `until_unix_secs`, `until_ts_ms`, `until_event_id`, `principal_id`, `upstream_id`, `thread_id`, `model`, `status_class`, `source_kind`
  - Body: None
  - Headers: None
- **Handler:** `handle_recent_events`
- **Storage operations:** `sqlite:RequestEventStore::list_request_events:SqliteStorage::list_request_events`, `postgres:RequestEventStore::list_request_events:PostgresStorage::list_request_events`
- **Cache / no-query path:** Dynamic filter queries
- **Side effects:** None
- **Expected UI:** Renders loading skeleton rows in RequestEventsTable, then displays up to 50 rows matching historical filters
- **Runtime result:** `PENDING`

### [UI-SRC-8290B2982481] Logs — SSE Live Event Stream Subscription

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-LOG-02`
- **Source:** `crates/cc-lb-admin/web/src/routes/logs.tsx:317#LogsPage`
- **Preconditions:** Live tailing enabled (until_unix_secs == null); Tab is active and online
- **Steps:** Mount /logs with until_unix_secs=undefined and userRequestedTailing=true
- **Scope:** `once` — Effective tailing state active
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Status indicator displays 'Connecting…' then 'Live' with green pulsing dot; incoming events appear at top of table with flash animation
- **Runtime result:** `PENDING`

### [UI-LOG-02] Logs — SSE Live Event Stream Subscription — useLiveEventStream

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-8290B2982481`
- **Source:** `crates/cc-lb-admin/web/src/lib/useLiveEventStream.ts:270#connect`
- **Preconditions:** Live tailing enabled (until_unix_secs == null); Tab is active and online
- **Steps:** Mount /logs with until_unix_secs=undefined and userRequestedTailing=true
- **Scope:** `once` — Effective tailing state active
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/events/stream`
  - Query: `principal_id`, `upstream_id`, `thread_id`, `model`, `status_class`, `source_kind`
  - Body: None
  - Headers: `last-event-id`
- **Handler:** `handle_events_stream`
- **Storage operations:** `sqlite:RequestEventStore::current_request_event_cursor:SqliteStorage::current_request_event_cursor`, `postgres:RequestEventStore::current_request_event_cursor:PostgresStorage::current_request_event_cursor`, `sqlite:RequestEventStore::query_request_events_between_cursors:SqliteStorage::query_request_events_between_cursors`, `postgres:RequestEventStore::query_request_events_between_cursors:PostgresStorage::query_request_events_between_cursors`
- **Cache / no-query path:** SSE: reads the current cursor and performs one reconnect backfill bounded by BACKFILL_MAX_EVENTS=500; reaching the cap emits a backfill-cap reset, otherwise delivery continues from in-memory event-bus/storage-tail broadcasts with heartbeats.
- **Side effects:** None
- **Expected UI:** Status indicator displays 'Connecting…' then 'Live' with green pulsing dot; incoming events appear at top of table with flash animation
- **Runtime result:** `PENDING`

### [UI-SRC-694A5E8030DA] Logs — Events Histogram Query

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-LOG-03`
- **Source:** `crates/cc-lb-admin/web/src/routes/logs.tsx:486#LogsPage`
- **Preconditions:** Initial view { a, b } established from firstPageEvents or now
- **Steps:** Mount /logs, wait for first page events to establish initial view domain, query histogram range
- **Scope:** `once` — Histogram range establishment / view changes
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Renders canvas bar chart with total requests (accent cyan) and error lane (danger red)
- **Runtime result:** `PENDING`

### [UI-LOG-03] Logs — Events Histogram Query — useEventsHistogram

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-694A5E8030DA`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:497#useEventsHistogram`
- **Preconditions:** Initial view { a, b } established from firstPageEvents or now
- **Steps:** Mount /logs, wait for first page events to establish initial view domain, query histogram range
- **Scope:** `once` — Histogram range establishment / view changes
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/events/histogram`
  - Query: `since_unix_secs`, `until_unix_secs`, `bucket_ms`, `principal_id`, `upstream_id`, `thread_id`, `model`, `status_class`, `source_kind`
  - Body: None
  - Headers: None
- **Handler:** `crate::events_routes::handle_events_histogram`
- **Storage operations:** `sqlite:RequestEventStore::request_event_histogram:SqliteStorage::request_event_histogram`, `postgres:RequestEventStore::request_event_histogram:PostgresStorage::request_event_histogram`
- **Cache / no-query path:** Computes time buckets and rejects requests exceeding MAX_HISTOGRAM_BUCKETS=240.
- **Side effects:** None
- **Expected UI:** Renders canvas bar chart with total requests (accent cyan) and error lane (danger red)
- **Runtime result:** `PENDING`

### [UI-LOG-04] Logs — Toggle Live Tail Button Click

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/routes/logs.tsx:802#LogsPage`
- **Preconditions:** until_unix_secs == null (disabled when until_unix_secs is set)
- **Steps:** Click 'Live tail' / 'Stop tail' toggle button in page header
- **Scope:** `once` — User toggle click
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Button toggles pressed visual state; header subtitle switches between 'live tailing' and 'paged'; status dot turns off or reconnects
- **Client-only reason:** Directly updates userRequestedTailing React state, which activates or tears down useLiveEventStream
- **Runtime result:** `PENDING`

### [UI-LOG-05] Logs — Select Principal Filter

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-LOG-05A`, `UI-LOG-05B`, `UI-LOG-05C`
- **Source:** `crates/cc-lb-admin/web/src/routes/logs.tsx:847#LogsPage`
- **Preconditions:** Principal list loaded via usePrincipalNameMap
- **Steps:** Open Principal LogSelect dropdown → Select a specific principal or 'All principals'
- **Scope:** `each_filter_combination` — Available principals list + empty option
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Navigates search params with principal_id; table resets to page 0, historical query refetches, SSE reconnects with principal_id filter, histogram updates
- **Runtime result:** `PENDING`

### [UI-LOG-05A] Logs — Select Principal Filter — useRecentEventsPage

- **Entry type:** `network_request`
- **Parent action:** `UI-LOG-05`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:622#useRecentEventsPage`
- **Preconditions:** Principal list loaded via usePrincipalNameMap
- **Steps:** Open Principal LogSelect dropdown → Select a specific principal or 'All principals'
- **Scope:** `each_filter_combination` — Available principals list + empty option
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/events/recent`
  - Query: `principal_id`, `limit`
  - Body: None
  - Headers: None
- **Handler:** `handle_recent_events`
- **Storage operations:** `sqlite:RequestEventStore::list_request_events:SqliteStorage::list_request_events`, `postgres:RequestEventStore::list_request_events:PostgresStorage::list_request_events`
- **Cache / no-query path:** Dynamic filter queries
- **Side effects:** None
- **Expected UI:** Navigates search params with principal_id; table resets to page 0, historical query refetches, SSE reconnects with principal_id filter, histogram updates
- **Runtime result:** `PENDING`

### [UI-LOG-05B] Logs — Select Principal Filter — useLiveEventStream

- **Entry type:** `network_request`
- **Parent action:** `UI-LOG-05`
- **Source:** `crates/cc-lb-admin/web/src/lib/useLiveEventStream.ts:270#connect`
- **Preconditions:** Principal list loaded via usePrincipalNameMap
- **Steps:** Open Principal LogSelect dropdown → Select a specific principal or 'All principals'
- **Scope:** `each_filter_combination` — Available principals list + empty option
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/events/stream`
  - Query: `principal_id`
  - Body: None
  - Headers: `last-event-id`
- **Handler:** `handle_events_stream`
- **Storage operations:** `sqlite:RequestEventStore::current_request_event_cursor:SqliteStorage::current_request_event_cursor`, `postgres:RequestEventStore::current_request_event_cursor:PostgresStorage::current_request_event_cursor`, `sqlite:RequestEventStore::query_request_events_between_cursors:SqliteStorage::query_request_events_between_cursors`, `postgres:RequestEventStore::query_request_events_between_cursors:PostgresStorage::query_request_events_between_cursors`
- **Cache / no-query path:** SSE: reads the current cursor and performs one reconnect backfill bounded by BACKFILL_MAX_EVENTS=500; reaching the cap emits a backfill-cap reset, otherwise delivery continues from in-memory event-bus/storage-tail broadcasts with heartbeats.
- **Side effects:** None
- **Expected UI:** Navigates search params with principal_id; table resets to page 0, historical query refetches, SSE reconnects with principal_id filter, histogram updates
- **Runtime result:** `PENDING`

### [UI-LOG-05C] Logs — Select Principal Filter — useEventsHistogram

- **Entry type:** `network_request`
- **Parent action:** `UI-LOG-05`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:497#useEventsHistogram`
- **Preconditions:** Principal list loaded via usePrincipalNameMap
- **Steps:** Open Principal LogSelect dropdown → Select a specific principal or 'All principals'
- **Scope:** `each_filter_combination` — Available principals list + empty option
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/events/histogram`
  - Query: `principal_id`, `since_unix_secs`, `until_unix_secs`, `bucket_ms`
  - Body: None
  - Headers: None
- **Handler:** `crate::events_routes::handle_events_histogram`
- **Storage operations:** `sqlite:RequestEventStore::request_event_histogram:SqliteStorage::request_event_histogram`, `postgres:RequestEventStore::request_event_histogram:PostgresStorage::request_event_histogram`
- **Cache / no-query path:** Computes time buckets and rejects requests exceeding MAX_HISTOGRAM_BUCKETS=240.
- **Side effects:** None
- **Expected UI:** Navigates search params with principal_id; table resets to page 0, historical query refetches, SSE reconnects with principal_id filter, histogram updates
- **Runtime result:** `PENDING`

### [UI-LOG-06] Logs — Select Upstream Filter

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-LOG-06A`, `UI-LOG-06B`, `UI-LOG-06C`
- **Source:** `crates/cc-lb-admin/web/src/routes/logs.tsx:856#LogsPage`
- **Preconditions:** Upstreams loaded via useUpstreams
- **Steps:** Open Upstream LogSelect dropdown → Select an upstream or 'All upstreams'
- **Scope:** `each_filter_combination` — Available upstreams list + empty option
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Navigates search params with upstream_id; table resets to page 0, historical query refetches, SSE reconnects, histogram updates
- **Runtime result:** `PENDING`

### [UI-LOG-06A] Logs — Select Upstream Filter — useRecentEventsPage

- **Entry type:** `network_request`
- **Parent action:** `UI-LOG-06`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:622#useRecentEventsPage`
- **Preconditions:** Upstreams loaded via useUpstreams
- **Steps:** Open Upstream LogSelect dropdown → Select an upstream or 'All upstreams'
- **Scope:** `each_filter_combination` — Available upstreams list + empty option
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/events/recent`
  - Query: `upstream_id`, `limit`
  - Body: None
  - Headers: None
- **Handler:** `handle_recent_events`
- **Storage operations:** `sqlite:RequestEventStore::list_request_events:SqliteStorage::list_request_events`, `postgres:RequestEventStore::list_request_events:PostgresStorage::list_request_events`
- **Cache / no-query path:** Dynamic filter queries
- **Side effects:** None
- **Expected UI:** Navigates search params with upstream_id; table resets to page 0, historical query refetches, SSE reconnects, histogram updates
- **Runtime result:** `PENDING`

### [UI-LOG-06B] Logs — Select Upstream Filter — useLiveEventStream

- **Entry type:** `network_request`
- **Parent action:** `UI-LOG-06`
- **Source:** `crates/cc-lb-admin/web/src/lib/useLiveEventStream.ts:270#connect`
- **Preconditions:** Upstreams loaded via useUpstreams
- **Steps:** Open Upstream LogSelect dropdown → Select an upstream or 'All upstreams'
- **Scope:** `each_filter_combination` — Available upstreams list + empty option
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/events/stream`
  - Query: `upstream_id`
  - Body: None
  - Headers: `last-event-id`
- **Handler:** `handle_events_stream`
- **Storage operations:** `sqlite:RequestEventStore::current_request_event_cursor:SqliteStorage::current_request_event_cursor`, `postgres:RequestEventStore::current_request_event_cursor:PostgresStorage::current_request_event_cursor`, `sqlite:RequestEventStore::query_request_events_between_cursors:SqliteStorage::query_request_events_between_cursors`, `postgres:RequestEventStore::query_request_events_between_cursors:PostgresStorage::query_request_events_between_cursors`
- **Cache / no-query path:** SSE: reads the current cursor and performs one reconnect backfill bounded by BACKFILL_MAX_EVENTS=500; reaching the cap emits a backfill-cap reset, otherwise delivery continues from in-memory event-bus/storage-tail broadcasts with heartbeats.
- **Side effects:** None
- **Expected UI:** Navigates search params with upstream_id; table resets to page 0, historical query refetches, SSE reconnects, histogram updates
- **Runtime result:** `PENDING`

### [UI-LOG-06C] Logs — Select Upstream Filter — useEventsHistogram

- **Entry type:** `network_request`
- **Parent action:** `UI-LOG-06`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:497#useEventsHistogram`
- **Preconditions:** Upstreams loaded via useUpstreams
- **Steps:** Open Upstream LogSelect dropdown → Select an upstream or 'All upstreams'
- **Scope:** `each_filter_combination` — Available upstreams list + empty option
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/events/histogram`
  - Query: `upstream_id`, `since_unix_secs`, `until_unix_secs`, `bucket_ms`
  - Body: None
  - Headers: None
- **Handler:** `crate::events_routes::handle_events_histogram`
- **Storage operations:** `sqlite:RequestEventStore::request_event_histogram:SqliteStorage::request_event_histogram`, `postgres:RequestEventStore::request_event_histogram:PostgresStorage::request_event_histogram`
- **Cache / no-query path:** Computes time buckets and rejects requests exceeding MAX_HISTOGRAM_BUCKETS=240.
- **Side effects:** None
- **Expected UI:** Navigates search params with upstream_id; table resets to page 0, historical query refetches, SSE reconnects, histogram updates
- **Runtime result:** `PENDING`

### [UI-LOG-07] Logs — Select Session Filter

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-LOG-07A`, `UI-LOG-07B`, `UI-LOG-07C`
- **Source:** `crates/cc-lb-admin/web/src/routes/logs.tsx:865#LogsPage`
- **Preconditions:** Session list extracted from loaded rows
- **Steps:** Open Session LogSelect dropdown → Select a session chip or 'All sessions'
- **Scope:** `each_filter_combination` — Unique session IDs from loaded rows
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Navigates search params with session (mapped to thread_id on wire); table resets to page 0, historical query refetches with thread_id, SSE reconnects, histogram updates
- **Runtime result:** `PENDING`

### [UI-LOG-07A] Logs — Select Session Filter — useRecentEventsPage

- **Entry type:** `network_request`
- **Parent action:** `UI-LOG-07`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:622#useRecentEventsPage`
- **Preconditions:** Session list extracted from loaded rows
- **Steps:** Open Session LogSelect dropdown → Select a session chip or 'All sessions'
- **Scope:** `each_filter_combination` — Unique session IDs from loaded rows
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/events/recent`
  - Query: `thread_id`, `limit`
  - Body: None
  - Headers: None
- **Handler:** `handle_recent_events`
- **Storage operations:** `sqlite:RequestEventStore::list_request_events:SqliteStorage::list_request_events`, `postgres:RequestEventStore::list_request_events:PostgresStorage::list_request_events`
- **Cache / no-query path:** Dynamic filter queries
- **Side effects:** None
- **Expected UI:** Navigates search params with session (mapped to thread_id on wire); table resets to page 0, historical query refetches with thread_id, SSE reconnects, histogram updates
- **Runtime result:** `PENDING`

### [UI-LOG-07B] Logs — Select Session Filter — useLiveEventStream

- **Entry type:** `network_request`
- **Parent action:** `UI-LOG-07`
- **Source:** `crates/cc-lb-admin/web/src/lib/useLiveEventStream.ts:270#connect`
- **Preconditions:** Session list extracted from loaded rows
- **Steps:** Open Session LogSelect dropdown → Select a session chip or 'All sessions'
- **Scope:** `each_filter_combination` — Unique session IDs from loaded rows
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/events/stream`
  - Query: `thread_id`
  - Body: None
  - Headers: `last-event-id`
- **Handler:** `handle_events_stream`
- **Storage operations:** `sqlite:RequestEventStore::current_request_event_cursor:SqliteStorage::current_request_event_cursor`, `postgres:RequestEventStore::current_request_event_cursor:PostgresStorage::current_request_event_cursor`, `sqlite:RequestEventStore::query_request_events_between_cursors:SqliteStorage::query_request_events_between_cursors`, `postgres:RequestEventStore::query_request_events_between_cursors:PostgresStorage::query_request_events_between_cursors`
- **Cache / no-query path:** SSE: reads the current cursor and performs one reconnect backfill bounded by BACKFILL_MAX_EVENTS=500; reaching the cap emits a backfill-cap reset, otherwise delivery continues from in-memory event-bus/storage-tail broadcasts with heartbeats.
- **Side effects:** None
- **Expected UI:** Navigates search params with session (mapped to thread_id on wire); table resets to page 0, historical query refetches with thread_id, SSE reconnects, histogram updates
- **Runtime result:** `PENDING`

### [UI-LOG-07C] Logs — Select Session Filter — useEventsHistogram

- **Entry type:** `network_request`
- **Parent action:** `UI-LOG-07`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:497#useEventsHistogram`
- **Preconditions:** Session list extracted from loaded rows
- **Steps:** Open Session LogSelect dropdown → Select a session chip or 'All sessions'
- **Scope:** `each_filter_combination` — Unique session IDs from loaded rows
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/events/histogram`
  - Query: `thread_id`, `since_unix_secs`, `until_unix_secs`, `bucket_ms`
  - Body: None
  - Headers: None
- **Handler:** `crate::events_routes::handle_events_histogram`
- **Storage operations:** `sqlite:RequestEventStore::request_event_histogram:SqliteStorage::request_event_histogram`, `postgres:RequestEventStore::request_event_histogram:PostgresStorage::request_event_histogram`
- **Cache / no-query path:** Computes time buckets and rejects requests exceeding MAX_HISTOGRAM_BUCKETS=240.
- **Side effects:** None
- **Expected UI:** Navigates search params with session (mapped to thread_id on wire); table resets to page 0, historical query refetches with thread_id, SSE reconnects, histogram updates
- **Runtime result:** `PENDING`

### [UI-LOG-08] Logs — Model Input Debounced Filter Change

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-LOG-08A`, `UI-LOG-08B`, `UI-LOG-08C`
- **Source:** `crates/cc-lb-admin/web/src/routes/logs.tsx:874#LogsPage`
- **Preconditions:** None
- **Steps:** Type in Model input field → Wait 300ms for debounce timer to fire
- **Scope:** `each_filter_combination` — Equivalence classes: [empty, exact model name, prefix string, special chars]
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** After 300ms debounce, router search updates with model; table resets to page 0, historical query refetches, SSE reconnects with model prefix filter, histogram updates
- **Runtime result:** `PENDING`

### [UI-LOG-08A] Logs — Model Input Debounced Filter Change — useRecentEventsPage

- **Entry type:** `network_request`
- **Parent action:** `UI-LOG-08`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:622#useRecentEventsPage`
- **Preconditions:** None
- **Steps:** Type in Model input field → Wait 300ms for debounce timer to fire
- **Scope:** `each_filter_combination` — Equivalence classes: [empty, exact model name, prefix string, special chars]
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/events/recent`
  - Query: `model`, `limit`
  - Body: None
  - Headers: None
- **Handler:** `handle_recent_events`
- **Storage operations:** `sqlite:RequestEventStore::list_request_events:SqliteStorage::list_request_events`, `postgres:RequestEventStore::list_request_events:PostgresStorage::list_request_events`
- **Cache / no-query path:** Dynamic filter queries
- **Side effects:** None
- **Expected UI:** After 300ms debounce, router search updates with model; table resets to page 0, historical query refetches, SSE reconnects with model prefix filter, histogram updates
- **Runtime result:** `PENDING`

### [UI-LOG-08B] Logs — Model Input Debounced Filter Change — useLiveEventStream

- **Entry type:** `network_request`
- **Parent action:** `UI-LOG-08`
- **Source:** `crates/cc-lb-admin/web/src/lib/useLiveEventStream.ts:270#connect`
- **Preconditions:** None
- **Steps:** Type in Model input field → Wait 300ms for debounce timer to fire
- **Scope:** `each_filter_combination` — Equivalence classes: [empty, exact model name, prefix string, special chars]
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/events/stream`
  - Query: `model`
  - Body: None
  - Headers: `last-event-id`
- **Handler:** `handle_events_stream`
- **Storage operations:** `sqlite:RequestEventStore::current_request_event_cursor:SqliteStorage::current_request_event_cursor`, `postgres:RequestEventStore::current_request_event_cursor:PostgresStorage::current_request_event_cursor`, `sqlite:RequestEventStore::query_request_events_between_cursors:SqliteStorage::query_request_events_between_cursors`, `postgres:RequestEventStore::query_request_events_between_cursors:PostgresStorage::query_request_events_between_cursors`
- **Cache / no-query path:** SSE: reads the current cursor and performs one reconnect backfill bounded by BACKFILL_MAX_EVENTS=500; reaching the cap emits a backfill-cap reset, otherwise delivery continues from in-memory event-bus/storage-tail broadcasts with heartbeats.
- **Side effects:** None
- **Expected UI:** After 300ms debounce, router search updates with model; table resets to page 0, historical query refetches, SSE reconnects with model prefix filter, histogram updates
- **Runtime result:** `PENDING`

### [UI-LOG-08C] Logs — Model Input Debounced Filter Change — useEventsHistogram

- **Entry type:** `network_request`
- **Parent action:** `UI-LOG-08`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:497#useEventsHistogram`
- **Preconditions:** None
- **Steps:** Type in Model input field → Wait 300ms for debounce timer to fire
- **Scope:** `each_filter_combination` — Equivalence classes: [empty, exact model name, prefix string, special chars]
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/events/histogram`
  - Query: `model`, `since_unix_secs`, `until_unix_secs`, `bucket_ms`
  - Body: None
  - Headers: None
- **Handler:** `crate::events_routes::handle_events_histogram`
- **Storage operations:** `sqlite:RequestEventStore::request_event_histogram:SqliteStorage::request_event_histogram`, `postgres:RequestEventStore::request_event_histogram:PostgresStorage::request_event_histogram`
- **Cache / no-query path:** Computes time buckets and rejects requests exceeding MAX_HISTOGRAM_BUCKETS=240.
- **Side effects:** None
- **Expected UI:** After 300ms debounce, router search updates with model; table resets to page 0, historical query refetches, SSE reconnects with model prefix filter, histogram updates
- **Runtime result:** `PENDING`

### [UI-LOG-09] Logs — Select Status Class Filter

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-LOG-09A`, `UI-LOG-09B`, `UI-LOG-09C`
- **Source:** `crates/cc-lb-admin/web/src/routes/logs.tsx:883#LogsPage`
- **Preconditions:** None
- **Steps:** Open Status LogSelect dropdown → Select 2xx, 3xx, 4xx, 5xx or 'All statuses'
- **Scope:** `each_filter_combination` — LOG_STATUS_CLASSES enum options
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Navigates search params with status (mapped to status_class on wire); table resets to page 0, historical query refetches, SSE reconnects, histogram updates
- **Runtime result:** `PENDING`

### [UI-LOG-09A] Logs — Select Status Class Filter — useRecentEventsPage

- **Entry type:** `network_request`
- **Parent action:** `UI-LOG-09`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:622#useRecentEventsPage`
- **Preconditions:** None
- **Steps:** Open Status LogSelect dropdown → Select 2xx, 3xx, 4xx, 5xx or 'All statuses'
- **Scope:** `each_filter_combination` — LOG_STATUS_CLASSES enum options
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/events/recent`
  - Query: `status_class`, `limit`
  - Body: None
  - Headers: None
- **Handler:** `handle_recent_events`
- **Storage operations:** `sqlite:RequestEventStore::list_request_events:SqliteStorage::list_request_events`, `postgres:RequestEventStore::list_request_events:PostgresStorage::list_request_events`
- **Cache / no-query path:** Dynamic filter queries
- **Side effects:** None
- **Expected UI:** Navigates search params with status (mapped to status_class on wire); table resets to page 0, historical query refetches, SSE reconnects, histogram updates
- **Runtime result:** `PENDING`

### [UI-LOG-09B] Logs — Select Status Class Filter — useLiveEventStream

- **Entry type:** `network_request`
- **Parent action:** `UI-LOG-09`
- **Source:** `crates/cc-lb-admin/web/src/lib/useLiveEventStream.ts:270#connect`
- **Preconditions:** None
- **Steps:** Open Status LogSelect dropdown → Select 2xx, 3xx, 4xx, 5xx or 'All statuses'
- **Scope:** `each_filter_combination` — LOG_STATUS_CLASSES enum options
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/events/stream`
  - Query: `status_class`
  - Body: None
  - Headers: `last-event-id`
- **Handler:** `handle_events_stream`
- **Storage operations:** `sqlite:RequestEventStore::current_request_event_cursor:SqliteStorage::current_request_event_cursor`, `postgres:RequestEventStore::current_request_event_cursor:PostgresStorage::current_request_event_cursor`, `sqlite:RequestEventStore::query_request_events_between_cursors:SqliteStorage::query_request_events_between_cursors`, `postgres:RequestEventStore::query_request_events_between_cursors:PostgresStorage::query_request_events_between_cursors`
- **Cache / no-query path:** SSE: reads the current cursor and performs one reconnect backfill bounded by BACKFILL_MAX_EVENTS=500; reaching the cap emits a backfill-cap reset, otherwise delivery continues from in-memory event-bus/storage-tail broadcasts with heartbeats.
- **Side effects:** None
- **Expected UI:** Navigates search params with status (mapped to status_class on wire); table resets to page 0, historical query refetches, SSE reconnects, histogram updates
- **Runtime result:** `PENDING`

### [UI-LOG-09C] Logs — Select Status Class Filter — useEventsHistogram

- **Entry type:** `network_request`
- **Parent action:** `UI-LOG-09`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:497#useEventsHistogram`
- **Preconditions:** None
- **Steps:** Open Status LogSelect dropdown → Select 2xx, 3xx, 4xx, 5xx or 'All statuses'
- **Scope:** `each_filter_combination` — LOG_STATUS_CLASSES enum options
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/events/histogram`
  - Query: `status_class`, `since_unix_secs`, `until_unix_secs`, `bucket_ms`
  - Body: None
  - Headers: None
- **Handler:** `crate::events_routes::handle_events_histogram`
- **Storage operations:** `sqlite:RequestEventStore::request_event_histogram:SqliteStorage::request_event_histogram`, `postgres:RequestEventStore::request_event_histogram:PostgresStorage::request_event_histogram`
- **Cache / no-query path:** Computes time buckets and rejects requests exceeding MAX_HISTOGRAM_BUCKETS=240.
- **Side effects:** None
- **Expected UI:** Navigates search params with status (mapped to status_class on wire); table resets to page 0, historical query refetches, SSE reconnects, histogram updates
- **Runtime result:** `PENDING`

### [UI-LOG-10] Logs — Select Source Kind Filter

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-LOG-10A`, `UI-LOG-10B`, `UI-LOG-10C`
- **Source:** `crates/cc-lb-admin/web/src/routes/logs.tsx:892#LogsPage`
- **Preconditions:** None
- **Steps:** Open Kind LogSelect dropdown → Select 'Exclude renewals' (default empty), 'All events', or 'Renewals only'
- **Scope:** `each_filter_combination` — Source kind options
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Navigates search params with source_kind; table resets to page 0, historical query refetches, SSE reconnects, histogram updates
- **Runtime result:** `PENDING`

### [UI-LOG-10A] Logs — Select Source Kind Filter — useRecentEventsPage

- **Entry type:** `network_request`
- **Parent action:** `UI-LOG-10`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:622#useRecentEventsPage`
- **Preconditions:** None
- **Steps:** Open Kind LogSelect dropdown → Select 'Exclude renewals' (default empty), 'All events', or 'Renewals only'
- **Scope:** `each_filter_combination` — Source kind options
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/events/recent`
  - Query: `source_kind`, `limit`
  - Body: None
  - Headers: None
- **Handler:** `handle_recent_events`
- **Storage operations:** `sqlite:RequestEventStore::list_request_events:SqliteStorage::list_request_events`, `postgres:RequestEventStore::list_request_events:PostgresStorage::list_request_events`
- **Cache / no-query path:** Dynamic filter queries
- **Side effects:** None
- **Expected UI:** Navigates search params with source_kind; table resets to page 0, historical query refetches, SSE reconnects, histogram updates
- **Runtime result:** `PENDING`

### [UI-LOG-10B] Logs — Select Source Kind Filter — useLiveEventStream

- **Entry type:** `network_request`
- **Parent action:** `UI-LOG-10`
- **Source:** `crates/cc-lb-admin/web/src/lib/useLiveEventStream.ts:270#connect`
- **Preconditions:** None
- **Steps:** Open Kind LogSelect dropdown → Select 'Exclude renewals' (default empty), 'All events', or 'Renewals only'
- **Scope:** `each_filter_combination` — Source kind options
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/events/stream`
  - Query: `source_kind`
  - Body: None
  - Headers: `last-event-id`
- **Handler:** `handle_events_stream`
- **Storage operations:** `sqlite:RequestEventStore::current_request_event_cursor:SqliteStorage::current_request_event_cursor`, `postgres:RequestEventStore::current_request_event_cursor:PostgresStorage::current_request_event_cursor`, `sqlite:RequestEventStore::query_request_events_between_cursors:SqliteStorage::query_request_events_between_cursors`, `postgres:RequestEventStore::query_request_events_between_cursors:PostgresStorage::query_request_events_between_cursors`
- **Cache / no-query path:** SSE: reads the current cursor and performs one reconnect backfill bounded by BACKFILL_MAX_EVENTS=500; reaching the cap emits a backfill-cap reset, otherwise delivery continues from in-memory event-bus/storage-tail broadcasts with heartbeats.
- **Side effects:** None
- **Expected UI:** Navigates search params with source_kind; table resets to page 0, historical query refetches, SSE reconnects, histogram updates
- **Runtime result:** `PENDING`

### [UI-LOG-10C] Logs — Select Source Kind Filter — useEventsHistogram

- **Entry type:** `network_request`
- **Parent action:** `UI-LOG-10`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:497#useEventsHistogram`
- **Preconditions:** None
- **Steps:** Open Kind LogSelect dropdown → Select 'Exclude renewals' (default empty), 'All events', or 'Renewals only'
- **Scope:** `each_filter_combination` — Source kind options
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/events/histogram`
  - Query: `source_kind`, `since_unix_secs`, `until_unix_secs`, `bucket_ms`
  - Body: None
  - Headers: None
- **Handler:** `crate::events_routes::handle_events_histogram`
- **Storage operations:** `sqlite:RequestEventStore::request_event_histogram:SqliteStorage::request_event_histogram`, `postgres:RequestEventStore::request_event_histogram:PostgresStorage::request_event_histogram`
- **Cache / no-query path:** Computes time buckets and rejects requests exceeding MAX_HISTOGRAM_BUCKETS=240.
- **Side effects:** None
- **Expected UI:** Navigates search params with source_kind; table resets to page 0, historical query refetches, SSE reconnects, histogram updates
- **Runtime result:** `PENDING`

### [UI-LOG-11] Logs — Histogram View Pan/Zoom and Selection Drag

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-LOG-11B`, `UI-LOG-11A`
- **Source:** `crates/cc-lb-admin/web/src/components/ui/TimeRangeStrip.tsx:250#TimeRangeStrip`
- **Preconditions:** Canvas rendered with valid width
- **Steps:** Drag bottom axis area to pan view → Mouse wheel on canvas to zoom domain in/out → Drag across canvas bars to brush-select time window [sel.a, sel.b] → Release mouse to commit selection
- **Scope:** `once` — Canvas drag actions
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Canvas updates shaded highlight in real time; on mouse up, commits selection calling commitSelection in logs.tsx; navigates since_unix_secs and until_unix_secs; table and histogram refetch with new bounds
- **Runtime result:** `PENDING`

### [UI-LOG-11B] Logs — Histogram View Pan/Zoom and Selection Drag — useRecentEventsPage

- **Entry type:** `network_request`
- **Parent action:** `UI-LOG-11`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:622#useRecentEventsPage`
- **Preconditions:** Canvas rendered with valid width
- **Steps:** Drag bottom axis area to pan view → Mouse wheel on canvas to zoom domain in/out → Drag across canvas bars to brush-select time window [sel.a, sel.b] → Release mouse to commit selection
- **Scope:** `once` — Canvas drag actions
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/events/recent`
  - Query: `since_unix_secs`, `until_unix_secs`, `limit`
  - Body: None
  - Headers: None
- **Handler:** `handle_recent_events`
- **Storage operations:** `sqlite:RequestEventStore::list_request_events:SqliteStorage::list_request_events`, `postgres:RequestEventStore::list_request_events:PostgresStorage::list_request_events`
- **Cache / no-query path:** Dynamic filter queries
- **Side effects:** None
- **Expected UI:** Canvas updates shaded highlight in real time; on mouse up, commits selection calling commitSelection in logs.tsx; navigates since_unix_secs and until_unix_secs; table and histogram refetch with new bounds
- **Runtime result:** `PENDING`

### [UI-LOG-11A] Logs — Histogram View Pan/Zoom and Selection Drag — useEventsHistogram

- **Entry type:** `network_request`
- **Parent action:** `UI-LOG-11`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:497#useEventsHistogram`
- **Preconditions:** Canvas rendered with valid width
- **Steps:** Drag bottom axis area to pan view → Mouse wheel on canvas to zoom domain in/out → Drag across canvas bars to brush-select time window [sel.a, sel.b] → Release mouse to commit selection
- **Scope:** `once` — Canvas drag actions
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/events/histogram`
  - Query: `since_unix_secs`, `until_unix_secs`, `bucket_ms`
  - Body: None
  - Headers: None
- **Handler:** `crate::events_routes::handle_events_histogram`
- **Storage operations:** `sqlite:RequestEventStore::request_event_histogram:SqliteStorage::request_event_histogram`, `postgres:RequestEventStore::request_event_histogram:PostgresStorage::request_event_histogram`
- **Cache / no-query path:** Computes time buckets and rejects requests exceeding MAX_HISTOGRAM_BUCKETS=240.
- **Side effects:** None
- **Expected UI:** Canvas updates shaded highlight in real time; on mouse up, commits selection calling commitSelection in logs.tsx; navigates since_unix_secs and until_unix_secs; table and histogram refetch with new bounds
- **Runtime result:** `PENDING`

### [UI-LOG-12] Logs — Absolute Time Bounds Entry and Calendar Pick

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-LOG-12A`, `UI-LOG-12B`
- **Source:** `crates/cc-lb-admin/web/src/components/ui/TimeRangeBounds.tsx:40#TimeRangeBounds`
- **Preconditions:** Valid date-time string in timezone format or blank
- **Steps:** Type YYYY-MM-DD HH:mm into 'From' or 'To' inputs → Or click calendar icon to open CalendarPopover and pick date → Click 'Apply' button
- **Scope:** `once` — Valid date combinations + error boundary cases
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Updates search parameters with since_unix_secs and until_unix_secs; triggers historical recent events and histogram queries; disables live tail if until is set
- **Runtime result:** `PENDING`

### [UI-LOG-12A] Logs — Absolute Time Bounds Entry and Calendar Pick — useRecentEventsPage

- **Entry type:** `network_request`
- **Parent action:** `UI-LOG-12`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:622#useRecentEventsPage`
- **Preconditions:** Valid date-time string in timezone format or blank
- **Steps:** Type YYYY-MM-DD HH:mm into 'From' or 'To' inputs → Or click calendar icon to open CalendarPopover and pick date → Click 'Apply' button
- **Scope:** `once` — Valid date combinations + error boundary cases
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/events/recent`
  - Query: `since_unix_secs`, `until_unix_secs`, `limit`
  - Body: None
  - Headers: None
- **Handler:** `handle_recent_events`
- **Storage operations:** `sqlite:RequestEventStore::list_request_events:SqliteStorage::list_request_events`, `postgres:RequestEventStore::list_request_events:PostgresStorage::list_request_events`
- **Cache / no-query path:** Dynamic filter queries
- **Side effects:** None
- **Expected UI:** Updates search parameters with since_unix_secs and until_unix_secs; triggers historical recent events and histogram queries; disables live tail if until is set
- **Runtime result:** `PENDING`

### [UI-LOG-12B] Logs — Absolute Time Bounds Entry and Calendar Pick — useEventsHistogram

- **Entry type:** `network_request`
- **Parent action:** `UI-LOG-12`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:497#useEventsHistogram`
- **Preconditions:** Valid date-time string in timezone format or blank
- **Steps:** Type YYYY-MM-DD HH:mm into 'From' or 'To' inputs → Or click calendar icon to open CalendarPopover and pick date → Click 'Apply' button
- **Scope:** `once` — Valid date combinations + error boundary cases
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/events/histogram`
  - Query: `since_unix_secs`, `until_unix_secs`, `bucket_ms`
  - Body: None
  - Headers: None
- **Handler:** `crate::events_routes::handle_events_histogram`
- **Storage operations:** `sqlite:RequestEventStore::request_event_histogram:SqliteStorage::request_event_histogram`, `postgres:RequestEventStore::request_event_histogram:PostgresStorage::request_event_histogram`
- **Cache / no-query path:** Computes time buckets and rejects requests exceeding MAX_HISTOGRAM_BUCKETS=240.
- **Side effects:** None
- **Expected UI:** Updates search parameters with since_unix_secs and until_unix_secs; triggers historical recent events and histogram queries; disables live tail if until is set
- **Runtime result:** `PENDING`

### [UI-LOG-13] Logs — Clear All Filters Button Click

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/routes/logs.tsx:917#LogsPage`
- **Preconditions:** At least one filter or time bound is active in search params
- **Steps:** Click 'Clear' button with X icon
- **Scope:** `once` — Clear button presence
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Navigates search to {}; all dropdowns reset to default; model input clears; time window resets to open now; SSE reconnects unfiltered
- **Client-only reason:** Invokes router navigate({ search: {} }), which triggers subsequent query key resets
- **Runtime result:** `PENDING`

### [UI-SRC-2874D8E9F106] Logs — Manual Refresh Button Click

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-LOG-14`
- **Source:** `crates/cc-lb-admin/web/src/routes/logs.tsx:826#LogsPage`
- **Preconditions:** None
- **Steps:** Click 'Refresh' button with RefreshCw icon in page header
- **Scope:** `once` — User action
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Increments paginationRequestGenerationRef, calls recent.refetch(); table rows update with fresh server data
- **Runtime result:** `PENDING`

### [UI-LOG-14] Logs — Manual Refresh Button Click — recent.refetch

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-2874D8E9F106`
- **Source:** `crates/cc-lb-admin/web/src/routes/logs.tsx:692#refreshLogs`
- **Preconditions:** None
- **Steps:** Click 'Refresh' button with RefreshCw icon in page header
- **Scope:** `once` — User action
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/events/recent`
  - Query: `limit`, `since_unix_secs`, `until_unix_secs`, `until_ts_ms`, `until_event_id`
  - Body: None
  - Headers: None
- **Handler:** `handle_recent_events`
- **Storage operations:** `sqlite:RequestEventStore::list_request_events:SqliteStorage::list_request_events`, `postgres:RequestEventStore::list_request_events:PostgresStorage::list_request_events`
- **Cache / no-query path:** Dynamic filter queries
- **Side effects:** None
- **Expected UI:** Increments paginationRequestGenerationRef, calls recent.refetch(); table rows update with fresh server data
- **Runtime result:** `PENDING`

### [UI-LOG-15] Logs — Export Visible Logs to JSON

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/routes/logs.tsx:832#LogsPage`
- **Preconditions:** At least one visible row loaded
- **Steps:** Click 'Export' button with Download icon in page header
- **Scope:** `once` — User action
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Generates a client-side Blob of visibleRows and triggers browser download of file cc-lb-events-YYYY-MM-DDTHH:mm.json
- **Client-only reason:** Client-only Blob creation and anchor download trigger using already-loaded visibleRows
- **Runtime result:** `PENDING`

### [UI-SRC-A6BE3F64E803] Logs — Navigate to Next Historical Page

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-LOG-16`
- **Source:** `crates/cc-lb-admin/web/src/routes/logs.tsx:637#nextPage`
- **Preconditions:** hasMore is true, not currently loading next page
- **Steps:** Click 'Next' button in pagination toolbar at bottom of table
- **Scope:** `once` — Pagination control
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Shows spinner on Next button, fetches next 50 rows using cursor from last row of current page, scrolls table container to top, updates page counter to 'Showing 51–100 of N+'
- **Runtime result:** `PENDING`

### [UI-LOG-16] Logs — Navigate to Next Historical Page — queryClient.fetchQuery(recentEventsPageQueryOptions)

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-A6BE3F64E803`
- **Source:** `crates/cc-lb-admin/web/src/routes/logs.tsx:662#nextPage`
- **Preconditions:** hasMore is true, not currently loading next page
- **Steps:** Click 'Next' button in pagination toolbar at bottom of table
- **Scope:** `once` — Pagination control
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/events/recent`
  - Query: `limit`, `until_ts_ms`, `until_event_id`, `since_unix_secs`, `until_unix_secs`, `principal_id`, `upstream_id`, `thread_id`, `model`, `status_class`, `source_kind`
  - Body: None
  - Headers: None
- **Handler:** `handle_recent_events`
- **Storage operations:** `sqlite:RequestEventStore::list_request_events:SqliteStorage::list_request_events`, `postgres:RequestEventStore::list_request_events:PostgresStorage::list_request_events`
- **Cache / no-query path:** Dynamic filter queries
- **Side effects:** None
- **Expected UI:** Shows spinner on Next button, fetches next 50 rows using cursor from last row of current page, scrolls table container to top, updates page counter to 'Showing 51–100 of N+'
- **Runtime result:** `PENDING`

### [UI-SRC-888A1F4D1DD9] Logs — Navigate to Previous Historical Page

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/routes/logs.tsx:698#previousPage`
- **Preconditions:** page > 0
- **Steps:** Click 'Prev' button in pagination toolbar
- **Scope:** `once` — Pagination control
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Decrements page index, displays cached page from loadedPages, scrolls table container to top
- **Client-only reason:** Client-side state transition rendering cached loadedPages[page - 1]
- **Runtime result:** `PENDING`

### [UI-LOG-17] Logs — Row Anchor Range Buttons (±1m, ±5m, ±30m)

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-LOG-17A`, `UI-LOG-17B`
- **Source:** `crates/cc-lb-admin/web/src/components/ui/RequestEventsTable.tsx:111#RequestEventRow`
- **Preconditions:** Row has valid event timestamp
- **Steps:** Hover over Timestamp cell of any request event row → Click one of the anchor radius buttons: '±1m', '±5m', or '±30m'
- **Scope:** `each_row` — Row timestamp × ANCHOR_RADII_SECS
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Calls focusAround in logs.tsx; re-centers histogram strip view on timestamp ± radius*3; navigates search with since_unix_secs = ts - radius and until_unix_secs = ts + radius; refetches recent and histogram queries
- **Runtime result:** `PENDING`

### [UI-LOG-17A] Logs — Row Anchor Range Buttons (±1m, ±5m, ±30m) — useRecentEventsPage

- **Entry type:** `network_request`
- **Parent action:** `UI-LOG-17`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:622#useRecentEventsPage`
- **Preconditions:** Row has valid event timestamp
- **Steps:** Hover over Timestamp cell of any request event row → Click one of the anchor radius buttons: '±1m', '±5m', or '±30m'
- **Scope:** `each_row` — Row timestamp × ANCHOR_RADII_SECS
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/events/recent`
  - Query: `since_unix_secs`, `until_unix_secs`, `limit`
  - Body: None
  - Headers: None
- **Handler:** `handle_recent_events`
- **Storage operations:** `sqlite:RequestEventStore::list_request_events:SqliteStorage::list_request_events`, `postgres:RequestEventStore::list_request_events:PostgresStorage::list_request_events`
- **Cache / no-query path:** Dynamic filter queries
- **Side effects:** None
- **Expected UI:** Calls focusAround in logs.tsx; re-centers histogram strip view on timestamp ± radius*3; navigates search with since_unix_secs = ts - radius and until_unix_secs = ts + radius; refetches recent and histogram queries
- **Runtime result:** `PENDING`

### [UI-LOG-17B] Logs — Row Anchor Range Buttons (±1m, ±5m, ±30m) — useEventsHistogram

- **Entry type:** `network_request`
- **Parent action:** `UI-LOG-17`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:497#useEventsHistogram`
- **Preconditions:** Row has valid event timestamp
- **Steps:** Hover over Timestamp cell of any request event row → Click one of the anchor radius buttons: '±1m', '±5m', or '±30m'
- **Scope:** `each_row` — Row timestamp × ANCHOR_RADII_SECS
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/events/histogram`
  - Query: `since_unix_secs`, `until_unix_secs`, `bucket_ms`
  - Body: None
  - Headers: None
- **Handler:** `crate::events_routes::handle_events_histogram`
- **Storage operations:** `sqlite:RequestEventStore::request_event_histogram:SqliteStorage::request_event_histogram`, `postgres:RequestEventStore::request_event_histogram:PostgresStorage::request_event_histogram`
- **Cache / no-query path:** Computes time buckets and rejects requests exceeding MAX_HISTOGRAM_BUCKETS=240.
- **Side effects:** None
- **Expected UI:** Calls focusAround in logs.tsx; re-centers histogram strip view on timestamp ± radius*3; navigates search with since_unix_secs = ts - radius and until_unix_secs = ts + radius; refetches recent and histogram queries
- **Runtime result:** `PENDING`

### [UI-SRC-091345AD21A8] Logs — Row Click Open Request Detail Drawer

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/ui/RequestEventsTable.tsx:89#RequestEventRow`
- **Preconditions:** Table row rendered with valid event
- **Steps:** Click anywhere on request event table row → Or press Enter / Space while row is keyboard-focused
- **Scope:** `each_row` — Loaded events list
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Sets selectedId; opens RequestEventDrawer slide-over from right; shows backdrop overlay
- **Client-only reason:** Sets selectedId state in RequestEventsTable to mount RequestEventDrawer
- **Runtime result:** `PENDING`

### [UI-SRC-C59FB001EF22] Logs — Fetch Full Event Diagnostics Detail

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-LOG-18`
- **Source:** `crates/cc-lb-admin/web/src/components/ui/RequestEventDrawer.tsx:94#RequestDetail`
- **Preconditions:** Event is not partial (_phase !== 'partial') and has event_id or request_id
- **Steps:** Open RequestEventDrawer for a finalized event
- **Scope:** `each_row` — Opened request event ID
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Fetches complete event from server; populates body bytes, detailed latency waterfall stages, upstream failure messages, and exact token/cost breakdown
- **Runtime result:** `PENDING`

### [UI-LOG-18] Logs — Fetch Full Event Diagnostics Detail — useRequestEventDetail

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-C59FB001EF22`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:526#useRequestEventDetail`
- **Preconditions:** Event is not partial (_phase !== 'partial') and has event_id or request_id
- **Steps:** Open RequestEventDrawer for a finalized event
- **Scope:** `each_row` — Opened request event ID
- **Risk:** `read_with_audit`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/events/detail/{event_id}`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `handle_event_detail`
- **Storage operations:** `sqlite:RequestEventStore::get_request_event:SqliteStorage::get_request_event`, `postgres:RequestEventStore::get_request_event:PostgresStorage::get_request_event`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Fetches raw unredacted event payload JSON from DB
- **Side effects:** Audit log write: record_admin_audit("request_event_detail_read", "/admin/v1/events/detail/{event_id}")
- **Expected UI:** Fetches complete event from server; populates body bytes, detailed latency waterfall stages, upstream failure messages, and exact token/cost breakdown
- **Runtime result:** `PENDING`

### [UI-SRC-6793FF938733] Logs — Fetch Upstreams for Options and Names

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-LOG-19`
- **Source:** `crates/cc-lb-admin/web/src/routes/logs.tsx:151#LogsPage`
- **Preconditions:** None
- **Steps:** Auto on mount of LogsPage
- **Scope:** `once` — Route mount
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Populates Upstream dropdown filter options and upstream name column in logs table
- **Runtime result:** `PENDING`

### [UI-LOG-19] Logs — Fetch Upstreams for Options and Names — useUpstreams

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-6793FF938733`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:418#useUpstreams`
- **Preconditions:** None
- **Steps:** Auto on mount of LogsPage
- **Scope:** `once` — Route mount
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/upstreams`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `list_upstreams`
- **Storage operations:** `sqlite:UpstreamStore::list:SqliteStorage::list`, `postgres:UpstreamStore::list:PostgresStorage::list`
- **Cache / no-query path:** Sets X-Total-Count header; in-memory keyset pagination on ?after (UUID) UpstreamResponse now serializes base_url: Option<Url> (crates/cc-lb-admin/src/v1/upstreams.rs:167,1587), so the field is present and null when the upstream has no custom base URL.
- **Side effects:** None
- **Expected UI:** Populates Upstream dropdown filter options and upstream name column in logs table
- **Runtime result:** `PENDING`

### [UI-SRC-476EE1297D09] Logs — Fetch Principals for Options and Names

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-LOG-20`
- **Source:** `crates/cc-lb-admin/web/src/routes/logs.tsx:219#LogsPage`
- **Preconditions:** None
- **Steps:** Auto on mount of LogsPage
- **Scope:** `once` — Route mount
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Populates Principal dropdown filter options and principal name column in logs table
- **Runtime result:** `PENDING`

### [UI-LOG-20] Logs — Fetch Principals for Options and Names — usePrincipals

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-476EE1297D09`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:427#usePrincipals`
- **Preconditions:** None
- **Steps:** Auto on mount of LogsPage
- **Scope:** `once` — Route mount
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/principals`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `list_principals`
- **Storage operations:** `sqlite:PrincipalStore::list:SqliteStorage::list`, `postgres:PrincipalStore::list:PostgresStorage::list`
- **Cache / no-query path:** Executes PrincipalStore::list twice: first with offset=0 and limit=1000 to derive the capped X-Total-Count, then with the requested offset/limit for the page; no COUNT SQL is executed.
- **Side effects:** None
- **Expected UI:** Populates Principal dropdown filter options and principal name column in logs table
- **Runtime result:** `PENDING`

### [UI-SRC-2B0EFC709378] Logs — SSE Reconnect Delta Catch-Up Query

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-LOG-21`
- **Source:** `crates/cc-lb-admin/web/src/lib/useLiveEventStream.ts:218#connect`
- **Preconditions:** lastCursor is known from prior stream events; isBackfill flag is true
- **Steps:** SSE connection drops and reconnects with lastCursorRef != null, or user returns to tab after pause
- **Scope:** `once` — Stream disconnect/reconnect lifecycle
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Backfills missed events between lastCursor and current server cursor without full table reset
- **Runtime result:** `PENDING`

### [UI-LOG-21] Logs — SSE Reconnect Delta Catch-Up Query — getJson('/admin/v1/events/delta')

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-2B0EFC709378`
- **Source:** `crates/cc-lb-admin/web/src/lib/useLiveEventStream.ts:223#connect`
- **Preconditions:** lastCursor is known from prior stream events; isBackfill flag is true
- **Steps:** SSE connection drops and reconnects with lastCursorRef != null, or user returns to tab after pause
- **Scope:** `once` — Stream disconnect/reconnect lifecycle
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/events/delta`
  - Query: `since_cursor`, `limit`, `principal_id`, `upstream_id`, `thread_id`, `model`, `status_class`, `source_kind`
  - Body: None
  - Headers: None
- **Handler:** `crate::events_routes::handle_events_delta`
- **Storage operations:** `sqlite:RequestEventStore::current_request_event_cursor:SqliteStorage::current_request_event_cursor`, `postgres:RequestEventStore::current_request_event_cursor:PostgresStorage::current_request_event_cursor`, `sqlite:RequestEventStore::query_request_events_between_cursors:SqliteStorage::query_request_events_between_cursors`, `postgres:RequestEventStore::query_request_events_between_cursors:PostgresStorage::query_request_events_between_cursors`
- **Cache / no-query path:** Reads a storage cursor and cursor-bounded page. build_delta_events_payload in the admin layer then applies residual filters in memory (including excluding renewal source_kind by default); exhausted is computed from the pre-filter row count.
- **Side effects:** None
- **Expected UI:** Backfills missed events between lastCursor and current server cursor without full table reset
- **Runtime result:** `PENDING`

### [UI-LOG-22] Logs — Force Reconnect Button Click on Terminal SSE Failure

- **Entry type:** `ui_action`
- **Atomic requests:** `REQ-SRC-B1BE83CDD51E`
- **Source:** `crates/cc-lb-admin/web/src/components/LiveTailFailureBanner.tsx:43#LiveTailFailureBanner`
- **Preconditions:** permanentFailure is true
- **Steps:** Click 'Retry now' button in LiveTailFailureBanner when SSE fails permanently (>5m)
- **Scope:** `once` — Failure banner action
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Calls forceReconnect(); attempts fresh connection to /admin/events/stream; resets failure counter on success
- **Runtime result:** `PENDING`

### [REQ-SRC-B1BE83CDD51E] Logs — Force Reconnect Button Click on Terminal SSE Failure — useLiveEventStream.forceReconnect

- **Entry type:** `network_request`
- **Parent action:** `UI-LOG-22`
- **Source:** `crates/cc-lb-admin/web/src/lib/useLiveEventStream.ts:436#forceReconnect`
- **Preconditions:** permanentFailure is true
- **Steps:** Click 'Retry now' button in LiveTailFailureBanner when SSE fails permanently (>5m)
- **Scope:** `once` — Failure banner action
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/events/stream`
  - Query: `principal_id`, `upstream_id`, `thread_id`, `model`, `status_class`, `source_kind`
  - Body: None
  - Headers: `last-event-id`
- **Handler:** `handle_events_stream`
- **Storage operations:** `sqlite:RequestEventStore::current_request_event_cursor:SqliteStorage::current_request_event_cursor`, `postgres:RequestEventStore::current_request_event_cursor:PostgresStorage::current_request_event_cursor`, `sqlite:RequestEventStore::query_request_events_between_cursors:SqliteStorage::query_request_events_between_cursors`, `postgres:RequestEventStore::query_request_events_between_cursors:PostgresStorage::query_request_events_between_cursors`
- **Cache / no-query path:** SSE: reads the current cursor and performs one reconnect backfill bounded by BACKFILL_MAX_EVENTS=500; reaching the cap emits a backfill-cap reset, otherwise delivery continues from in-memory event-bus/storage-tail broadcasts with heartbeats.
- **Side effects:** None
- **Expected UI:** Calls forceReconnect(); attempts fresh connection to /admin/events/stream; resets failure counter on success
- **Runtime result:** `PENDING`

### [UI-LOG-25] Logs — Dismiss Failure Banner

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/LiveTailFailureBanner.tsx:48#LiveTailFailureBanner`
- **Preconditions:** permanentFailure is true
- **Steps:** Click X (Dismiss) icon button on LiveTailFailureBanner
- **Scope:** `once` — User action
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Hides the banner from view until another permanent failure transition occurs
- **Client-only reason:** Client-only state update: setDismissed(true)
- **Runtime result:** `PENDING`

### [UI-LOG-23] Logs — Copy Identity Fields to Clipboard

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/ui/RequestEventIdentity.tsx:136#RequestEventIdentity`
- **Preconditions:** Drawer open, respective field is present
- **Steps:** Click Copy button next to Request ID → Click Copy button next to Key ID → Click Copy button next to Session ID → Click Copy button next to Observed Session ID → Click Copy button next to Parent Session ID → Click Copy button next to Agent ID → Click Copy button next to Parent Agent ID
- **Scope:** `each_row` — Identity fields present on event
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Copies string to clipboard and displays toast confirmation via sonner
- **Client-only reason:** Client-side navigator.clipboard.writeText action
- **Runtime result:** `PENDING`

### [UI-LOG-28] Logs — Close Detail Drawer

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/ui/RequestEventDrawer.tsx:141#RequestDetail`
- **Preconditions:** Drawer is currently open
- **Steps:** Click X button in drawer header → Or click outside on backdrop → Or press Escape
- **Scope:** `once` — User action
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Drawer slides out to the right; backdrop fades out; selectedId reset to null
- **Client-only reason:** Client-side Dialog state update calling onClose()
- **Runtime result:** `PENDING`

### [UI-LOG-29] Logs — Latency Stage Hover Popover and Sticky Click

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/ui/latency/LatencyTimeline.tsx:496#LatencyTimeline`
- **Preconditions:** Drawer open with latency data available
- **Steps:** Hover over any stage segment in the horizontal waterfall bar → Click a stage segment to lock sticky focus
- **Scope:** `each_row` — Stages present in event timing
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Popover displays stage duration and detailed description; click toggles sticky highlight with dimmed sibling segments
- **Client-only reason:** Client-only SVG/DOM interaction managing activeKey/stickyKey
- **Runtime result:** `PENDING`

### [UI-LOG-30] Logs — Streaming Event Markers (TTFT / Message Start / Deltas / Stop)

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/ui/latency/LatencyTimeline.tsx:962#LatencyTimeline`
- **Preconditions:** Event has streaming marker timestamps (e.g. first_delta_ms)
- **Steps:** Hover or click marker pins along the latency timeline
- **Scope:** `each_row` — Recorded streaming markers on event
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Displays Popover with exact marker description and offset relative to request start
- **Client-only reason:** Client-only popover display
- **Runtime result:** `PENDING`

### [UI-LOG-31] Logs — Pie Slice Hover and Sticky Click

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/ui/usage/PieChart.tsx:154#PieChart`
- **Preconditions:** Event has non-zero tokens or cost
- **Steps:** Hover over slice in TokenPie or CostPie donut chart → Click slice to lock sticky focus
- **Scope:** `each_row` — Token/cost categories present on event
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Center text changes to show selected slice label, count, and percentage; clicking applies white border stroke to slice
- **Client-only reason:** Client-side SVG path onMouseEnter/onClick state manipulation
- **Runtime result:** `PENDING`

### [UI-LOG-26] Logs — Rewrite Legacy ?time_range= Preset to Absolute Unix Bounds

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/routes/logs.tsx:236#LogsPage`
- **Preconditions:** URL contains legacy time_range param
- **Steps:** Navigate to /logs?time_range=1h (or 6h, 24h, 7d)
- **Scope:** `once` — Legacy preset strings
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Immediately replaces URL with since_unix_secs = Math.floor(now/1000) - width and removes time_range param, keeping right edge open for live tail
- **Client-only reason:** Client-side router replace navigation effect
- **Runtime result:** `PENDING`

### [UI-SRC-00BD57E4D253] Principals — Load Principals List

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PR-01`
- **Source:** `crates/cc-lb-admin/web/src/routes/principals.tsx:297#PrincipalsPage`
- **Preconditions:** User authenticated via AuthRequiredGate
- **Steps:** Navigate to /principals → Route component mounts and executes usePrincipals()
- **Scope:** `once` — All active principals
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Sidebar displays principal count header and list of principal cards with name, status dot, kind badge, allowed models/limits count, and revision
- **Runtime result:** `PENDING`

### [UI-PR-01] Principals — Load Principals List — usePrincipals

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-00BD57E4D253`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:427#usePrincipals`
- **Preconditions:** User authenticated via AuthRequiredGate
- **Steps:** Navigate to /principals → Route component mounts and executes usePrincipals()
- **Scope:** `once` — All active principals
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/principals`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `list_principals`
- **Storage operations:** `sqlite:PrincipalStore::list:SqliteStorage::list`, `postgres:PrincipalStore::list:PostgresStorage::list`
- **Cache / no-query path:** Executes PrincipalStore::list twice: first with offset=0 and limit=1000 to derive the capped X-Total-Count, then with the requested offset/limit for the page; no COUNT SQL is executed.
- **Side effects:** None
- **Expected UI:** Sidebar displays principal count header and list of principal cards with name, status dot, kind badge, allowed models/limits count, and revision
- **Runtime result:** `PENDING`

### [UI-SRC-241AEA7F7B55] Principals — Desktop Auto-Select First Principal

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/routes/principals.tsx:321#PrincipalsPage`
- **Preconditions:** principals.length > 0; window.matchMedia('(min-width: 768px)').matches
- **Steps:** Principals query finishes loading on desktop viewport (>=768px) → selectedId search param is absent
- **Scope:** `once` — First item in principals query array
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** URL updates to /principals?selectedId=<first_id>, mounting PrincipalDetail in main pane
- **Client-only reason:** Client-side routing navigation replacing search params without extra network call
- **Runtime result:** `PENDING`

### [UI-PR-02] Principals — Select Principal from Sidebar

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/routes/principals.tsx:393#PrincipalsPage`
- **Preconditions:** Principal list rendered
- **Steps:** Click principal item button in sidebar
- **Scope:** `each_principal` — GET /admin/v1/principals -> data.principals[].id
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Selected row receives active highlight border-accent/40 bg-accent/5, right pane renders PrincipalDetail
- **Client-only reason:** Updates TanStack router search params { selectedId: id }
- **Runtime result:** `PENDING`

### [UI-SRC-B7B21779622C] Principals — Action New URL Query Parameter Trigger

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/routes/principals.tsx:312#PrincipalsPage`
- **Preconditions:** action === 'new'
- **Steps:** Navigate to /principals?action=new
- **Scope:** `once` — Route search schema
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** CreatePrincipalModal opens, action param cleared from URL via replace: true
- **Client-only reason:** URL search param effect triggering modal state
- **Runtime result:** `PENDING`

### [UI-SRC-783B9A8424A8] Principals — Open Create Principal Modal via New Button

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/routes/principals.tsx:353#PrincipalsPage`
- **Preconditions:** Sidebar rendered
- **Steps:** Click '#btn-new-principal' in sidebar header or 'New principal' in empty state
- **Scope:** `once` — Sidebar UI
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** CreatePrincipalModal opens with empty name input, kind defaulting to 'machine', and empty default limits
- **Client-only reason:** Sets local state createOpen to true
- **Runtime result:** `PENDING`

### [UI-SRC-E9A473CD5D19] Principals — Create Principal Submit

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PR-23`
- **Source:** `crates/cc-lb-admin/web/src/routes/principals.tsx:2720#CreatePrincipalModal`
- **Preconditions:** name.trim() !== ''; createInFlight.current === false
- **Steps:** Fill Name input → Select Kind → Optionally add default limits → Click 'Create' submit button
- **Scope:** `once` — Operator input form
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **Expected UI:** Button shows 'Creating...' spinner, modal closes on success, toast 'Principal created', principals list refreshed
- **Runtime result:** `PENDING`

### [UI-PR-23] Principals — Create Principal Submit — useCreatePrincipal

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-E9A473CD5D19`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1206#useCreatePrincipal`
- **Preconditions:** name.trim() !== ''; createInFlight.current === false
- **Steps:** Fill Name input → Select Kind → Optionally add default limits → Click 'Create' submit button
- **Scope:** `once` — Operator input form
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/v1/principals`
  - Query: None
  - Body: `name`, `kind`, `default_limits`
  - Headers: `authorization`, `content-type`
- **Handler:** `create_principal`
- **Storage operations:** `sqlite:PrincipalStore::create:SqliteStorage::create`, `postgres:PrincipalStore::create:PostgresStorage::create`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Rebinds DynamicViewHolder principal views; invalidates TanStack queryKey ['principals']
- **Side effects:** Audit event `principal_create`; provisions principal record and default router terminal plugin chain QA restore: soft-delete the created principal after removing any test keys.
- **Expected UI:** Button shows 'Creating...' spinner, modal closes on success, toast 'Principal created', principals list refreshed
- **Runtime result:** `PENDING`

### [UI-PR-03] Principals — Toggle Principal Enabled State

- **Entry type:** `ui_action`
- **Atomic requests:** `REQ-SRC-C31B76D697EB`, `REQ-SRC-39C6706B40BF`
- **Source:** `crates/cc-lb-admin/web/src/routes/principals.tsx:528#PrincipalDetail`
- **Preconditions:** Principal selected; del.isPending === false; principalWritePending === false
- **Steps:** Click 'Enable' or 'Disable' button in detail header
- **Scope:** `each_principal` — GET /admin/v1/principals -> data.principals[]
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **Expected UI:** Header button shows spinner, status badge updates to Enabled/Disabled, toast confirms change
- **Runtime result:** `PENDING`

### [REQ-SRC-C31B76D697EB] Principals — Toggle Principal Enabled State — enable

- **Entry type:** `network_request`
- **Parent action:** `UI-PR-03`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1244#useTogglePrincipal`
- **Preconditions:** Principal selected; del.isPending === false; principalWritePending === false
- **Steps:** Click 'Enable' or 'Disable' button in detail header
- **Scope:** `each_principal` — GET /admin/v1/principals -> data.principals[]
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/v1/principals/{id}/enable`
  - Query: None
  - Body: None
  - Headers: `authorization`, `if-match`
  - Branch condition: `target_enabled === true`
- **Handler:** `enable_principal`
- **Storage operations:** `sqlite:PrincipalStore::set_enabled:SqliteStorage::set_enabled`, `postgres:PrincipalStore::set_enabled:PostgresStorage::set_enabled`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Rebinds DynamicViewHolder; invalidates queryKey ['principals']
- **Side effects:** Audit event `principal_enable`; activates principal traffic intake QA restore: POST the matching disable endpoint with the returned revision.
- **Expected UI:** Header button shows spinner, status badge updates to Enabled/Disabled, toast confirms change
- **Runtime result:** `PENDING`

### [REQ-SRC-39C6706B40BF] Principals — Toggle Principal Enabled State — disable

- **Entry type:** `network_request`
- **Parent action:** `UI-PR-03`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1244#useTogglePrincipal`
- **Preconditions:** Principal selected; del.isPending === false; principalWritePending === false
- **Steps:** Click 'Enable' or 'Disable' button in detail header
- **Scope:** `each_principal` — GET /admin/v1/principals -> data.principals[]
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/v1/principals/{id}/disable`
  - Query: None
  - Body: None
  - Headers: `authorization`, `if-match`
  - Branch condition: `target_enabled === false`
- **Handler:** `disable_principal`
- **Storage operations:** `sqlite:PrincipalStore::set_enabled:SqliteStorage::set_enabled`, `postgres:PrincipalStore::set_enabled:PostgresStorage::set_enabled`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Rebinds DynamicViewHolder; invalidates queryKey ['principals']
- **Side effects:** Audit event `principal_disable`; disables principal and halts proxy traffic for all its keys QA restore: POST the matching enable endpoint with the returned revision.
- **Expected UI:** Header button shows spinner, status badge updates to Enabled/Disabled, toast confirms change
- **Runtime result:** `PENDING`

### [UI-SRC-B1990946C344] Principals — Delete Principal via Confirm Dialog

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PR-04`
- **Source:** `crates/cc-lb-admin/web/src/routes/principals.tsx:583#PrincipalDetail`
- **Preconditions:** Principal selected; toggle.isPending === false; principalWritePending === false
- **Steps:** Click 'Delete' button in detail header → Confirm in dialog
- **Scope:** `each_principal` — GET /admin/v1/principals -> data.principals[]
- **Risk:** `destructive_write`
- **Production applicability:** `available`
- **Expected UI:** ConfirmDialog displays number of plugin chain entries to be removed; on success toasts 'Principal deleted' and unselects principal
- **Runtime result:** `PENDING`

### [UI-PR-04] Principals — Delete Principal via Confirm Dialog — useDeletePrincipal

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-B1990946C344`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1231#useDeletePrincipal`
- **Preconditions:** Principal selected; toggle.isPending === false; principalWritePending === false
- **Steps:** Click 'Delete' button in detail header → Confirm in dialog
- **Scope:** `each_principal` — GET /admin/v1/principals -> data.principals[]
- **Risk:** `destructive_write`
- **Production applicability:** `available`
- **HTTP:** `DELETE /admin/v1/principals/{id}`
  - Query: None
  - Body: None
  - Headers: `authorization`, `if-match`
- **Handler:** `delete_principal`
- **Storage operations:** `sqlite:PrincipalStore::soft_delete:SqliteStorage::soft_delete`, `postgres:PrincipalStore::soft_delete:PostgresStorage::soft_delete`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Rebinds DynamicViewHolder; invalidates queryKey ['principals'] and ['status']
- **Side effects:** Audit event `principal_delete`; soft-deletes principal row (sets deleted_at); cascades deletion of plugin chains; prevents future proxy routing QA restore: restore a database snapshot or recreate the principal and its deleted plugin-chain entries.
- **Expected UI:** ConfirmDialog displays number of plugin chain entries to be removed; on success toasts 'Principal deleted' and unselects principal
- **Runtime result:** `PENDING`

### [UI-SRC-B0C8E4E556BC] Principals — Mobile Back to Master List

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/routes/principals.tsx:510#PrincipalDetail`
- **Preconditions:** Mobile viewport; Principal selected
- **Steps:** Click '◀ Back' button in detail header on mobile viewport (<768px)
- **Scope:** `each_principal` — Mobile UI state
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Clears selectedId search param, returning to full-width master list
- **Client-only reason:** Calls onBack() which navigates to /principals with empty search
- **Runtime result:** `PENDING`

### [UI-SRC-74652446E668] Principals — Update Allowed Models

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PR-05`
- **Source:** `crates/cc-lb-admin/web/src/routes/principals.tsx:718#AllowedModelsCard`
- **Preconditions:** Principal selected; editing === true; editRevision !== null; principalWritePending === false
- **Steps:** Click 'Edit' on Allowed Models card → Type or modify comma-separated model names in textarea → Click 'Save'
- **Scope:** `each_principal` — GET /admin/v1/principals -> data.principals[].allowed_models
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **Expected UI:** Saves allowed_models array, toasts 'Allowed models updated', exits editing mode, renders badges for each model
- **Runtime result:** `PENDING`

### [UI-PR-05] Principals — Update Allowed Models — useSetAllowedModels

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-74652446E668`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1267#useSetAllowedModels`
- **Preconditions:** Principal selected; editing === true; editRevision !== null; principalWritePending === false
- **Steps:** Click 'Edit' on Allowed Models card → Type or modify comma-separated model names in textarea → Click 'Save'
- **Scope:** `each_principal` — GET /admin/v1/principals -> data.principals[].allowed_models
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `PUT /admin/v1/principals/{id}/allowed_models`
  - Query: None
  - Body: `models`, `expected_revision`
  - Headers: `authorization`, `content-type`
- **Handler:** `update_allowed_models`
- **Storage operations:** `sqlite:PrincipalStore::update:SqliteStorage::update`, `postgres:PrincipalStore::update:PostgresStorage::update`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Rebinds DynamicViewHolder; invalidates queryKey ['principals']
- **Side effects:** Audit event `principal_update` (allowed_models); updates the whitelist of models permitted for this principal QA restore: PUT the previous model list with the returned revision.
- **Expected UI:** Saves allowed_models array, toasts 'Allowed models updated', exits editing mode, renders badges for each model
- **Runtime result:** `PENDING`

### [UI-SRC-D74A22B571BC] Principals — Update Principal Default Limits

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PR-06`
- **Source:** `crates/cc-lb-admin/web/src/routes/principals.tsx:986#DefaultLimitsCard`
- **Preconditions:** Principal selected; editing === true; editRevision !== null; principalWritePending === false
- **Steps:** Click 'Edit' on Default Limits card → Add/modify/remove limits in LimitsEditor → Click 'Save'
- **Scope:** `each_principal` — GET /admin/v1/principals -> data.principals[].default_limits
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **Expected UI:** Saves limits array, toasts 'Default limits updated', renders formatted table with kind, window, and cap
- **Runtime result:** `PENDING`

### [UI-PR-06] Principals — Update Principal Default Limits — useUpdatePrincipalDefaultLimits

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-D74A22B571BC`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1289#useUpdatePrincipalDefaultLimits`
- **Preconditions:** Principal selected; editing === true; editRevision !== null; principalWritePending === false
- **Steps:** Click 'Edit' on Default Limits card → Add/modify/remove limits in LimitsEditor → Click 'Save'
- **Scope:** `each_principal` — GET /admin/v1/principals -> data.principals[].default_limits
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `PATCH /admin/v1/principals/{id}`
  - Query: None
  - Body: `default_limits`
  - Headers: `authorization`, `if-match`, `content-type`
- **Handler:** `update_principal`
- **Storage operations:** `sqlite:PrincipalStore::update:SqliteStorage::update`, `postgres:PrincipalStore::update:PostgresStorage::update`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Rebinds DynamicViewHolder; invalidates queryKey ['principals']
- **Side effects:** Audit event `principal_update`; partially patches limits, cache_keepalive, or allowed models QA restore: repeat the update with captured prior fields and the new revision.
- **Expected UI:** Saves limits array, toasts 'Default limits updated', renders formatted table with kind, window, and cap
- **Runtime result:** `PENDING`

### [UI-SRC-5EED9F4C1CB5] Principals — Recent Requests Principal Polling

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PR-24`
- **Source:** `crates/cc-lb-admin/web/src/routes/principals.tsx:596#RecentRequestsCard`
- **Preconditions:** Principal selected
- **Steps:** Mount PrincipalDetail pane for selected principal → Polls every 10,000ms
- **Scope:** `each_principal` — Selected principal ID
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Displays RequestEventsTable with columns for status, method, model, cost, tokens, time, limited to 5 rows
- **Runtime result:** `PENDING`

### [UI-PR-24] Principals — Recent Requests Principal Polling — useRecentEvents

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-5EED9F4C1CB5`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:476#useRecentEvents`
- **Preconditions:** Principal selected
- **Steps:** Mount PrincipalDetail pane for selected principal → Polls every 10,000ms
- **Scope:** `each_principal` — Selected principal ID
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/events/recent`
  - Query: `principal_id`, `limit`
  - Body: None
  - Headers: None
- **Handler:** `handle_recent_events`
- **Storage operations:** `sqlite:RequestEventStore::list_request_events:SqliteStorage::list_request_events`, `postgres:RequestEventStore::list_request_events:PostgresStorage::list_request_events`
- **Cache / no-query path:** Dynamic filter queries
- **Side effects:** None
- **Expected UI:** Displays RequestEventsTable with columns for status, method, model, cost, tokens, time, limited to 5 rows
- **Runtime result:** `PENDING`

### [UI-SRC-7FA58E7543A9] Principals — Load Router Plugin Chain

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PR-13A`
- **Source:** `crates/cc-lb-admin/web/src/routes/principals.tsx:1388#RouterSlotEditor`
- **Preconditions:** Principal selected
- **Steps:** Mount PrincipalDetail pane → RouterSlotEditor calls usePluginChain(principalId, 'router')
- **Scope:** `each_principal` — Selected principal ID
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Populates chain steps in Basic or Advanced tabs
- **Runtime result:** `PENDING`

### [UI-PR-13A] Principals — Load Router Plugin Chain — usePluginChain

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-7FA58E7543A9`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:454#usePluginChain`
- **Preconditions:** Principal selected
- **Steps:** Mount PrincipalDetail pane → RouterSlotEditor calls usePluginChain(principalId, 'router')
- **Scope:** `each_principal` — Selected principal ID
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/principals/{id}/plugin-chain`
  - Query: `slot`
  - Body: None
  - Headers: None
- **Handler:** `list_chain`
- **Storage operations:** `sqlite:PluginRegistryStore::list_chain_for_principal:SqliteStorage::list_chain_for_principal`, `postgres:PluginRegistryStore::list_chain_for_principal:PostgresStorage::list_chain_for_principal`
- **Cache / no-query path:** If query.slot is RuntimeOnly, returns empty list immediately without hitting DB
- **Side effects:** None
- **Expected UI:** Populates chain steps in Basic or Advanced tabs
- **Runtime result:** `PENDING`

### [UI-SRC-F380C424AEC6] Principals — Load Router Terminal Strategy

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PR-13B`
- **Source:** `crates/cc-lb-admin/web/src/routes/principals.tsx:1392#RouterSlotEditor`
- **Preconditions:** Principal selected
- **Steps:** Mount PrincipalDetail pane → RouterSlotEditor calls useRouterTerminalStrategy(principalId)
- **Scope:** `each_principal` — Selected principal ID
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Selects active radio option in TerminalStrategyRadioGroup
- **Runtime result:** `PENDING`

### [UI-PR-13B] Principals — Load Router Terminal Strategy — useRouterTerminalStrategy

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-F380C424AEC6`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:466#useRouterTerminalStrategy`
- **Preconditions:** Principal selected
- **Steps:** Mount PrincipalDetail pane → RouterSlotEditor calls useRouterTerminalStrategy(principalId)
- **Scope:** `each_principal` — Selected principal ID
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/principals/{id}/router-terminal`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `get_router_terminal`
- **Storage operations:** `sqlite:PrincipalStore::get_by_id:SqliteStorage::get_by_id`, `postgres:PrincipalStore::get_by_id:PostgresStorage::get_by_id`
- **Cache / no-query path:** Includes ETag header W/"{revision}"
- **Side effects:** None
- **Expected UI:** Selects active radio option in TerminalStrategyRadioGroup
- **Runtime result:** `PENDING`

### [UI-SRC-2C8DBDF42A9A] Principals — Router Editor Tab Switching

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/routes/principals.tsx:1424#RouterSlotEditor`
- **Preconditions:** RouterSlotEditor mounted
- **Steps:** Click 'Basic' or 'Advanced' tab
- **Scope:** `each_principal` — Tab UI
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** If complex chain, shows temporary notice 'Basic requires a router chain containing only subscription-preference' and remains on Advanced; otherwise switches tab view
- **Client-only reason:** Client state toggle activeTab
- **Runtime result:** `PENDING`

### [UI-SRC-72D80A37A7FC] Principals — Toggle Subscription Preference Plugin (Basic Tab)

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PR-15`, `UI-PR-15B`
- **Source:** `crates/cc-lb-admin/web/src/routes/principals.tsx:1414#RouterSlotEditor`
- **Preconditions:** Basic tab active; routerWriteBlocked === false
- **Steps:** In Basic tab, toggle 'Keep prompt cache warm by reusing upstreams' switch
- **Scope:** `each_principal` — Selected principal ID
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **Expected UI:** Switch indicates progress ('Turning on...' / 'Turning off...'), updates state once mutation succeeds
- **Runtime result:** `PENDING`

### [UI-PR-15] Principals — Toggle Subscription Preference Plugin (Basic Tab) — useInsertChainEntry

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-72D80A37A7FC`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1514#useInsertChainEntry`
- **Preconditions:** Basic tab active; routerWriteBlocked === false
- **Steps:** In Basic tab, toggle 'Keep prompt cache warm by reusing upstreams' switch
- **Scope:** `each_principal` — Selected principal ID
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/v1/principals/{id}/plugin-chain`
  - Query: None
  - Body: `slot`, `wasm_registry_id`, `order`
  - Headers: `authorization`, `content-type`
- **Handler:** `insert_chain`
- **Storage operations:** `sqlite:PluginRegistryStore::insert_chain_entry:SqliteStorage::insert_chain_entry`, `postgres:PluginRegistryStore::insert_chain_entry:PostgresStorage::insert_chain_entry`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Invalidates TanStack queryKey ['plugin-chain', principal_id]; triggers dynamic rebind headers
- **Side effects:** Inserts row into plugin_chains_v2; verifies principal exists and singleton slot constraint; records audit event 'plugin_chain_insert'; sends pg_notify 'cclb_plugin_changed' in Postgres; adds dynamic rebind headers; sets Location header QA restore: DELETE /admin/v1/plugin-chain-entries/{id} using returned entry ID and revision.
- **Expected UI:** Switch indicates progress ('Turning on...' / 'Turning off...'), updates state once mutation succeeds
- **Runtime result:** `PENDING`

### [UI-PR-15B] Principals — Toggle Subscription Preference Plugin (Basic Tab) — useDeleteChainEntry

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-72D80A37A7FC`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1555#useDeleteChainEntry`
- **Preconditions:** Basic tab active; routerWriteBlocked === false
- **Steps:** In Basic tab, toggle 'Keep prompt cache warm by reusing upstreams' switch
- **Scope:** `each_principal` — Selected principal ID
- **Risk:** `destructive_write`
- **Production applicability:** `available`
- **HTTP:** `DELETE /admin/v1/plugin-chain-entries/{id}`
  - Query: None
  - Body: None
  - Headers: `authorization`, `if-match`
- **Handler:** `delete_chain`
- **Storage operations:** `sqlite:PluginRegistryStore::delete_chain_entry:SqliteStorage::delete_chain_entry`, `postgres:PluginRegistryStore::delete_chain_entry:PostgresStorage::delete_chain_entry`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Invalidates TanStack queryKey ['plugin-chain']; triggers dynamic rebind headers
- **Side effects:** Deletes row from plugin_chains_v2; records audit event 'plugin_chain_delete'; sends pg_notify 'cclb_plugin_changed' in Postgres; adds dynamic rebind headers QA restore: POST /admin/v1/principals/{principal_id}/plugin-chain with original parameters to recreate the chain entry.
- **Expected UI:** Switch indicates progress ('Turning on...' / 'Turning off...'), updates state once mutation succeeds
- **Runtime result:** `PENDING`

### [UI-SRC-12BD8F041AC6] Principals — Change Router Terminal Strategy

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PR-14`
- **Source:** `crates/cc-lb-admin/web/src/routes/principals.tsx:1420#RouterSlotEditor`
- **Preconditions:** terminalStrategy.data exists; routerWriteBlocked === false
- **Steps:** Click a radio card in TerminalStrategyRadioGroup ('first-pick', 'round-robin', 'least-latency', 'cheapest')
- **Scope:** `each_principal` — Selected principal ID
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **Expected UI:** Card shows active border and spinner during mutation, status message 'Updating strategy...', toast 'Terminal strategy updated'
- **Runtime result:** `PENDING`

### [UI-PR-14] Principals — Change Router Terminal Strategy — useUpdateRouterTerminalStrategy

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-12BD8F041AC6`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1563#useUpdateRouterTerminalStrategy`
- **Preconditions:** terminalStrategy.data exists; routerWriteBlocked === false
- **Steps:** Click a radio card in TerminalStrategyRadioGroup ('first-pick', 'round-robin', 'least-latency', 'cheapest')
- **Scope:** `each_principal` — Selected principal ID
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `PUT /admin/v1/principals/{id}/router-terminal`
  - Query: None
  - Body: `strategy`
  - Headers: `authorization`, `if-match`, `content-type`
- **Handler:** `update_router_terminal`
- **Storage operations:** `sqlite:PrincipalStore::update:SqliteStorage::update`, `postgres:PrincipalStore::update:PostgresStorage::update`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Rebinds DynamicViewHolder; invalidates queryKey ['router-terminal', vars.id]
- **Side effects:** Audit event `principal_update` (router_terminal_strategy); changes routing terminal strategy (first-pick, round-robin, least-tokens, etc.) QA restore: PUT the previous strategy with the returned revision.
- **Expected UI:** Card shows active border and spinner during mutation, status message 'Updating strategy...', toast 'Terminal strategy updated'
- **Runtime result:** `PENDING`

### [UI-SRC-D4D3CDAFC561] Principals — Add Router Filter from Popover (Advanced Tab)

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PR-16B`
- **Source:** `crates/cc-lb-admin/web/src/routes/principals.tsx:1445#RouterSlotEditor`
- **Preconditions:** Advanced tab active; routerWriteBlocked === false; plugin not already in chain
- **Steps:** Click '+ Add filter' trigger → Select a plugin from the popover list
- **Scope:** `each_principal` — GET /admin/v1/plugins/registry
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **Expected UI:** Popover closes, popover trigger shows 'Adding...', new step appends before terminal step
- **Runtime result:** `PENDING`

### [UI-PR-16B] Principals — Add Router Filter from Popover (Advanced Tab) — useInsertChainEntry

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-D4D3CDAFC561`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1514#useInsertChainEntry`
- **Preconditions:** Advanced tab active; routerWriteBlocked === false; plugin not already in chain
- **Steps:** Click '+ Add filter' trigger → Select a plugin from the popover list
- **Scope:** `each_principal` — GET /admin/v1/plugins/registry
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/v1/principals/{id}/plugin-chain`
  - Query: None
  - Body: `slot`, `wasm_registry_id`, `order`
  - Headers: `authorization`, `content-type`
- **Handler:** `insert_chain`
- **Storage operations:** `sqlite:PluginRegistryStore::insert_chain_entry:SqliteStorage::insert_chain_entry`, `postgres:PluginRegistryStore::insert_chain_entry:PostgresStorage::insert_chain_entry`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Invalidates TanStack queryKey ['plugin-chain', principal_id]; triggers dynamic rebind headers
- **Side effects:** Inserts row into plugin_chains_v2; verifies principal exists and singleton slot constraint; records audit event 'plugin_chain_insert'; sends pg_notify 'cclb_plugin_changed' in Postgres; adds dynamic rebind headers; sets Location header QA restore: DELETE /admin/v1/plugin-chain-entries/{id} using returned entry ID and revision.
- **Expected UI:** Popover closes, popover trigger shows 'Adding...', new step appends before terminal step
- **Runtime result:** `PENDING`

### [UI-SRC-C4E25AD91ABF] Principals — Reorder Router Filters via Move Up/Down

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PR-16`
- **Source:** `crates/cc-lb-admin/web/src/routes/principals.tsx:1431#RouterSlotEditor`
- **Preconditions:** index > 0 for up, index < entries.length - 1 for down; routerWriteBlocked === false
- **Steps:** Click 'Move filter up' or 'Move filter down' arrow button on a filter step
- **Scope:** `each_principal` — Router filter entries
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **Expected UI:** Steps visually swap positions using FLIP animation, status indicator 'Reordering steps...'
- **Runtime result:** `PENDING`

### [UI-PR-16] Principals — Reorder Router Filters via Move Up/Down — useReorderChain

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-C4E25AD91ABF`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1540#useReorderChain`
- **Preconditions:** index > 0 for up, index < entries.length - 1 for down; routerWriteBlocked === false
- **Steps:** Click 'Move filter up' or 'Move filter down' arrow button on a filter step
- **Scope:** `each_principal` — Router filter entries
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/v1/principals/{id}/plugin-chain/reorder`
  - Query: None
  - Body: `entries`
  - Headers: `authorization`, `content-type`
- **Handler:** `reorder_chain`
- **Storage operations:** `sqlite:PluginRegistryStore::reorder_chain:SqliteStorage::reorder_chain`, `postgres:PluginRegistryStore::reorder_chain:PostgresStorage::reorder_chain`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Invalidates TanStack queryKey ['plugin-chain', principal_id]; triggers dynamic rebind headers
- **Side effects:** Updates order_value and increments revision for chain entries; checks order gap >= 2; records audit event 'plugin_chain_reorder'; sends pg_notify 'cclb_plugin_changed' in Postgres; adds dynamic rebind headers QA restore: POST /admin/v1/principals/{principal_id}/plugin-chain/reorder with prior order values and updated revisions.
- **Expected UI:** Steps visually swap positions using FLIP animation, status indicator 'Reordering steps...'
- **Runtime result:** `PENDING`

### [UI-SRC-797468122201] Principals — Remove Router Filter Step

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PR-16C`
- **Source:** `crates/cc-lb-admin/web/src/routes/principals.tsx:1449#RouterSlotEditor`
- **Preconditions:** routerWriteBlocked === false
- **Steps:** Click Trash icon on a filter step
- **Scope:** `each_principal` — Router filter entries
- **Risk:** `destructive_write`
- **Production applicability:** `available`
- **Expected UI:** Filter row displays spinner, gets removed from list, connector lines adjust
- **Runtime result:** `PENDING`

### [UI-PR-16C] Principals — Remove Router Filter Step — useDeleteChainEntry

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-797468122201`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1555#useDeleteChainEntry`
- **Preconditions:** routerWriteBlocked === false
- **Steps:** Click Trash icon on a filter step
- **Scope:** `each_principal` — Router filter entries
- **Risk:** `destructive_write`
- **Production applicability:** `available`
- **HTTP:** `DELETE /admin/v1/plugin-chain-entries/{id}`
  - Query: None
  - Body: None
  - Headers: `authorization`, `if-match`
- **Handler:** `delete_chain`
- **Storage operations:** `sqlite:PluginRegistryStore::delete_chain_entry:SqliteStorage::delete_chain_entry`, `postgres:PluginRegistryStore::delete_chain_entry:PostgresStorage::delete_chain_entry`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Invalidates TanStack queryKey ['plugin-chain']; triggers dynamic rebind headers
- **Side effects:** Deletes row from plugin_chains_v2; records audit event 'plugin_chain_delete'; sends pg_notify 'cclb_plugin_changed' in Postgres; adds dynamic rebind headers QA restore: POST /admin/v1/principals/{principal_id}/plugin-chain with original parameters to recreate the chain entry.
- **Expected UI:** Filter row displays spinner, gets removed from list, connector lines adjust
- **Runtime result:** `PENDING`

### [UI-SRC-9E71353DC28E] Principals — View Plugin Details Drawer

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/routes/principals.tsx:1129#PluginDetailDrawer`
- **Preconditions:** Filter entry exists and is associated with registry plugin
- **Steps:** Click filter name button in router chain
- **Scope:** `once` — Router filter entry wasm_registry_id
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Slide-over drawer opens showing plugin title, slot badges, markdown/prose sections, sha256 hash, and wire version
- **Client-only reason:** Renders detail view from already loaded plugin registry query data
- **Runtime result:** `PENDING`

### [UI-PR-18] Principals — Select Shape Slot Plugin

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PR-18A`, `UI-PR-18B`
- **Source:** `crates/cc-lb-admin/web/src/routes/principals.tsx:2003#ShapeSlotEditor`
- **Preconditions:** shapeWriteBlocked === false; pluginId !== currentPluginId
- **Steps:** Click a radio card in Shape slot editor ('None' or any compatible plugin)
- **Scope:** `each_principal` — GET /admin/v1/plugins/registry filtered by pluginSupportsSlot(p, 'shape')
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **Expected UI:** Card radio shows spinner, deletes previous entries sequentially, inserts new entry, toasts success
- **Runtime result:** `PENDING`

### [UI-PR-18A] Principals — Select Shape Slot Plugin — useDeleteChainEntry

- **Entry type:** `network_request`
- **Parent action:** `UI-PR-18`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1555#useDeleteChainEntry`
- **Preconditions:** shapeWriteBlocked === false; pluginId !== currentPluginId
- **Steps:** Click a radio card in Shape slot editor ('None' or any compatible plugin)
- **Scope:** `each_principal` — GET /admin/v1/plugins/registry filtered by pluginSupportsSlot(p, 'shape')
- **Risk:** `destructive_write`
- **Production applicability:** `available`
- **HTTP:** `DELETE /admin/v1/plugin-chain-entries/{id}`
  - Query: None
  - Body: None
  - Headers: `authorization`, `if-match`
- **Handler:** `delete_chain`
- **Storage operations:** `sqlite:PluginRegistryStore::delete_chain_entry:SqliteStorage::delete_chain_entry`, `postgres:PluginRegistryStore::delete_chain_entry:PostgresStorage::delete_chain_entry`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Invalidates TanStack queryKey ['plugin-chain']; triggers dynamic rebind headers
- **Side effects:** Deletes row from plugin_chains_v2; records audit event 'plugin_chain_delete'; sends pg_notify 'cclb_plugin_changed' in Postgres; adds dynamic rebind headers QA restore: POST /admin/v1/principals/{principal_id}/plugin-chain with original parameters to recreate the chain entry.
- **Expected UI:** Card radio shows spinner, deletes previous entries sequentially, inserts new entry, toasts success
- **Runtime result:** `PENDING`

### [UI-PR-18B] Principals — Select Shape Slot Plugin — useInsertChainEntry

- **Entry type:** `network_request`
- **Parent action:** `UI-PR-18`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1514#useInsertChainEntry`
- **Preconditions:** shapeWriteBlocked === false; pluginId !== currentPluginId
- **Steps:** Click a radio card in Shape slot editor ('None' or any compatible plugin)
- **Scope:** `each_principal` — GET /admin/v1/plugins/registry filtered by pluginSupportsSlot(p, 'shape')
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/v1/principals/{id}/plugin-chain`
  - Query: None
  - Body: `slot`, `wasm_registry_id`, `order`
  - Headers: `authorization`, `content-type`
- **Handler:** `insert_chain`
- **Storage operations:** `sqlite:PluginRegistryStore::insert_chain_entry:SqliteStorage::insert_chain_entry`, `postgres:PluginRegistryStore::insert_chain_entry:PostgresStorage::insert_chain_entry`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Invalidates TanStack queryKey ['plugin-chain', principal_id]; triggers dynamic rebind headers
- **Side effects:** Inserts row into plugin_chains_v2; verifies principal exists and singleton slot constraint; records audit event 'plugin_chain_insert'; sends pg_notify 'cclb_plugin_changed' in Postgres; adds dynamic rebind headers; sets Location header QA restore: DELETE /admin/v1/plugin-chain-entries/{id} using returned entry ID and revision.
- **Expected UI:** Card radio shows spinner, deletes previous entries sequentially, inserts new entry, toasts success
- **Runtime result:** `PENDING`

### [UI-SRC-4A31DF64FCC2] Principals — Reorder Observability Hooks via Drag-and-Drop

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PR-19B`
- **Source:** `crates/cc-lb-admin/web/src/routes/principals.tsx:2154#ObservabilityHookEditor`
- **Preconditions:** chainBusy === false; entries.length > 1
- **Steps:** Drag an observability hook item and drop at a new index
- **Scope:** `each_principal` — Observability hook chain entries
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **Expected UI:** Items reorder visually via SortableContext, header displays 'Saving order...', toast confirms 'Chain reordered'
- **Runtime result:** `PENDING`

### [UI-PR-19B] Principals — Reorder Observability Hooks via Drag-and-Drop — useReorderChain

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-4A31DF64FCC2`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1540#useReorderChain`
- **Preconditions:** chainBusy === false; entries.length > 1
- **Steps:** Drag an observability hook item and drop at a new index
- **Scope:** `each_principal` — Observability hook chain entries
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/v1/principals/{id}/plugin-chain/reorder`
  - Query: None
  - Body: `entries`
  - Headers: `authorization`, `content-type`
- **Handler:** `reorder_chain`
- **Storage operations:** `sqlite:PluginRegistryStore::reorder_chain:SqliteStorage::reorder_chain`, `postgres:PluginRegistryStore::reorder_chain:PostgresStorage::reorder_chain`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Invalidates TanStack queryKey ['plugin-chain', principal_id]; triggers dynamic rebind headers
- **Side effects:** Updates order_value and increments revision for chain entries; checks order gap >= 2; records audit event 'plugin_chain_reorder'; sends pg_notify 'cclb_plugin_changed' in Postgres; adds dynamic rebind headers QA restore: POST /admin/v1/principals/{principal_id}/plugin-chain/reorder with prior order values and updated revisions.
- **Expected UI:** Items reorder visually via SortableContext, header displays 'Saving order...', toast confirms 'Chain reordered'
- **Runtime result:** `PENDING`

### [UI-SRC-8AE44ADD2C3E] Principals — Remove Observability Hook

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PR-19C`
- **Source:** `crates/cc-lb-admin/web/src/routes/principals.tsx:2212#ObservabilityHookEditor`
- **Preconditions:** chainBusy === false
- **Steps:** Click Trash icon on hook item → Confirm in removal modal
- **Scope:** `each_principal` — Observability hook chain entries
- **Risk:** `destructive_write`
- **Production applicability:** `available`
- **Expected UI:** Hook item is deleted from chain, list updates, empty dashed CTA shown if count reaches 0
- **Runtime result:** `PENDING`

### [UI-PR-19C] Principals — Remove Observability Hook — useDeleteChainEntry

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-8AE44ADD2C3E`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1555#useDeleteChainEntry`
- **Preconditions:** chainBusy === false
- **Steps:** Click Trash icon on hook item → Confirm in removal modal
- **Scope:** `each_principal` — Observability hook chain entries
- **Risk:** `destructive_write`
- **Production applicability:** `available`
- **HTTP:** `DELETE /admin/v1/plugin-chain-entries/{id}`
  - Query: None
  - Body: None
  - Headers: `authorization`, `if-match`
- **Handler:** `delete_chain`
- **Storage operations:** `sqlite:PluginRegistryStore::delete_chain_entry:SqliteStorage::delete_chain_entry`, `postgres:PluginRegistryStore::delete_chain_entry:PostgresStorage::delete_chain_entry`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Invalidates TanStack queryKey ['plugin-chain']; triggers dynamic rebind headers
- **Side effects:** Deletes row from plugin_chains_v2; records audit event 'plugin_chain_delete'; sends pg_notify 'cclb_plugin_changed' in Postgres; adds dynamic rebind headers QA restore: POST /admin/v1/principals/{principal_id}/plugin-chain with original parameters to recreate the chain entry.
- **Expected UI:** Hook item is deleted from chain, list updates, empty dashed CTA shown if count reaches 0
- **Runtime result:** `PENDING`

### [UI-SRC-5DE97D4F1CFC] Principals — Load Principal API Keys

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PR-20`
- **Source:** `crates/cc-lb-admin/web/src/routes/principals.tsx:2427#ApiKeysCard`
- **Preconditions:** Principal selected
- **Steps:** Mount PrincipalDetail pane → ApiKeysCard calls usePrincipalKeys(principal.id)
- **Scope:** `each_principal` — Selected principal ID
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Displays table with columns: Label, Key ID, Last 4, Issued, Last Used, Status, and Revoke action button
- **Runtime result:** `PENDING`

### [UI-PR-20] Principals — Load Principal API Keys — usePrincipalKeys

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-5DE97D4F1CFC`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:441#usePrincipalKeys`
- **Preconditions:** Principal selected
- **Steps:** Mount PrincipalDetail pane → ApiKeysCard calls usePrincipalKeys(principal.id)
- **Scope:** `each_principal` — Selected principal ID
- **Risk:** `read_with_audit`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/principals/{id}/keys`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `list_keys`
- **Storage operations:** `sqlite:ManagedKeyStore::list_all:SqliteStorage::list_all`, `postgres:ManagedKeyStore::list_all:PostgresManagedKeyStore::list_all`, `sqlite:RequestEventStore::request_event_key_last_used:SqliteStorage::request_event_key_last_used`, `postgres:RequestEventStore::request_event_key_last_used:PostgresStorage::request_event_key_last_used`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Lists all managed keys; optionally enriches last-used timestamps from request events when storage is attached; appends an audit record. Key labels serialize as Option<String>: stored empty-string labels (legacy rows) are returned as JSON null (crates/cc-lb-admin/src/v1/keys.rs:268-272).
- **Side effects:** Audit log write: record_admin_audit("principal_keys_list", "/admin/v1/principals/{id}/keys")
- **Expected UI:** Displays table with columns: Label, Key ID, Last 4, Issued, Last Used, Status, and Revoke action button
- **Runtime result:** `PENDING`

### [UI-SRC-911C0971EF03] Principals — Issue New API Key

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PR-21`
- **Source:** `crates/cc-lb-admin/web/src/routes/principals.tsx:2439#ApiKeysCard`
- **Preconditions:** issueInFlight.current === false; issuePending === false
- **Steps:** Click 'Issue Key' button → Optionally enter Label in modal → Click 'Issue' button
- **Scope:** `each_principal` — Selected principal ID
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **Expected UI:** Button shows 'Issuing...', modal transitions to display plaintext_key with Copy button; modal cannot be dismissed by backdrop or Escape until operator clicks Done
- **Runtime result:** `PENDING`

### [UI-PR-21] Principals — Issue New API Key — useIssueKey

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-911C0971EF03`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1337#useIssueKey`
- **Preconditions:** issueInFlight.current === false; issuePending === false
- **Steps:** Click 'Issue Key' button → Optionally enter Label in modal → Click 'Issue' button
- **Scope:** `each_principal` — Selected principal ID
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/v1/principals/{id}/keys`
  - Query: None
  - Body: `label`
  - Headers: `authorization`, `content-type`
- **Handler:** `issue_key`
- **Storage operations:** `sqlite:ManagedKeyStore::issue:SqliteStorage::issue`, `postgres:ManagedKeyStore::issue:PostgresManagedKeyStore::issue`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`, `sqlite:ManagedKeyStore::revoke_zero_secrets:SqliteStorage::revoke_zero_secrets`, `postgres:ManagedKeyStore::revoke_zero_secrets:PostgresManagedKeyStore::revoke_zero_secrets`
- **Cache / no-query path:** Populates KeyStore fast-path memory lookup; invalidates queryKey ['principals', 'keys', id]
- **Side effects:** Audit event `principal_key_issue`; returns plaintext key ONCE to caller; if audit write fails, automatically compensates by invoking `key_store.revoke()` to destroy the key StorageError::InvalidInput (for example a non-empty label that fails identifier validation) returns 400 invalid_input; other errors return 500 internal_error. QA restore: Revoke the newly created key via POST /admin/v1/principals/{id}/keys/{key_id}/revoke.
- **Expected UI:** Button shows 'Issuing...', modal transitions to display plaintext_key with Copy button; modal cannot be dismissed by backdrop or Escape until operator clicks Done
- **Runtime result:** `PENDING`

### [UI-SRC-B4272A92476E] Principals — Revoke API Key

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PR-22`
- **Source:** `crates/cc-lb-admin/web/src/routes/principals.tsx:2444#ApiKeysCard`
- **Preconditions:** Key is active (revoked_at_unix_secs === null); revokeInFlight.current === false
- **Steps:** Click Trash icon on an active API key row → Confirm in Revoke API key? dialog
- **Scope:** `each_key` — GET /admin/v1/principals/${id}/keys -> data.keys[]
- **Risk:** `destructive_write`
- **Production applicability:** `available`
- **Expected UI:** Dialog closes, toast 'Key revoked', row status badge changes to 'Revoked' tone=danger
- **Runtime result:** `PENDING`

### [UI-PR-22] Principals — Revoke API Key — useRevokeKey

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-B4272A92476E`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1349#useRevokeKey`
- **Preconditions:** Key is active (revoked_at_unix_secs === null); revokeInFlight.current === false
- **Steps:** Click Trash icon on an active API key row → Confirm in Revoke API key? dialog
- **Scope:** `each_key` — GET /admin/v1/principals/${id}/keys -> data.keys[]
- **Risk:** `destructive_write`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/v1/principals/{id}/keys/{key_id}/revoke`
  - Query: None
  - Body: None
  - Headers: `authorization`
- **Handler:** `revoke_key`
- **Storage operations:** `sqlite:ManagedKeyStore::revoke_zero_secrets:SqliteStorage::revoke_zero_secrets`, `postgres:ManagedKeyStore::revoke_zero_secrets:PostgresManagedKeyStore::revoke_zero_secrets`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Removes from in-memory key router; invalidates queryKey ['principals', 'keys', vars.id]
- **Side effects:** Audit event `principal_key_revoke`; zeroes key salt/hashes and marks revoked; irreversible QA restore: Re-issue key or restore database snapshot (revocation zeroes secrets permanently).
- **Expected UI:** Dialog closes, toast 'Key revoked', row status badge changes to 'Revoked' tone=danger
- **Runtime result:** `PENDING`

### [UI-SRC-A3E7AE6357CE] Principals — Cache Keepalive Card Summary Polling (limit=0)

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PR-07`
- **Source:** `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveCard.tsx:143#CacheKeepaliveCard`
- **Preconditions:** Principal selected
- **Steps:** Mount CacheKeepaliveCard for selected principal → Initial load and continuous polling every 5,000ms
- **Scope:** `each_principal` — Selected principal ID
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Card displays 4 metric tiles: Renewing now, Sessions (last 5m), Renewals fired, and Cost saved. Values flash when updated.
- **Runtime result:** `PENDING`

### [UI-PR-07] Principals — Cache Keepalive Card Summary Polling (limit=0) — useCacheKeepaliveSummary

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-A3E7AE6357CE`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1830#useCacheKeepaliveSummary`
- **Preconditions:** Principal selected
- **Steps:** Mount CacheKeepaliveCard for selected principal → Initial load and continuous polling every 5,000ms
- **Scope:** `each_principal` — Selected principal ID
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/principals/{id}/cache-keepalive`
  - Query: `limit`
  - Body: None
  - Headers: None
- **Handler:** `list_cache_keepalive`
- **Storage operations:** `sqlite:PrincipalStore::get_by_id:SqliteStorage::get_by_id`, `postgres:PrincipalStore::get_by_id:PostgresStorage::get_by_id`, `sqlite:CacheKeepaliveSessionReadStore::read_cache_keepalive_summary_input:SqliteStorage::read_cache_keepalive_summary_input`, `postgres:CacheKeepaliveSessionReadStore::read_cache_keepalive_summary_input:PostgresStorage::read_cache_keepalive_summary_input`, `sqlite:CacheKeepaliveSessionReadStore::list_cache_keepalive_sessions:SqliteStorage::list_cache_keepalive_sessions`, `postgres:CacheKeepaliveSessionReadStore::list_cache_keepalive_sessions:PostgresStorage::list_cache_keepalive_sessions`, `sqlite:CacheKeepaliveSessionReadStore::list_cache_keepalive_turns_for_sessions:SqliteStorage::list_cache_keepalive_turns_for_sessions`, `postgres:CacheKeepaliveSessionReadStore::list_cache_keepalive_turns_for_sessions:PostgresStorage::list_cache_keepalive_turns_for_sessions`, `sqlite:UpstreamStore::get_by_id:SqliteStorage::get_by_id`, `postgres:UpstreamStore::get_by_id:PostgresStorage::get_by_id`
- **Cache / no-query path:** Reads principal and summary inputs, then loads batched turns for summary sessions even when limit=0. limit=0 skips only the mixed page and upstream-name lookups; nonzero limits also read the session/decision page and distinct upstream names.
- **Side effects:** None
- **Expected UI:** Card displays 4 metric tiles: Renewing now, Sessions (last 5m), Renewals fired, and Cost saved. Values flash when updated.
- **Runtime result:** `PENDING`

### [UI-SRC-921EE69973B8] Principals — Toggle Principal Cache Keepalive Enabled

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PR-08`
- **Source:** `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveCard.tsx:150#CacheKeepaliveCard`
- **Preconditions:** toggleLocked === false (togglePending === false && principalWritePending === false)
- **Steps:** Click switch in CacheKeepaliveCard header
- **Scope:** `each_principal` — Selected principal ID
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **Expected UI:** Switch translates thumb (translate-x-4 vs translate-x-0.5) and turns green (var(--color-ok)), toast confirms enabled/disabled
- **Runtime result:** `PENDING`

### [UI-PR-08] Principals — Toggle Principal Cache Keepalive Enabled — useUpdatePrincipalCacheKeepalive

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-921EE69973B8`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1313#useUpdatePrincipalCacheKeepalive`
- **Preconditions:** toggleLocked === false (togglePending === false && principalWritePending === false)
- **Steps:** Click switch in CacheKeepaliveCard header
- **Scope:** `each_principal` — Selected principal ID
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `PATCH /admin/v1/principals/{id}`
  - Query: None
  - Body: `cache_keepalive`
  - Headers: `authorization`, `if-match`, `content-type`
- **Handler:** `update_principal`
- **Storage operations:** `sqlite:PrincipalStore::update:SqliteStorage::update`, `postgres:PrincipalStore::update:PostgresStorage::update`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Rebinds DynamicViewHolder; invalidates queryKey ['principals']
- **Side effects:** Audit event `principal_update`; partially patches limits, cache_keepalive, or allowed models QA restore: repeat the update with captured prior fields and the new revision.
- **Expected UI:** Switch translates thumb (translate-x-4 vs translate-x-0.5) and turns green (var(--color-ok)), toast confirms enabled/disabled
- **Runtime result:** `PENDING`

### [UI-PR-09A] Principals — Save Cache Keepalive Advanced Settings

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PR-09`
- **Source:** `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSettingsDrawer.tsx:128#CacheKeepaliveSettingsDrawer`
- **Preconditions:** busy === false; all numeric fields >= 0 and finite
- **Steps:** Click 'Settings' on card → Modify lead times, max renewals, max duration, snapshot bytes, extra tools, or ambiguous flag → Click 'Save changes'
- **Scope:** `each_principal` — Selected principal ID
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **Expected UI:** Button shows 'Saving...', drawer closes on success with toast 'Cache keepalive settings updated'; on 412/409 shows refresh toast
- **Runtime result:** `PENDING`

### [UI-PR-09] Principals — Save Cache Keepalive Advanced Settings — useUpdatePrincipalCacheKeepalive

- **Entry type:** `network_request`
- **Parent action:** `UI-PR-09A`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1313#useUpdatePrincipalCacheKeepalive`
- **Preconditions:** busy === false; all numeric fields >= 0 and finite
- **Steps:** Click 'Settings' on card → Modify lead times, max renewals, max duration, snapshot bytes, extra tools, or ambiguous flag → Click 'Save changes'
- **Scope:** `each_principal` — Selected principal ID
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `PATCH /admin/v1/principals/{id}`
  - Query: None
  - Body: `cache_keepalive`
  - Headers: `authorization`, `if-match`, `content-type`
- **Handler:** `update_principal`
- **Storage operations:** `sqlite:PrincipalStore::update:SqliteStorage::update`, `postgres:PrincipalStore::update:PostgresStorage::update`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Rebinds DynamicViewHolder; invalidates queryKey ['principals']
- **Side effects:** Audit event `principal_update`; partially patches limits, cache_keepalive, or allowed models QA restore: repeat the update with captured prior fields and the new revision.
- **Expected UI:** Button shows 'Saving...', drawer closes on success with toast 'Cache keepalive settings updated'; on 412/409 shows refresh toast
- **Runtime result:** `PENDING`

### [UI-PR-10] Principals — Cache Keepalive Sessions List Query & Polling (limit omitted)

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PR-10A`
- **Source:** `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx:395#CacheKeepaliveSessionsDrawer`
- **Preconditions:** open === true; principalId non-empty
- **Steps:** Click 'Sessions' button on CacheKeepaliveCard → Drawer opens and initializes infinite query → Polls every 5,000ms when tab is visible
- **Scope:** `each_principal` — Selected principal ID
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Drawer displays horizon toggle, overview strip, status filter bar, and session list with FLIP animation
- **Runtime result:** `PENDING`

### [UI-PR-10A] Principals — Cache Keepalive Sessions List Query & Polling (limit omitted) — useCacheKeepaliveSessions

- **Entry type:** `network_request`
- **Parent action:** `UI-PR-10`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1808#useCacheKeepaliveSessions`
- **Preconditions:** open === true; principalId non-empty
- **Steps:** Click 'Sessions' button on CacheKeepaliveCard → Drawer opens and initializes infinite query → Polls every 5,000ms when tab is visible
- **Scope:** `each_principal` — Selected principal ID
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/principals/{id}/cache-keepalive`
  - Query: `horizon`, `status`, `error`, `cursor`
  - Body: None
  - Headers: None
- **Handler:** `list_cache_keepalive`
- **Storage operations:** `sqlite:PrincipalStore::get_by_id:SqliteStorage::get_by_id`, `postgres:PrincipalStore::get_by_id:PostgresStorage::get_by_id`, `sqlite:CacheKeepaliveSessionReadStore::read_cache_keepalive_summary_input:SqliteStorage::read_cache_keepalive_summary_input`, `postgres:CacheKeepaliveSessionReadStore::read_cache_keepalive_summary_input:PostgresStorage::read_cache_keepalive_summary_input`, `sqlite:CacheKeepaliveSessionReadStore::list_cache_keepalive_sessions:SqliteStorage::list_cache_keepalive_sessions`, `postgres:CacheKeepaliveSessionReadStore::list_cache_keepalive_sessions:PostgresStorage::list_cache_keepalive_sessions`, `sqlite:CacheKeepaliveSessionReadStore::list_cache_keepalive_turns_for_sessions:SqliteStorage::list_cache_keepalive_turns_for_sessions`, `postgres:CacheKeepaliveSessionReadStore::list_cache_keepalive_turns_for_sessions:PostgresStorage::list_cache_keepalive_turns_for_sessions`, `sqlite:UpstreamStore::get_by_id:SqliteStorage::get_by_id`, `postgres:UpstreamStore::get_by_id:PostgresStorage::get_by_id`
- **Cache / no-query path:** Reads principal and summary inputs, then loads batched turns for summary sessions even when limit=0. limit=0 skips only the mixed page and upstream-name lookups; nonzero limits also read the session/decision page and distinct upstream names.
- **Side effects:** None
- **Expected UI:** Drawer displays horizon toggle, overview strip, status filter bar, and session list with FLIP animation
- **Runtime result:** `PENDING`

### [UI-SRC-88A40F094821] Principals — Horizon Toggle Selection & Cursor Invalidation

- **Entry type:** `ui_action`
- **Atomic requests:** `REQ-SRC-68354F29C6F2`
- **Source:** `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx:392#CacheKeepaliveSessionsDrawer`
- **Preconditions:** Drawer open
- **Steps:** Click '24h', '7d', or 'All' in HorizonToggle
- **Scope:** `each_principal` — Horizon toggle buttons
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Button becomes active, overview strip text updates ('Last 24h:', 'Last 7d:', 'All time:'), query key changes, pagination cursor resets to page 1
- **Runtime result:** `PENDING`

### [REQ-SRC-68354F29C6F2] Principals — Horizon Toggle Selection & Cursor Invalidation — useCacheKeepaliveSessions

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-88A40F094821`
- **Source:** `crates/cc-lb-admin/web/src/lib/cacheKeepaliveApi.ts:158#cacheKeepaliveSessionsPath`
- **Preconditions:** Drawer open
- **Steps:** Click '24h', '7d', or 'All' in HorizonToggle
- **Scope:** `each_principal` — Horizon toggle buttons
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/principals/{id}/cache-keepalive`
  - Query: `horizon`
  - Body: None
  - Headers: None
- **Handler:** `list_cache_keepalive`
- **Storage operations:** `sqlite:PrincipalStore::get_by_id:SqliteStorage::get_by_id`, `postgres:PrincipalStore::get_by_id:PostgresStorage::get_by_id`, `sqlite:CacheKeepaliveSessionReadStore::read_cache_keepalive_summary_input:SqliteStorage::read_cache_keepalive_summary_input`, `postgres:CacheKeepaliveSessionReadStore::read_cache_keepalive_summary_input:PostgresStorage::read_cache_keepalive_summary_input`, `sqlite:CacheKeepaliveSessionReadStore::list_cache_keepalive_sessions:SqliteStorage::list_cache_keepalive_sessions`, `postgres:CacheKeepaliveSessionReadStore::list_cache_keepalive_sessions:PostgresStorage::list_cache_keepalive_sessions`, `sqlite:CacheKeepaliveSessionReadStore::list_cache_keepalive_turns_for_sessions:SqliteStorage::list_cache_keepalive_turns_for_sessions`, `postgres:CacheKeepaliveSessionReadStore::list_cache_keepalive_turns_for_sessions:PostgresStorage::list_cache_keepalive_turns_for_sessions`, `sqlite:UpstreamStore::get_by_id:SqliteStorage::get_by_id`, `postgres:UpstreamStore::get_by_id:PostgresStorage::get_by_id`
- **Cache / no-query path:** Reads principal and summary inputs, then loads batched turns for summary sessions even when limit=0. limit=0 skips only the mixed page and upstream-name lookups; nonzero limits also read the session/decision page and distinct upstream names.
- **Side effects:** None
- **Expected UI:** Button becomes active, overview strip text updates ('Last 24h:', 'Last 7d:', 'All time:'), query key changes, pagination cursor resets to page 1
- **Runtime result:** `PENDING`

### [UI-SRC-9E180B20857B] Principals — Status Filter Selection (All / State / Error)

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PR-11`
- **Source:** `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx:391#CacheKeepaliveSessionsDrawer`
- **Preconditions:** Drawer open
- **Steps:** Click a status filter chip in the filter bar
- **Scope:** `each_principal` — FILTERS array in CacheKeepaliveSessionsDrawer.tsx
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Filter chip receives active ring and opacity-100, session list updates to show matching sessions
- **Runtime result:** `PENDING`

### [UI-PR-11] Principals — Status Filter Selection (All / State / Error) — useCacheKeepaliveSessions

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-9E180B20857B`
- **Source:** `crates/cc-lb-admin/web/src/lib/cacheKeepaliveApi.ts:164#cacheKeepaliveSessionsPath`
- **Preconditions:** Drawer open
- **Steps:** Click a status filter chip in the filter bar
- **Scope:** `each_principal` — FILTERS array in CacheKeepaliveSessionsDrawer.tsx
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/principals/{id}/cache-keepalive`
  - Query: `status`, `error`
  - Body: None
  - Headers: None
- **Handler:** `list_cache_keepalive`
- **Storage operations:** `sqlite:PrincipalStore::get_by_id:SqliteStorage::get_by_id`, `postgres:PrincipalStore::get_by_id:PostgresStorage::get_by_id`, `sqlite:CacheKeepaliveSessionReadStore::read_cache_keepalive_summary_input:SqliteStorage::read_cache_keepalive_summary_input`, `postgres:CacheKeepaliveSessionReadStore::read_cache_keepalive_summary_input:PostgresStorage::read_cache_keepalive_summary_input`, `sqlite:CacheKeepaliveSessionReadStore::list_cache_keepalive_sessions:SqliteStorage::list_cache_keepalive_sessions`, `postgres:CacheKeepaliveSessionReadStore::list_cache_keepalive_sessions:PostgresStorage::list_cache_keepalive_sessions`, `sqlite:CacheKeepaliveSessionReadStore::list_cache_keepalive_turns_for_sessions:SqliteStorage::list_cache_keepalive_turns_for_sessions`, `postgres:CacheKeepaliveSessionReadStore::list_cache_keepalive_turns_for_sessions:PostgresStorage::list_cache_keepalive_turns_for_sessions`, `sqlite:UpstreamStore::get_by_id:SqliteStorage::get_by_id`, `postgres:UpstreamStore::get_by_id:PostgresStorage::get_by_id`
- **Cache / no-query path:** Reads principal and summary inputs, then loads batched turns for summary sessions even when limit=0. limit=0 skips only the mixed page and upstream-name lookups; nonzero limits also read the session/decision page and distinct upstream names.
- **Side effects:** None
- **Expected UI:** Filter chip receives active ring and opacity-100, session list updates to show matching sessions
- **Runtime result:** `PENDING`

### [UI-SRC-B75FF5FEE2B9] Principals — Sessions Infinite Pagination via Cursor

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PR-11B`
- **Source:** `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/CacheKeepaliveSessionsDrawer.tsx:535#CacheKeepaliveSessionsDrawer`
- **Preconditions:** query.hasNextPage === true; query.isFetchingNextPage === false
- **Steps:** Scroll to bottom of sessions list → Click 'Loading older sessions...' button
- **Scope:** `each_principal` — Infinite query pages[].next_cursor
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Button displays spinner, appends older session rows to the bottom of the list without duplicates (mergeLiveSessions)
- **Runtime result:** `PENDING`

### [UI-PR-11B] Principals — Sessions Infinite Pagination via Cursor — fetchNextPage

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-B75FF5FEE2B9`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1808#useCacheKeepaliveSessions`
- **Preconditions:** query.hasNextPage === true; query.isFetchingNextPage === false
- **Steps:** Scroll to bottom of sessions list → Click 'Loading older sessions...' button
- **Scope:** `each_principal` — Infinite query pages[].next_cursor
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/principals/{id}/cache-keepalive`
  - Query: `cursor`
  - Body: None
  - Headers: None
- **Handler:** `list_cache_keepalive`
- **Storage operations:** `sqlite:PrincipalStore::get_by_id:SqliteStorage::get_by_id`, `postgres:PrincipalStore::get_by_id:PostgresStorage::get_by_id`, `sqlite:CacheKeepaliveSessionReadStore::read_cache_keepalive_summary_input:SqliteStorage::read_cache_keepalive_summary_input`, `postgres:CacheKeepaliveSessionReadStore::read_cache_keepalive_summary_input:PostgresStorage::read_cache_keepalive_summary_input`, `sqlite:CacheKeepaliveSessionReadStore::list_cache_keepalive_sessions:SqliteStorage::list_cache_keepalive_sessions`, `postgres:CacheKeepaliveSessionReadStore::list_cache_keepalive_sessions:PostgresStorage::list_cache_keepalive_sessions`, `sqlite:CacheKeepaliveSessionReadStore::list_cache_keepalive_turns_for_sessions:SqliteStorage::list_cache_keepalive_turns_for_sessions`, `postgres:CacheKeepaliveSessionReadStore::list_cache_keepalive_turns_for_sessions:PostgresStorage::list_cache_keepalive_turns_for_sessions`, `sqlite:UpstreamStore::get_by_id:SqliteStorage::get_by_id`, `postgres:UpstreamStore::get_by_id:PostgresStorage::get_by_id`
- **Cache / no-query path:** Reads principal and summary inputs, then loads batched turns for summary sessions even when limit=0. limit=0 skips only the mixed page and upstream-name lookups; nonzero limits also read the session/decision page and distinct upstream names.
- **Side effects:** None
- **Expected UI:** Button displays spinner, appends older session rows to the bottom of the list without duplicates (mergeLiveSessions)
- **Runtime result:** `PENDING`

### [UI-SRC-BACAC4FD9C6A] Principals — Cache Keepalive Session Detail Polling

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PR-12`
- **Source:** `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/SessionDetailPane.tsx:234#SessionDetailPane`
- **Preconditions:** Session selected (sessionId !== null)
- **Steps:** Click a session row in CacheKeepaliveSessionsDrawer → SessionDetailPane mounts and polls every 5,000ms
- **Scope:** `each_session` — GET /admin/v1/principals/${id}/cache-keepalive -> rows[].id
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Displays Net P&L (+/-, avoided cache creation vs renewal spend), metadata list, turn-by-turn history with live/final turn tags and lock icons, collapsible schedule-time config snapshot, and raw JSON record
- **Runtime result:** `PENDING`

### [UI-PR-12] Principals — Cache Keepalive Session Detail Polling — useCacheKeepaliveSessionDetail

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-BACAC4FD9C6A`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1842#useCacheKeepaliveSessionDetail`
- **Preconditions:** Session selected (sessionId !== null)
- **Steps:** Click a session row in CacheKeepaliveSessionsDrawer → SessionDetailPane mounts and polls every 5,000ms
- **Scope:** `each_session` — GET /admin/v1/principals/${id}/cache-keepalive -> rows[].id
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/principals/{id}/cache-keepalive/{session_id}`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `get_cache_keepalive_detail`
- **Storage operations:** `sqlite:PrincipalStore::get_by_id:SqliteStorage::get_by_id`, `postgres:PrincipalStore::get_by_id:PostgresStorage::get_by_id`, `sqlite:CacheKeepaliveSessionReadStore::get_cache_keepalive_list_item:SqliteStorage::get_cache_keepalive_list_item`, `postgres:CacheKeepaliveSessionReadStore::get_cache_keepalive_list_item:PostgresStorage::get_cache_keepalive_list_item`, `sqlite:CacheKeepaliveSessionReadStore::get_cache_keepalive_session_for_principal:SqliteStorage::get_cache_keepalive_session_for_principal`, `postgres:CacheKeepaliveSessionReadStore::get_cache_keepalive_session_for_principal:PostgresStorage::get_cache_keepalive_session_for_principal`, `sqlite:CacheKeepaliveSessionReadStore::list_cache_keepalive_turns:SqliteStorage::list_cache_keepalive_turns`, `postgres:CacheKeepaliveSessionReadStore::list_cache_keepalive_turns:PostgresStorage::list_cache_keepalive_turns`, `sqlite:UpstreamStore::get_by_id:SqliteStorage::get_by_id`, `postgres:UpstreamStore::get_by_id:PostgresStorage::get_by_id`
- **Cache / no-query path:** Reads principal and a mixed session/decision item. Session items additionally read the encrypted session record and turns; all items read the upstream name.
- **Side effects:** None
- **Expected UI:** Displays Net P&L (+/-, avoided cache creation vs renewal spend), metadata list, turn-by-turn history with live/final turn tags and lock icons, collapsible schedule-time config snapshot, and raw JSON record
- **Runtime result:** `PENDING`

### [UI-SRC-2DEF2F42BB04] Principals — Session Detail Collapsible Sections

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/principals/cache-keepalive/SessionDetailPane.tsx:500#SessionDetailPane`
- **Preconditions:** Session detail loaded
- **Steps:** Click 'Config in effect at schedule time' or 'Raw session record' chevron
- **Scope:** `each_session` — Session detail view
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Toggles visibility of snapshot definitions and formatted JSON payload
- **Client-only reason:** Client-side state toggles showConfig and showRaw
- **Runtime result:** `PENDING`

### [UI-SRC-D94B76752475] Plugins — Plugins Page Mount & Registry Fetch

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PLUG-01`
- **Source:** `crates/cc-lb-admin/web/src/components/plugins/PluginsPage.tsx:145#PluginsPage`
- **Preconditions:** User is authenticated with admin session
- **Steps:** Navigate to /plugins → Page mounts and triggers usePluginRegistry hook
- **Scope:** `once` — Page navigation
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Renders plugin catalog or detail view depending on search param
- **Runtime result:** `PENDING`

### [UI-PLUG-01] Plugins — Plugins Page Mount & Registry Fetch — usePluginRegistry

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-D94B76752475`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:448#usePluginRegistry`
- **Preconditions:** User is authenticated with admin session
- **Steps:** Navigate to /plugins → Page mounts and triggers usePluginRegistry hook
- **Scope:** `once` — Page navigation
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/plugins/registry`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `list_registry`
- **Storage operations:** `sqlite:PluginRegistryStore::list_registry:SqliteStorage::list_registry`, `postgres:PluginRegistryStore::list_registry:PostgresStorage::list_registry`, `sqlite:PluginRegistryStore::get_blob_bytes:SqliteStorage::get_blob_bytes`, `postgres:PluginRegistryStore::get_blob_bytes:PostgresStorage::get_blob_bytes`
- **Cache / no-query path:** Paginates registry rows; synthesizes the built-in subscription-preference entry when storage did not return it; sorts/pages the combined set and reads each page entry blob to calculate size_bytes (missing blob reports size 0).
- **Side effects:** None
- **Expected UI:** Renders plugin catalog or detail view depending on search param
- **Runtime result:** `PENDING`

### [UI-PLUG-05] Plugins — Catalog Row Inspect Click

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/plugins/PluginCatalog.tsx:267#PluginCatalog`
- **Preconditions:** At least one plugin exists in catalog
- **Steps:** Hover or navigate to plugin table row → Click 'Inspect' button in Actions column
- **Scope:** `each_row` — Each catalog entry row
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Transitions view from catalog to PluginDetail for the selected plugin ID
- **Client-only reason:** TanStack Router search param navigation: sets plugin ID in URL
- **Runtime result:** `PENDING`

### [UI-PLUG-11] Plugins — Catalog Row Copy SHA256

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/plugins/PluginCatalog.tsx:238#PluginCatalog`
- **Preconditions:** Plugin row is rendered
- **Steps:** Locate File hash column on a table row → Click copy icon button beside the truncated SHA256 hex
- **Scope:** `each_row` — Each catalog entry row
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Toast 'Copied SHA256 to clipboard'
- **Client-only reason:** Browser Clipboard API interaction via useCopyButton
- **Runtime result:** `PENDING`

### [UI-PLUG-10] Plugins — Catalog Row Delete Button Click

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/plugins/PluginCatalog.tsx:274#PluginCatalog`
- **Preconditions:** !plugin.is_builtin; !gcPending
- **Steps:** Locate non-built-in plugin row → Click trash icon button in Actions column
- **Scope:** `each_row` — Each non-built-in plugin row
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Opens PluginDeleteDialog with pendingDelete state set
- **Client-only reason:** Component local state: sets pendingDelete object
- **Runtime result:** `PENDING`

### [UI-SRC-42AEDACD7B73] Plugins — Garbage Collection (Clean Orphaned Uploads)

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PLUG-04`
- **Source:** `crates/cc-lb-admin/web/src/components/plugins/PluginCatalog.tsx:77#runGc`
- **Preconditions:** !gcPending; !gcInFlight.current
- **Steps:** Click 'Clean orphaned uploads' button in card header (enabled even when every registered plugin is in use)
- **Scope:** `once` — Catalog header action
- **Risk:** `destructive_write`
- **Production applicability:** `available`
- **Expected UI:** Header shows 'Cleaning orphaned uploads — waiting for the server.' status and 'Cleaning...' button; on completion toasts 'Removed {N} orphaned upload blob(s)' or 'No orphaned upload blobs found' and refreshes the registry
- **Runtime result:** `PENDING`

### [UI-PLUG-04] Plugins — Garbage Collection (Clean Orphaned Uploads) — useGcPlugins

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-42AEDACD7B73`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1503#useGcPlugins`
- **Preconditions:** !gcPending; !gcInFlight.current
- **Steps:** Click 'Clean orphaned uploads' button in card header (enabled even when every registered plugin is in use)
- **Scope:** `once` — Catalog header action
- **Risk:** `destructive_write`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/v1/plugins/wasm/gc`
  - Query: None
  - Body: None
  - Headers: `authorization`
- **Handler:** `gc_wasm`
- **Storage operations:** `sqlite:PluginRegistryStore::list_orphan_blobs:SqliteStorage::list_orphan_blobs`, `postgres:PluginRegistryStore::list_orphan_blobs:PostgresStorage::list_orphan_blobs`, `sqlite:PluginRegistryStore::decrement_blob_refcount_or_delete:SqliteStorage::decrement_blob_refcount_or_delete`, `postgres:PluginRegistryStore::decrement_blob_refcount_or_delete:PostgresStorage::decrement_blob_refcount_or_delete`
- **Cache / no-query path:** Removes orphan WASM cache files from disk cache ({data_dir}/plugins/wasm/cache/{sha256_hex}.wasm); invalidates TanStack queryKey ['plugins', 'registry']
- **Side effects:** Queries orphan wasm blobs (sha256 present in wasm_blobs_v2 but not referenced by any wasm_registry_v2 row) and deletes only those orphan blobs plus their disk cache files; registry-referenced blobs are preserved even when their refcount is 0; returns list of removed sha256 hex strings and count; no audit recorded QA restore: None possible without re-uploading the unreferenced binaries.
- **Expected UI:** Header shows 'Cleaning orphaned uploads — waiting for the server.' status and 'Cleaning...' button; on completion toasts 'Removed {N} orphaned upload blob(s)' or 'No orphaned upload blobs found' and refreshes the registry
- **Runtime result:** `PENDING`

### [UI-SRC-F8C17F59AB34] Plugins — WASM Plugin File Upload (Browse or Drop)

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PLUG-02`
- **Source:** `crates/cc-lb-admin/web/src/components/plugins/PluginUploadCard.tsx:54#handleFile`
- **Preconditions:** !upload.isPending
- **Steps:** Click dropzone button (or press Enter/Space while focused) to open file browser, OR drag & drop .wasm file onto dropzone → Select valid .wasm file (<= 32 MiB)
- **Scope:** `each_plugin` — Each plugin upload action
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **Expected UI:** Dropzone displays 'Uploading…' with pulsing icon while pending; transitions to detail view on success
- **Runtime result:** `PENDING`

### [UI-PLUG-02] Plugins — WASM Plugin File Upload (Browse or Drop) — useUploadWasm

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-F8C17F59AB34`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1368#useUploadWasm`
- **Preconditions:** !upload.isPending
- **Steps:** Click dropzone button (or press Enter/Space while focused) to open file browser, OR drag & drop .wasm file onto dropzone → Select valid .wasm file (<= 32 MiB)
- **Scope:** `each_plugin` — Each plugin upload action
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/v1/plugins/wasm`
  - Query: None
  - Body: `original_filename`, `bytes`
  - Headers: `authorization`, `content-type`
- **Handler:** `upload_wasm`
- **Storage operations:** `sqlite:PluginRegistryStore::persist_wasm_upload:SqliteStorage::persist_wasm_upload`, `postgres:PluginRegistryStore::persist_wasm_upload:PostgresStorage::persist_wasm_upload`, `sqlite:PluginRegistryStore::replace_wasm_entry:SqliteStorage::replace_wasm_entry`, `postgres:PluginRegistryStore::replace_wasm_entry:PostgresStorage::replace_wasm_entry`, `sqlite:PluginRegistryStore::update_supported_slots:SqliteStorage::update_supported_slots`, `postgres:PluginRegistryStore::update_supported_slots:PostgresStorage::update_supported_slots`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Writes compiled/validated WASM binary to disk cache ({data_dir}/plugins/wasm/cache/{sha256_hex}.wasm); if replaced, deletes old cache file; invalidates TanStack queryKey ['plugins', 'registry']
- **Side effects:** Stores binary in wasm_blobs_v2; creates or updates wasm_registry_v2 entry; validates WebAssembly header, exports, interface components; records audit event 'plugin_registry_upload' (or 'plugin_registry_upload_attempt' on failure); sends pg_notify 'cclb_plugin_changed' in Postgres; adds dynamic rebind headers; sets Location header on CREATED; sets X-Idempotent header QA restore: DELETE /admin/v1/plugins/registry/{id} if fresh upload; or re-upload previous WASM version if replaced.
- **Expected UI:** Dropzone displays 'Uploading…' with pulsing icon while pending; transitions to detail view on success
- **Runtime result:** `PENDING`

### [UI-SRC-19B510951607] Plugins — Confirm Plugin Replacement (409 Conflict Resolution)

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PLUG-03`
- **Source:** `crates/cc-lb-admin/web/src/components/plugins/PluginUploadCard.tsx:196#ConfirmDialog`
- **Preconditions:** Upload failed with 409 replacement_confirmation_required; pendingReplacement state contains replaceRegistryId and expectedRevision
- **Steps:** Trigger upload of a plugin whose name exists with lower/same semver → Observe 'Confirm Plugin Replacement' modal showing current vs incoming versions and hashes → Click 'Replace' button
- **Scope:** `each_plugin` — Each duplicate replacement conflict
- **Risk:** `destructive_write`
- **Production applicability:** `available`
- **Expected UI:** Toast 'Replaced {filename}' on success, closes modal, and navigates to replaced plugin detail view
- **Runtime result:** `PENDING`

### [UI-PLUG-03] Plugins — Confirm Plugin Replacement (409 Conflict Resolution) — useUploadWasm

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-19B510951607`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1368#useUploadWasm`
- **Preconditions:** Upload failed with 409 replacement_confirmation_required; pendingReplacement state contains replaceRegistryId and expectedRevision
- **Steps:** Trigger upload of a plugin whose name exists with lower/same semver → Observe 'Confirm Plugin Replacement' modal showing current vs incoming versions and hashes → Click 'Replace' button
- **Scope:** `each_plugin` — Each duplicate replacement conflict
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/v1/plugins/wasm`
  - Query: None
  - Body: `original_filename`, `bytes`, `confirm_replacement`, `replace_registry_id`, `expected_revision`
  - Headers: `authorization`, `content-type`
- **Handler:** `upload_wasm`
- **Storage operations:** `sqlite:PluginRegistryStore::persist_wasm_upload:SqliteStorage::persist_wasm_upload`, `postgres:PluginRegistryStore::persist_wasm_upload:PostgresStorage::persist_wasm_upload`, `sqlite:PluginRegistryStore::replace_wasm_entry:SqliteStorage::replace_wasm_entry`, `postgres:PluginRegistryStore::replace_wasm_entry:PostgresStorage::replace_wasm_entry`, `sqlite:PluginRegistryStore::update_supported_slots:SqliteStorage::update_supported_slots`, `postgres:PluginRegistryStore::update_supported_slots:PostgresStorage::update_supported_slots`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Writes compiled/validated WASM binary to disk cache ({data_dir}/plugins/wasm/cache/{sha256_hex}.wasm); if replaced, deletes old cache file; invalidates TanStack queryKey ['plugins', 'registry']
- **Side effects:** Stores binary in wasm_blobs_v2; creates or updates wasm_registry_v2 entry; validates WebAssembly header, exports, interface components; records audit event 'plugin_registry_upload' (or 'plugin_registry_upload_attempt' on failure); sends pg_notify 'cclb_plugin_changed' in Postgres; adds dynamic rebind headers; sets Location header on CREATED; sets X-Idempotent header QA restore: DELETE /admin/v1/plugins/registry/{id} if fresh upload; or re-upload previous WASM version if replaced.
- **Expected UI:** Toast 'Replaced {filename}' on success, closes modal, and navigates to replaced plugin detail view
- **Runtime result:** `PENDING`

### [UI-PLUG-20] Plugins — Cancel Plugin Replacement

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/plugins/PluginUploadCard.tsx:164#ConfirmDialog`
- **Preconditions:** Replacement modal is visible
- **Steps:** In 'Confirm Plugin Replacement' dialog, click 'Cancel' button or press Escape
- **Scope:** `once` — Replacement dialog dismissal
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Closes dialog, retains current view
- **Client-only reason:** Local state reset (setPendingReplacement(null))
- **Runtime result:** `PENDING`

### [UI-PLUG-14] Plugins — Upload Action Modal Lifecycle

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/plugins/PluginsPage.tsx:164#PluginsPage`
- **Preconditions:** action=upload present in URL
- **Steps:** Navigate with URL search param action=upload → Modal opens automatically with focus on dropzone → consumeUploadAction removes action=upload from URL while preserving plugin selection → Click close button or complete upload to dismiss
- **Scope:** `once` — Deep-link or palette action
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Opens 'Upload plugin' modal, locks dismissal while uploading, closes on complete
- **Client-only reason:** Modal open state and TanStack Router search param synchronization
- **Runtime result:** `PENDING`

### [UI-SRC-FBE359698524] Plugins — Delete Dialog References Fetch

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PLUG-09`
- **Source:** `crates/cc-lb-admin/web/src/components/plugins/PluginDeleteDialog.tsx:20#usePluginReferences`
- **Preconditions:** pendingDelete != null; pendingDelete.refcount > 0
- **Steps:** Open delete dialog for a plugin that has refcount > 0
- **Scope:** `each_plugin` — Each referenced plugin deletion attempt
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Lists affected reference locations before allowing deletion
- **Runtime result:** `PENDING`

### [UI-PLUG-09] Plugins — Delete Dialog References Fetch — usePluginReferences

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-FBE359698524`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1422#usePluginReferences`
- **Preconditions:** pendingDelete != null; pendingDelete.refcount > 0
- **Steps:** Open delete dialog for a plugin that has refcount > 0
- **Scope:** `each_plugin` — Each referenced plugin deletion attempt
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/plugins/registry/{id}/references`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `get_registry_references`
- **Storage operations:** `sqlite:PluginRegistryStore::get_registry_entry_by_id:SqliteStorage::get_registry_entry_by_id`, `postgres:PluginRegistryStore::get_registry_entry_by_id:PostgresStorage::get_registry_entry_by_id`, `sqlite:PluginRegistryStore::get_blob_bytes:SqliteStorage::get_blob_bytes`, `postgres:PluginRegistryStore::get_blob_bytes:PostgresStorage::get_blob_bytes`, `sqlite:PluginRegistryStore::list_registry_references:SqliteStorage::list_registry_references`, `postgres:PluginRegistryStore::list_registry_references:PostgresStorage::list_registry_references`
- **Cache / no-query path:** Reads registry entry and blob bytes, then queries principal-chain and upstream warmup-dialect references in a transaction.
- **Side effects:** None
- **Expected UI:** Lists affected reference locations before allowing deletion
- **Runtime result:** `PENDING`

### [UI-SRC-9693EBA2D460] Plugins — Plugin Deletion Confirm (Simple & Cascade)

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PLUG-08`
- **Source:** `crates/cc-lb-admin/web/src/components/plugins/PluginDeleteDialog.tsx:118#ConfirmDialog`
- **Preconditions:** pendingDelete != null; !del.isPending; !fingerprintPending
- **Steps:** In delete dialog, wait for fingerprint if referenced → Click 'Delete plugin' (if zero refs) or 'Delete and remove uses' (if referenced)
- **Scope:** `each_plugin` — Each plugin deletion execution
- **Risk:** `destructive_write`
- **Production applicability:** `available`
- **Expected UI:** Toast 'Plugin deleted'; dialog closes (catalog) or detail view navigates back to catalog clearing ?plugin={id} (detail); registry refreshes and the deleted plugin's inactive plugin-references cache entry is removed
- **Runtime result:** `PENDING`

### [UI-PLUG-08] Plugins — Plugin Deletion Confirm (Simple & Cascade) — useDeletePlugin

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-9693EBA2D460`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1434#useDeletePlugin`
- **Preconditions:** pendingDelete != null; !del.isPending; !fingerprintPending
- **Steps:** In delete dialog, wait for fingerprint if referenced → Click 'Delete plugin' (if zero refs) or 'Delete and remove uses' (if referenced)
- **Scope:** `each_plugin` — Each plugin deletion execution
- **Risk:** `destructive_write`
- **Production applicability:** `available`
- **HTTP:** `DELETE /admin/v1/plugins/registry/{id}`
  - Query: `cascade`
  - Body: None
  - Headers: `authorization`, `if-match`, `x-reference-fingerprint`
- **Handler:** `delete_registry`
- **Storage operations:** `sqlite:PluginRegistryStore::delete_registry_entry:SqliteStorage::delete_registry_entry`, `postgres:PluginRegistryStore::delete_registry_entry:PostgresStorage::delete_registry_entry`, `sqlite:PluginRegistryStore::cascade_delete_registry_entry:SqliteStorage::cascade_delete_registry_entry`, `postgres:PluginRegistryStore::cascade_delete_registry_entry:PostgresStorage::cascade_delete_registry_entry`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Evicts compiled WASM cache file from disk ({data_dir}/plugins/wasm/cache/{sha256_hex}.wasm); invalidates TanStack queryKey ['plugins', 'registry'], ['plugin-references', id], and if cascade: ['plugin-chain'], ['upstreams']
- **Side effects:** Deletes registry entry row; if cascade: removes referencing chain entries in plugin_chains_v2 and unsets warmup_dialect_plugin in upstream_spec_v1; removes unreferenced wasm_blobs_v2 blob if no other registry entry references it; removes disk cache file; records audit event 'plugin_registry_delete'; sends pg_notify 'cclb_plugin_changed' in Postgres; adds dynamic rebind headers QA restore: Re-upload WASM binary via POST /admin/v1/plugins/wasm and recreate plugin chain / upstream bindings.
- **Expected UI:** Toast 'Plugin deleted'; dialog closes (catalog) or detail view navigates back to catalog clearing ?plugin={id} (detail); registry refreshes and the deleted plugin's inactive plugin-references cache entry is removed
- **Runtime result:** `PENDING`

### [UI-PLUG-19] Plugins — Cancel Plugin Deletion

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/plugins/PluginDeleteDialog.tsx:34#ConfirmDialog`
- **Preconditions:** pendingDelete != null; !del.isPending
- **Steps:** In delete dialog, click 'Cancel' button or press Escape
- **Scope:** `once` — Delete dialog dismissal
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Closes dialog and retains plugin row
- **Client-only reason:** Local state reset (onClose callback)
- **Runtime result:** `PENDING`

### [UI-PLUG-13] Plugins — Back to Catalog Button Click

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/plugins/PluginDetail.tsx:36#PluginDetail`
- **Preconditions:** Plugin detail view is displayed
- **Steps:** In plugin detail view, click 'Back to Catalog' ghost button
- **Scope:** `each_plugin` — Each plugin detail view
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Transitions view from detail back to catalog table
- **Client-only reason:** TanStack Router search param navigation: setSelectedPluginId(null)
- **Runtime result:** `PENDING`

### [UI-PLUG-12] Plugins — Detail Card Copy SHA256

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/plugins/PluginDetailIntegrity.tsx:31#PluginDetailIntegrity`
- **Preconditions:** Plugin detail view is mounted
- **Steps:** In 'File details' card, locate SHA256 row → Click copy icon button beside the truncated SHA256 hex
- **Scope:** `each_plugin` — Each plugin detail view
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Toast 'Copied SHA256 to clipboard'
- **Client-only reason:** Browser Clipboard API interaction via useCopyButton
- **Runtime result:** `PENDING`

### [UI-PLUG-16] Plugins — Inline Label Edit Form Toggle (Open & Cancel)

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/plugins/PluginDetailOperate.tsx:36#PluginDetailOperate`
- **Preconditions:** !plugin.is_builtin
- **Steps:** Click 'Edit' button beside current label in Manage this plugin card → Optionally edit text in input field → Click 'Cancel' button to discard changes
- **Scope:** `each_plugin` — Each non-built-in plugin detail view
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Toggles between label text display and inline edit form with focus
- **Client-only reason:** Component local state: isEditingLabel and editLabelValue
- **Runtime result:** `PENDING`

### [UI-SRC-824B79F88848] Plugins — Save Plugin Label Mutation

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PLUG-07`
- **Source:** `crates/cc-lb-admin/web/src/components/plugins/PluginDetailOperate.tsx:17#handleSaveLabel`
- **Preconditions:** !plugin.is_builtin; isEditingLabel === true; !patch.isPending
- **Steps:** In inline label edit form, enter new label string (or clear for null) → Click 'Save' button
- **Scope:** `each_plugin` — Each label update action
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **Expected UI:** Toast 'Label updated', closes edit form, updates label in header and catalog
- **Runtime result:** `PENDING`

### [UI-PLUG-07] Plugins — Save Plugin Label Mutation — usePatchPlugin

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-824B79F88848`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1483#usePatchPlugin`
- **Preconditions:** !plugin.is_builtin; isEditingLabel === true; !patch.isPending
- **Steps:** In inline label edit form, enter new label string (or clear for null) → Click 'Save' button
- **Scope:** `each_plugin` — Each label update action
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `PATCH /admin/v1/plugins/registry/{id}`
  - Query: None
  - Body: `label`
  - Headers: `authorization`, `if-match`, `content-type`
- **Handler:** `patch_registry`
- **Storage operations:** `sqlite:PluginRegistryStore::update_registry_label:SqliteStorage::update_registry_label`, `postgres:PluginRegistryStore::update_registry_label:PostgresStorage::update_registry_label`
- **Cache / no-query path:** Invalidates TanStack queryKey ['plugins', 'registry']; adds dynamic rebind headers (x-dynamic-rebind-*)
- **Side effects:** Updates human label of plugin registry entry; increments revision; sets updated_at; notifies 'cclb_plugin_changed' channel in PostgreSQL; triggers dynamic rebind headers; no audit recorded QA restore: Re-patch registry entry with prior label using updated If-Match revision.
- **Expected UI:** Toast 'Label updated', closes edit form, updates label in header and catalog
- **Runtime result:** `PENDING`

### [UI-PLUG-18] Plugins — Detail View Delete Button Click

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/plugins/PluginDetailOperate.tsx:102#PluginDetailOperate`
- **Preconditions:** !plugin.is_builtin
- **Steps:** In 'Manage this plugin' card, click 'Delete Plugin' danger button
- **Scope:** `each_plugin` — Each non-built-in plugin detail view
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Opens PluginDeleteDialog with current plugin details; on confirmed deletion the detail view calls onBack(), clearing ?plugin={id} and returning to the catalog
- **Client-only reason:** Component local state: sets pendingDelete object in PluginDetail
- **Runtime result:** `PENDING`

### [UI-PLUG-21] Plugins — Used By Reference Link Click

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/plugins/PluginDetail.tsx:62#PluginDetail`
- **Preconditions:** Plugin has at least one active reference in references query
- **Steps:** In 'Used by' section, locate reference list item → Click anchor link for principal (/principals?selectedId={id}) or upstream (/upstreams?selectedId={id})
- **Scope:** `each_row` — Each reference list item
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Navigates to the corresponding Principal or Upstream management page with selected entity
- **Client-only reason:** Standard browser anchor navigation
- **Runtime result:** `PENDING`

### [UI-PLUG-22] Plugins — Apply Plugin Target Navigation Links

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/plugins/PluginDetailApply.tsx:16#PluginDetailApply`
- **Preconditions:** Plugin has supported_slots declared in metadata
- **Steps:** In 'Use this plugin' section, review available target action buttons → Click desired target button
- **Scope:** `each_plugin` — Each applicable target slot
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Navigates to /principals or /upstreams where the plugin can be attached
- **Client-only reason:** Standard browser anchor navigation
- **Runtime result:** `PENDING`

### [UI-SRC-DF3F46767D73] Settings — System Status & Version Telemetry

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-SET-01`
- **Source:** `crates/cc-lb-admin/web/src/routes/settings.tsx:202#SettingsPage`
- **Preconditions:** Authenticated session
- **Steps:** Navigate to /settings or mount SettingsPage
- **Scope:** `once` — Single execution
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Renders build metadata, uptime, and generation
- **Runtime result:** `PENDING`

### [UI-SET-01] Settings — System Status & Version Telemetry — useStatus

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-DF3F46767D73`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:361#useStatus`
- **Preconditions:** Authenticated session
- **Steps:** Navigate to /settings or mount SettingsPage
- **Scope:** `once` — Single execution
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/status`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `status`
- **Storage operations:** `sqlite:UpstreamStore::list:SqliteStorage::list`, `postgres:UpstreamStore::list:PostgresStorage::list`
- **Cache / no-query path:** DB query for upstream names/IDs combined with in-memory DynamicViewHolder.replica_identity and build metadata
- **Side effects:** None
- **Expected UI:** Renders build metadata, uptime, and generation
- **Runtime result:** `PENDING`

### [UI-SRC-0744A60F49D3] Settings — Admin Token Guidance View

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/routes/settings.tsx:326#SettingsPage`
- **Preconditions:** authSession context has auth_mode === 'static_token'
- **Steps:** Mount SettingsPage with authSession.auth_mode === 'static_token'
- **Scope:** `once` — Single execution
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Displays static-token provider guidance for token_env and daemon restart
- **Client-only reason:** Conditional informational card rendered solely based on client AuthSessionContext
- **Runtime result:** `PENDING`

### [UI-SET-10] Settings — Change Locale Preference

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/routes/settings.tsx:360#SettingsPage`
- **Preconditions:** SettingsPage mounted
- **Steps:** Select option in Locale dropdown
- **Scope:** `once` — Equivalence classes: ['auto', 'en-US', 'ko-KR']
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Updates client localStorage['cclb.locale'] and triggers global re-formatting of numbers/dates
- **Client-only reason:** Client-only preference stored in localStorage and reacted to via useSyncExternalStore
- **Runtime result:** `PENDING`

### [UI-SET-11] Settings — Change Timezone Preference

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/routes/settings.tsx:377#SettingsPage`
- **Preconditions:** SettingsPage mounted
- **Steps:** Select option in Timezone dropdown
- **Scope:** `once` — Equivalence classes: ['auto', 'UTC', 'Asia/Seoul']
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Updates client localStorage['cclb.timezone'] and triggers global re-calculation of relative/absolute timestamps
- **Client-only reason:** Client-only preference stored in localStorage and reacted to via useSyncExternalStore
- **Runtime result:** `PENDING`

### [UI-SRC-5A139160911F] Settings — Localization Live Preview Clock Tick

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/routes/settings.tsx:182#SettingsPage`
- **Preconditions:** SettingsPage mounted
- **Steps:** 1000ms timer elapses
- **Scope:** `once` — Continuous 1000ms interval
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Updates preview timestamp string 'Current preview: {formatAbsolute(now)}'
- **Client-only reason:** Client-only setInterval updating local Date state
- **Runtime result:** `PENDING`

### [UI-SET-03] Settings — Load Configuration Draft Pipeline Data

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-SET-03B`, `UI-SET-03A`, `UI-SET-03C`
- **Source:** `crates/cc-lb-admin/web/src/routes/settings.tsx:435#ConfigDraftSection`
- **Preconditions:** Authenticated session
- **Steps:** Mount ConfigDraftSection
- **Scope:** `once` — Single execution
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Populates draft editor, revision badges, and schema checklist
- **Runtime result:** `PENDING`

### [UI-SET-03B] Settings — Load Configuration Draft Pipeline Data — useConfigDraft

- **Entry type:** `network_request`
- **Parent action:** `UI-SET-03`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:793#useConfigDraft`
- **Preconditions:** Authenticated session
- **Steps:** Mount ConfigDraftSection
- **Scope:** `once` — Single execution
- **Risk:** `read_with_audit`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/config/draft`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `get_config_draft`
- **Storage operations:** `sqlite:ConfigStore::get_config_draft:SqliteStorage::get_config_draft`, `postgres:ConfigStore::get_config_draft:PostgresStorage::get_config_draft`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Reads singleton config draft from DB
- **Side effects:** Audit log write: record_admin_audit("config_draft_read", "/admin/v1/config/draft")
- **Expected UI:** Populates draft editor, revision badges, and schema checklist
- **Runtime result:** `PENDING`

### [UI-SET-03A] Settings — Load Configuration Draft Pipeline Data — useConfigCurrent

- **Entry type:** `network_request`
- **Parent action:** `UI-SET-03`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:781#useConfigCurrent`
- **Preconditions:** Authenticated session
- **Steps:** Mount ConfigDraftSection
- **Scope:** `once` — Single execution
- **Risk:** `read_with_audit`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/config/current`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `get_config`
- **Storage operations:** `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** In-memory state.config.current_config() Arc clone; secrets are masked via mask_secret_like_values before serialization.
- **Side effects:** Audit log write: record_admin_audit("config_read", "/admin/v1/config/current")
- **Expected UI:** Populates draft editor, revision badges, and schema checklist
- **Runtime result:** `PENDING`

### [UI-SET-03C] Settings — Load Configuration Draft Pipeline Data — useConfigSchema

- **Entry type:** `network_request`
- **Parent action:** `UI-SET-03`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:787#useConfigSchema`
- **Preconditions:** Authenticated session
- **Steps:** Mount ConfigDraftSection
- **Scope:** `once` — Single execution
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/config/schema`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `get_config_schema`
- **Storage operations:** None
- **Cache / no-query path:** In-memory schemars schema generation; HTTP Cache-Control: max-age=60
- **Side effects:** None
- **Expected UI:** Populates draft editor, revision badges, and schema checklist
- **Runtime result:** `PENDING`

### [UI-SET-14] Settings — Edit Draft Textarea

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/routes/settings.tsx:740#ConfigDraftSection`
- **Preconditions:** Editor initialized and not locked by configPending
- **Steps:** Type or paste in JSON config textarea
- **Scope:** `once` — Equivalence classes: [valid JSON modification, invalid syntax string, empty string]
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Updates editor.text state; sets editorDirty=true; updates guidance text
- **Client-only reason:** Controlled React textarea local state
- **Runtime result:** `PENDING`

### [UI-SRC-67C497520AA2] Settings — Save Draft

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-SET-04`
- **Source:** `crates/cc-lb-admin/web/src/routes/settings.tsx:556#ConfigDraftSection`
- **Preconditions:** canSave === true: editor != null && (editorDirty || (!editor.hasSavedDraft && !serverRevisionChanged)) && !configPending
- **Steps:** Click Save button
- **Scope:** `once` — Single execution per draft edit
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **Expected UI:** Displays saving spinner; on success updates revision counter and saved time, toast 'Draft saved'
- **Runtime result:** `PENDING`

### [UI-SET-04] Settings — Save Draft — useSaveDraft

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-67C497520AA2`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1610#useSaveDraft`
- **Preconditions:** canSave === true: editor != null && (editorDirty || (!editor.hasSavedDraft && !serverRevisionChanged)) && !configPending
- **Steps:** Click Save button
- **Scope:** `once` — Single execution per draft edit
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `PUT /admin/config/draft`
  - Query: None
  - Body: `draft`, `expected_revision`
  - Headers: `authorization`, `content-type`
- **Handler:** `put_config_draft`
- **Storage operations:** `sqlite:ConfigStore::get_config_draft:SqliteStorage::get_config_draft`, `postgres:ConfigStore::get_config_draft:PostgresStorage::get_config_draft`, `sqlite:ConfigStore::put_config_draft:SqliteStorage::put_config_draft`, `postgres:ConfigStore::put_config_draft:PostgresStorage::put_config_draft`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Stores in-memory draft copy via CurrentConfig::put_draft_config; invalidates TanStack queryKey ['config', 'draft']
- **Side effects:** Audit event `config_draft_put`; increments draft revision in DB and resets last_validated_revision to null QA restore: Re-put previous draft configuration or clear draft.
- **Expected UI:** Displays saving spinner; on success updates revision counter and saved time, toast 'Draft saved'
- **Runtime result:** `PENDING`

### [UI-SRC-BFD8C956C3C5] Settings — Validate Draft

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-SET-05`
- **Source:** `crates/cc-lb-admin/web/src/routes/settings.tsx:593#ConfigDraftSection`
- **Preconditions:** canValidate === true: editor.hasSavedDraft === true && !editorDirty && !serverRevisionChanged && !configPending
- **Steps:** Click Validate button
- **Scope:** `once` — Single execution per saved revision
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **Expected UI:** Displays validating spinner; on success updates last validated revision slot (green or red with error text)
- **Runtime result:** `PENDING`

### [UI-SET-05] Settings — Validate Draft — useValidateConfig

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-BFD8C956C3C5`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1601#useValidateConfig`
- **Preconditions:** canValidate === true: editor.hasSavedDraft === true && !editorDirty && !serverRevisionChanged && !configPending
- **Steps:** Click Validate button
- **Scope:** `once` — Single execution per saved revision
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/config/draft/validate`
  - Query: None
  - Body: `expected_revision`
  - Headers: `authorization`, `content-type`
- **Handler:** `validate_config_draft`
- **Storage operations:** `sqlite:ConfigStore::get_config_draft:SqliteStorage::get_config_draft`, `postgres:ConfigStore::get_config_draft:PostgresStorage::get_config_draft`, `sqlite:ConfigStore::set_last_validated_revision:SqliteStorage::set_last_validated_revision`, `postgres:ConfigStore::set_last_validated_revision:PostgresStorage::set_last_validated_revision`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** No cache write; updates last_validated_revision in storage
- **Side effects:** Audit event `config_draft_validate`; checks TOML/JSON schema and semantic validity; marks revision as validated or stores validation error QA restore: Re-validate previous revision.
- **Expected UI:** Displays validating spinner; on success updates last validated revision slot (green or red with error text)
- **Runtime result:** `PENDING`

### [UI-SRC-EDE5449D42BD] Settings — Apply Draft Revision

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-SET-06`
- **Source:** `crates/cc-lb-admin/web/src/routes/settings.tsx:610#ConfigDraftSection`
- **Preconditions:** canApply === true: editor != null && canValidate && lastValidatedRevision != null && lastValidationError == null && lastValidatedRevision === editor.revision && !configPending
- **Steps:** Click Apply button
- **Scope:** `once` — Single execution per validated revision
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **Expected UI:** Displays applying spinner; toast 'Applied revision {n}'; history and current config queries refresh
- **Runtime result:** `PENDING`

### [UI-SET-06] Settings — Apply Draft Revision — useApplyConfig

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-EDE5449D42BD`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1586#useApplyConfig`
- **Preconditions:** canApply === true: editor != null && canValidate && lastValidatedRevision != null && lastValidationError == null && lastValidatedRevision === editor.revision && !configPending
- **Steps:** Click Apply button
- **Scope:** `once` — Single execution per validated revision
- **Risk:** `destructive_write`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/config/apply`
  - Query: None
  - Body: `expected_revision`
  - Headers: `authorization`
- **Handler:** `apply_config_draft`
- **Storage operations:** `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Atomically swaps DynamicView and CurrentConfig; clears in-memory draft; invalidates TanStack queryKey ['config', 'draft'], ['config', 'current']
- **Side effects:** Audit event `config_apply`; activates new runtime proxy state in memory without database entity mutation QA restore: Re-apply previous configuration draft or reload original config.
- **Expected UI:** Displays applying spinner; toast 'Applied revision {n}'; history and current config queries refresh
- **Runtime result:** `PENDING`

### [UI-SRC-EA3DE5874C4D] Settings — Trigger Daemon Configuration Hot Reload

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-SET-07`
- **Source:** `crates/cc-lb-admin/web/src/routes/settings.tsx:660#ConfigDraftSection`
- **Preconditions:** !configPending
- **Steps:** Click Reload button
- **Scope:** `once` — Single execution
- **Risk:** `external_action`
- **Production applicability:** `available`
- **Expected UI:** Displays reloading spinner; toast 'Reload triggered'
- **Runtime result:** `PENDING`

### [UI-SET-07] Settings — Trigger Daemon Configuration Hot Reload — useReloadConfig

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-EA3DE5874C4D`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1627#useReloadConfig`
- **Preconditions:** !configPending
- **Steps:** Click Reload button
- **Scope:** `once` — Single execution
- **Risk:** `external_action`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/config/reload`
  - Query: None
  - Body: None
  - Headers: `authorization`
- **Handler:** `reload_config`
- **Storage operations:** `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** No database storage entity modified (only audit entry written)
- **Side effects:** Audit event `config_reload_signal`; sends UNIX SIGHUP signal to pid (nix::sys::signal::kill) to trigger process config re-read from disk QA restore: Ensure valid configuration file exists on disk and send SIGHUP again.
- **Expected UI:** Displays reloading spinner; toast 'Reload triggered'
- **Runtime result:** `PENDING`

### [UI-SET-12] Settings — Retry Draft Editor Loading

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-SET-12B`, `UI-SET-12A`
- **Source:** `crates/cc-lb-admin/web/src/routes/settings.tsx:550#ConfigDraftSection`
- **Preconditions:** editorLoadError === true (either draft or current failed) && !editorRetrying && !configPending
- **Steps:** Click Retry button in error banner
- **Scope:** `once` — Single execution upon error
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Banner displays 'Retrying...'; triggers refetch of failed queries
- **Runtime result:** `PENDING`

### [UI-SET-12B] Settings — Retry Draft Editor Loading — draft.refetch

- **Entry type:** `network_request`
- **Parent action:** `UI-SET-12`
- **Source:** `crates/cc-lb-admin/web/src/routes/settings.tsx:551#handleRetryEditor`
- **Preconditions:** editorLoadError === true (either draft or current failed) && !editorRetrying && !configPending
- **Steps:** Click Retry button in error banner
- **Scope:** `once` — Single execution upon error
- **Risk:** `read_with_audit`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/config/draft`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `get_config_draft`
- **Storage operations:** `sqlite:ConfigStore::get_config_draft:SqliteStorage::get_config_draft`, `postgres:ConfigStore::get_config_draft:PostgresStorage::get_config_draft`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Reads singleton config draft from DB
- **Side effects:** Audit log write: record_admin_audit("config_draft_read", "/admin/v1/config/draft")
- **Expected UI:** Banner displays 'Retrying...'; triggers refetch of failed queries
- **Runtime result:** `PENDING`

### [UI-SET-12A] Settings — Retry Draft Editor Loading — current.refetch

- **Entry type:** `network_request`
- **Parent action:** `UI-SET-12`
- **Source:** `crates/cc-lb-admin/web/src/routes/settings.tsx:552#handleRetryEditor`
- **Preconditions:** editorLoadError === true (either draft or current failed) && !editorRetrying && !configPending
- **Steps:** Click Retry button in error banner
- **Scope:** `once` — Single execution upon error
- **Risk:** `read_with_audit`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/config/current`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `get_config`
- **Storage operations:** `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** In-memory state.config.current_config() Arc clone; secrets are masked via mask_secret_like_values before serialization.
- **Side effects:** Audit log write: record_admin_audit("config_read", "/admin/v1/config/current")
- **Expected UI:** Banner displays 'Retrying...'; triggers refetch of failed queries
- **Runtime result:** `PENDING`

### [UI-SET-13] Settings — Toggle Schema Coverage Checklist

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/routes/settings.tsx:756#ConfigDraftSection`
- **Preconditions:** schema.data available
- **Steps:** Click Coverage checklist trigger
- **Scope:** `once` — Toggle action
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Expands or collapses BaseCollapsible panel showing list of schema coverage fields
- **Client-only reason:** Client UI collapsible state managed by @base-ui/react/collapsible
- **Runtime result:** `PENDING`

### [UI-SRC-C9DD3E291979] Settings — Load Configuration History

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-SET-08`
- **Source:** `crates/cc-lb-admin/web/src/routes/settings.tsx:785#ConfigHistorySection`
- **Preconditions:** Authenticated session
- **Steps:** Mount ConfigHistorySection
- **Scope:** `once` — Single execution
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Renders table of up to 20 last applied revisions
- **Runtime result:** `PENDING`

### [UI-SET-08] Settings — Load Configuration History — useConfigHistory

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-C9DD3E291979`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:799#useConfigHistory`
- **Preconditions:** Authenticated session
- **Steps:** Mount ConfigHistorySection
- **Scope:** `once` — Single execution
- **Risk:** `read_with_audit`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/config/history`
  - Query: `limit=20`
  - Body: None
  - Headers: None
- **Handler:** `get_config_history`
- **Storage operations:** `sqlite:ConfigStore::list_config_history:SqliteStorage::list_config_history`, `postgres:ConfigStore::list_config_history:PostgresStorage::list_config_history`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Query parameter limit defaults to 20 and is passed through to storage SQL without a maximum clamp; limit=0 short-circuits to an empty result.
- **Side effects:** Audit log write: record_admin_audit("config_history_read", "/admin/v1/config/history")
- **Expected UI:** Renders table of up to 20 last applied revisions
- **Runtime result:** `PENDING`

### [UI-SRC-625C96B0933F] Settings — Restart-Required Fields Matrix View

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/routes/settings.tsx:871#RestartRequiredMatrix`
- **Preconditions:** SettingsPage mounted
- **Steps:** Mount SettingsPage
- **Scope:** `once` — 15 static field definitions
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Renders table listing fields where Hot reload = 'No' (danger badge) and explanation
- **Client-only reason:** Static client-side reference matrix
- **Runtime result:** `PENDING`

### [UI-SRC-49B5404AC804] Settings — Download Configuration Export Snapshot

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-SET-02`
- **Source:** `crates/cc-lb-admin/web/src/routes/settings.tsx:191#handleDownloadExport`
- **Preconditions:** SettingsPage mounted && !exportingRef.current
- **Steps:** Click Download export.json button
- **Scope:** `once` — Single execution
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Initiates browser blob download and shows feedback toast
- **Runtime result:** `PENDING`

### [UI-SET-02] Settings — Download Configuration Export Snapshot — downloadJson

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-49B5404AC804`
- **Source:** `crates/cc-lb-admin/web/src/lib/api.ts:267#downloadJson`
- **Preconditions:** SettingsPage mounted && !exportingRef.current
- **Steps:** Click Download export.json button
- **Scope:** `once` — Single execution
- **Risk:** `read_with_audit`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/export`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `export`
- **Storage operations:** `sqlite:UpstreamStore::list:SqliteStorage::list`, `postgres:UpstreamStore::list:PostgresStorage::list`, `sqlite:PrincipalStore::list:SqliteStorage::list`, `postgres:PrincipalStore::list:PostgresStorage::list`, `sqlite:PluginRegistryStore::list_registry:SqliteStorage::list_registry`, `postgres:PluginRegistryStore::list_registry:PostgresStorage::list_registry`, `sqlite:PluginRegistryStore::get_blob_bytes:SqliteStorage::get_blob_bytes`, `postgres:PluginRegistryStore::get_blob_bytes:PostgresStorage::get_blob_bytes`, `sqlite:PluginRegistryStore::list_chain_for_principal:SqliteStorage::list_chain_for_principal`, `postgres:PluginRegistryStore::list_chain_for_principal:PostgresStorage::list_chain_for_principal`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Database-backed export: paginates all upstreams, principals, plugin registry entries, blob bytes, and each principal plugin-chain slot; then appends an audit record.
- **Side effects:** Audit log write: record_admin_audit("config_export", "/admin/v1/export")
- **Expected UI:** Initiates browser blob download and shows feedback toast
- **Runtime result:** `PENDING`

### [UI-SRC-2FC5E3F9B148] Audit — Load Audit Trail & Supporting Entity Maps

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-AUD-01`, `UI-AUD-07`, `UI-AUD-08`
- **Source:** `crates/cc-lb-admin/web/src/routes/audit.tsx:140#AuditPage`
- **Preconditions:** Authenticated session
- **Steps:** Navigate to /audit or change query parameters
- **Scope:** `once` — Single execution on mount / search change
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Renders audit table with 9 columns (Timestamp, Principal, Actor, Route, Upstream, Action, Kind, Status, Detail)
- **Runtime result:** `PENDING`

### [UI-AUD-01] Audit — Load Audit Trail & Supporting Entity Maps — useAudit

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-2FC5E3F9B148`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:724#useAudit`
- **Preconditions:** Authenticated session
- **Steps:** Navigate to /audit or change query parameters
- **Scope:** `once` — Single execution on mount / search change
- **Risk:** `read_with_audit`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/audit`
  - Query: `limit=200`, `principal_id`, `since`, `until`
  - Body: None
  - Headers: None
- **Handler:** `query_audit`
- **Storage operations:** `sqlite:AuditStore::query_recent_audit:SqliteStorage::query_recent_audit`, `postgres:AuditStore::query_recent_audit:PostgresStorage::query_recent_audit`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Single storage dispatch: actor_authority+actor_subject selects AuditQueryScope::Actor, principal_id selects AuditQueryScope::Principal, otherwise AuditQueryScope::All; all scopes call AuditStore::query_recent_audit, which applies the scope filter and since/until bounds before LIMIT. since is inclusive; after is an exclusive lower bound converted to since = max(since, after+1). Results are newest-first: ORDER BY ts DESC with id DESC (SQLite) / seq DESC (Postgres) tie-break. limit defaults to 100, capped at 1000; limit=0 or until<since returns empty without SQL.
- **Side effects:** Audit log write: record_admin_audit("audit_query", "/admin/v1/audit")
- **Expected UI:** Renders audit table with 9 columns (Timestamp, Principal, Actor, Route, Upstream, Action, Kind, Status, Detail)
- **Runtime result:** `PENDING`

### [UI-AUD-07] Audit — Load Audit Trail & Supporting Entity Maps — usePrincipals

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-2FC5E3F9B148`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:427#usePrincipals`
- **Preconditions:** Authenticated session
- **Steps:** Navigate to /audit or change query parameters
- **Scope:** `once` — Single execution on mount / search change
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/principals`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `list_principals`
- **Storage operations:** `sqlite:PrincipalStore::list:SqliteStorage::list`, `postgres:PrincipalStore::list:PostgresStorage::list`
- **Cache / no-query path:** Executes PrincipalStore::list twice: first with offset=0 and limit=1000 to derive the capped X-Total-Count, then with the requested offset/limit for the page; no COUNT SQL is executed.
- **Side effects:** None
- **Expected UI:** Renders audit table with 9 columns (Timestamp, Principal, Actor, Route, Upstream, Action, Kind, Status, Detail)
- **Runtime result:** `PENDING`

### [UI-AUD-08] Audit — Load Audit Trail & Supporting Entity Maps — useUpstreams

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-2FC5E3F9B148`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:418#useUpstreams`
- **Preconditions:** Authenticated session
- **Steps:** Navigate to /audit or change query parameters
- **Scope:** `once` — Single execution on mount / search change
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/upstreams`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `list_upstreams`
- **Storage operations:** `sqlite:UpstreamStore::list:SqliteStorage::list`, `postgres:UpstreamStore::list:PostgresStorage::list`
- **Cache / no-query path:** Sets X-Total-Count header; in-memory keyset pagination on ?after (UUID) UpstreamResponse now serializes base_url: Option<Url> (crates/cc-lb-admin/src/v1/upstreams.rs:167,1587), so the field is present and null when the upstream has no custom base URL.
- **Side effects:** None
- **Expected UI:** Renders audit table with 9 columns (Timestamp, Principal, Actor, Route, Upstream, Action, Kind, Status, Detail)
- **Runtime result:** `PENDING`

### [UI-SRC-8A3FB89087F5] Audit — Refresh Audit Trail

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-AUD-05`
- **Source:** `crates/cc-lb-admin/web/src/routes/audit.tsx:204#refreshAudit`
- **Preconditions:** AuditPage mounted && !refreshInFlightRef.current
- **Steps:** Click Refresh button
- **Scope:** `once` — Single execution
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Re-queries GET /admin/audit with current search parameters
- **Runtime result:** `PENDING`

### [UI-AUD-05] Audit — Refresh Audit Trail — audit.refetch

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-8A3FB89087F5`
- **Source:** `crates/cc-lb-admin/web/src/routes/audit.tsx:209#refreshAudit`
- **Preconditions:** AuditPage mounted && !refreshInFlightRef.current
- **Steps:** Click Refresh button
- **Scope:** `once` — Single execution
- **Risk:** `read_with_audit`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/audit`
  - Query: `limit=200`, `principal_id`, `since`, `until`
  - Body: None
  - Headers: None
- **Handler:** `query_audit`
- **Storage operations:** `sqlite:AuditStore::query_recent_audit:SqliteStorage::query_recent_audit`, `postgres:AuditStore::query_recent_audit:PostgresStorage::query_recent_audit`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Single storage dispatch: actor_authority+actor_subject selects AuditQueryScope::Actor, principal_id selects AuditQueryScope::Principal, otherwise AuditQueryScope::All; all scopes call AuditStore::query_recent_audit, which applies the scope filter and since/until bounds before LIMIT. since is inclusive; after is an exclusive lower bound converted to since = max(since, after+1). Results are newest-first: ORDER BY ts DESC with id DESC (SQLite) / seq DESC (Postgres) tie-break. limit defaults to 100, capped at 1000; limit=0 or until<since returns empty without SQL.
- **Side effects:** Audit log write: record_admin_audit("audit_query", "/admin/v1/audit")
- **Expected UI:** Re-queries GET /admin/audit with current search parameters
- **Runtime result:** `PENDING`

### [UI-SRC-DD556FCB4A54] Audit — Filter by Principal ID

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-AUD-02`
- **Source:** `crates/cc-lb-admin/web/src/routes/audit.tsx:155#setPrincipalFilter`
- **Preconditions:** Principals query loaded
- **Steps:** Select principal from dropdown or select 'All principals'
- **Scope:** `each_principal` — Principals list + empty option
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Updates URL search param principal_id and re-runs audit query with filter
- **Runtime result:** `PENDING`

### [UI-AUD-02] Audit — Filter by Principal ID — useAudit

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-DD556FCB4A54`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:724#useAudit`
- **Preconditions:** Principals query loaded
- **Steps:** Select principal from dropdown or select 'All principals'
- **Scope:** `each_principal` — Principals list + empty option
- **Risk:** `read_with_audit`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/audit`
  - Query: `limit=200`, `principal_id`, `since`, `until`
  - Body: None
  - Headers: None
- **Handler:** `query_audit`
- **Storage operations:** `sqlite:AuditStore::query_recent_audit:SqliteStorage::query_recent_audit`, `postgres:AuditStore::query_recent_audit:PostgresStorage::query_recent_audit`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Single storage dispatch: actor_authority+actor_subject selects AuditQueryScope::Actor, principal_id selects AuditQueryScope::Principal, otherwise AuditQueryScope::All; all scopes call AuditStore::query_recent_audit, which applies the scope filter and since/until bounds before LIMIT. since is inclusive; after is an exclusive lower bound converted to since = max(since, after+1). Results are newest-first: ORDER BY ts DESC with id DESC (SQLite) / seq DESC (Postgres) tie-break. limit defaults to 100, capped at 1000; limit=0 or until<since returns empty without SQL.
- **Side effects:** Audit log write: record_admin_audit("audit_query", "/admin/v1/audit")
- **Expected UI:** Updates URL search param principal_id and re-runs audit query with filter
- **Runtime result:** `PENDING`

### [UI-SRC-00FD191B0EB1] Audit — Edit Range Start (From) Field

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/ui/TimeRangeBounds.tsx:60#TimeRangeBounds`
- **Preconditions:** AuditPage mounted
- **Steps:** Type or modify datetime in 'From' input
- **Scope:** `once` — Equivalence classes: [valid '2026-06-18 10:00', empty '', invalid 'abc', invalid hour '2026-06-18 25:00', DST nonexistent]
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Updates sinceStr input value; error text displayed on failed parse
- **Client-only reason:** Local component state in TimeRangeBounds / DateTimeField
- **Runtime result:** `PENDING`

### [UI-SRC-C7A6EA2F6025] Audit — Edit Range End (To) Field

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/ui/TimeRangeBounds.tsx:72#TimeRangeBounds`
- **Preconditions:** AuditPage mounted
- **Steps:** Type or modify datetime in 'To' input
- **Scope:** `once` — Equivalence classes: [valid '2026-06-18 12:00', empty '', invalid format, before start]
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Updates untilStr input value; error text displayed on failed parse
- **Client-only reason:** Local component state in TimeRangeBounds / DateTimeField
- **Runtime result:** `PENDING`

### [UI-SRC-FA47104074F4] Audit — Pick From Date in Calendar Popover

- **Entry type:** `ui_action`
- **Atomic requests:** `REQ-SRC-0829EA9261F6`
- **Source:** `crates/cc-lb-admin/web/src/components/ui/CalendarPopover.tsx:68#CalendarPopover`
- **Preconditions:** Calendar popover opened
- **Steps:** Click calendar icon button on 'From' field → Select date in DayPicker
- **Scope:** `once` — Date picker interaction
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Merges picked date with existing time (or 00:00) into sinceStr, closes popover, commits bounds
- **Runtime result:** `PENDING`

### [REQ-SRC-0829EA9261F6] Audit — Pick From Date in Calendar Popover — useAudit

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-FA47104074F4`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:724#useAudit`
- **Preconditions:** Calendar popover opened
- **Steps:** Click calendar icon button on 'From' field → Select date in DayPicker
- **Scope:** `once` — Date picker interaction
- **Risk:** `read_with_audit`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/audit`
  - Query: `limit=200`, `principal_id`, `since`, `until`
  - Body: None
  - Headers: None
- **Handler:** `query_audit`
- **Storage operations:** `sqlite:AuditStore::query_recent_audit:SqliteStorage::query_recent_audit`, `postgres:AuditStore::query_recent_audit:PostgresStorage::query_recent_audit`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Single storage dispatch: actor_authority+actor_subject selects AuditQueryScope::Actor, principal_id selects AuditQueryScope::Principal, otherwise AuditQueryScope::All; all scopes call AuditStore::query_recent_audit, which applies the scope filter and since/until bounds before LIMIT. since is inclusive; after is an exclusive lower bound converted to since = max(since, after+1). Results are newest-first: ORDER BY ts DESC with id DESC (SQLite) / seq DESC (Postgres) tie-break. limit defaults to 100, capped at 1000; limit=0 or until<since returns empty without SQL.
- **Side effects:** Audit log write: record_admin_audit("audit_query", "/admin/v1/audit")
- **Expected UI:** Merges picked date with existing time (or 00:00) into sinceStr, closes popover, commits bounds
- **Runtime result:** `PENDING`

### [UI-SRC-38E343D2A4C1] Audit — Pick To Date in Calendar Popover

- **Entry type:** `ui_action`
- **Atomic requests:** `REQ-SRC-66D17D1EF13A`
- **Source:** `crates/cc-lb-admin/web/src/components/ui/CalendarPopover.tsx:68#CalendarPopover`
- **Preconditions:** Calendar popover opened
- **Steps:** Click calendar icon button on 'To' field → Select date in DayPicker
- **Scope:** `once` — Date picker interaction
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Merges picked date with existing time (or 23:59) into untilStr, closes popover, commits bounds
- **Runtime result:** `PENDING`

### [REQ-SRC-66D17D1EF13A] Audit — Pick To Date in Calendar Popover — useAudit

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-38E343D2A4C1`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:724#useAudit`
- **Preconditions:** Calendar popover opened
- **Steps:** Click calendar icon button on 'To' field → Select date in DayPicker
- **Scope:** `once` — Date picker interaction
- **Risk:** `read_with_audit`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/audit`
  - Query: `limit=200`, `principal_id`, `since`, `until`
  - Body: None
  - Headers: None
- **Handler:** `query_audit`
- **Storage operations:** `sqlite:AuditStore::query_recent_audit:SqliteStorage::query_recent_audit`, `postgres:AuditStore::query_recent_audit:PostgresStorage::query_recent_audit`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Single storage dispatch: actor_authority+actor_subject selects AuditQueryScope::Actor, principal_id selects AuditQueryScope::Principal, otherwise AuditQueryScope::All; all scopes call AuditStore::query_recent_audit, which applies the scope filter and since/until bounds before LIMIT. since is inclusive; after is an exclusive lower bound converted to since = max(since, after+1). Results are newest-first: ORDER BY ts DESC with id DESC (SQLite) / seq DESC (Postgres) tie-break. limit defaults to 100, capped at 1000; limit=0 or until<since returns empty without SQL.
- **Side effects:** Audit log write: record_admin_audit("audit_query", "/admin/v1/audit")
- **Expected UI:** Merges picked date with existing time (or 23:59) into untilStr, closes popover, commits bounds
- **Runtime result:** `PENDING`

### [UI-SRC-D309B057EC9E] Audit — Apply Time Range Bounds

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-AUD-03`
- **Source:** `crates/cc-lb-admin/web/src/components/ui/TimeRangeBounds.tsx:87#TimeRangeBounds`
- **Preconditions:** TimeRangeBounds rendered
- **Steps:** Click Apply button beside datetime inputs
- **Scope:** `once` — Single execution
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Updates URL search params since and until and re-runs audit query
- **Runtime result:** `PENDING`

### [UI-AUD-03] Audit — Apply Time Range Bounds — useAudit

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-D309B057EC9E`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:724#useAudit`
- **Preconditions:** TimeRangeBounds rendered
- **Steps:** Click Apply button beside datetime inputs
- **Scope:** `once` — Single execution
- **Risk:** `read_with_audit`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/audit`
  - Query: `limit=200`, `principal_id`, `since`, `until`
  - Body: None
  - Headers: None
- **Handler:** `query_audit`
- **Storage operations:** `sqlite:AuditStore::query_recent_audit:SqliteStorage::query_recent_audit`, `postgres:AuditStore::query_recent_audit:PostgresStorage::query_recent_audit`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Single storage dispatch: actor_authority+actor_subject selects AuditQueryScope::Actor, principal_id selects AuditQueryScope::Principal, otherwise AuditQueryScope::All; all scopes call AuditStore::query_recent_audit, which applies the scope filter and since/until bounds before LIMIT. since is inclusive; after is an exclusive lower bound converted to since = max(since, after+1). Results are newest-first: ORDER BY ts DESC with id DESC (SQLite) / seq DESC (Postgres) tie-break. limit defaults to 100, capped at 1000; limit=0 or until<since returns empty without SQL.
- **Side effects:** Audit log write: record_admin_audit("audit_query", "/admin/v1/audit")
- **Expected UI:** Updates URL search params since and until and re-runs audit query
- **Runtime result:** `PENDING`

### [UI-AUD-04] Audit — Clear Filter Bar Filters

- **Entry type:** `ui_action`
- **Atomic requests:** `REQ-SRC-CAE9CDF1B8F6`
- **Source:** `crates/cc-lb-admin/web/src/routes/audit.tsx:279#AuditPage`
- **Preconditions:** activeFilterCount > 0 (principal_id, since, or until set)
- **Steps:** Click Clear button when activeFilterCount > 0
- **Scope:** `once` — Single execution
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Resets all search parameters, returns to unfiltered state, re-runs query
- **Runtime result:** `PENDING`

### [REQ-SRC-CAE9CDF1B8F6] Audit — Clear Filter Bar Filters — useAudit

- **Entry type:** `network_request`
- **Parent action:** `UI-AUD-04`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:724#useAudit`
- **Preconditions:** activeFilterCount > 0 (principal_id, since, or until set)
- **Steps:** Click Clear button when activeFilterCount > 0
- **Scope:** `once` — Single execution
- **Risk:** `read_with_audit`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/audit`
  - Query: `limit=200`
  - Body: None
  - Headers: None
- **Handler:** `query_audit`
- **Storage operations:** `sqlite:AuditStore::query_recent_audit:SqliteStorage::query_recent_audit`, `postgres:AuditStore::query_recent_audit:PostgresStorage::query_recent_audit`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Single storage dispatch: actor_authority+actor_subject selects AuditQueryScope::Actor, principal_id selects AuditQueryScope::Principal, otherwise AuditQueryScope::All; all scopes call AuditStore::query_recent_audit, which applies the scope filter and since/until bounds before LIMIT. since is inclusive; after is an exclusive lower bound converted to since = max(since, after+1). Results are newest-first: ORDER BY ts DESC with id DESC (SQLite) / seq DESC (Postgres) tie-break. limit defaults to 100, capped at 1000; limit=0 or until<since returns empty without SQL.
- **Side effects:** Audit log write: record_admin_audit("audit_query", "/admin/v1/audit")
- **Expected UI:** Resets all search parameters, returns to unfiltered state, re-runs query
- **Runtime result:** `PENDING`

### [UI-AUD-09] Audit — Clear Filters from Empty State Action

- **Entry type:** `ui_action`
- **Atomic requests:** `REQ-SRC-91FBA8F4830F`
- **Source:** `crates/cc-lb-admin/web/src/routes/audit.tsx:432#AuditPage`
- **Preconditions:** audit.data loaded with 0 rows && activeFilterCount > 0
- **Steps:** Click 'Clear filters' primary button in EmptyState
- **Scope:** `once` — Single execution
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Clears filters and reloads unfiltered audit entries
- **Runtime result:** `PENDING`

### [REQ-SRC-91FBA8F4830F] Audit — Clear Filters from Empty State Action — useAudit

- **Entry type:** `network_request`
- **Parent action:** `UI-AUD-09`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:724#useAudit`
- **Preconditions:** audit.data loaded with 0 rows && activeFilterCount > 0
- **Steps:** Click 'Clear filters' primary button in EmptyState
- **Scope:** `once` — Single execution
- **Risk:** `read_with_audit`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/audit`
  - Query: `limit=200`
  - Body: None
  - Headers: None
- **Handler:** `query_audit`
- **Storage operations:** `sqlite:AuditStore::query_recent_audit:SqliteStorage::query_recent_audit`, `postgres:AuditStore::query_recent_audit:PostgresStorage::query_recent_audit`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Single storage dispatch: actor_authority+actor_subject selects AuditQueryScope::Actor, principal_id selects AuditQueryScope::Principal, otherwise AuditQueryScope::All; all scopes call AuditStore::query_recent_audit, which applies the scope filter and since/until bounds before LIMIT. since is inclusive; after is an exclusive lower bound converted to since = max(since, after+1). Results are newest-first: ORDER BY ts DESC with id DESC (SQLite) / seq DESC (Postgres) tie-break. limit defaults to 100, capped at 1000; limit=0 or until<since returns empty without SQL.
- **Side effects:** Audit log write: record_admin_audit("audit_query", "/admin/v1/audit")
- **Expected UI:** Clears filters and reloads unfiltered audit entries
- **Runtime result:** `PENDING`

### [UI-AUD-06] Audit — Open Audit Detail Modal

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/routes/audit.tsx:360#AuditPage`
- **Preconditions:** Table has rendered rows
- **Steps:** Click any row in the audit table
- **Scope:** `each_row` — Each rendered audit row
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Opens modal titled 'Audit entry {request_id}' showing Summary and Raw payload
- **Client-only reason:** Client-only modal state setSelected(e)
- **Runtime result:** `PENDING`

### [UI-AUD-10] Audit — Close Audit Detail Modal

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/routes/audit.tsx:455#AuditPage`
- **Preconditions:** Audit detail modal is open (selected != null)
- **Steps:** Click Close button, press Escape, or click backdrop
- **Scope:** `each_row` — Each opened modal
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Closes modal dialog
- **Client-only reason:** Client-only modal state setSelected(null)
- **Runtime result:** `PENDING`

### [UI-SRC-BDC81AB9B863] Upstreams — Upstreams Master List Mount & Polling

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-UP-01`
- **Source:** `crates/cc-lb-admin/web/src/routes/upstreams.tsx:120#UpstreamsPage`
- **Preconditions:** Authenticated admin session
- **Steps:** Navigate to /upstreams → Hook useUpstreams executes on mount and polls every 15s
- **Scope:** `once` — Single page execution
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Renders upstream sidebar items with name, kind badge, status dot, and quota/usage summaries
- **Runtime result:** `PENDING`

### [UI-UP-01] Upstreams — Upstreams Master List Mount & Polling — useUpstreams

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-BDC81AB9B863`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:418#useUpstreams`
- **Preconditions:** Authenticated admin session
- **Steps:** Navigate to /upstreams → Hook useUpstreams executes on mount and polls every 15s
- **Scope:** `once` — Single page execution
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/upstreams`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `list_upstreams`
- **Storage operations:** `sqlite:UpstreamStore::list:SqliteStorage::list`, `postgres:UpstreamStore::list:PostgresStorage::list`
- **Cache / no-query path:** Sets X-Total-Count header; in-memory keyset pagination on ?after (UUID) UpstreamResponse now serializes base_url: Option<Url> (crates/cc-lb-admin/src/v1/upstreams.rs:167,1587), so the field is present and null when the upstream has no custom base URL.
- **Side effects:** None
- **Expected UI:** Renders upstream sidebar items with name, kind badge, status dot, and quota/usage summaries
- **Runtime result:** `PENDING`

### [UI-SRC-3FC895176F17] Upstreams — Subscription Quota Latest Polling

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-UP-02`
- **Source:** `crates/cc-lb-admin/web/src/routes/upstreams.tsx:128#UpstreamsPage`
- **Preconditions:** Upstreams loaded
- **Steps:** UpstreamsPage mounts with visible upstreams → useSubscriptionQuotaLatest queries merged snapshots for all upstreams every 5s
- **Scope:** `once` — Single page execution
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Populates sidebar mini-meters and detail card quota snapshot grid
- **Runtime result:** `PENDING`

### [UI-UP-02] Upstreams — Subscription Quota Latest Polling — useSubscriptionQuotaLatest

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-3FC895176F17`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:807#useSubscriptionQuotaLatest`
- **Preconditions:** Upstreams loaded
- **Steps:** UpstreamsPage mounts with visible upstreams → useSubscriptionQuotaLatest queries merged snapshots for all upstreams every 5s
- **Scope:** `once` — Single page execution
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/subscription-quotas/latest`
  - Query: `upstream_ids`, `windows`, `source`
  - Body: None
  - Headers: None
- **Handler:** `handle_latest`
- **Storage operations:** `sqlite:UpstreamStore::list:SqliteStorage::list`, `postgres:UpstreamStore::list:PostgresStorage::list`
- **Cache / no-query path:** Reads upstream records for names/filtering; quota snapshots come from the in-memory dynamic subscription-quota cache.
- **Side effects:** None
- **Expected UI:** Populates sidebar mini-meters and detail card quota snapshot grid
- **Runtime result:** `PENDING`

### [UI-SRC-F25206CE1E1B] Upstreams — Upstream 7d Usage Summary Query

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-UP-03`
- **Source:** `crates/cc-lb-admin/web/src/routes/upstreams.tsx:135#UpstreamsPage`
- **Preconditions:** Page mounted
- **Steps:** UpstreamsPage mounts → useUsage queries 7d hourly usage grouped by upstream
- **Scope:** `once` — Single page execution
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Renders aggregate cost and token count under non-OAuth upstreams in sidebar
- **Runtime result:** `PENDING`

### [UI-UP-03] Upstreams — Upstream 7d Usage Summary Query — useUsage

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-F25206CE1E1B`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:382#useUsage`
- **Preconditions:** Page mounted
- **Steps:** UpstreamsPage mounts → useUsage queries 7d hourly usage grouped by upstream
- **Scope:** `once` — Single page execution
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/usage`
  - Query: `range`, `step`, `group_by`, `projection`
  - Body: None
  - Headers: `if-none-match`
- **Handler:** `handle_dashboard_usage`
- **Storage operations:** `sqlite:UsageRollupStore::usage_rollup_checkpoint:SqliteStorage::usage_rollup_checkpoint`, `postgres:UsageRollupStore::usage_rollup_checkpoint:PostgresStorage::usage_rollup_checkpoint`, `sqlite:UsageRollupStore::query_usage_rollups_in_range:SqliteStorage::query_usage_rollups_in_range`, `postgres:UsageRollupStore::query_usage_rollups_in_range:PostgresStorage::query_usage_rollups_in_range`, `sqlite:RequestEventStore::request_event_principal_costs:SqliteStorage::request_event_principal_costs`, `postgres:RequestEventStore::request_event_principal_costs:PostgresStorage::request_event_principal_costs`
- **Cache / no-query path:** Principal totals projection uses a short-TTL single-flight cache; other branches use checkpoint-based weak ETag/304. Body construction reads usage rollups and conditionally principal costs for group_by=principal.
- **Side effects:** None
- **Expected UI:** Renders aggregate cost and token count under non-OAuth upstreams in sidebar
- **Runtime result:** `PENDING`

### [UI-SRC-E4C913E07FEC] Upstreams — Runtime Status Query

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-UP-04`
- **Source:** `crates/cc-lb-admin/web/src/routes/upstreams.tsx:150#UpstreamsPage`
- **Preconditions:** Page mounted
- **Steps:** UpstreamsPage mounts → useStatus queries runtime status for all upstreams every 15s
- **Scope:** `once` — Single page execution
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Color-coded dot next to upstream name representing runtime status
- **Runtime result:** `PENDING`

### [UI-UP-04] Upstreams — Runtime Status Query — useStatus

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-E4C913E07FEC`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:361#useStatus`
- **Preconditions:** Page mounted
- **Steps:** UpstreamsPage mounts → useStatus queries runtime status for all upstreams every 15s
- **Scope:** `once` — Single page execution
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/status`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `status`
- **Storage operations:** `sqlite:UpstreamStore::list:SqliteStorage::list`, `postgres:UpstreamStore::list:PostgresStorage::list`
- **Cache / no-query path:** DB query for upstream names/IDs combined with in-memory DynamicViewHolder.replica_identity and build metadata
- **Side effects:** None
- **Expected UI:** Color-coded dot next to upstream name representing runtime status
- **Runtime result:** `PENDING`

### [UI-UP-05] Upstreams — Select Upstream in Sidebar

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/routes/upstreams.tsx:273#UpstreamsPage`
- **Preconditions:** Upstreams list rendered
- **Steps:** Click on an upstream item row in the sidebar
- **Scope:** `each_row` — Each visible upstream in sidebar
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Updates search parameters ?selectedId={id}, mounts DetailView for the selected upstream
- **Client-only reason:** Client-side TanStack router navigation updating selectedId search param
- **Runtime result:** `PENDING`

### [UI-SRC-CD87608DA5D5] Upstreams — Upstream Subscription Metadata Query

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-UP-06`
- **Source:** `crates/cc-lb-admin/web/src/routes/upstreams.tsx:801#DetailView`
- **Preconditions:** Upstream kind is anthropic_oauth
- **Steps:** Select an OAuth upstream → useUpstreamSubscriptionMetadata fetches metadata on mount and polls every 15s
- **Scope:** `each_upstream` — Each selected OAuth upstream
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Displays metadata chips, tooltips, and subscription status banners
- **Runtime result:** `PENDING`

### [UI-UP-06] Upstreams — Upstream Subscription Metadata Query — useUpstreamSubscriptionMetadata

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-CD87608DA5D5`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:749#useUpstreamSubscriptionMetadata`
- **Preconditions:** Upstream kind is anthropic_oauth
- **Steps:** Select an OAuth upstream → useUpstreamSubscriptionMetadata fetches metadata on mount and polls every 15s
- **Scope:** `each_upstream` — Each selected OAuth upstream
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/upstreams/{id}/subscription-metadata`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `get_upstream_subscription_metadata`
- **Storage operations:** `sqlite:UpstreamSubscriptionMetadataStore::get_upstream_subscription_metadata:SqliteStorage::get_upstream_subscription_metadata`, `postgres:UpstreamSubscriptionMetadataStore::get_upstream_subscription_metadata:PostgresStorage::get_upstream_subscription_metadata`
- **Cache / no-query path:** Direct single-row metadata retrieval
- **Side effects:** None
- **Expected UI:** Displays metadata chips, tooltips, and subscription status banners
- **Runtime result:** `PENDING`

### [UI-SRC-1EEBA1898F34] Upstreams — Trigger Subscription Metadata Refresh

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-UP-07`
- **Source:** `crates/cc-lb-admin/web/src/routes/upstreams.tsx:1280#DetailView`
- **Preconditions:** OAuth upstream selected; Metadata strip visible
- **Steps:** Click the RefreshCw icon button in the metadata strip
- **Scope:** `each_upstream` — Each selected OAuth upstream
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **Expected UI:** Triggers background sync with Anthropic API, updates cached metadata, displays success toast
- **Runtime result:** `PENDING`

### [UI-UP-07] Upstreams — Trigger Subscription Metadata Refresh — useTriggerSubscriptionMetadataRefresh

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-1EEBA1898F34`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:768#useTriggerSubscriptionMetadataRefresh`
- **Preconditions:** OAuth upstream selected; Metadata strip visible
- **Steps:** Click the RefreshCw icon button in the metadata strip
- **Scope:** `each_upstream` — Each selected OAuth upstream
- **Risk:** `external_action`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/v1/upstreams/{id}/subscription-metadata/refresh`
  - Query: None
  - Body: None
  - Headers: `authorization`
- **Handler:** `refresh_upstream_subscription_metadata`
- **Storage operations:** `sqlite:UpstreamStore::complete_refresh:SqliteStorage::complete_refresh`, `postgres:UpstreamStore::complete_refresh:PostgresStorage::complete_refresh`, `sqlite:UpstreamSubscriptionMetadataStore::put_upstream_subscription_metadata:SqliteStorage::put_upstream_subscription_metadata`, `postgres:UpstreamSubscriptionMetadataStore::put_upstream_subscription_metadata:PostgresStorage::put_upstream_subscription_metadata`, `sqlite:OrganizationMetadataStore::put_organization_metadata:SqliteStorage::put_organization_metadata`, `postgres:OrganizationMetadataStore::put_organization_metadata:PostgresStorage::put_organization_metadata`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Invalidates cached subscription metadata; invalidates queryKey ['subscription-metadata', upstreamId]
- **Side effects:** External outbound HTTP call to Anthropic API using decrypted OAuth bearer token; updates subscription tiers & quotas; audit event `upstream_subscription_metadata_refresh` QA restore: restore prior subscription/organization metadata fixture; external provider reads and any token refresh cannot be undone. If the access token is within the refresh lookahead, LazyOAuthRefresher may persist rotated encrypted tokens through UpstreamStore::complete_refresh before fetching metadata.
- **Expected UI:** Triggers background sync with Anthropic API, updates cached metadata, displays success toast
- **Runtime result:** `PENDING`

### [UI-SRC-224914630CC5] Upstreams — Subscription Quota Series Query

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-UP-08A`
- **Source:** `crates/cc-lb-admin/web/src/routes/upstreams.tsx:862#DetailView`
- **Preconditions:** OAuth upstream selected
- **Steps:** Select an OAuth upstream or change quota range
- **Scope:** `each_upstream` — Each selected OAuth upstream
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Renders area curves for 5h, 7d, 7d_sonnet, 7d_opus, 7d_fable, and overage
- **Runtime result:** `PENDING`

### [UI-UP-08A] Upstreams — Subscription Quota Series Query — useSubscriptionQuotaSeries

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-224914630CC5`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:838#useSubscriptionQuotaSeries`
- **Preconditions:** OAuth upstream selected
- **Steps:** Select an OAuth upstream or change quota range
- **Scope:** `each_upstream` — Each selected OAuth upstream
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/subscription-quotas/series`
  - Query: `upstream_ids`, `windows`, `source`, `range_secs`, `bucket_secs`
  - Body: None
  - Headers: None
- **Handler:** `handle_series`
- **Storage operations:** `sqlite:UpstreamStore::list:SqliteStorage::list`, `postgres:UpstreamStore::list:PostgresStorage::list`, `sqlite:UpstreamSubscriptionQuotaAggregateStore::list_subscription_quota_slim_checkpoints:SqliteStorage::list_subscription_quota_slim_checkpoints`, `postgres:UpstreamSubscriptionQuotaAggregateStore::list_subscription_quota_slim_checkpoints:PostgresStorage::list_subscription_quota_slim_checkpoints`
- **Cache / no-query path:** Reads upstream records and slim subscription-quota checkpoints through UpstreamSubscriptionQuotaAggregateStore::list_subscription_quota_slim_checkpoints; the admin-local series helper builds buckets in memory. Empty upstream/window/source sets short-circuit without checkpoint SQL.
- **Side effects:** None
- **Expected UI:** Renders area curves for 5h, 7d, 7d_sonnet, 7d_opus, 7d_fable, and overage
- **Runtime result:** `PENDING`

### [UI-SRC-2351FFDF6073] Upstreams — Subscription Quota Analysis Query

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-UP-08B`
- **Source:** `crates/cc-lb-admin/web/src/routes/upstreams.tsx:869#DetailView`
- **Preconditions:** OAuth upstream selected
- **Steps:** Select an OAuth upstream or change quota range
- **Scope:** `each_upstream` — Each selected OAuth upstream
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Fills in snapshot burn stats, deficit multiplier, shortfall tokens, and caveat bullets
- **Runtime result:** `PENDING`

### [UI-UP-08B] Upstreams — Subscription Quota Analysis Query — useSubscriptionQuotaAnalysis

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-2351FFDF6073`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:882#useSubscriptionQuotaAnalysis`
- **Preconditions:** OAuth upstream selected
- **Steps:** Select an OAuth upstream or change quota range
- **Scope:** `each_upstream` — Each selected OAuth upstream
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/subscription-quotas/analysis`
  - Query: `upstream_ids`, `windows`, `source`, `range_secs`
  - Body: None
  - Headers: None
- **Handler:** `handle_analysis`
- **Storage operations:** `sqlite:UpstreamStore::list:SqliteStorage::list`, `postgres:UpstreamStore::list:PostgresStorage::list`, `sqlite:UsageRollupStore::query_usage_rollups_for_upstreams_in_range:SqliteStorage::query_usage_rollups_for_upstreams_in_range`, `postgres:UsageRollupStore::query_usage_rollups_for_upstreams_in_range:PostgresStorage::query_usage_rollups_for_upstreams_in_range`, `sqlite:UpstreamSubscriptionQuotaAggregateStore::list_subscription_quota_slim_checkpoints:SqliteStorage::list_subscription_quota_slim_checkpoints`, `postgres:UpstreamSubscriptionQuotaAggregateStore::list_subscription_quota_slim_checkpoints:PostgresStorage::list_subscription_quota_slim_checkpoints`
- **Cache / no-query path:** Reads upstreams, usage rollups, and slim subscription-quota checkpoints through the admin-local series helper; latest quota state comes from the in-memory dynamic cache.
- **Side effects:** None
- **Expected UI:** Fills in snapshot burn stats, deficit multiplier, shortfall tokens, and caveat bullets
- **Runtime result:** `PENDING`

### [UI-UP-09] Upstreams — Toggle Quota History Range

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-UP-09A`, `UI-UP-09B`
- **Source:** `crates/cc-lb-admin/web/src/routes/upstreams.tsx:1335#DetailView`
- **Preconditions:** OAuth upstream selected
- **Steps:** Click 1h, 6h, 24h, or 7d button in Quota History header
- **Scope:** `each_upstream` — Each selected OAuth upstream × 4 ranges
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Updates range state, recalculates bucketSecs and rangeSecs, and refetches series and analysis
- **Runtime result:** `PENDING`

### [UI-UP-09A] Upstreams — Toggle Quota History Range — useSubscriptionQuotaSeries

- **Entry type:** `network_request`
- **Parent action:** `UI-UP-09`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:838#useSubscriptionQuotaSeries`
- **Preconditions:** OAuth upstream selected
- **Steps:** Click 1h, 6h, 24h, or 7d button in Quota History header
- **Scope:** `each_upstream` — Each selected OAuth upstream × 4 ranges
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/subscription-quotas/series`
  - Query: `upstream_ids`, `windows`, `source`, `range_secs`, `bucket_secs`
  - Body: None
  - Headers: None
- **Handler:** `handle_series`
- **Storage operations:** `sqlite:UpstreamStore::list:SqliteStorage::list`, `postgres:UpstreamStore::list:PostgresStorage::list`, `sqlite:UpstreamSubscriptionQuotaAggregateStore::list_subscription_quota_slim_checkpoints:SqliteStorage::list_subscription_quota_slim_checkpoints`, `postgres:UpstreamSubscriptionQuotaAggregateStore::list_subscription_quota_slim_checkpoints:PostgresStorage::list_subscription_quota_slim_checkpoints`
- **Cache / no-query path:** Reads upstream records and slim subscription-quota checkpoints through UpstreamSubscriptionQuotaAggregateStore::list_subscription_quota_slim_checkpoints; the admin-local series helper builds buckets in memory. Empty upstream/window/source sets short-circuit without checkpoint SQL.
- **Side effects:** None
- **Expected UI:** Updates range state, recalculates bucketSecs and rangeSecs, and refetches series and analysis
- **Runtime result:** `PENDING`

### [UI-UP-09B] Upstreams — Toggle Quota History Range — useSubscriptionQuotaAnalysis

- **Entry type:** `network_request`
- **Parent action:** `UI-UP-09`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:882#useSubscriptionQuotaAnalysis`
- **Preconditions:** OAuth upstream selected
- **Steps:** Click 1h, 6h, 24h, or 7d button in Quota History header
- **Scope:** `each_upstream` — Each selected OAuth upstream × 4 ranges
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/subscription-quotas/analysis`
  - Query: `upstream_ids`, `windows`, `source`, `range_secs`
  - Body: None
  - Headers: None
- **Handler:** `handle_analysis`
- **Storage operations:** `sqlite:UpstreamStore::list:SqliteStorage::list`, `postgres:UpstreamStore::list:PostgresStorage::list`, `sqlite:UsageRollupStore::query_usage_rollups_for_upstreams_in_range:SqliteStorage::query_usage_rollups_for_upstreams_in_range`, `postgres:UsageRollupStore::query_usage_rollups_for_upstreams_in_range:PostgresStorage::query_usage_rollups_for_upstreams_in_range`, `sqlite:UpstreamSubscriptionQuotaAggregateStore::list_subscription_quota_slim_checkpoints:SqliteStorage::list_subscription_quota_slim_checkpoints`, `postgres:UpstreamSubscriptionQuotaAggregateStore::list_subscription_quota_slim_checkpoints:PostgresStorage::list_subscription_quota_slim_checkpoints`
- **Cache / no-query path:** Reads upstreams, usage rollups, and slim subscription-quota checkpoints through the admin-local series helper; latest quota state comes from the in-memory dynamic cache.
- **Side effects:** None
- **Expected UI:** Updates range state, recalculates bucketSecs and rangeSecs, and refetches series and analysis
- **Runtime result:** `PENDING`

### [UI-UP-10] Upstreams — Isolate Quota Window in Legend

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/routes/upstreams.tsx:1640#DetailView`
- **Preconditions:** Quota chart rendered with series data
- **Steps:** Click a window badge (e.g. 5h, 7d) in the Quota History legend slot below chart
- **Scope:** `each_row` — Each visible window in chart legend
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Dimmens non-selected legend items and hides non-isolated Area curves
- **Client-only reason:** Client state setIsolatedWindow toggle
- **Runtime result:** `PENDING`

### [UI-SRC-0D929B471D7A] Upstreams — Rename Upstream

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-UP-11`
- **Source:** `crates/cc-lb-admin/web/src/components/upstreams/InlineNameEditor.tsx:31#InlineNameEditor`
- **Preconditions:** Upstream detail pane open
- **Steps:** Click upstream name in DetailView header to enter editing mode (resets the save latch and seeds the input with the current name) → Type new unique name → Press Enter or blur the input to submit once (saveStartedRef latch makes Enter+blur a single mutation) → Or press Escape to cancel without a request
- **Scope:** `each_upstream` — Each upstream
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **Expected UI:** Updates upstream name across sidebar and header; shows success toast 'Name updated'; on error (e.g. 409 stale revision or name conflict) the name reverts and editing exits
- **Runtime result:** `PENDING`

### [UI-UP-11] Upstreams — Rename Upstream — useUpdateUpstream

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-0D929B471D7A`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1052#useUpdateUpstream`
- **Preconditions:** Upstream detail pane open
- **Steps:** Click upstream name in DetailView header to enter editing mode (resets the save latch and seeds the input with the current name) → Type new unique name → Press Enter or blur the input to submit once (saveStartedRef latch makes Enter+blur a single mutation) → Or press Escape to cancel without a request
- **Scope:** `each_upstream` — Each upstream
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `PUT /admin/v1/upstreams/{id}`
  - Query: None
  - Body: `name`
  - Headers: `authorization`, `if-match`, `content-type`
- **Handler:** `update_upstream`
- **Storage operations:** `sqlite:UpstreamStore::update:SqliteStorage::update`, `postgres:UpstreamStore::update:PostgresStorage::update`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Rebinds DynamicViewHolder; invalidates queryKey ['upstreams'] UpstreamResponse now serializes base_url: Option<Url> (crates/cc-lb-admin/src/v1/upstreams.rs:167,1586), so the field is present and null when the upstream has no custom base URL; credential secret and api_key_env provenance are not exposed in the response.
- **Side effects:** Audit event `upstream_update`; replaces upstream base_url, warmup config, or credentials QA restore: repeat the update with captured prior fields and the returned revision.
- **Expected UI:** Updates upstream name across sidebar and header; shows success toast 'Name updated'; on error (e.g. 409 stale revision or name conflict) the name reverts and editing exits
- **Runtime result:** `PENDING`

### [UI-SRC-C72B3D4F216B] Upstreams — Toggle Upstream Enabled Switch

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-UP-12`
- **Source:** `crates/cc-lb-admin/web/src/routes/upstreams.tsx:1070#DetailView`
- **Preconditions:** Upstream detail pane open
- **Steps:** Click the Enabled/Disabled switch in DetailView header
- **Scope:** `each_upstream` — Each upstream
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **Expected UI:** Toggles upstream status; shows success toast; triggers dynamic rebind in proxy engine
- **Runtime result:** `PENDING`

### [UI-UP-12] Upstreams — Toggle Upstream Enabled Switch — useUpdateUpstreamWarmupSettings

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-C72B3D4F216B`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1128#useUpdateUpstreamWarmupSettings`
- **Preconditions:** Upstream detail pane open
- **Steps:** Click the Enabled/Disabled switch in DetailView header
- **Scope:** `each_upstream` — Each upstream
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `PATCH /admin/v1/upstreams/{id}`
  - Query: None
  - Body: `enabled`, `warmup_enabled`
  - Headers: `authorization`, `if-match`, `content-type`
- **Handler:** `update_upstream`
- **Storage operations:** `sqlite:UpstreamStore::update:SqliteStorage::update`, `postgres:UpstreamStore::update:PostgresStorage::update`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Rebinds DynamicViewHolder; invalidates queryKey ['upstreams'] and ['warmup', 'summary'] UpstreamResponse now serializes base_url: Option<Url> (crates/cc-lb-admin/src/v1/upstreams.rs:167,1586), so the field is present and null when the upstream has no custom base URL; credential secret and api_key_env provenance are not exposed in the response.
- **Side effects:** Audit event `upstream_update`; partially updates upstream fields (e.g. toggles warmup_enabled) QA restore: repeat the update with captured prior fields and the returned revision.
- **Expected UI:** Toggles upstream status; shows success toast; triggers dynamic rebind in proxy engine
- **Runtime result:** `PENDING`

### [UI-SRC-9FD3FEC04C3D] Upstreams — Update Non-OAuth Upstream Settings

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-UP-13`
- **Source:** `crates/cc-lb-admin/web/src/components/upstreams/SettingsCard.tsx:34#SettingsCard`
- **Preconditions:** Non-OAuth upstream selected
- **Steps:** Click 'Edit' in SettingsCard → Modify Base URL or toggle API Key mode (env var vs literal value) → Click 'Save'
- **Scope:** `each_upstream` — Each non-OAuth upstream
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **Expected UI:** Exits edit mode and shows success toast; displays updated Base URL ('—' when base_url is null) and API Key source. In practice the API Key row always reads 'literal value (stored)': the 'env:{name}' branch is source-only/unreachable because UpstreamResponse omits api_key_env.
- **Runtime result:** `PENDING`

### [UI-UP-13] Upstreams — Update Non-OAuth Upstream Settings — useUpdateUpstream

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-9FD3FEC04C3D`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1052#useUpdateUpstream`
- **Preconditions:** Non-OAuth upstream selected
- **Steps:** Click 'Edit' in SettingsCard → Modify Base URL or toggle API Key mode (env var vs literal value) → Click 'Save'
- **Scope:** `each_upstream` — Each non-OAuth upstream
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `PUT /admin/v1/upstreams/{id}`
  - Query: None
  - Body: `base_url`, `api_key_value`, `api_key_env`
  - Headers: `authorization`, `if-match`, `content-type`
- **Handler:** `update_upstream`
- **Storage operations:** `sqlite:UpstreamStore::update:SqliteStorage::update`, `postgres:UpstreamStore::update:PostgresStorage::update`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Rebinds DynamicViewHolder; invalidates queryKey ['upstreams'] UpstreamResponse now serializes base_url: Option<Url> (crates/cc-lb-admin/src/v1/upstreams.rs:167,1586), so the field is present and null when the upstream has no custom base URL; credential secret and api_key_env provenance are not exposed in the response.
- **Side effects:** Audit event `upstream_update`; replaces upstream base_url, warmup config, or credentials QA restore: repeat the update with captured prior fields and the returned revision.
- **Expected UI:** Exits edit mode and shows success toast; displays updated Base URL ('—' when base_url is null) and API Key source. In practice the API Key row always reads 'literal value (stored)': the 'env:{name}' branch is source-only/unreachable because UpstreamResponse omits api_key_env.
- **Runtime result:** `PENDING`

### [UI-SRC-7F9359EA0052] Upstreams — Toggle API Usage Range (24h / 7d)

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-UP-14`
- **Source:** `crates/cc-lb-admin/web/src/components/upstreams/ApiUsageCard.tsx:89#ApiUsageCard`
- **Preconditions:** Non-OAuth upstream selected
- **Steps:** Click 24h or 7d toggle button in ApiUsageCard
- **Scope:** `each_upstream` — Each non-OAuth upstream
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Triggers useUsage query for model breakdown and updates chart x-axis domain
- **Runtime result:** `PENDING`

### [UI-UP-14] Upstreams — Toggle API Usage Range (24h / 7d) — useUsage

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-7F9359EA0052`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:382#useUsage`
- **Preconditions:** Non-OAuth upstream selected
- **Steps:** Click 24h or 7d toggle button in ApiUsageCard
- **Scope:** `each_upstream` — Each non-OAuth upstream
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/usage`
  - Query: `range`, `step`, `group_by`, `upstream_id`
  - Body: None
  - Headers: `if-none-match`
- **Handler:** `handle_dashboard_usage`
- **Storage operations:** `sqlite:UsageRollupStore::usage_rollup_checkpoint:SqliteStorage::usage_rollup_checkpoint`, `postgres:UsageRollupStore::usage_rollup_checkpoint:PostgresStorage::usage_rollup_checkpoint`, `sqlite:UsageRollupStore::query_usage_rollups_in_range:SqliteStorage::query_usage_rollups_in_range`, `postgres:UsageRollupStore::query_usage_rollups_in_range:PostgresStorage::query_usage_rollups_in_range`, `sqlite:RequestEventStore::request_event_principal_costs:SqliteStorage::request_event_principal_costs`, `postgres:RequestEventStore::request_event_principal_costs:PostgresStorage::request_event_principal_costs`
- **Cache / no-query path:** Principal totals projection uses a short-TTL single-flight cache; other branches use checkpoint-based weak ETag/304. Body construction reads usage rollups and conditionally principal costs for group_by=principal.
- **Side effects:** None
- **Expected UI:** Triggers useUsage query for model breakdown and updates chart x-axis domain
- **Runtime result:** `PENDING`

### [UI-SRC-6D1976AC76E2] Upstreams — Toggle Warmup Enabled

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-UP-15`
- **Source:** `crates/cc-lb-admin/web/src/components/upstreams/warmup/WarmupCardMinimal.tsx:196#WarmupCardMinimalInner`
- **Preconditions:** OAuth upstream selected
- **Steps:** Click the Warmup switch in WarmupCardMinimal header, or click 'Enable warmup' button in disabled banner
- **Scope:** `each_upstream` — Each OAuth upstream
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **Expected UI:** Updates warmup_enabled; displays success toast; schedules or deschedules background warmup loop
- **Runtime result:** `PENDING`

### [UI-UP-15] Upstreams — Toggle Warmup Enabled — useUpdateUpstreamWarmupSettings

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-6D1976AC76E2`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1128#useUpdateUpstreamWarmupSettings`
- **Preconditions:** OAuth upstream selected
- **Steps:** Click the Warmup switch in WarmupCardMinimal header, or click 'Enable warmup' button in disabled banner
- **Scope:** `each_upstream` — Each OAuth upstream
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `PATCH /admin/v1/upstreams/{id}`
  - Query: None
  - Body: `warmup_enabled`
  - Headers: `authorization`, `if-match`, `content-type`
- **Handler:** `update_upstream`
- **Storage operations:** `sqlite:UpstreamStore::update:SqliteStorage::update`, `postgres:UpstreamStore::update:PostgresStorage::update`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Rebinds DynamicViewHolder; invalidates queryKey ['upstreams'] and ['warmup', 'summary'] UpstreamResponse now serializes base_url: Option<Url> (crates/cc-lb-admin/src/v1/upstreams.rs:167,1586), so the field is present and null when the upstream has no custom base URL; credential secret and api_key_env provenance are not exposed in the response.
- **Side effects:** Audit event `upstream_update`; partially updates upstream fields (e.g. toggles warmup_enabled) QA restore: repeat the update with captured prior fields and the returned revision.
- **Expected UI:** Updates warmup_enabled; displays success toast; schedules or deschedules background warmup loop
- **Runtime result:** `PENDING`

### [UI-SRC-1B467A3726A3] Upstreams — Select Warmup Shape Plugin

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-UP-16`
- **Source:** `crates/cc-lb-admin/web/src/components/upstreams/warmup/WarmupCardMinimal.tsx:226#WarmupCardMinimalInner`
- **Preconditions:** OAuth upstream selected; Registered shape plugins available
- **Steps:** Select a shape plugin from the Shape plugin dropdown
- **Scope:** `each_upstream` — Each OAuth upstream × shape plugins
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **Expected UI:** Associates plugin wasm registry ID with upstream warmup; shows success toast
- **Runtime result:** `PENDING`

### [UI-UP-16] Upstreams — Select Warmup Shape Plugin — useUpdateUpstreamWarmupSettings

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-1B467A3726A3`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1128#useUpdateUpstreamWarmupSettings`
- **Preconditions:** OAuth upstream selected; Registered shape plugins available
- **Steps:** Select a shape plugin from the Shape plugin dropdown
- **Scope:** `each_upstream` — Each OAuth upstream × shape plugins
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `PATCH /admin/v1/upstreams/{id}`
  - Query: None
  - Body: `warmup_dialect_plugin`
  - Headers: `authorization`, `if-match`, `content-type`
- **Handler:** `update_upstream`
- **Storage operations:** `sqlite:UpstreamStore::update:SqliteStorage::update`, `postgres:UpstreamStore::update:PostgresStorage::update`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Rebinds DynamicViewHolder; invalidates queryKey ['upstreams'] and ['warmup', 'summary'] UpstreamResponse now serializes base_url: Option<Url> (crates/cc-lb-admin/src/v1/upstreams.rs:167,1586), so the field is present and null when the upstream has no custom base URL; credential secret and api_key_env provenance are not exposed in the response.
- **Side effects:** Audit event `upstream_update`; partially updates upstream fields (e.g. toggles warmup_enabled) QA restore: repeat the update with captured prior fields and the returned revision.
- **Expected UI:** Associates plugin wasm registry ID with upstream warmup; shows success toast
- **Runtime result:** `PENDING`

### [UI-SRC-329B12DA3BC3] Upstreams — Clear Warmup Shape Plugin

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-UP-17`
- **Source:** `crates/cc-lb-admin/web/src/components/upstreams/warmup/WarmupCardMinimal.tsx:266#WarmupCardMinimalInner`
- **Preconditions:** Shape plugin currently attached
- **Steps:** Select 'None (default request shape)' in plugin dropdown → Confirm deletion in ConfirmDialog
- **Scope:** `each_upstream` — Each OAuth upstream with attached plugin
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **Expected UI:** Removes warmup dialect plugin; dropdown resets to None; shows success toast
- **Runtime result:** `PENDING`

### [UI-UP-17] Upstreams — Clear Warmup Shape Plugin — useClearUpstreamWarmupDialectPlugin

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-329B12DA3BC3`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1107#useClearUpstreamWarmupDialectPlugin`
- **Preconditions:** Shape plugin currently attached
- **Steps:** Select 'None (default request shape)' in plugin dropdown → Confirm deletion in ConfirmDialog
- **Scope:** `each_upstream` — Each OAuth upstream with attached plugin
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `DELETE /admin/v1/upstreams/{id}/warmup-dialect-plugin`
  - Query: None
  - Body: None
  - Headers: `authorization`, `if-match`
- **Handler:** `delete_upstream_warmup_dialect_plugin`
- **Storage operations:** `sqlite:UpstreamStore::clear_warmup_dialect_plugin:SqliteStorage::clear_warmup_dialect_plugin`, `postgres:UpstreamStore::clear_warmup_dialect_plugin:PostgresStorage::clear_warmup_dialect_plugin`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Rebinds DynamicViewHolder; invalidates queryKey ['upstreams'] and ['warmup', 'summary'] UpstreamResponse now serializes base_url: Option<Url> (crates/cc-lb-admin/src/v1/upstreams.rs:167,1586), so the field is present and null when the upstream has no custom base URL; credential secret and api_key_env provenance are not exposed in the response.
- **Side effects:** Audit event `upstream_update` (warmup_dialect_plugin); disassociates dialect plugin so default HTTP warmup is used QA restore: PUT the prior warmup_dialect_plugin with the returned revision.
- **Expected UI:** Removes warmup dialect plugin; dropdown resets to None; shows success toast
- **Runtime result:** `PENDING`

### [UI-SRC-C41372BCA4B0] Upstreams — Fire Warmup Now

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-UP-18`
- **Source:** `crates/cc-lb-admin/web/src/components/upstreams/warmup/WarmupCardMinimal.tsx:298#WarmupCardMinimalInner`
- **Preconditions:** Warmup enabled on upstream; Cooldown not active
- **Steps:** Click 'Fire now' button in WarmupCardMinimal → Click 'Fire now' in ConfirmDialog
- **Scope:** `each_upstream` — Each OAuth upstream
- **Risk:** `external_action`
- **Production applicability:** `available`
- **Expected UI:** Triggers immediate upstream priming, displays toast or warning/error panel
- **Runtime result:** `PENDING`

### [UI-UP-18] Upstreams — Fire Warmup Now — useFireNowUpstreamWarmup

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-C41372BCA4B0`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1075#useFireNowUpstreamWarmup`
- **Preconditions:** Warmup enabled on upstream; Cooldown not active
- **Steps:** Click 'Fire now' button in WarmupCardMinimal → Click 'Fire now' in ConfirmDialog
- **Scope:** `each_upstream` — Each OAuth upstream
- **Risk:** `external_action`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/v1/upstreams/{id}/warmup/fire-now`
  - Query: None
  - Body: None
  - Headers: `authorization`
- **Handler:** `fire_now_upstream_warmup`
- **Storage operations:** `sqlite:UpstreamWarmupAttemptStore::insert_warmup_attempt:SqliteStorage::insert_warmup_attempt`, `postgres:UpstreamWarmupAttemptStore::insert_warmup_attempt:PostgresStorage::insert_warmup_attempt`, `sqlite:UpstreamStore::set_status:SqliteStorage::set_status`, `postgres:UpstreamStore::set_status:PostgresStorage::set_status`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Invalidates queryKey ['warmup', 'summary', upstreamId] and ['warmup', 'attempts', upstreamId]
- **Side effects:** External outbound probe HTTP request to Anthropic API upstream messages endpoint (or dialect plugin execution); records warmup attempt in database; updates upstream_status_v1.last_warmup_at; audit event `upstream_warmup_fire_now` QA restore: remove the generated warmup attempt fixture row and restore prior upstream_status_v1 last_warmup_at; outbound probe cannot be undone.
- **Expected UI:** Triggers immediate upstream priming, displays toast or warning/error panel
- **Runtime result:** `PENDING`

### [UI-SRC-4851B3D30282] Upstreams — Open Warmup History Drawer

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-UP-19`
- **Source:** `crates/cc-lb-admin/web/src/components/upstreams/warmup/WarmupCardMinimal.tsx:331#WarmupCardMinimalInner`
- **Preconditions:** OAuth upstream selected
- **Steps:** Click 'History' button in WarmupCardMinimal, or click 'Last run' attempt outcome badge
- **Scope:** `each_upstream` — Each OAuth upstream
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Slides open WarmupHistoryDrawer and triggers useWarmupAttempts infinite query
- **Runtime result:** `PENDING`

### [UI-UP-19] Upstreams — Open Warmup History Drawer — useWarmupAttempts

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-4851B3D30282`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1782#useWarmupAttempts`
- **Preconditions:** OAuth upstream selected
- **Steps:** Click 'History' button in WarmupCardMinimal, or click 'Last run' attempt outcome badge
- **Scope:** `each_upstream` — Each OAuth upstream
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/upstreams/{id}/warmup/attempts`
  - Query: `status`, `limit`, `before`
  - Body: None
  - Headers: None
- **Handler:** `super::upstream_warmup::list_upstream_warmup_attempts`
- **Storage operations:** `sqlite:UpstreamStore::get_by_id:SqliteStorage::get_by_id`, `postgres:UpstreamStore::get_by_id:PostgresStorage::get_by_id`, `sqlite:UpstreamWarmupAttemptStore::list_warmup_attempts_for_upstream:SqliteStorage::list_warmup_attempts_for_upstream`, `postgres:UpstreamWarmupAttemptStore::list_warmup_attempts_for_upstream:PostgresStorage::list_warmup_attempts_for_upstream`
- **Cache / no-query path:** Cursor-based pagination (?before=cursor) and status filtering (?outcome/status)
- **Side effects:** None
- **Expected UI:** Slides open WarmupHistoryDrawer and triggers useWarmupAttempts infinite query
- **Runtime result:** `PENDING`

### [UI-UP-20] Upstreams — Select Warmup Attempt in Drawer

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/upstreams/warmup/WarmupHistoryDrawer.tsx:281#WarmupHistoryDrawer`
- **Preconditions:** Warmup history drawer open with attempt records
- **Steps:** Click an attempt row in the WarmupHistoryDrawer list
- **Scope:** `each_row` — Each loaded warmup attempt
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Opens AttemptDetail pane showing narrative, freshness, timing, replica, cycle, HTTP status
- **Client-only reason:** Client state setSelected(attempt) toggle
- **Runtime result:** `PENDING`

### [UI-SRC-49171C2D2202] Upstreams — Start OAuth Authorization Flow

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-UP-22`
- **Source:** `crates/cc-lb-admin/web/src/routes/upstreams.tsx:1928#DetailView`
- **Preconditions:** OAuth upstream selected
- **Steps:** Click 'Connect' or 'Reconnect' button in OAuth Status card
- **Scope:** `each_upstream` — Each OAuth upstream
- **Risk:** `external_action`
- **Production applicability:** `available`
- **Expected UI:** Opens OAuth Authorization Modal with authorize URL and state token
- **Runtime result:** `PENDING`

### [UI-UP-22] Upstreams — Start OAuth Authorization Flow — useOAuthStart

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-49171C2D2202`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1153#useOAuthStart`
- **Preconditions:** OAuth upstream selected
- **Steps:** Click 'Connect' or 'Reconnect' button in OAuth Status card
- **Scope:** `each_upstream` — Each OAuth upstream
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/v1/upstreams/{id}/oauth/start`
  - Query: None
  - Body: None
  - Headers: `authorization`, `content-type`
- **Handler:** `start_oauth`
- **Storage operations:** `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Inserts PKCE flow into memory pkce_flows targeting existing upstream ID; request extractor requires an empty JSON object body (`{}`) even though StartRequest has no fields
- **Side effects:** Audit event `upstream_oauth_start`; returns authorize_url, state_token, revision for re-auth QA restore: allow the in-memory PKCE flow to expire after 900 seconds or restart the process.
- **Expected UI:** Opens OAuth Authorization Modal with authorize URL and state token
- **Runtime result:** `PENDING`

### [UI-SRC-0D6108CB1DBC] Upstreams — Complete OAuth Token Exchange

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-UP-23`
- **Source:** `crates/cc-lb-admin/web/src/routes/upstreams.tsx:2212#DetailView`
- **Preconditions:** OAuth Authorization Modal open with valid state token
- **Steps:** Click 'Open authorization URL' to authenticate with Anthropic → Paste authorization code into modal input → Click 'Complete'
- **Scope:** `each_upstream` — Each OAuth upstream
- **Risk:** `external_action`
- **Production applicability:** `available`
- **Expected UI:** Exchanges code for access/refresh tokens with Anthropic; stores credentials; closes modal; shows success toast
- **Runtime result:** `PENDING`

### [UI-UP-23] Upstreams — Complete OAuth Token Exchange — useOAuthComplete

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-0D6108CB1DBC`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1162#useOAuthComplete`
- **Preconditions:** OAuth Authorization Modal open with valid state token
- **Steps:** Click 'Open authorization URL' to authenticate with Anthropic → Paste authorization code into modal input → Click 'Complete'
- **Scope:** `each_upstream` — Each OAuth upstream
- **Risk:** `external_action`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/v1/upstreams/{id}/oauth/complete`
  - Query: None
  - Body: `state_token`, `code`
  - Headers: `authorization`, `content-type`
- **Handler:** `complete_oauth`
- **Storage operations:** `sqlite:UpstreamStore::store_oauth_tokens:SqliteStorage::store_oauth_tokens`, `postgres:UpstreamStore::store_oauth_tokens:PostgresStorage::store_oauth_tokens`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Invalidates in-memory upstream credentials and TanStack queryKey ['upstreams'], ['subscription-metadata']
- **Side effects:** External HTTP request to Anthropic token and metadata endpoints; persists updated AEAD encrypted tokens; audit event `upstream_oauth_complete` QA restore: repeat OAuth authorization with the prior account or restore encrypted credentials from a fixture snapshot.
- **Expected UI:** Exchanges code for access/refresh tokens with Anthropic; stores credentials; closes modal; shows success toast
- **Runtime result:** `PENDING`

### [UI-SRC-9D5B9DACBF9A] Upstreams — Delete Upstream

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-UP-24`
- **Source:** `crates/cc-lb-admin/web/src/routes/upstreams.tsx:1150#DetailView`
- **Preconditions:** Upstream selected
- **Steps:** Click 'Delete' button in DetailView header → Click 'Delete' in ConfirmDialog
- **Scope:** `each_upstream` — Each upstream
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **Expected UI:** Soft-deletes upstream from database, closes dialog, shows success toast, and clears selectedId
- **Runtime result:** `PENDING`

### [UI-UP-24] Upstreams — Delete Upstream — useDeleteUpstream

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-9D5B9DACBF9A`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1016#useDeleteUpstream`
- **Preconditions:** Upstream selected
- **Steps:** Click 'Delete' button in DetailView header → Click 'Delete' in ConfirmDialog
- **Scope:** `each_upstream` — Each upstream
- **Risk:** `destructive_write`
- **Production applicability:** `available`
- **HTTP:** `DELETE /admin/v1/upstreams/{id}`
  - Query: None
  - Body: None
  - Headers: `authorization`, `if-match`
- **Handler:** `delete_upstream`
- **Storage operations:** `sqlite:UpstreamStore::soft_delete:SqliteStorage::soft_delete`, `postgres:UpstreamStore::soft_delete:PostgresStorage::soft_delete`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Evicts upstream from proxy routing table; invalidates queryKey ['upstreams']
- **Side effects:** Audit event `upstream_delete`; soft-deletes upstream record (sets deleted_at); cancels in-flight warmup and disables future route selections QA restore: restore a database snapshot or recreate the upstream and credentials.
- **Expected UI:** Soft-deletes upstream from database, closes dialog, shows success toast, and clears selectedId
- **Runtime result:** `PENDING`

### [UI-UP-25] Upstreams — Open Create Upstream Modal

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/routes/upstreams.tsx:230#CreateUpstreamModal`
- **Preconditions:** Page mounted
- **Steps:** Click 'New' button in sidebar header or 'New upstream' in empty state, or arrive via ?action=new
- **Scope:** `once` — Single execution
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Opens CreateUpstreamModal on step 'type'
- **Client-only reason:** Client state setCreateOpen(true)
- **Runtime result:** `PENDING`

### [UI-SRC-5A9E89C6EEB0] Upstreams — Create Non-OAuth Upstream

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-UP-26`
- **Source:** `crates/cc-lb-admin/web/src/routes/upstreams.tsx:2362#CreateUpstreamModal`
- **Preconditions:** Create modal on step 'configure_non_oauth'
- **Steps:** In CreateUpstreamModal, select Anthropic API Key and click Continue → Fill in Name, optional Base URL, and API key (literal or env var) → Click 'Create'
- **Scope:** `once` — Single execution per creation
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **Expected UI:** Creates upstream in DB, closes modal, shows success toast, and refreshes upstream list
- **Runtime result:** `PENDING`

### [UI-UP-26] Upstreams — Create Non-OAuth Upstream — useCreateUpstream

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-5A9E89C6EEB0`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1008#useCreateUpstream`
- **Preconditions:** Create modal on step 'configure_non_oauth'
- **Steps:** In CreateUpstreamModal, select Anthropic API Key and click Continue → Fill in Name, optional Base URL, and API key (literal or env var) → Click 'Create'
- **Scope:** `once` — Single execution per creation
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/v1/upstreams`
  - Query: None
  - Body: `name`, `kind`, `base_url`, `api_key_value`, `api_key_env`
  - Headers: `authorization`, `content-type`
- **Handler:** `create_upstream`
- **Storage operations:** `sqlite:UpstreamStore::create:SqliteStorage::create`, `postgres:UpstreamStore::create:PostgresStorage::create`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** DynamicView rebinding; invalidates queryKey ['upstreams'] UpstreamResponse now serializes base_url: Option<Url> (crates/cc-lb-admin/src/v1/upstreams.rs:167,1586), so the field is present and null when the upstream has no custom base URL; credential secret and api_key_env provenance are not exposed in the response.
- **Side effects:** Audit event `upstream_create`; creates new upstream target for proxy load balancing; encrypts API key via AEAD QA restore: DELETE the created upstream using its returned ETag.
- **Expected UI:** Creates upstream in DB, closes modal, shows success toast, and refreshes upstream list
- **Runtime result:** `PENDING`

### [UI-SRC-427756CE86BB] Upstreams — Start OAuth Draft Flow

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-UP-27`
- **Source:** `crates/cc-lb-admin/web/src/routes/upstreams.tsx:2383#CreateUpstreamModal`
- **Preconditions:** Create modal on step 'oauth_handshake'
- **Steps:** In CreateUpstreamModal, select Anthropic OAuth and click Continue → Click 'Authorize with Anthropic'
- **Scope:** `once` — Single execution per OAuth creation
- **Risk:** `external_action`
- **Production applicability:** `available`
- **Expected UI:** Fetches state token and authorize URL, opens browser window to Anthropic authorization page
- **Runtime result:** `PENDING`

### [UI-UP-27] Upstreams — Start OAuth Draft Flow — useStartOauthDraft

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-427756CE86BB`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1637#useStartOauthDraft`
- **Preconditions:** Create modal on step 'oauth_handshake'
- **Steps:** In CreateUpstreamModal, select Anthropic OAuth and click Continue → Click 'Authorize with Anthropic'
- **Scope:** `once` — Single execution per OAuth creation
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/v1/oauth/draft/start`
  - Query: None
  - Body: None
  - Headers: `authorization`
- **Handler:** `start_oauth_draft`
- **Storage operations:** `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Stores PKCE handshake state in memory (pkce_flows table, 900s TTL); no DB entity write except audit
- **Side effects:** Audit event `upstream_oauth_draft_start`; generates PKCE code_verifier/challenge; returns authorize_url and state_token QA restore: allow the in-memory PKCE flow to expire after 900 seconds or restart the process.
- **Expected UI:** Fetches state token and authorize URL, opens browser window to Anthropic authorization page
- **Runtime result:** `PENDING`

### [UI-SRC-CB056EC17B20] Upstreams — Verify OAuth Draft Code

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-UP-28`
- **Source:** `crates/cc-lb-admin/web/src/routes/upstreams.tsx:2396#CreateUpstreamModal`
- **Preconditions:** Draft state token present; Code non-empty
- **Steps:** Paste authorization code returned from Anthropic into textarea → Click 'Verify and fetch account'
- **Scope:** `once` — Single execution per OAuth creation
- **Risk:** `external_action`
- **Production applicability:** `available`
- **Expected UI:** Exchanges code with Anthropic, fetches account & subscription preview, and advances modal to confirm step
- **Runtime result:** `PENDING`

### [UI-UP-28] Upstreams — Verify OAuth Draft Code — useCompleteOauthDraft

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-CB056EC17B20`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1644#useCompleteOauthDraft`
- **Preconditions:** Draft state token present; Code non-empty
- **Steps:** Paste authorization code returned from Anthropic into textarea → Click 'Verify and fetch account'
- **Scope:** `once` — Single execution per OAuth creation
- **Risk:** `external_action`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/v1/oauth/draft/complete`
  - Query: None
  - Body: `state_token`, `code`
  - Headers: `authorization`, `content-type`
- **Handler:** `complete_oauth_draft`
- **Storage operations:** `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Stores exchanged tokens & metadata in in-memory pkce_flows under PkceTarget::PendingDraft
- **Side effects:** External HTTP request to Anthropic OAuth token endpoint (code exchange) and subscription/org metadata endpoints; encrypts token with AEAD; audit event `upstream_oauth_draft_complete` QA restore: discard the completed in-memory draft; the provider-side authorization-code exchange cannot be undone.
- **Expected UI:** Exchanges code with Anthropic, fetches account & subscription preview, and advances modal to confirm step
- **Runtime result:** `PENDING`

### [UI-SRC-2533F30C81A8] Upstreams — Confirm and Create OAuth Upstream

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-UP-29`
- **Source:** `crates/cc-lb-admin/web/src/routes/upstreams.tsx:2411#CreateUpstreamModal`
- **Preconditions:** Draft verified, on step 'oauth_confirm'
- **Steps:** Review Account Preview (Plan, Rate, Org, Role, etc.) → Edit or accept prefilled Upstream Name → Click 'Save'
- **Scope:** `once` — Single execution per OAuth creation
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **Expected UI:** Persists new OAuth upstream in DB, closes modal, shows success toast, and selects new upstream
- **Runtime result:** `PENDING`

### [UI-UP-29] Upstreams — Confirm and Create OAuth Upstream — useCreateFromOauthDraft

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-2533F30C81A8`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1652#useCreateFromOauthDraft`
- **Preconditions:** Draft verified, on step 'oauth_confirm'
- **Steps:** Review Account Preview (Plan, Rate, Org, Role, etc.) → Edit or accept prefilled Upstream Name → Click 'Save'
- **Scope:** `once` — Single execution per OAuth creation
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/v1/upstreams/from-oauth-draft`
  - Query: None
  - Body: `state_token`, `name`
  - Headers: `authorization`, `content-type`
- **Handler:** `create_upstream_from_oauth_draft`
- **Storage operations:** `sqlite:UpstreamStore::create:SqliteStorage::create`, `postgres:UpstreamStore::create:PostgresStorage::create`, `sqlite:UpstreamStore::store_oauth_tokens:SqliteStorage::store_oauth_tokens`, `postgres:UpstreamStore::store_oauth_tokens:PostgresStorage::store_oauth_tokens`, `sqlite:UpstreamSubscriptionMetadataStore::put_upstream_subscription_metadata:SqliteStorage::put_upstream_subscription_metadata`, `postgres:UpstreamSubscriptionMetadataStore::put_upstream_subscription_metadata:PostgresStorage::put_upstream_subscription_metadata`, `sqlite:OrganizationMetadataStore::put_organization_metadata:SqliteStorage::put_organization_metadata`, `postgres:OrganizationMetadataStore::put_organization_metadata:PostgresStorage::put_organization_metadata`, `sqlite:UpstreamStore::hard_delete:SqliteStorage::hard_delete`, `postgres:UpstreamStore::hard_delete:PostgresStorage::hard_delete`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Consumes PKCE draft state; invalidates TanStack queryKey ['upstreams'] UpstreamResponse now serializes base_url: Option<Url> (crates/cc-lb-admin/src/v1/oauth.rs:137,1199), so the field is present and null when the upstream has no custom base URL; credential secret and api_key_env provenance are not exposed in the response.
- **Side effects:** Audit event `upstream_create_from_oauth_draft`; creates upstream spec, persists encrypted OAuth token, saves subscription metadata, reloads runtime upstreams QA restore: delete the created upstream and associated credentials/metadata; intermediate failures invoke hard_delete automatically.
- **Expected UI:** Persists new OAuth upstream in DB, closes modal, shows success toast, and selects new upstream
- **Runtime result:** `PENDING`

### [UI-SRC-057C5139E406] Upstreams — Upstream OAuth Status Query

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-UP-30`
- **Source:** `crates/cc-lb-admin/web/src/routes/upstreams.tsx:805#DetailView`
- **Preconditions:** Upstream kind is anthropic_oauth
- **Steps:** Select an OAuth upstream
- **Scope:** `each_upstream` — Each OAuth upstream
- **Risk:** `external_action`
- **Production applicability:** `available`
- **Expected UI:** Displays token binding status, expiration relative countdown, refresh token presence, and granted scopes
- **Runtime result:** `PENDING`

### [UI-UP-30] Upstreams — Upstream OAuth Status Query — useUpstreamOAuthStatus

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-057C5139E406`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:733#useUpstreamOAuthStatus`
- **Preconditions:** Upstream kind is anthropic_oauth
- **Steps:** Select an OAuth upstream
- **Scope:** `each_upstream` — Each OAuth upstream
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/upstreams/{id}/oauth/status`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `get_oauth_status`
- **Storage operations:** `sqlite:UpstreamStore::get_by_id:SqliteStorage::get_by_id`, `postgres:UpstreamStore::get_by_id:PostgresStorage::get_by_id`
- **Cache / no-query path:** Fetches upstream record then performs in-memory AEAD decryption of encrypted OAuth token bundle via state.aead
- **Side effects:** None
- **Expected UI:** Displays token binding status, expiration relative countdown, refresh token presence, and granted scopes
- **Runtime result:** `PENDING`

### [UI-SRC-83D0030A34AF] Upstreams — Warmup Summary Query

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-UP-31`
- **Source:** `crates/cc-lb-admin/web/src/components/upstreams/warmup/WarmupCardMinimal.tsx:160#WarmupCardMinimalInner`
- **Preconditions:** OAuth upstream selected
- **Steps:** Mount WarmupCardMinimal on OAuth upstream selection
- **Scope:** `each_upstream` — Each OAuth upstream
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Renders Next run countdown, Last run outcome badge, failure reason message, and incident status
- **Runtime result:** `PENDING`

### [UI-UP-31] Upstreams — Warmup Summary Query — useWarmupSummary

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-83D0030A34AF`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1769#useWarmupSummary`
- **Preconditions:** OAuth upstream selected
- **Steps:** Mount WarmupCardMinimal on OAuth upstream selection
- **Scope:** `each_upstream` — Each OAuth upstream
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/upstreams/{id}/warmup`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `super::upstream_warmup::get_upstream_warmup`
- **Storage operations:** `sqlite:UpstreamStore::get_by_id:SqliteStorage::get_by_id`, `postgres:UpstreamStore::get_by_id:PostgresStorage::get_by_id`, `sqlite:UpstreamWarmupAttemptStore::latest_warmup_attempt_for_upstream:SqliteStorage::latest_warmup_attempt_for_upstream`, `postgres:UpstreamWarmupAttemptStore::latest_warmup_attempt_for_upstream:PostgresStorage::latest_warmup_attempt_for_upstream`, `sqlite:UpstreamWarmupAttemptStore::list_warmup_attempts_for_upstream:SqliteStorage::list_warmup_attempts_for_upstream`, `postgres:UpstreamWarmupAttemptStore::list_warmup_attempts_for_upstream:PostgresStorage::list_warmup_attempts_for_upstream`, `sqlite:UpstreamWarmupAttemptStore::summarize_recent_warmup_attempts:SqliteStorage::summarize_recent_warmup_attempts`, `postgres:UpstreamWarmupAttemptStore::summarize_recent_warmup_attempts:PostgresStorage::summarize_recent_warmup_attempts`, `sqlite:SchedulerAdminHandle::next_run_for_upstream:SchedulerAdminHandle::next_run_for_upstream`, `postgres:SchedulerAdminHandle::next_run_for_upstream:SchedulerAdminHandle::next_run_for_upstream`
- **Cache / no-query path:** Reads the upstream (including dialect_plugin), latest attempt, recent 10 attempts, and 7-day summary. The scheduler next-run query executes only when state.scheduler is attached; otherwise next_scheduled_at_unix_secs is null without scheduler SQL.
- **Side effects:** None
- **Expected UI:** Renders Next run countdown, Last run outcome badge, failure reason message, and incident status
- **Runtime result:** `PENDING`

### [UI-SRC-AB584B8FD224] Upstreams — Shape Plugin Registry Query

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-UP-32`
- **Source:** `crates/cc-lb-admin/web/src/components/upstreams/warmup/WarmupCardMinimal.tsx:159#WarmupCardMinimalInner`
- **Preconditions:** OAuth upstream selected
- **Steps:** Mount WarmupCardMinimal
- **Scope:** `once` — Single execution per session/upstream mount
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Populates the Shape plugin dropdown with registered plugins supporting the shape slot
- **Runtime result:** `PENDING`

### [UI-UP-32] Upstreams — Shape Plugin Registry Query — usePluginRegistry

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-AB584B8FD224`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:448#usePluginRegistry`
- **Preconditions:** OAuth upstream selected
- **Steps:** Mount WarmupCardMinimal
- **Scope:** `once` — Single execution per session/upstream mount
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/plugins/registry`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `list_registry`
- **Storage operations:** `sqlite:PluginRegistryStore::list_registry:SqliteStorage::list_registry`, `postgres:PluginRegistryStore::list_registry:PostgresStorage::list_registry`, `sqlite:PluginRegistryStore::get_blob_bytes:SqliteStorage::get_blob_bytes`, `postgres:PluginRegistryStore::get_blob_bytes:PostgresStorage::get_blob_bytes`
- **Cache / no-query path:** Paginates registry rows; synthesizes the built-in subscription-preference entry when storage did not return it; sorts/pages the combined set and reads each page entry blob to calculate size_bytes (missing blob reports size 0).
- **Side effects:** None
- **Expected UI:** Populates the Shape plugin dropdown with registered plugins supporting the shape slot
- **Runtime result:** `PENDING`

### [UI-SRC-C28C003E8881] Upstreams — Recent Requests for Upstream

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-UP-33`
- **Source:** `crates/cc-lb-admin/web/src/routes/upstreams.tsx:941#DetailView`
- **Preconditions:** Upstream selected
- **Steps:** Select an upstream in DetailView
- **Scope:** `each_upstream` — Each selected upstream
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Renders RequestEventsTable filtered by upstream_id with method, status, duration, cost, and tokens
- **Runtime result:** `PENDING`

### [UI-UP-33] Upstreams — Recent Requests for Upstream — useRecentEvents

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-C28C003E8881`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:476#useRecentEvents`
- **Preconditions:** Upstream selected
- **Steps:** Select an upstream in DetailView
- **Scope:** `each_upstream` — Each selected upstream
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/events/recent`
  - Query: `upstream_id`, `limit`
  - Body: None
  - Headers: None
- **Handler:** `handle_recent_events`
- **Storage operations:** `sqlite:RequestEventStore::list_request_events:SqliteStorage::list_request_events`, `postgres:RequestEventStore::list_request_events:PostgresStorage::list_request_events`
- **Cache / no-query path:** Dynamic filter queries
- **Side effects:** None
- **Expected UI:** Renders RequestEventsTable filtered by upstream_id with method, status, duration, cost, and tokens
- **Runtime result:** `PENDING`

### [UI-SRC-9CDF9EE839AE] Upstreams — Filter Warmup History by Status

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-UP-34`
- **Source:** `crates/cc-lb-admin/web/src/components/upstreams/warmup/WarmupHistoryDrawer.tsx:240#WarmupHistoryDrawer`
- **Preconditions:** Warmup history drawer open
- **Steps:** Click filter chip: All, Success, Retrying, Failed, or Skipped
- **Scope:** `each_filter_combination` — Each selected upstream × 5 status filters
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Triggers useWarmupAttempts query with status filter and resets attempt list
- **Runtime result:** `PENDING`

### [UI-UP-34] Upstreams — Filter Warmup History by Status — useWarmupAttempts

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-9CDF9EE839AE`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1782#useWarmupAttempts`
- **Preconditions:** Warmup history drawer open
- **Steps:** Click filter chip: All, Success, Retrying, Failed, or Skipped
- **Scope:** `each_filter_combination` — Each selected upstream × 5 status filters
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/upstreams/{id}/warmup/attempts`
  - Query: `status`, `limit`
  - Body: None
  - Headers: None
- **Handler:** `super::upstream_warmup::list_upstream_warmup_attempts`
- **Storage operations:** `sqlite:UpstreamStore::get_by_id:SqliteStorage::get_by_id`, `postgres:UpstreamStore::get_by_id:PostgresStorage::get_by_id`, `sqlite:UpstreamWarmupAttemptStore::list_warmup_attempts_for_upstream:SqliteStorage::list_warmup_attempts_for_upstream`, `postgres:UpstreamWarmupAttemptStore::list_warmup_attempts_for_upstream:PostgresStorage::list_warmup_attempts_for_upstream`
- **Cache / no-query path:** Cursor-based pagination (?before=cursor) and status filtering (?outcome/status)
- **Side effects:** None
- **Expected UI:** Triggers useWarmupAttempts query with status filter and resets attempt list
- **Runtime result:** `PENDING`

### [UI-SRC-35D5E99A460E] Upstreams — Load Older Warmup Attempts

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-UP-35`
- **Source:** `crates/cc-lb-admin/web/src/components/upstreams/warmup/WarmupHistoryDrawer.tsx:290#WarmupHistoryDrawer`
- **Preconditions:** query.hasNextPage is true
- **Steps:** Scroll to bottom of attempts list in WarmupHistoryDrawer → Click 'Load older' button
- **Scope:** `each_upstream` — Each upstream with next_cursor
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Appends next 50 attempts to list
- **Runtime result:** `PENDING`

### [UI-UP-35] Upstreams — Load Older Warmup Attempts — useWarmupAttempts

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-35D5E99A460E`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1782#useWarmupAttempts`
- **Preconditions:** query.hasNextPage is true
- **Steps:** Scroll to bottom of attempts list in WarmupHistoryDrawer → Click 'Load older' button
- **Scope:** `each_upstream` — Each upstream with next_cursor
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/upstreams/{id}/warmup/attempts`
  - Query: `status`, `limit`, `before`
  - Body: None
  - Headers: None
- **Handler:** `super::upstream_warmup::list_upstream_warmup_attempts`
- **Storage operations:** `sqlite:UpstreamStore::get_by_id:SqliteStorage::get_by_id`, `postgres:UpstreamStore::get_by_id:PostgresStorage::get_by_id`, `sqlite:UpstreamWarmupAttemptStore::list_warmup_attempts_for_upstream:SqliteStorage::list_warmup_attempts_for_upstream`, `postgres:UpstreamWarmupAttemptStore::list_warmup_attempts_for_upstream:PostgresStorage::list_warmup_attempts_for_upstream`
- **Cache / no-query path:** Cursor-based pagination (?before=cursor) and status filtering (?outcome/status)
- **Side effects:** None
- **Expected UI:** Appends next 50 attempts to list
- **Runtime result:** `PENDING`

### [UI-UP-36] Upstreams — Toggle Warmup History Horizon (24h / 7d / All)

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/upstreams/warmup/WarmupHistoryDrawer.tsx:118#WarmupHistoryDrawer`
- **Preconditions:** Warmup history drawer open
- **Steps:** Click 24h, 7d, or All button in drawer header
- **Scope:** `each_filter_combination` — Each upstream × 3 horizons
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Recalculates OverviewStrip attempt count, success/failed/skipped breakdown, and dominant failure reason client-side
- **Client-only reason:** Client-side date filtering over already-fetched attempt array
- **Runtime result:** `PENDING`

### [UI-UP-37] Upstreams — Toggle Plugin Snapshot and Raw Record Collapsibles

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/upstreams/warmup/WarmupHistoryDrawer.tsx:492#AttemptDetail`
- **Preconditions:** Attempt detail pane open in drawer
- **Steps:** In AttemptDetail, click 'Shape plugin snapshot at attempt time' or 'Raw attempt record'
- **Scope:** `each_row` — Each inspected attempt
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Expands or collapses preformatted JSON viewers
- **Client-only reason:** Client state setPluginOpen / setRawOpen toggles
- **Runtime result:** `PENDING`

### [UI-UP-38] Upstreams — Toggle API Usage Metric (Tokens / Cost)

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/upstreams/ApiUsageCard.tsx:103#ApiUsageCard`
- **Preconditions:** Non-OAuth upstream selected
- **Steps:** Click Tokens or Cost toggle button in ApiUsageCard
- **Scope:** `each_upstream` — Each non-OAuth upstream
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Reformats chart Y-axis and area values between token sums and USD costs without network request
- **Client-only reason:** Client state onMetricChange reformatting pre-fetched usage bucket data
- **Runtime result:** `PENDING`

### [UI-UP-39] Upstreams — Expand/Collapse Metadata Strip

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/routes/upstreams.tsx:1268#DetailView`
- **Preconditions:** OAuth upstream selected with more than 5 metadata fields
- **Steps:** Click '+N more' or 'Less' button in metadata strip
- **Scope:** `each_upstream` — Each upstream with overflow metadata
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Toggles visibility of secondary metadata fields (Org, Role, Seat, Subscribed, Billing)
- **Client-only reason:** Client state setShowMoreMeta toggle
- **Runtime result:** `PENDING`

### [UI-UP-40] Upstreams — Mobile Back Navigation

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/routes/upstreams.tsx:1055#DetailView`
- **Preconditions:** Mobile viewport; Upstream selected
- **Steps:** On viewport < 768px with upstream selected, click '< Back' button
- **Scope:** `once` — Each mobile detail inspection
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Clears selectedId in search params, hides DetailView, and reveals sidebar list
- **Client-only reason:** Client-side TanStack router navigation
- **Runtime result:** `PENDING`

### [UI-SRC-517760F07481] Upstreams — Close Attempt Detail in Drawer

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/upstreams/warmup/WarmupHistoryDrawer.tsx:398#AttemptDetail`
- **Preconditions:** Attempt detail pane open
- **Steps:** In AttemptDetail pane, click 'Close ▶' button
- **Scope:** `each_row` — Each inspected attempt
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Clears selected attempt; expands attempt list to full drawer width
- **Client-only reason:** Client state setSelected(null)
- **Runtime result:** `PENDING`

### [UI-SRC-668F412D6C0E] Upstreams — Close Warmup History Drawer

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/upstreams/warmup/WarmupHistoryDrawer.tsx:224#WarmupHistoryDrawer`
- **Preconditions:** Warmup history drawer open
- **Steps:** Click 'X' button, click drawer backdrop, or press Escape
- **Scope:** `each_upstream` — Each OAuth upstream
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Closes drawer with slide-out animation, resets selected attempt to null
- **Client-only reason:** Client state setDrawerOpen(false)
- **Runtime result:** `PENDING`

### [UI-SRC-D96DA7353940] Upstreams — Copy Warmup Plugin Config JSON

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/upstreams/warmup/parts/WarmupConfigModal.tsx:37#WarmupConfigModal`
- **Preconditions:** WarmupConfigModal open
- **Steps:** In WarmupConfigModal, click 'Copy JSON' button
- **Scope:** `each_upstream` — Each attached plugin
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Copies formatted plugin config JSON to system clipboard; button temporarily reads 'Copied'
- **Client-only reason:** Clipboard API write (navigator.clipboard.writeText) via useCopyButton
- **Runtime result:** `PENDING`

### [UI-SRC-D61F3782A082] Plugins — Load Selected Plugin References

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PLUG-06`
- **Source:** `crates/cc-lb-admin/web/src/components/plugins/PluginDetail.tsx:25#PluginDetail`
- **Preconditions:** Authenticated admin session; A registry plugin is selected
- **Steps:** Select a plugin from the catalog → PluginDetail mounts → usePluginReferences(plugin.id) executes
- **Scope:** `each_plugin` — GET /admin/v1/plugins/registry response entries
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Used by card renders every returned principal-chain and upstream-warmup reference.
- **Runtime result:** `PENDING`

### [UI-PLUG-06] Plugins — Load Selected Plugin References — usePluginReferences

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-D61F3782A082`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1422#usePluginReferences`
- **Preconditions:** Authenticated admin session; A registry plugin is selected
- **Steps:** Select a plugin from the catalog → PluginDetail mounts → usePluginReferences(plugin.id) executes
- **Scope:** `each_plugin` — GET /admin/v1/plugins/registry response entries
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/plugins/registry/{id}/references`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `get_registry_references`
- **Storage operations:** `sqlite:PluginRegistryStore::get_registry_entry_by_id:SqliteStorage::get_registry_entry_by_id`, `postgres:PluginRegistryStore::get_registry_entry_by_id:PostgresStorage::get_registry_entry_by_id`, `sqlite:PluginRegistryStore::get_blob_bytes:SqliteStorage::get_blob_bytes`, `postgres:PluginRegistryStore::get_blob_bytes:PostgresStorage::get_blob_bytes`, `sqlite:PluginRegistryStore::list_registry_references:SqliteStorage::list_registry_references`, `postgres:PluginRegistryStore::list_registry_references:PostgresStorage::list_registry_references`
- **Cache / no-query path:** Reads registry entry and blob bytes, then queries principal-chain and upstream warmup-dialect references in a transaction.
- **Side effects:** None
- **Expected UI:** Used by card renders every returned principal-chain and upstream-warmup reference.
- **Runtime result:** `PENDING`

### [UI-SRC-7BDECC53787E] Principals — Add Observability Hook

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PR-19`
- **Source:** `crates/cc-lb-admin/web/src/routes/principals.tsx:2275#ObservabilityHookEditor`
- **Preconditions:** Principal selected; Plugin registry loaded; No conflicting write is pending
- **Steps:** Select an observability-capable plugin → Click Add
- **Scope:** `each_plugin` — GET /admin/v1/plugins/registry entries supporting slot 'observability_hook'
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **Expected UI:** Selected hook appears in the ordered observability chain after query invalidation.
- **Runtime result:** `PENDING`

### [UI-PR-19] Principals — Add Observability Hook — useInsertChainEntry

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-7BDECC53787E`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1514#useInsertChainEntry`
- **Preconditions:** Principal selected; Plugin registry loaded; No conflicting write is pending
- **Steps:** Select an observability-capable plugin → Click Add
- **Scope:** `each_plugin` — GET /admin/v1/plugins/registry entries supporting slot 'observability_hook'
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/v1/principals/{id}/plugin-chain`
  - Query: None
  - Body: `slot`, `wasm_registry_id`
  - Headers: `authorization`, `content-type`
- **Handler:** `insert_chain`
- **Storage operations:** `sqlite:PluginRegistryStore::insert_chain_entry:SqliteStorage::insert_chain_entry`, `postgres:PluginRegistryStore::insert_chain_entry:PostgresStorage::insert_chain_entry`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Invalidates TanStack queryKey ['plugin-chain', principal_id]; triggers dynamic rebind headers
- **Side effects:** Inserts row into plugin_chains_v2; verifies principal exists and singleton slot constraint; records audit event 'plugin_chain_insert'; sends pg_notify 'cclb_plugin_changed' in Postgres; adds dynamic rebind headers; sets Location header QA restore: DELETE /admin/v1/plugin-chain-entries/{id} using returned entry ID and revision.
- **Expected UI:** Selected hook appears in the ordered observability chain after query invalidation.
- **Runtime result:** `PENDING`

### [UI-SRC-D64907F28D4C] Principals — Load Observability Plugin Chain

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PR-13D`
- **Source:** `crates/cc-lb-admin/web/src/routes/principals.tsx:480#PrincipalDetail`
- **Preconditions:** Authenticated admin session; Principal selected
- **Steps:** Select a principal → PrincipalDetail mounts usePluginChain(principal.id, 'observability_hook')
- **Scope:** `each_principal` — GET /admin/v1/principals response entries
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** The observability_hook editor and delete-impact count reflect the returned chain entries.
- **Runtime result:** `PENDING`

### [UI-PR-13D] Principals — Load Observability Plugin Chain — usePluginChain

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-D64907F28D4C`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:454#usePluginChain`
- **Preconditions:** Authenticated admin session; Principal selected
- **Steps:** Select a principal → PrincipalDetail mounts usePluginChain(principal.id, 'observability_hook')
- **Scope:** `each_principal` — GET /admin/v1/principals response entries
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/principals/{id}/plugin-chain`
  - Query: `slot`
  - Body: None
  - Headers: None
- **Handler:** `list_chain`
- **Storage operations:** `sqlite:PluginRegistryStore::list_chain_for_principal:SqliteStorage::list_chain_for_principal`, `postgres:PluginRegistryStore::list_chain_for_principal:PostgresStorage::list_chain_for_principal`
- **Cache / no-query path:** If query.slot is RuntimeOnly, returns empty list immediately without hitting DB
- **Side effects:** None
- **Expected UI:** The observability_hook editor and delete-impact count reflect the returned chain entries.
- **Runtime result:** `PENDING`

### [UI-SRC-30AC7206755A] Principals — Load Shape Plugin Chain

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PR-13C`
- **Source:** `crates/cc-lb-admin/web/src/routes/principals.tsx:481#PrincipalDetail`
- **Preconditions:** Authenticated admin session; Principal selected
- **Steps:** Select a principal → PrincipalDetail mounts usePluginChain(principal.id, 'shape')
- **Scope:** `each_principal` — GET /admin/v1/principals response entries
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** The shape editor and delete-impact count reflect the returned chain entries.
- **Runtime result:** `PENDING`

### [UI-PR-13C] Principals — Load Shape Plugin Chain — usePluginChain

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-30AC7206755A`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:454#usePluginChain`
- **Preconditions:** Authenticated admin session; Principal selected
- **Steps:** Select a principal → PrincipalDetail mounts usePluginChain(principal.id, 'shape')
- **Scope:** `each_principal` — GET /admin/v1/principals response entries
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/principals/{id}/plugin-chain`
  - Query: `slot`
  - Body: None
  - Headers: None
- **Handler:** `list_chain`
- **Storage operations:** `sqlite:PluginRegistryStore::list_chain_for_principal:SqliteStorage::list_chain_for_principal`, `postgres:PluginRegistryStore::list_chain_for_principal:PostgresStorage::list_chain_for_principal`
- **Cache / no-query path:** If query.slot is RuntimeOnly, returns empty list immediately without hitting DB
- **Side effects:** None
- **Expected UI:** The shape editor and delete-impact count reflect the returned chain entries.
- **Runtime result:** `PENDING`

### [UI-SRC-2092158E84C1] Principals — Load Plugin Registry for Principal Editors

- **Entry type:** `ui_action`
- **Atomic requests:** `UI-PR-13E`
- **Source:** `crates/cc-lb-admin/web/src/routes/principals.tsx:1435#usePluginRegistry`
- **Preconditions:** Authenticated admin session; Principal selected
- **Steps:** Select a principal → Router, Shape, and Observability editors mount usePluginRegistry
- **Scope:** `each_principal` — GET /admin/v1/principals response entries
- **Risk:** `read`
- **Production applicability:** `available`
- **Expected UI:** Plugin pickers expose only registry entries compatible with each slot.
- **Runtime result:** `PENDING`

### [UI-PR-13E] Principals — Load Plugin Registry for Principal Editors — usePluginRegistry

- **Entry type:** `network_request`
- **Parent action:** `UI-SRC-2092158E84C1`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:448#usePluginRegistry`
- **Preconditions:** Authenticated admin session; Principal selected
- **Steps:** Select a principal → Router, Shape, and Observability editors mount usePluginRegistry
- **Scope:** `each_principal` — GET /admin/v1/principals response entries
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/plugins/registry`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `list_registry`
- **Storage operations:** `sqlite:PluginRegistryStore::list_registry:SqliteStorage::list_registry`, `postgres:PluginRegistryStore::list_registry:PostgresStorage::list_registry`, `sqlite:PluginRegistryStore::get_blob_bytes:SqliteStorage::get_blob_bytes`, `postgres:PluginRegistryStore::get_blob_bytes:PostgresStorage::get_blob_bytes`
- **Cache / no-query path:** Paginates registry rows; synthesizes the built-in subscription-preference entry when storage did not return it; sorts/pages the combined set and reads each page entry blob to calculate size_bytes (missing blob reports size 0).
- **Side effects:** None
- **Expected UI:** Plugin pickers expose only registry entries compatible with each slot.
- **Runtime result:** `PENDING`

### [UI-SRC-18907E842823] Global — Render Authenticated Administrator Identity Badge

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/layout/AppShell.tsx:168#Topbar`
- **Preconditions:** Authenticated session context is available
- **Steps:** Protected AppShell renders → Topbar reads AuthSessionContext → Identity label and kind are displayed
- **Scope:** `once` — Current authenticated admin session
- **Risk:** `read`
- **Production applicability:** `not_deployed` — Added after deployed commit e56d029e
- **Expected UI:** Topbar displays the current administrator identity and actor kind without an additional request.
- **Client-only reason:** Client rendering from the already-loaded AuthSessionContext.
- **Runtime result:** `PENDING`

### [UI-SRC-453C347CE4F4] Audit — Render Audit Actor Metadata

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/routes/audit.tsx:401#AuditPage`
- **Preconditions:** Audit query returned a row
- **Steps:** Audit rows render actor in the table → Open a row → Inspect authority, subject, kind, and email in the detail modal
- **Scope:** `each_row` — Every row returned by GET /admin/audit
- **Risk:** `read`
- **Production applicability:** `not_deployed` — Added after deployed commit e56d029e
- **Expected UI:** Actor identity columns and detail fields reflect the source row, using an em dash for absent legacy values.
- **Client-only reason:** Client rendering from the existing audit query result.
- **Runtime result:** `PENDING`

### [UI-SRC-F0FA0033252E] Shared Request Tables — Inspect Latency Responsibility Breakdown

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/ui/latency/LatencyCell.tsx:120#LatencyCell`
- **Preconditions:** A request event row has latency timing data
- **Steps:** Focus or hover the latency cell button → Inspect responsibility sparkline and popover sections
- **Scope:** `each_row` — Every visible request-event row with a LatencyCell
- **Risk:** `read`
- **Production applicability:** `not_deployed` — Added after deployed commit e56d029e
- **Expected UI:** Popover separates downstream, cc-lb, upstream network, upstream wait, renewal, and unattributed time without a new request.
- **Client-only reason:** Client computation and popover rendering from the existing request event.
- **Runtime result:** `PENDING`

### [PROD-UI-A5000384379C] Credentials — Load Credentials and OAuth Status

- **Entry type:** `ui_action`
- **Atomic requests:** `PROD-REQ-AE097B1BD13F`, `PROD-REQ-186B043F6662`, `PROD-REQ-8DB3CB46F307`
- **Source:** `crates/cc-lb-admin/web/src/routes/credentials.tsx:92#CredentialsPage`
- **Preconditions:** Authenticated session
- **Steps:** Navigate to /credentials
- **Scope:** `once` — Single page mount
- **Risk:** `read`
- **Production applicability:** `available` — Present only in the deployed production commit.
- **Expected UI:** Renders API keys table, OAuth cards grid, and summary tiles (Total, Expiring Soon, Expired, Active)
- **Runtime result:** `PENDING`

### [PROD-REQ-AE097B1BD13F] Credentials — Load Credentials and OAuth Status — useCredentials

- **Entry type:** `network_request`
- **Parent action:** `PROD-UI-A5000384379C`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:767#useCredentials`
- **Preconditions:** Authenticated session
- **Steps:** Navigate to /credentials
- **Scope:** `once` — Single page mount
- **Risk:** `read`
- **Production applicability:** `available` — Present only in the deployed production commit.
- **HTTP:** `GET /admin/credentials`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `crate::credentials::list_credentials`
- **Storage operations:** None
- **Cache / no-query path:** PostgreSQL (PrincipalStore::list, get_oauth_ciphertext); SQLite (PrincipalStore::list, get_oauth_ciphertext)
- **Side effects:** Read-only credential overview
- **Expected UI:** Renders API keys table, OAuth cards grid, and summary tiles (Total, Expiring Soon, Expired, Active)
- **Runtime result:** `PENDING`

### [PROD-REQ-186B043F6662] Credentials — Load Credentials and OAuth Status — useOAuthStatus

- **Entry type:** `network_request`
- **Parent action:** `PROD-UI-A5000384379C`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:773#useOAuthStatus`
- **Preconditions:** Authenticated session
- **Steps:** Navigate to /credentials
- **Scope:** `once` — Single page mount
- **Risk:** `read`
- **Production applicability:** `available` — Present only in the deployed production commit.
- **HTTP:** `GET /admin/oauth/status`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `crate::credentials::list_oauth_status`
- **Storage operations:** None
- **Cache / no-query path:** PostgreSQL (PrincipalStore::list, get_oauth_ciphertext); SQLite (PrincipalStore::list, get_oauth_ciphertext)
- **Side effects:** Read-only OAuth token overview
- **Expected UI:** Renders API keys table, OAuth cards grid, and summary tiles (Total, Expiring Soon, Expired, Active)
- **Runtime result:** `PENDING`

### [PROD-REQ-8DB3CB46F307] Credentials — Load Credentials and OAuth Status — usePrincipalNameMap

- **Entry type:** `network_request`
- **Parent action:** `PROD-UI-A5000384379C`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:744#usePrincipalNameMap`
- **Preconditions:** Authenticated session
- **Steps:** Navigate to /credentials
- **Scope:** `once` — Single page mount
- **Risk:** `read`
- **Production applicability:** `available` — Present only in the deployed production commit.
- **HTTP:** `GET /admin/v1/principals`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `list_principals`
- **Storage operations:** `sqlite:PrincipalStore::list:SqliteStorage::list`, `postgres:PrincipalStore::list:PostgresStorage::list`
- **Cache / no-query path:** Executes PrincipalStore::list twice: first with offset=0 and limit=1000 to derive the capped X-Total-Count, then with the requested offset/limit for the page; no COUNT SQL is executed.
- **Side effects:** Look up principal names for OAuth tokens
- **Expected UI:** Renders API keys table, OAuth cards grid, and summary tiles (Total, Expiring Soon, Expired, Active)
- **Runtime result:** `PENDING`

### [PROD-UI-76168321F814] Credentials — Rotate Credential / Force Refresh OAuth Token

- **Entry type:** `ui_action`
- **Atomic requests:** `PROD-REQ-B33C09B92A03`
- **Source:** `crates/cc-lb-admin/web/src/routes/credentials.tsx:102#CredentialsPage`
- **Preconditions:** Visible credential row or OAuth card
- **Steps:** Click 'Rotate' or 'Force refresh' button on credential row/card → Confirm in modal
- **Scope:** `item` — Credential row or OAuth card
- **Risk:** `reversible_write`
- **Production applicability:** `available` — Present only in the deployed production commit.
- **Expected UI:** Modal confirmation dialog, then toast notification
- **Runtime result:** `PENDING`

### [PROD-REQ-B33C09B92A03] Credentials — Rotate Credential / Force Refresh OAuth Token — postJson

- **Entry type:** `network_request`
- **Parent action:** `PROD-UI-76168321F814`
- **Source:** `crates/cc-lb-admin/web/src/routes/credentials.tsx:108#CredentialsPage`
- **Preconditions:** Visible credential row or OAuth card
- **Steps:** Click 'Rotate' or 'Force refresh' button on credential row/card → Confirm in modal
- **Scope:** `item` — Credential row or OAuth card
- **Risk:** `reversible_write`
- **Production applicability:** `available` — Present only in the deployed production commit.
- **HTTP:** `POST /admin/credentials/{provider}/{cred_id}/rotate`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `crate::credentials::rotate_credential`
- **Storage operations:** None
- **Cache / no-query path:** None (returns 501 rotate_unsupported)
- **Side effects:** Attempts rotation (returns 501 for OAuth in v0.4.9)
- **Expected UI:** Modal confirmation dialog, then toast notification
- **Runtime result:** `PENDING`

### [PROD-UI-6011B5C8167A] Credentials — Revoke Credential

- **Entry type:** `ui_action`
- **Atomic requests:** `PROD-REQ-32A95A5C0307`
- **Source:** `crates/cc-lb-admin/web/src/routes/credentials.tsx:102#CredentialsPage`
- **Preconditions:** Active credential or OAuth token
- **Steps:** Click 'Revoke' button on credential row/card → Confirm in destructive modal
- **Scope:** `item` — Credential row or OAuth card
- **Risk:** `destructive_write`
- **Production applicability:** `available` — Present only in the deployed production commit.
- **Expected UI:** Destructive confirm dialog, invalidates credentials query
- **Runtime result:** `PENDING`

### [PROD-REQ-32A95A5C0307] Credentials — Revoke Credential — postJson

- **Entry type:** `network_request`
- **Parent action:** `PROD-UI-6011B5C8167A`
- **Source:** `crates/cc-lb-admin/web/src/routes/credentials.tsx:108#CredentialsPage`
- **Preconditions:** Active credential or OAuth token
- **Steps:** Click 'Revoke' button on credential row/card → Confirm in destructive modal
- **Scope:** `item` — Credential row or OAuth card
- **Risk:** `destructive_write`
- **Production applicability:** `available` — Present only in the deployed production commit.
- **HTTP:** `POST /admin/credentials/{provider}/{cred_id}/revoke`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `crate::credentials::revoke_credential`
- **Storage operations:** None
- **Cache / no-query path:** PostgreSQL (storage.delete_oauth); SQLite (storage.delete_oauth)
- **Side effects:** Deletes OAuth credentials from storage
- **Expected UI:** Destructive confirm dialog, invalidates credentials query
- **Runtime result:** `PENDING`

### [PROD-UI-E68AB274C4E9] Status — Load System Status, Killswitch, and Observed Credentials

- **Entry type:** `ui_action`
- **Atomic requests:** `PROD-REQ-6B11261782D1`, `PROD-REQ-0F9F7B032EC5`, `PROD-REQ-26ECB56C1696`, `PROD-REQ-AE0413AAFCDF`
- **Source:** `crates/cc-lb-admin/web/src/routes/status.tsx:78#StatusPage`
- **Preconditions:** Authenticated session
- **Steps:** Navigate to /status
- **Scope:** `once` — Single page mount
- **Risk:** `read`
- **Production applicability:** `available` — Present only in the deployed production commit.
- **Expected UI:** Displays Restart-Required Changes list, System grid (Storage Backend, Plugin Chain Summary, Killswitch control), Credentials table, and OAuth Tokens grid
- **Runtime result:** `PENDING`

### [PROD-REQ-6B11261782D1] Status — Load System Status, Killswitch, and Observed Credentials — useStatus

- **Entry type:** `network_request`
- **Parent action:** `PROD-UI-E68AB274C4E9`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:395#useStatus`
- **Preconditions:** Authenticated session
- **Steps:** Navigate to /status
- **Scope:** `once` — Single page mount
- **Risk:** `read`
- **Production applicability:** `available` — Present only in the deployed production commit.
- **HTTP:** `GET /admin/v1/status`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `status`
- **Storage operations:** `sqlite:UpstreamStore::list:SqliteStorage::list`, `postgres:UpstreamStore::list:PostgresStorage::list`
- **Cache / no-query path:** DB query for upstream names/IDs combined with in-memory DynamicViewHolder.replica_identity and build metadata
- **Side effects:** Polled every 15s
- **Expected UI:** Displays Restart-Required Changes list, System grid (Storage Backend, Plugin Chain Summary, Killswitch control), Credentials table, and OAuth Tokens grid
- **Runtime result:** `PENDING`

### [PROD-REQ-0F9F7B032EC5] Status — Load System Status, Killswitch, and Observed Credentials — useCredentials

- **Entry type:** `network_request`
- **Parent action:** `PROD-UI-E68AB274C4E9`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:767#useCredentials`
- **Preconditions:** Authenticated session
- **Steps:** Navigate to /status
- **Scope:** `once` — Single page mount
- **Risk:** `read`
- **Production applicability:** `available` — Present only in the deployed production commit.
- **HTTP:** `GET /admin/credentials`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `crate::credentials::list_credentials`
- **Storage operations:** None
- **Cache / no-query path:** PostgreSQL (PrincipalStore::list, get_oauth_ciphertext); SQLite (PrincipalStore::list, get_oauth_ciphertext)
- **Side effects:** Read-only
- **Expected UI:** Displays Restart-Required Changes list, System grid (Storage Backend, Plugin Chain Summary, Killswitch control), Credentials table, and OAuth Tokens grid
- **Runtime result:** `PENDING`

### [PROD-REQ-26ECB56C1696] Status — Load System Status, Killswitch, and Observed Credentials — useOAuthStatus

- **Entry type:** `network_request`
- **Parent action:** `PROD-UI-E68AB274C4E9`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:773#useOAuthStatus`
- **Preconditions:** Authenticated session
- **Steps:** Navigate to /status
- **Scope:** `once` — Single page mount
- **Risk:** `read`
- **Production applicability:** `available` — Present only in the deployed production commit.
- **HTTP:** `GET /admin/oauth/status`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `crate::credentials::list_oauth_status`
- **Storage operations:** None
- **Cache / no-query path:** PostgreSQL (PrincipalStore::list, get_oauth_ciphertext); SQLite (PrincipalStore::list, get_oauth_ciphertext)
- **Side effects:** Read-only
- **Expected UI:** Displays Restart-Required Changes list, System grid (Storage Backend, Plugin Chain Summary, Killswitch control), Credentials table, and OAuth Tokens grid
- **Runtime result:** `PENDING`

### [PROD-REQ-AE0413AAFCDF] Status — Load System Status, Killswitch, and Observed Credentials — usePrincipalNameMap

- **Entry type:** `network_request`
- **Parent action:** `PROD-UI-E68AB274C4E9`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:744#usePrincipalNameMap`
- **Preconditions:** Authenticated session
- **Steps:** Navigate to /status
- **Scope:** `once` — Single page mount
- **Risk:** `read`
- **Production applicability:** `available` — Present only in the deployed production commit.
- **HTTP:** `GET /admin/v1/principals`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `list_principals`
- **Storage operations:** `sqlite:PrincipalStore::list:SqliteStorage::list`, `postgres:PrincipalStore::list:PostgresStorage::list`
- **Cache / no-query path:** Executes PrincipalStore::list twice: first with offset=0 and limit=1000 to derive the capped X-Total-Count, then with the requested offset/limit for the page; no COUNT SQL is executed.
- **Side effects:** Read-only
- **Expected UI:** Displays Restart-Required Changes list, System grid (Storage Backend, Plugin Chain Summary, Killswitch control), Credentials table, and OAuth Tokens grid
- **Runtime result:** `PENDING`

### [PROD-UI-168BF3B193F4] Status — Engage or Disengage Global Killswitch

- **Entry type:** `ui_action`
- **Atomic requests:** `PROD-REQ-817B78681D90`, `PROD-REQ-841024D77392`
- **Source:** `crates/cc-lb-admin/web/src/routes/status.tsx:217#StatusPage`
- **Preconditions:** Authenticated session, StatusPage mounted
- **Steps:** Click 'Engage killswitch' button -> Confirm in dialog, OR Click 'Disengage'
- **Scope:** `once` — Button click
- **Risk:** `destructive_write`
- **Production applicability:** `available` — Present only in the deployed production commit.
- **Expected UI:** Changes badge from OFF to ACTIVE (or vice versa), disables/enables traffic, shows toast
- **Runtime result:** `PENDING`

### [PROD-REQ-817B78681D90] Status — Engage or Disengage Global Killswitch — useKillswitch

- **Entry type:** `network_request`
- **Parent action:** `PROD-UI-168BF3B193F4`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1630#useKillswitch`
- **Preconditions:** Authenticated session, StatusPage mounted
- **Steps:** Click 'Engage killswitch' button -> Confirm in dialog, OR Click 'Disengage'
- **Scope:** `once` — Button click
- **Risk:** `destructive_write`
- **Production applicability:** `available` — Present only in the deployed production commit.
- **HTTP:** `POST /admin/killswitch`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `set_killswitch`
- **Storage operations:** None
- **Cache / no-query path:** PostgreSQL (storage.set_killswitch_enabled(true)); SQLite (storage.set_killswitch_enabled(true))
- **Side effects:** Engages emergency killswitch, stopping all proxy traffic
- **Expected UI:** Changes badge from OFF to ACTIVE (or vice versa), disables/enables traffic, shows toast
- **Runtime result:** `PENDING`

### [PROD-REQ-841024D77392] Status — Engage or Disengage Global Killswitch — useKillswitch

- **Entry type:** `network_request`
- **Parent action:** `PROD-UI-168BF3B193F4`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:1630#useKillswitch`
- **Preconditions:** Authenticated session, StatusPage mounted
- **Steps:** Click 'Engage killswitch' button -> Confirm in dialog, OR Click 'Disengage'
- **Scope:** `once` — Button click
- **Risk:** `destructive_write`
- **Production applicability:** `available` — Present only in the deployed production commit.
- **HTTP:** `DELETE /admin/killswitch`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `clear_killswitch`
- **Storage operations:** None
- **Cache / no-query path:** PostgreSQL (storage.set_killswitch_enabled(false)); SQLite (storage.set_killswitch_enabled(false))
- **Side effects:** Disengages emergency killswitch, resuming traffic
- **Expected UI:** Changes badge from OFF to ACTIVE (or vice versa), disables/enables traffic, shows toast
- **Runtime result:** `PENDING`

### [PROD-UI-3DF6CC4224CD] Global — Sidebar Navigation - Credentials

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/layout/Sidebar.tsx:22#NAV`
- **Preconditions:** Sidebar visible
- **Steps:** Click 'Credentials' link in sidebar
- **Scope:** `once` — Navigation click
- **Risk:** `read`
- **Production applicability:** `available` — Present only in the deployed production commit.
- **Expected UI:** Navigates to /credentials route
- **Client-only reason:** Router navigation link
- **Runtime result:** `PENDING`

### [PROD-UI-F22C53DA973A] Global — Sidebar Navigation - Status

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/layout/Sidebar.tsx:23#NAV`
- **Preconditions:** Sidebar visible
- **Steps:** Click 'Status' link in sidebar
- **Scope:** `once` — Navigation click
- **Risk:** `read`
- **Production applicability:** `available` — Present only in the deployed production commit.
- **Expected UI:** Navigates to /status route
- **Client-only reason:** Router navigation link
- **Runtime result:** `PENDING`

### [PROD-UI-7075CE908A3D] Global — Command Palette - Go to Status

- **Entry type:** `ui_action`
- **Atomic requests:** None
- **Source:** `crates/cc-lb-admin/web/src/components/CommandPalette.tsx:105#CommandPalette`
- **Preconditions:** Command palette open
- **Steps:** Open command palette (Cmd+K) → Select 'Go to Status'
- **Scope:** `once` — Command selection
- **Risk:** `read`
- **Production applicability:** `available` — Present only in the deployed production commit.
- **Expected UI:** Navigates to /status and closes command palette
- **Client-only reason:** Router navigation
- **Runtime result:** `PENDING`

### [PROD-UI-3BA367988C76] Plugins — Plugin Detail Global Killswitch Indicator

- **Entry type:** `ui_action`
- **Atomic requests:** `PROD-REQ-341C216AF806`
- **Source:** `crates/cc-lb-admin/web/src/components/plugins/PluginDetailOperate.tsx:140#PluginDetailOperate`
- **Preconditions:** Plugin selected
- **Steps:** Select plugin to view detail in Operate tab
- **Scope:** `item` — Selected plugin
- **Risk:** `read`
- **Production applicability:** `available` — Present only in the deployed production commit.
- **Expected UI:** Shows 'Global Killswitch' status row with Active/Inactive badge
- **Runtime result:** `PENDING`

### [PROD-REQ-341C216AF806] Plugins — Plugin Detail Global Killswitch Indicator — useStatus

- **Entry type:** `network_request`
- **Parent action:** `PROD-UI-3BA367988C76`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:395#useStatus`
- **Preconditions:** Plugin selected
- **Steps:** Select plugin to view detail in Operate tab
- **Scope:** `item` — Selected plugin
- **Risk:** `read`
- **Production applicability:** `available` — Present only in the deployed production commit.
- **HTTP:** `GET /admin/v1/status`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `status`
- **Storage operations:** `sqlite:UpstreamStore::list:SqliteStorage::list`, `postgres:UpstreamStore::list:PostgresStorage::list`
- **Cache / no-query path:** DB query for upstream names/IDs combined with in-memory DynamicViewHolder.replica_identity and build metadata
- **Side effects:** Read-only
- **Expected UI:** Shows 'Global Killswitch' status row with Active/Inactive badge
- **Runtime result:** `PENDING`

### [PROD-UI-01A4AC001FED] Plugins — Plugin Detail Global Chain Usage Summary

- **Entry type:** `ui_action`
- **Atomic requests:** `PROD-REQ-448A5BA63A6D`
- **Source:** `crates/cc-lb-admin/web/src/components/plugins/PluginDetailOperate.tsx:154#PluginDetailOperate`
- **Preconditions:** Plugin selected
- **Steps:** Select plugin to view detail in Operate tab
- **Scope:** `item` — Selected plugin
- **Risk:** `read`
- **Production applicability:** `available` — Present only in the deployed production commit.
- **Expected UI:** Displays total chain entries and number of principals with configured chain
- **Runtime result:** `PENDING`

### [PROD-REQ-448A5BA63A6D] Plugins — Plugin Detail Global Chain Usage Summary — useStatus

- **Entry type:** `network_request`
- **Parent action:** `PROD-UI-01A4AC001FED`
- **Source:** `crates/cc-lb-admin/web/src/lib/queries.ts:395#useStatus`
- **Preconditions:** Plugin selected
- **Steps:** Select plugin to view detail in Operate tab
- **Scope:** `item` — Selected plugin
- **Risk:** `read`
- **Production applicability:** `available` — Present only in the deployed production commit.
- **HTTP:** `GET /admin/v1/status`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `status`
- **Storage operations:** `sqlite:UpstreamStore::list:SqliteStorage::list`, `postgres:UpstreamStore::list:PostgresStorage::list`
- **Cache / no-query path:** DB query for upstream names/IDs combined with in-memory DynamicViewHolder.replica_identity and build metadata
- **Side effects:** Read-only
- **Expected UI:** Displays total chain entries and number of principals with configured chain
- **Runtime result:** `PENDING`

### [API-SRC-C767025D0EDC] Backend-Only — GET /

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:123`
- **Preconditions:** None (Public)
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /`
  - Query: None
  - Body: None
  - Headers: `if-none-match`
- **Handler:** `serve_index`
- **Storage operations:** None
- **Cache / no-query path:** Embedded index.html response with SHA-256 strong ETag and If-None-Match handling; matching validators return 304. index.html uses no-cache, must-revalidate.
- **Side effects:** None
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-276F2E8B8B8D] Backend-Only — GET /{*file}

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:124`
- **Preconditions:** None (Public)
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /{*file}`
  - Query: None
  - Body: None
  - Headers: `if-none-match`
- **Handler:** `serve_asset`
- **Storage operations:** None
- **Cache / no-query path:** Embedded asset response with SHA-256 strong ETag and If-None-Match/304 handling. assets/* use immutable one-year caching; other embedded files revalidate. Missing admin or admin/* paths return 404; other missing paths fall back to index.html for SPA routing.
- **Side effects:** None
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-7A787CD5C3CF] Backend-Only — GET /admin/health/state

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-server/src/app.rs:2367`
- **Preconditions:** None (Unauthenticated listener state check)
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/health/state`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `admin_server_state`
- **Storage operations:** None
- **Cache / no-query path:** In-memory ServerStateHandle atomic read
- **Side effects:** None
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SYS-02] Backend-Only — GET /internal/v1/partials/{event_id}

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/internal_partials.rs:20`
- **Preconditions:** Cluster token header (x-cluster-token matching state.cluster_token)
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /internal/v1/partials/{event_id}`
  - Query: None
  - Body: None
  - Headers: `x-cluster-token`
- **Handler:** `handle_internal_partial_fetch`
- **Storage operations:** None
- **Cache / no-query path:** PostgreSQL-only conditional mount: the server creates InternalPartialsState only for Postgres storage and merges this router only when that state exists. Handler reads the in-memory retained-partial snapshot ring; SQLite deployments receive router-level not-found behavior.
- **Side effects:** None
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-E5535B2DF6A8] Backend-Only — GET /admin/v1/config/current

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:50`
- **Preconditions:** require_admin_auth + authorize(AdminAction::SensitiveRead)
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read_with_audit`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/config/current`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `get_config`
- **Storage operations:** `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** In-memory state.config.current_config() Arc clone; secrets are masked via mask_secret_like_values before serialization.
- **Side effects:** Audit log write: record_admin_audit("config_read", "/admin/v1/config/current")
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-CA5516D80DC5] Backend-Only — GET /admin/v1/config/schema

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:52`
- **Preconditions:** require_admin_auth middleware
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/config/schema`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `get_config_schema`
- **Storage operations:** None
- **Cache / no-query path:** In-memory schemars schema generation; HTTP Cache-Control: max-age=60
- **Side effects:** None
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-51C67C36397F] Backend-Only — GET /admin/v1/config/draft

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:58`
- **Preconditions:** require_admin_auth + authorize(AdminAction::SensitiveRead)
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read_with_audit`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/config/draft`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `get_config_draft`
- **Storage operations:** `sqlite:ConfigStore::get_config_draft:SqliteStorage::get_config_draft`, `postgres:ConfigStore::get_config_draft:PostgresStorage::get_config_draft`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Reads singleton config draft from DB
- **Side effects:** Audit log write: record_admin_audit("config_draft_read", "/admin/v1/config/draft")
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-E9BB1496F09E] Backend-Only — GET /admin/v1/config/history

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:69`
- **Preconditions:** require_admin_auth + authorize(AdminAction::SensitiveRead)
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read_with_audit`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/config/history`
  - Query: `limit`
  - Body: None
  - Headers: None
- **Handler:** `get_config_history`
- **Storage operations:** `sqlite:ConfigStore::list_config_history:SqliteStorage::list_config_history`, `postgres:ConfigStore::list_config_history:PostgresStorage::list_config_history`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Query parameter limit defaults to 20 and is passed through to storage SQL without a maximum clamp; limit=0 short-circuits to an empty result.
- **Side effects:** Audit log write: record_admin_audit("config_history_read", "/admin/v1/config/history")
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-BACKEND-CONFIG-DIFF] Backend-Only — GET /admin/config/diff

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:70`
- **Preconditions:** require_admin_auth + authorize(AdminAction::SensitiveRead)
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read_with_audit`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/config/diff`
  - Query: `from_revision`, `to_revision`
  - Body: None
  - Headers: None
- **Handler:** `get_config_diff`
- **Storage operations:** `sqlite:ConfigStore::get_config_history:SqliteStorage::get_config_history`, `postgres:ConfigStore::get_config_history:PostgresStorage::get_config_history`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Fetches two revisions from DB, calculates structural JSON diff in memory
- **Side effects:** Audit log write: record_admin_audit("config_diff_read", "/admin/v1/config/diff")
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-903AA90CBF0D] Backend-Only — GET /admin/v1/config/diff

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:71`
- **Preconditions:** require_admin_auth + authorize(AdminAction::SensitiveRead)
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read_with_audit`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/config/diff`
  - Query: `from_revision`, `to_revision`
  - Body: None
  - Headers: None
- **Handler:** `get_config_diff`
- **Storage operations:** `sqlite:ConfigStore::get_config_history:SqliteStorage::get_config_history`, `postgres:ConfigStore::get_config_history:PostgresStorage::get_config_history`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Fetches two revisions from DB, calculates diff in memory
- **Side effects:** Audit log write: record_admin_audit("config_diff_read", "/admin/v1/config/diff")
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-15D90407F542] Backend-Only — GET /admin/v1/audit

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:48`
- **Preconditions:** require_admin_auth + authorize(AdminAction::SensitiveRead)
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read_with_audit`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/audit`
  - Query: `principal_id`, `since`, `after`, `until`, `limit`, `actor_authority`, `actor_subject`
  - Body: None
  - Headers: None
- **Handler:** `query_audit`
- **Storage operations:** `sqlite:AuditStore::query_recent_audit:SqliteStorage::query_recent_audit`, `postgres:AuditStore::query_recent_audit:PostgresStorage::query_recent_audit`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Single storage dispatch: actor_authority+actor_subject selects AuditQueryScope::Actor, principal_id selects AuditQueryScope::Principal, otherwise AuditQueryScope::All; all scopes call AuditStore::query_recent_audit, which applies the scope filter and since/until bounds before LIMIT. since is inclusive; after is an exclusive lower bound converted to since = max(since, after+1). Results are newest-first: ORDER BY ts DESC with id DESC (SQLite) / seq DESC (Postgres) tie-break. limit defaults to 100, capped at 1000; limit=0 or until<since returns empty without SQL.
- **Side effects:** Audit log write: record_admin_audit("audit_query", "/admin/v1/audit")
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-C2E19B96F128] Backend-Only — GET /admin/v1/dashboard/summary

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:75`
- **Preconditions:** require_admin_auth middleware
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/dashboard/summary`
  - Query: `range`
  - Body: None
  - Headers: `if-none-match`
- **Handler:** `crate::dashboard_routes::handle_dashboard_summary`
- **Storage operations:** `sqlite:UsageRollupStore::usage_rollup_checkpoint:SqliteStorage::usage_rollup_checkpoint`, `postgres:UsageRollupStore::usage_rollup_checkpoint:PostgresStorage::usage_rollup_checkpoint`, `sqlite:UsageRollupStore::query_usage_rollups_in_range:SqliteStorage::query_usage_rollups_in_range`, `postgres:UsageRollupStore::query_usage_rollups_in_range:PostgresStorage::query_usage_rollups_in_range`, `sqlite:UsageRollupStore::query_overview_excluded_error_buckets_in_range:SqliteStorage::query_overview_excluded_error_buckets_in_range`, `postgres:UsageRollupStore::query_overview_excluded_error_buckets_in_range:PostgresStorage::query_overview_excluded_error_buckets_in_range`
- **Cache / no-query path:** Reads usage-rollup checkpoint for weak ETag/304, then usage rollups and overview-excluded error buckets when a body is required.
- **Side effects:** None
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-6272C4C4303C] Backend-Only — GET /admin/v1/dashboard/usage

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:79`
- **Preconditions:** require_admin_auth middleware
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/dashboard/usage`
  - Query: `range`, `group_by`, `step`, `upstream_id`, `projection`
  - Body: None
  - Headers: `if-none-match`
- **Handler:** `crate::dashboard_routes::handle_dashboard_usage`
- **Storage operations:** `sqlite:UsageRollupStore::usage_rollup_checkpoint:SqliteStorage::usage_rollup_checkpoint`, `postgres:UsageRollupStore::usage_rollup_checkpoint:PostgresStorage::usage_rollup_checkpoint`, `sqlite:UsageRollupStore::query_usage_rollups_in_range:SqliteStorage::query_usage_rollups_in_range`, `postgres:UsageRollupStore::query_usage_rollups_in_range:PostgresStorage::query_usage_rollups_in_range`, `sqlite:RequestEventStore::request_event_principal_costs:SqliteStorage::request_event_principal_costs`, `postgres:RequestEventStore::request_event_principal_costs:PostgresStorage::request_event_principal_costs`
- **Cache / no-query path:** Principal totals projection uses a short-TTL single-flight cache; other branches use checkpoint-based weak ETag/304. Body construction reads usage rollups and conditionally principal costs for group_by=principal.
- **Side effects:** None
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-17E768052C98] Backend-Only — GET /admin/dashboard/usage

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/dashboard_routes.rs:29`
- **Preconditions:** require_admin_auth middleware
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/dashboard/usage`
  - Query: `range`, `group_by`, `step`, `upstream_id`, `projection`
  - Body: None
  - Headers: `if-none-match`
- **Handler:** `handle_dashboard_usage`
- **Storage operations:** `sqlite:UsageRollupStore::usage_rollup_checkpoint:SqliteStorage::usage_rollup_checkpoint`, `postgres:UsageRollupStore::usage_rollup_checkpoint:PostgresStorage::usage_rollup_checkpoint`, `sqlite:UsageRollupStore::query_usage_rollups_in_range:SqliteStorage::query_usage_rollups_in_range`, `postgres:UsageRollupStore::query_usage_rollups_in_range:PostgresStorage::query_usage_rollups_in_range`, `sqlite:RequestEventStore::request_event_principal_costs:SqliteStorage::request_event_principal_costs`, `postgres:RequestEventStore::request_event_principal_costs:PostgresStorage::request_event_principal_costs`
- **Cache / no-query path:** Principal totals projection uses a short-TTL single-flight cache; other branches use checkpoint-based weak ETag/304. Body construction reads usage rollups and conditionally principal costs for group_by=principal.
- **Side effects:** None
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-393CB1B46201] Backend-Only — GET /admin/v1/events/recent

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:83`
- **Preconditions:** require_admin_auth middleware
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/events/recent`
  - Query: `since_unix_secs`, `until_unix_secs`, `until_ts_ms`, `until_event_id`, `limit`, `principal_id`, `thread_id`, `model`, `upstream`, `upstream_id`, `status_class`, `source_kind`
  - Body: None
  - Headers: None
- **Handler:** `crate::events_routes::handle_recent_events`
- **Storage operations:** `sqlite:RequestEventStore::list_request_events:SqliteStorage::list_request_events`, `postgres:RequestEventStore::list_request_events:PostgresStorage::list_request_events`
- **Cache / no-query path:** Dynamic query builder branching based on filters: principal_id, thread_id, model, upstream, status_class, source_kind. Keysets: until_ts_ms, until_event_id
- **Side effects:** None
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-220A728FB03F] Backend-Only — GET /admin/events/histogram

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:91`
- **Preconditions:** require_admin_auth middleware
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/events/histogram`
  - Query: `since_unix_secs`, `until_unix_secs`, `bucket_ms`, `principal_id`, `thread_id`, `model`, `upstream`, `upstream_id`, `status_class`, `source_kind`
  - Body: None
  - Headers: None
- **Handler:** `crate::events_routes::handle_events_histogram`
- **Storage operations:** `sqlite:RequestEventStore::request_event_histogram:SqliteStorage::request_event_histogram`, `postgres:RequestEventStore::request_event_histogram:PostgresStorage::request_event_histogram`
- **Cache / no-query path:** Computes time buckets and rejects requests exceeding MAX_HISTOGRAM_BUCKETS=240.
- **Side effects:** None
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-149FAC449949] Backend-Only — GET /admin/events/delta

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/events_routes.rs:52`
- **Preconditions:** require_admin_auth middleware
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/events/delta`
  - Query: `since_cursor`, `limit`, `principal_id`, `thread_id`, `model`, `upstream`, `upstream_id`, `status_class`, `source_kind`
  - Body: None
  - Headers: None
- **Handler:** `handle_events_delta`
- **Storage operations:** `sqlite:RequestEventStore::current_request_event_cursor:SqliteStorage::current_request_event_cursor`, `postgres:RequestEventStore::current_request_event_cursor:PostgresStorage::current_request_event_cursor`, `sqlite:RequestEventStore::query_request_events_between_cursors:SqliteStorage::query_request_events_between_cursors`, `postgres:RequestEventStore::query_request_events_between_cursors:PostgresStorage::query_request_events_between_cursors`
- **Cache / no-query path:** Reads a storage cursor and cursor-bounded page. build_delta_events_payload in the admin layer then applies residual filters in memory (including excluding renewal source_kind by default); exhausted is computed from the pre-filter row count.
- **Side effects:** None
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-07C82F02A23B] Backend-Only — GET /admin/v1/events/stream

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:99`
- **Preconditions:** require_admin_auth middleware
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/events/stream`
  - Query: `principal_id`, `thread_id`, `model`, `upstream`, `upstream_id`, `status_class`, `source_kind`
  - Body: None
  - Headers: `last-event-id`
- **Handler:** `crate::events_routes::handle_events_stream`
- **Storage operations:** `sqlite:RequestEventStore::current_request_event_cursor:SqliteStorage::current_request_event_cursor`, `postgres:RequestEventStore::current_request_event_cursor:PostgresStorage::current_request_event_cursor`, `sqlite:RequestEventStore::query_request_events_between_cursors:SqliteStorage::query_request_events_between_cursors`, `postgres:RequestEventStore::query_request_events_between_cursors:PostgresStorage::query_request_events_between_cursors`
- **Cache / no-query path:** SSE: reads the current cursor and performs one reconnect backfill bounded by BACKFILL_MAX_EVENTS=500; reaching the cap emits a backfill-cap reset, otherwise delivery continues from in-memory event-bus/storage-tail broadcasts with heartbeats.
- **Side effects:** None
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-FC8E60B994F7] Backend-Only — GET /admin/subscription-quotas/latest

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/subscription_quotas.rs:57`
- **Preconditions:** require_admin_auth middleware
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/subscription-quotas/latest`
  - Query: `upstream_ids`, `windows`, `source`, `max_staleness_secs`
  - Body: None
  - Headers: None
- **Handler:** `handle_latest`
- **Storage operations:** `sqlite:UpstreamStore::list:SqliteStorage::list`, `postgres:UpstreamStore::list:PostgresStorage::list`
- **Cache / no-query path:** Reads upstream records for names/filtering; quota snapshots come from the in-memory dynamic subscription-quota cache.
- **Side effects:** None
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-063C7455BC4D] Backend-Only — GET /admin/subscription-quotas/series

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/subscription_quotas.rs:59`
- **Preconditions:** require_admin_auth middleware
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/subscription-quotas/series`
  - Query: `upstream_ids`, `windows`, `source`, `since_unix_secs`, `until_unix_secs`, `bucket_secs`, `max_points_per_series`
  - Body: None
  - Headers: None
- **Handler:** `handle_series`
- **Storage operations:** `sqlite:UpstreamStore::list:SqliteStorage::list`, `postgres:UpstreamStore::list:PostgresStorage::list`, `sqlite:UpstreamSubscriptionQuotaAggregateStore::list_subscription_quota_slim_checkpoints:SqliteStorage::list_subscription_quota_slim_checkpoints`, `postgres:UpstreamSubscriptionQuotaAggregateStore::list_subscription_quota_slim_checkpoints:PostgresStorage::list_subscription_quota_slim_checkpoints`
- **Cache / no-query path:** Reads upstream records and slim subscription-quota checkpoints through UpstreamSubscriptionQuotaAggregateStore::list_subscription_quota_slim_checkpoints; the admin-local series helper builds buckets in memory. Empty upstream/window/source sets short-circuit without checkpoint SQL.
- **Side effects:** None
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-B19EB1F130F2] Backend-Only — GET /admin/subscription-quotas/aggregate

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/subscription_quotas.rs:65`
- **Preconditions:** require_admin_auth middleware
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/subscription-quotas/aggregate`
  - Query: `upstream_ids`, `windows`, `source`, `max_staleness_secs`
  - Body: None
  - Headers: None
- **Handler:** `handle_aggregate`
- **Storage operations:** `sqlite:UpstreamStore::list:SqliteStorage::list`, `postgres:UpstreamStore::list:PostgresStorage::list`, `sqlite:UpstreamSubscriptionMetadataStore::list_upstream_subscription_metadata:SqliteStorage::list_upstream_subscription_metadata`, `postgres:UpstreamSubscriptionMetadataStore::list_upstream_subscription_metadata:PostgresStorage::list_upstream_subscription_metadata`, `sqlite:OrganizationMetadataStore::list_organization_metadata:SqliteStorage::list_organization_metadata`, `postgres:OrganizationMetadataStore::list_organization_metadata:PostgresStorage::list_organization_metadata`, `sqlite:UpstreamSubscriptionQuotaAggregateStore::list_subscription_quota_provider_lots:SqliteStorage::list_subscription_quota_provider_lots`, `postgres:UpstreamSubscriptionQuotaAggregateStore::list_subscription_quota_provider_lots:PostgresStorage::list_subscription_quota_provider_lots`, `sqlite:UsageTokenIntervalStore::sum_usage_tokens_for_intervals:SqliteStorage::sum_usage_tokens_for_intervals`, `postgres:UsageTokenIntervalStore::sum_usage_tokens_for_intervals:PostgresStorage::sum_usage_tokens_for_intervals`
- **Cache / no-query path:** Reads upstreams, subscription/organization metadata, provider-lot checkpoint history, and usage-rollup token sums for generated intervals; current quota candidates also come from the in-memory dynamic cache.
- **Side effects:** None
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-1CD1FD9DEBA0] Backend-Only — GET /admin/subscription-quotas/analysis

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/subscription_quotas.rs:72`
- **Preconditions:** require_admin_auth middleware
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/subscription-quotas/analysis`
  - Query: `upstream_ids`, `windows`, `since_unix_secs`, `until_unix_secs`, `source`
  - Body: None
  - Headers: None
- **Handler:** `handle_analysis`
- **Storage operations:** `sqlite:UpstreamStore::list:SqliteStorage::list`, `postgres:UpstreamStore::list:PostgresStorage::list`, `sqlite:UsageRollupStore::query_usage_rollups_for_upstreams_in_range:SqliteStorage::query_usage_rollups_for_upstreams_in_range`, `postgres:UsageRollupStore::query_usage_rollups_for_upstreams_in_range:PostgresStorage::query_usage_rollups_for_upstreams_in_range`, `sqlite:UpstreamSubscriptionQuotaAggregateStore::list_subscription_quota_slim_checkpoints:SqliteStorage::list_subscription_quota_slim_checkpoints`, `postgres:UpstreamSubscriptionQuotaAggregateStore::list_subscription_quota_slim_checkpoints:PostgresStorage::list_subscription_quota_slim_checkpoints`
- **Cache / no-query path:** Reads upstreams, usage rollups, and slim subscription-quota checkpoints through the admin-local series helper; latest quota state comes from the in-memory dynamic cache.
- **Side effects:** None
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-155F19B11A2E] Backend-Only — GET /admin/subscription-quotas/pool-history

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/subscription_quotas.rs:78`
- **Preconditions:** require_admin_auth middleware
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/subscription-quotas/pool-history`
  - Query: `windows`, `since_unix_secs`, `until_unix_secs`, `series_projection`, `max_points_per_series`
  - Body: None
  - Headers: None
- **Handler:** `handle_pool_history`
- **Storage operations:** `sqlite:PoolQuotaHistoryStore::list_latest_pool_quota_snapshot_summaries:SqliteStorage::list_latest_pool_quota_snapshot_summaries`, `postgres:PoolQuotaHistoryStore::list_latest_pool_quota_snapshot_summaries:PostgresStorage::list_latest_pool_quota_snapshot_summaries`, `sqlite:PoolQuotaHistoryStore::list_pool_quota_snapshot_summaries_in_range:SqliteStorage::list_pool_quota_snapshot_summaries_in_range`, `postgres:PoolQuotaHistoryStore::list_pool_quota_snapshot_summaries_in_range:PostgresStorage::list_pool_quota_snapshot_summaries_in_range`, `sqlite:PoolQuotaHistoryStore::list_pool_quota_chart_points_in_range:SqliteStorage::list_pool_quota_chart_points_in_range`, `postgres:PoolQuotaHistoryStore::list_pool_quota_chart_points_in_range:PostgresStorage::list_pool_quota_chart_points_in_range`
- **Cache / no-query path:** Always reads latest pooled summaries; series_projection=full reads raw summary rows, while chart reads raw or bucketed chart projections.
- **Side effects:** None
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SYS-04] Backend-Only — GET /admin/scheduler/status

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/scheduler.rs:23`
- **Preconditions:** require_admin_auth middleware
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/scheduler/status`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `status`
- **Storage operations:** `sqlite:SchedulerAdminHandle::recurring_runtime:SchedulerAdminHandle::recurring_runtime`, `postgres:SchedulerAdminHandle::recurring_runtime:SchedulerAdminHandle::recurring_runtime`
- **Cache / no-query path:** Combines in-memory connection pool stats (pool_in_use, pool_idle) with apalis scheduler job table queries
- **Side effects:** None
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SYS-03] Backend-Only — GET /admin/scheduler/failures

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/scheduler.rs:24`
- **Preconditions:** require_admin_auth middleware
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/scheduler/failures`
  - Query: `limit`, `offset`, `job_type`
  - Body: None
  - Headers: None
- **Handler:** `failures`
- **Storage operations:** `sqlite:SchedulerAdminHandle::list_failures:SchedulerAdminHandle::list_failures`, `postgres:SchedulerAdminHandle::list_failures:SchedulerAdminHandle::list_failures`
- **Cache / no-query path:** Query params ?limit (default 100, max 500) & ?offset. Sanitizes failure error summaries for sensitive tokens in memory
- **Side effects:** None
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-BD965DDC38E0] Backend-Only — GET /admin/v1/plugins/registry/{id}

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/v1/plugins.rs:41`
- **Preconditions:** require_admin_auth middleware
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/plugins/registry/{id}`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `get_registry`
- **Storage operations:** `sqlite:PluginRegistryStore::get_registry_entry_by_id:SqliteStorage::get_registry_entry_by_id`, `postgres:PluginRegistryStore::get_registry_entry_by_id:PostgresStorage::get_registry_entry_by_id`, `sqlite:PluginRegistryStore::get_blob_bytes:SqliteStorage::get_blob_bytes`, `postgres:PluginRegistryStore::get_blob_bytes:PostgresStorage::get_blob_bytes`
- **Cache / no-query path:** Includes ETag header W/"{revision}"
- **Side effects:** None
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-8895068281DE] Backend-Only — GET /admin/v1/plugin-chain-entries/{id}

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/v1/plugins.rs:59`
- **Preconditions:** require_admin_auth middleware
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/plugin-chain-entries/{id}`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `get_chain`
- **Storage operations:** `sqlite:PrincipalStore::list:SqliteStorage::list`, `postgres:PrincipalStore::list:PostgresStorage::list`, `sqlite:PluginRegistryStore::list_chain_for_principal:SqliteStorage::list_chain_for_principal`, `postgres:PluginRegistryStore::list_chain_for_principal:PostgresStorage::list_chain_for_principal`
- **Cache / no-query path:** Paginates principals with include_deleted=true, then scans the three stored plugin-chain slots (Router, ObservabilityHook, Shape) until the entry ID is found; RuntimeOnly slots are not stored or scanned.
- **Side effects:** None
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-166291C6FB8A] Backend-Only — GET /admin/v1/principals/{id}

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/v1/principals.rs:37`
- **Preconditions:** require_admin_auth middleware
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/principals/{id}`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `get_principal`
- **Storage operations:** `sqlite:PrincipalStore::get_by_id:SqliteStorage::get_by_id`, `postgres:PrincipalStore::get_by_id:PostgresStorage::get_by_id`
- **Cache / no-query path:** Includes ETag header W/"{revision}"
- **Side effects:** None
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-4B568BB20E22] Backend-Only — GET /admin/v1/principals/{id}/allowed_models

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/v1/principals.rs:50`
- **Preconditions:** require_admin_auth middleware
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/principals/{id}/allowed_models`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `get_allowed_models`
- **Storage operations:** `sqlite:PrincipalStore::get_by_id:SqliteStorage::get_by_id`, `postgres:PrincipalStore::get_by_id:PostgresStorage::get_by_id`
- **Cache / no-query path:** Includes ETag header W/"{revision}"
- **Side effects:** None
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-5E670B98E5A2] Backend-Only — GET /admin/principals/{id}/usage

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:26`
- **Preconditions:** require_admin_auth middleware
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/principals/{id}/usage`
  - Query: `range`, `step`, `group_by`
  - Body: None
  - Headers: None
- **Handler:** `principal_usage`
- **Storage operations:** `sqlite:UsageRollupStore::query_usage_rollups_in_range:SqliteStorage::query_usage_rollups_in_range`, `postgres:UsageRollupStore::query_usage_rollups_in_range:PostgresStorage::query_usage_rollups_in_range`
- **Cache / no-query path:** Validates the principal in the in-memory dynamic_view.principal_view before storage (404/no-query on miss), then reads usage rollups and filters the principal in memory.
- **Side effects:** None
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-BC21CB4F90EC] Backend-Only — GET /admin/v1/principals/{id}/usage

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:28`
- **Preconditions:** require_admin_auth middleware
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/principals/{id}/usage`
  - Query: `range`, `step`, `group_by`
  - Body: None
  - Headers: None
- **Handler:** `principal_usage`
- **Storage operations:** `sqlite:UsageRollupStore::query_usage_rollups_in_range:SqliteStorage::query_usage_rollups_in_range`, `postgres:UsageRollupStore::query_usage_rollups_in_range:PostgresStorage::query_usage_rollups_in_range`
- **Cache / no-query path:** Validates the principal in the in-memory dynamic_view.principal_view before storage (404/no-query on miss), then reads usage rollups and filters the principal in memory.
- **Side effects:** None
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-B7C3EDE0843A] Backend-Only — GET /admin/principals/{id}/limits

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:27`
- **Preconditions:** require_admin_auth middleware
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/principals/{id}/limits`
  - Query: `identity`
  - Body: None
  - Headers: None
- **Handler:** `principal_limits`
- **Storage operations:** None
- **Cache / no-query path:** PURE IN-MEMORY PATH: reads dynamic_view.principal_view and takes an in-memory snapshot via state.limit_engine.snapshot_for_principal
- **Side effects:** None
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-7E55E35928A2] Backend-Only — GET /admin/v1/principals/{id}/limits

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:29`
- **Preconditions:** require_admin_auth middleware
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/principals/{id}/limits`
  - Query: `identity`
  - Body: None
  - Headers: None
- **Handler:** `principal_limits`
- **Storage operations:** None
- **Cache / no-query path:** In-memory dynamic_view + LimitEngine snapshot
- **Side effects:** None
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-92FD477750A5] Backend-Only — GET /admin/principals/{id}/keys/{key_id}

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:30`
- **Preconditions:** require_admin_auth + authorize(AdminAction::SensitiveRead)
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/principals/{id}/keys/{key_id}`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `get_api_key`
- **Storage operations:** `sqlite:ManagedKeyStore::get:SqliteStorage::get`, `postgres:ManagedKeyStore::get:PostgresManagedKeyStore::get`
- **Cache / no-query path:** Direct single-key lookup in ManagedKeyStore
- **Side effects:** None
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-7383936FE537] Backend-Only — GET /admin/principals/{id}/keys/{key_id}/usage

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:45`
- **Preconditions:** require_admin_auth middleware
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/principals/{id}/keys/{key_id}/usage`
  - Query: `range`, `step`
  - Body: None
  - Headers: None
- **Handler:** `principal_key_usage`
- **Storage operations:** `sqlite:ManagedKeyStore::get:SqliteStorage::get`, `postgres:ManagedKeyStore::get:PostgresManagedKeyStore::get`, `sqlite:RequestEventStore::request_event_key_usage:SqliteStorage::request_event_key_usage`, `postgres:RequestEventStore::request_event_key_usage:PostgresStorage::request_event_key_usage`
- **Cache / no-query path:** Validates the principal in in-memory dynamic_view.principal_view before storage (404/no-query on miss), then verifies the managed key, enforces the 7-day range maximum, and reads key-usage time buckets.
- **Side effects:** None
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-7C0B903CAB5B] Backend-Only — GET /admin/v1/upstreams/{id}

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/v1/upstreams.rs:89`
- **Preconditions:** require_admin_auth middleware
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `GET /admin/v1/upstreams/{id}`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `get_upstream`
- **Storage operations:** `sqlite:UpstreamStore::list:SqliteStorage::list`, `postgres:UpstreamStore::list:PostgresStorage::list`
- **Cache / no-query path:** Paginates the complete upstream collection with UpstreamStore::list (page size 1000) and filters by stringified ID in memory; no get_by_id SQL is executed. Returns ETag from the matched revision. UpstreamResponse now serializes base_url: Option<Url> (crates/cc-lb-admin/src/v1/upstreams.rs:167,1587), so the field is present and null when the upstream has no custom base URL.
- **Side effects:** None
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-5E2D298E32DE] Backend-Only — POST /admin/principals/{id}/keys/{key_id}/revoke

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:32`
- **Preconditions:** require_admin_auth; non-GET method is authorized as AdminAction::Write
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `destructive_write`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/principals/{id}/keys/{key_id}/revoke`
  - Query: None
  - Body: None
  - Headers: `authorization`
- **Handler:** `revoke_api_key`
- **Storage operations:** `sqlite:ManagedKeyStore::revoke_zero_secrets:SqliteStorage::revoke_zero_secrets`, `postgres:ManagedKeyStore::revoke_zero_secrets:PostgresManagedKeyStore::revoke_zero_secrets`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Invalidates in-memory KeyStore cache; deletes index from fast-lookup map
- **Side effects:** Audit event `principal_key_revoke`; sets status to Revoked and zeroes secret salts/hashes in DB; permanently revokes key QA restore: Re-issue key or restore database snapshot (revocation zeroes secrets permanently).
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-D05CE36481FC] Backend-Only — POST /admin/principals/{id}/keys/{key_id}/disable

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:36`
- **Preconditions:** require_admin_auth; non-GET method is authorized as AdminAction::Write
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/principals/{id}/keys/{key_id}/disable`
  - Query: None
  - Body: None
  - Headers: `authorization`
- **Handler:** `disable_api_key`
- **Storage operations:** `sqlite:ManagedKeyStore::update:SqliteStorage::update`, `postgres:ManagedKeyStore::update:PostgresManagedKeyStore::update`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Updates KeyStore cached status to Disabled
- **Side effects:** Audit event `principal_key_disable`; prevents incoming proxy requests from authenticating with this key QA restore: Call POST /admin/principals/{id}/keys/{key_id}/enable.
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-EECEBC08D3B2] Backend-Only — POST /admin/principals/{id}/keys/{key_id}/enable

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:40`
- **Preconditions:** require_admin_auth; non-GET method is authorized as AdminAction::Write
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/principals/{id}/keys/{key_id}/enable`
  - Query: None
  - Body: None
  - Headers: `authorization`
- **Handler:** `enable_api_key`
- **Storage operations:** `sqlite:ManagedKeyStore::get:SqliteStorage::get`, `postgres:ManagedKeyStore::get:PostgresManagedKeyStore::get`, `sqlite:ManagedKeyStore::update:SqliteStorage::update`, `postgres:ManagedKeyStore::update:PostgresManagedKeyStore::update`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Updates KeyStore cached status to Active
- **Side effects:** Audit event `principal_key_enable`; permits incoming proxy requests authenticating with this key QA restore: Call POST /admin/principals/{id}/keys/{key_id}/disable.
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-EE67A2692EE2] Backend-Only — PUT /admin/v1/config/draft

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:58`
- **Preconditions:** require_admin_auth; non-GET method is authorized as AdminAction::Write
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `PUT /admin/v1/config/draft`
  - Query: None
  - Body: `draft`, `expected_revision`
  - Headers: `authorization`, `content-type`
- **Handler:** `put_config_draft`
- **Storage operations:** `sqlite:ConfigStore::get_config_draft:SqliteStorage::get_config_draft`, `postgres:ConfigStore::get_config_draft:PostgresStorage::get_config_draft`, `sqlite:ConfigStore::put_config_draft:SqliteStorage::put_config_draft`, `postgres:ConfigStore::put_config_draft:PostgresStorage::put_config_draft`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Stores in-memory draft copy via CurrentConfig::put_draft_config; invalidates TanStack queryKey ['config', 'draft']
- **Side effects:** Audit event `config_draft_put`; increments draft revision in DB and resets last_validated_revision to null QA restore: Re-put previous draft configuration or clear draft.
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-EEA580024B08] Backend-Only — POST /admin/v1/config/draft/validate

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:63`
- **Preconditions:** require_admin_auth; non-GET method is authorized as AdminAction::Write
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/v1/config/draft/validate`
  - Query: None
  - Body: `expected_revision`
  - Headers: `authorization`, `content-type`
- **Handler:** `validate_config_draft`
- **Storage operations:** `sqlite:ConfigStore::get_config_draft:SqliteStorage::get_config_draft`, `postgres:ConfigStore::get_config_draft:PostgresStorage::get_config_draft`, `sqlite:ConfigStore::set_last_validated_revision:SqliteStorage::set_last_validated_revision`, `postgres:ConfigStore::set_last_validated_revision:PostgresStorage::set_last_validated_revision`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** No cache write; updates last_validated_revision in storage
- **Side effects:** Audit event `config_draft_validate`; checks TOML/JSON schema and semantic validity; marks revision as validated or stores validation error QA restore: Re-validate previous revision.
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-B9B9B5853D97] Backend-Only — POST /admin/v1/config/apply

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:67`
- **Preconditions:** require_admin_auth; non-GET method is authorized as AdminAction::Write
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `destructive_write`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/v1/config/apply`
  - Query: None
  - Body: None
  - Headers: `authorization`
- **Handler:** `apply_config_draft`
- **Storage operations:** `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Atomically swaps DynamicView and CurrentConfig; clears in-memory draft; invalidates TanStack queryKey ['config', 'draft'], ['config', 'current']
- **Side effects:** Audit event `config_apply`; activates new runtime proxy state in memory without database entity mutation QA restore: Re-apply previous configuration draft or reload original config.
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-D6B9EEC29AA6] Backend-Only — POST /admin/v1/config/reload

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:73`
- **Preconditions:** require_admin_auth; non-GET method is authorized as AdminAction::Write
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `external_action`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/v1/config/reload`
  - Query: None
  - Body: None
  - Headers: `authorization`
- **Handler:** `reload_config`
- **Storage operations:** `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** No database storage entity modified (only audit entry written)
- **Side effects:** Audit event `config_reload_signal`; sends UNIX SIGHUP signal to pid (nix::sys::signal::kill) to trigger process config re-read from disk QA restore: Ensure valid configuration file exists on disk and send SIGHUP again.
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SYS-06] Backend-Only — POST /admin/v1/principals/{principal_id}/plugin-chain/rebalance

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/v1/plugins.rs:55`
- **Preconditions:** require_admin_auth; non-GET method is authorized as AdminAction::Write
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/v1/principals/{principal_id}/plugin-chain/rebalance`
  - Query: `slot`
  - Body: None
  - Headers: `authorization`
- **Handler:** `rebalance_chain`
- **Storage operations:** `sqlite:PluginRegistryStore::rebalance_chain:SqliteStorage::rebalance_chain`, `postgres:PluginRegistryStore::rebalance_chain:PostgresStorage::rebalance_chain`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** No cache write; invalidates TanStack queryKey ['plugin-chain', principal_id]; triggers dynamic rebind headers
- **Side effects:** Normalizes order_value gaps to 100-step increments using sparse_order::rebalance; increments revision for modified entries; records audit event 'plugin_chain_rebalance'; sends pg_notify 'cclb_plugin_changed' in Postgres; adds dynamic rebind headers QA restore: Reorder entries via reorder endpoint to prior order values if custom spacing was used.
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-B624778A38FE] Backend-Only — PUT /admin/v1/plugin-chain-entries/{id}

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/v1/plugins.rs:59`
- **Preconditions:** require_admin_auth; non-GET method is authorized as AdminAction::Write
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `PUT /admin/v1/plugin-chain-entries/{id}`
  - Query: None
  - Body: `config`, `sse_per_event`, `batched_events_per_flush`, `batched_flush_ms`
  - Headers: `authorization`, `if-match`, `content-type`
- **Handler:** `update_chain`
- **Storage operations:** `sqlite:PluginRegistryStore::update_chain_entry:SqliteStorage::update_chain_entry`, `postgres:PluginRegistryStore::update_chain_entry:PostgresStorage::update_chain_entry`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Triggers DynamicReview rebind; invalidates TanStack queryKey ['plugin-chain']
- **Side effects:** Updates config, sse_per_event, batched_events_per_flush, batched_flush_ms; increments revision; updates updated_at; records audit event 'plugin_chain_update'; sends pg_notify 'cclb_plugin_changed' in Postgres; adds dynamic rebind headers QA restore: PUT /admin/v1/plugin-chain-entries/{id} with original configuration values and updated If-Match revision.
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-B1B43DF36688] Backend-Only — PUT /admin/v1/principals/{id}

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/v1/principals.rs:36`
- **Preconditions:** require_admin_auth; non-GET method is authorized as AdminAction::Write
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `PUT /admin/v1/principals/{id}`
  - Query: None
  - Body: `name`, `allowed_models`, `allowed_upstreams`, `default_limits`, `cache_keepalive`
  - Headers: `authorization`, `if-match`, `content-type`
- **Handler:** `update_principal`
- **Storage operations:** `sqlite:PrincipalStore::update:SqliteStorage::update`, `postgres:PrincipalStore::update:PostgresStorage::update`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Rebinds DynamicViewHolder; invalidates queryKey ['principals']
- **Side effects:** Audit event `principal_update`; modifies principal limits, models, upstreams, or keepalive QA restore: repeat the update with captured prior fields and the new revision.
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SYS-05] Backend-Only — POST /admin/v1/router/preview

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/v1/router.rs:22`
- **Preconditions:** require_admin_auth; non-GET method is authorized as AdminAction::Write
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/v1/router/preview`
  - Query: None
  - Body: `principal_id`, `request_id`, `headers`, `body`
  - Headers: `authorization`, `content-type`
- **Handler:** `preview_route`
- **Storage operations:** None
- **Cache / no-query path:** Read-only simulation; no database queries executed; no storage touched
- **Side effects:** None (READ-ONLY HTTP POST). Simulates the candidate selection and router plugin chain in memory using RoutePreviewPort, returning routing trace and tier ranking without mutating any state or emitting audit logs
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-FA895179691B] Backend-Only — POST /admin/v1/upstreams/{id}/enable

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/v1/upstreams.rs:118`
- **Preconditions:** require_admin_auth; non-GET method is authorized as AdminAction::Write
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/v1/upstreams/{id}/enable`
  - Query: None
  - Body: None
  - Headers: `authorization`, `if-match`
- **Handler:** `enable_upstream`
- **Storage operations:** `sqlite:UpstreamStore::set_enabled:SqliteStorage::set_enabled`, `postgres:UpstreamStore::set_enabled:PostgresStorage::set_enabled`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Rebinds DynamicViewHolder; invalidates queryKey ['upstreams'] UpstreamResponse now serializes base_url: Option<Url> (crates/cc-lb-admin/src/v1/upstreams.rs:167,1586), so the field is present and null when the upstream has no custom base URL; credential secret and api_key_env provenance are not exposed in the response.
- **Side effects:** Audit event `upstream_enable`; activates upstream so load balancer can route requests to it QA restore: POST the disable endpoint with the returned revision.
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [API-SRC-F00657C8F5E0] Backend-Only — POST /admin/v1/upstreams/{id}/disable

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/v1/upstreams.rs:119`
- **Preconditions:** require_admin_auth; non-GET method is authorized as AdminAction::Write
- **Steps:** Direct API, operator, scheduler, health probe, or internal service caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `reversible_write`
- **Production applicability:** `available`
- **HTTP:** `POST /admin/v1/upstreams/{id}/disable`
  - Query: None
  - Body: None
  - Headers: `authorization`, `if-match`
- **Handler:** `disable_upstream`
- **Storage operations:** `sqlite:UpstreamStore::set_enabled:SqliteStorage::set_enabled`, `postgres:UpstreamStore::set_enabled:PostgresStorage::set_enabled`, `sqlite:AuditStore::append_audit:SqliteStorage::append_audit`, `postgres:AuditStore::append_audit:PostgresStorage::append_audit`
- **Cache / no-query path:** Rebinds DynamicViewHolder; invalidates queryKey ['upstreams'] UpstreamResponse now serializes base_url: Option<Url> (crates/cc-lb-admin/src/v1/upstreams.rs:167,1586), so the field is present and null when the upstream has no custom base URL; credential secret and api_key_env provenance are not exposed in the response.
- **Side effects:** Audit event `upstream_disable`; disables upstream from proxy candidate pool QA restore: POST the enable endpoint with the returned revision.
- **Expected UI:** No Admin Web caller. Validate through the authorized backend-only QA path.
- **Runtime result:** `PENDING`

### [PROD-API-5234FA674D91] Production Backend-Only — GET /admin/status

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:110 (e56d029e)`
- **Preconditions:** AdminAuth (legacy token)
- **Steps:** Direct deployed API or operator caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available` — Present only in the deployed production commit.
- **HTTP:** `GET /admin/status`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `crate::v1::status::status`
- **Storage operations:** None
- **Cache / no-query path:** PostgreSQL (via StatusResponse building); SQLite (via StatusResponse building)
- **Side effects:** Legacy status endpoint aliasing v1 status handler
- **Expected UI:** No deployed Admin Web caller was reconciled for this endpoint.
- **Runtime result:** `PENDING`

### [PROD-API-2B1E133DA8BF] Production Backend-Only — GET /admin/killswitch

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:111 (e56d029e)`
- **Preconditions:** AdminAuth (legacy token)
- **Steps:** Direct deployed API or operator caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `destructive_write`
- **Production applicability:** `available` — Present only in the deployed production commit.
- **HTTP:** `GET /admin/killswitch`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `get_killswitch`
- **Storage operations:** None
- **Cache / no-query path:** PostgreSQL (storage.killswitch_enabled); SQLite (storage.killswitch_enabled)
- **Side effects:** Query whether global emergency killswitch is currently engaged
- **Expected UI:** No deployed Admin Web caller was reconciled for this endpoint.
- **Runtime result:** `PENDING`

### [PROD-API-DCBF12986EB5] Production Backend-Only — GET /admin/v1/killswitch

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:116 (e56d029e)`
- **Preconditions:** AdminAuth (legacy token)
- **Steps:** Direct deployed API or operator caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `destructive_write`
- **Production applicability:** `available` — Present only in the deployed production commit.
- **HTTP:** `GET /admin/v1/killswitch`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `get_killswitch`
- **Storage operations:** None
- **Cache / no-query path:** PostgreSQL (storage.killswitch_enabled); SQLite (storage.killswitch_enabled)
- **Side effects:** v1 alias: Query whether global emergency killswitch is currently engaged
- **Expected UI:** No deployed Admin Web caller was reconciled for this endpoint.
- **Runtime result:** `PENDING`

### [PROD-API-8AA0DB7F3093] Production Backend-Only — POST /admin/v1/killswitch

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:117 (e56d029e)`
- **Preconditions:** AdminAuth (legacy token)
- **Steps:** Direct deployed API or operator caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `destructive_write`
- **Production applicability:** `available` — Present only in the deployed production commit.
- **HTTP:** `POST /admin/v1/killswitch`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `set_killswitch`
- **Storage operations:** None
- **Cache / no-query path:** PostgreSQL (storage.set_killswitch_enabled(true)); SQLite (storage.set_killswitch_enabled(true))
- **Side effects:** v1 alias: Engage global emergency killswitch
- **Expected UI:** No deployed Admin Web caller was reconciled for this endpoint.
- **Runtime result:** `PENDING`

### [PROD-API-C482408C3112] Production Backend-Only — DELETE /admin/v1/killswitch

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:118 (e56d029e)`
- **Preconditions:** AdminAuth (legacy token)
- **Steps:** Direct deployed API or operator caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `destructive_write`
- **Production applicability:** `available` — Present only in the deployed production commit.
- **HTTP:** `DELETE /admin/v1/killswitch`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `clear_killswitch`
- **Storage operations:** None
- **Cache / no-query path:** PostgreSQL (storage.set_killswitch_enabled(false)); SQLite (storage.set_killswitch_enabled(false))
- **Side effects:** v1 alias: Disengage global emergency killswitch
- **Expected UI:** No deployed Admin Web caller was reconciled for this endpoint.
- **Runtime result:** `PENDING`

### [PROD-API-6B655BAB2645] Production Backend-Only — GET /admin/v1/credentials

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:163 (e56d029e)`
- **Preconditions:** AdminAuth (legacy token)
- **Steps:** Direct deployed API or operator caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available` — Present only in the deployed production commit.
- **HTTP:** `GET /admin/v1/credentials`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `crate::credentials::list_credentials`
- **Storage operations:** None
- **Cache / no-query path:** PostgreSQL (PrincipalStore::list, get_oauth_ciphertext); SQLite (PrincipalStore::list, get_oauth_ciphertext)
- **Side effects:** v1 alias: Lists observed credentials across principals
- **Expected UI:** No deployed Admin Web caller was reconciled for this endpoint.
- **Runtime result:** `PENDING`

### [PROD-API-D8B657F6FCA4] Production Backend-Only — GET /admin/v1/oauth/status

- **Entry type:** `backend_endpoint`
- **Source:** `crates/cc-lb-admin/src/routes.rs:167 (e56d029e)`
- **Preconditions:** AdminAuth (legacy token)
- **Steps:** Direct deployed API or operator caller
- **Scope:** `backend_only` — not_applicable
- **Risk:** `read`
- **Production applicability:** `available` — Present only in the deployed production commit.
- **HTTP:** `GET /admin/v1/oauth/status`
  - Query: None
  - Body: None
  - Headers: None
- **Handler:** `crate::credentials::list_oauth_status`
- **Storage operations:** None
- **Cache / no-query path:** PostgreSQL (PrincipalStore::list, get_oauth_ciphertext); SQLite (PrincipalStore::list, get_oauth_ciphertext)
- **Side effects:** v1 alias: Lists active OAuth flow status and tokens
- **Expected UI:** No deployed Admin Web caller was reconciled for this endpoint.
- **Runtime result:** `PENDING`

## 5. Explicit Gaps

- **GAP-GLOBAL-01** (medium): The legacy inventory documented localStorage['cclb.auth.token'], whereas the actual codebase uses AUTH_TOKEN_KEY = 'cc-lb-admin-token' in src/lib/auth.ts:1.
- **GAP-GLOBAL-02** (low): The legacy inventory documented localStorage['theme'] with .dark class toggling. The actual codebase uses STORAGE_KEY = 'cclb.theme' in src/lib/theme.ts:10 and sets data-theme attribute on document.documentElement.
- **GAP-GLOBAL-03** (medium): CommandPalette.tsx lists Overview, Upstreams, Principals, Plugins, Logs, and Settings in the Pages group, but omits the /audit route, even though it exists in Sidebar.tsx.
- **GAP-OV-01** (high): The legacy inventory documented GET /admin/dashboard/usage for UI-OV-02. The actual codebase requests GET /admin/usage with query parameters range, step, group_by=principal, and projection=totals.
- **GAP-OV-02** (high): The legacy inventory documented a single GET /admin/dashboard/pool-history endpoint for UI-OV-03, UI-OV-04, and UI-OV-07C. The actual codebase splits this into two distinct endpoints: GET /admin/v1/subscription-quotas/aggregate (snapshot stacked bars) and GET /admin/v1/subscription-quotas/pool-history (multi-series Recharts area chart).
- **GAP-OV-03** (medium): UI-OV-12 was documented purely as a UI row click action, but selecting any finalized request row triggers an asynchronous network fetch to GET /admin/v1/events/detail/{eventId} via useRequestEventDetail.
- **GAP-LOGS-1** (low): SSE reconnect delta query (/admin/v1/events/delta) is only called when lastCursor is known and resumeNeedsBackfill is true. If connection drops on initial load before any event arrives, it reconnects directly without backfill.
- **GAP-LOGS-2** (low): Model filter is debounced by 300ms in client. Fast typing will not issue network requests per keystroke, but typing rapidly across the 300ms boundary produces sequential queries.
- **GAP-PRINCIPALS-1** (documented): {"source_contract":"Principals","gap_id":"GAP-PRINCIPALS-1","area":"Cache Keepalive Sessions List Query Limit","observation":"useCacheKeepaliveSummary explicitly queries with limit=0 to return metrics without rows. However, useCacheKeepaliveSessions does not specify a limit parameter, causing the request to omit limit and rely on backend default pagination size (e.g. 50 or 100)."}
- **GAP-PRINCIPALS-2** (documented): {"source_contract":"Principals","gap_id":"GAP-PRINCIPALS-2","area":"All Selector Asymmetry","observation":"HorizonToggle with 'all' sends horizon=all in the query string. In contrast, status filter 'all' omits the status parameter entirely (undefined). The 'error' filter chip also omits status but sets error=true in the query string."}
- **GAP-PRINCIPALS-3** (documented): {"source_contract":"Principals","gap_id":"GAP-PRINCIPALS-3","area":"Horizon Cursor Reset Behavior","observation":"Changing the horizon value creates a distinct TanStack Query key (['cache-keepalive', 'sessions', principalId, { horizon, ... }]). This resets the infinite query cursor back to page 1 (pageParam: undefined). Cursor metadata is not shared across horizons."}
- **GAP-PRINCIPALS-4** (documented): {"source_contract":"Principals","gap_id":"GAP-PRINCIPALS-4","area":"Settings Drawer LLM Judge Mockup","observation":"The LLM Judge control in CacheKeepaliveSettingsDrawer is visually rendered with opacity-50 and a badge 'Reserved for a future release', with no interactive handler or backend wire mapping."}
- **GAP-PRINCIPALS-5** (documented): {"source_contract":"Principals","gap_id":"GAP-PRINCIPALS-5","area":"Plaintext API Key Dismissal Guard","observation":"ApiKeysCard explicitly prevents Escape, backdrop click, or close button dismissal while a newly generated plaintext key is displayed, requiring the operator to click 'Done' after copying."}
- **GAP-PLUG-DOWNLOAD** (design_gap): WASM binary download feature is completely missing from both backend and frontend. The prompt asks for 'download' tracking, but neither GET /admin/v1/plugins/wasm/{id} nor any export button in PluginDetailIntegrity / PluginCatalog exists.
- **GAP-PLUG-BACKEND-ONLY-ENDPOINTS** (unreachable_surface): Four REST endpoints implemented in crates/cc-lb-admin/src/v1/plugins.rs are never called by the admin web UI: (1) GET /admin/v1/plugins/registry/{id} (UI fetches whole registry and finds by ID in memory), (2) POST /admin/v1/principals/{principal_id}/plugin-chain/rebalance (re-spacing order integers), (3) GET /admin/v1/plugin-chain-entries/{id} (single chain entry fetch), and (4) PUT /admin/v1/plugin-chain-entries/{id} (single chain entry update).
- **GAP-PLUG-HEALTH-METRICS** (display_only): PluginDetailOperate displays static text indicating 'Health metrics for individual plugins are not yet available in the dashboard' and points to raw Prometheus metric names (cc_lb_plugin_call_duration_seconds, cc_lb_plugin_trap_total). There is no live telemetry or chart in the UI.
- **GAP-SET-01** (documented): GET /admin/config/diff?from={revA}&to={revB} and qk.configDiff query key exist, but there is NO UI caller, modal, or comparison button in Admin Web. History rows in ConfigHistorySection are static table rows with no expansion or comparison triggers.
- **GAP-SET-02** (documented): Neither the backend axum routes (routes.rs) nor the frontend (settings.tsx) implements a draft discard / rollback endpoint or button. An operator must manually overwrite the draft text with the current configuration to discard draft changes.
- **GAP-AUD-01** (documented): AuditPage requests useAudit with limit='200'. The UI table displays at most 200 admin rows and a static 'No more entries' footer. There are no next/previous page buttons, cursor tokens, or infinite scrolling.
- **GAP-AUD-02** (documented): GET /admin/audit returns all audit_log_v1 rows matching query bounds, but the UI client explicitly filters for entries where admin_action != null || kind != null. Regular proxy request logs returned by the endpoint are silently omitted from the table view.
- **GAP-UPSTREAMS-1** (documented): WarmupConfigModal is conditionally rendered when pluginSnapshot exists with open={configOpen}, but configOpen is initialized to false and no button or interaction anywhere in WarmupCardMinimal or the UI ever calls setConfigOpen(true). It is an unreachable dead modal in production.
- **GAP-UPSTREAMS-2** (documented): WarmupRecentStrip is exported in parts/WarmupRecentStrip.tsx but is never imported or rendered by any component or route in the entire admin web application.
- **GAP-UPSTREAMS-3** (documented): Triggering Fire Now sends an actual /messages payload directly upstream to Anthropic, burning real account tokens and potentially incurring billable usage or rate limit depletion.
- **GAP-STATIC-RUNTIME** (runtime_pending): Static source reconciliation does not prove browser behavior, production entity coverage, latency, or deployed availability.
- **GAP-DEPLOYED-DELTA** (production_applicability_pending): No deployed-delta.json has been supplied yet; latest-source items stay out of an asserted production-complete matrix until applicability is reconciled.
- **GAP-NONEXISTENT-CONFIG-DISCARD** (nonexistent): No Admin Web action or backend route discards a config draft.
- **GAP-BACKEND-ONLY-CONFIG-DIFF** (backend_only): GET /admin/config/diff exists but has no Admin Web caller.
- **GAP-NONEXISTENT-CONFIG-DOWNLOAD** (nonexistent): The UI downloads a database export from GET /admin/v1/export; it has no standalone config download action.
- **GAP-NONEXISTENT-WASM-DOWNLOAD** (nonexistent): Neither frontend nor backend exposes uploaded WASM bytecode download.
- **GAP-UPSTREAMS-4** (documented): The 'env:{api_key_env}' API Key display branch is source-only/unreachable: UpstreamResponse (crates/cc-lb-admin/src/v1/upstreams.rs:163) omits api_key_env, so the server never returns the env var name (write-only contract: name resolved to ciphertext, never stored/exposed). Separately, a blank Base URL sends base_url: null, but the backend Option<Url> update treats null as no-change and retains the existing URL — clearing an override is not supported. This is a static source observation recorded separately from the runtime-progress overlay's settings_null_default_get_reload triage; it is not asserted as runtime-verified.

## 6. Legacy ID Mapping

| Legacy ID | Status | Canonical Target / Endpoint | Reason |
|---|---|---|---|
| **UI-ROOT-01** | `preserved` | UI-ROOT-01 | Same method/path and source-backed request occurrence. |
| **UI-ROOT-02** | `preserved` | UI-ROOT-02 | Same method/path and source-backed request occurrence. |
| **UI-ROOT-03** | `preserved` | UI-ROOT-03 | Same method/path and source-backed request occurrence. |
| **UI-CMD-01** | `preserved` | UI-CMD-01 | Same source-backed UI action. |
| **UI-CMD-02** | `preserved` | UI-CMD-02 | Same source-backed UI action. |
| **UI-TOP-01** | `preserved` | UI-TOP-01 | Same source-backed UI action. |
| **UI-TOP-02** | `preserved` | UI-TOP-02 | Same source-backed UI action. |
| **UI-OV-01** | `preserved` | UI-OV-01 | Same method/path and source-backed request occurrence. |
| **UI-OV-02** | `preserved` | UI-OV-02 | Same method/path and source-backed request occurrence. |
| **UI-OV-03** | `preserved` | UI-OV-03 | Same method/path and source-backed request occurrence. |
| **UI-OV-04** | `preserved` | UI-OV-04 | Same method/path and source-backed request occurrence. |
| **UI-OV-05** | `preserved` | UI-OV-05 | Same method/path and source-backed request occurrence. |
| **UI-OV-06** | `preserved` | UI-OV-06 | Same method/path and source-backed request occurrence. |
| **UI-OV-07** | `preserved` | UI-OV-07 | Same source-backed UI action. |
| **UI-OV-08** | `preserved` | UI-OV-08 | Same source-backed UI action. |
| **UI-OV-09** | `preserved` | UI-OV-09 | Same source-backed UI action. |
| **UI-OV-10** | `preserved` | UI-OV-10 | Same source-backed UI action. |
| **UI-OV-11** | `preserved` | UI-OV-11 | Same method/path and source-backed request occurrence. |
| **UI-OV-12** | `preserved` | UI-OV-12 | Same method/path and source-backed request occurrence. |
| **UI-UP-01** | `preserved` | UI-UP-01 | Same method/path and source-backed request occurrence. |
| **UI-UP-02** | `preserved` | UI-UP-02 | Same method/path and source-backed request occurrence. |
| **UI-UP-03** | `preserved` | UI-UP-03 | Same method/path and source-backed request occurrence. |
| **UI-UP-04** | `preserved` | UI-UP-04 | Same method/path and source-backed request occurrence. |
| **UI-UP-05** | `preserved` | UI-UP-05 | Same source-backed UI action. |
| **UI-UP-06** | `preserved` | UI-UP-06 | Same method/path and source-backed request occurrence. |
| **UI-UP-07** | `preserved` | UI-UP-07 | Same method/path and source-backed request occurrence. |
| **UI-UP-08** | `retired` | — | Legacy compound quota coordinator was replaced by distinct source-backed series and analysis actions. |
| **UI-UP-09** | `preserved` | UI-UP-09 | Same source-backed UI action. |
| **UI-UP-10** | `preserved` | UI-UP-10 | Same source-backed UI action. |
| **UI-UP-11** | `preserved` | UI-UP-11 | Same method/path and source-backed request occurrence. |
| **UI-UP-12** | `preserved` | UI-UP-12 | Same source-backed request occurrence with a corrected stale historical method/path. |
| **UI-UP-13** | `preserved` | UI-UP-13 | Same method/path and source-backed request occurrence. |
| **UI-UP-14** | `preserved` | UI-UP-14 | Same method/path and source-backed request occurrence. |
| **UI-UP-15** | `preserved` | UI-UP-15 | Same source-backed request occurrence with a corrected stale historical method/path. |
| **UI-UP-16** | `preserved` | UI-UP-16 | Same source-backed request occurrence with a corrected stale historical method/path. |
| **UI-UP-17** | `preserved` | UI-UP-17 | Same method/path and source-backed request occurrence. |
| **UI-UP-18** | `preserved` | UI-UP-18 | Same method/path and source-backed request occurrence. |
| **UI-UP-19** | `preserved` | UI-UP-19 | Same method/path and source-backed request occurrence. |
| **UI-UP-20** | `preserved` | UI-UP-20 | Same source-backed UI action. |
| **UI-UP-22** | `preserved` | UI-UP-22 | Same method/path and source-backed request occurrence. |
| **UI-UP-23** | `preserved` | UI-UP-23 | Same method/path and source-backed request occurrence. |
| **UI-UP-24** | `preserved` | UI-UP-24 | Same method/path and source-backed request occurrence. |
| **UI-UP-25** | `preserved` | UI-UP-25 | Same source-backed UI action. |
| **UI-UP-26** | `preserved` | UI-UP-26 | Same method/path and source-backed request occurrence. |
| **UI-UP-27** | `preserved` | UI-UP-27 | Same method/path and source-backed request occurrence. |
| **UI-UP-28** | `preserved` | UI-UP-28 | Same method/path and source-backed request occurrence. |
| **UI-UP-29** | `preserved` | UI-UP-29 | Same method/path and source-backed request occurrence. |
| **UI-PR-01** | `preserved` | UI-PR-01 | Same method/path and source-backed request occurrence. |
| **UI-PR-02** | `preserved` | UI-PR-02 | Same source-backed UI action. |
| **UI-PR-03** | `preserved` | UI-PR-03 | The historical combined write row is retained as the parent toggle action; enable and disable are separate atomic child requests. |
| **UI-PR-04** | `preserved` | UI-PR-04 | Same method/path and source-backed request occurrence. |
| **UI-PR-05** | `preserved` | UI-PR-05 | Same method/path and source-backed request occurrence. |
| **UI-PR-06** | `preserved` | UI-PR-06 | Same method/path and source-backed request occurrence. |
| **UI-PR-07** | `preserved` | UI-PR-07 | Same method/path and source-backed request occurrence. |
| **UI-PR-08** | `preserved` | UI-PR-08 | Same method/path and source-backed request occurrence. |
| **UI-PR-09** | `preserved` | UI-PR-09 | Same method/path and source-backed request occurrence. |
| **UI-PR-10** | `preserved` | UI-PR-10 | Same source-backed UI action. |
| **UI-PR-11** | `preserved` | UI-PR-11 | Same method/path and source-backed request occurrence. |
| **UI-PR-12** | `preserved` | UI-PR-12 | Same method/path and source-backed request occurrence. |
| **UI-PR-13** | `retired` | — | Legacy compound principal-detail coordinator was replaced by separately sourced chain and registry requests. |
| **UI-PR-14** | `preserved` | UI-PR-14 | Same method/path and source-backed request occurrence. |
| **UI-PR-15** | `preserved` | UI-PR-15 | Same method/path and source-backed request occurrence. |
| **UI-PR-16** | `preserved` | UI-PR-16 | Same method/path and source-backed request occurrence. |
| **UI-PR-18** | `preserved` | UI-PR-18 | Same source-backed UI action. |
| **UI-PR-19** | `preserved` | UI-PR-19 | Same method/path and source-backed request occurrence. |
| **UI-PR-20** | `preserved` | UI-PR-20 | Same method/path and source-backed request occurrence. |
| **UI-PR-21** | `preserved` | UI-PR-21 | Same method/path and source-backed request occurrence. |
| **UI-PR-22** | `preserved` | UI-PR-22 | Same method/path and source-backed request occurrence. |
| **UI-PR-23** | `preserved` | UI-PR-23 | Same method/path and source-backed request occurrence. |
| **UI-LOG-01** | `preserved` | UI-LOG-01 | Same method/path and source-backed request occurrence. |
| **UI-LOG-02** | `preserved` | UI-LOG-02 | Same method/path and source-backed request occurrence. |
| **UI-LOG-03** | `preserved` | UI-LOG-03 | Same method/path and source-backed request occurrence. |
| **UI-LOG-04** | `preserved` | UI-LOG-04 | Same source-backed UI action. |
| **UI-LOG-05** | `preserved` | UI-LOG-05 | Same source-backed UI action. |
| **UI-LOG-06** | `preserved` | UI-LOG-06 | Same source-backed UI action. |
| **UI-LOG-07** | `preserved` | UI-LOG-07 | Same source-backed UI action. |
| **UI-LOG-08** | `preserved` | UI-LOG-08 | Same source-backed UI action. |
| **UI-LOG-09** | `preserved` | UI-LOG-09 | Same source-backed UI action. |
| **UI-LOG-10** | `preserved` | UI-LOG-10 | Same source-backed UI action. |
| **UI-LOG-11** | `preserved` | UI-LOG-11 | Same source-backed UI action. |
| **UI-LOG-12** | `preserved` | UI-LOG-12 | Same source-backed UI action. |
| **UI-LOG-13** | `preserved` | UI-LOG-13 | Same source-backed UI action. |
| **UI-LOG-14** | `preserved` | UI-LOG-14 | Same method/path and source-backed request occurrence. |
| **UI-LOG-15** | `preserved` | UI-LOG-15 | Same source-backed UI action. |
| **UI-LOG-16** | `preserved` | UI-LOG-16 | Same method/path and source-backed request occurrence. |
| **UI-LOG-17** | `preserved` | UI-LOG-17 | Same source-backed UI action. |
| **UI-LOG-18** | `preserved` | UI-LOG-18 | Same method/path and source-backed request occurrence. |
| **UI-PLUG-01** | `preserved` | UI-PLUG-01 | Same method/path and source-backed request occurrence. |
| **UI-PLUG-02** | `preserved` | UI-PLUG-02 | Same method/path and source-backed request occurrence. |
| **UI-PLUG-03** | `preserved` | UI-PLUG-03 | Same method/path and source-backed request occurrence. |
| **UI-PLUG-04** | `preserved` | UI-PLUG-04 | Same method/path and source-backed request occurrence. |
| **UI-PLUG-05** | `preserved` | UI-PLUG-05 | Same source-backed UI action. |
| **UI-PLUG-06** | `preserved` | UI-PLUG-06 | Same method/path and source-backed request occurrence. |
| **UI-PLUG-07** | `preserved` | UI-PLUG-07 | Same method/path and source-backed request occurrence. |
| **UI-PLUG-08** | `preserved` | UI-PLUG-08 | Same method/path and source-backed request occurrence. |
| **UI-SET-01** | `preserved` | UI-SET-01 | Same method/path and source-backed request occurrence. |
| **UI-SET-02** | `preserved` | UI-SET-02 | Same method/path and source-backed request occurrence. |
| **UI-SET-03** | `preserved` | UI-SET-03 | Same source-backed UI action. |
| **UI-SET-04** | `preserved` | UI-SET-04 | Same method/path and source-backed request occurrence. |
| **UI-SET-05** | `preserved` | UI-SET-05 | Same method/path and source-backed request occurrence. |
| **UI-SET-06** | `preserved` | UI-SET-06 | Same method/path and source-backed request occurrence. |
| **UI-SET-07** | `preserved` | UI-SET-07 | Same method/path and source-backed request occurrence. |
| **UI-SET-08** | `preserved` | UI-SET-08 | Same method/path and source-backed request occurrence. |
| **UI-SET-10** | `preserved` | UI-SET-10 | Same source-backed UI action. |
| **UI-SET-11** | `preserved` | UI-SET-11 | Same source-backed UI action. |
| **UI-AUD-01** | `preserved` | UI-AUD-01 | Same method/path and source-backed request occurrence. |
| **UI-AUD-02** | `preserved` | UI-AUD-02 | Same method/path and source-backed request occurrence. |
| **UI-AUD-03** | `preserved` | UI-AUD-03 | Same method/path and source-backed request occurrence. |
| **UI-AUD-04** | `preserved` | UI-AUD-04 | Same source-backed UI action. |
| **UI-AUD-05** | `preserved` | UI-AUD-05 | Same method/path and source-backed request occurrence. |
| **UI-AUD-06** | `preserved` | UI-AUD-06 | Same source-backed UI action. |
| **API-SYS-01** | `retired` | — | GET /admin/health/state is not registered in current source; GET /admin/health is a distinct operation. |
| **API-SYS-02** | `moved_to_api_catalog` | GET /internal/v1/partials/{event_id} | The same backend-only operation is now sourced from the API catalog. |
| **API-SYS-03** | `moved_to_api_catalog` | GET /admin/scheduler/failures | The same backend-only operation is now sourced from the API catalog. |
| **API-SYS-04** | `moved_to_api_catalog` | GET /admin/scheduler/status | The same backend-only operation is now sourced from the API catalog. |
| **API-SYS-05** | `moved_to_api_catalog` | POST /admin/v1/router/preview | The same backend-only operation is now sourced from the API catalog. |
| **API-SYS-06** | `moved_to_api_catalog` | POST /admin/v1/principals/{id}/plugin-chain/rebalance | The same backend-only operation is now sourced from the API catalog. |
| **UI-CMD-03** | `preserved` | UI-CMD-03 | Same method/path and source-backed request occurrence. |
| **UI-CMD-04** | `preserved` | UI-CMD-04 | Same method/path and source-backed request occurrence. |
| **UI-CMD-05** | `preserved` | UI-CMD-05 | Same source-backed UI action. |
| **UI-CMD-06** | `preserved` | UI-CMD-06 | Same source-backed UI action. |
| **UI-TOP-03** | `preserved` | UI-TOP-03 | Same source-backed UI action. |
| **UI-TOP-04** | `preserved` | UI-TOP-04 | Same source-backed UI action. |
| **UI-TOP-05** | `preserved` | UI-TOP-05 | Same source-backed UI action. |
| **UI-CMD-07** | `preserved_alias` | UI-CMD-02 | Same source-backed UI action. |
| **UI-OV-07A** | `preserved_alias` | UI-OV-01 | Same method/path and source-backed request occurrence. |
| **UI-OV-07B** | `preserved_alias` | UI-OV-02 | Same method/path and source-backed request occurrence. |
| **UI-OV-07C** | `preserved_alias` | UI-OV-04 | Same method/path and source-backed request occurrence. |
| **UI-OV-13** | `preserved` | UI-OV-13 | Same method/path and source-backed request occurrence. |
| **UI-OV-14** | `preserved` | UI-OV-14 | Same method/path and source-backed request occurrence. |
| **UI-OV-15** | `preserved` | UI-OV-15 | Same method/path and source-backed request occurrence. |
| **UI-OV-16** | `preserved` | UI-OV-16 | Same source-backed UI action. |
| **UI-OV-17** | `preserved` | UI-OV-17 | Same source-backed UI action. |
| **UI-UP-08A** | `preserved` | UI-UP-08A | Same method/path and source-backed request occurrence. |
| **UI-UP-08B** | `preserved` | UI-UP-08B | Same method/path and source-backed request occurrence. |
| **UI-UP-09A** | `preserved` | UI-UP-09A | Same method/path and source-backed request occurrence. |
| **UI-UP-09B** | `preserved` | UI-UP-09B | Same method/path and source-backed request occurrence. |
| **UI-UP-30** | `preserved` | UI-UP-30 | Same method/path and source-backed request occurrence. |
| **UI-UP-31** | `preserved` | UI-UP-31 | Same method/path and source-backed request occurrence. |
| **UI-UP-32** | `preserved` | UI-UP-32 | Same method/path and source-backed request occurrence. |
| **UI-UP-33** | `preserved` | UI-UP-33 | Same method/path and source-backed request occurrence. |
| **UI-UP-34** | `preserved` | UI-UP-34 | Same method/path and source-backed request occurrence. |
| **UI-UP-35** | `preserved` | UI-UP-35 | Same method/path and source-backed request occurrence. |
| **UI-UP-36** | `preserved` | UI-UP-36 | Same source-backed UI action. |
| **UI-UP-37** | `preserved` | UI-UP-37 | Same source-backed UI action. |
| **UI-UP-38** | `preserved` | UI-UP-38 | Same source-backed UI action. |
| **UI-UP-39** | `preserved` | UI-UP-39 | Same source-backed UI action. |
| **UI-UP-40** | `preserved` | UI-UP-40 | Same source-backed UI action. |
| **UI-PR-07A** | `preserved_alias` | UI-PR-07 | Same method/path and source-backed request occurrence. |
| **UI-PR-09A** | `preserved` | UI-PR-09A | Same source-backed UI action. |
| **UI-PR-10A** | `preserved` | UI-PR-10A | Same method/path and source-backed request occurrence. |
| **UI-PR-11A** | `preserved_alias` | UI-PR-10A | Same method/path and source-backed request occurrence. |
| **UI-PR-11B** | `preserved` | UI-PR-11B | Same method/path and source-backed request occurrence. |
| **UI-PR-12A** | `preserved_alias` | UI-PR-12 | Same method/path and source-backed request occurrence. |
| **UI-PR-12B** | `preserved_alias` | UI-PR-10 | Same source-backed UI action. |
| **UI-PR-12C** | `preserved_alias` | UI-PR-10 | Same source-backed UI action. |
| **UI-PR-13A** | `preserved` | UI-PR-13A | Same method/path and source-backed request occurrence. |
| **UI-PR-13B** | `preserved` | UI-PR-13B | Same method/path and source-backed request occurrence. |
| **UI-PR-13C** | `preserved` | UI-PR-13C | Same method/path and source-backed request occurrence. |
| **UI-PR-13D** | `preserved` | UI-PR-13D | Same method/path and source-backed request occurrence. |
| **UI-PR-13E** | `preserved` | UI-PR-13E | Same method/path and source-backed request occurrence. |
| **UI-PR-15B** | `preserved` | UI-PR-15B | Same method/path and source-backed request occurrence. |
| **UI-PR-16B** | `preserved` | UI-PR-16B | Same method/path and source-backed request occurrence. |
| **UI-PR-16C** | `preserved` | UI-PR-16C | Same method/path and source-backed request occurrence. |
| **UI-PR-18A** | `preserved` | UI-PR-18A | Same method/path and source-backed request occurrence. |
| **UI-PR-18B** | `preserved` | UI-PR-18B | Same method/path and source-backed request occurrence. |
| **UI-PR-19B** | `preserved` | UI-PR-19B | Same method/path and source-backed request occurrence. |
| **UI-PR-19C** | `preserved` | UI-PR-19C | Same method/path and source-backed request occurrence. |
| **UI-PR-24** | `preserved` | UI-PR-24 | Same method/path and source-backed request occurrence. |
| **UI-LOG-05A** | `preserved` | UI-LOG-05A | Same method/path and source-backed request occurrence. |
| **UI-LOG-05B** | `preserved` | UI-LOG-05B | Same method/path and source-backed request occurrence. |
| **UI-LOG-05C** | `preserved` | UI-LOG-05C | Same method/path and source-backed request occurrence. |
| **UI-LOG-06A** | `preserved` | UI-LOG-06A | Same method/path and source-backed request occurrence. |
| **UI-LOG-06B** | `preserved` | UI-LOG-06B | Same method/path and source-backed request occurrence. |
| **UI-LOG-06C** | `preserved` | UI-LOG-06C | Same method/path and source-backed request occurrence. |
| **UI-LOG-07A** | `preserved` | UI-LOG-07A | Same method/path and source-backed request occurrence. |
| **UI-LOG-07B** | `preserved` | UI-LOG-07B | Same method/path and source-backed request occurrence. |
| **UI-LOG-07C** | `preserved` | UI-LOG-07C | Same method/path and source-backed request occurrence. |
| **UI-LOG-08A** | `preserved` | UI-LOG-08A | Same method/path and source-backed request occurrence. |
| **UI-LOG-08B** | `preserved` | UI-LOG-08B | Same method/path and source-backed request occurrence. |
| **UI-LOG-08C** | `preserved` | UI-LOG-08C | Same method/path and source-backed request occurrence. |
| **UI-LOG-09A** | `preserved` | UI-LOG-09A | Same method/path and source-backed request occurrence. |
| **UI-LOG-09B** | `preserved` | UI-LOG-09B | Same method/path and source-backed request occurrence. |
| **UI-LOG-09C** | `preserved` | UI-LOG-09C | Same method/path and source-backed request occurrence. |
| **UI-LOG-10A** | `preserved` | UI-LOG-10A | Same method/path and source-backed request occurrence. |
| **UI-LOG-10B** | `preserved` | UI-LOG-10B | Same method/path and source-backed request occurrence. |
| **UI-LOG-10C** | `preserved` | UI-LOG-10C | Same method/path and source-backed request occurrence. |
| **UI-LOG-11A** | `preserved` | UI-LOG-11A | Same method/path and source-backed request occurrence. |
| **UI-LOG-11B** | `preserved` | UI-LOG-11B | Same method/path and source-backed request occurrence. |
| **UI-LOG-12A** | `preserved` | UI-LOG-12A | Same method/path and source-backed request occurrence. |
| **UI-LOG-12B** | `preserved` | UI-LOG-12B | Same method/path and source-backed request occurrence. |
| **UI-LOG-17A** | `preserved` | UI-LOG-17A | Same method/path and source-backed request occurrence. |
| **UI-LOG-17B** | `preserved` | UI-LOG-17B | Same method/path and source-backed request occurrence. |
| **UI-LOG-19** | `preserved` | UI-LOG-19 | Same method/path and source-backed request occurrence. |
| **UI-LOG-20** | `preserved` | UI-LOG-20 | Same method/path and source-backed request occurrence. |
| **UI-LOG-21** | `preserved` | UI-LOG-21 | Same method/path and source-backed request occurrence. |
| **UI-LOG-22** | `preserved` | UI-LOG-22 | Same source-backed UI action. |
| **UI-LOG-23** | `preserved` | UI-LOG-23 | Same source-backed UI action. |
| **UI-PLUG-08B** | `preserved_alias` | UI-PLUG-08 | Same method/path and source-backed request occurrence. |
| **UI-PLUG-09** | `preserved` | UI-PLUG-09 | Same method/path and source-backed request occurrence. |
| **UI-PLUG-10** | `preserved` | UI-PLUG-10 | Same source-backed UI action. |
| **UI-PLUG-11** | `preserved` | UI-PLUG-11 | Same source-backed UI action. |
| **UI-PLUG-12** | `preserved` | UI-PLUG-12 | Same source-backed UI action. |
| **UI-PLUG-13** | `preserved` | UI-PLUG-13 | Same source-backed UI action. |
| **UI-PLUG-14** | `preserved` | UI-PLUG-14 | Same source-backed UI action. |
| **UI-PLUG-15** | `preserved_alias` | UI-PLUG-14 | Same source-backed UI action. |
| **UI-PLUG-16** | `preserved` | UI-PLUG-16 | Same source-backed UI action. |
| **UI-PLUG-17** | `preserved_alias` | UI-PLUG-16 | Same source-backed UI action. |
| **UI-PLUG-18** | `preserved` | UI-PLUG-18 | Same source-backed UI action. |
| **UI-PLUG-19** | `preserved` | UI-PLUG-19 | Same source-backed UI action. |
| **UI-PLUG-20** | `preserved` | UI-PLUG-20 | Same source-backed UI action. |
| **UI-PLUG-21** | `preserved` | UI-PLUG-21 | Same source-backed UI action. |
| **UI-PLUG-22** | `preserved` | UI-PLUG-22 | Same source-backed UI action. |
| **UI-SET-03A** | `preserved` | UI-SET-03A | Same method/path and source-backed request occurrence. |
| **UI-SET-03B** | `preserved` | UI-SET-03B | Same method/path and source-backed request occurrence. |
| **UI-SET-03C** | `preserved` | UI-SET-03C | Same method/path and source-backed request occurrence. |
| **UI-SET-12** | `preserved` | UI-SET-12 | Same source-backed UI action. |
| **UI-SET-12A** | `preserved` | UI-SET-12A | Same method/path and source-backed request occurrence. |
| **UI-SET-12B** | `preserved` | UI-SET-12B | Same method/path and source-backed request occurrence. |
| **UI-SET-13** | `preserved` | UI-SET-13 | Same source-backed UI action. |
| **UI-SET-14** | `preserved` | UI-SET-14 | Same source-backed UI action. |
| **API-BACKEND-CONFIG-DIFF** | `moved_to_api_catalog` | GET /admin/config/diff | The same backend-only operation is now sourced from the API catalog. |
| **UI-AUD-07** | `preserved` | UI-AUD-07 | Same method/path and source-backed request occurrence. |
| **UI-AUD-08** | `preserved` | UI-AUD-08 | Same method/path and source-backed request occurrence. |
| **UI-AUD-09** | `preserved` | UI-AUD-09 | Same source-backed UI action. |
| **UI-AUD-10** | `preserved` | UI-AUD-10 | Same source-backed UI action. |
