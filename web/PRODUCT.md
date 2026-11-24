# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Users

A team of several operators who run cc-lb together. They take turns watching the pool, and management work (issuing principal keys, editing upstreams, binding plugin chains) is spread across them, so the admin web must make state and recent changes legible to someone who did not make them.

## Product Purpose

cc-lb is an Anthropic-compatible multi-principal reverse proxy. It pools upstream credentials (Claude subscriptions and API keys) and routes requests from many principals across them. The admin web is where operators see whether the pool is healthy and how its quota is being used, change its configuration, and decide how to spend subscription quota well.

Success means an operator can, within seconds, answer: is anything down or about to run out, who is consuming what, and is the pool spending quota and cache efficiently.

## Operating Context

- Primary jobs, in priority order as confirmed by the team:
  1. Quota and usage monitoring: 5h / 7d / 7d_fable subscription windows, pool quota, per-upstream and per-principal consumption.
  2. Configuration management: upstreams, principals and their proxy keys, plugin uploads and plugin chains, settings.
  3. Cost optimization: use-it-or-lose-it subscription urgency, prompt-cache efficiency, keepalive.
- Incident diagnosis through logs and audit exists but is secondary to the three jobs above.
- Used on desktop and on mobile; both are first-class. Mobile use is real status checking away from the desk, not only an emergency fallback.
- Routes: overview (`/`), upstreams, principals, logs (live tail), audit, plugins, settings.
- Admin access is authenticated through configured admin auth providers (static token, OIDC, and similar) mapped to an admin identity; audit records who changed what.

## Capabilities and Constraints

- Served as an embedded SPA by the Rust admin listener; the SPA asset routes are the only unauthenticated admin routes.
- Stack: React 19, TanStack Router and Query, Tailwind CSS 4, Base UI, Recharts, Hanken Grotesk / Geist Mono.
- Terminology to keep: upstream, principal, proxy key, pool, 5h window, 7d window, plugin chain, filter / shape slots, warm-up, keepalive.

## Evidence on Hand

- Existing visual system: `DESIGN.md` in this directory.
- Prior QA and measurement evidence is maintained outside the public product record; do not fabricate customer-facing metrics.
- No customer-facing marketing content, testimonials, or public metrics exist; do not fabricate them.

## Product Principles

1. Usage over time first: how quota usage has moved and whether it will hit a limit before reset matter more than any single current value. Quota is always expressed as used, the same utilization Claude itself reports, so figures can be compared with Claude's own screens without conversion.
2. Legible to the next operator: state, ownership, and recent changes must be understandable without context from whoever made them.
3. Calm under pressure: dense but scannable; status color carries meaning, never decoration.
4. Safe configuration: destructive or pool-affecting changes are explicit, reversible where possible, and confirmed.
5. Same truth on every screen size: mobile shows the same facts in a different arrangement, not a reduced product.
6. Any fleet size: a pool may have one upstream and one principal, or dozens of each. Every list works at both ends: short lists stay quiet, long lists can be searched, filtered and sorted, and the selected item's detail never gets pushed off screen by the list.
7. Common path first, custom path on demand: an input flow (creating or connecting something) leads with the route almost every operator takes and keeps rare branches out of it. An option only some setups need (a custom upstream base URL, a non-default endpoint, an override) appears in that flow only when the operator opens it — a collapsed advanced section, an opt-in link, a separate step — and never sits beside the default path as an equal. This hides inputs, never state: a detail or settings view always shows the value in effect, defaults included (e.g. "Anthropic (default)" with a quiet Change), because the operator must always be able to see where traffic goes and what is configured.
8. One intent, one path: affordances that start the same job converge on a single flow; the product never forks one intent into near-duplicate paths that split its guidance from its action. Distinct surfaces may each offer the entry point — a header button, an empty state, the command palette — but none may become a second, shallower version of the flow. When two affordances would teach or start the same thing — e.g. a "Don't have a key?" link and a "Create an API key" action that both explain key creation — they merge into a single path that carries the whole job, guidance and action together.
9. Limited, never silent: when a credential's permissions limit a feature, the view names the mode the feature is in (e.g. reported vs. estimated) as a status, not as plain text, and a degraded mode carries one short nudge in "do X → get Y" form that leads to the full mode. A feature never quietly shows defaults, zeros or nothing in place of data it cannot get.
