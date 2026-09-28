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
- Terminology to keep: upstream, principal, proxy key, pool, 5h window, 7d window, plugin chain, filter / shape / observe slots, warm-up, keepalive.

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
