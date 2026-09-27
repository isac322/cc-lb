# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Users

A team of several operators who run cc-lb together. They take turns watching the pool, and management work (issuing principal keys, editing upstreams, binding plugin chains) is spread across them, so the admin web must make state and recent changes legible to someone who did not make them.

## Product Purpose

cc-lb is an Anthropic-compatible multi-principal reverse proxy. It pools upstream credentials (Claude subscriptions and API keys) and routes requests from many principals across them. The admin web is where operators see whether the pool is healthy and has headroom, change its configuration, and decide how to spend subscription quota well.

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
- Stack: React 19, TanStack Router and Query, Tailwind CSS 4, Base UI, Recharts, Geist / Geist Mono.
- Terminology to keep: upstream, principal, proxy key, pool, 5h window, 7d window, plugin chain, filter / shape / observe slots, warm-up, keepalive.

## Evidence on Hand

- Existing visual system: `DESIGN.md` in this directory.
- Prior QA and measurement: `docs/admin-web-qa-report-2026-09-18.md`, `docs/admin-web-production-measurement-2026-09-18.md`, `docs/frontend-state-audit.md`.
- No customer-facing marketing content, testimonials, or public metrics exist; do not fabricate them.

## Product Principles

1. Headroom first: remaining quota and imminent exhaustion are the most important facts on any screen that shows them.
2. Legible to the next operator: state, ownership, and recent changes must be understandable without context from whoever made them.
3. Calm under pressure: dense but scannable; status color carries meaning, never decoration.
4. Safe configuration: destructive or pool-affecting changes are explicit, reversible where possible, and confirmed.
5. Same truth on every screen size: mobile shows the same facts in a different arrangement, not a reduced product.
