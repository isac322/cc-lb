---
title: Quota routing
description: See how cost-first selection and weekly pacing shape upstream candidate choice.
slug: docs/concepts/quota-routing
---

The subscription preference filter chooses from eligible upstreams with a deterministic key. It considers cache and pricing inputs, quota pressure, urgency, and stable tie-breakers.

## Cost-first selection

When candidate costs are known, cc-lb retains candidates within the configured near-cost band of the cheapest candidate. Candidates without a cost are excluded while any known cost exists. When every cost is unknown, the routing decision keeps the full eligible set.

This keeps a warm, lower-cost candidate in contention without turning cost into a cache-hit guarantee.

## Weekly pace gate

Five-hour pressure is combined with weekly pressure only when the weekly snapshot shows that the current window may be needed before the weekly reset. Missing or stale snapshots fall back to the established pressure behavior. The selection trace records the formula version and bounded pressure fields for diagnosis.

## Inspecting a decision

Use request-event and routing metrics to inspect the selected tier, formula version, warning multiplier, and bounded reason fields. Do not treat a routing choice as a promise of a specific provider response or quota outcome.
