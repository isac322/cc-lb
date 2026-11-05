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

For plans that report a five-hour window, the weekly pace gate suppresses five-hour pressure when usable weekly snapshots show that the current five-hour window is not needed before the weekly reset. Unusable snapshots retain the established conservative fallback. The selection trace records the formula version and bounded pressure fields for diagnosis.

A provider-confirmed `Absent` five-hour window contributes no five-hour pressure and does not trigger the unusable-five-hour fallback. Weekly-only plans route on their reported weekly windows. `Unobserved` (no observation yet) and `Stale` (an outdated actual sample) remain distinct from `Absent` and retain conservative fallback behavior; missing response headers alone do not prove that a window is absent.

## Inspecting a decision

Use request-event and routing metrics to inspect the selected tier, formula version, warning multiplier, and bounded reason fields. Do not treat a routing choice as a promise of a specific provider response or quota outcome.
