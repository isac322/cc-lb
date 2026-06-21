# BDD OoS-Manual Scenarios (M0 deliverable, plan v3)

- Date: 2026-06-18
- Basis: plan v3 §7.3 "OoS-manual handling (exact ID list gate)"
- M1 entry blocking condition: M0 fails if this table is empty or incomplete
- Invariant: `converted + OoS-manual + blocked == 321` (all milestone gates)

## OoS-Manual Criteria

OoS-manual if **all** of the following conditions are met:
1. Automated assertions cannot preserve the core value of the scenario, for example, "must look natural to a human"
2. Replacing with automated assertions distorts the scenario intent
3. Can be replaced by other auxiliary tracks, such as visual-qa, playwright, or manual operator checks

Scenarios with ambiguous automation potential, such as F8.2 "reason is displayed on the operator dashboard", are classified as **automatable**. These aren't OoS because they can be covered by JSON API response and DOM representation assertions.

## Current OoS-Manual List (2 scenarios)

| ID | Writer | Persona | v5.2 Title (Source) | Reason for Non-Automatability | Alternative Track | Verification Responsibility |
|---|---|---|---|---|---|---|
| F4.1c | W1 | Alice | Clicking a team row leads to the detailed view of that team | "Clicking a team row" requires human click and visual confirmation of UI page transition. The visual flow itself is the intent. Replacing this with JSON API response assertions distorts the scenario intent and user UX. | playwright and visual-qa tracks (frontend-fanout-qa skill) | Frontend team v6 round |
| F4.11b | W1 | Alice | The meanings of both markers are guided together as user cognitive tooltips | "User cognitive tooltip" refers to human readability of tooltips and badges. While DOM assertions can verify marker existence and compare tooltip text, the intent of cognitive help requires human inspection. | Combination of playwright DOM assertions (automated) and visual-qa inspection (manual). The automated assertion part is split into a separate scenario to remain on the automated track. | Frontend team v6 round |

## Classified as Automatable (For Reference)

The following scenarios use expressions like "human looks" or "visible on one screen" on the surface but are classified as **automatable** for reference: F1.1a (active and first key on one screen is covered by asserting two fields in the JSON response), F4.1a/b (dashboard is covered by JSON API), F4.10 (usage graph is covered by asserting response series), F2.5 (key list screen is covered by asserting no secret in response body), F5.8 (credential status on one screen is covered by JSON status field).

## M1 Gate Requirements

At the time of entering M1 from M0, this table must satisfy the following:
1. Every OoS row must have all four columns filled: `id`, `Reason for Non-Automatability`, `Alternative Track`, and `Verification Responsibility`
2. Number of OoS rows + number of automatable rows + number of blocked rows == 321 (invariant)
3. This table must have been updated within 7 days of entering M1 to prevent stale data

Violation of any of these three conditions blocks M1 entry.

## Update Policy

- If a new OoS candidate is found during M0 to M5, add it to this table immediately and specify "OoS-manual: F<x.y> reason=..." in the commit message
- When reclassified as automatable, remove the row and update the `Status` column of the corresponding row in the mapping table (`bdd-test-conversion-map.md`)
