# cc-lb Admin Dashboard — BDD Scenarios

> **Source of truth for E2E test code.** Every scenario here is a contract: real-world behavior the dashboard MUST exhibit. The implementing `agent-browser` shell scripts under `e2e/*.sh` are the executable form of this document.

---

## Table of Contents

1. [Part 1 — BDD Primer](#part-1--bdd-primer)
2. [Part 2 — Project Conventions](#part-2--project-conventions)
3. [Part 3 — Ubiquitous Language (Glossary)](#part-3--ubiquitous-language-glossary)
4. [Part 4 — Feature Scenarios](#part-4--feature-scenarios)
   - [4.1 Authentication & Global Shell](#41-authentication--global-shell)
   - [4.2 Overview (`/`)](#42-overview-)
   - [4.3 Principal Limits (`/principals`)](#43-principal-limits-principals)
   - [4.4 Realtime Log (`/log`)](#44-realtime-log-log)
   - [4.5 Principal Management — Roster (`/management?tab=principals`)](#45-principal-management--roster-managementtabprincipals)
   - [4.6 Principal Management — Credentials (`/management?tab=credentials`)](#46-principal-management--credentials-managementtabcredentials)
   - [4.7 Settings (`/settings`)](#47-settings-settings)
   - [4.8 Upstreams (`/upstreams`)](#48-upstreams-upstreams)
   - [4.9 Admin Activity (`/activity`)](#49-admin-activity-activity)
   - [4.10 Credential Status (`/credentials`)](#410-credential-status-credentials)
   - [4.11 Plugins — Wasm Registry (`/plugins?tab=registry`)](#411-plugins--wasm-registry-pluginstabregistry)
   - [4.12 Plugins — Per-Principal Chains (`/plugins?tab=chains`)](#412-plugins--per-principal-chains-pluginstabchains)
   - [4.13 404 Not Found](#413-404-not-found)
5. [Part 5 — Cross-cutting Concerns](#part-5--cross-cutting-concerns)
6. [Part 6 — Implementation Appendix](#part-6--implementation-appendix)

---

## Part 1 — BDD Primer

### 1.1 What BDD is

**Behavior-Driven Development (BDD)** is a collaborative engineering practice (Dan North, 2006) that specifies system behavior in plain language **before** code is written. A BDD scenario is a concrete example of how the system behaves under a specific condition, written in a structured natural-language grammar called **Gherkin**.

```
Given <some initial context>
When  <an event happens>
Then  <ensure some outcome>
```

A scenario is a single, self-contained behavioral contract. It is a specification, a piece of living documentation, and—when automated—an executable acceptance test, all at once.

### 1.2 BDD vs. its neighbors

| | TDD | Acceptance Tests | BDD Scenarios | User Stories |
|---|---|---|---|---|
| **Audience** | Developer | QA/Dev | Three Amigos (PM + Dev + QA) | PM |
| **Granularity** | Single unit/function | Whole feature | Single behavior | Whole feature |
| **Direction** | Inside-out | Outside-in | Outside-in | n/a |
| **Language** | Code | Code or scripts | Gherkin | Free prose |
| **Lifetime** | Throwaway-ish | Long-lived | **Living documentation** | Backlog item |

BDD is **not** TDD: TDD verifies code-level correctness; BDD verifies that the *product* exhibits the *behavior* the business asked for. BDD scenarios are the executable acceptance criteria of a user story.

### 1.3 Gherkin keywords

| Keyword | Purpose |
|---|---|
| `Feature` | High-level capability and business value. |
| `Rule` | A business rule that groups related scenarios (optional). |
| `Background` | Steps shared by every scenario in the feature. Keep ≤ 3 steps. |
| `Scenario` (or `Example`) | One concrete example of behavior. |
| `Given` | Initial context / state. |
| `When` | The action or event under test. |
| `Then` | The expected, observable outcome. |
| `And` / `But` | Chain steps without repeating keywords. |
| `Scenario Outline` + `Examples` | Data-driven scenarios. |
| `@tag` | Group scenarios for selective runs (`@smoke`, `@regression`, ...). |

### 1.4 Declarative vs. imperative — the golden rule

A good UI scenario describes **what** the user is trying to achieve, never **how** they click their way there.

```gherkin
# ❌ Imperative — couples spec to DOM, breaks on every refactor
When I click the button with id "submit-btn"
And I see the modal with class ".confirmation-overlay"
And I click the second button inside the modal
Then the URL contains "/users/42"

# ✅ Declarative — describes business behavior
When I confirm the deletion of user "ada@lab.io"
Then the user "ada@lab.io" should no longer appear in the directory
```

Step definitions (`agent-browser` shell scripts with assert helpers) translate declarative phrases into snapshot-and-ref level actions. The scenario survives UI refactors; only the step definition changes.

### 1.5 One behavior per scenario

If a `Then` block uses more than one or two `And`s, you are probably testing more than one behavior. Split it.

### 1.6 Exception paths are first-class scenarios

A feature is not specified until its exception paths are specified. For every UI affordance, write a scenario for at least:

- **Cancel** — the user changes their mind via a Cancel button.
- **Close** — the user dismisses via the `X` icon, the `Escape` key, or backdrop click.
- **Invalid input** — required field empty, format wrong, value out of range, business rule violated.
- **Empty state** — no data exists yet.
- **Loading state** — request in flight.
- **Server error** — API returned 5xx or network failed.
- **Conflict** — optimistic concurrency error (stale revision).
- **Permission/auth error** — 401/403.

A "happy path only" feature file is an incomplete feature file.

---

## Part 2 — Project Conventions

### 2.1 File layout

```
e2e/
├── BDD_SCENARIOS.md         ← this document (the contract)
├── features/                ← (future) .feature files mirroring this doc
│   ├── overview.feature
│   ├── upstreams.feature
│   └── ...
├── helpers.sh               ← shared assert_* helpers (see §6.4)
├── seed.sh                  ← curl-based seed helpers (see §6.2)
├── auth.sh                  ← agent-browser implementations of the contract
├── upstreams.sh
├── principals.sh
├── plugins.sh
├── ...                      ← one script per surface (see §6.1)
├── fixtures/                ← echo.wasm, oversize.wasm, admin-state.json, etc.
└── legacy-playwright/       ← prior Playwright specs (kept for reference, NOT run)
    ├── upstreams.spec.ts
    ├── principals.spec.ts
    ├── plugins.spec.ts
    ├── global-setup.ts
    └── global-teardown.ts
```

The execution surface is `agent-browser` (Chromium via CDP, accessibility-tree snapshots, compact `@eN` refs). Playwright is NOT the runner — see Part 6 for the harness mapping.

### 2.2 Step style

- **Subject is the operator.** Write steps from the point of view of "the operator" (the human admin using cc-lb), never from "the test" or "the page".

  ```gherkin
  # ❌ Then the test should see "Saved"
  # ✅ Then I should see a confirmation that the change was saved
  ```

- **No CSS selectors, no element IDs, no `nth-of-type` in scenarios.** All locator concerns belong in step definitions.
- **Use the ubiquitous language from §3.** Never say "API key" if the product calls it a "credential".
- **Tense.** `Given` past, `When` present, `Then` future/present.

### 2.3 Tags used in this document

| Tag | Meaning |
|---|---|
| `@smoke` | Critical path. Must pass on every PR. |
| `@regression` | Full suite. Required before release. |
| `@auth` | Touches admin-token / bearer-token flow. |
| `@crud` | Create/read/update/delete pattern. |
| `@destructive` | Mutates server state in a non-reversible way. |
| `@conflict` | Exercises optimistic concurrency. |
| `@async` | Depends on Server-Sent Events / streaming. |
| `@a11y` | Keyboard / screen-reader / focus behavior. |
| `@wip` | Under specification — not yet implemented. |

### 2.4 Background block convention

Every feature file uses this Background unless explicitly overridden:

```gherkin
Background:
  Given the cc-lb server is running with a clean data directory
  And the seed upstream "dummy" is configured
  And I am signed in to the admin console with a valid admin token
```

This corresponds to `e2e/global-setup.ts` plus the `localStorage.setItem('cc-lb-admin-token', ...)` step in each `beforeEach`.

### 2.5 Authoring rules

1. **No incidental data.** If a scenario doesn't depend on the exact value, write `valid name`, not `"alpha-2026-05-30"`.
2. **No imperative chains.** A `When` that contains five `And`s is two scenarios pretending to be one.
3. **Outcomes are observable.** A `Then` step that requires inspecting the database without a UI signal is a bug in the spec or a bug in the UI.
4. **Every modal has 4 scenarios minimum**: happy submit, cancel button, close `X`, escape key.
5. **Every destructive action has a confirmation scenario AND a "user cancels the confirmation" scenario.**
6. **Every form has at least one validation scenario per field that has a validation rule.**

---

## Part 3 — Ubiquitous Language (Glossary)

| Term | Definition |
|---|---|
| **Admin token** | Bearer token stored in `localStorage` under `cc-lb-admin-token`. Required for every admin API call. |
| **Operator** | The human using the admin dashboard. There is no concept of multiple users. |
| **Upstream** | A configured outbound provider that the proxy forwards traffic to. Kinds: `anthropic_api_key`, `anthropic_oauth`, `custom`. |
| **Principal** | A logical caller identity on the inbound side. Holds API keys, allowed-model list, quota. |
| **Key** | A plaintext bearer token issued to a principal. Shown exactly once on creation. |
| **Credential** | An upstream-side secret (API key env var, OAuth refresh token). Status: `valid`, `expiring_soon`, `expired`, `revoked`. |
| **Plugin** | A WebAssembly module uploaded to the registry. May be assigned to per-principal chains. |
| **Chain** | An ordered list of plugins per principal. Two kinds: `router`, `observability`. |
| **Draft revision** | A pending, un-applied configuration change. Identified by a monotonically increasing revision number. |
| **Apply** | Validate + atomically promote a draft revision into the running configuration. |
| **Killswitch** | Global flag that rejects all inbound traffic. |
| **Snapshot** | Anthropic rate-limit window data observed from upstream response headers, scoped per `(identity_kind, identity_value)`. |
| **Identity kind** | `account`, `credential`, or `unobserved` — how cc-lb correlates rate-limit windows. |

---

## Part 4 — Feature Scenarios

---

### 4.1 Authentication & Global Shell

```gherkin
@auth @smoke
Feature: Admin token gating
  The console is unusable without a valid admin token.
  The operator must be able to set, change, and clear the token.

  Scenario: First-time visit prompts for admin token
    Given I have never visited the admin console before
    When I navigate to the console root
    Then I should be presented with the "Authentication Required" gate
    And the gate should explain that an admin token is required
    And the rest of the console should be hidden

  Scenario: Submitting a non-empty token unlocks the console
    Given I am on the admin token gate
    When I submit a non-empty bearer token
    Then the gate should disappear
    And the Overview page should render
    And the token should be persisted in browser storage so it survives a reload

  Scenario: Submitting an empty token leaves the gate visible
    Given I am on the admin token gate
    When I submit an empty bearer token
    Then I should remain on the gate
    And the rest of the console should still be hidden

  Scenario: Reload preserves the admin token
    Given I have signed in with a valid admin token
    When I reload the browser tab
    Then I should land directly on the Overview page without re-entering the token

  Scenario: Connection status reflects an invalid token
    Given I have signed in with an invalid admin token
    When the dashboard attempts to reach the admin API
    Then the topbar connection indicator should display "AUTH REQUIRED"

  Scenario: Editing the admin token from the topbar
    Given I am signed in
    When I open the "Admin Token" modal from the topbar
    And I replace the current token with a different valid token
    And I confirm the change
    Then the page should reload
    And the new token should be used for subsequent API calls

  Scenario: Cancelling the "Edit Admin Token" modal
    Given I am signed in
    And I have opened the "Admin Token" modal
    When I dismiss the modal via the Cancel button
    Then the modal should close
    And my current admin token should be unchanged

  Scenario: Closing the "Edit Admin Token" modal with the Escape key
    Given I have opened the "Admin Token" modal
    When I press the Escape key
    Then the modal should close
    And my current admin token should be unchanged

  Scenario: Clearing the admin token logs me out
    Given I am signed in
    And I have opened the "Admin Token" modal
    When I clear the token field and confirm
    Then the page should reload
    And I should be returned to the "Authentication Required" gate
```

```gherkin
@auth @async
Feature: Live connection indicator
  The topbar must reflect the real-time health of the admin connection.

  Scenario Outline: Indicator reflects connection state
    Given I am signed in
    When the admin API connection enters the "<state>" state
    Then the topbar indicator should read "<label>"
    And the indicator dot should be "<color>"

    Examples:
      | state         | label          | color |
      | live          | LIVE           | green |
      | reconnecting  | RECONNECTING   | amber |
      | unauthorized  | AUTH REQUIRED  | red   |
      | disconnected  | DISCONNECTED   | gray  |
```

```gherkin
@smoke
Feature: Sidebar navigation
  Every primary route must be reachable from the sidebar.

  Background:
    Given I am signed in

  Scenario Outline: Navigating to "<page>" via the sidebar
    When I click the sidebar link "<label>"
    Then the browser URL path should be "<path>"
    And the page heading should read "<heading>"
    And the "<label>" link should be visually marked as active

    Examples:
      | label       | path         | heading              |
      | Overview    | /            | Overview             |
      | Limits      | /principals  | Principal Limits     |
      | Live Log    | /log         | Realtime Log         |
      | Management  | /management  | Principal Management |
      | Settings    | /settings    | Settings             |
      | Upstreams   | /upstreams   | Upstreams            |
      | Audit       | /activity    | Admin Activity       |
      | Credentials | /credentials | Credential Status    |
      | Plugins     | /plugins     | Plugin Status        |
```

---

### 4.2 Overview (`/`)

```gherkin
@smoke
Feature: Overview KPI cards
  The Overview must summarize the proxy's recent traffic at a glance.

  Background:
    Given I am signed in
    And I am on the Overview page

  Scenario: Skeletons render while the summary is loading
    Given the summary endpoint is intentionally slow
    When the Overview page mounts
    Then four pulsing skeleton KPI cards should be visible
    And no numeric values should be shown yet

  Scenario: Cards render aggregate values when traffic has been observed
    Given the proxy has observed traffic in the selected range
    When the summary loads
    Then I should see a "Requests" card with a numeric count and a sparkline
    And I should see a "Tokens" card with a numeric count and a sparkline
    And I should see a "Virtual Cost" card with a USD value and a sparkline
    And I should see an "Error Rate" card with a percentage and a sparkline

  Scenario: Cards show "no traffic" placeholders when nothing has been observed
    Given the proxy has observed no traffic in the selected range
    When the summary loads
    Then each KPI card should display a dash placeholder
    And each KPI card should display the message "No traffic in last <range>" for the selected range

  Scenario: Summary load failure surfaces a retry affordance
    Given the summary endpoint will return a server error
    When the Overview page mounts
    Then I should see an inline error indicating that the summary failed to load
    And I should be able to retry the request from that error block

  Scenario Outline: Switching the time range refetches the summary
    When I select the "<range>" range
    Then the URL should reflect the "range=<range>" query parameter
    And every KPI card, chart, and principal usage card should refetch for the new range

    Examples:
      | range |
      | 15m   |
      | 1h    |
      | 6h    |
      | 24h   |
      | 7d    |
```

```gherkin
Feature: Overview resource error banner
  When upstreams or principals are in an error state, the Overview must surface this above the fold.

  Background:
    Given I am signed in

  Scenario: Banner appears when resources are in error
    Given 2 upstreams and 1 principal are reporting an error state
    When I land on the Overview page
    Then I should see a banner reading "3 resources currently in error state (2 upstreams, 1 principals)"
    And the banner should link directly to the Upstreams page
    And the banner should link directly to the Principal Limits page

  Scenario: Banner is hidden when no resources are in error
    Given no upstream and no principal is reporting an error state
    When I land on the Overview page
    Then the resource error banner should not be visible
```

```gherkin
Feature: Overview "Requests by model" chart

  Background:
    Given I am signed in
    And I am on the Overview page

  Scenario: Chart renders a stacked area when series data is available
    Given the time-series endpoint returns at least one model series
    When the chart loads
    Then I should see a stacked area chart with one stack per model
    And the chart should be associated with the currently selected time range

  Scenario: Chart shows an empty state when no series exist
    Given the time-series endpoint returns an empty result
    When the chart loads
    Then I should see a "Waiting for first request" message
    And the empty state should not display a chart canvas

  Scenario: Chart shows an error state with retry on failure
    Given the time-series endpoint will return a server error
    When the chart loads
    Then I should see an inline error explaining that chart data failed to load
    And I should be able to retry the request
```

```gherkin
Feature: Overview principal usage strip

  Background:
    Given I am signed in
    And I am on the Overview page

  Scenario: One card per active principal
    Given principals "alpha" and "beta" have produced traffic in the selected range
    When the usage strip loads
    Then I should see a horizontally scrollable card for "alpha"
    And I should see a horizontally scrollable card for "beta"
    And each card should display requests, tokens, error rate, and cost

  Scenario: Strip is hidden when no principal has produced traffic
    Given no principal has produced traffic in the selected range
    When the usage strip loads
    Then the usage strip should not be visible

  Scenario: Error rate above 5 % is highlighted
    Given principal "alpha" has an error rate of 8 % in the selected range
    When the usage strip loads
    Then the "alpha" card should highlight its error rate in red
```

```gherkin
@async
Feature: Overview embedded live log

  Background:
    Given I am signed in
    And I am on the Overview page

  Scenario: Stream indicator shows "live" when SSE is connected
    Given the live event stream is healthy
    When I look at the embedded live log
    Then the stream indicator should read "live" with a green pulse

  Scenario: Stream indicator shows "reconnecting" on transient disconnect
    Given the live event stream has been interrupted
    When the dashboard attempts to reconnect
    Then the stream indicator should read "reconnecting" or "connecting" with an amber pulse

  Scenario: Empty state until first event
    Given no event has arrived since the page mounted
    Then the embedded live log should display "Waiting for first request..."

  Scenario: "View full log" link routes to the Realtime Log page
    When I click "View full log" in the embedded live log
    Then I should land on the Realtime Log page
```

---

### 4.3 Principal Limits (`/principals`)

```gherkin
@smoke
Feature: Principal directory navigation
  The operator selects a principal from the left-hand directory to see that principal's limits.

  Background:
    Given I am signed in
    And the system has at least one configured principal
    And I am on the Principal Limits page

  Scenario: First principal is selected by default
    When the directory finishes loading
    Then the first principal in the directory should be highlighted as active
    And the page should load that principal's usage and limits

  Scenario: Selecting a different principal updates the URL and the panel
    Given principals "alpha" and "beta" exist
    When I click the directory entry "beta"
    Then the URL should reflect "principal=beta"
    And the "beta" entry should become the highlighted active principal
    And the usage and limits panels should refetch for "beta"

  Scenario: Directory empty state
    Given no principal is configured
    When the directory finishes loading
    Then the directory should display "No principals found."

  Scenario: Directory failure surfaces an error
    Given the principals endpoint will return a server error
    When the directory tries to load
    Then the directory should display "Failed to load principals" with the error detail
```

```gherkin
Feature: Selected principal's usage summary

  Background:
    Given I am signed in
    And I am viewing principal "alpha" on the Principal Limits page

  Scenario Outline: Range selector refetches the usage summary
    When I select the "<range>" range
    Then the URL should reflect "range=<range>"
    And the requests, tokens, error rate, and cost cards should refetch for "<range>"

    Examples:
      | range |
      | 15m   |
      | 1h    |
      | 6h    |
      | 24h   |
      | 7d    |

  Scenario: Cards show zeroes when "alpha" has produced no traffic
    Given "alpha" has produced no traffic in the selected range
    Then each usage card should display "0" or "0%" without a sparkline

  Scenario: Usage load failure surfaces an error
    Given the principal usage endpoint will return a server error
    When the usage summary loads
    Then I should see "Failed to load usage" with the error detail
```

```gherkin
Feature: Rate-limit snapshots panel
  Anthropic rate-limit headers are surfaced as account- or credential-scoped snapshots.

  Background:
    Given I am signed in
    And I am viewing principal "alpha" on the Principal Limits page

  Scenario: Empty state when no snapshots have been observed
    Given no rate-limit snapshot has been observed for "alpha"
    Then the rate-limit panel should display "No rate-limit snapshots observed"
    And the panel should explain that Anthropic limit headers populate this view as traffic flows

  Scenario: Account-observed grouping renders donut charts per window
    Given "alpha" has snapshots for identity_kind "account" with windows "5h" and "weekly"
    Then I should see one grouping card with an "account" badge
    And inside that grouping I should see a "5h" window and a "weekly" window
    And each window should render donut charts for requests, input_tokens, output_tokens, and tokens

  Scenario: "Unobserved" grouping shows a warning banner
    Given "alpha" has snapshots only for identity_kind "unobserved"
    Then I should see a grouping with an "unobserved" badge
    And the grouping should display a warning explaining that cc-lb is using the credential reference as a proxy identity because upstream did not return account-scoped rate-limit headers

  Scenario: Exhausted window is highlighted in red
    Given "alpha" has a window where the remaining quota is zero
    Then the corresponding donut chart should be highlighted in red
    And the donut should display the reset time
```

---

### 4.4 Realtime Log (`/log`)

```gherkin
@async @smoke
Feature: Realtime log streaming
  The Realtime Log streams live request events over SSE and lets the operator filter, pause, and inspect them.

  Background:
    Given I am signed in
    And I am on the Realtime Log page

  Scenario: Empty state before the first event arrives
    Given no event has been emitted and no recent event exists
    Then the log table should display "Waiting for traffic..."

  Scenario: Recent events are backfilled before live events arrive
    Given recent request events already exist
    When I open the Realtime Log page
    Then the table should initially be seeded with the recent events
    And new SSE events should prepend to that table

  Scenario: New events append to the table while the stream is live
    Given the stream connection status is "live"
    When the proxy emits a new request event
    Then a new row representing that event should appear in the table

  Scenario: Stream disconnection raises an error block above the table
    Given the live event stream encounters a network error
    Then an error block should appear above the log table card
    And the error block should display the underlying error message
    And the existing rows in the table should remain visible

  Scenario: Live log retains only the newest 2000 events
    Given more than 2000 matching events arrive while the log is open
    Then the table should retain the newest 2000 events
    And older events should be dropped from the client-side table
```

```gherkin
Feature: Log filter bar

  Background:
    Given I am signed in
    And I am on the Realtime Log page

  Scenario: Filtering by principal narrows the stream
    When I enter principal id "alpha" in the principal filter
    Then only events whose principal id matches "alpha" should remain in the table
    And the URL should reflect the active principal filter

  Scenario: Filtering by model narrows the stream
    When I enter model "claude-3-5-haiku" in the model filter
    Then only events whose model matches "claude-3-5-haiku" should remain in the table
    And the URL should reflect the active model filter

  Scenario Outline: Filtering by upstream kind
    When I select the upstream filter "<kind>"
    Then only events whose upstream kind is "<kind>" should remain in the table

    Examples:
      | kind                  |
      | Anthropic Direct      |
      | Bedrock Runtime       |
      | Bedrock Mantle        |
      | Vertex                |
      | Custom Anthropic Spec |

  Scenario Outline: Filtering by status class
    When I select the status filter "<status>"
    Then only events whose HTTP status falls into "<status>" should remain in the table

    Examples:
      | status |
      | 2xx    |
      | 3xx    |
      | 4xx    |
      | 5xx    |

  Scenario: Reverting a filter restores the unfiltered stream
    Given I have applied a principal filter
    When I clear that filter
    Then events matching any principal should be eligible to appear again
```

```gherkin
Feature: Pause and resume

  Background:
    Given I am signed in
    And I am on the Realtime Log page

  Scenario: Pausing buffers incoming events without showing them
    Given the stream is live
    When I press Pause
    And the proxy emits 12 new events
    Then the paused-buffer indicator should report 12 buffered events
    And the table should not yet display those events

  Scenario: Flushing while paused appends buffered events without resuming
    Given I have paused and the buffer contains 12 events
    When I press Flush
    Then those 12 events should appear in the table
    And the stream should still be paused

  Scenario: Resuming clears the buffer and re-attaches the live tail
    Given I have paused and the buffer is non-empty
    When I press Resume
    Then any buffered events should be appended
    And new events should arrive in real time again

  Scenario: Paused mode survives URL navigation
    Given I open the Realtime Log page with the URL parameter "paused=1"
    Then the stream should start in the paused state
    And incoming events should be buffered until I press Resume or Flush
```

```gherkin
Feature: Clearing the table

  Background:
    Given I am signed in
    And I am on the Realtime Log page

  Scenario: Clear removes already-displayed events
    Given the table already contains some events
    When I press Clear
    Then the table should become empty
    And the empty state should be visible again
    And new incoming events should still append normally
```

```gherkin
Feature: Row detail panel

  Background:
    Given I am signed in
    And I am on the Realtime Log page

  Scenario: Clicking a row opens its detail panel
    Given the table contains at least one event
    When I click a row
    Then a detail panel should open at the bottom of the page
    And the panel should show request id, timestamp, principal, model, upstream, status, duration, and token counts

  Scenario: Closing the detail panel
    Given the detail panel is open
    When I press the Close button on the panel
    Then the panel should be dismissed
    And the table should regain its full height
```

---

### 4.5 Principal Management — Roster (`/management?tab=principals`)

```gherkin
@smoke
Feature: Principal roster view

  Background:
    Given I am signed in
    And I am on the Principal Management page

  Scenario: Roster lists configured principals
    Given principal "admin" is configured
    Then the roster table should contain a row for "admin"
    And the row should display ID, status, quota, allowed-models count, and an actions column

  Scenario: Roster loading state
    Given the principals endpoint is intentionally slow
    Then the roster area should display "Loading principals..."

  Scenario: Roster empty state
    Given no principal is configured
    Then the roster table should display the generic "No data available" empty state

  Scenario: Roster failure surfaces an inline error
    Given the principals endpoint will return a server error
    Then the page should display "Error:" followed by the failure detail

  Scenario: An "Error" status badge reveals the apply-error detail
    Given principal "alpha" is in the "error" status because its last apply attempt failed
    When I click the "Error" status badge on the row for "alpha"
    Then a popover should reveal the apply error message
    And the popover should label the failure time as "unknown time" because the roster does not pass a lastApplyAt timestamp
```

```gherkin
@crud
Feature: Creating a new principal
  Creating a principal stages it in the current draft revision; it is not visible to the proxy until the draft is validated and applied.

  Background:
    Given I am signed in
    And I am on the Principal Management page

  Scenario: Happy path — create a principal and apply the draft
    When I open the "+ New principal" form
    And I supply a valid principal id
    And I add at least one allowed model
    And I save the principal to the draft
    And I go to the Settings page
    And I validate the draft
    And I apply the draft
    Then the roster should list the newly created principal
    And the draft revision banner should disappear

  Scenario: Cancelling creation via the Cancel button discards the form
    Given I have opened the "+ New principal" form
    And I have entered a partial principal id
    When I press Cancel
    Then the form should close
    And no new principal should appear in the roster
    And the draft revision should be unchanged

  Scenario: Closing the form via the Escape key
    Given I have opened the "+ New principal" form
    When I press the Escape key
    Then the form should close
    And no new principal should appear in the roster

  Scenario: Missing principal id disables the Save button
    Given I have opened the "+ New principal" form
    And the principal id field is empty
    Then the "Save to Draft" button should be disabled
    And no save request should be issued

  Scenario: Allowed-models editor refuses to add an empty pattern
    Given I have opened the "+ New principal" form
    And the allowed-model input is empty
    Then the "Add" button should be disabled

  Scenario: Adding then removing a model from the allowed list
    Given I have opened the "+ New principal" form
    When I add the model "claude-3-*"
    And I remove the model "claude-3-*" from the list
    Then the allowed-models list should be empty

  @a11y
  Scenario: Adding an allowed model via the Enter key
    Given I have opened the "+ New principal" form
    And the allowed-model input contains "claude-3-5-sonnet"
    When I press the Enter key while focus is on the allowed-model input
    Then "claude-3-5-sonnet" should be added to the allowed-models list
    And the allowed-model input should be cleared
    And the "Add" button should be re-disabled because the input is empty

  Scenario: Server failure during save shows an inline error
    Given the principals endpoint will return a server error
    And I have opened the "+ New principal" form with a valid principal id
    When I save to draft
    Then I should see an inline error at the bottom of the form
    And the form should remain open with my data intact
```

```gherkin
@crud
Feature: Editing a principal

  Background:
    Given I am signed in
    And principal "alpha" exists with allowed models ["claude-3-*"]
    And I am on the Principal Management page

  Scenario: Adding a model and applying the draft
    When I open the edit form for principal "alpha"
    And I add allowed model "gpt-4"
    And I save the principal to the draft
    And I validate and apply the draft from the Settings page
    Then the roster row for "alpha" should display "2 models"

  Scenario: The principal id field is read-only when editing
    When I open the edit form for principal "alpha"
    Then the principal id field should be disabled

  Scenario: Cancelling the edit form discards changes
    When I open the edit form for principal "alpha"
    And I add allowed model "gpt-4" but press Cancel
    Then the roster row for "alpha" should still report its original allowed-models count

  Scenario: Applying a live quota override succeeds without a draft
    When I open the edit form for principal "alpha"
    And I set a live override of 500 requests in the quota override section
    And I press "Apply Override"
    Then I should see confirmation that the live override was applied
    And the change should NOT add anything to the current draft revision

  Scenario: Applying an empty live quota override still submits
    When I open the edit form for principal "alpha"
    And I leave every override field empty
    And I press "Apply Override"
    Then an override request should be submitted with no override fields set
    And the form should report success or failure based on the server response
```

```gherkin
@destructive
Feature: Enabling and disabling a principal

  Background:
    Given I am signed in
    And principal "alpha" exists and is currently enabled

  Scenario: Disabling stages a draft change
    When I press Disable on the row for "alpha"
    Then the row should reflect a pending disabled status in the draft
    And the draft revision banner should announce the staged change

  Scenario: Re-enabling reverses the staged change
    Given I have previously staged "alpha" to be disabled in the current draft
    When I press Enable on the row for "alpha"
    Then the row should return to an enabled status in the draft
```

```gherkin
@auth @destructive
Feature: Issuing and revoking principal keys
  Keys are plaintext bearer tokens. Each key is shown exactly once.

  Background:
    Given I am signed in
    And principal "alpha" exists
    And I am on the Principal Management page

  Scenario: Opening the Keys panel
    When I press Keys on the row for "alpha"
    Then a modal listing existing keys for "alpha" should appear

  Scenario: Keys panel shows empty state when no keys exist
    Given principal "alpha" has no keys
    When I open the Keys panel for "alpha"
    Then the keys table should display the generic "No data available" empty state

  Scenario: Keys panel surfaces a load failure
    Given the keys endpoint will return a server error
    When I open the Keys panel for "alpha"
    Then the panel should display an inline error containing the failure detail

  Scenario: Issuing a new key
    Given the Keys panel for "alpha" is open
    When I press "Issue New Key"
    And I optionally enter a label
    And I confirm issuance
    Then a one-time plaintext key should be displayed with a warning that it will not be shown again
    And I should be offered a copy-to-clipboard affordance
    And after I press Dismiss the plaintext key should disappear
    And the keys list should now include an entry for the newly issued key

  Scenario: Cancelling the "Issue New Key" form before confirmation
    Given the "Issue New Key" form is open
    When I press Cancel
    Then the form should close
    And no new key should be added to the principal

  Scenario: Revoking a key requires confirmation
    Given principal "alpha" has at least one active key
    When I press Revoke on that key's row
    Then a confirmation modal should be displayed
    And the key should remain active until I confirm

  Scenario: Confirming the revocation
    Given the revoke confirmation modal is open for a key
    When I confirm revocation
    Then the confirmation modal should close
    And the key's row should mark the key as revoked

  Scenario: Cancelling the revocation
    Given the revoke confirmation modal is open for a key
    When I press Cancel
    Then the modal should close
    And the key should remain active

  Scenario: Closing the Keys panel
    Given the Keys panel is open
    When I close the panel
    Then the modal should disappear
    And focus should return to the principal row
```

```gherkin
Feature: Draft revision banner
  Whenever the operator has unapplied changes, a persistent banner offers a path to review and apply.

  Background:
    Given I am signed in

  Scenario: Banner is shown when an unapplied draft exists
    Given a draft revision is pending application
    When I view the Principal Management page
    Then a banner should be pinned at the bottom of the page describing the pending draft revision number
    And the banner should offer a link to "Review & apply"

  Scenario: Following the link routes to Settings
    Given the draft revision banner is visible
    When I click "Review & apply"
    Then I should land on the Settings page
```

---

### 4.6 Principal Management — Credentials (`/management?tab=credentials`)

```gherkin
Feature: Active credentials list

  Background:
    Given I am signed in
    And I am on the Principal Management page on the Credentials tab

  Scenario: Loading state
    Given the credentials endpoint is intentionally slow
    Then the credentials area should display "Loading credentials..."

  Scenario: Active credentials load failure surfaces an inline error
    Given the credentials endpoint will return a server error
    Then the credentials area should display "Error:" followed by the failure detail

  Scenario: Empty state when no credential exists
    Given no credential is configured
    Then the credentials table should display the generic "No data available" empty state

  Scenario: Credentials table renders one row per credential
    Given two credentials exist
    Then the table should contain two rows
    And each row should show provider, kind, identity, associated principals, status, expiry, and an actions column
```

```gherkin
@destructive
Feature: Rotating a credential

  Background:
    Given I am signed in
    And a rotatable credential exists for principal "alpha"

  Scenario: Happy path — rotating an API-key credential
    When I press Rotate on the credential row
    And I confirm rotation in the dialog
    Then a one-time plaintext credential secret should be displayed with a warning that it will not be shown again
    And the old credential should be marked revoked
    And after I dismiss, the credentials list should reflect the new credential

  Scenario: Rotating an OAuth credential is not supported in-place
    Given the credential's kind is "anthropic_oauth"
    When I press Rotate on the credential row
    And I confirm rotation in the dialog
    Then I should be informed that OAuth rotation requires re-running the OAuth flow
    And I should be offered a link to the Credentials page

  Scenario: Cancelling rotation
    Given the rotate confirmation modal is open
    When I press Cancel
    Then the modal should close
    And the credential should be unchanged
```

```gherkin
@destructive
Feature: Revoking a credential

  Background:
    Given I am signed in
    And an active credential exists

  Scenario: Confirming revocation marks the credential revoked
    When I press Revoke on the credential row
    And I confirm in the dialog
    Then the credentials list should mark that credential as revoked
    And the action should be irreversible

  Scenario: Cancelling revocation preserves the credential
    When I press Revoke on the credential row
    And I press Cancel in the confirmation dialog
    Then the dialog should close
    And the credential should remain active

  Scenario: Closing the confirmation dialog with the Escape key
    When I press Revoke on the credential row
    And I press the Escape key
    Then the dialog should close
    And the credential should remain active
```

---

### 4.7 Settings (`/settings`)

```gherkin
@smoke
Feature: Schema-driven settings form
  The settings form is generated dynamically from the active config schema. Edits are autosaved into the current draft revision.

  Background:
    Given I am signed in
    And I am on the Settings page

  Scenario: Sections are generated from the schema
    Then a section nav should list every group exposed by the active schema coverage checklist
    And the rendered form should omit "upstreams", "principals", and "plugins" sections in favor of their dedicated pages
    And clicking a section nav entry should scroll its corresponding section into view

  Scenario: Editing a field autosaves to the current draft after a short debounce
    When I change a string field to a new value
    Then within roughly half a second the save status indicator should read "Saving..." and then "Saved"
    And the draft revision number should be incremented

  Scenario: Number field signals out-of-range values via browser constraint validation
    Given a numeric field exposes a minimum of 1 and a maximum of 100
    When I enter the value 1000 into that field
    Then the browser should mark the field as invalid through its native constraint validation
    And the autosave will still persist the entered value into the draft until I correct it

  Scenario: Masked secret field hides its value behind a "redacted" badge
    Given a sensitive field has a redacted value on the server
    Then the field should display a "redacted" badge instead of the value
    And I should be offered a "Replace" affordance

  Scenario: Replacing a masked secret reveals an editable password input
    When I press "Replace" on a redacted field
    Then a password input should appear with a show/hide toggle
    And entering a new value should stage it in the draft

  Scenario: Toggling password visibility on a replaced secret
    Given I have pressed "Replace" on a redacted field so its password input is visible
    Then the secret input should hide its value as a password by default
    When I press the show/hide toggle on the field
    Then the secret input should display its value in plain text
    When I press the show/hide toggle again
    Then the secret input should hide its value as a password again

  Scenario: Boolean field toggles between true and false
    When I toggle a boolean field
    Then the new boolean value should be staged in the draft

  Scenario: Settings footer shows replica generation and recent replicas
    Then a footer at the bottom of the Settings page should report the current replica generation
    And the footer should list the recent replica IDs, or "none" when no other replica has been seen

  Scenario Outline: Settings cannot load required data
    Given the "<resource>" endpoint will return a server error
    When I open the Settings page
    Then the page should display an error state describing the "<resource>" failure

    Examples:
      | resource |
      | schema   |
      | draft    |

  Scenario: Editing a field issues only one save request per typing burst
    Given the autosave debounce is approximately 400 ms
    When I type a multi-character value into a string field over the span of one second
    Then the dashboard should issue exactly one PUT request containing the final typed value
    And the save status indicator should ultimately read "Saved"

  Scenario: Editing after a successful validation re-locks Apply
    Given I have validated the current draft and the chip reads "Validated Rev N"
    When I edit any field
    Then the "Validated Rev N" chip should disappear
    And the Apply button should become disabled again
```

```gherkin
@conflict
Feature: Draft autosave handles concurrent edits
  When another operator has already advanced the draft revision, my next autosave attempt must reconcile before it overwrites their work.

  Background:
    Given I am signed in
    And I am editing the Settings page

  Scenario: Stale draft revision triggers a transparent merge-and-retry
    Given another operator has advanced the draft to a newer revision since I started editing
    When my autosave fires
    Then the dashboard should refetch the latest draft
    And it should layer my top-level field edits onto that latest draft
    And it should retry the save with the new expected revision
    And the save status should ultimately read "Saved" without losing my edits

  Scenario: Unrecoverable conflict surfaces "Refresh to continue"
    Given the conflict cannot be reconciled by the auto-merge
    When my autosave fails
    Then the save status should read "Conflict"
    And the Apply button should be disabled
    And a "Refresh to continue" affordance should drop my local edits and re-fetch the latest draft when invoked
```

```gherkin
Feature: Validate & apply
  Configuration is applied atomically. Validation must succeed before Apply is allowed.

  Background:
    Given I am signed in
    And the draft revision contains pending changes

  Scenario: Validate succeeds and unlocks Apply
    When I press Validate
    Then the validation status should read "Validated Rev N" for the current draft
    And the Apply button should become enabled

  Scenario: Validate fails and reports the offending issue
    Given the current draft would be rejected by validation
    When I press Validate
    Then a validation error banner should appear with a human-readable message
    And the Apply button should remain disabled

  Scenario: Apply requires explicit confirmation
    Given the current draft has been validated
    When I press Apply
    Then a confirmation modal should appear naming the draft revision number

  Scenario: Confirming the apply atomically promotes the draft
    Given the Apply confirmation modal is open
    When I press "Confirm Apply"
    Then the server should reload with the new configuration
    And the draft revision banner should disappear
    And the draft and status panels should refresh to reflect the newly applied revision

  Scenario: Cancelling the apply confirmation leaves the draft intact
    Given the Apply confirmation modal is open
    When I press Cancel
    Then the modal should close
    And the draft revision should still be pending application

  @wip
  Scenario: Conflict detected during apply (NOT YET SURFACED IN UI)
    Given another operator has applied a competing draft between my Validate and my Apply
    When I press Apply
    Then the request should fail with a conflict on the server
    But the current Settings page silently swallows the apply error in its catch handler
    And no visible "Conflict" indicator is shown to the operator yet
```

```gherkin
Feature: Configuration history drawer

  Background:
    Given I am signed in
    And I am on the Settings page

  Scenario: Drawer lists recently applied revisions
    Given at least three revisions have been applied historically
    Then the History drawer should list those revisions with relative-time labels and a short summary

  Scenario: History drawer loading state
    Given the config history endpoint is intentionally slow
    Then the History drawer should display "Loading history..."

  Scenario: History drawer load failure
    Given the config history endpoint will return a server error
    Then the History drawer should display "Failed to load history" with the error detail

  Scenario: History list is fetched on drawer mount only
    Given the History drawer has already loaded a list of revisions
    When another operator applies a new revision in the background
    Then the History drawer should NOT automatically reflect that new revision
    And re-mounting the drawer is required to see it

  Scenario: Opening a revision opens a diff modal
    When I click a revision entry in the History drawer
    Then a modal should display the configuration diff between that revision and the current draft
    And the diff should be presented as a table of paths from-to-value

  Scenario: Diff modal reports no changes between two equal revisions
    Given the selected revision has no differences from the current draft
    When I open its diff modal
    Then the modal should display "No changes between revision …" wording

  Scenario: Diff modal surfaces a diff load failure
    Given the diff endpoint will return a server error
    When I open a revision's diff modal
    Then the modal should display "Failed to load diff" with the error detail

  Scenario: Closing the diff modal
    Given the diff modal is open
    When I press Close
    Then the modal should dismiss
    And I should return to the Settings page

  Scenario: Diff modal warns when the change set is truncated
    Given a revision contains more changes than the diff modal can render in full
    When I open its diff modal from the History drawer
    Then a warning banner should explain that some changes were truncated
    And the truncated set of changes should still be displayed in the diff table
```

```gherkin
Feature: Restart-required banner
  Some changes require a process restart to take effect.

  Background:
    Given I am signed in

  Scenario: Banner appears when at least one field is marked restart-required
    Given the active draft includes at least one restart-required change
    When I land on the Settings page
    Then a "Restart required" banner should be visible at the top of the page
    And expanding it should list affected fields with current value, new value, and reason

  Scenario: Banner is absent when no restart-required change exists
    Given the draft contains no restart-required change
    Then no restart-required banner should be visible on the Settings page
```

---

### 4.8 Upstreams (`/upstreams`)

```gherkin
@smoke
Feature: Upstreams listing

  Background:
    Given I am signed in
    And I am on the Upstreams page

  Scenario: Loading state
    Given the upstreams endpoint is intentionally slow
    Then the page should display a loading indicator

  Scenario: Empty state
    Given no upstream is configured
    Then I should see "No upstreams configured" with guidance to create one

  Scenario: Table renders one row per upstream
    Given upstream "dummy" of kind "custom" is configured
    Then the table should contain a row for "dummy"
    And the row should display its name, kind, status, revision, and action buttons
    And the "OAuth Expires" column should display a "-" placeholder
    And the "Created" column should display a "-" placeholder

  Scenario: Refresh re-fetches the upstreams list
    When I press Refresh
    Then the upstreams endpoint should be re-queried
    And any newly created upstream from a concurrent operator should now be visible

  Scenario: Listing failure surfaces an error
    Given the upstreams endpoint will return a server error
    Then I should see "Failed to load upstreams" with the error detail

  Scenario: An "Error" status badge reveals the apply-error detail
    Given upstream "dummy" is in the "error" status because its last apply attempt failed
    When I click the "Error" status badge on the row for "dummy"
    Then a popover should reveal the apply error message
    And the popover should display the timestamp of the last failed apply
```

```gherkin
Feature: Upstreams page reports mutation failures via a browser alert
  Mutations on the Upstreams page surface failure via window.alert(), not via an inline toast.

  Background:
    Given I am signed in
    And upstream "dummy" exists

  Scenario Outline: A failed "<action>" surfaces a browser alert
    Given the upstreams endpoint will return a server error when I attempt the "<action>" action
    When I trigger "<action>" on the row for "dummy"
    Then a browser alert should be raised containing the error message
    And after I dismiss the alert the upstream should remain in its previous state

    Examples:
      | action  |
      | Enable  |
      | Disable |
      | Edit    |
      | Delete  |
```

```gherkin
@crud @smoke
Feature: Creating an API-key upstream

  Background:
    Given I am signed in
    And I am on the Upstreams page

  Scenario: Happy path — create an Anthropic API-key upstream
    When I open the "+ New Upstream" dialog
    And I enter a unique name
    And I choose "Anthropic API Key" as the kind
    And I provide an API Key Env Var
    And I press Create
    Then the dialog should close
    And the new upstream should appear in the table with kind "anthropic_api_key"

  Scenario: Custom upstream creation uses the explicit base URL
    When I open the "+ New Upstream" dialog
    And I enter a unique name
    And I choose "Custom" as the kind
    And I provide a base URL
    And I press Create
    Then the new upstream should appear in the table with kind "custom"
    And subsequent traffic for that upstream should target the provided base URL

  Scenario: Name is required
    When I open the "+ New Upstream" dialog
    And I leave the name field blank
    And I press Create
    Then the dialog should remain open
    And I should be informed that a name is required

  Scenario: API Key Env Var is required when kind is "Anthropic API Key"
    When I open the "+ New Upstream" dialog
    And I choose "Anthropic API Key" as the kind
    And I leave the API Key Env Var blank
    And I press Create
    Then the dialog should remain open
    And I should be informed that an env var is required

  Scenario: Cancelling the create dialog discards the form
    Given the "+ New Upstream" dialog is open with partially entered data
    When I press Cancel
    Then the dialog should close
    And no upstream should be created

  Scenario: Closing the create dialog with the Escape key
    Given the "+ New Upstream" dialog is open
    When I press the Escape key
    Then the dialog should close
    And no upstream should be created

  Scenario: Closing the create dialog by clicking the backdrop
    Given the "+ New Upstream" dialog is open
    When I click outside the dialog body
    Then the dialog should close
    And no upstream should be created

  Scenario: Server failure on create shows an inline error
    Given the upstreams endpoint will return a server error
    And the "+ New Upstream" dialog is open with valid data
    When I press Create
    Then an inline error should appear at the bottom of the dialog
    And the dialog should remain open with my input intact
```

```gherkin
@crud
Feature: Creating an OAuth upstream and completing the PKCE flow

  Background:
    Given I am signed in
    And the OAuth provider is reachable
    And I am on the Upstreams page

  Scenario: Happy path — OAuth upstream is created up front, then OAuth is completed
    When I open the "+ New Upstream" dialog
    And I enter a unique name
    And I choose "Anthropic OAuth" as the kind
    And I press Create
    Then the upstream of kind "anthropic_oauth" should be created in an unconnected state
    And a "Connect OAuth" follow-up modal should appear with instructions
    When I press "Connect Claude OAuth"
    Then a popup or new tab should open targeting the Anthropic authorize URL
    When I paste a valid authorization code into the modal
    And I press Complete
    Then the modal should close
    And the row for the new upstream should now reflect the completed OAuth state

  Scenario: OAuth start falls back to current-window navigation when the popup is blocked
    Given the browser blocks the OAuth popup
    When I press "Connect Claude OAuth"
    Then the current window should navigate to the provider authorize URL

  Scenario: Authorization code is required to complete
    Given the "Connect OAuth" modal is open
    And the authorization code field is empty
    Then the "Complete" button should be disabled
    And no completion request should be issued

  Scenario: Restart resets the OAuth modal
    Given the "Connect OAuth" modal is in a partial state
    When I press Restart
    Then the modal should reset so I can begin the OAuth flow again

  Scenario: Closing the OAuth modal without completing leaves the upstream unconnected
    Given the upstream of kind "anthropic_oauth" has been created and the "Connect OAuth" modal is open
    When I dismiss the modal without completing
    Then the upstream should still be present in the table
    But it should remain in an unconnected state until OAuth completion
```

```gherkin
@crud
Feature: Editing an upstream

  Background:
    Given I am signed in
    And upstream "dummy" exists

  Scenario: Happy path — change the base URL
    When I open the edit dialog for "dummy"
    And I change the Base URL to a valid URL
    And I press "Save Changes"
    Then the dialog should close
    And the row for "dummy" should reflect the new revision

  Scenario: Name is required when editing
    When I open the edit dialog for "dummy"
    And I clear the name field
    And I press "Save Changes"
    Then the dialog should remain open with a validation error
    And the upstream should be unchanged

  Scenario: Cancelling the edit dialog discards the change
    When I open the edit dialog for "dummy"
    And I change the Base URL
    And I press Cancel
    Then the dialog should close
    And the upstream should be unchanged

  Scenario: Server-detected conflict opens a "Conflict Detected" modal
    Given another operator has modified upstream "dummy" since I opened the edit dialog
    When I press "Save Changes"
    Then a "Conflict Detected" modal should open showing the current revision
    And I should be offered a "Load Latest" affordance
```

```gherkin
@destructive
Feature: Enabling, disabling, and deleting an upstream

  Background:
    Given I am signed in
    And upstream "dummy" exists and is currently active

  Scenario: Disabling an active upstream
    When I press Disable on the row for "dummy"
    Then the row should display "Disabled"

  Scenario: Re-enabling a disabled upstream
    Given upstream "dummy" is currently disabled
    When I press Enable on the row for "dummy"
    Then the row should display "Active"

  Scenario: Deletion requires confirmation
    When I press Delete on the row for "dummy"
    Then a confirmation modal should appear
    And the upstream should remain until I confirm

  Scenario: Confirming deletion removes the upstream
    Given the delete confirmation modal for "dummy" is open
    When I confirm deletion
    Then the modal should close
    And the row for "dummy" should no longer be present

  Scenario: Cancelling deletion preserves the upstream
    Given the delete confirmation modal for "dummy" is open
    When I press Cancel
    Then the modal should close
    And the row for "dummy" should still be present

  Scenario: Deletion is blocked when the upstream is referenced
    Given upstream "dummy" is referenced by at least one principal or plugin chain
    When I confirm deletion
    Then a browser alert should be raised containing the server's reference error message
    And after I dismiss the alert the upstream should remain in the table
```

```gherkin
@conflict
Feature: Concurrency conflicts on upstreams

  Background:
    Given I am signed in
    And upstream "dummy" exists at revision N

  Scenario: Conflict on enable/disable
    Given another operator has advanced "dummy" to revision N+1
    When I press Disable on my stale view
    Then a "Conflict Detected" modal should open
    And nothing about "dummy" should change until I either cancel or load the latest

  Scenario: Loading the latest re-fetches the upstreams list
    Given the "Conflict Detected" modal is open
    When I press "Load Latest"
    Then the modal should close
    And the upstreams list should be refetched
```

---

### 4.9 Admin Activity (`/activity`)

```gherkin
Feature: Audit log

  Background:
    Given I am signed in
    And I am on the Admin Activity page

  Scenario: Loading state
    Given the audit endpoint is intentionally slow
    Then I should see a loading indicator

  Scenario: Empty state when nothing has been audited yet
    Given no administrative action has been recorded
    Then the page should display "No activity found"

  Scenario: Audit failure surfaces an error
    Given the audit endpoint will return a server error
    Then I should see "Failed to load audit events" with the error detail

  Scenario: Refresh re-queries the audit feed
    When I press Refresh
    Then the audit endpoint should be re-queried
    And any newly recorded events should be visible

  Scenario Outline: Filtering by action kind
    When I select action kind "<kind>" from the filter
    Then only audit events whose kind is "<kind>" should remain in the table

    Examples:
      | kind              |
      | config_apply      |
      | api_key_issue     |
      | api_key_revoke    |
      | credential_rotate |
      | credential_revoke |
      | principal_create  |
      | principal_update  |
      | principal_disable |
      | quota_override    |
      | killswitch_set    |
      | oauth_complete    |

  Scenario: Resetting the filter to "All Actions"
    Given I have filtered by a specific kind
    When I select "All Actions"
    Then all audit events should be eligible to appear again

  Scenario: Sensitive payload fields are redacted
    Given an audit event whose payload contains a sensitive key such as "token", "secret", "plaintext", "client_secret", "password", or "key_hash_b64"
    When I view its row
    Then the corresponding values should be replaced by a "<redacted>" placeholder

  Scenario: Hovering over a relative time reveals the absolute timestamp
    When I hover over the relative time of an audit row
    Then the absolute timestamp should be displayed as a tooltip
```

---

### 4.10 Credential Status (`/credentials`)

```gherkin
Feature: Credential status overview

  Background:
    Given I am signed in
    And I am on the Credential Status page

  Scenario: Loading state
    Given the credentials endpoint is intentionally slow
    Then the page should display a loading indicator

  Scenario: Empty state
    Given no credential is configured
    Then the page should display "No credentials found"

  Scenario: Refresh re-queries the credentials feed
    When I press Refresh
    Then the credentials endpoint should be re-queried

  Scenario Outline: Status chip reflects credential health
    Given a credential with status "<status>"
    Then its row should display a "<status>" chip
    And rows where action is required must display the "Action needed" alert

    Examples:
      | status         |
      | valid          |
      | expiring_soon  |
      | expired        |
      | revoked        |
      | missing        |

  Scenario: Hovering over a relative expiry reveals the absolute timestamp
    Given a credential has an expiry in the future
    When I hover over the relative expiry text on its row
    Then a tooltip should reveal the absolute expiration date and time

  Scenario: Row reports how many principals reference the credential
    Given a credential is referenced by exactly 2 principals
    Then its row should display "2 principals" as a count
    And the row should not enumerate the associated principal names

  Scenario: OAuth credential displays whether a refresh token is available
    Given an OAuth credential with a refresh token
    Then its refresh-token column should display "yes"

  Scenario: Non-OAuth credentials do not display a refresh-token value
    Given an API-key credential
    Then its refresh-token column should display "n/a"

  Scenario: "Never" expiry is rendered explicitly
    Given a credential without an expiry
    Then its expiry column should display "never"
```

---

### 4.11 Plugins — Wasm Registry (`/plugins?tab=registry`)

```gherkin
@smoke
Feature: Wasm registry listing

  Background:
    Given I am signed in
    And I am on the Plugins page on the Wasm Registry tab

  Scenario: Empty registry
    Given no plugin has been uploaded
    Then the table should display "No plugins uploaded yet."

  Scenario: Registry rows show name, sha256 prefix, size, and refcount
    Given a plugin "echo" has been uploaded
    Then the registry table should contain a row for "echo"
    And the row should show its sha256 prefix, size in KB, and refcount badge

  Scenario: Registry load failure surfaces a banner
    Given the plugin registry endpoint will return a server error
    When I open the Wasm Registry tab
    Then a red inline error banner should display the failure detail

  Scenario: Upload failure surfaces an inline error
    Given the plugin upload endpoint will return a server error
    When I attempt to upload a valid Wasm file
    Then a red inline error banner should display the upload failure detail
    And no new row should appear in the registry
```

```gherkin
@crud
Feature: Uploading a Wasm plugin

  Background:
    Given I am signed in
    And I am on the Plugins page on the Wasm Registry tab

  Scenario: Happy path
    When I select a valid Wasm file under 32 MiB
    And I supply a plugin name
    And I press Upload
    Then a progress bar should display upload progress
    And the new plugin should appear in the registry table when the upload completes

  Scenario: Upload button is disabled until a file is selected
    Given no file is selected
    Then the Upload button should be disabled

  Scenario: Files larger than 32 MiB are rejected client-side
    When I select a Wasm file larger than 32 MiB
    Then I should see an error "File exceeds 32 MiB limit"
    And the Upload button should not initiate any network request

  Scenario: Non-Wasm files are rejected client-side
    When I select a file that does not begin with the Wasm magic bytes
    Then I should see an error indicating the file is not a valid Wasm module
    And the Upload button should not initiate any network request

  Scenario: Name defaults to the filename without extension
    When I select a file named "echo.wasm"
    Then the plugin name field should be pre-filled with "echo"

  Scenario: Name is required
    When I clear the plugin name field
    Then the Upload button should be disabled
```

```gherkin
@destructive
Feature: Deleting a Wasm plugin from the registry

  Background:
    Given I am signed in
    And I am on the Plugins page on the Wasm Registry tab

  Scenario: Deleting an unused plugin removes it from the registry
    Given a plugin "echo" exists in the registry and is not in use
    When I press Delete on its row
    Then the row should be removed from the registry

  Scenario: Deletion is forbidden while the plugin is in use
    Given a plugin "echo" has refcount greater than zero
    Then the Delete button on its row should be disabled
    And the disabled button should expose a native title attribute such as "In use by <N> chains"
```

---

### 4.12 Plugins — Per-Principal Chains

The Plugins page renders the Wasm Registry tab by default. The chains UI is reached by clicking the "Per-Principal Chains" tab inside the page; query-string deep-linking such as `/plugins?tab=chains` is NOT implemented.

```gherkin
Feature: Selecting a principal exposes its chains

  Background:
    Given I am signed in
    And I am on the Plugins page
    And I have clicked the "Per-Principal Chains" tab

  Scenario: No principal selected by default
    Then a "Select Principal" dropdown should be visible
    And neither the router chain section nor the observability chain section should be shown

  Scenario: Selecting a principal reveals the router and observability chain editors
    Given principal "alpha" exists
    When I pick the principal "alpha" from the Select Principal dropdown
    Then a "router Chain" section should be visible
    And an "observability hook Chain" section should be visible
```

```gherkin
@crud
Feature: Adding a plugin to a chain

  Background:
    Given I am signed in
    And plugin "echo" exists in the registry
    And principal "alpha" exists
    And I have clicked the "Per-Principal Chains" tab
    And I have picked principal "alpha" from the Select Principal dropdown

  Scenario: Happy path — append a plugin to the router chain
    When I press "Add Plugin" in the router chain section
    And I select plugin "echo"
    And I leave the config JSON as the default empty object
    And I press Add
    Then the router chain should list "echo" as its newest entry

  Scenario: Plugin selection disables the Add button
    When I press "Add Plugin" in the router chain section
    And the plugin select is left unset
    Then the Add button in the form should be disabled
    And no add request should be issued

  Scenario: Invalid JSON in the config field is rejected
    When I press "Add Plugin" in the router chain section
    And I enter "{ not json }" into the config field
    And I press Add
    Then the form should not submit
    And I should see "Invalid JSON config"

  Scenario: Cancelling the Add Plugin form
    When I press "Add Plugin" in the router chain section
    And I press Cancel
    Then the form should close
    And no plugin should be added to the chain

  Scenario: Chain load failure surfaces an inline error
    Given the plugin-chain endpoint will return a server error for principal "alpha"
    Then the chain section should display the failure detail inline
```

```gherkin
@destructive
Feature: Removing a plugin from a chain

  Background:
    Given I am signed in
    And principal "alpha" has at least one plugin assigned to its router chain
    And I have clicked the "Per-Principal Chains" tab
    And I have picked principal "alpha" from the Select Principal dropdown

  Scenario: Removing the last plugin reveals the empty state
    Given the router chain for "alpha" has exactly one plugin
    When I remove that plugin
    Then the chain should display "No plugins in this chain."
```

```gherkin
@regression
Feature: Reordering a chain
  Plugins use a sparse-order key. Reordering may surface a "needs rebalance" error which can be resolved with auto-balance.

  Background:
    Given I am signed in
    And principal "alpha" has at least two plugins assigned to its router chain
    And I have clicked the "Per-Principal Chains" tab
    And I have picked principal "alpha" from the Select Principal dropdown

  Scenario: Happy path — drag a chain item to a new position
    When I drag the first plugin in the chain to the position of the second plugin
    Then the chain order should reflect my drop
    And no error banner should be visible

  Scenario: Order collision raises a "needs rebalance" banner
    Given two adjacent plugins in the chain occupy colliding order keys
    When I attempt to reorder
    Then a "Chain needs rebalancing before reordering." error should appear
    And an "Auto-balance" affordance should be offered

  Scenario: Auto-balance clears the rebalance error
    Given the rebalance error banner is visible
    When I press "Auto-balance"
    Then the chain should be re-keyed with spread order values
    And the rebalance error should disappear
    And subsequent reorders should succeed without error

  @a11y
  Scenario: Reordering a chain entirely from the keyboard
    Given principal "alpha" has at least two plugins assigned to its router chain
    And I have clicked the "Per-Principal Chains" tab
    And I have picked principal "alpha" from the Select Principal dropdown
    When I move keyboard focus to the drag handle of the first plugin
    And I press Space to lift it
    And I press the Down arrow to move it past the next item
    And I press Space to drop it
    Then the chain order should reflect the new position
    And focus should remain on the moved item
```

```gherkin
@wip
Feature: Plugin Status panel on the Plugins page (NOT YET WIRED)
  The PluginStatus.tsx component exists and can render Extism plugin runtime health, but the /plugins route currently renders only the PluginRegistry page. These scenarios document the intended UI once PluginStatus is wired into the page.

  Background:
    Given I am signed in
    And I am on the Plugins page

  Scenario: Status panel reports a healthy plugin
    Given a plugin has loaded successfully and reports zero failures
    Then the status panel should list that plugin as healthy

  Scenario: Status panel surfaces plugin failures
    Given a plugin has at least one runtime failure recorded
    Then the status panel should display its failure count and the last error message
    And the panel should distinguish "disabled" plugins from "loaded but failing" plugins
```

---

### 4.13 404 Not Found

```gherkin
Feature: Unknown routes
  Unrecognized paths should not crash the shell.

  Scenario: Unknown route displays the 404 page
    Given I am signed in
    When I navigate to an unknown console route
    Then I should see the "404 Not Found" page
    And the page should offer a way to return to the dashboard root
```

---

## Part 5 — Cross-cutting Concerns

These scenarios apply to every modal, every form, every table.

```gherkin
@a11y
Feature: Universal modal dismissal contract
  The shared Modal primitive supports two dismissal methods only: the Escape key (global keydown handler) and the X icon in the header. The semi-transparent backdrop is NOT click-dismissible. Per-feature Cancel buttons sit inside the modal body and behave equivalently to Escape for state-discarding purposes.

  Scenario Outline: Modal dismissal preserves underlying state
    Given a modal "<modal>" is open with partially entered data
    When I dismiss it via "<method>"
    Then the modal should close
    And the state underneath the modal should be unchanged

    Examples:
      | modal              | method        |
      | New Upstream       | Cancel button |
      | New Upstream       | Escape key    |
      | New Upstream       | X icon        |
      | Edit Upstream      | Cancel button |
      | Edit Upstream      | Escape key    |
      | Edit Upstream      | X icon        |
      | Delete Upstream    | Cancel button |
      | Delete Upstream    | Escape key    |
      | Delete Upstream    | X icon        |
      | Connect OAuth      | X icon        |
      | Connect OAuth      | Escape key    |
      | New Principal      | Cancel button |
      | New Principal      | Escape key    |
      | New Principal      | X icon        |
      | Issue Key          | Cancel button |
      | Issue Key          | Escape key    |
      | Revoke Key         | Cancel button |
      | Revoke Key         | Escape key    |
      | Credential Rotate  | Cancel button |
      | Credential Rotate  | Escape key    |
      | Credential Revoke  | Cancel button |
      | Credential Revoke  | Escape key    |
      | Apply Configuration| Cancel button |
      | Apply Configuration| Escape key    |
      | Configuration Diff | Close button  |
      | Configuration Diff | Escape key    |
      | Admin Token        | Cancel button |
      | Admin Token        | Escape key    |

  Scenario: Clicking the modal backdrop does NOT dismiss the modal
    Given any modal is open
    When I click the backdrop area outside the modal body
    Then the modal should remain open
    And no underlying state should change
```

```gherkin
@async
Feature: List endpoint failures — error contract per page
  Error rendering varies by page. Only endpoints whose page wraps the error in an ErrorState component receive an explicit retry button; other pages render plain text or a red banner.

  Scenario Outline: Endpoints rendered with a retry-capable ErrorState
    Given the "<endpoint>" endpoint will return a server error
    When I open the page that depends on "<endpoint>"
    Then I should see an inline error containing the failure detail
    And I should be able to retry the request from that error block

    Examples:
      | endpoint            |
      | overview summary    |
      | overview timeseries |
      | overview usage      |
      | upstreams           |
      | audit               |
      | credentials         |

  Scenario Outline: Endpoints rendered with a plain error message (no retry button)
    Given the "<endpoint>" endpoint will return a server error
    When I open the page that depends on "<endpoint>"
    Then I should see the failure detail rendered inline as plain text or a red banner
    And I should NOT expect a retry button on that error block

    Examples:
      | endpoint                |
      | principals directory    |
      | principals management   |
      | plugin registry         |
      | plugin chains           |
      | principal usage summary |
      | principal limits        |
```

```gherkin
@auth
Feature: 401 detection drives the topbar connection indicator
  Only the dashboard connection probes — the periodic `/admin/health` check and the global SSE stream — feed the topbar indicator. Arbitrary REST mutations that return 401 throw ApiError to their caller but do NOT flip the indicator on their own.

  Scenario: A 401 from the health probe marks the connection unauthorized
    Given I am signed in
    When the periodic admin health check returns 401
    Then the topbar connection indicator should read "AUTH REQUIRED" with a red dot
    And I should still be able to open the "Admin Token" modal to supply a new token

  Scenario: A 401 from the dashboard SSE stream marks the connection unauthorized
    Given I am signed in
    When the global admin event stream connection returns 401
    Then the topbar connection indicator should read "AUTH REQUIRED" with a red dot

  Scenario: A 401 from an arbitrary REST mutation does NOT auto-flip the topbar
    Given I am signed in and the topbar is "LIVE"
    When a single REST mutation such as creating an upstream returns 401
    Then the mutation should surface its own ApiError to the dialog or page
    And the topbar should remain "LIVE" until the next health probe or SSE event observes the 401
```

```gherkin
@a11y
Feature: Keyboard navigation through primary surfaces

  Scenario: I can tab from the sidebar through the topbar into the page content
    Given I am signed in
    When I press Tab repeatedly from a clean focus state
    Then focus should traverse the sidebar links, then the topbar controls, then the page-level actions in a predictable order
    And no focusable element should be hidden by being covered or by missing a focus ring
```

```gherkin
@conflict
Feature: Optimistic concurrency — how stale revisions are detected
  Different resources signal their expected revision differently. Upstream, plugin registry entry, and plugin-chain delete use the `If-Match: W/"<revision>"` request header. Plugin-chain reorder and config draft put `expected_revision` into the request body. Plugin-chain insert sends no revision at all. Principal management mutations through `useDraftPrincipals` send neither header nor expected_revision — they rely on the draft as the unit of concurrency control.

  Scenario Outline: Mutations on "<resource>" send the current revision as an If-Match header
    Given resource "<resource>" "X" exists at revision N
    When I trigger a mutation on "X"
    Then the request should carry an "If-Match" header containing W/"N"

    Examples:
      | resource              |
      | upstream              |
      | plugin registry entry |
      | plugin chain delete   |

  Scenario Outline: Mutations on "<resource>" send expected_revision in the request body
    Given resource "<resource>" "X" exists at revision N
    When I trigger a mutation on "X"
    Then the request body should include the field "expected_revision" set to N

    Examples:
      | resource             |
      | plugin chain reorder |
      | config draft         |

  Scenario: Plugin-chain insert sends no revision
    Given a plugin-chain insert is issued for a known principal
    Then the insert request should send neither an If-Match header nor an expected_revision field
    And the server should accept the insert based on its own internal sequencing

  Scenario: Principal management mutations rely on the draft, not per-resource revisions
    Given I am editing a principal through Principal Management
    When the dashboard saves the principal to the draft
    Then the request should not include an If-Match header
    And it should not include an expected_revision field for the principal itself
    But the enclosing draft PUT must include the draft's expected_revision

  Scenario Outline: Stale revision raises a conflict on "<resource>"
    Given resource "<resource>" "X" has advanced from revision N to revision N+1 on the server
    And my local view still believes "X" is at revision N
    When I trigger a mutation on "X"
    Then the server should respond with 409 Conflict
    And the dashboard should surface a conflict-specific affordance (e.g. "Load Latest" or "Refresh to continue")
    And no part of "X" should be mutated until I reconcile

    Examples:
      | resource             |
      | upstream             |
      | plugin chain reorder |
      | config draft         |
```

```gherkin
@async
Feature: Background polling keeps the dashboard fresh
  Several views poll the admin API on a fixed cadence so that the operator sees current data without manual refresh.

  Scenario Outline: "<view>" polls at "<interval>"
    Given I am on the page that hosts "<view>"
    When the polling tick fires
    Then the corresponding endpoint should be re-queried

    Examples:
      | view                          | interval |
      | Status overview               | 5 s      |
      | Overview dashboard summary    | 30 s     |
      | Overview usage series         | 30 s     |
      | Admin Activity audit feed     | 30 s     |
      | Credential Status credentials | 30 s     |
      | Plugin runtime status         | 30 s     |
      | Principal usage               | 60 s     |
      | Principal rate-limit snapshot | 60 s     |

  Scenario: Realtime Log recent events are fetched on mount and filter change only
    Given I am on the Realtime Log page
    When no filter changes and no remount occurs
    Then the recent-events endpoint should NOT be polled on any fixed interval
    But the live SSE stream is the sole source of new rows after initial load
    And changing any filter should re-fetch the recent events for the new filter set
```

```gherkin
@regression
Feature: Adversarial edge cases
  These scenarios cover known landmines in the implementation: defects already observed in the code, browser quirks, and time- or clock-related surprises. They exist so a future change cannot silently regress these specific behaviors without flagging a failing test.

  @a11y
  Scenario: Escape key with stacked modals only closes the topmost modal
    Given I have opened the "Keys" modal for principal "alpha"
    And I have pressed "Revoke" to open the revoke-confirmation modal on top of it
    When I press the Escape key
    Then the revoke-confirmation modal should close
    And the "Keys" modal should remain open

  Scenario: Closing a stacked modal preserves the background scroll lock
    Given I have opened the "Keys" modal for principal "alpha"
    And I have opened the revoke-confirmation modal on top of it
    When I cancel the revoke-confirmation modal
    Then the page background should remain scroll-locked
    Because the underlying "Keys" modal is still open

  @conflict
  Scenario: Autosave conflict merge preserves another operator's top-level edits
    Given another operator has changed a top-level field "telemetry.metrics_port" and advanced the draft
    When I edit a different top-level field "listener.port" and my autosave triggers a conflict merge
    Then the resulting draft should still contain the other operator's "telemetry.metrics_port" change
    And my "listener.port" change should also be present
    But operators must be aware that the merge is shallow at the top level only

  @conflict
  Scenario: Autosave conflict merge does NOT deep-merge nested objects
    Given another operator has changed a nested field inside the "listener" object
    When my local draft also edits a different nested field inside "listener" and conflict-merges
    Then my entire "listener" object will overwrite theirs because the merge is shallow at the top level
    And this is documented as a known limitation

  Scenario: Donut chart handles a negative remaining quota gracefully
    Given principal "alpha" has a snapshot whose remaining quota is negative (overage)
    When the rate-limit panel renders
    Then the donut chart should render as fully exhausted
    And the SVG proportions should remain valid
    And the numeric remaining value should still be displayed as reported by the server

  @auth
  Scenario: Dashboard degrades gracefully when localStorage is blocked
    Given my browser is configured to throw a SecurityError when localStorage is read
    When I navigate to the console
    Then the app should not crash with an uncaught SecurityError
    And the operator should still be able to recognize that the dashboard cannot persist its admin token

  Scenario: Rate-limit reset time is resilient to local clock skew
    Given my local system clock is skewed several minutes into the future
    And "alpha" has a snapshot whose reset is a few minutes in the future on the server
    Then the donut chart should display a positive remaining time
    And it should not erroneously render "resetting soon"

  Scenario: Status badge ignores stringified nulls in apply-error fields
    Given principal "alpha" exposes a last_apply_error value of the JSON string "null"
    Then the roster row should display a healthy status badge
    And it should not render an "Apply Error" badge whose body is the literal text "null"

  @smoke
  Scenario: Killswitch state is visible globally to the operator
    Given the global killswitch is currently enabled
    When I navigate anywhere in the admin console
    Then a persistent banner or indicator must communicate that the killswitch is active and all proxy traffic is being rejected
    So that the operator does not silently debug zero-traffic symptoms unaware of the killswitch
```

```gherkin
Feature: What this dashboard intentionally does NOT do
  Negative-space documentation. These behaviors must NOT exist; their absence is a feature.

  Scenario: There is no username/password account login
    Then there must be no username field anywhere in the authentication flow
    And there must be no password-account-login UI: the only credential the operator supplies is the admin bearer token
    But the admin token input MAY be rendered as <input type="password"> for input masking

  Scenario: There is no theme switcher
    Then the operator must not be able to switch between light and dark modes

  Scenario: There is no user-account management
    Then there must be no UI for creating, listing, or deleting other admin operators

  Scenario: Tables do not paginate
    Then upstreams, credentials, audit, and plugin tables must render all loaded rows in a single scrollable area without pagination controls

  Scenario: There is no duplicate action
    Then no row action labelled "Duplicate" should exist for upstreams, principals, or plugins

  Scenario: There is no log export
    Then the Realtime Log page must not expose any "Export" or "Download" affordance

  Scenario: There is no audit export
    Then the Admin Activity page must not expose any "Export" or "Download" affordance

  Scenario: There is no global toast system
    Then the dashboard must not render floating toast notifications
    And every success must be observable from in-place UI changes (e.g. table refresh, inline confirmation text, modal swap)
    And every Upstream-mutation failure surfaces via window.alert() rather than an inline toast

  Scenario: There is no killswitch toggle in the UI
    Given the killswitch state is observable from /admin/v1/status and from the audit log
    Then no button, switch, or menu item must toggle the killswitch from the dashboard
    And reactivating or activating the killswitch must require a direct admin API call

  Scenario: There is no "Reload Config" (SIGHUP) button
    Then no UI affordance must invoke POST /admin/config/reload
    And configuration reload happens implicitly as part of Apply

  Scenario: There is no "Garbage Collect Orphaned WASM" button
    Given the admin API exposes POST /admin/v1/plugins/wasm/gc
    Then no UI affordance must invoke that endpoint
    And the underlying useGcOrphaned hook must remain unwired

  Scenario: There is no enable/disable affordance on individual API keys
    Given the admin API supports POST /admin/principals/{id}/keys/{key_id}/enable and /disable
    Then the Keys panel must only offer "Issue" and "Revoke" — never per-key enable or disable

  Scenario: There is no "Export Configuration" button
    Given the useExport hook exists and can download /admin/v1/export
    Then no UI affordance must invoke that hook
    And the operator must not be able to download a config snapshot from the dashboard

  Scenario: The detailed Upstream health card is not wired
    Given UpstreamCard.tsx exists and would render breaker/bulkhead/drain/killswitch detail
    Then the Upstreams page must not include that card in its rendered tree
    And per-upstream runtime health detail must remain unavailable from the dashboard
```

---

## Authoring Checklist

Before adding a new scenario, check that it satisfies these constraints:

- [ ] Single behavior; one reason to fail.
- [ ] Declarative phrasing; no CSS selectors, no element IDs.
- [ ] Uses the ubiquitous language from §3.
- [ ] Tagged appropriately.
- [ ] Every modal added → cancel scenario + escape scenario + X-icon scenario (the shared Modal does NOT support backdrop-click dismissal).
- [ ] Every destructive action → confirm scenario + cancel-confirm scenario.
- [ ] Every form with required fields → at least one validation scenario per required field.
- [ ] Every list endpoint → loading scenario + empty scenario + error scenario.
- [ ] No incidental data ("alpha-2026-05-30") unless the value is part of the behavior.

---

## Part 6 — Implementation Appendix (agent-browser)

This appendix is **not part of the BDD contract**. It captures concrete `agent-browser` implementation guidance distilled from the cross-verification audits (Oracle, Ultrabrain, Artistry, Deep) so that the test author does not have to rediscover the harness shape from scratch.

`agent-browser` (Chromium via CDP, accessibility-tree snapshots, `@eN` refs) is the execution surface — NOT Playwright. The BDD contract itself is harness-agnostic; this appendix is the bridge between the contract and the tool.

### 6.1 Suggested test-script split

Each script is a shell program (`.sh`) that drives `agent-browser` against the admin console at `http://localhost:5173` (Vite dev server, proxied to the cc-lb admin API on `127.0.0.1:8082`). One script per feature surface, exit non-zero on any assertion failure.

| Script | Scope |
|---|---|
| `auth.sh` | Admin token gate, topbar token modal, 401 detection via health/SSE |
| `shell.sh` | Sidebar navigation, 404 page, negative-space checks (no toast, no theme switcher, no killswitch toggle, etc.) |
| `overview.sh` | KPI cards, time-series chart, principal usage strip, resource error banner |
| `log.sh` | Realtime Log table, embedded live log, SSE backfill, pause/resume/flush, filters, row detail |
| `principal-limits.sh` | `/principals` directory, usage summary, rate-limit snapshots |
| `management-principals.sh` | Roster, principal create/edit, allowed models, keys, quota override, draft banner |
| `management-credentials.sh` | Credentials tab (rotate, revoke) |
| `settings.sh` | Schema form, autosave, conflict-merge, validate/apply, history, diff modal, restart banner |
| `upstreams.sh` | Upstream CRUD, OAuth popup, browser-alert error contract, conflicts |
| `activity.sh` | Audit feed, filtering, redaction, tooltips |
| `credentials.sh` | Credential Status page |
| `plugins-registry.sh` | Wasm registry upload, delete, validation |
| `plugins-chains.sh` | Chain select, add, remove, reorder, rebalance |
| `polling.sh` | Background polling cadence (use `network requests` over real time intervals) |
| `cross-cutting.sh` | Universal modal dismissal, list-error contract, If-Match contract, keyboard traversal, adversarial edge cases |

Each script begins with a session setup block that restores the admin token and clears any prior state, e.g.:

```bash
SESSION="cc-lb-$(basename "$0" .sh)"
agent-browser --session "$SESSION" open "http://localhost:5173"
agent-browser --session "$SESSION" eval --stdin <<EOF
localStorage.setItem('cc-lb-admin-token', '$CC_LB_ADMIN_TOKEN');
location.reload();
EOF
agent-browser --session "$SESSION" wait --load networkidle
```

Use `--session` to keep scripts isolated so they can run in parallel without sharing cookies/localStorage/refs.

### 6.2 Seed-data dependencies

Seeding goes through the admin REST API with `curl -H "Authorization: Bearer $CC_LB_ADMIN_TOKEN"`, NOT through the UI. The dashboard is for behavior verification; setup is data-plane work.

| Seed group | Used by | How to seed |
|---|---|---|
| `dummy` upstream | Upstreams listing, edit, disable, delete, conflict scenarios | Already provisioned by `cc-lb-server` global setup; or `POST /admin/v1/upstreams` |
| `alpha`, `beta` principals | Principal Limits, Principal Management, plugin chains, log filters | `POST /admin/principals` then `POST /admin/config/draft/apply` |
| Active key for `alpha` | Key revoke / cancel / close, proxy traffic generation | `POST /admin/principals/alpha/keys` |
| Uploaded `echo.wasm` | Registry row, delete, chain add/remove/reorder | `POST /admin/v1/plugins/wasm/upload` (multipart) |
| Two chain entries with controllable sparse-order keys | Reorder, collision, rebalance scenarios | `POST /admin/v1/plugins/chains/{slot}/insert` twice; mutate `order_key` directly for collision case |
| Pending / validated / applied history | Settings apply, draft banner, history/diff scenarios | `PUT /admin/config/draft` → `POST /admin/config/draft/validate` → `POST /admin/config/draft/apply` (repeat for history depth) |
| Usage / request event rows | Overview KPI/chart/usage, Realtime Log, audit | Drive synthetic traffic through `fake-anthropic` upstream via `POST /v1/messages` on the proxy port |
| Credential matrix rows | Credential Status, credential rotate/revoke | Seed via admin API + manipulate credential status server-side |
| Error-state upstream / principal | Overview resource error banner, status badge popovers | Trigger an apply with bad config to produce a real `last_apply_error` |

### 6.3 Harness translation — Playwright pattern → agent-browser pattern

| What the BDD scenario needs | agent-browser command |
|---|---|
| Assert an element / text is visible | `agent-browser snapshot -i` then grep stdout for `<role>` / text; or `agent-browser get text @eN` |
| Click a button by visible label | `agent-browser find role button click --name "Save"` (no prior snapshot needed) |
| Click via ref | `agent-browser snapshot -i` then `agent-browser click @eN` (re-snapshot after page change!) |
| Fill an input by label | `agent-browser find label "Name" fill "value"` |
| Press a keyboard combination | `agent-browser press Escape` / `agent-browser press Control+a` |
| Open the page in a fresh session | `agent-browser --session "$NAME" open "http://localhost:5173/<route>"` |
| Wait for SPA navigation | `agent-browser wait --url "**/management"` or `agent-browser wait --load networkidle` |
| Wait for an element to appear | `agent-browser wait @eN` (after snapshot) or `agent-browser wait --text "Saved"` |
| Stub a 500 response (Playwright: `page.route`) | `agent-browser network route "**/admin/v1/upstreams" --status 500 --body '{"error":"boom"}'` |
| Stub a 409 conflict response | `agent-browser network route "**/admin/principals/**" --status 409 --body '{"code":"stale_revision"}'` |
| Abort a request entirely | `agent-browser network route "**/analytics" --abort` |
| Count how many times an endpoint was hit (polling assertions) | `agent-browser network requests --filter "**/admin/v1/status"` after waiting the interval |
| Capture all network traffic for a flow | `agent-browser network har start` … perform actions … `agent-browser network har stop ./flow.har` |
| Capture a `window.alert(...)` (Playwright: `page.on('dialog')`) | Override BEFORE triggering: `agent-browser eval --stdin <<EOF` then `window.__alerts=[];window.alert=m=>window.__alerts.push(m);EOF`; after triggering, `agent-browser eval "window.__alerts"` |
| Read clipboard contents (Playwright: `grantPermissions`) | Launch with `--browser-args "--enable-features=ClipboardContentSetting"` then `agent-browser eval "await navigator.clipboard.readText()"` |
| Upload a file (Playwright: `setInputFiles`) | `agent-browser upload @eN ./fixtures/echo.wasm` (path relative to CWD) |
| Drag-and-drop (Playwright: `page.dragTo`) | `agent-browser drag @e1 @e2`. dnd-kit is flaky with mouse drag — prefer the `@a11y` keyboard scenario: `agent-browser focus @handle` → `press Space` → `press ArrowDown` → `press Space` |
| Take a screenshot | `agent-browser screenshot --full ./out.png` |
| Take an annotated screenshot for debugging | `agent-browser screenshot --annotate ./debug.png` (labels map to `@eN` refs) |
| Multiple concurrent users (Playwright: separate `context`) | `agent-browser --session alice ...` and `agent-browser --session bob ...` |
| Wait for a popup window (Playwright: `context.waitForEvent('page')`) | Click the trigger, then `agent-browser tab` to list new tabs, switch with `agent-browser tab <id>` |
| Persist authenticated state across script runs | `agent-browser state save ./fixtures/admin-state.json` once, then `agent-browser --state ./fixtures/admin-state.json open ...` |
| Inspect SSE traffic | `agent-browser network requests --filter "**/admin/events/stream"` (one persistent request per connection); for event payloads, `agent-browser eval` an `EventSource` or read DOM updates |

**Snapshot discipline**: `@eN` refs become stale on every page change (click → navigate, form submit, dynamic re-render, dialog open). RE-SNAPSHOT before every ref interaction or you will silently target the wrong element.

### 6.4 Assertion helper conventions

`agent-browser` returns shell exit codes and human-readable output. Build a small assertion helper next to the scripts:

```bash
# e2e/helpers.sh
assert_snapshot_contains() {
  local SESSION="$1"; local NEEDLE="$2"
  agent-browser --session "$SESSION" snapshot -i | command grep -F "$NEEDLE" \
    || { echo "FAIL: snapshot missing: $NEEDLE"; exit 1; }
}

assert_text_equals() {
  local SESSION="$1"; local REF="$2"; local EXPECTED="$3"
  local ACTUAL
  ACTUAL=$(agent-browser --session "$SESSION" get text "$REF")
  [ "$ACTUAL" = "$EXPECTED" ] \
    || { echo "FAIL: expected '$EXPECTED' got '$ACTUAL'"; exit 1; }
}

assert_url_matches() {
  local SESSION="$1"; local PATTERN="$2"
  local URL
  URL=$(agent-browser --session "$SESSION" get url)
  [[ "$URL" == $PATTERN ]] \
    || { echo "FAIL: url '$URL' does not match '$PATTERN'"; exit 1; }
}

assert_request_count_at_least() {
  local SESSION="$1"; local PATTERN="$2"; local MIN="$3"
  local COUNT
  COUNT=$(agent-browser --session "$SESSION" network requests --filter "$PATTERN" \
            --json | jq 'length')
  [ "$COUNT" -ge "$MIN" ] \
    || { echo "FAIL: $PATTERN seen $COUNT times, expected >= $MIN"; exit 1; }
}
```

Each scenario implementation reads as a near-direct translation of its Gherkin steps, with `assert_*` helpers at every `Then` step.

### 6.5 Harness work classification

| Category | Notes |
|---|---|
| **Ready now** | All scenarios that need only `snapshot` / `click` / `fill` / `wait` against existing admin APIs (most of §4.1–§4.13 happy paths and existing dialogs). |
| **Minor harness work** | Seed helpers (curl-based admin API calls wrapped as bash functions); `network route` mock injection for loading / 500 / 409 states; `eval --stdin` setup to capture `window.alert` calls in Upstreams; clipboard permission via `--browser-args`. |
| **Significant harness work** | Deterministic Overview traffic (admin API seeding or `network route` fixtures for `/admin/dashboard/summary` and `/admin/usage`); live log SSE control (deterministic event emission via real proxy calls); polling cadence (real-time waits + `network requests` count, since `agent-browser` has no fake clock); full credential status matrix (storage seeding for `expiring_soon`, `expired`, `revoked`, `missing`); plugin runtime status (route interception); dnd-kit mouse drag (use keyboard sensor instead). |
| **Cannot be pure browser E2E** | Focus-ring coverage assertions (annotated screenshot + manual review or visual diff tool); proving an unwired hook stays unwired (static / unit assertions outside `agent-browser`); proving absence of hidden network calls without a specific user flow (combine `network har start/stop` with grep-based assertions). |

### 6.6 Known UI gaps and implementation truths

Findings from the audit that the test author must internalize before writing assertions:

- **Modal primitive**: Escape key handler is global (`document.addEventListener('keydown', ...)`), so two stacked modals will both close on a single Escape. The backdrop has NO click handler. `document.body.style.overflow` is set to `'hidden'` on mount and `'unset'` on unmount, so closing a top-of-stack modal incorrectly restores body scroll.
- **Plugins page**: `/plugins` renders `PluginRegistry` only. `PluginStatus.tsx` exists in the codebase but is NOT routed and NOT embedded. The `/plugins?tab=chains` query string is NOT read; tab state is local React state initialized to `'registry'`.
- **Per-principal chain insert**: sends NEITHER an `If-Match` header NOR an `expected_revision` body field. Reorder sends `expected_revision` in the body. Delete sends `If-Match`.
- **Principal management mutations** through `useDraftPrincipals` (create / update / enable / disable) send NEITHER `If-Match` NOR `expected_revision` for the principal itself. The enclosing draft PUT carries the draft's `expected_revision`.
- **Apply error**: `useConfigApply` exposes an `error`, but `Settings.tsx` ignores it inside an empty catch block. Apply conflicts therefore do NOT surface in the UI today.
- **Apply success**: only refetches draft and status. The History drawer's revision list is fetched on drawer mount only; it does NOT auto-refresh after an apply.
- **Autosave conflict merge** in `useConfigDraft` is a shallow top-level spread (`{ ...latestData.draft, ...newDraft }`), NOT a deep merge. Nested-object edits collide destructively at the top key level.
- **Upstream row**: `OAuth Expires` and `Created` columns are hard-coded to `-`. Only `name`, `kind`, `status`, `revision`, and actions are dynamic.
- **OAuth upstream creation**: the upstream is created on `Create` press, BEFORE the Connect OAuth follow-up modal opens. Dismissing the modal leaves an unconnected upstream in the table.
- **Upstream mutation failures** (Enable / Disable / Edit / Delete) surface via `window.alert(err.message)`. Create failures use the dialog's inline error state. There is NO global toast system.
- **Number-field validation** in the schema form uses the browser's native constraint-validation only (`min` / `max` on the `<input>`); the entered value is still persisted into the local draft and autosaved on debounce.
- **Required-field "validation"**: most forms (New Principal, Issue Key, Connect OAuth Complete, Add Plugin) DISABLE their submit button when invalid. They do NOT render a validation message.
- **Empty states for shared tables** (`Table.tsx`) render the generic literal `No data available`. Feature-specific empty states only appear when the page renders custom empty UI (e.g. Upstreams shows `No upstreams configured`, Plugins shows `No plugins uploaded yet.`, Audit shows `No activity found`).
- **In-use plugin delete**: the button is disabled and exposes a native `title` attribute such as `In use by <N> chains`. There is no custom tooltip component.
- **Credential row associated principals**: shows a count like `2 principals`. Names are not enumerated.
- **Realtime Log recent events**: fetched on mount and on filter change. NOT polled. SSE is the sole source of new rows once the page is mounted.
- **Topbar connection indicator**: driven solely by `useDashboardConnection`, which probes `/admin/health` periodically and opens the global SSE stream. Per-call 401s do NOT flip the indicator; they only throw `ApiError` to the calling component.
- **OAuth popup fallback**: if `window.open` returns null, the current window navigates to the authorize URL.
- **Dead / unwired code**: `PrincipalCreateDialog.tsx`, `UpstreamCard.tsx`, `PluginStatus.tsx`, `useGcOrphaned`, `useExport`, `useUpstreamHealth` all exist in the codebase but are not reachable from the rendered UI.

### 6.7 Audit history

| Round | Agents | Outcome |
|---|---|---|
| 1 | 6 parallel explore + librarian agents (Map features, BDD primer research, Rust admin API surface, fetch endpoints, forms/inputs/buttons, tests/lib/state) | Initial 55-feature / 202-scenario document. |
| 2 | 4 parallel agents with different models (Oracle: spec-vs-code correctness; Ultrabrain: Gherkin discipline; Artistry: adversarial edge cases; Deep: Playwright implementability) | 31 factual errors corrected, 13 missing scenarios added, 8 adversarial scenarios added, Per-Principal Chains routing facts corrected, ETag/If-Match contract split per resource, universal-retry contract split, Plugin Status panel marked `@wip`, polling table corrected. Test-file split and seed-data appendix added. |
| 3 | Operator directive | Execution surface switched from Playwright to `agent-browser`. Part 6 appendix rewritten as `agent-browser` harness guide: shell-script split, snapshot-and-ref discipline, Playwright→`agent-browser` translation table, bash `assert_*` helper conventions, harness-work classification reframed for CDP-via-CLI. The behavior contract in Parts 1–5 is harness-agnostic and was not touched. |
- [ ] `When` has at most one or two action steps; otherwise split.
