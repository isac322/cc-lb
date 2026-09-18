---
name: frontend-fanout-qa
description: Use this skill at the start of any frontend work that can affect UI/UX, visual design, layout, styling, interaction behavior, responsive behavior, themes, modals, forms, tables, charts, navigation, empty/error/loading states, or user-facing component semantics. This is an execution-loop skill, not post-hoc QA only: it owns requirement capture, proportional implementation, real browser QA, and fix/reverify until the UI/UX checklist is green. Use it even for small frontend changes when users may see, click, understand, or visually trust something differently. Do not use for purely non-visual code changes or trivial copy edits that cannot affect layout, meaning, or UX.
---

# Frontend UI/UX QA loop

This skill captures the workflow extracted from session `ses_example00000000`, where the user repeatedly corrected agents that skipped requirement tracking, treated UI/UX as superficial styling, claimed verification without opening the UI, delegated visual work poorly, or skipped checklist-based regression coverage.

Use it to make every UI/UX-affecting frontend change traceable from user request to browser-proven behavior. A full redesign is only the largest case; the same discipline applies to any visual or interaction change.

## Role and call timing

Call this skill before implementation starts whenever the request can affect what users see, click, understand, or visually trust. Do not wait until after code is written.

This skill owns the frontend UI/UX change loop end to end:

1. capture the user requirement and completion criteria;
2. identify affected surfaces, states, breakpoints, and regressions;
3. choose a proportional implementation path;
4. implement or delegate the change;
5. exercise the actual browser surface;
6. fix and reverify until the relevant checklist is green;
7. report outcome, evidence, and limits.

If the work later turns out to be purely non-visual logic, hand off to the normal implementation flow. If the work still has user-visible UI/UX impact, keep this skill active through final browser QA.

## Core principle

A frontend UI/UX change is not done when code compiles. It is done when the requested user-facing change is recorded, the affected surfaces are identified, the implementation is proportional to the request, the actual browser surface has been exercised, and every relevant checklist item is green.

Do not shrink UI/UX work into isolated code edits. If a change affects what users see, click, understand, or trust, treat it as a frontend surface change with observable behavior.

## When this skill applies

Use this skill for frontend work involving any of these:

- visual design, spacing, color, typography, density, hierarchy, or layout;
- component structure, information architecture, navigation, panels, drawers, tabs, or command palettes;
- modals, confirmations, forms, tables, charts, badges, status indicators, toasts, tooltips, or empty/error/loading states;
- hover, focus, active, selected, disabled, drag, scroll, overflow, truncation, keyboard, or mobile states;
- light/dark/system themes or semantic design tokens;
- UX around real backend/API state, especially where fake success or generic errors could mislead users;
- any user request that says the UI is bad, confusing, ugly, broken, hard to use, or needs browser verification.

Do not use it for purely internal frontend logic, type errors, refactors, or copy edits that cannot change layout, user meaning, interaction, or visual QA needs. If a text change changes button meaning, wraps/truncates, shifts layout, or affects user understanding, use the skill.

## Start by recording requirements

Before editing, create a requirement checklist. Scale the checklist to the task size, but never skip it.

Include:

- the exact user-requested change;
- affected pages, components, states, and breakpoints;
- existing behavior that must not regress;
- any design-system or token constraints;
- backend/API state the UI depends on;
- what browser evidence will prove the change worked.

For long or multi-agent work, save this in a durable file such as `.omo/plans/frontend-checklist.md` or `/tmp/<project>-frontend-checklist.md`. For small work, a concise todo/checklist in the working notes is enough, but it must still exist.

In multi-turn sessions where requirements evolve across many messages, periodically re-audit by scanning the entire session history from the first message, consolidating every explicit and implicit requirement into a single master checklist, and verifying the current codebase against it. Do not declare the task complete until every item is verified as active and non-regressed.

## Decide the proportional workflow

Choose the smallest workflow that still proves the user-facing result.

- **Small visual/UX change**: one focused implementer, affected callsite inventory, targeted browser QA on the changed component and nearby states.
- **Medium component/page change**: use `visual-engineering` with `frontend-ui-ux` and `agent-browser`; verify all variants and responsive/theme states touched by the change.
- **Broad redesign or unclear design direction**: research references, create a mockup or design direction first, then implement only after approval.
- **Integration-facing UX**: compare legacy behavior and trace backend/API truth before changing UI state rules.
- **Large/multi-surface work**: fan out inventory, implementation, browser QA, and fix slices across subagents.

## Mockup approval gate

When the user says not to implement yet, asks for references, or wants to inspect a web mockup before production work, treat this as a hard gate:

- Defer production code edits until the user explicitly approves the standalone mockup.
- Build the mockup in an isolated temporary or staging directory and open it in a real browser for visual review.
- Create the later implementation checklist, but state that production implementation is not complete, not done, and must not be claimed before production files are changed and browser QA passes.
- Mockup approval is only design-direction approval. It is not production QA and cannot satisfy the final browser QA gate.

## Delegation pattern

Use fan-out when it improves coverage or speed, not as theater. Each worker must own an observable slice.

- UI/UX/design/styling work should prefer `category="visual-engineering"` with `frontend-ui-ux` and `agent-browser` when available.
- Browser QA workers must run `agent-browser` themselves. Do not centralize all screenshots in the lead agent and hand static images to visual workers unless the worker truly cannot drive a browser.
- Split large work into independent slices:
  - requirement and legacy inventory;
  - affected component/page implementation;
  - browser verification by page/component/state group;
  - multimodal screenshot review when useful;
  - focused fix workers with non-overlapping file scopes.
- If a subagent aborts or hangs, cancel/relaunch a smaller browser-capable task. Do not replace browser QA with grep, typecheck, or source reading.

Every delegated prompt should include context, goal, downstream use, request, exact file scope, required browser surface, and report format.

## Implementation loop

1. Inventory first:
   - read affected routes/components/styles/tokens;
   - find callsites and variants;
   - map API contracts and mutation/error behavior when the UI depends on backend truth;
   - identify real user flows, not just files.
2. Implement proportionally:
   - follow the existing design system and component patterns;
   - keep obvious single-use logic local;
   - avoid speculative rewrites around a small request;
   - do not add backend scope casually. If backend work is required, state exactly why it is necessary for the user-visible UI result.
3. Preserve UX quality:
   - avoid native `confirm()` in polished dashboards when a proper dialog is expected;
   - use accessible modals/dialogs with title, body, destructive/non-destructive variants, keyboard behavior, and focus handling;
   - use real backend status/error truth instead of fake green/default success;
   - use semantic tokens/classes and remove hardcoded light/dark remnants when theme is affected;
   - write all hardcoded user-facing strings in the project's chosen language. Locale-aware dynamic formatting (e.g., `Intl.RelativeTimeFormat` outputting localized text) and native locale labels in selectors (e.g., `한국어 (ko-KR)`) are exempt.

## Browser QA loop

Run browser QA after every meaningful UI/UX implementation batch. Build/typecheck is only a gate to QA, not proof of UI correctness.

For fast feedback during visual tuning, iterate by injecting CSS/DOM changes directly in the live browser via the browser automation tool. Once values are decided, commit them to source, perform a full build and restart, and re-verify in a fresh browser session to confirm the persisted state matches the iteration. Do not return before verifying the persisted state.

1. Launch the matching surface:
   - production-like service when the user cares about real behavior;
   - dev server plus real backend when integration matters;
   - mock server only when the user explicitly approved mock-only work.
2. Exercise the affected surfaces in a real browser.
3. Cover the states relevant to the change, such as:
   - desktop and mobile breakpoints;
   - light/dark/system theme when touched;
   - hover/focus/active/selected/disabled;
   - modals, drawers, forms, tables, charts, status badges, toasts, tooltips;
   - empty/error/loading states;
   - scroll, overflow, truncation, responsive navigation.
4. Compile failures into a bug ledger with owner, evidence, fix, and reverify status.
5. Fix, rebuild/redeploy if needed, and reverify until the relevant checklist is green.

## Things the session taught the hard way

- Do not claim browser verification unless you actually opened the browser and interacted with the app.
- Do not treat UI/UX as theme polish or a code-only edit.
- Do not skip requirement capture; the checklist is the contract.
- Do not rely on mock success when the user cares about the real service.
- Do not skip legacy comparison; zero regression requires an explicit inventory.
- Do not centralize all screenshots in the lead agent when browser-capable subagents can self-verify.
- Do not add backend scope casually during a frontend task. If a backend change is needed, state why and keep it tied to the user-visible requirement.
- Do not show successful status from fake/default state. Status indicators must reflect the real backend condition they claim to represent.

## Success report format

Report in this order:

1. User-visible outcome: what now works.
2. Requirement/checklist coverage: requested items, affected surfaces, pass/fail count, unresolved items.
3. Browser QA evidence: exact surfaces, states, viewports, and interactions exercised.
4. Verification commands: typecheck/build/tests and their results.
5. Known limits: anything not verified and why.

## Trigger examples

Use this skill for prompts like:

- "이 버튼 상태가 이상해. hover/focus/disabled까지 자연스럽게 고쳐."
- "모달이 후져 보이는데 디자인 맞춰서 바꾸고 실제로 열어봐."
- "대시보드 UI/UX 전부 갈아엎어. agent-browser로 직접 보면서 고쳐."
- "모바일에서 테이블이 깨져. 체크리스트 만들고 브라우저로 검증해."
- "OAuth status가 fake green으로 보여. 실제 상태 기준으로 UI 고쳐."

Do not use it for prompts like:

- "타입 에러 하나 고쳐줘" with no user-visible UI impact.
- "변수명 바꿔줘" with no visual or UX effect.
- "문구 오타 하나 고쳐줘" unless the text change affects layout, meaning, or interaction.
