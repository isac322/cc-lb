# Extracted source session notes

Source session: `ses_example00000000`.
Coverage verified by session metadata: 1008 messages from 2026-05-31T14:48:51.100Z to 2026-06-01T10:46:16.464Z.

Key user corrections preserved:
- The user first asked for a full cc-lb dashboard UI/UX overhaul through a real local systemd service and agent-browser.
- The user redirected the agent to research references and show a web mockup before production implementation.
- The user rejected theme-only work and clarified that UI/UX meant layout, usability, widgets, and information architecture.
- The user caught fake verification: the agent had not actually opened agent-browser despite implying it had.
- The user insisted on visual-engineering delegation for visual work.
- The user required direct browser verification after coding, including every React component and mobile state.
- The user required a master checklist and cross-check against previous functionality.
- The user corrected an inefficient pattern where the lead agent captured all screenshots instead of browser-capable subagents self-verifying.
- The user required system/dark/light modes, system default, independent scrolling in principal split view, no dark-mode remnants, and all modals/hover/text/chart states checked.
- The user caught real integration regressions around admin token prompting, OAuth no-response, pending OAuth upstream visibility, fake OAuth status, API key paste support, last-4 display, native confirm dialogs, and first-item selection.
- Corrected workflow became: extract requirements -> fan out discovery -> compile checklist -> fan out browser QA -> fan out fixes -> rebuild/redeploy -> reverify until green.

Important paths from the source work:
- `crates/cc-lb-admin/web/`
- `crates/cc-lb-admin/web/src/routes/`
- `crates/cc-lb-admin/web/src/components/AuthRequiredGate.tsx`
- `crates/cc-lb-admin/web/src/components/ThemeToggle.tsx`
- `crates/cc-lb-admin/web/src/components/ConfirmDialog.tsx`
- `crates/cc-lb-admin/web/src/lib/api.ts`
