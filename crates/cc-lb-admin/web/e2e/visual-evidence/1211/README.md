# Issue #1211 visual evidence

Captured from PR head `ffdebd5d` with the repository's Playwright OpenAI-only admin fixture.

Command:

```sh
bunx playwright test e2e/openai-keepalive.spec.ts --config=playwright.config.ts
```

| Viewport | Card | Settings | Sessions |
|---|---|---|---|
| 390x844 | `cc-lb-1211-390-card.png` | `cc-lb-1211-390-settings.png` | `cc-lb-1211-390-sessions.png` |
| 768x1024 | `cc-lb-1211-768-card.png` | `cc-lb-1211-768-settings.png` | `cc-lb-1211-768-sessions.png` |
| 1440x900 | `cc-lb-1211-1440-card.png` | `cc-lb-1211-1440-settings.png` | `cc-lb-1211-1440-sessions.png` |

The flow checks the OpenAI Responses GPT-5.6+ switch, the principal-wide sessions label, the responsive card and drawer surfaces, and no horizontal overflow at each viewport.
