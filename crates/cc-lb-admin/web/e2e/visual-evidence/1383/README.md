# PR #1383 visual evidence

Captured from PR head `725539be4a37b367772b61add14e2f8ab626f112` against the built Admin Web SPA served via Vite preview (`bun run preview`) with sanitized Playwright route fixtures.

Capture script: an out-of-tree Playwright script. Browser: Chromium. Viewport height: 900px.

| Surface | File | Notes |
|---|---|---|
| Request logs table (mobile dark, 390px) | `after-logs-table-390.png` | Shows `<$0.0001` sub-cent costs, OpenAI/Anthropic/Gemini logos, inferred `gpt-5.4` logo on `count_tokens`, and omitted logo on unrouted `404` row |
| Request logs table (tablet dark, 768px) | `after-logs-table-768.png` | Compact table with zero horizontal overflow (`768 === 768`) |
| Request logs table (desktop dark, 1440px) | `after-logs-table-1440.png` | Full table showing `<$0.0001` cells, `0` logos on unrouted `404`/`400` rows, and OpenAI logo on `gpt-5.4` `count_tokens` |
| Request logs table (desktop light, 1440px) | `after-logs-table-light-1440.png` | Light theme verification across all states |
| Cost breakdown popover (desktop dark, 1440px) | `after-cost-popover-1440.png` | Shows `Input <$0.0001 (100%)` and `Total <$0.0001` with zero-cost categories omitted |
| Request detail drawer for plain HTTP (desktop dark, 1440px) | `after-http-drawer-1440.png` | Shows `Protocol HTTP`, `Cost Input <$0.0001`, and `Request timeline` (`Upstream 190 ms · 46%`, `Body 165 ms · 40%`) |

All captures passed horizontal-overflow checks (`scrollWidth <= innerWidth`), zero console/page errors, zero `$0.0000` occurrences, and visual inspection of every PNG. No credentials or production data appear.
