Before/after captures for PR #1210. Synthetic data only; every admin API call is mocked in `capture.spec.ts`.

Run master's admin SPA on 127.0.0.1:5181 and the PR branch's on 127.0.0.1:5182 (`bunx --bun vite --host 127.0.0.1 --port <port> --strictPort`), then:

    BASE_URL=http://127.0.0.1:5181 SIDE=before playwright test -c playwright.config.ts
    BASE_URL=http://127.0.0.1:5182 SIDE=after  playwright test -c playwright.config.ts
