// Real-server Playwright suite for Anthropic API-key upstreams (T2, plan
// §2 S6a, OI-3 local gate). Global setup builds the SPA, proves the bundle
// carries no preview code (PRE-01), and starts fake-anthropic, `cc-lb serve`
// (tmp SQLite, static admin token) and `vite preview` on free loopback ports.
// Anthropic-host-only states use FX-PW `page.route` envelopes; nothing here
// imports `src/preview/` (it no longer exists).
//
//   cargo build -p cc-lb-server -p fake-anthropic
//   bunx playwright test -c playwright.api-key.config.ts
//   bunx playwright test -c playwright.api-key.config.ts --repeat-each=20  # FLK-06

import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './e2e/api-key',
  testMatch: '*.spec.ts',
  fullyParallel: false,
  forbidOnly: true,
  retries: 0,
  workers: 1,
  reporter: [['line']],
  timeout: 60_000,
  expect: { timeout: 15_000 },
  globalSetup: './e2e/api-key/global-setup.ts',
  globalTeardown: './e2e/api-key/global-teardown.ts',
  use: {
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    video: 'off',
    // FLK-06: no reliance on animation timing.
    contextOptions: { reducedMotion: 'reduce' },
    viewport: { width: 1440, height: 900 },
  },
});
