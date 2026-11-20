import { defineConfig } from '@playwright/test';

// Mocked-network specs: every spec routes `/admin/**` itself, so no backend
// runs. The real-server API-key suite has its own config
// (playwright.api-key.config.ts) and is excluded here.
export default defineConfig({
  testDir: './e2e',
  testIgnore: ['api-key/**', 'csp.spec.ts'],
  fullyParallel: process.env.PW_FULLY_PARALLEL === '1' || Boolean(process.env.CI),
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.PW_WORKERS ? Number(process.env.PW_WORKERS) : (process.env.CI ? 4 : 1),
  reporter: 'line',
  use: {
    baseURL: 'http://localhost:5173',
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
  },
  webServer: {
    command:
      process.env.PW_USE_PREVIEW === '1'
        ? 'bun run preview -- --host 0.0.0.0 --port 5173 --strictPort'
        : 'bun run dev',
    port: 5173,
    reuseExistingServer: !process.env.CI,
  },
});
