import { defineConfig } from '@playwright/test';

// Mocked-network specs: every spec routes `/admin/**` itself, so no backend
// runs. The real-server API-key suite has its own config
// (playwright.api-key.config.ts) and is excluded here.
export default defineConfig({
  testDir: './e2e',
  testIgnore: 'api-key/**',
  fullyParallel: false,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: 1,
  reporter: 'line',
  use: {
    baseURL: 'http://localhost:5173',
    trace: 'on',
    screenshot: 'on',
  },
  webServer: {
    command: 'bun run dev',
    port: 5173,
    reuseExistingServer: !process.env.CI,
  },
});
