import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './e2e',
  fullyParallel: false,
  forbidOnly: !!process.env.CI,
  retries: 0,
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