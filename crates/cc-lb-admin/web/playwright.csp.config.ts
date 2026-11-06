import { defineConfig } from '@playwright/test';

const baseURL = process.env.CC_LB_CSP_BASE_URL;
const adminToken = process.env.CC_LB_CSP_ADMIN_TOKEN;

if (!baseURL) {
  throw new Error('CC_LB_CSP_BASE_URL is required for the packaged CSP suite');
}
if (!adminToken) {
  throw new Error('CC_LB_CSP_ADMIN_TOKEN is required for the packaged CSP suite');
}

export default defineConfig({
  testDir: './e2e',
  testMatch: 'csp.spec.ts',
  fullyParallel: false,
  forbidOnly: true,
  retries: 0,
  workers: 1,
  reporter: 'line',
  use: {
    baseURL,
    browserName: 'chromium',
    trace: 'on',
    screenshot: 'on',
  },
});
