import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: '/tmp/cclb-kafix-capture',
  timeout: 30_000,
  workers: 1,
  use: {
    browserName: 'chromium',
    headless: true,
    baseURL: process.env.BASE_URL ?? 'http://127.0.0.1:5182',
    colorScheme: 'dark',
  },
});
