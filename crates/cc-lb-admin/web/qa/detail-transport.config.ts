import { existsSync, readFileSync, statSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { defineConfig } from '@playwright/test';

const qaDir = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(qaDir, '../../../..');

function required(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function loopbackOrigin(value: string, name: string): string {
  const url = new URL(value);
  if (!['127.0.0.1', 'localhost', '::1'].includes(url.hostname))
    throw new Error(`${name} must use a loopback host`);
  if (url.username || url.password || url.pathname !== '/' || url.search || url.hash)
    throw new Error(`${name} must be a credential-free origin`);
  return url.origin;
}

const manifestPath = path.resolve(required('DETAIL_FIXTURE_MANIFEST'));
if (!existsSync(manifestPath) || !statSync(manifestPath).isFile())
  throw new Error(`fixture manifest does not exist: ${manifestPath}`);
const manifest = JSON.parse(readFileSync(manifestPath, 'utf8')) as {
  origins?: { direct?: string };
};
const directOrigin = loopbackOrigin(
  manifest.origins?.direct ?? required('DETAIL_DIRECT_ORIGIN'),
  'DETAIL_DIRECT_ORIGIN',
);
const evidenceDir = path.resolve(
  process.env.DETAIL_EVIDENCE_DIR ?? path.join(repoRoot, 'target', 'detail-transport-qa'),
);

export default defineConfig({
  testDir: qaDir,
  testMatch: 'detail-transport.spec.ts',
  outputDir: path.join(evidenceDir, 'playwright-output'),
  fullyParallel: false,
  forbidOnly: true,
  retries: 0,
  workers: 1,
  reporter: [['line']],
  timeout: 120_000,
  expect: { timeout: 15_000 },
  use: {
    baseURL: directOrigin,
    trace: 'off',
    screenshot: 'off',
    video: 'off',
  },
  webServer: undefined,
});
